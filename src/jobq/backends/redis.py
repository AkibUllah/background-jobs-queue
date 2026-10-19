"""Redis backend.

Every state transition that touches more than one key (enqueue with unique
check and dependency wiring, claim, complete with dependent release, fail with
dead-letter cascade, promotion, stalled recovery) is a single Lua script, so
concurrent workers and producers never observe a half-applied transition and
no locks are needed.

Key layout (``p`` is the configurable prefix, default ``jobq:``)::

    p job:<id>          HASH   job fields (see Job.to_fields)
    p q:<queue>         ZSET   ready jobs, score = -priority * 1e10 + seq
    p queues            SET    known queue names
    p seq               STRING monotonic counter for FIFO ordering
    p scheduled         ZSET   jobs waiting for run_at (score = run_at)
    p waiting           ZSET   jobs waiting on dependencies
    p running           ZSET   claimed jobs, score = last heartbeat
    p done / dead / all ZSET   indexes for the dashboard
    p unique            HASH   unique_key -> job id
    p deps:<id>         SET    jobs that wait for <id>
    p workers           HASH   worker id -> JSON, with p worker_hb ZSET heartbeats
    p schedules         HASH   name -> JSON, with p schedule_next ZSET next run
    p m:<minute>        HASH   per-minute counters (enqueued/succeeded/failed/retried)
"""

from __future__ import annotations

import json
from typing import Any

from redis import asyncio as aioredis

from jobq.backends.base import EnqueueOutcome, minute_of
from jobq.job import Job, JobStatus, MinuteBucket, QueueStats, Schedule, WorkerInfo

LUA_HELPERS = r"""
local p = ARGV[1]
local unpack = unpack or table.unpack
local function jobkey(id) return p .. 'job:' .. id end
local function bump(now, field)
  local k = p .. 'm:' .. math.floor(tonumber(now) / 60)
  redis.call('HINCRBY', k, field, 1)
  redis.call('EXPIRE', k, 10800)
end
local function push(id, queue, priority, front)
  local seq = tonumber(redis.call('INCR', p .. 'seq'))
  if front then seq = -seq end
  local score = -tonumber(priority) * 1e10 + seq
  redis.call('ZADD', p .. 'q:' .. queue, score, id)
  redis.call('SADD', p .. 'queues', queue)
  redis.call('HSET', jobkey(id), 'status', 'queued')
end
local function activate(id, now)
  local k = jobkey(id)
  local run_at = redis.call('HGET', k, 'run_at')
  local attempts = tonumber(redis.call('HGET', k, 'attempts') or '0') or 0
  if run_at and run_at ~= '' and tonumber(run_at) > tonumber(now) then
    redis.call('ZADD', p .. 'scheduled', tonumber(run_at), id)
    if attempts > 0 then
      redis.call('HSET', k, 'status', 'retrying')
    else
      redis.call('HSET', k, 'status', 'scheduled')
    end
  else
    local queue = redis.call('HGET', k, 'queue') or 'default'
    local priority = redis.call('HGET', k, 'priority') or '0'
    push(id, queue, priority, false)
  end
end
local function clear_unique(id)
  local uk = redis.call('HGET', jobkey(id), 'unique_key')
  if uk and uk ~= '' then
    local owner = redis.call('HGET', p .. 'unique', uk)
    if owner == id then redis.call('HDEL', p .. 'unique', uk) end
  end
end
local function mark_dead(id, status, err, now)
  local stack = {{id, status, err}}
  while #stack > 0 do
    local item = table.remove(stack)
    local cid, cstatus, cerr = item[1], item[2], item[3]
    local k = jobkey(cid)
    local queue = redis.call('HGET', k, 'queue')
    redis.call('HSET', k, 'status', cstatus, 'error', cerr, 'finished_at', now)
    redis.call('ZREM', p .. 'running', cid)
    redis.call('ZREM', p .. 'waiting', cid)
    redis.call('ZREM', p .. 'scheduled', cid)
    if queue then redis.call('ZREM', p .. 'q:' .. queue, cid) end
    redis.call('ZADD', p .. 'dead', tonumber(now), cid)
    clear_unique(cid)
    local ttl = tonumber(redis.call('HGET', k, 'result_ttl') or '86400') or 86400
    redis.call('EXPIRE', k, ttl)
    local dependents = redis.call('SMEMBERS', p .. 'deps:' .. cid)
    redis.call('DEL', p .. 'deps:' .. cid)
    for _, d in ipairs(dependents) do
      if redis.call('HGET', jobkey(d), 'status') == 'waiting' then
        table.insert(stack, {d, 'cancelled', 'dependency ' .. cid .. ' ' .. cstatus})
      end
    end
  end
end
"""

LUA_ENQUEUE = (
    LUA_HELPERS
    + r"""
local now, id = ARGV[2], ARGV[3]
local k = jobkey(id)
local fields = {}
local unique_key, depends_on, queue = '', '', 'default'
for i = 4, #ARGV, 2 do
  fields[#fields + 1] = ARGV[i]
  fields[#fields + 1] = ARGV[i + 1]
  if ARGV[i] == 'unique_key' then unique_key = ARGV[i + 1] end
  if ARGV[i] == 'depends_on' then depends_on = ARGV[i + 1] end
  if ARGV[i] == 'queue' then queue = ARGV[i + 1] end
end
if unique_key ~= '' then
  local existing = redis.call('HGET', p .. 'unique', unique_key)
  if existing then
    local st = redis.call('HGET', jobkey(existing), 'status')
    local active = st and st ~= 'succeeded' and st ~= 'failed' and st ~= 'cancelled'
    if active then
      return {existing, 'deduped'}
    end
  end
  redis.call('HSET', p .. 'unique', unique_key, id)
end
redis.call('HSET', k, unpack(fields))
redis.call('ZADD', p .. 'all', tonumber(now), id)
redis.call('ZREMRANGEBYRANK', p .. 'all', 0, -50001)
redis.call('SADD', p .. 'queues', queue)
bump(now, 'enqueued')
local pending = 0
if depends_on ~= '' then
  for dep in string.gmatch(depends_on, '[^,]+') do
    local st = redis.call('HGET', jobkey(dep), 'status')
    if st == false or st == 'succeeded' then
      -- already satisfied
    elseif st == 'failed' or st == 'cancelled' then
      mark_dead(id, 'cancelled', 'dependency ' .. dep .. ' ' .. st, now)
      return {id, 'cancelled'}
    else
      pending = pending + 1
      redis.call('SADD', p .. 'deps:' .. dep, id)
    end
  end
end
redis.call('HSET', k, 'deps_remaining', tostring(pending))
if pending > 0 then
  redis.call('HSET', k, 'status', 'waiting')
  redis.call('ZADD', p .. 'waiting', tonumber(now), id)
else
  activate(id, now)
end
return {id, 'enqueued'}
"""
)

LUA_CLAIM = (
    LUA_HELPERS
    + r"""
local now, worker = ARGV[2], ARGV[3]
for i = 4, #ARGV do
  local qk = p .. 'q:' .. ARGV[i]
  local popped = redis.call('ZPOPMIN', qk)
  while popped and #popped > 0 do
    local id = popped[1]
    local k = jobkey(id)
    if redis.call('EXISTS', k) == 1 then
      redis.call('HSET', k, 'status', 'running', 'worker_id', worker)
      redis.call('HSET', k, 'started_at', now)
      redis.call('HINCRBY', k, 'attempts', 1)
      redis.call('ZADD', p .. 'running', tonumber(now), id)
      return redis.call('HGETALL', k)
    end
    popped = redis.call('ZPOPMIN', qk)
  end
end
return nil
"""
)

LUA_COMPLETE = (
    LUA_HELPERS
    + r"""
local now, id, result = ARGV[2], ARGV[3], ARGV[4]
local k = jobkey(id)
if redis.call('EXISTS', k) == 0 then return 0 end
redis.call('ZREM', p .. 'running', id)
redis.call('HSET', k, 'status', 'succeeded', 'result', result, 'error', '')
redis.call('HSET', k, 'finished_at', now)
redis.call('ZADD', p .. 'done', tonumber(now), id)
redis.call('ZREMRANGEBYRANK', p .. 'done', 0, -10001)
clear_unique(id)
local ttl = tonumber(redis.call('HGET', k, 'result_ttl') or '86400') or 86400
redis.call('EXPIRE', k, ttl)
bump(now, 'succeeded')
local dependents = redis.call('SMEMBERS', p .. 'deps:' .. id)
redis.call('DEL', p .. 'deps:' .. id)
for _, d in ipairs(dependents) do
  local dk = jobkey(d)
  if redis.call('HGET', dk, 'status') == 'waiting' then
    local remaining = tonumber(redis.call('HINCRBY', dk, 'deps_remaining', -1))
    if remaining <= 0 then
      redis.call('ZREM', p .. 'waiting', d)
      activate(d, now)
    end
  end
end
return 1
"""
)

LUA_FAIL = (
    LUA_HELPERS
    + r"""
local now, id, err, retry_at = ARGV[2], ARGV[3], ARGV[4], ARGV[5]
local k = jobkey(id)
if redis.call('EXISTS', k) == 0 then return 0 end
redis.call('ZREM', p .. 'running', id)
if retry_at ~= '' then
  redis.call('HSET', k, 'status', 'retrying', 'error', err, 'run_at', retry_at)
  redis.call('ZADD', p .. 'scheduled', tonumber(retry_at), id)
  bump(now, 'retried')
  return 1
end
mark_dead(id, 'failed', err, now)
bump(now, 'failed')
return 2
"""
)

LUA_RELEASE = (
    LUA_HELPERS
    + r"""
local id = ARGV[2]
local k = jobkey(id)
if redis.call('HGET', k, 'status') ~= 'running' then return 0 end
redis.call('ZREM', p .. 'running', id)
local attempts = tonumber(redis.call('HGET', k, 'attempts') or '0') or 0
if attempts > 0 then redis.call('HSET', k, 'attempts', tostring(attempts - 1)) end
redis.call('HSET', k, 'started_at', '', 'worker_id', '')
local queue = redis.call('HGET', k, 'queue') or 'default'
push(id, queue, redis.call('HGET', k, 'priority') or '0', true)
return 1
"""
)

LUA_PROMOTE = (
    LUA_HELPERS
    + r"""
local now, limit = ARGV[2], tonumber(ARGV[3])
local due = redis.call('ZRANGEBYSCORE', p .. 'scheduled', '-inf', now, 'LIMIT', 0, limit)
for _, id in ipairs(due) do
  redis.call('ZREM', p .. 'scheduled', id)
  local k = jobkey(id)
  if redis.call('EXISTS', k) == 1 then
    local queue = redis.call('HGET', k, 'queue') or 'default'
    push(id, queue, redis.call('HGET', k, 'priority') or '0', false)
  end
end
return #due
"""
)

LUA_RECOVER = (
    LUA_HELPERS
    + r"""
local now, cutoff, stall = ARGV[2], ARGV[3], ARGV[4]
local stalled = redis.call('ZRANGEBYSCORE', p .. 'running', '-inf', cutoff)
local out = {}
for _, id in ipairs(stalled) do
  redis.call('ZREM', p .. 'running', id)
  local k = jobkey(id)
  if redis.call('EXISTS', k) == 1 then
    local worker = redis.call('HGET', k, 'worker_id') or ''
    local err = 'stalled: no heartbeat from worker ' .. worker .. ' for ' .. stall .. 's'
    local attempts = tonumber(redis.call('HGET', k, 'attempts') or '0') or 0
    local max_retries = tonumber(redis.call('HGET', k, 'max_retries') or '0') or 0
    if attempts <= max_retries then
      redis.call('HSET', k, 'error', err, 'worker_id', '', 'started_at', '')
      local queue = redis.call('HGET', k, 'queue') or 'default'
    push(id, queue, redis.call('HGET', k, 'priority') or '0', false)
      bump(now, 'retried')
    else
      mark_dead(id, 'failed', err, now)
      bump(now, 'failed')
    end
    out[#out + 1] = id
  end
end
return out
"""
)

LUA_RETRY = (
    LUA_HELPERS
    + r"""
local now, id = ARGV[2], ARGV[3]
local k = jobkey(id)
local st = redis.call('HGET', k, 'status')
if st ~= 'failed' and st ~= 'cancelled' then return 0 end
redis.call('ZREM', p .. 'dead', id)
redis.call('PERSIST', k)
redis.call('HSET', k, 'attempts', '0', 'error', '', 'finished_at', '')
redis.call('HSET', k, 'started_at', '', 'worker_id', '', 'run_at', '')
local uk = redis.call('HGET', k, 'unique_key')
if uk and uk ~= '' then redis.call('HSET', p .. 'unique', uk, id) end
local queue = redis.call('HGET', k, 'queue') or 'default'
push(id, queue, redis.call('HGET', k, 'priority') or '0', false)
return 1
"""
)

LUA_REQUEUE = (
    LUA_HELPERS
    + r"""
local now, id = ARGV[2], ARGV[3]
local k = jobkey(id)
local st = redis.call('HGET', k, 'status')
if not st or st == 'running' or st == 'queued' then return 0 end
for _, z in ipairs({'scheduled', 'waiting', 'dead', 'done'}) do
  redis.call('ZREM', p .. z, id)
end
local depends_on = redis.call('HGET', k, 'depends_on') or ''
for dep in string.gmatch(depends_on, '[^,]+') do redis.call('SREM', p .. 'deps:' .. dep, id) end
redis.call('PERSIST', k)
redis.call('HSET', k, 'deps_remaining', '0', 'run_at', '', 'error', '', 'finished_at', '')
redis.call('HSET', k, 'started_at', '', 'worker_id', '')
local uk = redis.call('HGET', k, 'unique_key')
if uk and uk ~= '' then redis.call('HSET', p .. 'unique', uk, id) end
local queue = redis.call('HGET', k, 'queue') or 'default'
push(id, queue, redis.call('HGET', k, 'priority') or '0', false)
return 1
"""
)

LUA_DELETE = (
    LUA_HELPERS
    + r"""
local id = ARGV[2]
local k = jobkey(id)
if redis.call('EXISTS', k) == 0 then return 0 end
local queue = redis.call('HGET', k, 'queue')
if queue then redis.call('ZREM', p .. 'q:' .. queue, id) end
for _, z in ipairs({'scheduled', 'waiting', 'running', 'dead', 'done', 'all'}) do
  redis.call('ZREM', p .. z, id)
end
clear_unique(id)
local depends_on = redis.call('HGET', k, 'depends_on') or ''
for dep in string.gmatch(depends_on, '[^,]+') do redis.call('SREM', p .. 'deps:' .. dep, id) end
redis.call('DEL', k)
return 1
"""
)

LUA_CLAIM_SCHEDULES = r"""
local p, now, lock = ARGV[1], ARGV[2], tonumber(ARGV[3])
local due = redis.call('ZRANGEBYSCORE', p .. 'schedule_next', '-inf', now, 'WITHSCORES')
local out = {}
for i = 1, #due, 2 do
  local name, score = due[i], due[i + 1]
  local spec = redis.call('HGET', p .. 'schedules', name)
  if spec then
    redis.call('ZADD', p .. 'schedule_next', tonumber(now) + lock, name)
    out[#out + 1] = name
    out[#out + 1] = score
    out[#out + 1] = spec
  else
    redis.call('ZREM', p .. 'schedule_next', name)
  end
end
return out
"""

STATUS_INDEX = {
    JobStatus.SCHEDULED: "scheduled",
    JobStatus.RETRYING: "scheduled",
    JobStatus.WAITING: "waiting",
    JobStatus.RUNNING: "running",
    JobStatus.SUCCEEDED: "done",
    JobStatus.FAILED: "dead",
    JobStatus.CANCELLED: "dead",
}
LIST_SCAN_CAP = 5000


def _pairs_to_dict(flat: list[str]) -> dict[str, str]:
    return {flat[i]: flat[i + 1] for i in range(0, len(flat), 2)}


class RedisBackend:
    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        *,
        prefix: str = "jobq:",
        client: aioredis.Redis | None = None,
    ) -> None:
        self.prefix = prefix
        self.redis: aioredis.Redis = client or aioredis.Redis.from_url(url, decode_responses=True)
        r = self.redis
        self._enqueue = r.register_script(LUA_ENQUEUE)
        self._claim = r.register_script(LUA_CLAIM)
        self._complete = r.register_script(LUA_COMPLETE)
        self._fail = r.register_script(LUA_FAIL)
        self._release = r.register_script(LUA_RELEASE)
        self._promote = r.register_script(LUA_PROMOTE)
        self._recover = r.register_script(LUA_RECOVER)
        self._retry = r.register_script(LUA_RETRY)
        self._requeue = r.register_script(LUA_REQUEUE)
        self._delete = r.register_script(LUA_DELETE)
        self._claim_schedules = r.register_script(LUA_CLAIM_SCHEDULES)

    # ------------------------------------------------------------ helpers
    def key(self, *parts: str) -> str:
        return self.prefix + ":".join(parts)

    async def _job_from_key(self, job_id: str) -> Job | None:
        fields = await self.redis.hgetall(self.key("job", job_id))
        return Job.from_fields(fields) if fields else None

    # ------------------------------------------------------------ producer
    async def enqueue(self, job: Job, now: float) -> tuple[Job, EnqueueOutcome]:
        job.created_at = now
        flat: list[str] = []
        for k, v in job.to_fields().items():
            flat.extend((k, v))
        result = await self._enqueue(keys=[], args=[self.prefix, repr(now), job.id, *flat])
        job_id, outcome = result[0], EnqueueOutcome(result[1])
        stored = await self._job_from_key(job_id)
        return (stored or job), outcome

    # ------------------------------------------------------------ worker
    async def claim(self, queues: list[str], worker_id: str, now: float) -> Job | None:
        flat = await self._claim(keys=[], args=[self.prefix, repr(now), worker_id, *queues])
        if not flat:
            return None
        return Job.from_fields(_pairs_to_dict(flat))

    async def heartbeat(self, job_ids: list[str], now: float) -> None:
        if not job_ids:
            return
        await self.redis.zadd(self.key("running"), {jid: now for jid in job_ids}, xx=True)

    async def complete(self, job_id: str, result: Any, now: float) -> None:
        payload = "" if result is None else json.dumps(result, default=repr, separators=(",", ":"))
        await self._complete(keys=[], args=[self.prefix, repr(now), job_id, payload])

    async def fail(self, job_id: str, error: str, now: float, retry_at: float | None) -> None:
        retry = "" if retry_at is None else repr(float(retry_at))
        await self._fail(keys=[], args=[self.prefix, repr(now), job_id, error, retry])

    async def release(self, job_id: str) -> None:
        await self._release(keys=[], args=[self.prefix, job_id])

    async def promote_scheduled(self, now: float, limit: int = 1000) -> int:
        return int(await self._promote(keys=[], args=[self.prefix, repr(now), limit]))

    async def recover_stalled(self, now: float, stall_timeout: float) -> list[str]:
        cutoff = now - stall_timeout
        out = await self._recover(
            keys=[], args=[self.prefix, repr(now), repr(cutoff), f"{stall_timeout:.0f}"]
        )
        return list(out or [])

    # ------------------------------------------------------------ workers
    async def register_worker(self, info: WorkerInfo, now: float) -> None:
        info.heartbeat_at = now
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(self.key("workers"), info.id, json.dumps(info.to_dict()))
            pipe.zadd(self.key("worker_hb"), {info.id: now})
            await pipe.execute()

    async def deregister_worker(self, worker_id: str) -> None:
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hdel(self.key("workers"), worker_id)
            pipe.zrem(self.key("worker_hb"), worker_id)
            await pipe.execute()

    async def list_workers(self, now: float, stale_after: float = 60.0) -> list[WorkerInfo]:
        cutoff = now - stale_after
        stale = await self.redis.zrangebyscore(self.key("worker_hb"), "-inf", cutoff)
        if stale:
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.hdel(self.key("workers"), *stale)
                pipe.zrem(self.key("worker_hb"), *stale)
                await pipe.execute()
        raw = await self.redis.hgetall(self.key("workers"))
        workers = [WorkerInfo.from_dict(json.loads(v)) for v in raw.values()]
        return sorted(workers, key=lambda w: w.started_at)

    # ------------------------------------------------------------ schedules
    async def save_schedule(self, schedule: Schedule) -> None:
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(self.key("schedules"), schedule.name, schedule.to_json())
            if schedule.enabled and schedule.next_run is not None:
                pipe.zadd(self.key("schedule_next"), {schedule.name: schedule.next_run})
            else:
                pipe.zrem(self.key("schedule_next"), schedule.name)
            await pipe.execute()

    async def list_schedules(self) -> list[Schedule]:
        raw = await self.redis.hgetall(self.key("schedules"))
        schedules = [Schedule.from_json(v) for v in raw.values()]
        return sorted(schedules, key=lambda s: s.name)

    async def delete_schedule(self, name: str) -> bool:
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hdel(self.key("schedules"), name)
            pipe.zrem(self.key("schedule_next"), name)
            removed, _ = await pipe.execute()
        return bool(removed)

    async def claim_due_schedules(self, now: float, lock: float) -> list[tuple[Schedule, float]]:
        out = await self._claim_schedules(keys=[], args=[self.prefix, repr(now), repr(lock)])
        due: list[tuple[Schedule, float]] = []
        for i in range(0, len(out or []), 3):
            schedule = Schedule.from_json(out[i + 2])
            due.append((schedule, float(out[i + 1])))
        return due

    async def set_schedule_next(self, name: str, next_run: float, last_run: float | None) -> None:
        raw = await self.redis.hget(self.key("schedules"), name)
        if raw is None:
            return
        schedule = Schedule.from_json(raw)
        schedule.next_run = next_run
        if last_run is not None:
            schedule.last_run = last_run
        await self.save_schedule(schedule)

    # ------------------------------------------------------------ inspection
    async def get_job(self, job_id: str) -> Job | None:
        return await self._job_from_key(job_id)

    async def _fetch_jobs(self, ids: list[str], index_key: str | None) -> list[Job]:
        if not ids:
            return []
        async with self.redis.pipeline(transaction=False) as pipe:
            for jid in ids:
                pipe.hgetall(self.key("job", jid))
            rows = await pipe.execute()
        jobs: list[Job] = []
        stale: list[str] = []
        for jid, fields in zip(ids, rows, strict=True):
            if fields:
                jobs.append(Job.from_fields(fields))
            else:
                stale.append(jid)
        if stale and index_key:
            await self.redis.zrem(index_key, *stale)
        return jobs

    async def list_jobs(
        self,
        status: JobStatus | None = None,
        queue: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[Job], int]:
        if status == JobStatus.QUEUED:
            names = [queue] if queue else sorted(await self.redis.smembers(self.key("queues")))
            ids: list[str] = []
            for name in names:
                ids.extend(await self.redis.zrange(self.key("q", name), 0, LIST_SCAN_CAP))
            jobs = await self._fetch_jobs(ids, None)
            jobs = [j for j in jobs if j.status == JobStatus.QUEUED]
            jobs.sort(key=lambda j: j.created_at, reverse=True)
            return jobs[offset : offset + limit], len(jobs)

        index_key = self.key(STATUS_INDEX[status]) if status else self.key("all")
        needs_filter = queue is not None or status in (JobStatus.SCHEDULED, JobStatus.RETRYING)
        needs_filter = needs_filter or status in (JobStatus.FAILED, JobStatus.CANCELLED)
        if needs_filter:
            ids = await self.redis.zrevrange(index_key, 0, LIST_SCAN_CAP)
            jobs = await self._fetch_jobs(ids, index_key)
            if status is not None:
                jobs = [j for j in jobs if j.status == status]
            if queue:
                jobs = [j for j in jobs if j.queue == queue]
            return jobs[offset : offset + limit], len(jobs)

        total = await self.redis.zcard(index_key)
        ids = await self.redis.zrevrange(index_key, offset, offset + limit - 1)
        jobs = await self._fetch_jobs(ids, index_key)
        return jobs, total

    async def delete_job(self, job_id: str) -> bool:
        return bool(await self._delete(keys=[], args=[self.prefix, job_id]))

    async def retry_job(self, job_id: str, now: float) -> Job | None:
        ok = await self._retry(keys=[], args=[self.prefix, repr(now), job_id])
        return await self._job_from_key(job_id) if ok else None

    async def requeue_job(self, job_id: str, now: float) -> Job | None:
        ok = await self._requeue(keys=[], args=[self.prefix, repr(now), job_id])
        return await self._job_from_key(job_id) if ok else None

    async def queue_stats(self) -> QueueStats:
        names = sorted(await self.redis.smembers(self.key("queues")))
        async with self.redis.pipeline(transaction=False) as pipe:
            for name in names:
                pipe.zcard(self.key("q", name))
            for index in ("scheduled", "waiting", "running", "done", "dead"):
                pipe.zcard(self.key(index))
            counts = await pipe.execute()
        per_queue = dict(zip(names, counts[: len(names)], strict=True))
        scheduled, waiting, running, done, dead = counts[len(names) :]
        return QueueStats(
            queues=per_queue,
            scheduled=scheduled,
            waiting=waiting,
            running=running,
            succeeded=done,
            dead=dead,
        )

    async def metrics(self, now: float, minutes: int = 60) -> list[MinuteBucket]:
        current = minute_of(now)
        wanted = list(range(current - minutes + 1, current + 1))
        async with self.redis.pipeline(transaction=False) as pipe:
            for m in wanted:
                pipe.hgetall(self.key("m", str(m)))
            rows = await pipe.execute()
        return [
            MinuteBucket(minute=m, **{k: int(v) for k, v in row.items()})
            for m, row in zip(wanted, rows, strict=True)
        ]

    # ------------------------------------------------------------ lifecycle
    async def clear(self) -> None:
        keys = [k async for k in self.redis.scan_iter(match=self.prefix + "*", count=1000)]
        if keys:
            await self.redis.delete(*keys)

    async def close(self) -> None:
        close = getattr(self.redis, "aclose", None) or self.redis.close
        await close()
