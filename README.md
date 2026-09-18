# yii2-queue-redis-ng

A Redis driver for [yii2-queue](https://github.com/yiisoft/yii2-queue) that adds what the stock
Redis driver lacks, as a drop-in replacement:

- **Job priority.** `->priority()` works, alone or combined with `->delay()`. The stock driver
  throws `NotSupportedException`.
- **Crash-safe reservation.** Taking a job and recording the reservation is one atomic step. The
  stock driver pops the job first and records it afterwards; a worker killed, or a connection
  dropped, between the two loses the job for good.
- **Predictable order.** One documented rule for new, delayed and retried jobs, with options.
- **No starvation, if you want it.** Optional aging bounds how long a less urgent job can be
  overtaken.
- **Fewer round trips.** A push is one call to Redis instead of three, a reservation one instead
  of five or six.

Same component, same console commands (`queue/listen`, `queue/run`, `queue/info`, …), same Redis
keys for messages, attempts, delayed and reserved jobs. Jobs already queued by the stock driver
are picked up as they are.

## Requirements

- PHP 8.1+
- yii2-queue 2.3.7+ and yii2-redis 2.0 or 2.1
- Redis 5.0+ or Valkey (needs `ZPOPMIN` / `BZPOPMIN` and Lua scripting)

The test suite runs against Redis 5.0, 6.2, 7.4, 8 and Valkey 8.

## Installation

```bash
composer require ldkafka/yii2-queue-redis-ng
```

## Configuration

Change the class of your queue component. Nothing else has to change.

```php
'components' => [
    'queue' => [
        'class' => \ldkafka\queue\redis\Queue::class, // was \yii\queue\redis\Queue::class
        'redis' => 'redis',
        'channel' => 'queue',
    ],
],
```

## Usage

```php
Yii::$app->queue->push($job);                               // default priority, 1024
Yii::$app->queue->priority(10)->push($job);                 // more urgent: lower runs first
Yii::$app->queue->priority(5000)->push($job);               // less urgent
Yii::$app->queue->delay(3600)->priority(10)->push($job);    // in an hour, and urgent then
```

A priority is an integer from `0` (most urgent) to `9000`. The direction and the default of `1024`
match the DB and Beanstalk drivers of yii2-queue, so switching drivers does not reorder your jobs.

## Ordering rules

A free worker takes the waiting job with the **lowest priority value**; among equal priorities, the
one that **joined the line first**. What "joined" means:

| Job | Joins its priority… |
|---|---|
| Pushed without delay | at the back, when it is pushed |
| Pushed with `delay()` | at the back, **when it becomes due**: `delay()` decides when the job is pushed, not when it runs |
| Retried (its TTR ran out: the worker died, or the job failed and may retry) | at the back, when the TTR runs out |

So inside one priority no job can be overtaken by a job that arrived after it, a job that keeps
failing cannot hold up the healthy ones, and a delayed job never jumps a queue. Things worth knowing:

- A delay is a lower bound. Under a backlog a due job waits its turn like any new job. Give
  time-critical delayed work a more urgent priority.
- Priority never makes a delayed job run early, and never interrupts a running job. It decides
  which job a **free** worker takes next.
- Jobs due in the same second keep their push order.
- "Due at T" orders exactly like "pushed at T", even when every worker was busy at T: due jobs are
  moved into line before any later push is numbered.
- With several workers, the order is the order in which jobs *start*.

The stock driver behaves differently: it puts due and retried jobs in front of the whole queue.

### Options

| Option | Default | Meaning |
|---|---|---|
| `defaultPriority` | `1024` | Priority of a job pushed without `priority()`. |
| `retryPosition` | `'back'` | `'back'`, or `'original'`: a retried job takes back the place it first held. |
| `delayPosition` | `'back'` | `'back'`, or `'original'`: a due job is placed as if pushed when `push()` ran. With both options set to `'original'` the order is that of the DB driver (`ORDER BY priority, id`). |
| `aging` | `0` | Seconds of head start one priority point is worth; `0` means strict priority. See below. |
| `batchSize` | `100` | Most jobs moved into line by one script call, which bounds how long a script holds Redis. When more jobs than this come due in the same moment, the rest follow on the next calls and can be numbered after a push made in between. |

Pushers and workers must use the same values, which a shared component configuration gives you.
Changing a value affects jobs that join the line afterwards; nothing is lost.

### Aging: strict priority without starvation

With strict priority (`aging => 0`) a more urgent job always goes first. If more urgent work keeps
arriving at least as fast as the workers clear it, less urgent jobs **never run**. Every strict
priority queue has this property, including the DB driver.

Set `aging` to make each priority point worth that many seconds of head start instead:

```php
'aging' => 1, // a job 100 points more urgent overtakes only jobs that joined less than 100 s before it
```

A job can then only be overtaken by more urgent jobs that arrive within
`priority difference × aging` seconds after it. Once that window has passed, everything newer is
behind it, so it runs as long as the workers process anything at all. To choose a value, decide how
long the least urgent job may be held back by the most urgent ones and divide by the difference
between their priorities: a one hour limit between priorities `0` and `1024` is `aging => 3.5`.

The trade: priority is no longer absolute. An urgent job pushed now goes behind a less urgent job
that has already waited longer than the window. All ordering rules above hold in both modes. Aging
cannot help when jobs permanently arrive faster than they are processed; then the backlog grows
whatever the order, but evenly, with no job singled out.

## Reliability

Every state change is a single Lua script, which Redis runs atomically:

| Step | This driver | Stock driver |
|---|---|---|
| Push | 1 call | `INCR`, `HSET`, `LPUSH`: a crash in between leaves an orphaned message |
| Reserve | 1 call: move due jobs, take the next job, record the reservation and the attempt | `BRPOP`, then `HGET`, `ZADD`, `HINCRBY`: a crash or a lost reply after the pop **loses the job** |
| Acknowledge | 1 call | 3 calls |

Delivery is at-least-once. A job whose worker died stays on record as reserved and joins the line
again when its TTR runs out. A job that outlives its TTR runs twice, as with every yii2-queue
driver, so give long jobs a TTR that fits.

All times come from the **Redis server clock**, so clock differences between your hosts can
neither reorder jobs nor release a reservation early.

An idle worker blocks on a small marker key (`BZPOPMIN`) that a push sets. If a wake-up were ever
missed, the worker would notice the job when the timeout of `queue/listen` ends (3 s by default);
the job itself is never at stake.

## Moving from the stock driver, and back

**To this driver.** Change the class and deploy. Jobs the stock driver left in its `waiting` list,
or still pushes there during a rolling deploy, are taken over at the default priority; delayed and
reserved jobs use the same keys in both drivers. While old and new code run side by side, jobs
pushed by old code may wait up to the listen timeout, and old workers do not see jobs pushed by
new code.

**Back to the stock driver.** The stock driver cannot see the sorted set, so hand the waiting jobs
back first, once the new pushers are stopped:

```php
Yii::$app->queue->flatten(); // returns the number of jobs moved, most urgent first
```

## How it works

Keys, for a channel `queue`:

| Key | Type | Content |
|---|---|---|
| `queue.message_id`, `queue.messages`, `queue.attempts`, `queue.delayed`, `queue.reserved` | | unchanged from the stock driver |
| `queue.prioritized` | sorted set | waiting jobs; the score is their place in line |
| `queue.priorities` | hash | priority of jobs that are not at the default |
| `queue.ranks` | hash | remembered places, only used with an `'original'` option |
| `queue.seq` | string | counter that numbers jobs as they join the line |
| `queue.marker` | sorted set | one member while work is waiting; wakes a blocked worker |
| `queue.waiting` | list | the stock driver's list; only read, to take over its jobs |

The score of a waiting job is `priority × 10¹² + sequence` under strict priority, which stays
readable in `redis-cli` (`1024000000000042` is priority 1024, 42nd to join), or
`(joined, ms + priority × aging, ms) × 1024 + sequence` with aging. Both stay below 2⁵³, where a
Redis score is still an exact integer; that is why priorities stop at 9000.

The scripts are in [`src/lua`](src/lua). They are sent once and then called by their SHA-1.

## Notes

- **Blocking and `dataTimeout`.** If the Redis connection has a `dataTimeout`, keep it above the
  timeout of `queue/listen`, as with the stock driver.
- **Redis Cluster.** The scripts use several keys, which must share a hash slot: put the channel in
  a hash tag, `'channel' => '{queue}'`.
- **Eviction.** A queue must not lose keys. Use `maxmemory-policy noeviction` for its database.
- **Order between two jobs** is not a guarantee of completion order: if the first fails, the second
  overtakes it, in every driver. Chain dependent work inside a job instead.
- **yii2-queue 3.** The unreleased, fully typed yii2-queue changes the signatures this class
  overrides; it will need a new major version of this package.

## Performance

A push costs `O(log n)` in a sorted set instead of `O(1)` in a list, which is around twenty steps
for a million waiting jobs and does not show next to a network round trip. Round trips are what
changes: on a latency-bound development machine this driver pushed about 2.5 times and
reserved-and-acknowledged about 4 times as many jobs per second as the stock driver, with or
without priorities. On a low-latency link the gap is smaller.

## Testing

The suite needs a Redis server. It uses a uniquely named channel per test and deletes only that
channel's keys, never the database.

```bash
docker run --rm -d -p 6379:6379 redis:7-alpine
composer test
```

`REDIS_HOST`, `REDIS_PORT`, `REDIS_DB` and `REDIS_PASSWORD` override the defaults in
`phpunit.xml.dist`. Besides the ordering rules, the suite runs the real console commands in
separate processes: parallel workers racing for one backlog, a worker killed in the middle of a
job, and a blocked listener woken by a push.

## License

BSD-3-Clause. See [LICENSE](LICENSE).
