# Changelog

All notable changes to this package are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [1.0.0] - 2026-09-18

### Added
- `ldkafka\queue\redis\Queue`: drop-in replacement for `yii\queue\redis\Queue`.
- Job priority through `->priority()`, from `0` (most urgent) to `9000`, default `1024`; can be
  combined with `->delay()`.
- Atomic push, reserve, acknowledge and remove as Lua scripts: a crashed worker or a dropped
  connection can no longer lose a job.
- One ordering rule: lowest priority value first, then first to join the line. Delayed jobs join
  when they are due, retried jobs when their TTR runs out.
- Options `defaultPriority`, `retryPosition` and `delayPosition` (`back` or `original`), `aging`
  and `batchSize`.
- Optional aging: a more urgent job only overtakes jobs that joined less than
  `priority difference × aging` seconds before it, so no job starves.
- All times taken from the Redis server clock.
- Blocking wait on a marker key (`BZPOPMIN`) instead of the job list.
- Takes over jobs queued by the stock driver; `flatten()` hands waiting jobs back to it.
- `StatisticsProvider` counting waiting jobs in the sorted set, for `queue/info`.
- PHPUnit suite against a real server, including multi-process worker tests; GitHub Actions
  workflow for PHP 8.1 to 8.4, Redis 5.0 to 8 and Valkey 8.
