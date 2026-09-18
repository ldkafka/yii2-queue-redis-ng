<?php

declare(strict_types=1);

namespace ldkafka\queue\redis;

use yii\base\InvalidArgumentException;
use yii\base\InvalidConfigException;
use yii\db\Exception as RedisException;
use yii\queue\redis\Queue as StockQueue;

/**
 * Redis queue driver with job priority, atomic reservation and configurable ordering.
 *
 * A drop-in replacement for {@see StockQueue}: same component, same console commands, same keys
 * for messages, attempts, delayed and reserved jobs. Waiting jobs live in a sorted set instead of
 * a list, and every state change is a single Lua script, so a worker that dies or a connection
 * that drops between two commands can no longer lose a job.
 *
 * ```php
 * 'queue' => [
 *     'class' => \ldkafka\queue\redis\Queue::class,
 *     'redis' => 'redis',
 *     'channel' => 'queue',
 * ],
 *
 * Yii::$app->queue->priority(10)->push($job);              // lower runs first, default 1024
 * Yii::$app->queue->delay(3600)->priority(10)->push($job); // delay and priority combine
 * ```
 *
 * Method parameters are untyped because the parent methods in yii2-queue 2.3 are; PHP does not
 * allow an override to narrow them.
 */
class Queue extends StockQueue
{
    /** The job joins the back of its priority, like a new push. */
    public const POSITION_BACK = 'back';
    /** The job takes back the place it was first given. */
    public const POSITION_ORIGINAL = 'original';

    /** Highest accepted priority value (the least urgent). Keeps scores exact in a Redis double. */
    public const MAX_PRIORITY = 9000;
    /** Highest accepted {@see $aging}, in seconds per priority point. */
    public const MAX_AGING = 86400;

    /**
     * @var int priority of a job pushed without `priority()`. Lower values run first.
     * 1024 matches the DB and Beanstalk drivers of yii2-queue.
     */
    public int $defaultPriority = 1024;
    /**
     * @var string where a job whose TTR expired rejoins its priority: {@see POSITION_BACK} or
     * {@see POSITION_ORIGINAL}.
     */
    public string $retryPosition = self::POSITION_BACK;
    /**
     * @var string where a delayed job joins its priority once it is due: {@see POSITION_BACK}
     * (as if pushed at the due time) or {@see POSITION_ORIGINAL} (as if pushed when `push()` ran).
     */
    public string $delayPosition = self::POSITION_BACK;
    /**
     * @var int|float seconds of head start one priority point is worth. `0` is strict priority:
     * a more urgent job always runs first, and lower priorities can starve under sustained load.
     * With a value above zero a more urgent job only overtakes jobs that joined less than
     * `priority difference * aging` seconds before it, so every job eventually runs.
     */
    public int|float $aging = 0;
    /**
     * @var int most jobs moved between sets in one script call. Bounds how long a script can
     * hold the Redis server.
     */
    public int $batchSize = 100;

    /** @var array<string, string> script name => source, shared by all instances */
    private static array $scripts = [];

    private ?StatisticsProvider $statisticsProvider = null;

    /**
     * @inheritdoc
     * @throws InvalidConfigException when an ordering option is out of range.
     */
    public function init(): void
    {
        parent::init();

        foreach (['retryPosition', 'delayPosition'] as $option) {
            if (!in_array($this->$option, [self::POSITION_BACK, self::POSITION_ORIGINAL], true)) {
                throw new InvalidConfigException("$option must be 'back' or 'original'.");
            }
        }
        if ($this->defaultPriority < 0 || $this->defaultPriority > self::MAX_PRIORITY) {
            throw new InvalidConfigException('defaultPriority must be between 0 and ' . self::MAX_PRIORITY . '.');
        }
        if ($this->aging < 0 || $this->aging > self::MAX_AGING) {
            throw new InvalidConfigException('aging must be between 0 and ' . self::MAX_AGING . ' seconds.');
        }
        if ($this->batchSize < 1) {
            throw new InvalidConfigException('batchSize must be at least 1.');
        }
    }

    /**
     * Clears the queue. Deletes the queue's own keys only, without scanning the keyspace.
     */
    public function clear(): void
    {
        $this->redis->executeCommand('DEL', array_merge($this->keys(), ["$this->channel.moving_lock"]));
    }

    /**
     * Removes a job by ID, whether it is waiting, delayed or reserved.
     *
     * @param int|string $id
     */
    public function remove($id): bool
    {
        return (bool) $this->script('remove', [$id]);
    }

    /**
     * Moves every waiting job back to the plain list of the stock driver, most urgent first.
     * Run it before switching a channel back to {@see StockQueue}, which cannot see the sorted set.
     *
     * @return int number of jobs moved.
     */
    public function flatten(): int
    {
        $total = 0;
        while (($moved = (int) $this->script('flatten')) > 0) {
            $total += $moved;
        }

        return $total;
    }

    /**
     * @inheritdoc
     */
    public function getStatisticsProvider()
    {
        return $this->statisticsProvider ??= new StatisticsProvider($this);
    }

    /**
     * @param int|string|null $timeout seconds to wait for a job, `0` to return at once.
     * @return array{0: string, 1: string, 2: int, 3: int}|null id, message, ttr, attempt.
     */
    protected function reserve($timeout): ?array
    {
        $job = $this->script('reserve');
        if ($job === null && $timeout > 0) {
            // A missed wake-up costs at most $timeout seconds; the job itself is never at stake.
            if ($this->redis->executeCommand('BZPOPMIN', ["$this->channel.marker", (string) $timeout])) {
                $job = $this->script('reserve');
            }
        }
        if ($job === null) {
            return null;
        }

        [$id, $payload, $attempt] = $job;
        [$ttr, $message] = explode(';', $payload, 2);

        return [$id, $message, (int) $ttr, (int) $attempt];
    }

    /**
     * @param int|string $id
     */
    protected function delete($id): void
    {
        $this->script('delete', [$id]);
    }

    /**
     * @inheritdoc
     * @param mixed $priority integer from 0 to {@see MAX_PRIORITY}; `null` means {@see $defaultPriority}.
     * @throws InvalidArgumentException when the priority is not such an integer.
     */
    protected function pushMessage($message, $ttr, $delay, $priority): int
    {
        if ($priority === null) {
            $priority = $this->defaultPriority;
        } elseif (filter_var($priority, FILTER_VALIDATE_INT) === false || $priority < 0 || $priority > self::MAX_PRIORITY) {
            throw new InvalidArgumentException('Job priority must be an integer between 0 and ' . self::MAX_PRIORITY . '.');
        }

        return (int) $this->script('push', ["$ttr;$message", $priority, (int) $delay]);
    }

    /**
     * Clock used for due times and ordering, in milliseconds. `null` lets the scripts read the
     * Redis server clock, so every pusher and worker shares one clock. Tests override it.
     */
    protected function now(): ?int
    {
        return null;
    }

    /**
     * Runs one of the bundled scripts.
     *
     * @param list<int|string> $arguments script-specific arguments, after the shared settings.
     * @return mixed the script's reply; `null` for a Lua `false`.
     */
    private function script(string $name, array $arguments = []): mixed
    {
        $source = self::$scripts[$name] ??= file_get_contents(__DIR__ . '/lua/common.lua')
            . file_get_contents(__DIR__ . "/lua/$name.lua");
        $keys = $this->keys();
        $params = array_map('strval', array_merge([count($keys)], $keys, [
            $this->now() ?? '',
            $this->defaultPriority,
            (int) round($this->aging * 1000),
            (int) ($this->retryPosition === self::POSITION_ORIGINAL),
            (int) ($this->delayPosition === self::POSITION_ORIGINAL),
            $this->batchSize,
        ], $arguments));

        try {
            return $this->redis->executeCommand('EVALSHA', array_merge([sha1($source)], $params));
        } catch (RedisException $e) {
            if (!str_contains($e->getMessage(), 'NOSCRIPT')) {
                throw $e;
            }

            return $this->redis->executeCommand('EVAL', array_merge([$source], $params));
        }
    }

    /**
     * @return list<string> the queue's keys, in the order the scripts expect them.
     */
    private function keys(): array
    {
        return array_map(fn (string $name): string => "$this->channel.$name", [
            'message_id', 'messages', 'waiting', 'prioritized', 'delayed', 'reserved',
            'attempts', 'priorities', 'marker', 'seq', 'ranks',
        ]);
    }
}
