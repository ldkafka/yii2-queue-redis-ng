<?php

declare(strict_types=1);

namespace ldkafka\queue\redis\tests;

use ldkafka\queue\redis\Queue;

/**
 * The driver with a hand-set clock and the worker's protected steps exposed, so tests can move
 * time forward instead of sleeping.
 */
class TestQueue extends Queue
{
    /** @var int|null current time in milliseconds; `null` uses the Redis server clock. */
    public ?int $clock = null;

    /**
     * @inheritdoc
     */
    protected function now(): ?int
    {
        return $this->clock;
    }

    /**
     * Reserves the next job the way a worker does.
     *
     * @return array{0: string, 1: string, 2: int, 3: int}|null
     */
    public function take(int $timeout = 0): ?array
    {
        return $this->reserve($timeout);
    }

    /**
     * Acknowledges a job the way a worker does after it ran.
     */
    public function acknowledge(int|string $id): void
    {
        $this->delete($id);
    }
}
