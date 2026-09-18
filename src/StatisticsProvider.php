<?php

declare(strict_types=1);

namespace ldkafka\queue\redis;

use yii\queue\redis\StatisticsProvider as StockStatisticsProvider;

/**
 * Queue statistics for `queue/info`. Waiting jobs are counted in the sorted set, plus any the
 * stock driver left in its plain list; the other counters are inherited.
 */
class StatisticsProvider extends StockStatisticsProvider
{
    /**
     * @inheritdoc
     */
    public function getWaitingCount(): int
    {
        $channel = $this->queue->channel;

        return (int) $this->queue->redis->zcard("$channel.prioritized")
            + (int) $this->queue->redis->llen("$channel.waiting");
    }
}
