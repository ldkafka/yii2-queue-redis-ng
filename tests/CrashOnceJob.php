<?php

declare(strict_types=1);

namespace ldkafka\queue\redis\tests;

use yii\queue\RetryableJobInterface;

/**
 * Kills its worker process the first time it runs, the way an out-of-memory kill would, and
 * completes on the second attempt.
 */
class CrashOnceJob extends FileJob implements RetryableJobInterface
{
    /**
     * @inheritdoc
     */
    public function execute($queue): void
    {
        if (!file_exists($this->file . '.crashed')) {
            touch($this->file . '.crashed');
            exit(9);
        }
        parent::execute($queue);
    }

    /**
     * @inheritdoc
     */
    public function getTtr(): int
    {
        return 1;
    }

    /**
     * @inheritdoc
     */
    public function canRetry($attempt, $error): bool
    {
        return true;
    }
}
