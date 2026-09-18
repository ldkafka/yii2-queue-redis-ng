<?php

declare(strict_types=1);

namespace ldkafka\queue\redis\tests;

use yii\base\BaseObject;
use yii\queue\JobInterface;

/**
 * Appends its name to a file, so a test can see what worker processes ran, and in which order.
 */
class FileJob extends BaseObject implements JobInterface
{
    public string $name = '';
    public string $file = '';

    /**
     * @inheritdoc
     */
    public function execute($queue): void
    {
        file_put_contents($this->file, $this->name . "\n", FILE_APPEND | LOCK_EX);
    }
}
