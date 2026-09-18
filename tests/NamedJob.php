<?php

declare(strict_types=1);

namespace ldkafka\queue\redis\tests;

use yii\base\BaseObject;
use yii\queue\JobInterface;

/**
 * A job that only carries a name, so tests can tell in which order jobs were handed out.
 */
class NamedJob extends BaseObject implements JobInterface
{
    public string $name = '';

    /**
     * @inheritdoc
     */
    public function execute($queue): void
    {
    }
}
