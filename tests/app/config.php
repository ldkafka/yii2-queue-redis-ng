<?php

declare(strict_types=1);

return [
    'id' => 'ng-test-app',
    'basePath' => __DIR__,
    'runtimePath' => sys_get_temp_dir() . '/yii2-queue-redis-ng',
    'vendorPath' => dirname(__DIR__, 2) . '/vendor',
    'bootstrap' => ['queue'],
    'components' => [
        'redis' => [
            'class' => yii\redis\Connection::class,
            'hostname' => getenv('REDIS_HOST') ?: '127.0.0.1',
            'port' => (int) (getenv('REDIS_PORT') ?: 6379),
            'database' => (int) (getenv('REDIS_DB') ?: 0),
            'password' => getenv('REDIS_PASSWORD') ?: null,
        ],
        'queue' => [
            'class' => ldkafka\queue\redis\Queue::class,
            'channel' => getenv('NG_TEST_CHANNEL') ?: 'ng_test_app',
        ],
    ],
];
