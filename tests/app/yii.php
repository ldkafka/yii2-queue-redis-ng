<?php

declare(strict_types=1);

// Console entry point of the test application: what `yii queue/listen` is in a real project.

require __DIR__ . '/../bootstrap.php';

exit((new yii\console\Application(require __DIR__ . '/config.php'))->run());
