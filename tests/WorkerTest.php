<?php

declare(strict_types=1);

namespace ldkafka\queue\redis\tests;

use ldkafka\queue\redis\Queue;
use PHPUnit\Framework\TestCase;
use yii\redis\Connection;

/**
 * Drives the real console commands (`queue/run`, `queue/listen`, `queue/info`) in separate
 * processes against a real Redis server and the server's own clock.
 */
class WorkerTest extends TestCase
{
    private Connection $redis;
    private Queue $queue;
    private string $channel;
    private string $file;
    /** @var list<resource> worker processes still to be reaped */
    private array $processes = [];
    /** @var array<int, array<int, resource>> output pipes by process */
    private array $pipes = [];

    protected function setUp(): void
    {
        $this->redis = new Connection([
            'hostname' => getenv('REDIS_HOST') ?: '127.0.0.1',
            'port' => (int) (getenv('REDIS_PORT') ?: 6379),
            'database' => (int) (getenv('REDIS_DB') ?: 0),
            'password' => getenv('REDIS_PASSWORD') ?: null,
        ]);
        try {
            $this->redis->open();
        } catch (\Throwable $e) {
            $this->markTestSkipped('No Redis server: ' . $e->getMessage());
        }

        $this->channel = 'ng_test_' . bin2hex(random_bytes(6));
        putenv("NG_TEST_CHANNEL=$this->channel"); // inherited by the worker processes
        $this->queue = new Queue(['redis' => $this->redis, 'channel' => $this->channel]);
        $this->file = tempnam(sys_get_temp_dir(), 'ng_');
    }

    protected function tearDown(): void
    {
        foreach ($this->processes as $process) {
            proc_terminate($process);
            proc_close($process);
        }
        if (isset($this->queue)) {
            $this->queue->clear();
            $this->redis->close();
            @unlink($this->file);
            @unlink($this->file . '.crashed');
        }
    }

    public function testQueueRunExecutesJobsInIsolatedProcessesByPriority(): void
    {
        $this->push('LOW', 2000);
        $this->push('NORMAL');
        $this->push('HIGH', 1);

        $this->assertSame(0, $this->yii('queue/run')[0]);

        $this->assertSame(['HIGH', 'NORMAL', 'LOW'], $this->ran());
        $info = $this->yii('queue/info')[1];
        $this->assertMatchesRegularExpression('/waiting: 0\b/', $info);
        $this->assertMatchesRegularExpression('/reserved: 0\b/', $info);
        $this->assertMatchesRegularExpression('/done: 3\b/', $info);
    }

    public function testParallelWorkersRunEveryJobExactlyOnce(): void
    {
        $expected = [];
        for ($i = 0; $i < 400; $i++) {
            $this->push($expected[] = "job$i", random_int(0, 50) * 10, 0);
        }

        $workers = [];
        for ($i = 0; $i < 4; $i++) {
            $workers[] = $this->spawn('queue/run', '--isolate=0');
        }
        foreach ($workers as $worker) {
            $this->assertSame(0, $this->wait($worker));
        }

        $ran = $this->ran();
        sort($ran);
        sort($expected);
        $this->assertSame($expected, $ran);
        $this->assertSame(0, $this->queue->getStatisticsProvider()->getWaitingCount());
        $this->assertSame('0', $this->redis->zcard("$this->channel.reserved"));
    }

    public function testOneWorkerRunsAMixedBacklogInExactOrder(): void
    {
        $expected = [];
        foreach ([300, 7, 1024, 7, 0, 300, 1024, 0] as $i => $priority) {
            $this->push("p$priority-$i", $priority);
            $expected[$priority][] = "p$priority-$i";
        }
        ksort($expected);

        $this->assertSame(0, $this->yii('queue/run', '--isolate=0')[0]);

        $this->assertSame(array_merge(...array_values($expected)), $this->ran());
    }

    public function testBlockedListenerIsWokenByAPush(): void
    {
        $this->spawn('queue/listen', '30', '--isolate=0');
        usleep(1_500_000); // let the worker boot and block on the empty queue

        $pushedAt = microtime(true);
        $this->push('WAKE');
        while ($this->ran() === [] && microtime(true) - $pushedAt < 10) {
            usleep(20_000);
        }

        $this->assertSame(['WAKE'], $this->ran());
        $this->assertLessThan(2.0, microtime(true) - $pushedAt, 'the listener waited for its 30 s timeout');
    }

    public function testJobOfACrashedWorkerIsRunAgain(): void
    {
        $this->queue->push(new CrashOnceJob(['name' => 'SURVIVOR', 'file' => $this->file]));

        $this->assertSame(9, $this->yii('queue/run', '--isolate=0')[0], 'the worker died in the middle of the job');
        $this->assertSame([], $this->ran());
        $this->assertSame('1', $this->redis->zcard("$this->channel.reserved"), 'the job is still on record');

        sleep(2); // its TTR of 1 s runs out
        $this->assertSame(0, $this->yii('queue/run', '--isolate=0')[0]);

        $this->assertSame(['SURVIVOR'], $this->ran());
        $this->assertSame('0', $this->redis->hlen("$this->channel.messages"));
    }

    public function testDelayedJobRunsOnceItIsDueOnTheServerClock(): void
    {
        $start = microtime(true);
        $this->push('LATER', 1, 2);
        $this->push('NOW', 5000);

        $this->assertSame(0, $this->yii('queue/run', '--isolate=0')[0]);
        $this->assertSame(['NOW'], $this->ran());

        $this->spawn('queue/listen', '1', '--isolate=0');
        while (count($this->ran()) < 2 && microtime(true) - $start < 10) {
            usleep(50_000);
        }

        $this->assertSame(['NOW', 'LATER'], $this->ran());
        $this->assertGreaterThan(1.0, microtime(true) - $start);
    }

    // ----------------------------------------------------------------------------------- helpers

    private function push(string $name, ?int $priority = null, int $delay = 0): void
    {
        $this->queue->delay($delay)->priority($priority)->push(new FileJob(['name' => $name, 'file' => $this->file]));
    }

    /**
     * @return list<string> names of the jobs that ran, in order.
     */
    private function ran(): array
    {
        clearstatcache();

        return array_values(array_filter(explode("\n", (string) file_get_contents($this->file))));
    }

    /**
     * Starts a console command of the test application.
     *
     * @return resource
     */
    private function spawn(string ...$arguments)
    {
        $command = array_merge([PHP_BINARY, __DIR__ . '/app/yii.php'], $arguments, ['--color=0']);
        $process = proc_open($command, [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        $this->assertIsResource($process);
        $this->processes[] = $process;
        $this->pipes[(int) $process] = $pipes;

        return $process;
    }

    /**
     * Waits for a process to end.
     *
     * @param resource $process
     * @return int its exit code.
     */
    private function wait($process, ?string &$output = null): int
    {
        $pipes = $this->pipes[(int) $process];
        $output = stream_get_contents($pipes[1]) . stream_get_contents($pipes[2]);
        array_map('fclose', $pipes);
        $this->processes = array_filter($this->processes, fn ($other) => $other !== $process);

        return proc_close($process);
    }

    /**
     * Runs a console command to the end.
     *
     * @return array{0: int, 1: string} exit code and output.
     */
    private function yii(string ...$arguments): array
    {
        $code = $this->wait($this->spawn(...$arguments), $output);

        return [$code, $output];
    }
}
