<?php

declare(strict_types=1);

namespace ldkafka\queue\redis\tests;

use ldkafka\queue\redis\Queue;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use yii\base\InvalidArgumentException;
use yii\base\InvalidConfigException;
use yii\queue\redis\Queue as StockQueue;
use yii\redis\Connection;

/**
 * Runs against a real Redis server (see phpunit.xml.dist). Every test uses its own channel and
 * removes only that channel's keys, so a shared server is safe.
 */
class QueueTest extends TestCase
{
    private Connection $redis;
    private string $channel;
    private TestQueue $queue;
    /** @var int start of the test's timeline, in milliseconds */
    private int $t0;

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
        $this->t0 = time() * 1000;
        $this->queue = $this->createQueue();
    }

    protected function tearDown(): void
    {
        if (isset($this->queue)) {
            $this->queue->clear();
            $this->redis->close();
        }
    }

    // ---------------------------------------------------------------------------------- ordering

    public function testRunsByPriorityThenPushOrder(): void
    {
        $this->push('A');
        $this->push('B', 10);
        $this->push('C');
        $this->push('D', 2000);
        $this->push('E', 10);
        $this->push('F', 0);

        $this->assertSame(['F', 'B', 'E', 'A', 'C', 'D'], $this->drain());
    }

    public function testExplicitDefaultPriorityIsTheSameAsNone(): void
    {
        $this->push('A');
        $this->push('B', 1024);
        $this->push('C');

        $this->assertSame(['A', 'B', 'C'], $this->drain());
        $this->assertSame('0', $this->redis->hlen("$this->channel.priorities"));
    }

    public function testRejectsPrioritiesOutsideTheRange(): void
    {
        foreach ([-1, Queue::MAX_PRIORITY + 1, 1.5, 'high'] as $priority) {
            try {
                $this->push('X', $priority);
                $this->fail('Accepted priority ' . var_export($priority, true));
            } catch (InvalidArgumentException) {
                $this->addToAssertionCount(1);
            }
        }
        $this->assertSame([], $this->drain());
    }

    // ----------------------------------------------------------------------------------- delay

    public function testDelayedJobJoinsTheLineWhenItIsDueEvenIfNoWorkerRan(): void
    {
        $this->push('OLD');
        $this->push('DELAYED', null, 5);
        $this->push('N1');
        $this->at(3);
        $this->push('N2');
        $this->at(10);
        $this->push('N3');

        $this->at(100);
        $this->assertSame(['OLD', 'N1', 'N2', 'DELAYED', 'N3'], $this->drain());
    }

    public function testDelayedJobIsNotRunEarlyHoweverUrgent(): void
    {
        $this->push('URGENT_LATER', 0, 60);
        $this->push('NORMAL');

        $this->assertSame(['NORMAL'], $this->drain());
        $this->at(60);
        $this->assertSame(['URGENT_LATER'], $this->drain());
    }

    public function testDueJobObeysItsPriority(): void
    {
        $this->push('A');
        $this->push('DELAYED_LOW', 5000, 5);
        $this->push('DELAYED_URGENT', 1, 5);
        $this->push('B');

        $this->at(100);
        $this->assertSame(['DELAYED_URGENT', 'A', 'B', 'DELAYED_LOW'], $this->drain());
    }

    public function testJobsDueInTheSameSecondKeepTheirPushOrder(): void
    {
        $names = [];
        for ($i = 1; $i <= 12; $i++) {
            $this->push($names[] = "J$i", null, 5);
        }

        $this->at(100);
        $this->assertSame($names, $this->drain());
    }

    public function testDelayedJobCanKeepItsOriginalPlace(): void
    {
        $this->queue = $this->createQueue(['delayPosition' => Queue::POSITION_ORIGINAL]);
        $this->push('OLD');
        $this->push('DELAYED', null, 5);
        $this->push('N1');

        $this->at(100);
        $this->assertSame(['OLD', 'DELAYED', 'N1'], $this->drain());
    }

    // ----------------------------------------------------------------------------------- retry

    public function testRetriedJobJoinsTheBackOfItsPriority(): void
    {
        $this->push('FLAKY', 5);
        $this->assertSame('FLAKY', $this->jobName($this->queue->take())); // never acknowledged
        $this->push('NEW1', 5);
        $this->push('NEW2', 5);
        $this->push('URGENT', 1);
        $this->push('LOW', 5000);

        $this->at(301); // default TTR is 300 s
        $this->assertSame(['URGENT', 'NEW1', 'NEW2', 'FLAKY#2', 'LOW'], $this->drain());
    }

    public function testRetriedJobCanKeepItsOriginalPlace(): void
    {
        $this->queue = $this->createQueue(['retryPosition' => Queue::POSITION_ORIGINAL]);
        $this->push('FLAKY', 5);
        $this->queue->take();
        $this->push('NEW1', 5);

        $this->at(301);
        $this->assertSame(['FLAKY#2', 'NEW1'], $this->drain());
    }

    public function testBothOriginalPositionsGiveTheOrderOfTheDbDriver(): void
    {
        $this->queue = $this->createQueue([
            'retryPosition' => Queue::POSITION_ORIGINAL,
            'delayPosition' => Queue::POSITION_ORIGINAL,
        ]);
        $this->push('A');
        $this->push('DELAYED', null, 5);
        $this->push('B');
        $this->queue->take(); // A, never acknowledged
        $this->push('C');

        $this->at(301);
        $this->assertSame(['A#2', 'DELAYED', 'B', 'C'], $this->drain()); // ORDER BY priority, id
    }

    // ----------------------------------------------------------------------------------- aging

    public function testStrictPriorityLetsEveryMoreUrgentJobOvertake(): void
    {
        $this->pushFloodAroundLowJob();

        $this->assertSame(['D10', 'D29', 'D31', 'LOW'], $this->drain());
    }

    public function testAgingBoundsHowLongAJobCanBeOvertaken(): void
    {
        // One second per priority point: a job 30 points more urgent gets a 30 s head start.
        $this->queue = $this->createQueue(['aging' => 1]);
        $this->pushFloodAroundLowJob();

        $this->assertSame(['D10', 'D29', 'LOW', 'D31'], $this->drain());
    }

    public function testAgingKeepsPushOrderInsideAPriority(): void
    {
        $this->queue = $this->createQueue(['aging' => 0.5]);
        foreach (['A', 'B', 'C', 'D'] as $name) {
            $this->push($name); // all within the same millisecond
        }
        $this->push('URGENT', 0);

        $this->assertSame(['URGENT', 'A', 'B', 'C', 'D'], $this->drain());
    }

    // ------------------------------------------------------------------------------ reliability

    public function testReservationIsRecordedTogetherWithThePop(): void
    {
        $id = $this->push('J', 7);
        [$reservedId, , $ttr, $attempt] = $this->queue->take();

        $this->assertSame((string) $id, $reservedId);
        $this->assertSame(300, $ttr);
        $this->assertSame(1, $attempt);
        $this->assertSame((string) (intdiv($this->t0, 1000) + 300), $this->redis->zscore("$this->channel.reserved", $id));
        $this->assertSame(Queue::STATUS_RESERVED, $this->queue->status($id));
    }

    public function testJobWhoseReplyWasLostIsDeliveredAgain(): void
    {
        $this->push('J');
        $this->queue->take(); // the worker dies here: nothing acknowledged

        $this->assertNull($this->queue->take());
        $this->at(301);
        $this->assertSame(['J#2'], $this->drain());
    }

    public function testAcknowledgedJobLeavesNothingBehind(): void
    {
        $this->queue = $this->createQueue(['retryPosition' => Queue::POSITION_ORIGINAL]);
        $id = $this->push('J', 7);
        $this->assertSame(Queue::STATUS_WAITING, $this->queue->status($id));

        $this->drain();

        $this->assertSame(Queue::STATUS_DONE, $this->queue->status($id));
        $this->assertSame(["$this->channel.message_id", "$this->channel.seq"], $this->remainingKeys());
    }

    public function testRemoveFindsAJobInAnyState(): void
    {
        $waiting = $this->push('WAITING', 3);
        $delayed = $this->push('DELAYED', null, 60);
        $reserved = $this->push('RESERVED', 1);
        $this->queue->take();

        $this->assertTrue($this->queue->remove($waiting));
        $this->assertTrue($this->queue->remove($delayed));
        $this->assertTrue($this->queue->remove($reserved));
        $this->assertFalse($this->queue->remove($waiting));

        $this->at(1000);
        $this->assertSame([], $this->drain());
        $this->assertSame(["$this->channel.message_id", "$this->channel.seq"], $this->remainingKeys());
    }

    public function testPayloadsAreBinarySafe(): void
    {
        $awkward = " ;
ÿ{\"'" . random_bytes(64);
        $large = str_repeat('x', 1024 * 1024);
        $this->push($awkward);
        $this->push($large);

        $this->assertSame([$awkward, $large], $this->drain());
    }

    public function testScriptsAreReloadedAfterTheServerForgetsThem(): void
    {
        $this->push('A');
        $this->redis->executeCommand('SCRIPT', ['FLUSH']);
        $this->push('B');

        $this->assertSame(['A', 'B'], $this->drain());
    }

    // ---------------------------------------------------------------------------------- blocking

    public function testWorkerBlocksWhileTheQueueIsEmpty(): void
    {
        $this->queue->clock = null; // real time from here on
        $start = microtime(true);

        $this->assertNull($this->queue->take(1));
        $this->assertGreaterThan(0.9, microtime(true) - $start);
    }

    public function testWorkerDoesNotBlockWhenAJobIsWaiting(): void
    {
        $this->queue->clock = null;
        $this->push('A');
        $this->push('B');
        $start = microtime(true);

        $this->assertSame('A', $this->jobName($this->queue->take(5)));
        $this->assertSame('B', $this->jobName($this->queue->take(5)));
        $this->assertLessThan(1.0, microtime(true) - $start);
        $this->assertSame('1', $this->redis->zcard("$this->channel.marker"), 'a push arms the wake-up marker');
    }

    public function testListenLoopRunsJobsThroughTheWorker(): void
    {
        $this->push('LOW', 2000);
        $this->push('HIGH', 1);
        $ran = [];
        $this->queue->messageHandler = function ($id, $message) use (&$ran): bool {
            $ran[] = $this->queue->serializer->unserialize($message)->name;
            return true;
        };

        $this->queue->run(false);

        $this->assertSame(['HIGH', 'LOW'], $ran);
        $this->assertSame(0, $this->queue->getStatisticsProvider()->getWaitingCount());
    }

    // --------------------------------------------------------------------- stock driver interplay

    public function testJobsPushedByTheStockDriverAreTakenOver(): void
    {
        $stock = new StockQueue(['redis' => $this->redis, 'channel' => $this->channel]);
        $stock->push(new NamedJob(['name' => 'STOCK1']));
        $stock->push(new NamedJob(['name' => 'STOCK2']));
        $stock->delay(2)->push(new NamedJob(['name' => 'STOCK_DELAYED']));
        $this->push('LOW', 5000);
        $this->push('URGENT', 5);

        $this->assertSame(['URGENT', 'STOCK1', 'STOCK2', 'LOW'], $this->drain());
        $this->at(10);
        $this->assertSame(['STOCK_DELAYED'], $this->drain());
    }

    public function testFlattenHandsWaitingJobsBackToTheStockDriver(): void
    {
        $this->push('LOW', 2000);
        $this->push('NORMAL');
        $this->push('URGENT', 1);

        $this->assertSame(3, $this->queue->flatten());

        $ran = [];
        $stock = new StockQueue(['redis' => $this->redis, 'channel' => $this->channel]);
        $stock->messageHandler = function ($id, $message) use (&$ran, $stock): bool {
            $ran[] = $stock->serializer->unserialize($message)->name;
            return true;
        };
        $stock->run(false);

        $this->assertSame(['URGENT', 'NORMAL', 'LOW'], $ran);
    }

    // -------------------------------------------------------------------------------- statistics

    public function testStatisticsCountEveryState(): void
    {
        $this->push('A');
        $this->push('B', 5);
        $this->push('C', null, 60);
        $this->push('D');
        $taken = $this->queue->take();
        $this->queue->acknowledge($taken[0]);
        $this->queue->take();

        $statistics = $this->queue->getStatisticsProvider();
        $this->assertSame(1, $statistics->getWaitingCount());
        $this->assertSame(1, (int) $statistics->getDelayedCount());
        $this->assertSame(1, (int) $statistics->getReservedCount());
        $this->assertSame(1, (int) $statistics->getDoneCount());
    }

    // ----------------------------------------------------------------------------- configuration

    #[DataProvider('invalidConfigurations')]
    public function testRejectsInvalidConfiguration(array $config): void
    {
        $this->expectException(InvalidConfigException::class);
        $this->createQueue($config);
    }

    public static function invalidConfigurations(): array
    {
        return [
            'retry position' => [['retryPosition' => 'front']],
            'delay position' => [['delayPosition' => '']],
            'default priority' => [['defaultPriority' => Queue::MAX_PRIORITY + 1]],
            'negative aging' => [['aging' => -1]],
            'huge aging' => [['aging' => Queue::MAX_AGING + 1]],
            'batch size' => [['batchSize' => 0]],
        ];
    }

    // ----------------------------------------------------------------------------------- helpers

    private function createQueue(array $config = []): TestQueue
    {
        return new TestQueue($config + ['redis' => $this->redis, 'channel' => $this->channel, 'clock' => $this->t0]);
    }

    /**
     * Moves the test clock to `$seconds` after the start of the test.
     */
    private function at(int $seconds): void
    {
        $this->queue->clock = $this->t0 + $seconds * 1000;
    }

    private function push(string $name, mixed $priority = null, int $delay = 0): int
    {
        return (int) $this->queue->delay($delay)->priority($priority)->push(new NamedJob(['name' => $name]));
    }

    /**
     * LOW is 30 priority points less urgent than the default jobs pushed 10, 29 and 31 s after it.
     */
    private function pushFloodAroundLowJob(): void
    {
        $this->push('LOW', 1054);
        foreach ([10, 29, 31] as $seconds) {
            $this->at($seconds);
            $this->push("D$seconds");
        }
    }

    /**
     * Takes and acknowledges jobs until none is waiting.
     *
     * @return list<string> job names in the order they were handed out; "NAME#2" is a second attempt.
     */
    private function drain(): array
    {
        $names = [];
        while (($job = $this->queue->take()) !== null) {
            $names[] = $this->jobName($job);
            $this->queue->acknowledge($job[0]);
        }

        return $names;
    }

    private function jobName(array $job): string
    {
        $name = $this->queue->serializer->unserialize($job[1])->name;

        return $job[3] > 1 ? "$name#$job[3]" : $name;
    }

    /**
     * @return list<string>
     */
    private function remainingKeys(): array
    {
        $keys = $this->redis->keys("$this->channel.*");
        sort($keys);

        return array_values(array_diff($keys, ["$this->channel.marker"]));
    }
}
