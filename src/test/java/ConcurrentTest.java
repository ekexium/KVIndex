import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class ConcurrentTest {

    KVIndex index = new KVIndex();

    final int queryCount = 20000;

    /**
     * A thread used to test the correctness of KVIndex.
     */
    class TestThread extends Thread {

        int threadId;                               // id of the thread
        ArrayList<byte[]> keyArr, valueArr;         // key and true value list
        Random random;                              // initialized by different seeds
        int n;                                      // size of the set of keys
        int queryCount;                             // number of queries
        boolean needVerify;                         // need to verify the result of the query
        CountDownLatch barrier;                     // used to start threads simultaneously
        CountDownLatch stopLatch;                   // count the # of finished threads
        AtomicLong totalTime;                       // used to sum up time consumption of queries

        TestThread(int threadId, int queryCount, ArrayList<byte[]> keyArr,
                   ArrayList<byte[]> valueArr, int seed, boolean needVerify,
                   CountDownLatch barrier, CountDownLatch stopLatch, AtomicLong totalTime) {
            this.threadId = threadId;
            this.keyArr = keyArr;
            this.valueArr = valueArr;
            this.n = keyArr.size();
            this.queryCount = queryCount;
            random = new Random(seed);
            this.needVerify = needVerify;
            this.barrier = barrier;
            this.stopLatch = stopLatch;
            this.totalTime = totalTime;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                long startTime = System.currentTimeMillis();
                Log.logi("Thread " + threadId + " begins at " + startTime);
                for (int i = 0; i < queryCount; i++) {
                    int k = random.nextInt(n);
                    byte[] value = index.get(keyArr.get(k));
                    if (needVerify && Arrays.compare(value, valueArr.get(k)) != 0) {
                        Log.loge("Test failed: query(" + Arrays.toString(keyArr.get(k)) + ") gets"
                                 + Arrays.toString(value) + ".\nWhile true value is "
                                 + Arrays.toString(valueArr.get(k)));
                    }
                }
                long usedTime = System.currentTimeMillis() - startTime;
                totalTime.getAndAdd(usedTime);
                Log.logi("Thread " + threadId + " test succeeded, used "
                         + usedTime + " ms. avg = " + ((double) usedTime / queryCount + "ms."));
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                stopLatch.countDown();
            }
        }
    }

    void testConcurrentCorrectness(boolean benchmark) {
        try {
            int[] Ns = {10000, 20000, 50000, 100000};
            int[] threadCounts = {1, 2, 4, 8};
            for (int N : Ns) {
                Log.logi("N = " + N);
                KVIndexTest.makeData(N);
                index.initialize(KVIndexTest.filename);
                for (int threadCount : threadCounts) {
                    Log.logi("N = " + N + ", threadCount = " + threadCount);
                    CountDownLatch barrier = new CountDownLatch(1);
                    CountDownLatch stopLatch = new CountDownLatch(threadCount);
                    Random random = new Random(System.currentTimeMillis());
                    AtomicLong totalTime = new AtomicLong();

                    for (int i = 0; i < threadCount; i++) {
                        new TestThread(i, queryCount, KVIndexTest.keys, KVIndexTest.values,
                                       random.nextInt(), !benchmark, barrier, stopLatch,
                                       totalTime).start();
                    }

                    // start threads
                    barrier.countDown();

                    // wait for all threads to finish
                    stopLatch.await();

                    Log.logi("All test threads finished, avg query time = "
                             + (double) (totalTime.get()) / (threadCount * queryCount));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] agrs) {
        ConcurrentTest tester = new ConcurrentTest();
        tester.testConcurrentCorrectness(true);
    }
}
