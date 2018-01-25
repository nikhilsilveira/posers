package jpmc;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntBinaryOperator;
import java.util.stream.IntStream;

import static java.util.stream.IntStream.rangeClosed;

/**
 * Interview question by Fahd of JPMC on Sat, 20th Jan 2018 at Ventura.
 * Q: you have threads of two types: Category1 and Category2. Assume a Category1 is running.
 * Now another thread is submitted for execution.
 * Case 1: the new thread is a Category1 -- new job should immediately start executing
 * Case 2: the new thread is a Category2 -- it should wait for currently executing thread to complete
 *
 * @version 1.0
 * @since 25 Jan 2018
 * @author Nikhil Silveira
 */
public class JpThreadScheduler {
    private BlockingQueue<Thread> jobQ;

    protected Lock lock = new ReentrantLock();
    protected Condition thread2Completed = lock.newCondition();

    JpThreadScheduler() {
        jobQ = new ArrayBlockingQueue<>(1);
    }

    /**
     * Scheduler flow.
     * 1. create and start first thread
     *  - some task in a loop, checking on each iteration for:
     *      - new job submitted to Q
     *      - compare new job class with executing
     * 2. create and submit second thread to Q
     */
    public void run() {

        // 1. Initialise & start the first thread
        Category1 sum1 = new Category1();
        sum1.setName("Sum1");
        sum1.setIntStream( rangeClosed(1, 4));
        sum1.setJobQ(jobQ);
        sum1.setLockAndCondition(lock, thread2Completed);
        sum1.start();

        // 2. Initialise second thread for Q
        Category1 sum2 = new Category1();
        sum2.setName("Sum2");
        sum2.setIntStream( rangeClosed(5, 8));
        sum2.setLockAndCondition(lock, thread2Completed);
        sum2.setJobQ(jobQ);

        Category2 min1 = new Category2();
        min1.setName("Min1");
        min1.setIntStream( rangeClosed(1, 4));
        min1.setJobQ(jobQ);

        // 3. place category 1 or 2 thread on Q for scenario to play out

        try {
            jobQ.put(sum2); // put another Category1 job
            //jobQ.put(avg1); // put a Category2 job
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String... args) {
        new JpThreadScheduler().run();
    }
}


/**
 * holds a volatile reference to the currently executing Thread class.
 */
class SelfAwareThread extends Thread {
    private int result;
    private BlockingQueue<Thread> jobQ;
    private IntStream intStream;
    private Lock lock;
    private Condition thread2Completed;

    protected IntBinaryOperator operation;

    /** a thread aware reference to the currently executing class,
     * since this will be updated across threads of Categories 1 and 2
     */
    volatile Class runningClass;


    @Override
    public void run() {
        if (runningClass==null) {
            runningClass = Thread.currentThread().getClass();
            System.out.println(
                    String.format("Running %s : %s",
                            runningClass.getName(),
                            Thread.currentThread().getName()
                    ));
        }

        int[] numbers = intStream.toArray();
        Thread otherCategoryJob = null;

        try {
            for (int i : numbers) {
                result = operation.applyAsInt(result, i);
                System.out.println(
                        String.format("%s : intermediate result after operand %d -> %d",
                                Thread.currentThread().getName(),
                                i,
                                result
                        ));

                // also check for new job on every iteration
                Thread polled = jobQ.poll();
                if (polled != null) {
                    System.out.println(
                            String.format("Running: %s & Polled: %s",
                                    runningClass.getName(),
                                    polled.getClass().getName())
                    );
                    if (runningClass.equals(polled.getClass())) {
                        System.out.println(
                                String.format("Start: %s & %s to await it's completion.",
                                        polled.getName(),
                                        Thread.currentThread().getName()
                                ));
                        polled.start();
                        lock.lock();
                        thread2Completed.await();
                        lock.unlock();
                    }
                    else {
                        otherCategoryJob = polled;
                        System.out.println("New category job submitted. Continue running " + Thread.currentThread().getName());
                    }
                }
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(Thread.currentThread().getName() + " : closing result : " + result);

        if (otherCategoryJob != null) {
            otherCategoryJob.start();
        }


        // We're using a Q of capacity 1.
        // When second job done, wakeup previous running job
        lock.lock();
        thread2Completed.signalAll();
        lock.unlock();
    }

    public void setIntStream(IntStream intStream) {
        this.intStream = intStream;
    }

    public void setJobQ(BlockingQueue<Thread> jobQ) {
        this.jobQ = jobQ;
    }


    public void setLockAndCondition(Lock lock, Condition thread2Completed) {
        this.lock = lock;
        this.thread2Completed = thread2Completed;
    }
}

/**
 * calculates sum of a bounded IntStream
 */
class Category1 extends SelfAwareThread {
    Category1() {
        operation = Integer::sum;
    }
}


/**
 * calculates average for a bounded IntStream
 */
class Category2 extends SelfAwareThread {
    Category2() {
        operation = Integer::min;
    }
}