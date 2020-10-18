/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.util.concurrent;

import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.ThreadExecutorMap;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 单线程单例EventExecutor。
 * 在执行execute添加任务时，它会自动启动线程。并且如果1秒钟内任务队列中一直没有待处理的任务，它将自动停止线程。
 * Single-thread singleton {@link EventExecutor}.  It starts the thread automatically and stops it when there is no
 * task pending in the task queue for 1 second.  Please note it is not scalable to schedule large number of tasks to
 * this executor; use a dedicated executor.
 */
public final class GlobalEventExecutor extends AbstractScheduledEventExecutor implements OrderedEventExecutor {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(GlobalEventExecutor.class);

    //1秒
    private static final long SCHEDULE_QUIET_PERIOD_INTERVAL = TimeUnit.SECONDS.toNanos(1);

    public static final GlobalEventExecutor INSTANCE = new GlobalEventExecutor();

    //线程安全的阻塞队列
    final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<Runnable>();
    //对象创建1秒钟后开始执行，固定延期1秒钟重复执行。无操作。属于固定延迟周期重复任务。
    //主要用于检查taskQueue，在无任务时长达到1秒后，停止线程运行。
    final ScheduledFutureTask<Void> quietPeriodTask = new ScheduledFutureTask<Void>(this, Executors.<Void>callable(new Runnable() {
        @Override
        public void run() {
            // NOOP
        }
    }, null), ScheduledFutureTask.deadlineNanos(SCHEDULE_QUIET_PERIOD_INTERVAL), -SCHEDULE_QUIET_PERIOD_INTERVAL);

    // because the GlobalEventExecutor is a singleton, tasks submitted to it can come from arbitrary threads and this
    // can trigger the creation of a thread from arbitrary thread groups; for this reason, the thread factory must not
    // be sticky about its thread group
    // visible for testing
    //这个工厂生产的线程类型为FastThreadLocalThread，使用FastThreadLocal保存每个线程对应的executor。
    final ThreadFactory threadFactory;
    //无限循环从taskQueue中取任务，同时检查taskQueue是否空闲时长达到1秒
    private final TaskRunner taskRunner = new TaskRunner();
    //是否已经有线程启动运行
    private final AtomicBoolean started = new AtomicBoolean();
    //当锁使用，非阻塞锁，保证同一时刻只能有一个线程执行逻辑
    volatile Thread thread;

    //表示不支持这个操作，如shutDown
    private final Future<?> terminationFuture = new FailedFuture<Object>(this, new UnsupportedOperationException());

    private GlobalEventExecutor() {
        //添加quietPeriodTask
        scheduledTaskQueue().add(quietPeriodTask);
        threadFactory = ThreadExecutorMap.apply(new DefaultThreadFactory(DefaultThreadFactory.toPoolName(getClass()), false, Thread.NORM_PRIORITY, null), this);
    }

    /**
     * 从taskQueue中获取任务，如果超过quietPeriodTask的延迟周期1s后，taskQueue还为空的话，
     * 就将quietPeriodTask从scheduledTaskQueue取出，放到taskQueue，再从taskQueue取任务，返回任务。
     * Take the next {@link Runnable} from the task queue and so will block if no task is currently present.
     * @return {@code null} if the executor thread has been interrupted or waken up.
     */
    Runnable takeTask() {
        BlockingQueue<Runnable> taskQueue = this.taskQueue;
        for (; ; ) {
            //因为执行takeTask()方法时，当前GlobalEventExecutor对象构造方法已经执行完毕，
            //所以scheduledTaskQueue不会为null，有可能为空，或者里面有一个quietPeriodTask。
            ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
            if (scheduledTask == null) {
                //出现scheduledTaskQueue为空的情况是，在quietPeriodTask的delayNanos时间内有大量的任务加到队列，
                //quietPeriodTask达到执行时间，被从scheduledTaskQueue取出，放到taskQueue,
                //但是从任务入队积压顺序上来说，quietPeriodTask暂时无法执行，导致此时scheduledTaskQueue为空
                Runnable task = null;
                try {
                    //任务积压，直接取
                    task = taskQueue.take();
                } catch (InterruptedException e) {
                    // Ignore
                }
                return task;
            } else {
                long delayNanos = scheduledTask.delayNanos();
                Runnable task = null;
                if (delayNanos > 0) {
                    try {
                        //从taskQueue获取任务，最多等待delayNanos
                        task = taskQueue.poll(delayNanos, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        // Waken up.
                        return null;
                    }
                }

                //两种情况：
                //1. delayNanos<=0 周期任务到了执行时间
                //2. 在delayNanos时间内，taskQueue一直为空，无新任务
                if (task == null) {
                    // We need to fetch the scheduled tasks now as otherwise there may be a chance that
                    // scheduled tasks are never executed if there is always one task in the taskQueue.
                    // This is for example true for the read task of OIO Transport
                    // See https://github.com/netty/netty/issues/1614
                    //将quietPeriodTask从scheduledTaskQueue取出，放到taskQueue
                    fetchFromScheduledTaskQueue();
                    task = taskQueue.poll();
                }

                if (task != null) {
                    return task;
                }
            }
        }
    }

    private void fetchFromScheduledTaskQueue() {
        long nanoTime = AbstractScheduledEventExecutor.nanoTime();
        Runnable scheduledTask = pollScheduledTask(nanoTime);
        //从scheduledTaskQueue中取出所有超期的定时任务
        while (scheduledTask != null) {
            taskQueue.add(scheduledTask);
            scheduledTask = pollScheduledTask(nanoTime);
        }
    }

    /**
     * Return the number of tasks that are pending for processing.
     *
     * <strong>Be aware that this operation may be expensive as it depends on the internal implementation of the
     * SingleThreadEventExecutor. So use it was care!</strong>
     */
    public int pendingTasks() {
        return taskQueue.size();
    }

    /**
     * Add a task to the task queue, or throws a {@link RejectedExecutionException} if this instance was shutdown
     * before.
     */
    private void addTask(Runnable task) {
        taskQueue.add(ObjectUtil.checkNotNull(task, "task"));
    }

    @Override
    public boolean inEventLoop(Thread thread) {
        //当前线程是否是运行taskRunner任务的线程
        return thread == this.thread;
    }

    @Override
    public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
        //不支持这个操作
        return terminationFuture();
    }

    @Override
    public Future<?> terminationFuture() {
        return terminationFuture;
    }

    @Override
    @Deprecated
    public void shutdown() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isShuttingDown() {
        return false;
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        return false;
    }

    /**
     * Waits until the worker thread of this executor has no tasks left in its task queue and terminates itself.
     * Because a new worker thread will be started again when a new task is submitted, this operation is only useful
     * when you want to ensure that the worker thread is terminated <strong>after</strong> your application is shut
     * down and there's no chance of submitting a new task afterwards.
     * @return {@code true} if and only if the worker thread has been terminated
     */
    public boolean awaitInactivity(long timeout, TimeUnit unit) throws InterruptedException {
        ObjectUtil.checkNotNull(unit, "unit");

        final Thread thread = this.thread;
        if (thread == null) {
            throw new IllegalStateException("thread was not started");
        }
        thread.join(unit.toMillis(timeout));
        return !thread.isAlive();
    }

    @Override
    public void execute(Runnable task) {
        //向taskQueue添加task
        addTask(ObjectUtil.checkNotNull(task, "task"));
        if (!inEventLoop()) {
            //属性字段thread为null 或者 thread != 当前线程
            //创建并启动线程
            //这里保证无论有多少线程提交多少任务，GlobalEventExecutor都是单线程运行的
            startThread();
        }
    }

    private void startThread() {
        //如果已启动，当前线程就没什么可做了，退出方法
        //相当于保证只有一个线程存在，保持单线程状态运行
        if (started.compareAndSet(false, true)) {
            //如果没有启动，将标识置为true
            //创建一个线程，分配任务taskRunner
            final Thread t = threadFactory.newThread(taskRunner);
            // Set to null to ensure we not create classloader leaks by holds a strong reference to the inherited
            // classloader.
            // See:
            // - https://github.com/netty/netty/issues/7290
            // - https://bugs.openjdk.java.net/browse/JDK-7008595
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    t.setContextClassLoader(null);
                    return null;
                }
            });

            // Set the thread before starting it as otherwise inEventLoop() may return false and so produce
            // an assert error.
            // See https://github.com/netty/netty/issues/4357
            //保存引用并启动
            thread = t;
            t.start();
        }
    }


    /**
     * 主要逻辑是：
     * 只有1个线程，循环从队列中取任务，执行任务，全程一直是单线程运行。
     * 如果taskQueue空闲时间超过1秒，就退出run()方法，单线程结束运行。
     */
    final class TaskRunner implements Runnable {
        @Override
        public void run() {
            for (; ; ) {
                //从taskQueue中取数据
                Runnable task = takeTask();
                if (task != null) {
                    try {
                        //执行任务
                        task.run();
                    } catch (Throwable t) {
                        //捕获所有异常，不抛出
                        logger.warn("Unexpected exception from the global event executor: ", t);
                    }

                    //如果是quietPeriodTask，执行完会放回到scheduledTaskQueue中
                    //如果不是quietPeriodTask，继续循环
                    if (task != quietPeriodTask) {
                        continue;
                    }
                }

                //从taskQueue中取出的是null 或者 task是quietPeriodTask
                Queue<ScheduledFutureTask<?>> scheduledTaskQueue = GlobalEventExecutor.this.scheduledTaskQueue;

                // Terminate if there is no task in the queue (except the noop task).
                //taskQueue为空 并且 scheduledTaskQueue为空或者只有一个对象，即上面放回来的周期重复任务quietPeriodTask
                //如果queue中除了noop任务以外没有任务了，终止循环，线程退出
                if (taskQueue.isEmpty() && (scheduledTaskQueue == null || scheduledTaskQueue.size() == 1)) {
                    // Mark the current thread as stopped.
                    // The following CAS must always success and must be uncontended,
                    // because only one thread should be running at the same time.
                    //因为是单线程，所以这里一定成功，将started置为false
                    boolean stopped = started.compareAndSet(true, false);
                    assert stopped;

                    //检查，在我们执行上面boolean stopped = started.compareAndSet(true, false);代码时，
                    //是否有线程调用了execute(task)，向taskQueue提交了新任务
                    // Check if there are pending entries added by execute() or schedule*() while we do CAS above.
                    if (taskQueue.isEmpty() && (scheduledTaskQueue == null || scheduledTaskQueue.size() == 1)) {
                        //taskQueue还是空的，没有新任务提交。这种可能有两种情况：
                        // A) 没有新任务提交 -> 安全终止，因为没有任务需要执行了
                        // B) 一个新的线程启动了，即我们将started置为了false，相当于释放了锁，此时有其他线程执行了execute(task)
                        //    改变了started状态为true，持有锁，创建了新线程，执行任务。 -> 安全终止，因为剩下的有新线程处理了。

                        // break; 退出循环，线程终止。

                        // A) No new task was added and thus there's nothing to handle
                        //    -> safe to terminate because there's nothing left to do
                        // B) A new thread started and handled all the new tasks.
                        //    -> safe to terminate the new thread will take care the rest
                        break;
                    }

                    //有新任务添加到队列，需要继续执行任务，不能终止。
                    //尝试重新获得锁，即将started置为true
                    // There are pending tasks added again.
                    if (!started.compareAndSet(false, true)) {
                        //获取锁失败了，表示现在有另一个新线程获得了锁，启动处理任务了，当前线程可以安全关闭了。
                        // startThread() started a new thread and set 'started' to true.
                        // -> terminate this thread so that the new thread reads from taskQueue exclusively.
                        break;
                    }

                    //重新获取锁成功，继续执行任务，处理队列里的任务。

                    // New tasks were added, but this worker was faster to set 'started' to true.
                    // i.e. a new worker thread was not started by startThread().
                    // -> keep this thread alive to handle the newly added entries.
                }
            }
        }
    }
}
