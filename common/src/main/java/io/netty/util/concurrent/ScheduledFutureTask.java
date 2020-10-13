/*
 * Copyright 2013 The Netty Project
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

import io.netty.util.internal.DefaultPriorityQueue;
import io.netty.util.internal.PriorityQueueNode;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
final class ScheduledFutureTask<V> extends PromiseTask<V> implements ScheduledFuture<V>, PriorityQueueNode {
    private static final long START_TIME = System.nanoTime();

    static long nanoTime() {
        return System.nanoTime() - START_TIME;
    }

    static long deadlineNanos(long delay) {
        long deadlineNanos = nanoTime() + delay;
        // Guard against overflow
        return deadlineNanos < 0 ? Long.MAX_VALUE : deadlineNanos;
    }

    static long initialNanoTime() {
        return START_TIME;
    }

    // set once when added to priority queue
    private long id;

    //非线程安全的
    private long deadlineNanos;
    /*
       0 - no repeat(0为不重复),
      >0 - repeat at fixed rate(>0固定频率重复),
      <0 - repeat with fixed delay(<0固定延迟重复)
     */
    private final long periodNanos;

    //当前任务在队列中的初始索引下标为-1，表示不在队列中
    private int queueIndex = INDEX_NOT_IN_QUEUE;

    ScheduledFutureTask(AbstractScheduledEventExecutor executor, Runnable runnable, long nanoTime) {
        super(executor, runnable);
        deadlineNanos = nanoTime;
        periodNanos = 0;
    }

    ScheduledFutureTask(AbstractScheduledEventExecutor executor, Runnable runnable, long nanoTime, long period) {
        super(executor, runnable);
        deadlineNanos = nanoTime;
        periodNanos = validatePeriod(period);
    }

    ScheduledFutureTask(AbstractScheduledEventExecutor executor, Callable<V> callable, long nanoTime, long period) {
        super(executor, callable);
        deadlineNanos = nanoTime;
        periodNanos = validatePeriod(period);
    }

    ScheduledFutureTask(AbstractScheduledEventExecutor executor, Callable<V> callable, long nanoTime) {
        super(executor, callable);
        deadlineNanos = nanoTime;
        periodNanos = 0;
    }

    private static long validatePeriod(long period) {
        if (period == 0) {
            throw new IllegalArgumentException("period: 0 (expected: != 0)");
        }
        return period;
    }

    ScheduledFutureTask<V> setId(long id) {
        if (this.id == 0L) {
            this.id = id;
        }
        return this;
    }

    @Override
    protected EventExecutor executor() {
        return super.executor();
    }

    public long deadlineNanos() {
        return deadlineNanos;
    }

    //set已消费
    void setConsumed() {
        //进行优化以避免在截止日期已过并且任务已出队后再次检查系统时钟
        // Optimization to avoid checking system clock again
        // after deadline has passed and task has been dequeued
        if (periodNanos == 0) {
            //一次性任务
            assert nanoTime() >= deadlineNanos;
            deadlineNanos = 0L;
        }
    }

    public long delayNanos() {
        return deadlineToDelayNanos(deadlineNanos());
    }

    static long deadlineToDelayNanos(long deadlineNanos) {
        return deadlineNanos == 0L ? 0L : Math.max(0L, deadlineNanos - nanoTime());
    }

    public long delayNanos(long currentTimeNanos) {
        return deadlineNanos == 0L ? 0L : Math.max(0L, deadlineNanos() - (currentTimeNanos - START_TIME));
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(delayNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        if (this == o) {
            return 0;
        }

        ScheduledFutureTask<?> that = (ScheduledFutureTask<?>) o;
        //距离截止时间的纳秒数 越近的优先级越高 也应该越先执行
        long d = deadlineNanos() - that.deadlineNanos();
        if (d < 0) {
            return -1;
        } else if (d > 0) {
            return 1;
        } else if (id < that.id) {
            return -1;
        } else {
            assert id != that.id;
            return 1;
        }
    }

    @Override
    public void run() {
        assert executor().inEventLoop();
        //要保证只有一个线程会执行这个逻辑
        try {
            if (delayNanos() > 0L) {
                //还没到deadlineNanos，也就是还没有到运行时间，都加到队列里去，按优先级处理，
                // 可以看下比较器中的优先级判断逻辑，有按到期时间比较的。
                //如果当前future已经取消了，不加、或者从队列中删除。
                // Not yet expired, need to add or remove from queue
                if (isCancelled()) {
                    scheduledExecutor().scheduledTaskQueue().removeTyped(this);
                } else {
                    //生成taskId，加到队列中
                    scheduledExecutor().scheduleFromEventLoop(this);
                }
                return;
            }

            //到时间了，deadlineNanos
            if (periodNanos == 0) {
                //一次性的任务
                if (setUncancellableInternal()) {
                    V result = runTask();
                    setSuccessInternal(result);
                }
            } else {
                //重复任务
                // check if is done as it may was cancelled
                if (!isCancelled()) {
                    //可能是netty认为周期重复的任务，应该一直进行下去，不应该结束，即不应该调用setSuccess，setFail之类的
                    runTask();

                    if (!executor().isShutdown()) {
                        if (periodNanos > 0) {
                            //固定频率重复的任务，应该在本次的截止时间基础上计算下次的执行时间。
                            deadlineNanos += periodNanos;
                        } else {
                            //固定延迟重复的任务，应该在任务执行完成时间，也就是当前时间基础上计算下次的执行时间。
                            //负负为正，注意这里实际是加法。
                            deadlineNanos = nanoTime() - periodNanos;
                        }

                        if (!isCancelled()) {
                            //没取消 加到队列里去 等待再次执行
                            scheduledExecutor().scheduledTaskQueue().add(this);
                        }
                    }
                }
            }
        } catch (Throwable cause) {
            //失败
            setFailureInternal(cause);
        }
    }

    private AbstractScheduledEventExecutor scheduledExecutor() {
        return (AbstractScheduledEventExecutor) executor();
    }

    /**
     * {@inheritDoc}
     * @param mayInterruptIfRunning this value has no effect in this implementation.
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean canceled = super.cancel(mayInterruptIfRunning);
        if (canceled) {
            //取消成功，从队列中删除
            scheduledExecutor().removeScheduled(this);
        }
        return canceled;
    }

    boolean cancelWithoutRemove(boolean mayInterruptIfRunning) {
        return super.cancel(mayInterruptIfRunning);
    }

    @Override
    protected StringBuilder toStringBuilder() {
        StringBuilder buf = super.toStringBuilder();
        buf.setCharAt(buf.length() - 1, ',');

        return buf.append(" deadline: ")
                .append(deadlineNanos)
                .append(", period: ")
                .append(periodNanos)
                .append(')');
    }

    @Override
    public int priorityQueueIndex(DefaultPriorityQueue<?> queue) {
        return queueIndex;
    }

    @Override
    public void priorityQueueIndex(DefaultPriorityQueue<?> queue, int i) {
        queueIndex = i;
    }
}
