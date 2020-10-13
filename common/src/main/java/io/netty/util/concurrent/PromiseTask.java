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

import java.util.concurrent.Callable;
import java.util.concurrent.RunnableFuture;

class PromiseTask<V> extends DefaultPromise<V> implements RunnableFuture<V> {

    private static final class RunnableAdapter<T> implements Callable<T> {
        final Runnable task;
        final T result;

        RunnableAdapter(Runnable task, T result) {
            this.task = task;
            this.result = result;
        }

        @Override
        public T call() {
            task.run();
            return result;
        }

        @Override
        public String toString() {
            return "Callable(task: " + task + ", result: " + result + ')';
        }
    }

    //哨兵任务，无操作，用名称标识状态，子类ScheduledFutureTask定时重复任务时会用到
    //在一些情况下我们认为任务已经结束了，不需要再执行了，此时在定时任务队列里面可能还有这个任务，
    //需要将task设置为空操作就可以解决这个问题。
    //已成功
    private static final Runnable COMPLETED = new SentinelRunnable("COMPLETED");
    //已取消
    private static final Runnable CANCELLED = new SentinelRunnable("CANCELLED");
    //失败
    private static final Runnable FAILED = new SentinelRunnable("FAILED");

    //Sentinel:哨兵
    private static class SentinelRunnable implements Runnable {
        private final String name;

        SentinelRunnable(String name) {
            this.name = name;
        }

        @Override
        public void run() { } // no-op

        @Override
        public String toString() {
            return name;
        }
    }

    // Strictly of type Callable<V> or Runnable
    private Object task;

    PromiseTask(EventExecutor executor, Runnable runnable, V result) {
        super(executor);
        task = result == null ? runnable : new RunnableAdapter<V>(runnable, result);
    }

    PromiseTask(EventExecutor executor, Runnable runnable) {
        super(executor);
        task = runnable;
    }

    PromiseTask(EventExecutor executor, Callable<V> callable) {
        super(executor);
        task = callable;
    }

    @Override
    public final int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public final boolean equals(Object obj) {
        return this == obj;
    }

    @SuppressWarnings("unchecked")
    final V runTask() throws Exception {
        final Object task = this.task;
        //子类ScheduledFutureTask的定时重复执行逻辑中，也是调用的这个方法
        //这里不会判断是不是COMPLETED，CANCELLED，FAILED。直接执行。
        //如果某时需要取消任务等情况，定时任务队列里面可能还有这个任务，此时将task设置为空操作就可以解决这个问题。
        if (task instanceof Callable) {
            return ((Callable<V>) task).call();
        }
        ((Runnable) task).run();
        return null;
    }

    @Override
    public void run() {
        try {
            //执行前尝试设为不可取消
            if (setUncancellableInternal()) {
                V result = runTask();
                //set成功结果
                //考虑到定时重复任务，需要替换任务对象task为NO-OP任务，COMPLETED
                setSuccessInternal(result);
            }
        } catch (Throwable e) {
            //set失败结果
            //考虑到定时重复任务，需要替换任务对象task为NO-OP任务，FAILED
            setFailureInternal(e);
        }
    }

    //只有在判定当前promiseTask已完成的时候，比如success，failed，cancel=true时，
    // 替换掉task的执行逻辑，避免被执行多次，替换后就是NO-OP，相当于空跑。
    private boolean clearTaskAfterCompletion(boolean done, Runnable result) {
        if (done) {
            //只有在定期ScheduledFutureTask的情况下才可能调用哨兵任务，
            // 在这种情况下，这是一个良性竞争，并且取消并且不使用（空）返回值。
            // The only time where it might be possible for the sentinel task
            // to be called is in the case of a periodic ScheduledFutureTask,
            // in which case it's a benign race with cancellation and the (null)
            // return value is not used.
            task = result;
        }
        return done;
    }

    @Override
    public final Promise<V> setFailure(Throwable cause) {
        //final覆盖，抛异常，表示不允许调用这个方法了
        throw new IllegalStateException();
    }

    protected final Promise<V> setFailureInternal(Throwable cause) {
        super.setFailure(cause);
        //调用这个方法本身就表示已经结束了，不需要再执行了，此时在定时任务队列里面可能还有这个任务，
        // 需要将task设置为空操作就可以解决这个问题。
        clearTaskAfterCompletion(true, FAILED);
        return this;
    }

    @Override
    public final boolean tryFailure(Throwable cause) {
        return false;
    }

    protected final boolean tryFailureInternal(Throwable cause) {
        return clearTaskAfterCompletion(super.tryFailure(cause), FAILED);
    }

    @Override
    public final Promise<V> setSuccess(V result) {
        throw new IllegalStateException();
    }

    protected final Promise<V> setSuccessInternal(V result) {
        super.setSuccess(result);
        //调用这个方法本身就表示已经结束了，不需要再执行了，此时在定时任务队列里面可能还有这个任务，
        // 需要将task设置为空操作就可以解决这个问题。
        clearTaskAfterCompletion(true, COMPLETED);
        return this;
    }

    @Override
    public final boolean trySuccess(V result) {
        return false;
    }

    protected final boolean trySuccessInternal(V result) {
        return clearTaskAfterCompletion(super.trySuccess(result), COMPLETED);
    }

    @Override
    public final boolean setUncancellable() {
        //final覆盖，抛异常，表示不允许调用这个方法了
        throw new IllegalStateException();
    }

    protected final boolean setUncancellableInternal() {
        return super.setUncancellable();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        //调用父类取消方法
        //如果某时需要取消任务，在定时任务队列里面可能还有这个任务，此时将task设置为空操作就可以解决这个问题。
        return clearTaskAfterCompletion(super.cancel(mayInterruptIfRunning), CANCELLED);
    }

    @Override
    protected StringBuilder toStringBuilder() {
        StringBuilder buf = super.toStringBuilder();
        buf.setCharAt(buf.length() - 1, ',');

        return buf.append(" task: ")
                  .append(task)
                  .append(')');
    }
}
