package kz.greetgo.kafkarpc;

import java.util.concurrent.*;

/**
 * Created by den on 20.05.16.
 */
public class ResultFuture<R> implements Future<R> {
    private volatile R result = null;
    private volatile boolean cancelled = false;
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (isDone()) {
            return false;
        } else {
            countDownLatch.countDown();
            cancelled = true;
            return true;
        }
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public boolean isDone() {
        return countDownLatch.getCount() == 0;
    }

    @Override
    public R get() throws InterruptedException, ExecutionException {
        countDownLatch.await();
        return result;
    }

    @Override
    public R get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (!countDownLatch.await(timeout, unit)) throw new TimeoutException();
        return result;
    }

    public void setResult(R result) {
        this.result = result;
        countDownLatch.countDown();
    }
}
