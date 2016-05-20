package kz.greetgo.kafkarpc;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by den on 20.05.16.
 */
public abstract class Synchronizer<Q, S> {
    private final AtomicLong nextRequestId = new AtomicLong();
    private final ConcurrentMap<Long, Pending<S>> pendings = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private static class Pending<S> {
        public final CountDownLatch latch = new CountDownLatch(1);
        public S response = null;
    }

    public S invoke(long timeout, TimeUnit unit, Q request) throws InterruptedException, TimeoutException { // TODO thows some
        final long id = nextRequestId.getAndIncrement();
        Pending<S> pending = new Pending<>();
        pendings.put(id, pending);
        try {
            send(id, request);
            //System.out.println(" "+request);
            if (!pending.latch.await(timeout, unit)) throw new TimeoutException();
        } finally {
            pending.latch.countDown();
            pendings.remove(id);
        }
        return pending.response;
    }

    protected abstract void send(long id, Q request);

    protected final void onResponse(long id, S response) {
        Pending<S> pending = pendings.get(id);
        if (pending == null) return; // invalid request
        pending.response = response;
        pending.latch.countDown();
    }

    protected void close() {
        if (!closed.compareAndSet(false, true)) return; // Close just ones by some thread
        for (Pending<S> pending : pendings.values()) {
            pending.latch.countDown();
        }
        pendings.clear();
    }
}
