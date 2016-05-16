package kz.greetgo.loadtest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Created by den on 14.05.16.
 */
public class DbThread extends Thread {
    private final Random rnd = ThreadLocalRandom.current();
    private final Supplier<Consumer<Consumer<Connection>>> cs;
    private final AtomicBoolean stillWork;
    private final int thread;
    private final DbStep step;
    private long begin, end;
    private long count = 0;

    public DbThread(Supplier<Consumer<Consumer<Connection>>> cs, AtomicBoolean stillWork, int thread, DbStep step) {
        this.cs = cs;
        this.stillWork = stillWork;
        this.thread = thread;
        this.step = step;
    }

    @Override
    public void run() {
        cs.get().accept(
                connection -> {
                    begin = DbLoad.stamp();
                    while (stillWork.get()) {
                        step.doStep(connection, rnd, thread, count);
                        count++;
                    }
                    end = DbLoad.stamp();
                }
        );
    }

    public long diration() {
        return end - begin;
    }

    public long count() {
        return count;
    }
}
