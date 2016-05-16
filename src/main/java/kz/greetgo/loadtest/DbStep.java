package kz.greetgo.loadtest;

import java.sql.Connection;
import java.util.Random;

/**
 * Created by den on 14.05.16.
 */
public interface DbStep {
    void doStep(Connection connection, Random rnd, int thread, long count);
}
