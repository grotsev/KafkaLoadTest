package kz.greetgo.loadtest;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

/**
 * Created by den on 14.05.16.
 */
public interface DbStep {
    void doStep(PreparedStatement ps, Random rnd) throws SQLException;
}
