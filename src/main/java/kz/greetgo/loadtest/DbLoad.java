package kz.greetgo.loadtest;

import com.despegar.jdbc.galera.GaleraClient;
import com.despegar.jdbc.galera.consistency.ConsistencyLevel;
import com.despegar.jdbc.galera.policies.ElectionNodePolicy;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Created by den on 11.05.16.
 */
public class DbLoad {
    public static void main(String[] args) throws IOException, InterruptedException {
        Properties config = new Properties();
        InputStream configStream = new FileInputStream(args[0]);
        config.load(configStream);

        int batchSize = Integer.parseInt(config.getProperty("batch.size", "1000"));
        int tableCount = Integer.parseInt(config.getProperty("table.count", "20"));
        int rowCount = Integer.parseInt(config.getProperty("row.count", "1000"));
        String tableName = config.getProperty("table.name", "test");
        int numThreads = Integer.parseInt(config.getProperty("num.select.threads", "4"));
        long sleepBeforeUpdate = Long.parseLong(config.getProperty("sleep.before.ms", "1000"));
        long sleepAfterUpdate = Long.parseLong(config.getProperty("sleep.after.ms", "1000"));
        long sleepFreeUpsert = Long.parseLong(config.getProperty("sleep.free.upsert.ms", "1000"));

        GaleraClient galeraClient = new GaleraClient.Builder()
                .poolName(config.getProperty("pool.name"))
                .seeds(config.getProperty("seeds"))
                .database(config.getProperty("database"))
                .user(config.getProperty("jdbc.username"))
                .password(config.getProperty("jdbc.password"))
                .discoverPeriod(Long.parseLong(config.getProperty("discover.period.ms" ,"2000")))
                .ignoreDonor(Boolean.parseBoolean(config.getProperty("ignore.donor", "true")))
                .retriesToGetConnection(Integer.parseInt(config.getProperty("retries.to.get.connection", "0")))
                .build();
        Supplier<Consumer<Consumer<Connection>>> cs = connection(galeraClient);

        List<String> tables = IntStream.range(0, tableCount).mapToObj(i -> tableName + i).collect(Collectors.toList());

        // create
        Consumer<Consumer<Connection>> cc = cs.get();
        cc.accept(batch(batchSize, tables.stream().flatMap(table -> Stream.of(
                "drop table if exists " + table,
                "create table if not exists " + table + " ( id int not null primary key, val varchar(50) )"
        ))));

        List<Long> mainStamps = new ArrayList<>();
        stamp(mainStamps); // 0

        // insert
        cc.accept(batch(batchSize, IntStream.range(0, tableCount).boxed().flatMap(
                table -> IntStream.range(0, rowCount).mapToObj(
                        row -> "insert " + tableName + table + " values (" + row + ", " + Math.random() + ")"
                ))));

        stamp(mainStamps); // 1

        AtomicBoolean stillWork = new AtomicBoolean(true);

        List<Long>[] threadStamps = new List[numThreads];

        { // select
            String select = selects(tables);
            IntStream.range(0, numThreads).forEach(selectThread -> new Thread() {
                @Override
                public void run() {
                    List<Long> stamps = new ArrayList<Long>();
                    threadStamps[selectThread] = stamps;

                    Random rnd = ThreadLocalRandom.current();
                    cs.get().accept(
                            connection -> {
                                try (PreparedStatement ps = connection.prepareStatement(select)) {
                                    stamp(stamps);
                                    while (stillWork.get()) {
                                        int id = rnd.nextInt(rowCount);
                                        ps.setInt(1, id);
                                        ResultSet rs = ps.executeQuery();
                                        while (rs.next()) {
                                            rs.getInt("id");
                                        }
                                        stamp(stamps);
                                    }
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }
                            }
                    );
                }
            }.start());
        }

        stamp(mainStamps); // 2

        Thread.sleep(sleepBeforeUpdate);

        stamp(mainStamps); // 3

        // insert
        cc.accept(batch(batchSize, IntStream.range(0, tableCount).boxed().flatMap(
                table -> IntStream.range(rowCount / 2, 3 * rowCount / 2).mapToObj(
                        row -> "insert " + tableName + table + " values (" + row + ", " + Math.random()
                                + ") on duplicate key update val = " + Math.random()
                ))));

        stamp(mainStamps); // 4

        Thread.sleep(sleepAfterUpdate);
        stillWork.set(false);
        Thread.sleep(1000);

        stamp(mainStamps); // 5

        // analyze stamps

        System.out.println("Insert: " + (mainStamps.get(1) - mainStamps.get(0)));
        System.out.println("Threads create: " + (mainStamps.get(2) - mainStamps.get(1)));
        System.out.println("Select before: " + (mainStamps.get(3) - mainStamps.get(2)));
        System.out.println("Selects within: " + (mainStamps.get(4) - mainStamps.get(3)));
        System.out.println("Selects after: " + (mainStamps.get(5) - mainStamps.get(4)));

        class Rng implements LongPredicate {
            private final long from;
            private final long to;

            Rng(int n) {
                from = mainStamps.get(n);
                to = mainStamps.get(n + 1);
            }

            @Override
            public boolean test(long s) {
                return from <= s && s < to;
            }
        }

        int[] totalCount = new int[7];
        Stream.of(threadStamps).forEach(
                stamps -> {
                    System.out.print("Before Within After (min max avg count tps)");
                    IntStream.of(2, 3, 4).forEach(
                            stage -> {
                                long duration = mainStamps.get(stage + 1) - mainStamps.get(stage);
                                long[] diffs = stamps.stream().mapToLong(Long::longValue).filter(new Rng(stage)).map(new Dif()).skip(1).toArray();
                                System.out.print(String.format(" (%d %d %d %d %d)"
                                        , LongStream.of(diffs).min().orElse(0)
                                        , LongStream.of(diffs).max().orElse(0)
                                        , (long) LongStream.of(diffs).average().orElse(0)
                                        , diffs.length
                                        , tps(diffs.length, duration)
                                ));
                                totalCount[stage] += diffs.length;
                            }
                    );
                    System.out.println();
                }
        );
        System.out.println(String.format("Total count select (before within after) (%d %d %d)", totalCount[2], totalCount[3], totalCount[4]));
        System.out.println(String.format("Total tps select (before within after) (%d %d %d)"
                , tps(totalCount[2], mainStamps.get(2 + 1) - mainStamps.get(2))
                , tps(totalCount[3], mainStamps.get(3 + 1) - mainStamps.get(3))
                , tps(totalCount[4], mainStamps.get(4 + 1) - mainStamps.get(4))
        ));

        //

        Thread.sleep(sleepFreeUpsert);
        stillWork.set(true);
        stamp(mainStamps); // 6

        { // free upsert
            IntStream.range(0, numThreads).forEach(thread -> new Thread() {
                @Override
                public void run() {
                    List<Long> stamps = new ArrayList<Long>();
                    threadStamps[thread] = stamps;

                    Random rnd = ThreadLocalRandom.current();
                    cs.get().accept(
                            connection -> {
                                try (Statement st = connection.createStatement()) {
                                    stamp(stamps);
                                    while (stillWork.get()) {
                                        // (2*rowCount) 50% updates and 50% inserts
                                        String upsert = "insert " + tableName + rnd.nextInt(tableCount)
                                                + " values (" + rnd.nextInt(2 * rowCount) + ", " + rnd.nextDouble()
                                                + ") on duplicate key update val = " + rnd.nextDouble();
                                        st.executeUpdate(upsert);
                                        stamp(stamps);
                                    }
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }
                            }
                    );
                }
            }.start());
        }

        Thread.sleep(sleepFreeUpsert);
        stillWork.set(false);
        Thread.sleep(1000);
        stamp(mainStamps); // 7

        Stream.of(threadStamps).forEach(
                stamps -> {
                    System.out.print("Free upsert (min max avg count tps)");
                    IntStream.of(6).forEach(
                            stage -> {
                                long duration = mainStamps.get(stage + 1) - mainStamps.get(stage);
                                long[] diffs = stamps.stream().mapToLong(Long::longValue).filter(new Rng(stage)).map(new Dif()).skip(1).toArray();
                                System.out.print(String.format(" (%d %d %d %d %d)"
                                        , LongStream.of(diffs).min().orElse(0)
                                        , LongStream.of(diffs).max().orElse(0)
                                        , (long) LongStream.of(diffs).average().orElse(0)
                                        , diffs.length
                                        , tps(diffs.length, duration)
                                ));
                                totalCount[stage] += diffs.length;
                            }
                    );
                    System.out.println();
                }
        );
        System.out.println(String.format("Total count free upsert %d", totalCount[6]));
        System.out.println(String.format("Total tps free upsert %d"
                , tps(totalCount[6], mainStamps.get(6 + 1) - mainStamps.get(6))
        ));
    }

    private static long tps(long count, long mks) {
        return (long) (1_000_000 * count / mks);
    }

    private static class Dif implements LongUnaryOperator {
        private long prev = 0;

        @Override
        public long applyAsLong(long l) {
            long result = l - prev;
            prev = l;
            return result;
        }
    }

    private static String selects(List<String> tables) {
        StringBuilder sb = new StringBuilder();
        sb.append("select * from");
        boolean tail = false;
        for (String table : tables) {
            if (tail) {
                sb.append(" left join ").append(table).append(" using (id)");
            } else {
                sb.append(" ").append(table);
                tail = true;
            }
        }
        sb.append(" where id = ?");
        return sb.toString();
    }

    private static final Supplier<Consumer<Consumer<Connection>>> connection(GaleraClient client) {
        return () -> consumer -> {
            try (Connection connection = client.getConnection()) {
                connection.setAutoCommit(false);
                consumer.accept(connection);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        };
    }

    private static Consumer<Connection> batch(int batchSize, Stream<String> sqls) {
        return connection -> {
            try (Statement statement = connection.createStatement()) {
                int n = 0;
                Iterator<String> it = sqls.iterator();
                while (it.hasNext()) {
                    String sql = it.next();
                    statement.addBatch(sql);
                    n++;
                    if (n == batchSize) {
                        statement.addBatch("commit");
                        statement.executeBatch();
                        statement.clearBatch();
                        n = 0;
                    }
                }
                if (n > 0) {
                    statement.addBatch("commit");
                    statement.executeBatch();
                    statement.clearBatch();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        };
    }

    public static void stamp(List<Long> stamps) {
        stamps.add(System.nanoTime() / 1000);
    }
}
