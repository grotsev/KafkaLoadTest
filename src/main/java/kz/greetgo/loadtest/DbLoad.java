package kz.greetgo.loadtest;

import com.despegar.jdbc.galera.GaleraClient;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
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
        int numTables = Integer.parseInt(config.getProperty("num.tables", "20"));
        int rowCount = Integer.parseInt(config.getProperty("row.count", "1000"));
        String tableName = config.getProperty("table.name", "test");
        int numSelectThreads = Integer.parseInt(config.getProperty("num.select.threads", "4"));
        long wSleep = Long.parseLong(config.getProperty("write.sleep.ms", "1000"));
        long rSleep = Long.parseLong(config.getProperty("read.sleep.ms", "1000"));
        long rwSleep = Long.parseLong(config.getProperty("read.write.sleep.ms", "1000"));

        GaleraClient galeraClient = new GaleraClient.Builder()
                .poolName(config.getProperty("pool.name"))
                .seeds(config.getProperty("seeds"))
                .database(config.getProperty("database"))
                .user(config.getProperty("jdbc.username"))
                .password(config.getProperty("jdbc.password"))
                .discoverPeriod(Long.parseLong(config.getProperty("discover.period.ms", "2000")))
                .ignoreDonor(Boolean.parseBoolean(config.getProperty("ignore.donor", "true")))
                .retriesToGetConnection(Integer.parseInt(config.getProperty("retries.to.get.connection", "0")))
                .build();
        Supplier<Consumer<Consumer<Connection>>> cs = connection(galeraClient);

        List<String> tables = IntStream.range(0, numTables).mapToObj(i -> tableName + i).collect(Collectors.toList());

        // create
        Consumer<Consumer<Connection>> cc = cs.get();
        cc.accept(batch(batchSize, tables.stream().flatMap(table -> Stream.of(
                "drop table if exists " + table,
                "create table if not exists " + table + " ( id int not null primary key, val varchar(50) )"
        ))));

        // DbSteps
        DbStep readStep = (PreparedStatement ps, Random rnd) -> {
            int id = rnd.nextInt(rowCount);
            ps.setInt(1, id);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                rs.getInt("id");
            }
        };

        String writeSql = "insert %s values (? , ?) on duplicate key update val = ?";
        DbStep writeStep = (PreparedStatement ps, Random rnd) -> {
            ps.setInt(1, rnd.nextInt(2 * rowCount)); // ~50% insert/update
            String val = Double.toString(rnd.nextDouble());
            ps.setString(2, val);
            ps.setString(3, val);
            ps.executeUpdate();
        };

        // DbThreads
        AtomicBoolean wWork = new AtomicBoolean(true);
        AtomicBoolean rWork = new AtomicBoolean(true);
        AtomicBoolean rwWork = new AtomicBoolean(true);

        DbThread[] wThreads = IntStream.range(0, numTables).mapToObj(
                thread -> new DbThread(cs, wWork, String.format(writeSql, tableName + thread), writeStep))
                .toArray(DbThread[]::new);

        DbThread[] rThreads = IntStream.range(0, numSelectThreads).mapToObj(
                thread -> new DbThread(cs, rWork, selects(tables), readStep))
                .toArray(DbThread[]::new);

        DbThread[] rwrThreads = IntStream.range(0, numSelectThreads).mapToObj(
                thread -> new DbThread(cs, rwWork, selects(tables), readStep))
                .toArray(DbThread[]::new);

        DbThread[] rwwThreads = IntStream.range(0, numTables).mapToObj(
                thread -> new DbThread(cs, rwWork, String.format(writeSql, tableName + thread), writeStep))
                .toArray(DbThread[]::new);

        // run
        long wBegin = stamp();
        Arrays.stream(wThreads).forEach(thread -> thread.start());
        Thread.sleep(wSleep);
        wWork.set(false);
        long wEnd = stamp();

        long rBegin = stamp();
        Arrays.stream(rThreads).forEach(thread -> thread.start());
        Thread.sleep(rSleep);
        rWork.set(false);
        long rEnd = stamp();

        long rwBegin = stamp();
        Arrays.stream(rwrThreads).forEach(thread -> thread.start());
        Arrays.stream(rwwThreads).forEach(thread -> thread.start());
        Thread.sleep(rwSleep);
        rwWork.set(false);
        long rwEnd = stamp();


        // analyze stamps

        Function<DbThread, long[]> pair = thread -> new long[]{thread.count(), thread.diration()};
        Consumer<long[]> printPair = array -> System.out.printf("count\t%d, duration\t%d, tps\t%d\n"
                , array[0], array[1], array[1] == 0 ? -1 : (long) (1_000_000 * array[0] / array[1]));
        BinaryOperator<long[]> sum = (a, b) -> new long[]{a[0] + b[0], a[1] + b[1]};

        System.out.println("Write");
        Arrays.stream(wThreads).map(pair).peek(printPair).reduce(sum).ifPresent(printPair);
        System.out.println("Read");
        Arrays.stream(rThreads).map(pair).peek(printPair).reduce(sum).ifPresent(printPair);
        System.out.println("Both Read");
        Arrays.stream(rwrThreads).map(pair).peek(printPair).reduce(sum).ifPresent(printPair);
        System.out.println("Both Write");
        Arrays.stream(rwwThreads).map(pair).peek(printPair).reduce(sum).ifPresent(printPair);
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

    public static long stamp() {
        return System.nanoTime() / 1000;
    }
}
