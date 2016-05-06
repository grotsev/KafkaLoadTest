package kz.greetgo.loadtest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.System.out;

/**
 * Created by den on 04.05.16.
 */
public class TestConsumer {

    public static void main(String[] args) throws IOException, SQLException, ExecutionException, InterruptedException {
        Properties config = config(args);
        String topicNames = config.getProperty("topic.names");
        int numTopics = Integer.valueOf(config.getProperty("num.topics"));
        int numColumns = Integer.valueOf(config.getProperty("num.columns"));
        int numInserts = Integer.valueOf(config.getProperty("num.inserts"));
        int numSelects = Integer.valueOf(config.getProperty("num.selects"));
        int numSelectThreads = Integer.valueOf(config.getProperty("num.select.threads"));
        boolean testWrite = Boolean.valueOf(config.getProperty("test.write", "true"));

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(config);
        List<String> topics = IntStream.range(0, numTopics).mapToObj(i -> topicNames + i).collect(Collectors.toList());
        consumer.subscribe(topics);

        final String jdbcUrl = config.getProperty("jdbc.url");
        final String jdbcUsername = config.getProperty("jdbc.username");
        final String jdbcPassword = config.getProperty("jdbc.password");

        final Queue<Connection> queue = new ConcurrentLinkedQueue<>();
        ThreadLocal<Connection> conn = ThreadLocal.withInitial(() -> {
            try {
                Connection c = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
                c.setAutoCommit(false);
                queue.add(c);
                return c;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        });

        long start = System.currentTimeMillis();
        long now;

        if (testWrite) { // write
            try (Statement stmt = conn.get().createStatement()) {
                for (String topic : topics)
                    stmt.executeUpdate(createTable(topic, numColumns));

                while (true) {
                    ConsumerRecords<Integer, String> records = consumer.poll(1000);
                    System.out.println("Batch records count " + records.count());
                    if (records.isEmpty()) break;
                    for (ConsumerRecord<Integer, String> record : records)
                        stmt.addBatch(upsert(record));
                    stmt.addBatch("commit");
                    stmt.executeBatch();
                    stmt.clearBatch();
                    //connection.commit();
                    consumer.commitSync();
                }

                now = System.currentTimeMillis();
                System.out.println("Write time: " + (now - start));
                start = now;
            }
        }

        // read
        ForkJoinPool forkJoinPool = new ForkJoinPool(numSelectThreads);
        Long count = forkJoinPool.submit(() ->
                (long)IntStream.range(0, numSelects).parallel().reduce(0, (acc, sel) -> {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();
                    try (Statement stmt = conn.get().createStatement()) {
                        String select = select(topics, sel); // rnd.nextInt(numInserts)
                        ResultSet rs = stmt.executeQuery(select);
                        int subCount = 0;
                        while (rs.next()) {
                            int id = rs.getInt("id");
                            if (id < 0) subCount++; // Against optimisation. It is never executed because (id >= 0)
                            subCount++;
                        }
                        return acc+subCount;
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    return 0;
                })
        ).get();

        now = System.currentTimeMillis();
        System.out.println("Read time: " + (now - start) + ", count: " + count);

        for (Connection c : queue) {
            c.close();
        }
    }

    private static String select(List<String> tables, int i0) {
        int i1 = i0 + 10;
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
        sb.append(" where id between ").append(i0).append(" and ").append(i1);
        return sb.toString();
    }

    private static String createTable(String tableName, int numColumns) {
        StringBuilder sb = new StringBuilder();
        sb.append("create table if not exists ");
        sb.append(tableName);
        sb.append(" (id int not null primary key");
        for (int col = 0; col < numColumns; col++) {
            sb.append(", value");
            sb.append(col);
            sb.append(" varchar(30)");
        }
        sb.append(")");
        return sb.toString();
    }

    private static String upsert(ConsumerRecord<Integer, String> record) {
        StringBuilder sb = new StringBuilder();
        String topic = record.topic();
        Integer key = record.key();
        String[] cols = record.value().split("\\|");
        sb.setLength(0);
        sb.append("insert ");
        sb.append(topic);
        sb.append(" values (");
        sb.append(key);
        for (String col : cols) {
            sb.append(", ");
            sb.append(col);
        }
        sb.append(") on duplicate key update ");
        for (int col = 0; col < cols.length; col++) {
            if (col > 0) sb.append(", ");
            sb.append("value");
            sb.append(col);
            sb.append(" = ");
            sb.append(cols[col]);
        }
        return sb.toString();
    }

    private static Properties config(String[] args) throws IOException {
        Properties config = new Properties();
        InputStream configStream = new FileInputStream(args[0]);// TestProducer.class.getResourceAsStream("config.properties");
        config.load(configStream);
        config.putIfAbsent("enable.auto.commit", "false");
        config.putIfAbsent("auto.commit.interval.ms", "1000");
        config.putIfAbsent("session.timeout.ms", "30000");
        config.putIfAbsent("auto.offset.reset", "earliest");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return config;
    }
}
