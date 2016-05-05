package kz.greetgo.loadtest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static java.lang.System.out;

/**
 * Created by den on 04.05.16.
 */
public class TestConsumer {

    public static void main(String[] args) throws IOException, SQLException {
        Properties config = config();
        Map<String, int[]> topics = TestProducer.topics(config);

        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(config);
        ArrayList<String> topicNames = new ArrayList<>(topics.keySet());
        consumer.subscribe(topicNames);

        String jdbcUrl = config.getProperty("jdbc.url");
        String jdbcUsername = config.getProperty("jdbc.username");
        String jdbcPassword = config.getProperty("jdbc.password");
        Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
        connection.setAutoCommit(false);

        Statement stmt = connection.createStatement();
        for (Map.Entry<String, int[]> entry : topics.entrySet()) {
            String createTable = createTable(entry.getKey(), entry.getValue()[0]);
            stmt.executeUpdate(createTable);
            //out.println(createTable);
        }

        long start = System.currentTimeMillis();
        while (true) {
            ConsumerRecords<Long, String> records = consumer.poll(1000);
            System.out.println("Count "+ records.count());
            if (records.isEmpty()) break;
            stmt.clearBatch();
            for (ConsumerRecord<Long, String> record : records) {
                String upsert = upsert(record);
                stmt.addBatch(upsert);
                //out.println(upsert);
            }

            long now = System.currentTimeMillis();
            System.out.println("Before commit "+ (now-start));

            stmt.executeBatch();
            connection.commit();
            consumer.commitSync();

            now = System.currentTimeMillis();
            System.out.println("After  commit "+(now-start));
        }

        stmt.close();
        connection.close();
    }

    private static String createTable(String tableName, int columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("create table if not exists ");
        sb.append(tableName);
        sb.append(" (id int not null primary key");
        for (int col = 0; col < columns; col++) {
            sb.append(", value");
            sb.append(col);
            sb.append(" varchar(30)");
        }
        sb.append(")");
        return sb.toString();
    }

    private static String upsert(ConsumerRecord<Long, String> record) {
        StringBuilder sb = new StringBuilder();
        String topic = record.topic();
        Long key = record.key();
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
        for (int col = 0; col<cols.length; col++) {
            if (col>0) sb.append(", ");
            sb.append("value");
            sb.append(col);
            sb.append(" = ");
            sb.append(cols[col]);
        }
        return sb.toString();
    }

    private static Properties config() throws IOException {
        Properties config = new Properties();
        InputStream configStream = TestProducer.class.getResourceAsStream("config.properties");
        config.load(configStream);
        config.putIfAbsent("enable.auto.commit", "false");
        config.putIfAbsent("auto.commit.interval.ms", "1000");
        config.putIfAbsent("session.timeout.ms", "30000");
        config.putIfAbsent("auto.offset.reset", "earliest");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return config;
    }
}
