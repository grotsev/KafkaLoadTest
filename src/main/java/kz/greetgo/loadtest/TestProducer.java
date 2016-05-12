package kz.greetgo.loadtest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

/**
 * Created by den on 03.05.16.
 */
public class TestProducer {

    public static void main(String[] args) throws IOException {
        Properties config = config(args);
        String topicNames = config.getProperty("topic.names");
        int numTopics = Integer.valueOf(config.getProperty("num.topics"));
        int numColumns = Integer.valueOf(config.getProperty("num.columns"));
        int numInserts = Integer.valueOf(config.getProperty("num.inserts"));
        int numUpdates = Integer.valueOf(config.getProperty("num.updates"));

        // TODO drop and create partitioned topics

        long start = System.currentTimeMillis();

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(config);
        StringBuilder sb = new StringBuilder();
        Random rnd = new Random();

        for (int topicNum = 0; topicNum < numTopics; topicNum++) {
            String topic = topicNames + topicNum;
            for (Integer row = 0; row < numInserts; row++)
                producer.send(new ProducerRecord<>(topic, row, row(numColumns, sb)));
            for (Integer row = 0; row < numUpdates; row++)
                producer.send(new ProducerRecord<>(topic, rnd.nextInt(numInserts), row(numColumns, sb)));
        }
        producer.close();

        long now = System.currentTimeMillis();
        System.out.println("Time: " + (now - start));
    }

    private static Properties config(String[] args) throws IOException {
        Properties config = new Properties();
        InputStream configStream = new FileInputStream(args[0]); // TestProducer.class.getResourceAsStream("config.properties");
        config.load(configStream);
        config.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return config;
    }

    private static String row(int cols, StringBuilder sb) {
        sb.setLength(0);
        for (int col = 0; col < cols; col++) {
            if (col > 0) sb.append("|");
            sb.append(Double.toString(Math.random()));
        }
        return sb.toString();
    }
}
