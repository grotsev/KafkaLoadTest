package kz.greetgo.loadtest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Created by den on 03.05.16.
 */
public class TestProducer {

    public static void main(String[] args) throws IOException {
        Properties config = config();
        Map<String, int[]> topics = topics(config);

        // TODO drop and create partitioned topics

        for (Map.Entry<String, int[]> entry : topics.entrySet()) {
            String topic = entry.getKey();
            int cols = entry.getValue()[0];
            int rowsIns = entry.getValue()[1];
            int rowsUpd = entry.getValue()[2];
            KafkaProducer<Long, String> producer = new KafkaProducer<>(config);
            StringBuilder sb = new StringBuilder();

            for (Long row = 0L; row < rowsIns; row++) {
                producer.send(new ProducerRecord<Long, String>(topic, row, row(cols, sb)));
            }

            Random rnd = new Random();
            for (Long row = 0L; row < rowsUpd; row++) {
                producer.send(new ProducerRecord<Long, String>(topic, (long)rnd.nextInt(rowsIns), row(cols, sb)));
            }

            producer.close();
        }
    }

    /** @return
     * int[0] cols
     * int[1] rows insert
     * int[2] rows update
     */
    public static Map<String, int[]> topics(Properties config) {
        return Arrays.stream(config.getProperty("topics").split(",")).collect(
                Collectors.toMap(
                        x -> x.split(":")[0],
                        x -> Arrays.stream(x.split(":")[1].split("x")).mapToInt(Integer::valueOf).toArray()
                ));
    }

    private static Properties config() throws IOException {
        Properties config = new Properties();
        InputStream configStream = TestProducer.class.getResourceAsStream("config.properties");
        config.load(configStream);
        config.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
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
