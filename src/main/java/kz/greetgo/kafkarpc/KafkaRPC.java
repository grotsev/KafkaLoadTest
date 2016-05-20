package kz.greetgo.kafkarpc;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.LongStream;

import static java.lang.System.out;

/**
 * Created by den on 19.05.16.
 */
public class KafkaRPC {

    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException, IOException {
        Properties config = config(args);
        String requestTopic = "request2";
        String responseTopic = requestTopic;
        final KafkaProducer<Long, GenericRecord> producer = new KafkaProducer(config);
        final KafkaConsumer<Long, GenericRecord> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(Arrays.asList(responseTopic));

        String schemaString = "{\"namespace\": \"example.avro\", \"type\": \"record\", "
                + "\"name\": \"RequestResponse\"," + "\"fields\": ["
                + "{\"name\": \"id\", \"type\": \"long\"},"
                + "{\"name\": \"key\", \"type\": \"long\"},"
                + "{\"name\": \"text\", \"type\": \"string\"}" + "]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);

        Synchronizer<GenericRecord, GenericRecord> sync = new Synchronizer<GenericRecord, GenericRecord>() {

            {
                Thread thread = new Thread() {
                    @Override
                    public void run() {
                        while (true) {
                            ConsumerRecords<Long, GenericRecord> records = consumer.poll(1000);
                            //if (records.isEmpty()) break;
                            for (ConsumerRecord<Long, GenericRecord> record : records) {
                                GenericRecord response = record.value();
                                long id = (Long) response.get("id");
                                onResponse(id, response);
                            }
                            consumer.commitSync();
                        }
                    }
                };
                thread.start();
            }

            @Override
            protected void send(long id, GenericRecord request) {
                request.put("id", id);
                Long key = (Long) request.get("key");
                producer.send(new ProducerRecord<Long, GenericRecord>(requestTopic, key, request));
            }
        };

        Thread.sleep(1000);
        LongStream.range(0, 10).forEach(
                i -> new Thread() {
                    @Override
                    public void run() {
                        GenericRecord request = new GenericData.Record(schema);
                        request.put("key", i);
                        request.put("text", "Ok" + i);
                        try {
                            GenericRecord response = sync.invoke(300, TimeUnit.SECONDS, request);
                            out.println(response);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (TimeoutException e) {
                            e.printStackTrace();
                        }

                    }
                }.start());
        Thread.sleep(500000);
        producer.close();
        // TODO pollingThread.stop
    }

    private static Properties config(String[] args) throws IOException { // TODO
        Properties config = new Properties();
        InputStream configStream = new FileInputStream(args[0]); // TestProducer.class.getResourceAsStream("config.properties");
        config.load(configStream);
        config.put("bootstrap.servers", "localhost:9092");
        config.put("acks", "all");
        config.put("retries", 0);
        config.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        config.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        config.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        config.put("schema.registry.url", "http://localhost:8081");
        return config;
    }
}
