package kz.greetgo.hotstandby;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by den on 24.05.16.
 */
public class HotStandby {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.JOB_ID_CONFIG, "standby");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "1");
        props.put(StreamsConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String topic = args[0];

        KStreamBuilder builder = new KStreamBuilder();

        final Serializer<String> stringSerializer = new StringSerializer();
        final Deserializer<String> stringDeserializer = new StringDeserializer();
        final Serializer<Long> longSerializer = new LongSerializer();
        final Deserializer<Long> longDeserializer = new LongDeserializer();

        KStream<String, String> source = builder.stream(topic);

        KTable<String, Long> counts = source
                .flatMapValues(new ValueMapper<String, Iterable<String>>() {
                    @Override
                    public Iterable<String> apply(String value) {
                        return Arrays.asList(value.toLowerCase().split(" "));
                    }
                }).map(new KeyValueMapper<String, String, KeyValue<String,String>>() {
                    @Override
                    public KeyValue<String, String> apply(String key, String value) {
                        return new KeyValue<String, String>(value, value);
                    }
                })
                .countByKey(stringSerializer, longSerializer, stringDeserializer, longDeserializer, "Counts");

        counts.to("streams-wordcount-output", stringSerializer, longSerializer);

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        // usually the streaming job would be ever running,
        // in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(60_000L);

        streams.close();
    }
}
