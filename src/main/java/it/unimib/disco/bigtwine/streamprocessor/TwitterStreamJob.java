package it.unimib.disco.bigtwine.streamprocessor;

import it.unimib.disco.bigtwine.commons.messaging.Event;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Properties;

public class TwitterStreamJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test");



        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("greetings", new SimpleStringSchema(), properties);

        DataStream<String> stream = env
                .addSource(consumer);

        DataStream<String> result = stream.map((json) -> {
            JsonDeserializer<Event> deserializer = new JsonDeserializer<>(Event.class);
            return deserializer.deserialize("greetings", json.getBytes());
        }).map(Event::getMessage);

        result.print();
        env.execute();
    }
}
