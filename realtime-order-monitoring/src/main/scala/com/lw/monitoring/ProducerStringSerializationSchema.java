package com.lw.monitoring;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.Tuple3;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class ProducerStringSerializationSchema implements KafkaSerializationSchema<String> {

    private String topic;

    public ProducerStringSerializationSchema(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
        return new ProducerRecord<byte[], byte[]>(topic, element.getBytes(StandardCharsets.UTF_8));
    }


}
