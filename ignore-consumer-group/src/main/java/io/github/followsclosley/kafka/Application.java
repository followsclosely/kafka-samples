package io.github.followsclosley.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

/**
 * The Application class demonstrates a simple Kafka consumer that reads messages from a specified topic.
 */
public class Application {

    /**
     * The main method initializes the Kafka consumer and reads messages from the specified topic.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "followsclosley-topic";

        // Create consumer configuration properties
        Map<String, Object> properties = Map.ofEntries(
                Map.entry(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"),
                Map.entry(GROUP_ID_CONFIG, "followsclosley-group-id"),
                Map.entry(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class),
                Map.entry(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class),
                Map.entry(AUTO_OFFSET_RESET_CONFIG, "latest")
        );

        // Create and configure Kafka consumer
        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties))
        {
            TopicPartition partition = new TopicPartition(topic, 0);
            List<TopicPartition> partitions = List.of(partition);
            consumer.assign(partitions);

            // Seek to the end of the partition to get the latest offset
            consumer.seekToEnd(partitions);

            // Calculate the start index to read the last message
            long index = consumer.position(partition) - 1;
            consumer.seek(partition, index);

            boolean keepOnReading = true;
            // poll for new data
            while(keepOnReading){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records){
                    System.out.println(
                      "Key: " + record.key() + "Partition: " + record.partition() + ", Offset:" + record.offset() + ", Value: " + record.value());
                }
            }
        }
    }
}
