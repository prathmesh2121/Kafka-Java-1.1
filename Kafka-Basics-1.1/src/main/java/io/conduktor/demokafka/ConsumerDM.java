package io.conduktor.demokafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDM
{
    private static Logger logger = LoggerFactory.getLogger(ConsumerDM.class.getSimpleName());
    private static String groupId = "G1";
    private static String topic = "Salary";


    public static void main(String[] args)
    {
        logger.info("Inside Consumer class");

        //Consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        // DE-SERIALIZER for Consumer
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset","earliest"); // earliest, latest

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe to topic
        consumer.subscribe(Arrays.asList(topic));

        // poll for new data
        while(true) {
            logger.info("Polling...");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1500));
//            Meaning:
//            The consumer asks Kafka: "Do you have any new messages for me?"
//            Kafka waits up to 1500 milliseconds (1.5 seconds).
//            If messages are available, Kafka returns them in records.
//            If no messages are available, it returns an empty collection.



            // process the records
            for (ConsumerRecord<String, String> record : records)
            {
                logger.info("Key: " + record.key() + " Value: " + record.value());
                logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
            }

            // commit the offset
            // Synchronously commits offsets after successful processing so the
            // consumer can resume from the correct position after a restart.
            consumer.commitSync();
        }





    }
}
