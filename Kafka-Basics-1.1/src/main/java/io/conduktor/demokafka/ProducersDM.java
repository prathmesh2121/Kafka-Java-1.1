package io.conduktor.demokafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducersDM
{
    private static Logger logger = LoggerFactory.getLogger(ProducersDM.class.getSimpleName());

    public static void main(String[] args)
    {
        logger.info("Inside Producer class");

        //producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        // set producer properties
        // use dot only for property name (not hyphen)
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("Organization", "1st","Tech Mahindra, Pune");

        // send producer record
        producer.send(producerRecord);

        // send all data and block until data is sent - synchronous
        producer.flush(); // default called before close

        // close producer
        producer.close();
    }
}
