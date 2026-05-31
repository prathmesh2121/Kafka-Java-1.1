package io.conduktor.demokafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducersDMCallBack
{
    private static Logger logger = LoggerFactory.getLogger(ProducersDMCallBack.class.getSimpleName());

    public static void main(String[] args)
    {
        logger.info("Inside Producer class");

        //producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        // set producer properties // SERIALIZER for Producer
        // use dot only for property name (not hyphen)
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        double val = 22000;
        for(int j=0; j<10; j++) {
            // if more than one loop then - data will be stored in different partition
            // because each loop will create different batches of data
            for (int i = 101; i <= 105; i++) {
                // if only one loop & callback then - data will be stored in same partition
                val = (val + (val * 0.4));

                String val2 = String.valueOf(val);
                String i2 = String.valueOf(i);


                // create producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("Salary", i2, val2);

                // send producer record
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            System.out.println("Record sent successfully");
                            System.out.println("Topic: " + recordMetadata.topic());
                            System.out.println("Partition: " + recordMetadata.partition());
                            System.out.println("Offset: " + recordMetadata.offset());
                        } else {
                            System.out.println("Error while producing " + e.getMessage());
                        }
                    }
                });

            }
        }

        // send all data and block until data is sent - synchronous
        producer.flush(); // default called before close

        // close producer
        producer.close();
    }
}
