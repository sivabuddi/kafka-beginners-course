package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

        private final static Logger log = LoggerFactory.getLogger(ProducerDemo.class);
        public static void main(String[] args) {

            log.info("I am a kafka producer");


            // create producer properties
            Properties properties=new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


            // create producer
            KafkaProducer<String, String> kafkaProducer=new KafkaProducer<>(properties);


            //create producer record
            ProducerRecord<String,String> producerRecord=new ProducerRecord<>("demo_java", "hello world");

            // send the data -asynchronous
            kafkaProducer.send(producerRecord);

            // flush  data-synchronous
            kafkaProducer.flush();

            // close
            kafkaProducer.close();




        }





}
