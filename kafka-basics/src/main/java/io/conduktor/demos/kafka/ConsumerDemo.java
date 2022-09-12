package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ConsumerDemo {

        private final static Logger log = LoggerFactory.getLogger(ConsumerDemo.class);
        public static void main(String[] args) {

            log.info("I am a kafka consumer");
            String bootstrapServer="127.0.0.1:9092";
            String groupId="my-second-applicaiton";
            String topic="demo_java";

            // create consumer configs

            Properties properties =new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            // create consumer

            KafkaConsumer<String,String> kafkaConsumer =new KafkaConsumer<>(properties);

            // subscribe consumer to our topic
            kafkaConsumer.subscribe(Arrays.asList(topic));


            // poll for new data
            while(true){

                //log.info("polling");

                ConsumerRecords<String,String> consumerRecord = kafkaConsumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String,String> record:consumerRecord){
                    log.info("Key: "+record.key()+", Value: "+record.value());
                    log.info("Parition: "+record.partition()+", Offset: "+record.offset());


                }


            }












        }





}
