package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

        private final static Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class);
        public static void main(String[] args) {

            log.info("I am a kafka consumer");
            String bootstrapServer="127.0.0.1:9092";
            String groupId="my-third-applicaiton";
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

            // get a reference to current thread

            final Thread mainThread=Thread.currentThread();

            // adding the shutdown hook
            Runtime.getRuntime().addShutdownHook(

                    new Thread(){

                        public void run(){

                            log.info("Detected shutdown, lets exit by calling consumer.wakeup()");
                            kafkaConsumer.wakeup();

                            try{
                                mainThread.join();
                            }catch(Exception exception){
                                exception.printStackTrace();
                            }

                        }

                    }


            );


            try {
                // subscribe consumer to our topic
                kafkaConsumer.subscribe(Arrays.asList(topic));


                // poll for new data
                while (true) {

                    //log.info("polling");

                    ConsumerRecords<String, String> consumerRecord = kafkaConsumer.poll(Duration.ofMillis(1000));

                    for (ConsumerRecord<String, String> record : consumerRecord) {
                        log.info("Key: " + record.key() + ", Value: " + record.value());
                        log.info("Parition: " + record.partition() + ", Offset: " + record.offset());


                    }


                }
            }catch(WakeupException wakeupException){
                 log.info("Wake up exception!");
            }catch(Exception exception){
                log.error("Unexpected Exception");
            }finally {
                kafkaConsumer.close();
                log.info("Consumer is gracefully closed");
            }



        }





}
