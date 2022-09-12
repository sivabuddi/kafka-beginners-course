package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private final static Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class);
    public static void main(String[] args) throws InterruptedException {

        log.info("I am a kafka producer call backs second time");


        // create producer properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // create producer
        KafkaProducer<String, String> kafkaProducer=new KafkaProducer<>(properties);

        for(int i=0;i<10;i++) {

            String topic="demo_java";
            String value="hello world "+i;
            String key ="id_"+i;
            //create producer record
            ProducerRecord<String,String> producerRecord=new ProducerRecord<>(topic,key,value);


            // send the data -asynchronous
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // everytime a record is successfully sent or an exception is thrown

                    if (exception == null) {
                        // the record was successfully sent
                        log.info("Received new meta data \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Key: " + producerRecord.key() + "\n" +
                                "Partition" + metadata.partition() + "\n" +
                                "Offsets" + metadata.offset() + "\n" +
                                "TimeStamp" + metadata.timestamp()
                        );

                    } else {
                        log.error("Error while producing", exception);
                    }


                }
            });


        }
        // flush  data-synchronous
        kafkaProducer.flush();

        // close
        kafkaProducer.close();



    }

}

