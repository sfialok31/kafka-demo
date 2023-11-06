package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {
  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final Logger logger = LoggerFactory.getLogger(Producer.class);

    String bootstrapServers = "127.0.0.1:9092";

    // create Producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, "0");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "5");
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");

    // create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


    for (int i=0; i<10; i++ ) {
      // create a producer record

      String topic = "topic_1";
      String value = "hello world " + i;
      String key = "id_" + i;

      ProducerRecord<String, String> record =
        new ProducerRecord<>(topic, key, value);

      logger.info("Key: " + key); // log the key


      // send data - asynchronous
      producer.send(record, (recordMetadata, e) -> {
        // executes every time a record is successfully sent or an exception is thrown
        if (e == null) {
          // the record was successfully sent
          logger.info("Received new metadata. \n" +
            "Topic:" + recordMetadata.topic() + "\n" +
            "Partition: " + recordMetadata.partition() + "\n" +
            "Offset: " + recordMetadata.offset() + "\n" +
            "Timestamp: " + recordMetadata.timestamp());
        } else {
          logger.error("Error while producing", e);
        }
      }).get(); // block the .send() to make it synchronous - don't do this in production!
    }

    // flush data
    producer.flush();
    // flush and close producer
    producer.close();
  }
}