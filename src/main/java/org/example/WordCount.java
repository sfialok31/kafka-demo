package org.example;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCount {
  static Logger logger = LoggerFactory.getLogger(WordCount.class.getName());

  public static void main(String[] args) {
    final Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount-101");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    final String inputTopic = "wordcount.topic.input";
    final String outputTopic = "wordcount.topic.output";
    final StreamsBuilder builder = new StreamsBuilder();;
    try (Serde<String> stringSerde = new Serdes.StringSerde(); Serde<Long> longSerde = new Serdes.LongSerde()) {
      final KStream<String, Long> outputStream = builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
        .peek((key, value) -> logger.info("Incoming record: key - {}, value - {}", key, value))
        .groupByKey()
        .count()
        .toStream()
        .peek((key, value) -> logger.info("Outgoing record: key - {}, value - {}", key, value));
      outputStream.to(outputTopic, Produced.with(stringSerde, longSerde));

      final Topology topology = builder.build();
      logger.info("Kafka Streams Topology\n{}", topology.describe());

      try (KafkaStreams kafkaStreams = new KafkaStreams(topology, props)) {
        final CountDownLatch latch = new CountDownLatch(1);
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          kafkaStreams.close();
          latch.countDown();
        }));

        try {
          kafkaStreams.start();
          latch.await();
        } catch (Throwable e) {
          System.exit(1);
        }
      }
      System.exit(0);
    }
  }
}
