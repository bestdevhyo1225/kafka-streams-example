package com.kafka.streams;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

public class Pipe {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamsApplicationId.STREAMS_PIPE);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamsConfigValue.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 토폴로지 만들기
        final StreamsBuilder builder = new StreamsBuilder();

        // streams-plaintext-input 토픽으로부터 새로운 입력 스트림을 생성한다.
        KStream<String, String> source = builder.stream(KafkaTopic.STREAMS_PLAINTEXT_INPUT);

        // source의 결과를 streams-pipe-output 토픽으로 전달한다.
        source.to(KafkaTopic.STREAMS_PIPE_OUTPUT);

        // 토폴로지를 만든다.
        final Topology topology = builder.build();

        System.out.println(topology.describe());

        // 카프카 스트림즈 만들기
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
                System.out.println("topology terminated");
            }
        });

        try {
            streams.start();
            System.out.println("topology started");
            latch.await();
        } catch (Throwable throwable) {
            System.exit(1);
        } finally {
            streams.close();
        }

        System.exit(0);
    }
}