package com.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class LineSplit {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamsApplicationId.STREAMS_LINE_SPLIT);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamsConfigValue.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 토폴로지 만들기
        final StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream(KafkaTopic.STREAMS_PLAINTEXT_INPUT)
            // flatMap, flatMapValues 메소드는 이전 스트림의 상태를 참고하지 않는다. -> '무상태 오퍼레이터' 라고도 한다.
            .flatMapValues((ValueMapper<String, Iterable<String>>) value -> Arrays.asList(value.split("\\W+")))
            .to(KafkaTopic.STREAMS_LINE_SPLIT_OUTPUT);

        // 토폴로지를 만든다.
        final Topology topology = builder.build();

        System.out.println(topology.describe());

        // 카프카 스트림즈 생성 및 실행
        try (KafkaStreams streams = new KafkaStreams(topology, props)) {
            final CountDownLatch latch = new CountDownLatch(1);
            streams.start();
            System.out.println("topology started");
            latch.await();
        } catch (Throwable throwable) {
            System.exit(1);
        }

        System.exit(0);
    }
}
