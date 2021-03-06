package com.kafka.streams.pipe;

import com.kafka.streams.KafkaTopic;
import com.kafka.streams.StreamsApplicationId;
import com.kafka.streams.StreamsConfigValue;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Pipe2 {

    public static void main(String[] args) {
        // 토폴로지 만들기
        final StreamsBuilder builder = new StreamsBuilder();

        // streams-plaintext-input 토픽으로부터 새로운 입력 스트림을 생성한다.
        // source의 결과를 streams-pipe-output 토픽으로 전달한다.
        builder.stream(KafkaTopic.STREAMS_PLAINTEXT_INPUT)
            .to(KafkaTopic.STREAMS_PIPE_OUTPUT);

        // 토폴로지를 만든다.
        final Topology topology = builder.build();

        System.out.println(topology.describe());

        // 카프카 스트림즈 생성 및 실행
        try (KafkaStreams streams = new KafkaStreams(topology, PipeConfig.getProperties())) {
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
