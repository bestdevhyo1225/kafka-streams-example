package com.kafka.streams.linesplit;

import com.kafka.streams.KafkaTopic;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.ValueMapper;

public class LineSplit2 {

    public static void main(String[] args) {
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
        try (KafkaStreams streams = new KafkaStreams(topology, LineSplitConfig.getProperties())) {
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
