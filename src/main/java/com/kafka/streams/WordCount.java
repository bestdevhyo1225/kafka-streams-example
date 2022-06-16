package com.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCount {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamsApplicationId.STREAMS_WORD_COUNT);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamsConfigValue.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 토폴로지 만들기
        final StreamsBuilder builder = new StreamsBuilder();

        // streams-plaintext-input 토픽으로부터 새로운 입력 스트림을 생성한다.
        builder.<String, String>stream(KafkaTopic.STREAMS_PLAINTEXT_INPUT)
            // flatMap(), flatMapValues() 메소드는 이전 스트림의 상태를 참고하지 않는다. -> '무상태 오퍼레이터' 라고도 한다.
            .flatMapValues((ValueMapper<String, Iterable<String>>) value ->
                Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"))
            )
            // groupBy() 메소드를 사용해서 같은 Key를 가진 데이터를 모은다.
            .groupBy((key, value) -> value)
            // groupBy() 메소드의 결과 값이 <String, Long> 인데 이것을 <Bytes, byte[]> 로 변환해서 counts-store 테이블 형태로 저장한다.
            // counts-store 테이블에 저장된 값은 KSQL(카프카 SQL)로 실시간 쿼리가 가능하다.
            .count(Materialized.as(StateStoreName.COUNTS_STORE))
            // count() 메소드의 결과로 생성된 counts-store 테이블 데이터를 스트림으로 변환한다.
            .toStream()
            // 변환한 스트림을 streams-word-count-output 토픽에 저장한다.
            // 저장할 때, 데이터 값은 각각 Bytes, byte[] 에서 Serdes.String(), Serdes.Long() 타입으로 변환한다.
            .to(KafkaTopic.STREAMS_WORD_COUNT_OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));

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
