package com.kafka.streams.linesplit;

import com.kafka.streams.StreamsApplicationId;
import com.kafka.streams.StreamsConfigValue;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

public class LineSplitConfig {

    public static Properties getProperties() {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamsApplicationId.STREAMS_LINE_SPLIT);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamsConfigValue.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return props;
    }
}
