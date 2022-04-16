package com.example.demo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author author
 * @create 2022-04-09-18:16
 */
public class kafkaConsumerFlink {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ParameterTool paramTool= ParameterTool.fromArgs(args);
        String host= paramTool.get("host");
        int port= paramTool.getInt("port");

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", host + ":" + port);

        // Consumer
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>("flink_topic", new SimpleStringSchema(), props);

        DataStreamSource<String> kafkaDS = env.addSource(consumer);

        kafkaDS.print();

        env.execute();

    }
}
