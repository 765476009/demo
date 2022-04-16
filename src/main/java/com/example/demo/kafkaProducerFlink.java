package com.example.demo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;
import java.util.Scanner;

/**
 * @author author
 * @create 2022-04-09-18:22
 */
public class kafkaProducerFlink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Scanner scanner=new Scanner(System.in);
        while (true){
            String str = scanner.next();

            DataStream<String> ds = env.fromElements(str);

            ds.print();
            ParameterTool paramTool= ParameterTool.fromArgs(args);
            String host= paramTool.get("host");
            int port= paramTool.getInt("port");

            Properties props = new Properties();
            props.setProperty("bootstrap.servers", host + ":" + port);

            // Producer
            FlinkKafkaProducer<String> producer =
                    new FlinkKafkaProducer<>("flink_topic",  new SimpleStringSchema(),  props);
            ds.addSink(producer);

            env.execute();
        }


    }
}
