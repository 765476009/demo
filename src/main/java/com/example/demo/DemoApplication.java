package com.example.demo;

import beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DemoApplication {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		String path = "C:\\Users\\765476009\\Desktop\\demo\\src\\main\\resources\\sensor.txt";

		DataStream<String> inputStream = env.readTextFile(path);
		// 转换成SensorReading类型
		DataStream<SensorReading> dataStream = inputStream.map(line -> {
			String[] fields = line.split(",");
			return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
		});

		// 分组
		KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

		// reduce聚合，取最大的温度值，以及当前最新的时间戳
		SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
			@Override
			public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
				return new SensorReading(value1.getId(), value2.getTimestamp(), Math.max(value1.getTemperature(), value2.getTemperature()));
			}
		});

		keyedStream.reduce( (curState, newData) -> {
			return new SensorReading(curState.getId(), newData.getTimestamp(), Math.max(curState.getTemperature(), newData.getTemperature()));
		});

		keyedStream.print();


		env.execute();

	}
}
