package com.example.demo;

import beans.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author author
 * @create 2022-04-10-18:27
 */
public class splitDemo {
    public static void main(String[] args)  throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> stringDataStream = env.readTextFile("C:\\Users\\765476009\\Desktop\\demo\\src\\main\\resources\\sensor.txt");

        DataStream<SensorReading> map = stringDataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fileds = value.split(",");
                return new SensorReading(fileds[0],new Long(fileds[1]),new Double(fileds[2]));
            }
        });
        SingleOutputStreamOperator<SensorReading> flatMap = stringDataStream.flatMap(new FlatMapFunction<String, SensorReading>() {
            @Override
            public void flatMap(String value, Collector<SensorReading> collector) throws Exception {
                String[] fileds = value.split(",");
                collector.collect( new SensorReading(fileds[0],new Long(fileds[1]),new Double(fileds[2])));
            }
        });
        DataStream<SensorReading> union = flatMap.union(map).shuffle();
        union.print();
        union.addSink(new MyJdbcFunction());



        env.execute();

    }
    public static class MyJdbcFunction extends RichSinkFunction<SensorReading>{
        PreparedStatement InsertStatement=null;
        PreparedStatement UpdateStatement=null;
        Connection connection = null;
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/cloud-user","root","123456");
            InsertStatement = connection.prepareStatement("insert into sensor_tem(id,tem) values (?, ?)");
            UpdateStatement = connection.prepareStatement("update sensor_tem set tem=? where id=?");
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            UpdateStatement.setDouble(1, value.getTemperature());
            UpdateStatement.setString(2, value.getId());
            UpdateStatement.execute();

            if (UpdateStatement.getUpdateCount()==0){

                InsertStatement.setString(1, value.getId());
                InsertStatement.setDouble(2, value.getTemperature());
                InsertStatement.execute();
            }
        }

        @Override
        public void close() throws Exception {
            InsertStatement.close();
            UpdateStatement.close();
            connection.close();
        }
    }
}
