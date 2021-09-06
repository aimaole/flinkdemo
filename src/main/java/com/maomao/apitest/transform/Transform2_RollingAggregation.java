package com.maomao.apitest.transform;

import com.maomao.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public class Transform2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        File file = new File("src\\main\\resources\\sensor");
        String sourceFile = file.getCanonicalPath();
        DataStreamSource<String> inputStream = env.readTextFile(sourceFile);
//        inputStream.print();

        SingleOutputStreamOperator<SensorReading> map = inputStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Long.parseLong(split[2]));
        });

        //分组
        KeyedStream<SensorReading, String> keyedStream = map.keyBy(SensorReading::getName);

        //滚动聚合  注意max与maxBy区别
        SingleOutputStreamOperator<SensorReading> value = keyedStream.maxBy("value");

        value.print();

        env.execute();


    }
}
