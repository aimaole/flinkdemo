package com.maomao.apitest.transform;

import com.maomao.apitest.beans.SensorReading;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.File;

public class Transform_Partition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        File file = new File("src\\main\\resources\\sensor");
        String sourceFile = file.getCanonicalPath();
        DataStreamSource<String> inputStream = env.readTextFile(sourceFile);

        SingleOutputStreamOperator<SensorReading> sensorReadingStream = inputStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Long.parseLong(split[2]));
        });

        // 1. shuffle
        DataStream<String> shuffle = inputStream.shuffle();

        //2. keyby
        KeyedStream<SensorReading, String> keyedStream = sensorReadingStream.keyBy(SensorReading::getName);

        //3, global
        DataStream<SensorReading> global = sensorReadingStream.global();

        inputStream.print("input");

        shuffle.print("shuffle");

        keyedStream.print("keyby");

        global.print("global");

        env.execute();


    }
}
