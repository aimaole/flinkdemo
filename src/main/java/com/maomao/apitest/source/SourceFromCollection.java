package com.maomao.apitest.source;

import com.maomao.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceFromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> source = env.fromCollection(
                Arrays.asList(
                        new SensorReading("name1", 1231312, 11),
                        new SensorReading("name12", 12313, 12)));


        DataStream<Integer> intg = env.fromElements(1, 2, 3);
        source.print("data");
        intg.print("int");
        env.execute();

    }


}

