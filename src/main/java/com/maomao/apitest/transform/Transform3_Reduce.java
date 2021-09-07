package com.maomao.apitest.transform;

import com.maomao.apitest.beans.SensorReading;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.File;

public class Transform3_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        File file = new File("src\\main\\resources\\sensor");
        String sourceFile = file.getCanonicalPath();
        DataStreamSource<String> inputStream = env.readTextFile(sourceFile);

        SingleOutputStreamOperator<SensorReading> map = inputStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Long.parseLong(split[2]));
        });
        //分组
        KeyedStream<SensorReading, String> keyedStream = map.keyBy(SensorReading::getName);

        //reduce取最大的温度值与最新的时间戳
//        SingleOutputStreamOperator<SensorReading> reduce = keyedStream.reduce(new ReduceFunction<SensorReading>() {
//            @Override
//            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
//                return new SensorReading(value1.getName(),value2.getTime(),Math.max(value1.getValue(),value2.getValue()));
//            }
//        });
        SingleOutputStreamOperator<SensorReading> reduce = keyedStream.reduce((x, y) -> new SensorReading(x.getName(), y.getTime(), Math.max(x.getValue(), y.getValue())));

        reduce.print();
        env.execute();
    }
}
