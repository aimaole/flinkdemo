package com.maomao.apitest.transform;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.File;

public class Transform1_Base {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        File file = new File("src\\main\\resources\\sensor");
        String sourceFile = file.getCanonicalPath();
        DataStreamSource<String> inputStream = env.readTextFile(sourceFile);

        //1.map
        SingleOutputStreamOperator<Integer> map = inputStream.map(f -> f.length());
        //2.flatMap
        SingleOutputStreamOperator<String> flatmapout = inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                for (String s : line.split(","))
                    collector.collect(s);
            }
        });
        //3.filter
        SingleOutputStreamOperator<String> filter = inputStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return StringUtils.isNotBlank(s);
            }
        });

        map.print();
        flatmapout.print();
        filter.print();

        env.execute();


    }
}
