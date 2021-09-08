package com.maomao.apitest.transform;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.File;

public class Transform_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File file = new File("src\\main\\resources\\sensor");
        String sourceFile = file.getCanonicalPath();
        DataStreamSource<String> inputStream = env.readTextFile(sourceFile);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = inputStream.map(new MyMap());

        map.print();

        env.execute();
    }


    public static  class MyMap extends RichMapFunction<String, Tuple2<String,Integer>>{
        @Override
        public Tuple2<String, Integer> map(String value) throws Exception {
            //获取上下文  getRuntimeContext()
            return new Tuple2<>(value,getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close");
        }
    }
}
