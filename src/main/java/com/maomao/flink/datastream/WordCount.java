package com.maomao.flink.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(4);

        DataStreamSource<String> stringDataStreamSource = env.fromElements(
                "Who's there?",
                "I think I hear them. Stand, ho! Who's there?");
        stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        for (String value : line.split(" ")) {
                            collector.collect(Tuple2.of(value, 1));
                        }

                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                })
                .sum(1)
                .print();

        //任务提交、开始
        env.execute();

    }

}
