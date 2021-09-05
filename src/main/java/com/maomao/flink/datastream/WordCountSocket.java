package com.maomao.flink.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountSocket {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//        env.setParallelism(4);
//        env.disableOperatorChaining();  关闭全局任务链合并

        //flink自带的ParameterTool工具  --host 127.0.0.1 --port 9000
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");


        env.socketTextStream(host, port)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] s = line.split(" ");
                        for (String value : s) {
                            collector.collect(Tuple2.of(value, 1));
                        }
                    }
                })
                .keyBy(0)
                .sum(1).slotSharingGroup("red")
//                .disableChaining() 关闭任务链合并
//                .startNewChain()  开始一个新的任务合并，前面断开，后面合并
                .print();


        //任务提交、开始
        env.execute();

    }

}
