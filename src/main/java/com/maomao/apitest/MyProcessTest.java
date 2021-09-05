package com.maomao.apitest;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author maohongqi
 * @Date 2019/12/14 11:33
 * @Version 1.0
 **/
public class MyProcessTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 数据源
        DataStreamSource<String> value = env.readTextFile("src/main/resources/test1");
        DataStream<Tuple2<String, String>> stream = value.map(f -> {
            String[] split = f.split(",");
           return  new Tuple2(split[0], split[1]);
        });
        // 对KeyedStream应用ProcessFunction
        DataStream<Tuple2<String, Long>> result = stream
                .keyBy(0)
                .process(new CountWithTimeoutFunctionJava());

        result.print();

        env.execute("MyProcessTest");

    }

}

/**
 * 存储在state中的数据类型
 */
class CountWithTimestampJava {
    public String key;
    public long count;
    public long lastModified;
}

/**
 * 维护了计数和超时间隔的ProcessFunction实现
 */
class CountWithTimeoutFunctionJava extends KeyedProcessFunction<Tuple, Tuple2<String, String>, Tuple2<String, Long>> {
    /**
     * 这个状态是通过 ProcessFunction 维护
     */
    private ValueState<CountWithTimestampJava> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestampJava.class));
    }

    @Override
    public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {

        // 查看当前计数
        CountWithTimestampJava current = state.value();
        if (current == null) {
            current = new CountWithTimestampJava();
            current.key = value.f0;
        }

        // 更新状态中的计数
        current.count++;

        // 设置状态的时间戳为记录的事件时间时间戳
        current.lastModified = ctx.timestamp();

        // 状态回写
        state.update(current);

        // 从当前事件时间开始注册一个60s的定时器
        ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {

        // 得到设置这个定时器的键对应的状态
        CountWithTimestampJava result = state.value();

        // 检查定时器是过时定时器还是最新定时器
        if (timestamp == result.lastModified + 60000) {
            // emit the state on timeout
            out.collect(new Tuple2<String, Long>(result.key, result.count));
        }
    }
}
