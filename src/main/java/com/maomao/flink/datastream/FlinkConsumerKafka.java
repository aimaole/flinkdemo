package com.maomao.flink.datastream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkConsumerKafka {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // 要设置启动检查点
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);


        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-group");

        //数据源配置，是一个kafka消息的消费者
//        FlinkKafkaConsumer<String> consumer =new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), props);
        DataStream<String> sourceStream = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), props))
                .name("aaa")
                .setParallelism(1);

        sourceStream.print();
        env.execute("flinkkafka");


    }
}