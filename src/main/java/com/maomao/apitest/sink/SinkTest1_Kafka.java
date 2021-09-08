package com.maomao.apitest.sink;

import com.maomao.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.File;

public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        File file = new File("src\\main\\resources\\sensor");
        String sourceFile = file.getCanonicalPath();
        DataStreamSource<String> inputStream = env.readTextFile(sourceFile);

        SingleOutputStreamOperator<String> sensorReadingStream = inputStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Long.parseLong(split[2])).toString();
        });

        sensorReadingStream.addSink(new FlinkKafkaProducer<String>("localhost:9092", "test", new SimpleStringSchema()));

        env.execute();

    }
}
