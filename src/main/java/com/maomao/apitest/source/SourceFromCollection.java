package com.maomao.apitest.source;

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

class SensorReading {
    private String name;
    private long time;
    private long value;

    public SensorReading(String name, long time, long value) {
        this.name = name;
        this.time = time;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "name='" + name + '\'' +
                ", time=" + time +
                ", value=" + value +
                '}';
    }
}