package com.maomao.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class SourceFromUDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<SensorReading> dataStreamSource = env.addSource(new MySensorSource());

        dataStreamSource.print();
        env.execute();

    }

    //实现自定义funcation
    public static class MySensorSource implements SourceFunction<SensorReading> {

        //定义一个标志位，控制数据的产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {

            while (running) {

                sourceContext.collect(new SensorReading("name",231312312,1));
                Thread.sleep(1000);

            }
        }

        @Override
        public void cancel() {
            running = false;

        }
    }


}