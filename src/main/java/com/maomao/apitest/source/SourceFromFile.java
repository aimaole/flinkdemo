package com.maomao.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.Arrays;

public class SourceFromFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        File file = new File("src\\main\\resources\\sensor");
        String sourceFile = file.getCanonicalPath();
        DataStreamSource<String> stringDataStreamSource = env.readTextFile(sourceFile);

        stringDataStreamSource.print();
        env.execute();

    }

}