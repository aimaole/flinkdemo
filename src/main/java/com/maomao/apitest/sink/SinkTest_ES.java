package com.maomao.apitest.sink;

import com.alibaba.fastjson.JSONObject;
import com.maomao.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.io.File;
import java.util.ArrayList;

public class SinkTest_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        File file = new File("src\\main\\resources\\sensor");
        String sourceFile = file.getCanonicalPath();
        DataStreamSource<String> inputStream = env.readTextFile(sourceFile);

        SingleOutputStreamOperator<SensorReading> sensorReadingStream = inputStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Long.parseLong(split[2]));
        });

        //定义es连接配置
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9092));

        sensorReadingStream
                .addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts, new MyESSinkFuncation()).build());

        env.execute();

    }

    //实现自定义的ES写入类


    public static class MyESSinkFuncation implements ElasticsearchSinkFunction<SensorReading> {

        @Override
        public void open() throws Exception {
            ElasticsearchSinkFunction.super.open();
        }

        @Override
        public void close() throws Exception {
            ElasticsearchSinkFunction.super.close();
        }

        @Override
        public void process(SensorReading element, RuntimeContext ctx, RequestIndexer indexer) {
            //实现写入操作
            JSONObject json= (JSONObject) JSONObject.toJSON(element);

            //创建请求，作为发送写入命令
            IndexRequest source = Requests.indexRequest()
                    .index("test")
                    .type("data")
                    .source(json);
            //index 发送请求
            indexer.add(source);

        }
    }
}
