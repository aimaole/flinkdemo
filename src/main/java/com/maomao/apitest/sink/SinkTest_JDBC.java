package com.maomao.apitest.sink;

import com.alibaba.fastjson.JSONObject;
import com.maomao.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;

public class SinkTest_JDBC {
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
                .addSink(new MyJdbcSiink());

        env.execute();

    }
    public static class MyJdbcSiink extends RichSinkFunction<SensorReading> {

        Connection connection = null;
        PreparedStatement insertStmt=null;
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
             insertStmt = connection.prepareStatement("insert into temperatures (name,time,value) values (?,?,?)");
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            //执行更新语句
            insertStmt.setString(1, value.getName());
            insertStmt.setLong(2, value.getTime());
            insertStmt.setLong(3, value.getValue());
            insertStmt.execute();
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            connection.close();
            super.close();
        }
    }

}
