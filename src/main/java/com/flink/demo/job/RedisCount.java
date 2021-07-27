package com.flink.demo.job;

import com.flink.demo.process.RedisProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RedisCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env
                .socketTextStream("localhost", 9999);

        dataStream.process(new RedisProcessFunction());

        env.execute("Window WordCount");
    }


}