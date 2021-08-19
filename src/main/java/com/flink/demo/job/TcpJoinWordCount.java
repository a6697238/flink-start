package com.flink.demo.job;

import com.flink.demo.bean.TimeEvent;
import com.flink.demo.process.BoundedOutOfOrdernessGenerator;
import com.flink.demo.process.TimeEventJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TcpJoinWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<TimeEvent> ds1 = env
                .socketTextStream("localhost", 9999)
                .process(new ProcessFunction<String, TimeEvent>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<TimeEvent> out) throws Exception {
                        TimeEvent timeEvent = new TimeEvent();
                        String[] str = value.split(",");
                        timeEvent.setMessage(str[0]);
                        timeEvent.setWaterTime(Long.parseLong(str[1]));
                        System.out.println("9999 data is : " + timeEvent);
                        out.collect(timeEvent);
                    }
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());
        ;

        DataStream<TimeEvent> ds2 = env
                .socketTextStream("localhost", 9998)
                .process(new ProcessFunction<String, TimeEvent>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<TimeEvent> out) throws Exception {
                        TimeEvent timeEvent = new TimeEvent();
                        String[] str = value.split(",");
                        timeEvent.setMessage(str[0]);
                        timeEvent.setWaterTime(Long.parseLong(str[1]));
                        System.out.println("9998 data is : " + timeEvent);
                        out.collect(timeEvent);
                    }
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());
        ;

        DataStream<TimeEvent> ds3 = env
                .socketTextStream("localhost", 9997)
                .process(new ProcessFunction<String, TimeEvent>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<TimeEvent> out) throws Exception {
                        TimeEvent timeEvent = new TimeEvent();
                        String[] str = value.split(",");
                        timeEvent.setMessage(str[0]);
                        timeEvent.setWaterTime(Long.parseLong(str[1]));
                        System.out.println("9997 data is : " + timeEvent);
                        out.collect(timeEvent);
                    }
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());
        ;

        ds1.join(ds2).where(new KeySelector<TimeEvent, String>() {
            @Override
            public String getKey(TimeEvent value) throws Exception {
                System.out.println("key is : " + value.getMessage());
                return value.getMessage();
            }
        }).equalTo(new KeySelector<TimeEvent, String>() {
            @Override
            public String getKey(TimeEvent value) throws Exception {
                System.out.println("key is : " + value.getMessage());
                return value.getMessage();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<TimeEvent, TimeEvent, TimeEvent>() {
                    @Override
                    public TimeEvent join(TimeEvent first, TimeEvent second)
                            throws Exception {
                        System.out.println("join 1 first + second" + first + second);
                        TimeEvent timeEvent = new TimeEvent();
                        timeEvent.setMessage(first.getMessage());
                        return timeEvent;//计算key相同的属性1值的和
                    }
                }).join(ds3).where(new KeySelector<TimeEvent, String>() {
            @Override
            public String getKey(TimeEvent value) throws Exception {
                System.out.println("key is : " + value.getMessage());
                return value.getMessage();
            }
        }).equalTo(new KeySelector<TimeEvent, String>() {
            @Override
            public String getKey(TimeEvent value) throws Exception {
                System.out.println("key is : " + value.getMessage());
                return value.getMessage();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new JoinFunction<TimeEvent, TimeEvent, TimeEvent>() {
                    @Override
                    public TimeEvent join(TimeEvent first, TimeEvent second)
                            throws Exception {
                        System.out.println("join 2 first + second" + first + second);
                        TimeEvent timeEvent = new TimeEvent();
                        timeEvent.setMessage(first.getMessage() + second.getMessage());
                        return timeEvent;//计算key相同的属性1值的和
                    }
                });
        env.execute("Flink JoinTest");

//        ds1.join(ds2).where(new KeySelector<TimeEvent, String>() {
//            @Override
//            public String getKey(TimeEvent timeEvent) throws Exception {
//                System.out.println("key is : " + timeEvent.getMessage());
//                return timeEvent.getMessage();
//            }
//        }).equalTo(
//                new KeySelector<TimeEvent, String>() {
//                    @Override
//                    public String getKey(TimeEvent timeEvent) throws Exception {
//                        System.out.println("key is : " + timeEvent.getMessage());
//                        return timeEvent.getMessage();
//                    }
//                })
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(10))).apply(new TimeEventJoinFunction()).print();

//        dataStream.print();

//        env.execute("Window WordCount");
    }


}