package com.flink.demo.job;

import com.flink.demo.bean.TimeEvent;
import com.flink.demo.process.BoundedOutOfOrdernessGenerator;
import org.apache.flink.api.common.functions.FlatMapFunction;
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

public class TcpWaterMarkWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

       env
                .socketTextStream("localhost", 9999)
                .process(new ProcessFunction<String, TimeEvent>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<TimeEvent> out) throws Exception {
                        TimeEvent timeEvent = new TimeEvent();
                        String[] str = value.split(",");
                        timeEvent.setMessage(str[0]);
                        timeEvent.setWaterTime(Long.parseLong(str[1]));
                        out.collect(timeEvent);
                    }
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator()).windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
               .process(new ProcessAllWindowFunction<TimeEvent, Object, TimeWindow>() {
                   @Override
                   public void process(Context context, Iterable<TimeEvent> elements, Collector<Object> out)
                           throws Exception {
                       for(TimeEvent timeEvent : elements){
                           System.out.println("cal " + timeEvent);
                       }
                   }
               });

//        dataStream.print();

        env.execute("Window WordCount");
    }



}