package com.flink.demo.job;

import com.flink.demo.bean.TimeEvent;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 功能描述：
 *
 * @Author: winghou
 * @Date: 2021/8/12 7:44 上午
 */
public class JoinChangeTest {

    private static final String[] TYPE = {"a", "b", "c", "d"};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        //添加自定义数据源,每秒发出一笔订单信息{商品名称,商品数量}
//        DataStreamSource<TimeEvent> orderSource1 = env
//                .addSource(new SourceFunction<TimeEvent>() {
//                    private volatile boolean isRunning = true;
//                    private final Random random = new Random();
//
//                    @Override
//                    public void run(SourceContext<TimeEvent> ctx) throws Exception {
//                        while (isRunning) {
//                            TimeUnit.SECONDS.sleep(1);
//                            TimeEvent timeEvent = new TimeEvent();
//                            timeEvent.setMessage(TYPE[random.nextInt(TYPE.length)]);
//                            timeEvent.setWaterTime(System.currentTimeMillis());
//                            System.out.println(new Date() + ",orderSource1提交元素:" + timeEvent);
//                            ctx.collect(timeEvent);
//                        }
//                    }
//
//                    @Override
//                    public void cancel() {
//                        isRunning = false;
//                    }
//
//                }, "orderSource1");
//
//
//        DataStreamSource<TimeEvent> orderSource2 = env
//                .addSource(new SourceFunction<TimeEvent>() {
//                    private volatile boolean isRunning = true;
//                    private final Random random = new Random();
//
//                    @Override
//                    public void run(SourceContext<TimeEvent> ctx) throws Exception {
//                        while (isRunning) {
//                            TimeUnit.SECONDS.sleep(1);
//                            TimeEvent timeEvent = new TimeEvent();
//                            timeEvent.setMessage(TYPE[random.nextInt(TYPE.length)]);
//                            timeEvent.setWaterTime(System.currentTimeMillis());
//                            System.out.println(new Date() + ",orderSource2提交元素:" + timeEvent);
//                            ctx.collect(timeEvent);
//                        }
//                    }
//
//                    @Override
//                    public void cancel() {
//                        isRunning = false;
//                    }
//
//                }, "orderSource2");

        DataStream<TimeEvent> orderSource1 = env
                .socketTextStream("localhost", 9999)
                .process(new ProcessFunction<String, TimeEvent>() {
                    private final Random random = new Random();
                    @Override
                    public void processElement(String value, Context ctx, Collector<TimeEvent> out) throws Exception {
                        TimeEvent timeEvent = new TimeEvent();

                        String[] str = value.split(",");
                        timeEvent.setMessage(TYPE[random.nextInt(TYPE.length)]);
                        timeEvent.setWaterTime(Long.parseLong(str[1]));
                        System.out.println("9999 data is : " + timeEvent);
                        out.collect(timeEvent);
                    }
                });

        DataStream<TimeEvent> orderSource2 = env
                .socketTextStream("localhost", 9998)
                .process(new ProcessFunction<String, TimeEvent>() {
                    private final Random random = new Random();

                    @Override
                    public void processElement(String value, Context ctx, Collector<TimeEvent> out) throws Exception {
                        TimeEvent timeEvent = new TimeEvent();
                        String[] str = value.split(",");
                        timeEvent.setMessage(TYPE[random.nextInt(TYPE.length)]);
//                        timeEvent.setMessage(str[0]);
                        timeEvent.setWaterTime(Long.parseLong(str[1]));
                        System.out.println("9998 data is : " + timeEvent);
                        out.collect(timeEvent);
                    }
                });

        orderSource1.join(orderSource2).where(new KeySelector<TimeEvent, String>() {
            @Override
            public String getKey(TimeEvent value) throws Exception {
//                System.out.println("key is : " + value.getMessage());
                return value.getMessage();
            }
        }).equalTo(new KeySelector<TimeEvent, String>() {
            @Override
            public String getKey(TimeEvent value) throws Exception {
//                System.out.println("key is : " + value.f0);
                return value.getMessage();
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<TimeEvent, TimeEvent, TimeEvent>() {
                    @Override
                    public TimeEvent join(TimeEvent first, TimeEvent second)
                            throws Exception {
                        TimeEvent timeEvent = new TimeEvent();
                        timeEvent.setMessage(first.getMessage() + second.getMessage());
                        return timeEvent;//计算key相同的属性1值的和
                    }
                }).print();
        env.execute("Flink JoinTest");
    }


}
