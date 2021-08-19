package com.flink.demo.process;

import com.flink.demo.bean.TimeEvent;
import java.text.SimpleDateFormat;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * 功能描述：
 *
 * @Author: winghou
 * @Date: 2021/8/11 4:47 下午
 */
public class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<TimeEvent> {

    private long currentMaxTimestamp = 0L;
    private long maxDelayAllowed = 3000L;


    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxDelayAllowed);
    }

    @Override
    public long extractTimestamp(TimeEvent element, long previousElementTimestamp) {
        long timestamp = element.getWaterTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lastMax = currentMaxTimestamp;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        System.out.println(Thread.currentThread() + " extractTimestamp is  " + element + " last is " + sdf.format(lastMax) +" current is  " + sdf.format(currentMaxTimestamp));
        return timestamp;
    }
}
