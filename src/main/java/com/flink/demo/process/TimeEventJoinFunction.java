package com.flink.demo.process;


import com.alibaba.fastjson.JSON;
import com.flink.demo.bean.TimeEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.JoinFunction;

/**
 * 功能描述：
 *
 * @Author: winghou
 * @Date: 2021/8/11 9:27 下午
 */
@Slf4j
public class TimeEventJoinFunction implements JoinFunction<TimeEvent, TimeEvent, TimeEvent> {

    @Override
    public TimeEvent join(TimeEvent timeEvent1, TimeEvent timeEvent2) throws Exception {
        TimeEvent timeEvent = new TimeEvent();
        timeEvent.setMessage(timeEvent1.getMessage()+timeEvent2.getMessage());
        log.info("join res timeEvent is : {}", JSON.toJSONString(timeEvent));
        return timeEvent;
    }
}
