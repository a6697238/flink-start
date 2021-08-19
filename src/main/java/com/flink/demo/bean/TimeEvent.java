package com.flink.demo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 功能描述：
 *
 * @Author: winghou
 * @Date: 2021/8/11 4:48 下午
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TimeEvent {

    private String message;

    private long waterTime;

}
