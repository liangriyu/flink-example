package com.landy.flink;

import com.landy.flink.demo.FlinkKafkaConsumer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liangriyu
 * @description: TODO
 * @date 2019/9/3
 */
public class Test {

    public static void main(String[] args){

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        try {
            FlinkKafkaConsumer consumer = new FlinkKafkaConsumer();
            consumer.getConsumer().setStartFromLatest();
            consumer.run(env);
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}