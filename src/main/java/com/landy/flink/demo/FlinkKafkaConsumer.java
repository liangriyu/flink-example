package com.landy.flink.demo;


import com.alibaba.fastjson.JSONObject;
import com.landy.flink.base.sink.HbaseSink;
import com.landy.flink.bean.LogMessage;
import com.landy.flink.utils.PropUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author liangriyu
 * @description: TODO
 * @date 2019/9/3
 */
public class FlinkKafkaConsumer {

    private StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    private Properties prop;

    public FlinkKafkaConsumer() throws Exception{
        this.prop = PropUtil.getProperties();
    }

    public FlinkKafkaConsumer011<String> createConsumer(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", prop.getProperty("bootstrap.servers"));
        properties.setProperty("group.id", prop.getProperty("group.id"));
        properties.put("enable.auto.commit", prop.getProperty("enable.auto.commit"));
        properties.put("auto.commit.interval.ms", prop.getProperty("auto.commit.interval.ms"));
        properties.put("auto.offset.reset", prop.getProperty("auto.offset.reset"));
        properties.put("key.deserializer", prop.getProperty("key.deserializer"));
        properties.put("value.deserializer", prop.getProperty("value.deserializer"));
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>(
                prop.getProperty("kafka.topic"),
                new SimpleStringSchema(),
                properties);
        return consumer;
    }

    public void consume(){
        FlinkKafkaConsumer011<String> consumer = this.createConsumer();
        env.enableCheckpointing(Long.parseLong(prop.getProperty("flink.checkpoint")));
        DataStream<String> stream = env.addSource(consumer);
        DataStream<LogMessage> message = stream.rebalance().map(new MapFunction<String, LogMessage>() {
            public LogMessage map(String value) throws Exception{
                return JSONObject.parseObject(value,LogMessage.class);
            }
        });
//        message.keyBy("uid").timeWindow(Time.minutes(5)).apply();
//        TumblingEventTimeWindows

    }

}