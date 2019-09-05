package com.landy.flink.demo;


import com.alibaba.fastjson.JSONObject;
import com.landy.flink.bean.LogMessage;
import com.landy.flink.utils.PropUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author liangriyu
 * @description: TODO
 * @date 2019/9/3
 */
public class FlinkKafkaConsumer {

    private Properties prop =  new Properties();
    private FlinkKafkaConsumer011<String> consumer011;

    public FlinkKafkaConsumer() throws Exception{

        this.prop.load(this.getClass().getClassLoader().getResourceAsStream("application.properties"));
        this.consumer011 = this.createConsumer();
    }

    private FlinkKafkaConsumer011<String> createConsumer(){
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

    public  FlinkKafkaConsumer011 getConsumer(){
        return consumer011;
    }

    public void run(StreamExecutionEnvironment env){
        env.enableCheckpointing(Long.parseLong(prop.getProperty("flink.checkpoint")));
        DataStream<String> stream = env.addSource(consumer011);
        DataStream<LogMessage> message = stream.rebalance().map(new MapFunction<String, LogMessage>() {
            public LogMessage map(String value) throws Exception{
                return JSONObject.parseObject(value,LogMessage.class);
            }
        });
        message.keyBy("uid").window(TumblingEventTimeWindows.of(Time.minutes(5))).aggregate(new SumAggregate(),new WindowResultFunction());
    }



    private static class SumAggregate
            implements AggregateFunction<LogMessage, Long, Long> {
        @Override
        public Long createAccumulator(){
            return 0L;
        }

        @Override
        public Long add(LogMessage msg, Long acc){
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc){
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2){
            return acc1 + acc2;
        }
    }

    /** 用于输出窗口的结果 */
    public static class WindowResultFunction implements WindowFunction<Long, ViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, org.apache.flink.util.Collector<ViewCount> collector) throws Exception {
            Long itemId = ((Tuple1<Long>) tuple).f0;
            Long count = iterable.iterator().next();
            collector.collect(ViewCount.of(itemId, timeWindow.getEnd(), count));
        }
    }

    /** 日志统计(窗口操作的输出类型) */
    public static class ViewCount {
        public long uid; // UID
        public long windowEnd; // 窗口结束时间戳
        public long viewCount; // 日志数量

        public static ViewCount of(long uid, long windowEnd, long viewCount){
            ViewCount result = new ViewCount();
            result.uid = uid;
            result.windowEnd = windowEnd;
            result.viewCount = viewCount;
            return result;
        }
    }

}

