package com.landy.flink.demo;


import com.alibaba.fastjson.JSONObject;
import com.landy.flink.bean.LogMessage;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;


import java.util.*;

/**
 * @author liangriyu
 * @description: TODO
 * @date 2019/9/3
 */
public class FlinkKafkaConsumer {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

    public void run() throws Exception {
        env.enableCheckpointing(Long.parseLong(prop.getProperty("flink.checkpoint")));
        consumer011.setStartFromEarliest();
        DataStream<String> stream = env.addSource(consumer011);
        System.out.println("........");
        DataStream<LogMessage> message = stream.map(new MapFunction<String, LogMessage>() {
            public LogMessage map(String value) throws Exception{
                return JSONObject.parseObject(value,LogMessage.class);
            }
        });
//        message.print();
        DataStream<ViewCount> windowedData = message.keyBy("uid").timeWindow(Time.seconds(10), Time.seconds(10)).aggregate(new SumAggregate(),new WindowResultFunction());
        windowedData.print();
        DataStream<String> topItems = windowedData
                .keyBy("windowEnd")
                .process(new TopNItems(1));
        topItems.print();
        env.execute("Flink Streaming Java API Skeleton");
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
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ViewCount> collector) throws Exception {
            Long uid = ((Tuple1<Long>) tuple).f0;
            Long count = iterable.iterator().next();
            collector.collect(ViewCount.of(uid, timeWindow.getEnd(), count));
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

    public static class TopNItems extends KeyedProcessFunction<Tuple, ViewCount, String> {

        private final int topSize;

        public TopNItems(int topSize){
            this.topSize = topSize;
        }

        // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
        private ListState<ViewCount> itemState;

        @Override
        public void open(Configuration parameters) throws Exception{
            super.open(parameters);
            // 状态的注册
            ListStateDescriptor<ViewCount> itemsStateDesc = new ListStateDescriptor<>(
                    "uid-state",
                    ViewCount.class);
            itemState = getRuntimeContext().getListState(itemsStateDesc);
        }

        @Override
        public void processElement(
                ViewCount event,
                Context context,
                Collector<String> collector) throws Exception {
            // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有数据
            TimerService timerService = context.timerService();
            if (context.timestamp() > timerService.currentWatermark()) {
                itemState.add(event);
                timerService.registerEventTimeTimer(timerService.currentWatermark()+1);
            }
        }

//        @Override
//        public void onTimer(
//                long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
//            List<ViewCount> allList = new ArrayList<>();
//            for (ViewCount item : itemState.get()) {
//                allList.add(item);
//            }
//            // 提前清除状态中的数据，释放空间
//            itemState.clear();
//            // 按照点击量从大到小排序
//            allList.sort(new Comparator<ViewCount>() {
//                @Override
//                public int compare(ViewCount o1, ViewCount o2){
//                    return (int) (o2.viewCount - o1.viewCount);
//                }
//            });
//            // 将排名信息格式化成 String, 便于打印
//            StringBuilder result = new StringBuilder();
//            result.append("====================================\n");
//            result.append("时间: ").append(new Date((timestamp-1))).append("\n");
//            for (int i=0;i<allList.size();i++) {
//                ViewCount currentItem = allList.get(i);
//                // No1:  商品ID=12224  浏览量=2413
//                result.append("No").append(i).append(":")
//                        .append("  UID=").append(currentItem.uid)
//                        .append("  数量=").append(currentItem.viewCount)
//                        .append("\n");
//            }
//            result.append("====================================\n\n");
//            out.collect(result.toString());
//        }
    }

}

