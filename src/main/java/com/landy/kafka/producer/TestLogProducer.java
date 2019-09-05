package com.landy.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class TestLogProducer {

    public final static String BROKER_LIST = "bigdata001:19092,bigdata002:19092,bigdata003:19092";
    public final static String TOPIC = "test-log";
    public final static boolean isAsync = true;

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Integer, String> producer = new KafkaProducer(props);
        try {
            long[] uids = {11610};
            String[] accounts = {"13810203018","13800138000"};
            String[] ips = {"172.18.53.101","127.0.0.1"};
            String[] urls = {"http://demo_gateway-server/app/simul/areaprice/list",
                    "http://demo_gateway-server/app/simul/predload/1004031",
                    "http://demo_gateway-server/app/simul/strat/main/pushcheck",
                    "http://demo_gateway-server/app/simul/strat/main/competplantlist/1004031",
                    "http://demo_gateway-server/app/simul/strat/section/detail",
                    "http://demo_gateway-server/app/simul/strat/dataentry/todaylastversion/2",
                    "http://demo_gateway-server/app/simul//strat/generator/site"
            };
            String[] methods = {"GET","POST"};
            String[] paramBodys = {"{\\\"reqBody\\\":{\\\"startTime\\\":\\\"2019-04-11\\\",\\\"endTime\\\":\\\"2019-04-18\\\",\\\"areaName\\\":\\\"茂名\\\"}}",
            null,"{\\\"reqBody\\\":{\\\"page\\\":1,\\\"pageSize\\\":5,\\\"dataType\\\":1,\\\"rpType\\\":1,\\\"smId\\\":1004031,\\\"deId\\\":1000085,\\\"runDate\\\":\\\"2019-04-19\\\"}}",
            null,"{\\\"reqBody\\\":{\\\"smId\\\":1004031,\\\"secType\\\":1,\\\"page\\\":1,\\\"pageSize\\\":5,\\\"secNameMid\\\":\\\"\\\"}}"};
            while (true) {
                Date cur = new Date();
                String cur_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(cur);
                for (int i = 0;i<uids.length;i++) {
                    int u= (int) Math.floor(Math.random()*urls.length);
                    int m= (int) Math.floor(Math.random()*methods.length);
                    int p= (int) Math.floor(Math.random()*paramBodys.length);
                    String msg = "{\"uid\":" + uids[i] + ",\"account\":\"" + accounts[i] + "\",\"ip\":\"" + ips[i] + "\",\"url\":\"" + urls[u] + "\",\"method\":\"" +methods[m] + "\",\"paramBody\":\"" +paramBodys[p]+ "\",\"dateTime\":\"" +cur_time + "\",\"ms\":\"" +cur.getTime()+ "\"}";
                    long startTime = System.currentTimeMillis();
                    if (isAsync) {
                        // Send asynchronously
                        System.out.println(msg);
                        producer.send(new ProducerRecord(TOPIC, msg), new MessageCallback(startTime, msg));
                    } else {
                        // Send synchronously
                        producer.send(new ProducerRecord(TOPIC,msg)).get();
                        System.out.println("Sent message: (" + msg + ")");
                    }
                }
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}