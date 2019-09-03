package com.landy.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * kafka回调类
 */
class MessageCallback implements Callback {

    private final long startTime;
    private final String message;

    public MessageCallback(long startTime, String message) {
        this.startTime = startTime;
        this.message = message;
    }

    public void onCompletion(RecordMetadata metadata, Exception e) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                    "message => (" + message + ") sent to partition(" + metadata.partition() +
                            "), " +
                            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            e.printStackTrace();
        }
    }
}