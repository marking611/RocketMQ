package com.example.schedule;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.common.message.Message;

/**
 * Scheduled messages differ from normal messages in that they won’t be delivered until a provided time later
 * 调度消息与普通消息不同，因为它们将在稍后一段时间后才能发送。
 * Created by makai on 2018/1/12.
 */
public class ScheduleMessageProducer {
    public static void main(String[] args) throws Exception {
        produce();
    }

    public static void produce() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("ExampleProducerGroup");
        producer.start();
        int totalMessagesToSend = 100;
        for (int i = 0; i < totalMessagesToSend; i++) {
            Message message = new Message("TestTopic", ("Hello scheduled message " + i).getBytes());
            // This message will be delivered to consumer 10 seconds later.
            message.setDelayTimeLevel(3);
            // Send the message
            producer.send(message);
        }

        // Shutdown producer after use.
        producer.shutdown();
    }
}
