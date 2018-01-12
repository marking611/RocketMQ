package com.example.broadcasting;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;

/**
 * Broadcasting is sending a message to all subscribers of a topic. If you want all subscribers receive messages about a topic, broadcasting is a good choice
 * 广播向一个主题的所有订阅者发送消息。如果您希望所有订阅者接收关于主题的消息，广播是一个不错的选择。
 * Created by makai on 2018/1/12.
 */
public class BroadcastProducer {
    public static void main(String[] args) throws Exception {
        produce();
    }

    public static void produce() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("broadcast");
        producer.start();

        for (int i = 0; i < 10; i++){
            Message msg = new Message("TopicTest",
                    "TagA",
                    "OrderID188",
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        producer.shutdown();
    }
}
