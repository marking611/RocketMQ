package com.example.order;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

/**
 * 发送范围和区块命令消息
 * Created by makai on 2018/1/12.
 */
public class OrderedProducer {
    public static void main(String[] args) throws Exception {
        produce();
    }

    public static void produce() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("order");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setInstanceName("Producer");
        producer.start();

        String[] tags = new String[]{"TagA", "TagB", "TagC"};
        for (int i = 0; i < 10; i++) {
            int orderId = i % 5;
            Message message = new Message("topicOrder", tags[i % tags.length], "key" + i, ("Hello RocketMQ" + i).getBytes());
            SendResult sendResult = producer.send(message, (list, msg, o) -> {
                Integer id = (Integer) o;
                int index = id % list.size();
                return list.get(index);
            }, orderId);
            System.out.println(sendResult);
        }

        producer.shutdown();
    }
}
