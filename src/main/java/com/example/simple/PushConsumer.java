package com.example.simple;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.nio.charset.Charset;
import java.util.List;

/**
 * PushConsumer使用方式给用户感觉是消息从RocketMQ服务器推到了应用客户端。
 * 实际PushConsumer内部是使用长轮询Pull方式从MetaQ服务器拉消息，然后再回调用户Listener方法
 * <p>
 * Created by makai on 2018/1/11.
 */
public class PushConsumer {
    public static void main(String[] args) throws Exception {
        consume();
    }

    public static void consume() throws Exception {
        /**
         * 一个应用创建一个Consumer，由应用来维护此对象，可以设置为全局对象或者单例
         * 注意：ConsumerGroupName需要由应用来保证唯一
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();
        consumer.setConsumerGroup("push");
        consumer.setNamesrvAddr("192.168.38.71:9876");
        consumer.setInstanceName("Consumer");

        /**
         * 订阅指定的topic下的tags分别等于TagA或TagC或者TagD
         */
        consumer.subscribe("topic1", "TagA || TagC || TagD");

        /**
         * 订阅指定的topic下的所有消息
         * 注意：一个consumer对象可以订阅多个topic
         */
        consumer.subscribe("topic2","*");

        /**
         * 默认msgs里只有一条消息，可以通过设置consumeMessageBatchMaxSize参数来批量接收消息
         */
        consumer.setConsumeMessageBatchMaxSize(1);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println(Thread.currentThread().getName()+" Receive New Message:"+list.size());
                list.forEach(message -> {
                    //获取body内容
                    System.out.println(new String(message.getBody(), Charset.defaultCharset()));

                    if (message.getTopic().equals("topic1")){
                        System.out.println("This is topic1");
                        //执行topic1的消费逻辑
                        String tags = message.getTags();
                        if (tags != null && tags.equals("TagA")){
                            System.out.println("This is TagA");
                        }else if (tags != null && tags.equals("TagC")){
                            System.out.println("This is TagC");
                        }else if (tags != null && tags.equals("TagD")){
                            System.out.println("This is TagD");
                        }
                    }else if (message.getTopic().equals("topic2")){
                        System.out.println("This is topic2");
                    }
                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        /**
         * Consumer对象在使用之前必须要调用start初始化，初始化一次即可
         */
        consumer.start();
    }
}
