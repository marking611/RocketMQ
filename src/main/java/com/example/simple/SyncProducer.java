package com.example.simple;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.TimeUnit;

/**
 * 同步
 * Created by makai on 2018/1/11.
 */
public class SyncProducer {
    public static void main(String[] args) throws Exception {
        produce();
    }

    /**
     * 一个应用创建一个Producer，由应用来维护此对象，可以设置为全局对象或者单例<br>
     * 注意：ProducerGroupName需要由应用来保证唯一<br>
     * ProducerGroup这个概念发送普通的消息时，作用不大，但是发送分布式事务消息时，比较关键，
     * 因为服务器会回查这个Group下的任意一个Producer
     */
    public static void produce() throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("sync");

        //默认情况下，一台服务器只能启动一个Producer或Consumer实例，所以如果需要在一台服务器启动多个实例，需要设置实例的名称
        producer.setNamesrvAddr("192.168.38.71:9876");
        producer.setInstanceName("Producer");

        /**
         * Producer对象在使用之前必须要调用start初始化，初始化一次即可。
         * 注意：切记不可以在每次发送消息时，都调用start方法
         */
        producer.start();

        /**
         * Producer对象可以发送多个topic，多个tag的消息
         * 注意：send方法是同步调用，只要不抛异常就标识成功。但是发送成功也可会有多种状态
         * 例如消息写入Master成功，但是Slave不成功，这种情况消息属于成功，但是对于个别应用如果对消息可靠性要求极高需要对这种情况做处理
         * 另外，消息可能会存在发送失败的情况，失败重试由应用来处理
         */
        for (int i = 0; i < 10; i++) {
            String topic;
            String tags;
            switch (i % 3) {
                case 0: {
                    topic = "topic1";
                    tags = "TagA";
                    break;
                }
                case 1: {
                    topic = "topic2";
                    tags = "TagB";
                    break;
                }
                default: {
                    topic = "topic3";
                    tags = "TagC";
                }
            }

            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message(topic, tags, ("Hello RocketMQ" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            //Call send message to deliver message to one of brokers.
            SendResult sendResult = producer.send(msg);
            System.out.println(sendResult);

            TimeUnit.MILLISECONDS.sleep(1000);

        }
        /**
         * 应用退出时，要调用shutdown来清理资源，关闭网络连接，从MetaQ服务器上注销自己
         * 注意：我们建议应用在JBOSS、Tomcat等容器的退出钩子里调用shutdown方法
         */
        producer.shutdown();
    }
}
