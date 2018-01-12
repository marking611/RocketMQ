package com.example.order;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by makai on 2018/1/12.
 */
public class OrderConsumer {
    public static void main(String[] args) throws Exception {
        consume();
    }

    public static void consume() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //订阅topicOrder的tags为TagA或者TagB的消息
        consumer.subscribe("topicOrder", "TagA || TagB || TagC");
        consumer.registerMessageListener(new MessageListenerOrderly() {
            AtomicLong consumeTimes = new AtomicLong(0); //计数（线程安全的）

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                consumeOrderlyContext.setAutoCommit(false);
                System.out.println("------------" + LocalDateTime.now() + "-------------");
                System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + list.size());
                list.forEach(messageExt -> System.out.println(new String(messageExt.getBody())));
                this.consumeTimes.incrementAndGet();
                if (this.consumeTimes.get() % 2 == 0) {
                    return ConsumeOrderlyStatus.SUCCESS;
                } else if (this.consumeTimes.get() % 3 == 0) {
                    return ConsumeOrderlyStatus.ROLLBACK;
                } else if (this.consumeTimes.get() % 4 == 0) {
                    return ConsumeOrderlyStatus.COMMIT;
                } else if (this.consumeTimes.get() % 5 == 0) {
                    consumeOrderlyContext.setSuspendCurrentQueueTimeMillis(3000);
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();
    }
}
