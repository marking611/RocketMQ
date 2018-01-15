package com.example.simple;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.topic.DeleteTopicSubCommand;

/**
 * Created by makai on 2018/1/15.
 */
public class DeleteTopic {
    public static void main(String[] args) {
        deleteTopic();
    }

    public static void deleteTopic(){
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt();
        try {
            adminExt.start();
            adminExt.setNamesrvAddr("localhost:9876");
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        String clusterName = "DefaultCluster";
        String topic = "topic1";
        try {
            DeleteTopicSubCommand.deleteTopic(adminExt,clusterName,topic);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
}
