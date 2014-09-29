package com.kafka.sample;

import org.spring.integeration.kafka.consumer.KafkaConsumerContainer;
import org.spring.integeration.kafka.consumer.KafkaConsumerImplTemplate;

/**
 * Created by peng on 2014/6/13.
 */
public class DemoTopicKafkaConsumerImpl extends KafkaConsumerImplTemplate {

    public DemoTopicKafkaConsumerImpl(KafkaConsumerContainer consumerContainer, String groupId, String topic) {
        super(consumerContainer, groupId, topic);
    }

    @Override
    public void receiveMessages(String key, String value) {
        System.out.println("##########################key:"+key+"#"+"value:"+value);
    }

    @Override
    public void receiveMessages(String value) {
        System.out.println("#########################value:"+value);
    }
}
