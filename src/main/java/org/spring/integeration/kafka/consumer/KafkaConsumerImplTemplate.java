package org.spring.integeration.kafka.consumer;

import org.spring.integeration.kafka.consumer.IKafkaConsumer;
import org.spring.integeration.kafka.consumer.KafkaConsumerBase;
import org.spring.integeration.kafka.consumer.KafkaConsumerContainer;

/**
 * Created by peng on 2014/6/11.
 */
public abstract class KafkaConsumerImplTemplate extends KafkaConsumerBase implements IKafkaConsumer {

    final String groupId;
    final String topic;

    public KafkaConsumerImplTemplate(KafkaConsumerContainer consumerContainer, String groupId, String topic) {
        super(consumerContainer);
        this.groupId = groupId;
        this.topic = topic;
        consumerContainer.register(this);
    }

    public abstract void receiveMessages(String key, String value);

    public abstract void receiveMessages(String value) ;

    @Override
    public String getGroupId() {
        return groupId;
    }

    @Override
    public String getTopic() {
        return topic;
    }
}
