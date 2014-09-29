package org.spring.integeration.kafka.producer;

import org.spring.integeration.kafka.producer.config.KafkaProducerConfig;
import org.springframework.beans.factory.FactoryBean;

/**
 * Created by peng on 2014/6/12.
 */
public class KafkaProducerFactory implements FactoryBean<IKafKaProducer> {
    String topic;
    KafkaProducerConfig kafkaProducerConfig;

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setKafkaProducerConfig(KafkaProducerConfig kafkaProducerConfig) {
        this.kafkaProducerConfig = kafkaProducerConfig;
    }

    @Override
    public IKafKaProducer getObject() throws Exception {
        return new KafKaProducerImpl(kafkaProducerConfig,topic);
    }

    @Override
    public Class<?> getObjectType() {
        return IKafKaProducer.class;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }
}
