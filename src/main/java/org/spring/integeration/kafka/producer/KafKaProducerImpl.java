package org.spring.integeration.kafka.producer;

import kafka.producer.KeyedMessage;
import org.spring.integeration.kafka.producer.config.KafkaProducerConfig;

public class KafKaProducerImpl extends KafkaProducerBase implements IKafKaProducer {

    private final String topic;

    public KafKaProducerImpl(KafkaProducerConfig kafkaProducerConfig,String topic) {
        super(kafkaProducerConfig);
        this.topic = topic;
    }

    @Override
    public void sendAsync(String key, String value) {
        final KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, value);
        executor.submit(new Runnable() {
            public void run() {
                producer.send(data);
            }
        });
    }

    @Override
    public void sendAsync(String value) {
        final KeyedMessage<String, String> data = new KeyedMessage<String, String>(this.topic,value);
        executor.submit(new Runnable() {
            @Override
            public void run() {
                producer.send(data);
            }
        });
    }

    @Override
    public void sendSync(String key, String value) {
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, value);
        producer.send(data);
    }

    @Override
    public void sendSync(String value) {
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic,value);
        producer.send(data);
    }
}