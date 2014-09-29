package org.spring.integeration.kafka.consumer;

import java.io.Serializable;

/**
 * Created by hrs on 14-3-17.
 */
public interface IConsumerContainer extends Serializable {

    /**
     *
     * 注册新的监听器
     * @param listener
     */
    void register(IKafkaConsumer listener);
}
