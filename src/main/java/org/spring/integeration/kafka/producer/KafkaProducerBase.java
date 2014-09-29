package org.spring.integeration.kafka.producer;

import kafka.javaapi.producer.Producer;
import org.spring.integeration.kafka.producer.config.KafkaProducerConfig;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hrs on 14-3-13.
 */
public class KafkaProducerBase implements InitializingBean, DisposableBean {

    protected Producer<String, String> producer = null;

    protected int processThreads = 4;

    protected ExecutorService executor;

    final protected KafkaProducerConfig kafkaProducerConfig;

    public KafkaProducerBase(KafkaProducerConfig kafkaProducerConfig) {
        this.kafkaProducerConfig = kafkaProducerConfig;
    }

    public void setProcessThreads(int processThreads) {
        if (processThreads <= 0) {
            throw new IllegalArgumentException("Invalid processThreads value:" + processThreads);
        }
        this.processThreads = processThreads;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (this.processThreads > 0) {
            this.executor = Executors.newFixedThreadPool(this.processThreads);
        }
        if(producer == null){
            producer = new Producer<String, String>(kafkaProducerConfig.buildProducerConfig());
        }
    }

    @Override
    public void destroy() throws Exception {
        if(producer!=null){
            producer.close();
        }
        if (this.executor != null) {
            this.executor.shutdown();
            this.executor = null;
        }
    }

}
