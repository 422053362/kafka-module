package org.spring.integeration.kafka.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spring.integeration.kafka.consumer.config.KafkaConsumerConfig;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hrs on 14-3-13.
 */
public class KafkaConsumerContainer implements IConsumerContainer, InitializingBean, DisposableBean {


    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerContainer.class);

    // 消费监听器列表
    private List<IKafkaConsumer> consumerList = new ArrayList<IKafkaConsumer>();

    // 消费端配置信息
    private final KafkaConsumerConfig kafkaConsumerConfig;

    // 消费者链接列表
    private final CopyOnWriteArraySet<ConsumerConnector> consumerConnectorList = new CopyOnWriteArraySet<ConsumerConnector>();

    public KafkaConsumerContainer(KafkaConsumerConfig kafkaConsumerConfig) {
        this.kafkaConsumerConfig = kafkaConsumerConfig;
    }

    @Override
    public void register(final IKafkaConsumer listener) {
        this.addListener(listener);
        this.consumerList.add(listener);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        for (IKafkaConsumer listener : this.consumerList) {
            this.addListener(listener);
        }
    }

    /**
     * 添加监听器
     *
     * @param consumer
     */
    private void addListener(IKafkaConsumer consumer) {
        ConsumerConnector consumerConnector = createConnector(consumer);
        this.consumerConnectorList.add(consumerConnector);
        this.poll(consumer, consumerConnector);
    }

    /**
     * 创建连接器
     *
     * @param consumer
     * @return
     */
    private ConsumerConnector createConnector(IKafkaConsumer consumer) {
        kafkaConsumerConfig.setGroupId( consumer.getGroupId());
        return Consumer.createJavaConsumerConnector(kafkaConsumerConfig.buildConsumerConfig());
    }

    /**
     * 获取消息
     *
     * @param listener
     * @param consumer
     */
    private void poll(final IKafkaConsumer listener, ConsumerConnector consumer) {
        int threadCount = listener.getProcessThreads();
        String topic = listener.getTopic();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, threadCount);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        final List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);
        for (final KafkaStream stream : streams) {
            threadPool.submit(new Runnable() {
                @Override
                public void run() {
                    ConsumerIterator<byte[], byte[]> it = stream.iterator();
                    while (it.hasNext()) {
                        try {
                            MessageAndMetadata<byte[], byte[]> message = it.next();
                            String key = null;
                            String value = null;
                            byte[] byteKey = message.key();
                            if (null != byteKey) {
                                key = new String(byteKey, "UTF-8");
                            }
                            value = new String(message.message(), "UTF-8");
                            if (null != key) {
                                LOG.debug("kafka poll message key=" + key + ",value=" + value);
                                listener.receiveMessages(key, value);
                            }else{
                                LOG.debug("kafka poll message value=" + value);
                                listener.receiveMessages(value);
                            }
                        } catch (UnsupportedEncodingException e) {
                            LOG.debug("kafka fetch message error", e);
                        }
                    }
                }
            });
        }
    }

    @Override
    public void destroy() throws Exception {
        for (ConsumerConnector consumerConnector : consumerConnectorList) {
            consumerConnector.shutdown();
            consumerConnector = null;
        }
        this.consumerConnectorList.clear();
        this.consumerList.clear();
    }

}
