package org.spring.integeration.kafka.producer;

/**
 * Created by peng on 2014/6/11.
 */
public interface IKafKaProducer {
    /**
     * 异步发送
     * @param key
     * @param value
     */
    public abstract void sendAsync(String key, String value);

    /**
     * 异步发送
     * @param value
     */
    public abstract void sendAsync(String value);

    /**
     * 同步发送
     * @param key
     * @param value
     */
    public abstract void sendSync(String key, String value);

    /**
     * 同步发送
     * @param value
     */
    public abstract void sendSync(String value);
}
