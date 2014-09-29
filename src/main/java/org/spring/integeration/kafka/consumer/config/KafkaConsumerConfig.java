package org.spring.integeration.kafka.consumer.config;

import kafka.consumer.ConsumerConfig;
import org.apache.commons.lang.StringUtils;

import java.util.Properties;

/**
 * Created by hrs on 14-3-13.
 */
public class KafkaConsumerConfig {

	private String zkConnect;

	private String zkSessionTimeoutMs;

	private String zkSyncTimeMs;

	private String autoCommitIntervalMs;

    private String groupId;

	public String getZkConnect() {
		return zkConnect;
	}

	public void setZkConnect(String zkConnect) {
		this.zkConnect = zkConnect;
	}

	public String getZkSessionTimeoutMs() {
		return zkSessionTimeoutMs;
	}

	public void setZkSessionTimeoutMs(String zkSessionTimeoutMs) {
		this.zkSessionTimeoutMs = zkSessionTimeoutMs;
	}

	public String getZkSyncTimeMs() {
		return zkSyncTimeMs;
	}

	public void setZkSyncTimeMs(String zkSyncTimeMs) {
		this.zkSyncTimeMs = zkSyncTimeMs;
	}

	public String getAutoCommitIntervalMs() {
		return autoCommitIntervalMs;
	}

	public void setAutoCommitIntervalMs(String autoCommitIntervalMs) {
		this.autoCommitIntervalMs = autoCommitIntervalMs;
	}

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public ConsumerConfig buildConsumerConfig() {
        Properties properties = new Properties();
		if (StringUtils.isBlank(zkConnect)) {
			throw new IllegalArgumentException("Blank zkConnect");
		}
		if (StringUtils.isNotBlank(zkSessionTimeoutMs)) {
			properties.put("zookeeper.session.timeout.ms", this.zkSessionTimeoutMs);
		}
		if (StringUtils.isNotBlank(zkSyncTimeMs)) {
			properties.put("zookeeper.sync.time.ms", this.zkSyncTimeMs);
		}
        if (StringUtils.isNotBlank(autoCommitIntervalMs)) {
            properties.put("auto.commit.interval.ms", this.autoCommitIntervalMs);
        }
        if (StringUtils.isBlank(groupId)) {
            throw new IllegalArgumentException("Blank groupId");
        }
        properties.put("group.id", this.groupId);
		properties.put("zookeeper.connect", this.zkConnect);
		return new ConsumerConfig(properties);
	}

}
