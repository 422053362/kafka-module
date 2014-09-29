package org.spring.integeration.kafka.consumer;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hrs on 14-3-13.
 */
public class KafkaConsumerBase implements InitializingBean, DisposableBean {


	private int processThreads = 4;

	protected ExecutorService executor;

    final KafkaConsumerContainer consumerContainer;

    public KafkaConsumerBase(KafkaConsumerContainer consumerContainer) {
        this.consumerContainer = consumerContainer;
    }

    public int getProcessThreads() {
		return processThreads;
	}

	public void setProcessThreads(int processThreads) {
		if (processThreads < 0) {
			throw new IllegalArgumentException("Invalid processThreads value:" + processThreads);
		}
		this.processThreads = processThreads;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (this.processThreads > 0) {
			this.executor = Executors.newFixedThreadPool(this.processThreads);
		}
	}

	@Override
	public void destroy() throws Exception {
		if (this.executor != null) {
			this.executor.shutdown();
			this.executor = null;
		}
	}

}
