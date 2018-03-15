package com.alibaba.rocketmq.common;

import java.io.Serializable;

import org.apache.rocketmq.common.protocol.body.ConsumerConnection;

public class ConsumerInfo implements Serializable {

	private static final long serialVersionUID = -3045479354627503633L;

	private ConsumerConnection consumerConnection;

	private String consumerGroup;

	public ConsumerConnection getConsumerConnection() {
		return consumerConnection;
	}

	public void setConsumerConnection(ConsumerConnection consumerConnection) {
		this.consumerConnection = consumerConnection;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

}
