package com.alibaba.rocketmq.common;

public class MessageQueueStatus {

	private String topic;
	private String brokerName;
	private int queueId;
	private long brokerOffset;
	private long consumerOffset;
	private long diff;

	public long getDiff() {
		return diff;
	}

	public void setDiff(long diff) {
		this.diff = diff;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getBrokerName() {
		return brokerName;
	}

	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}

	public int getQueueId() {
		return queueId;
	}

	public void setQueueId(int queueId) {
		this.queueId = queueId;
	}

	public long getBrokerOffset() {
		return brokerOffset;
	}

	public void setBrokerOffset(long brokerOffset) {
		this.brokerOffset = brokerOffset;
	}

	public long getConsumerOffset() {
		return consumerOffset;
	}

	public void setConsumerOffset(long consumerOffset) {
		this.consumerOffset = consumerOffset;
	}

}
