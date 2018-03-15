package com.alibaba.rocketmq.common;

public class HostInfo {

	private String clientId = "";
	private String clientAddr = ""; // import port
	private String clientHostAddress = ""; // without port
	private String bindQueues = "";
	private String consumerGroup = "";
	private String subscribeTopics = "";
	private String producerGroup = "";

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getClientAddr() {
		return clientAddr;
	}

	public void setClientAddr(String clientAddr) {
		this.clientAddr = clientAddr;
	}

	public String getBindQueues() {
		return bindQueues;
	}

	public void setBindQueues(String bindQueues) {
		this.bindQueues = bindQueues;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public String getSubscribeTopics() {
		return subscribeTopics;
	}

	public void setSubscribeTopics(String subscribeTopics) {
		this.subscribeTopics = subscribeTopics;
	}

	public String getProducerGroup() {
		return producerGroup;
	}

	public void setProducerGroup(String producerGroup) {
		this.producerGroup = producerGroup;
	}

	public String getClientHostAddress() {
		return clientHostAddress;
	}

	public void setClientHostAddress(String clientHostAddress) {
		this.clientHostAddress = clientHostAddress;
	}

}
