package com.alibaba.rocketmq.common;

import org.apache.rocketmq.remoting.protocol.LanguageCode;

public class ConsConnection {
	private String clientId;
	private String clientAddr; // with port
	private String clientHostAddr; // without port
	private LanguageCode language;
	private int version;
	private boolean offline = false;
	private String bindQueues;
	private String connectedBroker;
	private String consumerGroup;

	public String getClientHostAddr() {
		return clientHostAddr;
	}

	public void setClientHostAddr(String clientHostAddr) {
		this.clientHostAddr = clientHostAddr;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public String getConnectedBroker() {
		return connectedBroker;
	}

	public void setConnectedBroker(String connectedBroker) {
		this.connectedBroker = connectedBroker;
	}

	public String getBindQueues() {
		return bindQueues;
	}

	public void setBindQueues(String bindQueues) {
		this.bindQueues = bindQueues;
	}

	public boolean isOffline() {
		return offline;
	}

	public void setOffline(boolean offline) {
		this.offline = offline;
	}

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

	public LanguageCode getLanguage() {
		return language;
	}

	public void setLanguage(LanguageCode language) {
		this.language = language;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}
}
