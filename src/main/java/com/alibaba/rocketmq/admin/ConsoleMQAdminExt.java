package com.alibaba.rocketmq.admin;

import java.util.Set;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 所有运维接口都在这里实现
 * 
 */
public class ConsoleMQAdminExt extends ClientConfig {
	private final ConsoleMQAdminExtImpl defaultMQAdminExtImpl;
	private String adminExtGroup = "admin_ext_group_console";
	private String createTopicKey = MixAll.BENCHMARK_TOPIC;

	public ConsoleMQAdminExt() {
		this.defaultMQAdminExtImpl = new ConsoleMQAdminExtImpl(this, null);
	}

	public ConsoleMQAdminExt(RPCHook rpcHook) {
		this.defaultMQAdminExtImpl = new ConsoleMQAdminExtImpl(this, rpcHook);
	}

	public ConsoleMQAdminExt(final String adminExtGroup) {
		this.adminExtGroup = adminExtGroup;
		this.defaultMQAdminExtImpl = new ConsoleMQAdminExtImpl(this);
	}

	public void start() throws MQClientException {
		defaultMQAdminExtImpl.start();
	}

	public void shutdown() {
		defaultMQAdminExtImpl.shutdown();
	}

	public String getAdminExtGroup() {
		return adminExtGroup;
	}

	public void setAdminExtGroup(String adminExtGroup) {
		this.adminExtGroup = adminExtGroup;
	}

	public String getCreateTopicKey() {
		return createTopicKey;
	}

	public void setCreateTopicKey(String createTopicKey) {
		this.createTopicKey = createTopicKey;
	}

	public Set<QueueTimeSpan> queryConsumeTimeSpan(final String topic, final String group)
			throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
		return this.defaultMQAdminExtImpl.queryConsumeTimeSpan(topic, group);
	}

}
