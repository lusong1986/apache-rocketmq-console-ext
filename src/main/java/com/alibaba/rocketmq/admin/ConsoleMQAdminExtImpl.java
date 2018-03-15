package com.alibaba.rocketmq.admin;

import java.util.HashSet;
import java.util.Set;

import org.apache.rocketmq.client.admin.MQAdminExtInner;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;

public class ConsoleMQAdminExtImpl implements MQAdminExtInner {
	private final Logger log = ClientLogger.getLog();
	private final ConsoleMQAdminExt defaultMQAdminExt;
	private ServiceState serviceState = ServiceState.CREATE_JUST;
	private MQClientInstance mqClientInstance;
	private RPCHook rpcHook;

	public ConsoleMQAdminExtImpl(ConsoleMQAdminExt defaultMQAdminExt) {
		this(defaultMQAdminExt, null);
	}

	public ConsoleMQAdminExtImpl(ConsoleMQAdminExt defaultMQAdminExt, RPCHook rpcHook) {
		this.defaultMQAdminExt = defaultMQAdminExt;
		this.rpcHook = rpcHook;
	}

	public void start() throws MQClientException {
		switch (this.serviceState) {
		case CREATE_JUST:
			this.serviceState = ServiceState.START_FAILED;

			this.defaultMQAdminExt.changeInstanceNameToPID();

			this.mqClientInstance = MQClientManager.getInstance().getAndCreateMQClientInstance(this.defaultMQAdminExt,
					rpcHook);

			boolean registerOK = mqClientInstance.registerAdminExt(this.defaultMQAdminExt.getAdminExtGroup(), this);
			if (!registerOK) {
				this.serviceState = ServiceState.CREATE_JUST;
				throw new MQClientException("The adminExt group[" + this.defaultMQAdminExt.getAdminExtGroup()
						+ "] has created already, specifed another name please."//
						+ FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL), null);
			}

			mqClientInstance.start();

			log.info("the adminExt [{}] start OK", this.defaultMQAdminExt.getAdminExtGroup());

			this.serviceState = ServiceState.RUNNING;
			break;
		case RUNNING:
		case START_FAILED:
		case SHUTDOWN_ALREADY:
			throw new MQClientException("The AdminExt service state not OK, maybe started once, "//
					+ this.serviceState//
					+ FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
		default:
			break;
		}
	}

	public void shutdown() {
		switch (this.serviceState) {
		case CREATE_JUST:
			break;
		case RUNNING:
			this.mqClientInstance.unregisterAdminExt(this.defaultMQAdminExt.getAdminExtGroup());
			this.mqClientInstance.shutdown();

			log.info("the adminExt [{}] shutdown OK", this.defaultMQAdminExt.getAdminExtGroup());
			this.serviceState = ServiceState.SHUTDOWN_ALREADY;
			break;
		case SHUTDOWN_ALREADY:
			break;
		default:
			break;
		}
	}

	public Set<QueueTimeSpan> queryConsumeTimeSpan(final String topic, final String group)
			throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
		Set<QueueTimeSpan> spanSet = new HashSet<QueueTimeSpan>();
		TopicRouteData topicRouteData = this.examineTopicRouteInfo(topic);
		for (BrokerData bd : topicRouteData.getBrokerDatas()) {
			String addr = bd.selectBrokerAddr();
			if (addr != null) {
				spanSet.addAll(
						this.mqClientInstance.getMQClientAPIImpl().queryConsumeTimeSpan(addr, topic, group, 3000));
			}
		}

		return spanSet;
	}

	public TopicRouteData examineTopicRouteInfo(String topic)
			throws RemotingException, MQClientException, InterruptedException {
		return this.mqClientInstance.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 3000);
	}
}
