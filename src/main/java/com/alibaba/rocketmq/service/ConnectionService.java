package com.alibaba.rocketmq.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.cli.Option;
import org.apache.commons.lang.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.ProducerConnection;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.connection.ConsumerConnectionSubCommand;
import org.apache.rocketmq.tools.command.connection.ProducerConnectionSubCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.rocketmq.common.ConsConnection;
import com.alibaba.rocketmq.common.ConsumerConnectionExt;
import com.alibaba.rocketmq.common.HostInfo;
import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.service.mock.DefaultMQAdminExtMock;
import com.alibaba.rocketmq.validate.CmdTrace;

@Service
public class ConnectionService extends AbstractService {
	static final Logger logger = LoggerFactory.getLogger(ConnectionService.class);

	@Autowired
	private MongoProducerRelationService mongoProducerRelationService;

	private final Comparator<ConsConnection> comparator = new Comparator<ConsConnection>() {

		@Override
		public int compare(ConsConnection o1, ConsConnection o2) {
			final String clientId1 = o1.getClientId();
			final String clientId2 = o2.getClientId();

			final String ip1 = clientId1.substring(0, clientId1.indexOf("@"));
			final String ip2 = clientId2.substring(0, clientId2.indexOf("@"));

			long ipLong1 = Long.parseLong(ip1.replace(".", ""));
			long ipLong2 = Long.parseLong(ip2.replace(".", ""));

			if ((ipLong1 - ipLong2) > 0) {
				return 1;
			} else if ((ipLong1 - ipLong2) < 0) {
				return -1;
			}

			return o1.getClientAddr().compareTo(o2.getClientAddr());
		}
	};

	public Table hostProducerConsumerList() throws Throwable {
		Throwable t = null;
		final DefaultMQAdminExtMock defaultMQAdminExt = getDefaultMQAdminExtMock();
		try {
			defaultMQAdminExt.start();

			final ConcurrentHashMap<String/* Client address */, HostInfo> hostInfosMap = new ConcurrentHashMap<String, HostInfo>();
			final Set<String> onlineConsumerGroupNameList = getOnlineConsumerGroupList(defaultMQAdminExt);
			if (onlineConsumerGroupNameList.size() > 0) {
				for (final String consumerGroupName : onlineConsumerGroupNameList) {
					final ConsumerConnectionExt consumerConnectionExt = getConsumerConnectionWithBindQueues(
							defaultMQAdminExt, consumerGroupName);
					for (ConsConnection consConnection : consumerConnectionExt.getConnectionSet()) {
						HostInfo hostInfo = new HostInfo();

						final String hostAddr = getHostAddress(consConnection.getClientAddr());
						HostInfo oldHostnfo = hostInfosMap.putIfAbsent(hostAddr, hostInfo);
						if (null != oldHostnfo) {
							hostInfo = oldHostnfo;
						}

						hostInfo.setClientHostAddress(hostAddr);
						final String bindQueues = consConnection.getBindQueues();
						if (bindQueues != null) {
							hostInfo.setBindQueues(hostInfo.getBindQueues() + "\n"
									+ (hostInfo.getBindQueues().contains(bindQueues) ? "" : bindQueues));
						}

						if (!hostInfo.getClientAddr().contains(consConnection.getClientAddr())) {
							final String clientAddr = hostInfo.getClientAddr()
									+ (hostInfo.getClientAddr().isEmpty() ? "" : ",") + consConnection.getClientAddr()
									+ "(C)";
							hostInfo.setClientAddr(clientAddr);
						}

						hostInfo.setClientId(consConnection.getClientId());
						hostInfo.setConsumerGroup(consumerGroupName);
						final Set<String> topics = consumerConnectionExt.getSubscriptionTable().keySet();
						if (topics != null) {
							StringBuilder sb = new StringBuilder();
							for (final String topic : topics) {
								sb.append(topic + "<br>");
							}
							hostInfo.setSubscribeTopics(sb.toString());
						}
					}
				}
			}

			final Set<String> onlineProducerGroupNameList = defaultMQAdminExt.examineProducerGroups();
			if (onlineProducerGroupNameList.size() > 0) {
				for (String producerGroupName : onlineProducerGroupNameList) {
					if (producerGroupName.equals("CLIENT_INNER_PRODUCER")) {
						continue;
					}

					final ProducerConnection producerConnection = defaultMQAdminExt.examineProducerConnectionInfo(
							producerGroupName, MixAll.DEFAULT_TOPIC);
					for (Connection pConnection : producerConnection.getConnectionSet()) {
						HostInfo hostInfo = new HostInfo();

						final String hostAddr = getHostAddress(pConnection.getClientAddr());
						HostInfo oldHostnfo = hostInfosMap.putIfAbsent(hostAddr, hostInfo);
						if (null != oldHostnfo) {
							hostInfo = oldHostnfo;
						}

						hostInfo.setClientHostAddress(hostAddr);

						if (!hostInfo.getClientAddr().contains(pConnection.getClientAddr())) {
							final String clientAddr = hostInfo.getClientAddr()
									+ (hostInfo.getClientAddr().isEmpty() ? "" : ",") + pConnection.getClientAddr()
									+ "(P)";
							hostInfo.setClientAddr(clientAddr);
						}

						hostInfo.setClientId(pConnection.getClientId());
						hostInfo.setProducerGroup(producerGroupName);
					}
				}
			}

			Table table = new Table(new String[] { "clientId", "clientAddr", "owner", "producerGroup", "consumerGroup",
					"subscribeTopics", "bindQueues" }, hostInfosMap.size());

			ArrayList<HostInfo> hostProducerConsumerInfos = new ArrayList<HostInfo>(hostInfosMap.values());
			Collections.sort(hostProducerConsumerInfos, new Comparator<HostInfo>() {

				@Override
				public int compare(HostInfo o1, HostInfo o2) {
					return o1.getClientAddr().compareTo(o2.getClientAddr());
				}
			});

			for (final HostInfo hostInfo : hostProducerConsumerInfos) {
				Object[] tr = table.createTR();
				tr[0] = hostInfo.getClientId();
				tr[1] = hostInfo.getClientAddr();

				String owner = "NO";
				if (mongoProducerRelationService.getConsumerOwnerRelations().get(hostInfo.getConsumerGroup()) != null) {
					owner = mongoProducerRelationService.getConsumerOwnerRelations().get(hostInfo.getConsumerGroup());
				} else if (mongoProducerRelationService.getProducerOwnerRelations().get(hostInfo.getProducerGroup()) != null) {
					owner = mongoProducerRelationService.getProducerOwnerRelations().get(hostInfo.getProducerGroup());
				}
				tr[2] = owner;
				tr[3] = hostInfo.getProducerGroup();
				tr[4] = hostInfo.getConsumerGroup();
				tr[5] = hostInfo.getSubscribeTopics();
				tr[6] = hostInfo.getBindQueues();
				table.insertTR(tr);
			}

			return table;
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}
		throw t;
	}

	private static String getHostAddress(String clientAddress) {
		return clientAddress.substring(0, clientAddress.indexOf(":"));
	}

	public Table getOnlineProducerGroupList() throws Throwable {
		Throwable t = null;
		DefaultMQAdminExtMock defaultMQAdminExt = getDefaultMQAdminExtMock();
		try {
			defaultMQAdminExt.start();

			final Set<String> onlineProducerGroupNameList = defaultMQAdminExt.examineProducerGroups();
			if (onlineProducerGroupNameList.size() > 0) {
				Table table = new Table(new String[] { "producer group", "owner", "send topics" },
						onlineProducerGroupNameList.size());
				for (String producerGroupName : onlineProducerGroupNameList) {
					Object[] tr = table.createTR();
					tr[0] = producerGroupName;
					tr[1] = mongoProducerRelationService.getProducerOwnerRelations().get(producerGroupName) != null ? mongoProducerRelationService
							.getProducerOwnerRelations().get(producerGroupName) : "NO";
					tr[2] = mongoProducerRelationService.getProducerTopicRelations().get(producerGroupName) != null ? mongoProducerRelationService
							.getProducerTopicRelations().get(producerGroupName).toString()
							: "NO";
					table.insertTR(tr);
				}
				return table;
			} else {
				throw new IllegalStateException("onlineGroupNameList is blank");
			}
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}
		throw t;
	}

	public Table getOnlineConsumerGroupList() throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		try {
			defaultMQAdminExt.start();

			final List<String> onlineConsumerGroupNameList = new ArrayList<String>(
					getOnlineConsumerGroupList(defaultMQAdminExt));
			Collections.sort(onlineConsumerGroupNameList);
			if (onlineConsumerGroupNameList.size() > 0) {
				Table table = new Table(new String[] { "consumer group", "owner" }, onlineConsumerGroupNameList.size());
				for (String consumerGroupName : onlineConsumerGroupNameList) {
					Object[] tr = table.createTR();
					tr[0] = consumerGroupName;
					tr[1] = mongoProducerRelationService.getConsumerOwnerRelations().get(consumerGroupName) != null ? mongoProducerRelationService
							.getConsumerOwnerRelations().get(consumerGroupName) : "NO";
					table.insertTR(tr);
				}
				return table;
			} else {
				throw new IllegalStateException("onlineGroupNameList is blank");
			}
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}
		throw t;
	}

	public Set<String> getOnlineConsumerGroupList(DefaultMQAdminExt defaultMQAdminExt) throws RemotingException,
			MQClientException, InterruptedException, MQBrokerException {
		Set<String> groupNameList = new HashSet<String>();
		TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
		if (topicList.getTopicList().size() > 0) {
			for (String topicName : topicList.getTopicList()) {
				GroupList groplist = defaultMQAdminExt.queryTopicConsumeByWho(topicName);
				if (groplist != null && groplist.getGroupList().size() > 0) {
					groupNameList.addAll(groplist.getGroupList());
				}
			}
		}

		Set<String> onlineGroupNameList = new HashSet<String>();
		for (String group : groupNameList) {
			try {
				defaultMQAdminExt.examineConsumeStats(group);
				defaultMQAdminExt.examineConsumerConnectionInfo(group);
				onlineGroupNameList.add(group);
			} catch (Throwable e) {
			}
		}

		return onlineGroupNameList;
	}

	/**
	 * offline consumer from all brokers
	 * 
	 * @param consumerGroup
	 * @param clientIds
	 * @return
	 * @throws Throwable
	 */
	public Table offlineConsumerByClientIds(final String consumerGroup, final String clientIds) throws Throwable {
		Throwable t = null;
		try {
			final Map<String, String> offlineMap = offlineConsumerByClientIdsGroup(consumerGroup, clientIds);
			if (offlineMap.size() > 0) {
				return Table.Map2VTable(offlineMap);
			}
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		}
		throw t;
	}

	/**
	 * online consumer from all brokers
	 * 
	 * @param consumerGroup
	 * @param clientIds
	 * @return
	 * @throws Throwable
	 */
	public Table onlineConsumerByClientIds(final String consumerGroup, final String clientIds) throws Throwable {
		Throwable t = null;
		try {
			final Map<String, String> onlineMap = onlineConsumerByClientIdsGroup(consumerGroup, clientIds);
			if (onlineMap.size() > 0) {
				return Table.Map2VTable(onlineMap);
			}
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		}
		throw t;
	}

	public Map<String, String> onlineConsumerByClientIdsGroup(final String consumerGroup, final String clientIds)
			throws Throwable {
		DefaultMQAdminExtMock defaultMQAdminExt = getDefaultMQAdminExtMock();
		final Map<String, String> onlineMap = new HashMap<String, String>();
		try {
			defaultMQAdminExt.start();
			final Map<String, Boolean> offlineResult = defaultMQAdminExt.onlineConsumerClientIdsByGroup(consumerGroup,
					clientIds);

			for (Entry<String, Boolean> entry : offlineResult.entrySet()) {
				onlineMap.put(entry.getKey(), entry.getValue() ? "Success" : "failed");
			}
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			throw e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}

		return onlineMap;
	}

	public Map<String, String> offlineConsumerByClientIdsGroup(final String consumerGroup, final String clientIds)
			throws Throwable {
		DefaultMQAdminExtMock defaultMQAdminExt = getDefaultMQAdminExtMock();
		DefaultMQProducer defaultProducer = null;
		final Map<String, String> offlineMap = new HashMap<String, String>();
		try {
			defaultMQAdminExt.start();
			defaultProducer = this.getStartDefaultProducer();
			final MQClientAPIImpl mQClientAPIImpl = defaultProducer.getDefaultMQProducerImpl().getmQClientFactory()
					.getMQClientAPIImpl();

			final List<String> onlineConsumerClientIds = getOnlineClientIds(consumerGroup, defaultMQAdminExt,
					mQClientAPIImpl);
			System.out.println(">>>>>>>>>>>>online consumerClientIds:" + onlineConsumerClientIds);

			if (onlineConsumerClientIds.size() > 1) {
				final Map<String, Boolean> offlineResult = defaultMQAdminExt.offlineConsumerClientIdsByGroup(
						consumerGroup, clientIds);

				for (Entry<String, Boolean> entry : offlineResult.entrySet()) {
					offlineMap.put(entry.getKey(), entry.getValue() ? "Success" : "failed");
				}
			} else {
				throw new RuntimeException("can not offline the last consumer.");
			}
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			throw e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
			if (defaultProducer != null) {
				defaultProducer.shutdown();
			}
		}

		return offlineMap;
	}

	/**
	 * 根据host ip查询当前机器连接到broker的所有消息信息
	 * 
	 * @param hostIp
	 * @return
	 * @throws Throwable
	 */
	public ConsumerConnectionExt queryConsumerByIp(String hostIp) throws Throwable {
		Throwable t = null;
		DefaultMQAdminExtMock defaultMQAdminExt = getDefaultMQAdminExtMock();
		DefaultMQProducer defaultProducer = null;
		try {
			defaultMQAdminExt.start();

			final Set<String> onlineGroupNameList = getOnlineConsumerGroupList(defaultMQAdminExt);

			defaultProducer = this.getStartDefaultProducer();
			final MQClientAPIImpl mQClientAPIImpl = defaultProducer.getDefaultMQProducerImpl().getmQClientFactory()
					.getMQClientAPIImpl();

			final TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(MixAll.DEFAULT_TOPIC);

			final Map<ConsumerConnection, String> connectionGroupMap = new HashMap<ConsumerConnection, String>();
			final Map<ConsumerConnection, String> brokerMap = new HashMap<ConsumerConnection, String>();
			final List<ConsumerConnection> consumerConnections = new LinkedList<ConsumerConnection>();
			for (BrokerData bd : topicRouteData.getBrokerDatas()) {
				String brokerAddr = bd.selectBrokerAddr();
				if (brokerAddr != null) {
					for (String onlineGroupName : onlineGroupNameList) {
						ConsumerConnection consumerConnection = mQClientAPIImpl.getConsumerConnectionList(brokerAddr,
								onlineGroupName, 3000);
						connectionGroupMap.put(consumerConnection, onlineGroupName);
						brokerMap.put(consumerConnection, brokerAddr);
						consumerConnections.add(consumerConnection);
					}
				}
			}

			final TreeSet<ConsConnection> targetConnectionSet = new TreeSet<ConsConnection>(comparator);
			for (ConsumerConnection consumerConnection : consumerConnections) {
				HashSet<Connection> connectionSet = consumerConnection.getConnectionSet();
				for (Connection connection : connectionSet) {
					if (connection.getClientAddr().contains(hostIp + ":")) {
						final ConsConnection consConnection = new ConsConnection();
						consConnection.setClientAddr(connection.getClientAddr());
						consConnection.setClientId(connection.getClientId());
						consConnection.setLanguage(connection.getLanguage());
						consConnection.setVersion(connection.getVersion());
						final String connectedBroker = brokerMap.get(consumerConnection);
						consConnection.setConnectedBroker(connectedBroker);

						final String consumerGroup = connectionGroupMap.get(consumerConnection);
						final List<String> consumerClientIds = mQClientAPIImpl.getConsumerIdListByGroup(
								connectedBroker, consumerGroup, 5000);
						// System.out.println(">>>>>>>>>>>>>connectedBroker:" + connectedBroker
						// + ">>>>>>>>>>>>consumerClientIds:" + consumerClientIds);

						consConnection.setConsumerGroup(consumerGroup);
						consConnection.setOffline(!consumerClientIds.contains(connection.getClientId()));

						Map<String, String> queuesMap = defaultMQAdminExt.getQueuesByConsumerAddress(connection
								.getClientAddr());

						// System.out.println(">>>>>>>>>>>>>>>>for clientAddr:" + connection.getClientAddr()
						// + ">>>>>>>queuesMap:" + queuesMap);

						final StringBuilder bindQueuesString = new StringBuilder();
						try {
							final String bindQueues = queuesMap.get(connectedBroker);
							if (StringUtils.isNotEmpty(bindQueues)) {
								String[] bindQueueArray = org.apache.commons.lang.StringUtils.split(bindQueues, ",");

								Set<String> queueSet = new TreeSet<String>();
								for (String queue : bindQueueArray) {
									queueSet.add(queue);
								}

								for (String queue : queueSet) {
									bindQueuesString.append(queue + "<br>");
								}
							}
						} catch (Exception e) {
						}

						consConnection
								.setBindQueues(StringUtils.isNotEmpty(bindQueuesString.toString()) ? ("<b>[broker-"
										+ connectedBroker + "]:</b>" + bindQueuesString) : "");
						targetConnectionSet.add(consConnection);
					}
				}
			}

			if (targetConnectionSet.size() > 0) {
				ConsumerConnectionExt consumerConnectionExt = new ConsumerConnectionExt();
				consumerConnectionExt.setConnectionSet(targetConnectionSet);
				return consumerConnectionExt;
			} else {
				throw new RuntimeException("con not find connection for this ip.");
			}

		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);

			if (defaultProducer != null) {
				defaultProducer.shutdown();
			}
		}
		throw t;
	}

	private ConsumerConnectionExt getConsumerConnectionWithBindQueues(DefaultMQAdminExtMock defaultMQAdminExt,
			String consumerGroup) {
		try {
			final Map<String, ConsumerConnection> ccMap = defaultMQAdminExt
					.examineConsumerConnectionInfoByBroker(consumerGroup);
			if (ccMap.isEmpty()) {
				return new ConsumerConnectionExt();
			}

			final ConsumerConnection cc = ccMap.entrySet().iterator().next().getValue();
			ConsumerConnectionExt consumerConnectionExt = new ConsumerConnectionExt();
			consumerConnectionExt.setConsumeFromWhere(cc.getConsumeFromWhere());
			consumerConnectionExt.setConsumeType(cc.getConsumeType());
			consumerConnectionExt.setMessageModel(cc.getMessageModel());
			consumerConnectionExt.setSubscriptionTable(cc.getSubscriptionTable());

			final TreeSet<ConsConnection> consuConnectionSet = new TreeSet<ConsConnection>(comparator);
			for (Entry<String, ConsumerConnection> entry : ccMap.entrySet()) {
				final String brokerAddr = entry.getKey();

				HashSet<Connection> connectionSet = entry.getValue().getConnectionSet();
				for (Connection connection : connectionSet) {
					final ConsConnection consConnection = new ConsConnection();
					consConnection.setClientAddr(connection.getClientAddr());
					consConnection.setClientId(connection.getClientId());
					consConnection.setLanguage(connection.getLanguage());
					consConnection.setVersion(connection.getVersion());
					consConnection.setConnectedBroker(entry.getKey());

					final StringBuilder bindQueuesString = new StringBuilder();
					try {
						final String bindQueusByBroker = defaultMQAdminExt.getQueuesByBrokerAndConsumerAddress(
								brokerAddr, connection.getClientAddr());
						if (StringUtils.isNotEmpty(bindQueusByBroker)) {
							String[] bindQueueArray = org.apache.commons.lang.StringUtils.split(bindQueusByBroker, ",");

							Set<String> queueSet = new TreeSet<String>();
							for (String queue : bindQueueArray) {
								queueSet.add(queue);
							}

							for (String queue : queueSet) {
								bindQueuesString.append(queue + "<br>");
							}
						}
					} catch (Exception e) {
					}

					if (StringUtils.isNotEmpty(bindQueuesString.toString())) {
						final String bindQueues = "<b>[broker:" + brokerAddr + "]=</b>" + bindQueuesString;
						consConnection.setBindQueues(bindQueues);
						// System.out.println(">>>>>>>>>>>>>>>consumerGroup:" + consumerGroup +
						// ">>>>>>>>consConnection:"
						// + ToStringBuilder.reflectionToString(consConnection));
						consuConnectionSet.add(consConnection);
					}
				}
			}
			consumerConnectionExt.setConnectionSet(consuConnectionSet);
			return consumerConnectionExt;
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
		}

		return new ConsumerConnectionExt();
	}

	@CmdTrace(cmdClazz = ConsumerConnectionSubCommand.class)
	public ConsumerConnectionExt getConsumerConnection(String consumerGroup) throws Throwable {
		Throwable t = null;
		DefaultMQAdminExtMock defaultMQAdminExt = getDefaultMQAdminExtMock();
		DefaultMQProducer defaultProducer = null;
		try {
			defaultMQAdminExt.start();

			defaultProducer = this.getStartDefaultProducer();
			final MQClientAPIImpl mQClientAPIImpl = defaultProducer.getDefaultMQProducerImpl().getmQClientFactory()
					.getMQClientAPIImpl();

			final List<String> consumerClientIds = getOnlineClientIds(consumerGroup, defaultMQAdminExt, mQClientAPIImpl);
			// System.out.println(">>>>>>>>>>>>online consumerClientIds:" + consumerClientIds);

			final Map<String, ConsumerConnection> ccMap = defaultMQAdminExt
					.examineConsumerConnectionInfoByBroker(consumerGroup);
			if (ccMap.isEmpty()) {
				return new ConsumerConnectionExt();
			}

			final ConsumerConnection cc = ccMap.entrySet().iterator().next().getValue();
			ConsumerConnectionExt consumerConnectionExt = new ConsumerConnectionExt();
			consumerConnectionExt.setConsumeFromWhere(cc.getConsumeFromWhere());
			consumerConnectionExt.setConsumeType(cc.getConsumeType());
			consumerConnectionExt.setMessageModel(cc.getMessageModel());
			consumerConnectionExt.setSubscriptionTable(cc.getSubscriptionTable());

			final TreeSet<ConsConnection> consuConnectionSet = new TreeSet<ConsConnection>(comparator);
			for (Entry<String, ConsumerConnection> entry : ccMap.entrySet()) {
				final String brokerAddr = entry.getKey();
				HashSet<Connection> connectionSet = entry.getValue().getConnectionSet();
				for (Connection connection : connectionSet) {
					final ConsConnection consConnection = new ConsConnection();
					consConnection.setClientAddr(connection.getClientAddr());
					consConnection.setClientId(connection.getClientId());
					consConnection.setLanguage(connection.getLanguage());
					consConnection.setVersion(connection.getVersion());
					consConnection.setOffline(!consumerClientIds.contains(connection.getClientId()));

					final StringBuilder bindQueuesString = new StringBuilder();
					try {
						final String bindQueusByBroker = defaultMQAdminExt.getQueuesByBrokerAndConsumerAddress(
								brokerAddr, connection.getClientAddr());
						String[] bindQueueArray = org.apache.commons.lang.StringUtils.split(bindQueusByBroker, ",");

						Set<String> queueSet = new TreeSet<String>();
						for (String queue : bindQueueArray) {
							queueSet.add(queue);
						}

						for (String queue : queueSet) {
							bindQueuesString.append(queue + "<br>");
						}
					} catch (Exception e) {
					}

					String bindQueues = "<b>[broker:" + brokerAddr + "]=</b>" + bindQueuesString;
					consConnection.setBindQueues(bindQueues);
					// System.out.println(">>>>>>>>>>>>>>>consumerGroup:" + consumerGroup + ">>>>>>>>consConnection:"
					// + ToStringBuilder.reflectionToString(consConnection));
					consuConnectionSet.add(consConnection);
				}
			}

			consumerConnectionExt.setConnectionSet(consuConnectionSet);

			return consumerConnectionExt;
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
			if (defaultProducer != null) {
				defaultProducer.shutdown();
			}
		}
		throw t;
	}

	private List<String> getOnlineClientIds(String consumerGroup, DefaultMQAdminExt defaultMQAdminExt,
			final MQClientAPIImpl mQClientAPIImpl) throws RemotingException, MQClientException, InterruptedException,
			RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException {
		final TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(MixAll.DEFAULT_TOPIC);
		for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
			final String selectBrokerAddr = brokerData.selectBrokerAddr();

			try {
				final List<String> consumerClientIds = mQClientAPIImpl.getConsumerIdListByGroup(selectBrokerAddr,
						consumerGroup, 5000);
				// System.out.println(">>>>>>>>>>>>online consumerClientIds:" + consumerClientIds
				// + " on selectBrokerAddr:" + selectBrokerAddr);
				return consumerClientIds;
			} catch (Exception e) {
			}
		}
		return new ArrayList<String>();
	}

	static final ProducerConnectionSubCommand producerConnectionSubCommand = new ProducerConnectionSubCommand();

	public Collection<Option> getOptionsForGetProducerConnection() {
		return getOptions(producerConnectionSubCommand);
	}

	static final ConsumerConnectionSubCommand consumerConnectionSubCommand = new ConsumerConnectionSubCommand();

	public Collection<Option> getOptionsForGetConsumerConnection() {
		return getOptions(consumerConnectionSubCommand);
	}

	@CmdTrace(cmdClazz = ProducerConnectionSubCommand.class)
	public ProducerConnection getProducerConnection(String group, String topicName) throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		try {
			defaultMQAdminExt.start();
			ProducerConnection pc = defaultMQAdminExt.examineProducerConnectionInfo(group, topicName);
			return pc;
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}
		throw t;
	}

	private DefaultMQProducer getStartDefaultProducer() throws Throwable {
		Throwable t = null;

		DefaultMQProducer producer = new DefaultMQProducer("console_default_producer_group1");
		producer.setNamesrvAddr(configureInitializer.getNamesrvAddr());
		try {
			producer.start();
			return producer;
		} catch (Throwable e) {
			e.printStackTrace();
			t = e;
		}
		throw t;
	}

}
