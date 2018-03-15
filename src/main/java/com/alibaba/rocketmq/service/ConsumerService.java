package com.alibaba.rocketmq.service;

import static com.alibaba.rocketmq.common.Tool.str;
import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.consumer.ConsumerProgressSubCommand;
import org.apache.rocketmq.tools.command.consumer.DeleteSubscriptionGroupCommand;
import org.apache.rocketmq.tools.command.consumer.UpdateSubGroupSubCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.alibaba.rocketmq.common.ConsumerProgressStatus;
import com.alibaba.rocketmq.common.MessageQueueStatus;
import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.validate.CmdTrace;

@Service
public class ConsumerService extends AbstractService {

	static final Logger logger = LoggerFactory.getLogger(ConsumerService.class);

	static final ConsumerProgressSubCommand consumerProgressSubCommand = new ConsumerProgressSubCommand();

	public Collection<Option> getOptionsForConsumerProgress() {
		return getOptions(consumerProgressSubCommand);
	}

	public ConsumerProgressStatus queryConsumerProgress(String consumerGroup) throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		try {
			defaultMQAdminExt.start();
			final ConsumeStats consumeStats = defaultMQAdminExt.examineConsumeStats(consumerGroup);

			final List<MessageQueue> mqList = new LinkedList<MessageQueue>();
			mqList.addAll(consumeStats.getOffsetTable().keySet());
			Collections.sort(mqList);

			long diffTotal = 0L;
			final ConsumerProgressStatus consumerProgressStatus = new ConsumerProgressStatus();
			for (MessageQueue mq : mqList) {
				OffsetWrapper offsetWrapper = consumeStats.getOffsetTable().get(mq);

				long diff = offsetWrapper.getBrokerOffset() - offsetWrapper.getConsumerOffset();
				diffTotal += diff;

				MessageQueueStatus messageQueueStatus = new MessageQueueStatus();
				messageQueueStatus.setTopic(mq.getTopic());
				messageQueueStatus.setBrokerName(mq.getBrokerName());
				messageQueueStatus.setQueueId(mq.getQueueId());
				messageQueueStatus.setBrokerOffset(offsetWrapper.getBrokerOffset());
				messageQueueStatus.setConsumerOffset(offsetWrapper.getConsumerOffset());
				messageQueueStatus.setDiff(diff);

				consumerProgressStatus.addMessageQueueStatus(messageQueueStatus);
			}

			consumerProgressStatus.setDiffTotal(diffTotal);
			return consumerProgressStatus;
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}
		throw t;
	}

	@CmdTrace(cmdClazz = ConsumerProgressSubCommand.class)
	public Table consumerProgress(String consumerGroup) throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		try {
			defaultMQAdminExt.start();
			if (isNotBlank(consumerGroup)) {
				ConsumeStats consumeStats = defaultMQAdminExt.examineConsumeStats(consumerGroup);

				List<MessageQueue> mqList = new LinkedList<MessageQueue>();
				mqList.addAll(consumeStats.getOffsetTable().keySet());
				Collections.sort(mqList);
				String[] thead = new String[] { "topic", "broker name", "queue id", "broker offset", "consumer offset",
						"diff" };
				long diffTotal = 0L;
				Table table = new Table(thead, mqList.size());
				for (MessageQueue mq : mqList) {
					OffsetWrapper offsetWrapper = consumeStats.getOffsetTable().get(mq);

					long diff = offsetWrapper.getBrokerOffset() - offsetWrapper.getConsumerOffset();
					diffTotal += diff;

					Object[] tr = table.createTR();
					tr[0] = UtilAll.frontStringAtLeast(mq.getTopic(), 50);
					tr[1] = UtilAll.frontStringAtLeast(mq.getBrokerName(), 50);
					tr[2] = str(mq.getQueueId());
					tr[3] = str(offsetWrapper.getBrokerOffset());
					tr[4] = str(offsetWrapper.getConsumerOffset());
					tr[5] = str(diff);

					table.insertTR(tr);
				}

				table.addExtData("consume tps", str(consumeStats.getConsumeTps()));
				table.addExtData("diff total", str(diffTotal));

				return table;
			} else {
				String[] thead = new String[] { "#Group", "#Count", "#Version", "#Type", "#Model", "#TPS",
						"#Diff Total" };

				List<GroupConsumeInfo> groupConsumeInfoList = new LinkedList<GroupConsumeInfo>();
				TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
				for (String topic : topicList.getTopicList()) {
					if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
						String tconsumerGroup = topic.substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());

						try {
							ConsumeStats consumeStats = null;
							try {
								consumeStats = defaultMQAdminExt.examineConsumeStats(tconsumerGroup);
							} catch (Exception e) {
								logger.warn("examineConsumeStats exception, " + tconsumerGroup, e);
							}

							ConsumerConnection cc = null;
							try {
								cc = defaultMQAdminExt.examineConsumerConnectionInfo(tconsumerGroup);
							} catch (Exception e) {
								logger.warn("examineConsumerConnectionInfo exception, " + tconsumerGroup, e);
							}

							GroupConsumeInfo groupConsumeInfo = new GroupConsumeInfo();
							groupConsumeInfo.setGroup(tconsumerGroup);

							if (consumeStats != null) {
								groupConsumeInfo.setConsumeTps((int) consumeStats.getConsumeTps());
								groupConsumeInfo.setDiffTotal(consumeStats.computeTotalDiff());
							}

							if (cc != null) {
								groupConsumeInfo.setCount(cc.getConnectionSet().size());
								groupConsumeInfo.setMessageModel(cc.getMessageModel());
								groupConsumeInfo.setConsumeType(cc.getConsumeType());
								groupConsumeInfo.setVersion(cc.computeMinVersion());
							}

							groupConsumeInfoList.add(groupConsumeInfo);
						} catch (Exception e) {
							logger.warn("examineConsumeStats or examineConsumerConnectionInfo exception, "
									+ tconsumerGroup, e);
						}
						Collections.sort(groupConsumeInfoList);

						Table table = new Table(thead, groupConsumeInfoList.size());
						for (GroupConsumeInfo info : groupConsumeInfoList) {
							Object[] tr = table.createTR();
							tr[0] = UtilAll.frontStringAtLeast(info.getGroup(), 32);
							tr[1] = str(info.getCount());
							tr[2] = info.versionDesc();
							tr[3] = info.consumeTypeDesc();
							tr[4] = info.messageModelDesc();
							tr[5] = str(info.getConsumeTps());
							tr[6] = str(info.getDiffTotal());
							table.insertTR(tr);
						}
						return table;
					}
				}
			}

		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}
		throw t;
	}

	static final DeleteSubscriptionGroupCommand deleteSubscriptionGroupCommand = new DeleteSubscriptionGroupCommand();

	public Collection<Option> getOptionsForDeleteSubGroup() {
		return getOptions(deleteSubscriptionGroupCommand);
	}

	@CmdTrace(cmdClazz = DeleteSubscriptionGroupCommand.class)
	public boolean deleteSubGroup(String groupName, String brokerAddr, String clusterName) throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt adminExt = getDefaultMQAdminExt();
		try {
			if (isNotBlank(brokerAddr)) {
				adminExt.start();

				adminExt.deleteSubscriptionGroup(brokerAddr, groupName);

				return true;
			} else if (isNotBlank(clusterName)) {
				adminExt.start();

				Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(adminExt, clusterName);
				for (String master : masterSet) {
					adminExt.deleteSubscriptionGroup(master, groupName);
				}
				return true;
			} else {
				throw new IllegalStateException("brokerAddr or clusterName can not be all blank");
			}
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(adminExt);
		}
		throw t;
	}

	static final UpdateSubGroupSubCommand updateSubGroupSubCommand = new UpdateSubGroupSubCommand();

	@SuppressWarnings("unchecked")
	public Collection<Option> getOptionsForUpdateSubGroup() {
		Options options = new Options();

		Option opt = new Option("b", "brokerAddr", true, "broker address");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("c", "clusterName", true, "cluster name");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("g", "groupName", true, "consumer group name");
		opt.setRequired(true);
		options.addOption(opt);

		opt = new Option("s", "consumeEnable", true, "是否消费（true | false），默认：true");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("m", "consumeFromMinEnable", true, "是否从最小的offset开始消费（true | false），默认：false");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("d", "consumeBroadcastEnable", true, "是否广播（true | false），默认：false");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("q", "retryQueueNums", true, "retry queue nums, default:1");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("r", "retryMaxTimes", true, "retry max times, default:16");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("i", "brokerId", true, "broker id, default:0 (0:master)");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("w", "whichBrokerWhenConsumeSlowly", true, "消费慢时切换到从broker，默认：1（1：从broker）");
		opt.setRequired(false);
		options.addOption(opt);

		return options.getOptions();
	}

	@CmdTrace(cmdClazz = UpdateSubGroupSubCommand.class)
	public boolean updateSubGroup(String brokerAddr, String clusterName, String groupName, String consumeEnable,
			String consumeFromMinEnable, String consumeBroadcastEnable, String retryQueueNums, String retryMaxTimes,
			String brokerId, String whichBrokerWhenConsumeSlowly) throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();

		try {
			SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
			subscriptionGroupConfig.setConsumeBroadcastEnable(false);
			subscriptionGroupConfig.setConsumeFromMinEnable(false);

			// groupName
			subscriptionGroupConfig.setGroupName(groupName);

			// consumeEnable
			if (isNotBlank(consumeEnable)) {
				subscriptionGroupConfig.setConsumeEnable(Boolean.parseBoolean(consumeEnable.trim()));
			}

			// consumeFromMinEnable
			if (isNotBlank(consumeFromMinEnable)) {
				subscriptionGroupConfig.setConsumeFromMinEnable(Boolean.parseBoolean(consumeFromMinEnable.trim()));
			}

			// consumeBroadcastEnable
			if (isNotBlank(consumeBroadcastEnable)) {
				subscriptionGroupConfig.setConsumeBroadcastEnable(Boolean.parseBoolean(consumeBroadcastEnable.trim()));
			}

			// retryQueueNums
			if (isNotBlank(retryQueueNums)) {
				subscriptionGroupConfig.setRetryQueueNums(Integer.parseInt(retryQueueNums.trim()));
			}

			// retryMaxTimes
			if (isNotBlank(retryMaxTimes)) {
				subscriptionGroupConfig.setRetryMaxTimes(Integer.parseInt(retryMaxTimes.trim()));
			}

			// brokerId
			if (isNotBlank(brokerId)) {
				subscriptionGroupConfig.setBrokerId(Long.parseLong(brokerId.trim()));
			}

			// whichBrokerWhenConsumeSlowly
			if (isNotBlank(whichBrokerWhenConsumeSlowly)) {
				subscriptionGroupConfig.setWhichBrokerWhenConsumeSlowly(Long.parseLong(whichBrokerWhenConsumeSlowly
						.trim()));
			}

			if (isNotBlank(brokerAddr)) {

				defaultMQAdminExt.start();

				defaultMQAdminExt.createAndUpdateSubscriptionGroupConfig(brokerAddr, subscriptionGroupConfig);
				// System.out.printf("create subscription group to %s success.\n",
				// addr);
				// System.out.println(subscriptionGroupConfig);
				return true;

			} else if (isNotBlank(clusterName)) {

				defaultMQAdminExt.start();

				Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
				for (String addr : masterSet) {
					defaultMQAdminExt.createAndUpdateSubscriptionGroupConfig(addr, subscriptionGroupConfig);
					// System.out.printf("create subscription group to %s success.\n",
					// addr);
				}
				// System.out.println(subscriptionGroupConfig);
				return true;
			} else {
				throw new IllegalStateException("brokerAddr or clusterName can not be all blank");
			}

		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}

		throw t;
	}

}

class GroupConsumeInfo implements Comparable<GroupConsumeInfo> {
	private String group;
	private int version;
	private int count;
	private ConsumeType consumeType;
	private MessageModel messageModel;
	private int consumeTps;
	private long diffTotal;

	public String getGroup() {
		return group;
	}

	public String consumeTypeDesc() {
		if (this.count != 0) {
			return this.getConsumeType() == ConsumeType.CONSUME_ACTIVELY ? "PULL" : "PUSH";
		}
		return "";
	}

	public String messageModelDesc() {
		if (this.count != 0 && this.getConsumeType() == ConsumeType.CONSUME_PASSIVELY) {
			return this.getMessageModel().toString();
		}
		return "";
	}

	public String versionDesc() {
		if (this.count != 0) {
			return MQVersion.getVersionDesc(this.version);
		}
		return "";
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public ConsumeType getConsumeType() {
		return consumeType;
	}

	public void setConsumeType(ConsumeType consumeType) {
		this.consumeType = consumeType;
	}

	public MessageModel getMessageModel() {
		return messageModel;
	}

	public void setMessageModel(MessageModel messageModel) {
		this.messageModel = messageModel;
	}

	public long getDiffTotal() {
		return diffTotal;
	}

	public void setDiffTotal(long diffTotal) {
		this.diffTotal = diffTotal;
	}

	@Override
	public int compareTo(GroupConsumeInfo o) {
		if (this.count != o.count) {
			return o.count - this.count;
		}

		return (int) (o.diffTotal - diffTotal);
	}

	public int getConsumeTps() {
		return consumeTps;
	}

	public void setConsumeTps(int consumeTps) {
		this.consumeTps = consumeTps;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}
}
