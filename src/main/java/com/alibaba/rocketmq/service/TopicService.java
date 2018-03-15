package com.alibaba.rocketmq.service;

import static com.alibaba.rocketmq.common.Tool.str;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.time.DateUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.topic.DeleteTopicSubCommand;
import org.apache.rocketmq.tools.command.topic.TopicListSubCommand;
import org.apache.rocketmq.tools.command.topic.TopicRouteSubCommand;
import org.apache.rocketmq.tools.command.topic.TopicStatusSubCommand;
import org.apache.rocketmq.tools.command.topic.UpdateTopicSubCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.rocketmq.admin.ConsoleMQAdminExt;
import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.utils.MessageUtils;
import com.alibaba.rocketmq.validate.CmdTrace;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

@Service
public class TopicService extends AbstractService implements InitializingBean {

	private static final Logger logger = LoggerFactory.getLogger(TopicService.class);

	@Autowired
	private MongoService mongoService;

	@Autowired
	private MongoProducerRelationService mongoProducerRelationService;

	private static final int DLQ_BROKER_MAX_NUMS = 10;

	private static final int MESSAGE_MAX_NUMS = 30;

	private static final String HASH_CODE = "HASH_CODE";

	private final ConcurrentHashMap<String, AtomicBoolean> sendDlqFlag = new ConcurrentHashMap<String, AtomicBoolean>(
			32);

	@Override
	public void afterPropertiesSet() throws Exception {

	}

	public void resendAllDLQMsgs(final String DLQTOPIC_NAME) {
		Runnable runn = new Runnable() {
			public void run() {
				try {
					if (null == sendDlqFlag.get(DLQTOPIC_NAME)) {
						sendDlqFlag.put(DLQTOPIC_NAME, new AtomicBoolean(true));
						runSendDlq(DLQTOPIC_NAME);
					} else {
						logger.error(">>>>>>>>>>>>>have running the job for " + DLQTOPIC_NAME);
					}
				} catch (Throwable e) {
					logger.error("ResendDLQServiceMsg error", e);
				}
			}
		};
		new Thread(runn).start();
	}

	private void runSendDlq(final String DLQTOPIC_NAME) {
		logger.info(">>>>>>>>>>>>>start ResendDLQMsgScheduledThread>>>>>" + DLQTOPIC_NAME);
		final ConcurrentHashMap<String, String> sendMsgMap = new ConcurrentHashMap<String, String>(10000);
		final AtomicLong msgSizeCounter = new AtomicLong(0);
		final AtomicLong sendMsgCounter = new AtomicLong(0);

		final DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		final DefaultMQProducer producer = new DefaultMQProducer("rocketmq_console_producer_group");
		producer.setNamesrvAddr(configureInitializer.getNamesrvAddr());
		try {
			defaultMQAdminExt.start();
			producer.start();

			TopicStatsTable topicStatsTable = defaultMQAdminExt.examineTopicStats(DLQTOPIC_NAME);

			List<MessageQueue> mqList = new LinkedList<MessageQueue>();
			mqList.addAll(topicStatsTable.getOffsetTable().keySet());
			Collections.sort(mqList);
			for (MessageQueue messageQueue : mqList) {
				logger.info(">>>>>>>>>>>>>>>>>>>DLQ messageQueue:" + ToStringBuilder.reflectionToString(messageQueue));
			}

			for (final MessageQueue mq : mqList) {
				try {
					final long maxOffset = defaultMQAdminExt.maxOffset(mq);
					final long minOffset = defaultMQAdminExt.minOffset(mq);
					logger.info(mq.getBrokerName() + ">>>>>>>>>>>>>>>>>>>maxOffset:" + maxOffset);
					logger.info(">>>>>>>>>>>>>>>>>>>minOffset:" + minOffset);

					final int totalPageNum = (int) ((maxOffset - minOffset) / DLQ_BROKER_MAX_NUMS + 1);
					logger.info(">>>>>>>>>>>>>>>>>>>totalPageNum:" + totalPageNum);

					for (int pageNum = 1; pageNum <= totalPageNum; pageNum++) {
						long startOffset = maxOffset - DLQ_BROKER_MAX_NUMS * pageNum;
						if (startOffset < 0 || startOffset < minOffset) {
							startOffset = minOffset;
						}
						logger.info(">>>>>>>>>>>>>>>>>>>startOffset:" + startOffset);

						final List<MessageExt> msgList = queryMsgListByOffset(mq, startOffset);
						if (msgList != null && msgList.size() > 0) {
							msgSizeCounter.addAndGet(msgList.size());
							Comparator<MessageExt> c = new Comparator<MessageExt>() {
								public int compare(MessageExt o1, MessageExt o2) {
									return (int) (o2.getQueueOffset() - o1.getQueueOffset());
								}
							};
							Collections.sort(msgList, c);

							for (final MessageExt dlqMessageExt : msgList) {
								final String origin_message_id = dlqMessageExt
										.getProperty(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
								final MessageExt originalMessageExt = defaultMQAdminExt.viewMessage(origin_message_id);

								if (sendMsgMap.get(dlqMessageExt.getMsgId()) != null) {
									logger.warn(">>>>>>have sent this dlqmessage::" + dlqMessageExt.getMsgId());
									continue;
								}

								final SendResult sendResult = produceOriginal(producer, originalMessageExt);
								sendMsgMap.put(dlqMessageExt.getMsgId(), "1");
								if (sendResult != null) {
									sendMsgCounter.incrementAndGet();
									Thread.sleep(800);

									logger.info(">>>>>>dlqmessage sendResult:" + dlqMessageExt.getMsgId() + ">>>>"
											+ sendResult);

									try {
										final DBObject dbObject = new BasicDBObject();
										dbObject.put("DLQ_MESSAGEID", dlqMessageExt.getMsgId());
										dbObject.put("ORIGIN_MESSAGE_ID", origin_message_id);
										dbObject.put("NEW_MESSAGE_ID", sendResult.getMsgId());
										dbObject.put(MessageUtils.CREATE_TIME, new SimpleDateFormat(
												"yyyy-MM-dd HH:mm:ss").format(new Date()));
										dbObject.put("DLQ_TOPIC_NAME", DLQTOPIC_NAME);

										mongoService.getCollectionByName("DLQ_RESEND_HISTORY").insert(dbObject);
									} catch (Exception e) {
										logger.warn("#######store history to mongo warning" + e.getMessage());
									}
								} else {
									logger.error(">>>>>>dlqmessage sendResult is null for dlq messageId:"
											+ dlqMessageExt.getMsgId());
								}
							}
						}
					}
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				}
			}

			logger.info("########################" + DLQTOPIC_NAME + ", msgSizeCounter:" + msgSizeCounter
					+ ", have sent msgs:" + sendMsgCounter.get());
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
			producer.shutdown();
		}
	}

	private SendResult produceOriginal(final DefaultMQProducer producer, final MessageExt originalMessageExt)
			throws Throwable {
		try {
			String tagsTemp = originalMessageExt.getTags();
			if (StringUtils.isBlank(tagsTemp)) {
				tagsTemp = StringUtils.EMPTY;
			}

			String keysTemp = originalMessageExt.getKeys();
			if (StringUtils.isBlank(keysTemp)) {
				keysTemp = StringUtils.EMPTY;
			}

			String topic = originalMessageExt.getTopic();
			final Message newMsg = new Message(topic, tagsTemp, keysTemp, originalMessageExt.getBody());
			Map<String, String> properties = originalMessageExt.getProperties();
			for (Entry<String, String> entry : properties.entrySet()) {
				if (!MessageConst.STRING_HASH_SET.contains(entry.getKey())) {
					newMsg.putUserProperty(entry.getKey(), entry.getValue());
				}
			}
			newMsg.putUserProperty(HASH_CODE, "" + generateMsgHashCode(newMsg));

			int times = 3;
			SendResult sendResult = null;
			while (times > 0) {
				try {
					sendResult = producer.send(newMsg);
					times = 0;
				} catch (InterruptedException | RemotingException | MQClientException | MQBrokerException e) {
					logger.warn("Send message error, e={} , try times:" + times, e);
					times--;
				}
			}
			return sendResult;
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
		}

		return null;
	}

	public static int generateMsgHashCode(final Message msg) {
		int hashcode = 0;
		try {
			Message cloneMsg = (Message) deepClone(msg);
			if (cloneMsg != null) {
				// remove old hash code
				cloneMsg.getProperties().remove(HASH_CODE);

				// make sure _catChildMessageId1 is thread id
				hashcode = cloneMsg.hashCode();
			}
		} catch (Throwable e) {
			logger.error("generateMsgHashCode error " + e.getMessage());
		}
		return hashcode;
	}

	private static Object deepClone(Object src) {
		Object o = null;
		try {
			if (src != null) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos);
				oos.writeObject(src);
				oos.close();

				ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
				ObjectInputStream ois = new ObjectInputStream(bais);
				o = ois.readObject();
				ois.close();
			}
		} catch (Throwable e) {
		}
		return o;
	}

	class SchedualThreadFactory implements ThreadFactory {
		private final AtomicLong threadIndex = new AtomicLong(0);
		private final String threadNamePrefix;

		public SchedualThreadFactory(final String threadNamePrefix) {
			this.threadNamePrefix = threadNamePrefix;
		}

		@Override
		public Thread newThread(Runnable r) {
			return new Thread(r, threadNamePrefix + this.threadIndex.incrementAndGet());
		}
	}

	@CmdTrace(cmdClazz = TopicListSubCommand.class)
	public Table list() throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		try {
			defaultMQAdminExt.start();
			final TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
			final ArrayList<String> topicNameList = new ArrayList<String>(topicList.getTopicList());
			Collections.sort(topicNameList);

			final int row = topicNameList.size();
			if (row > 0) {
				Table table = new Table(new String[] { "topic", "producer", "owner" }, row);
				for (String topicName : topicNameList) {
					Object[] tr = table.createTR();
					tr[0] = topicName;

					String targetTopic = topicName;
					if (topicName.endsWith("_BLUE")) {
						targetTopic = topicName.substring(0, topicName.indexOf("_BLUE"));
					} else if (topicName.endsWith("_GREEN")) {
						targetTopic = topicName.substring(0, topicName.indexOf("_GREEN"));
					}

					final String producerGroupName = getProducerName(targetTopic);
					tr[1] = producerGroupName;
					tr[2] = mongoProducerRelationService.getProducerOwnerRelations().get(producerGroupName) != null ? mongoProducerRelationService
							.getProducerOwnerRelations().get(producerGroupName) : "NO";
					table.insertTR(tr);
				}
				return table;
			} else {
				throw new IllegalStateException("defaultMQAdminExt.fetchAllTopicList() is blank");
			}
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}
		throw t;
	}

	private String getProducerName(String targetTopic) {
		for (Entry<String, List<String>> producerEntry : mongoProducerRelationService.getProducerTopicRelations()
				.entrySet()) {
			if (producerEntry.getValue().contains(targetTopic)) {
				return producerEntry.getKey();
			}
		}

		return "NO";
	}

	public Set<String> getAllTopics() {
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		try {
			defaultMQAdminExt.start();
			TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
			return topicList.getTopicList();
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}

		return new HashSet<String>();
	}

	public Table dealDLQ(String topicName, int pageNum) throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();

		try {
			defaultMQAdminExt.start();

			TopicStatsTable topicStatsTable = defaultMQAdminExt.examineTopicStats(topicName);

			List<MessageQueue> mqList = new LinkedList<MessageQueue>();
			mqList.addAll(topicStatsTable.getOffsetTable().keySet());
			Collections.sort(mqList);

			String[] thead = new String[] { "messageId", "broker", "queueId", "queueOffset", "RETRY_TOPIC",
					"Store Timestamp", "body" };
			Table table = new Table(thead, mqList.size() * DLQ_BROKER_MAX_NUMS);
			for (final MessageQueue mq : mqList) {
				try {
					long maxOffset = defaultMQAdminExt.maxOffset(mq);
					long minOffset = defaultMQAdminExt.minOffset(mq);

					long startOffset = maxOffset - DLQ_BROKER_MAX_NUMS * pageNum;
					if (startOffset < 0 || startOffset < minOffset) {
						startOffset = minOffset;
					}

					List<MessageExt> msgList = queryMsgListByOffset(mq, startOffset);
					if (msgList != null && msgList.size() > 0) {
						// logger.info(">>>>>>>>>>>>>>>>>>>msgList.SIZE:" + msgList.size() + ">>>>>>>mq:"
						// + mq.getBrokerName());
						Comparator<MessageExt> c = new Comparator<MessageExt>() {
							public int compare(MessageExt o1, MessageExt o2) {
								return (int) (o2.getQueueOffset() - o1.getQueueOffset());
							}
						};
						Collections.sort(msgList, c);

						for (MessageExt messageExt : msgList) {
							Object[] tr = table.createTR();
							tr[0] = messageExt.getMsgId();
							tr[1] = UtilAll.frontStringAtLeast(mq.getBrokerName(), 32);
							tr[2] = str(messageExt.getQueueId());
							tr[3] = str(messageExt.getQueueOffset());
							tr[4] = messageExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC) + "";
							tr[5] = UtilAll.timeMillisToHumanString2(messageExt.getStoreTimestamp());
							tr[6] = MessageUtils.conver2String(messageExt);
							table.insertTR(tr);
						}
					}
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				}
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

	public Table queryMessages(String topicName, int pageNum, int dayBeforeToday, final AtomicInteger msgCounter)
			throws Throwable {
		Throwable t = null;
		final List<DBObject> msgList = new ArrayList<>(MESSAGE_MAX_NUMS * 2);

		String collectionName = MessageUtils.getMsgCollectionName(DateUtils.addDays(new Date(), dayBeforeToday * -1));
		DBCollection collection = mongoService.getCollectionByName(collectionName);
		if (collection != null) {
			long countByCollectionTopic = mongoService.countByCollectionTopic(collectionName, topicName);
			if (countByCollectionTopic > 0) {
				msgCounter.set((int) countByCollectionTopic);

				DBObject query = new BasicDBObject();
				query.put("topic", topicName);

				DBObject sortquery = new BasicDBObject();
				sortquery.put("storeTime", -1);

				DBCursor msgCursor = collection.find(query).sort(sortquery).skip(MESSAGE_MAX_NUMS * (pageNum - 1))
						.limit(MESSAGE_MAX_NUMS);
				while (msgCursor.hasNext()) {
					if (msgList.size() < MESSAGE_MAX_NUMS) {
						DBObject ret = msgCursor.next();
						msgList.add(ret);
					} else {
						break;
					}
				}
			}
		} else {
			throw new RuntimeException("no this collection:" + collectionName);
		}

		try {
			String[] thead = new String[] { "messageId", "bornHost", "storeHost", "queueId", "queueOffset", "topic",
					"storeTime", "bornTime", "body", "properties" };
			Table table = new Table(thead, MESSAGE_MAX_NUMS);
			for (final DBObject msg : msgList) {
				try {
					Object[] tr = table.createTR();
					tr[0] = 	"<a href=/rocketmq-console/message/queryMsgById.do?msgId="+ msg.get("msgId")+ ">" +msg.get("msgId") + "</a>";
					
					tr[1] = msg.get("bornHost") + "";
					tr[2] = msg.get("storeHost") + "";
					tr[3] = msg.get("queueId") + "";
					tr[4] = msg.get("queueOffset") + "";
					tr[5] = msg.get("topic") + "";
					tr[6] = msg.get("storeTime") + "";
					tr[7] = msg.get("bornTime") + "";
					tr[8] = msg.get("content") + "";
					tr[9] = msg.get("propertiesString") + "";
					table.insertTR(tr);
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				}
			}

			return table;
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
		}
		throw t;
	}

	private SendResult produce(final MessageExt originalMessageExt) throws Throwable {
		Throwable t = null;

		DefaultMQProducer producer = new DefaultMQProducer("rocketmq_console_producer_group");
		producer.setNamesrvAddr(configureInitializer.getNamesrvAddr());
		try {
			producer.start();

			String tagsTemp = originalMessageExt.getTags();
			if (StringUtils.isBlank(tagsTemp)) {
				tagsTemp = StringUtils.EMPTY;
			}

			String keysTemp = originalMessageExt.getKeys();
			if (StringUtils.isBlank(keysTemp)) {
				keysTemp = StringUtils.EMPTY;
			}

			String topic = originalMessageExt.getTopic();
			final Message newMsg = new Message(topic, tagsTemp, keysTemp, originalMessageExt.getBody());
			Map<String, String> properties = originalMessageExt.getProperties();
			for (Entry<String, String> entry : properties.entrySet()) {
				if (!MessageConst.STRING_HASH_SET.contains(entry.getKey())) {
					newMsg.putUserProperty(entry.getKey(), entry.getValue());
				}
			}

			return producer.send(newMsg);
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			producer.shutdown();
		}
		throw t;
	}

	public Table resend(String msgId) throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();

		try {
			defaultMQAdminExt.start();

			final MessageExt dlqMessageExt = defaultMQAdminExt.viewMessage(msgId);
			final String origin_message_id = dlqMessageExt.getProperty(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
			final MessageExt originalMessageExt = defaultMQAdminExt.viewMessage(origin_message_id);
			SendResult sendResult = produce(originalMessageExt);
			// logger.info(">>>>>>dlqmessage sendResult:" + msgId + ">>>>" + sendResult);

			String[] thead = new String[] { "send result" };
			Table table = new Table(thead, 1);
			Object[] tr = table.createTR();
			if (sendResult != null) {
				tr[0] = sendResult.toString();
			} else {
				tr[0] = "send result is null.";
			}
			table.insertTR(tr);

			return table;
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}
		throw t;
	}

	@CmdTrace(cmdClazz = TopicStatusSubCommand.class)
	public Table stats(String topicName) throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		ConsoleMQAdminExt consoleMQAdminExt = getConsoleMQAdminExt();

		try {
			consoleMQAdminExt.start();
			defaultMQAdminExt.start();

			TopicStatsTable topicStatsTable = defaultMQAdminExt.examineTopicStats(topicName);

			List<MessageQueue> mqList = new LinkedList<MessageQueue>();
			mqList.addAll(topicStatsTable.getOffsetTable().keySet());
			Collections.sort(mqList);

			String[] thead = new String[] { "#Broker Name", "#Queue ID", "#Min Offset", "#Max Offset", "#Last Updated" };
			Table table = new Table(thead, mqList.size());
			for (MessageQueue mq : mqList) {
				TopicOffset topicOffset = topicStatsTable.getOffsetTable().get(mq);

				String humanTimestamp = "";
				if (topicOffset.getLastUpdateTimestamp() > 0) {
					humanTimestamp = UtilAll.timeMillisToHumanString2(topicOffset.getLastUpdateTimestamp());
				}

				Object[] tr = table.createTR();
				tr[0] = UtilAll.frontStringAtLeast(mq.getBrokerName(), 32);
				tr[1] = str(mq.getQueueId());
				tr[2] = str(topicOffset.getMinOffset());
				tr[3] = str(topicOffset.getMaxOffset());
				tr[4] = humanTimestamp;

				table.insertTR(tr);
			}

			// defaultMQAdminExt.examineSubscriptionGroupConfig(addr, group)

			GroupList groplist = defaultMQAdminExt.queryTopicConsumeByWho(topicName);
			if (groplist != null && groplist.getGroupList().size() > 0) {
				table.addExtData("consumer group", StringUtils.join(groplist.getGroupList(), ","));

				HashSet<String> glist = groplist.getGroupList();
				for (String consumeGroup : glist) {
					Set<QueueTimeSpan> queueTimeSpanSet = consoleMQAdminExt.queryConsumeTimeSpan(topicName,
							consumeGroup);
					if (queueTimeSpanSet != null) {
						for (QueueTimeSpan queueTimeSpan : queueTimeSpanSet) {
							if (!((-1 == queueTimeSpan.getConsumeTimeStamp()) && (-1 == queueTimeSpan.getMaxTimeStamp()))) {
								String consumeProgress = consumeGroup + " consume progress in queue "
										+ queueTimeSpan.getMessageQueue().getQueueId() + " on broker "
										+ queueTimeSpan.getMessageQueue().getBrokerName();

								StringBuilder sb = new StringBuilder();
								if (-1 != queueTimeSpan.getConsumeTimeStamp()) {
									sb.append("consumeTimeStampStr:" + queueTimeSpan.getConsumeTimeStampStr());
								} else {
									sb.append("consumeTimeStampStr:" + queueTimeSpan.getMaxTimeStampStr());
								}
								table.addExtData(consumeProgress, sb.toString());
							}
						}
					}
				}
			} else {
				table.addExtData("consumer group", "NO_CONSUMER");
			}

			return table;
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
			shutdownConsoleMQAdminExt(consoleMQAdminExt);
		}
		throw t;
	}

	private List<MessageExt> queryMsgListByOffset(MessageQueue mq, long startOffset) {
		DefaultMQPullConsumer defaultMQPullConsumer = new DefaultMQPullConsumer(MixAll.TOOLS_CONSUMER_GROUP);
		defaultMQPullConsumer.setInstanceName(Long.toString(System.currentTimeMillis()));
		try {
			defaultMQPullConsumer.start();

			PullResult pullResult = defaultMQPullConsumer.pull(mq, "*", startOffset, DLQ_BROKER_MAX_NUMS + 1, 5000);
			if (pullResult != null) {
				// logger.info(mq.getBrokerName() + "," + mq.getTopic() + "," + mq.getQueueId()
				// + ">>>>>>>>>>>>>>>getMsgFoundList:" + pullResult.getMsgFoundList() + ","
				// + pullResult.getPullStatus());
				switch (pullResult.getPullStatus()) {
				case FOUND:
					List<MessageExt> msgList = pullResult.getMsgFoundList();
					// for (MessageExt messageExt : msgList) {
					// logger.info(">>>>>>>>>>>>>>>" + messageExt);
					// }
					return msgList;

				case NO_MATCHED_MSG:
				case NO_NEW_MSG:
				case OFFSET_ILLEGAL:
				default:
					break;
				}
			}
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
		} finally {
			defaultMQPullConsumer.shutdown();
		}

		return null;
	}

	final static UpdateTopicSubCommand updateTopicSubCommand = new UpdateTopicSubCommand();

	@SuppressWarnings("unchecked")
	public Collection<Option> getOptionsForUpdate() {
		Options options = new Options();

		Option opt = new Option("b", "brokerAddr", true, "broker address");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("c", "clusterName", true, "cluster name");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("t", "topic", true, "topic name");
		opt.setRequired(true);
		options.addOption(opt);

		opt = new Option("r", "readQueueNums", true, "read queue nums, default:8");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("w", "writeQueueNums", true, "write queue nums, default:8");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("p", "perm", true,
				"topic's permission (R:read[4] | W:write[2] | RW:read/write[6]), default:RW");
		opt.setRequired(false);
		options.addOption(opt);

		return options.getOptions();
	}

	@CmdTrace(cmdClazz = UpdateTopicSubCommand.class)
	public boolean update(String topic, String readQueueNums, String writeQueueNums, String perm, String brokerAddr,
			String clusterName, String order) throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();

		try {
			TopicConfig topicConfig = new TopicConfig();
			topicConfig.setReadQueueNums(8);
			topicConfig.setWriteQueueNums(8);
			topicConfig.setTopicName(topic);

			if (StringUtils.isNotBlank(readQueueNums)) {
				topicConfig.setReadQueueNums(Integer.parseInt(readQueueNums));
			}

			if (StringUtils.isNotBlank(writeQueueNums)) {
				topicConfig.setWriteQueueNums(Integer.parseInt(writeQueueNums));
			}

			if (StringUtils.isNotBlank(perm)) {
				topicConfig.setPerm(translatePerm(perm));
			}

			if (StringUtils.isNotBlank(order) && order.trim().equalsIgnoreCase("true")) {
				topicConfig.setOrder(true);
			}

			if (StringUtils.startsWithIgnoreCase(topic.trim(), "%DLQ%")
					|| StringUtils.startsWithIgnoreCase(topic.trim(), "%RETRY%")) {
				topicConfig.setReadQueueNums(1);
				topicConfig.setWriteQueueNums(1);
			}

			if (StringUtils.isNotBlank(brokerAddr)) {
				defaultMQAdminExt.start();
				defaultMQAdminExt.createAndUpdateTopicConfig(brokerAddr, topicConfig);
				return true;
			} else if (StringUtils.isNotBlank(clusterName)) {
				defaultMQAdminExt.start();

				Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
				for (String addr : masterSet) {
					defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);
				}
				return true;
			} else {
				throw new IllegalStateException("clusterName or brokerAddr can not be all blank");
			}
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}
		throw t;
	}

	final static DeleteTopicSubCommand deleteTopicSubCommand = new DeleteTopicSubCommand();

	public Collection<Option> getOptionsForDelete() {
		return getOptions(deleteTopicSubCommand);
	}

	@CmdTrace(cmdClazz = DeleteTopicSubCommand.class)
	public boolean delete(String topicName, String clusterName) throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt adminExt = getDefaultMQAdminExt();
		try {
			if (StringUtils.isNotBlank(clusterName)) {
				adminExt.start();
				Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(adminExt, clusterName);
				adminExt.deleteTopicInBroker(masterSet, topicName);
				Set<String> nameServerSet = null;
				if (StringUtils.isNotBlank(configureInitializer.getNamesrvAddr())) {
					String[] ns = configureInitializer.getNamesrvAddr().split(";");
					nameServerSet = new HashSet<String>(Arrays.asList(ns));
				}
				adminExt.deleteTopicInNameServer(nameServerSet, topicName);
				return true;
			} else {
				throw new IllegalStateException("clusterName is blank");
			}
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(adminExt);
		}
		throw t;
	}

	@CmdTrace(cmdClazz = TopicRouteSubCommand.class)
	public TopicRouteData route(String topicName) throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt adminExt = getDefaultMQAdminExt();
		try {
			adminExt.start();
			TopicRouteData topicRouteData = adminExt.examineTopicRouteInfo(topicName);
			return topicRouteData;
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(adminExt);
		}
		throw t;
	}

}
