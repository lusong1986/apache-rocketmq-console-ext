package com.alibaba.rocketmq.service;

import static com.alibaba.rocketmq.common.Tool.str;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.api.MessageTrack;
import org.apache.rocketmq.tools.command.message.QueryMsgByKeySubCommand;
import org.apache.rocketmq.tools.command.message.QueryMsgByOffsetSubCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.rocketmq.common.DecodedMongoMessage;
import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.utils.MessageUtils;
import com.alibaba.rocketmq.validate.CmdTrace;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

@Service
public class MessageService extends AbstractService implements InitializingBean {

	private static final Logger logger = LoggerFactory.getLogger(MessageService.class);

	private static final String UTF_8 = "utf-8";

	@Autowired
	private MongoService mongoService;

	@Autowired
	private ConnectionService connectionService;

	@Override
	public void afterPropertiesSet() throws Exception {

	}

	public Table msgGetStatistic() throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		try {
			defaultMQAdminExt.start();
			Table table = doGetList(defaultMQAdminExt);
			return table;
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}
		throw t;
	}

	public Table topicStatistic() throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		try {
			defaultMQAdminExt.start();
			Table table = doTopicStatisticList(defaultMQAdminExt);
			return table;
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}
		throw t;
	}

	private HashMap<String, TreeMap<String, Integer>> cache = new HashMap<String, TreeMap<String, Integer>>();

	private TreeMap<String/* topic */, Integer> getMsgCountByTopic(DBCollection collection) {
		final String collectionName = collection.getName();

		TreeMap<String, Integer> sorted_map = null;
		final String todayCollectionName = MessageUtils.getMsgCollectionName(new Date());
		if (!todayCollectionName.equals(collectionName)) {
			sorted_map = cache.get(collectionName);
			if (sorted_map != null) {
				return sorted_map;
			}
		}

		List<DBObject> pipeline = new ArrayList<DBObject>();

		/* Group操作 */
		DBObject groupFields = new BasicDBObject("_id", "$topic");
		groupFields.put("count", new BasicDBObject("$sum", 1));
		DBObject group = new BasicDBObject("$group", groupFields);
		pipeline.add(group);

		/* sort操作 */
		DBObject sortFields = new BasicDBObject("count", -1);
		DBObject sort = new BasicDBObject("$sort", sortFields);
		pipeline.add(sort);

		DBObject limit = new BasicDBObject("$limit", 10);
		pipeline.add(limit);

		AggregationOutput aggregationOutput = collection.aggregate(pipeline);
		Iterator<DBObject> results = aggregationOutput.results().iterator();
		// System.out.println(results);

		HashMap<String/* topic */, Integer/* msg count */> msgCountByTopic = new HashMap<String, Integer>();
		while (results.hasNext()) {
			DBObject baseobject = results.next(); // { "_id" : "eif-transcore_create_trans_response" , "count" : 150}
			msgCountByTopic.put(baseobject.get("_id") + "", Integer.valueOf(baseobject.get("count") + ""));
		}

		// 按value排序
		ValueComparator bvc = new ValueComparator(msgCountByTopic);
		sorted_map = new TreeMap<String, Integer>(bvc);
		sorted_map.putAll(msgCountByTopic);

		if (!todayCollectionName.equals(collectionName)) {
			cache.put(collectionName, sorted_map);
		}

		return sorted_map;
	}

	class ValueComparator implements Comparator<String> {

		Map<String, Integer> base;

		public ValueComparator(Map<String, Integer> base) {
			this.base = base;
		}

		public int compare(String a, String b) {
			if (base.get(a) > base.get(b)) {
				return -1;
			} else if (base.get(a) < base.get(b)) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	@SuppressWarnings("rawtypes")
	private Integer getTopicCount(final DBCollection collection) {
		Integer topicAllCountToday = 0;
		List topics = collection.distinct("topic");
		if (topics != null) {
			topicAllCountToday = topics.size();
		}
		return topicAllCountToday;
	}

	private Table doTopicStatisticList(DefaultMQAdminExt defaultMQAdminExt) throws Exception {
		final String today = MessageUtils.formatDate(new Date());
		final String yesterday = MessageUtils.formatDate(DateUtils.addDays(new Date(), -1));
		final String twodaysAgo = MessageUtils.formatDate(DateUtils.addDays(new Date(), -2));
		final String threeDaysAgo = MessageUtils.formatDate(DateUtils.addDays(new Date(), -3));

		// today
		final String todayCollectionName = MessageUtils.getMsgCollectionName(new Date());
		final DBCollection collectionToday = mongoService.getCollectionByName(todayCollectionName);
		Integer topicAllCountToday = getTopicCount(collectionToday);
		Map<String, Integer> msgCountByTopicToday = getMsgCountByTopic(collectionToday);

		final String yesCollectionName = MessageUtils.getMsgCollectionName(DateUtils.addDays(new Date(), -1));
		final DBCollection collectionYes = mongoService.getCollectionByName(yesCollectionName);
		Integer topicAllCountYes = getTopicCount(collectionYes);
		Map<String, Integer> msgCountByTopicYes = getMsgCountByTopic(collectionYes);

		final String twodaysagoCollectionName = MessageUtils.getMsgCollectionName(DateUtils.addDays(new Date(), -2));
		final DBCollection collection2daysago = mongoService.getCollectionByName(twodaysagoCollectionName);
		Integer topicAllCount2daysago = getTopicCount(collection2daysago);
		Map<String, Integer> msgCountByTopic2daysago = getMsgCountByTopic(collection2daysago);

		final String threedaysagoCollectionName = MessageUtils.getMsgCollectionName(DateUtils.addDays(new Date(), -3));
		final DBCollection collection3daysago = mongoService.getCollectionByName(threedaysagoCollectionName);
		Integer topicAllCount3daysago = getTopicCount(collection3daysago);
		Map<String, Integer> msgCountByTopic3daysago = getMsgCountByTopic(collection3daysago);

		Table statisticTable = new Table(new String[] { "统计日期", "topic总数" }, 4);

		addDayTopicStatisticTable(statisticTable, today, topicAllCountToday, msgCountByTopicToday);
		addDayTopicStatisticTable(statisticTable, yesterday, topicAllCountYes, msgCountByTopicYes);
		addDayTopicStatisticTable(statisticTable, twodaysAgo, topicAllCount2daysago, msgCountByTopic2daysago);
		addDayTopicStatisticTable(statisticTable, threeDaysAgo, topicAllCount3daysago, msgCountByTopic3daysago);

		return statisticTable;
	}

	private void addDayTopicStatisticTable(final Table statisticTable, final String day, final long topicAllCount,
			final Map<String, Integer> msgCountByTopicMap) {
		if (msgCountByTopicMap.isEmpty()) {
			return;
		}

		String[] instanceThead = new String[msgCountByTopicMap.size()];
		int index = 0;
		for (String topic : msgCountByTopicMap.keySet()) {
			instanceThead[index] = topic;
			index++;
		}

		Object[] countDataTR = statisticTable.createTR();
		countDataTR[0] = day;
		Table countDetailTable = new Table(new String[] { "总topic数量", "" + topicAllCount }, 1);
		countDataTR[1] = countDetailTable;
		statisticTable.insertTR(countDataTR);// A

		Object[] topicDetailTR = countDetailTable.createTR();
		topicDetailTR[0] = "top10发送topic";
		Table instanceTable = new Table(instanceThead, 1);
		topicDetailTR[1] = instanceTable;
		countDetailTable.insertTR(topicDetailTR);// B

		try {
			Object[] instanceTR = instanceTable.createTR();
			for (int i = 0; i < instanceThead.length; i++) {
				final Integer count = msgCountByTopicMap.get(instanceThead[i]);
				instanceTR[i] = count + "";
			}
			instanceTable.insertTR(instanceTR);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	private Table doGetList(DefaultMQAdminExt defaultMQAdminExt) throws Exception {
		final String today = MessageUtils.formatDate(new Date());
		final String yesterday = MessageUtils.formatDate(DateUtils.addDays(new Date(), -1));
		final String twodaysAgo = MessageUtils.formatDate(DateUtils.addDays(new Date(), -2));
		final String threeDaysAgo = MessageUtils.formatDate(DateUtils.addDays(new Date(), -3));

		Set<String> onlineConsumerGroupNameList = connectionService.getOnlineConsumerGroupList(defaultMQAdminExt);

		ClusterInfo clusterInfoSerializeWrapper = defaultMQAdminExt.examineBrokerClusterInfo();
		final Set<Entry<String, BrokerData>> brokerAddrTableEntrySet = clusterInfoSerializeWrapper.getBrokerAddrTable()
				.entrySet();

		// today
		Integer consumeMsgAllCountToday = 0;
		ConcurrentHashMap<String/* consumer group */, AtomicInteger/* consume count */> consumeCountMapByGroupToday = new ConcurrentHashMap<String, AtomicInteger>(
				onlineConsumerGroupNameList.size() * 2);
		for (Entry<String, BrokerData> dataEntry : brokerAddrTableEntrySet) {
			KVTable kvTable = defaultMQAdminExt.fetchBrokerRuntimeStats(dataEntry.getValue().selectBrokerAddr());
			String ret = kvTable.getTable().get(MessageUtils.MSG_GET_CONSUMER_GROUP_TIMES_TODAY_NOW);

			consumeMsgAllCountToday += getConsumeAllCountToday(ret, consumeCountMapByGroupToday);
		}

		// yesterday
		ConcurrentHashMap<String/* consumer group */, AtomicInteger/* consume count */> consumeCountMapByGroupYesterday = new ConcurrentHashMap<String, AtomicInteger>(
				onlineConsumerGroupNameList.size() * 2);
		Integer consumeMsgAllCountYesterday = getConsumeMsgCountFromHisTable(yesterday, onlineConsumerGroupNameList,
				brokerAddrTableEntrySet, consumeCountMapByGroupYesterday);

		ConcurrentHashMap<String, AtomicInteger> consumeCountMapByGroupTwodaysAgo = new ConcurrentHashMap<String, AtomicInteger>(
				onlineConsumerGroupNameList.size() * 2);
		Integer consumeMsgAllCountTwodaysAgo = getConsumeMsgCountFromHisTable(twodaysAgo, onlineConsumerGroupNameList,
				brokerAddrTableEntrySet, consumeCountMapByGroupTwodaysAgo);

		ConcurrentHashMap<String, AtomicInteger> consumeCountMapByGroupThreedaysAgo = new ConcurrentHashMap<String, AtomicInteger>(
				onlineConsumerGroupNameList.size() * 2);
		Integer consumeMsgAllCountThreedaysAgo = getConsumeMsgCountFromHisTable(threeDaysAgo,
				onlineConsumerGroupNameList, brokerAddrTableEntrySet, consumeCountMapByGroupThreedaysAgo);

		Table statisticTable = new Table(new String[] { "统计日期", "消息的消费量" }, 4);

		addDayGetStatisticTable(statisticTable, today, consumeMsgAllCountToday, new TreeMap<String, AtomicInteger>(
				consumeCountMapByGroupToday));
		addDayGetStatisticTable(statisticTable, yesterday, consumeMsgAllCountYesterday,
				new TreeMap<String, AtomicInteger>(consumeCountMapByGroupYesterday));
		addDayGetStatisticTable(statisticTable, twodaysAgo, consumeMsgAllCountTwodaysAgo,
				new TreeMap<String, AtomicInteger>(consumeCountMapByGroupTwodaysAgo));
		addDayGetStatisticTable(statisticTable, threeDaysAgo, consumeMsgAllCountThreedaysAgo,
				new TreeMap<String, AtomicInteger>(consumeCountMapByGroupThreedaysAgo));

		return statisticTable;
	}

	private Integer getConsumeMsgCountFromHisTable(final String day, final Set<String> onlineConsumerGroupNameList,
			final Set<Entry<String, BrokerData>> brokerAddrTableEntrySet,
			final ConcurrentHashMap<String, AtomicInteger> consumeCountMapByGroup) {
		Integer consumeMsgAllCount = 0;
		for (Entry<String, BrokerData> dataEntry : brokerAddrTableEntrySet) {
			DBObject query = new BasicDBObject();
			query.put(MessageUtils.DATA_COLLECTION_DATE, day);
			query.put(MessageUtils.BROKER_NAME, dataEntry.getValue().getBrokerName());
			DBObject quertConsumeResult = mongoService.getCollectionByName(MessageUtils.MQ_GET_HIS_TABLE)
					.findOne(query);
			if (quertConsumeResult != null) {
				consumeMsgAllCount += Integer.parseInt(quertConsumeResult.get(MessageUtils.ALL_GET_COUNT) + "");

				for (String consumerGroup : onlineConsumerGroupNameList) {
					Object ret = quertConsumeResult.get(consumerGroup);
					if (ret != null) {
						int consumerAllCountByGroup = getConsumeAllCountByGroup(ret);

						AtomicInteger newCounter = new AtomicInteger(0);
						AtomicInteger consumerCounter = consumeCountMapByGroup.putIfAbsent(consumerGroup, newCounter);
						if (null == consumerCounter) {
							newCounter.addAndGet(consumerAllCountByGroup);
						} else {
							consumerCounter.addAndGet(consumerAllCountByGroup);
						}
					}
				}
			}
		}
		return consumeMsgAllCount;
	}

	private int getConsumeAllCountToday(String ret, ConcurrentHashMap<String, AtomicInteger> consumeCountMapByGroupToday) {
		Integer consumerAllCount = 0;
		if (StringUtils.isNotBlank(ret)) {
			JSONObject jsonObject = JSON.parseObject(ret);
			Set<String> consumerGroups = jsonObject.keySet();
			for (String consumerGroup : consumerGroups) {
				int consumerAllCountByGroup = getConsumeAllCountByGroup(jsonObject.get(consumerGroup));
				consumerAllCount += consumerAllCountByGroup;

				AtomicInteger newCounter = new AtomicInteger(0);
				AtomicInteger consumerCounter = consumeCountMapByGroupToday.putIfAbsent(consumerGroup, newCounter);
				if (null == consumerCounter) {
					newCounter.addAndGet(consumerAllCountByGroup);
				} else {
					consumerCounter.addAndGet(consumerAllCountByGroup);
				}
			}
		}
		return consumerAllCount;
	}

	private int getConsumeAllCountByGroup(Object topicJson) {
		JSONObject json = JSON.parseObject(topicJson + "");
		int consumerAllCountByGroup = 0;
		for (Entry<String, Object> topicEntry : json.entrySet()) {
			Integer consumeMsgCount = Integer.parseInt(topicEntry.getValue() + "");
			consumerAllCountByGroup += consumeMsgCount;
		}
		return consumerAllCountByGroup;
	}

	private void addDayGetStatisticTable(final Table statisticTable, final String day, final long consumeMsgAllCount,
			final Map<String, AtomicInteger> consumeCountMapByGroup) {
		if (consumeCountMapByGroup.isEmpty()) {
			return;
		}

		String[] instanceThead = new String[consumeCountMapByGroup.size()];
		int index = 0;
		for (String consumerGroup : consumeCountMapByGroup.keySet()) {
			instanceThead[index] = consumerGroup;
			index++;
		}

		Object[] countDataTR = statisticTable.createTR();
		countDataTR[0] = day;
		Table countDetailTable = new Table(new String[] { "总消费量", "" + consumeMsgAllCount }, 1);
		countDataTR[1] = countDetailTable;
		statisticTable.insertTR(countDataTR);// A

		Object[] consumerGroupDetailTR = countDetailTable.createTR();
		consumerGroupDetailTR[0] = "按照group计算";
		Table instanceTable = new Table(instanceThead, 1);
		consumerGroupDetailTR[1] = instanceTable;
		countDetailTable.insertTR(consumerGroupDetailTR);// B

		try {
			Object[] instanceTR = instanceTable.createTR();
			for (int i = 0; i < instanceThead.length; i++) {
				instanceTR[i] = consumeCountMapByGroup.get(instanceThead[i]) + "";
			}
			instanceTable.insertTR(instanceTR);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public Table msgPutStatistic() throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		try {
			defaultMQAdminExt.start();
			Table table = doPutList(defaultMQAdminExt);
			return table;
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}
		throw t;
	}

	public Table dlqStatistic() throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		try {
			defaultMQAdminExt.start();
			Table table = doDLQList(defaultMQAdminExt);
			return table;
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}
		throw t;
	}

	private Table doDLQList(DefaultMQAdminExt defaultMQAdminExt) throws Exception {
		final String todayCollection = MessageUtils.getMsgCollectionName(new Date());
		long todayAllCount = 0;
		final Map<String, Long> todayCountMap = new TreeMap<String, Long>();

		Table statisticTable = new Table(new String[] { "统计日期", "DLQ总量" }, 10);

		final TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
		for (String topic : topicList.getTopicList()) {
			if (!topic.startsWith("%DLQ%")) {
				continue;
			}

			final long todayCount = mongoService.countByCollectionTopic(todayCollection, topic);
			if (todayCount > 0) {
				todayAllCount += todayCount;
				todayCountMap.put(topic, todayCount);
			}
		}

		if (todayAllCount > 0) {
			addDayDLQStatisticTable(statisticTable, todayCollection, todayAllCount, todayCountMap);
		}

		int counter = 0;
		for (int d = 1; d < 100; d++) {
			final String otherdayCollection = MessageUtils.getMsgCollectionName(DateUtils.addDays(new Date(), -1 * d));
			final Map<String, Long> otherdayCountMap = new TreeMap<String, Long>();

			long otherdayAllCount = 0;
			for (String topic : topicList.getTopicList()) {
				if (!topic.startsWith("%DLQ%")) {
					continue;
				}

				final long otherdayCount = getTopicMsgCountFromHisTable(otherdayCollection, topic);
				if (otherdayCount > 0) {
					otherdayAllCount += otherdayCount;
					otherdayCountMap.put(topic, otherdayCount);
				}
			}

			if (otherdayAllCount > 0) {
				addDayDLQStatisticTable(statisticTable, otherdayCollection, otherdayAllCount, otherdayCountMap);
				if (++counter >= 10) {
					break;
				}
			}
		}

		return statisticTable;
	}

	private void addDayDLQStatisticTable(final Table statisticTable, final String collectionName,
			final long dayAllCount, final Map<String, Long> dayCountMap) {
		if (dayCountMap.isEmpty()) {
			return;
		}

		String[] instanceThead = new String[dayCountMap.size()];
		int index = 0;
		for (String topic : dayCountMap.keySet()) {
			instanceThead[index] = topic;
			index++;
		}

		Object[] countDataTR = statisticTable.createTR();
		countDataTR[0] = collectionName;
		Table countDetailTable = new Table(new String[] { "DLQ总量", "" + dayAllCount }, 1);
		countDataTR[1] = countDetailTable;
		statisticTable.insertTR(countDataTR);// A

		Object[] topicDetailTR = countDetailTable.createTR();
		topicDetailTR[0] = "按照topic计算";
		Table instanceTable = new Table(instanceThead, 1);
		topicDetailTR[1] = instanceTable;
		countDetailTable.insertTR(topicDetailTR);// B

		try {
			Object[] instanceTR = instanceTable.createTR();
			for (int i = 0; i < instanceThead.length; i++) {
				instanceTR[i] = dayCountMap.get(instanceThead[i]) + "";
			}
			instanceTable.insertTR(instanceTR);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	private Table doPutList(DefaultMQAdminExt defaultMQAdminExt) throws Exception {
		final String todayCollection = MessageUtils.getMsgCollectionName(new Date());
		final String yesterdayCollection = MessageUtils.getMsgCollectionName(DateUtils.addDays(new Date(), -1));
		final String twodaysAgoCollection = MessageUtils.getMsgCollectionName(DateUtils.addDays(new Date(), -2));
		final String threeDaysAgoCollection = MessageUtils.getMsgCollectionName(DateUtils.addDays(new Date(), -3));

		final long todayAllCount = mongoService.countByCollection(todayCollection);
		final long yesterdayAllCount = getAllMsgCountFromHisTable(yesterdayCollection);
		final long twoDaysAgoAllCount = getAllMsgCountFromHisTable(twodaysAgoCollection);
		final long threeDaysAgoAllCount = getAllMsgCountFromHisTable(threeDaysAgoCollection);

		final Map<String, Long> todayCountMap = new TreeMap<String, Long>();
		final Map<String, Long> yesterdayCountMap = new TreeMap<String, Long>();
		final Map<String, Long> twodaysAgoCountMap = new TreeMap<String, Long>();
		final Map<String, Long> threedaysAgoCountMap = new TreeMap<String, Long>();

		TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
		int row = topicList.getTopicList().size();
		if (row > 0) {
			for (String topic : topicList.getTopicList()) {
				long todayCount = mongoService.countByCollectionTopic(todayCollection, topic);
				if (todayCount > 0) {
					todayCountMap.put(topic, todayCount);
				}

				long yesterdayCount = getTopicMsgCountFromHisTable(yesterdayCollection, topic);
				if (yesterdayCount > 0) {
					yesterdayCountMap.put(topic, yesterdayCount);
				}

				long twoDaysAgoCount = getTopicMsgCountFromHisTable(twodaysAgoCollection, topic);
				if (twoDaysAgoCount > 0) {
					twodaysAgoCountMap.put(topic, twoDaysAgoCount);
				}

				long threeDaysAgoCount = getTopicMsgCountFromHisTable(threeDaysAgoCollection, topic);
				if (threeDaysAgoCount > 0) {
					threedaysAgoCountMap.put(topic, threeDaysAgoCount);
				}

			}
		}

		Table statisticTable = new Table(new String[] { "统计日期", "消息的发送量" }, 4);

		addDayPutStatisticTable(statisticTable, todayCollection, todayAllCount, todayCountMap);
		addDayPutStatisticTable(statisticTable, yesterdayCollection, yesterdayAllCount, yesterdayCountMap);
		addDayPutStatisticTable(statisticTable, twodaysAgoCollection, twoDaysAgoAllCount, twodaysAgoCountMap);
		addDayPutStatisticTable(statisticTable, threeDaysAgoCollection, threeDaysAgoAllCount, threedaysAgoCountMap);

		return statisticTable;
	}

	private void addDayPutStatisticTable(final Table statisticTable, final String collectionName,
			final long dayAllCount, final Map<String, Long> dayCountMap) {
		if (dayCountMap.isEmpty()) {
			return;
		}

		String[] instanceThead = new String[dayCountMap.size()];
		int index = 0;
		for (String topic : dayCountMap.keySet()) {
			instanceThead[index] = topic;
			index++;
		}

		Object[] countDataTR = statisticTable.createTR();
		countDataTR[0] = collectionName;
		Table countDetailTable = new Table(new String[] { "总发送量", "" + dayAllCount }, 1);
		countDataTR[1] = countDetailTable;
		statisticTable.insertTR(countDataTR);// A

		Object[] topicDetailTR = countDetailTable.createTR();
		topicDetailTR[0] = "按照topic计算";
		Table instanceTable = new Table(instanceThead, 1);
		topicDetailTR[1] = instanceTable;
		countDetailTable.insertTR(topicDetailTR);// B

		try {
			Object[] instanceTR = instanceTable.createTR();
			for (int i = 0; i < instanceThead.length; i++) {
				instanceTR[i] = dayCountMap.get(instanceThead[i]) + "";
			}
			instanceTable.insertTR(instanceTR);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	private long getAllMsgCountFromHisTable(final String collection) {
		DBObject query = new BasicDBObject();
		query.put(MessageUtils.DATA_COLLECTION_NAME, collection);
		DBObject ret = mongoService.getCollectionByName(MessageUtils.MQ_PUT_HIS_TABLE).findOne(query);

		long allCount = 0;
		if (ret != null) {
			allCount = Long.parseLong("" + ret.get(MessageUtils.ALL_PUT_COUNT));
		}
		return allCount;
	}

	private long getTopicMsgCountFromHisTable(final String collection, final String topic) {
		DBObject query = new BasicDBObject();
		query.put(MessageUtils.DATA_COLLECTION_NAME, collection);
		DBObject ret = mongoService.getCollectionByName(MessageUtils.MQ_PUT_HIS_TABLE).findOne(query);

		long allCount = 0;
		if (ret != null && ret.get(topic) != null) {
			allCount = Long.parseLong("" + ret.get(topic));
		}
		return allCount;
	}

	public Table queryMsgById(String msgId) throws Throwable {
		Throwable t = null;
		Map<String, String> map = new LinkedHashMap<String, String>();
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		try {
			defaultMQAdminExt.start();
			MessageExt msg = defaultMQAdminExt.viewMessage(msgId);

			// DefaultMQAdminExtImpl.consumed() 方法这段在rancher环境有问题: storehost可能和broker addr不一致
			// if(addr.equals(RemotingUtil.socketAddress2String(msg.getStoreHost())))
			List<MessageTrack> messageTracks = defaultMQAdminExt.messageTrackDetail(msg);

			String bodyTmpFilePath = createBodyFile(msg);
			map.put("Topic", msg.getTopic());
			map.put("Tags", "[" + msg.getTags() + "]");
			map.put("Keys", "[" + msg.getKeys() + "]");
			map.put("Queue ID", String.valueOf(msg.getQueueId()));
			map.put("Queue Offset:", String.valueOf(msg.getQueueOffset()));
			map.put("CommitLog Offset:", String.valueOf(msg.getCommitLogOffset()));
			map.put("Born Timestamp:", UtilAll.timeMillisToHumanString2(msg.getBornTimestamp()));
			map.put("Store Timestamp:", UtilAll.timeMillisToHumanString2(msg.getStoreTimestamp()));
			map.put("Born Host:", RemotingHelper.parseSocketAddressAddr(msg.getBornHost()));
			map.put("Born Host Name:", msg.getBornHostNameString());
			map.put("Store Host:", RemotingHelper.parseSocketAddressAddr(msg.getStoreHost()));
			map.put("System Flag:", String.valueOf(msg.getSysFlag()));
			map.put("ReconsumeTimes", msg.getReconsumeTimes() + "");
			map.put("Delay", msg.getDelayTimeLevel() + "");
			map.put("StoreSize", msg.getStoreSize() + "");
			map.put("Properties:", msg.getProperties() != null ? msg.getProperties().toString() : "");
			map.put("Message Content:", MessageUtils.conver2String(msg));
			map.put("Message Body Path:", bodyTmpFilePath);
			map.put("Message Track:", messageTracks.toString());
			return Table.Map2VTable(map);
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);

		}
		throw t;
	}

	public JSONObject queryMsgByIdInJSON(String msgId) {
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		try {
			final JSONObject json = new JSONObject();
			defaultMQAdminExt.start();
			final MessageExt msg = defaultMQAdminExt.viewMessage(msgId);

			List<MessageTrack> messageTracks = defaultMQAdminExt.messageTrackDetail(msg);

			String bodyTmpFilePath = createBodyFile(msg);
			json.put("Topic", msg.getTopic());
			json.put("Tags", "[" + msg.getTags() + "]");
			json.put("Keys", "[" + msg.getKeys() + "]");
			json.put("QueueID", String.valueOf(msg.getQueueId()));
			json.put("QueueOffset", String.valueOf(msg.getQueueOffset()));
			json.put("CommitLogOffset", String.valueOf(msg.getCommitLogOffset()));
			json.put("BornTimestamp", UtilAll.timeMillisToHumanString2(msg.getBornTimestamp()));
			json.put("StoreTimestamp", UtilAll.timeMillisToHumanString2(msg.getStoreTimestamp()));
			json.put("BornHost", RemotingHelper.parseSocketAddressAddr(msg.getBornHost()));
			json.put("StoreHost", RemotingHelper.parseSocketAddressAddr(msg.getStoreHost()));
			json.put("SystemFlag", String.valueOf(msg.getSysFlag()));
			json.put("Properties", msg.getProperties() != null ? msg.getProperties().toString() : "");
			json.put("MessageContent", MessageUtils.conver2String(msg));
			json.put("MessageBodyPath", bodyTmpFilePath);
			json.put("MessageTrack", messageTracks.toString());
			return json;
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}
		return null;
	}

	private String createBodyFile(MessageExt msg) throws IOException {
		DataOutputStream dos = null;

		try {
			String bodyTmpFilePath = "/tmp/rocketmq/msgbodys";
			File file = new File(bodyTmpFilePath);
			if (!file.exists()) {
				file.mkdirs();
			}
			bodyTmpFilePath = bodyTmpFilePath + "/" + msg.getMsgId();
			dos = new DataOutputStream(new FileOutputStream(bodyTmpFilePath));
			dos.write(msg.getBody());
			return bodyTmpFilePath;
		} finally {
			if (dos != null)
				dos.close();
		}
	}

	static final QueryMsgByKeySubCommand queryMsgByKeySubCommand = new QueryMsgByKeySubCommand();

	public Collection<Option> getOptionsForQueryMsgByKey() {
		return getOptions(queryMsgByKeySubCommand);
	}

	@CmdTrace(cmdClazz = QueryMsgByKeySubCommand.class)
	public Table queryMsgByKey(String topicName, String msgKey, String fallbackHours) throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		try {
			defaultMQAdminExt.start();
			long h = 0;
			if (StringUtils.isNotBlank(fallbackHours)) {
				h = Long.parseLong(fallbackHours);
			}
			long end = System.currentTimeMillis() - (h * 60 * 60 * 1000);
			long begin = end - (48 * 60 * 60 * 1000);
			org.apache.rocketmq.client.QueryResult queryResult = defaultMQAdminExt.queryMessage(topicName, msgKey, 32,
					begin, end);

			String[] thead = new String[] { "#Message ID", "#QID", "#Offset" };
			int row = queryResult.getMessageList().size();
			Table table = new Table(thead, row);

			for (MessageExt msg : queryResult.getMessageList()) {
				String[] data = new String[] { msg.getMsgId(), String.valueOf(msg.getQueueId()),
						String.valueOf(msg.getQueueOffset()) };
				table.insertTR(data);
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

	static final QueryMsgByOffsetSubCommand queryMsgByOffsetSubCommand = new QueryMsgByOffsetSubCommand();

	public Collection<Option> getOptionsForQueryMsgByOffset() {
		return getOptions(queryMsgByOffsetSubCommand);
	}

	@CmdTrace(cmdClazz = QueryMsgByOffsetSubCommand.class)
	public Table queryMsgByOffset(String topicName, String brokerName, String queueId, String offset) throws Throwable {
		Throwable t = null;
		DefaultMQPullConsumer defaultMQPullConsumer = new DefaultMQPullConsumer(MixAll.TOOLS_CONSUMER_GROUP);

		defaultMQPullConsumer.setInstanceName(Long.toString(System.currentTimeMillis()));

		try {
			MessageQueue mq = new MessageQueue();
			mq.setTopic(topicName);
			mq.setBrokerName(brokerName);
			mq.setQueueId(Integer.parseInt(queueId));

			defaultMQPullConsumer.start();

			PullResult pullResult = defaultMQPullConsumer.pull(mq, "*", Long.parseLong(offset), 1);
			if (pullResult != null) {
				switch (pullResult.getPullStatus()) {
				case FOUND:
					Table table = queryMsgById(pullResult.getMsgFoundList().get(0).getMsgId());
					return table;
				case NO_MATCHED_MSG:
				case NO_NEW_MSG:
				case OFFSET_ILLEGAL:
				default:
					break;
				}
			} else {
				throw new IllegalStateException("pullResult is null");
			}
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			defaultMQPullConsumer.shutdown();
		}
		throw t;
	}

	@SuppressWarnings("unchecked")
	public Collection<Option> getOptionsForProduce() {
		Options options = new Options();

		Option opt = new Option("t", "topic", true, "message's topic name");
		opt.setRequired(true);
		options.addOption(opt);

		opt = new Option("b", "body", true, "message's body");
		opt.setRequired(true);
		options.addOption(opt);

		opt = new Option("a", "tags", true, "message's tags");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("k", "keys", true, "message's keys");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("p", "properties", true, "message's properties (json format)");
		opt.setRequired(false);
		options.addOption(opt);

		return options.getOptions();
	}

	@SuppressWarnings("unchecked")
	public Collection<Option> getOptionsForConsume() {
		Options options = new Options();

		Option opt = new Option("t", "topic", true, "topic name");
		opt.setRequired(true);
		options.addOption(opt);

		opt = new Option("s", "expression", true, "subscription expression");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("c", "consume", true, "是否消费（true | false），默认：true，重试：false");
		opt.setRequired(false);
		options.addOption(opt);

		return options.getOptions();
	}

	public SendResult produce(String topic, String body, String tags, String keys, String properties) throws Throwable {
		Throwable t = null;

		DefaultMQProducer producer = new DefaultMQProducer("rocketmq_console_producer_group");
		producer.setNamesrvAddr(configureInitializer.getNamesrvAddr());
		try {
			producer.start();

			String tagsTemp = tags;
			if (StringUtils.isBlank(tagsTemp)) {
				tagsTemp = StringUtils.EMPTY;
			}

			String keysTemp = keys;
			if (StringUtils.isBlank(keysTemp)) {
				keysTemp = StringUtils.EMPTY;
			}

			final Message msg = new Message(topic, tagsTemp, keysTemp, body.getBytes());

			if (StringUtils.isNotBlank(properties)) {
				JSONObject propsJson = JSON.parseObject(properties);
				for (Entry<String, Object> entry : propsJson.entrySet()) {
					msg.putUserProperty(entry.getKey(), entry.getValue() + "");
				}
			}

			return producer.send(msg);
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			producer.shutdown();
		}
		throw t;
	}

	public List<MessageExt> consume(String topic, String expression, final boolean consume) throws Throwable {
		Throwable t = null;

		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("rocketmq_console_consumer_group");
		consumer.setNamesrvAddr(configureInitializer.getNamesrvAddr());
		String expressionTemp = expression;
		if (StringUtils.isBlank(expressionTemp)) {
			expressionTemp = StringUtils.EMPTY;
		}
		consumer.subscribe(topic, expressionTemp);
		final List<MessageExt> messages = new ArrayList<MessageExt>();
		consumer.registerMessageListener(new MessageListenerConcurrently() {
			@Override
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				if (CollectionUtils.isNotEmpty(msgs)) {
					messages.addAll(msgs);
				}
				if (consume) {
					return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
				} else {
					return ConsumeConcurrentlyStatus.RECONSUME_LATER;
				}
			}
		});
		try {
			consumer.start();
			Thread.sleep(10000);
			return messages;
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			consumer.shutdown();
		}
		throw t;
	}

	private static final int MAX_MESSAGES_PER_PAGE = 10;

	private static final int MAX_FAILED_MESSAGES_PER_PAGE = 5;

	public Table unHandlePrepareMsgYesterday(int pageNum) throws Throwable {
		Throwable t = null;
		try {
			final String yesterdayCollection = MessageUtils.getMsgCollectionName(DateUtils.addDays(new Date(), -1));
			return unhandlePrepareMsg(pageNum, yesterdayCollection);
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
		}
		throw t;
	}

	public Table unHandlePrepareMsgToday(int pageNum) throws Throwable {
		Throwable t = null;
		try {
			final String todayCollection = MessageUtils.getMsgCollectionName(new Date());
			return unhandlePrepareMsg(pageNum, todayCollection);
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
		}
		throw t;
	}

	private Table unhandlePrepareMsg(int pageNum, final String todayCollection) {
		DBCollection collectionNow = mongoService.getCollectionByName(todayCollection);

		final List<DecodedMongoMessage> unhandledMsgs = new LinkedList<DecodedMongoMessage>();
		DBObject query = new BasicDBObject();
		query.put("preparedTransactionOffset", 0);
		query.put("sysFlag", 4);
		query.put("tranStatus", 0);
		final DBCursor cursor = collectionNow.find(query);

		int counterIndex = 0;
		long startOffset = MAX_MESSAGES_PER_PAGE * (pageNum - 1);
		if (startOffset < 0) {
			startOffset = 0;
		}

		while (cursor.hasNext()) {
			DBObject dbObject = cursor.next();

			if (counterIndex++ < startOffset) {
				continue;
			}

			DecodedMongoMessage decodedMongoMsg = new DecodedMongoMessage();
			MessageUtils.dbObject2Bean(dbObject, decodedMongoMsg);
			unhandledMsgs.add(decodedMongoMsg);

			if (unhandledMsgs.size() >= MAX_MESSAGES_PER_PAGE) {
				break;
			}
		}

		String[] thead = new String[] { "messageId", "broker", "queueId", "_catRootMessageId", "topic", "Store Time",
				"body" };
		Table table = new Table(thead, unhandledMsgs.size());
		for (final DecodedMongoMessage decodedMongoMessage : unhandledMsgs) {
			Object[] tr = table.createTR();
			tr[0] = decodedMongoMessage.getMsgId();
			tr[1] = decodedMongoMessage.getStoreHost();
			tr[2] = str(decodedMongoMessage.getQueueId());
			tr[3] = decodedMongoMessage.get_catRootMessageId();
			tr[4] = decodedMongoMessage.getTopic();
			tr[5] = decodedMongoMessage.getStoreTime();
			tr[6] = decodedMongoMessage.getContent();
			table.insertTR(tr);
		}

		return table;
	}

	private Table sendFailedMsgs(final String mongoCollectionName, int pNum) {
		final DBCollection collectionName = mongoService.getCollectionByName(mongoCollectionName);

		final List<DecodedMongoMessage> failedMsgs = new LinkedList<DecodedMongoMessage>();
		DBObject query = new BasicDBObject();
		query.put("topic", "FAILED_SEND_MESSAGE");
		final DBCursor cursor = collectionName.find(query);

		int counterIndex = 0;
		long startOffset = MAX_FAILED_MESSAGES_PER_PAGE * (pNum - 1);
		if (startOffset < 0) {
			startOffset = 0;
		}

		while (cursor.hasNext()) {
			DBObject dbObject = cursor.next();

			if (counterIndex++ < startOffset) {
				continue;
			}

			DecodedMongoMessage decodedMongoMsg = new DecodedMongoMessage();
			MessageUtils.dbObject2Bean(dbObject, decodedMongoMsg);
			failedMsgs.add(decodedMongoMsg);
			// System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>failed msg decodedMongoMsg:"
			// + ToStringBuilder.reflectionToString(decodedMongoMsg));

			if (failedMsgs.size() >= MAX_FAILED_MESSAGES_PER_PAGE) {
				break;
			}
		}

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String[] thead = new String[] { "failed messageId", "broker", "keys", "_catRootMessageId", "topic",
				"happen Time", "body", "error reason" };
		Table table = new Table(thead, failedMsgs.size());
		for (final DecodedMongoMessage decodedMongoMessage : failedMsgs) {

			try {
				final JSONObject failedMsgObject = JSON.parseObject(decodedMongoMessage.getContent());
				final String errStackTrace = "" + failedMsgObject.get("errStackTrace");
				String errMsg = "" + failedMsgObject.get("errMsg") + "<br>[stack]:"
						+ (errStackTrace.length() > 1400 ? errStackTrace.substring(0, 1400) : errStackTrace);

				Date happenTime = new Date(Long.valueOf("" + failedMsgObject.get("happenTime")));
				final JSONObject originalMsgJson = (JSONObject) failedMsgObject.get("msg");

				final String messageObject = "" + failedMsgObject.get("messageObject");

				String topic = "" + originalMsgJson.get("topic");
				String keys = "" + originalMsgJson.get("keys");

				final JSONObject propsJson = (JSONObject) originalMsgJson.get("properties");
				String _catRootMessageId = "" + propsJson.get("_catRootMessageId");

				Object[] tr = table.createTR();
				tr[0] = decodedMongoMessage.getMsgId();
				tr[1] = decodedMongoMessage.getStoreHost();
				tr[2] = keys;
				tr[3] = _catRootMessageId;
				tr[4] = topic;
				tr[5] = sdf.format(happenTime);
				tr[6] = messageObject;
				tr[7] = errMsg;
				table.insertTR(tr);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("expand decodedMongoMessage error:" + currentStackTrace(e));
			}
		}

		return table;
	}

	private static String currentStackTrace(Exception e) {
		StringBuilder sb = new StringBuilder();
		StackTraceElement[] stackTrace = e.getStackTrace();
		for (StackTraceElement ste : stackTrace) {
			sb.append("\n\t");
			sb.append(ste.toString());
		}

		return sb.toString();
	}

	/**
	 * 发送失败的消息列表
	 * 
	 * @param dayBeforeToday 0 表示今天，1表示昨天
	 * @param pNum
	 * @return
	 * @throws Throwable
	 */
	public Table sendFailedMsgs(int dayBeforeToday, int pNum) throws Throwable {
		Throwable t = null;
		try {
			final String mongoCollectionName = MessageUtils.getMsgCollectionName(DateUtils.addDays(new Date(), -1
					* dayBeforeToday));

			return sendFailedMsgs(mongoCollectionName, pNum);
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
		}
		throw t;
	}

	/**
	 * 重发失败的消息
	 * 
	 * @param msgId failed message
	 * @return
	 * @throws Throwable
	 */
	public Table resendMsg(String msgId) throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();

		DefaultMQProducer defaultProducer = null;
		try {
			defaultMQAdminExt.start();
			defaultProducer = this.getDefaultProducer();

			final MessageExt failedMessageExt = defaultMQAdminExt.viewMessage(msgId);
			String failedMsgBody = new String(failedMessageExt.getBody(), UTF_8);

			final JSONObject failedMsgObject = JSON.parseObject(failedMsgBody);
			System.out.println(">>>>>>>>>>>failed msg:" + failedMsgObject);

			final String messageJsonObject = "" + failedMsgObject.get("messageObject");

			final JSONObject originalMsgJson = (JSONObject) failedMsgObject.get("msg");
			String topic = "" + originalMsgJson.get("topic");
			String keys = "" + originalMsgJson.get("keys");
			String tags = "" + originalMsgJson.get("tags");

			// final JSONObject propsJson = (JSONObject) originalMsgJson.get("properties");

			SendResult result = defaultProducer.send(new Message(topic, tags, keys, messageJsonObject.getBytes(UTF_8)));

			String[] thead = new String[] { "send result" };
			Table table = new Table(thead, 1);
			Object[] tr = table.createTR();
			tr[0] = result.toString();
			table.insertTR(tr);

			return table;
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

	public Table commitTran(String msgId) throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();

		try {
			defaultMQAdminExt.start();

			final MessageExt prepareMessageExt = defaultMQAdminExt.viewMessage(msgId);
			commitTransaction(prepareMessageExt);
			logger.info("start to commit transaction:" + prepareMessageExt.toString());

			String[] thead = new String[] { "commit result" };
			Table table = new Table(thead, 1);
			Object[] tr = table.createTR();
			tr[0] = "have commit transaction.";
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

	public Table rollbackTran(String msgId) throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();

		try {
			defaultMQAdminExt.start();

			final MessageExt prepareMessageExt = defaultMQAdminExt.viewMessage(msgId);
			rollbackTransaction(prepareMessageExt, null);
			logger.info("start to rollback transaction:" + prepareMessageExt.toString());

			String[] thead = new String[] { "rollback result" };
			Table table = new Table(thead, 1);
			Object[] tr = table.createTR();
			tr[0] = "have rollback transaction.";
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

	/**
	 * oneway方式，根据本地事务执行结果确定事务消息正常发出
	 * 
	 * @param prepareMessageExt
	 * @return
	 * @throws MQTransactionException
	 */
	private void commitTransaction(final MessageExt prepareMessageExt) throws Throwable {
		confirmTransaction(prepareMessageExt, LocalTransactionState.COMMIT_MESSAGE, null);
	}

	/**
	 * 
	 * oneway方式，根据本地事务执行结果回滚事务消息
	 * 
	 * @param prepareMessageExt
	 * @param localExecuteException
	 * @return
	 * @throws MQTransactionException
	 */
	private void rollbackTransaction(final MessageExt prepareMessageExt, final Throwable localExecuteException)
			throws Throwable {
		confirmTransaction(prepareMessageExt, LocalTransactionState.ROLLBACK_MESSAGE, localExecuteException);
	}

	private void confirmTransaction(final MessageExt prepareMessageExt, LocalTransactionState localTransactionState,
			Throwable localException) throws Throwable {

		DefaultMQProducer defaultProducer = null;
		try {
			final String brokerAddr = getHostString(prepareMessageExt.getStoreHost());

			EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
			requestHeader.setCommitLogOffset(prepareMessageExt.getCommitLogOffset());
			switch (localTransactionState) {
			case COMMIT_MESSAGE:
				requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
				break;
			case ROLLBACK_MESSAGE:
				requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
				break;
			case UNKNOW:
				requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
				break;
			default:
				break;
			}

			final String prepareMsgPgroup = prepareMessageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
			requestHeader.setProducerGroup(prepareMsgPgroup);

			// 预发消息的queueOffset，其实是0，因为预发消息不进入ConsumeQueue
			requestHeader.setTranStateTableOffset(prepareMessageExt.getQueueOffset());
			requestHeader.setMsgId(prepareMessageExt.getMsgId());
			String remark = localException != null ? ("executeLocalTransactionBranch exception: " + localException
					.toString()) : null;

			defaultProducer = this.getDefaultProducer();
			defaultProducer.getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl()
					.endTransactionOneway(brokerAddr, requestHeader, remark, 3000);
		} catch (Throwable e) {
			logger.warn("confirmTransaction, local transaction execute " + localTransactionState
					+ ", but end broker transaction failed", e);
			throw e;
		} finally {
			if (defaultProducer != null) {
				defaultProducer.shutdown();
			}
		}
	}

	private DefaultMQProducer getDefaultProducer() throws Throwable {
		Throwable t = null;

		DefaultMQProducer producer = new DefaultMQProducer("rocketmq_console_producer_group");
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

	private static String getHostString(SocketAddress host) {
		if (host != null) {
			InetSocketAddress inetSocketAddress = (InetSocketAddress) host;
			return inetSocketAddress.getAddress().getHostAddress() + ":" + inetSocketAddress.getPort();
		}

		return "";
	}

}
