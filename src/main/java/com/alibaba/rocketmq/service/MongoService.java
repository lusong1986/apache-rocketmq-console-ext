package com.alibaba.rocketmq.service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.rocketmq.utils.MessageUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

@Service
public class MongoService extends AbstractService implements InitializingBean {

	static final Logger logger = LoggerFactory.getLogger(MongoService.class);

	@Value("${mongoRepSetHosts}")
	private String mongoRepSetHosts;

	@Value("${mongoDbName}")
	private String mongoDbName;

	@Value("${mongoUser}")
	private String mongoUser;

	@Value("${mongoPassword}")
	private String mongoPassword;

	@Autowired
	private TopicService topicService;

	private Datastore datastore;

	private final ScheduledExecutorService scheduledExecutorService = Executors
			.newSingleThreadScheduledExecutor(new SchedualThreadFactory("MongoServiceScheduledThread"));

	@Override
	public void afterPropertiesSet() throws Exception {
		final long initialDelay = computNextMorningTimeMillis() - System.currentTimeMillis(); // 凌晨0点10分开始执行
		final long period = 1000 * 60 * 60 * 24;
		this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					MongoService.this.createIndexAndstatisticYesterdayMsgs();
				} catch (Throwable e) {
					logger.error("statisticYesterdayMsgs error", e);
				}

			}
		}, initialDelay, period, TimeUnit.MILLISECONDS);

	}

	private static long computNextMorningTimeMillis() {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(System.currentTimeMillis());
		cal.add(Calendar.DAY_OF_MONTH, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 5);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);

		return cal.getTimeInMillis();
	}

	/**
	 * 创建今天mongodb表的索引和记录昨日put、get的统计数据
	 */
	private void createIndexAndstatisticYesterdayMsgs() {
		createIndexForTodayCollection();
		statisticYesterdayPutMsgs();
		statisticYesterdayGetMsgs();
	}

	private void createIndexForTodayCollection() {
		// logger.info(">>>>start to createIndex For todayCollectionName....");
		try {
			final String todayCollectionName = MessageUtils.getMsgCollectionName(new Date());
			// logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>start to createIndex For todayCollectionName:" +
			// todayCollectionName);
			final DBCollection collection = this.getCollectionByName(todayCollectionName);
			if (collection != null) {
				collection.createIndex(new BasicDBObject("topic", 1));
				collection.createIndex(new BasicDBObject("preparedTransactionOffset", 1));
				collection.createIndex(new BasicDBObject("commitLogOffset", 1));
				collection.createIndex(new BasicDBObject("sysFlag", 1));
				collection.createIndex(new BasicDBObject("tranStatus", 1));
			}
		} catch (Throwable e) {
			logger.error("createIndexForTodayCollection error", e);
		}
	}

	/**
	 * 从fetchBrokerRuntimeStats拿到昨天的消费数据放入历史表里面
	 * 
	 */
	private void statisticYesterdayGetMsgs() {
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		try {
			defaultMQAdminExt.start();

			ClusterInfo clusterInfoSerializeWrapper = defaultMQAdminExt.examineBrokerClusterInfo();
			for (Entry<String, BrokerData> dataEntry : clusterInfoSerializeWrapper.getBrokerAddrTable().entrySet()) {
				String brokerMasterAddr = dataEntry.getValue().selectBrokerAddr();
				// logger.info(">>>>>>>>>>brokerMasterAddr:" + brokerMasterAddr);
				KVTable kvTable = defaultMQAdminExt.fetchBrokerRuntimeStats(brokerMasterAddr);
				final String ret = kvTable.getTable().get(MessageUtils.MSG_GET_CONSUMER_GROUP_TIMES_TODAY_MORNING);

				final String lastDay = MessageUtils.formatDate(DateUtils.addDays(new Date(), -1));
				DBObject query = new BasicDBObject();
				query.put(MessageUtils.DATA_COLLECTION_DATE, lastDay);
				query.put(MessageUtils.BROKER_NAME, dataEntry.getValue().getBrokerName());
				DBCursor cursor = this.getCollectionByName(MessageUtils.MQ_GET_HIS_TABLE).find(query);
				if (cursor.length() > 0) {
					continue;
				}

				DBObject dbObject = new BasicDBObject();
				dbObject.put("brokerMasterAddr", brokerMasterAddr);
				dbObject.put(MessageUtils.BROKER_NAME, dataEntry.getValue().getBrokerName());
				dbObject.put(MessageUtils.CREATE_TIME, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
				dbObject.put(MessageUtils.DATA_COLLECTION_DATE, lastDay);

				Integer consumerAllCount = 0;
				if (StringUtils.isNotBlank(ret)) {
					JSONObject jsonObject = JSON.parseObject(ret);
					Set<String> consumerGroups = jsonObject.keySet();
					for (String consumerGroup : consumerGroups) {
						String text = jsonObject.get(consumerGroup) + "";
						JSONObject json = JSON.parseObject(text);

						for (Entry<String, Object> topicEntry : json.entrySet()) {
							// String topic = topicEntry.getKey();
							Integer consumeMsgCount = Integer.parseInt(topicEntry.getValue() + "");
							consumerAllCount += consumeMsgCount;
						}
						dbObject.put(consumerGroup, text);
					}
				} else {
					throw new RuntimeException("can not get consumer groups");
				}

				dbObject.put(MessageUtils.ALL_GET_COUNT, consumerAllCount);
				// System.out.println(">>>>>>>dbObject:" + dbObject);
				// 每个broker每天保存一条消费统计数据
				this.getCollectionByName(MessageUtils.MQ_GET_HIS_TABLE).insert(dbObject);
			}
		} catch (Throwable e) {
			logger.error("", e);
		}
	}

	/**
	 * 从mongodb拿到昨天记录表里面消息发送的数据，放到昨日put统计表里面
	 * 
	 */
	private void statisticYesterdayPutMsgs() {
		final String yesterdayCollection = MessageUtils.getMsgCollectionName(DateUtils.addDays(new Date(), -1));

		DBObject query = new BasicDBObject();
		query.put(MessageUtils.DATA_COLLECTION_NAME, yesterdayCollection);
		DBCursor cursor = this.getCollectionByName(MessageUtils.MQ_PUT_HIS_TABLE).find(query);
		if (cursor.length() > 0) {
			return;
		}

		DBObject dbObject = new BasicDBObject();

		long yesterdayAllCount = this.countByCollection(yesterdayCollection);
		dbObject.put(MessageUtils.ALL_PUT_COUNT, yesterdayAllCount);
		dbObject.put(MessageUtils.DATA_COLLECTION_NAME, yesterdayCollection);
		dbObject.put(MessageUtils.CREATE_TIME, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

		Set<String> topics = topicService.getAllTopics();
		if (topics.size() > 0) {
			for (String topic : topics) {
				long yesterdayTopicCount = this.countByCollectionTopic(yesterdayCollection, topic);
				if (yesterdayTopicCount > 0) {
					dbObject.put(topic, yesterdayTopicCount);
				}
			}

			// System.out.println(">>>>>>>dbObject:" + dbObject);
			this.getCollectionByName(MessageUtils.MQ_PUT_HIS_TABLE).insert(dbObject);
		} else {
			System.err.println("Can not get any topic, not save put history.");
		}
	}

	public long countByCollectionTopic(String collName, String topic) {
		DBObject query = new BasicDBObject();
		query.put("topic", topic);
		DBCollection dbCollection = getCollectionByName(collName);
		if (null == dbCollection) {
			return 0;
		}

		return dbCollection.count(query);
	}

	public long countByCollection(String collName) {
		DBCollection dbCollection = getCollectionByName(collName);
		if (null == dbCollection) {
			return 0;
		}

		return dbCollection.count();
	}

	public DBCollection getCollectionByName(String name) {
		try {
			DBCollection collection = getMQDatastore().getDB().getCollection(name);
			return collection;
		} catch (Exception e) {
		}

		return null;
	}

	private Datastore getMQDatastore() {
		if (null == datastore) {
			synchronized (this) {
				if (null == datastore) {
					MongoClient mongoClient = getMongoClient();
					Morphia morphia = new Morphia();
					datastore = morphia.createDatastore(mongoClient, mongoDbName);
				}
			}
		}
		return datastore;
	}

	private MongoClient getMongoClient() {
		List<MongoCredential> credentialsList = new LinkedList<MongoCredential>();
		MongoCredential credential = MongoCredential.createCredential(mongoUser, mongoDbName,
				mongoPassword.toCharArray());
		credentialsList.add(credential);
		MongoClient mgClient = new MongoClient(getAddresses(), credentialsList);
		return mgClient;
	}

	private List<ServerAddress> getAddresses() {
		if (mongoRepSetHosts == null || mongoRepSetHosts.trim().length() == 0) {
			throw new RuntimeException("not config mongoRepSetHosts");
		}

		List<ServerAddress> addresses = new ArrayList<ServerAddress>();
		String[] mongoHosts = mongoRepSetHosts.trim().split(",");
		if (mongoHosts != null && mongoHosts.length > 0) {
			for (String mongoHost : mongoHosts) {
				if (mongoHost != null && mongoHost.length() > 0) {
					String[] mongoServer = mongoHost.split(":");
					if (mongoServer != null && mongoServer.length == 2) {
						ServerAddress address = new ServerAddress(mongoServer[0].trim(),
								Integer.parseInt(mongoServer[1].trim()));
						addresses.add(address);
					}
				}
			}
		}
		return addresses;
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

}
