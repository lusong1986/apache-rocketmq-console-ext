package com.alibaba.rocketmq.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

@Service
public class MongoProducerRelationService extends AbstractService implements InitializingBean {

	private static final Logger logger = LoggerFactory.getLogger(MongoProducerRelationService.class);

	private static final String MQ_PRODUCER_RELATION = "mq_producer_relation";

	private static final String MQ_CONSUMER_RELATION = "mq_consumer_relation";

	private static final String MQ_TOPIC_RELATION = "mq_topic_relation";

	public static final Map<String, String> consumerGroupOwnerMap = new ConcurrentHashMap<String, String>();

	public static final Map<String, String> producerGroupOwnerMap = new ConcurrentHashMap<String, String>();

	public static final ConcurrentHashMap<String, List<String>> producerGroupTopicsMap = new ConcurrentHashMap<String, List<String>>();

	@Autowired
	private MongoService mongoService;

	@Override
	public void afterPropertiesSet() throws Exception {
		final ScheduledExecutorService scheduledExecutorService = Executors
				.newSingleThreadScheduledExecutor(new SchedualThreadFactory("MongoProducerRelationServiceThread"));
		scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					consumerGroupOwnerMap.clear();
					producerGroupOwnerMap.clear();
					producerGroupTopicsMap.clear();
				} catch (Throwable e) {
					logger.error("statisticYesterdayMsgs error", e);
				}

			}
		}, 1, 10, TimeUnit.MINUTES);

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

	public Map<String, String> getProducerOwnerRelations() {
		if (producerGroupOwnerMap.size() > 0) {
			return producerGroupOwnerMap;
		}

		final DBCollection collection = mongoService.getCollectionByName(MQ_PRODUCER_RELATION);
		if (collection != null) {
			DBCursor dbCursor = collection.find();
			while (dbCursor.hasNext()) {
				DBObject dbobject = dbCursor.next();
				producerGroupOwnerMap.put(dbobject.get("producer") + "", "" + dbobject.get("owner"));
			}
		}

		return producerGroupOwnerMap;
	}

	public Map<String, String> getConsumerOwnerRelations() {
		if (consumerGroupOwnerMap.size() > 0) {
			return consumerGroupOwnerMap;
		}

		final DBCollection collection = mongoService.getCollectionByName(MQ_CONSUMER_RELATION);
		if (collection != null) {
			DBCursor dbCursor = collection.find();
			while (dbCursor.hasNext()) {
				DBObject dbobject = dbCursor.next();
				final String consumerGroupName = dbobject.get("consumer") + "";
				final String ownerName = "" + dbobject.get("owner");
				consumerGroupOwnerMap.put(consumerGroupName, ownerName);
				consumerGroupOwnerMap.put(consumerGroupName + "_BLUE", ownerName);
				consumerGroupOwnerMap.put(consumerGroupName + "_GREEN", ownerName);
			}
		}

		return consumerGroupOwnerMap;
	}

	public Map<String, List<String>> getProducerTopicRelations() {
		if (producerGroupTopicsMap.size() > 0) {
			return producerGroupTopicsMap;
		}

		final DBCollection collection = mongoService.getCollectionByName(MQ_TOPIC_RELATION);
		if (collection != null) {
			DBCursor dbCursor = collection.find();
			while (dbCursor.hasNext()) {
				DBObject dbobject = dbCursor.next();

				List<String> topics = new ArrayList<>();
				topics.add("" + dbobject.get("topic"));
				List<String> old = producerGroupTopicsMap.putIfAbsent(dbobject.get("producer") + "", topics);
				if (old != null) {
					old.add("" + dbobject.get("topic"));
				}
			}
		}

		return producerGroupTopicsMap;
	}
}
