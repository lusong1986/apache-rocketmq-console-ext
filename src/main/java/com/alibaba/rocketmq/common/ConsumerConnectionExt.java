package com.alibaba.rocketmq.common;

import java.io.Serializable;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public class ConsumerConnectionExt implements Serializable {
	private static final long serialVersionUID = -3166492011162512010L;

	private TreeSet<ConsConnection> connectionSet = new TreeSet<ConsConnection>(new Comparator<ConsConnection>() {

		@Override
		public int compare(ConsConnection o1, ConsConnection o2) {
			int ret = o1.getClientId().compareTo(o2.getClientId());
			if (ret != 0) {
				return ret;
			}

			return o1.getClientAddr().compareTo(o2.getClientAddr());
		}
	});
	private ConcurrentMap<String/* Topic */, SubscriptionData> subscriptionTable = new ConcurrentHashMap<String, SubscriptionData>();
	private ConsumeType consumeType;
	private MessageModel messageModel;
	private ConsumeFromWhere consumeFromWhere;

	public TreeSet<ConsConnection> getConnectionSet() {
		return connectionSet;
	}

	public void setConnectionSet(TreeSet<ConsConnection> connectionSet) {
		this.connectionSet = connectionSet;
	}

	public ConcurrentMap<String, SubscriptionData> getSubscriptionTable() {
		return subscriptionTable;
	}

	public void setSubscriptionTable(ConcurrentMap<String, SubscriptionData> subscriptionTable) {
		this.subscriptionTable = subscriptionTable;
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

	public ConsumeFromWhere getConsumeFromWhere() {
		return consumeFromWhere;
	}

	public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
		this.consumeFromWhere = consumeFromWhere;
	}

}
