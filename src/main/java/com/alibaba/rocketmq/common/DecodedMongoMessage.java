package com.alibaba.rocketmq.common;

import java.io.Serializable;

public class DecodedMongoMessage implements Serializable {

	private static final long serialVersionUID = 7921763146996775373L;

	// 队列ID <PUT>
	private int queueId;
	// 存储记录大小
	private int storeSize;
	// 队列偏移量
	private long queueOffset;
	// 消息标志位 <PUT>
	private int sysFlag;
	// 消息在客户端创建时间戳 <PUT>
	private String bornTime;
	// 消息来自哪里 <PUT>
	private String bornHost;
	// 消息在服务器存储时间戳
	private String storeTime;
	// 消息存储在哪个服务器 <PUT>
	private String storeHost;
	// 消息ID
	private String msgId;
	// 消息对应的Commit Log Offset
	private long commitLogOffset;
	// 消息体CRC
	private int bodyCRC;
	// 当前消息被某个订阅组重新消费了几次（订阅组之间独立计数）
	private int reconsumeTimes;

	private long preparedTransactionOffset;

	private int tranStatus;

	private String _catRootMessageId;

	private String _catChildMessageId1;

	private String _catParentMessageId1;

	private String _catParentMessageId;

	/**
	 * 消息主题
	 */
	private String topic;

	private String tags;

	private String keys;

	/**
	 * 消息标志，系统不做干预，完全由应用决定如何使用
	 */
	private int flag;
	/**
	 * 消息属性
	 */
	private String propertiesString;
	/**
	 * 消息体
	 */
	private String content;

	public String get_catRootMessageId() {
		return _catRootMessageId;
	}

	public void set_catRootMessageId(String _catRootMessageId) {
		this._catRootMessageId = _catRootMessageId;
	}

	public String get_catChildMessageId1() {
		return _catChildMessageId1;
	}

	public void set_catChildMessageId1(String _catChildMessageId1) {
		this._catChildMessageId1 = _catChildMessageId1;
	}

	public String get_catParentMessageId1() {
		return _catParentMessageId1;
	}

	public void set_catParentMessageId1(String _catParentMessageId1) {
		this._catParentMessageId1 = _catParentMessageId1;
	}

	public String get_catParentMessageId() {
		return _catParentMessageId;
	}

	public void set_catParentMessageId(String _catParentMessageId) {
		this._catParentMessageId = _catParentMessageId;
	}

	public int getTranStatus() {
		return tranStatus;
	}

	public void setTranStatus(int tranStatus) {
		this.tranStatus = tranStatus;
	}

	public int getQueueId() {
		return queueId;
	}

	public void setQueueId(int queueId) {
		this.queueId = queueId;
	}

	public int getStoreSize() {
		return storeSize;
	}

	public void setStoreSize(int storeSize) {
		this.storeSize = storeSize;
	}

	public long getQueueOffset() {
		return queueOffset;
	}

	public void setQueueOffset(long queueOffset) {
		this.queueOffset = queueOffset;
	}

	public int getSysFlag() {
		return sysFlag;
	}

	public void setSysFlag(int sysFlag) {
		this.sysFlag = sysFlag;
	}

	public String getBornTime() {
		return bornTime;
	}

	public void setBornTime(String bornTime) {
		this.bornTime = bornTime;
	}

	public String getBornHost() {
		return bornHost;
	}

	public void setBornHost(String bornHost) {
		this.bornHost = bornHost;
	}

	public String getStoreTime() {
		return storeTime;
	}

	public void setStoreTime(String storeTime) {
		this.storeTime = storeTime;
	}

	public String getStoreHost() {
		return storeHost;
	}

	public void setStoreHost(String storeHost) {
		this.storeHost = storeHost;
	}

	public String getMsgId() {
		return msgId;
	}

	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}

	public long getCommitLogOffset() {
		return commitLogOffset;
	}

	public void setCommitLogOffset(long commitLogOffset) {
		this.commitLogOffset = commitLogOffset;
	}

	public int getBodyCRC() {
		return bodyCRC;
	}

	public void setBodyCRC(int bodyCRC) {
		this.bodyCRC = bodyCRC;
	}

	public int getReconsumeTimes() {
		return reconsumeTimes;
	}

	public void setReconsumeTimes(int reconsumeTimes) {
		this.reconsumeTimes = reconsumeTimes;
	}

	public long getPreparedTransactionOffset() {
		return preparedTransactionOffset;
	}

	public void setPreparedTransactionOffset(long preparedTransactionOffset) {
		this.preparedTransactionOffset = preparedTransactionOffset;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getFlag() {
		return flag;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}

	public String getPropertiesString() {
		return propertiesString;
	}

	public void setPropertiesString(String propertiesString) {
		this.propertiesString = propertiesString;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getTags() {
		return tags;
	}

	public void setTags(String tags) {
		this.tags = tags;
	}

	public String getKeys() {
		return keys;
	}

	public void setKeys(String keys) {
		this.keys = keys;
	}

}
