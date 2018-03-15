package com.alibaba.rocketmq.common;

import java.util.ArrayList;
import java.util.List;

public class ConsumerProgressStatus {

	private long diffTotal = 0L;

	private List<MessageQueueStatus> messageQueueConsumerList = new ArrayList<MessageQueueStatus>();

	public long getDiffTotal() {
		return diffTotal;
	}

	public void setDiffTotal(long diffTotal) {
		this.diffTotal = diffTotal;
	}

	public void addMessageQueueStatus(MessageQueueStatus messageQueueStatus) {
		messageQueueConsumerList.add(messageQueueStatus);
	}

	public List<MessageQueueStatus> getMessageQueueConsumerList() {
		return messageQueueConsumerList;
	}

	public void setMessageQueueConsumerList(List<MessageQueueStatus> messageQueueConsumerList) {
		this.messageQueueConsumerList = messageQueueConsumerList;
	}

}
