package com.alibaba.rocketmq.utils;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.mongodb.DBObject;

public class MessageUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(MessageUtils.class);

	private static final String MESSAGE_SERIALIZE_CLASS = "SERIALIZE_CLASS";

	private static final String DEFAULT_CHARSET = "UTF-8";

	private static final SimpleDateFormat MQ_COLLECTION_FMT = new SimpleDateFormat("yyyyMMdd");

	private static final String MQ_TABLE_PREFIX = "messages_";

	public static final String MQ_PUT_HIS_TABLE = "messages_put_history";

	public static final String MQ_GET_HIS_TABLE = "messages_get_history";

	public static final String DATA_COLLECTION_NAME = "dataCollectionName";

	public static final String DATA_COLLECTION_DATE = "dataCollectionDate";

	public static final String ALL_PUT_COUNT = "allPutCount";

	public static final String ALL_GET_COUNT = "allGetCount";

	public static final String BROKER_NAME = "brokerName";

	public static final String CREATE_TIME = "createTime";

	public static final String MSG_GET_CONSUMER_GROUP_TIMES_TODAY_NOW = "msgGetConsumerGroupTimesTodayNow";

	public static final String MSG_GET_CONSUMER_GROUP_TIMES_TODAY_MORNING = "msgGetConsumerGroupTimesTodayMorning";

	public static String getMsgCollectionName(Date date) {
		return MQ_TABLE_PREFIX + formatDate(date);
	}

	public static String formatDate(Date date) {
		return MQ_COLLECTION_FMT.format(date);
	}

	public static String conver2String(MessageExt messageExt) {
		Object messageObject = null;
		String serializeClassName = messageExt.getUserProperty(MESSAGE_SERIALIZE_CLASS);
		if (StringUtils.isNotBlank(serializeClassName)) {
			String bodyContentStr = null;
			try {
				bodyContentStr = new String(messageExt.getBody(), DEFAULT_CHARSET);
			} catch (Exception e) {
				LOGGER.error("failed to convert text-based Message content:{}", messageExt, e);
			}

			if (String.class.getName().equals(serializeClassName)) {
				messageObject = bodyContentStr;
			} else {
				// comment
				if (StringUtils.isNotBlank(bodyContentStr)) {
					try {
						messageObject = JSON.parseObject(bodyContentStr, Class.forName(serializeClassName));
					} catch (Exception e) {
						// LOGGER.info("failed to convert json-based Message content:{}" + messageExt.toString() + ","
						// + e.getMessage());
						messageObject = bodyContentStr;
					}
				}
			}
		} else {
			// try to deserializable bytes (old message)
			try {
				messageObject = new String(messageExt.getBody(), DEFAULT_CHARSET);
			} catch (Throwable e) {
				LOGGER.info("failed to deserializable Message content:{}", messageExt, e);
			}
		}

		if (messageObject != null) {
			return messageObject.toString();
		} else {
			return "bytes[" + messageExt.getBody().length + "]";
		}
	}

	public static <T> T dbObject2Bean(DBObject dbObject, T bean) {
		if (bean == null) {
			return null;
		}

		try {
			Field[] fields = bean.getClass().getDeclaredFields();
			for (Field field : fields) {
				String varName = field.getName();
				Object object = dbObject.get(varName);
				if (object != null) {
					BeanUtils.setProperty(bean, varName, object);
				}
			}
		} catch (Exception e) {
		}

		return bean;
	}

}
