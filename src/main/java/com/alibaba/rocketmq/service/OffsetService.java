package com.alibaba.rocketmq.service;

import static com.alibaba.rocketmq.common.Tool.bool;
import static com.alibaba.rocketmq.common.Tool.str;
import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.util.Collection;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.admin.RollbackStats;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.offset.ResetOffsetByTimeOldCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.validate.CmdTrace;

@Service
public class OffsetService extends AbstractService {

	static final Logger logger = LoggerFactory.getLogger(OffsetService.class);

	static final ResetOffsetByTimeOldCommand resetOffsetByTimeSubCommand = new ResetOffsetByTimeOldCommand();

	@SuppressWarnings("unchecked")
	public Collection<Option> getOptionsForResetOffsetByTime() {
		Options options = new Options();

		Option opt = new Option("g", "group", true, "consumer group name");
		opt.setRequired(true);
		options.addOption(opt);

		opt = new Option("t", "topic", true, "topic name");
		opt.setRequired(true);
		options.addOption(opt);

		opt = new Option("s", "timestamp", true,
				"时间戳（currentTimeMillis | yyyy-MM-dd#HH:mm:ss:SSS），例如：1456908863977或者2016-04-01#12:35:23:123");
		opt.setRequired(true);
		options.addOption(opt);

		opt = new Option("f", "force", true, "强制跳过未消费消息（true | false），默认：true");
		opt.setRequired(false);
		options.addOption(opt);

		return options.getOptions();
	}

	@CmdTrace(cmdClazz = ResetOffsetByTimeOldCommand.class)
	public Table resetOffsetByTime(String consumerGroup, String topic, String timeStampStr, String forceStr)
			throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		try {
			long timestamp = 0;
			try {
				// 直接输入 long 类型的 timestamp
				timestamp = Long.valueOf(timeStampStr);
			} catch (NumberFormatException e) {
				// 输入的为日期格式，精确到毫秒
				timestamp = UtilAll.parseDate(timeStampStr, UtilAll.YYYY_MM_DD_HH_MM_SS_SSS).getTime();
			}

			boolean force = true;
			if (isNotBlank(forceStr)) {
				force = bool(forceStr.trim());
			}
			defaultMQAdminExt.start();
			List<RollbackStats> rollbackStatsList = defaultMQAdminExt.resetOffsetByTimestampOld(consumerGroup, topic,
					timestamp, force);
			// System.out
			// .printf(
			// "rollback consumer offset by specified consumerGroup[%s], topic[%s], force[%s], timestamp(string)[%s], timestamp(long)[%s]\n",
			// consumerGroup, topic, force, timeStampStr, timestamp);
			//
			// System.out.printf("%-20s  %-20s  %-20s  %-20s  %-20s  %-20s\n",//
			// "#brokerName",//
			// "#queueId",//
			// "#brokerOffset",//
			// "#consumerOffset",//
			// "#timestampOffset",//
			// "#rollbackOffset" //
			// );
			String[] thead = new String[] { "broker name", "queue id", "broker offset", "consumer offset",
					"timestamp offset", "rollback offset" };
			Table table = new Table(thead, rollbackStatsList.size());

			for (RollbackStats rollbackStats : rollbackStatsList) {
				// System.out.printf("%-20s  %-20d  %-20d  %-20d  %-20d  %-20d\n",//
				// UtilAll.frontStringAtLeast(rollbackStats.getBrokerName(),
				// 32),//
				// rollbackStats.getQueueId(),//
				// rollbackStats.getBrokerOffset(),//
				// rollbackStats.getConsumerOffset(),//
				// rollbackStats.getTimestampOffset(),//
				// rollbackStats.getRollbackOffset() //
				// );
				Object[] tr = table.createTR();
				tr[0] = UtilAll.frontStringAtLeast(rollbackStats.getBrokerName(), 32);
				tr[1] = str(rollbackStats.getQueueId());
				tr[2] = str(rollbackStats.getBrokerOffset());
				tr[3] = str(rollbackStats.getConsumerOffset());
				tr[4] = str(rollbackStats.getTimestampOffset());
				tr[5] = str(rollbackStats.getRollbackOffset());
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
}
