package com.alibaba.rocketmq.service;

import static com.alibaba.rocketmq.common.Tool.str;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.cluster.ClusterListSubCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.validate.CmdTrace;

@Service
public class ClusterService extends AbstractService {

	static final Logger logger = LoggerFactory.getLogger(ClusterService.class);

	@CmdTrace(cmdClazz = ClusterListSubCommand.class)
	public Table list() throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		try {
			defaultMQAdminExt.start();
			Table table = doList(defaultMQAdminExt);
			return table;
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}
		throw t;
	}

	private Table doList(DefaultMQAdminExt defaultMQAdminExt) throws Exception {
		ClusterInfo clusterInfoSerializeWrapper = defaultMQAdminExt.examineBrokerClusterInfo();
		String[] instanceThead = new String[] { "broker id", "broker address", "version", "入队tps", "出队tps", "昨日入队数",
				"昨日出队数", "今日入队数", "今日出队数", "今日凌晨入队历史总数", "commitLogMaxOffset", "commitLogMinOffset" };

		Set<Map.Entry<String, Set<String>>> clusterSet = clusterInfoSerializeWrapper.getClusterAddrTable().entrySet();

		int clusterRow = clusterSet.size();
		Table clusterTable = new Table(new String[] { "cluster name", "broker detail" }, clusterRow);
		Iterator<Map.Entry<String, Set<String>>> itCluster = clusterSet.iterator();

		while (itCluster.hasNext()) {
			Map.Entry<String, Set<String>> next = itCluster.next();
			String clusterName = next.getKey();
			Set<String> brokerNameSet = new HashSet<String>();
			brokerNameSet.addAll(next.getValue());

			Object[] clusterTR = clusterTable.createTR();
			clusterTR[0] = clusterName;
			Table brokerTable = new Table(new String[] { "broker name", "broker instance" }, brokerNameSet.size());
			clusterTR[1] = brokerTable;
			clusterTable.insertTR(clusterTR);// A

			for (String brokerName : brokerNameSet) {
				Object[] brokerTR = brokerTable.createTR();
				brokerTR[0] = brokerName;
				final BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);
				if (brokerData != null) {
					final Set<Map.Entry<Long, String>> brokerAddrSet = brokerData.getBrokerAddrs().entrySet();
					Iterator<Map.Entry<Long, String>> itAddr = brokerAddrSet.iterator();

					Table instanceTable = new Table(instanceThead, brokerAddrSet.size());
					brokerTR[1] = instanceTable;
					brokerTable.insertTR(brokerTR);// B

					while (itAddr.hasNext()) {
						Object[] instanceTR = instanceTable.createTR();
						Map.Entry<Long, String> next1 = itAddr.next();
						double in = 0;
						double out = 0;
						String version = "";

						long InTotalYest = 0;
						long OutTotalYest = 0;
						long InTotalToday = 0;
						long OutTotalToday = 0;
						long commitLogMaxOffset = 0;
						long commitLogMinOffset = 0;

						try {
							KVTable kvTable = defaultMQAdminExt.fetchBrokerRuntimeStats(next1.getValue());
							commitLogMaxOffset = Long.valueOf(kvTable.getTable().get("commitLogMaxOffset"));
							commitLogMinOffset = Long.valueOf(kvTable.getTable().get("commitLogMinOffset"));

							String putTps = kvTable.getTable().get("putTps");
							String getTransferedTps = kvTable.getTable().get("getTransferedTps");
							version = kvTable.getTable().get("brokerVersionDesc");
							{
								String[] tpss = putTps.split(" ");
								if (tpss != null && tpss.length > 0) {
									in = Double.parseDouble(tpss[0]);
								}
							}

							{
								String[] tpss = getTransferedTps.split(" ");
								if (tpss != null && tpss.length > 0) {
									out = Double.parseDouble(tpss[0]);
								}
							}

							instanceTR[0] = "<a href=/rocketmq-console/broker/brokerStats.do?brokerAddr="
									+ next1.getValue() + ">" + str(next1.getKey().longValue()) + "</a>";
							instanceTR[1] = next1.getValue();
							instanceTR[2] = version;
							instanceTR[3] = str(in);
							instanceTR[4] = str(out);

							String msgPutTotalYesterdayMorning = kvTable.getTable().get("msgPutTotalYesterdayMorning");
							String msgPutTotalTodayMorning = kvTable.getTable().get("msgPutTotalTodayMorning");
							String msgPutTotalTodayNow = kvTable.getTable().get("msgPutTotalTodayNow");
							String msgGetTotalYesterdayMorning = kvTable.getTable().get("msgGetTotalYesterdayMorning");
							String msgGetTotalTodayMorning = kvTable.getTable().get("msgGetTotalTodayMorning");
							String msgGetTotalTodayNow = kvTable.getTable().get("msgGetTotalTodayNow");

							InTotalYest = Long.parseLong(msgPutTotalTodayMorning)
									- Long.parseLong(msgPutTotalYesterdayMorning);
							OutTotalYest = Long.parseLong(msgGetTotalTodayMorning)
									- Long.parseLong(msgGetTotalYesterdayMorning);

							InTotalToday = Long.parseLong(msgPutTotalTodayNow)
									- Long.parseLong(msgPutTotalTodayMorning);
							OutTotalToday = Long.parseLong(msgGetTotalTodayNow)
									- Long.parseLong(msgGetTotalTodayMorning);

							instanceTR[5] = str(InTotalYest);
							instanceTR[6] = str(OutTotalYest);
							instanceTR[7] = str(InTotalToday);
							instanceTR[8] = str(OutTotalToday);
							instanceTR[9] = msgPutTotalTodayMorning;
							instanceTR[10] = str(commitLogMaxOffset);
							instanceTR[11] = str(commitLogMinOffset);

							instanceTable.insertTR(instanceTR);// C
						} catch (Exception e) {
							logger.error(e.getMessage(), e);
						}
					}
				}
			}
		}
		return clusterTable;
	}

}
