package com.alibaba.rocketmq.service;

import org.springframework.stereotype.Service;

@Service
public class SysInfoService extends AbstractService {
//
//	static final Logger logger = LoggerFactory.getLogger(SysInfoService.class);
//
//	@Value("${rockusername}")
//	private String rockusername;
//
//	@Value("${rockpassword}")
//	private String rockpassword;
//
//	@Value("${rockport}")
//	private String rockport;
//
//	public Table list() throws Throwable {
//		Throwable t = null;
//		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
//		try {
//			defaultMQAdminExt.start();
//			Table table = getBrokerSysInfoList(defaultMQAdminExt);
//			return table;
//		} catch (Throwable e) {
//			logger.error(e.getMessage(), e);
//			t = e;
//		} finally {
//			shutdownDefaultMQAdminExt(defaultMQAdminExt);
//		}
//		throw t;
//	}
//
//	private Table getBrokerSysInfoList(DefaultMQAdminExt defaultMQAdminExt) throws Exception {
//		ClusterInfo clusterInfoSerializeWrapper = defaultMQAdminExt.examineBrokerClusterInfo();
//		String[] instanceThead = new String[] { "broker id", "broker address", "进程id", "jstat", "内存(M)", "load", "硬盘",
//				"IO" };
//
//		Set<Map.Entry<String, Set<String>>> clusterSet = clusterInfoSerializeWrapper.getClusterAddrTable().entrySet();
//
//		int clusterRow = clusterSet.size();
//		Table clusterTable = new Table(new String[] { "cluster name", "broker detail" }, clusterRow);
//		Iterator<Map.Entry<String, Set<String>>> itCluster = clusterSet.iterator();
//
//		while (itCluster.hasNext()) {
//			Map.Entry<String, Set<String>> next = itCluster.next();
//			String clusterName = next.getKey();
//			Set<String> brokerNameSet = new HashSet<String>();
//			brokerNameSet.addAll(next.getValue());
//
//			Object[] clusterTR = clusterTable.createTR();
//			clusterTR[0] = clusterName;
//			Table brokerTable = new Table(new String[] { "broker name", "broker instance" }, brokerNameSet.size());
//			clusterTR[1] = brokerTable;
//			clusterTable.insertTR(clusterTR);
//
//			for (String brokerName : brokerNameSet) {
//				Object[] brokerTR = brokerTable.createTR();
//				brokerTR[0] = brokerName;
//				BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);
//				if (brokerData != null) {
//					Set<Map.Entry<Long, String>> brokerAddrSet = brokerData.getBrokerAddrs().entrySet();
//					Iterator<Map.Entry<Long, String>> itAddr = brokerAddrSet.iterator();
//
//					Table instanceTable = new Table(instanceThead, brokerAddrSet.size());
//					brokerTR[1] = instanceTable;
//					brokerTable.insertTR(brokerTR);// B
//
//					while (itAddr.hasNext()) {
//						Object[] instanceTR = instanceTable.createTR();
//						Map.Entry<Long, String> brokerAddrEntry = itAddr.next();
//						try {
//							String brokerAdress = brokerAddrEntry.getValue();
//
//							instanceTR[0] = str(brokerAddrEntry.getKey().longValue());
//							instanceTR[1] = brokerAdress;
//
//							String brokerIp = brokerAdress.substring(0, brokerAdress.indexOf(":"));
//							RmtShellExecutor exe = new RmtShellExecutor(brokerIp, rockusername, rockpassword, rockport);
//
//							String brokerPsId = getBrokerPsId(exe);
//							if (StringUtils.isNotBlank(brokerPsId)) {
//								instanceTR[2] = brokerPsId;
//								instanceTR[3] = exe.exec("jstat -gcutil " + brokerPsId, true);
//								instanceTR[4] = exe.exec("free -m", true);
//								instanceTR[5] = exe.exec("uptime", true);
//								instanceTR[6] = exe.exec("df -h", true);
//								instanceTR[7] = exe.exec("iostat -k", true);
//								instanceTable.insertTR(instanceTR);// C
//							}
//						} catch (Exception e) {
//							logger.error(e.getMessage(), e);
//						}
//					}
//				}
//			}
//		}
//
//		return clusterTable;
//	}
//
//	private String getBrokerPsId(RmtShellExecutor exe) {
//		try {
//			String brokerPs = exe.exec("jps", false);
//
//			String brokerPsId = "";
//			String[] pslines = brokerPs.split("\n");
//			for (String psline : pslines) {
//				if (psline.contains("BrokerStartup")) {
//					brokerPsId = psline.substring(0, psline.indexOf("BrokerStartup")).trim();
//					return brokerPsId;
//				}
//			}
//		} catch (Exception e) {
//			logger.error(e.getMessage(), e);
//		}
//		return null;
//	}
//
}
