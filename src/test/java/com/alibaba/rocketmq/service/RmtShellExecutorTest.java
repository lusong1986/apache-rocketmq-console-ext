package com.alibaba.rocketmq.service;

public class RmtShellExecutorTest {

	public static void main(String args[]) throws Exception {
		String brokerIp = "192.168.56.101";
		String username = "anders";
		String password = "123";
		RmtShellExecutor exe = new RmtShellExecutor(brokerIp, username, password, "22");
		// System.out.println(exe.exec("uname -a && date && uptime && who"));

		String brokerPs = exe.exec("jps", false);
		System.out.println(brokerPs);

		String brokerPsId = "";
		String[] pslines = brokerPs.split("\n");
		for (String psline : pslines) {
			if (psline.contains("BrokerStartup")) {
				brokerPsId = psline.substring(0, psline.indexOf("BrokerStartup")).trim();
				System.out.println(brokerPsId);
				break;
			}
		}

		String jstatString = exe.exec("jstat -gcutil " + brokerPsId, false);
		System.out.println(jstatString);

		String memUse = exe.exec("free -m", false);
		System.out.println(memUse);

		String uptime = exe.exec("uptime", false);
		System.out.println(uptime);

		String disk = exe.exec("df -h", false);
		System.out.println(disk);

		String io = exe.exec("iostat -k", false);
		System.out.println(io);

		// exe.exec("uname -a && date && uptime && who");
	}

}
