package com.alibaba.rocketmq.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import ch.ethz.ssh2.ChannelCondition;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

/**
 * 远程执行shell脚本类
 * 
 * @author lusong
 */
public class RmtShellExecutor {

	private Connection conn;

	private String ip;

	private String usr;

	private String psword;

	private String rockport = "22";

	private static final int TIME_OUT = 1000 * 10;

	public RmtShellExecutor(String ip, String usr, String ps, String rockport) {
		this.ip = ip;
		this.usr = usr;
		this.psword = ps;
		this.rockport = rockport;
	}

	private boolean login() throws IOException {
		conn = new Connection(ip, Integer.parseInt(rockport));
		conn.connect();

		if (StringUtils.isBlank(psword)) {
			System.out.println(">>>>>>>call authenticateWithPublicKey.");
			return conn.authenticateWithPublicKey(usr, new File("/home/work/.ssh/id_rsa"), null);
		}

		return conn.authenticateWithPassword(usr, psword);
	}

	@SuppressWarnings("deprecation")
	public String exec(String cmds, boolean display) throws Exception {
		InputStream stdOut = null;
		BufferedReader br = null;
		Session session = null;

		String result = "";
		try {
			if (login()) {
				session = conn.openSession();
				session.execCommand(cmds);

				stdOut = new StreamGobbler(session.getStdout());
				br = new BufferedReader(new InputStreamReader(stdOut));

				StringBuilder sb = new StringBuilder();
				while (true) {
					String line = br.readLine();
					if (null == line) {
						break;
					}

					if (display) {
						sb.append("<nobr>" + line.replaceAll(" ", "&nbsp;").replaceAll("\t", "&nbsp;&nbsp;&nbsp;")
								+ "</nobr>");
						sb.append("<br/>");
					} else {
						sb.append(line);
						sb.append("\n");
					}
				}
				result = sb.toString();

				session.waitForCondition(ChannelCondition.EXIT_STATUS, TIME_OUT);

				session.getExitStatus();
				// System.out.println("exit status=" + ret);
			} else {
				throw new RuntimeException("login failed :" + ip);
			}
		} finally {
			if (conn != null) {
				conn.close();
			}

			if (session != null) {
				session.close();
			}

			if (br != null) {
				IOUtils.closeQuietly(br);
			}

			if (stdOut != null) {
				IOUtils.closeQuietly(stdOut);
			}
		}

		return result;
	}

}