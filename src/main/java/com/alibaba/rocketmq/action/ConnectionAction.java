package com.alibaba.rocketmq.action;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.body.ProducerConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.common.ConsConnection;
import com.alibaba.rocketmq.common.ConsumerConnectionExt;
import com.alibaba.rocketmq.common.ErrMsg;
import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.service.ConnectionService;

/**
 * 
 * @author lusong
 */
@Controller
@RequestMapping("/connection")
public class ConnectionAction extends AbstractAction {

	@Autowired
	ConnectionService connectionService;

	@Override
	protected String getFlag() {
		return "connection_flag";
	}

	@RequestMapping(value = "/hostProducerConsumerList.do", method = { RequestMethod.GET, RequestMethod.POST })
	public String hostProducerConsumerList(ModelMap map, HttpServletRequest request) {
		putPublicAttribute(map, "hostProducerConsumerList");
		try {
			Table table = connectionService.hostProducerConsumerList();
			putTable(map, table);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/consumerGroupList.do", method = { RequestMethod.GET, RequestMethod.POST })
	public String onlineConsumerGroupList(ModelMap map, HttpServletRequest request,
			@RequestParam(required = false) String consumerGroup) {
		putPublicAttribute(map, "consumerGroupList");
		try {
			Table table = connectionService.getOnlineConsumerGroupList();
			putTable(map, table);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/consumerConnection.do", method = { RequestMethod.GET, RequestMethod.POST })
	public String consumerConnection(ModelMap map, HttpServletRequest request,
			@RequestParam(required = false) String consumerGroup) {
		Collection<Option> options = connectionService.getOptionsForGetConsumerConnection();
		putPublicAttribute(map, "consumerConnection", options, request);
		try {
			if (request.getMethod().equals(GET)) {

			} else if (request.getMethod().equals(POST)) {
				checkOptions(options);
				ConsumerConnectionExt cc = connectionService.getConsumerConnection(consumerGroup);
				map.put("cc", cc);
				map.put("consumerGroup", consumerGroup);
			} else {
				throwUnknowRequestMethodException(request);
			}
		} catch (Throwable e) {
			putAlertMsg(e, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/offlineConsumerByClientIds.do", method = { RequestMethod.GET, RequestMethod.POST })
	public String offlineConsumerByClientIds(ModelMap map, HttpServletRequest request,
			@RequestParam(required = true) String consumerGroup, @RequestParam(required = true) String clientIds) {
		Collection<Option> options = getOfflineConsumerByClientIdsOptions();
		putPublicAttribute(map, "offlineConsumerByClientIds", options, request);
		try {
			if (request.getMethod().equals(GET)) {

			} else if (request.getMethod().equals(POST)) {
				Table table = connectionService.offlineConsumerByClientIds(consumerGroup, clientIds);
				putTable(map, table);
			} else {
				throwUnknowRequestMethodException(request);
			}
		} catch (Throwable e) {
			putAlertMsg(e, map);
		}
		return TEMPLATE;
	}

	protected Collection<Option> getOfflineConsumerByClientIdsOptions() {
		Options options = new Options();
		Option opt = new Option("i", "consumerGroup", true, "consumer group");
		opt.setRequired(true);
		options.addOption(opt);

		Option opt1 = new Option("j", "clientIds", true,
				"consumer clientids, split by comma, example:172.16.57.62@24935,172.16.57.63@13832");
		opt1.setRequired(true);
		options.addOption(opt1);

		@SuppressWarnings("unchecked")
		Collection<Option> col = options.getOptions();
		return col;
	}

	protected Collection<Option> getOnlineConsumerByClientIdsOptions() {
		Options options = new Options();
		Option opt = new Option("i", "consumerGroup", true, "consumer group");
		opt.setRequired(true);
		options.addOption(opt);

		Option opt1 = new Option("j", "clientIds", true, "consumer clientIP,  example:172.16.57.62");
		opt1.setRequired(true);
		options.addOption(opt1);

		@SuppressWarnings("unchecked")
		Collection<Option> col = options.getOptions();
		return col;
	}

	@RequestMapping(value = "/onlineConsumerByClientIds.do", method = { RequestMethod.GET, RequestMethod.POST })
	public String onlineConsumerByClientIds(ModelMap map, HttpServletRequest request,
			@RequestParam(required = true) String consumerGroup, @RequestParam(required = true) String clientIds) {
		Collection<Option> options = getOnlineConsumerByClientIdsOptions();
		putPublicAttribute(map, "onlineConsumerByClientIds", options, request);
		try {
			if (request.getMethod().equals(GET)) {

			} else if (request.getMethod().equals(POST)) {
				Table table = connectionService.onlineConsumerByClientIds(consumerGroup, clientIds);
				putTable(map, table);
			} else {
				throwUnknowRequestMethodException(request);
			}
		} catch (Throwable e) {
			putAlertMsg(e, map);
		}
		return TEMPLATE;
	}

	/**
	 * 脚本用到，下线client
	 * 
	 * @param request
	 * @param response
	 * @param consumerGroup
	 * @param clientIds
	 */
	@RequestMapping(value = "/offlineConsumerByClientIdsGroup.do", method = { RequestMethod.GET, RequestMethod.POST })
	public void offlineConsumerByClientIdsGroup(HttpServletRequest request, HttpServletResponse response,
			@RequestParam(required = false) String consumerGroup, @RequestParam(required = false) String clientIds) {

		PrintWriter out = null;
		try {
			response.setCharacterEncoding("UTF-8");
			response.setContentType("application/json; charset=utf-8");
			response.setDateHeader("Expires", 0);
			response.setHeader("Cache-Control", "no-cache");
			response.setHeader("Pragma", "no-cache");

			out = response.getWriter();
			Map<String, String> result = connectionService.offlineConsumerByClientIdsGroup(consumerGroup, clientIds);

			out.append(JSON.toJSONString(result));
		} catch (Throwable e) {
			out.append(JSON.toJSONString(new ErrMsg("offlineConsumerByClientIdsGroup failed " + e.getMessage(), 0)));
			e.printStackTrace();
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

	@RequestMapping(value = "/queryConsumerByIp.do", method = { RequestMethod.GET, RequestMethod.POST })
	public String queryConsumerByIp(ModelMap map, HttpServletRequest request,
			@RequestParam(required = false) String hostIp) {
		Collection<Option> options = getQueryConsumerByIpOptions();
		putPublicAttribute(map, "queryConsumerByIp", options, request);
		try {
			if (request.getMethod().equals(GET)) {

			} else if (request.getMethod().equals(POST)) {
				ConsumerConnectionExt cc = connectionService.queryConsumerByIp(hostIp);
				map.put("cc", cc);
			} else {
				throwUnknowRequestMethodException(request);
			}
		} catch (Throwable e) {
			putAlertMsg(e, map);
		}
		return TEMPLATE;
	}

	/**
	 * 脚本用到，查询消费者是否已经被下线
	 * 
	 * @param request
	 * @param response
	 * @param hostIp
	 */
	@RequestMapping(value = "/queryConsumerOnlineStatus.do", method = { RequestMethod.GET, RequestMethod.POST })
	public void queryConsumerOnlineStatus(HttpServletRequest request, HttpServletResponse response,
			@RequestParam(required = false) String hostIp) {

		PrintWriter out = null;
		try {
			response.setCharacterEncoding("UTF-8");
			response.setContentType("application/json; charset=utf-8");
			response.setDateHeader("Expires", 0);
			response.setHeader("Cache-Control", "no-cache");
			response.setHeader("Pragma", "no-cache");

			out = response.getWriter();
			ConsumerConnectionExt cc = connectionService.queryConsumerByIp(hostIp);

			ConsumerOfflineStatus consumerOfflineStatus = new ConsumerOfflineStatus();
			if (cc.getConnectionSet().size() > 0) {
				final ConsConnection first = cc.getConnectionSet().first();
				consumerOfflineStatus.setClientId(first.getClientId());
				consumerOfflineStatus.setOffline(first.isOffline());
				consumerOfflineStatus.setConsumerGroup(first.getConsumerGroup());

				String bindQueues = "";
				for (ConsConnection consConnection : cc.getConnectionSet()) {
					if (StringUtils.isNotBlank(consConnection.getBindQueues())) {
						bindQueues += consConnection.getBindQueues() + ",";
					}
				}
				consumerOfflineStatus.setBindQueues(bindQueues);
			}
			out.append(JSON.toJSONString(consumerOfflineStatus));
		} catch (Throwable e) {
			out.append(JSON.toJSONString(new ErrMsg("queryConsumerOnlineStatus failed " + e.getMessage(), 0)));
			e.printStackTrace();
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

	class ConsumerOfflineStatus {
		private String clientId;
		private boolean offline;
		private String consumerGroup;
		private String bindQueues;

		public String getBindQueues() {
			return bindQueues;
		}

		public void setBindQueues(String bindQueues) {
			this.bindQueues = bindQueues;
		}

		public String getConsumerGroup() {
			return consumerGroup;
		}

		public void setConsumerGroup(String consumerGroup) {
			this.consumerGroup = consumerGroup;
		}

		public String getClientId() {
			return clientId;
		}

		public void setClientId(String clientId) {
			this.clientId = clientId;
		}

		public boolean isOffline() {
			return offline;
		}

		public void setOffline(boolean offline) {
			this.offline = offline;
		}

	}

	protected Collection<Option> getQueryConsumerByIpOptions() {
		Options options = new Options();
		Option opt = new Option("i", "hostIp", true, "consumer host Ip");
		opt.setRequired(true);
		options.addOption(opt);

		@SuppressWarnings("unchecked")
		Collection<Option> col = options.getOptions();
		return col;
	}

	@RequestMapping(value = "/producerGroupList.do", method = { RequestMethod.GET, RequestMethod.POST })
	public String onlineProducerGroupList(ModelMap map, HttpServletRequest request,
			@RequestParam(required = false) String consumerGroup) {
		putPublicAttribute(map, "producerGroupList");
		try {
			Table table = connectionService.getOnlineProducerGroupList();
			putTable(map, table);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/producerConnection.do", method = { RequestMethod.GET, RequestMethod.POST })
	public String producerConnection(ModelMap map, HttpServletRequest request,
			@RequestParam(required = false) String producerGroup) {
		Collection<Option> options = connectionService.getOptionsForGetProducerConnection();
		putPublicAttribute(map, "producerConnection", options, request);
		try {
			if (request.getMethod().equals(GET)) {

			} else if (request.getMethod().equals(POST)) {
				checkOptions(options);
				ProducerConnection pc = connectionService.getProducerConnection(producerGroup, MixAll.BENCHMARK_TOPIC);
				map.put("pc", pc);
			} else {
				throwUnknowRequestMethodException(request);
			}
		} catch (Throwable e) {
			putAlertMsg(e, map);
		}
		return TEMPLATE;
	}

	@Override
	protected String getName() {
		return "Connection";
	}
}
