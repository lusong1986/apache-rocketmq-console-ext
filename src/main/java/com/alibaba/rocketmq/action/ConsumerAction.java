package com.alibaba.rocketmq.action;

import java.io.PrintWriter;
import java.util.Collection;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.cli.Option;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.common.ConsumerProgressStatus;
import com.alibaba.rocketmq.common.ErrMsg;
import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.service.ConsumerService;

/**
 * 
 * @author lusong
 */
@Controller
@RequestMapping("/consumer")
public class ConsumerAction extends AbstractAction {

	@Autowired
	ConsumerService consumerService;

	@Override
	protected String getFlag() {
		return "consumer_flag";
	}

	@RequestMapping(value = "/consumerProgress.do", method = { RequestMethod.GET, RequestMethod.POST })
	public String consumerProgress(ModelMap map, HttpServletRequest request,
			@RequestParam(required = false) String groupName) {
		Collection<Option> options = consumerService.getOptionsForConsumerProgress();
		putPublicAttribute(map, "consumerProgress", options, request);
		try {
			if (request.getMethod().equals(GET)) {

			} else if (request.getMethod().equals(POST)) {
				checkOptions(options);
				Table table = consumerService.consumerProgress(groupName);
				putTable(map, table);
			} else {
				throwUnknowRequestMethodException(request);
			}
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	/**
	 * 脚本用到，查询消费组是否有消息未消费完
	 * 
	 * @param request
	 * @param response
	 * @param hostIp
	 */
	@RequestMapping(value = "/queryConsumerProgress.do", method = { RequestMethod.GET, RequestMethod.POST })
	public void queryConsumerProgress(HttpServletRequest request, HttpServletResponse response,
			@RequestParam(required = true) String consumerGroup) {

		PrintWriter out = null;
		try {
			response.setCharacterEncoding("UTF-8");
			response.setContentType("application/json; charset=utf-8");
			response.setDateHeader("Expires", 0);
			response.setHeader("Cache-Control", "no-cache");
			response.setHeader("Pragma", "no-cache");

			out = response.getWriter();

			ConsumerProgressStatus consumerProgressStatus = consumerService.queryConsumerProgress(consumerGroup);

			out.append(JSON.toJSONString(consumerProgressStatus));
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

	@RequestMapping(value = "/deleteSubGroup.do", method = { RequestMethod.GET, RequestMethod.POST })
	public String deleteSubGroup(ModelMap map, HttpServletRequest request,
			@RequestParam(required = false) String groupName, @RequestParam(required = false) String brokerAddr,
			@RequestParam(required = false) String clusterName) {
		Collection<Option> options = consumerService.getOptionsForDeleteSubGroup();
		putPublicAttribute(map, "deleteSubGroup", options, request);
		try {
			if (request.getMethod().equals(GET)) {

			} else if (request.getMethod().equals(POST)) {
				checkOptions(options);
				consumerService.deleteSubGroup(groupName, brokerAddr, clusterName);
				putAlertTrue(map);
			} else {
				throwUnknowRequestMethodException(request);
			}
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/updateSubGroup.do", method = { RequestMethod.GET, RequestMethod.POST })
	public String updateSubGroup(ModelMap map, HttpServletRequest request,
			@RequestParam(required = false) String brokerAddr, @RequestParam(required = false) String clusterName,
			@RequestParam(required = false) String groupName, @RequestParam(required = false) String consumeEnable,
			@RequestParam(required = false) String consumeFromMinEnable,
			@RequestParam(required = false) String consumeBroadcastEnable,
			@RequestParam(required = false) String retryQueueNums,
			@RequestParam(required = false) String retryMaxTimes, @RequestParam(required = false) String brokerId,
			@RequestParam(required = false) String whichBrokerWhenConsumeSlowly) {
		Collection<Option> options = consumerService.getOptionsForUpdateSubGroup();
		putPublicAttribute(map, "updateSubGroup", options, request);
		try {
			if (request.getMethod().equals(GET)) {

			} else if (request.getMethod().equals(POST)) {
				checkOptions(options);
				consumerService.updateSubGroup(brokerAddr, clusterName, groupName, consumeEnable, consumeFromMinEnable,
						consumeBroadcastEnable, retryQueueNums, retryMaxTimes, brokerId, whichBrokerWhenConsumeSlowly);
				putAlertTrue(map);
			} else {
				throwUnknowRequestMethodException(request);
			}
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@Override
	protected String getName() {
		return "Consumer";
	}
}
