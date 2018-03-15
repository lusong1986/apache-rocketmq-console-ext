package com.alibaba.rocketmq.action;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.cli.Option;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.common.ErrMsg;
import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.service.MessageService;
import com.google.common.collect.Lists;

/**
 * 
 * @author lusong
 */
@Controller
@RequestMapping("/message")
public class MessageAction extends AbstractAction {

	@Autowired
	MessageService messageService;

	protected String getFlag() {
		return "message_flag";
	}

	@RequestMapping(value = "/dlqStatistic.do", method = RequestMethod.GET)
	public String dlqStatistic(ModelMap map) {
		putPublicAttribute(map, "dlqStatistic");
		try {
			Table table = messageService.dlqStatistic();
			putTable(map, table);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/msgPutStatistic.do", method = RequestMethod.GET)
	public String msgPutStatistic(ModelMap map) {
		putPublicAttribute(map, "msgPutStatistic");
		try {
			Table table = messageService.msgPutStatistic();
			putTable(map, table);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/msgGetStatistic.do", method = RequestMethod.GET)
	public String msgGetStatistic(ModelMap map) {
		putPublicAttribute(map, "msgGetStatistic");
		try {
			Table table = messageService.msgGetStatistic();
			putTable(map, table);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/topicStatistic.do", method = RequestMethod.GET)
	public String topicStatistic(ModelMap map) {
		putPublicAttribute(map, "topicStatistic");
		try {
			Table table = messageService.topicStatistic();
			putTable(map, table);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/unHandlePrepareMsg.do", method = RequestMethod.GET)
	public String unHandlePrepareMsgToday(ModelMap map, @RequestParam String pageNum) {
		putPublicAttribute(map, "unHandlePrepareMsg");
		try {
			int num = NumberUtils.toInt(pageNum, 1);
			if (num <= 0) {
				num = 1;
			}

			Table table = messageService.unHandlePrepareMsgToday(num);
			map.put("pageNum", num);
			putTable(map, table);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/sendFailedMsgs.do", method = RequestMethod.GET)
	public String sendFailedMsgs(ModelMap map, @RequestParam String dayBefore, @RequestParam String pageNum) {
		putPublicAttribute(map, "sendFailedMsgs");
		try {
			int dayBeforeToday = NumberUtils.toInt(dayBefore, 0); // 0表示今天
			if (dayBeforeToday <= 0) {
				dayBeforeToday = 0;
			}

			int pNum = NumberUtils.toInt(pageNum, 1);
			if (pNum <= 0) {
				pNum = 1;
			}

			Table table = messageService.sendFailedMsgs(dayBeforeToday, pNum);
			map.put("dayBefore", dayBeforeToday);
			map.put("pageNum", pNum);
			putTable(map, table);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/resendMsg.do", method = RequestMethod.GET)
	public String resendMsg(ModelMap map, @RequestParam String msgId) {
		putPublicAttribute(map, "resendMsg");
		try {
			Table table = messageService.resendMsg(msgId);

			putTable(map, table);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/unHandlePrepareMsgYesterday.do", method = RequestMethod.GET)
	public String unHandlePrepareMsgYesterday(ModelMap map, @RequestParam String pageNum) {
		putPublicAttribute(map, "unHandlePrepareMsgYesterday");
		try {
			int num = NumberUtils.toInt(pageNum, 1);
			if (num <= 0) {
				num = 1;
			}

			Table table = messageService.unHandlePrepareMsgYesterday(num);
			map.put("pageNum", num);
			putTable(map, table);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/commitTran.do", method = RequestMethod.GET)
	public String commitTran(ModelMap map, @RequestParam String msgId) {
		putPublicAttribute(map, "commitTran");
		try {
			Table table = messageService.commitTran(msgId);

			putTable(map, table);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/rollbackTran.do", method = RequestMethod.GET)
	public String rollbackTran(ModelMap map, @RequestParam String msgId) {
		putPublicAttribute(map, "rollbackTran");
		try {
			Table table = messageService.rollbackTran(msgId);

			putTable(map, table);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/queryMsgById.do", method = { RequestMethod.GET, RequestMethod.POST })
	public String queryMsgById(ModelMap map, HttpServletRequest request, @RequestParam(required = false) String msgId) {
		Option opt = new Option("i", "msgId", true, "Message Id");
		opt.setRequired(true);

		Collection<Option> options = Lists.asList(opt, new Option[0]);
		putPublicAttribute(map, "queryMsgById", options, request);

		try {
			if (request.getMethod().equals(GET)) {

			} else if (request.getMethod().equals(POST)) {
				checkOptions(options);
				Table table = messageService.queryMsgById(msgId);
				putTable(map, table);
			} else {
				throwUnknowRequestMethodException(request);
			}
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/queryMsgByIdInJson.do", method = { RequestMethod.GET, RequestMethod.POST })
	public void queryMsgByIdInJson(HttpServletRequest request, HttpServletResponse response,
			@RequestParam(required = false) String msgId) {
		PrintWriter out = null;
		try {
			response.setCharacterEncoding("UTF-8");
			response.setContentType("application/json; charset=utf-8");
			response.setDateHeader("Expires", 0);
			response.setHeader("Cache-Control", "no-cache");
			response.setHeader("Pragma", "no-cache");

			out = response.getWriter();
			out.append(JSON.toJSONString(messageService.queryMsgByIdInJSON(msgId)));
		} catch (Throwable e) {
			out.append(JSON.toJSONString(new ErrMsg("queryMsgByIdInJson failed " + e.getMessage(), 0)));
			e.printStackTrace();
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

	@RequestMapping(value = "/queryMsgByKey.do", method = { RequestMethod.GET, RequestMethod.POST })
	public String queryMsgByKey(ModelMap map, HttpServletRequest request, @RequestParam(required = false) String topic,
			@RequestParam(required = false) String msgKey, @RequestParam(required = false) String fallbackHours) {
		Collection<Option> options = messageService.getOptionsForQueryMsgByKey();
		putPublicAttribute(map, "queryMsgByKey", options, request);
		try {
			if (request.getMethod().equals(GET)) {

			} else if (request.getMethod().equals(POST)) {
				checkOptions(options);
				Table table = messageService.queryMsgByKey(topic, msgKey, fallbackHours);
				putTable(map, table);
			} else {
				throwUnknowRequestMethodException(request);
			}
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/queryMsgByOffset.do", method = { RequestMethod.GET, RequestMethod.POST })
	public String queryMsgByOffset(ModelMap map, HttpServletRequest request,
			@RequestParam(required = false) String topic, @RequestParam(required = false) String brokerName,
			@RequestParam(required = false) String queueId, @RequestParam(required = false) String offset) {
		Collection<Option> options = messageService.getOptionsForQueryMsgByOffset();
		putPublicAttribute(map, "queryMsgByOffset", options, request);
		try {
			if (request.getMethod().equals(GET)) {

			} else if (request.getMethod().equals(POST)) {
				checkOptions(options);
				Table table = messageService.queryMsgByOffset(topic, brokerName, queueId, offset);
				putTable(map, table);
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
		return "Message";
	}

	@RequestMapping(value = "/produce.do", method = { RequestMethod.GET, RequestMethod.POST })
	public String produce(ModelMap map, HttpServletRequest request, @RequestParam(required = false) String topic,
			@RequestParam(required = false) String body, @RequestParam(required = false) String tags,
			@RequestParam(required = false) String keys, @RequestParam(required = false) String properties) {
		Collection<Option> options = messageService.getOptionsForProduce();
		putPublicAttribute(map, "produce", options, request);
		try {
			if (request.getMethod().equals(GET)) {

			} else if (request.getMethod().equals(POST)) {
				checkOptions(options);
				SendResult sendResult = messageService.produce(topic, body, tags, keys, properties);
				putAlertMsg(sendResult.toString(), map);
			} else {
				throwUnknowRequestMethodException(request);
			}
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}

		return TEMPLATE;
	}

	@RequestMapping(value = "/consume.do", method = { RequestMethod.GET, RequestMethod.POST })
	public String consume(ModelMap map, HttpServletRequest request, @RequestParam(required = false) String topic,
			@RequestParam(required = false) String expression, @RequestParam(required = false) String consume) {
		Collection<Option> options = messageService.getOptionsForConsume();
		putPublicAttribute(map, "consume", options, request);
		try {
			if (request.getMethod().equals(GET)) {

			} else if (request.getMethod().equals(POST)) {
				checkOptions(options);
				boolean needConsumeTemp = true;
				if (StringUtils.isNotBlank(consume) && consume.trim().equalsIgnoreCase("false")) {
					needConsumeTemp = false;
				}
				List<MessageExt> messages = messageService.consume(topic, expression, needConsumeTemp);
				putAlertMsg(messages.toString(), map);
			} else {
				throwUnknowRequestMethodException(request);
			}
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}

		return TEMPLATE;
	}

}
