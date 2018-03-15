package com.alibaba.rocketmq.action;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.cli.Option;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.service.TopicService;

/**
 * 
 * @author lusong
 */
@Controller
@RequestMapping("/topic")
public class TopicAction extends AbstractAction {

	private static final SimpleDateFormat SFMT = new SimpleDateFormat("yyyyMMdd");

	@Autowired
	TopicService topicService;

	protected String getFlag() {
		return "topic_flag";
	}

	@Override
	protected String getName() {
		return "Topic";
	}

	@RequestMapping(value = "/list.do", method = RequestMethod.GET)
	public String list(ModelMap map) {
		putPublicAttribute(map, "list");
		try {
			Table table = topicService.list();
			putTable(map, table);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	/**
	 * 
	 * http://172.16.57.13:48080/rocketmq-console/topic/resendAllDLQ.do?topic=%25DLQ%
	 * 25eif-consumer-goutong&password=lusong1234520180115
	 * 
	 * 
	 * @param map
	 * @param topic
	 * @param password
	 * @return
	 */
	@RequestMapping(value = "/resendAllDLQ.do", method = RequestMethod.GET)
	public String resendAllDLQ(ModelMap map, @RequestParam String topic, @RequestParam String password) {
		putPublicAttribute(map, "resendAllDLQ");
		try {
			final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			if (password.equals("lusong12345" + sdf.format(new Date()))) {
				topicService.resendAllDLQMsgs(topic);
			} else {
				throw new RuntimeException("password is wrong");
			}
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/stats.do", method = RequestMethod.GET)
	public String stats(ModelMap map, @RequestParam String topic) {
		putPublicAttribute(map, "stats");
		try {
			Table table = topicService.stats(topic);
			putTable(map, table);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/dealDLQ.do", method = RequestMethod.GET)
	public String dealDLQ(ModelMap map, @RequestParam String topic, @RequestParam String pageNum) {
		putPublicAttribute(map, "dealDLQ");
		try {
			int num = NumberUtils.toInt(pageNum, 1);
			if (num <= 0) {
				num = 1;
			}

			Table table = topicService.dealDLQ(topic, num);

			map.put("topicName", topic);
			map.put("pageNum", num);
			putTable(map, table);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/resendmsg.do", method = RequestMethod.GET)
	public String resendmsg(ModelMap map, @RequestParam String msgId) {
		putPublicAttribute(map, "resendmsg");
		try {
			Table table = topicService.resend(msgId);
			putTable(map, table);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/add.do", method = RequestMethod.GET)
	public String add(ModelMap map) {
		putPublicAttribute(map, "add");
		Collection<Option> options = topicService.getOptionsForUpdate();
		putOptions(map, options);
		map.put(FORM_ACTION, "update.do");// add as update
		return TEMPLATE;
	}

	@RequestMapping(value = "/queryMessages.do", method = RequestMethod.GET)
	public String queryMessages(ModelMap map, @RequestParam String topic, @RequestParam String pageNum,
			@RequestParam(required = false) String dayBefore, @RequestParam(required = false) String day) {
		putPublicAttribute(map, "queryMessages");
		try {
			int num = NumberUtils.toInt(pageNum, 1);
			if (num <= 0) {
				num = 1;
			}

			int dayBeforeToday = 0;
			if (!StringUtils.isEmpty(day)) {
				Date date = SFMT.parse(day);

				Calendar dayCalendar = Calendar.getInstance();
				int today = dayCalendar.get(Calendar.DAY_OF_YEAR);
				final int nowYear = dayCalendar.get(Calendar.YEAR);

				dayCalendar.setTime(date);
				dayBeforeToday = today - dayCalendar.get(Calendar.DAY_OF_YEAR);
				final int finalYear = dayCalendar.get(Calendar.YEAR);
				if (nowYear - finalYear > 0) {
					for (int y = 0; y < nowYear - finalYear; y++) {
						dayCalendar.set(Calendar.YEAR, y + finalYear);
						dayBeforeToday += dayCalendar.getActualMaximum(Calendar.DAY_OF_YEAR);
					}
				}
			} else {
				dayBeforeToday = NumberUtils.toInt(dayBefore, 0); // 0表示今天
			}

			if (dayBeforeToday <= 0) {
				dayBeforeToday = 0;
			}

			final AtomicInteger msgCounter = new AtomicInteger();
			Table table = topicService.queryMessages(topic, num, dayBeforeToday, msgCounter);

			map.put("count", msgCounter.intValue());
			map.put("topicName", topic);
			map.put("pageNum", num);
			map.put("dayBefore", dayBeforeToday);
			map.put("day", SFMT.format(DateUtils.addDays(new Date(), dayBeforeToday * -1)));
			putTable(map, table);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/route.do", method = RequestMethod.GET)
	public String route(ModelMap map, @RequestParam String topic) {
		putPublicAttribute(map, "route");
		try {
			TopicRouteData topicRouteData = topicService.route(topic);
			map.put("topicRouteData", topicRouteData);
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/delete.do", method = { RequestMethod.GET, RequestMethod.POST })
	public String delete(ModelMap map, HttpServletRequest request, @RequestParam(required = false) String clusterName,
			@RequestParam String topic) {
		Collection<Option> options = topicService.getOptionsForDelete();
		putPublicAttribute(map, "delete", options, request);
		try {
			if (request.getMethod().equals(GET)) {

			} else if (request.getMethod().equals(POST)) {
				checkOptions(options);
				topicService.delete(topic, clusterName);
				putAlertTrue(map);
			} else {
				throwUnknowRequestMethodException(request);
			}
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}
		return TEMPLATE;
	}

	@RequestMapping(value = "/update.do", method = { RequestMethod.GET, RequestMethod.POST })
	public String update(ModelMap map, HttpServletRequest request, @RequestParam String topic,
			@RequestParam(required = false) String readQueueNums,
			@RequestParam(required = false) String writeQueueNums, @RequestParam(required = false) String perm,
			@RequestParam(required = false) String brokerAddr, @RequestParam(required = false) String clusterName,
			@RequestParam(required = false) String order) {
		Collection<Option> options = topicService.getOptionsForUpdate();
		putPublicAttribute(map, "update", options, request);
		try {
			if (request.getMethod().equals(GET)) {

			} else if (request.getMethod().equals(POST)) {
				checkOptions(options);
				topicService.update(topic, readQueueNums, writeQueueNums, perm, brokerAddr, clusterName, order);
				putAlertTrue(map);
			} else {
				throwUnknowRequestMethodException(request);
			}
		} catch (Throwable t) {
			putAlertMsg(t, map);
		}

		return TEMPLATE;
	}

}
