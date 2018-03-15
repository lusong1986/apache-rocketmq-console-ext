package com.alibaba.rocketmq.action;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.alibaba.rocketmq.service.SysInfoService;

/**
 * 
 * @author lusong
 */
@Controller
@RequestMapping("/sysinfo")
public class SysInfoAction extends AbstractAction {

	@Autowired
	SysInfoService sysInfoService;

	@Override
	protected String getFlag() {
		return "sysinfo_flag";
	}

	@Override
	protected String getName() {
		return "Sysinfo";
	}

//	@RequestMapping(value = "/list.do", method = RequestMethod.GET)
//	public String list(ModelMap map) {
//		putPublicAttribute(map, "list");
//		try {
//			Table table = sysInfoService.list();
//			putTable(map, table);
//		} catch (Throwable t) {
//			putAlertMsg(t, map);
//		}
//		return TEMPLATE;
//	}

}
