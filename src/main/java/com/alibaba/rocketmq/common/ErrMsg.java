package com.alibaba.rocketmq.common;

public class ErrMsg {

	private String errMsg;

	private int code;

	public ErrMsg(String errMsg, int code) {
		super();
		this.errMsg = errMsg;
		this.code = code;
	}

	public String getErrMsg() {
		return errMsg;
	}

	public void setErrMsg(String errMsg) {
		this.errMsg = errMsg;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

}
