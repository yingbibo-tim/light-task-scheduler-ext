package com.github.ltsopensource.queue.domain;

import com.github.ltsopensource.core.commons.utils.StringUtils;

/**
 * @author yingbibo
 * on 2019-12-04
 * email: yingbibo@canzhaoxi.com.cn
 */
public enum JobStatType {
	WAIT("wait"),
	RUNNING("running"),
	FINISH("finish"),
	FAIL("fail");


	public static JobStatType get(String name){
		if(!StringUtils.isEmpty(name)) {
			JobStatType[] enums = JobStatType.values();
			for (JobStatType jobStatType : enums) {
				if (name.equals(jobStatType.name())) {
					return jobStatType;
				}
			}
		}
		return null;
	}

	private String desc;

	JobStatType(String desc) {
		this.desc = desc;
	}
}
