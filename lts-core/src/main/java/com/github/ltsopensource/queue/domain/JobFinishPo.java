package com.github.ltsopensource.queue.domain;

import java.io.Serializable;

/**
 * @author yingbibo
 * on 2019-12-04
 * email: yingbibo@canzhaoxi.com.cn
 */
public class JobFinishPo implements Serializable {


	private static final long serialVersionUID = 6946029075023612544L;
	private Long id;
	private String taskId;
	private String taskTrackerNodeGroup;
	private String taskTrackerSubNodeGroup;
	private Long gmtModified;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public String getTaskTrackerNodeGroup() {
		return taskTrackerNodeGroup;
	}

	public void setTaskTrackerNodeGroup(String taskTrackerNodeGroup) {
		this.taskTrackerNodeGroup = taskTrackerNodeGroup;
	}

	public String getTaskTrackerSubNodeGroup() {
		return taskTrackerSubNodeGroup;
	}

	public void setTaskTrackerSubNodeGroup(String taskTrackerSubNodeGroup) {
		this.taskTrackerSubNodeGroup = taskTrackerSubNodeGroup;
	}

	public Long getGmtModified() {
		return gmtModified;
	}

	public void setGmtModified(Long gmtModified) {
		this.gmtModified = gmtModified;
	}
}


