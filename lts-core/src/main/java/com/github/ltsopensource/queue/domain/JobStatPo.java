package com.github.ltsopensource.queue.domain;

/**
 * @author yingbibo
 * on 2019-12-04
 * email: yingbibo@canzhaoxi.com.cn
 * 状态
 */
public class JobStatPo implements java.io.Serializable{

	private static final long serialVersionUID = -3670530214482010256L;
	private Long id;

	private String taskId;

	private String taskTrackerNodeGroup;

	private String taskTrackerSubNodeGroup;

	/**
	 * 请求来源
	 */
	private String serverFrom;

	private Long gmtModified;

	private Long dayRange;

	private JobStatType jobStatType;

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

	public Long getDayRange() {
		return dayRange;
	}

	public void setDayRange(Long dayRange) {
		this.dayRange = dayRange;
	}

	public Long getGmtModified() {
		return gmtModified;
	}

	public void setGmtModified(Long gmtModified) {
		this.gmtModified = gmtModified;
	}

	public String getServerFrom() {
		return serverFrom;
	}

	public void setServerFrom(String serverFrom) {
		this.serverFrom = serverFrom;
	}

	public JobStatType getJobStatType() {
		return jobStatType;
	}

	public void setJobStatType(JobStatType jobStatType) {
		this.jobStatType = jobStatType;
	}

	@Override
	public String toString() {
		return "JobStatPo{" +
				"id=" + id +
				", taskId='" + taskId + '\'' +
				", taskTrackerNodeGroup='" + taskTrackerNodeGroup + '\'' +
				", taskTrackerSubNodeGroup='" + taskTrackerSubNodeGroup + '\'' +
				", serverFrom='" + serverFrom + '\'' +
				", gmtModified=" + gmtModified +
				", dayRange=" + dayRange +
				", jobStatType=" + jobStatType +
				'}';
	}
}
