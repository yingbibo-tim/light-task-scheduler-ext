package com.github.ltsopensource.core.protocol.command;

/**
 * Job pull request
 * Robert HG (254963746@qq.com) on 3/25/15.
 */
public class JobPullRequest extends AbstractRemotingCommandBody {

	private static final long serialVersionUID = 9222159289387747395L;

	private String taskTrackerNodeGroup;

	private String taskTrackerSubNodeGroup;
	
	private Integer availableThreads;

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

	public Integer getAvailableThreads() {
        return availableThreads;
    }

    public void setAvailableThreads(Integer availableThreads) {
        this.availableThreads = availableThreads;
    }

}
