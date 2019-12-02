package com.github.ltsopensource.core.protocol.command;

/**
 * 取消(删除)任务
 * @author Robert HG (254963746@qq.com) on 11/7/15.
 */
public class JobCancelRequest extends AbstractRemotingCommandBody{

	private static final long serialVersionUID = 2945964772160028674L;

	private String taskId;

    private String taskTrackerNodeGroup;

    private String taskTrackerSubNodeGroup;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskTrackerNodeGroup() {
        return taskTrackerNodeGroup;
    }

    public String getTaskTrackerSubNodeGroup() {
        return taskTrackerSubNodeGroup;
    }

    public void setTaskTrackerSubNodeGroup(String taskTrackerSubNodeGroup) {
        this.taskTrackerSubNodeGroup = taskTrackerSubNodeGroup;
    }

    public void setTaskTrackerNodeGroup(String taskTrackerNodeGroup) {
        this.taskTrackerNodeGroup = taskTrackerNodeGroup;
    }
}
