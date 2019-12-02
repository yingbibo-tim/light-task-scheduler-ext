package com.github.ltsopensource.tasktracker.cmd;

import com.github.ltsopensource.cmd.HttpCmdProc;
import com.github.ltsopensource.cmd.HttpCmdRequest;
import com.github.ltsopensource.cmd.HttpCmdResponse;
import com.github.ltsopensource.core.cmd.HttpCmdNames;
import com.github.ltsopensource.core.commons.utils.StringUtils;
import com.github.ltsopensource.tasktracker.domain.TaskTrackerAppContext;

/**
 * 用于中断某个Job
 * @author Robert HG (254963746@qq.com) on 3/13/16.
 */
public class JobTerminateCmd implements HttpCmdProc {

    private TaskTrackerAppContext appContext;

    public JobTerminateCmd(TaskTrackerAppContext appContext) {
        this.appContext = appContext;
    }

    @Override
    public String nodeIdentity() {
        return appContext.getConfig().getIdentity();
    }

    @Override
    public String getCommand() {
        return HttpCmdNames.HTTP_CMD_JOB_TERMINATE;
    }

    @Override
    public HttpCmdResponse execute(HttpCmdRequest request) throws Exception {

        String jobId = request.getParam("jobId");
        String taskTrackerSubNodeGroup = request.getParam("subNodeGroup");
        if (StringUtils.isEmpty(jobId)) {
            return HttpCmdResponse.newResponse(false, "jobId can't be empty");
        }
        if(StringUtils.isEmpty(taskTrackerSubNodeGroup)){
            return HttpCmdResponse.newResponse(false, "taskTrackerSubNodeGroup can't be empty");

        }

        if (!appContext.getRunnerPool(taskTrackerSubNodeGroup).getRunningJobManager().running(jobId)) {
            return HttpCmdResponse.newResponse(false, "jobId dose not running in this TaskTracker now");
        }

        appContext.getRunnerPool(taskTrackerSubNodeGroup).getRunningJobManager().terminateJob(jobId);

        return HttpCmdResponse.newResponse(true, "Execute terminate Command success");
    }
}
