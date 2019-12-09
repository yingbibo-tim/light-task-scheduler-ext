package com.github.ltsopensource.jobtracker.sender;

import com.github.ltsopensource.biz.logger.domain.JobLogPo;
import com.github.ltsopensource.biz.logger.domain.LogType;
import com.github.ltsopensource.core.constant.Level;
import com.github.ltsopensource.core.json.JSON;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.core.support.JobDomainConverter;
import com.github.ltsopensource.core.support.SystemClock;
import com.github.ltsopensource.jobtracker.domain.JobTrackerAppContext;
import com.github.ltsopensource.queue.domain.JobPo;
import com.github.ltsopensource.queue.domain.JobStatPo;
import com.github.ltsopensource.queue.domain.JobStatType;
import com.github.ltsopensource.queue.support.JobStatUtils;
import com.github.ltsopensource.store.jdbc.exception.DupEntryException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Robert HG (254963746@qq.com) on 11/11/15.
 */
public class JobSender {

    private final Logger LOGGER = LoggerFactory.getLogger(JobSender.class);

    private JobTrackerAppContext appContext;

    public JobSender(JobTrackerAppContext appContext) {
        this.appContext = appContext;
    }

    public SendResult send(String taskTrackerNodeGroup,String taskTrackerSubNodeGroup,String taskTrackerIdentity, int size, SendInvoker invoker) {

        List<JobPo> jobPos = fetchJob(taskTrackerNodeGroup,taskTrackerSubNodeGroup, taskTrackerIdentity, size);

        List<JobStatPo> jobStatNeedUpdatePoList = new ArrayList<>();
        Iterator<JobPo> it = jobPos.iterator();
        while(it.hasNext()){
            boolean needSubmit = false;
            JobPo jobPo = it.next();
            List<JobStatPo> jobStatPoList = appContext.getJobStatQueue().getJobs(jobPo.getTaskTrackerNodeGroup(),jobPo.getTaskTrackerSubNodeGroup(),jobPo.getTaskId());
            if(jobStatPoList.size()>0) {
                for (JobStatPo jobStatPo : jobStatPoList) {
                    if(!JobStatUtils.checkJobNeedSubmit(appContext.getFinishJobQueue(),jobStatPo.getTaskId(),jobStatPo.getTaskTrackerNodeGroup(),jobPo.getTaskTrackerSubNodeGroup(),jobStatPo.getDayRange())){
                        JobStatUtils.addOrUpdateJobStat(appContext.getJobStatQueue(),jobStatPo.getTaskId(),jobStatPo.getTaskTrackerNodeGroup(),jobStatPo.getTaskTrackerSubNodeGroup(),jobStatPo.getServerFrom(),jobStatPo.getDayRange(),JobStatType.FINISH);
                    } else {
                        jobStatNeedUpdatePoList.add(jobStatPo);
                        needSubmit = true;
                    }
                }
            }else{
                needSubmit = true;
            }
            if(!needSubmit){
                it.remove();
            }
        }

        if (jobPos.size() == 0) {
            return new SendResult(false, JobPushResult.NO_JOB);
        }

        SendResult sendResult = invoker.invoke(jobPos);

        if (sendResult.isSuccess()) {
            List<JobLogPo> jobLogPos = new ArrayList<JobLogPo>(jobPos.size());
            for (JobPo jobPo : jobPos) {
                // 记录日志
                JobLogPo jobLogPo = JobDomainConverter.convertJobLog(jobPo);
                jobLogPo.setSuccess(true);
                jobLogPo.setLogType(LogType.SENT);
                jobLogPo.setLogTime(SystemClock.now());
                jobLogPo.setLevel(Level.INFO);
                jobLogPos.add(jobLogPo);
            }
            appContext.getJobLogger().log(jobLogPos);
            for(JobStatPo jobStatPo:jobStatNeedUpdatePoList){
                //更新状态
                JobStatUtils.changeJobStat(appContext.getJobStatQueue(),appContext.getFinishJobQueue(),jobStatPo.getTaskId(),jobStatPo.getTaskTrackerNodeGroup(),jobStatPo.getTaskTrackerSubNodeGroup(),JobStatType.RUNNING);            }
        }
        return sendResult;
    }

    private List<JobPo> fetchJob(String taskTrackerNodeGroup,String taskTrackerSubNodeGroup,String taskTrackerIdentity, int size) {
        List<JobPo> jobPos = new ArrayList<JobPo>(size);

        for (int i = 0; i < size; i++) {
            // 从mongo或者mysql里面 中取一个可运行的job
            final JobPo jobPo = appContext.getPreLoader().take(taskTrackerNodeGroup,taskTrackerSubNodeGroup, taskTrackerIdentity);
            if (jobPo == null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Job push failed: no job! nodeGroup=" + taskTrackerNodeGroup + ", identity=" + taskTrackerIdentity);
                }
                break;
            }

            // IMPORTANT: 这里要先切换队列
            try {
                //添加到执行任务队列里面
                appContext.getExecutingJobQueue().add(jobPo);
            } catch (DupEntryException e) {
                LOGGER.warn("ExecutingJobQueue already exist:" + JSON.toJSONString(jobPo));
                appContext.getExecutableJobQueue().resume(jobPo);
                continue;
            }
            //移除等待队列
            appContext.getExecutableJobQueue().remove(jobPo.getTaskTrackerNodeGroup(), jobPo.getJobId());

            jobPos.add(jobPo);
        }
        return jobPos;
    }

    public interface SendInvoker {
        SendResult invoke(List<JobPo> jobPos);
    }

    public static class SendResult {
        private boolean success;
        private Object returnValue;

        public SendResult(boolean success, Object returnValue) {
            this.success = success;
            this.returnValue = returnValue;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public Object getReturnValue() {
            return returnValue;
        }

        public void setReturnValue(Object returnValue) {
            this.returnValue = returnValue;
        }
    }

}
