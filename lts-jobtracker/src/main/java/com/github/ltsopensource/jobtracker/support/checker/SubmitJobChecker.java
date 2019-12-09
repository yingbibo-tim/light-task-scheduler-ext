package com.github.ltsopensource.jobtracker.support.checker;

import com.github.ltsopensource.core.domain.Job;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.core.support.SystemClock;
import com.github.ltsopensource.jobtracker.domain.JobTrackerAppContext;
import com.github.ltsopensource.queue.domain.JobFinishPo;

/**
 * @author yingbibo
 * on 2019-12-04
 * email: yingbibo@canzhaoxi.com.cn
 * 校验任务是否需要提交
 *
 * eg.几天内重复任务不需要提交
 *
 * 在添加到watie_queue 判断一下,如果已经在时间范围内成功的 则不提交
 * 在添加到executing_job_queue再判断一下
 */
public class SubmitJobChecker {

	private static final Logger LOGGER = LoggerFactory.getLogger(SubmitJobChecker.class);
	private JobTrackerAppContext appContext;

	public SubmitJobChecker(JobTrackerAppContext appContext) {
		this.appContext = appContext;
	}


	public boolean checkJobNeedSubmit(Long jobDayRange,String taskTrackerNodeGroup,String taskTrackerSubNodeGroup,String taskId){
		boolean needAdd = true;
		if(jobDayRange!=null) {
			JobFinishPo jobFinishPo = appContext.getFinishJobQueue().getJob(taskTrackerNodeGroup,taskTrackerSubNodeGroup,taskId);
			if(jobFinishPo!=null) {
				long nowTimeStamp = SystemClock.now();
				long lastTimeStamp = jobFinishPo.getGmtModified();
				if( (nowTimeStamp-lastTimeStamp)/1000/60/60/24<jobDayRange){
					needAdd = false;
				}
			}
		}
		return needAdd;
	}

}
