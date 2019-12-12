package com.github.ltsopensource.queue.support;

import com.github.ltsopensource.queue.CronJobQueue;
import com.github.ltsopensource.queue.ExecutableJobQueue;
import com.github.ltsopensource.queue.RepeatJobQueue;
import com.github.ltsopensource.queue.domain.JobPo;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yingbibo
 * on 2019-12-09
 * email: yingbibo@canzhaoxi.com.cn
 */
public class JobComposeUtils {

	public final static Integer COUNT = 10;

	public static boolean composeExecutableJob(JobPo jobPo, ExecutableJobQueue executableJobQueue){
		AtomicInteger incr = new AtomicInteger();
		while (incr.getAndIncrement()< COUNT){
			JobPo existJobPo = executableJobQueue.getJob(jobPo.getTaskTrackerNodeGroup(),jobPo.getTaskTrackerSubNodeGroup(), jobPo.getTaskId());
			if(existJobPo!=null){
				jobPo.setIsRunning(existJobPo.isRunning());
				jobPo.compose(existJobPo);
				if(executableJobQueue.selectiveUpdateByJobIdAndLastGmtModified(jobPo,existJobPo.getGmtModified())){
					return Boolean.TRUE;
				}
			}else{
				break;
			}
		}
		return Boolean.FALSE;
	}

	public static boolean composeCronJob(JobPo jobPo, CronJobQueue cronJobQueue){
		AtomicInteger incr = new AtomicInteger();
		while (incr.getAndIncrement()< COUNT){
			JobPo existJobPo = cronJobQueue.getJob(jobPo.getTaskTrackerNodeGroup(),jobPo.getTaskTrackerSubNodeGroup(), jobPo.getTaskId());
			if(existJobPo!=null){
				jobPo.setIsRunning(existJobPo.isRunning());
				jobPo.compose(existJobPo);
				if(cronJobQueue.selectiveUpdateByJobIdAndLastGmtModified(jobPo,existJobPo.getGmtModified())){
					return Boolean.TRUE;
				}
			}else{
				break;
			}
		}
		return Boolean.FALSE;
	}

	public static boolean composeRepeatJob(JobPo jobPo, RepeatJobQueue repeatJobQueue){
		AtomicInteger incr = new AtomicInteger();
		while (incr.getAndIncrement()< COUNT){
			JobPo existJobPo = repeatJobQueue.getJob(jobPo.getTaskTrackerNodeGroup(),jobPo.getTaskTrackerSubNodeGroup(), jobPo.getTaskId());
			if(existJobPo!=null){
				jobPo.setIsRunning(existJobPo.isRunning());
				jobPo.compose(existJobPo);
				if(repeatJobQueue.selectiveUpdateByJobIdAndLastGmtModified(jobPo,existJobPo.getGmtModified())){
					return Boolean.TRUE;
				}
			}else{
				break;
			}
		}
		return Boolean.FALSE;
	}

}
