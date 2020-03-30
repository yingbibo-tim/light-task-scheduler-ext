package com.github.ltsopensource.queue.support;

import com.github.ltsopensource.core.support.SystemClock;
import com.github.ltsopensource.queue.FinishJobQueue;
import com.github.ltsopensource.queue.JobStatQueue;
import com.github.ltsopensource.queue.domain.JobFinishPo;
import com.github.ltsopensource.queue.domain.JobStatPo;
import com.github.ltsopensource.queue.domain.JobStatType;

import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yingbibo
 * on 2019-12-09
 * email: yingbibo@canzhaoxi.com.cn
 */
public class JobStatUtils {


	private final static Long DAY_TIMESTAMP = 1000*60*60*24L-5*1000*60*60L;

	public static void changeJobStat(JobStatQueue jobStatQueue,FinishJobQueue finishJobQueue, String taskId,String taskTrackerNodeGroup,String taskTrackerSubNodeGroup,JobStatType jobStatType){
		List<JobStatPo> jobStatPoList = jobStatQueue.getJobs(taskTrackerNodeGroup,taskTrackerSubNodeGroup,taskId);
		for(JobStatPo jobStatPo:jobStatPoList){
			if(jobStatType.equals(JobStatType.FINISH)){
				addOrUpdateJobStat(jobStatQueue,jobStatPo.getTaskId(),jobStatPo.getTaskTrackerNodeGroup(),jobStatPo.getTaskTrackerSubNodeGroup(),jobStatPo.getServerFrom(),jobStatPo.getDayRange(),JobStatType.FINISH);
				continue;
			}
			if(checkJobNeedSubmit(finishJobQueue,taskId,taskTrackerNodeGroup,taskTrackerSubNodeGroup,jobStatPo.getDayRange())){
				addOrUpdateJobStat(jobStatQueue,jobStatPo.getTaskId(),jobStatPo.getTaskTrackerNodeGroup(),jobStatPo.getTaskTrackerSubNodeGroup(),jobStatPo.getServerFrom(),jobStatPo.getDayRange(), jobStatType);
			}else{
				addOrUpdateJobStat(jobStatQueue,jobStatPo.getTaskId(),jobStatPo.getTaskTrackerNodeGroup(),jobStatPo.getTaskTrackerSubNodeGroup(),jobStatPo.getServerFrom(),jobStatPo.getDayRange(),JobStatType.FINISH);
			}
		}
	}

	public static boolean checkJobNeedSubmit(FinishJobQueue finishJobQueue,String taskId,String taskTrackerNodeGroup,String taskTrackerSubNodeGroup,Long jobDayRange){
		boolean needAdd = true;
		if(jobDayRange!=null) {
			JobFinishPo jobFinishPo = finishJobQueue.getJob(taskTrackerNodeGroup,taskTrackerSubNodeGroup,taskId);
			if(jobFinishPo!=null) {
				long nowTimeStamp = SystemClock.now();
				long lastTimeStamp = jobFinishPo.getGmtModified();
				if( (nowTimeStamp-lastTimeStamp)/DAY_TIMESTAMP<jobDayRange){
					needAdd = false;
				}
			}
		}
		return needAdd;
	}

	public static boolean checkJobNeedSubmit(Long jobDayRange,Long finishTimeStamp){
		if(jobDayRange==null){
			return true;
		}
		long nowTimeStamp = SystemClock.now();
		return (nowTimeStamp - finishTimeStamp) / DAY_TIMESTAMP >= jobDayRange;
	}

	public static boolean addOrUpdateJobStat(JobStatQueue jobStatQueue,String taskId,String taskTrackerNodeGroup,String taskTrackerSubNodeGroup,String serverFrom,Long jobDayRange,JobStatType jobStatType){
		AtomicInteger incr = new AtomicInteger();
		while (incr.getAndIncrement()<10){
			JobStatPo jobStatPo =  jobStatQueue.getJob(taskTrackerNodeGroup,taskTrackerSubNodeGroup,serverFrom,taskId);
			if(jobStatPo!=null){
				if(jobStatQueue.update(jobStatPo.getId(),jobStatPo.getTaskTrackerNodeGroup(),jobDayRange, jobStatType.name(),jobStatPo.getJobStatType().name())){
					return true;
				}
			}else {
				jobStatPo = new JobStatPo();
				jobStatPo.setTaskId(taskId);
				jobStatPo.setTaskTrackerNodeGroup(taskTrackerNodeGroup);
				jobStatPo.setTaskTrackerSubNodeGroup(taskTrackerSubNodeGroup);
				jobStatPo.setServerFrom(serverFrom);
				jobStatPo.setDayRange(jobDayRange);
				jobStatPo.setGmtModified(SystemClock.now());
				jobStatPo.setJobStatType(jobStatType);
				jobStatQueue.add(jobStatPo);
				return true;
			}
		}
		return false;
	}

	/**
	 * 添加状态以后，要添加一个任务日志,
	 */
}
