package com.github.ltsopensource.jobtracker.support.checker;

import com.github.ltsopensource.core.factory.NamedThreadFactory;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.jobtracker.domain.JobTrackerAppContext;
import com.github.ltsopensource.jobtracker.support.ClientNotifier;
import com.github.ltsopensource.queue.domain.JobFinishPo;
import com.github.ltsopensource.queue.domain.JobStatPo;
import com.github.ltsopensource.queue.domain.JobStatType;
import com.github.ltsopensource.queue.support.JobStatUtils;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author yingbibo
 * on 2019-12-12
 * email: yingbibo@canzhaoxi.com.cn
 *
 * 任务状态修复检测
 */
public class JobStatChecker {

	private static final Logger LOGGER = LoggerFactory.getLogger(JobStatChecker.class);

	private ScheduledExecutorService JOB_STAT_EXECUTOR_SERVICE = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("LTS-JobStat-Executor", true));
	private ScheduledFuture<?> scheduledFuture;
	private AtomicBoolean start = new AtomicBoolean(false);
	private JobTrackerAppContext appContext;

	public JobStatChecker(JobTrackerAppContext appContext) {
		this.appContext = appContext;
	}

	public void start(){
		try {
			if(start.compareAndSet(false,true)){
				scheduledFuture = JOB_STAT_EXECUTOR_SERVICE.scheduleWithFixedDelay(new Runnable() {
					@Override
					public void run() {
						try {
							checkJobStat();
						} catch (Throwable t) {
							LOGGER.error(t.getMessage(), t);
						}
					}
				}, 30,60, TimeUnit.SECONDS);
			}
			LOGGER.info("JobStat checker started!");
		} catch (Throwable t) {
			LOGGER.error("JobStat checker start failed!",t);

		}
	}

	public void stop() {
		try {
			if (start.compareAndSet(true, false)) {
				scheduledFuture.cancel(true);
				JOB_STAT_EXECUTOR_SERVICE.shutdown();
			}
			LOGGER.info("JobStat checker stopped!");
		} catch (Throwable t) {
			LOGGER.error("JobStat checker stop failed!", t);
		}
	}

	private void checkJobStat(){
		Set<String> groupNodeSet = appContext.getTaskTrackerManager().getNodeGroups();
		for(String taskTrackerNodeGroup:groupNodeSet) {
			int page = 1;
			int pageSize = 1000;
			boolean hasNextPage = true;
			while(hasNextPage) {
				int start = (page-1)*pageSize;
				List<JobFinishPo> jobFinishPoList = appContext.getFinishJobQueue().getJobs(taskTrackerNodeGroup,start,pageSize);
				if(jobFinishPoList.size()==0){
					hasNextPage = false;
					continue;
				}
				for(JobFinishPo jobFinishPo:jobFinishPoList){
					updateJobStat(jobFinishPo);
				}
				page++;
			}
		}
	}

	private void updateJobStat(JobFinishPo jobFinishPo){
		List<JobStatPo> jobStatPoList = appContext.getJobStatQueue().getJobs(jobFinishPo.getTaskTrackerNodeGroup(),jobFinishPo.getTaskTrackerSubNodeGroup(),jobFinishPo.getTaskId());
		for(JobStatPo jobStatPo:jobStatPoList){
			if(!JobStatUtils.checkJobNeedSubmit(jobStatPo.getDayRange(),jobFinishPo.getGmtModified())){
				JobStatUtils.addOrUpdateJobStat(appContext.getJobStatQueue(),jobStatPo.getTaskId(),jobStatPo.getTaskTrackerNodeGroup(),jobStatPo.getTaskTrackerSubNodeGroup(),jobStatPo.getServerFrom(),jobStatPo.getDayRange(), JobStatType.FINISH);
			}
		}

	}



}
