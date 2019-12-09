package com.github.ltsopensource.queue;

import com.github.ltsopensource.queue.domain.JobStatPo;
import com.github.ltsopensource.queue.domain.JobStatType;

import java.util.List;

/**
 * @author yingbibo
 * on 2019-12-04
 * email: yingbibo@canzhaoxi.com.cn
 */
public interface JobStatQueue {

	/**
	 * 创建表
	 * @param taskTrackerNodeGroup 表名
	 * @return 创建是否成功
	 */
	boolean createQueue(String taskTrackerNodeGroup);

	/**
	 * 添加
	 * @param jobStatPo 对象
	 * @return
	 */
	boolean add(JobStatPo jobStatPo);
	JobStatPo getJob(String taskTrackerNodeGroup,String taskTrackerSubNodeGroup,String serverFrom,String taskId);
	List<JobStatPo> getJobs(String taskTrackerNodeGroup, String taskTrackerSubNodeGroup, String taskId);
	boolean update(long id,String taskTrackerNodeGroup,Long dayRange,String jobStat,String oldJobStat);
}
