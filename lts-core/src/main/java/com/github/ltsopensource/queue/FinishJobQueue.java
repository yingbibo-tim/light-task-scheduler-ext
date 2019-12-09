package com.github.ltsopensource.queue;

import com.github.ltsopensource.queue.domain.JobFeedbackPo;
import com.github.ltsopensource.queue.domain.JobFinishPo;
import com.github.ltsopensource.queue.domain.JobPo;

import java.util.List;

/**
 * @author yingbibo
 * on 2019-12-04
 * email: yingbibo@canzhaoxi.com.cn
 *
 * 已经成功完成的任务记录时间
 */
public interface FinishJobQueue {

	/**
	 * 创建表
	 * @param taskTrackerNodeGroup 表名
	 * @return 创建是否成功
	 */
	boolean createQueue(String taskTrackerNodeGroup);

	/**
	 * 添加
	 * @param jobFinishPo 对象
	 * @return
	 */
	boolean add(JobFinishPo jobFinishPo);
	JobFinishPo getJob(String taskTrackerNodeGroup,String taskTrackerSubNodeGroup,String taskId);
	void update(String taskTrackerNodeGroup,Long id);

}
