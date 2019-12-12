package com.github.ltsopensource.queue.mysql;

import com.github.ltsopensource.admin.request.JobQueueReq;
import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.core.support.JobQueueUtils;
import com.github.ltsopensource.core.support.SystemClock;
import com.github.ltsopensource.queue.FinishJobQueue;
import com.github.ltsopensource.queue.JobQueue;
import com.github.ltsopensource.queue.domain.JobFinishPo;
import com.github.ltsopensource.queue.mysql.support.RshHolder;
import com.github.ltsopensource.store.jdbc.JdbcAbstractAccess;
import com.github.ltsopensource.store.jdbc.builder.InsertSql;
import com.github.ltsopensource.store.jdbc.builder.SelectSql;
import com.github.ltsopensource.store.jdbc.builder.UpdateSql;

import java.util.List;

/**
 * @author yingbibo
 * on 2019-12-04
 * email: yingbibo@canzhaoxi.com.cn
 */
public class MysqlFinishJobQueue extends JdbcAbstractAccess implements FinishJobQueue {

	MysqlFinishJobQueue(Config config) {
		super(config);
	}

	@Override
	public boolean createQueue(String taskTrackerNodeGroup) {
		createTable(readSqlFile("sql/mysql/lts_job_finish_queue.sql", getTableName(taskTrackerNodeGroup)));
		return true;
	}

	@Override
	public boolean  add(JobFinishPo jobFinishPo) {
		new InsertSql(getSqlTemplate())
				.insertIgnore(getTableName(jobFinishPo.getTaskTrackerNodeGroup()))
				.columns("task_id","task_tracker_node_group","task_tracker_sub_node_group","gmt_modified")
				.values(jobFinishPo.getTaskId(),jobFinishPo.getTaskTrackerNodeGroup(),jobFinishPo.getTaskTrackerSubNodeGroup(),jobFinishPo.getGmtModified())
				.doInsert();
		return true;
	}

	@Override
	public JobFinishPo getJob(String taskTrackerNodeGroup, String taskTrackerSubNodeGroup, String taskId) {
		return new SelectSql(getSqlTemplate())
				.select()
				.all()
				.from()
				.table(getTableName(taskTrackerNodeGroup))
				.where("task_id = ?", taskId)
				.and("task_tracker_node_group = ?", taskTrackerNodeGroup)
				.and("task_tracker_sub_node_group = ?",taskTrackerSubNodeGroup)
				.single(RshHolder.JOB_FINISH_PO_RSH);
	}

	@Override
	public List<JobFinishPo> getJobs(String taskTrackerNodeGroup,int start, int size) {
		return new SelectSql(getSqlTemplate())
				.select()
				.all()
				.from()
				.table(getTableName(taskTrackerNodeGroup))
				.limit(start,size)
				.list(RshHolder.JOB_FINISH_PO_LIST_RSH);
	}

	@Override
	public void update(String taskTrackerNodeGroup,Long id) {
		new UpdateSql(getSqlTemplate())
				.update()
				.table(getTableName(taskTrackerNodeGroup))
				.set("gmt_modified", SystemClock.now())
				.where("id=?", id)
				.doUpdate();
	}

	private String getTableName(String taskTrackerNodeGroup) {
		return JobQueueUtils.getFinishJobQueueName(taskTrackerNodeGroup);
	}
}
