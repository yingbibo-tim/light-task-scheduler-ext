package com.github.ltsopensource.queue.mysql;

import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.core.support.JobQueueUtils;
import com.github.ltsopensource.core.support.SystemClock;
import com.github.ltsopensource.queue.JobStatQueue;
import com.github.ltsopensource.queue.domain.JobStatPo;
import com.github.ltsopensource.queue.domain.JobStatType;
import com.github.ltsopensource.queue.mysql.support.RshHolder;
import com.github.ltsopensource.store.jdbc.JdbcAbstractAccess;
import com.github.ltsopensource.store.jdbc.builder.InsertSql;
import com.github.ltsopensource.store.jdbc.builder.SelectSql;
import com.github.ltsopensource.store.jdbc.builder.UpdateSql;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yingbibo
 * on 2019-12-04
 * email: yingbibo@canzhaoxi.com.cn
 */
public class MysqlJobStatQueue extends JdbcAbstractAccess implements JobStatQueue {
	public MysqlJobStatQueue(Config config) {
		super(config);
	}

	@Override
	public boolean createQueue(String taskTrackerNodeGroup) {
		createTable(readSqlFile("sql/mysql/lts_job_stat_queue.sql", getTableName(taskTrackerNodeGroup)));
		return true;
	}

	@Override
	public boolean add(JobStatPo jobStatPo) {
		new InsertSql(getSqlTemplate())
				.insertIgnore(getTableName(jobStatPo.getTaskTrackerNodeGroup()))
				.columns("task_id","task_tracker_node_group","task_tracker_sub_node_group","server_from","day_range","gmt_modified","job_stat_type")
				.values(jobStatPo.getTaskId(),jobStatPo.getTaskTrackerNodeGroup(),jobStatPo.getTaskTrackerSubNodeGroup(),jobStatPo.getServerFrom(),jobStatPo.getDayRange(),jobStatPo.getGmtModified(),jobStatPo.getJobStatType()==null?null:jobStatPo.getJobStatType().name())
				.doInsert();
		return true;
	}

	@Override
	public JobStatPo getJob(String taskTrackerNodeGroup, String taskTrackerSubNodeGroup,String serverFrom,String taskId) {
		return new SelectSql(getSqlTemplate())
				.select()
				.all()
				.from()
				.table(getTableName(taskTrackerNodeGroup))
				.where("task_id = ?", taskId)
				.and("task_tracker_node_group = ?", taskTrackerNodeGroup)
				.and("task_tracker_sub_node_group = ?",taskTrackerSubNodeGroup)
				.and("server_from = ?",serverFrom)
				.single(RshHolder.JOB_STAT_PO_RSH);
	}

	@Override
	public List<JobStatPo> getJobs(String taskTrackerNodeGroup, String taskTrackerSubNodeGroup, String taskId) {
		return new SelectSql(getSqlTemplate())
				.select()
				.all()
				.from()
				.table(getTableName(taskTrackerNodeGroup))
				.where("task_id = ?", taskId)
				.and("task_tracker_node_group = ?", taskTrackerNodeGroup)
				.and("task_tracker_sub_node_group = ?",taskTrackerSubNodeGroup)
				.list(RshHolder.JOB_STAT_PO_LIST_RSH);
	}

	@Override
	public boolean update(long id,String taskTrackerNodeGroup,Long dayRange,String jobStat,String oldJobStat) {
		return new UpdateSql(getSqlTemplate())
				.update()
				.table(getTableName(taskTrackerNodeGroup))
				.set("job_stat_type", jobStat)
				.set("gmt_modified",SystemClock.now())
				.set("day_range",dayRange)
				.where("id = ?", id)
				.and("job_stat_type = ?",oldJobStat)
				.doUpdate()==1;
	}

	private String getTableName(String taskTrackerNodeGroup) {
		return JobQueueUtils.getJobStatQueueName(taskTrackerNodeGroup);
	}
}
