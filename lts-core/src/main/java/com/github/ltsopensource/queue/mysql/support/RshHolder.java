package com.github.ltsopensource.queue.mysql.support;

import com.github.ltsopensource.biz.logger.domain.JobLogPo;
import com.github.ltsopensource.biz.logger.domain.LogType;
import com.github.ltsopensource.core.cluster.NodeType;
import com.github.ltsopensource.core.commons.utils.StringUtils;
import com.github.ltsopensource.core.constant.Level;
import com.github.ltsopensource.core.domain.JobRunResult;
import com.github.ltsopensource.core.domain.JobType;
import com.github.ltsopensource.core.json.JSON;
import com.github.ltsopensource.core.json.TypeReference;
import com.github.ltsopensource.queue.domain.*;
import com.github.ltsopensource.store.jdbc.dbutils.ResultSetHandler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Robert HG (254963746@qq.com) on 5/31/15.
 */
public class RshHolder {

    public static final ResultSetHandler<JobPo> JOB_PO_RSH = new ResultSetHandler<JobPo>() {
        @Override
        public JobPo handle(ResultSet rs) throws SQLException {
            if (!rs.next()) {
                return null;
            }
            return getJobPo(rs);
        }
    };

    public static final ResultSetHandler<List<JobPo>> JOB_PO_LIST_RSH = new ResultSetHandler<List<JobPo>>() {
        @Override
        public List<JobPo> handle(ResultSet rs) throws SQLException {
            List<JobPo> jobPos = new ArrayList<JobPo>();
            while (rs.next()) {
                jobPos.add(getJobPo(rs));
            }
            return jobPos;
        }
    };

    public static final ResultSetHandler<JobFinishPo> JOB_FINISH_PO_RSH = new ResultSetHandler<JobFinishPo>() {
        @Override
        public JobFinishPo handle(ResultSet rs) throws SQLException {
            if(!rs.next()){
                return null;
            }
            return getJobFinishPo(rs);
        }
    };

    public static final ResultSetHandler<List<JobFinishPo>> JOB_FINISH_PO_LIST_RSH = new ResultSetHandler<List<JobFinishPo>>() {
        @Override
        public List<JobFinishPo> handle(ResultSet rs) throws SQLException {
            List<JobFinishPo> jobFinishPoList = new ArrayList<>();
            JobFinishPo jobFinishPo = getJobFinishPo(rs);
            jobFinishPoList.add(jobFinishPo);
            return jobFinishPoList;
        }
    };

    public static final ResultSetHandler<JobStatPo> JOB_STAT_PO_RSH = new ResultSetHandler<JobStatPo>() {
        @Override
        public JobStatPo handle(ResultSet rs) throws SQLException {
            if(!rs.next()){
                return null;
            }
           return getJobStatPo(rs);
        }
    };

    public static final ResultSetHandler<List<JobStatPo>> JOB_STAT_PO_LIST_RSH = new ResultSetHandler<List<JobStatPo>>() {
        @Override
        public List<JobStatPo> handle(ResultSet rs) throws SQLException {
            List<JobStatPo> jobPos = new ArrayList<JobStatPo>();
            while (rs.next()) {
                JobStatPo jobStatPo =  getJobStatPo(rs);
                jobPos.add(jobStatPo);
            }
            return jobPos;
        }
    };

    private static JobFinishPo getJobFinishPo(ResultSet rs)throws SQLException{
        JobFinishPo jobFinishPo = new JobFinishPo();
        jobFinishPo.setId(rs.getLong("id"));
        jobFinishPo.setTaskId(rs.getString("task_id"));
        jobFinishPo.setTaskTrackerNodeGroup(rs.getString("task_tracker_node_group"));
        jobFinishPo.setTaskTrackerSubNodeGroup(rs.getString("task_tracker_sub_node_group"));
        jobFinishPo.setGmtModified(rs.getLong("gmt_modified"));
        return jobFinishPo;
    }

    private static JobStatPo getJobStatPo(ResultSet rs)throws SQLException{
        JobStatPo jobStatPo = new JobStatPo();
        jobStatPo.setId(rs.getLong("id"));
        jobStatPo.setTaskId(rs.getString("task_id"));
        jobStatPo.setTaskTrackerNodeGroup(rs.getString("task_tracker_node_group"));
        jobStatPo.setTaskTrackerSubNodeGroup(rs.getString("task_tracker_sub_node_group"));
        if(rs.getObject("day_range")!=null) {
            jobStatPo.setDayRange(rs.getLong("day_range"));
        }
        jobStatPo.setServerFrom(rs.getString("server_from"));
        if(rs.getObject("gmt_modified")!=null) {
            jobStatPo.setGmtModified(rs.getLong("gmt_modified"));
        }
        jobStatPo.setJobStatType(JobStatType.get( rs.getString("job_stat_type")));
        return jobStatPo;
    }


    private static JobPo getJobPo(ResultSet rs) throws SQLException {
        JobPo jobPo = new JobPo();
        jobPo.setJobId(rs.getString("job_id"));
        jobPo.setPriority(rs.getInt("priority"));
        jobPo.setLastGenerateTriggerTime(rs.getLong("last_generate_trigger_time"));
        jobPo.setRetryTimes(rs.getInt("retry_times"));
        jobPo.setMaxRetryTimes(rs.getInt("max_retry_times"));
        jobPo.setRelyOnPrevCycle(rs.getBoolean("rely_on_prev_cycle"));
        jobPo.setInternalExtParams(JSON.parse(rs.getString("internal_ext_params"), new TypeReference<HashMap<String, String>>() {
        }));
        jobPo.setTaskId(rs.getString("task_id"));
        jobPo.setRealTaskId(rs.getString("real_task_id"));
        jobPo.setGmtCreated(rs.getLong("gmt_created"));
        jobPo.setGmtModified(rs.getLong("gmt_modified"));
        jobPo.setSubmitNodeGroup(rs.getString("submit_node_group"));
        jobPo.setTaskTrackerNodeGroup(rs.getString("task_tracker_node_group"));
        jobPo.setTaskTrackerSubNodeGroup(rs.getString("task_tracker_sub_node_group"));
        jobPo.setExtParams(JSON.parse(rs.getString("ext_params"), new TypeReference<HashMap<String, String>>() {
        }));
        String jobType = rs.getString("job_type");
        if (StringUtils.isNotEmpty(jobType)) {
            jobPo.setJobType(JobType.valueOf(jobType));
        }
        jobPo.setIsRunning(rs.getBoolean("is_running"));
        jobPo.setTaskTrackerIdentity(rs.getString("task_tracker_identity"));
        jobPo.setCronExpression(rs.getString("cron_expression"));
        jobPo.setNeedFeedback(rs.getBoolean("need_feedback"));
        jobPo.setTriggerTime(rs.getLong("trigger_time"));
        jobPo.setRepeatCount(rs.getInt("repeat_count"));
        jobPo.setRepeatedCount(rs.getInt("repeated_count"));
        jobPo.setRepeatInterval(rs.getLong("repeat_interval"));
        return jobPo;
    }

    public static final ResultSetHandler<List<JobFeedbackPo>> JOB_FEED_BACK_LIST_RSH = new ResultSetHandler<List<JobFeedbackPo>>() {
        @Override
        public List<JobFeedbackPo> handle(ResultSet rs) throws SQLException {
            List<JobFeedbackPo> jobFeedbackPos = new ArrayList<JobFeedbackPo>();
            while (rs.next()) {
                JobFeedbackPo jobFeedbackPo = new JobFeedbackPo();
                jobFeedbackPo.setId(rs.getString("id"));
                jobFeedbackPo.setJobRunResult(JSON.parse(rs.getString("job_result"), new TypeReference<JobRunResult>() {
                }));
                jobFeedbackPo.setGmtCreated(rs.getLong("gmt_created"));
                jobFeedbackPos.add(jobFeedbackPo);
            }
            return jobFeedbackPos;
        }
    };

    public static final ResultSetHandler<List<NodeGroupPo>> NODE_GROUP_LIST_RSH = new ResultSetHandler<List<NodeGroupPo>>() {
        @Override
        public List<NodeGroupPo> handle(ResultSet rs) throws SQLException {
            List<NodeGroupPo> list = new ArrayList<NodeGroupPo>();
            while (rs.next()) {
                NodeGroupPo nodeGroupPo = new NodeGroupPo();
                nodeGroupPo.setNodeType(NodeType.valueOf(rs.getString("node_type")));
                nodeGroupPo.setName(rs.getString("name"));
                nodeGroupPo.setGmtCreated(rs.getLong("gmt_created"));
                list.add(nodeGroupPo);
            }
            return list;
        }
    };

    public static final ResultSetHandler<List<JobLogPo>> JOB_LOGGER_LIST_RSH = new ResultSetHandler<List<JobLogPo>>() {
        @Override
        public List<JobLogPo> handle(ResultSet rs) throws SQLException {
            List<JobLogPo> result = new ArrayList<JobLogPo>();
            while (rs.next()) {
                JobLogPo jobLogPo = new JobLogPo();
                jobLogPo.setLogTime(rs.getLong("log_time"));
                jobLogPo.setGmtCreated(rs.getLong("gmt_created"));
                jobLogPo.setLogType(LogType.valueOf(rs.getString("log_type")));
                jobLogPo.setSuccess(rs.getBoolean("success"));
                jobLogPo.setMsg(rs.getString("msg"));
                jobLogPo.setTaskTrackerIdentity(rs.getString("task_tracker_identity"));
                jobLogPo.setLevel(Level.valueOf(rs.getString("level")));
                jobLogPo.setTaskId(rs.getString("task_id"));
                jobLogPo.setRealTaskId(rs.getString("real_task_id"));
                String jobType = rs.getString("job_type");
                if (StringUtils.isNotEmpty(jobType)) {
                    jobLogPo.setJobType(JobType.valueOf(jobType));
                }
                jobLogPo.setJobId(rs.getString("job_id"));
                jobLogPo.setPriority(rs.getInt("priority"));
                jobLogPo.setSubmitNodeGroup(rs.getString("submit_node_group"));
                jobLogPo.setTaskTrackerNodeGroup(rs.getString("task_tracker_node_group"));
                jobLogPo.setTaskTrackerSubNodeGroup(rs.getString("task_tracker_sub_node_group"));
                jobLogPo.setExtParams(JSON.parse(rs.getString("ext_params"), new TypeReference<Map<String, String>>() {
                }));
                jobLogPo.setInternalExtParams(JSON.parse(rs.getString("internal_ext_params"), new TypeReference<HashMap<String, String>>() {
                }));
                jobLogPo.setNeedFeedback(rs.getBoolean("need_feedback"));
                jobLogPo.setCronExpression(rs.getString("cron_expression"));
                jobLogPo.setTriggerTime(rs.getLong("trigger_time"));
                jobLogPo.setRetryTimes(rs.getInt("retry_times"));
                jobLogPo.setMaxRetryTimes(rs.getInt("max_retry_times"));
                jobLogPo.setDepPreCycle(rs.getBoolean("rely_on_prev_cycle"));
                jobLogPo.setRepeatCount(rs.getInt("repeat_count"));
                jobLogPo.setRepeatedCount(rs.getInt("repeated_count"));
                jobLogPo.setRepeatInterval(rs.getLong("repeat_interval"));
                result.add(jobLogPo);
            }
            return result;
        }
    };



}


