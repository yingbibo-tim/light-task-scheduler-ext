package com.github.ltsopensource.jobtracker.support.cluster;


import com.github.ltsopensource.core.cluster.Node;
import com.github.ltsopensource.core.cluster.NodeType;
import com.github.ltsopensource.core.commons.concurrent.ConcurrentHashSet;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.jobtracker.channel.ChannelWrapper;
import com.github.ltsopensource.jobtracker.domain.JobTrackerAppContext;
import com.github.ltsopensource.jobtracker.domain.TaskTrackerNode;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Robert HG (254963746@qq.com) on 8/16/14.
 *         Task Tracker 管理器 (对 TaskTracker 节点的记录 和 可用线程的记录)
 */
public class TaskTrackerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskTrackerManager.class);

    // 单例
    private final Table<String,String,Set<TaskTrackerNode>> NODE_TABLE = Tables.synchronizedTable(HashBasedTable.<String, String, Set<TaskTrackerNode>>create());

    private JobTrackerAppContext appContext;

    public TaskTrackerManager(JobTrackerAppContext appContext) {
        this.appContext = appContext;
    }

    /**
     * get all connected node group
     */
    public Set<String> getNodeGroups() {
        return NODE_TABLE.rowKeySet();
    }

    /**
     * 添加节点
     */
    public void addNode(Node node) {
        //  channel 可能为 null 当任务节点一注册但还开始发送pull任务的时候,channel 为空
        ChannelWrapper channel = appContext.getChannelManager().getChannel(node.getGroup(),
                node.getNodeType(), node.getIdentity());

        String[] subGroups = node.getSubGroups().split(",");
        String[] subThreads = node.getSubThreads().split(",");
        for(int i=0;i<subGroups.length;i++){
            String subGroup = subGroups[i];
            int sunThreadNum = Integer.parseInt(subThreads[i]);
            Set<TaskTrackerNode> taskTrackerNodes = NODE_TABLE.get(node.getGroup(),subGroup);
            if (taskTrackerNodes == null ) {
                taskTrackerNodes = new ConcurrentHashSet<TaskTrackerNode>();
                //是为了保证 group subGroup 存在唯一
                Set<TaskTrackerNode> oldSet = NODE_TABLE.row(node.getGroup()).putIfAbsent(subGroup,taskTrackerNodes);
                if(oldSet!=null){
                    taskTrackerNodes = oldSet;
                }
            }
            TaskTrackerNode taskTrackerNode = new TaskTrackerNode(node.getGroup(),subGroup,
                    sunThreadNum, node.getIdentity(), channel);
            LOGGER.info("Add TaskTracker node:{}", taskTrackerNode);
            taskTrackerNodes.add(taskTrackerNode);


        }
        // create executable queue
        appContext.getExecutableJobQueue().createQueue(node.getGroup());
        appContext.getNodeGroupStore().addNodeGroup(NodeType.TASK_TRACKER, node.getGroup());



    }

    /**
     * 删除节点
     *
     * @param node
     */
    public void removeNode(Node node) {
        Map<String,Set<TaskTrackerNode>> map = NODE_TABLE.row(node.getGroup());
        if(map!=null&&map.size()>0) {
            for (Set<TaskTrackerNode> taskTrackerNodes : map.values()) {
                if(taskTrackerNodes!=null&&taskTrackerNodes.size()!=0){
                    TaskTrackerNode taskTrackerNode = new TaskTrackerNode(node.getIdentity());
                    taskTrackerNode.setNodeGroup(node.getGroup());
                    LOGGER.info("Remove TaskTracker node:{}", taskTrackerNode);
                    taskTrackerNodes.remove(taskTrackerNode);
                }
            }
        }
    }

    public TaskTrackerNode getTaskTrackerNode(String nodeGroup,String subNodeGroup, String identity) {
        Set<TaskTrackerNode> taskTrackerNodes = NODE_TABLE.get(nodeGroup,subNodeGroup);
        if (taskTrackerNodes == null || taskTrackerNodes.size() == 0) {
            return null;
        }
        for(TaskTrackerNode taskTrackerNode : taskTrackerNodes){
            if (taskTrackerNode.getIdentity().equals(identity)) {
                if (taskTrackerNode.getChannel() == null || taskTrackerNode.getChannel().isClosed()) {
                    // 如果 channel 已经关闭, 更新channel, 如果没有channel, 略过
                    ChannelWrapper channel = appContext.getChannelManager().getChannel(
                            taskTrackerNode.getNodeGroup(), NodeType.TASK_TRACKER, taskTrackerNode.getIdentity());
                    if (channel != null) {
                        // 更新channel
                        taskTrackerNode.setChannel(channel);
                        LOGGER.info("update node channel , taskTackerNode={}", taskTrackerNode);
                        return taskTrackerNode;
                    }
                } else {
                    // 只有当channel正常的时候才返回
                    return taskTrackerNode;
                }
            }
        }
        return null;
    }

    /**
     * 更新节点的 可用线程数
     * @param timestamp        时间戳, 只有当 时间戳大于上次更新的时间 才更新可用线程数
     */
    public void updateTaskTrackerAvailableThreads(
            String nodeGroup,
            String subNodeGroup,
            String identity,
            Integer availableThreads,
            Long timestamp) {

        Set<TaskTrackerNode> taskTrackerNodes = NODE_TABLE.get(nodeGroup,subNodeGroup);

        if (taskTrackerNodes != null && taskTrackerNodes.size() != 0) {
            for (TaskTrackerNode trackerNode : taskTrackerNodes) {
                if (trackerNode.getIdentity().equals(identity) && (trackerNode.getTimestamp() == null || trackerNode.getTimestamp() <= timestamp)) {
                    trackerNode.setAvailableThread(availableThreads);
                    trackerNode.setTimestamp(timestamp);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("更新节点线程数: {}", trackerNode);
                    }
                }
            }
        }
    }
}
