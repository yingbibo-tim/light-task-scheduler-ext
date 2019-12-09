CREATE TABLE IF NOT EXISTS `{tableName}` (
  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,
  `task_id` varchar(64) DEFAULT NULL,
  `task_tracker_node_group` varchar(64) DEFAULT NULL,
  `task_tracker_sub_node_group` varchar(64) DEFAULT NULL,
  `gmt_modified` bigint(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_taskId_taskTrackerNodeGroup_taskTrackerSubNodeGroup` (`task_id`,`task_tracker_node_group`,`task_tracker_sub_node_group`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务完成时间记录表';