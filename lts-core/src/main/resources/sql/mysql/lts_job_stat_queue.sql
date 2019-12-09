CREATE TABLE IF NOT EXISTS `{tableName}` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `task_id` varchar(64) DEFAULT NULL,
  `task_tracker_node_group` varchar(64) DEFAULT NULL,
  `task_tracker_sub_node_group` varchar(64) DEFAULT NULL,
  `server_from` varchar(64) DEFAULT NULL,
  `day_range` bigint(20) DEFAULT NULL,
  `gmt_modified` bigint(20) DEFAULT NULL,
  `job_stat_type` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_taskId_taskNode_taskSubNode_serverFrom` (`task_id`,`task_tracker_node_group`,`task_tracker_sub_node_group`,`server_from`)
) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=utf8mb4;