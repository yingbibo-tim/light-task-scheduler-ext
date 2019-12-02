package com.github.ltsopensource.tasktracker.runner;

import com.github.ltsopensource.tasktracker.domain.TaskTrackerAppContext;
import com.github.ltsopensource.tasktracker.expcetion.NoAvailableJobRunnerException;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yingbibo
 * on 2019-11-25
 * email: yingbibo@canzhaoxi.com.cn
 *
 * RunnerPool 工厂类
 * 主要用于runnerPool的生成
 * 用于subGroupNode 副分类
 *
 */
public class RunnerPoolFactory {

	private Map<String,RunnerPool> runnerMap = new HashMap<String, RunnerPool>();
	public RunnerPoolFactory(final TaskTrackerAppContext appContext){
		Map<String,Integer> subNodeGroupMap =  appContext.getConfig().getSubNodeGroupMap();
		for(String subGroupNodeName:subNodeGroupMap.keySet()){
			runnerMap.put(subGroupNodeName,getRunnerPool(appContext,subGroupNodeName,subNodeGroupMap.get(subGroupNodeName)));
		}
	}

	public RunnerPool getRunnerPool(String subGroupNodeName)throws NoAvailableJobRunnerException {
		if(runnerMap.containsKey(subGroupNodeName)){
			return runnerMap.get(subGroupNodeName);
		}
		throw new NoAvailableJobRunnerException("subGroupNodeName not found .. please check "+subGroupNodeName);
	}

	public void shutDown(){
		for(RunnerPool runnerPool:runnerMap.values()){
			runnerPool.shutDown();
		}
	}

	private RunnerPool getRunnerPool(final TaskTrackerAppContext appContext,String subGroupNodeName,Integer subGroupNodeThreadNum){
		return new RunnerPool(appContext,subGroupNodeThreadNum);
	}


}
