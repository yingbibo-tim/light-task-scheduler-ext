package com.github.ltsopensource.tasktracker.processor;

import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.core.protocol.command.CommandBodyWrapper;
import com.github.ltsopensource.core.protocol.command.JobAskRequest;
import com.github.ltsopensource.core.protocol.command.JobAskResponse;
import com.github.ltsopensource.remoting.Channel;
import com.github.ltsopensource.remoting.exception.RemotingCommandException;
import com.github.ltsopensource.remoting.protocol.RemotingCommand;
import com.github.ltsopensource.remoting.protocol.RemotingProtos;
import com.github.ltsopensource.tasktracker.domain.TaskTrackerAppContext;
import com.github.ltsopensource.tasktracker.expcetion.NoAvailableJobRunnerException;
import com.github.ltsopensource.tasktracker.runner.RunnerPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Robert HG (254963746@qq.com)
 */
public class JobAskProcessor extends AbstractProcessor {

    private final Logger LOGGER = LoggerFactory.getLogger(JobAskProcessor.class);

    protected JobAskProcessor(TaskTrackerAppContext appContext) {
        super(appContext);
    }

    @Override
    public RemotingCommand processRequest(Channel channel,
                                          RemotingCommand request) throws RemotingCommandException {

        JobAskRequest requestBody = request.getBody();


        Map<String,String> jobIdsWithSubNodeNameMap = requestBody.getJobIdsWithSubNodeMap();

        Map<String,List<String>> requestMap = new HashMap<String,List<String>>();
        for(String jobId:jobIdsWithSubNodeNameMap.keySet()){
            String subNodeName = jobIdsWithSubNodeNameMap.get(jobId);
            List<String> jobIds = requestMap.computeIfAbsent(subNodeName, k -> new ArrayList<>());
            jobIds.add(jobId);
        }
        List<String> notExistJobIds = new ArrayList<>();
        for(String subNodeName:requestMap.keySet()){
            try {
                List<String> partNoExitsJobIds = appContext.getRunnerPool(subNodeName).getRunningJobManager().getNotExists(requestMap.get(subNodeName));
                if(partNoExitsJobIds!=null&&partNoExitsJobIds.size()>0){
                    notExistJobIds.addAll(partNoExitsJobIds);
                }
            } catch (NoAvailableJobRunnerException e) {
               LOGGER.error("No subNodeName {} ",subNodeName);
            }
        }
        JobAskResponse responseBody = CommandBodyWrapper.wrapper(appContext, new JobAskResponse());
        responseBody.setJobIds(notExistJobIds);
        return RemotingCommand.createResponseCommand(
                RemotingProtos.ResponseCode.SUCCESS.code(), "查询成功", responseBody);
    }
}
