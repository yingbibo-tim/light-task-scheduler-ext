package com.github.ltsopensource.core.protocol.command;

import com.github.ltsopensource.remoting.exception.RemotingCommandFieldCheckException;

import java.util.List;
import java.util.Map;

/**
 * @author Robert HG (254963746@qq.com)
 */
public class JobAskRequest extends AbstractRemotingCommandBody {

	private static final long serialVersionUID = 1993281575847386175L;
	
	Map<String,String> jobIdsWithSubNodeMap;

    public Map<String, String> getJobIdsWithSubNodeMap() {
        return jobIdsWithSubNodeMap;
    }

    public void setJobIdsWithSubNodeMap(Map<String, String> jobIdsWithSubNodeMap) {
        this.jobIdsWithSubNodeMap = jobIdsWithSubNodeMap;
    }

    @Override
    public void checkFields() throws RemotingCommandFieldCheckException {
        if (jobIdsWithSubNodeMap == null || jobIdsWithSubNodeMap.size() == 0) {
            throw new RemotingCommandFieldCheckException("jobIdsWithSubNodeMap could not be empty");
        }
    }
}
