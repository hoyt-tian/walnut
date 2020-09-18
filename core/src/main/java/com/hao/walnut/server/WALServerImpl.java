package com.hao.walnut.server;

import com.hao.walnut.WALException;
import com.hao.walnut.log.LogAppendRequest;
import com.hao.walnut.log.LogDataItem;
import com.hao.walnut.log.LogInstance;
import com.hao.walnut.log.LogInstanceConfig;
import lombok.extern.slf4j.Slf4j;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.Consumer;

@Slf4j
public class WALServerImpl implements WALServer {


    Map<String, LogInstance> logInstanceMap = new ConcurrentHashMap<>();

    private String workspace = "./";

    @Override
    public Future<Long> append(String fd, byte[] data, Consumer<Long> callback) throws WALException {
        LogInstance logInstance = this.getLogInstance(fd);
        LogAppendRequest logAppendRequest = new LogAppendRequest();
        logAppendRequest.setFd(fd);
        logAppendRequest.setData(data);
        logAppendRequest.setCallback(callback);
        return logInstance.append(logAppendRequest);
    }

    protected LogInstance getLogInstance(String fd) throws WALException {
        if (!logInstanceMap.containsKey(fd)) {
            int retry = 0;
            int maxRetry = 10;
            while(retry < maxRetry) {
                try {
                    LogInstanceConfig logInstanceConfig = new LogInstanceConfig();
                    logInstanceConfig.setWorkspace(workspace);
                    logInstanceConfig.setFileName(fd);
                    LogInstance logInstance = new LogInstance(logInstanceConfig);
                    logInstanceMap.put(fd, logInstance);
                    break;
                } catch (WALException e) {
                    retry++;
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException interruptedException) {
                        log.error("{}", interruptedException.getMessage());
                        break;
                    }
                }
            }
            if (retry == maxRetry) {
                throw new WALException("fail to create log instance");
            }
        }
        return logInstanceMap.get(fd);
    }


    @Override
    public byte[] read(String fd, long offset) throws WALException {
        LogInstance logInstance = this.getLogInstance(fd);
        LogDataItem logDataItem = logInstance.read(offset);
        return logDataItem.getData();
    }
}
