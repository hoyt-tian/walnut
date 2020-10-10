package com.hao.walnut.server;

import com.hao.walnut.log.LogFile;
import com.hao.walnut.log.LogFileConf;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

@Slf4j
public class LogFileServer {

    LogFileServerConf logFileServerConf;
    Map<String, LogFile> logFileMap = new ConcurrentHashMap<>();

    public LogFileServer(LogFileServerConf logFileServerConf) {
        this.logFileServerConf = logFileServerConf;
        File workspace = new File(logFileServerConf.workspace);
        if (!workspace.exists()) {
            workspace.mkdirs();
            log.info("创建workspace目录{}", workspace.getAbsolutePath());
        }
    }

    /**
     * 获取LogFile实例，若不存在则创建
     * @param fileName
     * @return
     */
    protected LogFile get(String fileName) throws IOException {
        if (logFileMap.containsKey(fileName)) {
            return logFileMap.get(fileName);
        }
        synchronized (logFileMap) {
            if (logFileMap.containsKey(fileName)) {
                return logFileMap.get(fileName);
            }
            LogFileConf logFileConf = new LogFileConf();
            logFileConf.setWorkspace(logFileServerConf.workspace);
            logFileConf.setFileName(fileName);
            LogFile logFile = new LogFile(logFileConf);
            logFileMap.put(fileName, logFile);
            return logFile;
        }
    }
    /**
     * 读取给定文件的第offset条消息数据
     * @param fileName
     * @param seqId 消息序号，从0开始
     * @return
     */
    public byte[] read(String fileName, long seqId) throws IOException {
        LogFile logFile = get(fileName);
        return logFile.read(seqId);
    }

    /**
     * 向文件追加数据，如果不存在就创建文件
     * @param fileName
     * @param data
     * @return
     */
    public long append(String fileName, byte[] data) throws IOException {
        LogFile logFile = get(fileName);
        return logFile.append(data);
    }

    /**
     * 异步增加文件数据
     * @param fileName
     * @param data
     * @return
     */
    public Future<Long> appendAsync(String fileName, byte[] data) throws IOException {
        LogFile logFile = get(fileName);
        return logFile.appendAsync(data);
    }

    public void close() throws IOException {
        for(LogFile logFile : logFileMap.values()) {
            logFile.close();
        }
    }
}
