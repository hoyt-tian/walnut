package com.hao.walnut.log;

import com.hao.walnut.WALException;
import com.hao.walnut.log.raf.RafLogDataFile;
import com.hao.walnut.log.raf.RafLogIndexFile;
import com.hao.walnut.util.promise.Promise;
import com.hao.walnut.util.promise.Retry;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class LogInstance {
    /**
     * 数据文件
     */
    LogDataFile logDataFile;

    /**
     * 索引文件
     */
    LogIndexFile logIndexFile;

    /**
     * 写操作锁
     */
    ReentrantLock writeQueueLock = new ReentrantLock();

    /**
     * 新增数据请求队列
     */
    Queue<LogAppendRequest> logAppendRequestQueue = new LinkedList<>();

    /**
     * 消费线程池
     */
    ExecutorService consumeExectorService = Executors.newSingleThreadExecutor();

    /**
     * 回调线程池
     */
    ExecutorService callbackExecutorService = Executors.newCachedThreadPool();

    public LogInstance(LogInstanceConfig logInstanceConfig) throws WALException {

        String dataFileName = String.format("%s%s%s.dat", logInstanceConfig.workspace, File.separator, logInstanceConfig.fileName);
        String idxFileName = String.format("%s%s%s.idx", logInstanceConfig.workspace, File.separator, logInstanceConfig.fileName);

        logDataFile = LogFileFactory.createLogDataFile(dataFileName, logInstanceConfig.mode);
        logIndexFile = LogFileFactory.createLogIndexFile(idxFileName, logInstanceConfig.mode);
    }

    /**
     * 给定索引序号，返回这个序号对应的日志消息。比如当参数为5时，代表索引数据中的第5条记录，从索引文件找到数据文件偏移，再从数据文件中读取出日志数据
     * @param offset 索引序号，从0开始
     * @return
     * @throws WALException
     */
    public LogDataItem read(long offset) throws WALException {
        long dataOffset = logIndexFile.dataOffset(offset);
        return logDataFile.read(dataOffset);
    }

    /**
     * 写入一条消息，返回这条消息的索引序号
     * @param logAppendRequest
     * @return
     */
    public Future<Long> append(LogAppendRequest logAppendRequest) {
        while(true) {
            try {
                if (writeQueueLock.tryLock(50, TimeUnit.MILLISECONDS)) {
                    logAppendRequestQueue.add(logAppendRequest);
                    writeQueueLock.unlock();
                    return consumeExectorService.submit(new WriteRequestQueueConsumer());
                } else {
                    Thread.sleep(50);
                }
            } catch (InterruptedException e) {
                log.error("Fail to append into write request queue {}", e);
                e.printStackTrace();
                return null;
            }
        }
    }

    protected long process(LogAppendRequest logAppendRequest) {
        try {
            log.trace("procces AppendRequest, {}", logAppendRequest);
            long bytePos = logDataFile.append(logAppendRequest.getData());
            final long idx = logIndexFile.append(bytePos);
            if (logAppendRequest.isAsync() && logAppendRequest.getCallback() != null) {
                callbackExecutorService.submit(() -> logAppendRequest.getCallback().accept(idx));
            }
            return idx;
        } catch (IOException e) {
            log.error("write to disk fail {}", e.getMessage());
            e.printStackTrace();
            return -1l;
        }
    }

    class WriteRequestQueueConsumer implements Callable<Long> {

        @Override
        public Long call() throws Exception {
            for(int i = 0; i < 5; i++) {
                if (LogInstance.this.writeQueueLock.tryLock(2, TimeUnit.MILLISECONDS)) {
                    if (LogInstance.this.logAppendRequestQueue.size() > 0) {
                        LogAppendRequest request = LogInstance.this.logAppendRequestQueue.poll();
                        LogInstance.this.writeQueueLock.unlock();
                        return LogInstance.this.process(request);
                    } else {
                        LogInstance.this.writeQueueLock.unlock();
                        log.warn("empty write queue");
                        Thread.sleep(i*10 + 2);
                    }
                }
            }
            return -1l;
        }
    }
}
