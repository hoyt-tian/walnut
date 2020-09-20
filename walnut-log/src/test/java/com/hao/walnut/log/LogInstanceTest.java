package com.hao.walnut.log;


import com.hao.walnut.TestData;
import com.hao.walnut.core.WALException;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.*;

@Slf4j
@Ignore
public class LogInstanceTest {

    String workspace = "./testData";
    String format(Date date) {
        return String.format("%s_%s_%s_%s_%s_%s", date.getYear() + 1900, date.getMonth() + 1, date.getDate(), date.getHours(), date.getMinutes(), date.getSeconds());
    }

    void doTest(LogInstanceConfig logInstanceConfig, int max) throws WALException, InterruptedException {
        long start = System.currentTimeMillis();
        LogInstance logInstance = new LogInstance(logInstanceConfig);
        Assert.assertNotNull(logInstance);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        for(int i = 0; i < max; i++) {
            final int idx = i;
            executorService.submit(() -> {
                LogAppendRequest logAppendRequest = new LogAppendRequest();
                logAppendRequest.setFd(logAppendRequest.fd);
                logAppendRequest.setData((TestData.msg + idx).getBytes(StandardCharsets.UTF_8));
                return logInstance.append(logAppendRequest).get();
            });
        }
        executorService.shutdown();
        while(!executorService.isTerminated()) {
            Thread.sleep(2);
        }
        log.info("totoal cost {}", System.currentTimeMillis() - start);
    }

    @Test
    public void testWithRaf() throws WALException, InterruptedException, ExecutionException {
        Date date = new Date();
        String fileName = String.format("raf-%s", format(date));
        LogInstanceConfig logInstanceConfig = new LogInstanceConfig();
        logInstanceConfig.setWorkspace(workspace);
        logInstanceConfig.setFileName(fileName);
        this.doTest(logInstanceConfig, 1000000);
    }

    @Test
    public void testWithChannel() throws WALException, InterruptedException {
        Date date = new Date();
        String fileName = String.format("channel-%s", format(date));
        LogInstanceConfig logInstanceConfig = new LogInstanceConfig();
        logInstanceConfig.setWorkspace(workspace);
        logInstanceConfig.setFileName(fileName);
        logInstanceConfig.setMode(LogFileFactory.Mode_Channel);
        this.doTest(logInstanceConfig, 1000000);
    }

    @Test
    public void testWithMmap() throws WALException, InterruptedException {
        Date date = new Date();
        String fileName = String.format("mmap", format(date));
        LogInstanceConfig logInstanceConfig = new LogInstanceConfig();
        logInstanceConfig.setWorkspace(workspace);
        logInstanceConfig.setFileName(fileName);
        logInstanceConfig.setMode(LogFileFactory.Mode_Mmap);
        this.doTest(logInstanceConfig, 1);
    }

}
