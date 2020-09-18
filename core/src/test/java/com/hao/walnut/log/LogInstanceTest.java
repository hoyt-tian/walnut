package com.hao.walnut.log;

import com.github.houbb.junitperf.core.annotation.JunitPerfConfig;
import com.github.houbb.junitperf.core.rule.JunitPerfRule;
import com.hao.walnut.WALException;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.*;

@Slf4j
public class LogInstanceTest {

    @Rule
    public JunitPerfRule junitPerfRule = new JunitPerfRule();

    String format(Date date) {
        return String.format("%s_%s_%s_%s_%s_%s", date.getYear() + 1900, date.getMonth() + 1, date.getDate(), date.getHours(), date.getMinutes(), date.getSeconds());
    }

    void doTest(LogInstanceConfig logInstanceConfig, int max) throws WALException, InterruptedException {
        long start = System.currentTimeMillis();
        LogInstance logInstance = new LogInstance(logInstanceConfig);
        Assert.assertNotNull(logInstance);
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        for(int i = 0; i < max; i++) {
            final int idx = i;
            executorService.submit(() -> {
                LogAppendRequest logAppendRequest = new LogAppendRequest();
                logAppendRequest.setFd(logAppendRequest.fd);
                logAppendRequest.setData(("abcdefghijklmnopqrstuvwxyz中文测试把数据量搞大搞大搞大abcdefghijklmnopqrstuvwxyz中文测试把数据量搞大搞大搞大abcdefghijklmnopqrstuvwxyz中文测试把数据量搞大搞大搞大abcdefghijklmnopqrstuvwxyz中文测试把数据量搞大搞大搞大abcdefghijklmnopqrstuvwxyz中文测试把数据量搞大搞大搞大"+idx).getBytes(StandardCharsets.UTF_8));
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
        logInstanceConfig.setWorkspace("./");
        logInstanceConfig.setFileName(fileName);
        this.doTest(logInstanceConfig, 100000);
    }

    @Test
    public void testWithChannel() throws WALException, InterruptedException {
        Date date = new Date();
        String fileName = String.format("channel-%s", format(date));
        LogInstanceConfig logInstanceConfig = new LogInstanceConfig();
        logInstanceConfig.setWorkspace("./");
        logInstanceConfig.setFileName(fileName);
        logInstanceConfig.setMode(LogFileFactory.Mode_Channel);
        this.doTest(logInstanceConfig, 100000);
    }

    @Test
    @JunitPerfConfig()
    public void testWithMmap() throws WALException, InterruptedException {
        Date date = new Date();
        String fileName = String.format("mmap-%s", format(date));
        LogInstanceConfig logInstanceConfig = new LogInstanceConfig();
        logInstanceConfig.setWorkspace("./");
        logInstanceConfig.setFileName(fileName);
        logInstanceConfig.setMode(LogFileFactory.Mode_Channel);
        this.doTest(logInstanceConfig, 100000);
    }

}
