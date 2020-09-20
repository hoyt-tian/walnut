package com.hao.walnut.server;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class LogServerTest {

    @Test
    public void test() throws IOException, InterruptedException {
        LogFileServerConf logFileServerConf = new LogFileServerConf();
        logFileServerConf.setWorkspace("./testLogServer_"+System.currentTimeMillis());
        LogFileServer logFileServer = new LogFileServer(logFileServerConf);
        long idx = logFileServer.append("test1", "hello".getBytes());
        Assert.assertEquals(idx, 0);
        idx = logFileServer.append("test2", "world".getBytes());
        Assert.assertEquals(idx, 0);
        idx = logFileServer.append("test1", "world".getBytes());
        Assert.assertEquals(idx, 1);
        idx = logFileServer.append("test2", "hello".getBytes());
        Assert.assertEquals(idx, 1);
        Assert.assertArrayEquals("hello".getBytes(), logFileServer.read("test1", 0));
        Assert.assertArrayEquals("world".getBytes(), logFileServer.read("test1", 1));
        Assert.assertArrayEquals("world".getBytes(), logFileServer.read("test2", 0));
        Assert.assertArrayEquals("hello".getBytes(), logFileServer.read("test2", 1));

        doPerformace(logFileServer);

        logFileServer.close();
        File t1data = new File(logFileServerConf.getWorkspace() + File.separator + "test1.dat");
        File t1index = new File(logFileServerConf.getWorkspace()+ File.separator  + "test1.idx");
        File t2data = new File(logFileServerConf.getWorkspace() + File.separator  + "test2.dat");
        File t2index = new File(logFileServerConf.getWorkspace() + File.separator  + "test2.idx");
        t1data.delete();
        t1index.delete();
        t2data.delete();
        t2index.delete();
        File ws = new File(logFileServerConf.workspace);
        ws.delete();
    }

    protected void doPerformace(LogFileServer logFileServer) throws InterruptedException {
        StringBuilder sb = new StringBuilder();

        sb.append("start=========");
        for(int i = 0; i < 1024; i++) {
            sb.append((char)(i%128));
        }
        sb.append("========finish");
        String testData = sb.toString();
        ExecutorService executorService = Executors.newFixedThreadPool(256);
        int Max = 10000;
        AtomicInteger done = new AtomicInteger();
        long start = System.currentTimeMillis();
        for(int i = 0; i < Max; i++) {
            final int idx = i%2 + 1;
            executorService.execute(() -> {
                try {
                    logFileServer.append("test" + idx, testData.getBytes() );
                    done.incrementAndGet();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        while(done.get() != Max) {
            Thread.sleep(10);
        }
        log.info("Total time cost {}", System.currentTimeMillis() - start);
    }

}
