package com.hao.walnut.server;

import com.hao.walnut.TestData;
import com.hao.walnut.core.WALException;
import com.hao.walnut.log.LogFileFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Slf4j
public class WALServerImplTest {

    @Test
    public void testReadAndWrite() throws InterruptedException {
        long start = System.currentTimeMillis();
        WALServerConfig walServerConfig = new WALServerConfig();
        walServerConfig.setWorkspace("./testData");
        walServerConfig.setMode(LogFileFactory.Mode_Mmap);
        WALServer walServer = new WALServer(walServerConfig);

        ExecutorService executorService = Executors.newFixedThreadPool(32);


        for(int i = 0; i < 1; i++) {
            final int idx = i;
            executorService.submit(() -> {
                try {
                    int fidx = idx % 4;
                    Future f = walServer.append("testFd" + fidx, (TestData.msg+idx).getBytes(StandardCharsets.UTF_8), (offset) -> {
                        log.info("testFd {}", offset);
                    });
                    f.get();
                } catch (WALException e) {
                    log.error(e.getMessage());
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    log.error(e.getMessage());
                    e.printStackTrace();
                }
            });
        }

        executorService.shutdown();
        while(executorService.isTerminated() == false) {
            Thread.sleep(10);
        }
        log.info("total cost {}", System.currentTimeMillis() - start);
    }
}
