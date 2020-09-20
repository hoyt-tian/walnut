package com.hao.walnut.mapfile;

import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class MappedFilePerformance {
    static File temp;
    static File data;
    static String testString;
    @BeforeClass
    public static void parepare() throws IOException {
        temp = new File("./temp_performance_"+ System.currentTimeMillis());
        temp.mkdirs();
        log.info("创建临时数据目录{}", temp.getAbsolutePath());
        data = new File(temp.getAbsolutePath() + File.separator + "data.dat");
        StringBuilder sb = new StringBuilder();

        sb.append("start=========");
        for(int i = 0; i < 2048; i++) {
            sb.append((char)(i%128));
        }
        sb.append("========finish");
        testString = sb.toString();
    }

    @Test
    public void testInsertion() throws IOException, InterruptedException {
        MappedFileConf mappedFileConf = new MappedFileConf();
        mappedFileConf.file = data;
        MappedFile mappedFile = new MappedFile(mappedFileConf);
        ExecutorService executorService = Executors.newCachedThreadPool();
        int Max = 100000;
        final AtomicInteger lock = new AtomicInteger();
        long start = System.currentTimeMillis();
        for(int i = 0; i < Max; i++) {
            final int idx = i;
            executorService.execute(() -> {
                try {
//                    log.info("try to append {}", idx);
                      mappedFile.append(testString.getBytes()).get();
//                    log.info("finish append {}", idx);
                    lock.incrementAndGet();
                } catch (IOException | InterruptedException | ExecutionException e) {
                    log.error("{}", e.getMessage());
                    e.printStackTrace();
                }
            });
        }
        executorService.shutdown();
        while(lock.get() < Max) {
            Thread.sleep(100);
        }
        mappedFile.close();
        log.info("性能测试完毕{}", System.currentTimeMillis() - start);
    }

    @AfterClass
    public static void teardown() {
        data.delete();
        temp.delete();
    }
}
