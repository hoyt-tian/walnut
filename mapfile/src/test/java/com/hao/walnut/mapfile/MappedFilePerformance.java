package com.hao.walnut.mapfile;

import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.*;

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
        for(int i = 0; i < 128; i++) {
            sb.append((char)i);
        }
        sb.append("========finish");
        testString = sb.toString();
    }

    @Test
    public void testInsertion() throws IOException, InterruptedException, ExecutionException {
        MappedFile mappedFile = new MappedFile(data, 512);
        ExecutorService executorService = Executors.newCachedThreadPool();
        int Max = 100;
        long start = System.currentTimeMillis();
        for(int i = 0; i < Max; i++) {
            final int idx = i;
            executorService.execute(() -> {
                try {
//                    log.info("try to append {}", idx);
                      mappedFile.append(testString.getBytes()).get(100, TimeUnit.MILLISECONDS);
//                    log.info("finish append {}", idx);
                } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
                    log.error("{}", e.getMessage());
                    e.printStackTrace();
                }
            });
        }
        executorService.shutdown();
        while(!executorService.isTerminated()) {
            Thread.sleep(100);
        }
        mappedFile.close();
        log.info("性能测试完毕{}", System.currentTimeMillis() - start);
    }

    @AfterClass
    public static void teardown() {
//        data.delete();
//        temp.delete();
    }
}
