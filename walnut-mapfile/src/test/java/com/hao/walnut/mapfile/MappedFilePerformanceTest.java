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
public class MappedFilePerformanceTest {
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
        ExecutorService executorService = Executors.newFixedThreadPool(1000);
        int Max = 1000000;
        Semaphore semaphore = new Semaphore(1-Max);
        long start = System.currentTimeMillis();
        for(int i = 0; i < Max; i++) {
            executorService.execute(() -> {
                try {
//                    log.info("try to append {}", idx);
                      Future<WriteResponse> future = mappedFile.append(testString.getBytes());
                      future.get();
//                    log.info("finish append {}", idx);
                    semaphore.release();
                } catch (IOException | InterruptedException | ExecutionException e) {
                    log.error("{}", e.getMessage());
                    e.printStackTrace();
                }
            });
        }
        executorService.shutdown();
        semaphore.acquire();
        mappedFile.close();
        long timeCost = System.currentTimeMillis() - start;
        long totalBytes = Max * testString.getBytes().length / (1024 * 1024) ;
        log.info("性能测试完毕，处理{}条数据的时间开销为={}, tps={}, 数据总量为={}MB({}MB/s)", Max, timeCost, Max/timeCost * 1000, totalBytes, totalBytes* 1000/(double)timeCost);
    }

    @AfterClass
    public static void teardown() {
//        data.delete();
//        temp.delete();
    }
}
