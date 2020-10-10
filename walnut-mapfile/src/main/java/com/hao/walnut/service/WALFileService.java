package com.hao.walnut.service;

import com.hao.walnut.mapfile.MappedFile;
import com.hao.walnut.mapfile.MappedFileConf;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class WALFileService {
    ExecutorService writeExecutorService;
    Map<String, MappedFile> fileMap = new ConcurrentHashMap<>();
    public WALFileService() {
        this(256);
    }

    public WALFileService(int maxWriteCount) {
        this.writeExecutorService = Executors.newFixedThreadPool(maxWriteCount);
    }

    public MappedFile touch(MappedFileConf mappedFileConf) throws IOException {
        String filePath = mappedFileConf.getFile().getAbsolutePath();
        if (fileMap.containsKey(filePath)) {
            return fileMap.get(filePath);
        }
        MappedFile mappedFile = new MappedFile(mappedFileConf);
        fileMap.put(mappedFileConf.getFile().getAbsolutePath(), mappedFile);
        return mappedFile;
    }

    public MappedFile get(String fileName) throws IOException {
        if (fileMap.containsKey(fileName)) {
            return fileMap.get(fileName);
        }
        synchronized (fileMap) {
            if (fileMap.containsKey(fileName)) {
                return fileMap.get(fileName);
            }
            MappedFileConf mappedFileConf = new MappedFileConf();
            mappedFileConf.setFile(new File(fileName));
            mappedFileConf.setWriteExecutorService(writeExecutorService);
            return touch(mappedFileConf);
        }
    }
}
