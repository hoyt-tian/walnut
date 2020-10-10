package com.hao.walnut.mapfile;

import lombok.Getter;
import lombok.Setter;

import java.io.File;
import java.util.concurrent.ExecutorService;

@Getter
@Setter
public class MappedFileConf {
    static final int DefaultRangeSize = 1024 * 1024 * 200;

    File file;
    int rangeSize = DefaultRangeSize;
    FlushStrategy flushStrategy = FlushStrategy.Batch;
    int maxWriteThread = 32;
    int batchFlushByteSize = 1024 * 100;
    int flushTimeout = 1000 * 5;
    ExecutorService writeExecutorService;
}
