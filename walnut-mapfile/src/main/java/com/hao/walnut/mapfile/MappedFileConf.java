package com.hao.walnut.mapfile;

import lombok.Getter;
import lombok.Setter;

import java.io.File;

@Getter
@Setter
public class MappedFileConf {
    static final int DefaultRangeSize = 1024 * 1024 * 200;

    File file;
    int rangeSize = DefaultRangeSize;
    FlushStrategy flushStrategy = FlushStrategy.Batch;
    int maxWriteThread = 128;
    int batchFlushByteSize = 1024 * 4;
    int flushTimeout = 100;
}
