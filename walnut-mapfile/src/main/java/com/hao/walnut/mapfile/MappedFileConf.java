package com.hao.walnut.mapfile;

import java.io.File;

public class MappedFileConf {
    static final int DefaultRangeSize = 1024 * 1024 * 200;

    File file;
    int rangeSize = DefaultRangeSize;
    FlushStrategy flushStrategy = FlushStrategy.Batch;
    int maxWriteThread = 128;
}
