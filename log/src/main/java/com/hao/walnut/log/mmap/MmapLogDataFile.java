package com.hao.walnut.log.mmap;

import com.hao.walnut.core.WALException;
import com.hao.walnut.log.LogDataFile;
import com.hao.walnut.log.LogDataItem;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

@Slf4j
public class MmapLogDataFile extends MmapLogFile implements LogDataFile {
    public MmapLogDataFile(File file) throws WALException {
        super(file, 1024 * 1024 * 1024);
    }

    @Override
    public LogDataItem read(long start) throws WALException {
        try {
            LogDataItem logDataItem = new LogDataItem();
            logDataItem.setSize(this.readInt(start));
            logDataItem.setData(new byte[logDataItem.getSize()]);
            this.readBytes(start+4, logDataItem.getData());
            return logDataItem;
        } catch (IOException e) {
            throw new WALException(e.getMessage());
        }
    }

    @Override
    public synchronized long append(byte[] data) throws IOException {
        try {
            long position = this.fileSize();
            this.writeInt(position, data.length);
            this.writeBytes(position + 4, data);
            log.info("append {}", data.length + 4);
            return position;
        } catch (Exception e) {
            return -1l;
        }

    }
}
