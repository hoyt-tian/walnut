package com.hao.walnut.log.mmap;

import com.hao.walnut.WALException;
import com.hao.walnut.log.LogDataFile;
import com.hao.walnut.log.LogDataItem;
import java.io.File;
import java.io.IOException;

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
    public long append(byte[] data) throws IOException {
        long position = this.fileSize();
        this.writeBytes(position, data);
        return position;
    }
}
