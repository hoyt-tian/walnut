package com.hao.walnut.log.channel;

import com.hao.walnut.WALException;
import com.hao.walnut.log.LogDataFile;
import com.hao.walnut.log.LogDataItem;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.io.IOException;

@Slf4j
public class ChannelLogDataFile extends ChannelLogFile implements LogDataFile {
    public ChannelLogDataFile(File file) throws WALException {
        super(file);
    }

    @Override
    public LogDataItem read(long start) throws WALException {
        try {
            LogDataItem logDataItem = new LogDataItem();
            logDataItem.setSize(readInt(start));
            logDataItem.setData(new byte[logDataItem.getSize()]);
            readBytes(start + 4, logDataItem.getData());
            return logDataItem;
        } catch (IOException e) {
            log.error("{}", e.getMessage());
            throw new WALException("fail to read size %s", e.getMessage());
        }
    }

    @Override
    public long append(byte[] data) throws IOException {
        long position = fileSize();
        writeBytes(fileSize(), data);
        return position;
    }
}
