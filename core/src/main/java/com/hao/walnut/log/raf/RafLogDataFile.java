package com.hao.walnut.log.raf;

import com.hao.walnut.WALException;
import com.hao.walnut.log.LogDataFile;
import com.hao.walnut.log.LogDataItem;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

@Slf4j
public class RafLogDataFile extends RafLogFile implements LogDataFile {
    public RafLogDataFile(File file) throws WALException {
        super(file);
    }

    @Override
    public LogDataItem read(long start) throws WALException {
        LogDataItem item = new LogDataItem();
        synchronized (this.randomAccessFile) {
            try {
                this.randomAccessFile.seek(start);
                item.setSize(this.randomAccessFile.readInt());
                item.setData(new byte[item.getSize()]);
                this.randomAccessFile.read(item.getData());
            } catch (IOException e) {
                throw new WALException("%s", e.getMessage());
            }

        }
        return item;
    }

    @Override
    public synchronized long append(byte[] data) throws IOException {
        long pos = randomAccessFile.length();
        this.randomAccessFile.seek(pos);
        this.randomAccessFile.writeInt(data.length);
        this.randomAccessFile.write(data);
        return pos;
    }
}
