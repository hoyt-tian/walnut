package com.hao.walnut.log;

import com.hao.walnut.WALException;
import com.hao.walnut.log.channel.ChannelLogDataFile;
import com.hao.walnut.log.channel.ChannelLogIndexFile;
import com.hao.walnut.log.mmap.MmapLogDataFile;
import com.hao.walnut.log.mmap.MmapLogIndexFile;
import com.hao.walnut.log.raf.RafLogDataFile;
import com.hao.walnut.log.raf.RafLogIndexFile;

import java.io.File;

public class LogFileFactory {
    public static final String Mode_Channel = "channel";
    public static final String Mode_Raf = "raf";
    public static final String Mode_Mmap = "mmap";

    public static LogDataFile createLogDataFile(String name, String mode) throws WALException {
        switch (mode.toLowerCase()) {
            case Mode_Mmap:
                return new MmapLogDataFile(new File(name));
            case Mode_Channel:
                return new ChannelLogDataFile(new File(name));
            case Mode_Raf:
            default:
                return new RafLogDataFile(new File(name));
        }
    }

    public static LogIndexFile createLogIndexFile(String name, String mode) throws WALException {
        switch (mode.toLowerCase()) {
            case Mode_Mmap:
                return new MmapLogIndexFile(new File(name));
            case Mode_Channel:
                return new ChannelLogIndexFile(new File(name));
            case Mode_Raf:
            default:
                return new RafLogIndexFile(new File(name));
        }
    }
}
