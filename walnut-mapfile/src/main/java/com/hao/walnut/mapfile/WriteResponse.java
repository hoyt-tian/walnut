package com.hao.walnut.mapfile;

import lombok.Getter;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

@Getter
public class WriteResponse {
    boolean success;
    int writeCount;
    WriteRequest writeRequest;
    long gmtWrite;
    long gmtCommit;
    long gmtCreate = System.currentTimeMillis();
    List<WriteResponse> children;
    Semaphore commitLock;
    public long position() {
        return writeRequest.mappedRange.startOffset + writeRequest.getPosition();
    }
    public int writeBytesCount() {
        if (children == null) {
            return writeCount;
        } else {
            int writeCount = 0;
            for(WriteResponse c : children) {
                writeCount += c.writeCount;
            }
            return writeCount;
        }
    }

    public void commit() {
        gmtCommit = System.currentTimeMillis();
        if (children == null) {
            getWriteRequest().mappedRange.commitPosition.addAndGet(writeCount);
        } else {
            for(WriteResponse resp : children) {
                resp.getWriteRequest().mappedRange.commitPosition.addAndGet(resp.writeCount);
                resp.gmtCommit = gmtCommit;
            }
        }
    }
}
