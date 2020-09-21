package com.hao.walnut.mapfile;

import lombok.Getter;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

@Getter
public class WriteResponse {
    boolean success;
    int writeCount;
    WriteRequest writeRequest;
    long gmtWrite;
    long gmtCommit = 0;
    long gmtCreate = System.currentTimeMillis();
    List<WriteResponse> children;
    public long position() {
        return writeRequest.mappedRange.startOffset + writeRequest.getPosition();
    }
}
