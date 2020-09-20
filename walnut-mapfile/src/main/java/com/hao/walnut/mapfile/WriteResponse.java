package com.hao.walnut.mapfile;

import lombok.Getter;

import java.util.List;

@Getter
public class WriteResponse {
    boolean success;
    int writeCount;
    WriteRequest writeRequest;
    long gmtWrite;
    long gmtFlush;
    long gmtCreate = System.currentTimeMillis();
    List<WriteResponse> children;

    public long position() {
        return writeRequest.mappedRange.startOffset + writeRequest.getPosition();
    }
}
