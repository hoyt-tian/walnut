package com.hao.walnut.mapfile;

import java.util.List;

public class WriteResponse {
    boolean success;
    int writeCount;
    WriteRequest writeRequest;
    long gmtWrite;
    long gmtFlush;
    long gmtCreate = System.currentTimeMillis();
    List<WriteResponse> children;
}
