package com.hao.walnut.mapfile;

import lombok.extern.slf4j.Slf4j;
import java.nio.ByteBuffer;
import java.util.List;

@Slf4j
public class WriteRequest {
    int position;
    ByteBuffer data;
    MappedRange mappedRange;
    List<WriteRequest> children;
}
