package com.hao.walnut.mapfile;

import lombok.Getter;
import java.nio.ByteBuffer;
import java.util.List;

@Getter
public class WriteRequest {
    int position;
    transient ByteBuffer data;
    transient MappedRange mappedRange;
    List<WriteRequest> children;
    WriteRequest parent;
}
