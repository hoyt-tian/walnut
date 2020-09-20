package com.hao.walnut.mapfile;

import lombok.Getter;
import java.nio.ByteBuffer;
import java.util.List;

@Getter
public class WriteRequest {
    int position;
    ByteBuffer data;
    MappedRange mappedRange;
    List<WriteRequest> children;
}
