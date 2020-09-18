package com.hao.walnut.log;

import com.hao.walnut.WALRequest;
import lombok.Getter;
import lombok.Setter;

import java.util.function.Consumer;

@Getter
@Setter
public class LogAppendRequest implements LogRequest{
    boolean async;
    String fd;
    byte[] data;
    Consumer<Long> callback;
}
