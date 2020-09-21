package com.hao.walnut.mapfile;

import java.io.IOException;

@FunctionalInterface
public interface WriteCallback {
    void call(WriteResponse writeResponse) throws IOException;
}
