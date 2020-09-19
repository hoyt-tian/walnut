package com.hao.walnut.log;

import java.io.IOException;

public interface LogFile {
    long fileSize() throws IOException;
}
