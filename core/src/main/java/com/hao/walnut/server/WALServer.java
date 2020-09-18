package com.hao.walnut.server;

import com.hao.walnut.WALException;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public interface WALServer {

    /**
     * 异步写入到文件
     * @param fd
     * @param data
     * @param callback
     * @throws WALException
     */
    Future<Long> append(String fd, byte[] data, Consumer<Long> callback) throws WALException;

    /**
     *
     * @param fd
     * @param offset
     * @return
     * @throws WALException
     */
    byte[] read(String fd, long offset) throws WALException;
}
