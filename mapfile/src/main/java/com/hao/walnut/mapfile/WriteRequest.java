package com.hao.walnut.mapfile;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Slf4j
public class WriteRequest {
    int position;
    ByteBuffer data;
    MappedRange mappedRange;
    List<WriteRequest> children;
    static ExecutorService executorService = Executors.newCachedThreadPool();

    /**
     * 调度写入操作
     * @return
     */
    public Future<WriteResponse> execute() {
        if (children == null) {
            log.info("execute single insertion @={}, size={}", this.position + mappedRange.startOffset,data.capacity());
            return executorService.submit(() -> {
                WriteResponse writeResponse = new WriteResponse();
                writeResponse.writeRequest = this;
                try {
                    writeResponse.writeCount = mappedRange.write(position, data);
                    writeResponse.gmtWrite = System.currentTimeMillis();
                    writeResponse.success = true;
                    return writeResponse;
                } catch (IOException e) {
                    writeResponse.success = false;
                    return writeResponse;
                }
            });
        } else {
            WriteRequest left = children.get(0);
            WriteRequest right = children.get(1);
            log.info("execute insertion with children, left=[@={},size={}], right=[@={},size={}]",left.position + left.mappedRange.startOffset, left.data.capacity(), right.position + right.mappedRange.startOffset, right.data.capacity());
            return executorService.submit(() -> {
                WriteResponse response = new WriteResponse();
                response.writeRequest = this;
                response.success = true;
                response.children = new LinkedList<>();
                for(WriteRequest writeRequest : children) {
                    Future<WriteResponse> responseFuture = writeRequest.execute();
                    try {
                        WriteResponse resp = responseFuture.get();
                        response.success &= resp.success;
                        response.children.add(resp);
                    } catch (InterruptedException|ExecutionException e) {
                        response.success = false;
                        log.error("{}", e.getMessage());
                        return response;
                    }
                }
                return response;
            });
        }
    }
}
