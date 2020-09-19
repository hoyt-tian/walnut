package com.hao.walnut.core;

public class WALException extends Exception {
    public WALException(String format) {
        super(format);
    }

    public WALException(String format, Object...args) {
        super(String.format(format, args));
    }
}
