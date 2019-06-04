package com.micro.core.redis;

import io.vertx.core.buffer.Buffer;

public interface BufferIO
{
    public String getKey();
    public void writeToBuffer(Buffer buff);
    public int readFromBuffer(Buffer buffer);
}
