package com.micro.core.redis;

import io.vertx.core.buffer.Buffer;

import java.nio.charset.Charset;

public class TestCase implements BufferIO
{
    private String id;
    private int num;
    private long age;
    private static final Charset UTF8 = Charset.forName("UTF-8");

    public TestCase(String id, int num, long age)
    {
        this.id  = id;
        this.num = num;
        this.age = age;
    }
    public TestCase() {};

    @Override
    public void writeToBuffer(Buffer buff)
    {
        byte[] bytes = this.id.getBytes(UTF8);
        buff.appendInt(bytes.length).appendBytes(bytes);
        buff.appendInt(num);
        buff.appendLong(age);
    }

    @Override
    public int readFromBuffer(Buffer buffer)
    {
        if (buffer == null)
        {
            return 0;
        }
        int pos = 0;
        int len = buffer.getInt(pos);
        pos += 4;
        byte[] bytes = buffer.getBytes(pos, pos + len);
        pos += len;
        id = new String(bytes, UTF8);
        num = buffer.getInt(pos);
        pos += 4;
        age = buffer.getLong(pos);
        pos +=8;
        return pos;
    }

    public String toString()
    {
        return new StringBuilder(100).append("id:").append(id).append(" num:").append(num).append(" age:").append(age).toString();
    }

    public String getKey() {return  id;}
}
