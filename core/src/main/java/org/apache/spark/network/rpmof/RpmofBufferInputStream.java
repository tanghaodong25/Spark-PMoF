package org.apache.spark.network.rpmof;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class RpmofBufferInputStream extends InputStream {
    private final RpmofBuffer rpmofBuffer;
    private final ByteBuffer buf;

    public RpmofBufferInputStream(RpmofBuffer rpmofBuffer) {
        this.rpmofBuffer = rpmofBuffer;
        this.buf = rpmofBuffer.nioByteBuffer();
    }

    public int read() {
        if (!buf.hasRemaining()) {
            return -1;
        }
        return buf.get() & 0xFF;
    }

    public int read(byte[] bytes, int off, int len) throws IOException {
        if (!buf.hasRemaining()) {
            return -1;
        }

        len = Math.min(len, buf.remaining());
        buf.get(bytes, off, len);
        return len;
    }

    public void close() {
        rpmofBuffer.close();
    }

    public int available() {
        return buf.remaining();
    }
}
