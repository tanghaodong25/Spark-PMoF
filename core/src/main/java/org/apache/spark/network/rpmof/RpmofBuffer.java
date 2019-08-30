package org.apache.spark.network.rpmof;

import com.intel.hpnl.core.HpnlBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.shuffle.rpmof.RpmofTransferService;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class RpmofBuffer extends ManagedBuffer {
    private final long size;
    private final long address;
    private final ByteBuffer byteBuffer;
    private ByteBuf buf;
    private final HpnlBuffer hpnlBuffer;

    public RpmofBuffer(long bufferSize, RpmofTransferService transferService) throws IOException {
        this.size = bufferSize;
        this.buf = PooledByteBufAllocator.DEFAULT.directBuffer((int) this.size, (int)this.size);
        this.address = this.buf.memoryAddress();
        this.byteBuffer = this.buf.nioBuffer(0, (int)size);
        this.hpnlBuffer = transferService.regAsRmaBuffer(this.byteBuffer, this.address, this.size);
    }

    public int getBufferId() {
        return this.hpnlBuffer.getBufferId();
    }

    public long size() {
        return this.size;
    }

    public ByteBuffer nioByteBuffer() {
        return byteBuffer;
    }

    public ManagedBuffer release() {
        return this;
    }

    public ManagedBuffer close() {
        this.buf.release();
        return this;
    }

    public Object convertToNetty() {
        return null;
    }

    public InputStream createInputStream() {
        return new RpmofBufferInputStream(this);
    }

    public ManagedBuffer retain() {
        if (this.buf != null) {
            this.buf.retain();
        }
        return this;
    }
}
