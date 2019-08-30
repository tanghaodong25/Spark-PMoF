package org.apache.spark.shuffle.rpmof

import java.io.OutputStream
import java.nio.ByteBuffer

import com.intel.hpnl.core.HpnlBuffer
import io.netty.buffer.{ByteBuf, PooledByteBufAllocator}
import org.apache.spark.storage.ShuffleBlockId

class RpmofOutputStream(shuffleBlockId: ShuffleBlockId) extends OutputStream {

  val transferService: RpmofTransferService = RpmofTransferService.getTransferServiceInstance()

  val bufferSize: Int = 1024*1024*4
  var total: Int = 0
  val buf: ByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(bufferSize, bufferSize)
  val byteBuffer: ByteBuffer = buf.nioBuffer(0, bufferSize)
  val rmaBuffer: HpnlBuffer = transferService.regAsRmaBuffer(byteBuffer, buf.memoryAddress(), bufferSize)

  var closed = false

  override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
    if (total+len > bufferSize) {
      flush()
    }
    byteBuffer.put(bytes, off, len)
    total += len
  }

  override def write(byte: Int): Unit = {
    byteBuffer.putInt(byte)
    total += 4
  }

  override def flush(): Unit = {
    transferService.pushBlock(shuffleBlockId.shuffleId,
                              shuffleBlockId.mapId,
                              shuffleBlockId.reduceId,
                              buf.memoryAddress(),
                              total,
                              rmaBuffer.getRKey)
    reset()
  }

  def size(): Int = {
    total
  }

  def reset(): Unit = {
    total = 0
    byteBuffer.clear()
  }

  override def close(): Unit = {
    if (!closed) {
      flush()
      reset()
      buf.release()
      closed = true
    }
  }
}
