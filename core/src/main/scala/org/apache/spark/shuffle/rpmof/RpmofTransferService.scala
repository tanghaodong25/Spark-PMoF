package org.apache.spark.shuffle.rpmof

import java.nio.ByteBuffer
import java.util.Random
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.google.flatbuffers.FlatBufferBuilder
import com.intel.hpnl.core.{Connection, Handler, HpnlBuffer}
import com.intel.hpnl.service.Client
import org.apache.spark.network.rpmof.{BlockMetadataMsg, GetBlockMetadataMsg, PushBlockMsg}
import org.apache.spark.{SparkConf, SparkEnv}

class RpmofTransferService(conf: SparkConf) {
  private var nextReqId: AtomicLong = _
  private var client: Client = _
  private val pmofServerHost: String = conf.get("spark.shuffle.pmof.server_ip", "172.168.0.143")
  private val pmofServerPort: Int = conf.getInt("spark.shuffle.pmof.server_port", 9100)
  private val bufferSize: Int = conf.getInt("spark.shuffle.pmof.buffer_size", 4096*3)
  private val bufferNbr: Int = conf.getInt("spark.shuffle.pmof.buffer_number", 32)
  private var con: Connection = _
  private val outstandingPushBlockCallback: ConcurrentHashMap[Long, PushBlockCallback] =
    new ConcurrentHashMap[Long, PushBlockCallback]()
  private val outstandingGetBlockMetadataCallback: ConcurrentHashMap[Long, GetBlockMetadataCallback] =
    new ConcurrentHashMap[Long, GetBlockMetadataCallback]()
  private val outstandingGetBlockCallback: ConcurrentHashMap[Long, GetBlockCallback] =
    new ConcurrentHashMap[Long, GetBlockCallback]()

  def init(): Unit = {
    client = new Client(1, bufferNbr)

    val recvCallback = new RecvCallback
    val readCallback = new ReadCallback
    client.setRecvCallback(recvCallback)
    client.setReadCallback(readCallback)

    client.initBufferPool(bufferNbr, bufferSize, bufferNbr)
    client.start()

    con = client.connect(pmofServerHost, pmofServerPort.toString, 0)
    assert(con != null)

    val random = new Random().nextInt(Integer.MAX_VALUE)
    nextReqId = new AtomicLong(random)
  }

  def close(): Unit = {
    client.shutdown()
    client.join()
  }

  def regAsRmaBuffer(byteBuffer: ByteBuffer, address: Long, size: Long): HpnlBuffer = {
    client.regRmaBufferByAddress(byteBuffer, address, size)
  }

  def getRmaBufferById(bufferId: Int): ByteBuffer = {
    client.getRmaBufferByBufferId(bufferId)
  }

  def pushBlock(shuffleId: Int, mapId: Int, partitionId: Int, address: Long, length: Long, rkey: Long): Unit = {
    val builder = new FlatBufferBuilder()
    val msg = PushBlockMsg.createPushBlockMsg(builder, shuffleId, mapId, partitionId, address, length, rkey)
    builder.finish(msg)
    val byteArray = builder.sizedByteArray
    val byteBuffer = ByteBuffer.wrap(byteArray)

    val latch = new CountDownLatch(1)
    val pushBlockCallback = new PushBlockCallback {
      override def onSuccess(): Unit = {
        latch.countDown()
      }

      override def onFailure(e: Throwable): Unit = {}
    }
    val seq = nextReqId.getAndIncrement()
    outstandingPushBlockCallback.put(seq, pushBlockCallback)

    con.send(byteBuffer, 0, seq)

    latch.wait()
  }

  def getBlockMetadata(shuffleId: Int,
                       startPartition: Int,
                       endParition: Int,
                       getBlockMetadataCallback: GetBlockMetadataCallback): Unit = {
    val builder = new FlatBufferBuilder()
    val msg = GetBlockMetadataMsg.createGetBlockMetadataMsg(builder, shuffleId, startPartition, endParition)
    builder.finish(msg)
    val byteArray = builder.sizedByteArray
    val byteBuffer = ByteBuffer.wrap(byteArray)
    val seq = nextReqId.getAndIncrement()
    outstandingGetBlockMetadataCallback.put(seq, getBlockMetadataCallback)

    con.send(byteBuffer, 1, seq)
  }

  def getBlock(bufferId: Int,
               localOffset: Int,
               remoteAddress: Long,
               remoteLength: Long,
               remoteRkey: Long,
               getBlockCallback: GetBlockCallback): Unit = {
    val seq = nextReqId.getAndIncrement()
    outstandingGetBlockCallback.put(seq, getBlockCallback)

    con.read(bufferId, localOffset, remoteLength, remoteAddress, remoteRkey)
  }

  class RecvCallback extends Handler {
    override def handle(connection: Connection, bufferId: Int, bufferSize: Int): Unit = {
      val buffer: HpnlBuffer = con.getRecvBuffer(bufferId)
      val seq = buffer.getSeq
      val msgType = buffer.getType
      if (msgType == 0.toByte) {
        outstandingPushBlockCallback.get(seq).onSuccess()
      } else if (msgType == 1.toByte) {
        val byteBuffer: ByteBuffer = buffer.get(bufferSize)
        val blockMetadata = BlockMetadataMsg.getRootAsBlockMetadataMsg(byteBuffer)
        outstandingGetBlockMetadataCallback.get(seq).onSuccess(blockMetadata)
      } else {

      }
    }
  }

  class ReadCallback extends Handler {
    override def handle(connection: Connection, bufferId: Int, bufferSize: Int): Unit = {
      val buffer: HpnlBuffer = con.getRecvBuffer(bufferId)
      val seq = buffer.getSeq
      outstandingGetBlockCallback.get(seq).onSuccess(bufferSize)
    }
  }
}

object RpmofTransferService {
  final val conf: SparkConf = SparkEnv.get.conf

  private val initialized = new AtomicBoolean(false)
  private var transferService: RpmofTransferService = _

  def getTransferServiceInstance(): RpmofTransferService = {
    if (!initialized.get()) {
      RpmofTransferService.this.synchronized {
        if (initialized.get()) return transferService
        transferService = new RpmofTransferService(conf)
        transferService.init()
        initialized.set(true)
        transferService
      }
    } else {
      transferService
    }
  }
}


