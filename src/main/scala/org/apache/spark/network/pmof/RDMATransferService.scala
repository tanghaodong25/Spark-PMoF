package org.apache.spark.network.pmof

import java.util.Random
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.client.ChunkReceivedCallback
import org.apache.spark.network.shuffle.protocol.OpenBlocks
import org.apache.spark.network.shuffle.{BlockFetchingListener, TempFileManager}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.BlockManager
import org.apache.spark.{SparkConf, SparkEnv}

class RDMATransferService(conf: SparkConf, val hostname: String, var port: Int) extends TransferService {

  private var server: RDMAServer = _
  private var recvHandler: ServerRecvHandler = _
  private var clientFactory: RDMAClientFactory = _
  private var appId: String = _
  private var nextReqId: AtomicInteger = _

  private val serializer = new JavaSerializer(conf)

  override def fetchBlocks(host: String,
                           port: Int,
                           executId: String,
                           blockIds: Array[String],
                           blockFetchingListener: BlockFetchingListener,
                           tempFileManager: TempFileManager): Unit = {}

  def fetchBlocks(reqHost: String,
                  reqPort: Int,
                  execId: String,
                  blockIds: Array[String],
                  callback: ChunkReceivedCallback,
                  tempFileManager: TempFileManager): Unit = {
    val client = clientFactory.createClient(reqHost, reqPort)
    val openBlocks: OpenBlocks = new OpenBlocks(appId, execId, blockIds)
    client.send(openBlocks.toByteBuffer, nextReqId.getAndIncrement(), callback)
  }

  override def close(): Unit = {
    if (clientFactory != null) {
      clientFactory.stop()
      clientFactory.waitToStop()
    }
    if (server != null) {
      server.stop()
      server.waitToStop()
    }
  }

  override def init(blockManager: BlockDataManager): Unit = {
    this.server = new RDMAServer(hostname, port)
    this.appId = conf.getAppId
    this.recvHandler = new ServerRecvHandler(server, appId, serializer, blockManager)
    this.server.setRecvHandler(recvHandler)
    this.clientFactory = new RDMAClientFactory()
    this.server.init()
    this.server.start()
    this.port = server.port
    val random = new Random().nextInt(Integer.MAX_VALUE)
    this.nextReqId = new AtomicInteger(random)
  }
}

object RDMATransferService {
  val env: SparkEnv = SparkEnv.get
  val conf: SparkConf = env.conf
  private var initialized = 0
  private var transferService: RDMATransferService = _
  def getTransferServiceInstance(blockManager: BlockManager): RDMATransferService = synchronized {
    if (initialized == 0) {
      transferService = new RDMATransferService(conf, blockManager.shuffleServerId.host, 0)
      transferService.init(blockManager)
      initialized = 1
      transferService
    } else {
      transferService
    }
  }
}