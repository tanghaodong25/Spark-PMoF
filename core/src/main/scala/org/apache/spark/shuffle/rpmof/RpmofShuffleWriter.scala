package org.apache.spark.shuffle.rpmof

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleWriter}

private[spark] class RpmofShuffleWriter[K, V, C](handle: BaseShuffleHandle[K, V, C],
                                                 mapId: Int,
                                                 context: TaskContext) extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = _

  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics

  private val serInstance: SerializerInstance = dep.serializer.newInstance()
  private val objectStream = new RemotePmofObjectStream(blockManager.serializerManager, serInstance, handle, mapId)

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    if (dep.mapSideCombine) {
      return
    }

    while (records.hasNext) {
      val entry = records.next()
      objectStream.write(entry._1, entry._2)
    }
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    objectStream.close
    None
  }
}
