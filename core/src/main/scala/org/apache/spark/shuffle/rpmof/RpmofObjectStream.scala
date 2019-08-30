package org.apache.spark.shuffle.rpmof

import java.io.OutputStream

import org.apache.spark.serializer.{SerializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.storage.ShuffleBlockId

class RemotePmofObjectStream[K, V, C](serializerManager: SerializerManager,
                                      serializerInstance: SerializerInstance,
                                      handle: BaseShuffleHandle[K, V, C],
                                      mapId: Int) {
  private val partitioner = handle.dependency.partitioner
  private val numPartitions = partitioner.numPartitions
  private val byteStreamArray = new Array[OutputStream](numPartitions)
  private val wrappedStreamArray = new Array[OutputStream](numPartitions)
  private val serializationStreamArray = new Array[SerializationStream](numPartitions)
  private val closed = false

  for (i <- 0 to numPartitions-1) {
    val shuffleBlockId = new ShuffleBlockId(handle.dependency.shuffleId, mapId, i)
    byteStreamArray(i) = new RpmofOutputStream(shuffleBlockId)
    wrappedStreamArray(i) = serializerManager.wrapStream(shuffleBlockId, byteStreamArray(i))
    serializationStreamArray(i) = serializerInstance.serializeStream(wrappedStreamArray(i))
  }

  def write(key: Any, value: Any) : Unit = {
    val partitionId = partitioner.getPartition(key)
    serializationStreamArray(partitionId).writeObject(key)
    serializationStreamArray(partitionId).writeObject(value)
  }

  def close(): Unit = {
    if (!closed) {
      for (serializationStream <- serializationStreamArray) {
        serializationStream.close()
      }
      for (wrappedStream <- wrappedStreamArray) {
        wrappedStream.close()
      }
      for (byteStream <- byteStreamArray) {
        byteStream.close()
      }
    }
  }
}
