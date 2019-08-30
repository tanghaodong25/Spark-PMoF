package org.apache.spark.shuffle.rpmof

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.shuffle._
import org.apache.spark.internal.Logging

private[spark] class RpmofShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {
  override def registerShuffle[K, V, C](shuffleId: Int, numMaps: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    new BaseShuffleHandle(shuffleId, numMaps, dependency)
  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V] = {
    new RpmofShuffleWriter(handle.asInstanceOf[BaseShuffleHandle[K, V, _]], mapId, context)
  }

  override def getReader[K, C](handle: ShuffleHandle, startPartition: Int, endPartition: Int, context: TaskContext): ShuffleReader[K, C] = {
    new RpmofShuffleReader(handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = ???

  override def shuffleBlockResolver: ShuffleBlockResolver = ???

  override def stop(): Unit = ???
}
