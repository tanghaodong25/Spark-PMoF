/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.pmof

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.network.pmof.PmofTransferService
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleWriter}
import org.apache.spark.storage._
import org.apache.spark.util.collection.pmof.PmemExternalSorter
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.storage.pmof._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

private[spark] class PmemShuffleWriter[K, V, C](
                                                 shuffleBlockResolver: PmemShuffleBlockResolver,
                                                 metadataResolver: MetadataResolver,
                                                 handle: BaseShuffleHandle[K, V, C],
                                                 mapId: Int,
                                                 context: TaskContext,
                                                 conf: SparkConf
                                                 )
  extends ShuffleWriter[K, V] with Logging {
  private val dep = handle.dependency
  private val blockManager = SparkEnv.get.blockManager
  private var mapStatus: MapStatus = _
  private val stageId = dep.shuffleId
  private val partitioner = dep.partitioner
  private val numPartitions = partitioner.numPartitions
  private val serInstance: SerializerInstance = dep.serializer.newInstance()
  private val numMaps = handle.numMaps
  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics
  logDebug("This stage has "+ numMaps + " maps")

  val enable_rdma: Boolean = conf.getBoolean("spark.shuffle.pmof.enable_rdma", defaultValue = true)
  val enable_pmem: Boolean = conf.getBoolean("spark.shuffle.pmof.enable_pmem", defaultValue = true)

  val partitionLengths: Array[Long] = Array.fill[Long](numPartitions)(0)
  var set_clean: Boolean = true
  private var sorter: PmemExternalSorter[K, V, _] = _

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private var stopping = false

 
  /** 
  * Call PMDK to write data to persistent memory
  * Original Spark writer will do write and mergesort in this function,
  * while by using pmdk, we can do that once since pmdk supports transaction.
  */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // TODO: keep checking if data need to spill to disk when PM capacity is not enough.
    // TODO: currently, we apply processed records to PM.

    val partitionBufferArray = (0 until numPartitions).toArray.map( partitionId => 
      new PmemBlockObjectStream(
        blockManager.serializerManager,
        serInstance,
        context.taskMetrics(),
        ShuffleBlockId(stageId, mapId, partitionId),
        conf,
        numMaps,
        numPartitions))

    if (dep.mapSideCombine) { // do aggragation
      if (dep.aggregator.isDefined) {
        sorter = new PmemExternalSorter[K, V, C](context, handle, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
				sorter.setPartitionByteBufferArray(partitionBufferArray)
        sorter.insertAll(records)
        sorter.forceSpillToPmem()
      } else {
        throw new IllegalStateException("Aggregator is empty for map-side combine")
      }
    } else { // no aggregation
      while (records.hasNext) {     
        // since we need to write same partition (key, value) together, do a partition index here
        val elem = records.next()
        val partitionId: Int = partitioner.getPartition(elem._1)
        partitionBufferArray(partitionId).write(elem._1, elem._2)
      }
      for (partitionId <- 0 until numPartitions) {
        partitionBufferArray(partitionId).maybeSpill(force = true)
      }
    }

    var spilledPartition = 0
    val partitionSpilled: ArrayBuffer[Int] = ArrayBuffer[Int]()
    while (spilledPartition < numPartitions) {
      if (partitionBufferArray(spilledPartition).ifSpilled()) {
        partitionSpilled.append(spilledPartition)
      }
      spilledPartition += 1
    }
    val data_addr_map = mutable.HashMap.empty[Int, Array[(Long, Int)]]
    var output_str : String = ""

    for (i <- partitionSpilled) {
      if (enable_rdma)
        data_addr_map(i) = partitionBufferArray(i).getPartitionMeta().map{ info => (info._1, info._2)}
      partitionLengths(i) = partitionBufferArray(i).size
      output_str += "\tPartition " + i + ": " + partitionLengths(i) + ", records: " + partitionBufferArray(i).records + "\n"
    }
    for (i <- 0 until numPartitions) {
      partitionBufferArray(i).close()
    }

    logDebug("shuffle_" + dep.shuffleId + "_" + mapId + ": \n" + output_str)

    val shuffleServerId = blockManager.shuffleServerId
    if (enable_rdma) {
      val rkey = partitionBufferArray(0).getRkey()
      metadataResolver.commitPmemBlockInfo(stageId, mapId, data_addr_map, rkey)
      val blockManagerId: BlockManagerId =
        BlockManagerId(shuffleServerId.executorId, PmofTransferService.shuffleNodesMap(shuffleServerId.host),
          PmofTransferService.getTransferServiceInstance(blockManager).port, shuffleServerId.topologyInfo)
      mapStatus = MapStatus(blockManagerId, partitionLengths)
    } else {
      mapStatus = MapStatus(shuffleServerId, partitionLengths)
    }
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        Option(mapStatus)
      } else {
        None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        writeMetrics.incWriteTime(System.nanoTime - startTime)
        sorter = null
      }
    }
  }
}
