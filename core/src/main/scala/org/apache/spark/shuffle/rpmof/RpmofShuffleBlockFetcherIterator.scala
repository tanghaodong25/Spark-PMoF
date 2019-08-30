package org.apache.spark.shuffle.rpmof

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

import java.io.{File, IOException, InputStream}
import java.util.concurrent.LinkedBlockingQueue

import javax.annotation.concurrent.GuardedBy
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.rpmof.{BlockMetadataMsg, RpmofBuffer}
import org.apache.spark.network.shuffle.TempFileManager
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage._
import org.apache.spark.{SparkException, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

private[spark]
final class RpmofShuffleBlockFetcherIterator(context: TaskContext,
                                             blockManager: BlockManager,
                                             shuffleId: Int,
                                             startPartition: Int,
                                             endPartition: Int,
                                             streamWrapper: (BlockId, InputStream) => InputStream)
  extends Iterator[(BlockId, InputStream)] with TempFileManager with Logging {

  import RpmofShuffleBlockFetcherIterator._

  private[this] var numBlocksToFetch = 0

  /**
    * The number of blocks processed by the caller. The iterator is exhausted when
    * [[numBlocksProcessed]] == [[numBlocksToFetch]].
    */
  private[this] var numBlocksProcessed = 0

  /**
    * A queue to hold our results. This turns the asynchronous model provided by
    * [[org.apache.spark.network.BlockTransferService]] into a synchronous model (iterator).
    */
  private[this] val results = new LinkedBlockingQueue[FetchResult]

  /**
    * Current [[FetchResult]] being processed. We track this so we can release the current buffer
    * in case of a runtime exception when processing the current buffer.
    */
  @volatile private[this] var currentResult: SuccessFetchResult = _

  private[this] val shuffleMetrics = context.taskMetrics().createTempShuffleReadMetrics()

  /**
    * Whether the iterator is still active. If isZombie is true, the callback interface will no
    * longer place fetched blocks into [[results]].
    */
  @GuardedBy("this")
  private[this] var isZombie = false

  /**
    * A set to store the files used for shuffling remote huge blocks. Files in this set will be
    * deleted when cleanup. This is a layer of defensiveness against disk file leaks.
    */
  @GuardedBy("this")
  private[this] val shuffleFilesSet = mutable.HashSet[File]()

  private[this] val rmaRequestQueue = new LinkedBlockingQueue[RmaRequest]()

  private[this] val transferService: RpmofTransferService = RpmofTransferService.getTransferServiceInstance()

  private[this] val maxBytesInFlight = 40 * 1024 * 1024
  private[this] val rmaBufferSize = 4 * 1024 * 1024

  initialize()

  def initialize(): Unit = {
    context.addTaskCompletionListener(_ => cleanup())
    startFetch()
  }

  def startFetch(): Unit = {
    startFetchMetadata()
  }

  private class RmaBlock(val shuffleBlockId: ShuffleBlockId, val address: Long, val length: Long, val rkey: Long) {}

  private class RmaRequest() {
    val arrayBuffer: ArrayBuffer[RmaBlock] = new ArrayBuffer[RmaBlock]()
  }

  def startFetchMetadata(): Unit = {
    val getBlockMetadataCallback: GetBlockMetadataCallback = new GetBlockMetadataCallback {
      override def onSuccess(blockMetadata: BlockMetadataMsg): Unit = {
        val blockMetadataLength = blockMetadata.blockMetadataLength()
        numBlocksToFetch += blockMetadataLength
        var rmaRequest = new RmaRequest()
        var requestSize = 0
        for (i <- 0 until blockMetadataLength) {
          val shuffleId = blockMetadata.blockMetadata(i).shuffleId()
          val mapId = blockMetadata.blockMetadata(i).mapId()
          val reduceId = blockMetadata.blockMetadata(i).reduceId()
          val address = blockMetadata.blockMetadata(i).buffer()
          val length = blockMetadata.blockMetadata(i).size()
          assert(length <= rmaBufferSize)
          val rkey = blockMetadata.blockMetadata(i).rkey()
          if (requestSize+length > maxBytesInFlight) {
            rmaRequestQueue.put(rmaRequest)
            Future {
              fetchRemoteBlocks()
            }
            rmaRequest = new RmaRequest()
            requestSize = 0
          } else {
            rmaRequest.arrayBuffer += new RmaBlock(ShuffleBlockId(shuffleId, mapId, reduceId), address, length, rkey)
            requestSize += length
          }
        }
        rmaRequestQueue.put(rmaRequest)
        Future {
          fetchRemoteBlocks()
        }
        requestSize = 0
      }

      override def onFailure(e: Throwable): Unit = {
      }
    }
    transferService.getBlockMetadata(shuffleId, startPartition, endPartition, getBlockMetadataCallback)
  }

  def fetchRemoteBlocks(): Unit = {
    val rmaRequest = rmaRequestQueue.poll()
    if (rmaRequest == null) {
      return
    }
    sendRequest(rmaRequest)
  }

  def sendRequest(rmaRequest: RmaRequest): Unit = {
    for (i <- rmaRequest.arrayBuffer.indices) {
      val shuffleBlockId = rmaRequest.arrayBuffer(i).shuffleBlockId

      val remoteAddress = rmaRequest.arrayBuffer(i).address
      val remoteLength = rmaRequest.arrayBuffer(i).length
      val remoteRkey = rmaRequest.arrayBuffer(i).rkey

      val rpmofBuffer = new RpmofBuffer(rmaBufferSize, transferService)

      val getBlockCallback = new GetBlockCallback {
        override def onSuccess(bufferSize: Int): Unit = {
          assert(remoteLength == bufferSize)
          results.put(SuccessFetchResult(shuffleBlockId, bufferSize, rpmofBuffer))
        }

        override def onFailure(e: Throwable): Unit = {}
      }

      transferService.getBlock(rpmofBuffer.getBufferId,
        0,
        remoteAddress,
        remoteLength,
        remoteRkey,
        getBlockCallback)
    }
  }

    /**
    * Fetches the next (BlockId, InputStream). If a task fails, the ManagedBuffers
    * underlying each InputStream will be freed by the cleanup() method registered with the
    * TaskCompletionListener. However, callers should close() these InputStreams
    * as soon as they are no longer needed, in order to release memory as early as possible.
    *
    * Throws a FetchFailedException if the next block could not be fetched.
    */
  override def next(): (BlockId, InputStream) = {
    if (!hasNext) {
      throw new NoSuchElementException
    }

    numBlocksProcessed += 1

    var result: FetchResult = null
    var input: InputStream = null
    // Take the next fetched result and try to decompress it to detect data corruption,
    // then fetch it one more time if it's corrupt, throw FailureFetchResult if the second fetch
    // is also corrupt, so the previous stage could be retried.
    // For local shuffle block, throw FailureFetchResult for the first IOException.
    while (result == null) {
      val startFetchWait = System.currentTimeMillis()
      result = results.take()
      val stopFetchWait = System.currentTimeMillis()
      shuffleMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)

      result match {
        case SuccessFetchResult(blockId, size, buf) =>
          shuffleMetrics.incRemoteBlocksFetched(1)
          shuffleMetrics.incRemoteBytesReadToDisk(size)
          val in = try {
            buf.createInputStream()
          } catch {
            // The exception could only be throwed by local shuffle block
            case e: IOException =>
              assert(buf.isInstanceOf[FileSegmentManagedBuffer])
              logError("Failed to create input stream from local block", e)
              buf.release()
              throwFetchFailedException(blockId, _, e)
          }

          input = streamWrapper(blockId, in)
        // Only copy the stream if it's wrapped by compression or encryption, also the size of
        // block is small (the decompressed block is smaller than maxBytesInFlight)
        case FailureFetchResult(blockId, address, e) =>
          throwFetchFailedException(blockId, address, e)
      }

      // Send fetch requests up to maxBytesInFlight
      Future {
        fetchRemoteBlocks()
      }
    }

    currentResult = result.asInstanceOf[SuccessFetchResult]
    (currentResult.blockId, new RpmofBufferReleasingInputStream(input, this))
  }

  // Decrements the buffer reference count.
  // The currentResult is set to null to prevent releasing the buffer again on cleanup()
  private[storage] def releaseCurrentResultBuffer(): Unit = {
    // Release the current buffer if necessary
    if (currentResult != null) {
      currentResult.buf.release()
    }
    currentResult = null
  }

  override def createTempFile(): File = {
    blockManager.diskBlockManager.createTempLocalBlock()._2
  }

  override def registerTempFileToClean(file: File): Boolean = synchronized {
    if (isZombie) {
      false
    } else {
      shuffleFilesSet += file
      true
    }
  }

  /**
    * Mark the iterator as zombie, and release all buffers that haven't been deserialized yet.
    */
  private[this] def cleanup() {
    synchronized {
      isZombie = true
    }
    releaseCurrentResultBuffer()
    // Release buffers in the results queue
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(_, _, buf) =>
          buf.release()
        case _ =>
      }
    }
    shuffleFilesSet.foreach { file =>
      if (!file.delete()) {
        logWarning("Failed to cleanup shuffle fetch temp file " + file.getAbsolutePath)
      }
    }
  }

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch


  private def throwFetchFailedException(blockId: BlockId, address: BlockManagerId, e: Throwable) = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, e)
      case _ =>
        throw new SparkException(
          "Failed to get block " + blockId + ", which is not a shuffle block", e)
    }
  }
}


/**
  * Helper class that ensures a ManagedBuffer is released upon InputStream.close()
  */
private class RpmofBufferReleasingInputStream(private val delegate: InputStream,
                                              private val iterator: RpmofShuffleBlockFetcherIterator)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int = delegate.read()

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      iterator.releaseCurrentResultBuffer()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = delegate.skip(n)

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = delegate.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = delegate.read(b, off, len)

  override def reset(): Unit = delegate.reset()
}

private[storage]
object RpmofShuffleBlockFetcherIterator {
  case class FetchRequest(address: BlockManagerId, blocks: Seq[(BlockId, Long)]) {
    val size: Long = blocks.map(_._2).sum
  }

  private[storage] sealed trait FetchResult {
    val blockId: BlockId
  }

  private[storage] case class SuccessFetchResult(blockId: BlockId,
                                                 size: Long,
                                                 buf: ManagedBuffer) extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }

  private[storage] case class FailureFetchResult(
                                                  blockId: BlockId,
                                                  address: BlockManagerId,
                                                  e: Throwable)
    extends FetchResult

}
