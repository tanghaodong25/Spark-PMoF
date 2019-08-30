package org.apache.spark.shuffle.rpmof

import java.nio.ByteBuffer

import org.apache.spark.network.rpmof.BlockMetadataMsg

trait PushBlockCallback {
  def onSuccess(): Unit
  def onFailure(e: Throwable): Unit
}

trait GetBlockMetadataCallback {
  def onSuccess(blockMetadata: BlockMetadataMsg): Unit
  def onFailure(e: Throwable): Unit
}

trait GetBlockCallback {
  def onSuccess(bufferSize: Int): Unit
  def onFailure(e: Throwable): Unit
}
