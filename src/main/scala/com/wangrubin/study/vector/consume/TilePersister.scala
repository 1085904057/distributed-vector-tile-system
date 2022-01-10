package com.wangrubin.study.vector.consume

import com.wangrubin.study.vector.feature.TileCoord

trait TilePersister extends Serializable {
  def persist(tileCoord: TileCoord, tileBytes: Array[Byte]): Unit
}
