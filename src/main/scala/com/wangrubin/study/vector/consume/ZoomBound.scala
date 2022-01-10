package com.wangrubin.study.vector.consume

class ZoomBound(levels: Array[Int]) extends Serializable {
  val minLevel: Int = levels.min
  val maxLevel: Int = levels.max

  def contains(targetLevel: Int): Boolean = {
    levels.contains(targetLevel)
  }
}

object ZoomBound {
  def apply(minZoomLevel: Int, maxZoomLevel: Int): ZoomBound = {
    require(minZoomLevel <= maxZoomLevel, "min zoom level must be less or equals max zoom level")
    new ZoomBound((minZoomLevel to maxZoomLevel).toArray)
  }
}
