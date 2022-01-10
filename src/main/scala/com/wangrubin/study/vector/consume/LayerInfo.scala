package com.wangrubin.study.vector.consume

case class LayerInfo(layerName: String, zoomBound: ZoomBound, attrFields: Array[String]) {
  private var layerId: Byte = _

  def setLayerId(layerId: Byte): Unit = {
    this.layerId = layerId
  }

  def getLayerId: Byte = {
    require(null != layerId, "layer id can't be null")
    layerId
  }
}
