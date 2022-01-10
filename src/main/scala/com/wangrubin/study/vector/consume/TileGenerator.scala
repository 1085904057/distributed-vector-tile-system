package com.wangrubin.study.vector.consume

import com.wangrubin.study.vector.feature.{TileCoord, TileInfo, WrapFeature}
import no.ecc.vectortile.VectorTileEncoder
import TileGenerator.{EXTENT, MAX_FEATURE_NUM}

import scala.collection.JavaConverters.mapAsJavaMapConverter

private[vector] class TileGenerator(layerInfos: Array[LayerInfo], persister: TilePersister) extends Serializable {
  def maxLevel: Int = {
    layerInfos.map(_.zoomBound.maxLevel).max
  }

  def minLevel: Int = {
    layerInfos.map(_.zoomBound.minLevel).min
  }

  def generateAndReduce(tileInfo: TileInfo, tileFeatures: Array[WrapFeature]): Array[WrapFeature] = {
    val sampleFeatures = if (tileFeatures.length > MAX_FEATURE_NUM) sample(tileFeatures) else tileFeatures
    generateTile(tileInfo.tileCoord, sampleFeatures)
    reduceFeatures(tileInfo.tileCoord, sampleFeatures)
  }

  private def generateTile(tileCoord: TileCoord, sampleFeatures: Array[WrapFeature]): Unit = {
    if (sampleFeatures.nonEmpty) {
      val tileZoomLevel = tileCoord.zoomLevel
      val validLayerInfos = layerInfos.filter(_.zoomBound.contains(tileZoomLevel))
        .map(info => (info.getLayerId, info)).toMap
      persister.persist(tileCoord, encode(sampleFeatures, validLayerInfos))
    }
  }

  private def reduceFeatures(tileCoord: TileCoord, sampleFeatures: Array[WrapFeature]): Array[WrapFeature] = {
    val tileZoomLevel = tileCoord.zoomLevel
    val validLayerIds = layerInfos.filter(_.zoomBound.minLevel < tileZoomLevel).map(_.getLayerId)
    sampleFeatures.filter(feature => {
      validLayerIds.contains(feature.layerId)
    })
  }

  private def sample(features: Array[WrapFeature]): Array[WrapFeature] = {
    if (features.length <= MAX_FEATURE_NUM) return features
    val numPerGroup = Math.round(features.length / MAX_FEATURE_NUM)
    features.grouped(numPerGroup).map(groupFeatures => {
      groupFeatures.head
    }).toArray
  }

  private def encode(tileFeatures: Array[WrapFeature], validLayerInfos: Map[Byte, LayerInfo]): Array[Byte] = {
    val encoder = new VectorTileEncoder(EXTENT, 8, false, true)
    tileFeatures.foreach(feature => {
      val layerInfo = validLayerInfos(feature.layerId)
      encoder.addFeature(layerInfo.layerName, layerInfo.attrFields.zip(feature.attributes).toMap.asJava, feature.geom)
    })
    encoder.encode()
  }
}

object TileGenerator {
  val EXTENT = 4096
  val MAX_FEATURE_NUM = 50000
}
