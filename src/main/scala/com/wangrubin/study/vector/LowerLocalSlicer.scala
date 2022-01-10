package com.wangrubin.study.vector

import com.wangrubin.study.vector.consume.TileGenerator
import com.wangrubin.study.vector.consume.TileGenerator.EXTENT
import com.wangrubin.study.vector.feature.{TileCoord, TileInfo, WrapFeature}
import com.wangrubin.study.vector.index.QuadNode
import org.locationtech.jts.geom.Envelope

import scala.collection.mutable.ArrayBuffer

class LowerLocalSlicer[T](localRoot: QuadNode, consumer: TileGenerator) {
  private val minZoomLevel = consumer.minLevel
  private val maxZoomLevel = consumer.maxLevel
  private val root = new QuadNodeWithData(localRoot.tileInfo)

  def slice(projectFeatures: Array[WrapFeature]): (TileInfo, Array[WrapFeature]) = {
    val pixelFeatures = asPixelAndBuild(projectFeatures)
    val topLevel = Math.max(minZoomLevel, root.tileCoord.zoomLevel)
    for (zoomLevel <- (topLevel to maxZoomLevel).reverse) {
      val deltaZoomLevel = zoomLevel - root.tileCoord.zoomLevel
      val minRowNum = root.tileCoord.rowNum << deltaZoomLevel
      val minColumnNum = root.tileCoord.columnNum << deltaZoomLevel
      root.generateTile(minRowNum, minColumnNum, zoomLevel)
      if (zoomLevel != topLevel) pixelFeatures.foreach(_.scrollUp())
    }
    if (topLevel > minZoomLevel) {
      ((root.tileInfo, pixelFeatures))
    } else null
  }

  private def asPixelAndBuild(projectFeatures: Array[WrapFeature]): Array[WrapFeature] = {
    //build quadtree index
    val bottomLevel = Math.max(root.tileInfo.tileCoord.zoomLevel, consumer.maxLevel)
    val leafNodes = new ArrayBuffer[QuadNode]()
    root.split(bottomLevel, leafNodes)

    //convert features to pixel coordinates and insert into quadtree
    val referX = root.tileInfo.env.getMinX
    val referY = root.tileInfo.env.getMaxY
    val tileWidth = leafNodes.head.tileInfo.env.getWidth
    val tileHeight = leafNodes.head.tileInfo.env.getHeight
    projectFeatures.map(feature => {
      //convert projection coordinates to pixel coordinates
      feature.geom.getCoordinates.foreach(coord => {
        val pixelX = Math.round((coord.getX - referX) / tileWidth * EXTENT)
        val pixelY = Math.round((referY - coord.getY) / tileHeight * EXTENT)
        coord.setX(pixelX)
        coord.setY(pixelY)
        coord
      })
      feature.geom.geometryChanged()
      root.insert(feature.projectEnv, feature)
      feature
    })
  }

  private class QuadNodeWithData(tileInfo: TileInfo) extends QuadNode(tileInfo) {
    val tileCoord: TileCoord = tileInfo.tileCoord
    val wrapFeatures = new ArrayBuffer[WrapFeature]()

    override protected def createNode(tileInfo: TileInfo): QuadNode = {
      new QuadNodeWithData(tileInfo)
    }

    def insert(geomEnv: Envelope, wrappedFeature: WrapFeature): Unit = {
      if (this.tileInfo.env.intersects(geomEnv)) {
        this.wrapFeatures += wrappedFeature
        if (!isLeaf) {
          for (subIndex <- 0 until 4) {
            this.subnodes(subIndex).asInstanceOf[QuadNodeWithData].insert(geomEnv, wrappedFeature)
          }
        }
      }
    }

    def generateTile(minRow: Int, minColumn: Int, zoomLevel: Int): Unit = {
      if (this.tileCoord.zoomLevel == zoomLevel) {
        val currMinPixelX = (this.tileCoord.columnNum - minColumn) * EXTENT
        val currMinPixelY = (this.tileCoord.rowNum - minRow) * EXTENT
        wrapFeatures.foreach(wrapFeature => {
          wrapFeature.relocate(currMinPixelX, currMinPixelY)
        })
        consumer.generateAndReduce(tileInfo, wrapFeatures.toArray)
      } else {
        for (subIndex <- 0 until 4) {
          this.subnodes(subIndex).asInstanceOf[QuadNodeWithData].generateTile(minRow, minColumn, zoomLevel)
        }
      }
    }
  }

}
