package com.wangrubin.study.vector.app

import com.wangrubin.study.vector.BalanceDistSlicer
import com.wangrubin.study.vector.consume.{FilePersister, LayerInfo, ZoomBound}
import com.wangrubin.study.vector.crs.DefaultVectorCRS
import com.wangrubin.study.vector.feature.WrapFeature
import com.wangrubin.study.vector.util.WKTUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

object VectorTileApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("VectorTileApp")
      .setMaster("local[*]")
    val spark = new SparkContext(sparkConf)

    val start = System.currentTimeMillis()

    val roadPath = "file:///E:/论文实验/xian_rn_2016.txt"
    val trajPath = "file:///E:/trajectory_xian"

    val persister = new FilePersister("E:/tile")
    val distributeSlicer = new BalanceDistSlicer(DefaultVectorCRS, persister)

    val roadRdd = spark.textFile(roadPath, 10).map(buildRoad)
    val roadLayerName = "road"
    val roadLayerInfo = LayerInfo(roadLayerName, ZoomBound(0, 14), Array("segment_length"))

    val trajRdd = spark.textFile(trajPath, 10).map(buildTraj)
    val trajLayerName = "traj"
    val trajLayerInfo = LayerInfo(trajLayerName, ZoomBound(10, 14), Array("traj_length"))

    distributeSlicer.slice((roadLayerInfo, roadRdd), (trajLayerInfo, trajRdd))

    val total = (System.currentTimeMillis() - start) / 1E3
    println(total)
    // spark.parallelize(Seq(s"time:$total s")).repartition(1).saveAsTextFile(storePath)
  }

  val factory = new GeometryFactory()

  def buildRoad(line: String): WrapFeature = {
    val fields = line.split("\t")
    val coordinates = fields(11).split(",").map(coordStr => {
      val attrs = coordStr.split(" ").map(_.toDouble)
      new Coordinate(attrs(1), attrs(0))
    })
    val geom = factory.createLineString(coordinates)
    WrapFeature(geom, Array(geom.getNumPoints))
  }

  def buildTraj(line: String): WrapFeature = {
    val geom = WKTUtils.read(line)
    WrapFeature(geom, Array(geom.getNumPoints))
  }
}
