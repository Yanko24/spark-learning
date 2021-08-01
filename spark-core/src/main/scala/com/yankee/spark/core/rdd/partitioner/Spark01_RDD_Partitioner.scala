package com.yankee.spark.core.rdd.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/18 20:57
 */
object Spark01_RDD_Partitioner {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("NBA", "xxxxx"),
      ("CBA", "xxxxx"),
      ("WNBA", "xxxxx"),
      ("CBA", "xxxxx"),
      ("DBA", "xxxxx")
    ), 3)
    val partitionerRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)
    partitionerRDD.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()
  }

  /**
   * 自定义分区器
   */
  class MyPartitioner extends Partitioner {
    // 分区数量
    override def numPartitions: Int = 3

    // 根据数据的Key值返回数据的分区索引（从0开始）
    override def getPartition(key: Any): Int = {
      //if (key == "NBA") {
      //  0
      //} else if (key == "CBA") {
      //  1
      //} else {
      //  2
      //}
      key match {
        case "NBA" => 0
        case "CBA" => 1
        case _ => 2
      }
    }
  }
}
