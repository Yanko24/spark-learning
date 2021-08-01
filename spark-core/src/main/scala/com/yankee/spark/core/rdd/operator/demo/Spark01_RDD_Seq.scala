package com.yankee.spark.core.rdd.operator.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/7 11:26
 */
object Spark01_RDD_Seq {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    // 时间戳 省份 城市 用户 广告
    /*
    1.获取原始数据：时间戳 省份 城市 用户 广告
    2.将原始数据进行结构的转换，方便统计
    3.时间戳 省份 城市 用户 广告 --> ((省份,广告),1)
    4.将转换后的数据进行分组聚合 ((省份,广告),sum)
    5.将聚合的结果进行结构的转换 ((省份,广告),sum) --> (省份,(广告,sum))
    6.将转换结构后的数据按照省份进行分组 (省份,[(广告,sum),(广告,sum)])
    7.将分组后的数据按照sum降序排序，并且取top3
     */
    val dataRDD: RDD[String] = sc.textFile("data/agent.log")
    val mapRDD: RDD[((String, String), Int)] = dataRDD.map(
      line => {
        val datas: Array[String] = line.split(" ")
        ((datas(1), datas(4)), 1)
      }
    )
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)
    val newMapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((pre, ad), sum) => {
        (pre, (ad, sum))
      }
    }
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )
    resultRDD.collect().foreach(println)


    // TODO 关闭环境
    sc.stop()
  }
}
