package com.yankee.spark.core.demo.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/20 19:17
 */
object Spark05_Requirement_HostCategoryTop10SessionAnalysis {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    // 1.读取原始的日志数据
    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
    actionRDD.cache()

    val top10Ids: Array[String] = top10Category(actionRDD)

    // 1.过滤原始数据，保留点击和前10品类ID
    val filterActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          top10Ids.contains(datas(6))
        } else {
          false
        }
      }
    )

    // 2.根据品类ID和SessionID进行点击量的统计
    val reduceRDD: RDD[((String, String), Int)] = filterActionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)

    // 3.将统计结果进行结构的转换
    val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((cid, sid), sum) => {
        (cid, (sid, sum))
      }
    }

    // 4.相同的品类进行分区
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

    // 5.将分组后的数据进行点击量的排序
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )

    resultRDD.foreach(println)

    // TODO 关闭环境
    sc.stop()
  }

  // 计算top10
  def top10Category(actionRDD: RDD[String]): Array[String] = {
    // 2.转换数据结构
    // 点击的场合：（品类ID，（1，0，0））
    // 下单的场合：（品类ID，（0，1，1））
    // 支付的场合：（品类ID，（0，0，1））
    val flatMapRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          // 点击的场合
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          // 下单的场合
          val ids: Array[String] = datas(8).split(",")
          ids.map((_, (0, 1, 0)))
        } else if (datas(10) != "null") {
          // 支付的场合
          val ids: Array[String] = datas(10).split(",")
          ids.map((_, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    // 3.将相同品类ID的数据进行分组聚合
    // (品类ID，（点击数量，下单数量，支付数量）)
    val analysisRDD: RDD[(String, (Int, Int, Int))] = flatMapRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    // 4.将统计结果按照数量进行降序排列，取前10名
    analysisRDD.sortBy(_._2, ascending = false).take(10).map(_._1)
  }
}
