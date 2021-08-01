package com.yankee.spark.core.demo.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/20 19:17
 */
object Spark03_Requirement_HostCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    // Q：actionRDD重用次数太多
    // Q：cogroup性能可能较低
    // Q：存在大量的shuffle操作（reduceByKey）
    // reduceByKey：聚合算子，spark会提供优化，会有缓存

    // 1.读取原始的日志数据
    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")

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
    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, ascending = false).take(10)

    // 5.将结果采集到控制台打印
    resultRDD.foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
