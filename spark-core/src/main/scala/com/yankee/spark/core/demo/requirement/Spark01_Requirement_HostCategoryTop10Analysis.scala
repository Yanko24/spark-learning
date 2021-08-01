package com.yankee.spark.core.demo.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/20 19:17
 */
object Spark01_Requirement_HostCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    // 1.读取原始的日志数据
    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
    // 2.统计品类的点击数量：（品类ID，点击数量）
    val clickActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(6) != "-1"
      }
    )
    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)
    // 3.统计品类的下单数量：（品类ID，下单数量）
    val orderActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(8) != "null"
      }
    )
    val orderCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        val cid: String = datas(8)
        val cids: Array[String] = cid.split(",")
        cids.map((_, 1))
      }
    ).reduceByKey(_ + _)
    // 4.统计品类的支付数量：（品类ID，支付数量）
    val payActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(10) != "null"
      }
    )
    val payCountRDD: RDD[(String, Int)] = payActionRDD.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        val cid: String = datas(10)
        val cids: Array[String] = cid.split(",")
        cids.map((_, 1))
      }
    ).reduceByKey(_ + _)
    // 5.将品类进行排序，并且取前10名
    //  点击数量排序，下单数量排序，支付数量排序
    //  元组排序：先比较第一个，再比较第二个，再比较第三个，以此类推
    //  (品类ID, (点击数量, 下单数量, 支付数量))
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    val analysisRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clickCnt: Int = 0
        val click: Iterator[Int] = clickIter.iterator
        if (click.hasNext) {
          clickCnt = click.next()
        }

        var orderCnt: Int = 0
        val order: Iterator[Int] = orderIter.iterator
        if (order.hasNext) {
          orderCnt = order.next()
        }

        var payCnt: Int = 0
        val pay: Iterator[Int] = payIter.iterator
        if (pay.hasNext) {
          payCnt = pay.next()
        }

        (clickCnt, orderCnt, payCnt)
      }
    }
    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, ascending = false).take(10)

    // 6.将结果采集到控制台打印
    resultRDD.foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
