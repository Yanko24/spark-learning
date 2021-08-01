package com.yankee.spark.core.demo.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/20 19:17
 */
object Spark02_Requirement_HostCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    // Q：actionRDD重用次数太多
    // Q：cogroup性能可能较低

    // 1.读取原始的日志数据
    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
    actionRDD.cache()
    // 2.统计品类的点击数量：（品类ID，（点击数量，0，0））
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
    // 3.统计品类的下单数量：（品类ID，（0，下单数量，0））
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
    // 4.统计品类的支付数量：（品类ID，（0，0，支付数量））
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
    // cogroup可能在使用过程中存在shuffle
    val clickRDD: RDD[(String, (Int, Int, Int))] = clickCountRDD.map {
      case (cid, count) => {
        (cid, (count, 0, 0))
      }
    }
    val orderRDD: RDD[(String, (Int, Int, Int))] = orderCountRDD.map {
      case (cid, count) => {
        (cid, (0, count, 0))
      }
    }
    val payRDD: RDD[(String, (Int, Int, Int))] = payCountRDD.map {
      case (cid, count) => {
        (cid, (0, 0, count))
      }
    }
    // 将三个数据源合并在一起，统一进行聚合计算
    val sourceRDD: RDD[(String, (Int, Int, Int))] = clickRDD.union(orderRDD).union(payRDD)
    val analysisRDD: RDD[(String, (Int, Int, Int))] = sourceRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, ascending = false).take(10)

    // 6.将结果采集到控制台打印
    resultRDD.foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
