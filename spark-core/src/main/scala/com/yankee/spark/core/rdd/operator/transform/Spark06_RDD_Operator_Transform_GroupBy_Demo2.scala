package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author Yankee
 * @date 2021/3/6 13:38
 */
object Spark06_RDD_Operator_Transform_GroupBy_Demo2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——groupBy
    // 分组和分区没有必然的关系
    val rdd: RDD[String] = sc.textFile("data/apache.log")
    val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(line => {
      val datas: Array[String] = line.split(" ")
      val time: String = datas(3)
      val format: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val date: Date = format.parse(time)
      val format2: SimpleDateFormat = new SimpleDateFormat("HH")
      val hour: String = format2.format(date)
      (hour, 1)
    }).groupBy(_._1)
    timeRDD.map {
      case (hour, iter) => {
        (hour, iter.size)
      }
    }.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
