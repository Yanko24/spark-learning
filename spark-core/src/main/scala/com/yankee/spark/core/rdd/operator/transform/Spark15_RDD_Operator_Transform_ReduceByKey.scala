package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/6 14:45
 */
object Spark15_RDD_Operator_Transform_ReduceByKey {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——reduceByKey
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))
    // reduceByKey：相同的Key的数据进行Value数据的聚合操作
    // Scala语言中一般的聚合操作都是两两聚合，Spark基于Scala开发的，所以它的聚合也是两两聚合
    // reduceByKey中如果Key的数据只有一个，是不会参与运算的。
    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
      println(s"x = ${x}, y = ${y}")
      x + y
    })
    reduceRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
