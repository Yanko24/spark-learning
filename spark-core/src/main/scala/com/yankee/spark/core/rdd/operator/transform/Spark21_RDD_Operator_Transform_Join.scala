package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/6 14:45
 */
object Spark21_RDD_Operator_Transform_Join {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——join
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)), 2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)), 2)
    val rdd3: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("c", 5), ("d", 6)), 2)
    val rdd4: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("c", 5), ("a", 6)), 2)

    // join：两个不同数据源的数据，相同的Key的Value会连接在一起，形成元组
    //       两个数据源的数据Key没有匹配上，那么数据不会出现在结果中
    //       两个数据源的数据Key存在多个相同时，会依次匹配，可能会出现笛卡尔乘积，数据量会几何性增长，会导致性能降低。
    println(rdd.join(rdd2).collect().mkString(","))
    println(rdd.join(rdd3).collect().mkString(","))
    println(rdd.join(rdd4).collect().mkString(","))


    // TODO 关闭环境
    sc.stop()
  }
}
