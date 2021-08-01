package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/6 14:45
 */
object Spark16_RDD_Operator_Transform_GroupByKey {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——groupByKey
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))
    // groupByKey：将数据源中的数据，相同Key的数据分在一个组中，形成一个对偶元组
    //             元组中的第一个元素都是Key
    //             元组中的第二个元素就是相同Key的Value的集合
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    groupRDD.collect().foreach(println)

    // groupBy
    val groupRDD2: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    groupRDD2.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
