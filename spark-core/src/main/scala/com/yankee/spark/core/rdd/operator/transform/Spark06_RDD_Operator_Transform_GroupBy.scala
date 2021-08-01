package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/6 13:38
 */
object Spark06_RDD_Operator_Transform_GroupBy {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——groupBy
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // groupBy会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
    // 相同的key值的数据会放置在一个组中
    def groupFunction(num: Int): Int = {
      num % 2
    }

    val groupByRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)
    groupByRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
