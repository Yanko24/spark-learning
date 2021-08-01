package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/6 14:45
 */
object Spark12_RDD_Operator_Transform_SortBy_Demo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——sortBy
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)
    // sortBy：可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序的方式
    // 默认情况下，不会改变分区。但是中间存在shuffle操作
    //val orderByRDD: RDD[(String, Int)] = rdd.sortBy(t => t._1)
    val orderByRDD: RDD[(String, Int)] = rdd.sortBy(t => t._1.toInt)
    orderByRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
