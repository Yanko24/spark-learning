package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/6 14:45
 */
object Spark14_RDD_Operator_Transform_PartitionBy {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——partitionBy
    val rdd: RDD[(Int, Int)] = sc.makeRDD(List(1, 2, 3, 4), 2).map((_, 1))
    // 隐式转换（二次编译）
    // partitionBy：根据指定的分区规则对数进行重新分区
    // 如果重分区的分区器和当前RDD的分区器一样，此时Spark不会进行任何操作
    val partitionerRDD: RDD[(Int, Int)] = rdd.partitionBy(new HashPartitioner(2))
    val value: RDD[(Int, Int)] = partitionerRDD.partitionBy(new HashPartitioner(2))
    value.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()
  }
}
