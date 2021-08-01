package com.yankee.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/1 21:27
 */
object Spark01_RDD_Memory_Parallelism {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 创建RDD
    // RDD的并行度 & 分区
    // makeRDD方法可以传递第二个参数，这个参数表示分区的数量
    // 第二个参数可以不传递，那么makeRDD方法会只用默认值，defaultParallelism（默认的并行度）
    // scheduler.conf.getInt("spark.default.parallelism", totalCores)
    // spark在默认情况下，从配置对象中获取配置参数：spark.default.parallelism
    // 如果获取不到，那么使用totalCores属性，这个属性取值为当前环境的最大可用核数
    val data: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // 将处理的数据保存成分区文件
    data.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()
  }
}
