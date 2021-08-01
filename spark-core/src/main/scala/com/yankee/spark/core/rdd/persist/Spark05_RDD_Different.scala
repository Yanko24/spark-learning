package com.yankee.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/17 21:03
 */
object Spark05_RDD_Different {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    /*
    cache：将数据临时存储在内存中进行数据重用
      会在血缘关系中添加新的依赖。一旦出现问题可以从头开始计算
    persist：将数据临时存储在磁盘文件中进行数据重用
      涉及到磁盘IO，性能较低，但是数据安全
      如果作业执行完毕，临时保存的数据文件就会丢失
    checkpoint：将数据长久的保存在磁盘文件中进行数据重用
      涉及到磁盘IO，性能较低，但是数据安全
      为了保证数据安全，所以一般情况下，会独立执行作业
      为了能够提高效率，一般情况下需要和cache联合使用
      执行过程中，会切断血缘关系。重新建立新的血缘关系
     */

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint")

    // TODO 业务逻辑
    val list: List[String] = List("hello scala", "hello spark")
    val rdd: RDD[String] = sc.makeRDD(list, 2)
    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatMapRDD.map(word => {
      (word, 1)
    })
    // 增加缓存，避免再重新跑一个Job做checkpoint
    //mapRDD.cache()
    // 数据检查点：针对mapRDD做检查点计算
    mapRDD.checkpoint()

    println(mapRDD.toDebugString)
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    reduceRDD.collect().foreach(println)
    println("******************************")
    println(mapRDD.toDebugString)
    groupRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
