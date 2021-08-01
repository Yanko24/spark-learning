package com.yankee.spark.core.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Yankee
 * @date 2021/3/18 21:34
 */
object Spark04_ACC_WordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "hello", "zookeeper"), 2)
    // 累加器：WordCount
    // 创建累加器对象，向Spark进行注册
    val wcAcc: MyAccumulator = new MyAccumulator
    sc.register(wcAcc, "WordCountAcc")

    rdd.foreach(
      word => {
        // 数据的累加（使用累加器）
        wcAcc.add(word)
      }
    )

    // 获取累加器累加的结果
    println(wcAcc.value)

    // TODO 关闭环境
    sc.stop()
  }

  /**
   * 自定义数据累加器：WordCount
   *  1.自定义AccumulatorV2，定义泛型
   *    IN：累加器输入的数据类型
   *    OUT：累加器返回的数据类型
   *  2.重写方法
   */
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
    private var wcMap = mutable.Map[String, Long]()

    // 判断是否初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    // 复制新的累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    // 重置累加器
    override def reset(): Unit = {
      wcMap.clear()
    }

    // 获取累加器需要计算的值
    override def add(word: String): Unit = {
      val newCnt: Long = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, newCnt)
    }

    // 合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1: mutable.Map[String, Long] = this.wcMap
      val map2: mutable.Map[String, Long] = other.value
      map2.foreach {
        case (word, count) => {
          val newCount: Long = map1.getOrElse(word, 0L) + count
          map1.update(word, newCount)
        }
      }
    }

    // 累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }
}
