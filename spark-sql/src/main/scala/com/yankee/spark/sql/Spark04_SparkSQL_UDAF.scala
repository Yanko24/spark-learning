package com.yankee.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

/**
 * @author Yankee
 * @date 2021/3/28 22:54
 */
object Spark04_SparkSQL_UDAF {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // TODO 执行逻辑操作
    val df: DataFrame = spark.read.json("data/user.json")
    df.createOrReplaceTempView("user")

    // 2.4.6版本中未涉及
    spark.udf.register("avgAge", functions.udaf(new MyAvgUDAF()))

    spark.sql("select avgAge(age) from user").show()

    // TODO 关闭环境
    spark.close()
  }

  /**
   * 自定义聚合函数：计算年龄的平均值
   * 1.继承org.apache.spark.sql.expressions.Aggregator，定义泛型
   * IN：输入的数据类型
   * BUF：Buff缓冲区数据类型
   * OUT：输出的数据类型
   * 2.重写方法（6个）
   */
  case class Buff(var total: Long, var count: Long)

  class MyAvgUDAF extends Aggregator[Long, Buff, Long] {
    // 初始值或零值
    // 缓冲区的初始化
    override def zero: Buff = {
      Buff(0, 0L)
    }

    // 根据输入的数据来更新缓冲区的数据
    override def reduce(buff: Buff, in: Long): Buff = {
      buff.total = buff.total + in
      buff.count += 1
      buff
    }

    // 合并缓冲区
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total = buff1.total + buff2.total
      buff1.count += buff2.count
      buff1
    }

    // 计算结果
    override def finish(reduction: Buff): Long = {
      reduction.total / reduction.count
    }

    // 缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    // 输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
