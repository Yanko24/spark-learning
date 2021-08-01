package com.yankee.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

/**
 * @author Yankee
 * @date 2021/3/28 22:54
 */
object Spark04_SparkSQL_UDAF2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // TODO 执行逻辑操作
    val df: DataFrame = spark.read.json("data/user.json")
    df.createOrReplaceTempView("user")

    // 早期版本中，不能在sql中使用强类型的UDAF操作
    // 早期的UDAF强类型聚合函数使用DSL语法操作
    import spark.implicits._
    val ds: Dataset[User] = df.as[User]

    // 将查询的UDAF函数转换为查询的列对象
    val udafCol: TypedColumn[User, Long] = new MyAvgUDAF().toColumn

    ds.select(udafCol).show()

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
  case class User(username: String, age: Long, height: Double)

  case class Buff(var total: Long, var count: Long)

  class MyAvgUDAF extends Aggregator[User, Buff, Long] {
    // 初始值或零值
    // 缓冲区的初始化
    override def zero: Buff = {
      Buff(0, 0L)
    }

    // 根据输入的数据来更新缓冲区的数据
    override def reduce(buff: Buff, in: User): Buff = {
      buff.total = buff.total + in.age
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
