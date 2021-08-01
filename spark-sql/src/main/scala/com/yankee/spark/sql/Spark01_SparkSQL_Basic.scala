package com.yankee.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author Yankee
 * @date 2021/3/28 22:54
 */
object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    // TODO 执行逻辑操作
    // TODO DataFrame
    val df: DataFrame = spark.read.json("data/user.json")
    // DataFrame => SQL
    df.createOrReplaceTempView("user")
    spark.sql("select * from user").show()
    spark.sql("select avg(age) from user").show()
    // DataFrame => DSL
    // 再使用DataFrame时，如果涉及到操作转换操作，需要引入转换规则
    df.select("age", "username").show()
    df.select($"age" + 1).show()
    df.select('age + 1).show()

    // TODO DataSet
    // DataFrame其实是特定泛型的DataSet
    val seq: Seq[Int] = Seq(1, 2, 3, 4)
    val ds: Dataset[Int] = seq.toDS()
    ds.show()

    // TODO RDD <=> DataFrame
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    val df1: DataFrame = rdd.toDF("id", "name", "age")
    val rowRDD: RDD[Row] = df1.rdd

    // TODO DataFrame <=> DataSet
    val ds1: Dataset[User] = df1.as[User]
    val df2: DataFrame = ds1.toDF()
    df2.show()

    // TODO RDD <=> DataSet
    val ds2: Dataset[User] = rdd.map {
      case (id, name, age) => User(id, name, age)
    }.toDS()
    val rdd2: RDD[User] = ds2.rdd
    rdd2.collect().foreach(println)

    // TODO 关闭环境
    spark.close()
  }

  case class User(id: Int, name: String, age: Int)
}
