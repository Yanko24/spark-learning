package com.yankee.spark.mysql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author Yankee
 * @date 2021/4/1 10:55
 */
object Spark01_SpakSQL_JDBC {
  def main(args: Array[String]): Unit = {
    // TODO SparkSQL的运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // TODO 业务逻辑
    // 读取MySQL的数据
    val df: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://master:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "yangqi")
      .option("password", "xiaoer")
      .option("dbtable", "user")
      .load()

    // 保存数据
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://master:3306/spark-sql?userUnicode=true&characterEncoding=utf8")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "yangqi")
      .option("password", "xiaoer")
      .option("dbtable", "people")
      .mode(SaveMode.Append)
      .save()

    // TODO 关闭环境
    spark.close()
  }
}
