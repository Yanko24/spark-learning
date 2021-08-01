package com.yankee.spark.hive

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author Yankee
 * @date 2021/4/1 13:24
 */
object Spark01_SparkSQL_Hive {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建SparkSession环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    // TODO 业务逻辑
    // 使用SparkSQL连接外置Hive
    // 1.拷贝hive-site.xml文件到resource目录下
    // 2.启用Hive的支持
    // 3.增加对应的依赖关系（包含MySQL的驱动）
    spark.sql("show databases").show()
    spark.sql("select * from ids").show()

    // TODO 关闭环境
    spark.close()
  }
}
