package com.yankee.spark.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author Yankee
 * @date 2021/4/1 20:30
 */
object Spark01_SparkSQL_Demo_Create {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    // TODO 业务逻辑
    spark.sql("use sparksql")

    spark.sql(
      """
        |CREATE TABLE `user_visit_action` (
        | `date` string,
        | `user_id` bigint,
        | `session_id` string,
        | `page_id` bigint,
        | `action_time` string,
        | `search_keyword` string,
        | `click_category_id` bigint,
        | `click_product_id` bigint,
        | `order_category_ids` string,
        | `order_product_ids` string,
        | `pay_category_ids` string,
        | `pay_product_ids` string,
        | `city_id` bigint
        |)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql("load data inpath '/user/hadoop/data/user_visit_action_sql.txt' into table sparksql.user_visit_action")

    spark.sql(
      """
        |CREATE TABLE `product_info` (
        | `product_id` bigint,
        | `product_name` string,
        | `extend_info` string
        |)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql("load data inpath '/user/hadoop/data/product_info.txt' into table sparksql.product_info")

    spark.sql(
      """
        |CREATE TABLE `city_info` (
        | `city_id` bigint,
        | `city_name` string,
        | `area` string
        |)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql("load data inpath '/user/hadoop/data/city_info.txt' into table sparksql.city_info")

    spark.sql("select * from user_visit_action").show

    // TODO 关闭环境
    spark.close()
  }
}
