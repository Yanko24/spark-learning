package com.yankee.spark.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author Yankee
 * @date 2021/4/1 21:27
 */
object Spark02_SparkSQL_Demo_Operator {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    // TODO 业务操作
    spark.sql("use sparksql")

    spark.sql(
      """
        |SELECT
        |	T3.*
        |FROM (
        |	SELECT
        |		T2.*,
        |		RANK() OVER(PARTITION BY T2.area ORDER BY T2.clickCnt DESC) rank
        |	FROM (
        |		SELECT
        |			T1.area,
        |			T1.product_name,
        |			COUNT(1) clickCnt
        |		FROM (
        |			SELECT
        |				A.*,
        |				P.product_name,
        |				C.city_name,
        |				C.area
        |			FROM user_visit_action A
        |			JOIN product_info P ON A.click_product_id = P.product_id
        |			JOIN city_info C ON A.city_id = C.city_id
        |			WHERE A.click_product_id <> -1
        |		) T1
        |		GROUP BY T1.area, T1.product_name
        |	) T2
        |) T3
        |WHERE T3.rank <= 3
        |""".stripMargin).show()

    // TODO 关闭环境
    spark.close()
  }
}
