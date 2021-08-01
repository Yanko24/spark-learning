package com.yankee.spark.demo

import org.apache.spark.{SPARK_REPO_URL, SparkConf}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author Yankee
 * @date 2021/4/1 21:27
 */
object Spark03_SparkSQL_Demo_Operator {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    // TODO 业务操作
    spark.sql("use sparksql")

    // 查询基本数据
    spark.sql(
      """
        |SELECT
        |	A.*,
        |	P.product_name,
        |	C.city_name,
        |	C.area
        |FROM user_visit_action A
        |JOIN product_info P ON A.click_product_id = P.product_id
        |JOIN city_info C ON A.city_id = C.city_id
        |WHERE A.click_product_id <> -1
        |""".stripMargin).createOrReplaceTempView("T1")

    spark.udf.register("cityRemark", functions.udaf(new CityRemarkUDAF))

    // 根据区域，商品进行数据聚合
    spark.sql(
      """
        |SELECT
        |	T1.area,
        |	T1.product_name,
        |	COUNT(1) clickCnt,
        | cityRemark(city_name) city_remark
        |FROM T1
        |GROUP BY T1.area, T1.product_name
        |""".stripMargin).createOrReplaceTempView("T2")

    // 区域内对点击数量进行排行
    spark.sql(
      """
        |SELECT
        |	T2.*,
        |	RANK() OVER(PARTITION BY T2.area ORDER BY T2.clickCnt DESC) rank
        |FROM T2
        |""".stripMargin).createOrReplaceTempView("T3")

    // 取前三名
    spark.sql(
      """
        |SELECT
        |	T3.area,
        | T3.product_name,
        | T3.clickCnt,
        | T3.city_remark
        |FROM T3
        |WHERE T3.rank <= 3
        |""".stripMargin).show(false)

    // TODO 关闭环境
    spark.close()
  }

  case class Buffer(var total: Long, var cityMap: mutable.Map[String, Long])

  /**
   * 自定义聚合函数，实现城市备注功能
   * 1.继承Aggregator，定义泛型
   * IN：城市的名称String
   * BUF：[总的点击数量, Map[(city, cnt), (city, cnt)]]
   * OUT：备注信息String
   * 2.重写方法
   */
  class CityRemarkUDAF extends Aggregator[String, Buffer, String] {
    // 缓冲区初始化
    override def zero: Buffer = {
      Buffer(0L, mutable.Map[String, Long]())
    }

    // 更新缓冲区
    override def reduce(buff: Buffer, city: String): Buffer = {
      buff.total += 1
      val newCnt = buff.cityMap.getOrElse(city, 0L) + 1
      buff.cityMap.update(city, newCnt)
      buff
    }

    // 合并
    override def merge(buff1: Buffer, buff2: Buffer): Buffer = {
      buff1.total += buff2.total
      // 两个Map的合并
      //buff1.cityMap = buff1.cityMap.foldLeft(buff2.cityMap) {
      //  case (map, (city, cnt)) => {
      //    val newCnt: Long = map.getOrElse(city, 0L) + cnt
      //    map.update(city, newCnt)
      //    map
      //  }
      //}
      buff2.cityMap.foreach {
        case (city, cnt) => {
          val newCnt: Long = buff1.cityMap.getOrElse(city, 0L) + cnt
          buff1.cityMap.update(city, newCnt)
        }
      }
      buff1
    }

    // 计算结果
    override def finish(buff: Buffer): String = {
      val remarkList: ListBuffer[String] = ListBuffer[String]()

      val totalCnt: Long = buff.total
      val cityMap: mutable.Map[String, Long] = buff.cityMap

      // 降序排列
      val cityCntList: List[(String, Long)] = cityMap.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }
      ).take(2)

      val hasMore: Boolean = cityMap.size > 2
      var resultSum: Long = 0L
      cityCntList.foreach {
        case (city, cnt) => {
          val result: Long = cnt * 100 / totalCnt
          remarkList.append(s"${city} ${result}%")
          resultSum += result
        }
      }
      if (hasMore) {
        remarkList.append(s"其他 ${100 - resultSum}%")
      }

      remarkList.mkString("，")
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }

}
