package com.yankee.spark.core.demo.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Yankee
 * @date 2021/3/20 19:17
 */
object Spark04_Requirement_HostCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    // Q：actionRDD重用次数太多
    // Q：cogroup性能可能较低
    // Q：存在大量的shuffle操作（reduceByKey）
    // reduceByKey：聚合算子，spark会提供优化，会有缓存

    // 1.读取原始的日志数据
    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")

    val acc: HotCategoryAccumulator = new HotCategoryAccumulator
    sc.register(acc, "hotCategory")

    // 2.转换数据结构
    // 点击的场合：（品类ID，（1，0，0））
    // 下单的场合：（品类ID，（0，1，1））
    // 支付的场合：（品类ID，（0，0，1））
    actionRDD.foreach(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          // 点击的场合
          acc.add((datas(6), "click"))
        } else if (datas(8) != "null") {
          // 下单的场合
          val ids: Array[String] = datas(8).split(",")
          ids.foreach(
            id => {
              acc.add((id, "order"))
            }
          )
        } else if (datas(10) != "null") {
          // 支付的场合
          val ids: Array[String] = datas(10).split(",")
          ids.foreach(
            id => {
              acc.add((id, "pay"))
            }
          )
        }
      }
    )

    val accumulatorValue: mutable.Map[String, HotCategory] = acc.value
    val categories: Iterable[HotCategory] = accumulatorValue.values

    val sort: List[HotCategory] = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        } else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) {
            true
          } else if (left.orderCnt == right.orderCnt) {
            left.payCnt > right.payCnt
          } else {
            false
          }
        } else {
          false
        }
      }
    )

    // 5.将结果采集到控制台打印
    sort.take(10).foreach(println)

    // TODO 关闭环境
    sc.stop()
  }

  case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

  /**
   * 自定义累加器
   * 1.继承AccumulatorV2，定义泛型
   * IN：（品类ID，行为类型）
   * OUT：mutable.Map[String, HotCategory]
   * 2.重写方法（6）
   */
  class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {
    private val hotCategoryMap = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = {
      hotCategoryMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator()
    }

    override def reset(): Unit = {
      hotCategoryMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cid: String = v._1
      val actionType: String = v._2
      val category: HotCategory = hotCategoryMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if (actionType == "click") {
        category.clickCnt += 1
      } else if (actionType == "order") {
        category.orderCnt += 1
      } else if (actionType == "pay") {
        category.payCnt += 1
      }
      hotCategoryMap.update(cid, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1: mutable.Map[String, HotCategory] = this.hotCategoryMap
      val map2: mutable.Map[String, HotCategory] = other.value
      map2.foreach {
        case (cid, hc) => {
          val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          map1.update(cid, category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = hotCategoryMap
  }

}
