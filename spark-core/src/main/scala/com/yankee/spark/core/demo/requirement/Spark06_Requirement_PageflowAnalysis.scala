package com.yankee.spark.core.demo.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/20 19:17
 */
object Spark06_Requirement_PageflowAnalysis {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    // 1.读取原始的日志数据
    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
    val actionDataRDD: RDD[UserVisitAction] = actionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        UserVisitAction(datas(0), datas(1).toLong, datas(2), datas(3).toLong, datas(4), datas(5), datas(6).toLong, datas(7).toLong, datas(8),
          datas(9), datas(10), datas(11), datas(12).toLong)
      }
    )
    actionDataRDD.cache()

    // TODO 计算分母
    val pageidToCountMap: Map[Long, Long] = actionDataRDD.map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap

    // TODO 计算分子
    // 根据session分组，分组后需要根据访问时间排序
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionDataRDD.groupBy(_.session_id)
    val mvRDD: RDD[(String, List[((Long, Long), Int)])] =
      sessionRDD.mapValues(
      iter => {
        val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)
        val flowIds: List[Long] = sortList.map(_.page_id)
        val pageflowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)
        pageflowIds.map(
          ids => {
            (ids, 1)
          }
        )
      }
    )
    val flatRDD: RDD[((Long, Long), Int)] = mvRDD.map(_._2).flatMap(list => list)
    val dataRDD: RDD[((Long, Long), Int)] = flatRDD.reduceByKey(_ + _)

    // TODO 计算单跳转换率
    dataRDD.foreach {
      case ((pageid1, pageid2), sum) => {
        val lon: Long = pageidToCountMap.getOrElse(pageid1, 0L)
        printf(s"页面${pageid1}跳转到页面${pageid2}的单跳转换率为：%.2f%%\n", (sum.toDouble / lon) * 100)
      }
    }

    // TODO 关闭环境
    sc.stop()
  }
}
