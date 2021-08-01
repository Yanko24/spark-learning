package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/6 14:45
 */
object Spark08_RDD_Operator_Transform_Sample {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——sample
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    // 1.第一个参数表示，抽取数据后是否将数据放回：true（放回），false（丢弃）
    // 2.第二个参数表示，如果抽取不放回的场合：数据源中每条数据可能被抽取的概率，基准值的概念
    //                如果抽取放回的场合：数据源中的每条数据被抽取的可能次数
    // 3.第三个参数表示，抽取数据时随机算法的种子
    //                如果不传递第三个参数，那么使用的是当前系统时间

    /*
    抽取数据不放回（伯努利算法）
      伯努利算法：又叫0、1分布。
      具体实现：根据种子和随机算法算出一个数和第二个参数设置几率比较，小于第二个参数要，大于不要
      第一个参数：抽取的数据是否放回，fasle：不放回
      第二个参数：抽取的几率，返回在[0,1]之间，0：全不取；1：全取
      第三个参数：随机数种子

    抽取数据放回（泊松算法）
      第一个参数：抽取的数据是否放回，true：放回
      第二个参数：重复抽取的几率，范围大于等于0，表示每一个元素被期望抽取到的次数
      第三个参数：随机数种子
     */

    //println(rdd.sample(
    //  false,
    //  0.4
    //  //1
    //).collect().mkString(","))

    println(rdd.sample(
      true,
      2
      //1
    ).collect().mkString(","))

    // TODO 关闭环境
    sc.stop()
  }
}
