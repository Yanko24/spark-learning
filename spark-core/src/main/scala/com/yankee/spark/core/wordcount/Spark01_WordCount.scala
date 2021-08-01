package com.yankee.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/2/23 22:42
 */
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    // TODO 建立和Spark框架的连接
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 执行业务操作
    // 1.读取文件，获取一行一行的数据
    val lines: RDD[String] = sc.textFile("datas")
    // 2.将一行数据拆分成一个一个的单词（分词）
    val words: RDD[String] = lines.flatMap(_.split(" "))
    // 3.将数据根据单词进行分组，便于统计
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    // 4.对分组后的数据进行转换
    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    // 5.将转换的结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)
    // TODO 关闭连接
    sc.stop()
  }
}
