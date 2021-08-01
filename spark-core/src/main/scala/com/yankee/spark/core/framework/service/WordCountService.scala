package com.yankee.spark.core.framework.service

import com.yankee.spark.core.framework.common.TService
import com.yankee.spark.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * @author Yankee
 * @date 2021/3/25 22:12
 * @descript 服务层
 */
class WordCountService extends TService {
  private val wordCountDao: WordCountDao = new WordCountDao

  /**
   * 数据分析
   *
   * @return Array[(String, Int)]
   */
  def dataAnalysis(): Array[(String, Int)] = {
    val lines: RDD[String] = wordCountDao.readFile("data/words.txt")
    // WordCount
    val array: Array[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect()
    array
  }
}
