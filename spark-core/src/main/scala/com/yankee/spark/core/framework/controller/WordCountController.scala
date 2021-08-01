package com.yankee.spark.core.framework.controller

import com.yankee.spark.core.framework.common.TController
import com.yankee.spark.core.framework.service.WordCountService

/**
 * @author Yankee
 * @date 2021/3/25 22:11
 * @descript 控制层
 */
class WordCountController extends TController {
  private val wordCountService: WordCountService = new WordCountService

  def execute(): Unit = {
    val array: Array[(String, Int)] = wordCountService.dataAnalysis()
    array.foreach(println)
  }
}
