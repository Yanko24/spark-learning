package com.yankee.spark.core.framework.application

import com.yankee.spark.core.framework.common.TApplication
import com.yankee.spark.core.framework.controller.WordCountController

/**
 * @author Yankee
 * @date 2021/3/25 22:11
 */
object WordCountApplication extends App with TApplication {
  // 启动应用程序
  start() {
    // TODO 业务逻辑
    val wordCountController: WordCountController = new WordCountController
    wordCountController.execute()
  }
}
