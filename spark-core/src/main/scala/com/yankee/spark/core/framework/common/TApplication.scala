package com.yankee.spark.core.framework.common

import com.yankee.spark.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/25 22:25
 */
trait TApplication {
  def start(master: String = "local[*]", app: String = this.getClass.getSimpleName.filter(!_.equals('$')))(op: => Unit): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster(master).setAppName(app)
    val sc: SparkContext = new SparkContext(conf)
    EnvUtil.put(sc)

    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }

    // TODO 关闭环境
    sc.stop()
    EnvUtil.clear()
  }
}
