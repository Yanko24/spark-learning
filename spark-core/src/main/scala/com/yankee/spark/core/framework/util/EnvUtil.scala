package com.yankee.spark.core.framework.util

import org.apache.spark.SparkContext

/**
 * @author Yankee
 * @date 2021/3/25 22:36
 */
object EnvUtil {
  private val scLocal: ThreadLocal[SparkContext] = new ThreadLocal[SparkContext]()

  /**
   * 将sc连接放入ThreadLocal内存中
   * @param sc
   */
  def put(sc: SparkContext): Unit = {
    scLocal.set(sc)
  }

  def take(): SparkContext = {
    scLocal.get()
  }

  def clear(): Unit = {
    scLocal.remove()
  }
}
