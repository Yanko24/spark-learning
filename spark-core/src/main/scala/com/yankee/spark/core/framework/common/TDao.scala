package com.yankee.spark.core.framework.common

import com.yankee.spark.core.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

/**
 * @author Yankee
 * @date 2021/3/25 22:32
 */
trait TDao {
  /**
   * 读取数据文件
   *
   * @param path
   * @return RDD[String]
   */
  def readFile(path: String): RDD[String] = {
    EnvUtil.take().textFile(path)
  }
}
