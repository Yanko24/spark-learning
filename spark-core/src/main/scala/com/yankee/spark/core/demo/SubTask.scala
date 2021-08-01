package com.yankee.spark.core.demo

/**
 * @author Yankee
 * @date 2021/2/27 22:17
 */
class SubTask extends Serializable {
  var datas: List[Int] = _
  var logic: (Int) => Int = _

  // 计算
  def compute(): List[Int] = {
    datas.map(logic)
  }
}
