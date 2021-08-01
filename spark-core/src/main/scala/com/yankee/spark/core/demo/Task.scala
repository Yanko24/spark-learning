package com.yankee.spark.core.demo

/**
 * @author Yankee
 * @date 2021/2/27 22:05
 */
class Task extends Serializable {
  val datas = List(1, 2, 3, 4)

  // val logic: (Int) => Int = _ * 2
  val logic = (num: Int) => {num * 2}
}
