package com.yankee.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/7 11:48
 */
object Spark12_RDD_Operator_Action_Demo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD行动算子
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val user: User = new User
    // SparkException: Task not serializable
    // Caused by: java.io.NotSerializableException: com.yankee.spark.core.rdd.operator.action.Spark12_RDD_Operator_Action_Demo$User
    // RDD算子中传递的函数是会进行闭包操作，那么就会进行检测功能
    // 闭包检测
    rdd.foreach(
      num => {
        println("age = " + (user.age + num))
      }
    )

    // TODO 关闭环境
    sc.stop()
  }

   class User extends Serializable {
  // 样例类在编译时，会自动混入序列化特质（实现可序列化接口）
  //case class User() {
    var age: Int = 30
  }
}
