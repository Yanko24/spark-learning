package com.yankee.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Yankee
 * @date 2021/3/9 19:40
 */
object Spark04_WordCount_Demo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    wordCount11(sc)

    // TODO 关闭环境
    sc.stop()
  }

  // groupBy
  def wordCount1(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val groupRDD: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordCount: RDD[(String, Int)] = groupRDD.mapValues(iter => iter.size)
    println(wordCount.collect().mkString(","))
  }

  // groupByKey
  def wordCount2(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = words.map((_, 1))
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    val wordCount: RDD[(String, Int)] = groupRDD.mapValues(iter => iter.size)
    println(wordCount.collect().mkString(","))
  }

  // reduceByKey
  def wordCount3(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    println(wordCount.collect().mkString(","))
  }

  // aggregateByKey
  def wordCount4(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = mapRDD.aggregateByKey(0)(_ + _, _ + _)
    println(wordCount.collect().mkString(","))
  }

  // foldByKey
  def wordCount5(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = mapRDD.foldByKey(0)(_ + _)
    println(wordCount.collect().mkString(","))
  }

  // combinerByKey
  def wordCount6(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = mapRDD.combineByKey(key => key, _ + _, _ + _)
    println(wordCount.collect().mkString(","))
  }

  // countByKey
  def wordCount7(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: collection.Map[String, Long] = mapRDD.countByKey()
    println(wordCount.mkString(","))
  }

  // countByValue
  def wordCount8(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordCount: collection.Map[String, Long] = words.countByValue()
    println(wordCount.mkString(","))
  }

  // reduce
  def wordCount9(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[mutable.Map[String, Long]] = words.map(
      word => {
        mutable.Map[String, Long]((word, 1))
      }
    )
    val wordCount: mutable.Map[String, Long] = mapRDD.reduce(
      (map1, map2) => {
        map2.foreach {
          case (word, count) => {
            val newCount: Long = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
          }
        }
        map1
      }
    )
    println(wordCount)
  }

  // aggregate
  def wordCount10(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordCount: mutable.Map[String, Int] = words.aggregate(mutable.Map[String, Int]())(
      (map, word) => {
        map(word) = map.getOrElse(word, 0) + 1
        map
      },
      (map1, map2) => {
        map2.foreach {
          case (word, count) => {
            val newCount: Int = map1.getOrElse(word, 0) + count
            map1.update(word, newCount)
          }
        }
        map1
      }
    )
    println(wordCount)
  }

  // fold
  def wordCount11(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[mutable.Map[String, Int]] = words.map(word => mutable.Map(word -> 1))
    val wordCount: mutable.Map[String, Int] = mapRDD.fold(mutable.Map[String, Int]())(
      (map1, map2) => {
        map2.foreach {
          case (word, count) => {
            val newCount: Int = map1.getOrElse(word, 0) + count
            map1.update(word, newCount)
          }
        }
        map1
      }
    )
    println(wordCount)
  }
}
