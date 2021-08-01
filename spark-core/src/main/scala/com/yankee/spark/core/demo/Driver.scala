package com.yankee.spark.core.demo

import java.io
import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

/**
 * @author Yankee
 * @date 2021/2/27 21:57
 */
object Driver {
  def main(args: Array[String]): Unit = {
    // 连接服务器
    val client1: Socket = new Socket("localhost", 9999)
    val client2: Socket = new Socket("localhost", 8888)

    val task: Task = new Task

    // 输出流
    val out1: OutputStream = client1.getOutputStream
    val stream1: ObjectOutputStream = new ObjectOutputStream(out1)

    val subTask: SubTask = new SubTask()
    subTask.logic = task.logic
    subTask.datas = task.datas.take(task.datas.length / 2)

    // 发送数据
    stream1.writeObject(subTask)
    stream1.flush()
    stream1.close()
    out1.close()
    client1.close()

    // 输出流
    val out2: OutputStream = client2.getOutputStream
    val stream2: ObjectOutputStream = new ObjectOutputStream(out2)

    val subTask2: SubTask = new SubTask()
    subTask2.logic = task.logic
    subTask2.datas = task.datas.takeRight(task.datas.length / 2)
    // 发送数据
    stream2.writeObject(subTask2)
    stream2.flush()
    stream2.close()
    out2.close()
    client2.close()

    println("客户端数据发送完毕")
  }
}
