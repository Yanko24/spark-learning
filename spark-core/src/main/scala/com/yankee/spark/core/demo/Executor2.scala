package com.yankee.spark.core.demo

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

/**
 * @author Yankee
 * @date 2021/2/27 21:58
 */
object Executor2 {
  def main(args: Array[String]): Unit = {
    // 启动服务器，接收数据
    val server: ServerSocket = new ServerSocket(8888)
    println("服务器启动，等待接收数据")

    // 等待客户端的连接
    val client: Socket = server.accept()
    val in: InputStream = client.getInputStream
    val stream: ObjectInputStream = new ObjectInputStream(in)

    val task: SubTask = stream.readObject().asInstanceOf[SubTask]
    val list: List[Int] = task.compute()
    println(s"计算节点[8888]计算的结果为：$list")

    stream.close()
    in.close()
    client.close()
  }
}
