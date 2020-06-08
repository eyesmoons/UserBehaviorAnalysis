package com.atguigu.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/6/8 14:17
  */

// 定义到账事件的样例类
case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)

object TxMatch {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1. 读取数据转换成样例类
    val orderResource = getClass.getResource("/OrderLog.csv")
//    val orderEventStream: KeyedStream[OrderEvent, String] = env.readTextFile(orderResource.getPath)
    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map(data => {
      val dataArray = data.split(",")
      OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter( _.txId != "" )    // 只要pay事件
      .keyBy(_.txId)


    val receiptResource = getClass.getResource("/ReceiptLog.csv")
//    val receiptEventStream: KeyedStream[ReceiptEvent, String] = env.readTextFile(receiptResource.getPath)
    val receiptEventStream = env.socketTextStream("localhost", 8888)
      .map(data => {
      val dataArray = data.split(",")
      ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.txId)

    // 2. 连接两条流，做分别计算
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream.connect( receiptEventStream )
      .process( new TxPayMatchDetect() )

    // 3. 定义不匹配的侧输出流标签
    val unmatchedPays = new OutputTag[OrderEvent]("unmatched-pays")
    val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatched-receipts")
    // 4. 打印输出
    resultStream.print("matched")
    resultStream.getSideOutput(unmatchedPays).print("unmatched-pays")
    resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts")

    env.execute("tx match job")
  }
}

// 自定义CoProcessFunction，用状态保存另一条流已来的数据
class TxPayMatchDetect() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  // 定义状态，用来保存已经来到的pay事件和receipt事件
  lazy val payEventState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-event", classOf[OrderEvent]))
  lazy val receiptEventState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-event", classOf[ReceiptEvent]))

  // 订单事件流里的数据，每来一个就调用一次processElement1
  override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 订单支付事件来了，要考察当前是否已经来过receipt
    val receipt = receiptEventState.value()
    if( receipt != null ){
      // 1. 如果来过，正常匹配输出到主流
      out.collect( (pay, receipt) )
      // 清空状态
      payEventState.clear()
      receiptEventState.clear()
    } else {
      // 2. 如果receipt还没来，注册定时器等待
      ctx.timerService().registerEventTimeTimer(pay.timestamp * 1000L + 5000L)
      // 更新状态
      payEventState.update(pay)
    }
  }

  override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 到账事件来了，要考察当前是否已经来过pay
    val pay = payEventState.value()
    if( pay != null ){
      // 1. 如果来过，正常匹配输出到主流
      out.collect( (pay, receipt) )
      // 清空状态
      payEventState.clear()
      receiptEventState.clear()
    } else {
      // 2. 如果receipt还没来，注册定时器等待
      ctx.timerService().registerEventTimeTimer(receipt.timestamp * 1000L + 3000L)
      // 更新状态
      receiptEventState.update(receipt)
    }
  }

  // 定时器触发，需要判断状态中的值
  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 判断两个状态，哪个不为空，那么就是另一个没来
    if(payEventState.value() != null)
      ctx.output(new OutputTag[OrderEvent]("unmatched-pays"), payEventState.value())
    if(receiptEventState.value() != null )
      ctx.output(new OutputTag[ReceiptEvent]("unmatched-receipts"), receiptEventState.value())

    // 清空状态
    payEventState.clear()
    receiptEventState.clear()
  }
}