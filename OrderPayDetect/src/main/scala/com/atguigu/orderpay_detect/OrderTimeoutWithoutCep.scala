package com.atguigu.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/6/8 10:21
  */
object OrderTimeoutWithoutCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取数据转换成样例类
    val resource = getClass.getResource("/OrderLog.csv")
    //    val orderEventStream: DataStream[OrderEvent] = env.readTextFile(resource.getPath)
    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 按照orderId分组处理，主流输出正常支付结果，侧输出流输出订单超时结果
    val orderResultStream: DataStream[OrderResult] = orderEventStream
      .keyBy(_.orderId)
      .process(new OrderPayMatchDetect())

    // 打印输出
    orderResultStream.print("payed")
    orderResultStream.getSideOutput(new OutputTag[OrderResult]("timeout")).print("timeout")

    env.execute("order timeout without cep job")
  }
}

// 实现自定义KeyedProcessFunction，按照当前数据的类型，以及之前的状态，判断要做的操作
class OrderPayMatchDetect() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
  // 定义状态，用来保存之前的create和pay事件是否已经来过
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed", classOf[Boolean]))
  lazy val isCreatedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-created", classOf[Boolean]))
  // 定义状态，保存定时器时间戳
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  // 定义侧输出流标签，用于输出超时订单结果
  val orderTimeoutOutputtag = new OutputTag[OrderResult]("timeout")

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    // 迟到数据放入侧输出流
    //    if(value.timestamp * 1000L < ctx.timerService().currentWatermark())
    //      ctx.output(new OutputTag[OrderEvent]("late"), value)
    // 先拿到当前状态
    val isPayed = isPayedState.value()
    val isCreated = isCreatedState.value()
    val timerTs = timerTsState.value()

    // 判断当前事件的类型，以及之前的状态，不同的组合，有不同的处理流程
    // 情况1：来的是create，接下来判断是否pay过
    if (value.eventType == "create") {
      // 情况1.1：如果已经pay过，匹配成功，输出到主流
      if (isPayed) {
        out.collect(OrderResult(value.orderId, "payed"))
        // 已经输出结果了，清空状态和定时器
        isPayedState.clear()
        isCreatedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      }
      // 情况1.2: 如果还没pay过，要注册一个定时器，开始等待
      else {
        val ts = value.timestamp * 1000L + 15 * 60 * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        // 更新状态
        timerTsState.update(ts)
        isCreatedState.update(true)
      }
    }

    // 情况2：来的是pay，那么要继续判断是否已经create过
    else if (value.eventType == "pay") {
      // 情况2.1：如果已经pay，还需要判断create和pay的时间差是否超过15分钟
      if (isCreated) {
        // 2.1.1: 如果没有超时，主流输出正常结果
        if (value.timestamp * 1000L <= timerTs) {
          out.collect(OrderResult(value.orderId, "payed"))
        }
        // 2.1.2: 如果已经超时，侧输出流输出超时结果
        else {
          ctx.output(orderTimeoutOutputtag, OrderResult(value.orderId, "payed but already timeout"))
        }
        // 已经输出结果了，清空状态和定时器
        isPayedState.clear()
        isCreatedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      }
      // 情况2.2：如果没有create过，可能是create丢失，也可能是乱序，需要等待create
      else {
        // 注册一个当前pay事件时间戳的定时器
        ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L)
        // 更新状态
        timerTsState.update(value.timestamp * 1000L)
        isPayedState.update(true)
      }
    }
  }

  // 定时器触发，肯定有一个事件没等到，输出一个异常信息
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    if (isPayedState.value()) {
      // 如果isPayed为true，说明一定是pay先来，而且没等到create
      ctx.output(orderTimeoutOutputtag, OrderResult(ctx.getCurrentKey, "payed but not created"))
    } else {
      // 如果isPayed为false，说明一定是create先来，等pay没等到
      ctx.output(orderTimeoutOutputtag, OrderResult(ctx.getCurrentKey, "timeout"))
    }
    // 清空状态
    isPayedState.clear()
    isCreatedState.clear()
    timerTsState.clear()
  }
}