package com.lw.monitoring.model

/**
 * @param json
 */

case class Scala_KTrade(json: String) extends Scala_KBase(json) {

  val id: Int = record.getString("id").toInt

  //订单号
  val orderId: Int = record.getString("order_id").toInt

  //购买数量
  val buyCount: Int = record.getString("buy_count").toInt

  //产品ID
  val pId: String = record.getString("p_id")

  //购买金额
  val totalPrice: Double = record.getString("total_price").toDouble

  //购买时间
  val orderTime: String = record.getString("create_time")

  //用户ID
  val userId: Int = record.getString("user_id").toInt

  //订单状态
  val orderStatus: String = record.getString("order_status")
}
