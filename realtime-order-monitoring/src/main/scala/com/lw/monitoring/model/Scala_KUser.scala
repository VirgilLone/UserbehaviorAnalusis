package com.lw.monitoring.model

case class Scala_KUser(json: String) extends Scala_KBase(json) {

  val id: String = record.getString("id")
  val userName: String = record.getString("user_name")
  val city: String = record.getString("city")

}
