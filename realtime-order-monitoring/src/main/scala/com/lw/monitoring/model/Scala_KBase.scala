package com.lw.monitoring.model

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

class Scala_KBase(jsonStr: String){

  val jsonObject: JSONObject = JSON.parseObject(jsonStr)
  val jsonArray: JSONArray = jsonObject.getJSONArray("data")
  val record: JSONObject = jsonArray.getJSONObject(0)
  val dmlType: String = jsonObject.getString("type")


}
