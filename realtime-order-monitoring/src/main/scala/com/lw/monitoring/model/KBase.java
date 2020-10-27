package com.lw.monitoring.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 获取kafka中json串的data中数组的内容
 */
public class KBase {

    private String dmlType;
    JSONObject record = new JSONObject();

    KBase(String json) {

        try {
            JSONObject jsonObject = JSON.parseObject(json);
            JSONArray jsonArray = jsonObject.getJSONArray("data");
            this.record = jsonArray.getJSONObject(0);
            this.dmlType = jsonObject.getString("type");
        } catch (Exception e) {
            System.err.println(e.getCause());
        }
    }
}
