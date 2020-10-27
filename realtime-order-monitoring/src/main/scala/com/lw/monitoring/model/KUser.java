package com.lw.monitoring.model;


public class KUser extends KBase {
    private String id;
    private String userName;
    private String city;

    public KUser(String json) {
        super(json);

        this.id = record.getString("id");
        this.userName = record.getString("user_name");
        this.city = record.getString("city");
    }

    public String getId() {
        return id;
    }

    public String getUserName() {
        return userName;
    }

    public String getCity() {
        return city;
    }
}
