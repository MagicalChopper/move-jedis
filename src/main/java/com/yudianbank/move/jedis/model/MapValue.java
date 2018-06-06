package com.yudianbank.move.jedis.model;

public class MapValue {
    private String type;
    private Object obj;

    public MapValue() {
    }

    public MapValue(String type, Object obj) {
        this.type = type;
        this.obj = obj;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Object getObj() {
        return obj;
    }

    public void setObj(Object obj) {
        this.obj = obj;
    }
}
