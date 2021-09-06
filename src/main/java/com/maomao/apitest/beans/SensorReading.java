package com.maomao.apitest.beans;

import java.io.Serializable;

public class SensorReading implements Serializable {
    private String name;
    private long time;
    private long value;

    public SensorReading(String name, long time, long value) {
        this.name = name;
        this.time = time;
        this.value = value;
    }

    //flink的POJO必须有
    public SensorReading() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "name='" + name + '\'' +
                ", time=" + time +
                ", value=" + value +
                '}';
    }
}