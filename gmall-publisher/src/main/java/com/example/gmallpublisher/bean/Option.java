package com.example.gmallpublisher.bean;

/**
 * @Auther: wxf
 * @Date: 2023/2/7 17:07:42
 * @Description: Option
 * @Version 1.0.0
 */
public class Option {
    private String name;
    private Long value;

    public Option() {
    }

    public Option(String name, Long value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }
}