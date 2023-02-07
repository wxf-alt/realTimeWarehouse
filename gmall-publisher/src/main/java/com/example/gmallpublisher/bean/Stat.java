package com.example.gmallpublisher.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Auther: wxf
 * @Date: 2023/2/7 17:02:33
 * @Description: Stat
 * @Version 1.0.0
 */
public class Stat {

    private String title;
    private List<Option> options = new ArrayList<>();

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Option> getOptions() {
        return options;
    }

    public void addOptions(Option opt){
        options.add(opt);
    }

}