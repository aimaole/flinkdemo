package com.maomao.test;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author maohongqi
 * @Date 2019/11/25 17:32
 * @Version 1.0
 **/
public class TestMM {
    public static void main(String[] args) {
        String ss = "a,b,c";
        String sss = "a,b,c,d";
        int i = sss.indexOf("b,c,d");
        System.out.println(i);
        List<String> list = new ArrayList<>();
        System.out.println(list.isEmpty());

    }
}
