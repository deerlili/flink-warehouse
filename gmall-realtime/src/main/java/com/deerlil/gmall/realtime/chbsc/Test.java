package com.deerlil.gmall.realtime.chbsc;

import com.alibaba.fastjson.JSONObject;

import java.text.MessageFormat;
import java.util.ArrayList;

/**
 * com.deerlil.gmall.realtime.test test
 *
 * @author lixx
 * @date 2022/6/15 14:21
 */
public class Test {
    public static void main(String[] args) {
        ArrayList<ParameterModel> parameters=new ArrayList<ParameterModel>();

        parameters.add(new ParameterModel("message","hello"));
        parameters.add(new ParameterModel("username","Terry Dan"));
        parameters.add(new ParameterModel("languagecode","zh-CN"));

        Security security=new Security("BlockingPDA","UW14dlkydHBibWRRUkVFJTNE",parameters);


        System.out.println("Command：AIService.GetResponse");
        System.out.println("ParameterString："+security.GetParameterString());
        System.out.println("AppID："+security.getAppId());
        System.out.println("Time："+security.getTime());
        System.out.println("DynamicPassword："+security.getDynamicPassword());
        System.out.println("RequestSign："+security.GetSign());

    }

}
