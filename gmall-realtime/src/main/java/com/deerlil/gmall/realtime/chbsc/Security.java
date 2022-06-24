package com.deerlil.gmall.realtime.chbsc;


import com.deerlil.gmall.realtime.chbsc.ParameterModel;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

public class Security {

    private ArrayList<ParameterModel> Parameters=new ArrayList<ParameterModel>();
    private String AppId="";
    private String AppSecret="";
    private String Time="";
    private String DynamicPassword="";

    public ArrayList<ParameterModel> getParameters() {
        return Parameters;
    }

    public void setParameters(ArrayList<ParameterModel> parameters) {
        Parameters = parameters;
    }

    public String getAppId() {
        return AppId;
    }

    public void setAppId(String appId) {
        AppId = appId;
    }

    public String getAppSecret() {
        return AppSecret;
    }

    public void setAppSecret(String appSecret) {
        AppSecret = appSecret;
    }

    public String getTime() {
        return Time;
    }

    public void setTime(String time) {
        Time = time;
    }

    public String getDynamicPassword() {
        return DynamicPassword;
    }

    public void setDynamicPassword(String dynamicPassword) {
        DynamicPassword = dynamicPassword;
    }

    public Security(String appid, String appsecret, ArrayList<ParameterModel> parameters)
    {
        Collections.sort(parameters, new Comparator<ParameterModel>() {
            @Override
            public int compare(ParameterModel o1, ParameterModel o2) {
                return o1.getParameterName().compareTo(o2.getParameterName());
            }
        });

        this.Parameters=parameters;
        this.AppId=appid;
        this.AppSecret=appsecret;
        this.Time=GetDate();
        this.DynamicPassword=GetDynamicPassword();


    }

    private  String MakeSignPlain()
    {
        String result=GetParameterString();
        result+=MessageFormat.format("&Time={0}",this.Time);
        result+=MessageFormat.format("&DynamicPassword={0}",this.DynamicPassword);
        result=result.toUpperCase();
        return  result;

    }

    private String GetDate()
    {
        Date day=new Date();
        SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(day);

    }
    private String GetDynamicPassword()
    {
        String result="0";
        int min = 100000;
        int max = 999999;
        int range = max - min;
        int randomNum=(int)(Math.random()*range+min);
        result=String.valueOf(randomNum);
        return result;
    }

    public String GetParameterString()
    {
        String result="";
        for(int i=0;i<this.Parameters.size();i++)
        {
            result += MessageFormat.format("{0}={1}&", this.Parameters.get(i).getParameterName(), EnscapeChar(Parameters.get(i).getParameterValue()));

        }
        if(result.length()>0)
        {
            result=result.substring(0,result.length()-1);
        }
        return result;
    }

    private  String EnscapeChar(String str)
    {
        str=(str==null?"":str);
        str=str.replace("&","Δ26Δ");
        str=str.replace("=","Δ3DΔ");
        return  str;
    }

    public String GetSign()
    {
        String result="";
        String signplain=MakeSignPlain();
        result=HMACSHA256.Encrypt(this.AppSecret,signplain);
        return  result;
    }


}
