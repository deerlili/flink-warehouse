package com.deerlil.gmall.realtime.chbsc;

public class ParameterModel {

    private String ParameterName="";
    private String ParameterValue="";

    public ParameterModel(String parameterName, String parameterValue) {
        ParameterName = parameterName;
        ParameterValue = parameterValue;
    }

    public String getParameterName() {
        return ParameterName;
    }

    public void setParameterName(String parameterName) {
        ParameterName = parameterName;
    }

    public String getParameterValue() {
        return ParameterValue;
    }

    public void setParameterValue(String parameterValue) {
        ParameterValue = parameterValue;
    }
}
