package com.deerlili.gmall.realtime.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * JdbcUtil Hbase Jdbc Util
 *
 * @author lixx
 * @date 2022/7/18 23:33
 **/

public class JdbcUtil {
    /**
     * select * from t1;
     * return:
     *  xxx,xxx,xxx
     *  xxx,xxx,xxx
     * @param connection
     * @param querySql
     * @param cls
     * @param underScoreToCamel
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> cls, boolean underScoreToCamel) throws Exception {
        //创建集合用于存放查询结果数据
        ArrayList<T> arrayList = new ArrayList<>();
        //预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);
        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        //解析resultSet
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            //创建泛型对
            T t = cls.newInstance();
            //给泛型赋值
            for (int i = 1; i < columnCount+1; i++) {
                //获取列名
                String columnName = metaData.getColumnName(i);
                //判断时候需要装换为骆峰命名
                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }
                //获取列值
                Object object = resultSet.getObject(i);
                //给泛型对象赋值
                //JavaBean
                BeanUtils.setProperty(t, columnName,object);
                //JsonObject
                //BeanUtils.copyProperty(t, columnName, object);
            }
            //将对象添加到集合
            arrayList.add(t);
        }
        preparedStatement.close();
        resultSet.close();

        return arrayList;
    }

    public static void main(String[] args) {
        String s = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, "aa_dd".toLowerCase());
        System.out.println(s);
    }
}
