package com.deerlili.gmall.realtime.app.function;

import com.deerlili.gmall.realtime.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * SplitFunction
 *
 * @author lixx
 * @date 2022/8/17 17:22
 * @des 自定义 UDTF 函数实现分词功能
 */
@FunctionHint(output = @DataTypeHint("ROW<s STRING>"))
public class SplitFunction extends TableFunction<Row> {
    public void eval(String str) {
        try {
            //分词
            List<String> keyWords = KeyWordUtil.splitKeyWord(str);
            //遍历写出
            for (String keyWord: keyWords) {
                collect(Row.of(keyWord));
            }
        } catch (IOException e) {
            collect(Row.of(str));
        }

    }
}
