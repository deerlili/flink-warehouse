package com.deerlili.gmall.realtime.utils;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * KeyWordUtil 分词工具类
 *
 * @author lixx
 * @date 2022/8/17 16:34
 */
public class KeyWordUtil {

    public static List<String> splitKeyWord(String keyWord) throws IOException {
        ArrayList<String> list = new ArrayList<>();
        StringReader reader = new StringReader(keyWord);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);
        while (true) {
            Lexeme next = ikSegmenter.next();
            if (next != null) {
                String text = next.getLexemeText();
                list.add(text);
            } else {
                break;
            }
        }
        return list;
    }

    public static List<String> splitKeyWordByJieba(String keyWord) {
        ArrayList<String> list = new ArrayList<>();
        JiebaSegmenter segmenter = new JiebaSegmenter();
        List<SegToken> process = segmenter.process(keyWord, JiebaSegmenter.SegMode.INDEX);
        for (SegToken token:process){
            list.add(token.word);
        }
        return list;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(splitKeyWord("iphone11和ipad123123评估苹果手机"));
        System.out.println(splitKeyWordByJieba("iphone11和ipad123123评估苹果手机"));
    }
}
