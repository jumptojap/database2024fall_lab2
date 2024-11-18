package com.zhuzheng.database2024fall_lab2.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: DataStorageManagerUtil
 * Package: com.zhuzheng.database2024fall_lab2.util
 * Description:
 *
 * @Author: east_moon
 * @Create: 2024/11/17 - 20:04
 * Version: v1.0
 */
public class DataStorageManagerUtil {
    public static void copyCharArray(char[] source, char[] target){
        for(int i = 0 ; i < source.length; i++){
            target[i] = source[i];
        }
    }
    public static List<String> readAll(BufferedReader reader) throws IOException {
        List<String> lineList = new ArrayList<>();
        String lineContext = null;
        while((lineContext = reader.readLine()) != null){
            lineList.add(lineContext);
        }
        return lineList;
    }
}
