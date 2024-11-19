package com.zhuzheng.database2024fall_lab2.util;

import com.zhuzheng.database2024fall_lab2.memory.BufferFrame;

/**
 * ClassName: BufferFrameUtil
 * Package: com.zhuzheng.database2024fall_lab2.util
 * Description:
 *
 * @Author: east_moon
 * @Create: 2024/11/19 - 10:12
 * Version: v1.0
 */
public class BufferFrameUtil {
    public static void copyBufferFrame(BufferFrame source, BufferFrame target){
        target.setNumChars(source.getNumChars());
        for(int i = 0; i < source.getNumChars(); i++){
            target.getField()[i] = source.getField()[i];
        }
    }
}
