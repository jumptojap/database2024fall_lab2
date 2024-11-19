package com.zhuzheng.database2024fall_lab2.memory;

import com.zhuzheng.database2024fall_lab2.constant.BufferFrameConstant;
import lombok.*;

import java.util.Arrays;

/**
 * ClassName: BufferFrame
 * Package: com.zhuzheng.database2024fall_lab2
 * Description:
 *
 * @Author: east_moon
 * @Create: 2024/11/17 - 15:50
 * Version: v1.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BufferFrame {

    private char[] field = new char[BufferFrameConstant.FRAME_SIZE];
    private int numChars = 0;

    @Override
    public String toString() {
        return "BufferFrame{" +
                "field=" + new String(field, 0, numChars) +
                ", numChars=" + numChars +
                '}';
    }

    /**
     * 保证frame的干净，逻辑上清除frame的内容
     */
    public void clean(){
        numChars = 0;
    }
}
