package com.zhuzheng.database2024fall_lab2.memory;

import com.zhuzheng.database2024fall_lab2.constant.BufferFrameConstant;
import lombok.Data;
import lombok.ToString;

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
}
