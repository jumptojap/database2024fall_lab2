package com.zhuzheng.database2024fall_lab2.memory;

import com.zhuzheng.database2024fall_lab2.constant.BufferControlBlockConstant;
import com.zhuzheng.database2024fall_lab2.constant.BufferFrameConstant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: BufferControlBlock
 * Package: com.zhuzheng.database2024fall_lab2.memory
 * Description:
 *
 * @Author: east_moon
 * @Create: 2024/11/18 - 20:33
 * Version: v1.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BufferControlBlock {
    private Integer pageId;
    private Integer frameId;
    private int dirty = BufferControlBlockConstant.CLEAN;
    BufferControlBlock prev;
    BufferControlBlock next;
}
