package com.zhuzheng.database2024fall_lab2.memory;

import java.io.IOException;

/**
 * ClassName: BufferManager
 * Package: com.zhuzheng.database2024fall_lab2.memory
 * Description:
 *
 * @Author: east_moon
 * @Create: 2024/11/18 - 19:38
 * Version: v1.0
 */
public interface BufferManager {
    int numIOs();
    void removeBCB(BufferControlBlock bufferControlBlock);
    void addBCBToHead(BufferControlBlock bufferControlBlock);
    void setDirty(BufferControlBlock bufferControlBlock);
    void unSetDirty(BufferControlBlock bufferControlBlock);

    /**
     * 运行结束时统一写回脏页面
     */
    void writeDirtys() throws IOException;
    void writeDity(int pageId) throws IOException;
    String read(int pageId) throws IOException;
    void write(int pageId) throws IOException;
    int numFreeFrames();

    /**
     * 根据pageId建立page和frame的映射关系
     * @param pageId
     * @return
     */
    BufferControlBlock fixPage(int pageId);

    /**
     * 根据pageId解除page和frame的映射关系，根据dirty位判断是否需要写回writeDirty(pageId);
     * @param pageId
     * @return
     */
    void unFixPage(int pageId) throws IOException;

    /**
     * 要建立page和frame的映射关系时，根据pageId为frameId分配一个空闲frame
     * @param pageId
     * @return
     */
    int hash(int pageId);
    void initialPages() throws IOException;
    void close() throws IOException;
}
