package com.zhuzheng.database2024fall_lab2.io;

import com.zhuzheng.database2024fall_lab2.memory.BufferFrame;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

/**
 * ClassName: DataStorageManager
 * Package: com.zhuzheng.database2024fall_lab2
 * Description:
 *
 * @Author: east_moon
 * @Create: 2024/11/17 - 15:45
 * Version: v1.0
 */
public interface DataStorageManager {
    int openFile(String filename);
    int closeFile();
    BufferFrame readPage(int pageId) throws IOException;
    void writePage(int pageId, BufferFrame frm) throws IOException;
    void seek(int offset, int pos) throws IOException;
    File getFile();
    void incNumPages();
    int getNumPages();
    void setUse(int pageId, int useBit);
    int getUse(int pageId);
    int getNumIOs();
    boolean pageExists(int pageId);
    int initialPages(List<Integer> pageIdList, List<BufferFrame> frmList);
}
