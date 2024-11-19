package com.zhuzheng.database2024fall_lab2.test;

import com.zhuzheng.database2024fall_lab2.constant.DataStorageManagerConstant;
import com.zhuzheng.database2024fall_lab2.io.DataStorageManager;
import com.zhuzheng.database2024fall_lab2.io.DataStorageManagerImpl;
import com.zhuzheng.database2024fall_lab2.memory.BufferFrame;
import com.zhuzheng.database2024fall_lab2.util.DataStorageManagerUtil;
import lombok.extern.java.Log;
import lombok.extern.log4j.Log4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
/**
 * ClassName: DataStorageManagerTest
 * Package: com.zhuzheng.database2024fall_lab2.test
 * Description:
 * @Author: east_moon
 * @Create: 2024/11/18 - 9:03
 * Version: v1.0
*/
@SpringBootTest
class DataStorageManagerTest {
    DataStorageManager dataStorageManager;
    @BeforeEach
    void setUp() {
        dataStorageManager = new DataStorageManagerImpl();
    }

    @AfterEach
    void tearDown() throws IOException {
        File file = new File("C:\\Users\\wjsww\\Desktop\\database\\database2024fall_lab2-master\\target\\classes\\data.dbf");
        file.delete();
        file.createNewFile();
    }
    @Test
    public void testOpenFile(){
        String absolutePath = dataStorageManager.getFile().getAbsolutePath();
        assertEquals("C:\\Users\\wjsww\\Desktop\\database\\database2024fall_lab2-master\\target\\classes\\data.dbf", absolutePath, "文件打开不成功");
    }
    @Test
    public void testWriteSinglePage(){
        int pageId = 0;
        BufferFrame bufferFrame = new BufferFrame();
        String pageContext = "The context of page" + pageId + " is xxxxx";
        DataStorageManagerUtil.copyCharArray(pageContext.toCharArray(), bufferFrame.getField());
        bufferFrame.setNumChars(pageContext.length());
        dataStorageManager.writePage(pageId, bufferFrame);
        assertEquals(1, dataStorageManager.getNumPages());
        assertEquals(2, dataStorageManager.getNumIOs());
        assertEquals(DataStorageManagerConstant.PAGE_USED, dataStorageManager.getUse(pageId));
        assertEquals(true, dataStorageManager.pageExists(pageId));
        for(int i = 1; i < DataStorageManagerConstant.MAX_PAGES; i++){
            assertEquals(false, dataStorageManager.pageExists(i));
            assertEquals(DataStorageManagerConstant.PAGE_NOT_USED, dataStorageManager.getUse(i));
        }
    }
    @Test
    public void testReadSinglePage(){
        int pageId = 0;
        BufferFrame bufferFrame = new BufferFrame();
        String pageContext = "The context of page" + pageId + " is xxxxx";
        DataStorageManagerUtil.copyCharArray(pageContext.toCharArray(), bufferFrame.getField());
        bufferFrame.setNumChars(pageContext.length());
        dataStorageManager.writePage(pageId, bufferFrame);
        BufferFrame frame = dataStorageManager.readPage(pageId);
        assertEquals(frame.toString(), bufferFrame.toString());
        assertEquals(1, dataStorageManager.getNumPages());
        assertEquals(3, dataStorageManager.getNumIOs());
        assertEquals(DataStorageManagerConstant.PAGE_USED, dataStorageManager.getUse(pageId));
        assertEquals(true, dataStorageManager.pageExists(pageId));
        for(int i = 1; i < DataStorageManagerConstant.MAX_PAGES; i++){
            assertEquals(false, dataStorageManager.pageExists(i));
            assertEquals(DataStorageManagerConstant.PAGE_NOT_USED, dataStorageManager.getUse(i));
        }
    }
    //@Test
    public void testSlowReadAllPages(){
        for(int i = 0; i < DataStorageManagerConstant.MAX_PAGES; i++){
            int pageId = i;
            BufferFrame bufferFrame = new BufferFrame();
            String pageContext = "The context of page" + pageId + " is xxxxx";
            DataStorageManagerUtil.copyCharArray(pageContext.toCharArray(), bufferFrame.getField());
            bufferFrame.setNumChars(pageContext.length());
            assertEquals(DataStorageManagerConstant.PAGE_NOT_USED, dataStorageManager.getUse(pageId));
            assertEquals(false, dataStorageManager.pageExists(pageId));
            assertEquals(i, dataStorageManager.getNumPages());
            assertEquals(2 * i, dataStorageManager.getNumIOs());
            dataStorageManager.writePage(pageId, bufferFrame);
            assertEquals(2 * i + 2, dataStorageManager.getNumIOs());
            assertEquals(true, dataStorageManager.pageExists(pageId));
            assertEquals(DataStorageManagerConstant.PAGE_USED, dataStorageManager.getUse(pageId));
            assertEquals(i + 1, dataStorageManager.getNumPages());
        }
        for(int i = 0; i < DataStorageManagerConstant.MAX_PAGES; i++){
            int pageId = i;
            BufferFrame bufferFrame = new BufferFrame();
            String pageContext = "The context of page" + pageId + " is xxxxx";
            DataStorageManagerUtil.copyCharArray(pageContext.toCharArray(), bufferFrame.getField());
            bufferFrame.setNumChars(pageContext.length());
            assertEquals(i + 2 * DataStorageManagerConstant.MAX_PAGES, dataStorageManager.getNumIOs());
            BufferFrame frame = dataStorageManager.readPage(pageId);
            assertEquals(i + 1 + 2 * DataStorageManagerConstant.MAX_PAGES, dataStorageManager.getNumIOs());
            assertEquals(frame.toString(), bufferFrame.toString());
        }
    }
    @Test
    public void testFastReadAllPages(){
        List<Integer> pageIdList = new ArrayList<>();
        List<BufferFrame> frmList = new ArrayList<>();

        for(int i = 0; i < DataStorageManagerConstant.MAX_PAGES; i++) {
            int pageId = i;
            BufferFrame bufferFrame = new BufferFrame();
            String pageContext = "The context of page" + pageId + " is xxxxx";
            DataStorageManagerUtil.copyCharArray(pageContext.toCharArray(), bufferFrame.getField());
            bufferFrame.setNumChars(pageContext.length());
            pageIdList.add(pageId);
            frmList.add(bufferFrame);
            assertEquals(DataStorageManagerConstant.PAGE_NOT_USED, dataStorageManager.getUse(pageId));
            assertEquals(false, dataStorageManager.pageExists(pageId));
        }
        assertEquals(0, dataStorageManager.getNumPages());
        dataStorageManager.initialPages(pageIdList, frmList);
        assertEquals(DataStorageManagerConstant.MAX_PAGES, dataStorageManager.getNumPages());
        for(int i = 0; i < DataStorageManagerConstant.MAX_PAGES; i++){
            assertEquals(DataStorageManagerConstant.PAGE_USED, dataStorageManager.getUse(i));
            assertEquals(true, dataStorageManager.pageExists(i));
        }
        for(int i = 0; i < DataStorageManagerConstant.MAX_PAGES; i++){
            int pageId = i;
            BufferFrame bufferFrame = new BufferFrame();
            String pageContext = "The context of page" + pageId + " is xxxxx";
            DataStorageManagerUtil.copyCharArray(pageContext.toCharArray(), bufferFrame.getField());
            bufferFrame.setNumChars(pageContext.length());
            assertEquals(i , dataStorageManager.getNumIOs());
            BufferFrame frame = dataStorageManager.readPage(pageId);
            assertEquals(i + 1 , dataStorageManager.getNumIOs());
            assertEquals(frame.toString(), bufferFrame.toString());
        }
    }
}