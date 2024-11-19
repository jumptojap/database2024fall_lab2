package com.zhuzheng.database2024fall_lab2.test;

import com.zhuzheng.database2024fall_lab2.constant.BufferManagerConstant;
import com.zhuzheng.database2024fall_lab2.io.DataStorageManager;
import com.zhuzheng.database2024fall_lab2.io.DataStorageManagerImpl;
import com.zhuzheng.database2024fall_lab2.memory.BufferManager;
import com.zhuzheng.database2024fall_lab2.memory.BufferManagerImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ClassName: BufferManagerTest
 * Package: com.zhuzheng.database2024fall_lab2.test
 * Description:
 *
 * @Author: east_moon
 * @Create: 2024/11/19 - 13:04
 * Version: v1.0
 */
@SpringBootTest
class BufferManagerTest {
    private BufferManager bufferManager;
    private DataStorageManager dataStorageManager;
    @BeforeEach
    void setUp() throws IOException {
        File file = new File("C:\\Users\\wjsww\\Desktop\\database\\database2024fall_lab2-master\\target\\classes\\data.dbf");
        file.delete();
        file.createNewFile();
        dataStorageManager = new DataStorageManagerImpl();
        bufferManager = new BufferManagerImpl(dataStorageManager);
        bufferManager.initialPages();
    }

    @AfterEach
    void tearDown() throws IOException {

    }
    @Test
    public void testWriteAndReadSinglePage(){
        assertEquals(0, bufferManager.numIOs());
        assertEquals(BufferManagerConstant.BUFFER_SIZE, bufferManager.numFreeFrames());
        bufferManager.write(1);
        assertEquals(0, bufferManager.numIOs());
        assertEquals(BufferManagerConstant.BUFFER_SIZE - 1, bufferManager.numFreeFrames());
        bufferManager.read(1);
        bufferManager.writeDirtys();
        assertEquals(2, bufferManager.numIOs());
    }
    @Test
    public void testReadAndWriteSinglePage(){
        assertEquals(0, bufferManager.numIOs());
        assertEquals(BufferManagerConstant.BUFFER_SIZE, bufferManager.numFreeFrames());
        bufferManager.read(1);
        assertEquals(1, bufferManager.numIOs());
        assertEquals(BufferManagerConstant.BUFFER_SIZE - 1, bufferManager.numFreeFrames());
        bufferManager.write(1);
        assertEquals(1, bufferManager.numIOs());
        bufferManager.writeDirtys();
        assertEquals(3, bufferManager.numIOs());
    }
    @Test
    public void testWriteAndReadMultiplePages(){
        assertEquals(0, bufferManager.numIOs());
        assertEquals(BufferManagerConstant.BUFFER_SIZE, bufferManager.numFreeFrames());
        for(int i = 0; i < BufferManagerConstant.BUFFER_SIZE; i++){
            bufferManager.write(i);
        }
        assertEquals(0, bufferManager.numIOs());
        assertEquals(0, bufferManager.numFreeFrames());
        //page0被最近访问,page1被最久访问
        bufferManager.read(0);
        assertEquals(0, bufferManager.numIOs());
        //page1被LRU算法置换出去，page2被最久访问
        bufferManager.read(BufferManagerConstant.BUFFER_SIZE);
        assertEquals(3, bufferManager.numIOs());
        bufferManager.read(0);
        assertEquals(3, bufferManager.numIOs());
        //page2被LRU算法置换出去，page3为最久访问
        bufferManager.read(1);
        assertEquals(6, bufferManager.numIOs());
        bufferManager.write(512);
        assertEquals(6, bufferManager.numIOs());
        bufferManager.write(2048);
        assertEquals(8, bufferManager.numIOs());
        bufferManager.writeDirtys();
        assertEquals(8 + 2 * (BufferManagerConstant.BUFFER_SIZE - 2), bufferManager.numIOs());
    }
}