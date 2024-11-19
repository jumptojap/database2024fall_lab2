package com.zhuzheng.database2024fall_lab2.io;

import com.zhuzheng.database2024fall_lab2.constant.DataStorageManagerConstant;
import com.zhuzheng.database2024fall_lab2.memory.BufferFrame;
import com.zhuzheng.database2024fall_lab2.util.DataStorageManagerUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: DataStorageManagerImpl
 * Package: com.zhuzheng.database2024fall_lab2
 * Description:
 *
 * @Author: east_moon
 * @Create: 2024/11/17 - 16:00
 * Version: v1.0
 */
@Slf4j
@Component
public class FastDataStorageManagerImpl implements DataStorageManager {
    private BufferedReader reader;
    private BufferedWriter writer;
    private File file;
    private int numPages;
    private int numIOs;
    private List<String> dataInMemory;
    /**
     * int数组pages代表基页，Index代表数据页的页号
     * pages[index]中记录了第index + 1页中的数据被存储在了data.dbf文件的第几行
     */
    private int[] pages;
    private int[] used;
    public FastDataStorageManagerImpl() {
        log.info("DataStorageManager初始化...");
        openFile(DataStorageManagerConstant.FILE_NAME);
        pages = new int[DataStorageManagerConstant.MAX_PAGES];
        /**
         * 初始时基页（Base Page）pages中对应的数据页（Data Page）全为空（-1）
         * 即对应数据页在data.dbf文件中没有与之对应的行号
         */
        for (int i = 0; i < pages.length; i++) {
            pages[i] = DataStorageManagerConstant.PAGE_NOT_EXISTS;
        }
        used = new int[DataStorageManagerConstant.MAX_PAGES];
        for (int i = 0; i < used.length; i++) {
            //used[i] = DataStorageManagerConstant.PAGE_NOT_USED;
            setUse(i, DataStorageManagerConstant.PAGE_NOT_USED);
        }
        numIOs = 0;
        numPages = 0;
        dataInMemory = new ArrayList<>();
    }

    @Override
    public void openFile(String filename){
        String filePath = this.getClass().getClassLoader()
                .getResource(filename).getPath();
        file = new File(filePath);
        log.info("打开文件:{}",file.getName());
    }

    @Override
    public void closeFile() throws IOException {

        log.info("关闭文件");
        writer = new BufferedWriter(new FileWriter(file), DataStorageManagerConstant.IO_BUFFER_SIZE);
        for(int i = 0; i < dataInMemory.size(); i++){
            writer.write(dataInMemory.get(i));
            writer.newLine();
        }
        writer.close();
        file = null;
    }

    @Override
    public BufferFrame readPage(int pageId) throws IOException {
        if(!pageExists(pageId)){
            //TODO 查询基页算作一次IO，存疑？
            //numIOs++;
            return null;
        }
        //根据页号从data.dbf中读取数据页，算作一次IO
        numIOs++;
        String lineContent = dataInMemory.get(pageId);
        BufferFrame bufferFrame = new BufferFrame();
        DataStorageManagerUtil.copyCharArray(lineContent.toCharArray(),
                bufferFrame.getField());
        bufferFrame.setNumChars(lineContent.length());
        return bufferFrame;

    }

    @Override
    public void writePage(int pageId, BufferFrame frm) throws IOException {
        numIOs++;
        if(!pageExists(pageId)){
            pages[pageId] = dataInMemory.size();
            incNumPages();
            dataInMemory.add(new String(frm.getField(), 0, frm.getNumChars()));
        }else{
            dataInMemory.set(pages[pageId], new String(frm.getField(), 0, frm.getNumChars()));
        }
        used[pageId] = DataStorageManagerConstant.PAGE_USED;
        numIOs++;

    }



    @Override
    public void seek(int offset, int pos) throws IOException {

    }

    @Override
    public File getFile() {
        return file;
    }

    @Override
    public void incNumPages() {
        numPages++;
    }

    @Override
    public int getNumPages() {
        return numPages;
    }

    @Override
    public void setUse(int pageId, int useBit) {
        used[pageId] = useBit;
    }

    @Override
    public int getUse(int pageId) {
        return used[pageId];
    }

    @Override
    public int getNumIOs() {
        return numIOs;
    }

    @Override
    public boolean pageExists(int pageId) {
        return pages[pageId] != DataStorageManagerConstant.PAGE_NOT_EXISTS;
    }



    @Override
    public void initialPages(List<Integer> pageIdList, List<BufferFrame> frmList) throws IOException {
//        writer = new BufferedWriter(new FileWriter(file), DataStorageManagerConstant.IO_BUFFER_SIZE);
        int totalChar = 0;
        for(int i = 0; i < pageIdList.size(); i++){
            int pageId = pageIdList.get(i);
            BufferFrame frm = frmList.get(i);
            totalChar = totalChar + frm.getNumChars();
            pages[pageId] = i;
            incNumPages();
            used[pageId] = DataStorageManagerConstant.PAGE_USED;
            dataInMemory.add(new String(frm.getField(), 0, frm.getNumChars()));
        }
    }
}
