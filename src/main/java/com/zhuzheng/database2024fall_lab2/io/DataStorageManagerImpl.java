package com.zhuzheng.database2024fall_lab2.io;

import com.zhuzheng.database2024fall_lab2.constant.DataStorageManagerConstant;
import com.zhuzheng.database2024fall_lab2.memory.BufferFrame;
import com.zhuzheng.database2024fall_lab2.util.DataStorageManagerUtil;
import lombok.Data;
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
public class DataStorageManagerImpl implements DataStorageManager {
    private BufferedReader reader;
    private BufferedWriter writer;
    private File file;
    private int numPages;
    private int numIOs;
    /**
     * int数组pages代表基页，Index代表数据页的页号
     * pages[index]中记录了第index + 1页中的数据被存储在了data.dbf文件的第几行
     */
    private int[] pages;
    private int[] used;
    public DataStorageManagerImpl() {
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
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        closeFile();
    }

    @Override
    public int openFile(String filename){
        String filePath = this.getClass().getClassLoader()
                .getResource(filename).getPath();
        file = new File(filePath);
        log.info("打开文件:{}",file.getName());
        return 0;
    }

    @Override
    public int closeFile() {
        file = null;
        log.info("关闭文件");
        return 0;
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
        reader = new BufferedReader(new FileReader(file), DataStorageManagerConstant.IO_BUFFER_SIZE);
        //定位数据页在data.dbf文件中的前一行，方便读取
        seek(pages[pageId], DataStorageManagerConstant.POS_START);

        String lineContent = null;
        lineContent = reader.readLine();
        reader.close();
        BufferFrame bufferFrame = new BufferFrame();
        DataStorageManagerUtil.copyCharArray(lineContent.toCharArray(),
                bufferFrame.getField());
        bufferFrame.setNumChars(lineContent.length());
        return bufferFrame;

    }

    @Override
    public void writePage(int pageId, BufferFrame frm) throws IOException {
        reader = new BufferedReader(new FileReader(file), DataStorageManagerConstant.IO_BUFFER_SIZE);
        numIOs++;
        List<String> lineList = DataStorageManagerUtil.readAll(reader);
        if(!pageExists(pageId)){
            pages[pageId] = lineList.size();
            incNumPages();
            lineList.add(new String(frm.getField(), 0, frm.getNumChars()));
        }else{
            lineList.set(pages[pageId], new String(frm.getField(), 0, frm.getNumChars()));
        }
        used[pageId] = DataStorageManagerConstant.PAGE_USED;

        writer = new BufferedWriter(new FileWriter(file), DataStorageManagerConstant.IO_BUFFER_SIZE);
        numIOs++;
        for(String line : lineList){
            writer.write(line);
            writer.newLine();
        }
        writer.close();
        reader.close();
    }

    @Override
    public void seek(int offset, int pos) throws IOException {
        int targetLine = offset + pos;
        int currentLine = 0;
        if(currentLine == targetLine){
            return;
        }
        String lineContent = null;

        while((lineContent = reader.readLine()) != null){
            currentLine++;
            if(currentLine == targetLine){
                return;
            }
        }
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
    public int initialPages(List<Integer> pageIdList, List<BufferFrame> frmList) {
        try {
           writer = new BufferedWriter(new FileWriter(file), DataStorageManagerConstant.IO_BUFFER_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
            return DataStorageManagerConstant.WRITE_FAILED;
        }
        List<String> lineList = new ArrayList<>();
        int totalChar = 0;
        for(int i = 0; i < pageIdList.size(); i++){
            int pageId = pageIdList.get(i);
            BufferFrame frm = frmList.get(i);
            totalChar = totalChar + frm.getNumChars();
            pages[pageId] = i;
            incNumPages();
            used[pageId] = DataStorageManagerConstant.PAGE_USED;
            lineList.add(new String(frm.getField(), 0, frm.getNumChars()));
        }
        for(String line : lineList){
            try {
                writer.write(line);
                writer.newLine();
            } catch (IOException e) {
                e.printStackTrace();
                return DataStorageManagerConstant.WRITE_FAILED;
            }
        }
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return DataStorageManagerConstant.SIZE_OF_CHAR * totalChar;
    }
}
