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
@Component
@Slf4j
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
//        try {
//            reader = new BufferedReader(new FileReader(file), DataStorageManagerConstant.IO_BUFFER_SIZE);
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
        log.info("打开文件:{}",file.getName());
        return 0;
    }

    @Override
    public int closeFile() {
//        try {
//            reader.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        file = null;
        log.info("关闭文件");
        return 0;
    }

    @Override
    public BufferFrame readPage(int pageId) {
        if(!pageExists(pageId)){
            // TODO 查询基页算作一次IO，存疑？
            //numIOs++;
            return null;
        }
        //根据页号从data.dbf中读取数据页，算作一次IO
        numIOs++;
        try {
            reader = new BufferedReader(new FileReader(file), DataStorageManagerConstant.IO_BUFFER_SIZE);
            //reader.mark(DataStorageManagerConstant.IO_BUFFER_SIZE);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        //定位数据页在data.dbf文件中的前一行，方便读取
        int seek = seek(pages[pageId], DataStorageManagerConstant.POS_START);
        if(seek == DataStorageManagerConstant.LINE_FIND_IN_FILE){
            String lineContent = null;
            try {
                lineContent = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
            try {
                reader.close();
                //reader.reset();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
            BufferFrame bufferFrame = new BufferFrame();
            DataStorageManagerUtil.copyCharArray(lineContent.toCharArray(),
                    bufferFrame.getField());
            bufferFrame.setNumChars(lineContent.length());

            return bufferFrame;
        }
        return null;
    }

    @Override
    public int writePage(int pageId, BufferFrame frm) {
        try {
            reader = new BufferedReader(new FileReader(file), DataStorageManagerConstant.IO_BUFFER_SIZE);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return DataStorageManagerConstant.WRITE_FAILED;
        }
//        try {
//            reader.mark(DataStorageManagerConstant.IO_BUFFER_SIZE);
//        } catch (IOException e) {
//            e.printStackTrace();
//            return DataStorageManagerConstant.WRITE_FAILED;
//        }
        List<String> lineList = null;
        try {
            numIOs++;
            lineList = DataStorageManagerUtil.readAll(reader);
            //reader.reset();
        } catch (IOException e) {
            e.printStackTrace();
            return DataStorageManagerConstant.WRITE_FAILED;
        }
        if(!pageExists(pageId)){
            pages[pageId] = lineList.size();
            incNumPages();
            lineList.add(new String(frm.getField(), 0, frm.getNumChars()));
        }else{
            lineList.set(pages[pageId], new String(frm.getField(), 0, frm.getNumChars()));
        }
        used[pageId] = DataStorageManagerConstant.PAGE_USED;
        try {
            writer = new BufferedWriter(new FileWriter(file), DataStorageManagerConstant.IO_BUFFER_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
            return DataStorageManagerConstant.WRITE_FAILED;
        }
        try {
            numIOs++;
            for(String line : lineList){
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return DataStorageManagerConstant.WRITE_FAILED;
        }
        try {
            writer.close();
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return DataStorageManagerConstant.SIZE_OF_CHAR * frm.getNumChars();
    }

    @Override
    public int seek(int offset, int pos) {
        int targetLine = offset + pos;
        int currentLine = 0;
        if(currentLine == targetLine){
            return DataStorageManagerConstant.LINE_FIND_IN_FILE;
        }
        String lineContent = null;
        try {
            while((lineContent = reader.readLine()) != null){
                currentLine++;
                if(currentLine == targetLine){
                    return DataStorageManagerConstant.LINE_FIND_IN_FILE;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return DataStorageManagerConstant.LINE_NOT_FIND_IN_FILE;
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
