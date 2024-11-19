package com.zhuzheng.database2024fall_lab2;

import com.zhuzheng.database2024fall_lab2.constant.DataStorageManagerConstant;
import com.zhuzheng.database2024fall_lab2.memory.BufferManager;
import com.zhuzheng.database2024fall_lab2.memory.BufferManagerImpl;
import lombok.extern.log4j.Log4j;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
@Slf4j
@SpringBootApplication
public class Lab2Application {
    private static String traceFileName = "data-5w-50w-zipf.txt";
    private static void init(BufferManager bufferManager) throws IOException {
        File file = new File("C:\\Users\\wjsww\\Desktop\\database\\database2024fall_lab2-master\\target\\classes\\data.dbf");
        file.delete();
        file.createNewFile();
        bufferManager.initialPages();
    }
    private static List<String> read(String filename) throws IOException {
        String filePath = Lab2Application.class.getClassLoader()
                .getResource(filename).getPath();
        File file = new File(filePath);
        BufferedReader reader = new BufferedReader(new FileReader(file), DataStorageManagerConstant.IO_BUFFER_SIZE);
        String line = null;
        List<String> lineList = new ArrayList<>();
        while((line = reader.readLine()) != null){
            lineList.add(line);
        }
        return lineList;
    }
    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis(); // 开始时间
        ApplicationContext context = SpringApplication.run(Lab2Application.class, args);
        BufferManager bufferManager = context.getBean(BufferManager.class);
        List<String> traceList = null;
        init(bufferManager);
        traceList = read(traceFileName);
        for(int i = 0; i < traceList.size(); i++){
            String traceLine = traceList.get(i);
            int index = traceLine.indexOf(",");
            Integer operator = Integer.parseInt(traceLine.substring(0, index));
            Integer pageId = Integer.parseInt(traceLine.substring(index + 1)) - 1;
            log.info("开始处理第{}条trace:{}", i + 1, traceLine);
            if(operator == 0){
                //0代表read
                bufferManager.read(pageId);
            }else if(operator == 1){
                //1代表write
                bufferManager.write(pageId);
            }
        }
        bufferManager.writeDirtys();
        bufferManager.close();
        long end = System.currentTimeMillis(); // 结束时间
        long elapsedMillis = end - start; // 计算耗时

        // 转换为分钟和秒
        long seconds = (elapsedMillis / 1000) % 60;
        long minutes = (elapsedMillis / 1000) / 60;
        log.info("运行时间: {} 分 {} 秒", minutes, seconds);
        log.info("扫描完该trace文件后，一共进行了{}次IO", bufferManager.numIOs());
    }

}
