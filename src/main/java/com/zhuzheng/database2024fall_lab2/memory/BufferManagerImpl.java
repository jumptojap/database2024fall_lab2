package com.zhuzheng.database2024fall_lab2.memory;

import com.zhuzheng.database2024fall_lab2.constant.BufferControlBlockConstant;
import com.zhuzheng.database2024fall_lab2.constant.BufferFrameConstant;
import com.zhuzheng.database2024fall_lab2.constant.BufferManagerConstant;
import com.zhuzheng.database2024fall_lab2.constant.DataStorageManagerConstant;
import com.zhuzheng.database2024fall_lab2.io.DataStorageManager;
import com.zhuzheng.database2024fall_lab2.util.BufferFrameUtil;
import com.zhuzheng.database2024fall_lab2.util.DataStorageManagerUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ClassName: BufferManagerImpl
 * Package: com.zhuzheng.database2024fall_lab2.memory
 * Description:
 *
 * @Author: east_moon
 * @Create: 2024/11/18 - 19:38
 * Version: v1.0
 */
@Component
@Slf4j
public class BufferManagerImpl implements BufferManager{
    @Autowired
    private DataStorageManager dataStorageManager;
    //frame列表，容量为1024，可以通过frameId从列表中取出对应frame
    private BufferFrame[] bufferFrames;
    //frameId转换到与之对应的pageId，如果ftop[frameId] = null 代表该frame未被使用
    private Integer[] ftop;
    //根据pageId访问BCB的hashMap
    private Map<Integer, BufferControlBlock> map;
    //BCB双向链表的头节点，无具体值，方便统一插入删除
    private BufferControlBlock head;
    //BCB双向链表的尾节点，无具体值，方便统一插入删除
    private BufferControlBlock tail;
    //BCB最大上限是1024，size记录了其实际个数
    private int size;
    public BufferManagerImpl(){
        bufferFrames = new BufferFrame[BufferManagerConstant.BUFFER_SIZE];
        for(int i = 0; i < bufferFrames.length; i++){
            bufferFrames[i] = new BufferFrame();
        }
        ftop = new Integer[BufferManagerConstant.BUFFER_SIZE];
        for(int i = 0; i < ftop.length; i++){
            ftop[i] = null;
        }
        map = new HashMap<>();
        head = new BufferControlBlock();
        tail = new BufferControlBlock();
        head.next = tail;
        tail.prev = head;
        size = 0;
    }
    public BufferManagerImpl(DataStorageManager dataStorageManager){
        this();
        this.dataStorageManager = dataStorageManager;
    }

    @Override
    public int numIOs() {
        return dataStorageManager.getNumIOs();
    }

    @Override
    public void removeBCB(BufferControlBlock bufferControlBlock) {
        if(bufferControlBlock.prev != null)
            bufferControlBlock.prev.next = bufferControlBlock.next;
        if(bufferControlBlock.next != null){
            bufferControlBlock.next.prev = bufferControlBlock.prev;
        }
        bufferControlBlock.prev = null;
        bufferControlBlock.next = null;
    }

    @Override
    public void addBCBToHead(BufferControlBlock bufferControlBlock) {
        bufferControlBlock.next = head.next;
        head.next.prev = bufferControlBlock;
        bufferControlBlock.prev = head;
        head.next = bufferControlBlock;
    }

    @Override
    public void setDirty(BufferControlBlock bufferControlBlock) {
        bufferControlBlock.setDirty(BufferControlBlockConstant.DIRTY);
    }

    @Override
    public void unSetDirty(BufferControlBlock bufferControlBlock) {

    }

    @Override
    public void writeDirtys() throws IOException {
        for(BufferControlBlock bufferControlBlock = head.next;
        bufferControlBlock != tail; bufferControlBlock = bufferControlBlock.next){
            if(bufferControlBlock.getDirty() == BufferControlBlockConstant.DIRTY){
                int pageId = bufferControlBlock.getPageId();
                int framId = bufferControlBlock.getFrameId();
                dataStorageManager.writePage(pageId, bufferFrames[framId]);
            }else{
                //log.info("page{}是干净的",bufferControlBlock.getPageId());
            }
        }
    }

    @Override
    public void writeDity(int pageId) throws IOException {
        BufferControlBlock bufferControlBlock = map.get(pageId);
        Integer frameId = bufferControlBlock.getFrameId();
        BufferFrame bufferFrame = bufferFrames[frameId];
        if(bufferControlBlock.getDirty() == BufferControlBlockConstant.DIRTY){
            dataStorageManager.writePage(pageId, bufferFrame);
        }
    }

    @Override
    public String read(int pageId) throws IOException {
        if(map.containsKey(pageId)){
            BufferControlBlock bufferControlBlock = map.get(pageId);
            int frameId = bufferControlBlock.getFrameId();
            //调整该BCB在双向链表中的位置
            removeBCB(bufferControlBlock);
            addBCBToHead(bufferControlBlock);
            return new String(bufferFrames[frameId].getField(), 0,
                    bufferFrames[frameId].getNumChars());
        }else{
            //如果没有空闲的frame，需要通过LRU算法淘汰一个与frame绑定的page
            if(numFreeFrames() == 0){
                int deletePageId = tail.prev.getPageId();
                unFixPage(deletePageId);
            }
            //建立起page和frame的对应关系，因为是写入操作，脏位不需要调整
            BufferControlBlock bufferControlBlock = fixPage(pageId);
            BufferFrame targetFrame = bufferFrames[bufferControlBlock.getFrameId()];
            BufferFrame sourceFrame = dataStorageManager.readPage(pageId);
            BufferFrameUtil.copyBufferFrame(sourceFrame, targetFrame);
            return new String(targetFrame.getField(), 0,
                    targetFrame.getNumChars());
        }
    }

    @Override
    public void write(int pageId) throws IOException {
        if(map.containsKey(pageId)){
            BufferControlBlock bufferControlBlock = map.get(pageId);
            int frameId = bufferControlBlock.getFrameId();
            //调整该BCB在双向链表中的位置
            removeBCB(bufferControlBlock);
            addBCBToHead(bufferControlBlock);
            BufferFrame bufferFrame = bufferFrames[frameId];
            String content = new String(bufferFrame.getField(), 0, bufferFrame.getNumChars());
            int index = content.lastIndexOf("n");
            content = content.substring(0,index + 1) +
                    (Integer.parseInt(content.substring(index + 1)) + 1);
            DataStorageManagerUtil.copyCharArray(content.toCharArray(), bufferFrame.getField());
            bufferFrame.setNumChars(content.length());
            setDirty(bufferControlBlock);
        }else{
            if(numFreeFrames() == 0){
                int deletePageId = tail.prev.getPageId();
                unFixPage(deletePageId);
            }
            BufferControlBlock bufferControlBlock = fixPage(pageId);
            BufferFrame targetFrame = bufferFrames[bufferControlBlock.getFrameId()];
            //根据实验要求，可以把写入内容定死
            String pageContent = "The content of page" + (pageId + 1) + " is version0";
            BufferFrame sourceFrame = BufferFrame.builder()
                    .field(pageContent.toCharArray())
                    .numChars(pageContent.length())
                    .build();
            BufferFrameUtil.copyBufferFrame(sourceFrame, targetFrame);
            setDirty(bufferControlBlock);
        }
    }

    @Override
    public int numFreeFrames() {
        return BufferManagerConstant.BUFFER_SIZE - size;
    }

    @Override
    public BufferControlBlock fixPage(int pageId) {
        //根据hash函数寻找与pageId对应的frameId
        int frameId = hash(pageId);
        BufferFrame bufferFrame = bufferFrames[frameId];
        BufferControlBlock bufferControlBlock = BufferControlBlock.builder()
                .frameId(frameId)
                .pageId(pageId)
                .build();
        //插入BCB双线链表链头
        addBCBToHead(bufferControlBlock);
        map.put(pageId, bufferControlBlock);
        ftop[frameId] = pageId;
        //page和frame的对应关系建立好以后，BCB的实际个数加一
        size++;
        return bufferControlBlock;
    }

    @Override
    public void unFixPage(int pageId) throws IOException {
        BufferControlBlock bufferControlBlock = map.get(pageId);
        Integer frameId = bufferControlBlock.getFrameId();
        BufferFrame bufferFrame = bufferFrames[frameId];
        ftop[frameId] = null;
        removeBCB(bufferControlBlock);
        //page和frame的对应关系解除以后，BCB的实际个数减一
        size--;
        writeDity(pageId);
        map.remove(pageId);
        //保证frame的干净
        bufferFrame.clean();
    }

    @Override
    public int hash(int pageId) {
        //采用最为简单的哈希函数
        int frameId = pageId % BufferManagerConstant.BUFFER_SIZE;
        //产生哈希冲突后采用线性探测法
        while(ftop[frameId] != null){
            frameId = (frameId + 1) % BufferManagerConstant.BUFFER_SIZE;
        }
        return frameId;
    }

    @Override
    public void initialPages() {
        List<Integer> pageIdList = new ArrayList<>();
        List<BufferFrame> frmList = new ArrayList<>();

        for(int i = 0; i < DataStorageManagerConstant.MAX_PAGES; i++) {
            int pageId = i;
            BufferFrame bufferFrame = new BufferFrame();
            String pageContext = "The content of page" + (pageId + 1) + " is version0";
            DataStorageManagerUtil.copyCharArray(pageContext.toCharArray(), bufferFrame.getField());
            bufferFrame.setNumChars(pageContext.length());
            pageIdList.add(pageId);
            frmList.add(bufferFrame);
        }
        dataStorageManager.initialPages(pageIdList, frmList);
    }
}
