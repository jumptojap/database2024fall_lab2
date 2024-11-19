package com.zhuzheng.database2024fall_lab2.constant;

/**
 * ClassName: DataStorageManagerConstant
 * Package: com.zhuzheng.database2024fall_lab2.constant
 * Description:
 *
 * @Author: east_moon
 * @Create: 2024/11/17 - 18:39
 * Version: v1.0
 */
public class DataStorageManagerConstant {
    public static final String FILE_NAME = "data.dbf";
    public static final int MAX_PAGES = 50000;
    public static final int PAGE_NOT_EXISTS = -1;
    public static final int PAGE_NOT_USED = 0;
    public static final int PAGE_USED = 1;
    public static final int POS_START = 0;
    public static final int LINE_NOT_FIND_IN_FILE = -1;
    public static final int LINE_FIND_IN_FILE = 0;
    public static final int WRITE_FAILED = -1;
    public static final int SIZE_OF_CHAR = 2;
    public static final int IO_BUFFER_SIZE = 2 * 1024 * 1024;
}
