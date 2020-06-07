package com.tanknavy.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;

/**
 * Author: Alex Cheng 6/5/2020 9:46 PM
 */
public class LogUtils {

    //为了效率，flume阶段不做复杂判断
    public static boolean valuateStart(String log) {
        if(log == null){
            return false;
        }
        if(!log.trim().startsWith("{") || !log.trim().endsWith("}")){ //json格式{}开头结尾的字符串
            return false;
        }
        return true;
    }

    //
    public static boolean valuateEvent(String log) {
        if(log == null){
            return false;
        }

        //timestamp|{}
        String[] logContents = log.split("\\|");//|特殊字符表明二选一，\|正则，\\|转义处理

        if(logContents.length != 2){
            return false;
        }
        if(logContents[0].length() != 13 || !NumberUtils.isDigits(logContents[0])){ //服务器时间戳13位
            return false;
        }

        //判断json
        if(!logContents[1].trim().startsWith("{") || !logContents[1].trim().endsWith("}")){ //json格式{}开头结尾的字符串
            return false;
        }

        return true;
    }
}
