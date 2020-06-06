package com.tanknavy.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex Cheng 6/5/2020 9:01 PM
 * flume日志ETL两种json类型，
 * 1）event log格式 : timestamp|{嵌套json}
 * 2) start log格式: 单一json{}
 */


public class LogETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) { //每个事件处理结果
        //1)获取数据
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));

        //2)校验，启动日志(json， k/v:(en:start)类型)，事件日志(服务器时间|json)
        if (log.contains("start")){
            if(LogUtils.valuateStart(log)){
                return event;
            }
        }else {
            if(LogUtils.valuateEvent(log)){
                return event;
            }
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) { //事件结果集合
        ArrayList<Event> interceptors = new ArrayList<Event>();
        for (Event event : events) {
            Event intercept = intercept(event);
            if(intercept !=null) {
                interceptors.add(intercept);
            }
        }
        return interceptors;
    }

    @Override
    public void close() {

    }

    //记得静态内部类，flume配置时通过静态builder来调用这个对象，LogETLInterceptor$Builder
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }


}
