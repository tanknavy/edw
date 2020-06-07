package com.tanknavy.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Author: Alex Cheng 6/6/2020 9:54 AM
 * 分类型拦截器
 * a1.sources.r1.selector.header = topic
 * a1.channels.c1.kafka.topic = topic_start
 * a1.channels.c1.kafka.topic = topic_event
 *
 */
public class LogTypeInterceptor implements Interceptor {
    @Override
    public void initialize() {
        
    }

    @Override
    public Event intercept(Event event) {
        // input json: start/event， body数据不做改变，在header里面做标记
        //1) get body
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));

        //2) 获取headers
        Map<String, String> headers = event.getHeaders();

        //3) 向header中添加值,//flume里面配置kafka作为channel & sink
        if(log.contains("start")){
            headers.put("topic", "topic_start");
        }else{
            headers.put("topic", "topic_event");
        }

        //body数据不需要任何修改，只是在headers里面添加k/v作为标识
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        ArrayList<Event> interceptors = new ArrayList<>();
        for (Event event : events) {
            Event intercept = intercept(event);
            interceptors.add(intercept);
        }
        return interceptors;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

}
