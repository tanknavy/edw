package com.tanknavy.hive;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Author: Alex Cheng 6/17/2020 8:48 PM
 */
public class BaseFieldUDF extends UDF {
    // timestamp|{publicJson,[event json list]} 嵌套json对象

    public String evaluate(String line, String jsonKeysString){
        StringBuilder sb = new StringBuilder();//线程不安全
        //StringBuffer stringBuffer = new StringBuffer();//线程安全

        //1.获取所有key
        String[] jsonKeys = jsonKeysString.split(",");

        //2. timestamp|json
        String[] logContents = line.split("\\|"); //注意转义

        //3.校验
        if (logContents.length !=2  || StringUtils.isBlank(logContents[1])){
            return "";
        }

        //4.创建json对象
        try{
            // 用于公共字段
            JSONObject jsonObject = new JSONObject(logContents[1]);
            //公共部分的json对象
            JSONObject cmJson = jsonObject.getJSONObject("cm"); //common json fields
            
            //为了将json对象放入hive表中，
            for (int i = 0; i < jsonKeys.length; i++) {
                String jsonKey = jsonKeys[i].trim();

                if(cmJson.has(jsonKey)){
                    sb.append(cmJson.getString(jsonKey)).append("\t"); //字段值+tab分割
                }else {
                    sb.append("\t"); //字段值+tab分割
                }
                
            }

            //拼接事件字段(不同事件类型有不同栏位)
            sb.append(jsonObject.getString("et")).append("\t"); //event的值是一个list
            //事件时间
            sb.append(logContents[0]).append("\t");
            //事件类型再使用UDTF处理

        }catch (JSONException e){
            e.printStackTrace();
        }

        return sb.toString();

    }


    //测试UDF
    public static void main(String[] args) {
        String line = "1541217850324|{\"cm\":{\"mid\":\"m7856\",\"uid\":\"u8739\",\"ln\":\"-74.8\",\"sv\":\"V2.2.2\",\"os\":\"8.1.3\",\"g\":\"P7XC9126@gmail.com\",\"nw\":\"3G\",\"l\":\"es\",\"vc\":\"6\",\"hw\":\"640*960\",\"ar\":\"MX\",\"t\":\"1541204134250\",\"la\":\"-31.7\",\"md\":\"huawei-17\",\"vn\":\"1.1.2\",\"sr\":\"O\",\"ba\":\"Huawei\"},\"ap\":\"weather\",\"et\":[{\"ett\":\"1541146624055\",\"en\":\"display\",\"kv\":{\"goodsid\":\"n4195\",\"copyright\":\"ESPN\",\"content_provider\":\"CNN\",\"extend2\":\"5\",\"action\":\"2\",\"extend1\":\"2\",\"place\":\"3\",\"showtype\":\"2\",\"category\":\"72\",\"newstype\":\"5\"}},{\"ett\":\"1541213331817\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"15\",\"action\":\"3\",\"extend1\":\"\",\"type1\":\"\",\"type\":\"3\",\"loading_way\":\"1\"}},{\"ett\":\"1541126195645\",\"en\":\"ad\",\"kv\":{\"entry\":\"3\",\"show_style\":\"0\",\"action\":\"2\",\"detail\":\"325\",\"source\":\"4\",\"behavior\":\"2\",\"content\":\"1\",\"newstype\":\"5\"}},{\"ett\":\"1541202678812\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1541184614380\",\"action\":\"3\",\"type\":\"4\",\"content\":\"\"}},{\"ett\":\"1541194686688\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"3\"}}]}";
        String x = new BaseFieldUDF().evaluate(line, "mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t");
        System.out.println(x);
    }

}
