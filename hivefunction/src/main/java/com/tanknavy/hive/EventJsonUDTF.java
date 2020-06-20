package com.tanknavy.hive;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: Alex Cheng 6/17/2020 10:41 PM
 */
public class EventJsonUDTF extends GenericUDTF {

    //initialize方法已经过时，在hive 2.x中用新的，用于返回需要UDTF输出的字段fieldNames和字段类型fieldTypes
    //@Deprecated //这样使用会报错，使用Override重写
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

        //https://www.jianshu.com/p/772bead323d0
        if(argOIs.length != 1){
            throw new UDFArgumentException("ExplodeStringUDTF takes exactly one argument.");
        }
        if(argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector)argOIs[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING){
            throw new UDFArgumentTypeException(0, "ExplodeStringUDTF takes a string as a parameter.");
        }

        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldTypes = new ArrayList<>();
        fieldNames.add("event_name");
        fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("event_json");
        fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldTypes);
    }

    //可以不用写，和父类一模一样
//    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
//        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();
//        ObjectInspector[] udtfInputOIs = new ObjectInspector[inputFields.size()];
//
//        for(int i = 0; i < inputFields.size(); ++i) {
//            udtfInputOIs[i] = ((StructField)inputFields.get(i)).getFieldObjectInspector();
//        }
//
//        return this.initialize(udtfInputOIs);
//    }


    @Override //支持一进一出和多进多出，输入一条/多条记录，返回多条记录
    public void process(Object[] objects) throws HiveException {
        //获取输入数据
        String input = objects[0].toString(); //一进多出，取第一个元素
        if(StringUtils.isBlank(input)){
            return;
        }else {
            try{
                JSONArray ja = new JSONArray(input); //输入的是一个json数组
                
                if(null == ja){
                    return;
                }
                for (int i = 0; i < ja.length(); i++){
                    String[] results = new String[2]; //json串的名称和kv元素

                    //取值时使用getString,还需进一步处理使用getJSONObject，ctrl+alt+t来surround with
                    try {
                        //容易抛出异常
                        results[0] = ja.getJSONObject(i).getString("en"); //事件名称
                        results[1] = ja.getString(i); //第i个对象，整个事件对象
                    } catch (JSONException e) {
                        e.printStackTrace();

                        continue;//如果抛出异常，还是继续处理下一条记录
                    }

                    forward(results);//写出去
                }

            }catch (JSONException e){
                e.printStackTrace();
            }

        }
    }

    @Override
    public void close() throws HiveException {

    }
}
