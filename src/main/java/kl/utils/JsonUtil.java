package kl.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: lixz
 * @Date: 2021/12/09/14:30
 * @Description: json解析工具
 */
public class JsonUtil {


    public static void main(String[] args) throws Exception {

        String str = "{\"meta_data\":[{\"name\":{\"string\":\"INFA_SEQUENCE\"},\"value\":{\"string\":\"2,PWX_GENERIC,1,,2,3,D4083BF4A3B7DC000000000000083BF4A3B77100000073000263C1001466870038000100000000000100000000,00,000001EB162AF7A9\"},\"type\":null},{\"name\":{\"string\":\"INFA_TABLE_NAME\"},\"value\":{\"string\":\"d8oracapt.lpedorit_LPEDORITEM\"},\"type\":null},{\"name\":{\"string\":\"INFA_OP_TYPE\"},\"value\":{\"string\":\"INSERT_EVENT\"},\"type\":null},{\"name\":{\"string\":\"DTL__CAPXRESTART1\"},\"value\":{\"string\":\"1Ag79KO33AAAAAAAAAg79KO3cQAAAHMAAmPBABRmhwA4AAEAAAAAAAEAAAAA\"},\"type\":null},{\"name\":{\"string\":\"DTL__CAPXRESTART2\"},\"value\":{\"string\":\"AAAB6xYq96k=\"},\"type\":null},{\"name\":{\"string\":\"DTL__CAPXUOW\"},\"value\":{\"string\":\"MFgwMDA0LjAwNy4wMDAwRkFFQw==\"},\"type\":null},{\"name\":{\"string\":\"DTL__CAPXUSER\"},\"value\":{\"string\":\"GGS\"},\"type\":null},{\"name\":{\"string\":\"DTL__CAPXTIMESTAMP\"},\"value\":{\"string\":\"202109200901280000000000\"},\"type\":null},{\"name\":{\"string\":\"DTL__CAPXACTION\"},\"value\":{\"string\":\"I\"},\"type\":null},{\"name\":{\"string\":\"DTL__CAPXROWID\"},\"value\":null,\"type\":null}],\"columns\":{\"array\":[{\"name\":{\"string\":\"EDORACCEPTNO\"},\"value\":{\"string\":\"20210920000034\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":{\"string\":\"110\"},\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"EDORNO\"},\"value\":{\"string\":\"20210920000034\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"EDORAPPNO\"},\"value\":{\"string\":\"20210920000034\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"EDORTYPE\"},\"value\":{\"string\":\"ZB\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"DISPLAYTYPE\"},\"value\":{\"string\":\"1\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"GRPCONTNO\"},\"value\":{\"string\":\"00000000000000000000\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"CONTNO\"},\"value\":{\"string\":\"P2019310000097373\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"INSUREDNO\"},\"value\":{\"string\":\"000000\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"POLNO\"},\"value\":{\"string\":\"21014600193\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"MANAGECOM\"},\"value\":{\"string\":\"86310000\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"EDORVALIDATE\"},\"value\":{\"string\":\"202109200000000000000000\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"EDORAPPDATE\"},\"value\":{\"string\":\"202109200000000000000000\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"EDORSTATE\"},\"value\":{\"string\":\"1\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"UWFLAG\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"UWOPERATOR\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"UWDATE\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"UWTIME\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"CHGPREM\"},\"value\":{\"string\":\"0.00\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"CHGAMNT\"},\"value\":{\"string\":\"0.00\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"GETMONEY\"},\"value\":{\"string\":\"0.00\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"GETINTEREST\"},\"value\":{\"string\":\"0.00\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"OPERATOR\"},\"value\":{\"string\":\"001\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"MAKEDATE\"},\"value\":{\"string\":\"202109200000000000000000\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"MAKETIME\"},\"value\":{\"string\":\"09:01:25\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"MODIFYDATE\"},\"value\":{\"string\":\"202109200000000000000000\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"MODIFYTIME\"},\"value\":{\"string\":\"09:01:25\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"EDORREASONNO\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"EDORREASON\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"REASON\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"REASONCODE\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"APPROVEGRADE\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"APPROVESTATE\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"APPROVEOPERATOR\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"APPROVEDATE\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}},{\"name\":{\"string\":\"APPROVETIME\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":false}}]}}";

        //打印
        System.out.println(ParseCDCJson(str));
    }

    /**
     *Informatic CDC JSON串转换为Map+ArrayList结构
     * 注：data中的每个字段值为数组类型，该数组的第一个元素为新值，第二个为旧值
     * 转换后结构示例：
     * {
     *     meta_data:{INFA_TABLE_NAME:lccont_LCCONT,INFA_OP_TYPE:UPDATE_EVENT},
     *     columns:{GRPCONTNO:[000,111],CONTNO:[P2021,P2021],PRTNO:[新值,旧值]}
     * }
     * @param jsonStr Informatic CDC JSON串
     * @return
     * @exception Exception 解析异常直接抛出，如有异常发生，说明该条数据不可用
     */
    public static HashMap<String,Object> ParseCDCJson(String jsonStr) throws Exception{
        //结果容器
        HashMap<String,Object> res = new HashMap<String,Object>();
        HashMap<String,String> meta_res = new HashMap<String,String>();
        HashMap<String,ArrayList<String>> data_res = new HashMap<String,ArrayList<String>>();
        //转换成JSON对象
        JSONObject json = JSON.parseObject(jsonStr);
//        json = json.getJSONObject("message");//logstash转发用
        /**
         * 解析元数据meta_data
         */
        JSONArray meta_data = (JSONArray)json.get("meta_data");
        Iterator meta_data_it = meta_data.iterator();
        while(meta_data_it.hasNext()){
            JSONObject meta_data_one = (JSONObject)meta_data_it.next();
            JSONObject name = (JSONObject)meta_data_one.get("name");
            JSONObject value = (JSONObject)meta_data_one.get("value");
            String nv = null;
            String vv = null;
            if(name!=null){
                nv = name.getString("string");
            }
            if(value!=null){
                vv = value.getString("string");
            }
            meta_res.put(nv,vv);
        }
        /**
         * 解析数据columns
         */
        JSONObject columns = (JSONObject)json.get("columns");
        JSONArray array = columns.getJSONArray("array");
        Iterator<Object> array_it = array.iterator();
        while(array_it.hasNext()){
            JSONObject array_one = (JSONObject)array_it.next();
            JSONObject name = (JSONObject)array_one.get("name");
            JSONObject value = (JSONObject)array_one.get("value");
            JSONObject beforeImage = (JSONObject)array_one.get("beforeImage");
            ArrayList<String> v = new ArrayList<String>();
            if(value!=null){
                v.add(value.getString("string"));
            }else{
                v.add(null);
            }
            if(beforeImage!=null){
                v.add(beforeImage.getString("string"));
            }else{
                v.add(null);
            }
            data_res.put(name.getString("string"),v);
        }
        //数据放入容器
        res.put("meta_data",meta_res);
        res.put("columns",data_res);
        return res;
    }
}
