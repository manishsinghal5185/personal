package com.capgemini.commons;
import com.capgemini.ingestionServiceRunner;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class auditOrchestrator {
    protected static final Logger log= LoggerFactory.getLogger(auditOrchestrator.class);
    public static Response postToAuditApi(String jsonPayload,String auditApiUrl,String authString){
        Client client=ClientBuilder.newClient();
        Entity<String> jsonEntity=Entity.json(jsonPayload);
        Response response=client.target(auditApiUrl)
                .request()
                .header("Content-type",MediaType.APPLICATION_JSON)
                .post(jsonEntity);
        log.info("API Response:{}",response.toString());
        return response;
    }
    public static String buildJsonFromAttrubutes(HashMap<String,String> payloadAttribute){
        if (!payloadAttribute.equals(null) && payloadAttribute.size()>0){
            JSONObject jsonObject=new JSONObject(payloadAttribute);
            return "["+jsonObject.toString()+"]";
        }
        return null;
    }
    public static HashMap<String,String> keyValueArrayToHashMap(String[] key, String[] value ){
        HashMap<String,String> hashMap=new HashMap<String,String>();
        if (key.length>0 && value.length>0){
            if (key.length==value.length){
                for (int i=0;i<key.length;i++){
                    hashMap.put(key[i],value[i]);
                }
            }
            else {
                log.error("Key Array Length is not equal to value array length");
                System.exit(1);
            }
        }else {
            log.error("Key Array Length is not equal to value array length");
            System.exit(1);
        }
        return hashMap;
    }
}
