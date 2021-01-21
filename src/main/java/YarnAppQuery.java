import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import util.HttpService;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class YarnAppQuery{
    String url;
    String applicationId;
    Map<String, String> args = new HashMap<>();

    public static YarnAppQuery builder(String host, String port){
        return new YarnAppQuery(host, port);
    }
    public YarnAppQuery(String host, String port){
        url= host+":"+port+"/ws/v1/cluster/apps";
    }


    public YarnAppQuery setApplicationId(String applicationId) {
        this.applicationId = applicationId;
        return this;
    }

    public YarnAppQuery setLimit(int limit){
        args.put("limit", Integer.toString(limit));
        return this;
    }
    public YarnAppQuery setUser(String user){
        args.put("user", user);
        return this;
    }
    public YarnAppQuery setStates(String states){
        args.put("states", states);
        return this;
    }
    public YarnAppQuery setApplicationTypes(String applicationTypes){
        args.put("applicationTypes", applicationTypes);
        return this;
    }
    public YarnAppQuery setStartedTimeBegin(Long startedTimeBegin){
        args.put("startedTimeBegin", Long.toString(startedTimeBegin));
        return this;
    }
    public YarnAppQuery setStartedTimeEnd(Long startedTimeEnd){
        args.put("startedTimeEnd", Long.toString(startedTimeEnd));
        return this;
    }
    public YarnAppQuery setFinishedTimeBegin(Long finishedTimeBegin){
        args.put("finishedTimeBegin", Long.toString(finishedTimeBegin));
        return this;
    }
    public YarnAppQuery setFinishedTimeEnd(Long finishedTimeEnd){
        args.put("finishedTimeEnd", Long.toString(finishedTimeEnd));
        return this;
    }
    public YarnAppQuery setQueue(String queue){
        args.put("queue", queue);
        return this;
    }
    public List<entities.YarnApp> get(){
        String queryUrl = url;
        String parameters = "";
        for(Map.Entry<String, String> entry : args.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            parameters = parameters +key+"="+value+ "&";
        }
        if(applicationId!=null&&applicationId.length()>0){
            queryUrl = queryUrl+"/"+applicationId;
        }
        if(parameters.length()>0){
            queryUrl = queryUrl + "?"+parameters;
        }

        List<entities.YarnApp> list = new ArrayList<>();
        String strResponse = "";
        HashMap<String,String> requestMap = new HashMap<>();
        requestMap.put("content-type","application/json");
        try {
            HttpURLConnection con = HttpService.getConnection( HttpService.HttpMethod.GET, queryUrl, requestMap, null);
            int statusCode = con.getResponseCode();
            strResponse = HttpService.inputStreamToString(con.getInputStream());
            if(statusCode != 200){
                throw new Exception(strResponse);
            }
            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject)parser.parse(strResponse);
            JSONObject jsonAppsObject = (JSONObject)json.get("apps");
            if(jsonAppsObject==null){
                ObjectMapper mapper = new ObjectMapper(new JsonFactory());
                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                entities.YarnApp app = mapper.readValue((json.get("app")).toString(), entities.YarnApp.class);
                list.add(app);
                return list;
            }

            JSONArray jsonAppObject = (JSONArray)jsonAppsObject.get("app");
            for (int i =0;i<jsonAppObject.size();i++ ){

                ObjectMapper mapper = new ObjectMapper(new JsonFactory());
                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                entities.YarnApp app = mapper.readValue((jsonAppObject.get(i)).toString(), entities.YarnApp.class);
                list.add(app);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return list;
    }

    public boolean kill(){
        try {
            if(applicationId==null){
                throw new Exception("ApplicationId not set. To kill an application with YarnAppQuery, applicationId needs to be set using setApplicationId");
            }
            String queryUrl = url+"/"+applicationId+"/state";
            HashMap<String,String> requestMap = new HashMap<>();
            requestMap.put("content-type","application/json");
            HttpURLConnection con = HttpService.getConnection( HttpService.HttpMethod.PUT, queryUrl, requestMap, "{\"state\": \"KILLED\"}");
            int statusCode = con.getResponseCode();

            String strResponse = HttpService.inputStreamToString(con.getInputStream());
            if(statusCode != 200){
                throw new Exception(strResponse);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return false;
        }
        return true;
    }
}
