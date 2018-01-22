import com.google.gson.Gson;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class BuildEvent extends RichMapFunction<String,Tuple4<String,LocalDateTime,Integer,Float>> {
    @Override
    public Tuple4<String, LocalDateTime, Integer, Float> map(String jsonString) throws Exception {
        Gson gson = new Gson();
        Map<String, String> map = new HashMap<String, String>();
        Map<String, String> myMap = gson.fromJson(jsonString, map.getClass());

        DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        LocalDateTime timestamp = LocalDateTime.from(f.parse(myMap.get("time")));
        String device_id = myMap.get("device_id");
        Integer temp = Integer.parseInt(myMap.get("temp"));
        Float kws = Float.parseFloat(myMap.get("kws"));

        return new Tuple4<>(device_id, timestamp, temp, kws);
    }
}
