import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import com.google.gson.Gson;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class sensorStream {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "group1");

        // create a stream of sensor readings
        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer08<>("device_activity_stream", new SimpleStringSchema(), properties).setStartFromEarliest());
        DataStream<Tuple4<String,LocalDateTime,Integer,Float>> eventStream = messageStream.map(new BuildEvent());

        // messageStream.filter((FilterFunction) jsonNode -> ( jsonNode.get("temp").asInt() >= 0)).writeAsText("out.txt").setParallelism(1);
        eventStream.map((MapFunction) event -> event.getClass().toString()+"$").writeAsText("out.txt").setParallelism(1);;
        env.execute("JSON example");

    }
}

public static class BuildEvent extends RichMapFunction<String,Tuple4<String,LocalDateTime,Integer,Float>> {
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
