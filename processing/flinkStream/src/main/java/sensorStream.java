import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import com.google.gson.Gson;

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
        DataStream<ObjectNode> messageStream = env.addSource(new FlinkKafkaConsumer08<>("device_activity_stream", new JSONDeserializationSchema(), properties).setStartFromEarliest());
        // DataStream<Tuple4<String,LocalDateTime,Integer,Float>> eventStream = messageStream.map(new BuildEvent());

        // messageStream.rebalance().filter((FilterFunction) jsonNode -> ( jsonNode.get("temp").asInt() >= 0)).writeAsText("out.txt").setParallelism(1);
        // eventStream.map((MapFunction) event -> event.f1+"$").writeAsText("out.txt").setParallelism(1);;

        messageStream.rebalance().map((MapFunction<ObjectNode, String>) node -> "Kafka and Flink says: " + node.get("Temp")).writeAsText("out.txt").setParallelism(1);

        env.execute("JSON example");

    }
}
