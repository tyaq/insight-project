import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;

import java.util.Properties;

public class sensorStream {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "group1");

        //Monitor latency
        env.getConfig().setLatencyTrackingInterval(1);

        // create a stream of sensor readings
        DataStream<ObjectNode> messageStream = env.addSource(
            new FlinkKafkaConsumer08<>(
                "device_activity_stream",
                new JSONDeserializationSchema(),
                properties)
                .setStartFromEarliest())
            .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>())
            .name("Kafka Topic: device_activity_stream");

        // Use ingestion time == event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //defrost detection
        messageStream.filter((FilterFunction<ObjectNode>) node -> node.get("temp").asInt() >= 0)
            .map((MapFunction<ObjectNode, String>) node -> node.get("device_id")+": "+node.get("temp"))
            .writeAsText("defrost.txt")
            .setParallelism(1);

        //Door Open
        messageStream.map((MapFunction<ObjectNode, String>) node -> node.get("device_id")+": "+node.get("kws")).writeAsText("door.txt").setParallelism(1);

        env.execute("JSON example");

    }
}
