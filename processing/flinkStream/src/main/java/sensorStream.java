import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
        properties.setProperty("auto.offset.reset", "smallest");

        // create a stream of sensor readings
        DataStream<ObjectNode> messageStream = env.addSource(new FlinkKafkaConsumer08<>("device_activity_stream", new JSONDeserializationSchema(), properties));

        // messageStream.filter((FilterFunction) jsonNode -> ( jsonNode.get("temp").asInt() >= 0)).writeAsText("out.txt").setParallelism(1);
        messageStream.map((MapFunction) x -> (x.getClass().toString())).writeAsText("out.txt").setParallelism(1);;
        env.execute("JSON example");

    }
}
