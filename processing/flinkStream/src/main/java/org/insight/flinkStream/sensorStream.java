package org.insight.flinkStream;

import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.Properties;

public class sensorStream {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "group1");

        // Use ingestion time == event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //Monitor latency
        env.getConfig().setLatencyTrackingInterval(10);

        // create a stream of sensor readings
        DataStream<DeviceMessage> messageStream = env.addSource(
            new FlinkKafkaConsumer08<>(
                "device_activity_stream",
                new JSONDeserializationSchema(),
                properties)
                .setStartFromEarliest())
            .map(new DeviceMessageMap())
            .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>())
            .name("Kafka Topic: device_activity_stream");

        /* Legend
        f0: String deviceID
        f1: Long timestamp
        f2: String sensorName1 (temp)
        f3: Long sensorValue1 (temp)
        f4: String sensorName2 (kw)
        f5: Long sensorValue2 (kw)
        */

        //defrost detection
        messageStream.rebalance().keyBy("f0").filter((FilterFunction<DeviceMessage>) node -> node.f3 >= 0)
            .map((MapFunction<DeviceMessage, String>) node -> node.f0+": "+node.f3)
            .writeAsText("defrost.txt")
            .setParallelism(1);

      env.execute("JSON example");

    }

    public static class DeviceMessageMap extends RichMapFunction<ObjectNode,DeviceMessage> {

      @Override
      public DeviceMessage map(ObjectNode node) throws Exception {
        String deviceID = node.get("device-id").toString();
        Long timestamp = Long.parseLong(node.get("time").toString());
        String sensorName1 = node.get("sensor-name-1").toString();
        Long sensorValue1 = Long.parseLong(node.get("sensor-value-1").toString());
        String sensorName2 = node.get("sensor-name-2").toString();
        Long sensorValue2 = Long.parseLong(node.get("sensor-value-2").toString());

        return new DeviceMessage(deviceID,timestamp,sensorName1,sensorValue1,sensorName2,sensorValue2);
      }
    }

    public static class DeviceMessage extends Tuple6<String,Long,String,Long,String,Long> {
        DeviceMessage(String deviceID,Long timestamp,String sensorName1,Long sensorValue1,String sensorName2,Long sensorValue2) {
          new Tuple6<String,Long,String,Long,String,Long>(deviceID,timestamp,sensorName1,sensorValue1,sensorName2,sensorValue2);
        }
    }
}
