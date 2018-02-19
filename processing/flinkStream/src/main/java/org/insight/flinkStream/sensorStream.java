package org.insight.flinkStream;

import static java.lang.Math.abs;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Properties;
import org.apache.flink.util.Collector;

public class sensorStream {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "localhost:2181");
    properties.setProperty("group.id", "group1");

    // Use ingestion time == event time
    // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    //Monitor latency
    env.getConfig().setLatencyTrackingInterval(10);

    // create a stream of sensor readings //.setStartFromEarliest())
    DataStream < Tuple6 < String, Float, String, Float, String, Float >> messageStream = env.addSource(
        new FlinkKafkaConsumer08 < > (
            "device_activity_stream",
            new JSONDeserializationSchema(),
            properties))
        .map(new DeviceMessageMap())
        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor < Tuple6 < String, Float, String, Float, String, Float >> () {

          @Override
          public long extractAscendingTimestamp(Tuple6 < String, Float, String, Float, String, Float > node) {
            return node.f1.longValue();
          }
        })
        .name("Kafka Topic: device_activity_stream");

    final int TEMPERATURE_WARNING_THRESHOLD = -9999;
    final int DEFROST_THRESHOLD = 0;
  /* Legend
  f0: String deviceID
  f1: Float timestamp
  f2: String sensorName1 (temp)
  f3: Float sensorValue1 (temp)
  f4: String sensorName2 (kw)
  f5: Float sensorValue2 (kw)
  */

    //Warehouse Data
    DataStream < Tuple6 < String, Date, String, Float, String, Float >> timeline = messageStream.map((MapFunction < Tuple6 < String, Float, String, Float, String, Float > , Tuple6 < String, Date, String, Float, String, Float >> ) node -> new Tuple6 < String, Date, String, Float, String, Float > (node.f0, new Date(node.f1.longValue()), node.f2, node.f3, node.f4, node.f5));
    CassandraSink.addSink(timeline)
        .setQuery("INSERT INTO hypespace.timeline (deviceID, time_stamp,sensorName1,sensorValue1,sensorName2,sensorValue2) " +
            "values (?, ?, ?, ?, ?, ?) USING TTL 7200;")
        .setHost("10.0.0.6")
        .build();

    //Defrost detection
    DataStream < Tuple2 < String, Boolean >> defrostResult = messageStream.keyBy("f0").filter((FilterFunction < Tuple6 < String, Float, String, Float, String, Float >> ) node -> node.f3 >= DEFROST_THRESHOLD)
        .map((MapFunction < Tuple6 < String, Float, String, Float, String, Float > , Tuple2 < String, Boolean >> ) node -> new Tuple2 < String, Boolean > (node.f0.toString(), Boolean.TRUE));

    //Update the results to sink
    CassandraSink.addSink(defrostResult)
        .setQuery("INSERT INTO hypespace.status (deviceID, defrosted) " +
            "values (?, ?);")
        .setHost("10.0.0.6")
        .build();


    // Door Open Detection
    // Warning Pattern: Temp Rising
    // Alert Pattern: Energy Also Rising
    Pattern < Tuple6 < String, Float, String, Float, String, Float > , ? > doorWarningPattern = Pattern. < Tuple6 < String, Float, String, Float, String, Float >> begin("first")
        .where(new SimpleCondition < Tuple6 < String, Float, String, Float, String, Float >> () {

          @Override
          public boolean filter(Tuple6 < String, Float, String, Float, String, Float > node) throws Exception {
            return true;
          }
        })
        .next("second").where(new IterativeCondition < Tuple6 < String, Float, String, Float, String, Float >> () {
          @Override
          public boolean filter(Tuple6 < String, Float, String, Float, String, Float > node, Context < Tuple6 < String, Float, String, Float, String, Float >> context) throws Exception {
            final Iterator < Tuple6 < String, Float, String, Float, String, Float >> itr = context.getEventsForPattern("first").iterator();
            Tuple6 < String, Float, String, Float, String, Float > lastEvent = itr.next();

            while (itr.hasNext()) {
              lastEvent = itr.next();
            }

            return node.f3.longValue() > lastEvent.f3.longValue();
          }
        })
        .within(Time.seconds(10));


    // Create a pattern stream from our warning pattern
    PatternStream < Tuple6 < String, Float, String, Float, String, Float >> doorPatternStream = CEP.pattern(
        messageStream.keyBy("f0"),
        doorWarningPattern);

    // Generate state map for devices
    DataStream < Tuple2 < String, Boolean >> alerts = doorPatternStream.flatSelect(
        (Map < String, List < Tuple6 < String, Float, String, Float, String, Float >>> pattern, Collector < Tuple2 < String, Boolean >> out) -> {
          Tuple6 < String,
              Float,
              String,
              Float,
              String,
              Float > first = (Tuple6 < String, Float, String, Float, String, Float > ) pattern.get("first").get(0);
          Tuple6 < String,
              Float,
              String,
              Float,
              String,
              Float > second = (Tuple6 < String, Float, String, Float, String, Float > ) pattern.get("second").get(0);

          if (first.f5.floatValue() < second.f5.floatValue()) {
            out.collect(new Tuple2 < String, Boolean > (first.f0.toString(), Boolean.TRUE));
          } else {
            out.collect(new Tuple2 < String, Boolean > (first.f0.toString(), Boolean.FALSE));
          }
        });

    //Update the results to sink
    CassandraSink.addSink(alerts)
        .setQuery("INSERT INTO hypespace.status (deviceID, doorOpen) " +
            "values (?, ?);")
        .setHost("10.0.0.6")
        .build();

    //Efficiency
    //Calculate work
    DataStream < Tuple6 < String, Float, String, Float, String, Float >> work = messageStream.keyBy("f0")
        .timeWindow(Time.seconds(30), Time.seconds(1))
        .sum("f5")
        .keyBy("f0");

    //Calculate delta t
    DataStream < Tuple2 < String, Float >> efficiency = work
        .map((MapFunction < Tuple6 < String, Float, String, Float, String, Float > , Tuple3 < String, Float, Float >> ) node -> new Tuple3 < String, Float, Float > (node.f0, node.f3, node.f5))
        .keyBy("f0")
        .reduce(new ReduceFunction < Tuple3 < String, Float, Float >> () {

          @Override
          public Tuple3 < String, Float, Float > reduce(Tuple3 < String, Float, Float > v1, Tuple3 < String, Float, Float > v2) throws Exception {
            float dt = 0;
            if (v1.f1.floatValue() - v2.f1.floatValue() < 0 ) {
              dt = abs(v1.f1.floatValue() - v2.f1.floatValue());
            }
            return new Tuple3 < String, Float, Float > (v1.f0.toString(), dt / v2.f2.floatValue(), Float.parseFloat("0"));
          }

        })
        .map((MapFunction < Tuple3 < String, Float, Float > , Tuple2 < String, Float >> ) node -> new Tuple2 < String, Float > (node.f0.toString(), 100 * node.f1.floatValue()));;

    //Update the results to sink
    CassandraSink.addSink(efficiency)
        .setQuery("INSERT INTO hypespace.status (deviceID, efficiency) " +
            "values (?, ?);")
        .setHost("10.0.0.6")
        .build();

    env.execute("JSON example");

  }

  public static class DeviceMessageMap extends RichMapFunction < ObjectNode, Tuple6 < String, Float, String, Float, String, Float >> {

    @Override
    public Tuple6 < String,
        Float,
        String,
        Float,
        String,
        Float > map(ObjectNode node) throws Exception {
      String deviceID = node.get("device-id").toString();
      Float timestamp = Float.parseFloat(node.get("time").toString());
      String sensorName1 = node.get("sensor-name-1").toString();
      Float sensorValue1 = Float.parseFloat(node.get("sensor-value-1").toString());
      String sensorName2 = node.get("sensor-name-2").toString();
      Float sensorValue2 = Float.parseFloat(node.get("sensor-value-2").toString());

      return new Tuple6 < String, Float, String, Float, String, Float > (deviceID, timestamp, sensorName1, sensorValue1, sensorName2, sensorValue2);
    }
  }
}