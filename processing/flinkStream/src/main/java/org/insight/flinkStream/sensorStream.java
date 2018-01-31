package org.insight.flinkStream;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.CassandraTupleSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Properties;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import java.sql.Types;

import org.insight.flinkStream.Config;

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
        DataStream<Tuple6<String,Float,String,Float,String,Float>> messageStream = env.addSource(
            new FlinkKafkaConsumer08<>(
                "device_activity_stream",
                new JSONDeserializationSchema(),
                properties))
            .map(new DeviceMessageMap())
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple6<String,Float,String,Float,String,Float>>() {

              @Override
              public long extractAscendingTimestamp(Tuple6<String,Float,String,Float,String,Float> node) {
                return node.f1.longValue();
              }
            })
            .name("Kafka Topic: device_activity_stream");

        final int TEMPERATURE_WARNING_THRESHOLD = -15;
        final int DEFROST_THRESHOLD = 0;

        /* Legend
        f0: String deviceID
        f1: Long timestamp
        f2: String sensorName1 (temp)
        f3: Long sensorValue1 (temp)
        f4: String sensorName2 (kw)
        f5: Long sensorValue2 (kw)
        */

        //defrost detection
        DataStream<Tuple2<String,Boolean>> defrostResult = messageStream.keyBy("f0").filter((FilterFunction<Tuple6<String,Float,String,Float,String,Float>>) node -> node.f3 >= DEFROST_THRESHOLD)
            .map((MapFunction<Tuple6<String,Float,String,Float,String,Float>, Tuple2<String,Boolean>>) node -> new Tuple2<String,Boolean>(node.f0,Boolean.TRUE));

      //Update the results to sink
      CassandraSink.addSink(defrostResult)
          .setQuery("INSERT INTO hypespace.defrostStatus (deviceID, defrosted) " +
              "values (?, ?);")
          .setHost("localhost")
          .build();


        //Door Open Detection
        //Warning Pattern: Temp Rising and Energy Rising
      // Warning pattern: Two consecutive temperature events whose temperature is higher than the given threshold
      // appearing within a time interval of 10 seconds
      Pattern<Tuple6<String,Float,String,Float,String,Float>, ?> doorTempWarningPattern = Pattern.<Tuple6<String,Float,String,Float,String,Float>>begin("first")
          .where(new IterativeCondition<Tuple6<String,Float,String,Float,String,Float>>() {

            @Override
            public boolean filter(Tuple6<String,Float,String,Float,String,Float> node, Context<Tuple6<String,Float,String,Float,String,Float>> ctx) throws Exception {
              return node.f3.floatValue() >= TEMPERATURE_WARNING_THRESHOLD;
            }
          })
          .next("second")
          .where(new IterativeCondition<Tuple6<String,Float,String,Float,String,Float>>() {

            @Override
            public boolean filter(Tuple6<String,Float,String,Float,String,Float> node, Context<Tuple6<String,Float,String,Float,String,Float>> ctx) throws Exception {
              return node.f3.floatValue() >= TEMPERATURE_WARNING_THRESHOLD;
            }
          })
          .within(Time.seconds(10));

      // Create a pattern stream from our warning pattern
      PatternStream<Tuple6<String,Float,String,Float,String,Float>> tempPatternStream = CEP.pattern(
          messageStream.keyBy("f0"),
          doorTempWarningPattern);

      // Generate temperature warnings for each matched warning pattern
      DataStream<Tuple2<String,Float>> warnings = tempPatternStream.select(
          (Map<String, List<Tuple6<String,Float,String,Float,String,Float>>> pattern) -> {
            Tuple6<String,Float,String,Float,String,Float> first = (Tuple6<String,Float,String,Float,String,Float>) pattern.get("first").get(0);
            Tuple6<String,Float,String,Float,String,Float> second = (Tuple6<String,Float,String,Float,String,Float>) pattern.get("second").get(0);

            return new Tuple2<String,Float>(first.f0,(first.f3.floatValue() + second.f3.floatValue()) / 2);
          }
      );

      // Alert pattern: Two consecutive temperature warnings appearing within a time interval of 20 seconds
      Pattern<Tuple2<String,Float>, ?> tempAlertPattern = Pattern.<Tuple2<String,Float>>begin("first")
          .next("second")
          .within(Time.seconds(20));

      // Create a pattern stream from our alert pattern
      PatternStream<Tuple2<String,Float>> tempAlertPatternStream = CEP.pattern(
          warnings.keyBy("f0"),
          tempAlertPattern);

      // Generate a temperature alert only iff the second temperature warning's average temperature is higher than
      // first warning's temperature
      DataStream<Tuple2<String,Boolean>> alerts = tempAlertPatternStream.flatSelect(
          (Map<String, List<Tuple2<String,Float>>> pattern, Collector<Tuple2<String,Boolean>> out) -> {
            Tuple2<String,Float> first = pattern.get("first").get(0);
            Tuple2<String,Float> second = pattern.get("second").get(0);

            if (first.f1.floatValue() < second.f1.floatValue()) {
              out.collect(new Tuple2<String,Boolean>(first.f0,Boolean.TRUE));
            } else {
              out.collect(new Tuple2<String,Boolean>(first.f0,Boolean.FALSE));
            }
          });

      // Print the warning and alert events to stdout
      //warnings.print();
      //alerts.print();

      //Update the results to sink
      CassandraSink.addSink(defrostResult)
          .setQuery("INSERT INTO hypespace.doorStatus (deviceID, doorOpen) " +
              "values (?, ?);")
          .setHost("localhost")
          .build();


      env.execute("JSON example");

    }

    public static class DeviceMessageMap extends RichMapFunction<ObjectNode,Tuple6<String,Float,String,Float,String,Float>> {

      @Override
      public Tuple6<String,Float,String,Float,String,Float> map(ObjectNode node) throws Exception {
        String deviceID = node.get("device-id").toString();
        Float timestamp = Float.parseFloat(node.get("time").toString());
        String sensorName1 = node.get("sensor-name-1").toString();
        Float sensorValue1 = Float.parseFloat(node.get("sensor-value-1").toString());
        String sensorName2 = node.get("sensor-name-2").toString();
        Float sensorValue2 = Float.parseFloat(node.get("sensor-value-2").toString());

        return new Tuple6<String,Float,String,Float,String,Float>(deviceID,timestamp,sensorName1,sensorValue1,sensorName2,sensorValue2);
      }
    }


//  public static class OutputToDefrostStatusMap implements SinkFunction<Tuple2<String, Boolean>> {
//    @Override
//    public Tuple2<String, Boolean> (Tuple2<String, Boolean> in) {
//
//      String query = String.format("INSERT INTO defrostStatus (deviceID, defrosted)\n"
//          + "    VALUES (%s', %b);",in.f0,in.f1);
//
//      JDBCOutputFormat jdbcOutput = JDBCOutputFormat.buildJDBCOutputFormat()
//          .setDrivername("org.postgresql.Driver")
//          .setDBUrl(Config.DBURL)
//          .setUsername(Config.USER)
//          .setPassword(Config.PASS)
//          .setQuery(query)
//          .setBatchInterval(128)
//          .finish();
//      System.out.println(jdbcOutput);
//      return in;
//    }
//  }
//  private static JDBCOutputFormat createJDBCSink() {
//    return JDBCOutputFormat.buildJDBCOutputFormat()
//        .setDrivername("org.postgresql.Driver")
//        .setDBUrl(Config.DBURL)
//        .setUsername(Config.USER)
//        .setPassword(Config.PASS)
//        .setQuery("INSERT INTO defrostStatus (deviceID, defrosted) VALUES (?,?)")
//        .setSqlTypes(new int[] { Types.VARCHAR, Types.FLOAT })
//        .setBatchInterval(128)
//        .finish();
//  }
}
