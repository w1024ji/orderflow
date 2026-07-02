package com.orderflow;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class ImbalanceJob {

    // ── 설정 ────────────────────────────────────────────────────
    static final String KAFKA_BROKER    = "kafka.data-pipeline.svc.cluster.local:9092";
    static final String TOPIC           = "orderbook.raw";
    static final String GROUP_ID        = "flink-imbalance-consumer";
    static final String SCHEMA_REGISTRY = "http://schema-registry.data-pipeline.svc.cluster.local:8081";
    static final String REDIS_URI       = "redis://redis-master.data-pipeline.svc.cluster.local:6379";
    static final String S3_OUTPUT_PATH = "s3://orderflow-data/metrics/imbalance";
    static final String SCHEMA_STR = "{"
        + "\"type\":\"record\","
        + "\"name\":\"OrderBookEvent\","
        + "\"namespace\":\"com.orderflow\","
        + "\"fields\":["
        + "{\"name\":\"symbol\",\"type\":\"string\"},"
        + "{\"name\":\"event_time\",\"type\":\"long\"},"
        + "{\"name\":\"last_update_id\",\"type\":\"long\"},"
        + "{\"name\":\"bids\",\"type\":{\"type\":\"array\",\"items\":"
        + "{\"type\":\"record\",\"name\":\"PriceLevel\","
        + "\"fields\":[{\"name\":\"price\",\"type\":\"string\"},{\"name\":\"quantity\",\"type\":\"string\"}]}}},"
        + "{\"name\":\"asks\",\"type\":{\"type\":\"array\",\"items\":\"PriceLevel\"}}"
        + "]}";

    // ── 내부 데이터 클래스 ───────────────────────────────────────
    static class OrderBookEvent {
        String symbol;
        long eventTime;
        long lastUpdateId;
        List<double[]> bids;
        List<double[]> asks;

        public OrderBookEvent() {
            bids = new ArrayList<>();
            asks = new ArrayList<>();
        }
    }

    static class ImbalanceResult {
        String symbol;
        long windowStart;
        long windowEnd;
        double imbalance;
        double weightedBids;
        double weightedAsks;

        @Override
        public String toString() {
            return String.format(
                "[%s] window=%d~%d | imbalance=%.4f (bids=%.2f, asks=%.2f)",
                symbol, windowStart, windowEnd, imbalance, weightedBids, weightedAsks
            );
        }

        // S3에 한 줄로 쓸 CSV 형태
        public String toCsv() {
            return String.format("%s,%d,%d,%.6f,%.4f,%.4f",
                symbol, windowStart, windowEnd, imbalance, weightedBids, weightedAsks);
        }
    }

    // ── 메인 ────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {

        // Flink 환경 설정 (S3 포함)
        org.apache.flink.configuration.Configuration flinkConfig =
            new org.apache.flink.configuration.Configuration();
        flinkConfig.setString("s3.endpoint", "s3.us-east-1.amazonaws.com");
        flinkConfig.setString("s3.endpoint.region", "us-east-1");
        flinkConfig.setString("s3.path.style.access", "false");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        env.enableCheckpointing(30_000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);

        // S3 Hadoop 설정
        org.apache.hadoop.conf.Configuration hadoopConf =
            new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com");
        hadoopConf.set("fs.s3a.endpoint.region", "us-east-1");
        hadoopConf.set("fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider");

        // Avro 스키마
        Schema schema = new Schema.Parser().parse(SCHEMA_STR);

        // Kafka Source
        KafkaSource<GenericRecord> kafkaSource = KafkaSource.<GenericRecord>builder()
            .setBootstrapServers(KAFKA_BROKER)
            .setTopics(TOPIC)
            .setGroupId(GROUP_ID)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(
                ConfluentRegistryAvroDeserializationSchema.forGeneric(
                    schema, SCHEMA_REGISTRY
                )
            )
            .build();

        DataStream<GenericRecord> rawStream = env.fromSource(
            kafkaSource,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );

        DataStream<OrderBookEvent> eventStream = rawStream
            .map(record -> parseEvent(record))
            .filter(e -> e != null)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<OrderBookEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((e, t) -> e.eventTime)
            );

        DataStream<ImbalanceResult> resultStream = eventStream
            .keyBy(e -> e.symbol)
            .window(TumblingEventTimeWindows.of(Time.seconds(1)))
            .aggregate(
                new WeightedImbalanceAggregator(),
                new ImbalanceWindowFunction()
            );

        // ── 싱크 1: 콘솔 출력 ───────────────────────────────────
        resultStream.print();

        // ── 싱크 2: Redis ────────────────────────────────────────
        resultStream.addSink(new RedisSink(REDIS_URI));

        // ── 싱크 3: S3 (CSV, 5분마다 롤링) ─────────────────────
        FileSink<String> s3Sink = FileSink
            .forRowFormat(
                new Path(S3_OUTPUT_PATH),
                new SimpleStringEncoder<String>("UTF-8")
            )
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withRolloverInterval(Duration.ofMinutes(5))
                    .withInactivityInterval(Duration.ofMinutes(1))
                    .withMaxPartSize(MemorySize.ofMebiBytes(128))
                    .build()
            )
            .withOutputFileConfig(
                OutputFileConfig.builder()
                    .withPartPrefix("imbalance")
                    .withPartSuffix(".csv")
                    .build()
            )
            .build();

        resultStream
            .map(r -> r.toCsv())
            .sinkTo(s3Sink);

        env.execute("OrderFlow Imbalance Job");
    }

    // ── Redis 싱크 ───────────────────────────────────────────────
    static class RedisSink extends RichSinkFunction<ImbalanceResult> {
        private final String redisUri;
        private transient RedisClient redisClient;
        private transient StatefulRedisConnection<String, String> connection;
        private transient RedisCommands<String, String> commands;

        public RedisSink(String redisUri) {
            this.redisUri = redisUri;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            redisClient = RedisClient.create(redisUri);
            connection  = redisClient.connect();
            commands    = connection.sync();
        }

        @Override
        public void invoke(ImbalanceResult result, Context context) {
            // key: "imbalance:BTCUSDT"
            // value: "0.8803" (가장 최신 지표)
            String key   = "imbalance:" + result.symbol;
            String value = String.format("%.6f", result.imbalance);
            commands.set(key, value);

            // 상세 정보도 Hash로 저장
            String hashKey = "imbalance:detail:" + result.symbol;
            commands.hset(hashKey, "imbalance",    String.format("%.6f", result.imbalance));
            commands.hset(hashKey, "weighted_bids", String.format("%.4f", result.weightedBids));
            commands.hset(hashKey, "weighted_asks", String.format("%.4f", result.weightedAsks));
            commands.hset(hashKey, "window_start",  String.valueOf(result.windowStart));
            commands.hset(hashKey, "window_end",    String.valueOf(result.windowEnd));
        }

        @Override
        public void close() {
            if (connection != null) connection.close();
            if (redisClient != null) redisClient.shutdown();
        }
    }

    // ── Avro 파싱 ────────────────────────────────────────────────
    static OrderBookEvent parseEvent(GenericRecord record) {
        try {
            OrderBookEvent event = new OrderBookEvent();
            event.symbol       = record.get("symbol").toString();
            event.eventTime    = (long) record.get("event_time");
            event.lastUpdateId = (long) record.get("last_update_id");

            org.apache.avro.generic.GenericArray<?> bids =
                (org.apache.avro.generic.GenericArray<?>) record.get("bids");
            for (Object level : bids) {
                org.apache.avro.generic.GenericRecord l =
                    (org.apache.avro.generic.GenericRecord) level;
                event.bids.add(new double[]{
                    Double.parseDouble(l.get("price").toString()),
                    Double.parseDouble(l.get("quantity").toString())
                });
            }

            org.apache.avro.generic.GenericArray<?> asks =
                (org.apache.avro.generic.GenericArray<?>) record.get("asks");
            for (Object level : asks) {
                org.apache.avro.generic.GenericRecord l =
                    (org.apache.avro.generic.GenericRecord) level;
                event.asks.add(new double[]{
                    Double.parseDouble(l.get("price").toString()),
                    Double.parseDouble(l.get("quantity").toString())
                });
            }

            return event;
        } catch (Exception e) {
            System.err.println("[!] Parse failed: " + e.getMessage());
            return null;
        }
    }

    // ── 가중 불균형 집계 ─────────────────────────────────────────
    static class Accumulator {
        OrderBookEvent latest = null;
    }

    static class WeightedImbalanceAggregator
        implements AggregateFunction<OrderBookEvent, Accumulator, Accumulator> {

        @Override public Accumulator createAccumulator() { return new Accumulator(); }

        @Override
        public Accumulator add(OrderBookEvent event, Accumulator acc) {
            if (acc.latest == null || event.lastUpdateId > acc.latest.lastUpdateId)
                acc.latest = event;
            return acc;
        }

        @Override public Accumulator getResult(Accumulator acc) { return acc; }

        @Override
        public Accumulator merge(Accumulator a, Accumulator b) {
            if (a.latest == null) return b;
            if (b.latest == null) return a;
            return a.latest.lastUpdateId > b.latest.lastUpdateId ? a : b;
        }
    }

    static class ImbalanceWindowFunction
        extends ProcessWindowFunction<Accumulator, ImbalanceResult, String, TimeWindow> {

        @Override
        public void process(
            String symbol, Context ctx,
            Iterable<Accumulator> elements,
            Collector<ImbalanceResult> out
        ) {
            Accumulator acc = elements.iterator().next();
            if (acc.latest == null) return;

            OrderBookEvent ob = acc.latest;
            double weightedBids = 0.0;
            double weightedAsks = 0.0;
            int levels = Math.min(10, Math.max(ob.bids.size(), ob.asks.size()));

            for (int i = 0; i < levels; i++) {
                double weight = 1.0 - (i * 0.1);
                if (i < ob.bids.size()) weightedBids += ob.bids.get(i)[1] * weight;
                if (i < ob.asks.size()) weightedAsks += ob.asks.get(i)[1] * weight;
            }

            double total     = weightedBids + weightedAsks;
            double imbalance = (total == 0) ? 0 : (weightedBids - weightedAsks) / total;

            ImbalanceResult result  = new ImbalanceResult();
            result.symbol           = symbol;
            result.windowStart      = ctx.window().getStart();
            result.windowEnd        = ctx.window().getEnd();
            result.imbalance        = imbalance;
            result.weightedBids     = weightedBids;
            result.weightedAsks     = weightedAsks;

            out.collect(result);
        }
    }
}