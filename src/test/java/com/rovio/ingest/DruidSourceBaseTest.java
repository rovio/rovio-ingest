/*
 * Copyright 2021 Rovio Entertainment Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.rovio.ingest;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import com.rovio.ingest.WriterContext.ConfKeys;
import com.rovio.ingest.util.NormalizeTimeColumnUDF;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.realtime.firehose.IngestSegmentFirehose;
import org.apache.druid.segment.realtime.firehose.WindowedStorageAdapter;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.utils.CompressionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.Interval;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.skife.jdbi.v2.DBI;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.rovio.ingest.DataSegmentCommitMessage.MAPPER;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static org.apache.spark.sql.functions.column;
import static org.junit.Assert.assertEquals;

@Testcontainers
public class DruidSourceBaseTest extends SharedJavaSparkContext {

    public static final String segmentsTable = MetadataStorageTablesConfig.fromBase("druid").getSegmentsTable();

    public static final String DATA_SOURCE = "temp";
    public static final String DB_NAME = "temp";
    public static final long VERSION_TIME_MILLIS = 1569961771384L;

    public static String dbUser = "user";
    public static String dbPass = "pass";

    protected SparkSession spark;
    protected Map<String, String> options;

    @TempDir
    public File testFolder;

    @Container
    public static MySQLContainer<?> MYSQL = getMySQLContainer();

    public static MySQLContainer<?> getMySQLContainer() {
        // MySQL 8 requires mysql-connector-java 8.x so we test against that.
        // The mysql-connector-java 8.x also works with MySQL 5
        return new MySQLContainer<>("mysql:8.0.28")
                .withUsername(dbUser)
                .withPassword(dbPass)
                .withDatabaseName(DB_NAME);
    }

    public static PostgreSQLContainer<?> getPostgreSQLContainer() {
        // 15.1 is the latest postgres image at the time of writing this.
        // The jdbc module versioning (42.2.23 in pom) doesn't seem to follow postgres image versioning.
        return new PostgreSQLContainer<>("postgres:15.1")
                .withUsername(dbUser)
                .withPassword(dbPass)
                .withDatabaseName(DB_NAME);
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        prepareDatabase(MYSQL);
    }

    public static void prepareDatabase(JdbcDatabaseContainer<?> jdbc) throws SQLException {
        if (jdbc instanceof MySQLContainer) {
            // Druid requires its MySQL database to be created with an UTF8 charset.
            runSql(jdbc, String.format("ALTER DATABASE %s CHARACTER SET utf8mb4", DB_NAME));
            // Could use sqlConnector.createSegmentTable() instead
            runSql(jdbc, String.format("CREATE TABLE %1$s (\n"
                            + "  id VARCHAR(255) NOT NULL,\n"
                            + "  dataSource VARCHAR(255) NOT NULL,\n"
                            + "  created_date VARCHAR(255) NOT NULL,\n"
                            + "  start VARCHAR(255) NOT NULL,\n"
                            + "  `end` VARCHAR(255) NOT NULL,\n"
                            + "  partitioned BOOLEAN NOT NULL,\n"
                            + "  version VARCHAR(255) NOT NULL,\n"
                            + "  used BOOLEAN NOT NULL,\n"
                            + "  payload BLOB NOT NULL,\n"
                            + "  PRIMARY KEY (id)\n"
                            + ")",
                    segmentsTable));
        } else {
            // Could use sqlConnector.createSegmentTable() instead
            runSql(jdbc, String.format("CREATE TABLE %1$s (\n"
                            + "  id VARCHAR(255) NOT NULL,\n"
                            + "  dataSource VARCHAR(255) NOT NULL,\n"
                            + "  created_date VARCHAR(255) NOT NULL,\n"
                            + "  start VARCHAR(255) NOT NULL,\n"
                            + "  \"end\" VARCHAR(255) NOT NULL,\n"
                            + "  partitioned BOOLEAN NOT NULL,\n"
                            + "  version VARCHAR(255) NOT NULL,\n"
                            + "  used BOOLEAN NOT NULL,\n"
                            + "  payload BYTEA NOT NULL,\n"
                            + "  PRIMARY KEY (id)\n"
                            + ")",
                    segmentsTable));
        }
    }

    public static String getConnectionString(JdbcDatabaseContainer<?> jdbc) {
        String connectionString = jdbc.getJdbcUrl();
        if (jdbc instanceof MySQLContainer) {
            if (!connectionString.contains("useSSL=")) {
                String separator = connectionString.contains("?") ? "&" : "?";
                connectionString = connectionString + separator + "useSSL=false";
            }
        }
        return connectionString;
    }

    @BeforeEach
    public void before() {
        DateTimeUtils.setCurrentMillisFixed(VERSION_TIME_MILLIS);

        // Create test dataset.
        spark = SparkSession.builder().sparkContext(sc()).config("spark.sql.session.timeZone", "UTC").config("spark.driver.bindAddress", "127.0.0.1").config("spark.master", "local").getOrCreate();
        // This is for some udf-level unit-testing. JVM API users don't need to register this udf.
        spark.udf().register("normalizeTimeColumn", new NormalizeTimeColumnUDF(), DataTypes.LongType);

        options = getDataSourceOptions(MYSQL);
        options.put(ConfKeys.DEEP_STORAGE_LOCAL_DIRECTORY, testFolder.toString());

        // note: these are ignored when DEEP_STORAGE_TYPE = local
        options.put(ConfKeys.DEEP_STORAGE_S3_BUCKET, "my-bucket");
        options.put(ConfKeys.DEEP_STORAGE_S3_BASE_KEY, "druid/prod/segments");

        NullHandling.initializeForTests();
        EmittingLogger.registerEmitter(Mockito.mock(ServiceEmitter.class));
    }

    public static Map<String, String> getDataSourceOptions(JdbcDatabaseContainer<?> jdbc) {
        Map<String, String> options = new HashMap<>();
        options.put(ConfKeys.DATA_SOURCE, DATA_SOURCE);
        options.put(ConfKeys.METADATA_DB_TYPE, jdbc instanceof MySQLContainer ? "mysql" : "postgres");
        options.put(ConfKeys.METADATA_DB_URI, getConnectionString(jdbc));
        options.put(ConfKeys.METADATA_DB_USERNAME, dbUser);
        options.put(ConfKeys.METADATA_DB_PASSWORD, dbPass);
        options.put(ConfKeys.TIME_COLUMN, "date");
        options.put(ConfKeys.DEEP_STORAGE_TYPE, "local");
        return options;
    }

    @BeforeEach
    public void setUp() throws SQLException {
        setUpDb(MYSQL);
    }

    public static void setUpDb(JdbcDatabaseContainer<?> jdbc) throws SQLException {
        runSql(jdbc, "TRUNCATE TABLE " + segmentsTable);
    }

    @AfterEach
    public void after() {
        DateTimeUtils.setCurrentMillisSystem();
    }

    private static void runSql(JdbcDatabaseContainer<?> jdbc, String sql) throws SQLException {
        try (Connection connection = jdbc.createConnection("");
             Statement statement  = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    static Dataset<Row> loadCsv(SparkSession spark, String fileName) {
        Dataset<Row> dataset = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("timestampFormat", "yyyy-MM-dd")
                .load(DruidSourceBaseTest.class.getResource(fileName).getPath());

        return dataset.withColumn("i_dau", column("dau").cast(DataTypes.IntegerType))
                .withColumn("s_dau", column("dau").cast(DataTypes.ShortType))
                .withColumn("b_dau", column("dau").cast(DataTypes.ByteType))
                .withColumn("f_revenue", column("revenue").cast(DataTypes.FloatType));
    }

    static void verifySegmentPath(Path root, Interval interval, String version, int numShards, boolean isMonth) throws IOException {
        for (DateTime current = interval.getStart(); current.isBefore(interval.getEnd()); current = isMonth ? current.plusMonths(1) : current.plusDays(1)) {
            String relativePath = current + "_" + (isMonth ? current.plusMonths(1) : current.plusDays(1));
            assertEquals(numShards,
                    Files.list(Paths.get(root.toString(), relativePath, version))
                            .filter(p -> Files.isDirectory(p.toAbsolutePath(), NOFOLLOW_LINKS))
                            .count());
        }
    }

    static void verifySegmentTable(Interval interval, String version, boolean used, int expectedRowCount) {
        String sql = "SELECT count(id) as c from %1s" +
                " where dataSource = :dataSource" +
                " and start < :end" +
                " and end >= :start" +
                " and version = :version" +
                " and used = :used";

        long rowCount = (long) DBI
                .open(getConnectionString(MYSQL), dbUser, dbPass)
                .createQuery(String.format(sql, segmentsTable))
                .bind("dataSource", DATA_SOURCE)
                .bind("start", interval.getStart().toString())
                .bind("end", interval.getEnd().toString())
                .bind("version", version)
                .bind("used", used)
                .first().get("c");

        assertEquals(expectedRowCount, rowCount);
    }

  protected Table<Integer, ImmutableMap<String, Object>, ImmutableMap<String, Object>> readSegmentData(Path root,
                                                                                                       Interval interval,
                                                                                                       String version,
                                                                                                       int numShards,
                                                                                                       boolean isMonth) throws IOException {
        Table<Integer, ImmutableMap<String, Object>, ImmutableMap<String, Object>> rows = HashBasedTable.create();
        for (DateTime current = interval.getStart(); current.isBefore(interval.getEnd()); current = isMonth ? current.plusMonths(1) : current.plusDays(1)) {
            String relativePath = current + "_" + (isMonth ? current.plusMonths(1) : current.plusDays(1));
            for (int i = 0; i < numShards; i++) {
                Path zipPath = Paths.get(root.toString(), relativePath, version, String.valueOf(i), "index.zip");
                ImmutableMap<ImmutableMap<String, Object>, ImmutableMap<String, Object>> data = readSegmentZip(interval, zipPath);
                for (Map.Entry<ImmutableMap<String, Object>, ImmutableMap<String, Object>> entry : data.entrySet()) {
                    rows.put(i, entry.getKey(), entry.getValue());
                }
            }
        }

        return rows;
    }

    private ImmutableMap<ImmutableMap<String, Object>, ImmutableMap<String, Object>> readSegmentZip(Interval interval, Path zipPath) throws IOException {
        Path tmpDir = Files.createTempDirectory(testFolder.toPath(),"temp");
        tmpDir.toFile().deleteOnExit();
        ImmutableMap.Builder<ImmutableMap<String, Object>, ImmutableMap<String, Object>> values = ImmutableMap.builder();

        CompressionUtils.unzip(zipPath.toFile(), tmpDir.toFile());
        IndexIO indexIO = new IndexIO(MAPPER, () -> 0);
        QueryableIndexStorageAdapter queryableIndexStorageAdapter = new QueryableIndexStorageAdapter(indexIO.loadIndex(tmpDir.toFile()));
        WindowedStorageAdapter windowedStorageAdapter = new WindowedStorageAdapter(queryableIndexStorageAdapter, interval);
        try (IngestSegmentFirehose firehose = new IngestSegmentFirehose(
                Collections.singletonList(windowedStorageAdapter),
                TransformSpec.NONE,
                ImmutableList.copyOf(queryableIndexStorageAdapter.getAvailableDimensions()),
                ImmutableList.copyOf(queryableIndexStorageAdapter.getAvailableMetrics()),
                null)) {

            while (firehose.hasMore()) {
                InputRow inputRow = firehose.nextRow();
                if (inputRow != null) {
                    ImmutableMap.Builder<String, Object> dimensions = ImmutableMap.builder();
                    ImmutableMap.Builder<String, Object> metrics = ImmutableMap.builder();
                    inputRow.getDimensions().forEach(d -> dimensions.put(d, inputRow.getRaw(d)));
                    dimensions.put("__time", inputRow.getTimestamp());
                    queryableIndexStorageAdapter.getAvailableMetrics().forEach(m -> metrics.put(m, inputRow.getMetric(m)));
                    values.put(dimensions.build(), metrics.build());
                }
            }
        }
        return values.build();
    }
}
