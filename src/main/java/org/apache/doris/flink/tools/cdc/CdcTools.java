package org.apache.doris.flink.tools.cdc;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.doris.flink.tools.cdc.mysql.MysqlDatabaseSync;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** cdc sync tools. */
public class CdcTools {
    private static final List<String> EMPTY_KEYS =
            Collections.singletonList(DatabaseSyncConfig.PASSWORD);
    private static StreamExecutionEnvironment flinkEnvironmentForTesting;
    private static JobClient jobClient;

    public static void main(String[] args) throws Exception {
        System.out.println("Input args: " + Arrays.asList(args) + ".\n");
        String operation = args[0].toLowerCase();
        String[] opArgs = Arrays.copyOfRange(args, 1, args.length);
        MultipleParameterTool params = MultipleParameterTool.fromArgs(opArgs);
        if (operation.equals(DatabaseSyncConfig.MYSQL_SYNC_DATABASE)) {
            createMySQLSyncDatabase(params);
        } else {
            System.out.println("Unknown operation " + operation);
            System.exit(1);
        }
    }

    private static void createMySQLSyncDatabase(MultipleParameterTool params) throws Exception {
        Preconditions.checkArgument(params.has(DatabaseSyncConfig.MYSQL_CONF));
        Map<String, String> mysqlMap = getConfigMap(params, DatabaseSyncConfig.MYSQL_CONF);
        Configuration mysqlConfig = Configuration.fromMap(mysqlMap);
        DatabaseSync databaseSync = new MysqlDatabaseSync();
        syncDatabase(params, databaseSync, mysqlConfig, SourceConnector.MYSQL);
    }

    private static void syncDatabase(
            MultipleParameterTool params,
            DatabaseSync databaseSync,
            Configuration config,
            SourceConnector sourceConnector)
            throws Exception {
        String jobName = params.get(DatabaseSyncConfig.JOB_NAME);
        String database = params.get(DatabaseSyncConfig.DATABASE);
        String tablePrefix = params.get(DatabaseSyncConfig.TABLE_PREFIX);
        String tableSuffix = params.get(DatabaseSyncConfig.TABLE_SUFFIX);
        String includingTables = params.get(DatabaseSyncConfig.INCLUDING_TABLES);
        String excludingTables = params.get(DatabaseSyncConfig.EXCLUDING_TABLES);
        String multiToOneOrigin = params.get(DatabaseSyncConfig.MULTI_TO_ONE_ORIGIN);
        String multiToOneTarget = params.get(DatabaseSyncConfig.MULTI_TO_ONE_TARGET);
        String schemaChangeMode = params.get(DatabaseSyncConfig.SCHEMA_CHANGE_MODE);
        boolean createTableOnly = params.has(DatabaseSyncConfig.CREATE_TABLE_ONLY);
        boolean ignoreDefaultValue = params.has(DatabaseSyncConfig.IGNORE_DEFAULT_VALUE);
        boolean ignoreIncompatible = params.has(DatabaseSyncConfig.IGNORE_INCOMPATIBLE);
        boolean singleSink = params.has(DatabaseSyncConfig.SINGLE_SINK);

        Preconditions.checkArgument(params.has(DatabaseSyncConfig.SINK_CONF));
        Map<String, String> sinkMap = getConfigMap(params, DatabaseSyncConfig.SINK_CONF);
        DorisTableConfig tableConfig =
                new DorisTableConfig(getConfigMap(params, DatabaseSyncConfig.TABLE_CONF));
        Configuration sinkConfig = Configuration.fromMap(sinkMap);

        // 解析Kafka配置（必填）
        Preconditions.checkArgument(params.has(DatabaseSyncConfig.KAFKA_CONF), "kafka-conf is required");
        Map<String, String> kafkaMap = getConfigMap(params, DatabaseSyncConfig.KAFKA_CONF);
        Configuration kafkaConfig = Configuration.fromMap(kafkaMap);

        StreamExecutionEnvironment env =
                Objects.nonNull(flinkEnvironmentForTesting)
                        ? flinkEnvironmentForTesting
                        : StreamExecutionEnvironment.getExecutionEnvironment();
        databaseSync
                .setEnv(env)
                .setDatabase(database)
                .setConfig(config)
                .setTablePrefix(tablePrefix)
                .setTableSuffix(tableSuffix)
                .setIncludingTables(includingTables)
                .setExcludingTables(excludingTables)
                .setMultiToOneOrigin(multiToOneOrigin)
                .setMultiToOneTarget(multiToOneTarget)
                .setIgnoreDefaultValue(ignoreDefaultValue)
                .setSinkConfig(sinkConfig)
                .setTableConfig(tableConfig)
                .setCreateTableOnly(createTableOnly)
                .setSingleSink(singleSink)
                .setIgnoreIncompatible(ignoreIncompatible)
                .setSchemaChangeMode(schemaChangeMode)
                .setKafkaConfig(kafkaConfig)
                .create();

        boolean needExecute = databaseSync.build();
        if (!needExecute) {
            // create table only
            return;
        }
        if (StringUtils.isNullOrWhitespaceOnly(jobName)) {
            jobName =
                    String.format(
                            "%s-Doris Sync Database: %s",
                            sourceConnector.getConnectorName(),
                            config.getString(
                                    DatabaseSyncConfig.DATABASE_NAME, DatabaseSyncConfig.DB));
        }
        if (Objects.nonNull(flinkEnvironmentForTesting)) {
            jobClient = env.executeAsync();
        } else {
            env.execute(jobName);
        }
    }

    @VisibleForTesting
    public static JobClient getJobClient() {
        return jobClient;
    }

    // Only for testing, please do not use it in actual environment
    @VisibleForTesting
    public static void setStreamExecutionEnvironmentForTesting(
            StreamExecutionEnvironment environment) {
        flinkEnvironmentForTesting = environment;
    }

    @VisibleForTesting
    public static Map<String, String> getConfigMap(MultipleParameterTool params, String key) {
        if (!params.has(key)) {
            System.out.println(
                    "Can not find key ["
                            + key
                            + "] from args: "
                            + params.toMap().toString()
                            + ".\n");
            return null;
        }

        Map<String, String> map = new HashMap<>();
        for (String param : params.getMultiParameter(key)) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2) {
                map.put(kv[0].trim(), kv[1].trim());
                continue;
            } else if (kv.length == 1 && EMPTY_KEYS.contains(kv[0])) {
                map.put(kv[0].trim(), "");
                continue;
            }

            System.out.println("Invalid " + key + " " + param + ".\n");
            return null;
        }
        return map;
    }
}
