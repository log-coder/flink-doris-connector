package org.apache.doris.flink.tools.cdc.utils;

import org.apache.flink.util.CollectionUtil;

import org.apache.doris.flink.catalog.doris.DorisSchemaFactory;
import org.apache.doris.flink.catalog.doris.DorisSystem;
import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.exception.DorisSystemException;
import org.apache.doris.flink.tools.cdc.DorisTableConfig;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;

public class DorisTableUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DorisTableUtil.class);

    public static void tryCreateTableIfAbsent(
            DorisSystem dorisSystem,
            String targetDb,
            String dorisTable,
            SourceSchema schema,
            DorisTableConfig tableConfig,
            boolean ignoreIncompatible)
            throws DorisSystemException {

        if (!dorisSystem.tableExists(targetDb, dorisTable)) {
            if (tableConfig.isConvertUniqToPk()
                    && CollectionUtil.isNullOrEmpty(schema.primaryKeys)
                    && !CollectionUtil.isNullOrEmpty(schema.uniqueIndexs)) {
                schema.primaryKeys = new ArrayList<>(schema.uniqueIndexs);
            }

            TableSchema dorisSchema =
                    DorisSchemaFactory.createTableSchema(
                            targetDb,
                            dorisTable,
                            schema.getFields(),
                            schema.getPrimaryKeys(),
                            tableConfig,
                            schema.getTableComment());
            try {
                dorisSystem.createTable(dorisSchema);
            } catch (Exception ex) {
                handleTableCreationFailure(ex, ignoreIncompatible);
            }
        }
    }

    /**
     * Handle table creation failure.
     *
     * @param ex Exception that occurred during table creation
     * @param ignoreIncompatible Whether to ignore incompatible schema errors
     * @throws DorisSystemException if table creation fails and errors should not be ignored
     */
    private static void handleTableCreationFailure(Exception ex, boolean ignoreIncompatible)
            throws DorisSystemException {
        if (ignoreIncompatible && ex.getCause() instanceof SQLSyntaxErrorException) {
            LOG.warn(
                    "Doris schema and source table schema are not compatible. Error: {} ",
                    ex.getCause().toString());
        } else {
            throw new DorisSystemException("Failed to create table due to: ", ex);
        }
    }
}
