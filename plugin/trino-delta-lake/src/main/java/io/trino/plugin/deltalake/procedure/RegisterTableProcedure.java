/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.deltalake.procedure;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.DeltaLakeMetadataFactory;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastore;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_FILESYSTEM_ERROR;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.DUMMY_DATA_COLUMNS;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.deltaTableProperties;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.setDeltaStorageFormat;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;

public class RegisterTableProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle REGISTER_TABLE;

    private static final String PROCEDURE_NAME = "register_table";
    private static final String SYSTEM_SCHEMA = "system";

    private static final String SCHEMA_NAME = "SCHEMA_NAME";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String TABLE_LOCATION = "TABLE_LOCATION";

    static {
        try {
            REGISTER_TABLE = lookup().unreflect(RegisterTableProcedure.class.getMethod("registerTable", ConnectorSession.class, String.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final DeltaLakeMetadataFactory metadataFactory;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final boolean registerTableProcedureEnabled;

    @Inject
    public RegisterTableProcedure(DeltaLakeMetadataFactory metadataFactory, TrinoFileSystemFactory fileSystemFactory, DeltaLakeConfig deltaLakeConfig)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.registerTableProcedureEnabled = requireNonNull(deltaLakeConfig, "deltaLakeConfig is null").isRegisterTableProcedureEnabled();
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                SYSTEM_SCHEMA,
                PROCEDURE_NAME,
                List.of(
                        new Procedure.Argument(SCHEMA_NAME, VARCHAR),
                        new Procedure.Argument(TABLE_NAME, VARCHAR),
                        new Procedure.Argument(TABLE_LOCATION, VARCHAR)),
                REGISTER_TABLE.bindTo(this));
    }

    public void registerTable(
            ConnectorSession clientSession,
            String schemaName,
            String tableName,
            String tableLocation)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doRegisterTable(
                    clientSession,
                    schemaName,
                    tableName,
                    tableLocation);
        }
    }

    private void doRegisterTable(
            ConnectorSession session,
            String schemaName,
            String tableName,
            String tableLocation)
    {
        if (!registerTableProcedureEnabled) {
            throw new TrinoException(PERMISSION_DENIED, "register_table procedure is disabled");
        }
        checkProcedureArgument(schemaName != null && !schemaName.isEmpty(), "schema_name cannot be null or empty");
        checkProcedureArgument(tableName != null && !tableName.isEmpty(), "table_name cannot be null or empty");
        checkProcedureArgument(tableLocation != null && !tableLocation.isEmpty(), "table_location cannot be null or empty");

        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        DeltaLakeMetastore metastore = metadataFactory.create(session.getIdentity()).getMetastore();

        if (!metastore.getDatabase(schemaName).isPresent()) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaTableName.getSchemaName()));
        }

        if (metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName()).isPresent()) {
            throw new TrinoException(TABLE_ALREADY_EXISTS, format("Table '%s' already exists", schemaTableName));
        }

        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        validateLocation(fileSystem, tableLocation);

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setOwner(Optional.of(session.getUser()))
                .setTableType(EXTERNAL_TABLE.name()) // Always create External table
                .setDataColumns(DUMMY_DATA_COLUMNS)
                .setParameters(deltaTableProperties(session, tableLocation, true));

        setDeltaStorageFormat(tableBuilder, tableLocation, new Path(tableLocation));
        Table table = tableBuilder.build();

        PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(table.getOwner().orElseThrow());
        metastore.createTable(
                session,
                table,
                principalPrivileges);
    }

    private void validateLocation(TrinoFileSystem fileSystem, String tableLocation)
    {
        Path transactionLogDir = getTransactionLogDir(new Path(tableLocation));
        locationExists(fileSystem, transactionLogDir.toString());
        transactionLogExists(fileSystem, transactionLogDir.toString());
    }

    private void locationExists(TrinoFileSystem fileSystem, String location)
    {
        try {
            if (!fileSystem.newInputFile(location).exists()) {
                throw new TrinoException(GENERIC_USER_ERROR, format("Location %s does not exist", location));
            }
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, format("Failed checking table location %s", location), e);
        }
    }

    private void transactionLogExists(TrinoFileSystem fileSystem, String transactionLogDir)
    {
        try {
            if (!fileSystem.listFiles(transactionLogDir).hasNext()) {
                throw new TrinoException(GENERIC_USER_ERROR, format("No transaction log found in delta log directory %s", transactionLogDir));
            }
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, format("Failed checking transaction log directory %s", transactionLogDir), e);
        }
    }
}
