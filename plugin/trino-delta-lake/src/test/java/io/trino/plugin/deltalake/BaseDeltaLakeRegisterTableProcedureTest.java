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
package io.trino.plugin.deltalake;

import io.trino.hdfs.HdfsContext;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteDirectoryContents;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public abstract class BaseDeltaLakeRegisterTableProcedureTest
        extends AbstractTestQueryFramework
{
    protected static final String SCHEMA = "test_tables_register_table" + randomTableSuffix();
    protected static final String CATALOG_NAME = "delta_register_table";
    protected File metastoreDir;
    protected HiveMetastore metastore;
    protected HdfsContext hdfsContext;

    @Test
    public void testRegisterTableWithTableLocation()
    {
        String tableName = "test_register_table_with_table_location" + randomTableSuffix();

        assertQuerySucceeds(format("CREATE TABLE %s AS SELECT 1 as a, 'INDIA' as b, true as c", tableName));
        assertThat(query(format("SELECT * FROM %s", tableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true')");

        String tableLocation = getTableLocation(tableName);
        String showCreateTableOld = (String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue();

        // Drop table from hive metastore and use the same table name to register again with the transaction logs
        dropTableFromMetastore(tableName);

        assertQuerySucceeds(format("CALL %s.system.register_table('%s', '%s', '%s')", CATALOG_NAME, SCHEMA, tableName, tableLocation));
        String showCreateTableNew = (String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue();
        assertThat(showCreateTableOld).isEqualTo(showCreateTableNew);
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testRegisterTableWithComments()
    {
        String tableName = "test_register_table_with_comments_" + randomTableSuffix();

        assertQuerySucceeds(format("CREATE TABLE %s (a, b, c) COMMENT 'my-table-comment' AS VALUES (1, 'INDIA', true), (2, 'USA', false)", tableName));
        assertThat(query(format("SELECT * FROM %s", tableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false')");

        String tableLocation = getTableLocation(tableName);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(tableName);

        assertQuerySucceeds("CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");
        assertThat(getTableComment(tableName)).isEqualTo("my-table-comment");

        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testRegisterTableWithDifferentTableName()
    {
        String tableName = "test_register_table_with_different_table_name_old_" + randomTableSuffix();

        assertQuerySucceeds(format("CREATE TABLE %s AS SELECT 1 as a, 'INDIA' as b, true as c", tableName));
        assertThat(query(format("SELECT * FROM %s", tableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true')");

        String showCreateTableOld = (String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue();
        String tableLocation = getTableLocation(tableName);
        // Drop table from hive metastore and use the same table name to register again with the transaction logs
        dropTableFromMetastore(tableName);

        String tableNameNew = "test_register_table_with_different_table_name_new_" + randomTableSuffix();
        assertQuerySucceeds(format("CALL %s.system.register_table('%s', '%s', '%s')", CATALOG_NAME, SCHEMA, tableNameNew, tableLocation));
        String showCreateTableNew = (String) computeActual("SHOW CREATE TABLE " + tableNameNew).getOnlyValue();
        assertThat(showCreateTableOld).isEqualTo(showCreateTableNew.replaceFirst(tableNameNew, tableName));
        assertUpdate(format("DROP TABLE %s", tableNameNew));
    }

    @Test
    public void testRegisterTableWithDroppedTable()
    {
        String tableName = "test_register_table_with_dropped_table_" + randomTableSuffix();

        assertQuerySucceeds(format("CREATE TABLE %s AS SELECT 1 as a, 'INDIA' as b, true as c", tableName));
        assertThat(query(format("SELECT * FROM %s", tableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true')");

        String tableLocation = getTableLocation(tableName);
        // Drop table to verify register_table call fails when no transaction log can be found (table doesn't exist)
        assertUpdate(format("DROP TABLE %s", tableName));

        String tableNameNew = "test_register_table_with_dropped_table_new_" + randomTableSuffix();
        assertQueryFails(format("CALL %s.system.register_table('%s', '%s', '%s')", CATALOG_NAME, SCHEMA, tableNameNew, tableLocation),
                ".*Location (.*) does not exist.*");
    }

    @Test
    public void testRegisterTableWithNoTransactionLog()
            throws IOException
    {
        String tableName = "test_register_table_with_no_transaction_log_" + randomTableSuffix();

        assertQuerySucceeds(format("CREATE TABLE %s AS SELECT 1 as a, 'INDIA' as b, true as c", tableName));
        assertThat(query(format("SELECT * FROM %s", tableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true')");

        String tableLocation = getTableLocation(tableName);
        String tableNameNew = "test_register_table_with_no_transaction_log_new_" + randomTableSuffix();

        // Delete files under transaction log directory to verify register_table call fails
        deleteDirectoryContents(Path.of(getTransactionLogDir(new org.apache.hadoop.fs.Path(tableLocation)).toString()), ALLOW_INSECURE);

        assertQueryFails(format("CALL %s.system.register_table('%s', '%s', '%s')", CATALOG_NAME, SCHEMA, tableNameNew, tableLocation),
                ".*No transaction log found in delta log directory.*");

        dropTableFromMetastore(tableName);
        deleteRecursively(Path.of(tableLocation), ALLOW_INSECURE);
    }

    @Test
    public void testRegisterTableWithNonExistingTableLocation()
    {
        String tableName = "test_register_table_with_non_existing_table_location_" + randomTableSuffix();
        String tableLocation = "/test/delta-lake/hive/warehouse/orders_5-581fad8517934af6be1857a903559d44";
        assertQueryFails(format("CALL %s.system.register_table('%s', '%s', '%s')", CATALOG_NAME, SCHEMA, tableName, tableLocation),
                ".*Location (.*) does not exist.*");
    }

    @Test
    public void testRegisterTableWithNonExistingSchema()
    {
        String tableLocation = "/test/delta-lake/hive/warehouse/orders_5-581fad8517934af6be1857a903559d44";
        assertQueryFails(format("CALL %s.system.register_table('%s', '%s', '%s')", CATALOG_NAME, SCHEMA + "_new", "delta_table_1", tableLocation),
                ".*Schema '(.*)' does not exist.*");
    }

    @Test
    public void testRegisterTableWithExistingTable()
    {
        String tableName = "test_register_table_with_existing_table_" + randomTableSuffix();

        assertQuerySucceeds(format("CREATE TABLE %s AS SELECT 1 as a, 'INDIA' as b, true as c", tableName));
        assertThat(query(format("SELECT * FROM %s", tableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true')");
        String tableLocation = getTableLocation(tableName);

        assertQueryFails(format("CALL %s.system.register_table('%s', '%s', '%s')", CATALOG_NAME, SCHEMA, tableName, tableLocation),
                ".*Table '(.*)' already exists.*");
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testRegisterTableWithInvalidURIScheme()
    {
        String tableName = "test_register_table_with_invalid_uri_scheme_" + randomTableSuffix();
        String tableLocation = "invalid://hadoop-master:9000/test/delta-lake/hive/orders_5-581fad8517934af6be1857a903559d44";
        assertQueryFails(format("CALL %s.system.register_table('%s', '%s', '%s')", CATALOG_NAME, SCHEMA, tableName, tableLocation),
                ".*Failed checking table location (.*)");
    }

    @Test
    public void testRegisterTableWithInvalidParameter()
    {
        String tableName = "test_register_table_with_invalid_parameter_" + randomTableSuffix();
        String tableLocation = "/test/delta-lake/hive/table1/";

        assertQueryFails(format("CALL %s.system.register_table('%s', '%s')", CATALOG_NAME, SCHEMA, tableName),
                ".*'TABLE_LOCATION' is missing.*");
        assertQueryFails(format("CALL %s.system.register_table('%s')", CATALOG_NAME, SCHEMA),
                ".*'TABLE_NAME' is missing.*");
        assertQueryFails(format("CALL %s.system.register_table()", CATALOG_NAME),
                ".*'SCHEMA_NAME' is missing.*");

        assertQueryFails(format("CALL %s.system.register_table(null, '%s', '%s')", CATALOG_NAME, tableName, tableLocation),
                ".*schema_name cannot be null or empty.*");
        assertQueryFails(format("CALL %s.system.register_table('%s', null, '%s')", CATALOG_NAME, SCHEMA, tableLocation),
                ".*table_name cannot be null or empty.*");
        assertQueryFails(format("CALL %s.system.register_table('%s', '%s', null)", CATALOG_NAME, SCHEMA, tableName),
                ".*table_location cannot be null or empty.*");

        assertQueryFails(format("CALL %s.system.register_table('', '%s', '%s')", CATALOG_NAME, tableName, tableLocation),
                ".*schema_name cannot be null or empty.*");
        assertQueryFails(format("CALL %s.system.register_table('%s', '', '%s')", CATALOG_NAME, SCHEMA, tableLocation),
                ".*table_name cannot be null or empty.*");
        assertQueryFails(format("CALL %s.system.register_table('%s', '%s', '')", CATALOG_NAME, SCHEMA, tableName),
                ".*table_location cannot be null or empty.*");
    }

    private String getTableLocation(String tableName)
    {
        return (String) computeScalar("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM " + tableName);
    }

    private void dropTableFromMetastore(String tableName)
    {
        metastore.dropTable(SCHEMA, tableName, false);
        tableNotExist(tableName);
    }

    private void tableNotExist(String tableName)
    {
        assertThat(metastore.getTable(SCHEMA, tableName)).as("Table in metastore should be dropped").isEmpty();
    }

    private String getTableComment(String tableName)
    {
        return (String) computeScalar("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = '" + CATALOG_NAME + "' AND schema_name = '" + getSession().getSchema().orElseThrow() + "' AND table_name = '" + tableName + "'");
    }
}
