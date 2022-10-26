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
import io.trino.plugin.hive.metastore.Table;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class BaseDeltaLakeTableWithCustomLocation
        extends AbstractTestQueryFramework
{
    protected static final String SCHEMA = "test_tables_with_custom_location" + randomTableSuffix();
    protected static final String CATALOG_NAME = "delta_with_custom_location";
    protected File metastoreDir;
    protected HiveMetastore metastore;
    protected HdfsContext hdfsContext;

    @Test
    public void testTableHasUuidSuffixInLocation()
    {
        String tableName = "table_with_uuid" + randomTableSuffix();
        assertQuerySucceeds(format("CREATE TABLE %s AS SELECT 1 as val", tableName));
        Optional<Table> table = metastore.getTable(SCHEMA, tableName);
        assertTrue(table.isPresent(), "Table should exists");
        String location = table.get().getStorage().getLocation();
        assertThat(location).matches(format(".*%s-[0-9a-f]{32}", tableName));
    }

    @Test
    public void testCreateAndDrop()
            throws IOException
    {
        String tableName = "test_create_and_drop" + randomTableSuffix();
        assertQuerySucceeds(format("CREATE TABLE %s AS SELECT 1 as val", tableName));
        Table table = metastore.getTable(SCHEMA, tableName).orElseThrow();
        assertThat(table.getTableType()).isEqualTo(TableType.MANAGED_TABLE.name());

        Path tableLocation = new Path(table.getStorage().getLocation());
        FileSystem fileSystem = HDFS_ENVIRONMENT.getFileSystem(hdfsContext, tableLocation);
        assertTrue(fileSystem.exists(tableLocation), "The directory corresponding to the table storage location should exist");
        List<MaterializedRow> materializedRows = computeActual("SELECT \"$path\" FROM " + tableName).getMaterializedRows();
        assertEquals(materializedRows.size(), 1);
        String filePath = (String) materializedRows.get(0).getField(0);
        assertTrue(fileSystem.exists(new Path(filePath)), "The data file should exist");
        assertQuerySucceeds(format("DROP TABLE %s", tableName));
        assertFalse(metastore.getTable(SCHEMA, tableName).isPresent(), "Table should be dropped");
        assertFalse(fileSystem.exists(new Path(filePath)), "The data file should have been removed");
        assertFalse(fileSystem.exists(tableLocation), "The directory corresponding to the dropped Delta Lake table should be removed");
    }

    @Test
    public void testCreateTableWithExistingLocation()
    {
        String tableName = "test_register_table_" + randomTableSuffix();

        assertQuerySucceeds(format("CREATE TABLE %s AS SELECT 1 as a, 'INDIA' as b, true as c", tableName));
        assertThat(query(format("SELECT * FROM %s", tableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true')");

        String tableLocation = (String) computeScalar("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM " + tableName);

        assertQueryFails(format("CREATE TABLE %s.%s.%s (dummy int) with (location = '%s')", CATALOG_NAME, SCHEMA, tableName + "_new", tableLocation),
                ".*Using CREATE TABLE with an existing table is deprecated, instead use the system\\.register_table\\(\\) procedure.*");
        assertUpdate(format("DROP TABLE %s", tableName));
    }
}
