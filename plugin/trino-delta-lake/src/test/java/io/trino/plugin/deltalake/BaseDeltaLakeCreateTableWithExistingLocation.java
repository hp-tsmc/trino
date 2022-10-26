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

import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public abstract class BaseDeltaLakeCreateTableWithExistingLocation
        extends AbstractTestQueryFramework
{
    protected static final String SCHEMA = "test_tables_create_table_with_existing_location" + randomTableSuffix();
    protected static final String CATALOG_NAME = "delta_create_table_with_existing_location";
    protected File metastoreDir;
    protected HiveMetastore metastore;
    protected HdfsContext hdfsContext;

    @Test
    public void testCreateTableWithExistingLocation()
    {
        String tableName = "test_create_table_" + randomTableSuffix();

        assertQuerySucceeds(format("CREATE TABLE %s AS SELECT 1 as a, 'INDIA' as b, true as c", tableName));
        assertThat(query(format("SELECT * FROM %s", tableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true')");

        String tableLocation = (String) computeScalar("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM " + tableName);

        assertQuerySucceeds(format("CREATE TABLE %s.%s.%s (dummy int) with (location = '%s')", CATALOG_NAME, SCHEMA, tableName + "_new", tableLocation));
        metastore.dropTable(SCHEMA, tableName + "_new", false);
        assertUpdate(format("DROP TABLE %s", tableName));
        assertThat(metastore.getTable(SCHEMA, tableName)).as("Table should be dropped from metastore").isEmpty();
        assertThat(metastore.getTable(SCHEMA, tableName + "_new")).as("Table should be dropped from metastore").isEmpty();
    }
}
