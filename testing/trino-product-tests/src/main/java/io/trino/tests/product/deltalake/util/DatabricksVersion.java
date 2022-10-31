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
package io.trino.tests.product.deltalake.util;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DatabricksVersion
        implements Comparable<DatabricksVersion>
{
    public static final DatabricksVersion DATABRICKS_113_RUNTIME_VERSION = new DatabricksVersion(11, 3);
    public static final DatabricksVersion DATABRICKS_104_RUNTIME_VERSION = new DatabricksVersion(10, 4);
    public static final DatabricksVersion DATABRICKS_91_RUNTIME_VERSION = new DatabricksVersion(9, 1);
    public static final DatabricksVersion UNKNOWN_RUNTIME_VERSION = new DatabricksVersion(-1, -1);

    private static final Pattern DATABRICKS_VERSION_PATTERN = Pattern.compile("(\\d+)\\.(\\d+)");

    private final int majorVersion;
    private final int minorVersion;

    public DatabricksVersion(int majorVersion, int minorVersion)
    {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
    }

    @Override
    public int compareTo(DatabricksVersion other)
    {
        if (majorVersion < other.majorVersion) {
            return -1;
        }
        if (majorVersion > other.majorVersion) {
            return 1;
        }

        return Integer.compare(minorVersion, other.minorVersion);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatabricksVersion that = (DatabricksVersion) o;
        return majorVersion == that.majorVersion && minorVersion == that.minorVersion;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(majorVersion, minorVersion);
    }

    @Override
    public String toString()
    {
        return majorVersion + "." + minorVersion;
    }

    public static DatabricksVersion createFromString(String versionString)
    {
        Matcher matcher = DATABRICKS_VERSION_PATTERN.matcher(versionString);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Cannot parse Databricks version " + versionString);
        }
        int majorVersion = Integer.parseInt(matcher.group(1));
        int minorVersion = Integer.parseInt(matcher.group(2));
        return new DatabricksVersion(majorVersion, minorVersion);
    }
}
