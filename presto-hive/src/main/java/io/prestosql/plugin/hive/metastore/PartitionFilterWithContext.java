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
package io.prestosql.plugin.hive.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.authentication.HiveContext;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.plugin.hive.metastore.HiveTableName.hiveTableName;
import static io.prestosql.plugin.hive.metastore.HiveTableNameWithContext.hiveTableNameWithContext;
import static java.util.Objects.requireNonNull;

@Immutable
public class PartitionFilterWithContext
{
    private final HiveTableNameWithContext hiveTableName;
    private final List<String> parts;

    @JsonCreator
    public PartitionFilterWithContext(@JsonProperty("hiveTableName") HiveTableNameWithContext hiveTableName, @JsonProperty("parts") List<String> parts)
    {
        this.hiveTableName = requireNonNull(hiveTableName, "hiveTableName is null");
        this.parts = ImmutableList.copyOf(requireNonNull(parts, "parts is null"));
    }

    public static PartitionFilterWithContext partitionFilterWithContext(HiveContext context, String databaseName, String tableName, List<String> parts)
    {
        return new PartitionFilterWithContext(hiveTableNameWithContext(context, databaseName, tableName), parts);
    }

    @JsonProperty
    public HiveTableNameWithContext getHiveTableName()
    {
        return hiveTableName;
    }

    @JsonProperty
    public List<String> getParts()
    {
        return parts;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hiveTableName", hiveTableName)
                .add("parts", parts)
                .toString();
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

        PartitionFilterWithContext other = (PartitionFilterWithContext) o;
        return Objects.equals(hiveTableName, other.hiveTableName) &&
                Objects.equals(parts, other.parts);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(hiveTableName, parts);
    }
}
