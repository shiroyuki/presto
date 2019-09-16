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
package io.prestosql.plugin.hive.metastore.thrift;

import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.authentication.HiveContext;
import io.prestosql.plugin.hive.metastore.HivePrincipal;
import io.prestosql.plugin.hive.metastore.HivePrivilegeInfo;
import io.prestosql.plugin.hive.metastore.PartitionWithStatistics;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;

public interface ThriftMetastore
{
    void createDatabase(HiveContext context, Database database);

    void dropDatabase(HiveContext context, String databaseName);

    void alterDatabase(HiveContext context, String databaseName, Database database);

    void createTable(HiveContext context, Table table);

    void dropTable(HiveContext context, String databaseName, String tableName, boolean deleteData);

    void alterTable(HiveContext context, String databaseName, String tableName, Table table);

    List<String> getAllDatabases();

    List<String> getAllTables(String databaseName);

    List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue);

    List<String> getAllViews(String databaseName);

    Optional<Database> getDatabase(String databaseName);

    void addPartitions(HiveContext context, String databaseName, String tableName, List<PartitionWithStatistics> partitions);

    void dropPartition(HiveContext context, String databaseName, String tableName, List<String> parts, boolean deleteData);

    void alterPartition(HiveContext context, String databaseName, String tableName, PartitionWithStatistics partition);

    Optional<List<String>> getPartitionNames(HiveContext context, String databaseName, String tableName);

    Optional<List<String>> getPartitionNamesByParts(HiveContext context, String databaseName, String tableName, List<String> parts);

    Optional<Partition> getPartition(HiveContext context, String databaseName, String tableName, List<String> partitionValues);

    List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames);

    Optional<Table> getTable(HiveContext context, String databaseName, String tableName);

    Set<ColumnStatisticType> getSupportedColumnStatistics(Type type);

    PartitionStatistics getTableStatistics(HiveContext context, String databaseName, String tableName);

    Map<String, PartitionStatistics> getPartitionStatistics(HiveContext context, String databaseName, String tableName, Set<String> partitionNames);

    void updateTableStatistics(HiveContext context, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update);

    void updatePartitionStatistics(HiveContext context, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update);

    void createRole(HiveContext context, String role, String grantor);

    void dropRole(HiveContext context, String role);

    Set<String> listRoles();

    void grantRoles(HiveContext context, Set<String> roles, Set<HivePrincipal> grantees, boolean withAdminOption, HivePrincipal grantor);

    void revokeRoles(HiveContext context, Set<String> roles, Set<HivePrincipal> grantees, boolean adminOptionFor, HivePrincipal grantor);

    Set<RoleGrant> listRoleGrants(HivePrincipal principal);

    void grantTablePrivileges(HiveContext context, String databaseName, String tableName, String tableOwner, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges);

    void revokeTablePrivileges(HiveContext context, String databaseName, String tableName, String tableOwner, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges);

    Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal principal);

    default Optional<List<FieldSchema>> getFields(HiveContext context, String databaseName, String tableName)
    {
        Optional<Table> table = getTable(context, databaseName, tableName);
        if (!table.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }

        if (table.get().getSd() == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Table is missing storage descriptor");
        }

        return Optional.of(table.get().getSd().getCols());
    }
}
