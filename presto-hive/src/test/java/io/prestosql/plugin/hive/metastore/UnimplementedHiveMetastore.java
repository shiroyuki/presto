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

import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.authentication.HiveContext;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

class UnimplementedHiveMetastore
        implements HiveMetastore
{
    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getAllDatabases()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Table> getTable(HiveContext context, String databaseName, String tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionStatistics getTableStatistics(HiveContext context, String databaseName, String tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(HiveContext context, String databaseName, String tableName, Set<String> partitionNames)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateTableStatistics(HiveContext context, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updatePartitionStatistics(HiveContext context, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createDatabase(HiveContext context, Database database)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(HiveContext context, String databaseName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameDatabase(HiveContext context, String databaseName, String newDatabaseName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(HiveContext context, Table table, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(HiveContext context, String databaseName, String tableName, boolean deleteData)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceTable(HiveContext context, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(HiveContext context, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commentTable(HiveContext context, String databaseName, String tableName, Optional<String> comment)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addColumn(HiveContext context, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameColumn(HiveContext context, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropColumn(HiveContext context, String databaseName, String tableName, String columnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Partition> getPartition(HiveContext context, String databaseName, String tableName, List<String> partitionValues)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<List<String>> getPartitionNames(HiveContext context, String databaseName, String tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<List<String>> getPartitionNamesByParts(HiveContext context, String databaseName, String tableName, List<String> parts)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(HiveContext context, String databaseName, String tableName, List<String> partitionNames)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addPartitions(HiveContext context, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(HiveContext context, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(HiveContext context, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal prestoPrincipal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantTablePrivileges(HiveContext context, String databaseName, String tableName, String tableOwner, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeTablePrivileges(HiveContext context, String databaseName, String tableName, String tableOwner, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createRole(HiveContext context, String role, String grantor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropRole(HiveContext context, String role)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> listRoles()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantRoles(HiveContext context, Set<String> roles, Set<HivePrincipal> grantees, boolean withAdminOption, HivePrincipal grantor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeRoles(HiveContext context, Set<String> roles, Set<HivePrincipal> grantees, boolean adminOptionFor, HivePrincipal grantor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        throw new UnsupportedOperationException();
    }
}
