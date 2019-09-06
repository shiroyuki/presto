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

public interface HiveMetastore
{
    Optional<Database> getDatabase(String databaseName);

    List<String> getAllDatabases();

    Optional<Table> getTable(String databaseName, String tableName);

    Set<ColumnStatisticType> getSupportedColumnStatistics(Type type);

    PartitionStatistics getTableStatistics(String databaseName, String tableName);

    Map<String, PartitionStatistics> getPartitionStatistics(String databaseName, String tableName, Set<String> partitionNames);

    void updateTableStatistics(HiveContext context, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update);

    void updatePartitionStatistics(HiveContext context, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update);

    List<String> getAllTables(String databaseName);

    List<String> getAllViews(String databaseName);

    void createDatabase(HiveContext context, Database database);

    void dropDatabase(HiveContext context, String databaseName);

    void renameDatabase(HiveContext context, String databaseName, String newDatabaseName);

    void createTable(HiveContext context, Table table, PrincipalPrivileges principalPrivileges);

    void dropTable(HiveContext context, String databaseName, String tableName, boolean deleteData);

    /**
     * This should only be used if the semantic here is drop and add. Trying to
     * alter one field of a table object previously acquired from getTable is
     * probably not what you want.
     */
    void replaceTable(HiveContext context, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges);

    void renameTable(HiveContext context, String databaseName, String tableName, String newDatabaseName, String newTableName);

    void commentTable(HiveContext context, String databaseName, String tableName, Optional<String> comment);

    void addColumn(HiveContext context, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment);

    void renameColumn(HiveContext context, String databaseName, String tableName, String oldColumnName, String newColumnName);

    void dropColumn(HiveContext context, String databaseName, String tableName, String columnName);

    Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues);

    Optional<List<String>> getPartitionNames(String databaseName, String tableName);

    Optional<List<String>> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts);

    Map<String, Optional<Partition>> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames);

    void addPartitions(HiveContext context, String databaseName, String tableName, List<PartitionWithStatistics> partitions);

    void dropPartition(HiveContext context, String databaseName, String tableName, List<String> parts, boolean deleteData);

    void alterPartition(HiveContext context, String databaseName, String tableName, PartitionWithStatistics partition);

    void createRole(HiveContext context, String role, String grantor);

    void dropRole(HiveContext context, String role);

    Set<String> listRoles();

    void grantRoles(HiveContext context, Set<String> roles, Set<HivePrincipal> grantees, boolean withAdminOption, HivePrincipal grantor);

    void revokeRoles(HiveContext context, Set<String> roles, Set<HivePrincipal> grantees, boolean adminOptionFor, HivePrincipal grantor);

    Set<RoleGrant> listRoleGrants(HivePrincipal principal);

    void grantTablePrivileges(HiveContext context, String databaseName, String tableName, String tableOwner, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges);

    void revokeTablePrivileges(HiveContext context, String databaseName, String tableName, String tableOwner, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges);

    Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal principal);
}
