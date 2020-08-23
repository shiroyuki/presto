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
package io.prestosql.plugin.sqlserver;

import com.google.common.base.Joiner;
import io.airlift.slice.Slice;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.PredicatePushdownController;
import io.prestosql.plugin.jdbc.PredicatePushdownController.DomainPushdownResult;
import io.prestosql.plugin.jdbc.SliceWriteFunction;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.plugin.jdbc.ColumnMapping.DISABLE_PUSHDOWN;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class SqlServerClient
        extends BaseJdbcClient
{
    private static final Joiner DOT_JOINER = Joiner.on(".");

    // SqlServer supports 2100 parameters in prepared statement, let's create a space for about 4 big IN predicates
    private static final int SQL_SERVER_MAX_LIST_EXPRESSIONS = 500;

    private static final PredicatePushdownController SIMPLIFY_UNSUPPORTED_PUSHDOWN = domain -> {
        Domain pushedDown = domain;
        if (domain.getValues().getRanges().getRangeCount() > SQL_SERVER_MAX_LIST_EXPRESSIONS) {
            pushedDown = domain.simplify();
        }
        // TODO (https://github.com/prestosql/presto/issues/4596) eliminate filter above table scan
        return new DomainPushdownResult(pushedDown, domain);
    };

    @Inject
    public SqlServerClient(BaseJdbcConfig config, ConnectionFactory connectionFactory)
    {
        super(config, "\"", connectionFactory);
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        if (!schemaName.equals(newTable.getSchemaName())) {
            throw new PrestoException(NOT_SUPPORTED, "Table rename across schemas is not supported");
        }

        String sql = format(
                "sp_rename %s, %s",
                singleQuote(catalogName, schemaName, tableName),
                singleQuote(newTable.getTableName()));
        execute(identity, sql);
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        String sql = format(
                "sp_rename %s, %s, 'COLUMN'",
                singleQuote(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName(), jdbcColumn.getColumnName()),
                singleQuote(newColumnName));
        execute(identity, sql);
    }

    @Override
    protected void copyTableSchema(Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String sql = format(
                "SELECT %s INTO %s FROM %s WHERE 0 = 1",
                columnNames.stream()
                        .map(this::quoted)
                        .collect(joining(", ")),
                quoted(catalogName, schemaName, newTableName),
                quoted(catalogName, schemaName, tableName));
        execute(connection, sql);
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        if (jdbcTypeName.equals("varbinary")) {
            return Optional.of(varbinaryColumnMapping());
        }

        // TODO (https://github.com/prestosql/presto/issues/4593) implement proper type mapping
        return super.toPrestoType(session, connection, typeHandle)
                .map(columnMapping -> new ColumnMapping(
                        columnMapping.getType(),
                        columnMapping.getReadFunction(),
                        columnMapping.getWriteFunction(),
                        SIMPLIFY_UNSUPPORTED_PUSHDOWN));
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("bit", booleanWriteFunction());
        }

        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded() || varcharType.getBoundedLength() > 4000) {
                dataType = "nvarchar(max)";
            }
            else {
                dataType = "nvarchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            String dataType;
            if (charType.getLength() > 4000) {
                dataType = "nvarchar(max)";
            }
            else {
                dataType = "nchar(" + charType.getLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, charWriteFunction());
        }

        if (type instanceof VarbinaryType) {
            return WriteMapping.sliceMapping("varbinary(max)", varbinaryWriteFunction());
        }

        // TODO implement proper type mapping
        return super.toWriteMapping(session, type);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> {
            String start = "SELECT ";
            checkArgument(sql.startsWith(start));
            return "SELECT TOP " + limit + " " + sql.substring(start.length());
        });
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    private static String singleQuote(String... objects)
    {
        return singleQuote(DOT_JOINER.join(objects));
    }

    private static String singleQuote(String literal)
    {
        return "\'" + literal + "\'";
    }

    public static ColumnMapping varbinaryColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                VARBINARY,
                (resultSet, columnIndex) -> wrappedBuffer(resultSet.getBytes(columnIndex)),
                varbinaryWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    private static SliceWriteFunction varbinaryWriteFunction()
    {
        return new SliceWriteFunction() {
            @Override
            public void set(PreparedStatement statement, int index, Slice value)
                    throws SQLException
            {
                statement.setBytes(index, value.getBytes());
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                statement.setBytes(index, null);
            }
        };
    }
}
