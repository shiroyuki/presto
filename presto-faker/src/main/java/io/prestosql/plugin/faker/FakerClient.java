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
package io.prestosql.plugin.faker;

import io.prestosql.plugin.faker.operator.FilePlugin;
import io.prestosql.plugin.faker.operator.PluginFactory;

import javax.inject.Inject;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class FakerClient
{
    @Inject
    public FakerClient(FakerConfig config)
    {
        requireNonNull(config, "config is null");
    }

    public List<String> getSchemaNames()
    {
        return Stream.of(FakerType.values())
                .map(FakerType::toString)
                .collect(Collectors.toList());
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        return new HashSet<>();
    }

    public FlexTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");

        FilePlugin plugin = PluginFactory.create(schema);
        List<FakerColumn> columns = plugin.getFields(schema, tableName);
        return new FlexTable(tableName, columns);
    }
}
