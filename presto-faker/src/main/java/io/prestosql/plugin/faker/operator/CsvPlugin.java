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
package io.prestosql.plugin.faker.operator;

import com.google.common.base.Splitter;
import com.google.common.io.ByteSource;
import io.prestosql.plugin.faker.FakerColumn;
import io.prestosql.spi.PrestoException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CsvPlugin
        implements FilePlugin
{
    private static final Splitter SPLITTER = Splitter.on(",").trimResults();

    @Override
    public List<FakerColumn> getFields(String schema, String table)
    {
        List<FakerColumn> columnTypes = new LinkedList<>();
        List<String> fields = SPLITTER.splitToList(table);
        fields.forEach(field -> columnTypes.add(new FakerColumn(field, VARCHAR)));
        return columnTypes;
    }

    @Override
    public List<String> getIterator(ByteSource byteSource)
    {
        try {
            return byteSource.asCharSource(UTF_8).readLines();
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to get iterator");
        }
    }

    @Override
    public List<String> splitToList(List<String> lines)
    {
        String line = lines.get(0);
        return SPLITTER.splitToList(line);
    }

    @Override
    public boolean skipFirstLine()
    {
        return true;
    }
}
