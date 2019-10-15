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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestFakerRecordSetProvider
{
    private static final URI CSV_URI = URI.create("http://s3.amazonaws.com/presto-example/v2/numbers-1.csv");

    @Test
    public void testGetRecordSet()
    {
        FakerRecordSetProvider recordSetProvider = new FakerRecordSetProvider(new FakerConnectorId("test"));
        RecordSet recordSet = recordSetProvider.getRecordSet(FakerTransactionHandle.INSTANCE, SESSION, new FakerSplit("test", "csv", CSV_URI.toString()), ImmutableList.of(
                new FakerColumnHandle("test", "text", createUnboundedVarcharType(), 0),
                new FakerColumnHandle("test", "value", createUnboundedVarcharType(), 1)));
        assertNotNull(recordSet, "recordSet is null");

        RecordCursor cursor = recordSet.cursor();
        assertNotNull(cursor, "cursor is null");

        Map<String, String> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getSlice(1).toStringUtf8());
        }
        assertEquals(data, ImmutableMap.<String, String>builder()
                .put("two", "2")
                .put("three", "3")
                .build());
    }
}
