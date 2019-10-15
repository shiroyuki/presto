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

import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestFakerSplit
{
    private final FakerSplit split = new FakerSplit("connectorId", "schemaName", "tableName");

    @Test
    public void testAddresses()
    {
        // http split with default port
        FakerSplit httpSplit = new FakerSplit("connectorId", "schemaName", "tableName");
        assertEquals(httpSplit.isRemotelyAccessible(), true);

        // http split with custom port
        httpSplit = new FakerSplit("connectorId", "schemaName", "tableName");
        assertEquals(httpSplit.isRemotelyAccessible(), true);

        // http split with default port
        FakerSplit httpsSplit = new FakerSplit("connectorId", "schemaName", "tableName");
        assertEquals(httpsSplit.isRemotelyAccessible(), true);

        // http split with custom port
        httpsSplit = new FakerSplit("connectorId", "schemaName", "tableName");
        assertEquals(httpsSplit.isRemotelyAccessible(), true);
    }

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<FakerSplit> codec = jsonCodec(FakerSplit.class);
        String json = codec.toJson(split);
        FakerSplit copy = codec.fromJson(json);
        assertEquals(copy.getConnectorId(), split.getConnectorId());
        assertEquals(copy.getSchemaName(), split.getSchemaName());
        assertEquals(copy.getTableName(), split.getTableName());

        assertEquals(copy.isRemotelyAccessible(), true);
    }
}
