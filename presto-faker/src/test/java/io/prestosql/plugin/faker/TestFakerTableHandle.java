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
import io.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestFakerTableHandle
{
    private final FakerTableHandle tableHandle = new FakerTableHandle("connectorId", "schemaName", "tableName");

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<FakerTableHandle> codec = jsonCodec(FakerTableHandle.class);
        String json = codec.toJson(tableHandle);
        FakerTableHandle copy = codec.fromJson(json);
        assertEquals(copy, tableHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(new FakerTableHandle("connector", "schema", "table"), new FakerTableHandle("connector", "schema", "table"))
                .addEquivalentGroup(new FakerTableHandle("connectorX", "schema", "table"), new FakerTableHandle("connectorX", "schema", "table"))
                .addEquivalentGroup(new FakerTableHandle("connector", "schemaX", "table"), new FakerTableHandle("connector", "schemaX", "table"))
                .addEquivalentGroup(new FakerTableHandle("connector", "schema", "tableX"), new FakerTableHandle("connector", "schema", "tableX"))
                .check();
    }
}
