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
package io.prestosql.type;

import io.airlift.slice.Slice;
import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.prestosql.type.JsonType.JSON;
import static java.lang.String.format;

public class TestArrayOperatorsLegacy
        extends AbstractTestFunctions
{
    public TestArrayOperatorsLegacy() {}

    @BeforeClass
    public void setUp()
    {
        registerScalar(getClass());
    }

    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.JSON)
    public static Slice uncheckedToJson(@SqlType("varchar(x)") Slice slice)
    {
        return slice;
    }

    @Test
    public void testArrayToJson()
    {
        assertFunction("cast(cast (null as ARRAY<BIGINT>) AS JSON)", JSON, null);
        assertFunction("cast(ARRAY[] AS JSON)", JSON, "[]");
        assertFunction("cast(ARRAY[null, null] AS JSON)", JSON, "[null,null]");

        assertFunction("cast(ARRAY[true, false, null] AS JSON)", JSON, "[true,false,null]");

        assertFunction("cast(cast(ARRAY[1, 2, null] AS ARRAY<TINYINT>) AS JSON)", JSON, "[1,2,null]");
        assertFunction("cast(cast(ARRAY[12345, -12345, null] AS ARRAY<SMALLINT>) AS JSON)", JSON, "[12345,-12345,null]");
        assertFunction("cast(cast(ARRAY[123456789, -123456789, null] AS ARRAY<INTEGER>) AS JSON)", JSON, "[123456789,-123456789,null]");
        assertFunction("cast(cast(ARRAY[1234567890123456789, -1234567890123456789, null] AS ARRAY<BIGINT>) AS JSON)", JSON, "[1234567890123456789,-1234567890123456789,null]");

        assertFunction("CAST(CAST(ARRAY[3.14E0, nan(), infinity(), -infinity(), null] AS ARRAY<REAL>) AS JSON)", JSON, "[3.14,\"NaN\",\"Infinity\",\"-Infinity\",null]");
        assertFunction("CAST(ARRAY[3.14E0, 1e-323, 1e308, nan(), infinity(), -infinity(), null] AS JSON)", JSON, "[3.14,1.0E-323,1.0E308,\"NaN\",\"Infinity\",\"-Infinity\",null]");
        assertFunction("CAST(ARRAY[DECIMAL '3.14', null] AS JSON)", JSON, "[3.14,null]");
        assertFunction("CAST(ARRAY[DECIMAL '12345678901234567890.123456789012345678', null] AS JSON)", JSON, "[12345678901234567890.123456789012345678,null]");

        assertFunction("cast(ARRAY['a', 'bb', null] AS JSON)", JSON, "[\"a\",\"bb\",null]");
        assertFunction(
                "cast(ARRAY[JSON '123', JSON '3.14', JSON 'false', JSON '\"abc\"', JSON '[1, \"a\", null]', JSON '{\"a\": 1, \"b\": \"str\", \"c\": null}', JSON 'null', null] AS JSON)",
                JSON,
                "[123,3.14,false,\"abc\",[1,\"a\",null],{\"a\":1,\"b\":\"str\",\"c\":null},null,null]");

        assertFunction(
                "CAST(ARRAY[TIMESTAMP '1970-01-01 00:00:01', null] AS JSON)",
                JSON,
                format("[\"%s\",null]", sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0, TEST_SESSION)));
        assertFunction(
                "CAST(ARRAY[DATE '2001-08-22', DATE '2001-08-23', null] AS JSON)",
                JSON,
                "[\"2001-08-22\",\"2001-08-23\",null]");

        assertFunction(
                "cast(ARRAY[ARRAY[1, 2], ARRAY[3, null], ARRAY[], ARRAY[null, null], null] AS JSON)",
                JSON,
                "[[1,2],[3,null],[],[null,null],null]");
        assertFunction(
                "cast(ARRAY[MAP(ARRAY['b', 'a'], ARRAY[2, 1]), MAP(ARRAY['three', 'none'], ARRAY[3, null]), MAP(), MAP(ARRAY['h2', 'h1'], ARRAY[null, null]), null] AS JSON)",
                JSON,
                "[{\"a\":1,\"b\":2},{\"none\":null,\"three\":3},{},{\"h1\":null,\"h2\":null},null]");
        assertFunction(
                "cast(ARRAY[ROW(1, 2), ROW(3, CAST(null as INTEGER)), CAST(ROW(null, null) AS ROW(INTEGER, INTEGER)), null] AS JSON)",
                JSON,
                "[[1,2],[3,null],[null,null],null]");
        assertFunction("CAST(ARRAY [12345.12345, 12345.12345, 3.0] AS JSON)", JSON, "[12345.12345,12345.12345,3.00000]");
        assertFunction(
                "CAST(ARRAY [123456789012345678901234567890.87654321, 123456789012345678901234567890.12345678] AS JSON)",
                JSON,
                "[123456789012345678901234567890.87654321,123456789012345678901234567890.12345678]");
    }
}
