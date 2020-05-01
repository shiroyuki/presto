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
package io.prestosql.plugin.kudu;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PrestoException;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static io.prestosql.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunnerTpch;
import static io.prestosql.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestKuduDistributedQueries
        extends AbstractTestQueryFramework
{
    private TestingKuduServer kuduServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        kuduServer = new TestingKuduServer();
        return createKuduQueryRunnerTpch(kuduServer, Optional.of(""), ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        kuduServer.close();
    }

    @Test(dataProvider = "testDataMappingSmokeTestDataProvider")
    public void testDataMappingSmokeTest(DataMappingTestSetup dataMappingTestSetup)
    {
        String prestoTypeName = dataMappingTestSetup.getPrestoTypeName();
        String sampleValueLiteral = dataMappingTestSetup.getSampleValueLiteral();
        String highValueLiteral = dataMappingTestSetup.getHighValueLiteral();

        String tableName = "test_data_mapping_smoke_" + prestoTypeName.replaceAll("[^a-zA-Z0-9]", "_") + "_" + randomTableSuffix();

        Runnable setup = () -> {
            // TODO test with both CTAS *and* CREATE TABLE + INSERT, since they use different connector API methods.
            String createTable = "" +
                    "CREATE TABLE " + tableName + " AS " +
                    "SELECT CAST(id AS varchar) id, CAST(value AS " + prestoTypeName + ") value " +
                    "FROM (VALUES " +
                    "  ('null value', NULL), " +
                    "  ('sample value', " + sampleValueLiteral + "), " +
                    "  ('high value', " + highValueLiteral + ")) " +
                    " t(id, value)";
            assertUpdate(createTable, 3);
        };
        if (dataMappingTestSetup.isUnsupportedType()) {
            String typeNameBase = prestoTypeName.replaceFirst("\\(.*", "");
            String expectedMessagePart = format("(%1$s.*not (yet )?supported)|((?i)unsupported.*%1$s)|((?i)not supported.*%1$s)", Pattern.quote(typeNameBase));
            assertThatThrownBy(setup::run)
                    .hasMessageFindingMatch(expectedMessagePart)
                    .satisfies(e -> assertThat(getPrestoExceptionCause(e)).hasMessageFindingMatch(expectedMessagePart));
            return;
        }
        setup.run();

        // without pushdown, i.e. test read data mapping
        System.out.println(computeActual("SELECT * FROM " + tableName));
        assertQuery("SELECT id FROM " + tableName + " WHERE rand() = 42 OR value IS NULL", "VALUES 'null value'");
        assertQuery("SELECT id FROM " + tableName + " WHERE rand() = 42 OR value IS NOT NULL", "VALUES ('sample value'), ('high value')");
        assertQuery("SELECT id FROM " + tableName + " WHERE rand() = 42 OR value = " + sampleValueLiteral, "VALUES 'sample value'");
        assertQuery("SELECT id FROM " + tableName + " WHERE rand() = 42 OR value = " + highValueLiteral, "VALUES 'high value'");

        assertQuery("SELECT id FROM " + tableName + " WHERE value IS NULL", "VALUES 'null value'");
        assertQuery("SELECT id FROM " + tableName + " WHERE value IS NOT NULL", "VALUES ('sample value'), ('high value')");
        assertQuery("SELECT id FROM " + tableName + " WHERE value = " + sampleValueLiteral, "VALUES 'sample value'");
        assertQuery("SELECT id FROM " + tableName + " WHERE value != " + sampleValueLiteral, "VALUES 'high value'");
        assertQuery("SELECT id FROM " + tableName + " WHERE value <= " + sampleValueLiteral, "VALUES 'sample value'");
        assertQuery("SELECT id FROM " + tableName + " WHERE value > " + sampleValueLiteral, "VALUES 'high value'");
        assertQuery("SELECT id FROM " + tableName + " WHERE value <= " + highValueLiteral, "VALUES ('sample value'), ('high value')");

        assertQuery("SELECT id FROM " + tableName + " WHERE value IS NULL OR value = " + sampleValueLiteral, "VALUES ('null value'), ('sample value')");
        assertQuery("SELECT id FROM " + tableName + " WHERE value IS NULL OR value != " + sampleValueLiteral, "VALUES ('null value'), ('high value')");
        assertQuery("SELECT id FROM " + tableName + " WHERE value IS NULL OR value <= " + sampleValueLiteral, "VALUES ('null value'), ('sample value')");
        assertQuery("SELECT id FROM " + tableName + " WHERE value IS NULL OR value > " + sampleValueLiteral, "VALUES ('null value'), ('high value')");
        assertQuery("SELECT id FROM " + tableName + " WHERE value IS NULL OR value <= " + highValueLiteral, "VALUES ('null value'), ('sample value'), ('high value')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @DataProvider
    public final Object[][] testDataMappingSmokeTestDataProvider()
    {
        return testDataMappingSmokeTestData().stream()
                .map(this::filterDataMappingSmokeTestData)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(dataMappingTestSetup -> new Object[] {dataMappingTestSetup})
                .toArray(Object[][]::new);
    }

    private List<DataMappingTestSetup> testDataMappingSmokeTestData()
    {
        return ImmutableList.<DataMappingTestSetup>builder()
//                .add(new DataMappingTestSetup("char(3)", "'ab '", "'zzz'"))
                .add(new DataMappingTestSetup("varchar(3)", "'de '", "'zzz'"))
                .build();
    }

    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getPrestoTypeName();
        if (typeName.equals("time")
                || typeName.equals("timestamp with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.equals("date") // date gets stored as varchar
                || typeName.equals("varbinary")) { // TODO: https://github.com/prestosql/presto/issues/3597
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    protected static final class DataMappingTestSetup
    {
        private final String prestoTypeName;
        private final String sampleValueLiteral;
        private final String highValueLiteral;

        private final boolean unsupportedType;

        public DataMappingTestSetup(String prestoTypeName, String sampleValueLiteral, String highValueLiteral)
        {
            this(prestoTypeName, sampleValueLiteral, highValueLiteral, false);
        }

        private DataMappingTestSetup(String prestoTypeName, String sampleValueLiteral, String highValueLiteral, boolean unsupportedType)
        {
            this.prestoTypeName = requireNonNull(prestoTypeName, "prestoTypeName is null");
            this.sampleValueLiteral = requireNonNull(sampleValueLiteral, "sampleValueLiteral is null");
            this.highValueLiteral = requireNonNull(highValueLiteral, "highValueLiteral is null");
            this.unsupportedType = unsupportedType;
        }

        public String getPrestoTypeName()
        {
            return prestoTypeName;
        }

        public String getSampleValueLiteral()
        {
            return sampleValueLiteral;
        }

        public String getHighValueLiteral()
        {
            return highValueLiteral;
        }

        public boolean isUnsupportedType()
        {
            return unsupportedType;
        }

        public DataMappingTestSetup asUnsupported()
        {
            return new DataMappingTestSetup(
                    prestoTypeName,
                    sampleValueLiteral,
                    highValueLiteral,
                    true);
        }

        @Override
        public String toString()
        {
            // toString is brief because it's used for test case labels in IDE
            return prestoTypeName + (unsupportedType ? "!" : "");
        }
    }

    static RuntimeException getPrestoExceptionCause(Throwable e)
    {
        return Throwables.getCausalChain(e).stream()
                .filter(TestKuduDistributedQueries::isPrestoException)
                .findFirst() // TODO .collect(toOptional()) -- should be exactly one in the causal chain
                .map(RuntimeException.class::cast)
                .orElseThrow(() -> new IllegalArgumentException("Exception does not have PrestoException cause", e));
    }

    private static boolean isPrestoException(Throwable exception)
    {
        requireNonNull(exception, "exception is null");

        if (exception instanceof PrestoException || exception instanceof ParsingException) {
            return true;
        }

        if (exception.getClass().getName().equals("io.prestosql.client.FailureInfo$FailureException")) {
            try {
                String originalClassName = exception.toString().split(":", 2)[0];
                Class<? extends Throwable> originalClass = Class.forName(originalClassName).asSubclass(Throwable.class);
                return PrestoException.class.isAssignableFrom(originalClass) ||
                        ParsingException.class.isAssignableFrom(originalClass);
            }
            catch (ClassNotFoundException e) {
                return false;
            }
        }

        return false;
    }
}
