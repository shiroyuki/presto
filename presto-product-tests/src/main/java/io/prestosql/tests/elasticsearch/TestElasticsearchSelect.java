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
package io.prestosql.tests.elasticsearch;

import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.query.QueryResult;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.SQLException;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.ELASTICSEARCH;

public class TestElasticsearchSelect
        extends ProductTest
{
    private static final String ELASTICSEARCH_CATALOG = "elasticsearch";

    @Test(groups = ELASTICSEARCH)
    public void testShowSchema()
            throws SQLException
    {
        QueryResult queryResult = query("SHOW SCHEMAS IN " + ELASTICSEARCH_CATALOG);
        assertThat(queryResult).containsOnly(
                row("information_schema"),
                row("tpch"));

        onElasticsearch();

        query("select * from elasticsearch.tpch.posts");
    }

    private void onElasticsearch()
    {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(ELASTICSEARCH_CATALOG, 9200, "http")));

        IndexRequest request = new IndexRequest(
                "posts",
                "doc",
                "1");
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        request.source(jsonString, XContentType.JSON);

        try {
            IndexResponse response = client.index(request);
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }
}
