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

import org.testcontainers.containers.DockerComposeContainer;

import java.io.Closeable;
import java.io.File;

public class KuduServer
        implements Closeable
{
    private final DockerComposeContainer<?> dockerContainer;

    public KuduServer()
    {
        dockerContainer = new DockerComposeContainer(new File("conf/compose-test.yml"))
                .withEnv("KUDU_MASTERS", "kudu-master-1,kudu-master-2,kudus-master-3");
        dockerContainer.start();
    }

    public String getMasterAddresses()
    {
        return "localhost:7051,localhost:7151,localhost:7251";
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
