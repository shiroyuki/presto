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
package io.prestosql.plugin.kinesis;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.transaction.IsolationLevel;

import java.util.ArrayList;
import java.util.List;

import static io.prestosql.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.prestosql.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class KinesisConnector
        implements Connector
{
    private static final Logger log = Logger.get(KinesisConnector.class);

    private final KinesisMetadata metadata;
    private final KinesisSplitManager splitManager;
    private final KinesisRecordSetProvider recordSetProvider;

    private final List<PropertyMetadata<?>> propertyList;

    private List<ConnectorShutdown> shutdownObjects;

    @Inject
    public KinesisConnector(
            KinesisMetadata metadata,
            KinesisSplitManager splitManager,
            KinesisRecordSetProvider recordSetProvider,
            KinesisSessionProperties sessionProperties)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.propertyList = sessionProperties.getSessionProperties();
        this.shutdownObjects = new ArrayList();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean b)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return KinesisTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return this.propertyList;
    }

    @Override
    public final void shutdown()
    {
        for (ConnectorShutdown obj : this.shutdownObjects) {
            try {
                obj.shutdown();
            }
            catch (Exception ex) {
                log.error("Error when shutting down class in Kinesis connector.", ex);
            }
        }

        return;
    }
}
