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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.log.Logger;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.SchemaTableName;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Kinesis version of Presto Plugin interface.
 * <p>
 * The connector manager injects the type manager and node manager, and then calls getServices
 * to get the connector factory.
 */
public class KinesisPlugin
        implements Plugin
{
    private static final Logger log = Logger.get(KinesisPlugin.class);

    private Optional<Supplier<Map<SchemaTableName, KinesisStreamDescription>>> tableDescriptionSupplier = Optional.empty();
    private Map<String, String> optionalConfig = ImmutableMap.of();
    private Optional<Class<? extends KinesisClientProvider>> altProviderClass = Optional.empty();

    private KinesisConnectorFactory factory;

    private static ClassLoader getClassLoader()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = KinesisPlugin.class.getClassLoader();
        }
        return classLoader;
    }

    public synchronized void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(requireNonNull(optionalConfig, "optionalConfig is null"));
    }

    @Override
    public synchronized Iterable<ConnectorFactory> getConnectorFactories()
    {
        if (factory == null) {
            this.factory = new KinesisConnectorFactory(getClassLoader(), tableDescriptionSupplier, optionalConfig, altProviderClass);
        }
        return ImmutableList.of(this.factory);
    }

    @VisibleForTesting
    public synchronized void setTableDescriptionSupplier(Supplier<Map<SchemaTableName, KinesisStreamDescription>> tableDescriptionSupplier)
    {
        this.tableDescriptionSupplier = Optional.of(requireNonNull(tableDescriptionSupplier, "tableDescriptionSupplier is null"));
    }

    @VisibleForTesting
    public <T extends KinesisClientProvider> void setAltProviderClass(Class<T> aType)
    {
        // Note: this can be used for other cases besides testing but that was the original motivation
        altProviderClass = Optional.of(requireNonNull(aType, "Provider class type is null"));
    }

    @VisibleForTesting
    public synchronized Injector getInjector()
    {
        if (this.factory != null) {
            return this.factory.getInjector();
        }
        else {
            return null;
        }
    }
}
