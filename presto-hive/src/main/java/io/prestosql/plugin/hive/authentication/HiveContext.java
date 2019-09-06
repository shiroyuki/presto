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
package io.prestosql.plugin.hive.authentication;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.security.ConnectorIdentity;

import static java.util.Objects.requireNonNull;

public final class HiveContext
{
    private final String username;

    public HiveContext(ConnectorIdentity identity)
    {
        requireNonNull(identity, "identity is null");
        this.username = requireNonNull(identity.getUser(), "identity.getUser() is null");
    }

    public HiveContext(ConnectorSession session)
    {
        requireNonNull(session, "session is null");
        this.username = requireNonNull(session.getIdentity().getUser(), "session.getIdentity().getUser() is null");
    }

    public String getUsername()
    {
        return username;
    }
}
