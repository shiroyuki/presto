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

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class HiveIdentity
{
    private final String username;

    // TODO: This should be called only by CachingHiveMetastore
    public HiveIdentity()
    {
        this(new ConnectorIdentity("dummy_identity", Optional.empty(), Optional.empty()));
    }

    public HiveIdentity(ConnectorSession session)
    {
        this(requireNonNull(session, "session is null").getIdentity());
    }

    public HiveIdentity(ConnectorIdentity identity)
    {
        requireNonNull(identity, "identity is null");
        this.username = requireNonNull(identity.getUser(), "identity.getUser() is null");
    }

    public String getUsername()
    {
        return username;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("username", username)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HiveIdentity other = (HiveIdentity) o;
        return Objects.equals(username, other.username);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(username);
    }
}
