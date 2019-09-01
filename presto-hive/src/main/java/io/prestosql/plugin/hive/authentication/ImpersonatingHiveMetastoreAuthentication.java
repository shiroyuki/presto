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

import io.prestosql.plugin.hive.ForHiveMetastore;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.transport.TTransport;

import javax.inject.Inject;

import static io.prestosql.plugin.hive.authentication.UserGroupInformationUtils.executeActionInDoAs;
import static java.util.Objects.requireNonNull;

public class ImpersonatingHiveMetastoreAuthentication
        implements HiveMetastoreAuthentication
{
    private final HiveAuthentication hiveAuthentication;

    @Inject
    public ImpersonatingHiveMetastoreAuthentication(@ForHiveMetastore HiveAuthentication hiveAuthentication)
    {
        this.hiveAuthentication = requireNonNull(hiveAuthentication);
    }

    @Override
    public TTransport authenticate(TTransport rawTransport, String hiveMetastoreHost)
    {
        return rawTransport;
    }

    @Override
    public String getUsername()
    {
        return hiveAuthentication.getUserGroupInformation().getUserName();
    }

    @Override
    public <R, E extends Exception> R doAs(String user, GenericExceptionAction<R, E> action)
            throws E
    {
        System.out.println("ImpersonatingHiveMetastoreAuthentication " + createProxyUser(user).getUserName());
        return executeActionInDoAs(createProxyUser(user), action);
    }

    private UserGroupInformation createProxyUser(String user)
    {
        return UserGroupInformation.createProxyUser(user, hiveAuthentication.getUserGroupInformation());
    }
}
