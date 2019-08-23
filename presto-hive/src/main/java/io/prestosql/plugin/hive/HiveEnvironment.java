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
package io.prestosql.plugin.hive;

import io.prestosql.plugin.hive.authentication.GenericExceptionAction;
import io.prestosql.plugin.hive.authentication.HiveMetastoreAuthentication;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class HiveEnvironment
{
    private final HiveMetastoreAuthentication hiveMetastoreAuthentication;

    @Inject
    public HiveEnvironment(HiveMetastoreAuthentication hiveMetastoreAuthentication)
    {
        this.hiveMetastoreAuthentication = requireNonNull(hiveMetastoreAuthentication, "hiveMetastoreAuthentication is null");
    }

    public <R, E extends Exception> R doAs(String user, GenericExceptionAction<R, E> action)
            throws E
    {
        return hiveMetastoreAuthentication.doAs(user, action);
    }

    public void doAs(String user, Runnable action)
    {
        hiveMetastoreAuthentication.doAs(user, action);
    }
}
