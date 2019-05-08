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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.session.PropertyMetadata;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import static io.prestosql.spi.session.PropertyMetadata.booleanProperty;
import static io.prestosql.spi.session.PropertyMetadata.integerProperty;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;

public final class KinesisSessionProperties
{
    public static final String PRESTO_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String UNSET_TIMESTAMP = "2000-01-01 00:00:00.000";

    public static final String CHECKPOINT_ENABLED = "checkpoint-enabled"; // boolean
    public static final String ITERATION_NUMBER = "iteration_number"; // int
    public static final String CHECKPOINT_LOGICAL_NAME = "checkpoint_logical_name"; // string
    public static final String MAX_BATCHES = "max_batches"; // int
    public static final String BATCH_SIZE = "batch_size"; // int
    public static final String ITER_FROM_TIMESTAMP = "iter_from_timestamp"; // boolean
    public static final String ITER_OFFSET_SECONDS = "iter_offset_seconds"; // long
    public static final String ITER_START_TIMESTAMP = "iter_start_timestamp"; // string timestamp format

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public KinesisSessionProperties(KinesisConfig config)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(booleanProperty(
                        CHECKPOINT_ENABLED,
                        "Are checkpoints used in this session?",
                        config.isCheckpointEnabled(),
                        false))
                .add(integerProperty(
                        ITERATION_NUMBER,
                        "Checkpoint iteration number",
                        config.getIteratorNumber(),
                        false))
                .add(stringProperty(
                        CHECKPOINT_LOGICAL_NAME,
                        "checkpoint logical name",
                        config.getLogicalProcessName(),
                        false))
                .add(PropertyMetadata.integerProperty(
                        MAX_BATCHES,
                        "max number of calls to Kinesis per query",
                        config.getMaxBatches(),
                        false))
                .add(PropertyMetadata.integerProperty(
                        BATCH_SIZE,
                        "Record limit in calls to Kinesis",
                        config.getBatchSize(),
                        false))
                .add(PropertyMetadata.booleanProperty(
                        ITER_FROM_TIMESTAMP,
                        "Start from timestamp not trim horizon",
                        config.isIteratorFromTimestamp(),
                        false))
                .add(PropertyMetadata.longProperty(
                        ITER_OFFSET_SECONDS,
                        "Seconds before current time to start iterator",
                        config.getIteratorOffsetSeconds(),
                        false))
                .add(PropertyMetadata.stringProperty(
                        ITER_START_TIMESTAMP,
                        "Timestamp in Presto format to start iterator",
                        UNSET_TIMESTAMP,
                        false))
                .build();
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static String getUnsetTimestamp(ConnectorSession session)
    {
        return session.getProperty(UNSET_TIMESTAMP, String.class);
    }

    public static boolean isCheckpointEnabled(ConnectorSession session)
    {
        return session.getProperty(CHECKPOINT_ENABLED, Boolean.class);
    }

    public static int getIterationNumber(ConnectorSession session)
    {
        return session.getProperty(ITERATION_NUMBER, Integer.class);
    }

    public static String getCheckpointLogicalName(ConnectorSession session)
    {
        return session.getProperty(CHECKPOINT_LOGICAL_NAME, String.class);
    }

    public static int getMaxBatches(ConnectorSession session)
    {
        return session.getProperty(MAX_BATCHES, Integer.class);
    }

    public static int getBatchSize(ConnectorSession session)
    {
        return session.getProperty(BATCH_SIZE, Integer.class);
    }

    public static boolean isIterFromTimestamp(ConnectorSession session)
    {
        return session.getProperty(ITER_FROM_TIMESTAMP, Boolean.class);
    }

    public static long getIterOffsetSeconds(ConnectorSession session)
    {
        return session.getProperty(ITER_OFFSET_SECONDS, Long.class);
    }

    public static long getIterStartTimestamp(ConnectorSession session)
    {
        String value = session.getProperty(ITER_START_TIMESTAMP, String.class);
        if (value.equals(UNSET_TIMESTAMP)) {
            return 0;
        }
        else {
            return getTimestampAsLong(value, session);
        }
    }

    public static long getTimestampAsLong(String tsValue, ConnectorSession session)
    {
        // Parse this as a date and return the long timestamp value (2016-07-10 17:03:56.124).
        // They will be entering timestamps in their session's timezone.  Use session.getTimeZoneKey().
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(PRESTO_TIMESTAMP_FORMAT);

        if (!session.getTimeZoneKey().getId().equals(TimeZone.getDefault().getID())) {
            TimeZone sessionTz = TimeZone.getTimeZone(session.getTimeZoneKey().getId());
            simpleDateFormat.setTimeZone(sessionTz);
        }

        Date result = simpleDateFormat.parse(tsValue, new ParsePosition(0));
        return result.getTime();
    }
}
