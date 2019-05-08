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

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class KinesisConfig
{
    private String defaultSchema = "default";

    /**
     * Folder holding the JSON description files for Kinesis streams.
     */
    private String tableDescriptionLocation = "etc/kinesis/";

    private boolean hideInternalColumns = true;

    private String awsRegion = "us-east-1";

    private int batchSize = 10000;

    private int maxBatches = 600;

    private int fetchAttempts = 2;

    private Duration sleepTime = new Duration(1000, TimeUnit.MILLISECONDS);

    /**
     * Use an initial shard iterator type of AT_TIMESTAMP starting iteratorOffsetSeconds before the current time.
     * <p>
     * When false, an initial shard iterator type of TRIM_HORIZON will be used.
     */
    private boolean iteratorFromTimestamp = true;

    /**
     * When iteratorFromTimestamp is true, the shard iterator will start at iteratorOffsetSeconds before
     * the current time.
     */
    private long iteratorOffsetSeconds = 86400;

    private String accessKey;

    private String secretKey;

    private boolean logKinesisBatches = true;

    private boolean checkpointEnabled;

    private long dynamoReadCapacity = 50L;

    private long dynamoWriteCapacity = 10L;

    private Duration checkpointInterval = new Duration(60000, TimeUnit.MILLISECONDS);

    private String logicalProcessName = "process1";

    private int iteratorNumber;

    @NotNull
    public String getTableDescriptionLocation()
    {
        return tableDescriptionLocation;
    }

    @Config("kinesis.table-description-location")
    public KinesisConfig setTableDescriptionLocation(String tableDescriptionLocation)
    {
        this.tableDescriptionLocation = tableDescriptionLocation;
        return this;
    }

    public boolean isHideInternalColumns()
    {
        return hideInternalColumns;
    }

    @Config("kinesis.hide-internal-columns")
    public KinesisConfig setHideInternalColumns(boolean hideInternalColumns)
    {
        this.hideInternalColumns = hideInternalColumns;
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("kinesis.default-schema")
    public KinesisConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    public String getAccessKey()
    {
        return this.accessKey;
    }

    @Config("kinesis.access-key")
    public KinesisConfig setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    public String getSecretKey()
    {
        return this.secretKey;
    }

    @Config("kinesis.secret-key")
    public KinesisConfig setSecretKey(String secretKey)
    {
        this.secretKey = secretKey;
        return this;
    }

    public String getAwsRegion()
    {
        return awsRegion;
    }

    @Config("kinesis.aws-region")
    public KinesisConfig setAwsRegion(String awsRegion)
    {
        this.awsRegion = awsRegion;
        return this;
    }

    public int getBatchSize()
    {
        return this.batchSize;
    }

    @Config("kinesis.batch-size")
    public KinesisConfig setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
        return this;
    }

    public int getMaxBatches()
    {
        return this.maxBatches;
    }

    @Config("kinesis.max-batches")
    public KinesisConfig setMaxBatches(int maxBatches)
    {
        this.maxBatches = maxBatches;
        return this;
    }

    public int getFetchAttempts()
    {
        return this.fetchAttempts;
    }

    @Config("kinesis.fetch-attempts")
    public KinesisConfig setFetchAttempts(int fetchAttempts)
    {
        this.fetchAttempts = fetchAttempts;
        return this;
    }

    public Duration getSleepTime()
    {
        return this.sleepTime;
    }

    @Config("kinesis.sleep-time")
    public KinesisConfig setSleepTime(Duration sleepTime)
    {
        this.sleepTime = sleepTime;
        return this;
    }

    public boolean isLogBatches()
    {
        return logKinesisBatches;
    }

    @Config("kinesis.log-batches")
    public KinesisConfig setLogBatches(boolean logBatches)
    {
        this.logKinesisBatches = logBatches;
        return this;
    }

    public boolean isIteratorFromTimestamp()
    {
        return iteratorFromTimestamp;
    }

    @Config("kinesis.iter-from-timestamp")
    public KinesisConfig setIteratorFromTimestamp(boolean iteratorFromTimestamp)
    {
        this.iteratorFromTimestamp = iteratorFromTimestamp;
        return this;
    }

    public long getIteratorOffsetSeconds()
    {
        return iteratorOffsetSeconds;
    }

    @Config("kinesis.iter-offset-seconds")
    public KinesisConfig setIteratorOffsetSeconds(long iteratorOffsetSeconds)
    {
        this.iteratorOffsetSeconds = iteratorOffsetSeconds;
        return this;
    }

    public boolean isCheckpointEnabled()
    {
        return checkpointEnabled;
    }

    @Config("kinesis.checkpoint-enabled")
    public KinesisConfig setCheckpointEnabled(boolean checkpointEnabled)
    {
        this.checkpointEnabled = checkpointEnabled;
        return this;
    }

    public long getDynamoReadCapacity()
    {
        return dynamoReadCapacity;
    }

    @Config("kinesis.dynamo-read-capacity")
    public KinesisConfig setDynamoReadCapacity(long dynamoReadCapacity)
    {
        this.dynamoReadCapacity = dynamoReadCapacity;
        return this;
    }

    public long getDynamoWriteCapacity()
    {
        return dynamoWriteCapacity;
    }

    @Config("kinesis.dynamo-write-capacity")
    public KinesisConfig setDynamoWriteCapacity(long dynamoWriteCapacity)
    {
        this.dynamoWriteCapacity = dynamoWriteCapacity;
        return this;
    }

    public Duration getCheckpointInterval()
    {
        return checkpointInterval;
    }

    @Config("kinesis.checkpoint-interval")
    public KinesisConfig setCheckpointInterval(Duration checkpointInterval)
    {
        this.checkpointInterval = checkpointInterval;
        return this;
    }

    public String getLogicalProcessName()
    {
        return logicalProcessName;
    }

    @Config("kinesis.checkpoint-logical-name")
    public KinesisConfig setLogicalProcessName(String logicalPrcessName)
    {
        this.logicalProcessName = logicalPrcessName;
        return this;
    }

    public int getIteratorNumber()
    {
        return iteratorNumber;
    }

    @Config("kinesis.iteration-number")
    public KinesisConfig setIteratorNumber(int iteratorNumber)
    {
        this.iteratorNumber = iteratorNumber;
        return this;
    }
}
