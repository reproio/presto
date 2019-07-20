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
package com.facebook.presto.kafka;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;
import io.airlift.log.Logger;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import javax.inject.Inject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static com.facebook.presto.kafka.KafkaHandleResolver.convertLayout;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Kafka specific implementation of {@link ConnectorSplitManager}.
 */
public class KafkaSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(KafkaSplitManager.class);

    private final String connectorId;
    private final KafkaSimpleConsumerManager consumerManager;
    private final Set<HostAddress> nodes;

    @Inject
    public KafkaSplitManager(
            KafkaConnectorId connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            KafkaSimpleConsumerManager consumerManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");

        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.nodes = ImmutableSet.copyOf(kafkaConnectorConfig.getNodes());
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        KafkaTableLayoutHandle kafkaLayout = convertLayout(layout);
        KafkaTableHandle kafkaTableHandle = kafkaLayout.getTable();
        TupleDomain<ColumnHandle> tupleDomain = kafkaLayout.getTupleDomain();
        try {
            SimpleConsumer simpleConsumer = consumerManager.getConsumer(selectRandom(nodes));

            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(ImmutableList.of(kafkaTableHandle.getTopicName()));
            TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);

            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

            Domain partitionIdDomain = null;
            Domain partitionOffsetDomain = null;

            if (tupleDomain.getDomains().isPresent()) {
                for (Map.Entry<ColumnHandle, Domain> entry : tupleDomain.getDomains().get().entrySet()) {
                    KafkaColumnHandle columnHandle = (KafkaColumnHandle) entry.getKey();
                    if (columnHandle.getName().equals("_partition_id")) {
                        partitionIdDomain = entry.getValue();
                    }
                    if (columnHandle.getName().equals("_partition_offset")) {
                        partitionOffsetDomain = entry.getValue();
                    }
                }
            }

            for (TopicMetadata metadata : topicMetadataResponse.topicsMetadata()) {
                for (PartitionMetadata part : metadata.partitionsMetadata()) {
                    if (partitionIdDomain != null) {
                        if (!partitionIdDomain.contains(Domain.singleValue(BigintType.BIGINT, (long) part.partitionId()))) {
                            continue;
                        }
                    }
                    log.debug("Adding Partition %s/%s", metadata.topic(), part.partitionId());

                    Broker leader = part.leader();
                    if (leader == null) {
                        throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Leader election in progress for Kafka topic '%s' partition %s", metadata.topic(), part.partitionId()));
                    }

                    HostAddress partitionLeader = HostAddress.fromParts(leader.host(), leader.port());

                    SimpleConsumer leaderConsumer = consumerManager.getConsumer(partitionLeader);
                    // Kafka contains a reverse list of "end - start" pairs for the splits

                    long[] offsets = findAllOffsets(leaderConsumer, metadata.topic(), part.partitionId());

                    long partitionOffsetMax = offsets[0];
                    long partitionOffsetMin = offsets[offsets.length - 1];
                    if (partitionOffsetDomain != null) {
                        for (Range range : partitionOffsetDomain.getValues().getRanges().getOrderedRanges()) {
                            if (range.isSingleValue()) {
                                partitionOffsetMin = (long) range.getSingleValue();
                                partitionOffsetMax = partitionOffsetMin;
                            }
                            else {
                                if (!range.getLow().isLowerUnbounded()) {
                                    switch (range.getLow().getBound()) {
                                        case ABOVE:
                                        case EXACTLY:
                                            partitionOffsetMin = (long) range.getLow().getValue();
                                            break;
                                        case BELOW:
                                            throw new IllegalArgumentException("Low marker should never use BELOW bound");
                                        default:
                                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                                    }
                                }
                                if (!range.getHigh().isUpperUnbounded()) {
                                    switch (range.getHigh().getBound()) {
                                        case ABOVE:
                                            throw new IllegalArgumentException("High marker should never use ABOVE bound");
                                        case EXACTLY:
                                            partitionOffsetMax = (long) range.getHigh().getValue() + 1;
                                            break;
                                        case BELOW:
                                            partitionOffsetMax = (long) range.getHigh().getValue();
                                            break;
                                        default:
                                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                                    }
                                }
                            }
                        }
                    }

                    for (int i = offsets.length - 1; i > 0; i--) {
                        if (partitionOffsetMax <= offsets[i] || partitionOffsetMin > offsets[i - 1]) {
                            continue;
                        }
                        KafkaSplit split = new KafkaSplit(
                                connectorId,
                                metadata.topic(),
                                kafkaTableHandle.getKeyDataFormat(),
                                kafkaTableHandle.getMessageDataFormat(),
                                kafkaTableHandle.getKeyDataSchemaLocation().map(KafkaSplitManager::readSchema),
                                kafkaTableHandle.getMessageDataSchemaLocation().map(KafkaSplitManager::readSchema),
                                part.partitionId(),
                                Long.max(partitionOffsetMin, offsets[i]),
                                Long.min(partitionOffsetMax, offsets[i - 1]),
                                partitionLeader);
                        splits.add(split);
                    }
                }
            }

            return new FixedSplitSource(splits.build());
        }
        catch (Exception e) { // Catch all exceptions because Kafka library is written in scala and checked exceptions are not declared in method signature.
            if (e instanceof PrestoException) {
                throw e;
            }
            throw new PrestoException(KAFKA_SPLIT_ERROR, format("Cannot list splits for table '%s' reading topic '%s'", kafkaTableHandle.getTableName(), kafkaTableHandle.getTopicName()), e);
        }
    }

    private static String readSchema(String dataSchemaLocation)
    {
        InputStream inputStream = null;
        try {
            if (isURI(dataSchemaLocation.trim().toLowerCase(ENGLISH))) {
                try {
                    inputStream = new URL(dataSchemaLocation).openStream();
                }
                catch (MalformedURLException e) {
                    // try again before failing
                    inputStream = new FileInputStream(dataSchemaLocation);
                }
            }
            else {
                inputStream = new FileInputStream(dataSchemaLocation);
            }
            return CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Could not parse the Avro schema at: " + dataSchemaLocation, e);
        }
        finally {
            closeQuietly(inputStream);
        }
    }

    private static void closeQuietly(InputStream stream)
    {
        try {
            if (stream != null) {
                stream.close();
            }
        }
        catch (IOException ignored) {
        }
    }

    private static boolean isURI(String location)
    {
        try {
            URI.create(location);
        }
        catch (Exception e) {
            return false;
        }
        return true;
    }

    private static long[] findAllOffsets(SimpleConsumer consumer, String topicName, int partitionId)
    {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topicName, partitionId);

        // The API implies that this will always return all of the offsets. So it seems a partition can not have
        // more than Integer.MAX_VALUE-1 segments.
        //
        // This also assumes that the lowest value returned will be the first segment available. So if segments have been dropped off, this value
        // should not be 0.
        PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), Integer.MAX_VALUE);
        OffsetRequest offsetRequest = new OffsetRequest(ImmutableMap.of(topicAndPartition, partitionOffsetRequestInfo), kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
        OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);

        if (offsetResponse.hasError()) {
            short errorCode = offsetResponse.errorCode(topicName, partitionId);
            throw new RuntimeException("could not fetch data from Kafka, error code is '" + errorCode + "'");
        }

        return offsetResponse.offsets(topicName, partitionId);
    }

    private static <T> T selectRandom(Iterable<T> iterable)
    {
        List<T> list = ImmutableList.copyOf(iterable);
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }
}
