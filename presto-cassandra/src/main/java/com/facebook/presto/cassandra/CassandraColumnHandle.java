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
package com.facebook.presto.cassandra;

import com.facebook.presto.cassandra.util.CassandraCqlUtils;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects.ToStringHelper;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CassandraColumnHandle
        implements ColumnHandle
{
    private final String connectorId;
    private final String name;
    private final int ordinalPosition;
    private final CassandraTypeWithTypeArguments cassandraTypeWithTypeArguments;
    private final boolean partitionKey;
    private final boolean clusteringKey;
    private final boolean indexed;
    private final boolean hidden;

    @JsonCreator
    public CassandraColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("name") String name,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("cassandraType") CassandraTypeWithTypeArguments cassandraTypeWithTypeArguments,
            @JsonProperty("partitionKey") boolean partitionKey,
            @JsonProperty("clusteringKey") boolean clusteringKey,
            @JsonProperty("indexed") boolean indexed,
            @JsonProperty("hidden") boolean hidden)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.name = requireNonNull(name, "name is null");
        checkArgument(ordinalPosition >= 0, "ordinalPosition is negative");
        this.ordinalPosition = ordinalPosition;
        this.cassandraTypeWithTypeArguments = requireNonNull(cassandraTypeWithTypeArguments, "cassandraType is null");
        this.partitionKey = partitionKey;
        this.clusteringKey = clusteringKey;
        this.indexed = indexed;
        this.hidden = hidden;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    public CassandraType getCassandraType()
    {
        return cassandraTypeWithTypeArguments.getCassandraType();
    }

    @JsonProperty
    public CassandraTypeWithTypeArguments getCassandraTypeWithTypeArguments()
    {
        return cassandraTypeWithTypeArguments;
    }

    public List<CassandraTypeWithTypeArguments> getTypeArguments()
    {
        return cassandraTypeWithTypeArguments.getTypeArguments();
    }

    @JsonProperty
    public boolean isPartitionKey()
    {
        return partitionKey;
    }

    @JsonProperty
    public boolean isClusteringKey()
    {
        return clusteringKey;
    }

    @JsonProperty
    public boolean isIndexed()
    {
        return indexed;
    }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(CassandraCqlUtils.cqlNameToSqlName(name), getType(), null, hidden);
    }

    public Type getType()
    {
        return getCassandraType().getNativeType();
    }

    public CassandraTypeWithTypeArguments getFullType()
    {
        return cassandraTypeWithTypeArguments;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                connectorId,
                name,
                ordinalPosition,
                cassandraTypeWithTypeArguments,
                partitionKey,
                clusteringKey,
                indexed,
                hidden);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        CassandraColumnHandle other = (CassandraColumnHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.name, other.name) &&
                Objects.equals(this.ordinalPosition, other.ordinalPosition) &&
                Objects.equals(this.cassandraTypeWithTypeArguments, other.cassandraTypeWithTypeArguments) &&
                Objects.equals(this.partitionKey, other.partitionKey) &&
                Objects.equals(this.clusteringKey, other.clusteringKey) &&
                Objects.equals(this.indexed, other.indexed) &&
                Objects.equals(this.hidden, other.hidden);
    }

    @Override
    public String toString()
    {
        ToStringHelper helper = toStringHelper(this)
                .add("connectorId", connectorId)
                .add("name", name)
                .add("ordinalPosition", ordinalPosition)
                .add("cassandraTypeWithTypeArguments", cassandraTypeWithTypeArguments)
                .add("partitionKey", partitionKey)
                .add("clusteringKey", clusteringKey)
                .add("indexed", indexed)
                .add("hidden", hidden);

        return helper.toString();
    }
}
