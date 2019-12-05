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

import com.datastax.driver.$internal.com.google.common.reflect.TypeToken;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeTokens;
import com.datastax.driver.core.utils.Bytes;
import com.facebook.presto.cassandra.util.CassandraCqlUtils;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.net.InetAddresses.toAddrString;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public class CassandraTypeWithTypeArguments
        implements FullCassandraType
{
    private final CassandraType cassandraType;
    private final List<CassandraTypeWithTypeArguments> typeArguments;

    @JsonCreator
    public CassandraTypeWithTypeArguments(
            @JsonProperty CassandraType cassandraType,
            @JsonProperty List<CassandraTypeWithTypeArguments> typeArguments)
    {
        this.cassandraType = requireNonNull(cassandraType, "cassandraType is null");
        this.typeArguments = requireNonNull(typeArguments, "typeArguments is null");
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
        CassandraTypeWithTypeArguments that = (CassandraTypeWithTypeArguments) o;
        return cassandraType == that.cassandraType &&
                typeArguments.equals(that.typeArguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cassandraType, typeArguments);
    }

    @Override
    @JsonProperty
    public CassandraType getCassandraType()
    {
        return cassandraType;
    }

    @Override
    @JsonProperty
    public List<CassandraTypeWithTypeArguments> getTypeArguments()
    {
        return typeArguments;
    }

    public TypeToken<?> getTypeToken()
    {
        if (typeArguments.size() == 0) {
            return TypeToken.of(cassandraType.getJavaType());
        }
        else {
            switch (cassandraType) {
                case SET:
                    return TypeTokens.setOf(typeArguments.get(0).getTypeToken());
                case LIST:
                    return TypeTokens.listOf(typeArguments.get(0).getTypeToken());
                case MAP:
                    return TypeTokens.mapOf(typeArguments.get(0).getTypeToken(), typeArguments.get(1).getTypeToken());
                default:
                    throw new IllegalStateException("Handling of type " + cassandraType
                            + " is not implemented");
            }
        }
    }

    public static CassandraTypeWithTypeArguments getCassandraTypeWithTypeArguments(DataType dataType)
    {
        if (dataType.isCollection()) {
            ImmutableList.Builder<CassandraTypeWithTypeArguments> builder = ImmutableList.builder();
            for (DataType typeArgument : dataType.getTypeArguments()) {
                builder.add(getCassandraTypeWithTypeArguments(typeArgument));
            }
            return new CassandraTypeWithTypeArguments(CassandraType.getCassandraType(dataType.getName()), builder.build());
        }
        else {
            return new CassandraTypeWithTypeArguments(CassandraType.getCassandraType(dataType.getName()), ImmutableList.of());
        }
    }

    public static NullableValue getColumnValue(Row row, int i, CassandraTypeWithTypeArguments fullCassandraType)
    {
        CassandraType cassandraType = fullCassandraType.getCassandraType();
        List<CassandraTypeWithTypeArguments> typeArguments = fullCassandraType.getTypeArguments();
        Type nativeType = cassandraType.getNativeType();
        if (row.isNull(i)) {
            return NullableValue.asNull(nativeType);
        }
        else {
            switch (cassandraType) {
                case ASCII:
                case TEXT:
                case VARCHAR:
                    return NullableValue.of(nativeType, utf8Slice(row.getString(i)));
                case INT:
                    return NullableValue.of(nativeType, (long) row.getInt(i));
                case BIGINT:
                case COUNTER:
                    return NullableValue.of(nativeType, row.getLong(i));
                case BOOLEAN:
                    return NullableValue.of(nativeType, row.getBool(i));
                case DOUBLE:
                    return NullableValue.of(nativeType, row.getDouble(i));
                case FLOAT:
                    return NullableValue.of(nativeType, (long) floatToRawIntBits(row.getFloat(i)));
                case DECIMAL:
                    return NullableValue.of(nativeType, row.getDecimal(i).doubleValue());
                case UUID:
                case TIMEUUID:
                    return NullableValue.of(nativeType, utf8Slice(row.getUUID(i).toString()));
                case TIMESTAMP:
                    return NullableValue.of(nativeType, row.getTimestamp(i).getTime());
                case INET:
                    return NullableValue.of(nativeType, utf8Slice(toAddrString(row.getInet(i))));
                case VARINT:
                    return NullableValue.of(nativeType, utf8Slice(row.getVarint(i).toString()));
                case BLOB:
                case CUSTOM:
                    return NullableValue.of(nativeType, wrappedBuffer(row.getBytesUnsafe(i)));
                case SET:
                    checkTypeArguments(cassandraType, 1, typeArguments);
                    Set<?> set = row.getSet(i, typeArguments.get(0).getTypeToken());
                    return NullableValue.of(nativeType, utf8Slice(buildArrayValue(set, typeArguments.get(0))));
                case LIST:
                    checkTypeArguments(cassandraType, 1, typeArguments);
                    List<?> list = row.getList(i, typeArguments.get(0).getTypeToken());
                    return NullableValue.of(nativeType, utf8Slice(buildArrayValue(list, typeArguments.get(0))));
                case MAP:
                    checkTypeArguments(cassandraType, 2, typeArguments);
                    CassandraTypeWithTypeArguments keyType = typeArguments.get(0);
                    CassandraTypeWithTypeArguments valueType = typeArguments.get(1);
                    Map<?, ?> map = row.getMap(i, keyType.getTypeToken(), valueType.getTypeToken());
                    return NullableValue.of(nativeType, utf8Slice(buildMapValue(map, keyType, valueType)));
                default:
                    throw new IllegalStateException("Handling of type " + cassandraType
                            + " is not implemented");
            }
        }
    }

    public static NullableValue getColumnValueForPartitionKey(Row row, int i, CassandraTypeWithTypeArguments fullCassandraType)
    {
        CassandraType cassandraType = fullCassandraType.getCassandraType();
        Type nativeType = cassandraType.getNativeType();
        if (row.isNull(i)) {
            return NullableValue.asNull(nativeType);
        }
        switch (cassandraType) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return NullableValue.of(nativeType, utf8Slice(row.getString(i)));
            case UUID:
            case TIMEUUID:
                return NullableValue.of(nativeType, utf8Slice(row.getUUID(i).toString()));
            default:
                return getColumnValue(row, i, fullCassandraType);
        }
    }

    private static void checkTypeArguments(CassandraType type, int expectedSize,
                                           List<CassandraTypeWithTypeArguments> typeArguments)
    {
        if (typeArguments == null || typeArguments.size() != expectedSize) {
            throw new IllegalArgumentException("Wrong number of type arguments " + typeArguments
                    + " for " + type);
        }
    }

    private static String buildMapValue(Map<?, ?> map, CassandraTypeWithTypeArguments keyType, CassandraTypeWithTypeArguments valueType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(objectToString(entry.getKey(), keyType));
            sb.append(":");
            sb.append(objectToString(entry.getValue(), valueType));
        }
        sb.append("}");
        return sb.toString();
    }

    @VisibleForTesting
    static String buildArrayValue(Collection<?> collection, CassandraTypeWithTypeArguments elemType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Object value : collection) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(objectToString(value, elemType));
        }
        sb.append("]");
        return sb.toString();
    }

    private static String objectToString(Object object, CassandraTypeWithTypeArguments elemType)
    {
        switch (elemType.cassandraType) {
            case ASCII:
            case TEXT:
            case VARCHAR:
            case UUID:
            case TIMEUUID:
            case INET:
            case VARINT:
                return CassandraCqlUtils.quoteStringLiteralForJson(object.toString());

            case TIMESTAMP:
                return CassandraCqlUtils.quoteStringLiteralForJson(DateTimeFormatter.ISO_INSTANT.format(((Date) object).toInstant()));

            case BLOB:
            case CUSTOM:
                return CassandraCqlUtils.quoteStringLiteralForJson(Bytes.toHexString((ByteBuffer) object));

            case INT:
            case BIGINT:
            case COUNTER:
            case BOOLEAN:
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
                return object.toString();
            case SET:
                return buildArrayValue((Set<?>) object, elemType.getTypeArguments().get(0));
            case LIST:
                return buildArrayValue((List<?>) object, elemType.getTypeArguments().get(0));
            case MAP:
                return buildMapValue((Map<?, ?>) object, elemType.getTypeArguments().get(0), elemType.getTypeArguments().get(1));
            default:
                throw new IllegalStateException("Handling of type " + elemType + " is not implemented");
        }
    }

    @Override
    public String toString()
    {
        if (typeArguments != null) {
            return cassandraType.toString() + typeArguments.toString();
        }
        else {
            return cassandraType.toString();
        }
    }
}
