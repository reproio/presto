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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import com.facebook.presto.cassandra.util.CassandraCqlUtils;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.net.InetAddresses.toAddrString;
import static java.lang.Float.intBitsToFloat;
import static java.util.Objects.requireNonNull;

public enum CassandraType
        implements FullCassandraType
{
    ASCII(createUnboundedVarcharType(), String.class),
    BIGINT(BigintType.BIGINT, Long.class),
    BLOB(VarbinaryType.VARBINARY, ByteBuffer.class),
    CUSTOM(VarbinaryType.VARBINARY, ByteBuffer.class),
    BOOLEAN(BooleanType.BOOLEAN, Boolean.class),
    COUNTER(BigintType.BIGINT, Long.class),
    DECIMAL(DoubleType.DOUBLE, BigDecimal.class),
    DOUBLE(DoubleType.DOUBLE, Double.class),
    FLOAT(RealType.REAL, Float.class),
    INET(createVarcharType(Constants.IP_ADDRESS_STRING_MAX_LENGTH), InetAddress.class),
    INT(IntegerType.INTEGER, Integer.class),
    TEXT(createUnboundedVarcharType(), String.class),
    TIMESTAMP(TimestampType.TIMESTAMP, Date.class),
    UUID(createVarcharType(Constants.UUID_STRING_MAX_LENGTH), java.util.UUID.class),
    TIMEUUID(createVarcharType(Constants.UUID_STRING_MAX_LENGTH), java.util.UUID.class),
    VARCHAR(createUnboundedVarcharType(), String.class),
    VARINT(createUnboundedVarcharType(), BigInteger.class),
    LIST(createUnboundedVarcharType(), null),
    MAP(createUnboundedVarcharType(), null),
    SET(createUnboundedVarcharType(), null);

    private static class Constants
    {
        private static final int UUID_STRING_MAX_LENGTH = 36;
        // IPv4: 255.255.255.255 - 15 characters
        // IPv6: FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF - 39 characters
        // IPv4 embedded into IPv6: FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:255.255.255.255 - 45 characters
        private static final int IP_ADDRESS_STRING_MAX_LENGTH = 45;
    }

    private final Type nativeType;
    private final Class<?> javaType;

    CassandraType(Type nativeType, Class<?> javaType)
    {
        this.nativeType = requireNonNull(nativeType, "nativeType is null");
        this.javaType = javaType;
    }

    public Type getNativeType()
    {
        return nativeType;
    }

    public Class<?> getJavaType()
    {
        return javaType;
    }

    public int getTypeArgumentSize()
    {
        switch (this) {
            case LIST:
            case SET:
                return 1;
            case MAP:
                return 2;
            default:
                return 0;
        }
    }

    public static CassandraType getCassandraType(DataType.Name name)
    {
        switch (name) {
            case ASCII:
                return ASCII;
            case BIGINT:
                return BIGINT;
            case BLOB:
                return BLOB;
            case BOOLEAN:
                return BOOLEAN;
            case COUNTER:
                return COUNTER;
            case CUSTOM:
                return CUSTOM;
            case DECIMAL:
                return DECIMAL;
            case DOUBLE:
                return DOUBLE;
            case FLOAT:
                return FLOAT;
            case INET:
                return INET;
            case INT:
                return INT;
            case LIST:
                return LIST;
            case MAP:
                return MAP;
            case SET:
                return SET;
            case TEXT:
                return TEXT;
            case TIMESTAMP:
                return TIMESTAMP;
            case TIMEUUID:
                return TIMEUUID;
            case UUID:
                return UUID;
            case VARCHAR:
                return VARCHAR;
            case VARINT:
                return VARINT;
            default:
                return null;
        }
    }

    public static String getColumnValueForCql(Row row, int i, CassandraType cassandraType)
    {
        if (row.isNull(i)) {
            return null;
        }
        else {
            switch (cassandraType) {
                case ASCII:
                case TEXT:
                case VARCHAR:
                    return CassandraCqlUtils.quoteStringLiteral(row.getString(i));
                case INT:
                    return Integer.toString(row.getInt(i));
                case BIGINT:
                case COUNTER:
                    return Long.toString(row.getLong(i));
                case BOOLEAN:
                    return Boolean.toString(row.getBool(i));
                case DOUBLE:
                    return Double.toString(row.getDouble(i));
                case FLOAT:
                    return Float.toString(row.getFloat(i));
                case DECIMAL:
                    return row.getDecimal(i).toString();
                case UUID:
                case TIMEUUID:
                    return row.getUUID(i).toString();
                case TIMESTAMP:
                    return Long.toString(row.getTimestamp(i).getTime());
                case INET:
                    return CassandraCqlUtils.quoteStringLiteral(toAddrString(row.getInet(i)));
                case VARINT:
                    return row.getVarint(i).toString();
                case BLOB:
                case CUSTOM:
                    return Bytes.toHexString(row.getBytesUnsafe(i));
                default:
                    throw new IllegalStateException("Handling of type " + cassandraType
                            + " is not implemented");
            }
        }
    }

    public static String getColumnValueForCql(Object object, CassandraType cassandraType)
    {
        switch (cassandraType) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return CassandraCqlUtils.quoteStringLiteral(((Slice) object).toStringUtf8());
            case INT:
            case BIGINT:
                return object.toString();
            default:
                throw new IllegalStateException("Handling of type " + cassandraType
                        + " is not implemented");
        }
    }

    @Override
    public CassandraType getCassandraType()
    {
        if (getTypeArgumentSize() == 0) {
            return this;
        }
        else {
            // must not be called for types with type arguments
            throw new IllegalStateException();
        }
    }

    @Override
    public List<CassandraTypeWithTypeArguments> getTypeArguments()
    {
        if (getTypeArgumentSize() == 0) {
            return null;
        }
        else {
            // must not be called for types with type arguments
            throw new IllegalStateException();
        }
    }

    public Object getJavaValue(Object nativeValue)
    {
        switch (this) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return ((Slice) nativeValue).toStringUtf8();
            case BIGINT:
            case BOOLEAN:
            case DOUBLE:
            case COUNTER:
                return nativeValue;
            case INET:
                return InetAddresses.forString(((Slice) nativeValue).toStringUtf8());
            case INT:
                return ((Long) nativeValue).intValue();
            case FLOAT:
                // conversion can result in precision lost
                return intBitsToFloat(((Long) nativeValue).intValue());
            case DECIMAL:
                // conversion can result in precision lost
                // Presto uses double for decimal, so to keep the floating point precision, convert it to string.
                // Otherwise partition id doesn't match
                return new BigDecimal(nativeValue.toString());
            case TIMESTAMP:
                return new Date((Long) nativeValue);
            case UUID:
            case TIMEUUID:
                return java.util.UUID.fromString(((Slice) nativeValue).toStringUtf8());
            case BLOB:
            case CUSTOM:
                return ((Slice) nativeValue).toStringUtf8();
            case VARINT:
                return new BigInteger(((Slice) nativeValue).toStringUtf8());
            case SET:
            case LIST:
            case MAP:
            default:
                throw new IllegalStateException("Back conversion not implemented for " + this);
        }
    }

    public Object validatePartitionKey(Object value)
    {
        switch (this) {
            case ASCII:
            case TEXT:
            case VARCHAR:
            case BIGINT:
            case BOOLEAN:
            case DOUBLE:
            case INET:
            case INT:
            case FLOAT:
            case DECIMAL:
            case TIMESTAMP:
            case UUID:
            case TIMEUUID:
                return value;
            case COUNTER:
            case BLOB:
            case CUSTOM:
            case VARINT:
            case SET:
            case LIST:
            case MAP:
            default:
                // todo should we just skip partition pruning instead of throwing an exception?
                throw new PrestoException(NOT_SUPPORTED, "Unsupport partition key type: " + this);
        }
    }

    public Object validateClusteringKey(Object value)
    {
        switch (this) {
            case ASCII:
            case TEXT:
            case VARCHAR:
            case BIGINT:
            case BOOLEAN:
            case DOUBLE:
            case INET:
            case INT:
            case FLOAT:
            case DECIMAL:
            case TIMESTAMP:
            case UUID:
            case TIMEUUID:
                return value;
            case COUNTER:
            case BLOB:
            case CUSTOM:
            case VARINT:
            case SET:
            case LIST:
            case MAP:
            default:
                // todo should we just skip partition pruning instead of throwing an exception?
                throw new PrestoException(NOT_SUPPORTED, "Unsupported clustering key type: " + this);
        }
    }

    public static CassandraType toCassandraType(Type type)
    {
        if (type.equals(BooleanType.BOOLEAN)) {
            return BOOLEAN;
        }
        else if (type.equals(BigintType.BIGINT)) {
            return BIGINT;
        }
        else if (type.equals(IntegerType.INTEGER)) {
            return INT;
        }
        else if (type.equals(DoubleType.DOUBLE)) {
            return DOUBLE;
        }
        else if (type.equals(RealType.REAL)) {
            return FLOAT;
        }
        else if (isVarcharType(type)) {
            return TEXT;
        }
        else if (type.equals(DateType.DATE)) {
            return TEXT;
        }
        else if (type.equals(VarbinaryType.VARBINARY)) {
            return BLOB;
        }
        else if (type.equals(TimestampType.TIMESTAMP)) {
            return TIMESTAMP;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }
}
