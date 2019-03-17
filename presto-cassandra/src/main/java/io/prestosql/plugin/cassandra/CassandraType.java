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
package io.prestosql.plugin.cassandra;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.utils.Bytes;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.plugin.cassandra.util.CassandraCqlUtils;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.SqlDecimal;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.net.InetAddresses.toAddrString;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.util.Objects.requireNonNull;

public final class CassandraType
        implements FullCassandraType
{
    public static final CassandraType ASCII = new CassandraType(createUnboundedVarcharType(), String.class, DataType.Name.ASCII);
    public static final CassandraType BIGINT = new CassandraType(BigintType.BIGINT, Long.class, DataType.Name.BIGINT);
    public static final CassandraType BLOB = new CassandraType(VarbinaryType.VARBINARY, ByteBuffer.class, DataType.Name.BLOB);
    public static final CassandraType CUSTOM = new CassandraType(VarbinaryType.VARBINARY, ByteBuffer.class, DataType.Name.CUSTOM);
    public static final CassandraType BOOLEAN = new CassandraType(BooleanType.BOOLEAN, Boolean.class, DataType.Name.BOOLEAN);
    public static final CassandraType COUNTER = new CassandraType(BigintType.BIGINT, Long.class, DataType.Name.COUNTER);
    public static final CassandraType DECIMAL = new CassandraType(DoubleType.DOUBLE, BigDecimal.class, DataType.Name.DECIMAL);
    public static final CassandraType DOUBLE = new CassandraType(DoubleType.DOUBLE, Double.class, DataType.Name.DOUBLE);
    public static final CassandraType FLOAT = new CassandraType(RealType.REAL, Float.class, DataType.Name.FLOAT);
    public static final CassandraType INET = new CassandraType(createVarcharType(Constants.IP_ADDRESS_STRING_MAX_LENGTH), InetAddress.class, DataType.Name.INET);
    public static final CassandraType INT = new CassandraType(IntegerType.INTEGER, Integer.class, DataType.Name.INT);
    public static final CassandraType SMALLINT = new CassandraType(SmallintType.SMALLINT, Short.class, DataType.Name.SMALLINT);
    public static final CassandraType TINYINT = new CassandraType(TinyintType.TINYINT, Byte.class, DataType.Name.TINYINT);
    public static final CassandraType TEXT = new CassandraType(createUnboundedVarcharType(), String.class, DataType.Name.TEXT);
    public static final CassandraType DATE = new CassandraType(DateType.DATE, LocalDate.class, DataType.Name.DATE);
    public static final CassandraType TIMESTAMP = new CassandraType(TimestampType.TIMESTAMP, Date.class, DataType.Name.TIMESTAMP);
    public static final CassandraType UUID = new CassandraType(createVarcharType(Constants.UUID_STRING_MAX_LENGTH), java.util.UUID.class, DataType.Name.UUID);
    public static final CassandraType TIMEUUID = new CassandraType(createVarcharType(Constants.UUID_STRING_MAX_LENGTH), java.util.UUID.class, DataType.Name.TIMEUUID);
    public static final CassandraType VARCHAR = new CassandraType(createUnboundedVarcharType(), String.class, DataType.Name.VARCHAR);
    public static final CassandraType VARINT = new CassandraType(createUnboundedVarcharType(), BigInteger.class, DataType.Name.VARINT);
    public static final CassandraType LIST = new CassandraType(createUnboundedVarcharType(), null, DataType.Name.LIST);
    public static final CassandraType MAP = new CassandraType(createUnboundedVarcharType(), null, DataType.Name.MAP);
    public static final CassandraType SET = new CassandraType(createUnboundedVarcharType(), null, DataType.Name.SET);
//    public static final CassandraType UDT = new CassandraType(createRowType(), UDTValue.class, DataType.Name.UDT);

    public final Type nativeType;
    public final Class<?> javaType;
    public final DataType.Name name;

    @JsonCreator
    CassandraType(DataType type)
    {
        CassandraType cassandraType = getCassandraType(type);
        nativeType = cassandraType.nativeType;
        javaType = cassandraType.javaType;
        name = type.getName();
    }

    CassandraType(Type nativeType, Class<?> javaType, DataType.Name name)
    {
        this.nativeType = requireNonNull(nativeType, "nativeType is null");
        this.javaType = javaType;
        this.name = name;
    }

    private static RowType createRowType()
    {
        // Expand getTypeArgumentSize to get udt definitions?
        return RowType.from(ImmutableList.of(new RowType.Field(Optional.of("dummy"), IntegerType.INTEGER), new RowType.Field(Optional.of("dummy2"), createUnboundedVarcharType())));
    }

    private static RowType createRowType(DataType type)
    {
        UserType userType = (UserType) type;
        List<RowType.Field> values = new ArrayList<>();
        for (String fieldName : userType.getFieldNames()) {
            CassandraType cassandraType = CassandraType.getCassandraType(userType.getFieldType(fieldName));
            RowType.Field field = new RowType.Field(Optional.of(fieldName), cassandraType.nativeType);
            values.add(field);
        }

        return RowType.from(ImmutableList.copyOf(values));
    }

    private static CassandraType getCassandraType(DataType type)
    {
        DataType.Name name = type.getName();
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
            case DATE:
                return DATE;
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
            case SMALLINT:
                return SMALLINT;
            case TEXT:
                return TEXT;
            case TIMESTAMP:
                return TIMESTAMP;
            case TIMEUUID:
                return TIMEUUID;
            case TINYINT:
                return TINYINT;
            case UDT:
                return new CassandraType(createRowType(type), UDTValue.class, DataType.Name.UDT);
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

    public static NullableValue getColumnValue(Row row, int position, FullCassandraType fullCassandraType)
    {
        return getColumnValue(row, position, fullCassandraType.getCassandraType(), fullCassandraType.getTypeArguments());
    }

    public static NullableValue getColumnValue(Row row, int position, CassandraType cassandraType, List<CassandraType> typeArguments)
    {
        Type nativeType = cassandraType.getNativeType();
        if (row.isNull(position)) {
            return NullableValue.asNull(nativeType);
        }
        else {
            switch (cassandraType.getName()) {
                case ASCII:
                case TEXT:
                case VARCHAR:
                    return NullableValue.of(nativeType, utf8Slice(row.getString(position)));
                case INT:
                    return NullableValue.of(nativeType, (long) row.getInt(position));
                case SMALLINT:
                    return NullableValue.of(nativeType, (long) row.getShort(position));
                case TINYINT:
                    return NullableValue.of(nativeType, (long) row.getByte(position));
                case BIGINT:
                case COUNTER:
                    return NullableValue.of(nativeType, row.getLong(position));
                case BOOLEAN:
                    return NullableValue.of(nativeType, row.getBool(position));
                case DOUBLE:
                    return NullableValue.of(nativeType, row.getDouble(position));
                case FLOAT:
                    return NullableValue.of(nativeType, (long) floatToRawIntBits(row.getFloat(position)));
                case DECIMAL:
                    return NullableValue.of(nativeType, row.getDecimal(position).doubleValue());
                case UUID:
                case TIMEUUID:
                    return NullableValue.of(nativeType, utf8Slice(row.getUUID(position).toString()));
                case TIMESTAMP:
                    return NullableValue.of(nativeType, row.getTimestamp(position).getTime());
                case DATE:
                    return NullableValue.of(nativeType, (long) row.getDate(position).getDaysSinceEpoch());
                case INET:
                    return NullableValue.of(nativeType, utf8Slice(toAddrString(row.getInet(position))));
                case VARINT:
                    return NullableValue.of(nativeType, utf8Slice(row.getVarint(position).toString()));
                case BLOB:
                case CUSTOM:
                    return NullableValue.of(nativeType, wrappedBuffer(row.getBytesUnsafe(position)));
                case SET:
                    checkTypeArguments(cassandraType, 1, typeArguments);
                    return NullableValue.of(nativeType, utf8Slice(buildSetValue(row, position, typeArguments.get(0))));
                case LIST:
                    checkTypeArguments(cassandraType, 1, typeArguments);
                    return NullableValue.of(nativeType, utf8Slice(buildListValue(row, position, typeArguments.get(0))));
                case MAP:
                    checkTypeArguments(cassandraType, 2, typeArguments);
                    return NullableValue.of(nativeType, utf8Slice(buildMapValue(row, position, typeArguments.get(0), typeArguments.get(1))));
                case UDT:
                    RowType rowType = buildUdtType(typeArguments);
                    return NullableValue.of(rowType, buildUdtValue(rowType, row, position));
                default:
                    throw new IllegalStateException("Handling of type " + cassandraType
                            + " is not implemented");
            }
        }
    }

    public static NullableValue getColumnValueForPartitionKey(Row row, int position, CassandraType cassandraType, List<CassandraType> typeArguments)
    {
        Type nativeType = cassandraType.getNativeType();
        if (row.isNull(position)) {
            return NullableValue.asNull(nativeType);
        }
        switch (cassandraType.getName()) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return NullableValue.of(nativeType, utf8Slice(row.getString(position)));
            case UUID:
            case TIMEUUID:
                return NullableValue.of(nativeType, utf8Slice(row.getUUID(position).toString()));
            default:
                return getColumnValue(row, position, cassandraType, typeArguments);
        }
    }

    private static String buildSetValue(Row row, int position, CassandraType elemType)
    {
        return buildArrayValue(row.getSet(position, elemType.javaType), elemType);
    }

    private static String buildListValue(Row row, int position, CassandraType elemType)
    {
        return buildArrayValue(row.getList(position, elemType.javaType), elemType);
    }

    private static String buildMapValue(Row row, int position, CassandraType keyType, CassandraType valueType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<?, ?> entry : row.getMap(position, keyType.javaType, valueType.javaType).entrySet()) {
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

    private static Block buildUdtValue(RowType rowType, Row row, int position)
    {
        UserType dataType = (UserType) row.getColumnDefinitions().getType(position);
        System.out.println(dataType.getFieldNames());

        List<Object> values = new ArrayList<>();
        for (int i = 0; i < rowType.getTypeParameters().size(); i++) {
            Object value = row.getUDTValue(position).getObject(i);
            values.add(value);
        }
        return rowBlockOf(rowType.getTypeParameters(), values);
    }

    private static RowType buildUdtType(List<CassandraType> cassandraTypes)
    {
        List<RowType.Field> values = new ArrayList<>();
        for (CassandraType cassandraType : cassandraTypes) {
            // TODO
            RowType.Field field = new RowType.Field(Optional.of(cassandraType.getName().name()), cassandraType.nativeType);
            values.add(field);
        }

        return RowType.from(ImmutableList.copyOf(values));
    }

    public static Block rowBlockOf(List<Type> parameterTypes, List<Object> values)
    {
        RowType rowType = RowType.anonymous(parameterTypes);
        BlockBuilder blockBuilder = rowType.createBlockBuilder(null, 1);
        BlockBuilder singleRowBlockWriter = blockBuilder.beginBlockEntry();
        for (int i = 0; i < values.size(); i++) {
            CassandraType cassandraType = toCassandraType(parameterTypes.get(i), ProtocolVersion.V3);
            appendToBlockBuilder(parameterTypes.get(i), values.get(i), singleRowBlockWriter);
        }
        blockBuilder.closeEntry();
        return rowType.getObject(blockBuilder, 0);
    }

    // HACK: Let's hack here!!!!!!!!!!!!!!!!!!!!!!!!!!!
    public static void appendToBlockBuilder(Type type, Object element, BlockBuilder blockBuilder)
    {
        Class<?> javaType = type.getJavaType();
        if (element == null) {
            blockBuilder.appendNull();
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY) && element instanceof Iterable<?>) {
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            for (Object subElement : (Iterable<?>) element) {
                appendToBlockBuilder(type.getTypeParameters().get(0), subElement, subBlockBuilder);
            }
            blockBuilder.closeEntry();
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.ROW) && element instanceof Iterable<?>) {
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            int field = 0;
            for (Object subElement : (Iterable<?>) element) {
                appendToBlockBuilder(type.getTypeParameters().get(field), subElement, subBlockBuilder);
                field++;
            }
            blockBuilder.closeEntry();
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.MAP) && element instanceof Map<?, ?>) {
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) element).entrySet()) {
                appendToBlockBuilder(type.getTypeParameters().get(0), entry.getKey(), subBlockBuilder);
                appendToBlockBuilder(type.getTypeParameters().get(1), entry.getValue(), subBlockBuilder);
            }
            blockBuilder.closeEntry();
        }
        else if (javaType == boolean.class) {
            type.writeBoolean(blockBuilder, (Boolean) element);
        }
        else if (javaType == long.class) {
            if (element instanceof SqlDecimal) {
                type.writeLong(blockBuilder, ((SqlDecimal) element).getUnscaledValue().longValue());
            }
            else if (REAL.equals(type)) {
                type.writeLong(blockBuilder, floatToRawIntBits(((Number) element).floatValue()));
            }
            else {
                type.writeLong(blockBuilder, ((Number) element).longValue());
            }
        }
        else if (javaType == double.class) {
            type.writeDouble(blockBuilder, ((Number) element).doubleValue());
        }
        else if (javaType == Slice.class) {
            if (element instanceof String) {
                type.writeSlice(blockBuilder, Slices.utf8Slice(element.toString()));
            }
            else if (element instanceof byte[]) {
                type.writeSlice(blockBuilder, Slices.wrappedBuffer((byte[]) element));
            }
            else if (element instanceof SqlDecimal) {
                type.writeSlice(blockBuilder, Decimals.encodeUnscaledValue(((SqlDecimal) element).getUnscaledValue()));
            }
            else {
                type.writeSlice(blockBuilder, (Slice) element);
            }
        }
        else {
            type.writeObject(blockBuilder, element);
        }
    }

    @VisibleForTesting
    static String buildArrayValue(Collection<?> collection, CassandraType elemType)
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

    private static void checkTypeArguments(CassandraType type, int expectedSize, List<CassandraType> typeArguments)
    {
        if (typeArguments == null || typeArguments.size() != expectedSize) {
            throw new IllegalArgumentException("Wrong number of type arguments " + typeArguments
                    + " for " + type);
        }
    }

    public static String getColumnValueForCql(Row row, int position, CassandraType cassandraType)
    {
        if (row.isNull(position)) {
            return null;
        }
        else {
            switch (cassandraType.getName()) {
                case ASCII:
                case TEXT:
                case VARCHAR:
                    return CassandraCqlUtils.quoteStringLiteral(row.getString(position));
                case INT:
                    return Integer.toString(row.getInt(position));
                case SMALLINT:
                    return Short.toString(row.getShort(position));
                case TINYINT:
                    return Byte.toString(row.getByte(position));
                case BIGINT:
                case COUNTER:
                    return Long.toString(row.getLong(position));
                case BOOLEAN:
                    return Boolean.toString(row.getBool(position));
                case DOUBLE:
                    return Double.toString(row.getDouble(position));
                case FLOAT:
                    return Float.toString(row.getFloat(position));
                case DECIMAL:
                    return row.getDecimal(position).toString();
                case UUID:
                case TIMEUUID:
                    return row.getUUID(position).toString();
                case TIMESTAMP:
                    return Long.toString(row.getTimestamp(position).getTime());
                case DATE:
                    return row.getDate(position).toString();
                case INET:
                    return CassandraCqlUtils.quoteStringLiteral(toAddrString(row.getInet(position)));
                case VARINT:
                    return row.getVarint(position).toString();
                case BLOB:
                case CUSTOM:
                case UDT:
                    return Bytes.toHexString(row.getBytesUnsafe(position));
                default:
                    throw new IllegalStateException("Handling of type " + cassandraType
                            + " is not implemented");
            }
        }
    }

    private static String objectToString(Object object, CassandraType elemType)
    {
        switch (elemType.getName()) {
            case ASCII:
            case TEXT:
            case VARCHAR:
            case UUID:
            case TIMEUUID:
            case TIMESTAMP:
            case DATE:
            case INET:
            case VARINT:
                return CassandraCqlUtils.quoteStringLiteralForJson(object.toString());

            case BLOB:
            case CUSTOM:
                return CassandraCqlUtils.quoteStringLiteralForJson(Bytes.toHexString((ByteBuffer) object));

            case SMALLINT:
            case TINYINT:
            case INT:
            case BIGINT:
            case COUNTER:
            case BOOLEAN:
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
            case UDT:
                return object.toString();
            default:
                throw new IllegalStateException("Handling of type " + elemType + " is not implemented");
        }
    }

    public static CassandraType toCassandraType(Type type, ProtocolVersion protocolVersion)
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
        else if (type.equals(SmallintType.SMALLINT)) {
            return SMALLINT;
        }
        else if (type.equals(TinyintType.TINYINT)) {
            return TINYINT;
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
            return protocolVersion.toInt() <= ProtocolVersion.V3.toInt() ? TEXT : DATE;
        }
        else if (type.equals(VarbinaryType.VARBINARY)) {
            return BLOB;
        }
        else if (type.equals(TimestampType.TIMESTAMP)) {
            return TIMESTAMP;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    public Type getNativeType()
    {
        return nativeType;
    }

    public DataType.Name getName()
    {
        return name;
    }

    public Class<?> getJavaType()
    {
        return javaType;
    }

    public int getTypeArgumentSize()
    {
        switch (this.getName()) {
            case LIST:
            case SET:
                return 1;
            case MAP:
                return 2;
            default:
                return 0;
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
    public List<CassandraType> getTypeArguments()
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
        switch (this.getName()) {
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
            case SMALLINT:
            case TINYINT:
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
            case DATE:
                return LocalDate.fromDaysSinceEpoch(((Long) nativeValue).intValue());
            case UUID:
            case TIMEUUID:
                return java.util.UUID.fromString(((Slice) nativeValue).toStringUtf8());
            case BLOB:
            case CUSTOM:
            case UDT:
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

    public boolean isSupportedPartitionKey()
    {
        switch (this.getName()) {
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
                return true;
            case COUNTER:
            case BLOB:
            case CUSTOM:
            case VARINT:
            case SET:
            case LIST:
            case MAP:
            case UDT:
            default:
                return false;
        }
    }

    public Object validateClusteringKey(Object value)
    {
        switch (this.getName()) {
            case ASCII:
            case TEXT:
            case VARCHAR:
            case BIGINT:
            case BOOLEAN:
            case DOUBLE:
            case INET:
            case INT:
            case SMALLINT:
            case TINYINT:
            case FLOAT:
            case DECIMAL:
            case TIMESTAMP:
            case DATE:
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CassandraType that = (CassandraType) o;
        return Objects.equals(nativeType, that.nativeType) &&
                Objects.equals(javaType, that.javaType) &&
                name == that.name;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nativeType, javaType, name);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return name.name();
    }

    private static class Constants
    {
        private static final int UUID_STRING_MAX_LENGTH = 36;
        // IPv4: 255.255.255.255 - 15 characters
        // IPv6: FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF - 39 characters
        // IPv4 embedded into IPv6: FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:255.255.255.255 - 45 characters
        private static final int IP_ADDRESS_STRING_MAX_LENGTH = 45;
    }
}
