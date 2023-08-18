package com.linkedin.avro.fastserde.logical.types;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import javax.management.InstanceAlreadyExistsException;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.FastSerdeCache;
import com.linkedin.avro.fastserde.FastSerializer;
import com.linkedin.avro.fastserde.Utils;
import com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesTest1;
import com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecord;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelperCommon;
import com.linkedin.avroutil1.compatibility.AvroVersion;

public class LogicalTypeSpecificSerializerTest {

    private final int v1HeaderLength = 10;

    //  @org.testng.annotations.Ignore
    @Test(groups = {"serializationTest"})
    public void shouldWriteLongAsLogicalTypeInstant() throws IOException {
        // given
        LocalDate localDate = LocalDate.of(2023, 8, 11);
        Instant instant = localDate.atStartOfDay().toInstant(ZoneOffset.UTC);
        Instant nextDay = instant.plus(1, ChronoUnit.DAYS);
        FastSerdeLogicalTypesTest1.Builder builder = FastSerdeLogicalTypesTest1.newBuilder()
                .setUnionOfArrayAndMap(Lists.newArrayList(LocalTime.now(), LocalTime.now().plusMinutes(1)))
                .setTimestampMillisMap(createTimestampMillisMap())
                .setNullableArrayOfDates(Lists.newArrayList(List.of(localDate.minusDays(1), localDate)))
                .setArrayOfDates(Lists.newArrayList(localDate, localDate.plusDays(1), localDate.plusDays(2)))
//                .setDecimalOrDateUnion(new BigDecimal("12.34"))
                .setDecimalOrDateUnion(localDate)
                .setTimestampMillisField(instant)
                .setTimestampMicrosField(instant)
                .setTimeMillisField(LocalTime.of(14, 17, 45, 12345))
                .setTimeMicrosField(LocalTime.of(14, 17, 45, 12345))
                .setDateField(localDate)
                .setNestedLocalTimestampMillis(createLocalTimestampRecord(nextDay));
        injectUuidField(builder);
        FastSerdeLogicalTypesTest1 inputData = builder.build();

        byte[] bytesWithHeader = verifySerializers(inputData);
        verifyDeserializers(bytesWithHeader);
    }

    private byte[] verifySerializers(FastSerdeLogicalTypesTest1 data) throws IOException {
        FastSerdeCache fastSerdeCache = FastSerdeCache.getDefaultInstance();

        @SuppressWarnings("unchecked")
        FastSerializer<FastSerdeLogicalTypesTest1> fastGenericSerializer = (FastSerializer<FastSerdeLogicalTypesTest1>) fastSerdeCache
                .buildFastGenericSerializer(data.getSchema(), copyConversions(data.getSpecificData(), new GenericData()));

        @SuppressWarnings("unchecked")
        FastSerializer<FastSerdeLogicalTypesTest1> fastSpecificSerializer = (FastSerializer<FastSerdeLogicalTypesTest1>) fastSerdeCache
                .buildFastSpecificSerializer(data.getSchema(), copyConversions(data.getSpecificData(), new SpecificData()));

        GenericDatumWriter<FastSerdeLogicalTypesTest1> genericDatumWriter = new GenericDatumWriter<>(
                data.getSchema(), copyConversions(data.getSpecificData(), new GenericData()));

        SpecificDatumWriter<FastSerdeLogicalTypesTest1> specificDatumWriter = new SpecificDatumWriter<>(
                data.getSchema(), copyConversions(data.getSpecificData(), new SpecificData()));

        fixConversionsIfAvro19(data.getSpecificData());

        byte[] fastGenericBytes = serialize(fastGenericSerializer, data);
        byte[] fastSpecificBytes = serialize(fastSpecificSerializer, data);
        byte[] genericBytes = serialize(genericDatumWriter, data);
        byte[] specificBytes = serialize(specificDatumWriter, data);
        byte[] bytesWithHeader = data.toByteBuffer().array();

        byte[] expectedHeaderBytes = ByteBuffer.wrap(new byte[v1HeaderLength])
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(new byte[]{(byte) 0xC3, (byte) 0x01}) // BinaryMessageEncoder.V1_HEADER
                .putLong(Utils.getSchemaFingerprint(data.getSchema()))
                .array();

        // all 5 serializing methods should return the same array of bytes
        Assert.assertEquals(Arrays.copyOf(bytesWithHeader, v1HeaderLength), expectedHeaderBytes);
        Assert.assertEquals(fastGenericBytes, Arrays.copyOfRange(bytesWithHeader, v1HeaderLength, bytesWithHeader.length));
        Assert.assertEquals(fastGenericBytes, fastSpecificBytes);
        Assert.assertEquals(fastGenericBytes, genericBytes);
        Assert.assertEquals(fastGenericBytes, specificBytes);

        return bytesWithHeader;
    }

    private <T> byte[] serialize(FastSerializer<T> fastSerializer, T data) throws IOException {
        InMemoryEncoder encoder = new InMemoryEncoder();
        fastSerializer.serialize(data, encoder);
        return encoder.toByteArray();
    }

    private <T> byte[] serialize(DatumWriter<T> datumWriter, T data) throws IOException {
        InMemoryEncoder encoder = new InMemoryEncoder();
        datumWriter.write(data, encoder);

        return encoder.toByteArray();
    }

    private void verifyDeserializers(byte[] bytesWithHeader) throws IOException {
        FastSerdeLogicalTypesTest1 data = FastSerdeLogicalTypesTest1.fromByteBuffer(ByteBuffer.wrap(bytesWithHeader));
        byte[] bytes = Arrays.copyOfRange(bytesWithHeader, v1HeaderLength, bytesWithHeader.length);
        Schema schema = data.getSchema();
        Supplier<Decoder> decoderSupplier = () -> DecoderFactory.get().binaryDecoder(bytes, null);

        FastSerdeCache fastSerdeCache = FastSerdeCache.getDefaultInstance();

        @SuppressWarnings("unchecked")
        FastDeserializer<GenericData.Record> fastGenericDeserializer = (FastDeserializer<GenericData.Record>) fastSerdeCache
                .buildFastGenericDeserializer(schema, schema, copyConversions(data.getSpecificData(), new GenericData()));

        @SuppressWarnings("unchecked")
        FastDeserializer<FastSerdeLogicalTypesTest1> fastSpecificDeserializer = (FastDeserializer<FastSerdeLogicalTypesTest1>) fastSerdeCache
                .buildFastSpecificDeserializer(schema, schema, copyConversions(data.getSpecificData(), new SpecificData()));

        GenericDatumReader<GenericData.Record> genericDatumReader = new GenericDatumReader<>(
                schema, schema, copyConversions(data.getSpecificData(), new GenericData()));

        SpecificDatumReader<FastSerdeLogicalTypesTest1> specificDatumReader = new SpecificDatumReader<>(
                schema, schema, copyConversions(data.getSpecificData(), new SpecificData()));

        // deserializing with different serializers/writers
        GenericData.Record deserializedFastGeneric = fastGenericDeserializer.deserialize(decoderSupplier.get());
        FastSerdeLogicalTypesTest1 deserializedFastSpecific = fastSpecificDeserializer.deserialize(decoderSupplier.get());
        GenericData.Record deserializedGenericReader = genericDatumReader.read(null, decoderSupplier.get());
        FastSerdeLogicalTypesTest1 deserializedSpecificReader = specificDatumReader.read(null, decoderSupplier.get());

        // TODO porównać obiekty !?!?

        Assert.assertEquals(deserializedFastGeneric.toString(), data.toString());
        Assert.assertEquals(deserializedFastSpecific.toString(), data.toString());
        Assert.assertEquals(deserializedGenericReader.toString(), data.toString());
        Assert.assertEquals(deserializedSpecificReader.toString(), data.toString());
    }

    private <T extends GenericData> T copyConversions(SpecificData fromSpecificData, T toModelData) {
        Optional.ofNullable(fromSpecificData.getConversions())
                .orElse(Collections.emptyList())
                .forEach(toModelData::addLogicalTypeConversion);

        fixConversionsIfAvro19(toModelData);

        return toModelData;
    }

    private <T extends GenericData> void fixConversionsIfAvro19(T modelData) {
        if (AvroCompatibilityHelperCommon.getRuntimeAvroVersion() == AvroVersion.AVRO_1_9) {
            // TODO these conversions should be injected by default.
            // Missing DecimalConversion causes conflict with stringable feature (BigDecimal is considered as string, not bytes)
            // details: GenericData.resolveUnion() + SpecificData.getSchemaName()

            List<Conversion<?>> requiredConversions = Lists.newArrayList(
                    new Conversions.DecimalConversion(),
                    new TimeConversions.TimeMillisConversion(),
                    new TimeConversions.TimeMicrosConversion(),
                    new TimeConversions.TimestampMicrosConversion());

            // LocalTimestampMillisConversion not available in 1.9

            for (Conversion<?> conversion : requiredConversions) {
                modelData.addLogicalTypeConversion(conversion);
            }
        }

        // TODO in 1.10 field of type UUID is generated as CharSequence
        // toModelData.addLogicalTypeConversion(new Conversions.UUIDConversion());
    }

    private Map<CharSequence, Instant> createTimestampMillisMap() {
        Map<CharSequence, Instant> map = new HashMap<>();
        map.put("one", toInstant(LocalDate.of(2023, 8, 18)));
        map.put("two", toInstant(LocalDate.of(2023, 8, 19)));
        map.put("three", toInstant(LocalDate.of(2023, 8, 20)));

        return map;
    }

    private Instant toInstant(LocalDate localDate) {
        return localDate.atStartOfDay(ZoneId.systemDefault()).toInstant();
    }

    private LocalTimestampRecord createLocalTimestampRecord(Instant nestedTimestamp) {
        LocalTimestampRecord.Builder builder = LocalTimestampRecord.newBuilder();

        try {
            Object mixedNestedTimestamp, nullableMixedNestedTimestamp;
            if (Utils.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_9)) {
//                mixedNestedTimestamp = LocalDate.of(2023, 8, 16).atStartOfDay(); // TODO data provider
//                nullableMixedNestedTimestamp = LocalDate.of(2023, 8, 16).atStartOfDay();

                mixedNestedTimestamp = LocalDate.of(2023, 8, 16);
                nullableMixedNestedTimestamp = LocalDate.of(2023, 8, 16);

                builder.getClass().getMethod("setNestedTimestamp", LocalDateTime.class)
                        .invoke(builder, LocalDateTime.ofInstant(nestedTimestamp, ZoneId.systemDefault()));
                builder.getClass().getMethod("setNullableNestedTimestamp", LocalDateTime.class)
                        .invoke(builder, LocalDateTime.ofInstant(nestedTimestamp.plusSeconds(10), ZoneId.systemDefault()));
            } else {
                mixedNestedTimestamp = LocalDate.of(2023, 8, 16).atStartOfDay()
                        .toInstant(ZoneOffset.UTC).toEpochMilli(); // TODO - different versions
                nullableMixedNestedTimestamp = LocalDate.of(2023, 8, 16).atStartOfDay()
                        .toInstant(ZoneOffset.UTC).toEpochMilli();

                builder.getClass().getMethod("setNestedTimestamp", Long.TYPE)
                        .invoke(builder, nestedTimestamp.toEpochMilli());
                builder.getClass().getMethod("setNullableNestedTimestamp", Long.class)
                        .invoke(builder, nestedTimestamp.toEpochMilli() + 10L);
            }

            builder.setMixedNestedTimestamp(mixedNestedTimestamp);
            builder.setNullableMixedNestedTimestamp(nullableMixedNestedTimestamp);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        return builder.build();
    }

    private void injectUuidField(FastSerdeLogicalTypesTest1.Builder builder) {
        try {
            if (Utils.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_10)) {
                builder.getClass().getMethod("setUuidField", UUID.class)
                        .invoke(builder, UUID.randomUUID());
            } else {
                builder.getClass().getMethod("setUuidField", CharSequence.class)
                        .invoke(builder, UUID.randomUUID().toString());
            }
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
