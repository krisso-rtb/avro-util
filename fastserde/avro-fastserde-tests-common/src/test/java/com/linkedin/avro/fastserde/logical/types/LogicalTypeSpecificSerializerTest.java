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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

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
import org.testng.annotations.DataProvider;
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
import com.linkedin.avroutil1.compatibility.SchemaNormalization;

public class LogicalTypeSpecificSerializerTest {

    private final int v1HeaderLength = 10;

    @DataProvider
    public static Object[][] logicalTypesTestCases() {
        LocalDate now = LocalDate.now();
        LocalDate localDate = LocalDate.of(2023, 8, 11);

        Map<String, LocalDate> mapOfDates = new HashMap<>();
        mapOfDates.put("today", now);
        mapOfDates.put("yesterday", now.minusDays(1));
        mapOfDates.put("tomorrow", now.plusDays(1));

        Object[] unionOfArrayAndMapOptions = {
                Lists.newArrayList(LocalTime.now(), LocalTime.now().plusMinutes(1)), mapOfDates};
        Object[] nullableArrayOfDatesOptions = {
                null, Lists.newArrayList(localDate, localDate.plusDays(11), localDate.plusDays(22))};
        Object[] decimalOrDateOptions = {new BigDecimal("3.14"), LocalDate.of(2023, 3, 14)};
        Object[] nullableUnionOfDateAndLocalTimestampOptions = {null, now.minusDays(12), localDate.atStartOfDay()};
        Object[] unionOfDateAndLocalTimestampOptions = {now.minusDays(12), localDate.atStartOfDay()};

        List<Object[]> allOptions = new ArrayList<>();

        for (Object unionOfArrayAndMap : unionOfArrayAndMapOptions) {
            for (Object nullableArrayOfDates : nullableArrayOfDatesOptions) {
                for (Object decimalOrDate : decimalOrDateOptions) {
                    for (Object nullableUnionOfDateAndLocalTimestamp: nullableUnionOfDateAndLocalTimestampOptions) {
                        for (Object unionOfDateAndLocalTimestamp : unionOfDateAndLocalTimestampOptions) {
                            allOptions.add(new Object[]{unionOfArrayAndMap, nullableArrayOfDates, decimalOrDate,
                                    nullableUnionOfDateAndLocalTimestamp, unionOfDateAndLocalTimestamp});
                        }
                    }
                }
            }
        }

        return allOptions.toArray(new Object[0][]);
    }

    @Test(groups = "serializationTest", dataProvider = "logicalTypesTestCases")
    public void shouldWriteAndReadLogicalTypes(Object unionOfArrayAndMap, List<LocalDate> nullableArrayOfDates,
            Object decimalOrDate, Object nullableUnionOfDateAndLocalTimestamp, Object unionOfDateAndLocalTimestamp) throws IOException {
        // given
        LocalDate localDate = LocalDate.of(2023, 8, 11);
        Instant instant = localDate.atStartOfDay().toInstant(ZoneOffset.UTC);

        FastSerdeLogicalTypesTest1.Builder builder = FastSerdeLogicalTypesTest1.newBuilder()
                .setUnionOfArrayAndMap(unionOfArrayAndMap)
                .setTimestampMillisMap(createTimestampMillisMap())
                .setNullableArrayOfDates(nullableArrayOfDates)
                .setArrayOfDates(Lists.newArrayList(localDate, localDate.plusDays(1), localDate.plusDays(2)))
                .setUnionOfDecimalOrDate(decimalOrDate)
                .setTimestampMillisField(instant)
                .setTimestampMicrosField(instant)
                .setTimeMillisField(LocalTime.of(14, 17, 45, 12345))
                .setTimeMicrosField(LocalTime.of(14, 17, 45, 12345))
                .setDateField(localDate)
                .setNestedLocalTimestampMillis(createLocalTimestampRecord(nullableUnionOfDateAndLocalTimestamp, unionOfDateAndLocalTimestamp));
        injectUuidField(builder);
        FastSerdeLogicalTypesTest1 inputData = builder.build();

        // all serializers produce the same array of bytes
        byte[] bytesWithHeader = verifySerializers(inputData);

        // all deserializers create (logically) the same data (in generic or specific representation)
        verifyDeserializers(bytesWithHeader);
    }

    private byte[] verifySerializers(FastSerdeLogicalTypesTest1 data) throws IOException {
        // given
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

        // when
        byte[] fastGenericBytes = serialize(fastGenericSerializer, data);
        byte[] fastSpecificBytes = serialize(fastSpecificSerializer, data);
        byte[] genericBytes = serialize(genericDatumWriter, data);
        byte[] specificBytes = serialize(specificDatumWriter, data);
        byte[] bytesWithHeader = data.toByteBuffer().array(); // contains 10 extra bytes at the beginning

        byte[] expectedHeaderBytes = ByteBuffer.wrap(new byte[v1HeaderLength])
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(new byte[]{(byte) 0xC3, (byte) 0x01}) // BinaryMessageEncoder.V1_HEADER
                .putLong(SchemaNormalization.parsingFingerprint64(data.getSchema()))
                .array();

        // then all 5 serializing methods should return the same array of bytes
        Assert.assertEquals(Arrays.copyOf(bytesWithHeader, v1HeaderLength), expectedHeaderBytes);
        Assert.assertEquals(fastGenericBytes, Arrays.copyOfRange(bytesWithHeader, v1HeaderLength, bytesWithHeader.length));
        Assert.assertEquals(fastGenericBytes, fastSpecificBytes);
        Assert.assertEquals(fastGenericBytes, genericBytes);
        Assert.assertEquals(fastGenericBytes, specificBytes);

        return bytesWithHeader;
    }

    private void verifyDeserializers(byte[] bytesWithHeader) throws IOException {
        // given
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

        // when deserializing with different serializers/writers
        GenericData.Record deserializedFastGeneric = fastGenericDeserializer.deserialize(decoderSupplier.get());
        FastSerdeLogicalTypesTest1 deserializedFastSpecific = fastSpecificDeserializer.deserialize(decoderSupplier.get());
        GenericData.Record deserializedGenericReader = genericDatumReader.read(null, decoderSupplier.get());
        FastSerdeLogicalTypesTest1 deserializedSpecificReader = specificDatumReader.read(null, decoderSupplier.get());

        // then
        Assert.assertEquals(deserializedFastSpecific, data);
        Assert.assertEquals(deserializedSpecificReader, data);
        assertEquals(deserializedFastGeneric, data);
        assertEquals(deserializedGenericReader, data);
    }

    private void assertEquals(GenericData.Record actual, FastSerdeLogicalTypesTest1 expected) throws IOException {
        Assert.assertEquals(actual.toString(), expected.toString());

        GenericDatumWriter<GenericData.Record> genericDatumWriter = new GenericDatumWriter<>(
                actual.getSchema(), copyConversions(expected.getSpecificData(), new GenericData()));

        SpecificDatumWriter<FastSerdeLogicalTypesTest1> specificDatumWriter = new SpecificDatumWriter<>(
                expected.getSchema(), copyConversions(expected.getSpecificData(), new SpecificData()));

        byte[] genericBytes = serialize(genericDatumWriter, actual);
        byte[] expectedBytes = serialize(specificDatumWriter, expected);

        Assert.assertEquals(genericBytes, expectedBytes);
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

    private LocalTimestampRecord createLocalTimestampRecord(
            Object nullableUnionOfDateAndLocalTimestamp, Object unionOfDateAndLocalTimestamp) {
        Instant nestedTimestamp = toInstant(LocalDate.of(2023, 8, 21));
        LocalTimestampRecord.Builder builder = LocalTimestampRecord.newBuilder();

        try {
            if (Utils.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_9)) {
                builder.getClass().getMethod("setNestedTimestamp", LocalDateTime.class)
                        .invoke(builder, LocalDateTime.ofInstant(nestedTimestamp, ZoneId.systemDefault()));
                builder.getClass().getMethod("setNullableNestedTimestamp", LocalDateTime.class)
                        .invoke(builder, LocalDateTime.ofInstant(nestedTimestamp.plusSeconds(10), ZoneId.systemDefault()));
            } else {
                nullableUnionOfDateAndLocalTimestamp = Optional.ofNullable(toInstant(nullableUnionOfDateAndLocalTimestamp))
                        .map(Instant::toEpochMilli)
                        .orElse(null);
                unionOfDateAndLocalTimestamp = toInstant(unionOfDateAndLocalTimestamp).toEpochMilli();

                builder.getClass().getMethod("setNestedTimestamp", Long.TYPE)
                        .invoke(builder, nestedTimestamp.toEpochMilli());
                builder.getClass().getMethod("setNullableNestedTimestamp", Long.class)
                        .invoke(builder, nestedTimestamp.toEpochMilli() + 10L);
            }

            builder.setNullableUnionOfDateAndLocalTimestamp(nullableUnionOfDateAndLocalTimestamp);
            builder.setUnionOfDateAndLocalTimestamp(unionOfDateAndLocalTimestamp);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        return builder.build();
    }

    private Instant toInstant(Object maybeDate) {
        if (maybeDate == null) {
            return null;
        } else if (maybeDate instanceof LocalDate) {
            return ((LocalDate) maybeDate).atStartOfDay(ZoneId.systemDefault()).toInstant();
        } else if (maybeDate instanceof LocalDateTime) {
            return ((LocalDateTime) maybeDate).toInstant(ZoneOffset.UTC);
        } else {
            throw new UnsupportedOperationException(maybeDate + " is not supported (yet)");
        }
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
