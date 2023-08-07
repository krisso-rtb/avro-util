package com.linkedin.avro.fastserde.logical.types;

import static com.linkedin.avro.fastserde.FastSerdeTestsSupport.getCodeGenDirectory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.data.TimeConversions.TimeMicrosConversion;
import org.apache.avro.data.TimeConversions.TimestampMicrosConversion;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.FastGenericSerializerGeneratorTest;
import com.linkedin.avro.fastserde.FastSerdeCache;
import com.linkedin.avro.fastserde.FastSerializer;
import com.linkedin.avro.fastserde.Utils;
import com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesTest1;
import com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecord;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelperCommon;
import com.linkedin.avroutil1.compatibility.AvroVersion;

public class LogicalTypeSpecificSerializerTest {

  private File tempDir;
  private ClassLoader classLoader;

  @BeforeTest(groups = {"serializationTest"})
  public void prepare() throws Exception {
    tempDir = getCodeGenDirectory();

    classLoader = URLClassLoader.newInstance(new URL[]{tempDir.toURI().toURL()},
            FastGenericSerializerGeneratorTest.class.getClassLoader());
  }

//  @org.testng.annotations.Ignore
  @Test(groups = {"serializationTest"})
  public void shouldWriteLongAsLogicalTypeInstant() throws IOException {
    // given
    LocalDate localDate = LocalDate.of(2023, 8, 11);
    Instant instant = localDate.atStartOfDay().toInstant(ZoneOffset.UTC);
    Instant nextDay = instant.plus(1, ChronoUnit.DAYS);
    FastSerdeLogicalTypesTest1.Builder builder = FastSerdeLogicalTypesTest1.newBuilder()
            .setDecimalOrDateUnion(new BigDecimal("12.34"))
            .setTimestampMillisField(instant)
            .setTimestampMicrosField(instant)
            .setTimeMillisField(LocalTime.of(14, 17, 45, 12345))
            .setTimeMicrosField(LocalTime.of(14, 17, 45, 12345))
            .setDateField(localDate)
            .setNestedLocalTimestampMillis(createLocalTimestampRecord(nextDay));
    injectUuidField(builder);
    FastSerdeLogicalTypesTest1 inputData = builder.build();

//    System.out.println("inputData.toByteBuffer().array():\n" + Arrays.toString(inputData.toByteBuffer().array()));
//
//    InMemoryEncoder encoder = new InMemoryEncoder();
//
//    new SpecificDatumWriter<FastSerdeLogicalTypesTest1>(inputData.getSchema(), inputData.getSpecificData())
//            .write(inputData, encoder);
//
//    System.out.println("SpecificDatumWriter:\n" + Arrays.toString(encoder.toByteArray()));
//
    Schema schema = inputData.getSchema();
    SpecificData specificData = inputData.getSpecificData();

    if (AvroCompatibilityHelperCommon.getRuntimeAvroVersion() == AvroVersion.AVRO_1_9) {
      // TODO it should be injected by default.
      // TODO conversion is must-have otherwise it's in conflict with stringable feature (BigDecimal is considered as string, not bytes).
      // details: GenericData.resolveUnion() + SpecificData.getSchemaName()
      specificData.addLogicalTypeConversion(new Conversions.DecimalConversion());
      specificData.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
      specificData.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
      specificData.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());

      // LocalTimestampMillisConversion not available in 1.9
    }

    GenericData genericData = new GenericData();
    Optional.ofNullable(specificData.getConversions())
            .orElse(Collections.emptyList())
            .forEach(genericData::addLogicalTypeConversion);

    FastSerdeCache fastSerdeCache = FastSerdeCache.getDefaultInstance();

    @SuppressWarnings("unchecked")
    FastSerializer<FastSerdeLogicalTypesTest1> fastSpecificSerializer = (FastSerializer<FastSerdeLogicalTypesTest1>) fastSerdeCache
            .buildFastSpecificSerializer(schema, specificData);
//
//    encoder = new InMemoryEncoder();
//    fastSpecificSerializer.serialize(inputData, encoder);
//    System.out.println("fastSpecificSerializer:\n" + Arrays.toString(encoder.toByteArray()));

    @SuppressWarnings("unchecked")
    FastSerializer<FastSerdeLogicalTypesTest1> fastGenericSerializer = (FastSerializer<FastSerdeLogicalTypesTest1>) fastSerdeCache
            .buildFastGenericSerializer(schema, genericData);

    FastDeserializer<?> fastSpecificDeserializer = fastSerdeCache.buildFastSpecificDeserializer(schema, schema, specificData);
    FastDeserializer<?> fastGenericDeserializer = fastSerdeCache.buildFastGenericDeserializer(schema, schema, genericData);

    verify(inputData, fastSpecificSerializer, fastSpecificDeserializer);
    verify(inputData, fastSpecificSerializer, fastGenericDeserializer);
    verify(inputData, fastGenericSerializer, fastGenericDeserializer);
    verify(inputData, fastGenericSerializer, fastSpecificDeserializer);
  }

  private LocalTimestampRecord createLocalTimestampRecord(Instant nestedTimestamp) {
    LocalTimestampRecord.Builder builder = LocalTimestampRecord.newBuilder();

    try {
      if (Utils.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_9)) {
        builder.getClass().getMethod("setNestedTimestamp", LocalDateTime.class)
                .invoke(builder, LocalDateTime.ofInstant(nestedTimestamp, ZoneId.systemDefault()));
      } else {
        builder.getClass().getMethod("setNestedTimestamp", Long.TYPE)
                .invoke(builder, nestedTimestamp.toEpochMilli());
      }
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

//  private <T> void verify(T data, FastSerializer<T> fastSerializer, FastDeserializer<?> fastDeserializer) throws IOException {
  private void verify(FastSerdeLogicalTypesTest1 data, FastSerializer<FastSerdeLogicalTypesTest1> fastSerializer, FastDeserializer<?> fastDeserializer) throws IOException {
    InMemoryEncoder encoder = new InMemoryEncoder();

    new SpecificDatumWriter<FastSerdeLogicalTypesTest1>(data.getSchema(), data.getSpecificData())
                .write(data, encoder);

    System.out.println("SpecificDatumWriter: " + Arrays.toString(encoder.toByteArray()));

    ///////////////////
    encoder = new InMemoryEncoder();
    fastSerializer.serialize(data, encoder);

    System.out.println("fastSerializer:      " + Arrays.toString(encoder.toByteArray()));
    System.out.println("native:              " + Arrays.toString(data.toByteBuffer().array()));

    ///////////////////

    Object decoded = new SpecificDatumReader<FastSerdeLogicalTypesTest1>(data.getSchema(), data.getSchema(), data.getSpecificData())
            .read(null, encoder.toDecoder());
    Assert.assertEquals(decoded, data);

    GenericData genericData = new GenericData();
    Optional.ofNullable(data.getSpecificData().getConversions())
            .orElse(Collections.emptyList())
            .forEach(genericData::addLogicalTypeConversion);

    decoded = new GenericDatumReader<>(data.getSchema(), data.getSchema(), genericData)
            .read(null, encoder.toDecoder());
    
    // TODO compare serialized bytes
    Assert.assertEquals(decoded.toString(), data.toString());
//
//    new GenericDatumWriter<FastSerdeLogicalTypesTest1>(data.getSchema(), data.getSpecificData())
//            .write(data, encoder);
//
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//    Encoder binaryEncoder = AvroCompatibilityHelper.newBinaryEncoder(baos, true, null); //new BinaryEncoder(baos);

    encoder = new InMemoryEncoder();
    fastSerializer.serialize(data, encoder);

    Object deserialized = fastDeserializer.deserialize(encoder.toDecoder());

    // TODO compare serialized byte[]
    Assert.assertEquals(data.toString(), deserialized.toString());
  }
}
