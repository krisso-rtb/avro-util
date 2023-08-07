package com.linkedin.avro.fastserde.logical.types;

import static com.linkedin.avro.fastserde.FastSerdeTestsSupport.createField;
import static com.linkedin.avro.fastserde.FastSerdeTestsSupport.createRecord;
import static com.linkedin.avro.fastserde.FastSerdeTestsSupport.getCodeGenDirectory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Instant;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import com.linkedin.avro.fastserde.FastGenericDatumReader;
import com.linkedin.avro.fastserde.FastGenericSerializerGenerator;
import com.linkedin.avro.fastserde.FastGenericSerializerGeneratorTest;
import com.linkedin.avro.fastserde.FastSerdeCache;
import com.linkedin.avro.fastserde.FastSerializer;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;

public class LogicalTypeGenericSerializerTest {

  private File tempDir;
  private ClassLoader classLoader;

  @BeforeTest(groups = {"serializationTest"})
  public void prepare() throws Exception {
    tempDir = getCodeGenDirectory();

    classLoader = URLClassLoader.newInstance(new URL[]{tempDir.toURI().toURL()},
            FastGenericSerializerGeneratorTest.class.getClassLoader());
  }

//  @Ignore
  @Test(groups = {"serializationTest"})
  public void shouldWriteLongAsLogicalTypeInstant() {
    Schema fieldSchema = Schema.create(Schema.Type.LONG);
//    fieldSchema.addProp(LogicalType.LOGICAL_TYPE_PROP, "timestamp-millis");

    LogicalTypes.timestampMillis().addToSchema(fieldSchema);

    GenericData modelData = new GenericData();
    modelData.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());

    Schema.Field field = createField("timestampAsInstant", fieldSchema);
    Schema recordSchema = createRecord("record", field); // LogicalTypesTest1.SCHEMA$;

    GenericRecord genericRecord = new GenericData.Record(recordSchema);
    genericRecord.put("timestampAsInstant", Instant.now());

    Decoder genericDecoder = dataAsBinaryDecoder(genericRecord, recordSchema, modelData);
    GenericRecord decodedRecord = decodeRecord(recordSchema, genericDecoder, modelData);

    genericDecoder = dataAsBinaryDecoder(genericRecord, recordSchema, modelData);
    decodedRecord = decodeRecord(recordSchema, genericDecoder, modelData);

    Object timestampAsInstant = decodedRecord.get("timestampAsInstant");
    Assert.assertTrue(timestampAsInstant instanceof Instant);
  }

  public <T> Decoder dataAsBinaryDecoder(T data, Schema schema, GenericData modelData) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder binaryEncoder = AvroCompatibilityHelper.newBinaryEncoder(baos, true, null); //new BinaryEncoder(baos);

    try {
      FastGenericSerializerGenerator<T> fastGenericSerializerGenerator =
              new FastGenericSerializerGenerator<>(schema, tempDir, classLoader, null, modelData);
      FastSerializer<T> fastSerializer = fastGenericSerializerGenerator.generateSerializer();
      fastSerializer.serialize(data, binaryEncoder);
      binaryEncoder.flush();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return DecoderFactory.get().binaryDecoder(baos.toByteArray(), null);
  }

  public <T> T decodeRecord(Schema schema, Decoder decoder, GenericData modelData) {
//    GenericDatumReader<T> datumReader = new GenericDatumReader<>(schema, schema, modelData);
    FastGenericDatumReader<T, GenericData> datumReader = new FastGenericDatumReader<>(
            schema, schema, FastSerdeCache.getDefaultInstance(), modelData);

    try {
      return datumReader.read(null, decoder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
