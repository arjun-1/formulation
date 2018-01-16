import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.avro.{AvroRuntimeException, Schema}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import scala.util.control.NonFatal

package object formulation extends AvroDsl {

  /**
    * Constructs a member for a record
    *
    * @param avro One of the combinators of `Avro[A]`, see primitives supported
    * @param getter A function which gives access to the the field in the case class
    * @param defaultValue The default value, use this if you want to introduce new fields and maintain compatibility
    * @param documentation Documentation for this member, it doesn't break compatibility (https://avro.apache.org/docs/1.8.1/spec.html#Parsing+Canonical+Form+for+Schemas)
    * @param aliases Aliases for this member, it doesn't break compatibility (https://avro.apache.org/docs/1.8.1/spec.html#Parsing+Canonical+Form+for+Schemas)
    * @tparam A The member type of the record
    * @tparam B The type of the record it self
    *
    * @return A fully constructed `Member[Avro, A, B]`
    */
  def member[A, B](avro: Avro[A], getter: B => A, defaultValue: Option[A] = None, documentation: Option[String] = None, aliases: Seq[String] = Seq.empty): Member[Avro, A, B] =
    Member[Avro, A, B](avro, getter, aliases, defaultValue.map(avro.apply[AvroDefaultValuePrinter].print), documentation)

  /**
    * Encodes a value of type `A` which has a `AvroEncoder` and `AvroSchema` (gained by having a implicit `Avro[A]` in scope).
    *
    * @param value The value to encode
    * @param R The AvroEncoder for this type
    * @param S The AvroSchema for this type
    * @tparam A The type of the value to encode
    * @return A `AvroEncodeResult` which also contains the used `Schema`, useful when want to store it for example.
    */
  def encoded[A](value: A)(implicit R: AvroEncoder[A], S: AvroSchema[A]): AvroEncodeResult = {
    val os = new ByteArrayOutputStream()

    try {
      val schema = S.generateSchema
      val (usedSchema, record : GenericRecord) = R.encode(schema, value)
      val dataWriter = new GenericDatumWriter[GenericRecord](usedSchema)
      val encoder = EncoderFactory.get().directBinaryEncoder(os, null)

      dataWriter.write(record, encoder)

      encoder.flush()

      AvroEncodeResult(usedSchema, os.toByteArray)
    }
    finally {
      os.close()
    }
  }

  /**
    * Encodes a value of type `A` which has a `AvroEncoder` and `AvroSchema` (gained by having a implicit `Avro[A]` in scope).
    * @param value The value to encode
    * @param R The AvroEncoder for this type
    * @param S The AvroSchema for this type
    * @tparam A The type of the value to encode
    * @return A array of bytes
    */
  def encode[A](value: A)(implicit R: AvroEncoder[A], S: AvroSchema[A]): Array[Byte] = encoded(value).payload

  /**
    * Decodes a array of bytes in to a entity
    *
    * @param bytes The Avro payload
    * @param writerSchema The Schema which was used to write this payload
    * @param readerSchema The Schema which will be used to read this payload
    * @param R The AvroDecoder for this type
    * @param S The AvroSchema for this type
    * @tparam A The type of the value to decode
    * @return Either a error or the decode value.
    */
  def decode[A](bytes: Array[Byte], writerSchema: Option[Schema] = None, readerSchema: Option[Schema] = None)
               (implicit R: AvroDecoder[A], S: AvroSchema[A]): Either[Throwable, A] = {

    val in = new ByteArrayInputStream(bytes)

    try {
      def schema = S.generateSchema
      val wSchema = writerSchema.getOrElse(schema)
      val rSchema = readerSchema.getOrElse(schema)
      val datumReader = new GenericDatumReader[GenericRecord](wSchema, rSchema)
      val binDecoder = DecoderFactory.get().directBinaryDecoder(in, null)
      val record = datumReader.read(null, binDecoder)

      R.decode(rSchema, record)
    }
    catch {
      case NonFatal(ex) => Left(ex)
    }
    finally {
      in.close()
    }
  }

  /**
    * Returns the Avro Schema for a type
    * @param A A implicit Avro available
    * @tparam A The type of the value to get the Schema from
    * @return An Avro Schema
    */
  def schema[A](implicit A: Avro[A]): Schema = A.apply[AvroSchema].generateSchema
}
