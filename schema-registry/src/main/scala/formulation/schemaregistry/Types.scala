package formulation.schemaregistry

import java.nio.ByteBuffer

import cats._
import cats.implicits._
import formulation.{Avro, AvroDecodeError, AvroDecoder, AvroEncodeResult, AvroEncoder, AvroSchema, AvroSchemaCompatibility}
import org.apache.avro.Schema

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

final class SchemaRegistry[F[_]] private(client: SchemaRegistryClient[F])(implicit M: MonadError[F, Throwable]) {

  /**
    * Encodes the a entity according the confluent format: https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html (see wire format)
    *
    * @param value The entity which has a implicit Avro[A] definition available in scope
    * @tparam A The entity we want to decode to
    * @return The payload as a array of bytes
    */
  def encode[A: AvroSchema : AvroEncoder](value: A): F[Array[Byte]] = {
    def format(identifier: Int, payload: Array[Byte]): Array[Byte] = {
      val byteBuffer = ByteBuffer.allocate(5)
      byteBuffer.put(0.toByte)
      byteBuffer.putInt(identifier)
      byteBuffer.array() ++ payload
    }

    def encodeRaw(): F[AvroEncodeResult] =
      try { M.pure(formulation.encoded(value)) } catch { case NonFatal(ex) => M.raiseError(ex) }

    for {
      res <- encodeRaw()
      identifier <- client.getIdBySchema(res.usedSchema)
      payload <- identifier match {
        case Some(id) => M.pure(format(id, res.payload))
        case None => M.raiseError[Array[Byte]](new Throwable(s"There was no schema registered for ${res.usedSchema.getFullName}"))
      }
    } yield payload
  }

  /**
    * Decodes a payload according to the confluent format: https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html (see wire format)
    *
    * @param bytes The payload
    * @tparam A The entity we want to decode to
    * @return Attempt[A], which might be a error or a success case
    */
  def decode[A: AvroSchema : AvroDecoder](bytes: Array[Byte]): F[Either[List[AvroDecodeError], A]] = {
    val bb = ByteBuffer.wrap(bytes)

    for {
      _ <- if (bb.get(0) == 0.toByte) M.pure(())
      else M.raiseError(new Throwable("First byte was not the magic byte (0x0)"))
      identifier = bb.getInt(1)
      schema <- client.getSchemaById(identifier)
      entity <- schema match {
        case Some(s) => M.pure(formulation.decode[A](bb.getByteArray(5), writerSchema = Some(s)))
        case None => M.raiseError(new Throwable(s"There was no schema in the registry for identifier $identifier"))
      }
    } yield entity
  }

  /**
    * Verifies if the schema's are compatible with the current schema's already being registerd in the registry. In case of a union (ADT), we register multiple schema's
    *
    * Each schema's fullName (namespace.name) is used as subject
    *
    * @param avro The avro definition to register
    * @tparam A The type of case class you want to register
    * @return A list of SchemaRegistryCompatibilityResult
    */
  def verifyCompatibility[A](avro: Avro[A], desired: AvroSchemaCompatibility = AvroSchemaCompatibility.Full): F[List[SchemaRegistryCompatibilityResult]] = {
    val schema = avro.apply[AvroSchema].generateSchema

    def run(s: Schema): F[SchemaRegistryCompatibilityResult] = for {
      _ <- client.setCompatibilityLevel(s.getFullName, desired)
      result <- client.checkCompatibility(s).map(SchemaRegistryCompatibilityResult(s, _))
    } yield result

    schema.getType match {
      case Schema.Type.RECORD => run(schema).map(_ :: Nil)
      case Schema.Type.UNION =>
        schema
          .getTypes
          .asScala
          .toList
          .filterNot(_.getType == Schema.Type.NULL)
          .traverse[F, SchemaRegistryCompatibilityResult](run)
      case _ =>
        M.raiseError(new Throwable(s"We cannot verify compatibility of the type: ${schema.getType} as it has no fullname"))
    }
  }

  /**
    * Register the schema's for the specified Avro[A] type. In case it's a union (ADT), we register multiple schema's.
    *
    * Each schema's fullName (namespace.name) is used as subject
    *
    * @param avro The avro definition to register
    * @tparam A The type of case class you want to register
    * @return A list of SchemaRegistryRegisterResult
    */
  def registerSchemas[A](avro: Avro[A]): F[List[SchemaRegistryRegisterResult]] = {
    val schema = avro.apply[AvroSchema].generateSchema

    schema.getType match {
      case Schema.Type.RECORD =>
        client.registerSchema(schema).map(compat => List(SchemaRegistryRegisterResult(schema, compat)))
      case Schema.Type.UNION =>
        schema
          .getTypes
          .asScala
          .toList
          .filterNot(_.getType == Schema.Type.NULL)
          .traverse[F, SchemaRegistryRegisterResult](s => client.registerSchema(s).map(id => SchemaRegistryRegisterResult(s, id)))
      case _ =>
        M.raiseError(new Throwable(s"We cannot register the type: ${schema.getType} as it has no fullname"))
    }
  }

  private implicit class RichByteBuffer(bb: ByteBuffer) {
    def getByteArray(offset: Int): Array[Byte] = {
      bb.array().slice(offset, bb.array().length)
    }
  }

}

object SchemaRegistry {
  def apply[F[_]](client: SchemaRegistryClient[F])(implicit F: MonadError[F, Throwable]): SchemaRegistry[F] =
    new SchemaRegistry[F](client)
}

final case class SchemaRegistryCompatibilityResult(schema: Schema, compatible: Boolean)

final case class SchemaRegistryRegisterResult(schema: Schema, identifier: Int)

trait SchemaRegistryClient[F[_]] {
  def getSchemaById(id: Int): F[Option[Schema]]

  def getIdBySchema(schema: Schema): F[Option[Int]]

  def registerSchema(schema: Schema): F[Int]

  def checkCompatibility(schema: Schema): F[Boolean]

  def getCompatibilityLevel(subject: String): F[Option[AvroSchemaCompatibility]]

  def setCompatibilityLevel(subject: String, desired: AvroSchemaCompatibility): F[AvroSchemaCompatibility]
}

object SchemaRegistryClient {
  def lruCached[F[_]](by: SchemaRegistryClient[F], maxEntries: Int)(implicit M: Monad[F]): SchemaRegistryClient[F] = new SchemaRegistryClient[F] {
    private val idCache = lruCache[Int, Schema]
    private val schemaCache = lruCache[Schema, Int]

    override def getSchemaById(id: Int): F[Option[Schema]] = synchronized(idCache.getOrUpdate(id, by.getSchemaById(id)))

    override def getIdBySchema(schema: Schema): F[Option[Int]] = synchronized(schemaCache.getOrUpdate(schema, by.getIdBySchema(schema)))

    override def registerSchema(schema: Schema): F[Int] = by.registerSchema(schema)

    override def checkCompatibility(schema: Schema): F[Boolean] = by.checkCompatibility(schema)

    private def lruCache[A, B]: mutable.Map[A, B] =
      new java.util.LinkedHashMap[A, B]() {
        override def removeEldestEntry(eldest: java.util.Map.Entry[A, B]): Boolean = size > maxEntries
      }.asScala

    private implicit class RichLinkedHashMap[A, B](map: mutable.Map[A, B]) {
      def getOrUpdate(key: A, value: => F[Option[B]]): F[Option[B]] = {
        map.get(key) match {
          case Some(v) => M.pure(Some(v))
          case None =>
            M.flatMap(value) {
              case Some(v) =>
                map += (key -> v)
                M.pure(Some(v))
              case None => M.pure(None)
            }
        }
      }
    }

    override def getCompatibilityLevel(subject: String): F[Option[AvroSchemaCompatibility]] = by.getCompatibilityLevel(subject)

    override def setCompatibilityLevel(subject: String, desired: AvroSchemaCompatibility): F[AvroSchemaCompatibility] = by.setCompatibilityLevel(subject, desired)
  }
}