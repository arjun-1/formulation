package formulation

import java.nio.ByteBuffer
import java.time.Instant
import java.util.UUID

import org.apache.avro.{Conversions, LogicalTypes, Schema}
import shapeless.CNil

import scala.annotation.implicitNotFound

/**
  * Type class which encodes a value in to a tuple of a used Schema, plus a the encoded value. In the end it will be a
  * `GenericRecord`
  * @param name The RecordName (fully qualified name)
  * @tparam A The type of the value to encode
  */
@implicitNotFound(msg = "AvroEncoder[${A}] not found, did you implicitly define Avro[${A}]?")
abstract class AvroEncoder[A](val name: Option[RecordName]) {
  def encode(schema: Schema, value: A): (Schema, Any)
}

object AvroEncoder {

  import scala.collection.JavaConverters._

  def by[A, B](fa: AvroEncoder[A])(f: B => A): AvroEncoder[B] = new AvroEncoder[B](fa.name) {
    override def encode(schema: Schema, value: B): (Schema, Any) = fa.encode(schema, f(value))
  }

  def create[A](f: (Schema, A) => (Schema, Any)): AvroEncoder[A] = new AvroEncoder[A](None) {
    override def encode(schema: Schema, value: A): (Schema, Any) = f(schema, value)
  }

  def identity[A]: AvroEncoder[A] = new AvroEncoder[A](None) {
    override def encode(schema: Schema, value: A): (Schema, Any) = schema -> value
  }

  def createNamed[A](namespace: String, name: String)(f: (Schema, A) => Any): AvroEncoder[A] = new AvroEncoder[A](Some(RecordName(namespace, name))) {
    override def encode(schema: Schema, value: A): (Schema, Any) = schema -> f(schema, value)
  }

  implicit def apply[A](implicit A: Avro[A]): AvroEncoder[A] = A.apply[AvroEncoder]

  implicit val interpreter: AvroAlgebra[AvroEncoder] = new AvroAlgebra[AvroEncoder] with AvroEncoderRecordN {

    override val string: AvroEncoder[String] = identity

    override val int: AvroEncoder[Int] = identity

    override val bool: AvroEncoder[Boolean] = identity

    override val float: AvroEncoder[Float] = identity

    override val byteArray: AvroEncoder[Array[Byte]] = AvroEncoder.create((s, v) => s -> ByteBuffer.wrap(v))

    override val double: AvroEncoder[Double] = identity

    override val long: AvroEncoder[Long] = identity

    override val cnil: AvroEncoder[CNil] = AvroEncoder.create((s, v) => s -> null)

    override val uuid: AvroEncoder[UUID] = by(string)(_.toString)
    
    override val instant: AvroEncoder[Instant] = by(long)(_.toEpochMilli)

    override def bigDecimal(scale: Int, precision: Int): AvroEncoder[BigDecimal] = AvroEncoder.create { case (s, v: BigDecimal) =>
      val decimalType = LogicalTypes.decimal(precision, scale)
      val decimalConversion = new Conversions.DecimalConversion

      s -> decimalConversion.toBytes(v.setScale(scale).bigDecimal, null, decimalType)
    }

    override def imap[A, B](fa: AvroEncoder[A])(f: A => B)(g: B => A): AvroEncoder[B] =
      AvroEncoder.create((schema, v) => fa.encode(schema, g(v)))

    override def option[A](from: AvroEncoder[A]): AvroEncoder[Option[A]] = AvroEncoder.create {
      case (schema, Some(value)) => from.encode(schema.getTypes.asScala.find(_.getType != Schema.Type.NULL).orNull, value)
      case (schema, None) => schema -> null
    }

    override def list[A](of: AvroEncoder[A]): AvroEncoder[List[A]] =
      AvroEncoder.create((schema, list) => schema -> list.map(of.encode(schema.getElementType, _)._2).asJava)

    override def pmap[A, B](fa: AvroEncoder[A])(f: A => Either[Throwable, B])(g: B => A): AvroEncoder[B] =
      AvroEncoder.create((schema, b) => fa.encode(schema, g(b)))

    override def set[A](of: AvroEncoder[A]): AvroEncoder[Set[A]] = by(list(of))(_.toList)

    override def vector[A](of: AvroEncoder[A]): AvroEncoder[Vector[A]] = by(list(of))(_.toList)

    override def seq[A](of: AvroEncoder[A]): AvroEncoder[Seq[A]] = by(list(of))(_.toList)

    override def map[K, V](of: AvroEncoder[V])(mapKey: String => Either[Throwable, K])(contramapKey: K => String): AvroEncoder[Map[K, V]] =
      AvroEncoder.create((schema, mm) => schema -> mm.map { case (k, v) => contramapKey(k) -> of.encode(schema.getValueType, v)._2 }.asJava)

    override def or[A, B](fa: AvroEncoder[A], fb: AvroEncoder[B]): AvroEncoder[Either[A, B]] =
      AvroEncoder.create { case (schema, value) =>

        def encode[Z](encoder: AvroEncoder[Z], value: Z) = encoder.name match {
          case Some(n) =>
            val idx = schema.getIndexNamed(s"${n.namespace}.${n.name}")
            val types = schema.getTypes.asScala

            encoder.encode(types(idx), value)

          case None => encoder.encode(schema, value)
        }

        value match {
          case Left(left) => encode(fa, left)
          case Right(right) => encode(fb, right)
        }
      }

  }
}