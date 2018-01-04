package formulation

import java.nio.ByteBuffer

import org.apache.avro.{Conversions, LogicalTypes, Schema}
import shapeless.CNil

import scala.annotation.implicitNotFound

@implicitNotFound(msg = "AvroEncoder[${A}] not found, did you implicitly define Avro[${A}]?")
abstract class AvroEncoder[A](val name: Option[RecordFqdn]) {
  def encode(schema: Schema, value: A): Any
}

object AvroEncoder {

  import scala.collection.JavaConverters._

  def by[A, B](fa: AvroEncoder[A])(f: B => A): AvroEncoder[B] = new AvroEncoder[B](fa.name) {
    override def encode(schema: Schema, value: B): Any = fa.encode(schema, f(value))
  }

  def create[A](f: (Schema, A) => Any): AvroEncoder[A] = new AvroEncoder[A](None) {
    override def encode(schema: Schema, value: A): Any = f(schema, value)
  }

  def createNamed[A](namespace: String, name: String)(f: (Schema, A) => Any): AvroEncoder[A] = new AvroEncoder[A](Some(RecordFqdn(namespace, name))) {
    override def encode(schema: Schema, value: A): Any = f(schema, value)
  }

  implicit def apply[A](implicit A: Avro[A]): AvroEncoder[A] = A.apply[AvroEncoder]

  implicit val interpreter: AvroAlgebra[AvroEncoder] = new AvroAlgebra[AvroEncoder] with AvroEncoderRecordN {

    override val string: AvroEncoder[String] = AvroEncoder.create((_, v) => v)

    override val int: AvroEncoder[Int] = AvroEncoder.create((_, v) => v)

    override val bool: AvroEncoder[Boolean] = AvroEncoder.create((_, v) => v)

    override val float: AvroEncoder[Float] = AvroEncoder.create((_, v) => v)

    override val byteArray: AvroEncoder[Array[Byte]] = AvroEncoder.create((_, v) => ByteBuffer.wrap(v))

    override val double: AvroEncoder[Double] = AvroEncoder.create((_, v) => v)

    override val long: AvroEncoder[Long] = AvroEncoder.create((_, v) => v)

    override val cnil: AvroEncoder[CNil] = AvroEncoder.create((_, v) => null)

    override def bigDecimal(scale: Int, precision: Int): AvroEncoder[BigDecimal] = AvroEncoder.create { case (_, v: BigDecimal) =>
      val decimalType = LogicalTypes.decimal(precision, scale)
      val decimalConversion = new Conversions.DecimalConversion

      decimalConversion.toBytes(v.setScale(scale).bigDecimal, null, decimalType)
    }

    override def imap[A, B](fa: AvroEncoder[A])(f: A => B)(g: B => A): AvroEncoder[B] =
      AvroEncoder.create((schema, v) => fa.encode(schema, g(v)))

    override def option[A](from: AvroEncoder[A]): AvroEncoder[Option[A]] = AvroEncoder.create {
      case (schema, Some(value)) => from.encode(schema, value)
      case (_, None) => null
    }

    override def list[A](of: AvroEncoder[A]): AvroEncoder[List[A]] =
      AvroEncoder.create((schema, list) => list.map(of.encode(schema.getElementType, _)).asJava)

    override def pmap[A, B](fa: AvroEncoder[A])(f: A => Attempt[B])(g: B => A): AvroEncoder[B] =
      AvroEncoder.create((schema, b) => fa.encode(schema, g(b)))

    override def set[A](of: AvroEncoder[A]): AvroEncoder[Set[A]] = by(list(of))(_.toList)

    override def vector[A](of: AvroEncoder[A]): AvroEncoder[Vector[A]] = by(list(of))(_.toList)

    override def seq[A](of: AvroEncoder[A]): AvroEncoder[Seq[A]] = by(list(of))(_.toList)

    override def map[K, V](of: AvroEncoder[V])(mapKey: String => Attempt[K])(contramapKey: K => String): AvroEncoder[Map[K, V]] =
      AvroEncoder.create((schema, mm) => mm.map { case (k, v) => contramapKey(k) -> of.encode(schema.getValueType, v) }.asJava)

    override def or[A, B](fa: AvroEncoder[A], fb: AvroEncoder[B]): AvroEncoder[Either[A, B]] =
      AvroEncoder.create { case (schema, value) =>

        def encode[Z](encoder: AvroEncoder[Z], value: Z) = encoder.name match {
          case Some(fqdn) =>
            val idx = schema.getIndexNamed(s"${fqdn.namespace}.${fqdn.name}")
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