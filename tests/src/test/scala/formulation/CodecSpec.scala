package formulation

import java.time.{Instant, LocalDate, LocalDateTime, ZonedDateTime}
import java.util.UUID

import org.scalatest._
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class CodecSpec extends WordSpec with Matchers with GeneratorDrivenPropertyChecks with ArbitraryHelpers {

  "Codec" should {

    "work with Int" in {
      forAll { (a: Int) => assert(a, int) }
    }
    "work with Imap" in {
      forAll { (a: Int) => assert(UserId(a), int.imap(UserId.apply)(_.id)) }
    }
    "work with String" in {
      forAll { (a: String) => assert(a, string) }
    }
    "work with Float" in {
      forAll { (a: Float) => assert(a, float) }
    }
    "work with Bool" in {
      forAll { (a: Boolean) => assert(a, bool) }
    }
    "work with Double" in {
      forAll { (a: Double) => assert(a, double) }
    }
    "work with Long" in {
      forAll { (a: Long) => assert(a, long) }
    }
    "work with UUID" in {
      forAll { (a: UUID) => assert(a, uuid) }
    }
    "work with LocalDate" in {
      forAll { (a: LocalDate) => assert(a, localDate) }
    }
    "work with ZonedDateTime" in {
      forAll { (a: ZonedDateTime) => assert(a, zonedDateTime) }
    }
    "work with Instant" in {
      forAll { (a: Instant) => assert(a, instant) }
    }
    "work with LocalDateTime" in {
      forAll { (a: LocalDateTime) => assert(a, localDateTime) }
    }
    "work with ByteArray" in {
      forAll { (a: Array[Byte]) => assert(a, byteArray) }
    }
    "work with BigDecimal" in {
      forAll { (a: BigDecimal) => assert(a, bigDecimal(300, 300)) }
    }
    "work with Option" in {
      forAll { (a: Option[Int]) => assert(a, option(int)) }
    }
    "work with List" in {
      forAll { (a: List[Int]) => assert(a, list(int)) }
    }
    "work with Set" in {
      forAll { (a: Set[Int]) => assert(a, set(int)) }
    }
    "work with Seq" in {
      forAll { (a: Seq[Int]) => assert(a, seq(int)) }
    }
    "work with Vector" in {
      forAll { (a: Vector[Int]) => assert(a, vector(int)) }
    }
    "work with Map" in {
      forAll(mapGen) { (a: Map[String, Int]) =>
        assert(a, map(int)(Right.apply)(identity))
      }
    }

    "work with Either" in {
      forAll { (a: Either[Int, String]) =>
        assert(a, or(int, string))
      }
    }

    "work with ADT with discriminator" in {
      forAll { (a: BookingProcess) =>
        assert(a, BookingProcess.codec)
      }
    }
    "work with ADT without discriminator" in {
      forAll { (a: Fault) =>
        assert(a, Fault.codec)
      }
    }
    "work with Option with list" in {
      forAll { (a: Option[List[String]]) =>
        assert(a, option(list(string)))
      }
    }

    "work with Option with record" in {
      forAll { (a: Option[UserV1]) =>
        assert(a, option(UserV1.codec))
      }
    }
  }

  def assert[A](entity: A, avroPart: Avro[A]): Boolean = {

    implicit def codec: Avro[Generic[A]] = Generic.codec(avroPart)

    val record = Generic(entity)
    val bytes = encode(record)

    decode[Generic[A]](bytes) == Right(entity)
  }
}
