package formulation

import eu.timepit.refined.api.Refined
import eu.timepit.refined._
import eu.timepit.refined.collection._
import eu.timepit.refined.numeric.Positive
import formulation.refined._
import org.scalatest.{EitherValues, Matchers, WordSpec}

class RefinedSpec extends WordSpec with Matchers with EitherValues {
  "Refined" should {
    "be compatible" in {
      val schemaRefined = AvroSchema[PersonRefined].generateSchema
      val schemaUnrefined = AvroSchema[PersonUnrefined].generateSchema

      AvroSchemaCompatibility(schemaRefined, schemaUnrefined) shouldBe AvroSchemaCompatibility.Full
    }

    "succeed decoding with valid bytes" in {
      val entity = PersonRefined(refineMV("Mark"), refineMV(1337))
      val bytes = encode(entity)

      decode[PersonRefined](bytes) shouldBe Right(entity)
    }

    "fail decoding with invalid bytes" in {
      val entity = PersonUnrefined("Mark", -1)
      val bytes = encode(entity)

      decode[PersonRefined](bytes).left shouldBe "Predicate failed: (-1 > 0)."
    }
  }
}

case class PersonUnrefined(name: String, age: Int)

object PersonUnrefined {
  implicit val codec: Avro[PersonUnrefined] = record2("user", "Person")(PersonUnrefined.apply)(
    "name" -> member(string, _.name),
    "age" -> member(int, _.age)
  )
}

case class PersonRefined(name: String Refined NonEmpty, age: Int Refined Positive)

object PersonRefined {
  implicit val codec: Avro[PersonRefined] = record2("user", "Person")(PersonRefined.apply)(
    "name" -> member(string.refine[NonEmpty], _.name),
    "age" -> member(int.refine[Positive], _.age)
  )
}