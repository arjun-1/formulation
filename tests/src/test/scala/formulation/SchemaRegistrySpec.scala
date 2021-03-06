package formulation

import cats._
import cats.data.{EitherT, StateT}
import cats.implicits._
import formulation.schemaregistry.{SchemaRegistryClient, _}
import org.apache.avro.Schema
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{EitherValues, Matchers, WordSpec}

class SchemaRegistrySpec extends WordSpec with GeneratorDrivenPropertyChecks with Matchers with ArbitraryHelpers with EitherValues {

  case class SchemaEntry(id: Int, subject: String, schema: Schema)
  case class SchemaRegistryState(compatLevels: Map[String, AvroSchemaCompatibility], entries: List[SchemaEntry])

  private val sr = SchemaRegistry[StateT[EitherT[Eval, Throwable, ?], SchemaRegistryState, ?]](client)
  private val entries = List(
    SchemaEntry(1, "formulation.DateSelected", schema[BookingProcess.DateSelected]),
    SchemaEntry(2, "formulation.Cancelled", schema[BookingProcess.Cancelled]),
    SchemaEntry(3, "formulation.NotStarted", schema[BookingProcess.NotStarted]),
    SchemaEntry(4, "event.Completed", schema[Event.Completed]),
    SchemaEntry(5, "event.Failed", schema[Event.Failed]),
    SchemaEntry(6, "event.Started", schema[Event.Started])
  )
  private def registryState(schemaEntries: List[SchemaEntry] = entries, compatLevels: Map[String, AvroSchemaCompatibility] = Map.empty) =
    SchemaRegistryState(compatLevels, schemaEntries)

  "SchemaRegistry" should {

    "encode/decode into symmetrical results with unions" in {
      forAll { (event: Event) =>
        (sr.encode(event) >>= sr.decode[Event]).runA(registryState()).value.value shouldBe Right(Right(event))
      }
    }

    "encode/decode into symmetrical results with records" in {
      forAll { (user: UserV1) =>
        (sr.encode(user) >>= sr.decode[UserV1]).runA(registryState(List(SchemaEntry(1, "user.User", schema[UserV1])))).value.value shouldBe Right(Right(user))
      }
    }

    "encode/decode into symmetrical results with compatible records" in {
      forAll { (user: Either[UserV1, UserV2]) =>
        def prg = user match {
          case Left(u) => sr.encode(u) >>= sr.decode[UserV1]
          case Right(u) => sr.encode(u) >>= sr.decode[UserV1]
        }

        def asUserV1 = user match {
          case Left(u) => u
          case Right(u) => UserV1(userId = u.userId, username = u.username, email = u.email, password = u.password)
        }

        val schemaEntries = List(
          SchemaEntry(1, "user.User", schema[UserV1]),
          SchemaEntry(2, "user.User", schema[UserV2])
        )

        prg.runA(registryState(schemaEntries)).value.value shouldBe Right(Right(asUserV1))
      }
    }

    "verify compatibility - return true for all members in union" in {

      val result = sr.verifyCompatibility(BookingProcess.codec).run(registryState(List.empty)).value.value

      val expectedState = registryState(
        schemaEntries = List.empty,
        compatLevels = Map(
          "formulation.Cancelled" -> AvroSchemaCompatibility.Full,
          "formulation.DateSelected" -> AvroSchemaCompatibility.Full,
          "formulation.NotStarted" -> AvroSchemaCompatibility.Full
        )
      )

      val expectedResults = List(
        SchemaRegistryCompatibilityResult(schema[BookingProcess.Cancelled], true),
        SchemaRegistryCompatibilityResult(schema[BookingProcess.DateSelected], true),
        SchemaRegistryCompatibilityResult(schema[BookingProcess.NotStarted], true)
      )

      result shouldBe Right(expectedState -> expectedResults)
    }


    "verify compatibility - return false for incompatible record" in {

      implicit val codec: Avro[Generic[Int]] = Generic.codec(int)

      sr.verifyCompatibility(UserV1.codec).runA(registryState(List(SchemaEntry(1, "user.User", schema[Generic[Int]])))).value.value shouldBe Right(
        List(SchemaRegistryCompatibilityResult(schema[UserV1], false))
      )
    }

    "register - be idempotent" in {
      val expectedResults = List(
        SchemaRegistryRegisterResult(schema[BookingProcess.Cancelled], 2),
        SchemaRegistryRegisterResult(schema[BookingProcess.DateSelected], 1),
        SchemaRegistryRegisterResult(schema[BookingProcess.NotStarted], 3)
      )

      val expectedState = registryState()

      sr.registerSchemas(BookingProcess.codec).run(registryState()).value.value shouldBe Right(expectedState -> expectedResults)
    }

    "register - insert union values" in {
      sr.registerSchemas(BookingProcess.codec).runA(registryState(List.empty)).value.value shouldBe Right(
        List(
          SchemaRegistryRegisterResult(schema[BookingProcess.Cancelled], 1),
          SchemaRegistryRegisterResult(schema[BookingProcess.DateSelected], 2),
          SchemaRegistryRegisterResult(schema[BookingProcess.NotStarted], 3)
        )
      )
    }
    "register - insert record values" in {
      implicit val codec: Avro[Generic[Int]] = Generic.codec(int)

      sr.registerSchemas(codec).runA(registryState(List.empty)).value.value shouldBe Right(List(SchemaRegistryRegisterResult(schema[Generic[Int]], 1)))
    }

    "fail encoding when there is no schema registered" in {
      sr.encode(BookingProcess.NotStarted(0)).runA(registryState(List.empty)).value.value.left.value.getMessage shouldBe "There was no schema registered for formulation.NotStarted"
    }

    "fail decoding when the message doesn't start with the magic byte" in {
      sr.decode[BookingProcess](Array(1, 2, 3).map(_.toByte)).runA(registryState()).value.value.left.value.getMessage shouldBe "First byte was not the magic byte (0x0)"
    }

    "fail decoding when the message identifier is not found in the schema registry" in {
      def prg = for {
        bytes <- sr.encode(BookingProcess.NotStarted(0))
        _ <- StateT.modify[EitherT[Eval, Throwable, ?], SchemaRegistryState](_.copy(entries = List.empty))
        result <- sr.decode[BookingProcess](bytes)
      } yield result

      prg.runA(registryState()).value.value.left.value.getMessage shouldBe "There was no schema in the registry for identifier 3"
    }

    "fail compatibility check when the type is not union/record" in {
      sr.verifyCompatibility(int).runA(registryState()).value.value.left.value.getMessage shouldBe "We cannot verify compatibility of the type: INT as it has no fullname"
    }

    "fail registering a schema when the type is not union/record" in {
      sr.registerSchemas(int).runA(registryState()).value.value.left.value.getMessage shouldBe "We cannot register the type: INT as it has no fullname"
    }

  }

  private def client[F[_]](implicit F: Monad[F]): SchemaRegistryClient[StateT[F, SchemaRegistryState, ?]] = new SchemaRegistryClient[StateT[F, SchemaRegistryState, ?]] {
    override def getSchemaById(id: Int): StateT[F, SchemaRegistryState, Option[Schema]] =
      StateT.inspect[F, SchemaRegistryState, Option[Schema]](_.entries.find(_.id == id).map(_.schema))

    override def getIdBySchema(schema: Schema): StateT[F, SchemaRegistryState, Option[Int]] =
      StateT.inspect[F, SchemaRegistryState, Option[Int]](_.entries.find(x => x.schema == schema && x.subject == schema.getFullName).map(_.id))

    override def registerSchema(schema: Schema): StateT[F, SchemaRegistryState, Int] =
      for {
        existingId <- StateT.inspect[F, SchemaRegistryState, Option[Int]](_.entries.find(_.schema == schema).map(_.id))
        id <- existingId match {
          case Some(id) => StateT.pure[F, SchemaRegistryState, Int](id)
          case None =>
            for {
              maxId <- StateT.inspect[F, SchemaRegistryState, Int](x => if (x.entries.nonEmpty) x.entries.maxBy(_.id).id else 0)
              _ <- StateT.modify[F, SchemaRegistryState](sr => sr.copy(entries = SchemaEntry(maxId + 1, schema.getFullName, schema) :: sr.entries))
            } yield maxId + 1
        }
      } yield id

    override def checkCompatibility(schema: Schema): StateT[F, SchemaRegistryState, Boolean] =
      StateT.inspect[F, SchemaRegistryState, Boolean](_.entries.filter(_.subject == schema.getFullName).forall(y => AvroSchemaCompatibility(y.schema, schema) == AvroSchemaCompatibility.Full))

    override def getCompatibilityLevel(subject: String): StateT[F, SchemaRegistryState, Option[AvroSchemaCompatibility]] =
      StateT.inspect[F, SchemaRegistryState, Option[AvroSchemaCompatibility]](_.compatLevels.get(subject))

    override def setCompatibilityLevel(subject: String, desired: AvroSchemaCompatibility): StateT[F, SchemaRegistryState, AvroSchemaCompatibility] =
      StateT.modify[F, SchemaRegistryState](sr => sr.copy(compatLevels = sr.compatLevels + (subject -> desired))) >> StateT.pure(desired)
  }

}
