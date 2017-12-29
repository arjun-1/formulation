import sbt._


object Boilerplate {

  import scala.StringContext._

  implicit final class BlockHelper(val sc: StringContext) extends AnyVal {
    def block(args: Any*): String = {
      val interpolated = sc.standardInterpolator(treatEscapes, args)
      val rawLines = interpolated split '\n'
      val trimmedLines = rawLines map {
        _ dropWhile (_.isWhitespace)
      }
      trimmedLines mkString "\n"
    }
  }


  val templates: Seq[Template] = Seq(
    GenAvroAlgebra,
    GenAvroDsl,
    GenAvroSchema,
    GenAvroDecoder,
    GenAvroEncoder
  )

  val header = "// auto-generated boilerplate" // TODO: put something meaningful here?


  /** Returns a seq of the generated files.  As a side-effect, it actually generates them... */
  def gen(dir: File) = for (t <- templates) yield {
    val tgtFile = t.filename(dir)
    IO.write(tgtFile, t.body)
    tgtFile
  }

  val maxArity = 22

  final class TemplateVals(val arity: Int) {
    val synTypes = (0 until arity) map (n => s"A$n")
    val synVals = (0 until arity) map (n => s"a$n")
    val synTypedVals = (synVals zip synTypes) map { case (v, t) => v + ":" + t }
    val `A..N` = synTypes.mkString(", ")
    val `a..n` = synVals.mkString(", ")
    val `_.._` = Seq.fill(arity)("_").mkString(", ")
    val `(A..N)` = if (arity == 1) "Tuple1[A]" else synTypes.mkString("(", ", ", ")")
    val `(_.._)` = if (arity == 1) "Tuple1[_]" else Seq.fill(arity)("_").mkString("(", ", ", ")")
    val `(a..n)` = if (arity == 1) "Tuple1(a)" else synVals.mkString("(", ", ", ")")
    val `a:A..n:N` = synTypedVals mkString ", "
  }

  trait Template {
    def filename(root: File): File

    def content(tv: TemplateVals): String

    def range = 1 to maxArity

    def body: String = {
      def expandInstances(contents: IndexedSeq[Array[String]], acc: Array[String] = Array.empty): Array[String] =
        if (!contents.exists(_ exists (_ startsWith "-")))
          acc map (_.tail)
        else {
          val pre = contents.head takeWhile (_ startsWith "|")
          val instances = contents flatMap {
            _ dropWhile (_ startsWith "|") takeWhile (_ startsWith "-")
          }
          val next = contents map {
            _ dropWhile (_ startsWith "|") dropWhile (_ startsWith "-")
          }
          expandInstances(next, acc ++ pre ++ instances)
        }

      val rawContents = range map { n => content(new TemplateVals(n)) split '\n' filterNot (_.isEmpty) }
      val headerLines = header split '\n'
      val instances = expandInstances(rawContents)
      val footerLines = rawContents.head.reverse.takeWhile(_ startsWith "|").map(_.tail).reverse
      (headerLines ++ instances ++ footerLines) mkString "\n"
    }
  }

  object GenAvroAlgebra extends Template {
    def filename(root: File) = root /  "formulation" / "AvroAlgebraRecordN.scala"

    def content(tv: TemplateVals) = {
      import tv._

      val params = synTypes map { tpe => s"param$tpe: (String, Member[F, $tpe, Z])"} mkString ", "

      block"""
         |package formulation
         |
         |trait AvroAlgebraRecordN[F[_]] {
         -  def record${arity}[${`A..N`}, Z](namespace: String, name: String)(f: (${`A..N`}) => Z)($params): F[Z]
         |}
      """
    }
  }

  object GenAvroDsl extends Template {
    def filename(root: File) = root /  "formulation" / "AvroDslRecordN.scala"

    def content(tv: TemplateVals) = {
      import tv._

      val params = synTypes map { tpe => s"param$tpe: (String, Member[Avro, $tpe, Z])"} mkString ", "
      val applies = synTypes map { tpe => s"param$tpe._1 -> param$tpe._2.mapTypeClass(naturalTransformation)"} mkString ", "

      block"""
        |package formulation
        |
        |import cats._
        |
        |trait AvroDslRecordN {
        |  private def naturalTransformation[G[_] : AvroAlgebra]: (Avro ~> G) = new (Avro ~> G) {
        |    override def apply[A](fa: Avro[A]): G[A] = fa.apply[G]
        |  }
        -  def record$arity[${`A..N`}, Z](namespace: String, name: String)(f: (${`A..N`}) => Z)($params): Avro[Z] = new Avro[Z] { def apply[F[_] : AvroAlgebra]: F[Z] = implicitly[AvroAlgebra[F]].record$arity(namespace, name)(f)($applies) }
        |}
        |
      """
    }
  }

  object GenAvroSchema extends Template {
    def filename(root: File) = root /  "formulation" / "AvroSchemaRecordN.scala"

    def content(tv: TemplateVals) = {
      import tv._

      val params = synTypes map { tpe => s"param$tpe: (String, Member[AvroSchema, $tpe, Z])"} mkString ", "
      val applies = synTypes map { tpe => s"new Field(param$tpe._1, param$tpe._2.typeClass.generateSchema, null, null)"} mkString ", "
      block"""
        |package formulation
        |
        |import org.apache.avro.Schema
        |import org.apache.avro.Schema.Field
        |
        |import cats._
        |import scala.collection.JavaConverters._
        |
        |trait AvroSchemaRecordN { self: AvroAlgebra[AvroSchema] =>
        -  def record$arity[${`A..N`}, Z](namespace: String, name: String)(f: (${`A..N`}) => Z)($params): AvroSchema[Z] = AvroSchema.create(Schema.createRecord(name, "", namespace, false, List($applies).asJava))
        |}
        |
      """
    }
  }

  object GenAvroDecoder extends Template {
    def filename(root: File) = root /  "formulation" / "AvroDecoderRecordN.scala"

    def content(tv: TemplateVals) = {
      import tv._

      val params = synTypes map { tpe => s"param$tpe: (String, Member[AvroDecoder, $tpe, Z])"} mkString ", "
      val applies = synTypes map { tpe => s"${tpe.toLowerCase} <- param$tpe._2.typeClass.decode(r.get(param$tpe._1))"} mkString "; "

      block"""
        |package formulation
        |
        |import org.apache.avro.generic.GenericRecord
        |
        |trait AvroDecoderRecordN { self: AvroAlgebra[AvroDecoder] =>
        |  private def record[A](f: GenericRecord => Either[Throwable, A]) = AvroDecoder.partial { case record: GenericRecord => f(record) }
        |
        -  def record$arity[${`A..N`}, Z](namespace: String, name: String)(f: (${`A..N`}) => Z)($params): AvroDecoder[Z] = record { r => for { $applies } yield f(${`a..n`}) }
        |}
        |
      """
    }
  }

  object GenAvroEncoder extends Template {
    def filename(root: File) = root /  "formulation" / "AvroEncoderRecordN.scala"

    def content(tv: TemplateVals) = {
      import tv._

      val params = synTypes map { tpe => s"param$tpe: (String, Member[AvroEncoder, $tpe, Z])"} mkString ", "
      val applies = synTypes map { tpe => s"r.put(param$tpe._1, param$tpe._2.typeClass.encode(s.getField(param$tpe._1).schema(), param$tpe._2.getter(v)))"} mkString "; "

      block"""
        |package formulation
        |
        |import org.apache.avro.generic.GenericData
        |
        |trait AvroEncoderRecordN { self: AvroAlgebra[AvroEncoder] =>
        -  def record$arity[${`A..N`}, Z](namespace: String, name: String)(f: (${`A..N`}) => Z)($params): AvroEncoder[Z] = AvroEncoder.create { case (s, v) => val r = new GenericData.Record(s); $applies; r }
        |}
        |
      """
    }
  }
}