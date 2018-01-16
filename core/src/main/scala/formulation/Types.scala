package formulation

import cats.{Invariant, ~>}
import org.apache.avro.Schema
import org.codehaus.jackson.JsonNode
import shapeless.ops.coproduct.Align
import shapeless.{Coproduct, Generic}

final case class Member[F[_], A, B] private (
    typeClass: F[A],
    getter: B => A,
    aliases: Seq[String] = Seq.empty,
    defaultValue: Option[JsonNode] = None,
    documentation: Option[String] = None
) {
  def mapTypeClass[G[_]](f: F ~> G): Member[G, A, B] = copy(typeClass = f(typeClass))
}


trait Transformer[F[_], I, O] {
  def apply(fi: F[I]): F[O]
}

object Transformer {
  implicit def alignedCoproduct[F[_], I <: Coproduct, Repr <: Coproduct, O](implicit
                                                                            I: Invariant[F],
                                                                            G: Generic.Aux[O, Repr],
                                                                            TA: Align[Repr, I],
                                                                            FA: Align[I, Repr]): Transformer[F, I, O] = new Transformer[F, I, O] {
    override def apply(fi: F[I]): F[O] =
      I.imap(fi)(a => G.from(FA(a)))(b => TA(G.to(b)))
  }
}

final case class RecordName(namespace: String, name: String)

final case class AvroEncodeResult(usedSchema: Schema, payload: Array[Byte])