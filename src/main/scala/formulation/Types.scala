package formulation


trait ~>[F[_], G[_]] {
  def apply[A](fa: F[A]): G[A]
}

trait Functor[F[_]] {
  def map[A, B](fa: F[A])(f: A => B): F[B]
}

trait Traverse[F[_]] extends Functor[F] {
  def traverse[G[_]: Applicative, A, B](fa: F[A])(f: A => G[B]): G[F[B]]
}

object Traverse {
  implicit val listInstance: Traverse[List] = new Traverse[List] {
    def map[A,B](l: List[A])(f: A => B): List[B] = l map f
    def traverse[G[_], A, B](l: List[A])(f: A => G[B])(implicit G: Applicative[G]): G[List[B]] = {
      l.foldRight(G.pure(List.empty[B])) {
        (hd, init) => Applicative.map2(f(hd), init)(_ :: _)
      }
    }
  }
}

trait Applicative[F[_]] extends Functor[F] {
  def pure[A](a: A): F[A]
  def ap[A,B](fa: F[A])(f: F[A => B]): F[B]
}

object Applicative {
  private[formulation] def map2[F[_],A,B,C](fa: F[A], fb: F[B])(f: (A, B) => C)(implicit F: Applicative[F]): F[C] =
    F.ap(fb)(F.ap(fa)(F.pure(f.curried)))

  implicit def either[L]: Applicative[Either[L, ?]] = new Applicative[Either[L, ?]] {
    override def pure[A](a: A): Either[L, A] = Right(a)
    override def ap[A, B](fa: Either[L, A])(f: Either[L, A => B]): Either[L, B] = f.flatMap(ff => fa.map(ff))
    override def map[A, B](fa: Either[L, A])(f: A => B): Either[L, B] = fa.map(f)
  }

  implicit val attempt: Applicative[Attempt] = new Applicative[Attempt] {
    override def pure[A](a: A): Attempt[A] = Attempt.Success(a)
    override def ap[A, B](fa: Attempt[A])(f: Attempt[A => B]): Attempt[B] = f.flatMap(ff => fa.map(ff))
    override def map[A, B](fa: Attempt[A])(f: A => B): Attempt[B] = fa.map(f)
  }
}


sealed trait Attempt[+A] { self =>
  def flatMap[B](f: A => Attempt[B]): Attempt[B] = self match {
    case Attempt.Success(value) => f(value)
    case Attempt.Error(error) => Attempt.Error(error)
  }

  def map[B](f: A => B): Attempt[B] = self match {
    case Attempt.Success(value) => Attempt.Success(f(value))
    case Attempt.Error(error) => Attempt.Error(error)
  }
}

object Attempt {
  final case class Success[A](value: A) extends Attempt[A]
  final case class Error(error: Throwable) extends Attempt[Nothing]
}


final case class Member[F[_], A, B](typeClass: F[A], getter: B => A, defaultValue: Option[A] = None) {
  def mapTypeClass[G[_]](f: F ~> G): Member[G, A, B] = copy(typeClass = f(typeClass))
}