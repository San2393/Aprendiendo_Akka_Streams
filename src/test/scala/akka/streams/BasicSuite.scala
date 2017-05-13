package akka.streams

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import akka.{Done, NotUsed}
import org.scalatest.FunSuite

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, TimeoutException}

class BasicSuite extends FunSuite {

  test("Probando Source") {
    implicit val system = ActorSystem("Prueba")
    implicit val materializer = ActorMaterializer()

    val source: Source[Int, NotUsed] = Source(List(3, 3, 4))

    source.filter(_ % 2 == 0).runForeach(x => assert(x == 4))

    source.fold(0)((acu, item) => acu + item).runForeach(x => assert(x == 10))

    source.scan(0)((acu, item) => acu + item).runForeach(x => assert(x == 10))

    source.zipWith(Source(1 to 2))((x, y) => assert(x != y))

  }

  test("Probando Source y Sink toMat") {
    implicit val system = ActorSystem("Prueba")
    implicit val materializer = ActorMaterializer()

    val source: Source[Int, NotUsed] = Source(Array(5, 8).toVector)

    val sink: Sink[Int, Future[Int]] = Sink.fold(0)(_ + _)

    val r1: RunnableGraph[Future[Int]] = source.toMat(sink)(Keep.right)

    assert(Await.result(r1.run, Duration.Inf) == 13)

  }


  test("Probando Source y Sink runWith") {
    implicit val system = ActorSystem("Prueba")
    implicit val materializer = ActorMaterializer()

    val source: Source[String, NotUsed] = Source("Hola" :: "Mundo" :: Nil)

    val sink: Sink[String, Future[String]] = Sink.fold("")(_ + " " + _)

    val r1 = source.runWith(sink)

    assert(Await.result(r1, Duration.Inf) == " Hola Mundo")

  }


  test("Probando Source + Flow = Source") {
    implicit val system = ActorSystem("Prueba")
    implicit val materializer = ActorMaterializer()

    val source: Source[Int, NotUsed] = Source(List(5))

    val flow: Flow[Int, Int, NotUsed] = Flow[Int].map(_ * 2)

    val r1: Source[Int, NotUsed] = source.via(flow)

    r1.runForeach(x => assert(x == 5))
  }

  test("Probando Sink + Flow = Sink") {
    implicit val system = ActorSystem("Prueba")
    implicit val materializer = ActorMaterializer()

    val sink: Sink[Any, Future[Done]] = Sink.foreach(x => assert(x == 2))

    val flow: Flow[Int, Int, NotUsed] = Flow[Int].map(_ * 2)

    val r1: Sink[Int, NotUsed] = flow.to(sink)

    Source(1 :: Nil).runWith(r1)
  }

  test("Probando Source, Sink y Flow") {
    implicit val system = ActorSystem("Prueba")
    implicit val materializer = ActorMaterializer()

    val source: Source[Int, NotUsed] = Source(1 :: 5 :: 8 :: Nil)

    val sink: Sink[Int, Future[Int]] = Sink.fold(0)(_ + _)

    val flow: Flow[Int, Int, NotUsed] = Flow[Int].map(_ * 2)

    val r1: Future[Int] = source.via(flow).runWith(sink)

    assert(Await.result(r1, Duration.Inf) == 28)

  }

  test("Se puede combinar Flow") {
    implicit val system = ActorSystem("Prueba")
    implicit val materializer = ActorMaterializer()

    val source: Source[Int, NotUsed] = Source(1 to 10)

    val sink: Sink[Int, Future[Int]] = Sink.fold(0)(_ - _)

    val flow: Flow[Int, Int, NotUsed] = Flow[Int].map(_ * 2)
    val flow2: Flow[Int, Int, NotUsed] = Flow[Int].map(_ * -1)

    val r1: Future[Int] = source.via(flow).via(flow2).runWith(sink)

    assert(Await.result(r1, Duration.Inf) == 110)

  }

  test("No solo se hace con numeros finitos") {
    implicit val system = ActorSystem("Prueba")
    implicit val materializer = ActorMaterializer()

    val source: Source[Int, NotUsed] = Source.repeat(3)

    val sink: Sink[Int, Future[Int]] = Sink.fold(0)(_ + _)

    val flow: Flow[Int, Int, NotUsed] = Flow[Int].map(_ * 2)

    val r1: Future[Int] = source.via(flow).runWith(sink)

    assertThrows[TimeoutException] {
      assert(Await.result(r1, 5 seconds) == 5)
    }

    //En caso de querer parar usamos take método podemos crear un punto de parada artificial que nos impide evaluar de forma indefinida
    val r2: Future[Int] = source.take(2).via(flow).runWith(sink)
    assert(Await.result(r2, Duration.Inf) == 12)


  }

  test("Enviar todos los valores que llegan a un Sink para un Actor") {
    implicit val system = ActorSystem("Prueba")
    implicit val materializer = ActorMaterializer()

    val actor = system.actorOf(Props(new Actor {
      override def receive: PartialFunction[Any, Unit] = {
        case m => assert(m == "Hola" || m == "Termine")
      }
    }))

    val sink: Sink[String, NotUsed] = Sink.actorRef(actor, "Termine")

    Source("Hola" :: Nil).runWith(sink)

  }


}
