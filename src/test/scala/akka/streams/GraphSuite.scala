package akka.streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, Zip, ZipWith}
import akka.util.Timeout
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class GraphSuite extends FunSuite {

  implicit val system = ActorSystem("Prueba")
  implicit val materializer = ActorMaterializer()
  implicit val timeout = Timeout(5.seconds)

  test("Ejemplo con broadcast") {

    val sink = Sink.head[Int]
    val flow = Flow[Int].map(_ * 2)

    val a: RunnableGraph[Future[Int]] =
      RunnableGraph.fromGraph(GraphDSL.create(sink) {
        implicit builder =>
          sinkBuilder =>
            import GraphDSL.Implicits._

            val broadcast = builder.add(Broadcast[Int](1))
            Source.single(1) ~> broadcast.in
            broadcast.out(0) ~> flow ~> sinkBuilder.in
            ClosedShape
      })

    a.run().foreach(x => assert(x == 2))
  }

  test("Ejemplo con ZipWith") {

    val sink = Sink.head[Int]
    val flow = Flow[Int].map(_ * 2)

    val a: RunnableGraph[Future[Int]] =
      RunnableGraph.fromGraph(GraphDSL.create(sink) {
        implicit builder =>
          sinkBuilder =>
            import GraphDSL.Implicits._

            val zipWith = builder.add(ZipWith[Int, Int, Int](math.max))
            Source.single(1) ~> zipWith.in0
            Source.single(5) ~> zipWith.in1
            zipWith.out ~> flow ~> sinkBuilder.in
            ClosedShape
      })

    a.run().foreach(x => assert(x == 10))
  }

  test("Ejemplo con merge") {

    val sink: Sink[Int, Future[Int]] = Sink.fold(0)(_ + _)
    val flow = Flow[Int].map(_ * 1)

    val a: RunnableGraph[Future[Int]] =
      RunnableGraph.fromGraph(GraphDSL.create(sink) {
        implicit builder =>
          sinkBuilder =>
            import GraphDSL.Implicits._

            val in = Source(1 to 10)

            val bcast = builder.add(Broadcast[Int](2))
            val merge = builder.add(Merge[Int](2))


            in ~> bcast ~> flow ~> merge ~> flow ~> sinkBuilder
            bcast ~> flow ~> merge


            ClosedShape
      })

    a.run().foreach(x => assert(x == 110))
  }

  test("Ejemplo con partial Graphs") {

    val pickMaxOfThree: Graph[UniformFanInShape[Int, Int], NotUsed] = GraphDSL.create() {
      implicit b =>
        import GraphDSL.Implicits._

        val zip1 = b.add(ZipWith[Int, Int, Int](math.max))
        val zip2 = b.add(ZipWith[Int, Int, Int](math.max))
        zip1.out ~> zip2.in0

        UniformFanInShape(zip2.out, zip1.in0, zip1.in1, zip2.in1)
    }

    val resultSink = Sink.head[Int]

    val g = RunnableGraph.fromGraph(GraphDSL.create(resultSink) {
      implicit b =>
        sink =>
          import GraphDSL.Implicits._

          // importing the partial graph will return its shape (inlets & outlets)
          val pm3: UniformFanInShape[Int, Int] = b.add(pickMaxOfThree)

          Source.single(1) ~> pm3.in(0)
          Source.single(3) ~> pm3.in(1)
          Source.single(2) ~> pm3.in(2)
          pm3.out ~> sink.in
          ClosedShape
    })

    val max: Future[Int] = g.run()

    assert(Await.result(max, 300.millis) == 3)
  }

  test("Ejemplo Source con partial Graphs") {
    val parcial = Source.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      // prepare graph elements
      val zip = b.add(Zip[Int, Int]())

      val datos = Source.fromIterator(() => Iterator.from(1))

      // connect the graph
      datos.filter(_ % 2 != 0) ~> zip.in0
      datos.filter(_ % 2 == 0) ~> zip.in1

      // expose port
      SourceShape(zip.out)
    })

    parcial.runWith(Sink.head).foreach(x => assert(x == (1, 2)))
  }


  test("Combianando Source y Sinks") {
    val source1 = Source(1 :: Nil)
    val source2 = Source(2 :: Nil)

    val merge: Source[Int, NotUsed] = Source.combine(source1, source2)(Merge(_))
    val result = merge.runWith(Sink.fold(0)(_ + _))

    assert(Await.result(result, 1 seconds) == 3)


  }

}
