package akka.rabbitmq

import akka.actor.{ActorSystem, Props}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink}
import akka.stream.{ActorMaterializer, ClosedShape, FlowShape}
import akka.util.Timeout
import com.spingo.op_rabbit.Directives._
import com.spingo.op_rabbit.PlayJsonSupport._
import com.spingo.op_rabbit.stream.RabbitSource
import com.spingo.op_rabbit.{RecoveryStrategy, _}
import com.timcharper.acked.AckedSource
import org.scalatest.FunSuite
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._



class Prueba extends FunSuite {

  implicit val actorSystem = ActorSystem("SystemTest")
  implicit val materializer = ActorMaterializer()
  implicit val timeout = Timeout(5 seconds)
  implicit val recoveryStrategy = RecoveryStrategy.none

  test("Ejemplo practico") {
    val rabbitControl = actorSystem.actorOf(Props(new RabbitControl))
    val recibirTopic = "recibiendo"
    val envioQueue = queue("TestEnvio")
    val recibirQueue = topic(queue("TestRecibo"), List(recibirTopic))
    val resultSink: Sink[Datos, Future[Datos]] = Sink.head


    val flowTest =
      Flow.fromGraph(GraphDSL.create() { implicit b =>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val broadcast = b.add(Broadcast[Numero](1))
        val broadcast2 = b.add(Broadcast[Datos](1))

        broadcast.out(0).map(x => Datos(x)) ~> broadcast2.in

        FlowShape(broadcast.in, broadcast2.out(0))
      })

    implicit val dataFormat = Json.format[Numero]
    val datos: AckedSource[Numero, SubscriptionRef] =
      RabbitSource(
        rabbitControl,
        channel(qos = 1),
        consume(envioQueue),
        body(as[Numero]))

    val datosSource = datos


    datosSource.runForeach(x => println(s"Me enviaron de la consola: $x"))


    val datosEnviar = RunnableGraph.fromGraph(GraphDSL.create(resultSink) {
      implicit builder =>
        sinkBuilder =>
          import GraphDSL.Implicits._

          val broadcast = builder.add(Broadcast[Numero](1))
          datosSource.wrappedRepr.map(_._2) ~> broadcast.in
          broadcast.out(0) ~> flowTest ~> sinkBuilder.in
          ClosedShape
    })

    implicit val dataFormat2 = Json.format[Datos]

    datosEnviar.run foreach { x =>
      rabbitControl ! Message.topic(x, recibirTopic)
    }

    RabbitSource(rabbitControl,
      channel(qos = 1),
      consume(recibirQueue),
      body(as[Datos])).runForeach(x => println(s"Llegaron a la consola: $x"))

  }

}
