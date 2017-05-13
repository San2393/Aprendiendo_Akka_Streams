package akka.rabbitmq

import akka.NotUsed
import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout
import com.spingo.op_rabbit.Directives._
import com.spingo.op_rabbit.Message.{Ack, ConfirmResponse}
import com.spingo.op_rabbit.PlayJsonSupport._
import com.spingo.op_rabbit._
import com.spingo.op_rabbit.stream.{MessagePublisherSink, RabbitSource}
import com.timcharper.acked.AckedSource
import org.scalatest.FunSuite
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

case class Data(id: Int)


class OpRabbitSuite extends FunSuite {


  implicit val actorSystem = ActorSystem("SystemTest")
  implicit val materializer = ActorMaterializer()
  implicit val timeout = Timeout(5 seconds)
  implicit val recoveryStrategy = RecoveryStrategy.nack(false)

  test("Primer ejemplo op-rabbitMQ") {
    val rabbitControl = actorSystem.actorOf(Props(new RabbitControl))
    implicit val dataFormat = Json.format[Data]


    val demoQueue = Queue("Ejemplo", durable = false, autoDelete = true)


    RabbitSource(rabbitControl,
      channel(qos = 3),
      consume(demoQueue),
      body(as[Data]).map {
        x =>
          assert(x == Data(1))
          ack
      }).runAck

    val publisher = Publisher.queue(demoQueue)

    val sourceDatos = Source.single(1)
    val flowData: Flow[Int, Data, NotUsed] = Flow[Int].map(x => Data(x))
    val flowMsg: Flow[Data, Message, NotUsed] = Flow[Data].map(x => Message(x, publisher))
    sourceDatos.via(flowData).via(flowMsg).runWith(Sink.foreach(x => rabbitControl ! x))


  }


  test("Prueba recibiendo notificación cuando envias un mensaje ") {

    val demoQueue = Queue("Prueba", durable = false, autoDelete = true)
    val rabbitControl = actorSystem.actorOf(Props(new RabbitControl))
    implicit val dataFormat = Json.format[Data]


    val a: Seq[Future[ConfirmResponse]] = (1 to 1).map(x =>
      (rabbitControl ? Message.queue(
        Data(x),
        queue = demoQueue.queueName)
        ).mapTo[ConfirmResponse])

    a.foreach(x => x.foreach(y => assert(y == Ack(0) || y == Ack(1))))

  }

  test("Otra forma de publicar un mensaje") {
    val rabbitControl = actorSystem.actorOf(Props(new RabbitControl))
    implicit val dataFormat = Json.format[Data]

    val demoQueue = Queue("TestQueue2")


    AckedSource(1 to 1).
      map(d => Message.queue(Data(d), demoQueue.queueName)).
      to(MessagePublisherSink(rabbitControl))
      .run


  }


  test("Consumir en RabbitSource") {
    val rabbitControl = actorSystem.actorOf(Props(new RabbitControl))
    implicit val dataFormat = Json.format[Data]

    val demoQueue = Queue("TestQueue2")
    import Directives._

    RabbitSource(rabbitControl,
      channel(qos = 3),
      consume(demoQueue),
      body(as[Data]).map {
        x =>
          assert(x == Data(1))
          ack
      }).runAck
  }

  test("Usando Exchange amq.topic") {
    val rabbitControl = actorSystem.actorOf(Props(new RabbitControl))
    implicit val dataFormat = Json.format[Data]

    import Directives._

    RabbitSource(rabbitControl,
      channel(qos = 3),
      consume(topic(queue("Pruebas",
        durable = true,
        autoDelete = true),
        List("some-topic.#"))),
      body(as[Data])
    ).runAck

  }

}
