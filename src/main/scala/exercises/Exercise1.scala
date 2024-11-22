package exercises

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.{ClosedShape, FlowShape}
import akka.stream.scaladsl.{Balance, Broadcast, Flow, GraphDSL, Merge, Sink, Source}

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Random, Success}

object Exercise1 extends App:

  implicit val actorSystem: ActorSystem = ActorSystem("Exercise1")
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  case class Wash(id: Int)
  case class Dry(id: Int)
  case class Finished(id: Int)

  val tasks: Seq[Wash] = (1 to 20).map(Wash.apply)

  def washStage =
    Flow[Wash].map((x: Wash) =>
      Thread.sleep(Random.nextInt(1000))
      Dry(x.id))

  def dryStage =
    Flow[Dry].map((x: Dry) =>
      Thread.sleep(Random.nextInt(1000))
      Finished(x.id))

  val parallelStage: Flow[Wash, Finished, NotUsed] = Flow.fromGraph(GraphDSL.create() {
    implicit builder =>
    import GraphDSL.Implicits._

//    val in = Source(tasks)
//    val out = Sink.foreach(println)

    val balance = builder.add(Balance[Wash](2))
    val merge = builder.add(Merge[Finished](2))

    val washer1, washer2 = washStage
    val dryer1, dryer2 = dryStage

    balance ~> washer1.async ~> dryer1.async ~> merge
    balance ~> washer2.async ~> dryer2.async ~> merge

      FlowShape(balance.in, merge.out)
  })

  val runnableGraph = Source(tasks)
    .via(parallelStage)
    .watchTermination()(
      (_, future) =>
        future.onComplete {
          case Success(value) =>
            println(s"Stream completed successfully $value")
          case Failure(error) =>
            println(s"Stream completed with failure: $error")
        })

  runnableGraph
    .runForeach(println)
    .onComplete(_ => actorSystem.terminate())
