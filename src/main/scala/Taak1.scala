import java.nio.file.{Path, Paths}
import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.{ClosedShape, FlowShape, Graph, IOResult, OverflowStrategy}
import akka.stream.scaladsl.{Balance, Broadcast, FileIO, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.util.ByteString

import java.nio.file.StandardOpenOption.*
import scala.collection.mutable
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.collection.mutable.Map as MutMap
import Question1.*
import Question2.*
import Question3.*
import Question4.*


object Taak1 extends App:

  implicit val actorSystem: ActorSystem = ActorSystem("Taak1")
  implicit val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher

  val resourcesFolder: String = "src/main/resources"
  val pathCSVFile: Path = Paths.get(s"$resourcesFolder/basketball.csv")
  val source: Source[ByteString, Future[IOResult]] = FileIO.fromPath(pathCSVFile)

  val runnableGraph: RunnableGraph[Future[IOResult]] =
    source
      .via(FileDataToMatch.byteStringToMatchFlow)
      .via(Question3.flow)
      .to(Question3.sink)

  val graph: RunnableGraph[NotUsed] = RunnableGraph.fromGraph(GraphDSL.create() {
    implicit builder =>
      import GraphDSL.Implicits._

      val source1 = builder.add(FileIO.fromPath(pathCSVFile))
      val broadcast = builder.add(Broadcast[Match](4))
      val byteStringToMatch = builder.add(FileDataToMatch.byteStringToMatchFlow)
      
      source1 ~> byteStringToMatch ~> broadcast ~> Question1.flowBalanced.async ~> Question1.sink.async
                                      broadcast ~> Question2.flowBalanced.async ~> Question2.sink.async
                                      broadcast ~> Question3.flowBalanced.async ~> Question3.sink.async
                                      broadcast ~> Question4.flowBalanced.async ~> Question4.sink.async
      ClosedShape
  })

  graph.run().onComplete(_ => // onComplete not exist since the graph has no Future, so there is no way to know when the stream is done
// Without this Thread.sleep the stream will sometimes prematurely terminate, which will result in no output from the sink.
// This has (maybe?) something to do with the termination before the flushing of the results when using fold/reduce (which block the flow of data until it is finished computing).
    Thread.sleep(1000)
    actorSystem.terminate()
  )



//  runnableGraph.run().onComplete(_ =>
//// Without this Thread.sleep the stream will sometimes prematurely terminate, which will result in no output from the sink.
//// This has (maybe?) something to do with the termination before the flushing of the results when using fold/reduce (which block the flow of data until it is finished computing).
//    Thread.sleep(1000)
//    actorSystem.terminate()
//  )
