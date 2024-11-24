import java.nio.file.{Path, Paths}
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{FlowShape, Graph, IOResult, OverflowStrategy}
import akka.stream.scaladsl.{Balance, FileIO, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.util.ByteString

import java.nio.file.StandardOpenOption.*
import scala.collection.mutable
import scala.concurrent.{ExecutionContextExecutor, Future}

import scala.collection.mutable.Map as MutMap
//import WinCounter.*


object Taak1 extends App:

  implicit val actorSystem: ActorSystem = ActorSystem("Taak1")
  implicit val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher

  val resourcesFolder: String = "src/main/resources"
  val pathCSVFile: Path = Paths.get(s"$resourcesFolder/basketball.csv")
  val source: Source[ByteString, Future[IOResult]] = FileIO.fromPath(pathCSVFile)

  val runnableGraph: RunnableGraph[Future[IOResult]] =
    source
      .via(FileDataToMatch.byteStringToMatchFlow)
      .via(Question1.flowBalanced)
      .to(Question1.sink)

  runnableGraph.run().onComplete(_ =>
// Without this Thread.sleep the stream will sometimes prematurely terminate, which will result in no output from the sink.
// This has (maybe?) something to do with the termination before the flushing of the results when using fold/reduce (which block the flow of data until it is finished computing).
    Thread.sleep(1000)
    actorSystem.terminate()
  )
