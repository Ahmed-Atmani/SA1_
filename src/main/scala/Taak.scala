import java.nio.file.{Path, Paths}
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{FlowShape, Graph, IOResult, OverflowStrategy}
import akka.stream.scaladsl.{Balance, FileIO, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.util.ByteString

import java.nio.file.StandardOpenOption.*
import scala.concurrent.{ExecutionContextExecutor, Future}

class Match(val id: String) extends Serializable

object Taak1 extends App:

  implicit val actorSystem: ActorSystem = ActorSystem("Taak1")
  implicit val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher

  val resourcesFolder: String = "src/main/resources"
  val pathCSVFile: Path = Paths.get(s"$resourcesFolder/basketball.csv")

  val source: Source[ByteString, Future[IOResult]] = FileIO.fromPath(pathCSVFile)

  val csvParsing: Flow[ByteString, List[ByteString], NotUsed] = CsvParsing.lineScanner()

  val mappingHeader: Flow[List[ByteString], Map[String, ByteString], NotUsed] = CsvToMap.toMap()

  val flowMatch: Flow[Map[String, ByteString], ByteString, NotUsed] = Flow[Map[String, ByteString]]
    .map(tempMap => {
      tempMap.map(element => {
        println(element._1)
        println(element._2)
        println("---")
        (element._1, element._2.utf8String)
      })
    }).map(record => {
      Match(
        id = record("lose_seed")//,
//        matchable_type = record("matchable_type"),
//        start_date = record("started_at"),
//        end_date = record("ended_at"),
//        start_station = Station(record("start_address_number").toInt, record("start_address")),
//        end_station = Station(record("end_address_number").toInt, record("end_address")),
//        start_location = Location(record("start_lat").toDouble, record("start_lng").toDouble),
//        end_location = Location(record("end_lat").toDouble, record("end_lng").toDouble))
      )}).map(matchObj => {
    ByteString(matchObj.id)
  })

//  val flowOut: Flow[Match, ByteString, NotUsed] = Flow[Match].map(match => {
//    ByteString(s"${match.id},${match.start_station.id},${match.end_station.id}\n")
//  })
//
//  val flowSelectedStations: Graph[FlowShape[Match, ByteString], NotUsed] =
//    Flow.fromGraph(
//      GraphDSL.create() {
//        implicit builder =>
//          import GraphDSL.Implicits._
//
//          val balance = builder.add(Balance[Match](4))
//          val merge = builder.add(Merge[Match](4))
//          val toFlow = builder.add(flowOut)
//
//          val station96Buf = Flow[Match].buffer(10000, OverflowStrategy.fail)
//          val station239Buf = Flow[Match].buffer(100, OverflowStrategy.backpressure)
//          val station234Buf = Flow[Match].buffer(10, OverflowStrategy.dropHead)
//          val station110Buf = Flow[Match].buffer(50, OverflowStrategy.dropTail)
//
//          val station96: Flow[Match, Match, NotUsed] = Flow[Match].filter(match => match.start_station.id == 96 || match.end_station.id == 96)
//          val station239: Flow[Match, Match, NotUsed] = Flow[Match].filter(match => match.start_station.id == 239 || match.end_station.id == 239)
//          val station234: Flow[Match, Match, NotUsed] = Flow[Match].filter(match => match.start_station.id == 234 || match.end_station.id == 234)
//          val station110: Flow[Match, Match, NotUsed] = Flow[Match].filter(match => match.start_station.id == 110 || match.end_station.id == 110)
//
//          balance ~> station96Buf ~> station96 ~> merge
//          balance ~> station239Buf ~> station239 ~> merge
//          balance ~> station234Buf ~> station234 ~> merge
//          balance ~> station110Buf ~> station110 ~> merge ~> toFlow
//
//          FlowShape(balance.in, toFlow.out)
//      }
//    )

  val sink = FileIO.toPath(Paths.get(s"$resourcesFolder/results/taak2_results.txt"), Set(CREATE, WRITE, APPEND))

  //  val sink2 = Sink.foreach(println)

  val runnableGraph: RunnableGraph[Future[IOResult]] =
    source
      .via(csvParsing)
      .via(mappingHeader)
      .via(flowMatch)
//      .via(flowSelectedStations)
      .to(sink)

  runnableGraph.run().onComplete(_ => actorSystem.terminate())
