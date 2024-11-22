package solutions

import java.nio.file.{Path, Paths}
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{FlowShape, Graph, IOResult, OverflowStrategy}
import akka.stream.scaladsl.{Balance, FileIO, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.util.ByteString

import java.nio.file.StandardOpenOption.*
import scala.concurrent.{ExecutionContextExecutor, Future}

case class Station(id: Int, name: String)

case class Location(latitude: Double, longitude: Double)

case class Ride(id: String,
                rideable_type: String,
                start_date: String,
                end_date: String,
                start_station: Station,
                end_station: Station,
                start_location: Location,
                end_location: Location)

object Exercise2 extends App:

  implicit val actorSystem: ActorSystem = ActorSystem("Exercise2")
  implicit val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher

  val resourcesFolder: String = "src/main/resources"
  val pathCSVFile: Path = Paths.get(s"$resourcesFolder/Divvy_Trips_2020_Q1.csv")

  val source: Source[ByteString, Future[IOResult]] = FileIO.fromPath(pathCSVFile)

  val csvParsing: Flow[ByteString, List[ByteString], NotUsed] = CsvParsing.lineScanner()

  val mappingHeader: Flow[List[ByteString], Map[String, ByteString], NotUsed] = CsvToMap.toMap()

  val flowRide: Flow[Map[String, ByteString], Ride, NotUsed] = Flow[Map[String, ByteString]]
    .map(tempMap => {
    tempMap.map(element => {
      println(element._1)
      println(element._2)
      println("---")

      (element._1, element._2.utf8String)
    })
  }).map(record => {
    Ride(
      id = record("ride_id"),
      rideable_type = record("rideable_type"),
      start_date = record("started_at"),
      end_date = record("ended_at"),
      start_station = Station(record("start_address_number").toInt, record("start_address")),
      end_station = Station(record("end_address_number").toInt, record("end_address")),
      start_location = Location(record("start_lat").toDouble, record("start_lng").toDouble),
      end_location = Location(record("end_lat").toDouble, record("end_lng").toDouble))
  })

  val flowOut: Flow[Ride, ByteString, NotUsed] = Flow[Ride].map(ride => {
    ByteString(s"${ride.id},${ride.start_station.id},${ride.end_station.id}\n")
  })

  val flowSelectedStations: Graph[FlowShape[Ride, ByteString], NotUsed] =
    Flow.fromGraph(
      GraphDSL.create() {
        implicit builder =>
        import GraphDSL.Implicits._

        val balance = builder.add(Balance[Ride](4))
        val merge = builder.add(Merge[Ride](4))
        val toFlow = builder.add(flowOut)

        val station96Buf = Flow[Ride].buffer(10000, OverflowStrategy.fail)
        val station239Buf = Flow[Ride].buffer(100, OverflowStrategy.backpressure)
        val station234Buf = Flow[Ride].buffer(10, OverflowStrategy.dropHead)
        val station110Buf = Flow[Ride].buffer(50, OverflowStrategy.dropTail)

        val station96: Flow[Ride, Ride, NotUsed] = Flow[Ride].filter(ride => ride.start_station.id == 96 || ride.end_station.id == 96)
        val station239: Flow[Ride, Ride, NotUsed] = Flow[Ride].filter(ride => ride.start_station.id == 239 || ride.end_station.id == 239)
        val station234: Flow[Ride, Ride, NotUsed] = Flow[Ride].filter(ride => ride.start_station.id == 234 || ride.end_station.id == 234)
        val station110: Flow[Ride, Ride, NotUsed] = Flow[Ride].filter(ride => ride.start_station.id == 110 || ride.end_station.id == 110)

        balance ~> station96Buf ~> station96 ~> merge
        balance ~> station239Buf ~> station239 ~> merge
        balance ~> station234Buf ~> station234 ~> merge
        balance ~> station110Buf ~> station110 ~> merge ~> toFlow

        FlowShape(balance.in, toFlow.out)
    }
  )

  val sink = FileIO.toPath(Paths.get(s"$resourcesFolder/results/exercise2.txt"), Set(CREATE, WRITE, APPEND))

//  val sink2 = Sink.foreach(println)

  val runnableGraph: RunnableGraph[Future[IOResult]] =
    source
      .via(csvParsing)
      .via(mappingHeader)
      .via(flowRide)
      .via(flowSelectedStations)
      .to(sink)

  runnableGraph.run().onComplete(_ => actorSystem.terminate())
