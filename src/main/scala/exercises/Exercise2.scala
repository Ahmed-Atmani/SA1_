package exercises

import java.nio.file.{Path, Paths}
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{FlowShape, Graph, IOResult, OverflowStrategy}
import akka.stream.scaladsl.{Balance, FileIO, Flow, GraphDSL, Merge, RunnableGraph, Source}
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.util.ByteString

import java.io.File
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

  val csvParsing: Flow[ByteString, List[ByteString], NotUsed] = CsvParsing.lineScanner() // Parse the file with Alpakka

  val mappingHeader: Flow[List[ByteString], Map[String, ByteString], NotUsed] = CsvToMap.toMap() // Construct the mapping with Alpakka

  val flowRide: Flow[Map[String, ByteString], Ride, NotUsed] =  Flow[Map[String, ByteString]].map(tempMap => {
    tempMap.map(element => {
      (element._1, element._2.utf8String)}).map(record => {
      Ride(
        id = record("ride_id"),
        rideable_type = record("rideable_type"),
        start_date = record("started_at"),
        end_date = record("ended_at"),
        start_station = Location(record("start_address_number"), record("start_address")),
        end_station = Location(record("end_address_number"), record("end_address")),
        start_location = ???, end_location = ???
      )
    }
    )
  )// Objectify the values

  val flowSelectedStations: Type = _  // Implement a custom FlowShape for this Flow using the GraphDSL

  val sink: Sink[String] = Sink.foreach(println)

  val runnableGraph: Type = _

  runnableGraph.run().onComplete(_ => actorSystem.terminate())
