import akka.NotUsed
import akka.stream.{FlowShape, Graph, OverflowStrategy}
import akka.stream.scaladsl.{Balance, FileIO, Flow, GraphDSL, Merge}
import akka.util.ByteString
import Taak1.resourcesFolder

import java.nio.file.StandardOpenOption.*
import java.nio.file.Paths
import scala.collection.mutable.Map as MutMap


object Question1:
  val flowGraph: Graph[FlowShape[Match, ByteString], NotUsed] =
    Flow.fromGraph(
      GraphDSL.create() {
        implicit builder =>
          import GraphDSL.Implicits._

          val balance = builder.add(Balance[Match](2))
          val merge = builder.add(Merge[MultiCounter](2))
          val flowOut = builder.add(Flow[ByteString])

          val groupAndCount: Flow[Match, MultiCounter, NotUsed] = Flow[Match]
            .groupBy(1000, (m: Match) => m.win_team.name)
            .map((m: Match) =>
              if m.day == "Sunday"
              then SingleCounter(m.win_team.name, 1)
              else SingleCounter(m.win_team.name, 0))
            .reduce(_ + _)
            .mergeSubstreams
            .map((c: SingleCounter) => MultiCounter(c.name, c.counter, Question1.printFunc))

          val counterReducer = Flow[MultiCounter].reduce(_ + _)

          // A second reducer to merge the results of the two pipelines together
          val counterReducer2 = Flow[MultiCounter].reduce(_ + _)

          val toByteString = Flow[MultiCounter].map(w => ByteString(w.toString))
          val buffer = Flow[Match].buffer(20, OverflowStrategy.backpressure)

//          val printer = Flow[MultiCounter].map(m => {println(m); m})

          balance ~>  buffer ~> groupAndCount.async ~> counterReducer.async ~> merge ~> counterReducer2 ~> toByteString ~> flowOut
          balance ~>  buffer ~> groupAndCount.async ~> counterReducer.async ~> merge

          FlowShape(balance.in, flowOut.out)})

  val sink = FileIO.toPath(Paths.get(s"$resourcesFolder/results/Q1.txt"), Set(CREATE, WRITE, TRUNCATE_EXISTING))

  def printFunc(map: MutMap[String, Int]): String =
    var str: String = ""
    map.foreach((team, win_count) => str += s"Name: $team --> Won Games on Sundays: $win_count\n")
    str

