import akka.NotUsed
import akka.stream.{FlowShape, Graph, OverflowStrategy}
import akka.stream.scaladsl.{Balance, FileIO, Flow, GraphDSL, Merge}
import akka.util.ByteString
import Taak1.resourcesFolder

import java.nio.file.StandardOpenOption.*
import java.nio.file.Paths
import scala.collection.mutable.Map as MutMap


object Question1:
  val flow: Flow[Match, WinCounter, NotUsed] = Flow[Match]
    .map((m: Match) =>
      if m.day == "Sunday"
      then WinCounter(m.win_team.name, 1, Question1.printFunc)
      else WinCounter(m.win_team.name, 0, Question1.printFunc))
    .reduce(_ + _)

  val flowBalanced: Graph[FlowShape[Match, ByteString], NotUsed] =
    Flow.fromGraph(
      GraphDSL.create() {
        implicit builder =>
          import GraphDSL.Implicits._

          val balance = builder.add(Balance[Match](2))
          val merge = builder.add(Merge[WinCounter](2))
          val flowOut = builder.add(Flow[ByteString])

          val toCounterConverter = Flow[Match]
            .map((m: Match) =>
              if m.day == "Sunday"
              then WinCounter(m.win_team.name, 1, Question1.printFunc)
              else WinCounter(m.win_team.name, 0, Question1.printFunc))
          val counterReducer = Flow[WinCounter].reduce(_ + _)

          // A second reducer to merge the results of the two pipelines together
          val counterReducer2 = Flow[WinCounter].reduce(_ + _)

          val toByteString = Flow[WinCounter].map(w => ByteString(w.toString))
          val buffer = Flow[Match].buffer(20, OverflowStrategy.backpressure)

          balance ~> buffer ~> toCounterConverter.async ~> counterReducer.async ~> merge ~> counterReducer2 ~> toByteString ~> flowOut
          balance ~> buffer ~> toCounterConverter.async ~> counterReducer.async ~> merge

          FlowShape(balance.in, flowOut.out)})

  val sink = FileIO.toPath(Paths.get(s"$resourcesFolder/results/Q1.txt"), Set(CREATE, WRITE))

  def printFunc(map: MutMap[String, Int]): String =
    var str: String = ""
    map.foreach((team, win_count) => str += s"Name: $team --> Won Games on Sundays: $win_count\n")
    str

