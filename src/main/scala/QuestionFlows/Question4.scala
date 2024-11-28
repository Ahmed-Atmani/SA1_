package QuestionFlows

import Common.{Constants, Match, MultiCounter, SingleCounter}
import akka.NotUsed
import akka.stream.scaladsl.{Balance, FileIO, Flow, GraphDSL, Merge}
import akka.stream.{FlowShape, Graph, OverflowStrategy}
import akka.util.ByteString

import java.nio.file.Paths
import java.nio.file.StandardOpenOption.*
import scala.collection.mutable.Map as MutMap


object Question4:
  val flowGraph: Graph[FlowShape[Match, ByteString], NotUsed] =
    Flow.fromGraph(
      GraphDSL.create() {
        implicit builder =>
          import GraphDSL.Implicits.*

          val balance = builder.add(Balance[Match](Constants.balanceSize))
          val merge = builder.add(Merge[SingleCounter](Constants.balanceSize))
          val buffer = Flow[Match].buffer(Constants.bufferSize, OverflowStrategy.backpressure)
          val flowOut = builder.add(Flow[ByteString])

          val filterYear = builder.add(Flow[Match].filter((m: Match) =>
            1980 <= m.season && m.season <= 1990))

          val groupAndCount: Flow[Match, SingleCounter, NotUsed] = Flow[Match]
            .groupBy(Constants.maxSubStreams, (m: Match) => m.lose_team.name)
            .map((m: Match) => SingleCounter(m.lose_team.name))
            .reduce(_ + _)
            .mergeSubstreams

          val toMultiCounter = Flow[SingleCounter].map((s: SingleCounter) =>
            MultiCounter(s.name, s.counter, printFunc))
          val multiCounterReducer = Flow[MultiCounter].reduce(_ + _)
          val toByteString = Flow[MultiCounter].map(w => ByteString(w.toString))

          // First filter out matches not on Sunday, so that the two pipelines gets a more equal amount of work after filtering
          filterYear ~> balance ~> buffer ~> groupAndCount.async ~> merge ~> toMultiCounter ~> multiCounterReducer ~> toByteString ~> flowOut
                        balance ~> buffer ~> groupAndCount.async ~> merge

          FlowShape(filterYear.in, flowOut.out)})

  val sink = FileIO.toPath(Paths.get(Constants.pathQ4), Set(CREATE, WRITE, TRUNCATE_EXISTING))

  def printFunc(map: MutMap[String, Int]): String =
    var str: String = ""
    map.foreach((team, count) => str += s"Name: $team --> Lost games beteween 1980 and 1990: $count\n")
    str
