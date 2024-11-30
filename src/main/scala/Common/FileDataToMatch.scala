package Common

import akka.NotUsed
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl.{Flow, GraphDSL}
import akka.stream.{FlowShape, Graph, OverflowStrategy}
import akka.util.ByteString


/**
 * Object that contains `byteStringToMatchFlow`, which is a FlowGraph that converts file data to Match objects.
 * This is based on the code from WPO6
 */
object FileDataToMatch:

  val byteStringToMatchFlow: Graph[FlowShape[ByteString, Match], NotUsed] =
      Flow.fromGraph(
        GraphDSL.create() {
          implicit builder =>
            import GraphDSL.Implicits.*

            val fileDataToLines = builder.add(CsvParsing.lineScanner())
            val linesToMaps: Flow[List[ByteString], Map[String, ByteString], NotUsed] = CsvToMap.toMap()
            val mapsToMatch: Flow[Map[String, ByteString], Match, NotUsed] = Flow[Map[String, ByteString]]
              .map(tempMap => {
                tempMap.map(element => {
                  (element._1, element._2.utf8String)
                })
              }).map(record => {
                Match(
                  season = record("season").toInt,
                  round = record("round").toInt,
                  days_from_epoch = record("days_from_epoch").toInt,
                  game_date = record("game_date"),
                  day = record("day"),
                  win_team = Team(seed = record("win_seed").toInt,
                    region = record("win_region"),
                    market = record("win_market"),
                    name = record("win_name"),
                    alias = record("win_alias"),
                    team_id = record("win_team_id"),
                    school_ncaa = record("win_school_ncaa"),
                    code_ncaa = record("win_code_ncaa").toInt,
                    kaggle_team_id = record("win_kaggle_team_id").toInt),
                  win_pts = record("win_pts").toInt,
                  lose_team = Team( seed = record("lose_seed").toInt,
                    region = record("lose_region"),
                    market = record("lose_market"),
                    name = record("lose_name"),
                    alias = record("lose_alias"),
                    team_id = record("lose_team_id"),
                    school_ncaa = record("lose_school_ncaa"),
                    code_ncaa = record("lose_code_ncaa").toInt,
                    kaggle_team_id = record("lose_kaggle_team_id").toInt),
                  lose_pts = record("lose_pts").toInt,
                  num_ot = record("num_ot").toInt,
                  academic_year = record("academic_year").toInt,
                )
              })

            val flowOut = builder.add(Flow[Match])

            fileDataToLines ~> linesToMaps ~> mapsToMatch ~> flowOut
            
            FlowShape(fileDataToLines.in, flowOut.out)})