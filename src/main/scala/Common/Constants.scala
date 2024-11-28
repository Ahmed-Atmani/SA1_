package Common

import java.nio.file.{Path, Paths}

object Constants:
  val maxSubStreams: Int = 1000
  
  val bufferSize: Int = 20
  val balanceSize: Int = 2

  val resourcesFolder: String = "src/main/resources"
  val pathCSVFile: Path = Paths.get(s"$resourcesFolder/basketball.csv")
  val pathQ1: String = s"$resourcesFolder/results/Q1.txt"
  val pathQ2: String = s"$resourcesFolder/results/Q2.txt"
  val pathQ3: String = s"$resourcesFolder/results/Q3.txt"
  val pathQ4: String = s"$resourcesFolder/results/Q4.txt"

