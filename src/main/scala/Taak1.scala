import java.nio.file.{Path, Paths}
import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, RunnableGraph, Source}
import akka.util.ByteString
import scala.concurrent.{ExecutionContextExecutor, Future}


object Taak1 extends App:

  implicit val actorSystem: ActorSystem = ActorSystem("Taak1")
  implicit val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher

  val resourcesFolder: String = "src/main/resources"
  val pathCSVFile: Path = Paths.get(s"$resourcesFolder/basketball.csv")
  val source: Source[ByteString, Future[IOResult]] = FileIO.fromPath(pathCSVFile)

  val graphQ1: RunnableGraph[Future[IOResult]] =
    source
      .via(FileDataToMatch.byteStringToMatchFlow)
      .via(Question1.flowGraph)
      .to(Question1.sink)

  val graphQ2: RunnableGraph[Future[IOResult]] =
    source
      .via(FileDataToMatch.byteStringToMatchFlow)
      .via(Question2.flowGraph)
      .to(Question2.sink)

  val graphQ3: RunnableGraph[Future[IOResult]] =
    source
      .via(FileDataToMatch.byteStringToMatchFlow)
      .via(Question3.flowGraph)
      .to(Question3.sink)

  val graphQ4: RunnableGraph[Future[IOResult]] =
    source
      .via(FileDataToMatch.byteStringToMatchFlow)
      .via(Question4.flowGraph)
      .to(Question4.sink)

  graphQ1.run().onComplete(_ =>
    println("Finished Q1")
    graphQ2.run().onComplete(_ =>
      println("Finished Q2")
      graphQ3.run().onComplete(_ =>
        println("Finished Q3")
        graphQ4.run().onComplete(_ =>
          println("Finished Q4")
          // Without this Thread.sleep the stream will sometimes prematurely terminate, which will result in no output from the sink.
          // This has (maybe?) something to do with the termination before the flushing of the results when using fold/reduce (which block the flow of data until it is finished computing).
          Thread.sleep(1000)
          actorSystem.terminate()
        )
      )
    )
  )
