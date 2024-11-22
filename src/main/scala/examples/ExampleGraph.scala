package examples

import akka.actor.ActorSystem
import akka.stream.ClosedShape
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source}

object ExampleGraph extends App {

  implicit val actorSystem: ActorSystem = ActorSystem("Graph")

  val graph = RunnableGraph.fromGraph(GraphDSL.create() {
    implicit builder =>
    import GraphDSL.Implicits._
    
    val in = Source(1 to 10)
    val out = Sink.foreach(println)

    val bcast = builder.add(Broadcast[Int](2))
    val merge = builder.add(Merge[Int](2))

    val f1, f2, f3, f4 = Flow[Int].map(_ + 10)

    in ~> f1 ~> bcast ~> f2 ~> merge ~> f3 ~> out
                bcast ~> f4 ~> merge
    ClosedShape
  })

  graph.run()

}
