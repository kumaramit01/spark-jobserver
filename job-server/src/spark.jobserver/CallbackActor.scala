package spark.jobserver

import akka.actor.{ActorSystem, Props}
import akka.util.Timeout
import ooyala.common.akka.InstrumentedActor
import spark.jobserver.CommonMessages._
import spark.jobserver.CommonMessages.JobFinished
import spark.jobserver.CommonMessages.JobStarted
import spark.jobserver.CommonMessages.JobValidationFailed
import spark.jobserver.io.JobDAO
import spray.client.pipelining._
import scala.concurrent.Future
import scala.concurrent.duration._

import spray.http.HttpRequest
import spray.http.HttpResponse


object CallbackActor {

  // Akka 2.2.x style actor props for actor creation
  def props(): Props = Props(classOf[CallbackActor])
}

class CallbackActor(jobDao: JobDAO) extends InstrumentedActor {

  implicit val system: ActorSystem = ActorSystem()
  implicit val timeout: Timeout = Timeout(3.seconds)
  import system.dispatcher // implicit execution context
  val pipeline: HttpRequest => Future[HttpResponse] = sendReceive



  def wrappedReceive: Receive = {

    case JobStarted(jobId, context, startTime) =>
      println("job started: "+ jobId)

    case JobFinished(jobId, startTime) =>
      println("job finished: "+ jobId)

    case JobValidationFailed(jobId, endTime, err) =>
      println("job validation: "+ jobId)

    case JobErroredOut(jobId, endTime, err) =>
      println("job erred out: "+ jobId)

  }

}