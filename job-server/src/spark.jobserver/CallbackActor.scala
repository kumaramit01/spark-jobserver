package spark.jobserver

import akka.actor.{ActorSystem, Props}
import akka.util.Timeout
import ooyala.common.akka.InstrumentedActor
import spark.jobserver.CommonMessages.JobErroredOut
import spark.jobserver.CommonMessages.JobFinished
import spark.jobserver.CommonMessages.JobStarted
import spark.jobserver.CommonMessages.JobValidationFailed
import spark.jobserver.io.{JobInfo, JobDAO}
import spray.client.pipelining._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import spray.http.{HttpRequest, Uri, HttpResponse}


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

    case JobStarted(jobId, context, startTime) =>{

      val jobInfoOpt = jobDao.getJobInfos.get(jobId)
      jobInfoOpt match{
        case Some(jobInfo) if(jobInfo.callbackUrl != None)=>{
          callback(jobInfo,"STARTED")
        }
        case _ =>
      }
    }

    case JobFinished(jobId, startTime) =>{
      println("job finished: "+ jobId)
      val jobInfoOpt = jobDao.getJobInfos.get(jobId)
      jobInfoOpt match{
        case Some(jobInfo) if(jobInfo.callbackUrl != None)=>{
          callback(jobInfo,"FINISHED")
        }
        case _ =>
      }

    }

    case JobValidationFailed(jobId, endTime, err) =>{
      println("job validation: "+ jobId)
      val jobInfoOpt = jobDao.getJobInfos.get(jobId)
      jobInfoOpt match{
        case Some(jobInfo) if(jobInfo.callbackUrl != None)=>{
          callback(jobInfo,"INVALID")
        }
        case _ =>
      }

    }

    case JobErroredOut(jobId, endTime, err) => {
      println("job erred out: "+ jobId)
      val jobInfoOpt = jobDao.getJobInfos.get(jobId)
      jobInfoOpt match{
        case Some(jobInfo) if(jobInfo.callbackUrl != None)=>{
          callback(jobInfo,"ERROR")
        }
        case _ =>
      }

    }

  }


  def callback(info: JobInfo, status: String)={
    val jobId = info.jobId
    require(info.callbackUrl != None)
    val callbackUrl = info.callbackUrl.get
    val uri = Uri(callbackUrl + "?jobId=" + jobId + "&status=" + status)
    val responseFuture: Future[HttpResponse] = pipeline(Get(uri))
    responseFuture onComplete {
      case Success(httpResponse) =>
        logger.info("Posted to the " + uri + " status: " + httpResponse.status)
        logger.info("content: " + httpResponse.entity.asString)
      case Failure(error) =>
        val msg = s"Failed to call the callback uri  $callbackUrl for $jobId with status $status"
        logger.error( msg )
    }
  }


}