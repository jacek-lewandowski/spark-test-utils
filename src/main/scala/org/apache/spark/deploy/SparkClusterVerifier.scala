package org.apache.spark.deploy

import scala.concurrent.duration._
import scala.concurrent.{Await, TimeoutException}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

import akka.actor.{ActorRef, ActorSelection, ActorSystem}
import akka.pattern._
import akka.util.Timeout
import org.apache.spark.Logging
import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, RequestWorkerState, WorkerStateResponse, _}

object SparkClusterVerifier extends Logging {

  def verify(
    protocol: String = "akka.tcp",
    host: String = "127.0.0.1",
    port: Int = 7077,
    retries: Int = 10,
    retryWait: FiniteDuration = 2 seconds,
    timeout: FiniteDuration = 10 seconds,
    keepCondition: Option[FiniteDuration] = None
  )(condition: ClusterStatus ⇒ Boolean)(implicit actorSystem: ActorSystem): ClusterStatus = {

    val masterActor = actorSystem.actorSelection(s"$protocol://sparkMaster@$host:$port/user/Master")

    val tries = for (
      tryNumber ← (1 to retries).toStream;
      masterState ← checkMaster(masterActor, tryNumber, retryWait, timeout, tryNumber == retries);
      workerStates ← checkWorkers(masterState.registeredWorkers.map(worker ⇒ worker.actor), timeout)
      if condition(ClusterStatus(masterState, workerStates))
    ) yield ClusterStatus(masterState, workerStates)

    tries match {
      case result #:: remaining ⇒
        keepCondition match {
          case Some(duration) ⇒
            logInfo(s"Sleeping for $duration to check whether the condition is still valid")
            Thread.sleep(duration.toMillis)
            remaining match {
              case _ #:: _ ⇒ result
              case _ ⇒ throw new AssertionError("The condition could not be kept")
            }
          case _ ⇒ result
        }

      case _ ⇒
        throw new AssertionError("The condition could not be met")
    }
  }

  def checkMaster(
    masterActor: ActorSelection,
    tryNumber: Int,
    retryWait: FiniteDuration,
    timeout: FiniteDuration,
    throwTimeout: Boolean
  ): Option[MasterStatus] = {

    if (tryNumber > 1) Thread.sleep(retryWait.toMillis)
    logInfo(s"$tryNumber try to get the response from Spark Master at $masterActor")

    implicit val _timeout = Timeout(timeout)

    Try(Await.result(masterActor ? RequestMasterState, timeout)) match {
      case Success(r: MasterStateResponse) ⇒
        val state = MasterStatus(
          r.host, r.port, r.status.toString, r.workers.map(w ⇒ WorkerStatus(
            w.id, w.host, w.port, w.cores, w.memory, w.actor, w.webUiPort, w.publicAddress)).toList)
        logInfo(s"Got the response from Spark Master at $masterActor: $state")
        Some(state)

      case Failure(cause) ⇒
        println(s"${cause.getClass.getCanonicalName}: ${cause.getMessage}")
        if (throwTimeout) {
          throw new TimeoutException(cause.getMessage)
        }
        None

      case _ ⇒
        throw new AssertionError("Invalid response was received")
    }
  }

  def checkWorker(
    workerRef: ActorRef,
    timeout: FiniteDuration
  ): Option[RealWorkerStatus] = {

    logInfo(s"Try to get the response from Spark Worker at $workerRef")

    implicit val _timeout = Timeout(timeout)

    Try(Await.result(workerRef ? RequestWorkerState, timeout)) match {
      case Success(r: WorkerStateResponse) ⇒
        val state = RealWorkerStatus(
          r.workerId, r.cores, r.coresUsed, r.memory, r.memoryUsed, r.port
        )
        logInfo(s"Got the response from Spark Worker at $workerRef: $state")
        Some(state)

      case Failure(cause) ⇒
        logInfo(s"$workerRef - ${cause.getClass.getCanonicalName}: ${cause.getMessage}")
        None

      case _ ⇒
        throw new AssertionError("Invalid response was received")
    }
  }

  def checkWorkers(
    workersRefs: Iterable[ActorRef],
    timeout: FiniteDuration
  ): Option[Set[RealWorkerStatus]] = {

    Try(workersRefs.flatMap(workerRef ⇒ checkWorker(workerRef, timeout))) match {
      case Success(states) ⇒
        Some(states.toIterator.toSet)

      case Failure(cause) ⇒
        logInfo(s"Failed to get response from workers - ${cause.getClass.getCanonicalName}: ${cause.getMessage}")
        None
    }
  }

}

case class ClusterStatus(
  masterState: MasterStatus,
  workersState: Set[RealWorkerStatus])

case class MasterStatus(
  host: String,
  port: Int,
  status: String,
  registeredWorkers: List[WorkerStatus])

case class RealWorkerStatus(
  workerId: String,
  cores: Int,
  coresUsed: Int,
  memory: Int,
  memoryUsed: Int,
  port: Int)

case class WorkerStatus(
  id: String,
  host: String,
  port: Int,
  cores: Int,
  memory: Int,
  actor: ActorRef,
  webUiPort: Int,
  publicAddress: String)
