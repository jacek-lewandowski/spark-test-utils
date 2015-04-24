package org.apache.spark.deploy

import scala.language.postfixOps

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.apache.spark.Logging

object TestUtil extends Logging {

  // @formatter:off
  private val akkaConfString =
    s"""
       |akka {
       |  loggers = [""akka.event.slf4j.Slf4jLogger""]
       |  log-config-on-startup = on
       |  log-dead-letters = 10
       |  log-dead-letters-during-shutdown = on
       |  stdout-loglevel = "DEBUG"
       |  jvm-exit-on-fatal-error = off
       |  actor {
       |    provider = "akka.remote.RemoteActorRefProvider"
       |    debug {
       |      autoreceive = on
       |      lifecycle = on
       |      fsm = on
       |      event-stream = on
       |    }
       |  }
       |  debug {
       |    receive = on
       |    autoreceive = on
       |  }
       |  remote {
       |    log-remote-lifecycle-events = on
       |    log-received-messages = on
       |    log-sent-messages = on
       |    enabled-transports = ["akka.remote.netty.tcp"]
       |    netty.tcp {
       |      hostname = "127.0.0.1"
       |      port = 7777
       |    }
       |    transport-failure-detector {
       |      heartbeat-interval = 1000
       |      acceptable-heartbeat-pause = 6000
       |      threshold = 300.0
       |    }
       |  }
       |}
     """.stripMargin
  // @formatter:on

  val akkaConf = ConfigFactory.parseString(akkaConfString)

  def withActorSystemDo[T](work: ActorSystem ⇒ T): T = {
    val actorSystem = ActorSystem("test", akkaConf)
    try {
      work(actorSystem)
    } finally {
      Thread.sleep(500)
      actorSystem.shutdown()
      actorSystem.awaitTermination()
    }
  }

}
