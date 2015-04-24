package org.apache.spark.deploy

import org.apache.spark.{SparkContext, Logging, SparkConf}

/**
 * @author Jacek Lewandowski
 */
object Test extends Logging {

  def main(args: Array[String]) {
    System.setProperty("log.prefix", SparkRunner.colorFmt("97"))
    val master = SparkRunner.startMaster()
    val workers = new Array[SparkRunner](3)

    TestUtil.withActorSystemDo { implicit actorSystem ⇒

      logInfo("Waiting for master to start-up")
      SparkClusterVerifier.verify() { status ⇒
        status.masterState.registeredWorkers.size == 0 && status.workersState.size == 0
      }

      logInfo("Starting a worker and waiting for master to register it")
      workers(0) = SparkRunner.startWorker(1, master)
      SparkClusterVerifier.verify() { status ⇒
        status.masterState.registeredWorkers.size == 1 && status.workersState.size == 1
      }

      logInfo("Starting another worker and waiting for master to register it")
      workers(1) = SparkRunner.startWorker(2, master)
      SparkClusterVerifier.verify() { status ⇒
        status.masterState.registeredWorkers.size == 2 && status.workersState.size == 2
      }

      val conf = new SparkConf()
        .setAppName("test")
        .setMaster(s"spark://${master.host}:${master.port}")
        .set("spark.executor.memory", "512M")

      logInfo("Starting SparkContext")
      val sc = new SparkContext(conf)

      SparkClusterVerifier.verify() { status ⇒
        status.workersState.forall(worker ⇒ worker.coresUsed > 0)
      }

      logInfo("Stopping SparkContext")
      sc.stop()

      SparkClusterVerifier.verify() { status ⇒
        status.workersState.forall(worker ⇒ worker.coresUsed == 0)
      }

    }

    for (worker ← workers if worker != null) worker.stop()
    master.stop()
  }

}
