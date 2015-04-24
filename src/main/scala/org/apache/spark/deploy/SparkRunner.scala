package org.apache.spark.deploy

import java.nio.file.attribute.PosixFileAttributes
import java.nio.file.{Files, Path, Paths}

import scala.collection.JavaConversions._

import org.apache.spark.Logging

trait SparkRunner extends Logging {
  @volatile var process: Option[Process] = None

  def spawnSparkDaemon(executable: String, env: Map[String, Any], args: List[String]*): Process = {
    val cmd = executable :: args.flatten.toList
    logInfo(s"Executing command: ${cmd.mkString(" ")} with env = $env")
    val builder = new ProcessBuilder(cmd)
    builder.environment().putAll(env.mapValues(_.toString))
    builder.inheritIO().start()
  }

  def start()

  def stop()

  def host: String

  def port: Int

}

object SparkRunner {
  val esc = 27.toChar
  def colorFmt(color: String) = s"$esc[${color}m"

  val sparkHome = sys.env
    .getOrElse("SPARK_HOME", throw new IllegalArgumentException("SPARK_HOME must be defined"))

  def logConfigurationArg(logConfiguration: Path, color: String) = {
    val path = if (!logConfiguration.isAbsolute) {
      val url = getClass.getClassLoader.getResource(logConfiguration.normalize().toString)
      Paths.get(url.toURI).toAbsolutePath.toUri
    } else {
      logConfiguration.normalize().toUri
    }
    s"-Dlog4j.configuration=$path -Dlog.prefix=${colorFmt(color)}"
  }

  def hostArg(host: String) = List("--host", host)

  def portArg(port: Int) = List("--port", port.toString)

  def masterArg(host: String, port: Int) = List(s"spark://$host:$port")

  def startMaster(
    host: String = "127.0.0.1",
    port: Int = 7077,
    logConfiguration: Path = Paths.get("log4j-master.properties")
  ): SparkRunner = {
    val runner = new SparkMasterRunner(host, port, logConfiguration)
    runner.start()

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = runner.stop()
    })

    runner
  }

  def startWorker(host: String, port: Int, master: SparkRunner, logConfiguration: Path, color: String): SparkRunner = {
    val runner = new SparkWorkerRunner(host, port, master.host, master.port, logConfiguration, color)
    runner.start()
    runner
  }

  def startWorker(n: Int, master: SparkRunner): SparkRunner = {
    startWorker("127.0.0.1", 6000 + n * 100, master, Paths.get("log4j-worker.properties"), IndexedSeq("93", "96", "95", "92")(n - 1))
  }
}

class SparkMasterRunner(
  val host: String,
  val port: Int,
  logConfiguration: Path,
  color: String = "97"
) extends SparkRunner with Logging {

  import org.apache.spark.deploy.SparkRunner._

  val sparkMasterStartScript = Paths.get(sparkHome, "sbin", "start-master.sh")
  val sparkMasterStopScript = Paths.get(sparkHome, "sbin", "stop-master.sh")

  val env = Map(
    "SPARK_MASTER_IP" → host,
    "SPARK_MASTER_PORT" → port,
    "SPARK_MASTER_OPTS" → s"${logConfigurationArg(logConfiguration, color)} -Dspark.deploy.recoveryMode=NONE"
  )

  def start(): Unit = {
    val process = spawnSparkDaemon(sparkMasterStartScript.toString, env)

    val result = try {
      process.waitFor()
    } catch {
      case ex: InterruptedException ⇒
        logWarning("Spark Master startup process has been interrupted.")
        process.destroy()
        throw ex
    }

    if (result != 0) {
      throw new Exception(s"Failed to start Spark Master, exit code was $result")
    }
  }

  def stop(): Unit = {
    val process = spawnSparkDaemon(sparkMasterStopScript.toString, env)

    val result = try {
      process.waitFor()
    } catch {
      case ex: InterruptedException ⇒
        logWarning("Spark Master stop process has been interrupted.")
        process.destroy()
        throw ex
    }

    if (result != 0) {
      throw new Exception(s"Failed to stop Spark Master, exit code was $result")
    }
  }
}

class SparkWorkerRunner(
  val host: String,
  val port: Int,
  masterHost: String,
  masterPort: Int,
  logConfiguration: Path,
  color: String = "97"
) extends SparkRunner with Logging {

  import org.apache.spark.deploy.SparkRunner._

  val sparkClassScript = Paths.get(sparkHome, "bin", "spark-class")

  val workerDir = Files.createTempDirectory("spark-worker").toAbsolutePath

  val env = Map(
    "SPARK_WORKER_OPTS" → logConfigurationArg(logConfiguration, color),
    "SPARK_WORKER_DIR" → workerDir
  )

  private class InternalRunner extends Thread {
    override def run(): Unit = {
      val classNameArg = List("org.apache.spark.deploy.worker.Worker")
      val process = spawnSparkDaemon(
        sparkClassScript.toString,
        env,
        classNameArg,
        masterArg(masterHost, masterPort),
        hostArg(host),
        portArg(port)
      )
      val result = try {
        process.waitFor()
      } catch {
        case ex: InterruptedException ⇒
          process.destroy()
          logWarning("Spark Worker process has been interrupted.")
          0
      }

      if (result != 0)
        throw new Exception(s"Failed to run Spark Worker, exit code was $result")
    }
  }

  private val internalRunner = new InternalRunner

  override def start(): Unit = {
    internalRunner.start()
  }

  override def stop(): Unit = {
    internalRunner.interrupt()
  }
}
