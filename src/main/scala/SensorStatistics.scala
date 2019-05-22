import java.io.File
import java.nio.file.Path
import akka.stream.scaladsl.{FileIO, Flow, Framing, Sink, Source}
import scala.concurrent.ExecutionContext
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.util.ByteString
import scala.util.{Failure, Success, Try}

object SensorStatistics extends App {

  implicit val actorSystem: ActorSystem = ActorSystem("sensor-statistics")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = ExecutionContext.global
  val MAX_GROUPS = 1000

  val directory = args.headOption.getOrElse {
    Console.err.println("Please provide a directory")
    sys.exit(1)
  }
  val files = getListOfFiles(directory)
  val lines = Framing.delimiter(ByteString(System.lineSeparator), maximumFrameLength = 256, allowTruncation = true)
    .map(bs => bs.utf8String)

  val filesSource = Source(files.map(_.toPath))
  val aggregatingMeasurementsFlow = Flow[Path]
    .flatMapConcat(FileIO.fromPath(_, chunkSize = 128).via(lines).drop(1))
    .map(measurement => measurement.split(','))
    .groupBy(MAX_GROUPS, _.head)
    .fold(Sensor())((aggregatedMeasurements, newMeasurement) => addMeasurement(aggregatedMeasurements, newMeasurement))
    .mergeSubstreams
  val result = filesSource.via(aggregatingMeasurementsFlow).runWith(Sink.seq)

  result.onComplete({
    case Success(sensors) => printGeneralStatistics(sensors)
      printSensorStatistics(sensors)
      actorSystem.terminate()
    case err => println(s"Error occured: $err")
      actorSystem.terminate()
  })

  private def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  private def addMeasurement(aggregated: Sensor,
                             newMeasurement: Array[String]): Sensor = Try(newMeasurement(1).toInt) match {
      case Success(value) =>
        val min = scala.math.min(value, aggregated.min.getOrElse(value))
        val max = scala.math.max(value, aggregated.max.getOrElse(value))
        val sum = value + aggregated.sum.getOrElse(0L)
        Sensor(newMeasurement.head, Some(min), Some(max), Some(sum), aggregated.successful + 1, aggregated.failed)
      case Failure(_) => aggregated.copy(id = newMeasurement.head, failed = aggregated.failed + 1)
    }

  private def printGeneralStatistics(sensors: Seq[Sensor]): Unit = {
    val processedFiles = files.length
    val processedMeasurements = sensors.map(s => s.successful + s.failed).sum
    val failedMeasurements = sensors.map(_.failed).sum

    println(
      s"""Num of processed files: $processedFiles
         |Num of processed measurements: $processedMeasurements
         |Num of failed measurements: $failedMeasurements
         |
         |Sensors with highest avg humidity:
         |
         |sensor-id,min,avg,max""".stripMargin
    )
  }

  private def printSensorStatistics(sensors: Seq[Sensor]): Unit =
    sensors.map(sensor => sensor.copy(avg = sensor.sum.map(_ / sensor.successful)))
      .sortBy(_.avg)(Ordering[Option[Long]].reverse)
      .foreach(s => println(s"${s.id},${getPrintableResult(s.min)},${getPrintableResult(s.avg)},${getPrintableResult(s.max)}"))

  private def getPrintableResult[T](result: Option[T]): Any = result.getOrElse("NaN")
}
