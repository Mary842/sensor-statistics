case class Sensor(id: String = "", min: Option[Int] = None, max: Option[Int] = None, sum: Option[Long] = None,
                  successful: Long = 0, failed: Long = 0, avg: Option[Long] = None)