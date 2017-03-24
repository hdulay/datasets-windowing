import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

case class Event(val timestamp: Int, val event_id: String, val advertiser_id: Int,
                 val user_id: String, val event_type: String)

case class Impression(val published: Int, val advertiser_id: Int,
                      val creative_id: Int, val user_id: String)

/**
  * Created by hkdulay on 3/23/17.
  */
object Main extends App {

  val spark = SparkSession
    .builder()
    .appName("Events")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val events = spark.read.csv("data-engineer/events.csv").map { row =>
    new Event(
      row.getString(0).toInt,
      row.getString(1),
      row.getString(2).toInt,
      row.getString(3),
      row.getString(4))
  }

  val impressions = spark.read.csv("data-engineer/impressions.csv").map { row =>
    new Impression(
      row.getString(0).toInt,
      row.getString(1).toInt,
      row.getString(2).toInt,
      row.getString(3))
  }

  val join = events.join(impressions, Seq("advertiser_id", "user_id")).filter("timestamp > published")

  val window = Window.partitionBy($"user_id", $"advertiser_id", $"event_type", $"creative_id").orderBy($"timestamp")
  val prev = lag($"timestamp", 1).over(window).as("prev")
  val diff = ($"timestamp" - lag($"timestamp", 1).over(window)).as("diff")
  val select = join.select($"advertiser_id", $"event_type", prev, diff)
  val count_of_users = select
    .where("diff >= 60 or diff is null")
    .groupBy($"advertiser_id", $"event_type")
    .count()
    .orderBy($"advertiser_id", $"event_type")

  count_of_users.show

  count_of_users.coalesce(1).write.mode(SaveMode.Overwrite).csv("count_of_users")

  spark.close()
}

