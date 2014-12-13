import scala.collection.mutable.ArrayBuffer

import spray.json.DefaultJsonProtocol
import spray.json.DeserializationException
import spray.json.JsArray
import spray.json.JsNumber
import spray.json.JsObject
import spray.json.JsString
import spray.json.JsValue
import spray.json.JsonFormat
import spray.json.pimpAny

trait JsonFormats extends DefaultJsonProtocol {
  case class SendTweet(userId: Int, time: Long, msg: String)

  implicit val tweetFormat = jsonFormat3(SendTweet)

  implicit object TimelineJsonFormat extends JsonFormat[Project4Server.Tweets] {
    def write(c: Project4Server.Tweets) = JsObject(
      "authorId" -> JsNumber(c.authorId),
      "message" -> JsString(c.message),
      "timeStamp" -> JsString(c.timeStamp.toString),
      "tweetId" -> JsString(c.tweetId),
      "mentions" -> JsArray(c.mentions.map(_.toJson).toVector),
      "hashTags" -> JsArray(c.hashtags.map(_.toJson).toVector))

    def read(value: JsValue) = {
      value.asJsObject.getFields("tweetId", "authorId", "message", "timeStamp", "mentions", "hashTags") match {
        case Seq(JsString(tweetId), JsNumber(authorId), JsString(message), JsString(timeStamp), JsArray(mentions), JsArray(hashTags)) =>
          new Project4Server.Tweets(tweetId, authorId.toInt, message, timeStamp.toLong, mentions.map(_.convertTo[String]).to[ArrayBuffer], hashTags.map(_.convertTo[String]).to[ArrayBuffer])
        case _ => throw new DeserializationException("Tweets expected")
      }
    }
  }

}