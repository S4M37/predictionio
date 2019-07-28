package org.example.ecommercerecommendation

case class DataSourceParams(appName: String) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
    EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    // create a RDD of (entityID, User)
    val usersRDD: RDD[(String, User)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "user"
    )(sc).map { case (entityId, properties) =>
      val user = try {
        User()
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" user ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, user)
    }.cache()

    // create a RDD of (entityID, Item)
    val itemsRDD: RDD[(String, Item)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "item"
    )(sc).map { case (entityId, properties) =>
      val item = try {
        // Assume categories is optional property of item.
        Item(categories = properties.getOpt[List[String]]("categories"))
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" item ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, item)
    }.cache()

    val eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("view", "click", "buy")),
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("item")))(sc)
      .cache()

    val viewEventsRDD: RDD[ViewEvent] = eventsRDD
      .filter { event => event.event == "view" }
      .map { event =>
        try {
          ViewEvent(
            user = event.entityId,
            item = event.targetEntityId.get,
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to ViewEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }

    val clickEventsRDD: RDD[ClickEvent] = eventsRDD
      .filter { event => event.event == "click" }
      .map { event =>
        try {
          ClickEvent(
            user = event.entityId,
            item = event.targetEntityId.get,
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to ClickEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }

    val buyEventsRDD: RDD[BuyEvent] = eventsRDD
      .filter { event => event.event == "buy" }
      .map { event =>
        try {
          BuyEvent(
            user = event.entityId,
            item = event.targetEntityId.get,
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to BuyEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }

    val rateEventsRDD: RDD[RateEvent] = eventsRDD
      .filter { event => event.event == "click" || event.event == "view" }
      .groupBy(event => (event.entityId, event.targetEntityId.get))
      .map(groupItem => {
        var score = 0
        groupItem._2.foreach(f => {
          if (f.event == "click") {
            score += 2
          } else if (f.event == "view") {
            score += 1
          }
        })
        try {
          val rateEvent = (groupItem._1._1,
            groupItem._1._2,
            score,
            groupItem._2.map { e => e.eventTime.getMillis }.collectFirst { case e => e }.get)
          println(rateEvent)
          rateEvent
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${groupItem} to RateEvent." +
              s" Exception: ${e}.")
            throw e
        }
      })

    new TrainingData(
      users = usersRDD,
      items = itemsRDD,
      viewEvents = viewEventsRDD,
      clickEvents = clickEventsRDD,
      buyEvents = buyEventsRDD,
      rateEvents = rateEventsRDD
    )
  }
}

case class User()

case class Item(categories: Option[List[String]])

case class ViewEvent(user: String, item: String, t: Long)

case class ClickEvent(user: String, item: String, t: Long)

case class BuyEvent(user: String, item: String, t: Long)

case class RateEvent(user: String, item: String, rating: Double, t: Long)

class TrainingData(
                    val users: RDD[(String, User)],
                    val items: RDD[(String, Item)],
                    val viewEvents: RDD[ViewEvent],
                    val clickEvents: RDD[ClickEvent],
                    val buyEvents: RDD[BuyEvent],
                    val rateEvents: RDD[RateEvent]
                  ) extends Serializable {
  override def toString = {
    s"users: [${users.count()} (${users.take(2).toList}...)]" +
      s"items: [${items.count()} (${items.take(2).toList}...)]" +
      s"viewEvents: [${viewEvents.count()}] (${viewEvents.take(2).toList}...)" +
      s"clickEvents: [${clickEvents.count()}] (${clickEvents.take(2).toList}...)" +
      s"buyEvents: [${buyEvents.count()}] (${buyEvents.take(2).toList}...)" +
      s"rateEvents: [${rateEvents.count()}] (${rateEvents.take(2).toList}...)"
  }
}
