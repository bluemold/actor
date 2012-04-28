package bluemold.actor

/**
 * SimpleActor<br/>
 * Author: Neil Essy<br/>
 * Created: 5/17/11<br/>
 * <p/>
 * [Description]
 */

abstract class SimpleActor( implicit strategy: ActorStrategy ) extends AbstractActor with ActorRef {
  override def isTailMessaging = false

  protected implicit final def self: ActorRef = this

  protected def currentStrategy: ActorStrategy = strategy
}

abstract class SimpleSupervisedActor( implicit strategy: ActorStrategy ) extends AbstractSupervisedActor with ActorRef  {
  override def isTailMessaging = false

  protected implicit final def self: ActorRef = this

  protected def currentStrategy: ActorStrategy = strategy
}


