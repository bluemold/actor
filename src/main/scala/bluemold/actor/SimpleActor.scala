package bluemold.actor

/**
 * SimpleActor<br/>
 * Author: Neil Essy<br/>
 * Created: 5/17/11<br/>
 * <p/>
 * [Description]
 */

abstract class SimpleActor( implicit strategy: ActorStrategy ) extends AbstractActor with ActorRef {
  protected implicit final def self: ActorRef = this

  protected def currentStrategy: ActorStrategy = strategy

  protected def handleException(t: Throwable) { t.printStackTrace() }
}

abstract class SimpleSupervisedActor( implicit strategy: ActorStrategy ) extends AbstractSupervisedActor with ActorRef  {
  protected implicit final def self: ActorRef = this

  protected def currentStrategy: ActorStrategy = strategy
}


