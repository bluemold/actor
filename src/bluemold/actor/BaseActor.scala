package bluemold.actor

import bluemold.cluster.UUID
import bluemold.concurrent.NonLockingHashMap
import java.lang.UnsupportedOperationException

/**
 * BaseActor<br/>
 * Author: Neil Essy<br/>
 * Created: 5/27/11<br/>
 * <p/>
 * [Description]
 */
object BaseActor {
  val byClassName = new NonLockingHashMap[String,NonLockingHashMap[LocalActorRef,BaseRegisteredActor]]
  val byId = new NonLockingHashMap[String,NonLockingHashMap[LocalActorRef,BaseRegisteredActor]]
  val byUUID = new NonLockingHashMap[UUID,BaseRegisteredActor]
  
  def register( registeredActor: BaseRegisteredActor ) {
    val className = registeredActor._getClass.getName
    val id = registeredActor._getId
    val uuid = registeredActor._getUUID
    byClassName.getOrElseUpdate( className, new NonLockingHashMap[LocalActorRef,BaseRegisteredActor]() )
            .put( registeredActor._localRef, registeredActor )
    byId.getOrElseUpdate( id, new NonLockingHashMap[LocalActorRef,BaseRegisteredActor]() )
            .put( registeredActor._localRef, registeredActor )
    byUUID.put( uuid, registeredActor )
  }
  def unRegister( registeredActor: BaseRegisteredActor ) {
    val className = registeredActor._getClass.getName
    val id = registeredActor._getId
    val uuid = registeredActor._getUUID
    byClassName.get( className ) match {
      case Some( map ) => map.remove( registeredActor._localRef )
      case None => // nothing to remove
    }
    byId.get( id ) match {
      case Some( map ) => map.remove( registeredActor._localRef )
      case None => // nothing to remove
    }
    byUUID.remove( uuid )
  }
  def getAllByClassName( className: String ): List[ActorRef] = {
    byClassName.get( className ) match {
      case Some( map ) => map.keySet.toList
      case None => Nil
    }
  }
  def getAll(): List[ActorRef] = {
    var ret: List[ActorRef] = Nil
    val iterator = byUUID.values.iterator
    while ( iterator.hasNext )
      ret ::= iterator.next()._localRef
    ret
  }
  def getAllById( id: String ): List[ActorRef] = {
    byId.get( id ) match {
      case Some( map ) => map.keySet.toList
      case None => Nil
    }
  }
  def getByUUID( uuid: UUID ): ActorRef = {
    byUUID.get( uuid ) match {
      case Some( actor ) => actor._localRef
      case None => null
    }
  }

}

class BaseActor( actor: Actor )( implicit strategy: ActorStrategy ) extends AbstractActor with ActorRef {

  protected implicit final def self: ActorRef = this

  protected final def currentStrategy: ActorStrategy = strategy

  protected def init() {
    actor._self = this
    actor._init()
  }

  protected def react: PartialFunction[Any, Unit] = actor._react

  override def start(): ActorRef = _start()

  override def stop() { 
    _stop()
    actor._self = null
  }

  protected def handleException(t: Throwable) { t.printStackTrace() }

  override private[actor] def getTimeout(): Long = {
    if ( actor.isInstanceOf[WithTimeout] ) {
      actor.asInstanceOf[WithTimeout].getTimeout
    } else 0
  }
}

class BaseSupervisedActor( actor: SupervisedActor, parent: ActorRef )( implicit strategy: ActorStrategy ) extends AbstractSupervisedActor with ActorRef {

  protected implicit final def self: ActorRef = this

  protected final def currentStrategy: ActorStrategy = strategy

  protected def init() {
    actor._self = this
    actor._init()
  }

  protected def react: PartialFunction[Any, Unit] = actor._react

  override def start(): ActorRef = _start()

  override def stop() { _stop() }

  protected def getParent: ActorRef = parent

  override private[actor] def doGetParent: ActorRef = parent

  override private[actor] def doLink(actor: ActorRef) { link( actor ) }

  override private[actor] def doUnlink(actor: ActorRef) { unlink( actor ) }

  override private[actor] def doGetChildActors: List[ActorRef] = getChildActors

  override private[actor] def getTimeout(): Long = {
    if ( actor.isInstanceOf[WithTimeout] ) {
      actor.asInstanceOf[WithTimeout].getTimeout
    } else 0
  }
}

class BaseRegisteredActor( actor: RegisteredActor, parent: ActorRef )( implicit strategy: ActorStrategy ) extends AbstractSupervisedActor with RegisteredActorLike {

  private[actor] val _localRef = new LocalActorRef( this )
  protected implicit final def self: ActorRef = _localRef

  protected final def currentStrategy: ActorStrategy = strategy

  protected def init() {
    actor._self = _localRef
    actor._init()
  }

  protected def react: PartialFunction[Any, Unit] = actor._react

  override def start(): ActorRef = { BaseActor.register( this ); _start() }

  override def stop() { BaseActor.unRegister( this ); _stop() }

  protected def getParent: ActorRef = parent

  override private[actor] final def doGetParent: ActorRef = parent

  override private[actor] final def doLink(actor: ActorRef) { link( actor ) }

  override private[actor] final def doUnlink(actor: ActorRef) { unlink( actor ) }

  override private[actor] final def doGetChildActors: List[ActorRef] = getChildActors

  override private[actor] final def _getUUID: UUID = _localRef._getUUID

  protected def getUUID: UUID = _getUUID

  override private[actor] def getTimeout(): Long = {
    if ( actor.isInstanceOf[WithTimeout] ) {
      actor.asInstanceOf[WithTimeout].getTimeout
    } else 0
  }
}

class LocalActorRef( baseActor: BaseRegisteredActor ) extends ActorRef {
  final val uuid = new UUID()
  final def _getUUID: UUID = uuid

  final val registeredActorRef = new RegisteredActorRef( uuid )

  def stop() { baseActor.stop() }

  def start(): ActorRef = baseActor.start()

  def forward(msg: Any)(implicit sender: ActorRef) { baseActor.forward( msg ) }

  def !!(msg: Any)(implicit sender: ActorRef): Future[Any] = baseActor !! msg

  def !?(msg: Any)(react: PartialFunction[Any, Unit])(implicit sender: ActorRef) { baseActor.!?(msg)(react) }

  def ?(msg: Any)(react: PartialFunction[Any, Unit])(implicit sender: ActorRef) { baseActor.?(msg)(react) }

  def !(msg: Any)(implicit sender: ActorRef) { baseActor ! msg }

  def issueReply(msg: Any)(implicit sender: ActorRef) { baseActor.issueReply( msg ) }

  private[actor] def currentReplyChannel: ReplyChannel = baseActor.currentReplyChannel

  private[actor] def blockOn(replyAction: ReplyAction) { baseActor.blockOn( replyAction ) }

  private[actor] def isBlockingOnAsync: Boolean = baseActor.isBlockingOnAsync

  private[actor] def doGetNextStrategy(): ActorStrategy = baseActor.doGetNextStrategy()

  private[actor] def getCurrentStrategy(): ActorStrategy = baseActor.getCurrentStrategy()

  private[actor] def getTimeout(): Long = baseActor.getTimeout()

  private[actor] def doGetParent: ActorRef = baseActor.doGetParent

  private[actor] def doLink(actor: ActorRef) { baseActor.doLink( actor ) }

  private[actor] def doUnlink(actor: ActorRef) { baseActor.doUnlink( actor ) }

  private[actor] def doGetChildActors: List[ActorRef] = baseActor.doGetChildActors

  private[actor] def _requeue(msgs: List[(Any, ReplyChannel)]) { baseActor._requeue( msgs ) } 

  private[actor] def _requeue(msg: Any, sender: ReplyChannel) { baseActor._requeue( msg, sender ) }

  private[actor] def _become(react: PartialFunction[Any, Unit]) { baseActor._become( react ) }

  private[actor] def _reply( msg: Any ) { baseActor._reply( msg ) }

  def isStopped: Boolean = baseActor.isStopped

  def isActive: Boolean = baseActor.isActive

  def isPreStart: Boolean = baseActor.isActive
}

class RegisteredActorRef( uuid: UUID ) extends ActorRef {
  final def _getUUID: UUID = uuid
  
  // TODO

  def stop() {}

  def start(): ActorRef = throw new UnsupportedOperationException()

  def forward(msg: Any)(implicit sender: ActorRef) {}

  def !!(msg: Any)(implicit sender: ActorRef): Future[Any] = null

  def !?(msg: Any)(react: PartialFunction[Any, Unit])(implicit sender: ActorRef) {}

  def ?(msg: Any)(react: PartialFunction[Any, Unit])(implicit sender: ActorRef) {}

  def !(msg: Any)(implicit sender: ActorRef) {}

  def issueReply(msg: Any)(implicit sender: ActorRef) {}

  private[actor] def currentReplyChannel: ReplyChannel = null

  private[actor] def blockOn(replyAction: ReplyAction) {}

  private[actor] def isBlockingOnAsync: Boolean = false

  private[actor] def doGetNextStrategy(): ActorStrategy = throw new UnsupportedOperationException()

  private[actor] def getCurrentStrategy(): ActorStrategy = throw new UnsupportedOperationException()

  private[actor] def getTimeout(): Long = throw new UnsupportedOperationException()

  private[actor] def doGetParent: ActorRef = throw new UnsupportedOperationException()

  private[actor] def doLink(actor: ActorRef) { throw new UnsupportedOperationException() }

  private[actor] def doUnlink(actor: ActorRef) { throw new UnsupportedOperationException() }

  private[actor] def doGetChildActors: List[ActorRef] = throw new UnsupportedOperationException()

  private[actor] def _requeue(msgs: List[(Any, ReplyChannel)]) { throw new UnsupportedOperationException() }

  private[actor] def _requeue(msg: Any, sender: ReplyChannel) { throw new UnsupportedOperationException() }

  private[actor] def _become(react: PartialFunction[Any, Unit]) { throw new UnsupportedOperationException() }

  private[actor] def _reply( msg: Any ) { throw new UnsupportedOperationException() }

  def isStopped: Boolean = false // todo

  def isActive: Boolean = false // todo

  def isPreStart: Boolean = false // todo
}