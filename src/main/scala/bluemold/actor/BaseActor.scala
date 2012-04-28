package bluemold.actor

import java.lang.UnsupportedOperationException
import bluemold.actor.Actor._
import java.io.ObjectStreamException

/**
 * BaseActor<br/>
 * Author: Neil Essy<br/>
 * Created: 5/27/11<br/>
 * <p/>
 * [Description]
 */
class BaseActor( actor: Actor )( implicit strategy: ActorStrategy ) extends AbstractActor with ActorRef {

  override def isTailMessaging = actor.isTailMessaging

  protected implicit final def self: ActorRef = this

  protected final def currentStrategy: ActorStrategy = strategy

  protected def init() {
    actor._self = this
    actor._init()
  }

  protected def react: PartialFunction[Any, Unit] = actor._react

  override def start()(implicit sender: ActorRef): ActorRef = _start()(sender)

  override def stop()(implicit sender: ActorRef) { 
    _stop()(sender)
    actor._self = null
  }

  override private[actor] def getTimeout(): Long = {
    if ( actor.isInstanceOf[WithTimeout] ) {
      actor.asInstanceOf[WithTimeout].getTimeout
    } else 0
  }
}

class BaseSupervisedActor( actor: SupervisedActor, parent: ActorRef )( implicit strategy: ActorStrategy ) extends AbstractSupervisedActor with ActorRef {

  override def isTailMessaging = actor.isTailMessaging

  protected implicit final def self: ActorRef = this

  protected final def currentStrategy: ActorStrategy = strategy

  protected def init() {
    actor._self = this
    actor._init()
  }

  protected def react: PartialFunction[Any, Unit] = actor._react

  override def start()(implicit sender: ActorRef): ActorRef = _start()(sender)

  override def stop()(implicit sender: ActorRef) { _stop()(sender) }

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

  override protected def handleException(t: Throwable) { val parent = getParent; if ( parent != null ) parent ! ActorFailure( self, t ) }
}

class BaseRegisteredActor( actor: RegisteredActor, parent: ActorRef )( implicit strategy: ActorStrategy ) extends AbstractSupervisedActor with RegisteredActorLike {

  override def isTailMessaging = actor.isTailMessaging

  private[actor] val _localRef = new LocalActorRef( this )
  protected implicit final def self: ActorRef = _localRef

  protected final def currentStrategy: ActorStrategy = strategy
  
  private[actor] def _actor = actor

  protected def init() {
    actor._self = _localRef
    actor._init()
  }

  protected def react: PartialFunction[Any, Unit] = actor._react

  override def start()( implicit sender: ActorRef ): ActorRef = {
    strategy.getNode.register( this );
    _start()(sender)
  }

  override def stop()( implicit sender: ActorRef ) {
    strategy.getNode.unRegister( this );
    _stop()(sender)
  }

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

  override protected def handleException(t: Throwable) {
    val parent = getParent;
    if ( parent != null && parent.isInstanceOf[SupervisedActorLike] )
      parent ! ActorFailure( self, t )
    else t.printStackTrace()
  }
}

@SerialVersionUID(1L)
final class LocalActorRef( baseActor: BaseRegisteredActor ) extends ActorRef with Serializable {
  val uuid = new UUID( baseActor.getCurrentStrategy().getNode.getNodeId )
  def _getUUID: UUID = uuid

  def stop()(implicit sender: ActorRef) { baseActor.stop() }

  def start()(implicit sender: ActorRef): ActorRef = baseActor.start()

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

  private[actor] def _onTimeout(delay: Long)(body: => Any)(implicit sender: ActorRef) = baseActor._onTimeout( delay )( body )( sender )

  private[actor] def _requeue(msgs: List[(Any, ReplyChannel)]) { baseActor._requeue( msgs ) } 

  private[actor] def _requeue(msg: Any, sender: ReplyChannel) { baseActor._requeue( msg, sender ) }

  private[actor] def _become(react: PartialFunction[Any, Unit]) { baseActor._become( react ) }

  private[actor] def _reply( msg: Any ) { baseActor._reply( msg ) }

  def checkStatus()(implicit sender: ActorRef) { baseActor.checkStatus() }
  
  def isStopped(implicit sender: ActorRef): Boolean = baseActor.isStopped

  def isActive(implicit sender: ActorRef): Boolean = baseActor.isActive

  def isPreStart(implicit sender: ActorRef): Boolean = baseActor.isActive

  def isTailMessaging = false

  @throws(classOf[ObjectStreamException])
  def writeReplace(): AnyRef = new TransientActorRef( uuid )
}

@SerialVersionUID(1L)
final class TransientActorRef( uuid: UUID ) extends Serializable {
  @throws(classOf[ObjectStreamException])
  def readResolve(): AnyRef = {
    val node = Node.forSerialization.get()
    if ( node._nodeId == uuid.nodeId )
      node.getByUUID( uuid )
    else new RemoteActorRef( uuid, node )
  }
}

@SerialVersionUID(1L)
final class RemoteActorRef( uuid: UUID, node: Node ) extends ActorRef with Serializable {

  def _getUUID = uuid
  def _getNode = node

  def stop()(implicit sender: ActorRef) {
    sender match {
      case localSender: LocalActorRef => node.send( uuid.nodeId, StopActorNodeMessage( uuid, localSender._getUUID ), localSender )
      case _ => throw new IllegalArgumentException( "Only registered actors are allowed to send messages across a network")
    }
  }

  def start()(implicit sender: ActorRef): ActorRef = throw new UnsupportedOperationException()

  def forward(msg: Any)(implicit sender: ActorRef) {
    sender.currentReplyChannel match {
      case future: AbstractFuture => {
        implicit val wrapperStrategy = new WrapperActorStrategy( sender.getCurrentStrategy(), node )
        val futureActor = actorOf( new FutureActor( future ) ).start()
        node.send( uuid, msg )( futureActor )
      }
      case replyAction: ReplyAction => {
        val actee = replyAction._getActee
        val wrapperStrategy = new WrapperActorStrategy( actee.getCurrentStrategy(), node )
        val replyActor = actorOf( new ReplyActor( replyAction ) )(wrapperStrategy,actee).start()
        node.send( uuid, msg )( replyActor )
      }
      case actor: RemoteActorRef => {
        node.send( uuid, msg )( actor )
      }
      case actor: LocalActorRef => {
        node.send( uuid, msg )( actor )
      }
      case actor: ActorRef => {
        throw new IllegalArgumentException( "Only registered actors are allowed to send messages across a network")
      }
    }
  }

  def !!(msg: Any)(implicit sender: ActorRef): Future[Any] = {
    // even though this is technically feasible, we don't want non-registered actors initiating network messages
    if ( sender.isInstanceOf[LocalActorRef]) { 
      val future = new AbstractFuture
      future.setExpiration( sender.getTimeout() )
      implicit val wrapperStrategy = new WrapperActorStrategy( sender.getCurrentStrategy(), node )
      val futureActor = actorOf( new FutureActor( future ) ).start()
      node.send( uuid, msg )( futureActor )
      future
    } else throw new IllegalArgumentException( "Only registered actors are allowed to send messages across a network" )
  }

  def !?(msg: Any)(react: PartialFunction[Any, Unit])(implicit sender: ActorRef) {
    // even though this is technically feasible, we don't want non-registered actors initiating network messages
    if ( sender.isInstanceOf[LocalActorRef]) { 
      val replyAction = new BlockingReplyAction( sender, react, sender.currentReplyChannel )
      sender.blockOn( replyAction )
      implicit val wrapperStrategy = new WrapperActorStrategy( sender.getCurrentStrategy(), node )
      val replyActor = actorOf( new ReplyActor( replyAction ) ).start()
      node.send( uuid, msg )( replyActor )
    } else throw new IllegalArgumentException( "Only registered actors are allowed to send messages across a network" )
  }

  def ?(msg: Any)(react: PartialFunction[Any, Unit])(implicit sender: ActorRef) {
    // even though this is technically feasible, we don't want non-registered actors initiating network messages
    if ( sender.isInstanceOf[LocalActorRef]) { 
      val replyAction = if ( sender.isBlockingOnAsync ) {
        val blockingReplyAction = new BlockingReplyAction( sender, react, sender.currentReplyChannel )
        sender.blockOn( blockingReplyAction )
        blockingReplyAction
      } else new ReplyAction( sender, react, sender.currentReplyChannel ) 
      implicit val wrapperStrategy = new WrapperActorStrategy( sender.getCurrentStrategy(), node )
      val replyActor = actorOf( new ReplyActor( replyAction ) ).start()
      node.send( uuid, msg )( replyActor )
    } else throw new IllegalArgumentException( "Only registered actors are allowed to send messages across a network" )
  }

  def !(msg: Any)(implicit sender: ActorRef) {
    // even though this is technically feasible, we don't want non-registered actors initiating network messages
    if ( sender.isInstanceOf[LocalActorRef] ) {
      val localActorRef = sender.asInstanceOf[LocalActorRef]
      if ( localActorRef.getCurrentStrategy().getNode == node )
        node.send( uuid, msg)( sender )
      else {
        implicit val wrapperStrategy = new WrapperActorStrategy( sender.getCurrentStrategy(), node )
        val wrapperActor = actorOf( new WrapperActor( sender ) ).start()
        node.send( uuid, msg )( wrapperActor )
      }
    } else throw new IllegalArgumentException( "Only registered actors are allowed to send messages across a network")
  }

  def issueReply(msg: Any)(implicit sender: ActorRef) { this.!(msg) }

  private[actor] def currentReplyChannel: ReplyChannel = throw new UnsupportedOperationException()

  private[actor] def blockOn(replyAction: ReplyAction) { throw new UnsupportedOperationException() }

  private[actor] def isBlockingOnAsync: Boolean = throw new UnsupportedOperationException()

  private[actor] def doGetNextStrategy(): ActorStrategy = throw new UnsupportedOperationException()

  private[actor] def getCurrentStrategy(): ActorStrategy = throw new UnsupportedOperationException()

  private[actor] def getTimeout(): Long = throw new UnsupportedOperationException()

  private[actor] def doGetParent: ActorRef = throw new UnsupportedOperationException()

  private[actor] def doLink(actor: ActorRef) { throw new UnsupportedOperationException() }

  private[actor] def doUnlink(actor: ActorRef) { throw new UnsupportedOperationException() }

  private[actor] def doGetChildActors: List[ActorRef] = throw new UnsupportedOperationException()

  private[actor] def _requeue(msgs: List[(Any, ReplyChannel)]) { throw new UnsupportedOperationException() }

  private[actor] def _requeue(msg: Any, sender: ReplyChannel) { throw new UnsupportedOperationException() }

  private[actor] def _onTimeout(delay: Long)(body: => Any)(implicit sender: ActorRef) = { throw new UnsupportedOperationException() }

  private[actor] def _become(react: PartialFunction[Any, Unit]) { throw new UnsupportedOperationException() }

  private[actor] def _reply( msg: Any ) { throw new UnsupportedOperationException() }

  // Status is dynamically queried for remote actors.
  // Methods are answered with stale information until new information is returned. 
  @volatile var stopped = false
  @volatile var lastStatusCheck = System.currentTimeMillis()

  def isTailMessaging = false

  def checkStatus()( implicit sender: ActorRef ) {
    lastStatusCheck = System.currentTimeMillis()
    implicit val wrapperStrategy = new WrapperActorStrategy( sender.getCurrentStrategy(), node )
    val statusActor = actorOf( new StatusActor( this ) ).start()
    statusActor match {
      case localStatusActor: LocalActorRef => node.send( uuid.nodeId, StatusRequestNodeMessage( uuid, localStatusActor._getUUID ), localStatusActor )
      case _ => // todo: what do we do here?
    }
  }

  def isStopped( implicit sender: ActorRef ): Boolean = {
    if ( lastStatusCheck < System.currentTimeMillis() - 60000 )
      checkStatus()
    stopped
  }

  def isActive( implicit sender: ActorRef ): Boolean = {
    if ( lastStatusCheck < System.currentTimeMillis() - 60000 )
      checkStatus()
    ! stopped
  }

  def isPreStart( implicit sender: ActorRef ): Boolean = false

  @throws(classOf[ObjectStreamException])
  def writeReplace(): AnyRef = new TransientActorRef( uuid )
}

final class WrapperActor( actor: ActorRef ) extends RegisteredActor {
  override def isTailMessaging = actor.isTailMessaging

  protected def init() {}
  protected def react: PartialFunction[Any, Unit] = {
    case msg: Any => {
      if ( replyChannel.isInstanceOf[ActorRef] )
        actor.!( msg )( replyChannel.asInstanceOf[ActorRef] )
      else throw new IllegalStateException( "What Happened!" )
      self.stop()
    }
  }
}

final class ReplyActor( replyAction: ReplyAction ) extends RegisteredActor {
  protected def init() {}
  protected def react: PartialFunction[Any, Unit] = {
    case msg: Any => {
      if ( replyChannel.isInstanceOf[ActorRef] )
        replyAction.issueReply( msg )( replyChannel.asInstanceOf[ActorRef] )
      else throw new IllegalStateException( "What Happened!" )
      self.stop()
    }
  }
}

final class FutureActor( future: AbstractFuture ) extends RegisteredActor {
  protected def init() {}
  protected def react: PartialFunction[Any, Unit] = {
    case msg: Any => {
      if ( replyChannel.isInstanceOf[ActorRef] )
        future.issueReply( msg )( replyChannel.asInstanceOf[ActorRef] )
      else throw new IllegalStateException( "What Happened!" )
      self.stop()
    }
  }
}

final class StatusActor( actor: RemoteActorRef ) extends RegisteredActor {
  protected def init() {}
  protected def react: PartialFunction[Any, Unit] = {
    case stopped: Boolean => 
      self.stop()
      actor.stopped = stopped
    case _ =>
      self.stop()
      throw new IllegalStateException( "What Happened!" )
  }
}

final class WrapperActorStrategy( strategy: ActorStrategy, node: Node ) extends ActorStrategy {
  def getNode: Node = node

  def setDefaultTimeout(newDefault: Long) { strategy.setDefaultTimeout(newDefault) }

  def getDefaultTimeout(): Long = strategy.getDefaultTimeout()

  def getNextStrategy(): ActorStrategy = strategy.getNextStrategy()

  def getNextBalancedStrategy() = strategy.getNextBalancedStrategy()

  def enqueue(actor: AbstractActor) { strategy.enqueue( actor ) }

  def send(msg: Any, actor: AbstractActor, sender: ReplyChannel) { strategy.send( msg, actor, sender ) }
}