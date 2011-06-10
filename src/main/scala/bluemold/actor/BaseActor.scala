package bluemold.actor

import java.lang.UnsupportedOperationException
import bluemold.actor.Actor._

/**
 * BaseActor<br/>
 * Author: Neil Essy<br/>
 * Created: 5/27/11<br/>
 * <p/>
 * [Description]
 */
class BaseActor( actor: Actor )( implicit strategy: ActorStrategy ) extends AbstractActor with ActorRef {

  protected implicit final def self: ActorRef = this

  protected final def currentStrategy: ActorStrategy = strategy

  protected def init() {
    actor._self = this
    actor._init()
  }

  protected def react: PartialFunction[Any, Unit] = actor._react

  override def start()(implicit sender: ActorRef): ActorRef = _start()

  override def stop()(implicit sender: ActorRef) { 
    _stop()
    actor._self = null
  }

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

  override def start()(implicit sender: ActorRef): ActorRef = _start()

  override def stop()(implicit sender: ActorRef) { _stop() }

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
    strategy.getCluster.register( this );
    _start()
  }

  override def stop()( implicit sender: ActorRef ) {
    strategy.getCluster.unRegister( this );
    _stop()
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

final class LocalActorRef( baseActor: BaseRegisteredActor ) extends ActorRef {
  val uuid = new UUID( baseActor.getCurrentStrategy().getCluster.getClusterId )
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

  private[actor] def _requeue(msgs: List[(Any, ReplyChannel)]) { baseActor._requeue( msgs ) } 

  private[actor] def _requeue(msg: Any, sender: ReplyChannel) { baseActor._requeue( msg, sender ) }

  private[actor] def _become(react: PartialFunction[Any, Unit]) { baseActor._become( react ) }

  private[actor] def _reply( msg: Any ) { baseActor._reply( msg ) }

  def checkStatus()(implicit sender: ActorRef) { baseActor.checkStatus() }
  
  def isStopped(implicit sender: ActorRef): Boolean = baseActor.isStopped

  def isActive(implicit sender: ActorRef): Boolean = baseActor.isActive

  def isPreStart(implicit sender: ActorRef): Boolean = baseActor.isActive
}

final class RemoteActorRef( uuid: UUID, cluster: Cluster ) extends ActorRef {

  def _getUUID = uuid
  def _getCluster = cluster

  def stop()(implicit sender: ActorRef) {
    if ( sender.isInstanceOf[LocalActorRef]) { 
      cluster.send( uuid.clusterId, StopActorClusterMessage( uuid, sender.asInstanceOf[LocalActorRef]._getUUID ) )
    } else throw new IllegalArgumentException( "Only registered actors are allowed to send messages across a cluster")
  }

  def start()(implicit sender: ActorRef): ActorRef = throw new UnsupportedOperationException()

  def forward(msg: Any)(implicit sender: ActorRef) {
    sender.currentReplyChannel match {
      case future: AbstractFuture => {
        implicit val wrapperStrategy = new WrapperActorStrategy( sender.getCurrentStrategy(), cluster )
        val futureActor = actorOf( new FutureActor( future ) ).start()
        cluster.send( uuid, msg )( futureActor )
      }
      case replyAction: ReplyAction => {
        val actee = replyAction._getActee
        val wrapperStrategy = new WrapperActorStrategy( actee.getCurrentStrategy(), cluster )
        val replyActor = actorOf( new ReplyActor( replyAction ) )(wrapperStrategy,actee).start()
        cluster.send( uuid, msg )( replyActor )
      }
      case actor: RemoteActorRef => {
        if ( actor._getCluster == cluster )
          // let the reply go directly, bypassing this node
          cluster.send( uuid, msg, actor._getUUID )
        else {
          // forward across clusters
          implicit val wrapperStrategy = new WrapperActorStrategy( sender.getCurrentStrategy(), cluster )
          val wrapperActor = actorOf( new WrapperActor( sender ) ).start()
          cluster.send( uuid, msg )( wrapperActor )
        }
      }
      case actor: LocalActorRef => {
        cluster.send( uuid, msg, actor._getUUID )
      }
      case actor: ActorRef => {
        throw new IllegalArgumentException( "Only registered actors are allowed to send messages across a cluster")
      }
    }
  }

  def !!(msg: Any)(implicit sender: ActorRef): Future[Any] = {
    // even though this is technically feasible, we don't want non-registered actors initiating cluster messages
    if ( sender.isInstanceOf[LocalActorRef]) { 
      val future = new AbstractFuture
      future.setExpiration( sender.getTimeout() )
      implicit val wrapperStrategy = new WrapperActorStrategy( sender.getCurrentStrategy(), cluster )
      val futureActor = actorOf( new FutureActor( future ) ).start()
      cluster.send( uuid, msg )( futureActor )
      future
    } else throw new IllegalArgumentException( "Only registered actors are allowed to send messages across a cluster" )
  }

  def !?(msg: Any)(react: PartialFunction[Any, Unit])(implicit sender: ActorRef) {
    // even though this is technically feasible, we don't want non-registered actors initiating cluster messages
    if ( sender.isInstanceOf[LocalActorRef]) { 
      val replyAction = new BlockingReplyAction( sender, react, sender.currentReplyChannel )
      sender.blockOn( replyAction )
      implicit val wrapperStrategy = new WrapperActorStrategy( sender.getCurrentStrategy(), cluster )
      val replyActor = actorOf( new ReplyActor( replyAction ) ).start()
      cluster.send( uuid, msg )( replyActor )
    } else throw new IllegalArgumentException( "Only registered actors are allowed to send messages across a cluster" )
  }

  def ?(msg: Any)(react: PartialFunction[Any, Unit])(implicit sender: ActorRef) {
    // even though this is technically feasible, we don't want non-registered actors initiating cluster messages
    if ( sender.isInstanceOf[LocalActorRef]) { 
      val replyAction = if ( sender.isBlockingOnAsync ) {
        val blockingReplyAction = new BlockingReplyAction( sender, react, sender.currentReplyChannel )
        sender.blockOn( blockingReplyAction )
        blockingReplyAction
      } else new ReplyAction( sender, react, sender.currentReplyChannel ) 
      implicit val wrapperStrategy = new WrapperActorStrategy( sender.getCurrentStrategy(), cluster )
      val replyActor = actorOf( new ReplyActor( replyAction ) ).start()
      cluster.send( uuid, msg )( replyActor )
    } else throw new IllegalArgumentException( "Only registered actors are allowed to send messages across a cluster" )
  }

  def !(msg: Any)(implicit sender: ActorRef) {
    // even though this is technically feasible, we don't want non-registered actors initiating cluster messages
    if ( sender.isInstanceOf[LocalActorRef] ) {
      val localActorRef = sender.asInstanceOf[LocalActorRef]
      if ( localActorRef.getCurrentStrategy().getCluster == cluster )
        cluster.send( uuid, msg, localActorRef._getUUID )
      else {
        implicit val wrapperStrategy = new WrapperActorStrategy( sender.getCurrentStrategy(), cluster )
        val wrapperActor = actorOf( new WrapperActor( sender ) ).start()
        cluster.send( uuid, msg )( wrapperActor )
      }
    } else throw new IllegalArgumentException( "Only registered actors are allowed to send messages across a cluster")
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

  private[actor] def _become(react: PartialFunction[Any, Unit]) { throw new UnsupportedOperationException() }

  private[actor] def _reply( msg: Any ) { throw new UnsupportedOperationException() }

  // Status is dynamically queried for remote actors.
  // Methods are answered with stale information until new information is returned. 
  @volatile var stopped = false
  @volatile var lastStatusCheck = System.currentTimeMillis()
  
  def checkStatus()( implicit sender: ActorRef ) {
    lastStatusCheck = System.currentTimeMillis()
    implicit val wrapperStrategy = new WrapperActorStrategy( sender.getCurrentStrategy(), cluster )
    val statusActor = actorOf( new StatusActor( this ) ).start()
    cluster.send( uuid.clusterId, StatusRequestClusterMessage( uuid, statusActor.asInstanceOf[LocalActorRef]._getUUID ) )
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
}

final class WrapperActor( actor: ActorRef ) extends RegisteredActor {
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

final class WrapperActorStrategy( strategy: ActorStrategy, cluster: Cluster ) extends ActorStrategy {
  def getCluster: Cluster = cluster

  def setDefaultTimeout(newDefault: Long) { strategy.setDefaultTimeout(newDefault) }

  def getDefaultTimeout(): Long = strategy.getDefaultTimeout()

  def getNextStrategy(): ActorStrategy = strategy.getNextStrategy()

  def enqueue(actor: AbstractActor) { strategy.enqueue( actor ) }

  def send(msg: Any, actor: AbstractActor, sender: ReplyChannel) { strategy.send( msg, actor, sender ) }
}