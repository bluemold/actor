package bluemold.actor

import java.lang.ThreadLocal
import org.bluemold.unsafe.Unsafe
import annotation.tailrec
import java.util.concurrent.Semaphore

object Actor {
  def actorOf( actor: Actor )( implicit strategy: ActorStrategy, parent: ActorRef ): ActorRef = {
    actor match {
      case actor: RegisteredActor => {
        val base = new BaseRegisteredActor( actor, parent )
        base._localRef
      }
      case actor: SupervisedActor => {
        new BaseSupervisedActor( actor, parent )
      }
      case _ => {
        new BaseActor( actor )
      }
    }
  }
  def actorOf( actor: Actor, affinity: ActorRef )( implicit strategy: ActorStrategy, parent: ActorRef ): ActorRef = {
    val actorStrategy = {
      val affinityStrategy = try { affinity.getCurrentStrategy() } catch { case _ => null } // swallow in case of not supported
      if ( affinityStrategy != null ) affinityStrategy else strategy
    }
    actor match {
      case actor: RegisteredActor => {
        val base = new BaseRegisteredActor( actor, parent )( actorStrategy )
        base._localRef
      }
      case actor: SupervisedActor => {
        new BaseSupervisedActor( actor, parent )( actorStrategy )
      }
      case _ => {
        new BaseActor( actor )( actorStrategy )
      }
    }
  }
  def defaultFactory = ActorStrategyFactory.defaultStrategyFactory
  def defaultStrategy = defaultFactory.getStrategy
}
object ActorStrategyFactory {

  @volatile private var _defaultStrategyFactory: ActorStrategyFactory = null

  def setDefaultStrategyFactory( factory: ActorStrategyFactory ) {
    if ( _defaultStrategyFactory == null ) {
      synchronized {
        if ( _defaultStrategyFactory == null ) {
          _defaultStrategyFactory = factory
        }
      }
    }
  }

  implicit def defaultStrategyFactory: ActorStrategyFactory = {
    if ( _defaultStrategyFactory == null ) {
      synchronized {
        if ( _defaultStrategyFactory == null ) {
          _defaultStrategyFactory = new FiberStrategyFactory()
        }
      }
    }
    _defaultStrategyFactory
  }
  
}

object StrategyFactoryClassLoader {
  implicit def wrap( classLoader: ClassLoader ) = StrategyFactoryClassLoader( classLoader )
  implicit def getStrategyFactoryClassLoader = StrategyFactoryClassLoader( Thread.currentThread().getContextClassLoader )
}

case class StrategyFactoryClassLoader( classLoader: ClassLoader )

trait ActorStrategyFactory {
  def getStrategy: ActorStrategy
  def shutdownNow()
  def waitForShutdown()
  def printStats()
}

case class ActorFailure( actor: ActorRef, t: Throwable )

object ActorStrategy {
  implicit def defaultStrategy( implicit strategyFactory: ActorStrategyFactory ): ActorStrategy = strategyFactory.getStrategy
}

trait ActorStrategy {
  def send( msg: Any, actor: AbstractActor, sender: ReplyChannel )
  def enqueue( actor: AbstractActor )
  def getNextStrategy(): ActorStrategy
  def getDefaultTimeout(): Long
  def setDefaultTimeout( newDefault: Long )
  def getCluster: Cluster
}

// TODO: Model ThreadActorRef so threads can interact with actors in a natural way

object ThreadActorRef {
  val mailboxOffset = Unsafe.objectDeclaredFieldOffset( classOf[ThreadActorRef], "mailbox" )
}
final class ThreadActorRef extends ActorRef {
  import ThreadActorRef._

  val sem = new Semaphore(0)
  @volatile var mailbox: List[(Any,ReplyChannel)] = Nil 
  
  private[actor] final def hasMsg: Boolean = ! mailbox.isEmpty

  @tailrec
  private[actor] final def pushMsg( msg: Any, sender: ReplyChannel ) {
    val curMailbox = mailbox
    if ( ! Unsafe.compareAndSwapObject( this, mailboxOffset, curMailbox, (msg,sender) :: curMailbox ) )
      pushMsg( msg, sender )
  }

  @tailrec
  private[actor] final def pushMsgs( msgs: List[(Any,ReplyChannel)] ) {
    val curMailbox = mailbox
    if ( ! Unsafe.compareAndSwapObject( this, mailboxOffset, curMailbox, msgs ::: curMailbox ) )
      pushMsgs( msgs )
  }

  @tailrec
  private[actor] final def popMsg(): (Any,ReplyChannel) = {
    val curMailbox = mailbox
    curMailbox match {
      case head :: tail => {
        if ( ! Unsafe.compareAndSwapObject( this, mailboxOffset, curMailbox, tail ) )
          popMsg()
        else
          head
      }
      case Nil => null
    }
  }

  @tailrec
  private[actor] final def popAllMsg(): List[(Any,ReplyChannel)] = {
    val curMailbox = mailbox
    curMailbox match {
      case Nil => Nil
      case _ => {
        if ( ! Unsafe.compareAndSwapObject( this, mailboxOffset, curMailbox, Nil ) )
          popAllMsg()
        else
          curMailbox
      }
    }
  }

  @tailrec
  def requeue(msgs: List[(Any, ReplyChannel)]) {
    val curMailbox = mailbox
    if ( ! Unsafe.compareAndSwapObject( this, mailboxOffset, curMailbox, curMailbox ::: msgs ) )
      requeue( msgs )
  }

  def requeue(msg: Any, sender: ReplyChannel) {
    pushMsg( msg, sender )
    sem.release()
  }
  
  
  def !(msg: Any)(implicit sender: ActorRef) {
    pushMsg( msg, sender )
    sem.release()
  }

  def ?(msg: Any)(react: PartialFunction[Any, Unit])(implicit sender: ActorRef) {
    throw new UnsupportedOperationException( "Async request to thread is not supported" )
  }

  def !?(msg: Any)(react: PartialFunction[Any, Unit])(implicit sender: ActorRef) {
    throw new UnsupportedOperationException( "Sync request to thread is not supported" )
  }

  def !!(msg: Any)(implicit sender: ActorRef): Future[Any] = {
    throw new UnsupportedOperationException( "Future request to thread is not supported" )
  }

  def forward(msg: Any)(implicit sender: ActorRef) {
    // TODO
  }

  def start()(implicit sender: ActorRef): ActorRef = throw new UnsupportedOperationException( "Can not start a thread actor ref" )

  def stop()(implicit sender: ActorRef) { throw new UnsupportedOperationException( "Can not stop a thread actor ref" ) }

  def issueReply(msg: Any)(implicit sender: ActorRef) {
    pushMsg( msg, sender )
    sem.release()
  }

  @tailrec
  private[actor] def blockOn( replyAction: ReplyAction ) {
    val available = sem.availablePermits()
    sem.acquire( if ( available > 0 ) available else 1 )
    val msgs = popAllMsg()
    var found = false
    for ( (msg,replyChannel) <- msgs ) {
      msg match {
        case replyMsg: ReplyMsg => {
          if ( replyAction == replyMsg.replyAction ) {
            found = true
            replyAction.react( replyMsg.msg )
          }
        }
        case _ => // do nothing
      } 
    }
    if ( ! found )
      blockOn( replyAction )
  }

  private[actor] def currentReplyChannel: ReplyChannel = null

  private[actor] def isBlockingOnAsync: Boolean = true

  private[actor] def doGetNextStrategy(): ActorStrategy = throw new UnsupportedOperationException( "Thread actor ref has no strategy" )

  private[actor] def getCurrentStrategy(): ActorStrategy = throw new UnsupportedOperationException( "Thread actor ref has no strategy" )

  var timeout: Long = 0

  def setTimeout( newTimeout: Long ) { timeout = newTimeout }

  private[actor] def getTimeout(): Long = timeout

  def checkStatus()(implicit sender: ActorRef) {}

  def isPreStart(implicit sender: ActorRef): Boolean = false

  def isActive(implicit sender: ActorRef): Boolean = true

  def isStopped(implicit sender: ActorRef): Boolean = false

  private[actor] def _onTimeout(delay: Long)(body: => Any)(implicit sender: ActorRef) = null

  private[actor] def _requeue(msgs: List[(Any, ReplyChannel)]) {}

  private[actor] def _requeue(msg: Any, sender: ReplyChannel) {}

  private[actor] def _become(react: PartialFunction[Any, Unit]) {}

  private[actor] def _reply( msg: Any ) {}

  private[actor] def doGetParent: ActorRef = null

  private[actor] def doLink(actor: ActorRef) {}

  private[actor] def doUnlink(actor: ActorRef) {}

  private[actor] def doGetChildActors: List[ActorRef] = Nil

  private[actor] def _getUUID: UUID = null
}

trait ReplyChannel {
  def issueReply( msg: Any )(implicit sender: ActorRef)
}

object Future {
  def onAllComplete[T]( futures: List[Future[T]] )( react: PartialFunction[List[T],Unit] )( implicit sender: ActorRef ) {
    onAllComplete0( Nil, futures, react )
  }
  private def onAllComplete0[T]( acc: List[T], futures: List[Future[T]], react: PartialFunction[List[T],Unit] )( implicit sender: ActorRef ) {
    futures match {
      case head :: tail => head ? { case value: T => onAllComplete0( value :: acc, tail, react ) }
      case Nil => react( acc.reverse )
    }
  }
  def blockOnAllComplete[T]( futures: List[Future[T]] )( react: PartialFunction[List[T],Unit] )( implicit sender: ActorRef ) {
    blockOnAllComplete0( Nil, futures, react )
  }
  private def blockOnAllComplete0[T]( acc: List[T], futures: List[Future[T]], react: PartialFunction[List[T],Unit] )( implicit sender: ActorRef ) {
    futures match {
      case head :: tail => head ! { case value: T => blockOnAllComplete0( value :: acc, tail, react ) }
      case Nil => react( acc.reverse )
    }
  }
}
trait Future[+T] {
  def isCompleted: Boolean
  def isExpired: Boolean
  def map[S]( f: (T) => S ): Future[S]
  def to( actor: ActorRef )
  def replyWith()( implicit sender: ActorRef )
  def ?( react: PartialFunction[T,Unit] )( implicit sender: ActorRef )
  def !( react: PartialFunction[T,Unit] )( implicit sender: ActorRef )
}

trait CancelableEvent {
  def cancel(): Boolean
}

trait ActorLike {
  protected def self: ActorRef
  protected def getNextStrategy(): ActorStrategy
  protected def currentStrategy: ActorStrategy
  protected def react: PartialFunction[Any, Unit]
  protected def become( react: PartialFunction[Any, Unit] )
  protected def requeue( msg: Any, sender: ReplyChannel )
  protected def requeue( msgs: List[(Any, ReplyChannel)] )
  protected def onTimeout( delay: Long )( body: => Any )(implicit sender: ActorRef): CancelableEvent

  protected def handleException( t: Throwable )
  final private[actor] def _handleException( t: Throwable ) { handleException( t ) }

  final protected def getCluster: Cluster = currentStrategy.getCluster
}

trait SupervisedActorLike extends ActorLike {
  protected def getParent: ActorRef
  protected def link( actor: ActorRef )
  protected def unlink( actor: ActorRef )
  protected def getChildActors: List[ActorRef]
}

trait RegisteredActorLike extends SupervisedActorLike {
  protected def getUUID: UUID
  protected def getId: String = "none"
  final private[actor] def _getId: String = getId
  final private[actor] def _getClass = getClass
}

trait Actor extends ActorLike {
  private[actor] var _self: ActorRef = _
  final private[actor] def _init() { init() }
  final private[actor] def _react = react
  final protected implicit def self: ActorRef = _self
  final protected implicit def getNextStrategy(): ActorStrategy = _self.doGetNextStrategy()
  final protected def currentStrategy: ActorStrategy = _self.getCurrentStrategy()

  final protected def actorOf( actor: Actor ): ActorRef = Actor.actorOf( actor )( getNextStrategy(), self )

  protected def handleException( t: Throwable ) { t.printStackTrace() }

  protected def init()
  protected def react: PartialFunction[Any, Unit]
  protected def replyChannel: ReplyChannel = _self.currentReplyChannel
  protected def reply( msg: Any ) { _self._reply( msg ) }
  protected def become( react: PartialFunction[Any, Unit] ) { _self._become( react ) }
  protected def requeue( msg: Any, sender: ReplyChannel ) { _self._requeue( msg, sender ) }
  protected def requeue( msgs: List[(Any, ReplyChannel)] ) { _self._requeue( msgs ) }
  protected def onTimeout( delay: Long )( body: => Any )(implicit sender: ActorRef): CancelableEvent =
    _self._onTimeout( delay )( body )( sender )
}
object ActorRef {
  implicit def defaultActorRef: ActorRef = getThreadActorRef

  val threadActorRefs = new ThreadLocal[ThreadActorRef]

  def getThreadActorRef = {
    val threadActorRef = threadActorRefs.get()
    if ( threadActorRef == null ) {
      val newThreadActorRef = new ThreadActorRef
      threadActorRefs.set( newThreadActorRef )
      newThreadActorRef
    } else threadActorRef
  }
}
trait ActorRef extends ReplyChannel {
  def !( msg: Any )( implicit sender: ActorRef )
  def ?( msg: Any )( react: PartialFunction[Any,Unit] )( implicit sender: ActorRef )
  def !?( msg: Any )( react: PartialFunction[Any,Unit] )( implicit sender: ActorRef )
  def !!( msg: Any )( implicit sender: ActorRef ): Future[Any]
  def forward( msg: Any )( implicit sender: ActorRef )

  def start()( implicit sender: ActorRef ): ActorRef
  def stop()( implicit sender: ActorRef )
  def isPreStart( implicit sender: ActorRef ): Boolean
  def isActive( implicit sender: ActorRef ): Boolean
  def isStopped( implicit sender: ActorRef ): Boolean
  def checkStatus()( implicit sender: ActorRef )

  private[actor] def getTimeout(): Long
  private[actor] def getCurrentStrategy(): ActorStrategy
  private[actor] def doGetNextStrategy(): ActorStrategy
  private[actor] def isBlockingOnAsync: Boolean
  private[actor] def blockOn( replyAction: ReplyAction ) 
  private[actor] def currentReplyChannel: ReplyChannel 

  private[actor] def doGetParent: ActorRef
  private[actor] def doLink( actor: ActorRef )
  private[actor] def doUnlink( actor: ActorRef )
  private[actor] def doGetChildActors: List[ActorRef]
  
  private[actor] def _reply( msg: Any )
  private[actor] def _become( react: PartialFunction[Any, Unit] )
  private[actor] def _requeue( msg: Any, sender: ReplyChannel )
  private[actor] def _requeue( msgs: List[(Any, ReplyChannel)] )
  private[actor] def _onTimeout( delay: Long )( body: => Any )(implicit sender: ActorRef): CancelableEvent

  private[actor] def _getUUID: UUID

  override def equals(obj: AnyRef) = obj match {
    case ref: ActorRef => ( this eq ref ) || ( _getUUID != null && _getUUID == ref._getUUID )
    case _ => false
  }
}

trait SupervisedActor extends Actor with SupervisedActorLike {
  protected def getParent: ActorRef = self.doGetParent
  protected def getChildActors: List[ActorRef] = self.doGetChildActors
  protected def unlink(actor: ActorRef) { self.doUnlink( actor ) }
  protected def link(actor: ActorRef) { self.doLink( actor ) }
}

trait RegisteredActor extends SupervisedActor with RegisteredActorLike {
  protected def getUUID: UUID = self._getUUID
}

trait WithTimeout extends ActorLike {
  var timeout = currentStrategy.getDefaultTimeout()
  def getTimeout = timeout
  def setTimeout( newTimeout: Long ) { timeout = newTimeout }
}
