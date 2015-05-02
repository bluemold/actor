package bluemold.actor

import org.bluemold.unsafe.Unsafe
import annotation.tailrec
import java.util.{Timer, TimerTask}
import java.lang.{RuntimeException, Throwable}
import java.io._

case object StartMsg
case object StopMsg
private[actor] object AbstractActor {
  import org.bluemold.unsafe.Unsafe._

  val queueCountOffset = objectDeclaredFieldOffset( classOf[AbstractActor], "queueCount" )
  val threadOffset = objectDeclaredFieldOffset( classOf[AbstractActor], "thread" )
  val mailboxOffset = objectDeclaredFieldOffset( classOf[AbstractActor], "mailbox" )
  
  val timer = new Timer( true )

  val answerOffset = objectDeclaredFieldOffset( classOf[AbstractFuture], "answer" )
  val actionsOffset = objectDeclaredFieldOffset( classOf[AbstractFuture], "actions" )
  val expiringActionOffset = objectDeclaredFieldOffset( classOf[ExpiringReplyAction], "answer" )
  val expiringBlockingActionOffset = objectDeclaredFieldOffset( classOf[ExpiringBlockingReplyAction], "answer" )

  val emptyBehavior: PartialFunction[Any,Unit] = { case _ => }
}
abstract class AbstractActor extends ActorLike {
  import AbstractActor._
  // makes react accessible to 
  private[actor] final var behavior: PartialFunction[Any,Unit] = _
  // current sender, future, or promise, or maybe thread local
  final var sender: ReplyChannel = null
  final var _stopped: Boolean = _
  final var postSender: ReplyChannel = null 
  final var postMsg: Any = null
  final var postbox: List[(Any,ReplyChannel)] = Nil 
  final var _tailMessaging: Boolean = _

  // @volatile var mailbox = new ConcurrentLinkedQueue[Any] 
  @volatile var mailbox: List[(Any,ReplyChannel)] = Nil 
  @volatile var queueCount: Int = -1 // doubles as status ( -1 means waiting to start, -2 means stopped ), used by most strategies
  @volatile var thread: Thread = null // used by some strategies

  protected implicit def self: ActorRef // could be handled as thread local
  def _isTailMessaging = self.isTailMessaging // used by some strategies

  @tailrec
  private[actor] final def incQueueCount() {
    val current = queueCount
    if ( current >= 0 ) {
      if ( ! Unsafe.compareAndSwapInt( this, queueCountOffset, current, current + 1 ) )
        incQueueCount()
    }
  }

  @tailrec
  private[actor] final def decQueueCount() {
    val current = queueCount
    if ( current >= 0 ) {
      if ( current == 0 )
        throw new RuntimeException( "What Happened!" )
      if ( ! Unsafe.compareAndSwapInt( this, queueCountOffset, current, current - 1 ) )
        decQueueCount()
    }
  }

  private[actor] final def gainControl( thread: Thread ): Boolean = {
    Unsafe.compareAndSwapObject( this, threadOffset, null, thread )
  }

  private[actor] final def releaseControl( thread: Thread ): Boolean = {
    Unsafe.compareAndSwapObject( this, threadOffset, thread, null )
  }
  
  final private[actor] def _start()(implicit sender: ActorRef): ActorRef = {
    if ( Unsafe.compareAndSwapInt( this, queueCountOffset, -1, 0 ) )
      this.!(StartMsg)(sender)
    self
  }
  
  final private[actor] def _stop()(implicit sender: ActorRef) {
    queueCount = -2
  }

  def stop()(implicit sender: ActorRef) { _stop()(sender) }

  def start()(implicit sender: ActorRef): ActorRef = _start()(sender)

  @inline
  private final def ensureContext( msg: Any, sender: ActorRef ): Any = 
    if ( sender.isInstanceOf[LocalActorRef] || sender.isInstanceOf[AbstractActor] ) {
      val senderClassLoader = sender.getCurrentStrategy().getClassLoader
      val classLoader = currentStrategy.getClassLoader
      if ( senderClassLoader != classLoader ) {
        Node.forSerialization.set( getNode )
        try {
          val out = new ByteArrayOutputStream
          val objectOut = new ObjectOutputStream( out )
          objectOut.writeObject( msg )
          objectOut.flush()
          new ObjectInputStream( new ByteArrayInputStream( out.toByteArray ) ) {
            override def resolveClass( desc: ObjectStreamClass ) = Class.forName( desc.getName, false, classLoader )
          }.readObject()
        } finally {
          Node.forSerialization.remove()
        }
      } else msg
    } else msg

  final def !( msg: Any )( implicit sender: ActorRef ) {
    if ( sender.getCurrentStrategy() ne currentStrategy )
      currentStrategy.send( ensureContext( msg, sender ), this, sender );
    else currentStrategy.send( msg, this, sender );
  }
  final def ?(msg: Any)(react: PartialFunction[Any, Unit])(implicit sender: ActorRef) {
    val replyAction = if ( sender.isBlockingOnAsync ) {
      val blockingReplyAction = new BlockingReplyAction( sender, react, sender.currentReplyChannel )
      blockingReplyAction
    } else new ReplyAction( sender, react, sender.currentReplyChannel ) 
    if ( sender.getCurrentStrategy() ne currentStrategy )
      currentStrategy.send( ensureContext( msg, sender ), this, replyAction );
    else currentStrategy.send( msg, this, replyAction );
    if ( sender.isBlockingOnAsync )
      sender.blockOn( replyAction )
  }
  final def !?(msg: Any)(react: PartialFunction[Any, Unit])(implicit sender: ActorRef) {
    val replyAction = new BlockingReplyAction( sender, react, sender.currentReplyChannel )
    if ( sender.getCurrentStrategy() ne currentStrategy )
      currentStrategy.send( ensureContext( msg, sender ), this, replyAction );
    else currentStrategy.send( msg, this, replyAction );
    sender.blockOn( replyAction )
  }
  final def !!(msg: Any)(implicit sender: ActorRef): Future[Any] = {
    val future = new AbstractFuture
    future.setExpiration( sender.getTimeout() )
    if ( sender.getCurrentStrategy() ne currentStrategy )
      currentStrategy.send( ensureContext( msg, sender ), this, future );
    else currentStrategy.send( msg, this, future );
    future
  }
  final def forward(msg: Any)(implicit sender: ActorRef) {
    if ( sender.getCurrentStrategy() ne currentStrategy )
      currentStrategy.send( ensureContext( msg, sender ), this, sender.currentReplyChannel );
    else currentStrategy.send( msg, this, sender.currentReplyChannel );
  }

  @inline
  final def reply( msg: Any ) {
    sender.issueReply(msg)(self)
  }
  final def issueReply( msg: Any )(implicit sender: ActorRef) {
    if ( sender.getCurrentStrategy() ne currentStrategy )
      currentStrategy.send( ensureContext( msg, sender ), this, sender );
    else currentStrategy.send( msg, this, sender );
  }
  private[actor] def currentReplyChannel: ReplyChannel = sender
  private[actor] def isBlockingOnAsync: Boolean = false
  private[actor] def blockOn( replyAction: ReplyAction ) {
    val existingBehavior = behavior
    var msgs: List[(Any, ReplyChannel)] = Nil 
    behavior = {
      case replyMsg: ReplyMsg => {
        if ( replyAction == replyMsg.replyAction ) {
          requeue( msgs )
          behavior = existingBehavior
          sender = replyAction.replyChannel
          replyAction.react( replyMsg.msg )
        } else msgs ::= (( replyMsg, sender ))
      }
      case msg: Any => msgs ::= (( msg, sender ))
    }
  }

  private[actor] final def pushAndEnqueueIfNeeded( msg: Any, sender: ReplyChannel ) {
    var curMailbox = mailbox
    while ( ! Unsafe.compareAndSwapObject( this, mailboxOffset, curMailbox, (msg,sender) :: curMailbox ) )
      curMailbox = mailbox
    if ( queueCount == 0 ) {
      var current = queueCount
      if ( current >= 0 ) {
        while ( ! Unsafe.compareAndSwapInt( this, queueCountOffset, current, current + 1 ) )
          current = queueCount
      }
      currentStrategy.enqueue( this )
    }
  }
  private[actor] final def enqueueIfNeeded() {
    if ( queueCount == 0 ) {
      var current = queueCount
      if ( current >= 0 ) {
        while ( ! Unsafe.compareAndSwapInt( this, queueCountOffset, current, current + 1 ) )
          current = queueCount
      }
      currentStrategy.enqueue( this )
    }
  }

  private[actor] final def _isPreStart = queueCount == -1 
  private[actor] final def _isActive = queueCount >= 0
  private[actor] final def _isStopped = queueCount == -2

  final def checkStatus()(implicit sender: ActorRef) {}
  final def isPreStart(implicit sender: ActorRef) = _isPreStart 
  final def isActive(implicit sender: ActorRef) = _isActive
  final def isStopped(implicit sender: ActorRef) = _isStopped


  protected implicit final def getNextStrategy() = currentStrategy.getNextStrategy()
  
  private[actor] final def _init() {
    _tailMessaging = _isTailMessaging
    init()
  }
  
  @inline
  private[actor] final def _behavior( msg: Any ) {
    if ( behavior == null ) {
      _tailMessaging = _isTailMessaging
      init()
      val initialBehavior = react
      if ( initialBehavior == null )
        behavior = emptyBehavior
      else behavior = initialBehavior
    }
    if ( StopMsg == msg ) {
      _stopped = true
      queueCount = -2
    } else if ( StartMsg != msg ) staticBehavior( msg ) 
  }
  
  protected def staticBehavior( msg: Any ) { behavior( msg ) }

  protected def init()

  // respond to the next message with this react
  protected def become ( react: PartialFunction[Any, Unit] ) { this.behavior = react }

  // message box methods, leave implementation to specific type of actor
  private[actor] def doGetParent: ActorRef = null
  private[actor] def doLink( actor: ActorRef ) {}
  private[actor] def doUnlink( actor: ActorRef ) {}
  private[actor] def doGetChildActors: List[ActorRef] = Nil

  private[actor] def _getUUID: UUID = null

  private[actor] def doGetNextStrategy(): ActorStrategy = getNextStrategy()

  private[actor] def getCurrentStrategy(): ActorStrategy = currentStrategy

  private[actor] def getTimeout(): Long = {
    if ( this.isInstanceOf[WithTimeout] ) {
      this.asInstanceOf[WithTimeout].getTimeout
    } else 0
  }

  protected def handleException( t: Throwable ) { t.printStackTrace() }

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
    if ( curMailbox.isEmpty ) Nil
    else if ( ! Unsafe.compareAndSwapObject( this, mailboxOffset, curMailbox, Nil ) ) popAllMsg()
    else curMailbox
  }

  @tailrec
  protected final def requeue(msgs: List[(Any, ReplyChannel)]) {
    val curMailbox = mailbox
    if ( ! Unsafe.compareAndSwapObject( this, mailboxOffset, curMailbox, curMailbox ::: msgs ) )
      requeue( msgs )
  }

  protected final def requeue(msg: Any, sender: ReplyChannel) {
    pushMsg( msg, sender )
  }

  protected final def onTimeout( delay: Long )( body: => Any )(implicit sender: ActorRef): CancelableEvent = {
    if ( sender == self ) {
      if ( ! sender.isBlockingOnAsync ) {
        val react: PartialFunction[Any, Unit] = { case _ => body }
        new DelayedActorReply( delay, Nil, new ReplyAction( sender, react, sender.currentReplyChannel ), sender )
      } else throw new RuntimeException( "On timeout is for non-blocking actors only" )
    } else throw new RuntimeException( "Only the actor can call a timeout on itself" )
  }

  private[actor] def _reply( msg: Any ) { reply( msg ) }
  private[actor] def _become( react: PartialFunction[Any, Unit] ) { become( react ) }
  private[actor] def _requeue( msg: Any, sender: ReplyChannel ) { requeue( msg, sender ) }
  private[actor] def _requeue( msgs: List[(Any, ReplyChannel)] ) { requeue( msgs ) }
  private[actor] def _onTimeout( delay: Long )( body: => Any )(implicit sender: ActorRef): CancelableEvent =
    onTimeout( delay )( body )( sender )
}

abstract class AbstractSupervisedActor extends AbstractActor with SupervisedActorLike {
  var childActors: List[ActorRef] = Nil

  @tailrec
  private final def removeChildActor( dst: List[ActorRef], src: List[ActorRef], target: ActorRef ): List[ActorRef] = {
    src match {
      case head :: tail => {
        if ( head != target ) removeChildActor( head :: dst, tail, target )
        else removeChildActor( dst, tail, target )
      }
      case Nil => dst
    }
  }

  protected final def link( actor: ActorRef ) { if ( ! childActors.contains( actor ) ) childActors ::= actor }
  protected final def unlink( actor: ActorRef ) { if ( childActors.contains( actor ) ) childActors = removeChildActor( Nil, childActors, actor ) }
  protected final def getChildActors: List[ActorRef] = childActors
  override private[actor] def doLink( actor: ActorRef ) { link( actor ) }
  override private[actor] def doUnlink( actor: ActorRef ) { unlink( actor ) }
  override private[actor] def doGetChildActors: List[ActorRef] = getChildActors
}

final class DelayedActorReply( delay: Long, msg: Any, replyChannel: ReplyChannel, sender: ActorRef ) extends TimerTask with CancelableEvent {
  import AbstractActor._

  if ( delay > 0 )
    timer.schedule( this, delay )
  else run()

  def run() {
    replyChannel.issueReply( msg )( sender )
  }
}

trait BlockingReply
final class ReplyMsg( _msg: Any, _replyAction: ReplyAction ) {
  def replyAction = _replyAction
  def msg = _msg
}
class ReplyAction( _actee: ActorRef, _react: PartialFunction[Any, Unit], _acteeReplyChannel: ReplyChannel ) extends ReplyChannel {
  final def _getActee = _actee
  final def react = _react
  final def replyChannel = _acteeReplyChannel
  def issueReply(msg: Any)(implicit sender: ActorRef) {
    _actee ! new ReplyMsg( msg, this )
  }
}
final class BlockingReplyAction( __actee: ActorRef, __react: PartialFunction[Any, Unit], __acteeReplyChannel: ReplyChannel )
        extends ReplyAction( __actee, __react, __acteeReplyChannel ) with BlockingReply
final class ExpiringReplyAction( __actee: ActorRef, __react: PartialFunction[Any, Unit], __acteeReplyChannel: ReplyChannel )
        extends ReplyAction( __actee, __react, __acteeReplyChannel ) {
  import AbstractActor._

  @volatile
  var answer: Any = null

  setExpiration( __actee.getTimeout() )

  def setExpiration( delay: Long ) {
    if ( delay > 0 )
      timer.schedule( new Timeout(), delay ) 
  }

  override def issueReply(msg: Any)(implicit sender: ActorRef) {
    val success = Unsafe.compareAndSwapObject( ExpiringReplyAction.this, expiringActionOffset, null, msg )
    if ( success ) {
      __actee ! new ReplyMsg( msg, this )
    }
  }

  class Timeout extends TimerTask {
    def run() {
      val exception = new Throwable( "Future timed out!" )
      val timedOut = Unsafe.compareAndSwapObject( ExpiringReplyAction.this, expiringActionOffset, null, exception )
      if ( timedOut ) {
        __actee ! new ReplyMsg( exception, ExpiringReplyAction.this )
      }
    }
  }
}
final class ExpiringBlockingReplyAction( __actee: ActorRef, __react: PartialFunction[Any, Unit], __acteeReplyChannel: ReplyChannel )
        extends  ReplyAction( __actee, __react, __acteeReplyChannel ) with BlockingReply {
  import AbstractActor._

  @volatile
  var answer: Any = null

  setExpiration( __actee.getTimeout() )

  def setExpiration( delay: Long ) {
    if ( delay > 0 )
      timer.schedule( new Timeout(), delay ) 
  }

  override def issueReply(msg: Any)(implicit sender: ActorRef) {
    val success = Unsafe.compareAndSwapObject( ExpiringBlockingReplyAction.this, expiringBlockingActionOffset, null, msg )
    if ( success ) {
      __actee ! new ReplyMsg( msg, this )
    }
  }

  class Timeout extends TimerTask {
    def run() {
      val exception = new Throwable( "Future timed out!" )
      val timedOut = Unsafe.compareAndSwapObject( ExpiringBlockingReplyAction.this, expiringBlockingActionOffset, null, exception )
      if ( timedOut ) {
        __actee ! new ReplyMsg( exception, ExpiringBlockingReplyAction.this )
      }
    }
  }
}

trait FutureWithAddReplyChannel[+T] extends Future[T] {
  def addReplyChannel( channel: ReplyChannel )
}

final class AbstractFuture extends FutureWithAddReplyChannel[Any] with ReplyChannel {
  import AbstractActor._

  @volatile
  var answer: (Any,ActorRef) = null
  @volatile
  var actions: List[ReplyChannel] = Nil

  def setExpiration( delay: Long ) {
    if ( delay > 0 )
      timer.schedule( new Timeout(), delay ) 
  }

  def issueReply( msg: Any )( implicit sender: ActorRef ) {
    val success = Unsafe.compareAndSwapObject( AbstractFuture.this, answerOffset, null, (( msg, sender )) )
    if ( success ) {
      val actions = popActions()
      if ( actions != null )
        issueReply( msg, actions )
    }
  }

  @tailrec
  private def issueReply(msg: Any, actions: List[ReplyChannel] )(implicit sender: ActorRef) {
    actions match {
      case head :: tail => {
        head.issueReply( msg )
        issueReply( msg, tail )
      }
      case Nil => // done
    }
  }

  @tailrec
  private def queueAction( replyAction: ReplyChannel ): Boolean = {
    val currentActions = actions
    if ( currentActions != null ) {
      if ( ! Unsafe.compareAndSwapObject( AbstractFuture.this, actionsOffset, currentActions, replyAction :: currentActions ) )
        queueAction( replyAction )
      else true
    } else false
  } 

  @tailrec
  private def popActions(): List[ReplyChannel] = {
    val currentActions = actions
    if ( currentActions != null && ! Unsafe.compareAndSwapObject( AbstractFuture.this, actionsOffset, currentActions, null ) )
        popActions()
    else currentActions
  }

  def !( react: PartialFunction[Any, Unit] )(implicit sender: ActorRef) {
    val replyAction = new BlockingReplyAction( sender, react, sender.currentReplyChannel )
    queueHelper( replyAction )
    sender.blockOn( replyAction )
  }

  def ?( react: PartialFunction[Any, Unit] )(implicit sender: ActorRef) {
    val replyAction = if ( sender.isBlockingOnAsync ) {
      val blockingReplyAction = new BlockingReplyAction( sender, react, sender.currentReplyChannel )
      blockingReplyAction
    } else new ReplyAction( sender, react, sender.currentReplyChannel ) 
    queueHelper( replyAction )
    if ( sender.isBlockingOnAsync )
      sender.blockOn( replyAction )
  }
  
  @tailrec
  def queueHelper( replyAction: ReplyAction )(implicit sender: ActorRef) {
    val currentAnswer = answer
    if ( currentAnswer == null ) {
      if ( ! queueAction( replyAction ) ) queueHelper( replyAction )
    } else {
      sender.!( new ReplyMsg( currentAnswer._1, replyAction ) )( currentAnswer._2 )
    }
  }

  def replyWith()(implicit sender: ActorRef) {
    val replyChannel = sender.currentReplyChannel
    val currentAnswer = answer
    if ( currentAnswer == null ) {
      if ( ! queueAction( replyChannel ) ) replyWith()
    } else {
      replyChannel.issueReply( currentAnswer._1 )( currentAnswer._2 )
    }
  }

  def addReplyChannel( channel: ReplyChannel ) {
    val currentAnswer = answer
    if ( currentAnswer == null ) {
      if ( ! queueAction( channel ) ) addReplyChannel( channel )
    } else {
      channel.issueReply( currentAnswer._1 )( currentAnswer._2 )
    }
  }

  def to( actor: ActorRef ) {
    val currentAnswer = answer
    if ( currentAnswer == null ) {
      if ( ! queueAction( actor ) ) to( actor )
    } else {
      actor.issueReply( currentAnswer._1 )( currentAnswer._2 )
    }
  }

  def map[S](f: ( Any ) => S): Future[S] = new ChildFuture[S,Any]( this, f )

  def isExpired: Boolean = answer match {
    case ( t: Throwable, null ) => true
    case _ => false
  }

  def isCompleted: Boolean = answer match {
    case ( t: Throwable, null ) => false
    case ( msg: Any, sender: ActorRef ) => true
    case _ => false
  }
  
  class Timeout extends TimerTask {
    def run() {
      val exception = new Throwable( "Future timed out!" )
      val timedOut = Unsafe.compareAndSwapObject( AbstractFuture.this, answerOffset, null, (( exception, null )) )
      if ( timedOut ) {
        val actions = popActions()
        if ( actions != null )
          issueReply( exception, actions )
      }
    }
  }
}

final class ChildFuture[+T,U]( parent: FutureWithAddReplyChannel[U], g: (U) => T ) extends FutureWithAddReplyChannel[T] {
  def !(react: PartialFunction[T, Unit])(implicit sender: ActorRef) {
    parent ! { case msg => react( g( msg ) ) }
  }

  def ?(react: PartialFunction[T, Unit])(implicit sender: ActorRef) {
    parent ? { case msg => react( g( msg ) ) }
  }

  def replyWith()(implicit sender: ActorRef) {
    parent.addReplyChannel( new ReplyChannelModifier( g, sender.currentReplyChannel ) )
  }

  def to(actor: ActorRef) {
    parent.addReplyChannel( new ReplyChannelModifier( g, actor ) )
  }

  def map[S](f: ( T ) => S): Future[S] = new ChildFuture[S,T]( this, f )

  def isExpired: Boolean = parent.isExpired

  def isCompleted: Boolean = parent.isCompleted

  def addReplyChannel(channel: ReplyChannel) {
    parent.addReplyChannel( new ReplyChannelModifier( g, channel ) )
  }
}

class ReplyChannelModifier[T,S]( f: ( T ) => S, channel: ReplyChannel ) extends ReplyChannel {
  def issueReply(msg: Any)(implicit sender: ActorRef) {
    msg match {
      case t: T => channel.issueReply( f( t ) )
    }
  }
}