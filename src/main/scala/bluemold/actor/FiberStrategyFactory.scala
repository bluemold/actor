package bluemold.actor

import annotation.tailrec
import bluemold.concurrent.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{TimeUnit, CountDownLatch}
import org.bluemold.unsafe.Unsafe

/**
 * FiberStrategyFactory<br/>
 * Author: Neil Essy<br/>
 * Created: 5/20/11<br/>
 * <p/>
 * [Description]
 */
object FiberStrategyFactory {
  val useDaemon = AtomicBoolean.create( true )
  def setDaemon( choice: Boolean ) { useDaemon.set( choice ) }
}
class FiberStrategyFactory( implicit node: Node, sfClassLoader: StrategyFactoryClassLoader ) extends ActorStrategyFactory {
  import FiberStrategyFactory._
  val concurrency = Runtime.getRuntime.availableProcessors()
  val waitTime = 1 // milliseconds
  val maxConsecutiveEmptyLoops = 100
  val maxSameThreadDepth = 100
  val dropDownDepth = 75
  val maxSameThreadWidth = 2000
  val maxAffinity = 50
  val nextStrategy = AtomicInteger.create() 

  val queueOffset = Unsafe.objectDeclaredFieldOffset( classOf[SimpleStrategy], "queue" )
  
  val threads: Array[SimpleStrategy] = {
    var index = -1
    Array.fill[SimpleStrategy](concurrency)( { index += 1; new SimpleStrategy( index ).start() } )
  }

  def printStats() {
    threads foreach { _.printStats() }
  }
  
  def shutdownNow() {
    threads foreach { _.done.set( true ) }
  }

  def shutdownWhenDepleted() {
    threads foreach { _.doneOnWait.set( true ) } 
  }

  def shutdownWhenAllDepleted() {
    threads foreach { _.doneOnAllWait.set( true ) }
  }

  def waitForShutdown() {
    threads foreach { _.thread.join() }
  }

  def getStrategy: ActorStrategy = {
    val strategy = threads( nextStrategy.getAndModIncrement( concurrency ) )
    strategy.actorCount.incrementAndGet();
    strategy
  }

  final class SimpleStrategy( index: Int ) extends ActorStrategy with Runnable {
    val done = AtomicBoolean.create()
    val doneOnWait = AtomicBoolean.create()
    val doneOnAllWait = AtomicBoolean.create()
    val thread = new Thread( this, "FiberStrategy-" + index )
    thread.setContextClassLoader( sfClassLoader.classLoader )
    thread.setDaemon( useDaemon.get() )
    thread.setPriority( Thread.NORM_PRIORITY )
    val actorCount = AtomicInteger.create()

    var affinity: Int = 0
    var nextStrategy: Int = 0
    var consecutiveEmptyLoops = 0
    var sameThreadDepth = 0
    var sameThreadWidth = 0
/*
    var postCount = 0L
    var postBoxCount = 0L
    var queuedCount = 0L
    var postToQueuedCount = 0L
*/

    @volatile var waiting: CountDownLatch = null
    @volatile var queue: List[AbstractActor] = Nil

    def printStats() {
      println( "Strategy #" + index )
      println( "Actors: " + actorCount.get() )
/*
      println( "PostSent: " + postCount )
      println( "PostBoxed: " + postBoxCount )
      println( "Queued: " + queuedCount )
      println( "PostBoxToQueued: " + postToQueuedCount )
*/
    }
  
    def queueIsEmpty = queue.isEmpty

    @tailrec
    def queueActor( actor: AbstractActor ) {
      val curQueue = queue
      if ( ! Unsafe.compareAndSwapObject( this, queueOffset, curQueue, actor :: curQueue ) )
        queueActor( actor )
    }

    @tailrec
    def pollEntireQueue(): List[AbstractActor] = {
      val curQueue = queue
      curQueue match {
        case Nil => Nil
        case _ => {
          if ( ! Unsafe.compareAndSwapObject( this, queueOffset, curQueue, Nil ) )
            pollEntireQueue()
          else
            curQueue
        }
      }
    }

    override def run() {
      nextStrategy = index
      loop()
    }

    @tailrec
    private def processActors( actors: List[AbstractActor] ) {
      actors match {
        case actor :: tail => {
          consecutiveEmptyLoops = 0
          sameThreadDepth = 0
          sameThreadWidth = 0

          // process actors messages
          var msgs = actor.popAllMsg()
          while ( msgs ne Nil ) {
            // inline of processMsgs( msgs, actor )
            sameThreadWidth += 1
            val (msg,sender) = msgs.head
            if ( ! actor._stopped ) { // isActive
              // inline of processMsg( msg, actor, sender)
              try {
                actor.sender = sender
                msg match {
                  case replyMsg: ReplyMsg => {
                    val replyAction = replyMsg.replyAction
                    if ( replyAction.isInstanceOf[BlockingReply] )
                      actor._behavior( replyMsg )
                    else {
                      actor.sender = replyAction.replyChannel
                      replyAction.react( replyMsg.msg )
                    }
                  }
                  case anyMsg => actor._behavior( anyMsg )
                }
              }
              catch { case t: Exception => actor._handleException( t ) }
              // end inline of processMsg( msg, actor, sender)
              // now check and process postSender
              if ( actor.postSender ne null ) {
                sameThreadWidth += 1
                try {
                  actor.sender = actor.postSender
                  val pMsg = actor.postMsg
                  actor.postSender = null
                  actor.postMsg = null
                  pMsg match {
                    case replyMsg: ReplyMsg => {
                      val replyAction = replyMsg.replyAction
                      if ( replyAction.isInstanceOf[BlockingReply] )
                        actor._behavior( replyMsg )
                      else {
                        actor.sender = replyAction.replyChannel
                        replyAction.react( replyMsg.msg )
                      }
                    }
                    case anyMsg => actor._behavior( anyMsg )
                  }
                }
                catch { case t: Exception => actor._handleException( t ) }
              }
              // now check and process postbox
              var postbox = actor.postbox
              while ( postbox ne Nil ) {
                actor.postbox = Nil
                while ( postbox ne Nil ) {
                  sameThreadWidth += 1
                  val (pMsg,pReplyChannel) = postbox.head
                  postbox = postbox.tail
                  try {
                    actor.sender = pReplyChannel
                    pMsg match {
                      case replyMsg: ReplyMsg => {
                        val replyAction = replyMsg.replyAction
                        if ( replyAction.isInstanceOf[BlockingReply] )
                          actor._behavior( replyMsg )
                        else {
                          actor.sender = replyAction.replyChannel
                          replyAction.react( replyMsg.msg )
                        }
                      }
                      case anyMsg => actor._behavior( anyMsg )
                    }
                  }
                  catch { case t: Exception => actor._handleException( t ) }
                }
                postbox = actor.postbox
                if ( postbox ne Nil ) {
                  actor.postbox = Nil
                  if( sameThreadWidth >= maxSameThreadWidth ) {
                    postbox foreach { ms => actor.pushMsg( ms._1, ms._2 )/*; postToQueuedCount += 1*/ }
                    postbox = Nil
                  }
                }
              }
              // end postbox processing
              actor.sender = null
            }
            msgs = msgs.tail
            // end inline of processMsgs( msgs, actor )
            if ( ( msgs eq Nil ) && sameThreadWidth < maxSameThreadWidth && actor.hasMsg )
              msgs = actor.popAllMsg()
          }

          // Queuing logic
          if ( ! actor._stopped ) { // isActive
            if ( ! actor.hasMsg ) {
              // No more messages in the mailbox, don't re-queue
              actor.decQueueCount()
              // double check the need for queuing, always the last thing
              if ( actor.hasMsg )
                actor.enqueueIfNeeded()
            } else if ( actor.queueCount > 1 ) {
              // It's already queued, don't re-queue
              actor.decQueueCount()
              // double check the need for queuing, always the last thing
              if ( actor.hasMsg )
                actor.enqueueIfNeeded()
            } else {
              // Re-queue actor
              queueActor( actor )
            }
          }
          
          // re-curse
          processActors( tail )
        }
        case Nil => // end of the line
      }
    }

    @tailrec
    private def loop() {
      pollEntireQueue() match {
        case Nil => {
          consecutiveEmptyLoops += 1
          if ( consecutiveEmptyLoops >= maxConsecutiveEmptyLoops ) {
            consecutiveEmptyLoops -= 1
            hibernate()
          }
        }
        case actors => processActors( actors )
      }
      if ( ! done.get() )
        loop()
    }

    def hibernate() {
      waiting = new CountDownLatch(1)
      if ( queueIsEmpty ) {
        if ( doneOnWait.get() ) {
          done.set( true )
        } else if ( doneOnAllWait.get() ) {
          if ( threads.forall( ( strategy: SimpleStrategy ) => strategy.waiting != null || strategy.done.get() ) )
            done.set( true )
        }  
        if ( ! done.get() ) {
          try {
            waiting.await( waitTime, TimeUnit.MILLISECONDS )
          } catch {
            case t: InterruptedException => 
          }
        }
      }
      waiting = null
    }

    def enqueue( actor: AbstractActor ) {
      queueActor( actor )
      val waiting = this.waiting
      if ( waiting != null )
        waiting.countDown()
    }

    def nextStrategyIndex(): Int = {
      val nextStrategy = this.nextStrategy
      if ( nextStrategy == index ) {
        affinity += 1
        if ( affinity >= maxAffinity ) {
          affinity = 0
          this.nextStrategy = ( nextStrategy + 1 ) % concurrency
        }
      } else this.nextStrategy = ( nextStrategy + 1 ) % concurrency
      nextStrategy
    }

    def getNode: Node = node

    def getClassLoader = sfClassLoader.classLoader

    def getNextStrategy(): ActorStrategy = { val strategy = threads( nextStrategyIndex() ); strategy.actorCount.incrementAndGet(); strategy }
    def getNextBalancedStrategy(): ActorStrategy = getStrategy

    var defaultTimeout: Long = 60000 // milliseconds

    def setDefaultTimeout(newDefault: Long) { defaultTimeout = newDefault }

    def getDefaultTimeout(): Long = defaultTimeout

    def start(): SimpleStrategy = { nextStrategy = index; thread.start(); this }

/*
    @tailrec
    def processMsgs( msgs: List[(Any,ReplyChannel)], actor: AbstractActor ) {
      msgs match {
        case (msg,sender) :: tail => {
          if ( actor._isActive ) {
            processMsg( msg, actor, sender)
            processMsgs( tail, actor )
          }
        }
        case Nil => // do nothing
      }
    }

    def processMsg( msg: Any, actor: AbstractActor, sender: ReplyChannel ) {
      try {
        actor.sender = sender
        msg match {
          case replyMsg: ReplyMsg => {
            val replyAction = replyMsg.replyAction
            if ( replyAction.isInstanceOf[BlockingReply] )
              actor._behavior( replyMsg )
            else {
              actor.sender = replyAction.replyChannel
              replyAction.react( replyMsg.msg )
            }
          }
          case plainMsg => actor._behavior( plainMsg )
        }
      }
      catch { case t: Exception => actor._handleException( t ) }
      finally { actor.sender = null }
    }
*/

    def send( msg: Any, actor: AbstractActor, sender: ReplyChannel ) {
      if ( Thread.currentThread() ne thread ) {
        if ( actor.queueCount >= 0 ) { // isActive
          actor.pushAndEnqueueIfNeeded( msg, sender )
//          queuedCount += 1
        }
      } else if ( sameThreadDepth > maxSameThreadDepth ) {
        if ( ! actor._stopped ) { // isActive
          if ( sameThreadWidth <= maxSameThreadWidth && ( actor.sender ne null ) ) {
            if ( actor.postSender eq null ) {
              actor.postSender = sender
              actor.postMsg = msg
//              postCount += 1
            } else {
              actor.postbox ::= (msg,sender)
//              postBoxCount += 1
            }
          } else {
            actor.pushAndEnqueueIfNeeded( msg, sender )
//            queuedCount += 1
          }
        }
      } else if ( sameThreadWidth > maxSameThreadWidth ) {
        if ( ! actor._stopped ) { // isActive
          actor.pushAndEnqueueIfNeeded( msg, sender )
//          queuedCount += 1
        }
      } else {
        if ( ! actor._stopped ) { // isActive
          val oldSender = actor.sender
          if ( ( oldSender eq null ) || ( sameThreadDepth <= dropDownDepth && actor._tailMessaging ) ) {
            sameThreadDepth += 1
            sameThreadWidth += 1
            // inline of processMsg( msg, actor, sender )
            try {
              actor.sender = sender
              msg match {
                case replyMsg: ReplyMsg => {
                  val replyAction = replyMsg.replyAction
                  if ( replyAction.isInstanceOf[BlockingReply] )
                    actor._behavior( replyMsg )
                  else {
                    actor.sender = replyAction.replyChannel
                    replyAction.react( replyMsg.msg )
                  }
                }
                case anyMsg => actor._behavior( anyMsg )
              }
            }
            catch { case t: Exception => actor._handleException( t ) }
            // end inline of processMsg( msg, actor, sender )
            // now check and process postSender
            if ( actor.postSender ne null ) {
              sameThreadWidth += 1
              try {
                actor.sender = actor.postSender
                val pMsg = actor.postMsg
                actor.postSender = null
                actor.postMsg = null
                pMsg match {
                  case replyMsg: ReplyMsg => {
                    val replyAction = replyMsg.replyAction
                    if ( replyAction.isInstanceOf[BlockingReply] )
                      actor._behavior( replyMsg )
                    else {
                      actor.sender = replyAction.replyChannel
                      replyAction.react( replyMsg.msg )
                    }
                  }
                  case anyMsg => actor._behavior( anyMsg )
                }
              }
              catch { case t: Exception => actor._handleException( t ) }
            }
            // now check and process postbox
            var postbox = actor.postbox
            while ( postbox ne Nil ) {
              actor.postbox = Nil
              while ( postbox ne Nil ) {
                sameThreadWidth += 1
                val (pMsg,pReplyChannel) = postbox.head
                postbox = postbox.tail
                try {
                  actor.sender = pReplyChannel
                  pMsg match {
                    case replyMsg: ReplyMsg => {
                      val replyAction = replyMsg.replyAction
                      if ( replyAction.isInstanceOf[BlockingReply] )
                        actor._behavior( replyMsg )
                      else {
                        actor.sender = replyAction.replyChannel
                        replyAction.react( replyMsg.msg )
                      }
                    }
                    case anyMsg => actor._behavior( anyMsg )
                  }
                }
                catch { case t: Exception => actor._handleException( t ) }
              }
              postbox = actor.postbox
              if ( postbox ne Nil ) {
                actor.postbox = Nil
                if( sameThreadWidth >= maxSameThreadWidth ) {
                  postbox foreach { ms => actor.pushMsg( ms._1, ms._2 )/*; postToQueuedCount += 1*/ }
                  actor.enqueueIfNeeded()
                  postbox = Nil
                }
              }
            }                
            // end postbox processing
            actor.sender = oldSender
            sameThreadDepth -=1
          } else {
            if ( actor.postSender eq null ) {
              actor.postSender = sender
              actor.postMsg = msg
//              postCount += 1
            } else {
              actor.postbox ::= (msg,sender)
//              postBoxCount += 1
            }
          }
        } // isActive
      }
    }
  }
}
