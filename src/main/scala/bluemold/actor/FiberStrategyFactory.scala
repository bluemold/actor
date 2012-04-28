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
object StrategyFactoryClassLoader {
  implicit def wrap( classLoader: ClassLoader ) = StrategyFactoryClassLoader( classLoader )
  implicit def getStrategyFactoryClassLoader = StrategyFactoryClassLoader( Thread.currentThread().getContextClassLoader )
}
case class StrategyFactoryClassLoader( classLoader: ClassLoader )
class FiberStrategyFactory( implicit cluster: Cluster, sfClassLoader: StrategyFactoryClassLoader ) extends ActorStrategyFactory {
  import FiberStrategyFactory._
  val concurrency = Runtime.getRuntime.availableProcessors()
  val waitTime = 1 // milliseconds
  val maxConsecutiveEmptyLoops = 100
  val maxSameThreadDepth = 50
  val maxSameThreadWidth = 1000
  val maxAffinity = 15
  val nextStrategy = AtomicInteger.create() 

  val queueOffset = Unsafe.objectDeclaredFieldOffset( classOf[SimpleStrategy], "queue" )

  @volatile var threads: Array[SimpleStrategy] = null 

  def printStats() {
    threads foreach { ( strategy: SimpleStrategy ) => println( strategy.actorCount.get() ) }
  }
  
  def shutdownNow() {
    if ( threads != null ) {
      threads.foreach( ( strategy: SimpleStrategy ) => {
        strategy.done.set( true )
      } )
    }
  }

  def shutdownWhenDepleted() {
    if ( threads != null ) {
      threads.foreach( ( strategy: SimpleStrategy ) => {
        strategy.doneOnWait.set( true )
      } )
    }
  }

  def shutdownWhenAllDepleted() {
    if ( threads != null ) {
      threads.foreach( ( strategy: SimpleStrategy ) => {
        strategy.doneOnAllWait.set( true )
      } )
    }
  }

  def waitForShutdown() {
    threads.foreach( ( strategy: SimpleStrategy ) => {
      strategy.thread.join()
    } )
  }

  def getStrategy: ActorStrategy = {
    if ( threads == null ) {
      synchronized {
        if ( threads == null ) {
          var index = -1
          threads = Array.fill[SimpleStrategy](concurrency)( { index += 1; new SimpleStrategy( index ).start() } )
        }
      }
    }
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

    @volatile var waiting: CountDownLatch = null
    var consecutiveEmptyLoops = 0
    var sameThreadDepth = 0
    var sameThreadWidth = 0

    @volatile var queue: List[AbstractActor] = Nil

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
          while ( ! msgs.isEmpty ) {
            // inline of processMsgs( msgs, actor )
            msgs match {
              case (msg,sender) :: tail => {
                if ( actor._isActive ) {
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
                      case msg => actor._behavior( msg )
                    }
                  }
                  catch { case t: Throwable => actor._handleException( t ) }
                  finally { actor.sender = null }
                  // end inline of processMsg( msg, actor, sender)
                  msgs = tail
                }
              }
              case Nil => // never reached
            }
            // end inline of processMsgs( msgs, actor )
          }

          // Queuing logic
          val isActive = actor._isActive
          if ( ! isActive || ! actor.hasMsg ) {
            // No more messages in the mailbox, don't re-queue
            actor.decQueueCount()
            // double check the need for queuing, always the last thing
            if ( isActive && actor.hasMsg )
              actor.enqueueIfNeeded()
          } else if ( actor.queueCount > 1 ) {
            // It's already queued, don't re-queue
            actor.decQueueCount()
            // double check the need for queuing, always the last thing
            if ( isActive && actor.hasMsg )
              actor.enqueueIfNeeded()
          } else {
            // Re-queue actor
            queueActor( actor )
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

    def getCluster: Cluster = cluster

    def getNextStrategy(): ActorStrategy = { val strategy = threads( nextStrategyIndex() ); strategy.actorCount.incrementAndGet(); strategy }

    var defaultTimeout: Long = 60000 // milliseconds

    def setDefaultTimeout(newDefault: Long) { defaultTimeout = newDefault }

    def getDefaultTimeout(): Long = defaultTimeout

    def start(): SimpleStrategy = { nextStrategy = index; thread.start(); this }

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
      catch { case t: Throwable => actor._handleException( t ) }
      finally { actor.sender = null }
    }

    def send( msg: Any, actor: AbstractActor, sender: ReplyChannel ) {
      if ( Thread.currentThread() == thread && sameThreadDepth <= maxSameThreadDepth && sameThreadWidth <= maxSameThreadWidth ) {
        if ( actor.sender == null )
        {
          sameThreadDepth += 1
          if ( actor._isActive ) {
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
                case msg => actor._behavior( msg )
              }
            }
            catch { case t: Throwable => actor._handleException( t ) }
            finally { actor.sender = null }
            // end inline of processMsg( msg, actor, sender )
          } // isActive
          sameThreadDepth -=1
        } else if ( actor._isTailMessaging ) {
          val oldSender = actor.sender;
          sameThreadDepth += 1
          if ( actor._isActive ) {
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
                case msg => actor._behavior( msg )
              }
            }
            catch { case t: Throwable => actor._handleException( t ) }
            finally { actor.sender = oldSender }
            // end inline of processMsg( msg, actor, sender )
          } // isActive
          sameThreadDepth -=1
        } else {
          actor.pushMsg( msg, sender )
          actor.enqueueIfNeeded()
        }
      } else {
        actor.pushMsg( msg, sender )
        actor.enqueueIfNeeded()
      }
    }
  }
}
