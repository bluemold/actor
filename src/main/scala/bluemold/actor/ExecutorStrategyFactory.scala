package bluemold.actor

import java.util.concurrent.{ThreadFactory, TimeUnit, Executors}
import bluemold.concurrent.{AtomicBoolean, AtomicInteger}

/**
 * ExecutorStrategyFactory
 * Author: Neil Essy
 * Created: 8/10/11
 * <p/>
 * [Description]
 */
object ExecutorStrategyFactory {
  val useDaemon = AtomicBoolean.create( true )
  def setDaemon( choice: Boolean ) { useDaemon.set( choice ) }
}
class ExecutorStrategyFactory( implicit cluster: Cluster, sfClassLoader: StrategyFactoryClassLoader ) extends ActorStrategyFactory {
  import ExecutorStrategyFactory._

  val concurrency = Runtime.getRuntime.availableProcessors()

  val threadFactory = new ThreadFactory {
    def newThread( r: Runnable ) = {
      val thread = new Thread( r )
      thread.setContextClassLoader( sfClassLoader.classLoader )
      thread.setDaemon( useDaemon.get() )
      thread.setPriority( Thread.NORM_PRIORITY )
      thread
    }
  }

  val pool = Executors.newFixedThreadPool( concurrency, threadFactory )

  val strategy = new Strategy

  def getStrategy = {
    strategy.actorCount.incrementAndGet();
    strategy
  }

  def shutdownNow() { pool.shutdown() }

  def waitForShutdown() {
    while ( ! pool.isTerminated )
      pool.awaitTermination( 1, TimeUnit.SECONDS )
  }

  def printStats() {
    println( strategy.actorCount.get() )
  }

  class Strategy extends ActorStrategy {

    val actorCount = AtomicInteger.create()

    def send( msg: Any, actor: AbstractActor, sender: ReplyChannel ) {
      actor.pushMsg( msg, sender )
      actor.enqueueIfNeeded()
    }

    def enqueue( actor: AbstractActor ) {
      pool.execute( new ProcessActorMsgs( actor ) )
    }

    def getNextStrategy() = {
      actorCount.incrementAndGet()
      this
    }

    var defaultTimeout: Long = 60000 // milliseconds

    def setDefaultTimeout(newDefault: Long) { defaultTimeout = newDefault }

    def getDefaultTimeout(): Long = defaultTimeout

    def getCluster = cluster

    class ProcessActorMsgs( actor: AbstractActor ) extends Runnable {
      def run() {
        actor.synchronized {
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
                      case msg: Any => actor._behavior( msg )
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
            enqueue( actor )
          }
        }
      }
    }
  }
}