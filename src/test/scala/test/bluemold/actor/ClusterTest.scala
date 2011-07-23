package test.bluemold.actor

import junit.framework._
import bluemold.actor.Actor._
import java.util.concurrent.CountDownLatch
import bluemold.actor.{ActorRef, RegisteredActor, FiberStrategyFactory, UDPCluster}

object ClusterTest {
    def suite: Test = {
        val suite = new TestSuite(classOf[ClusterTest]);
        suite
    }

    def main(args : Array[String]) {
        junit.textui.TestRunner.run(suite);
    }
}

/**
 * Unit test for actor clustering.
 */
class ClusterTest extends TestCase("cluster") {

  val latchOne = new CountDownLatch(1)
  val latchTwo = new CountDownLatch(1)

  def testBasics() {
    val fred = clusterOne()
    val george = clusterTwo()

    fred ! "announce"
    george ! "announce"
    
    latchOne.await()
    latchTwo.await()
    
    synchronized { wait( 1000 ) }
  }
  
  def clusterOne(): ActorRef = {
    implicit val cluster = UDPCluster.getCluster( "clusterOne", "default" )
    implicit val strategyFactory = new FiberStrategyFactory()
    actorOf( new ClusterActor( "Fred", latchOne ) ).start()
  }
    
  def clusterTwo(): ActorRef = {
    implicit val cluster = UDPCluster.getCluster( "clusterTwo", "default" )
    implicit val strategyFactory = new FiberStrategyFactory()
    actorOf( new ClusterActor( "George", latchTwo) ).start()
  }

  class ClusterActor( name: String, latch: CountDownLatch ) extends RegisteredActor {
    var count: Int = _

    protected def init() {
      count = 10
    }

    protected def react: PartialFunction[Any, Unit] = {
      case "announce" => {
        println( getCluster )
        getCluster.sendAll( classOf[ClusterActor].getName, name )
      }
      case msg: String => {
        if ( msg != name ) {
          count -= 1
          println( name + " count: " + count + " heard: " + msg )
          if ( count > 0 ) {
            reply( name )
          } else {
            reply( name )
            latch.countDown()
            self.stop()
          }
        } else println( name + " heard echo: " + msg )
      }
    }

  }
}
