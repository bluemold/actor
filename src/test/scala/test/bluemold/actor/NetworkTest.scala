package test.bluemold.actor

import junit.framework._
import bluemold.actor.Actor._
import java.util.concurrent.CountDownLatch
import bluemold.actor.{ActorRef, RegisteredActor, FiberStrategyFactory, UDPNode}

object NetworkTest {
    def suite: Test = {
        val suite = new TestSuite(classOf[NetworkTest]);
        suite
    }

    def main(args : Array[String]) {
        junit.textui.TestRunner.run(suite);
    }
}

/**
 * Unit test for actor clustering.
 */
class NetworkTest extends TestCase("network") {

  val latchOne = new CountDownLatch(1)
  val latchTwo = new CountDownLatch(1)

  def testBasics() {
    val fred = nodeOne()
    val george = nodeTwo()

    fred ! "announce"
    george ! "announce"
    
    latchOne.await()
    latchTwo.await()
    
    synchronized { wait( 1000 ) }
  }
  
  def nodeOne(): ActorRef = {
    implicit val node = UDPNode.getNode( "nodeOne", "default" )
    implicit val strategyFactory = new FiberStrategyFactory()
    actorOf( new NodeActor( "Fred", latchOne ) ).start()
  }
    
  def nodeTwo(): ActorRef = {
    implicit val node = UDPNode.getNode( "nodeTwo", "default" )
    implicit val strategyFactory = new FiberStrategyFactory()
    actorOf( new NodeActor( "George", latchTwo) ).start()
  }

  class NodeActor( name: String, latch: CountDownLatch ) extends RegisteredActor {
    var count: Int = _

    protected def init() {
      count = 10
    }

    protected def react: PartialFunction[Any, Unit] = {
      case "announce" => {
        println( getNode )
        getNode.sendAll( classOf[NodeActor], name )
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
