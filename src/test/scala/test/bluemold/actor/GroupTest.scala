package test.bluemold.actor

import junit.framework._
import bluemold.actor.Actor._
import java.util.concurrent.CountDownLatch
import bluemold.actor._
import collection.immutable.HashMap
import bluemold.actor.GroupActor.GroupActorState

object GroupTest {
    def suite: Test = {
        val suite = new TestSuite(classOf[GroupTest]);
        suite
    }

    def main(args : Array[String]) {
        junit.textui.TestRunner.run(suite);
    }
}

/**
 * Unit test for group actors.
 */
class GroupTest extends TestCase("group") {

  val latchOne = new CountDownLatch(1)
  val latchTwo = new CountDownLatch(1)

  def testBasics() {
    val fred = nodeOne()
    val george = nodeTwo()

    fred ! "announce"
    george ! "announce"
    
    latchOne.await()
    latchTwo.await()
    
    synchronized { wait( 10000 ) }
    fred.stop()
    george.stop()
    println( "Actors Stopped!")
    synchronized { wait( 10000 ) }
  }
  
  def nodeOne(): ActorRef = {
    implicit val node = UDPNode.getNode( "nodeOne", "default" )
    implicit val strategyFactory = new FiberStrategyFactory()
    actorOf( new GroupTestActor( "Fred", latchOne ) ).start()
  }
    
  def nodeTwo(): ActorRef = {
    implicit val node = UDPNode.getNode( "nodeTwo", "default" )
    implicit val strategyFactory = new FiberStrategyFactory()
    actorOf( new GroupTestActor( "George", latchTwo) ).start()
  }

  class GroupTestActor( name: String, latch: CountDownLatch ) extends GroupActor {
    var count: Int = _

    override protected def getId = "test"

    val myDefinition = new GroupActor.AllGroup( getId )

    def definition = myDefinition

    protected def init() {
      initGroupActor()
      count = 10
    }
    val myReact: PartialFunction[Any, Unit] = {
      case "announce" => {
        println( getNode )
        getNode.sendAll( classOf[GroupTestActor], name )
      }
      case msg: String => {
        if ( msg != name ) {
          if ( count > 0 ) {
            count -= 1
            println( name + " count: " + count + " heard: " + msg )
            if ( count > 0 ) {
              reply( name )
            } else {
              reply( name )
              latch.countDown()
            }
          } else println( name + " heard extra: " + msg )
        } else println( name + " heard echo: " + msg )
      }
    }
    val combinedReact = groupReact orElse myReact
    protected def react: PartialFunction[Any, Unit] = combinedReact

    protected def stateChanged(state: GroupActorState) { println( name + ": State Changed: " + state ) }

    protected def sharedStateChanged(groupState: HashMap[String, Any]) { println( name + ": Shared State changed!" ) }

    protected def leaderChanged(iamleader: Boolean) { println( name + ( if ( iamleader ) ": I am the leader" else ": I am no longer the leader" ) ) }
  }
}
