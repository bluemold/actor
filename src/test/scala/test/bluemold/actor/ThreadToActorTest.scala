package test.bluemold.actor

import junit.framework._
import bluemold.actor.Actor
import bluemold.actor.Actor._
import java.util.concurrent.CountDownLatch


object ThreadToActorTest {
    def suite: Test = {
        val suite = new TestSuite(classOf[ThreadToActorTest]);
        suite
    }

    def main(args : Array[String]) {
        junit.textui.TestRunner.run(suite);
    }
}

/**
 * Unit test for simple App.
 */
class ThreadToActorTest extends TestCase("threadToActor") {

    /**
     * Rigourous Tests :-)
     */
    def testBasics() {
      val actor = actorOf( new Actor {

        protected def react: PartialFunction[Any, Unit] = {
          case msg: Int => reply( msg + 1 ) 
          case msg: String => reply( msg + "!" ) 
          case msg => reply( msg ) 
        }

        protected def init() {}

      }).start()
      
      interact {
        ( actor ? "hi" ) { case msg => println( msg ) }
        ( actor ? 1 ) {
          case msg => ( actor ? msg ) {
            case msg => println( msg )
          }
        }
      }
    }
    

}
