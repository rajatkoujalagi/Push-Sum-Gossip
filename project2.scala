import akka.actor._
import scala.util.Random
import scala.concurrent.duration.DurationInt
import scala.concurrent._
import scala.concurrent.duration._
object project2 extends App {
  val numNodes:Int = args(0).toInt
  val topology:String = args(1)
  val algorithm:String = args(2)  
  val system = ActorSystem("PROJECT2")
  val timer = system.actorOf(Props[Timer])
    
  var actorList:List[ActorRef] = List()  
  if(algorithm=="gossip"){
	  for(i<-0 to numNodes-1)
	  {
		  var actor=system.actorOf(Props(new GossipNode(timer)))	
		  actor ! i
		  actorList = actorList ::: List(actor)
	  }
  }
  else if(algorithm=="push-sum"){
    for(i<-0 to numNodes-1)
	  {
		  var actor=system.actorOf(Props(new PushSumNode(timer)))	
		  actor ! i
		  actorList = actorList ::: List(actor)
	  }
  }
  else{
    println("Invalid algorithm")
    System.exit(0)
  }
  
  if(topology=="full")
  {    
    for(i<-0 to numNodes-1)
    {
      actorList(i) ! actorList.slice(0, i) ::: actorList.slice(i+1, numNodes)
    } 
  }
  else if(topology=="line")
  {
    actorList(0) ! List(actorList(1))
    actorList(numNodes-1) ! List(actorList(numNodes-2))
    
    for(i<-1 to numNodes-2)
    {
      actorList(i) ! List(actorList(i-1),actorList(i+1))    
    }
  }
  else if(topology=="2D")
  {
    val k:Int = Math.sqrt(numNodes).toInt
    val limit:Int = k*k-1
    
    actorList = actorList.take(k*k)
    actorList(0) ! List(actorList(1),actorList(k))
    actorList(k-1) ! List(actorList(k-2),actorList(2*k-1))
    actorList(limit) ! List(actorList(limit-k),actorList(limit-1))
    actorList(limit-k+1) ! List(actorList(limit-k+2),actorList(limit+1-2*k))
    for(i<-1 to limit)
    {
      if(i!=0 && i!=k-1 && i!=limit && i!=limit-k+1)
      {	      
          if(i<k)
	      {
	        actorList(i) ! List(actorList(i-1),actorList(i+1),actorList(i+k)) 
	      }
	      else if(i>limit-k)
	      {
	        actorList(i) ! List(actorList(i-1),actorList(i+1),actorList(i-k))
	      }
	      else if(i%k==0)
	      {
	        actorList(i) ! List(actorList(i+1),actorList(i+k),actorList(i-k))
	      }
	      else if(i%k==k-1)
	      {
	        actorList(i) ! List(actorList(i-1),actorList(i+k),actorList(i-k))
	      }
	      else
	      {
	        actorList(i) ! List(actorList(i-1),actorList(i+1),actorList(i-k),actorList(i+k))      
	      }
      }
    }
  }
  else if(topology=="imp2D"){    
    val k:Int = Math.sqrt(numNodes).toInt
    val limit:Int = k*k-1
    actorList = actorList.take(k*k)
    actorList(0) ! List(actorList(1),actorList(k),actorList(Random.nextInt(limit+1)))
    actorList(k-1) ! List(actorList(k-2),actorList(2*k-1),actorList(Random.nextInt(limit+1)))
    actorList(limit) ! List(actorList(limit-k),actorList(limit-1),actorList(Random.nextInt(limit+1)))
    actorList(limit-k+1) ! List(actorList(limit-k+2),actorList(limit+1-2*k),actorList(Random.nextInt(limit+1)))
    for(i<-1 to limit)
    {
      if(i!=0 && i!=k-1 && i!=limit && i!=limit-k+1)
      {	      
          if(i<k)
	      {
	        actorList(i) ! List(actorList(i-1),actorList(i+1),actorList(i+k),actorList(Random.nextInt(limit+1))) 
	      }
	      else if(i>limit-k)
	      {
	        actorList(i) ! List(actorList(i-1),actorList(i+1),actorList(i-k),actorList(Random.nextInt(limit+1)))
	      }
	      else if(i%k==0)
	      {
	        actorList(i) ! List(actorList(i+1),actorList(i+k),actorList(i-k),actorList(Random.nextInt(limit+1)))
	      }
	      else if(i%k==k-1)
	      {
	        actorList(i) ! List(actorList(i-1),actorList(i+k),actorList(i-k),actorList(Random.nextInt(limit+1)))
	      }
	      else
	      {
	        actorList(i) ! List(actorList(i-1),actorList(i+1),actorList(i-k),actorList(i+k),actorList(Random.nextInt(limit+1)))      
	      }
      }
    }
  }
  else{
    println("Invalid topology")
    System.exit(0)
  }
  
  println("Topology Built")
  if(topology=="2D"||topology=="imp2D")
    timer ! Math.pow(Math.sqrt(numNodes).toInt,2).toInt
  else
    timer ! numNodes
  if(algorithm=="gossip")
    actorList(0) ! Rumour
  else if(algorithm=="push-sum")
      actorList(0) ! Value(0,0)
  else 
    println("Invalid algo")
  
    
  
}

class GossipNode(timer:ActorRef) extends Actor{
  var id:Int = -1
  var counter:Int=0
  var neighbours:List[ActorRef]=List()
  var len=0
   def receive ={
     case neigh:List[ActorRef]=>
       neighbours=neigh
       len=neighbours.length
     case i:Int=>
       id=i
     
     case Rumour=>
       counter+=1
       //println("Actor "+id+" received the rumour "+counter+" times")         
       if(counter==1){
         timer ! HeardRumour
         neighbours(Random.nextInt(len)) ! Rumour
         import context.dispatcher      
      context.system.scheduler.schedule(0 milliseconds, 10 milliseconds,self,WakeUp)
      //println("Actor "+id+" is now sending the rumour")
       }
       else if(counter>=10){
         /*
         for(i<-neighbours){
           i ! Dead(self)
         }
         context.stop(self)
         */
         //println("Actor "+id+" has stopped transmitting")
       }       
       else{          
           neighbours(Random.nextInt(len)) ! Rumour      
       }
     case WakeUp=>
       if(counter<10){         
       neighbours(Random.nextInt(len)) ! Rumour
       }
     case Dead(a)=>       
       neighbours = neighbours diff List(a)       
       len=neighbours.length
     case "print"=>println(s"Actor $id has $len neighbours")            
   }
}


class PushSumNode(timer:ActorRef) extends Actor{  
  var s,w,h1,h2,d,id:Double = 1  
  var counter,cons:Int=0
  var neighbours:List[ActorRef]=List()
  var len=0
  def receive={
    case x:Int=>
      id=x
      s=x+1
    case neigh:List[ActorRef]=>
      neighbours=neigh
      len=neighbours.length
    case Value(p,q)=>
      //println(s"Actor $id received $p and $q")
      /*
      if(len==0){
        timer ! Converged
        context.stop(self)
      }*/
      counter+=1
      if(counter==1){
        import context.dispatcher      
      context.system.scheduler.schedule(0 milliseconds, 100 milliseconds,self,WakeUp)
      }
      
      d=((s+p)/(w+q)) - (s/w)
      //println(d)
      if(p==0||Math.abs(d) > Math.pow(10,-10)){
      cons=0
      s=s+p
      h1=s/2.0
      s=h1
      w=w+q
      h2=w/2.0
      w=h2
        
      println(s/w)
      //println(s"Actor $id is transmitting")
      
      neighbours(Random.nextInt(len)) ! Value(h1,h2)
      
      }
      else{
        cons+=1
        if(cons==3){
           timer ! Converged
           /*for(i<-neighbours){
             i ! Dead(self)
           }             
           context.stop(self)
           */
        }
        //println("Actor "+id+" is stopping")
        //neighbours(Random.nextInt(len)) ! Value((s+p)/2,(w+q)/2)
        //context.stop(self)
      }
    case WakeUp =>
      //println(s"Actor $id received wake up message")
      /*if(len==0){
        timer ! Converged
        context.stop(self)
      }*/
      s/=2
      w/=2
      
      neighbours(Random.nextInt(len)) ! Value(s,w)
      
    case Dead(act)=>      
      neighbours=neighbours diff List(act)
      len=neighbours.length
    case "print"=>println(neighbours.length)
        
       
  }
}

class Timer extends Actor{
  var counter:Int=0
  var numNodes:Int = 0
  var startTime,endTime:Long = 0  
  def receive={
    case HeardRumour=>
      counter+=1
      //println(s"$counter nodes have received the rumour")
      if(counter==numNodes){
        endTime=System.currentTimeMillis
        println("Time taken to converge gossip "+(endTime-startTime))
        context.system.shutdown
        System.exit(0)
      }
    case Converged=>
      counter+=1
      //println(s"$counter nodes have converged")
      if(counter==numNodes){
        endTime=System.currentTimeMillis
        println("Time taken to converge push-sum "+(endTime-startTime))
        context.system.shutdown
        System.exit(0)
      }
    case n:Int=>
      numNodes=n
      startTime=System.currentTimeMillis
      println(s"Num nodes are $numNodes")
    
  }
}
case object Converged
case class Value(s:Double,w:Double)
case class Dead(actor_to_remove:ActorRef)
case object Rumour
case object WakeUp
case object HeardRumour
