Push-Sum-Gossip
===============
Built a gossip simulator for group communication (http://www.cs.cornell.edu/johannes/papers/2003/focs2003-gossip.pdf) with akka actors and measured convergence time for different kind of topologies (fully connected, 2d grid, 2d grid with certain randomness, line topology).

Implemented the push-sum algorithm with Akka actors to calculate average of all node values(a real world example would be calculating average temperature of different sensors) via Gossip.

The convergence time was tested to be logarithmic in number of nodes.
