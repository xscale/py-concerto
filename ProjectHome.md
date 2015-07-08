Concerto is a Python implementation of the Sinfonia (paper: http://research.microsoft.com/users/aguilera/papers/sinfonia-aguilera-sosp2007.pdf) distributed shared memory abstraction that was presented at SOSP in 2007.

This project started out as the result of a challenge: to implement the core of Sinfonia in any language in under three hours. The challenge was met, but the implementation was ugly. What is presented here is the result of some very preliminary beautification work.

Neither Python nor this implementation are likely to perform well under any heavy loads, so Concerto is only likely to be practical as a learning aid. A Java version is in the works.

Still missing is the recovery protocol which allows minitransactions to complete in the event of the coordinator failing.

The authors of the Sinfonia paper are in no way connected with this project.