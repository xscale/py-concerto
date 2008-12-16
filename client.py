#################################################################
# client.py
# Copyright (c) 2008 Henry Robinson
# This software is provided with ABSOLUTELY NO WARRANTY, nor
# implication of fitness for purpose
# This software is licensed under the GPL v2.

# Client side object that runs the transaction protocol
# Running this script connects to a (currently hard-coded) set
# of memory nodes and runs many simultaneous transactions as a
# stress test.


import socket, pickle, memnode, random, threading, time, md5

class ConcertoClient( object ):
    retryCount = 5
    def __init__( self ):
        self.socket = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
        
    def do_transaction( self, mmt ):
        outstanding = {}
        for lmt in mmt.lmts.values( ):
            txt = pickle.dumps( ("EXEC", mmt.id, lmt.compares, lmt.reads, lmt.writes, lmt.lid ) )            
            self.socket.sendto( txt, lmt.address )
        nr = 0
        abort = False
        while nr < len(mmt.lmts):
            (resp, addr) = self.socket.recvfrom( 2048 )        
            resp = pickle.loads( resp )
            print "Received response: ", resp
            nr += 1
            if resp[1][1] != memnode.VOTE_OK: 
                if resp[1][1] == memnode.BAD_LOCK:
                    lmt = mmt.lmts[ resp[0] ]
                    if lmt.retry_count < retryCount:
                        lmt.retry_count += 1
                        nr -= 1
                        txt = pickle.dumps( ("EXEC", mmt.id, lmt.compares, lmt.reads, lmt.writes, lmt.lid ) )            
                        self.socket.sendto( txt, lmt.address )                        
                    else:
                        abort = True
                        break
                else:
                    abort = True
                    break
        if abort:
            command = "ABRT"
        else:
            command = "COMT"
        for lmt in mmt.lmts.values( ):
            txt = pickle.dumps( (command, mmt.id) )
            self.socket.sendto( txt, lmt.address )
        return (not abort, resp[1])
    
if __name__ == "__main__":
    class test_thread( threading.Thread ):
        tid = 0
        ntrials = 30
        host = "localhost"
        def __init__( self ):
            threading.Thread.__init__( self )
            self.st = test_thread.tid * test_thread.ntrials
            self.thid = test_thread.tid
            test_thread.tid += 1
            self.time = time.time( )
            self.transident = (int((self.time * 1000000.0) % 1000000) << 16) + (self.thid << 8)
        def run( self ):
            print "Running thread %s" % self.thid
            c = ConcertoClient( )
            for n in xrange( self.st, self.st + test_thread.ntrials ):
                tid = self.transident + n
                m = memnode.MultiMiniTransaction( tid )
                l = memnode.LocalMiniTransaction( {},{0:0,10:0,20:0}, {random.randint(0,999):random.randint(0,512)}, tid ,(test_thread.host, 21567))
#                l2 = memnode.LocalMiniTransaction( {},{0:0,10:0,20:0}, {random.randint(0,999):random.randint(0,512)}, tid ,(test_thread.host, 21568))
                m.add_transaction( l )
#                m.add_transaction( l2 )
                print "Starting transaction %s" % tid
                success = c.do_transaction( m )
                print "Transaction %s :: successful == %s (retry count :: %s)" % (tid,success,m.retry_count( ))
            print "Thread %s finished!" % self.thid

    num_threads = 10

    threads = [ test_thread( ) for x in xrange(num_threads) ]
    for t in threads:
        t.start( )

    for t in threads:
        if t.isAlive( ):
            t.join( )
        
#    sock = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
#    txt = pickle.dumps( ("REPORT", -1) )
#        txt = pickle.dumps( ("TERMINATE", -1) )
#    sock.sendto( txt, ("localhost", 21568) )
#    sock.sendto( txt, ("localhost", 21567) )

#    data = sock.recvfrom( 4096 )
#    print pickle.loads( data[0] )
