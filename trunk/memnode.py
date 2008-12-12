#################################################################
# memnode.py
# Copyright (c) 2008 Henry Robinson
# This software is provided with ABSOLUTELY NO WARRANTY, nor
# implication of fitness for purpose
# This software is licensed under the GPL v2.

# Implementation of a memory node.
# Currently, 'memory' is implemented as an array of integers. There is a single lock per location.
# If we were being more sensible about this, we could use a tree structure to do range locks.
# And a bunch of other things.

import threading, mutex, time, array

VOTE_OK = 0
BAD_LOCK = 1
BAD_CMP = 2

class MemoryNode( object ):
    def __init__( self, size = 1024 ):
        self.locations = array.array( 'i', [ 0 for x in xrange(size) ] )
        # locks[x] = (loc-lock, read-lock, write-lock)
        # protocol for taking the read lock:
        # take the location lock, test the write lock, take the read lock, release the location lock
        # for taking the write lock:
        # take the location lock, test-and-take the read lock, test-and-take the write lock, release the location lock

        self.locks = [ (threading.Lock( ), 0, 0) for x in xrange(size)] 
        self.redo_log = []
        self.transactions = {}

    def take_read_locks( self, compares, id ):
        successful = []
        for c in compares:
            loc,_,_ = self.locks[c]
            loc.acquire( )
            _,read,write = self.locks[c]            
            if write == 0:
                successful.append( c )
                self.locks[c] = (loc,read+1, write)
                loc.release( )
            else:
                for s in successful:
                    loc2,_,_ = self.locks[s]
                    loc2.acquire( )
                    # We have to re-read read and write, because they could have changed
                    # while we were acquiring the lock
                    _,read,write = self.locks[s]
                    self.locks[s] = (loc2, read-1, write)
                    loc2.release( )
                loc.release( )
                return False
        return True

    def take_write_locks( self, writes, id ):
        successful = []
        good = True
        for c in writes:
            loc, _, _ = self.locks[c]
            loc.acquire( )
            _, read, write = self.locks[c]            
            if not read == 0:
                good = False
                loc.release( )
                break
            if not write == 0:
                good = False
            if good:
                successful.append( c )
                self.locks[c] = (loc, read, write+1)
            loc.release( )                
        if not good:
            for s in successful:
                loc, _, _ = self.locks[s]
                loc.acquire( )
                _, read, write = self.locks[s]                
                self.locks[s] = (loc, read, write-1)
                loc.release( )
            return False
        return True

    def release_locks( self, mt ):
        locs = set( mt.compares ).union( mt.reads ) #.union( mt.writes )
        for l in locs:
            loc, _, _ = self.locks[l]
            loc.acquire( )
            _, read, write = self.locks[l]            
            self.locks[l] = (loc, read-1, write)
            loc.release( )
        for l in mt.writes:
            loc, _, _ = self.locks[l]
            loc.acquire( )
            _, read, write = self.locks[l]            
            self.locks[l] = (loc, read, write-1)
            loc.release( )

    def check_compare( self, compares ):
        for c in compares:
            if self.locations[c] != compares[c]:
                return False
        return True

    def apply_writems( self, writes ):
        for w in writes:
            self.locations[w] = writes[w]

    def read_data( self, reads ):
        ret = {}
        for r in reads:
            ret[r] = self.locations[r]
        return ret

    def exec_and_prepare( self, mt ):
        self.transactions[ mt.id ] = mt
        vote = VOTE_OK
        # we want to avoid taking locks for a location more than once. Otherwise there are difficulties if we try and read, write or compare the same location
        # The easiest way to do this is to only take the 'strongest' locks
        compares, reads, writes = set( mt.compares ), set( mt.reads ), set( mt.writes )
        compares = (compares - reads) - writes
        reads = reads - writes
        # short-circuit execution will ensure the right thing is done here
        if not (self.take_read_locks( compares, mt.id ) 
                and self.take_read_locks( reads, mt.id ) 
                and self.take_write_locks( writes, mt.id )):
            vote = BAD_LOCK
        data = None
        if not self.check_compare( mt.compares ):
            self.release_locks( mt )
            vote = BAD_CMP
        else:
            data = self.read_data( mt.reads )
            self.redo_log.append( (mt.id,[],mt.writes) )
        if vote != 0:
            mt.state = STATE_ABORTED
        else:
            mt.state = STATE_COMMITTED
        return (mt.id, vote, data)

    def commit( self, id ):
        mt = self.transactions[ id ]
        self.apply_writems( mt.writes )
        self.release_locks( mt )
        mt.state = STATE_COMMITTED

    def abort( self, tid ):
        mt = self.transactions[ tid ]
        if mt.state != STATE_ABORTED:
            self.release_locks( mt )
        mt.state = STATE_ABORTED

    def report( self, printlocs = True ):
        txt = "%s transactions received" % (len(self.transactions),) + "\n%s transactions committed" % (len( [t for t in self.transactions.values( ) if t.state == STATE_COMMITTED] ), ) + "\n%s transactions aborted" % (len( [t for t in self.transactions.values( ) if t.state == STATE_ABORTED] ), ) + "\n%s transactions pending" % (len( [t for t in self.transactions.values( ) if t.state == STATE_EXEC_PREPARE] ), )
        if printlocs: txt += "\n" + str(self.locations )
        return txt


#####
# MultiMiniTransactions have a unique - across all memory nodes - id. There can only be one
# LocalMiniTransaction per node per MMT (although this is not enforced by the code yet).
# LMTs exist on the client and server side. They are given a unique - per MMT - id, but also
# keep track of the MMT id so we can find out to which MMT they belong. 
            
STATE_NONE = 0
STATE_EXEC_PREPARE = 1
STATE_ABORTED = 2
STATE_COMMITTED = 3

# A LMT only applies to one node
class LocalMiniTransaction( object ):    
    def __init__( self, compares, reads, writes, id, address=None):
        self.compares = compares
        self.reads = reads
        self.writes = writes
        self.id = id
        self.state = STATE_NONE
        self.address = address
        self.lid = 0
        self.retry_count = 0

import time

# This is a client-side class - bundle together a bunch of LMTs and then send them over the network.
class MultiMiniTransaction( object ):
    def __init__( self, tid ):
        self.lmts = { }
        self.id = tid
        self.lmtids = 0
        self.time = time.time( )

    def gen_tid( self, base ):
        self.id = (int((self.time * 1000000.0) % 1000000) << 16) + (base << 8)


    def add_transaction( self, lmt ):
        lmt.lid = self.lmtids
        self.lmtids += 1
        self.lmts[lmt.lid] = lmt

    def retry_count( self ):
        return sum( l.retry_count for l in self.lmts.values( ) )

if __name__ == "__main__":

    class ServerThread( threading.Thread ):
        def __init__( self, addr ):
            self.server = ConcertoServer( addr )
            threading.Thread.__init__( self )

        def run( self ):
            self.server.start( )

    numservers = 2
    baseaddr = 21567
    threads = [ServerThread( ("localhost", 21567+x) ) for x in xrange(numservers) ]
    for t in threads:
        t.start( )
    for t in [t for t in threads if t.isAlive( )]:
        t.join( )

