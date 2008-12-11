import memnode, socket, pickle, Queue, threading, time

class ConcertoServer( object ):
#---------------------------------------------------------------------------
    class server_thread( threading.Thread ):
        def __init__( self, queue, server ):
            threading.Thread.__init__( self )
            self.queue = queue
            self.abort = False
            self.server = server

        def doAbort( self ):
            self.abort = True

        def run( self ):
            while not self.abort:
                command = None
                try:
                    command = self.queue.get( True, 10 ) # blocking wait, wake up after 10s to check if we're aborting
                except:
                    pass
                if command:
                    self.server.process_command( command )
#---------------------------------------------------------------------------
    class heartbeat_thread(threading.Thread):    
        def __init__(self, server, interval=2.0):
            threading.Thread.__init__( self )
            self.server = server
            self.abort = False
            self.interval = interval

        def doAbort( self ):
            self.abort = True

        def run(self ):
            while not self.abort:
                txt = pickle.dumps( "HB-SEND" )
                for addr in self.server.heartbeats:
#                    print "Sending heartbeat to %s" % (addr,)
                    self.server.socket.sendto( txt, addr )
                time.sleep( self.interval )
#---------------------------------------------------------------------------    

    class ClientCommand( object ):
        def __init__( self, data = None, addr = None ):
            if data:
                self.construct( data, addr )

        def construct( self, data, addr ):
            self.addr = addr
            t = pickle.loads( data )
            # to do - check well-formedness here
            self.command = t[0]
            self.tid = t[1]
            if self.command == "EXEC":
                self.compares = t[2]
                self.reads = t[3]
                self.writes = t[4]
                self.lid = t[5]
            if self.command == "HEARTBEAT":
                self.hbaddr = t[2]

#---------------------------------------------------------------------------

    def __init__( self, addr, numthreads = 3 ):
        self.socket = None
        self.mn = memnode.MemoryNode( )
        self.address = addr
        self.command_queue = Queue.Queue( )
        self.numthreads = numthreads
        self.threads = [ ConcertoServer.server_thread( self.command_queue, 
                                                       self ) 
                         for i in xrange(self.numthreads) ]
        self.abort = False
        self.heartbeats = set( )
        self.hb_thread = ConcertoServer.heartbeat_thread( self )

    def do_abort( self ):
        self.abort = True

    def start( self ):
        print "Concerto server starting at %s" % (self.address,)
        self.socket = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
        self.socket.bind( self.address )
        for t in self.threads:
            t.start( )
        self.hb_thread.start( )
        self.mainloop( )

    def add_heartbeat(self, addr):
        self.heartbeats.add( addr )
            
    def process_command(self, command):
        if command.command == "REPORT":
            txt = "--------------------Server at %s\n" % (self.address,) + self.mn.report( ) + "\n------------------------------------"
            bytes = pickle.dumps( txt )
            self.socket.sendto( bytes, command.addr )
        if command.command == "TERMINATE":
            if command.addr[0] == "localhost" or command.addr[0] == "127.0.0.1":
                self.do_abort( )
        if command.command == "EXEC":
            mt = memnode.LocalMiniTransaction( command.compares, command.reads, command.writes, command.tid )
            print "Writes: ", command.writes
            print "Compares: ", command.compares
            print "Reads: ", command.reads
            success = self.mn.exec_and_prepare( mt )
            bytes = pickle.dumps( (command.lid, success) )
            self.socket.sendto( bytes, command.addr )
        if command.command == "COMT":
            self.mn.commit( command.tid )
        if command.command == "ABRT":
            self.mn.abort( command.tid )            
        if command.command == "HEARTBEAT":
            self.add_heartbeat( command.hbaddr )
    

    def mainloop( self ):
        while not self.abort:
            # TO DO: deal with fragmentation
            data, addr = self.socket.recvfrom( 2048 )
            command = ConcertoServer.ClientCommand( data, addr )            
            print addr[0]
            print command.command
            if command.command == "TERMINATE":
                # ensure the abort flag gets set before we get a chance to block on
                # the next packet
                self.process_command( command )
            else:
                self.command_queue.put( command )
        self.stop( )

    def stop( self ):
        print "Concerto is exiting..."
        print "Waiting for threads to finish..."
        self.hb_thread.doAbort()
        for t in self.threads:
            t.doAbort( )
        for t in self.threads:
            if t.isAlive( ): # hack to make ctrl-c work for now
                t.join( )
                print "Thread finished"
        print "All done, exiting."

import sys
if __name__ == "__main__":
    class ServerThread( threading.Thread ):
        def __init__( self, addr ):
            self.server = ConcertoServer( addr )
            threading.Thread.__init__( self )

        def run( self ):
            self.server.start( )

    numservers = 1
    baseport = 21567

    if len( sys.argv ) >= 2:
        numservers = int( sys.argv[1] )
    if len( sys.argv ) >= 3:
        baseport = int( sys.argv[2] )
        
    threads = [ServerThread( ("localhost", baseport+x) ) for x in xrange(numservers) ]
    for t in threads:
        t.start( )
    for t in threads:
        if t.isAlive( ):
            t.join( )
            print "joined %s" % t.getName( )
