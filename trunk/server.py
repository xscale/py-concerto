import memnode, socket, pickle, Queue, threading

class ConcertoServer( object ):
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
                    if command.command == "EXEC": 
                        mt = memnode.LocalMiniTransaction( command.compares, command.reads, command.writes, command.tid )
                        success = self.server.mn.exec_and_prepare( mt )
                        self.server.socket.sendto( pickle.dumps( (command.lid,success) ), command.addr )
                    if command.command == "COMT":
                        self.server.mn.commit( command.tid )
                    if command.command == "ABRT":
                        self.server.mn.abort( command.tid )            

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
    

    def __init__( self, addr, numthreads = 3 ):
        self.socket = None
        self.mn = memnode.MemoryNode( )
        self.address = addr
        self.command_queue = Queue.Queue( )
        self.numthreads = numthreads
        self.threads = [ ConcertoServer.server_thread( self.command_queue, self ) for i in xrange(self.numthreads) ]

    def start( self ):
        print "Concerto server starting at %s" % (self.address,)
        self.socket = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
        self.socket.bind( self.address )
        for t in self.threads:
            t.start( )
        self.mainloop( )

    def mainloop( self ):
        abort = False
        while not abort:
            # TO DO: deal with fragmentation
            data, addr = self.socket.recvfrom( 2048 )
            command = ConcertoServer.ClientCommand( data, addr )            
            print addr[0]
            if addr[0] == "localhost" or addr[0] == "127.0.0.1":
                if command.command == "REPORT":
                    print "--------------------Server at %s" % (self.address,)
                    self.mn.report( )
                    print "------------------------------------"
                if command.command == "TERMINATE":
                    abort = True
                else:
                    self.command_queue.put( command )
            else:
                    self.command_queue.put( command )
        self.stop( )

    def stop( self ):
        print "Concerto is exiting..."
        print "Waiting for threads to finish..."
        for t in self.threads:
            t.doAbort( )
        for t in self.threads:
            if t.isAlive( ):
                t.join( )
                print "Thread finished"
        print "All done, exiting."

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
