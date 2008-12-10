import socket, pickle, sys, threading, Queue

def parse_command(text):
    return text.split( " " )

class server_monitor(object):
#-------------------------------------------------------------------------
    class hbthread(threading.Thread):
        def __init__(self, monitor):
            threading.Thread.__init__( self )
            self.queue = monitor.queue
            self.monitor = monitor

        def run( self ):
            txt = pickle.dumps( ("HEARTBEAT",-1,self.monitor.sckaddr) )
            self.monitor.socket.sendto( txt, self.monitor.addr )
            while not self.monitor.abort:
                (data, addr) = self.monitor.socket.recvfrom( 2048 )
                msg = pickle.loads( data )
                if msg == "HB-SEND":
                    self.queue.put( msg )
#-------------------------------------------------------------------------
    class monitor_thread(threading.Thread):
        def __init__(self, server):
            threading.Thread.__init__( self )
            self.server = server
        def run( self ):
            while not self.server.abort:
                try:
                    self.server.queue.get( True, 3 )
                    self.server.heartbeat( )
                except:
                    self.server.suspect( )
    
#-------------------------------------------------------------------------

    def __init__(self, addr):
        self.queue = Queue.Queue( )
        self.addr = addr
        self.socket = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
        self.sckaddr = ("localhost", addr[1]+1000)
        self.socket.bind( self.sckaddr )
        self.hb = server_monitor.hbthread( self )
        self.mt = server_monitor.monitor_thread( self )
        self.abort = False
        
    def suspect( self ):
        print "Suspecting %s has died" % (self.addr,)
    def heartbeat( self ):
        print "Received heartbeat from %s" % (self.addr,)
            
    def start( self ):
        self.hb.start( )
        self.mt.start( )

    def stop( self ):
        self.abort = True
        self.mt.join( )


if __name__ == '__main__':
    # shell client. give it an address and a port to connect to (only talks to one memnode at a time)
    # good for sending command messages
    prompt = ">>> "
    abort = False
    sock = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
    server = None
    while not abort:
        try:
            text = raw_input( prompt )
        except:
            print "\nExiting..."
            sys.exit( )
        cmd = parse_command( text )

        if cmd[0] == "REPORT":
            if not server:
                print "Not connected"
            else:
                txt = pickle.dumps( ("REPORT", -1) )
                sock.sendto( txt, server )
                print "Waiting..."
                data = sock.recvfrom( 4096 )
                print pickle.loads( data[0] )
        if cmd[0] == "CONNECT":
            # should check these here, really
            server = (cmd[1],int(cmd[2]))
            print "Connected to %s" % cmd
        if cmd[0] == "KILL":
            if not server:
                print "Not connected"
            else:
                txt = pickle.dumps( ("TERMINATE", -1) )
                sock.sendto( txt, server )
                print "Killed %s" % (server,)

        if cmd[0] == "HEARTBEAT":
            if not server:
                print "Not connected"
            else:
                monitor = server_monitor( server )
                monitor.start( )
