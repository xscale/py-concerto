import socket, pickle, time, threading, Queue

class ConcertoMonitor(object):
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
        self.hb = ConcertoMonitor.hbthread( self )
        self.mt = ConcertoMonitor.monitor_thread( self )
        self.abort = False
        self.susp_cb = None
        self.hb_cb = None

    def set_suspect( self, cb ):
        self.susp_cb = cb

    def set_heartbeat( self, cb ):
        self.hb_cb = cb
        
    def suspect( self ):
        if self.susp_cb: self.susp_cb( )

    def heartbeat( self ):
        if self.hb_cb: self.hb_cb( )
            
    def start( self ):
        self.hb.start( )
        self.mt.start( )

    def stop( self ):
        self.abort = True        
        if self.mt.isAlive():
            self.mt.join( )
        self.socket.close( )
