#################################################################
# shell.py
# Copyright (c) 2008 Henry Robinson
# This software is provided with ABSOLUTELY NO WARRANTY, nor
# implication of fitness for purpose
# This software is licensed under the GPL v2.

# run this with python shell.py to get a very simply command line interface
# which you can use you talk to individual memory nodes.

# Commands are:
# connect host port
# -- starts a connection to the memory node at (host,port), will fail if heartbeat
# -- messages aren't received

# report
# -- prints some statistics on the number of transactions received and committed
# -- and the contents of memory

# kill
# -- asks the memory node to terminate. Normally does so cleanly.

# heartbeat
# -- prints the number of heartbeats received

# trans
# -- execute a transaction. You'll be prompted for a list of locations to read,
# -- and a set of pairs to compare and also to write

import socket, pickle, monitor, time, memnode, client



class ConcertoShell(object):
    """This interactive shell allows you to connect to one Concerto server and send it commands.
    """

    def __init__(self, prompt = ">>> "):
        self.prompt = prompt
        self.abort = False
        self.socket = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
        self.server = None
        self.monitor = None
        self.hbs = 0
        self.client = client.ConcertoClient( )

    def parse_command(self, text):
        text = text.split( " " )
        text = map( lambda x: x.upper( ), text )
        return text

    def heartbeat( self ):
        self.hbs += 1

    def is_connected(self):
        """Returns true if we are currently talking to a server
        """
        if self.server: return True
        return False

    def do_abort(self):
        """Sets the abort flag, next time there's any input we'll quit.
        """
        self.abort = True
        if self.monitor: self.monitor.stop( )
    

    def connect(self, host, port):
        self.hbs = 0
        self.server = (host,port)
        if self.monitor: self.monitor.stop( )
        self.monitor = monitor.ConcertoMonitor( self.server )
        self.monitor.set_heartbeat( lambda : self.heartbeat( ) )
        self.monitor.start( )
        # If this is the first time we are asking for heartbeats we
        # might get one back really quickly
        # however, it's possible heartbeats are already coming our way
        # so in the worst case we have to wait for the full interval
        time.sleep( 0.5 )
        if self.hbs == 0:
            time.sleep( 2.0 )
        if self.hbs == 0:
            print "Could not connect"
            self.server = None
            self.monitor.stop( )
            return False
        print "Connected to %s" % (self.server,)
        return True

    def transaction(self, ):
        reads = raw_input( "Locations to read separated by commas, press enter for none:\n " )
        reads = [ int( x.strip( ) ) for x in reads.split( "," ) if x != '' ]
        compares = raw_input( "Locations to compare as location:value separated by commas, press enter for none:\n " )
        compares = [ ( int( x.strip( ).split(":")[0] ),
                       int( x.strip( ).split(":")[1] ) ) for x in compares.split( "," ) if x != '' ]
        
        writes = raw_input( "Type in the locations to write as location:value separated by commas, press enter for none:\n " )
        writes = [ ( int( x.strip( ).split(":")[0] ),
                     int( x.strip( ).split(":")[1] ) ) for x in writes.split( "," ) if x != '' ]
        read_dict, write_dict, compare_dict = { }, { }, { }
        for r in reads: read_dict[r] = 1
        for (l,d) in writes: write_dict[l] = d
        for (l,d) in compares: compare_dict[l] = d
        
        m = memnode.MultiMiniTransaction( 10 )
        m.gen_tid( 10 )
        transid = m.id
        l = memnode.LocalMiniTransaction( compare_dict, read_dict, write_dict, transid, self.server )
        m.add_transaction( l )
        print "Starting transaction %s" % transid
        success = self.client.do_transaction( m )
        print "Transaction %s :: successful == %s (retry count :: %s)" % (transid,success,m.retry_count( ))
        return success
    
        

    def start(self, ):
        """Kick-off the main loop.
        """
        print "ConcertoShell v0.1"
        while not self.abort:
            try:
                text = raw_input( self.prompt )
                cmd = self.parse_command( text )
                if cmd[0] == "REPORT":
                    if not self.is_connected( ):
                        print "Not connected"
                    else:
                        txt = pickle.dumps( ("REPORT", -1) )
                        self.socket.sendto( txt, self.server )
                        print "Waiting..."
                        data = self.socket.recvfrom( 4096 )
                        print pickle.loads( data[0] )
                if cmd[0] == "CONNECT":
                    self.connect( cmd[1], int(cmd[2]) )
                if cmd[0] == "KILL":
                    if not self.is_connected( ):
                        print "Not connected"
                    else:
                        txt = pickle.dumps( ("TERMINATE", -1) )
                        self.socket.sendto( txt, self.server )
                        if self.monitor: self.monitor.stop( )
                        print "Killed %s" % (self.server,)
                if cmd[0] == "QUIT":
                    self.do_abort( )
                if cmd[0] == "TRANS":
                    self.transaction( )                    
                if cmd[0] == "HEARTBEAT":
                    if self.is_connected( ):
                        print "%s heartbeats received from %s" % (self.hbs, self.server)
                    else:
                        print "Not connected"
            except KeyboardInterrupt, EOFError: # To do - specialise this for the right exception
                self.do_abort( )
        print "\nExiting...\n"
        
if __name__ == '__main__':
    # shell client. give it an address and a port to connect to (only talks to one memnode at a time)
    # good for sending command messages
    shell = ConcertoShell( )
    shell.start( )
