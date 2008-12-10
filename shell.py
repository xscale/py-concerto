import socket, pickle, sys

def parse_command(text):
    return text.split( " " )


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
