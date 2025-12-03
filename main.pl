use strict;
use warnings;
use threads;
use threads::shared;
use builtin qw(true false);
no warnings qw(experimental);
use FindBin qw($Bin);
use IO::Socket::INET;
use List::Util qw(any);
use Digest::SHA qw(sha1);
use MIME::Base64 qw(encode_base64);
use Time::HiRes qw(gettimeofday);

use lib "$Bin";
use Websocket;

use constant WEBSOCKET_SETUP => do {
    local $/;
    open(my $fh, "<", "$Bin/websocket_setup.html") or die "could not open file: $!";
    my $f = <$fh>;
    my $template = "HTTP/1.1 200 OK\r\n" .
                   "Connection: close\r\n" .
                   "Content-Type: text/html\r\n" .
                   "Content-Length: %u\r\n" .
                   "\r\n";
    my $response = sprintf($template, length($f)) . $f;
};

use constant USERS_ONLINE => do {
    local $/;
    open(my $fh, "<", "$Bin/users_online.html") or die "could not open file: $!" ;
    <$fh>;
};

use constant CHAT => do {
    local $/;
    open(my $fh, "<", "$Bin/chat.html") or die "could not open file: $!";
    <$fh>;
};

our %users : shared = ();

main();

sub main {
    
    my $server = IO::Socket::INET->new(
        Type      => SOCK_STREAM,
        Proto     => "tcp",
        LocalHost => "0.0.0.0",
        LocalPort => 80,
        ReusePort => 1,
        Listen    => SOMAXCONN
    ) or die "can't open socket: $IO::Socket::errstr";
    
    my @threads;
    while(1) {
        my $client = $server->accept();
        if($client == 0) {
            warn "accept() failed: $!\n";
        } else {
            push(@threads, threads->create(\&t, $client)->detach());
        }
    }

    $server->close();
}

sub t {
    
    my ($client) = @_;

    print "Connection from " . $client->peerhost() . ":" . $client->peerport() . "\n";
    
    my @lines;
    {
        my $header = "";
        my $bytes_read = $client->sysread($header, 4096);
        if(!defined $bytes_read) {
            die "sysread() failed: $!";
        } elsif($bytes_read == 0) {
            $client->close();
            return "connection closed";
        }
        if($header !~ /\r\n\r\n$/) {
            die "http header's longer than 4 KiB";
        }
        @lines = split(/\r\n/, $header);
    }
    
    # uninteresting http header?
    
    if($lines[0] ne "GET / HTTP/1.1") {
        $client->send("HTTP/1.1 204 No Content\r\n\r\n") or die "send() failed: $!";
        $client->close();
        return;
    }
    
    # websocket?
    
    if(!((any { /^Connection:\s*Upgrade$/i } @lines) && (any { /^Upgrade:\s*websocket$/i } @lines))) {
        $client->send(WEBSOCKET_SETUP) or die "send() failed: $!";
        return;
    }
    
    # if we get here we're dealing with a websocket header
 
    my $websocket_key;
    foreach my $line (@lines) {
        if($line =~ /^Sec-WebSocket-Key:\s*(.*)$/i) {
            $websocket_key = $1;
        }
    }
    if(!defined $websocket_key) {
        return "expected a websocket key at this point";
    }
    
    my $accept_key = encode_base64(sha1($websocket_key . "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"), "");
    my $template = "HTTP/1.1 101 Switching Protocols\r\n" .
                   "Connection: Upgrade\r\n" .
                   "Upgrade: websocket\r\n" .
                   "Sec-WebSocket-Accept: %s\r\n" .
                   "\r\n";
    my $response = sprintf($template, $accept_key);
    $client->send($response) or die "send() failed: $!";
    
    # websocket connection's been established
    
    my $username = Websocket::read($client);
    {
        lock(%users);
        $users{$username} = &shared_clone({
            requests => [],
            chatting_with => "",
            is_new_message => false,
            message => ""
        });
    }
    
    # response types
    
    use constant {
        PAGE => 0,
        SNIPPET => 1,
    };
    my ($type, $page);
    
    $client->blocking(0);

    # users online
    
    my $get_time = sub {
        return int(gettimeofday() * 1000); # milliseconds
    };
    
    $type = PAGE;
    my $rows = "";
    my $user_template = '<p>%s<button class="class_button_chat" id="id_chat_%s">Chat</button></p>';
    my $request_template = '<p>User %s wants to chat with you.' .
                           '<button class="class_button_accept" id="id_accept_%s">Accept</button></p>';
    
    my $wait_time = 250;
    my $last_time = $get_time->() - $wait_time;
    my $data = "";
    while(1) {
        if($get_time->() - $last_time >= $wait_time) {
            $rows = "";
            foreach my $u (keys %users) {
                if($u ne $username) {
                    $rows .= sprintf($user_template, $u, $u);
                }
            }
            foreach my $r (@{ $users{$username}->{requests} }) {
                $rows .= sprintf($request_template, $r, $r);
            }
            $page = pack("C*", $type) . sprintf(USERS_ONLINE, $rows);
            Websocket::send($client, $page);

            $last_time = $get_time->();
        }

        last if($users{$username}->{chatting_with} ne "");
        
        eval {
            $data = Websocket::read($client);
        };
        if($@) {
            last if($@ eq "connection closed\n");
            next if($@ eq "nothing to read right now\n");
            warn "$@";
        } else {
            if($data =~ /^id_chat_(.*)$/) {
                $data = $1;
                {
                    lock(%users);
                    push(@{ $users{$data}->{requests} }, $username);
                }
            } elsif($data =~ /^id_accept_(.*)$/) {
                $data = $1;
                {
                    lock(%users);
                    $users{$username}->{chatting_with} = $data;
                    $users{$data}->{chatting_with} = $username;
                }
                last;
            }
        }
    }

    # chat

    my $chatting_with = $users{$username}->{chatting_with};

    $type = PAGE;
    $page = pack("C*", $type) . sprintf(CHAT, $chatting_with);
    Websocket::send($client, $page);

    $type = SNIPPET;
    my $message_template = "<p>%s: %s</p>";

    my $me = $users{$username};
    my $them = $users{$chatting_with};
    while(1) {
        eval {
            lock(%users);
            $me->{message} = Websocket::read($client);
        };
        if($@) {
            if($@ eq "connection closed\n") {
                lock(%users);
                $them->{chatting_with} = "";
                delete($users{$username});
                last;
            }
            warn "$@" if($@ ne "nothing to read right now\n");
        } else {
            lock(%users);
            last if(not exists $users{$chatting_with});
            Websocket::send($client, pack("C*", $type) . sprintf($message_template, $username, $me->{message}));
            $me->{is_new_message} = true;
        }

        if($them->{is_new_message} eq true) {
            lock(%users);
            if(not exists $users{$chatting_with}) {
                Websocket::send($client, pack("C*", $type) . "<p>$chatting_with left.</p>");
                last;
            }
            Websocket::send($client, pack("C*", $type) . sprintf($message_template, $chatting_with, $them->{message}));
            $them->{is_new_message} = false;
        }
    }
}
