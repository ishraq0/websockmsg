package Websocket;

use strict;
use warnings;
use Errno qw(EAGAIN EWOULDBLOCK);

# the minimum websocket frame size allowed for a frame sent by a client to a server
use constant MIN_FRAME_SIZE => 6;

sub read {
    
    my ($client) = @_;
    
    my ($frame, $frame_size, $unmasked_payload) = ("", 0, "");
    while(1) {
        
        my $read_into_frame = sub {
        
            my ($client, $frame) = @_;
            
            my $buf = "";
            my $bytes_read = $client->sysread($buf, 131072); # 2^17 (= 131072)
            if(!defined $bytes_read) {
                if($! == EAGAIN || $! == EWOULDBLOCK) {
                    die "nothing to read right now\n";
                } else {
                    die "sysread() failed: $!";
                }
            } elsif($bytes_read == 0) {
                $client->close();
                die "connection closed\n";
            }
            $$frame .= $buf;
            
            return length($$frame);
        };
        
        while($frame_size < MIN_FRAME_SIZE) {
            $frame_size = $read_into_frame->($client, \$frame);
        }
        
        my @bytes;
        
        @bytes = unpack("C" . MIN_FRAME_SIZE, $frame);
        
        my $is_fin = ($bytes[0] & 0b1_0000000) >> 7;
        my $opcode = $bytes[0] & 0b0000_1111;
        
        my $is_masked = ($bytes[1] & 0b1_0000000) >> 7;
        if(!$is_masked) {
            $client->close();
            return "the client sent us an unmasked frame";
        }
        
        my $payload_len = $bytes[1] & 0b0_1111111;
        my $mask;
        my $ext = 0;
        
        if($payload_len <= 125) {
            $mask = pack("C*", @bytes[2 .. 5]);
        } elsif($payload_len == 126) {
            $ext = 2;
        } elsif($payload_len == 127) {
            $ext = 8;
        }
        
        my $header_size = MIN_FRAME_SIZE + $ext;
        
        while($frame_size < $header_size) {
            $frame_size = $read_into_frame->($client, \$frame);
        }
        @bytes = unpack("C" . $header_size, $frame);
        if($ext == 2) {
            $payload_len = unpack("n", pack("C*", @bytes[2 .. 3]));
            $mask = pack("C*", @bytes[4 .. 7]);
        } elsif($ext == 8) {
            $payload_len = unpack("Q>", pack("C*", @bytes[2 .. 9]));
            $mask = pack("C*", @bytes[10 .. 13]);
        }
        
        my $received = $frame_size - $header_size;
        while($received < $payload_len) {
            $frame_size = $read_into_frame->($client, \$frame);
            $received = $frame_size - $header_size;
        }
        
        # NOTE: by this point $frame contains the frame header + payload, but it may also contain
        #       stuff from the next frame (some or all of the next frame header, payload).

        my $mask_rep = $mask x (int($payload_len / 4) + 1);
        $mask_rep = substr($mask_rep, 0, $payload_len);
        my $payload = substr($frame, $header_size, $payload_len);
        $unmasked_payload .= $payload ^ $mask_rep;
        
        # snip off the current frame so we can start processing the next frame, if any
        
        $frame = substr($frame, $header_size + $payload_len);
        $frame_size = length($frame);
        
        if($is_fin) {
            last;
        }
    }
    
    return $unmasked_payload;
};

sub send {
    
    my ($client, $data) = @_;
    
    my @frame_header;

    # fin, rsv, binary
    @frame_header = (0b1_000_0010);
    
    my $payload_len = length($data);
    if($payload_len <= 125) {
        push(@frame_header, $payload_len);
    } else {
        if($payload_len < 2**16) {
            push(@frame_header, (126, unpack("C*", pack("n", $payload_len))));
        } else {
            push(@frame_header, (127, unpack("C*", unpack("Q>", $payload_len))));
        }
    }
    
    my $frame = pack("C*", @frame_header);
    $frame .= $data;
    
    $client->send($frame) or die "send() failed: $!";
};

1;
