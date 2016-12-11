#!/usr/bin/perl
use strict;
use warnings;
use Carp;
use JSON;
use Data::Dumper;
use lib './lib';
use ABC::RabbitMQ::Batch;
use Time::HiRes qw(sleep);

our $VERSION = '0.1';

my $should_stop = 0;
local $SIG{INT} = \&signal_handler;
local $SIG{TERM} = \&signal_handler;

my $rb = ABC::RabbitMQ::Batch->new('localhost', { user => 'guest', password => 'guest' }) or croak;

while (!$should_stop) {
    my $result = $rb->process({
        channel_id => 1,
        queue_in   => 'test_in',
        queue_out  => 'test_out',
        handler    => \&msg_handler,
        batch      => { size => 10, timeout => 2 }
    });
    sleep 0.1;
}

exit(0);
###

sub msg_handler {
    my $messages = shift;
    my $new_mesages = [];
    if (rand() < 0.05) {
        croak("Sometimes handler just dies");
    }
    if (rand() < 0.05) {
        carp('Returned empty hashref for no reason');
        return [];
    }

    for my $msg (@$messages) {
        my $body = from_json($msg->{body});
        $body->{processed} = 1;
        my $new_msg = {
            body         => to_json($body)
        };
        push(@$new_mesages, $new_msg);
    }
    if (rand() < 0.05 && @$new_mesages > 0) {
        carp('Dropped 1 message for no reason');
        pop(@$new_mesages);
    }
    printf "Processed %d messages\n", scalar(@$new_mesages);
    return $new_mesages;
}

sub signal_handler {
    $should_stop = 1;
    return;
}