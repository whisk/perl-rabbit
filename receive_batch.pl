use strict;
use warnings;
use v5.18;
use JSON;
use Data::Dumper;
$| = 1;

use lib './lib';
use ABC::RabbitMQ::Batch;

my $should_stop = 0;
$SIG{INT} = \&signal_handler;
$SIG{TERM} = \&signal_handler;

my $rb = ABC::RabbitMQ::Batch->new('localhost', { user => 'guest', password => 'guest' });

while (!$should_stop) {
    my $messages = $rb->get(1, 'test_in', 10, { incomplete_timeout => 5 });
    print Dumper($messages);
}

$rb->close();
exit(0);
###

sub signal_handler {
    $should_stop = 1;
}