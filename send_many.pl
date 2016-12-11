use strict;
use warnings;
use v5.18;
use Net::AMQP::RabbitMQ;
use JSON;
use Time::HiRes qw(sleep);
$| = 1;

my $CHANNEL_ID = 1;
my $QUEUE_NAME = 'test_in';
my $DELIVERY_MODE = 2;
my $MESSAGES_CNT = 10;
my $MAX_DELAY = 0.1;

my $mq = Net::AMQP::RabbitMQ->new();
$mq->connect('localhost', { user => 'guest', password => 'guest' });
$mq->channel_open($CHANNEL_ID);
my $json = JSON->new;

for (my $i = 0; $i < $MESSAGES_CNT; $i++) {
    my $msg_body = $json->encode({msg => sprintf("Message number %d", $i)});
    $mq->publish($CHANNEL_ID, $QUEUE_NAME, $msg_body, undef, {delivery_mode => $DELIVERY_MODE});
    print "Sent message: $msg_body\n";
    sleep(rand() * $MAX_DELAY);
}
printf "Sent %d messages\n", $MESSAGES_CNT;

$mq->channel_close($CHANNEL_ID);
$mq->disconnect();