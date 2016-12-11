package ABC::RabbitMQ::Batch;

use strict;
use warnings;
use Net::AMQP::RabbitMQ;
use Time::HiRes qw(time);

sub new {
    my ($class, $rabbit_hostname, $rabbit_options) = @_;
    my $mq = Net::AMQP::RabbitMQ->new();
    $mq->connect($rabbit_hostname, $rabbit_options);
    my $self = bless {
        mq => $mq,
    }, $class;

    return $self;
}

sub get {
    my ($self, $channel_id, $queue, $batch_size, $opts) = @_;
    $opts->{incomplete_timeout} ||= 5;
    $opts->{sleep} ||= 1;

    $self->{mq}->channel_open($channel_id);
    my $i = 0;
    my $batch_activity_ts = time();
    my $messages = [];
    while ($i < $batch_size) {
        my $msg = $self->{mq}->get($channel_id, $queue, {no_ack => 0});
        if ($msg) {
            $batch_activity_ts = time();
            $i++;
            push(@$messages, $msg);
        } else {
            if (time() - $batch_activity_ts > $opts->{incomplete_timeout}) {
                last;
            } else {
                sleep($opts->{sleep});
            }
        }
    }
    foreach my $msg (@$messages) {
        $self->{mq}->ack($channel_id, $msg->{delivery_tag});
    }
    $self->{mq}->channel_close($channel_id);
    return $messages;
}

sub close {
    my $self = shift;
    $self->{mq}->disconnect();
}

sub DESTROY {
    my $self = shift;
    $self->close();
}

1;