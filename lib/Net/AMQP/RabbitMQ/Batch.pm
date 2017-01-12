package Net::AMQP::RabbitMQ::Batch;

use strict;
use warnings;
use Carp qw(carp croak cluck);
use Carp::Assert;
use Try::Tiny;
use Net::AMQP::RabbitMQ;
use Time::HiRes qw(time);
use Data::Dumper;
our $VERSION = '0.2001';

=head1 NAME

Net::AMQP::RabbitMQ::Batch - simple batch processing of messages

=head1 SYNOPSIS

    my $rb = Net::AMQP::RabbitMQ::Batch->new('localhost', { user => 'guest', password => 'guest' }) or croak;
    $rb->process({
        channel_id => 1,
        queue_in   => 'test_in',
        queue_out  => 'test_out',
        handler    => \&msg_handler,
        batch      => { size => 10, timeout => 2, ignore_size => 0 }
    });

    sub msg_handler {
        my $messages = shift;
        # work with 10 messages
        return $messages;
    }

=cut

sub new {
    my ($class, $rabbit_hostname, $rabbit_options) = @_;
    croak('No hostname given') unless $rabbit_hostname;
    croak('No connection options given') unless $rabbit_options;

    return bless {
        mq => $class->_get_mq($rabbit_hostname, $rabbit_options),
    }, $class;
}

sub _get_mq {
    my ($class, $rabbit_hostname, $rabbit_options) = @_;
    my $mq = Net::AMQP::RabbitMQ->new();
    $mq->connect($rabbit_hostname, $rabbit_options) or croak;
    return $mq;
}

sub process {
    my ($self, $options) = @_;
    my $channel_id = $options->{channel_id} or croak('No channel_id given');
    my $queue_in = $options->{queue_in} or croak('No queue_in given');
    my $queue_out = $options->{queue_out} or croak('No queue_out given');
    my $handler = $options->{handler} or croak('No handler given');

    try {
        $self->{mq}->channel_open($channel_id);
        my $messages = $self->_get($channel_id, $queue_in, {no_ack => 0}, $options->{batch});
        my $processed_messages = undef;
        try {
            $processed_messages = &$handler($messages);
        } catch {
            cluck("Handler error: $_");
        };
        if ($self->_check_messages($messages, $processed_messages, $options)) {
            $self->_publish($processed_messages, $channel_id, $queue_out, $options->{publish_options}, $options->{publish_props});
            if ($options->{manual_ack}) {
                $self->_ack_messages($messages, $channel_id);
            } else {
                $self->_ack_messages($processed_messages, $channel_id);
            }
        } else {
            $self->_reject_messages($messages, $channel_id, 1); # explicitly requeue
        }
    } catch {
        croak("Error: $_");
    } finally {
        $self->{mq}->channel_close($channel_id); # all unacked messages are redelivered
    };
    return 1;
}

sub _get {
    my ($self, $channel_id, $queue, $mq_opts, $opts) = @_;
    assert($channel_id);
    assert($queue);
    $opts->{size} ||= 10;
    $opts->{timeout} ||= 5;
    $opts->{sleep} ||= 1;

    my $batch_activity_ts = time();
    my $messages = [];
    while (scalar(@$messages) < $opts->{size}) {
        my $msg = $self->{mq}->get($channel_id, $queue, $mq_opts);
        if ($msg) {
            $batch_activity_ts = time();
            push(@$messages, $msg);
        } else {
            if (time() - $batch_activity_ts > $opts->{timeout}) {
                last;
            } else {
                sleep($opts->{sleep});
            }
        }
    }
    return $messages;
}

sub _publish {
    my ($self, $messages, $channel_id, $queue, $mq_options, $mq_props) = @_;
    assert(ref($messages) eq 'ARRAY');
    assert($channel_id);
    assert($queue);

    foreach my $msg (@$messages) {
        if (defined $msg->{body}) {
            $self->{mq}->publish($channel_id, $queue, $msg->{body}, $mq_options, $mq_props);
        }
    }
    return;
}

sub _ack_messages {
    my ($self, $messages, $channel_id) = @_;
    assert(ref($messages) eq 'ARRAY');
    assert($channel_id);

    foreach my $msg (@$messages) {
        assert($msg->{delivery_tag});
        $self->{mq}->ack($channel_id, $msg->{delivery_tag});
    }
    return;
}

sub _reject_messages {
    my ($self, $messages, $channel_id, $requeue) = @_;
    assert(ref($messages) eq 'ARRAY');
    assert($channel_id);

    foreach my $msg (@$messages) {
        assert($msg->{delivery_tag});
        $self->{mq}->reject($channel_id, $msg->{delivery_tag}, $requeue); # requeue
    }
}

sub _check_messages {
    my ($self, $messages, $processed_messages, $options) = @_;
    assert(ref($messages) eq 'ARRAY');
    assert(ref($options) eq 'HASH');

    if (ref($processed_messages) ne 'ARRAY') {
        carp('Invalid handler output (expected ARRAYREF)');
        return 0;
    }
    if ($options->{manual_ack}) {
        foreach my $msg ($processed_messages) {
            if (!$msg->{delivery_tag}) {
                carp('No message delivery tag (it is required for manual_ack)');
                return 0;
            }
        }
    }
    if (!$options->{batch}->{ignore_size} && scalar(@$messages) != scalar(@$processed_messages)) {
        carp(sprintf('Numbers of incoming and processed messages do not match (expected %d, got %d). '
            . 'Discarding this batch', scalar(@$messages), scalar(@$processed_messages)));
        return 0;
    }
    return 1;
}

sub DESTROY {
    my $self = shift;
    $self->{mq}->disconnect();
    return;
}

1;
