package ABC::RabbitMQ::Batch;

use strict;
use warnings;
use Carp qw(carp croak cluck);
use Carp::Assert;
use Try::Tiny;
use Net::AMQP::RabbitMQ;
use Time::HiRes qw(time);
use Data::Dumper;
our $VERSION = '0.1';

=head1 NAME

ABC::RabbitMQ::Batch - simple batch processing of messages

=head1 SYNOPSIS

    my $rb = ABC::RabbitMQ::Batch->new('localhost', { user => 'guest', password => 'guest' }) or croak;
    $rb->process({
        channel_id => 1,
        queue_in   => 'test_in',
        queue_out  => 'test_out',
        handler    => \&msg_handler,
        batch      => { size => 10, timeout => 2 }
    });

    sub msg_handler {
        my $messages = shift;
        # work with 10 messages
        return $messages;
    }

=cut

sub new {
    my ($class, $rabbit_hostname, $rabbit_options) = @_;
    my $mq = Net::AMQP::RabbitMQ->new();
    $mq->connect($rabbit_hostname, $rabbit_options);
    my $self = bless {
        mq => $mq,
    }, $class;

    return $self;
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
        }
        catch {
            cluck("Handler error: $@");
        };
        if ($self->_check_messages($messages, $processed_messages)) {
            $self->_publish($processed_messages, $channel_id, $queue_out, $options->{publish_options}, $options->{publish_props});
            $self->_ack_messages($messages, $channel_id);
        }
    }
    catch {
        croak("Error: $@");
    }
    finally {
        $self->{mq}->channel_close($channel_id);
    };
    return 1;
}

sub _get {
    my ($self, $channel_id, $queue, $mq_opts, $opts) = @_;
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
        assert($msg->{body});
        $self->{mq}->publish($channel_id, $queue, $msg->{body}, $mq_options, $mq_props);
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

sub _check_messages {
    my ($self, $messages, $processed_messages) = @_;
    if (ref($processed_messages) ne 'ARRAY') {
        carp('Ivalid handler output');
        return 0;
    }
    if (scalar(@$messages) != scalar(@$processed_messages)) {
        carp('Number of incoming and processed messages does not match');
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