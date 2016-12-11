# Perl Rabbit

Experiments with RabbitMQ: batch processing of messages.

## Synopsis

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

## Description

Assume you have two RabbitMQ queues - *in* and *out*. You need to get messages from *in*, process them somehow and put them to *out*. But you would like to do it in batches, processing many messages at once.

This module:

 * gets messages from in queue and publish them to out queue
 * uses your handler to batch process messages
 * keeps persistency - if processing fails, nothing lost from in queue, nothing published to out queue

## Prerequisites

* Net::AMQP::RabbitMQ;
* Carp::Assert;
* Try::Tiny;

## Usage

Define messages handler:

    sub msg_handler {
        my $messages = shift;
        # work with hashref of messages
        return $messages;
    }

* `$messages` is an arrayref of message objects:
```
    {
      body => 'Magic Transient Payload', # the reconstructed body
      routing_key => 'nr_test_q',        # route the message took
      delivery_tag => 1,                 # (used for acks)
      ....
      # Not all of these will be present. Consult the RabbitMQ reference for more details.
      props => { ... }
    }
```
* `return` arrayref of message objects (only `body` is required):
```
    { body => 'Processed message' }
```

Connect to RabbitMQ:

        my $rb = ABC::RabbitMQ::Batch->new('localhost', { user => 'guest', password => 'guest' }) or croak;

Process a batch:

    $rb->process({
        channel_id => 1,
        queue_in   => 'test_in',
        queue_out  => 'test_out',
        handler    => \&msg_handler,
        batch      => { size => 10, timeout => 2 }
    });

You might like to wrap it with some `while(1) {...}` loop. See `process_in_batches.pl` for example.

## Known Issues

* Not a CPAN module yet
* Can not set infinity timeout (use very long int)
* Only very simple in queue -> out queue processing
* No POD
* No tests yet which is very sad

# License

MIT