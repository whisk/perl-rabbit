#!/bin/bash

perl -MPod::Markdown -e 'Pod::Markdown->new->filter(@ARGV)' lib/Net/AMQP/RabbitMQ/Batch.pm > README.md
