RABBITMQADMIN_PORT = 15672

Vagrant.configure(2) do |config|
  config.vm.box = 'bento/ubuntu-16.04'
  config.vm.provider 'virtualbox' do |vb|
    vb.memory = '2048'
    vb.cpus   = 2
  end

  config.vm.network 'forwarded_port',
                    guest: RABBITMQADMIN_PORT,
                    host: RABBITMQADMIN_PORT

  config.vm.provision 'base', type: 'shell', inline: <<-SCRIPT
    export DEBIAN_FRONTEND=noninteractive
    echo 'deb http://www.rabbitmq.com/debian/ testing main' | \
      tee /etc/apt/sources.list.d/rabbitmq.list
    wget -O- https://www.rabbitmq.com/rabbitmq-release-signing-key.asc |
      apt-key add -
    apt-get update && apt-get install -y rabbitmq-server perl librabbitmq-dev

    # Rabbit
    rabbitmq-plugins enable rabbitmq_management
    wget -O /usr/local/bin/rabbitmqadmin "http://localhost:15672/cli/rabbitmqadmin" && \
      chmod a+x /usr/local/bin/rabbitmqadmin

    # Rabbit users
    rabbitmqctl add_user root NTQ4YzZiZjJjMmVh && \
      rabbitmqctl set_user_tags root administrator && \
      rabbitmqctl set_permissions -p / root ".*" ".*" ".*"

    # Perl
    export PERL_MM_USE_DEFAULT=1
    cpan -i Variable::Magic YAML Log::Log4perl
    cpan -i LWP::UserAgent # for Net::AMQP::RabbitMQ, optional
    cpan -i -f Net::AMQP::RabbitMQ # WARN: some tests fails
  SCRIPT

  config.vm.provision 'rabbit-abc', type: 'shell', inline: <<-SCRIPT
    export PERL_MM_USE_DEFAULT=1
    cpan -i JSON
    rabbitmqadmin declare queue name=test_in
    rabbitmqadmin declare queue name=test_out
  SCRIPT
end
