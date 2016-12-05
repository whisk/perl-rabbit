Vagrant.configure(2) do |config|
  config.vm.box = 'bento/ubuntu-16.04'
  config.vm.provider 'virtualbox' do |vb|
    vb.memory = '2048'
    vb.cpus   = 2
  end

  config.vm.provision 'base', type: 'shell', inline: <<-SCRIPT
    export DEBIAN_FRONTEND=noninteractive
    echo 'deb http://www.rabbitmq.com/debian/ testing main' | \
      tee /etc/apt/sources.list.d/rabbitmq.list
    wget -O- https://www.rabbitmq.com/rabbitmq-release-signing-key.asc |
      apt-key add -
    apt-get update && apt-get install -y rabbitmq-server
  SCRIPT
end
