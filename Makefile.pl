use ExtUtils::MakeMaker;

WriteMakefile(
    NAME            => 'ABC::RabbitMQ::Batch',
    AUTHOR          => 'Alex Svetkin',
    LICENSE         => 'MIT',
    VERSION_FROM    => 'lib/ABC/RabbitMQ/Batch.pm',
    ABSTRACT_FROM   => 'lib/ABC/RabbitMQ/Batch.pm',
    PREREQ_PM       => {
        'Carp'                => 0,
        'Carp::Assert'        => 0,
        'Try::Tiny'           => 0,
        'Time::HiRes'         => 0,
        'Net::AMQP::RabbitMQ' => 0
    },
    TEST_REQUIRES   => {
        'Test::Simple' => 0
    }
)