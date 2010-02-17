use inc::Module::Install;
name 'Net-MPRPC-Client';
all_from 'lib/Net/MPRPC/Client.pm';

requires 'Data::MessagePack' => '0.08';
requires 'IO::Select';
requires 'IO::Socket::INET';
requires 'IO::Socket::UNIX';
requires 'Try::Tiny';

test_requires 'Test::More';
author_tests('xt');

auto_include;
auto_set_repository;

WriteAll;
