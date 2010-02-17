use inc::Module::Install;
name 'Net-MPRPC-Client';
all_from 'lib/Net/MPRPC/Client.pm';

test_requires 'Test::More';
use_test_base;
author_tests('xt');

auto_include;
auto_set_repository;

WriteAll;
