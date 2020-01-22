use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 4;

my $tempdir = TestLib::tempdir;
my $tde_params_ok = "echo password";
my $tde_params_ko = "echo wrong_password";

my $node;
my $startlsn;
my $endlsn;
my $xlogdir;
my $datadir;

sub init_and_start_tde_cluster
{
	$node = get_new_node('main');
	$node->init(enable_encryption => 1);

	$xlogdir = $node->data_dir."/pg_wal";
        $datadir = $node->data_dir;

	$node->start;
	$node->psql('postgres', 'CREATE TABLE test_table(x integer)');
        $node->psql('postgres',
                'INSERT INTO test_table(x) SELECT y FROM generate_series(1, 10) a(y);');
        $startlsn =
                $node->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn()');

        $node->psql('postgres',
                'INSERT INTO test_table(x) SELECT y FROM generate_series(1, 5000) a(y);');

        $endlsn =
                $node->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn()');
}

init_and_start_tde_cluster();

command_ok([ 'pg_waldump', '-c', $tde_params_ok, '-p', $xlogdir, '-D', $datadir, '-s', $startlsn, '-e', $endlsn, '-n', '100' ],
                'pg_waldump with correct cluster passphrase');

command_fails([ 'pg_waldump', '-c', $tde_params_ko, '-p', $xlogdir, '-D', $datadir, '-s', $startlsn, '-e', $endlsn, '-n', '100' ],
                'pg_waldump with incorrect cluster passphrase');

$node->append_conf('postgresql.conf', qq[cluster_passphrase_command = '$tde_params_ko']);
$node->reload;
$node->psql('postgres', 'select pg_rotate_encryption_key();');

command_ok([ 'pg_waldump', '-c', $tde_params_ko, '-p', $xlogdir, '-D', $datadir, '-s', $startlsn, '-e', $endlsn, '-n', '100' ],
                'pg_waldump with correct cluster passphrase after key rotation');

command_fails([ 'pg_waldump', '-c', $tde_params_ok, '-p', $xlogdir, '-D', $datadir, '-s', $startlsn, '-e', $endlsn, '-n', '100' ],
                'pg_waldump with incorrect cluster passphrase after key rotation');

$node->stop;
$node->teardown_node;
