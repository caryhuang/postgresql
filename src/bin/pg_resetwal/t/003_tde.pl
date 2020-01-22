use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 4;

my $tempdir = TestLib::tempdir;
my $tde_params_ok = "echo password";
my $tde_params_ko = "echo wrong_password";

my $node;
my $xlogdir;
my $datadir;

sub init_tde_cluster
{
	$node = get_new_node('main');
	$node->init(enable_encryption => 1);

	$xlogdir = $node->data_dir."/pg_wal";
        $datadir = $node->data_dir;
}

init_tde_cluster();

command_ok([ 'pg_resetwal', '-C', $tde_params_ok, '-D', $datadir ],
                'pg_resetwal with correct cluster passphrase');

command_fails([ 'pg_resetwal', '-C', $tde_params_ko, '-D', $datadir ],
                'pg_resetwal with incorrect cluster passphrase');

$node->start;
$node->append_conf('postgresql.conf', qq[cluster_passphrase_command = '$tde_params_ko']);
$node->reload;
$node->psql('postgres', 'select pg_rotate_encryption_key();');
$node->stop;

command_ok([ 'pg_resetwal', '-C', $tde_params_ko, '-D', $datadir ],
                'pg_resetwal with correct cluster passphrase after key rotation');

command_fails([ 'pg_resetwal', '-C', $tde_params_ok, '-D', $datadir ],
                'pg_resetwal with incorrect cluster passphrase after key rotation');

$node->teardown_node;
