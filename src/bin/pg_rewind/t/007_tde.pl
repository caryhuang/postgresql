use strict;
use warnings;
use TestLib;
use Test::More tests => 5;

use FindBin;
use lib $FindBin::RealBin;

use RewindTest;

my $tde_params_ok = "echo password";
my $tde_params_ko = "echo wrong_password";

sub run_test
{
	my $test_mode = shift;

	RewindTest::setup_cluster($test_mode, ['-e','aes-128','-c', $tde_params_ok]);
	RewindTest::start_master();

	# Create a database in master with a table.
	master_psql('CREATE DATABASE inmaster');
	master_psql('CREATE TABLE inmaster_tab (a int)', 'inmaster');

	RewindTest::create_standby($test_mode);

	# Create another database with another table, the creation is
	# replicated to the standby.
	master_psql('CREATE DATABASE beforepromotion');
	master_psql('CREATE TABLE beforepromotion_tab (a int)',
		'beforepromotion');

	RewindTest::promote_standby();

	# Create databases in the old master and the new promoted standby.
	master_psql('CREATE DATABASE master_afterpromotion');
	master_psql('CREATE TABLE master_promotion_tab (a int)',
		'master_afterpromotion');
	standby_psql('CREATE DATABASE standby_afterpromotion');
	standby_psql('CREATE TABLE standby_promotion_tab (a int)',
		'standby_afterpromotion');

	# The clusters are now diverged.

	RewindTest::run_pg_rewind($test_mode, $tde_params_ok);

	# Check that the correct databases are present after pg_rewind.
	check_query(
		'SELECT datname FROM pg_database ORDER BY 1',
		qq(beforepromotion
inmaster
postgres
standby_afterpromotion
template0
template1
),
		'database names');
	
	RewindTest::clean_rewind_test();
	return;
}

# Run the test in both modes in TDE.
run_test('tde-local');
run_test('tde-remote');

exit(0);
