#!/stf/biobin/perl -w

BEGIN {
    # Needed to make my libraries available to Perl64:
    # use lib '/stf/biocgi/tilfordc/released';
    # Allows usage of beta modules to be tested:
    my $prog = $0; my $dir = `pwd`;
    if ($prog =~ /working/ || $dir =~ /working/) {
	require lib;
	import lib '/stf/biocgi/tilfordc/perllib';
    }
    $| = 1;
    print '';
}

my $VERSION = 
    ' $Id$ ';

use strict;
use BMS::MapTracker::LoadHelper;
use BMS::CommonCAT;

=head1 Overview

Recursively run a loader on a directory until empty:

 mapTrackerLoader.pl -empty -basedir /work5/tilfordc/maptracker/SMILES &

Repeat a load on a directory 50 times, every 5 minutes

 mapTrackerLoader.pl -repeat 50 -sleep 300 -basedir /work5/tilfordc/maptracker/SMILES &

=head1 Options

=head2 Loading Options

     -sleep Default 0. The amount of time to sleep after checking for
            tasks. If non-zero and -repeat or -empty are not provided, then
            the loader will run forever.

    -repeat Default 0. A fixed number of times for the loader to check for
            tasks. Between checks, it will sleep for -sleep seconds,
            or 5 minutes if -sleep is zero.

     -empty Default 0. If non-zero, the the loader will keep checking
            for tasks until the load directory is empty. Between
            checks, it will sleep for -sleep seconds, or 5 minutes if
            -sleep is zero.

   -basedir Default '' (the default load directory). Optional path to
            a non-standard load directory.

=head2 Alternative Modes

    -vacuum Requests a vacuum of part or all of the database. If no
            arguments (or 'all') are passed, then all tables will be
            vacuumed. Otherwise, the argument will be treated as a
            whitespace/comma separated list of tables to vacuum.

      -full Used with -vacuum, will trigger a full vacuum if true.

   -analyze Similar to vacuum, but analyzes the requested tables.

=head2 Verbosity / Debugging

 -benchmark Shows benchmarks when program finishes

  -progress Time (seconds) between progress notes

   -dumpsql Default 0. Values 1-3 report increasingly verbose SQL
            queries. Generally not recommended.

=cut

my $args =  &PARSEARGS
    ( -cautious   => undef,
      -prefix     => undef,
      -vacuum     => undef,
      -nocgi      => 1,
      -cautious   => 1,
      -sleep      => 0,
      -dumpsql    => 0,
      -limit      => 100,
      -time       => 60 * 60 * 5, 
      -evilmalloc => 0,
      @_ );

my $mt = BMS::MapTracker->new( -username => 'tilfordc',
                               -dumpsql  => $args->{DUMPSQL}, );
my $basedir = $args->{BASEDIR} || $args->{LOADDIR};

my $lh = BMS::MapTracker::LoadHelper->new
    ( -username  => 'tilfordc',
      -tracker   => $mt,
      -loadtoken => $args->{TOKEN} || $args->{LOADTOKEN},
      -testmode  => $args->{TESTMODE},
      -basedir   => $basedir );


my $tab;
my $doBench = $args->{BENCHMARK} || $args->{SHOWBENCH};
if ($args->{VACUUM}) {
    &vacuum( $args->{VACUUM} );
} elsif ($args->{ANALYZE}) {
    &analyze( $args->{ANALYZE} );
} elsif (my $cdir = $args->{CONCAT} || $args->{CONCATENATE} || 
         $args->{COLLECT}) {
    &concatenate( $cdir );
} elsif ($args->{CLEAN}) {
    $lh->delete_duplicates;
} else {
    my $repeat = $args->{REPEAT} || 0;
    my $st     = $args->{SLEEP}  || 0;
    my $empty  = $args->{EMPTY}  || 0;
    while (1) {
        my $retval = $lh->process_ready( -benchmark => $doBench );
        if ($empty) {
            # Continue processing until the directory is empty
            my $cmd = "du -s ".$lh->{READYDIR};
            my $sz  = `$cmd`;
            unless ($sz =~ /^\s*0/) {
                $lh->benchstart('Sleep for empty');
                sleep($st || 600);
                $lh->benchend('Sleep for empty');
                next;
            }
        } elsif ($repeat) {
            # Repeat process for R number of times
            if (--$repeat > 0) {
                $lh->benchstart('Sleep for repeat');
                sleep($st || 600);
                $lh->benchend('Sleep for repeat');
                next;
            }
        } elsif ($st) {
            # Repeat indefinitely
            $lh->benchstart('Sleep forever');
            sleep($st);
            $lh->benchend('Sleep forever');
            next;
        }
        last;
    }
}

warn $lh->showbench() if ($doBench);

sub concatenate {
    my ($dir) = @_;
    return unless ($dir && -d $dir);
    my $llh = BMS::MapTracker::LoadHelper->new( -username => 'tilfordc',
                                                -basedir  => $dir );
    $llh->collect_files( %{$args} );
}

sub vacuum {
    my @tables = &get_tables( @_ );
    foreach my $table (@tables) {
        $table = 'all' unless ($table);
        $lh->lock_task("Vacuum $table");
        my $sql = "VACUUM";
        $sql   .= ' FULL' if ($args->{FULL});
        # $sql   .= ' ANALYZE';
        my $what = lc($sql);
        $sql .= " $table" unless ($table eq 'all');
        my $token = ($table eq 'all') ? "All Tables" : "Table $table";
        $mt->_showSQL($sql, "Vacuum of $token - tiddy database") 
            if ($mt->{DUMPSQL} > 2);
        my $start = time;
        $mt->dbi->do($sql);
        $lh->_log("$token - $what ", undef, time - $start);
        $lh->unlock_task("Vacuum $table");
    }
}

sub analyze {
    my @tables = &get_tables( @_ );
    foreach my $table (@tables) {
        $table = 'all' unless ($table);
        my $sql = "ANALYZE";
        $sql .= " $table" unless ($table eq 'all');
        my $token = ($table eq 'all') ? "All Tables" : "Table $table";
        $mt->_showSQL($sql, "Analysis of $token - improve indices") 
            if ($mt->{DUMPSQL} > 2);
        my $start = time;
        $mt->dbi->do($sql);
        $lh->_log("$token - analyze ", undef, time - $start)
    }
}

sub get_tables {
    my ($req) = @_;
    my @alltables = 
        qw(authority class_list namespace load_status searchdb transform
           species species_alias
           location mapping seq_class seq_length seq_species
           seqname edge edge_meta edge_auth_hist);
    my @tables = ('all');
    @tables = split(/[\s\t\,\n\r]/, lc($req)) if ($req && $req ne '1');
    return @tables;
}
