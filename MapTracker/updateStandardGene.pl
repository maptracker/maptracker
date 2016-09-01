#!/stf/biobin/perl -w

BEGIN {
    # Needed to make my libraries available to Perl64:
    # use lib '/stf/biocgi/tilfordc/released';
    use lib '/stf/biocgi/tilfordc/patch_lib';
    # Allows usage of beta modules to be tested:
    my $prog = $0; my $dir = `pwd`;
    if ($prog =~ /working/ || $dir =~ /working/) {
        require lib;
        import lib '/stf/biocgi/tilfordc/perllib';
        $ENV{IS_BETA} = 1;
    } else {
        $ENV{IS_BETA} = 0;
    }
    # warn "$prog + $dir = $isbeta";
    $| = 1;
    print '';
    # print "Content-type: text/plain\n\n";
}

my $isbeta  = $ENV{IS_BETA};
my $VERSION = 
    ' $Id$ ';

use strict;
use BMS::BmsArgumentParser;
use BMS::MapTracker::StandardGeneHeavy;


=pod

alias usg maptracker/MapTracker/updateStandardGene.pl \
      -ageall '10 mar 2015' -fork 6

usg -rebuild 

usg -taxa 'homo sapiens' &
usg -taxa 'mus musculus' &
usg -taxa 'rattus norvegicus' &
usg -taxa 'bos taurus' &
usg -taxa 'macaca mulatta' &
usg -taxa 'canis familiaris' &
usg -taxa 'pan troglodytes' &
usg -taxa 'Caenorhabditis elegans' &
usg -taxa 'Influenza A virus' &
usg -taxa 'Human immunodeficiency virus 1' &
usg -taxa 'Human papillomavirus' &
usg -taxa 'Hepatitis C virus' &
usg -taxa '' &
usg -taxa '' &
usg -taxa '' &


Next day:

usg -blacklist -taxa 'homo sapiens'
usg -blacklist -taxa 'mus musculus'
usg -blacklist -taxa 'rattus norvegicus'
usg -blacklist -taxa 'bos taurus'
usg -blacklist -taxa 'macaca mulatta'
usg -blacklist -taxa 'canis familiaris'
usg -blacklist -taxa 'pan troglodytes'
usg -blacklist -taxa 'Caenorhabditis elegans'

=cut


my $args = BMS::BmsArgumentParser->new
    ( -nocgi      => $ENV{HTTP_HOST} ? 0 : 1,
      -mode       => 'convert',
      -progress   => 120,
      -verbose    => 1,
      -errormail  => 'charles.tilford@bms.com',
      -tiddlywiki => 'StandardGene' );



$args->shell_coloring( );

my $safe      = '1 Jan 2013';
my $limit     = $args->val(qw(limit));
my $vb        = $args->val(qw(vb verbose)) || 0;
my $dumpSql   = $args->val(qw(dumpsql));
my $isTrial   = $args->val(qw(trial istrial));
my $reBuild   = $args->val(qw(build rebuild));
my $progress  = $vb ? $args->val(qw(prog progress)) : 0;
my $age       = $args->val(qw(age ageall)) || $safe;
my $cAge      = $args->val(qw(cloudage ageall)) || $safe;
my $taxReq    = $args->val(qw(tax taxa species));
my $dbname    = $args->val(qw(dbname db)) || $isbeta ? 'sgbeta' : 'stndgene';
my $doBlack   = $args->val(qw(blacklist));

my $sgh = BMS::MapTracker::StandardGeneHeavy->new
    ( -rebuild  => $reBuild,
      -age      => $age,
      -cloudage => $cAge,
      -dbname   => $dbname,
      -clobber  => $args->val(qw(clobber)),
      -verbose  => $vb, );

$sgh->dbh->dumpsql( $args->val(qw(dumpsql sqldump)) );
$sgh->dbh->no_environment( 1 );

if (my $bench = $args->val(qw(showbench bench benchmark))) {
    my $f = $bench < 1 ? $bench : $bench / 100;
    print $sgh->show_bench( -shell => 1, -minfrac => $f );
}

&load_tax($taxReq);
&blacklist();



sub load_tax {
    my $tax = shift;
    return if (!$tax || $doBlack);
    $sgh->load_taxa
        ( -taxa     => $tax,
          -fork     => $args->val(qw(fork)),
          -basic    => $args->val(qw(basiconly basic)),
          -progress => $progress,
          -limit    => $limit, );
    $doBlack = 1;
}

sub blacklist {
    return unless ($doBlack);
    my @params = 
        ( -number     => $args->val(qw(number)) || 100,
          -fraction   => $args->val(qw(fraction)),
          -taxa       => $taxReq,
          -genesource => $args->val(qw(genesource)),
          -wordsource => $args->val(qw(wordsource)),
          -limit      => $limit,
          -dumpsql    => $dumpSql,
          -verbose    => $vb,
          -trial      => $isTrial );
    $sgh->blacklist( @params );
    if (my $wsid = $sgh->get_wordSource('Text Fragments')) {
        # Force blacklist of RefSeq prefixes, which get isolated by
        # splitting on _
        my @force = qw(NM NR XM XR NP XP NT NC NG AC AP NW NZ YP ZP NS);
        $sgh->blacklist( @params,
                         -force => \@force,
                         -wsid  => $wsid);
    }
    
}
