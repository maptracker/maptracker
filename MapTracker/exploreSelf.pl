#!/stf/sys64/bin/perl -w

BEGIN {
    # Needed to make my libraries available to Perl64:
    use lib '/stf/biocgi/tilfordc/released';
    # Allows usage of beta modules to be tested:
    my $prog = $0;
    my $pwd = `pwd`;
    if ($prog =~ /working/ || $pwd =~ /working/) {
	warn "\n*** THIS VERSION IS BETA SOFTWARE ***\n\n";
	require lib;
	import lib '/stf/biocgi/tilfordc/perllib';
    }
    require lib;
    import lib '/stf/biocgi/tilfordc/patch_lib';
}

my $VERSION = ' $Id$ ';

use strict;
use BMS::Branch;
use BMS::CommonCAT;
use BMS::MapTracker::LoadHelper;
use BMS::BmsArgumentParser;

my $debug = BMS::Branch->new( -skipkey => [],
                              -fh      => *STDERR,
			      -format  => 'text', );

my $args = BMS::BmsArgumentParser->new
    ( -nocgi    => $ENV{'HTTP_HOST'} ? 0 : 1,
      -cache    => 20000,
      -testmode => 1,
      -duration => 60,
      -sumfile  => '/work5/tilfordc/maptracker/DB_Stats.txt',
      -basedir  => '/work5/tilfordc/maptracker/rapidload/',
      );

$args->shell_coloring( );

my $lh = BMS::MapTracker::LoadHelper->new
    ( -username => 'tilfordc',
      -testmode => $args->{TESTMODE},
      -basedir  => $args->{BASEDIR},
      );


my $start    = time;
my $duration = $args->{DURATION};
my $vb       = $args->val(qw(verbose vb));
my $mt       = $lh->tracker;
my $dbi      = $mt->dbi;
my $edgemax  = $dbi->lastval('edge_seq');
my $dntype   = $mt->get_type('DENORM')->id;
my $bulksize = $args->val(qw(bulk bulksize)) || 40;
my $bulkList = join(",", map { '?' } (1..$bulksize));

my $getedge  = $dbi->prepare
    ( -name  => "Sample edges from database",
      -level => 3,
      -sql   =>
      "SELECT name1, name2, type_id FROM edge".
      " WHERE edge_id IN ( $bulkList ) AND live = 't' AND type_id != $dntype" );


srand ( time() ^ ($$ + ($$<<15)) );

my $tinfo = $mt->type_information;

my @okself = qw(PRIMARYACC ALIAS OVERLAPS CONTAINS HOMOLOGOUS HOMOLOGUE ORTHOLOGUE MAPONTOLOGY CHILDOF SAMEAS PHYSICAL GENETIC SIMILAR REPORTSTO PARALOGUE PARTOF COREGULATED ANTIREGULATED LEXICAL DEPRECATEDFOR TENUOUS RELIABLE ISOMER DIFFREG DENORM);

foreach my $tok (@okself) {
    my $type = $mt->get_type($tok);
    $tinfo->{$type->id}{SELF_OK} = 1;
}

my (%specificity, %fastc, %cParents);
foreach my $class ($mt->get_all_classes) {
    my $name = $class->name;
    $specificity{ $name } = $class->specificity;
    $fastc{ $name } = $class;
    # $cParents{ $name } = [ map { $_->name } $class->me_and_the_folks() ];
    
}


my %observed;
my %seen_ids;
my ($edges) = (0,0);
my %recovered;
while (time - $start < $duration) {
    my @eids;
    while ($#eids + 1 < $bulksize) {
        my $id = int( rand() * $edgemax );
        push @eids, $id unless ( $seen_ids{ $id }++ );
    }

    my $got = $getedge->selectall_arrayref( @eids );
    $args->msg("[+]", ($#{$got}+1)." edges from ".($#eids+1)." queries")
        if ($vb);

    foreach my $row ( @{$got} ) {
        my ($sid1, $sid2, $tid) = @{$row};
        my @ids = ($sid1, $sid2);
        my @pair;
        for my $i (0..1) {
            my $seq = $mt->get_seq($ids[$i]);
            my @classes = map {$_->me_and_the_folks} $seq->each_class;
            my %cnames  = map { $_->name => 1 } @classes;
            delete $cnames{Unknown};
            my $nametag = join(",", sort keys %cnames);
            $pair[$i] = $nametag;
        }
        next unless ($pair[0] && $pair[1]);
        my $key = join("\t", @pair);
        $recovered{$tid}{edges}{$key}++;
        $recovered{$tid}{count}++;
        $edges++;
    }
}
$args->msg("[#]","Total of $edges edges recoverd") if ($vb);

my %final;
my @tids = sort { $a <=> $b } keys %recovered;
my $min_count  = 5;
my $min_ratio  = 0.01;
my $spec_bonus = 0.05;
my $summary    = "$edges edges were analyzed\n";
foreach my $tid (@tids) {
    my $total = $recovered{$tid}{count};
    my $tdat  = $tinfo->{$tid};
    my $issym = $tdat->{SYM};
    my $sbad  = !$tdat->{SELF_OK};
    my @allk  = keys %{$recovered{$tid}{edges}};
    my $reads = $tdat->{FOR};
    $summary .= sprintf("%s [%d]\n", $reads, $total);
    while ($#allk > -1) {
        # What is the best edge to represent the majority of these data?
        my %flat;
        foreach my $key (@allk) {
            my $count = $recovered{$tid}{edges}{$key};
            my ($lkey, $rkey) = split(/\t/, $key);
            my @lft = split(/\,/, $lkey);
            my @rgt = split(/\,/, $rkey);
            my %pairs;
            foreach my $cn1 (@lft) {
                foreach my $cn2 (@rgt) {
                    next if ($cn1 eq $cn2 && $sbad);
                    my @pair = ($cn1, $cn2);
                    @pair = sort @pair if ($issym);
                    $pairs{ join("\t", @pair) }++;
                }
            }
            foreach my $pkey (keys %pairs) {
                $flat{$pkey} += $count;
            }
        }
        my @ranked;
        while (my ($pkey, $count) = each %flat) {
            # Ignore classes that encompass too few examples
            next if ($count < $min_count);
            my $ratio = $count / $total;
            # Ignore classes that explain a trival number of examples
            next if ($ratio < $min_ratio);
            my ($cn1, $cn2) = split(/\t/, $pkey);
            my $spec = $specificity{$cn1} + $specificity{ $cn2 };
            push @ranked, [ $count, $spec, $pkey ];
        }
        last if ($#ranked < 0);

        @ranked = sort { $b->[0] <=> $a->[0] || $b->[1] <=> $a->[1] } @ranked;
        my $best = 0;
        for my $i (1..$#ranked) {
            # How many fewer entries does this entry have?
            my $count_diff = $ranked[$best][0] - $ranked[$i][0];
            # How much more specific is this entry?
            my $spec_diff  = $ranked[$i][1] - $ranked[$best][1];
            # Generate a bonus for higher specificity
            my $bonus = $spec_bonus * $spec_diff * $ranked[$i][0];
            if ($count_diff < $bonus) {
                my ($old, $new) = ($ranked[$best], $ranked[$i]);
                $summary .= sprintf
                    ("  Prefering '%s' to '%s' (%d+%d vs %d+%d)\n", 
                     $new->[2], $old->[2], 
                     $new->[0], $new->[1], $old->[0], $old->[1]);
                $best = $i;
            }
        }
        my ($cn1, $cn2) = split(/\t/, $ranked[$best][2]);
        my ($count, $spec) = @{$ranked[$best]};

        $final{"$cn1\t$cn2\t$tid"} = 
            [ 'Edge Representation', &rounded($count / $total, 5),
              'Database Representation', &rounded($count / $edges, 5), ];
        my ($fc1, $fc2) = map { $fastc{$_} } ($cn1, $cn2);

        # Strip out entries that are encapsulated by what we just found
        my @survivors;
        foreach my $key (@allk) {
            my ($lkey, $rkey) = split(/\t/, $key);
            my @lc = map { $fastc{$_} } split(/\,/, $lkey);
            my @rc = map { $fastc{$_} } split(/\,/, $rkey);
            # This entry is a more specific child of what was kept:
            next if ($fc1->has_child(@lc) && $fc2->has_child(@rc));
            next if ($issym && $fc2->has_child(@lc) && $fc1->has_child(@rc));
            
            push @survivors, $key;
        }
        @allk = @survivors;
        # print join("\t", $cn1, $reads, $cn2, $count, $spec, $total) . "\n";
        $summary .= sprintf("    %s --> %s [ %d + %d ] %d remain\n", 
                            $cn1, $cn2, $count, $spec, $#allk + 1);
    }
}

my %classes;
while (my ($key, $tags) = each %final) {
    my ($cn1, $cn2, $tid) = split(/\t/, $key);
    map { $classes{$_}++ } ( $cn1, $cn2 );
    $lh->set_edge( -name1      => '#CLASSES#' . $cn1,
                   -name2      => '#CLASSES#' .  $cn2,
                   -type       => '#DB_Stats#' . $tid,
                   -allow_self => 1,
                   -tags       => $tags );
}

foreach my $cn (keys %classes) {
    $lh->kill_edge( -name1 => '#CLASSES#' . $cn,
                    -space => 'db_stats' );
    $lh->kill_edge( -name2 => '#CLASSES#' . $cn,
                    -space => 'db_stats' );
    $lh->set_class( '#CLASSES#' . $cn, $cn );
}

#$lh->data_content();
$lh->write();

$lh->process_ready() if ($args->{BASEDIR});

my $sfile = $args->{SUMFILE};
open(FILE, ">$sfile") || die "Could not write to '$sfile':\n  $!\n  ";
print FILE $summary;
close FILE;

sub rounded {
    my ($val, $sigfig) = @_;
    my $round = 10 ** $sigfig;
    return int(0.5 + ($round * $val)) / $round;
}
