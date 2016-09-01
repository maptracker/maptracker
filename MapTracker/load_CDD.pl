#!/stf/biobin/perl -w

BEGIN {
    # Needed to make my libraries available to Perl64:
    use lib '/stf/biocgi/tilfordc/released';
    # Allows usage of beta modules to be tested:
    my $prog = $0;
    my $pwd = `pwd`;
    if ($prog =~ /working/ || $pwd =~ /working/) {
	print "\n*** THIS VERSION IS BETA SOFTWARE ***\n\n";
	require lib;
	import lib '/stf/biocgi/tilfordc/perllib';
    }
    $| = 1;
    print '';
}
my $VERSION = ' $Id$ ';

use strict;
use BMS::BmsArgumentParser;
use BMS::MapTracker::LoadHelper;
# use BMS::MapTracker::Shared;
use BMS::MapTracker::PopulateByGenbank;

my $args = BMS::BmsArgumentParser->new
    ( -wget     => "/work5/tilfordc/WGET/cdd_cmd.sh",
      -verbose  => 1,
      -testmode => 1,
      -cache    => 5000,
      );

$args->shell_coloring();

my $wcmd    = $args->{WGET} || '-UNDEF-';
my $vb      = $args->{VERBOSE};
my $tm      = $args->{TESTMODE};
my $limit   = $args->{LIMIT};
my $cache   = $args->{CACHE};
my $onlynew = $args->val(qw(newonly onlynew));
my $basedir = $args->val(qw(basedir loaddir));

my $lh      = BMS::MapTracker::LoadHelper->new
    ( -testmode => $tm, 
      -testfile => $args->val(qw(testfile)),
      -basedir  => $basedir,
      -username => 'NCBI' );

my $mt      = $lh->tracker;
my $pbg     = BMS::MapTracker::PopulateByGenbank->new( -loadhelper => $lh );

$pbg->warn_on_null_query( 0 );
unless (-e $wcmd) {
    die "I could not find the wget shell script '$wcmd'\n  ";
}

my %giLookup;

# Parse file to figure out where stuff is going
my %vars;
open (PW, "<$wcmd") || die "Failed to read wget shell '$wcmd':\n  $!\n  ";
while (<PW>) {
    if (/^\s*([A-Z]+)\s*\=\s*(\S+)/) {
        my ($key, $val) = ($1, $2);
        if ($val =~ /^\'(.+)\'$/ || $val =~ /^\"(.+)\"$/) {
            $val = $1;
        }
        $vars{$key} = $val;
    }
}

foreach my $key qw(TARGDIR SOURCEDIR LOGFILE) {
    $args->death("Could not parse the $key variable from file",
                 $wcmd) unless ($vars{$key});
}

my $log  = $vars{TARGDIR} .'/'. $vars{LOGFILE};
my $dir  = $vars{TARGDIR} .'/'. $vars{SOURCEDIR};

unless ($args->{NOWGET} || $args->{NOGET}) {
    $args->msg("Updating CDD data from FTP site",$wcmd,`date`);
    system($wcmd);
    $args->msg("  ","Done - " . `date`);
}

$args->death("Failed to find the wget log file", $log) unless (-e $log);

my %wgetFiles;
open (LOG, "<$log") || die "Failed to read wget log '$log'\n  $!\n  ";
while(<LOG>) {
    if (/ \-\s+(.+)\s+(saved)/ ||
        /no newer than local file\s+(.+)\s+-- (not) retrieving/) {
        my ($full, $state) = ($1, $2);
        $full    =~ s/\P{IsASCII}//g;
        $full    =~ s/^[\`\'\"]//; 
        $full    =~ s/[\`\'\"]$//; 
        my @path = split(/\//, $full);
        $wgetFiles{ $path[-1] } = {
            path => $full,
            new  => $state eq 'saved' ? 1 : 0,
        };
    }
}
close LOG;

my $termMap;

if ($args->{FORCE} || $wgetFiles{'cddid.tbl.gz'}{new}) {
    $termMap = &parse_cdd_terms( $wgetFiles{'cddid.tbl.gz'}{path});
}

if ($termMap) {
    &parse_fasta_directory( "$dir/fasta", $termMap );
}

$lh->write();

if (my $arr = $giLookup{-1}) {
    $args->msg("A total of ".( $#{$arr} +1)." GI ids could not be found");
}

$args->msg("Data are awaiting loading in:", $basedir) if ($basedir && !$tm);

sub parse_cdd_terms {
    my ($file) = @_;
    return unless ($file);
    $args->msg("Parsing CDD terminology");
    if ($file =~ /\.gz/) {
        open(FILE, "gunzip -c $file|") ||
            die "Failed to gunzip CDD file '$file':\n  $!\n  ";
    } else {
        open(FILE, "<$file") || 
            die "Failed to read CDD file '$file':\n  $!\n  ";
    }
    my %termMap;
    my $counter = 0;
    my $setName = "Conserved Domain Database";
    $lh->set_class($setName, 'Collection');
    while (<FILE>) {
        chomp;
        my ($cdid, $oid, $token, $desc, $len) = split(/\t/, $_);
        my $cdd = "CDD:$cdid";
        $lh->set_edge( -name1 => $cdd,
                       -name2 => $setName,
                       -type  => 'MEMBEROF' );
        
        $lh->set_class($cdd, 'cdd');
        $lh->set_length($cdd, $len);
        $lh->set_edge( -name1 => $cdd,
                       -name2 => $oid,
                       -type  => 'MAPONTOLOGY' );
        $lh->set_edge( -name1 => $cdd,
                       -name2 => "#FreeText#$token",
                       -type  => 'SHORTFOR' ) unless ($token =~ /^\d+$/);
        $lh->set_edge( -name1 => $cdd,
                       -name2 => "#FreeText#$desc",
                       -type  => 'COMMENT' );
        $counter++;
        push @{$termMap{ $oid }}, $cdd;
        if ($limit && $counter >= $limit) {
            last;
        }
        $lh->write_threshold_quick( $cache );
    }
    close FILE;
    return \%termMap;
}

sub parse_fasta_directory {
    my ($dir, $termMap) = @_;
    my @doms = sort keys %{$termMap};
    my $dnum = $#doms+1;
    $args->msg("Parsing sequence assignments for $dnum CDD domains");
    my $counter = 0;
    my $start = time;
    my $last  = $start;
    foreach my $domain (@doms) {
        if ($#{$termMap->{$domain}} > 0) {
            $args->msg("$domain has multiple CDD assignments:",
                       join(", ", @{$termMap->{$domain}}));
            next;
        }
        my $cdd = $termMap->{$domain}[0];
        my $file = "$dir/$domain.FASTA";
        unless (-e $file) {
            $args->msg("Could not find alignment file $file");
            next;
        }
        &parse_fasta_file( $file, $cdd );
        $counter++;
        if (time - $last > 120) {
            my $dt = `date`; chomp $dt;
            $args->msg("   ",sprintf("%5d entries done [%s]", $counter, $dt));
            $last = time;
        }
    }
}

sub parse_fasta_file {
    my ($file, $cdd) = @_;
    my @seqs;
    my $data;
    open(FILE, "<$file") || die "Failed to read fasta file '$file':\n  $!\n  ";
    # $args->msg("Parsing fasta file", $file);
    while (<FILE>) {
        s/[\n\r]+$//;
        if (/^\>([^ ]+) ?(.*)/) {
            my ($head, $desc) = ($1, $2);
            push @seqs, $data if ($data);
            my @bits = split(/\|/, $head);
            my $tag = shift @bits;
            my $id  = shift @bits;
            $data = {
                tag  => $tag  || "",
                id   => $id   || "",
                desc => $desc || '',
                seq  => '',
            };
            my ($dbt, $rel) = @bits;
            if ($dbt && $rel) {
                $data->{other} = join(":", @bits);
                if ($dbt eq 'pdb') {
                    if ($rel =~ /^[A-Z0-9]{4}$/) {
                        $rel .= '_' . $bits[2] if ($bits[2]);
                        $data->{pdb} = $rel;
                    } else {
                        $args->msg("Malformed PDB ID: $id = $rel",$file);
                    }
                } elsif ($dbt eq 'sp') {
                    if ($rel =~ /^([OPQ]\d[A-Z\d]{3}\d|[A-NR-Z]\d[A-Z][A-Z\d]{2}\d)(\-\d+)?(\.\d+)?$/) {
                        $data->{swissprot} = $rel;
                    } else {
                        $args->msg("Malformed SwissProt ID: $id = $rel",$file);
                    }
                } elsif ($dbt eq 'ref') {
                    if ($rel =~ /^[ANXY]P_(\d{9}|\d{6})\.\d+$/ ||
                        $rel =~ /^ZP_\d{8}\.\d+$/) {
                        $data->{refseq} = $rel;
                    } else {
                        $args->msg("Malformed RefSeq ID: $id = $rel", $file);
                    }
                } elsif ($dbt eq 'gb' || $dbt eq 'dbj' || $dbt eq 'emb') {
                    if ($rel =~ /^[A-Z]+\d+(\.\d+)?$/ ||
                        $rel =~ /^ZP_\d{8}\.\d+$/) {
                        $data->{general} = $rel;
                    } else {
                        $args->msg("Malformed General ID: $dbt:$id = $rel",
                                   $file);
                    }
                } else {
                    $args->msg("Unknown extra stuff: $id = ".join("|", @bits),
                               $file) unless 
                                   ($dbt =~ /^tpe|tpg|tpd|prf|pir|gnl|bbm$/);
                    $data->{gionly} = 1;
                }
            } else {
                $data->{gionly} = 1;
            }
        } else {
            # Sequence data
            $data->{seq} .= $_;
        }
    }
    close FILE;
    push @seqs, $data;
    my $con = shift @seqs;
    unless ($con && $con->{tag} eq 'lcl' && $con->{id} eq 'consensus') {
        $args->msg("$cdd Consensus not found in $file");
        return;
    }
    $con = $con->{seq};
    foreach my $seq (@seqs) {
        unless ($seq->{tag} eq 'gi') {
            $args->msg("Unusual sequence entry", $args->branch($seq))
                unless ($seq->{tag} eq 'lcl');
            next;
        }
        my $gi = $seq->{id};
        my $acc;
        if (defined $giLookup{ $gi }) {
            $acc = $giLookup{ $gi };
            next unless ($acc);
        } elsif (my $reliable = $seq->{refseq} || $seq->{swissprot} ||
                 $seq->{general}) {
            $acc = $giLookup{ $gi } = $reliable;
        } elsif (my $pdb = $seq->{pdb}) {
            if (1) {
                $lh->kill_edge( -name2 => $pdb,
                                type   => 'is a reliable alias for' );
                $lh->kill_class( $pdb, $pdb =~ /\_/ ? 'pdbchain' : 'pdb');
            }
            $pdb = "#PDB#$pdb";
            $lh->set_edge( -name1 => "gi$gi",
                           -name2 => $pdb,
                           type   => 'is a reliable alias for' );
            $lh->set_class( $pdb, $pdb =~ /\_/ ? 'pdbchain' : 'pdb');
            $acc = $giLookup{ $gi } = $pdb;
        } elsif (my $mtseq = $mt->get_seq("#None#gi$gi")) {
            $args->msg($seq->{other}) unless ($seq->{gionly});
            # We found the GI in maptracker
            my @edges = $mtseq->read_edges
                ( -keeptype => 'is a reliable alias for' );
            if ($#edges == 0) {
                $acc = $edges[0]->other_seq( $mtseq )->namespace_name;
            } elsif ($#edges > 0) {
                my %unv;
                foreach my $edge (@edges) {
                    my $accU = $edge->other_seq( $mtseq )->namespace_name;
                    my $sv = 0;
                    if ($accU =~ /^(.+)\.(\d+)$/) { ($accU,$sv) = ($1,$2) };
                    push @{$unv{$accU}}, $sv;
                }
                my @unvs = keys %unv;
                if ($#unvs == 0) {
                    $acc = $unvs[0];
                    my ($sv) = sort { $b <=> $a } @{$unv{$acc}};
                    $acc .= ".$sv" if ($sv);
                }
            }
            $giLookup{ $gi } = $acc if ($acc);
        } else {
            $lh->process_gi($gi);
        }
        if (!$acc) {
            # Could not find entry, use SeqStore
            my @bs = $pbg->add_by_accession("GI:$gi");
            if ($#bs == 0) {
                my ($accU, $sv) = $pbg->accession_for_bs($bs[0]);
                $acc = $accU;
                $acc .= ".$sv" if ($sv);
                $pbg->analyze;
                $giLookup{ $gi } = $acc;
            } elsif ($#bs > 0) {
                $args->msg("gi:$gi returns ".($#bs+1)." hits from $file");
                $giLookup{ $gi } = 0;
                push @{$giLookup{-2}}, $gi;
            } else {
                $giLookup{ $gi } = 0;
                push @{$giLookup{-1}}, $gi;
            }
            $pbg->clear_bioseqs;
        }
        unless ($acc) {
            
            next;
        }
        my $sdata = $seq->{seq};
        my @tags;
        if ($acc =~ /^(.+)\.(\d+)$/) {
            # Assign to unversioned accession, but note the version in a tag
            push @tags, ['Sequence Version', $acc, undef];
            $acc = $1;
        }

        $lh->set_edge( -name1 => $cdd,
                       -name2 => $acc,
                       -type  => 'FEATURE',
                       -tags  => \@tags );
    }
    $lh->write_threshold_quick( $cache );
}
