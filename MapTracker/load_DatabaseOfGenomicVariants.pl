#!/stf/sys64/bin/perl -w

BEGIN {
    # Needed to make my libraries available to Perl64:
    # use lib '/stf/biocgi/tilfordc/released';
    use lib '/stf/biocgi/tilfordc/patch_lib';
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
use BMS::Branch;
use BMS::CommonCAT;
use BMS::MapTracker::LoadHelper;


my $args  = &PARSEARGS
    ( -nocgi     => $ENV{HTTP_HOST} ? 0 : 1,
      -dir       => '/work5/tilfordc/WGET/DatabaseOfGenomicVariants',
      -testmode  => 1,
      -help      => undef,
      -clean     => 0,
      -limit     => 0,
      -progress  => 180,
      -benchmark => 0,
      -basedir   => '/work5/tilfordc/maptracker/DGV',
      -verbose   => 1 );


my $baseDir   = $args->{LOADDIR} || $args->{BASEDIR};
my $limit     = $args->{LIMIT};
my $nocgi     = $args->{NOCGI};
my $tm        = $args->{TESTMODE};
my $vb        = $args->{VERBOSE};
my $debug     = BMS::Branch->new( -format => $nocgi ? 'text' : 'html');

my $lh = BMS::MapTracker::LoadHelper->new
    ( -username  => 'DGV',
      -userdesc  => 'Database of Genomic Variants',
      -basedir   => $baseDir,
      -testmode  => $tm, );

my ($dbVers, $genomeBuild);

my $varType = {
    CopyNumber => 'CNV',
    InDel      => 'InDel',
};

my $chrFrm = "homo_sapiens.chromosome.%s.%s";

&process();

sub process {
    my $files = &get_files();
    $debug->branch($files);
    while (my ($type, $path) = each %{$files}) {
        &process_file($type, $path);
    }
}

sub process_file {
    my ($type, $path) = @_;
    warn "[$type] $path\n";
    open(FILE, "<$path") || die "Failed to read '$path':\n  $!\n  ";
    my $head = <FILE>;
    $head =~ s/[\n\r]+$//;
    my @cols = map { lc($_) } split(/\t/, $head);
    my $cn   = $#cols;
    my $num  = 0;
    my @chrBits = qw(chr start end);
    while (<FILE>) {
        s/[\n\r]+$//;
        my @row = split(/\t/);
        my %hash = map { $cols[$_], $row[$_] } (0..$cn);
        my $id   = $hash{variationid};
        unless ($id) {
            warn "No Variation ID defined\n";
            $debug->branch(\%hash);
            next;
        }
        $lh->set_class($id, 'Database of Genomic Variants');
        my $vt   = $hash{variationtype} || '';
        my $mtc  = $varType{$vt};
        if ($mtc) {
            $lh->set_class($id, $mtc);
        } else {
            warn "No MapTracker class mapped to variationtype=$vt\n";
        }
        if (my $pmid = $hash{pubmedid}) {
            if ($pmid =~ /^\d+$/) {
                $lh->set_edge( -name1 => $id,
                               -name2 => "PMID:$pmid",
                               -type  => 'is referenced in' );
            } else {
                warn "Malformed PMID for $id = '$pmid'\n";
            }
        }
        my ($chr, $start, $end) = map { $hash->{$_} } @chrBits;
        if ($chr && $start && $end) {
            if ($chr =~ /^chr(\d+|MT|X|Y)$/) {
                
            } else {
                warn "Odd Chromosome for $id: $chr\n";
            }
        }
        $num++;
        last if ($limit && $num >= $limit);
    }
    close FILE;
}

sub get_files {
    my $dir = $args->{DIR};
    $dir =~ s/\/+$//;
    opendir(DIR, $dir) || die "Failed to read directory '$dir':\n  $!\n  ";
    my %versions;
    foreach my $path (readdir DIR) {
        if ($path =~ /^(\S+)\.hg(\d+)\.v(\d+)\.txt$/) {
            $versions{$3}{$2}{$1} = "$dir/$path";
        }
    }
    closedir DIR;
    ($dbVers) = sort { $b <=> $a } keys %versions;
    unless ($dbVers) {
        die "No matching files found in $dir\n";
    }
    my ($hgNum) = sort { $b <=> $a } keys %{$versions{$dbVers}};
    my %num_to_build = ( 18 => 'NCBI36' );
    $genomeBuild = $num_to_build{ $hgNum };
    unless ($genomeBuild) {
        die "Human Genome $hgNum does not have a Build Number mapped for it\n";
    }
    return $versions{$dbVers}{$hgNum};
}
