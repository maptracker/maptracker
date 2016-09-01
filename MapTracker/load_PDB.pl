#!/stf/biobin/perl -w

my $isBeta;
BEGIN {
    # Needed to make my libraries available to Perl64:
    # use lib '/stf/biocgi/tilfordc/released';
    # use lib '/apps/sys/perl/lib/site_perl/5.12.0/';
    # Make sure 5.10 libraries are moved to end of INC:
    my (@incOther, @inc510);
    map { if (/5\.10/) { push @inc510, $_ } else { push @incOther, $_ } } @INC;
    @INC = (@incOther ,@inc510);
    # warn join("\n", @INC);
    use lib '/stf/biocgi/tilfordc/patch_lib';
    # Allows usage of beta modules to be tested:
    my $prog = $0; my $dir = `pwd`;
    if ($prog =~ /working/ || $dir =~ /working/) {
        require lib;
        import lib '/stf/biocgi/tilfordc/perllib';
        $isBeta = 1;
    } else {
        $isBeta = 0;
    }
    $| = 1;
    print '';
    # print "Content-type: text/plain\n\n";
}

use strict;
use Bio::SeqIO;
use BMS::BmsArgumentParser;
use BMS::Utilities::SequenceUtilities;

my $args = BMS::BmsArgumentParser->new
    ( -file  => '',
      -block => 80, );

my $input = $args->val(qw(file input seqres));
my $block = $args->val(qw(block)) || 80;

my $su    = BMS::Utilities::SequenceUtilities->new();

if (!$input) {
    $args->msg("Please provide the path to the PDB fasta file",
               "It is generally named pdb_seqres.txt",
               "In the past it has been available from:",
               "ftp://ftp.wwpdb.org/pub/pdb/derived_data/pdb_seqres.txt");
    exit;
}

&process( $input );

sub process {
    my $file = shift;
    my $io = Bio::SeqIO->new( -file => $file, -format => 'fasta' );
    my %seqs;
    while (my $bs = $io->next_seq()) {
        my $id = $bs->display_id();
        my $seq = $bs->seq();
        my $desc = $bs->desc();
        my %d;
        while ($desc =~ /(([a-z]+)\:(\S+))/) {
            my ($rep, $k, $v) = ($1, $2, $3);
            $desc =~ s/\Q$rep\E/ /g;
            $d{$k} = $v;
        }
        $desc =~ s/^\s+//;
        if ($desc =~ /^(DNA|PROTEIN) \((.+)\)?/) {
            $desc = $2;
        }
        $desc =~ s/\s+$//;
        $desc =~ s/\s+/ /g;
        $d{""} = $desc;
        my $mol = lc($d{mol} ||= "unknown");
        my $typ = $mol =~ /prot/ ? 'aa' : $mol =~ /na/ ? 'nt' : 'unk';
        if ($typ eq 'nt') {
            # There are weird characters in nucleotide sequences
            my $safe = $su->safe_dna( $seq, 'U' );
            $typ = 'unk' unless ($safe eq  $seq);
        }
        my $srt = "ZZZZ $id";
        if ($id =~ /^(\d+)(.+)_(.+)$/) {
            $srt = sprintf("%09d %9s %9s", $1, $2, $3);
        }
        push @{$seqs{$typ}}, [$id, $seq, \%d, $srt];
    }
    undef $io;
    foreach my $typ (sort keys %seqs) {
        my $out = "pdb_$typ.fa";
        open(OUT, ">$out") || $args->death("Failed to write output", $out, $!);
        my @dats = sort { $a->[3] cmp $b->[3] } @{$seqs{$typ}};
        foreach my $dat (@dats) {
            my ($id, $seq, $d) = @{$dat};
            my @line = ($id);
            if (my $desc = $d->{""}) {
                push @line, $desc;
                delete $d->{""};
            }
            foreach my $k (sort keys %{$d}) {
                if (my $v = $d->{$k}) { push @line, "$k:$v"  }
            }
            print OUT ">".join(' ', @line)."\n";
            my $sl = length($seq);
            for (my $b = 0; $b < $sl; $b += $block) {
                print OUT substr($seq, $b, $block)."\n";
            }
        }
        close OUT;
        if (my $seqtype = $typ eq 'nt' ? 'F' : $typ eq 'aa' ? 'T' : '') {
            my $cmd = "/stf/biobin/makeblastable $out $seqtype";
            system($cmd);
        }
        $args->msg("[$typ]", scalar(@dats)." sequences", $out);
    }
}


=pod

This was what this 'script' had as of 1 Nov 2007

The DBREF record provides cross-reference links between PDB sequences and the corresponding database entry or entries.

COLUMNS DATA TYPE FIELD DEFINITION
----------------------------------------------------------------------
1 - 6 Record name "DBREF "
8 - 11 IDcode idCode ID code of this entry.
13 Character chainID Chain identifier.
15 - 18 Integer seqBegin Initial sequence number of the PDB sequence segment.
19 AChar insertBegin Initial insertion code of the PDB sequence segment.
21 - 24 Integer seqEnd Ending sequence number of the PDB sequence segment.
25 AChar insertEnd Ending insertion code of the PDB sequence segment.
27 - 32 LString database Sequence database name.
34 - 41 LString dbAccession Sequence database accession code.
43 - 54 LString dbIdCode Sequence database identification code.
56 - 60 Integer dbseqBegin Initial sequence number of the database seqment.
61 AChar idbnsBeg Insertion code of initial residue of the segment, if PDB is the reference.
63 - 67 Integer dbseqEnd Ending sequence number of the database segment.
68 AChar dbinsEnd Insertion code of the ending residue of the segment, if PDB is the reference.

=cut
