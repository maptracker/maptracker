#!/stf/biobin/perl -w

BEGIN {
    # Allows usage of beta modules to be tested:
    my $prog = $0; my $dir = `pwd`;
    if ($prog =~ /working/ || $dir =~ /working/) {
	warn "\n\n *** This is Beta Software ***\n\n";
	require lib;
	import lib '/stf/biocgi/tilfordc/perllib';
    }
    require lib;
    # import lib '/stf/biocgi/tilfordc/released/Bio/SeqIO';
}

my $VERSION = ' $Id$ ';

# '(PseudoCap|ApiDB_CryptoDB|Pathema|PBR|CGNC|EcoGene|Xenbase|VectorBase|MaizeGDB|TAIR|GeneDB)'

=head1 load_Gene.pl - LocusLink loader script for MapTracker


=head2 Standard Run

 load_Gene.pl -testmode 0 -update -clear -doload

=head2 Fast load of basic information

 load_Gene.pl -testmode 0 -update -clear -basic 20 -taxa common -doload

=head1 OPTIONS

    -limit Default 0. If non-zero, then only that number of records
           will be processed.

    -cache Default 100000. The number of database rows to cache before
           writing to MapTracker.

     -help Default 0. If true, then just show these options. Also -h
           will work.

     -taxa Defaul 0. If set, then only taxa for that species will be
           processed.

     -task Default all. You can specify specific tasks to perform if
           you wish, multiple tasks separated by commas or
           spaces. Available tasks are:

           info go pubmed sts accessions unigene history generif interact

           In addition, you can append an integer to the end of any
           task name. This will cause that task to be assigned that
           number of processors. For example, you could specify info3,
           which would assign 3 CPUs to parsing the info task.

     -loci Comma-separated list of specific loci to load

    -clear Remove all prior history files

   -update Default 0. If true, then wget will be used to get new data
           from the NCBI

 -testmode Default 1. If true, then will dump output to screen rather
           than DB

    -align Default 1. If true, then RefSeq and non-RefSeq entries from
           the same locus will be aligned to one-another.

      -log Log file path, default Gene_Parse_Log.txt

   -errors Error file path, default Gene_Parse_Errors.txt

=cut

use strict;
use BMS::MapTracker::LoadHelper;
use BMS::Fetch3;
use BMS::FriendlyClustalw;
use BMS::ArgumentParser;

my @commonSpecs = 
    (
     'Arabidopsis thaliana',
     'Bos taurus',
     'Caenorhabditis elegans',
     'Canis lupus familiaris',
     'Cavia porcellus',
     'Danio rerio',
     'Drosophila melanogaster',
     'Gallus gallus',
     'Hepatitis C virus',
     'Homo sapiens',
     'Macaca fascicularis',
     'Macaca mulatta',
     'Mus musculus',
     'Oryctolagus cuniculus',
     'Pan troglodytes',
     'Rattus norvegicus',
     'Saccharomyces cerevisiae',
);

my $args = BMS::ArgumentParser->new
    ( -dir      => '/work5/tilfordc/gene/DATA',
      -nocgi    => 1,
      -testmode => 1,
      -h        => undef,
      -help     => undef,
      -cache    => 100000,
      -verbose  => 1,
      -basedir  => 'LocusLink',
      -doload   => 0,
      -clear    => 0,
      -taxa     => '',
      -task     => 'info6 go2 pubmed sts accessions30 unigene history generif interact',
      -errors   => 'Gene_Parse_Errors.txt',
      -log      => 'Gene_Parse_Log.txt',
      -history  => 'Gene_Parse_History',
      -align    => 1,
      );

if ($args->{H} || $args->{HELP}) {
    print "\n\n"; system("pod2text $0"); print "\n\n";
    exit;
}

my $dir      = $args->{DIR};
my $vb       = $args->{VERBOSE};
my $cache    = $args->{CACHE};
my $limit    = $args->{LIMIT};
my $tm       = $args->{TESTMODE};
my $based    = $args->{BASEDIR};
my $doload   = $args->{DOLOAD};
my $doClear  = $args->val(qw(clobber clear redo));
my $hist     = $args->{HISTORY};
my $doBasic  = $args->val(qw(basic dobasic));
my $taskText = lc($args->val(qw(tasks task)));
my $testfile = $args->val(qw(testfile));
my $compOnly = $args->{COMPARE};
my $doAlign  = $args->{ALIGN} ? 1 : 0;
$tm      .= ' hush' if ($tm);
my $doxrefs = 1;

my $allLoci = "All LocusLink";
if ($doBasic) {
    my $an = 10;
    if ($doBasic =~ /^\d+/) {
        $an = $doBasic;
    } elsif ($taskText =~ /acc\S*(\d*)/) {
        $an = $1 || 1;
    }
    $doAlign  = 0;
    $taskText = "info accession$an";
    $doxrefs  = 0;
}

my %ignoreXref = map { uc($_) => 1 } qw(TAIR MaizeGDB CGNC ApiDB_CryptoDB PseudoCap);
# CGNC = Chicken Gene Nomenclature Committee
# PseudoCap = Pseudomonas aeruginosa Community Annotation Project 

# Task tags
my ($doing, $tnum, $subfork, $forknum, $prior);

# &deprecate_residual_locs() if (0);
unlink($testfile) if ($testfile);
if (my $doWget = $args->val(qw(wget update))) {
    my $cmd = "/work5/tilfordc/WGET/gene_cmd.sh";
    &wMsg("WGET: Mirroring NCBI FTP data", $cmd,`date`);
    my $st = time;
    system($cmd);
    my $tt = time - $st;
    unless ($tt > 20) {
        $args->death("Apparently failed to mirror NCBI Gene data",
                     $cmd, "Took $tt seconds, should take ~4 minutes");
    }
    &wMsg("Done.", `date`);
    if ($doWget =~ /only/i) {
        $args->msg("Request to only update");
        exit;
    }
    $doClear = 1;
} else {
    &wMsg("[***]", "USING EXISTING DATA - NO UPDATE!",
          "Use -wget if you wish to update mirrored data");
}

unless (-d $hist) {
    mkdir($hist);
    chmod(0777, $hist);
}
&wMsg("Parsing history will be stored in directory $hist/");
if ($doClear) {
    &wMsg("  All previous history files removed.");
} else {
    &wMsg("  Genes recorded in history files will NOT be reprocessed.");
}
if ($based && !$tm) {
    my @bits = ("DB Load files written to:",$based);
    push @bits, "You will need to manually load the files or also specify -doload" unless ($doload);
    &wMsg(@bits);
}

my %history;
opendir(HISTDIR, $hist) || $args->death
    ("Failed to read contents of history directory", $hist, $!);
foreach my $file (readdir HISTDIR) {
    if ($doClear) {
        unlink("$hist/$file");
        next;
    }
    open(HISTFILE, "<$hist/$file") ||
        $args->death("Failed to read history file", $file, $!);
    while (<HISTFILE>) {
        chomp;
        my ($task, $num, $date) = split("\t", $_);
        push @{$history{$task}}, $num;
    }
    close HISTFILE;
}
closedir HISTDIR;
foreach my $task (sort keys %history) {
    my ($least) = sort { $a <=> $b } @{$history{$task}};
    $history{$task} = $least;
    &wMsg(sprintf("  %15s : %5d prior entries", $task, $least));
}


# Object references:
my ($lh, $mt, $fcw, $fetch, $filedat);
# Static data structures:
my ($seqgrp, $type_of_gene, $matchtaxa, $matchloci, $logfh );

if (0) {
    $based = "/cxfs/stf/biocgi/tilfordc/working/temp";
    &_init_mt();
    &_kill_edge_auths('LOC204', undef, 'is a locus containing');
    foreach my $real ('NM_001625', 'NM_013411', 'NM_172199') {
        $lh->set_edge( -name1 => 'LOC204',
                       -name2 => $real,
                       -type  => 'is a locus containing',
                       -auth  => 'locuslink', );
    }
    $lh->write();
    $lh->process_ready unless ($tm);
    die;
}

if ($limit) {
    &wMsg('',"User requests -limit $limit",'');
}

&wMsg("Starting parsing: " . `date`);
foreach my $tag ('Errors', 'Log') {
    my $file = $args->{uc($tag)};
    next unless ($file);
    unlink($file);
    &wMsg(sprintf("  %10s -> %s", $tag, $file));
}
&wMsg('');

my $globalstart = time;
my $uncertain = {};
if ($compOnly) {
    if ($taskText =~ /(accessions\d+)/) {
        $taskText = $1;
    } else {
        die "You have specified -compare. This flag will result in only\n".
            "accessions being processed, please specify that in your\n".
            "tasks (you passed '$taskText')\n  ";
    }
}
my @tasks = split(/[\s\,]+/, $taskText);

my @pidarray;
# Fork off a child for each of the main files we need to process
for my $t (0..$#tasks) {
    my $task = $tasks[$t];
    my $sf = 1;
    if ($task =~ /^(.+?)(\d+)$/) {
        # If a number is tacked on to the 
        ($task, $sf) = ($1, $2);
    }
    if ($task =~ /hist/) {
        $task = 'history';
    } elsif ($task =~ /acc/) {
        $task = 'accessions';
        if ($doAlign) {
            &wMsg("Alignments will be performed (-align 1)");
        } else {
            &wMsg("Alignments will NOT be performed (-align 0)");
        }
    }
    # Standardize text to lowercase with first letter upper case:
    substr($task, 0, 1) = uc(substr($task, 0, 1));
    $history{$task} ||= 0;
    for my $s (1..$sf) {
        my $pid;
        if ($pid = fork) {
            # parent $pid = pid of child...
            push @pidarray, $pid;
            &wMsg(sprintf("  Spawning PID %d : %s%s", $pid, $task,
                         $sf == 1 ? '' : ' #' . $s));
        } elsif (defined $pid) {
            # $pid is zero but defined - this is the child
            # Each child calls one of the 'read' methods, eg read_info()
            my $method = 'read_' . lc($task).'()';
            # $tnum is just used to minimize redundant messages 
            $tnum  = $t + $s - 1;
            # $doing is a string describing the current child's task
            $doing = $task;
            # $subfork allows a task to be multiply forked
            $subfork = $s - 1;
            # $forknum is the total number of subforks
            $forknum = $sf - 1;
            # $prior is number of previously analyzed records
            $prior = $history{$task};
            eval($method);
            &wMsg($@) if ($@);
            &_finish;
            exit;
        } else {
            die "Failure to fork process for $task\n\n";
        }
    }
}
&wMsg('');

$tnum = 1;

my $failed = 0;
foreach my $pid (@pidarray) {
    waitpid($pid, 0);
    if (my $exit_value = $? >> 8) {
        &wMsg("    Child $pid exits with exit value $exit_value");
        $failed++;
    }
}
die "$failed children failed!!\n  " if ($failed);

my $gt = (time - $globalstart) / 3600;

&set_descriptions();

&wMsg(sprintf("\nAll tasks done - %.2f hours", $gt),'');

sub wMsg {
    return unless ($vb);
    $args->msg(@_);
}

sub read_info {
    &_init_mt();
    my @cols = qw( tax_id locid symbol loctag alias_symbols xrefs chr chr_band
                   desc gene_type sym_is_auth name_is_auth sym_status);
    &_open_file('gene_info', \@cols);
    while (my $record = &_next_record) {
        &process_info($record);
    }
}

sub process_info {
    my ($rec) = @_;
    my $locid = $rec->{locid};
    return unless ($locid);
    my $taxid = $rec->{tax_id};

    # Gene symbols:
    my $sym = &process_info_symbol($rec);

    # Make organizational locus groups for each species:
    my ($tx) = $mt->get_taxa($taxid);
    if ($tx) {
        my $lgroup = $tx->name . " LocusLink";
        $lh->set_class($lgroup, "Group");
        $lh->set_edge( -name1 => $locid,
                       -name2 => $lgroup,
                       -type  => 'is a member of' );
        $lh->set_edge( -name1 => $lgroup,
                       -name2 => $allLoci,
                       -type  => 'is a member of' );
    }

    $lh->set_taxa($locid, $taxid);


    # Description:
    &_kill_edge_auths($locid, undef, 'shortfor');
    if (my $desc = $rec->{desc}) {
        $desc = '#FREETEXT#' . $desc;
        $lh->set_class($desc, 'text');
        foreach my $term ($locid, $sym) {
            # Assign to both LocusID and symbol, if possible.
            next unless ($term);
            $lh->set_edge( -name1 => $term,
                           -name2 => $desc,
                           -type  => 'shortfor' );
        }
    }

    # General type
    # The resolution of this data has dropped - it used to include
    # terms such as 'phenotype' and 'QTL'.
    my $gt = $type_of_gene->{ $rec->{gene_type} };
    if ($gt) {
        &_kill_class_auths( $locid );
    }
    $lh->set_class( $locid, $gt || 'locus' );

    if ($doxrefs) {
        # Old assignments - Ensembl Gene IDs
        &_kill_edge_auths($locid, undef, 'sameas');

        # dbXrefs
        my %toKill;
        foreach my $xdat (@{$rec->{xrefs}}) {
            my ($xref, $ereq, $clean) = @{$xdat};
            $toKill{MIM}++ if ($xref =~ /^MIM\:/);
            # Passing RELIABLE indicates that we can reliably link the xref in
            # both directions
            my @edges = ($ereq eq 'RELIABLE') ? 
                ('is a reliable alias for','is reliably aliased by') : ($ereq);
            foreach my $edge (@edges) {
                $lh->set_edge( -name1 => $locid,
                               -name2 => $xref,
                               -type  => $edge );
            }
        }
        foreach my $prefix (keys %toKill) {
            &_kill_edge_auths($locid, $prefix . ':%', 'has attribute');
        }
    }
    
    # Chromosome and band
    
}

sub process_info_symbol {
    my ($rec) = @_;
    my $locid = $rec->{locid};
    &_kill_edge_auths($locid, undef, 'reliable');

    my $sym = $rec->{symbol};
    # Do not even bother annoying load helper with integer symbols
    # Hopefully most of these obnoxious IDs are in the aliases...
    $sym = '' if ($sym && ($sym eq 'NEWENTRY' || $sym =~ /^\d+$/));

    return $sym unless ($sym);

    if ($sym eq $locid) {
        # The locus ID itself is being used for the symbol. GRR
        return '';
    }

    my $taxid = $rec->{tax_id};
    $sym = '#GENESYMBOLS#' . $sym;
    $lh->set_taxa($sym, $taxid);
    my @tags;
    if (my $lt = $rec->{loctag}) {
        # These are often HGNC identifiers
        push @tags, [ 'Locus Tag', $lt, undef ];
        if ($lt =~ /^[A-Z]{3,}\:\d+$/) {
            # ABC:123 - This is reliable enough to make more direct
            # linkages in both directions
            $lh->set_edge( -name1 => $lt,
                           -name2 => $locid,
                           -type  => 'reliable' );
            $lh->set_edge( -name1 => $locid,
                           -name2 => $lt,
                           -type  => 'reliable' );
        }
    }
    if (my $stat = $rec->{sym_status}) {
        my $nstat = 'Interim';
        if ($stat eq 'O') {
            $lh->set_class($sym, 'OFFICIALSYM' );
            $nstat = 'Official';
        } else {
            $lh->set_class($sym, 'GENESYMBOL' );
        }
        push @tags, [ 'Authorized Nomenclature', 
                      '#META_TAGS#'.$nstat, undef ];
    } else {
        $lh->set_class($sym, 'GENESYMBOL' );
    }
    $lh->set_edge( -name1 => $locid,
                   -name2 => $sym,
                   -type  => 'reliable',
                   -tags  => \@tags );

    if (my $alias_list = $rec->{alias_symbols}) {
        foreach my $alias (split(/\|/, $alias_list)) {
            next if ($alias =~ /^\d+$/); # Ignore integer IDs
            $alias = '#GENESYMBOLS#' . $alias;
            $lh->set_taxa($alias, $taxid);
            $lh->set_class($alias, 'GENESYMBOL' );
            $lh->set_edge( -name1 => $sym,
                           -name2 => $alias,
                           -tags  => [['Locus', $locid, undef]],
                           -type  => 'lexical' );
        }
    }
    return $doBasic ? "" : $sym;
}

sub read_go {
    &_init_mt();
    my @cols = qw( tax_id locid goid evidence qualifier godesc pmids );
    &_open_file('gene2go', \@cols, 'clustered');
    my @locus;
    while (my $record = &_next_record) {
        next unless ($record->{locid});
        if ($#locus > -1 && $locus[0]{locid} ne $record->{locid}) {
            # We have started parsing a new locus
            &process_go( \@locus );
            @locus = (); $filedat->{clustcount}++;
            if ($limit && $filedat->{clustcount} >= $limit + $prior) {
                last;
            }
        }
        push @locus, $record;
    }
    &process_go( \@locus );
}

sub process_go {
    my ($list) = @_;
    return if ($#{$list} < 0);
    if ($forknum && ($filedat->{clustcount} % ($forknum+1)) != $subfork) {
        # 1. This task has multiple subforks
        # 2. The modulus of this cluster does not match the subfork
        # This cluster is not assigned to this task
        return;
    }

    my $locid = $list->[0]{locid};
    if ($history{$doing} && $history{$doing} >= $filedat->{clustcount}) {
        # This record was captured by a prior analysis
        return;
    }
    if ($history{$doing}) {
        &msg({}, "Skipped previously analyzed records", $history{$doing})
            if (!$forknum || !$subfork);
        $history{$doing} = 0;
    }

    $lh->kill_edge( -name1 => 'GO:%',
                    -name2 => $locid,
                    -type  => 'attribute',
                    -auth  => 0,
                    -override => 1 );
                    
    # &_kill_edge_auths('GO:%', $locid, 'attribute');
    my %bygo;
    foreach my $rec (@{$list}) {
        my $goid = $rec->{goid};
        my $qual = $rec->{qualifier} || "";
        push @{$bygo{$goid}{$qual}}, [ $rec->{evidence}, $rec->{pmids} ];
    }
    while (my ($goid, $quals) = each %bygo) {
        while (my ($qual, $data) = each %{$bygo{$goid}}) {
            my $edge = 'attribute';
            my @tags;
            if ($qual eq 'NOT') {
                $edge = 'isnot';
                push @tags, 
                [ 'Negated Edge', '#Reads_As#is attributed to', undef ];
            } elsif ($qual) {
                push @tags, [ 'GO Qualifier', $qual, undef ];
            }
            
            # Set basic tags
            my %tagbits;
            foreach my $dat (@{$data}) {
                my ($ecode, $pmids) = @{$dat};
                $tagbits{'GO Evidence'}{'#Evidence_Codes#'.uc($ecode||'NR')}++;
                map { $tagbits{'Referenced In'}{$_}++ } @{$pmids};
            }
            while (my ($tag, $vals) = each %tagbits) {
                foreach my $val (keys %{$vals}) {
                    push @tags, [ $tag, $val, undef ];
                }
            }
            $lh->set_edge( -name1 => $goid,
                           -name2 => $locid,
                           -type  => $edge,
                           -tags  => \@tags );
        }
    }
}

sub read_pubmed {
    &_init_mt();
    my @cols = qw( tax_id locid pmids);
    &_open_file('gene2pubmed', \@cols);
    while (my $record = &_next_record) {
        my $locid = $record->{locid};
        next unless ($locid);
        foreach my $pmid (@{$record->{pmids}}) {
            $lh->set_class($pmid, 'pubmed');
            $lh->set_edge( -name1 => $locid,
                           -name2 => $pmid,
                           -type  => 'reference' );
        }
    }
}

sub read_sts {
    &_init_mt();
    my @cols = qw( locid stsid );
    &_open_file('gene2sts', \@cols);
    while (my $record = &_next_record) {
        my $locid = $record->{locid};
        next unless ($locid);
        my $sid = $record->{stsid};
        if ($sid =~ /^\d+$/) {
            my $sts = "UniSTS:$sid";
            $lh->set_edge( -name1 => $sts,
                           -name2 => $locid,
                           -type  => 'probes' );
            $lh->set_class( $sts, 'sts');
        } else {
             &msg($record, "UniSTS ID malformed", $sid);
        }
    }
}

sub read_unigene {
    &_init_mt();
    my @cols = qw( locid ugid );
    &_open_file('gene2unigene', \@cols);
    
    while (my $record = &_next_record) {
        my $locid = $record->{locid};
        next unless ($locid);
        my $ugid = $record->{ugid};
        my $prfx;
        if ($ugid =~ /^([A-Z][a-z]+)\.\d+$/) {
            $prfx = $1;
        } else {
            &msg($record, "UniGene ID malformed", $ugid);
            $filedat->{num}--;
            next;
        }
        # Make sure we only clear other unigene IDs!
        $lh->kill_edge( -name1 => $locid,
                        -name2 => "$prfx.%",
                        -type  => 'memberof' );
        $lh->set_edge( -name1 => $locid,
                       -name2 => $ugid,
                       -type  => 'memberof' );
    }
}

sub read_history {
    &_init_mt();
    my @cols = qw( tax_id locid depid depsym );
    &_open_file('gene_history', \@cols);
    while (my $record = &_next_record) {
        my $depid = $record->{depid};
        next unless ($depid);
        my $taxid = $record->{tax_id};
        my ($tx)  = $mt->get_taxa($taxid);
        if ($tx) {
            # Remove the locus from the taxa group
            my $lgroup = $tx->name . " LocusLink";
            $lh->kill_edge( -name1 => $depid,
                            -name2 => $lgroup,
                            -type  => 'is a member of' );
        }
        $lh->set_class($depid, 'deprecated');
        if (my $locid = $record->{locid}) {
            $lh->set_edge( -name1 => $depid,
                           -name2 => $locid,
                           -type  => 'deprecatedfor' );
        }
        if (my $sym = $record->{depsym}) {
            # The symbol itself may be deprecated - but that
            # was not clear from the documentation
            unless ($sym =~ /^(LOC)?\d+$/) {
                $sym = '#GENESYMBOLS#' . $sym;
                $lh->set_edge( -name1 => $depid,
                               -name2 => $sym,
                               -type  => 'reliable' );
            }
        }
    }
}

sub read_generif {
    &_init_mt();
    my @cols = qw( tax_id locid pmids timestamp text );
    &_open_file('../GeneRIF/generifs_basic', \@cols);
    while (my $record = &_next_record) {
        my $locid = $record->{locid};
        my $text  = $record->{text};
        next unless ($locid && $text);
        $text = '#FREETEXT#' . $text;
        $lh->set_class($text, 'text');
        foreach my $pmid (@{$record->{pmids}}) {
            $lh->set_edge( -name1 => $locid,
                           -name2 => $text,
                           -type  => 'comment',
                           -tags  => [['Referenced In', $pmid, undef]], );
        }
    }
}

sub read_accessions {
    &_init_mt();
    $fcw   = BMS::FriendlyClustalw->new( -algorithm => 'needle' );
    $fcw->loadHelperDescriptions( $lh ) unless ($subfork);
    $fetch = BMS::Fetch3->new( -format  => 'fasta', -version => 1,);
    my @cols = qw( tax_id locid status rna_acc rna_gi protein_acc protein_gi
                   gdna_acc gdna_gi gdna_start gdna_end gdna_strand );
    &_open_file('gene2accession', \@cols, 'clustered');
    my @locus;
    while (my $record = &_next_record) {
        next unless ($record->{locid});
        if ($#locus > -1 && $locus[0]{locid} ne $record->{locid}) {
            # We have started parsing a new locus
            &process_locus_acc( \@locus );
            @locus = (); $filedat->{clustcount}++;
            if ($limit && $filedat->{clustcount} >= $limit + $prior) {
                last;
            }
        }
        push @locus, $record;
    }
    &process_locus_acc( \@locus );
}

sub process_locus_acc {
    my ($list) = @_;
    return if ($#{$list} < 0);
    if ($forknum && ($filedat->{clustcount} % ($forknum+1)) != $subfork) {
        # 1. This task has multiple subforks
        # 2. The modulus of this cluster does not match the subfork
        # This cluster is not assigned to this task
        return;
    }
    if ($history{$doing} && $history{$doing} >= $filedat->{clustcount}) {
        # This record was captured by a prior analysis
        return;
    }
    if ($history{$doing}) {
        &msg({}, "Skipped previously analyzed records", $history{$doing})
            if (!$forknum || !$subfork);
        $history{$doing} = 0;
    }
    
    my $proto_rec = $list->[0];
    my $locid = $proto_rec->{locid};
    unless ($doBasic) {
        foreach my $edge ('is a locus containing',
                          'is a locus with protein',
                          'is fully contained by',
                          'overlaps with', ) {
            &_kill_edge_auths($locid, undef, $edge);
        }
    }
    # If any of the sequences have proteins associated, then we
    # will assume that RNAs should be mRNAs.
    my $protcount = 0;
    map { $_->{prot_acc} ? $protcount++ : 0 } @{$list};
    my $baseclass = $protcount ? 'mRNA' : 'RNA';
    my (%edges, %similar);
    foreach my $rec (@{$list}) {
        my %translate;
        foreach my $type ('rna', 'protein', 'gdna') {
            my ($acc, $gi) = ($rec->{$type.'_acc'}, $rec->{$type.'_gi'});
            next unless ($acc);
            next if ($doBasic && $acc !~ /^[NX][MRP]_/);
            my ($idU, $class, $idV) = 
                $lh->process_refseq($acc, $rec->{tax_id}, 'shutup', 'oneVers');
            my $auth;
            if ($idU) {
                # Valid RefSeq ID
                $auth = 'RefSeq';
                if ($type eq 'rna') {
                    # Associate with the locus
                    $edges{'is a locus containing'}{$idU} =1;
                } elsif ($type eq 'gdna' && !$doBasic) {
                    # We ASSUME that RefSeq genomic sequences will fully
                    # contain the locus. This will be valid for chromosomes
                    # (NC_ and NG_), but may be dodgy for contigs (NT_)
                    $edges{'is fully contained by'}{$idU} =1;
                }
                $similar{$type}{RefSeq}{$idU} = $idV || $idU;
            } elsif ($doBasic) {
                next;
            } else {
                # It was not a refseq
                $auth = 'NCBI';
                $class = ($type eq 'rna') ? $baseclass : $type;
                my $maxV;
                if ($acc =~ /^(\S+)\.(\d+)$/) {
                    ($idU, $maxV, $idV) = ($1, $2, $acc);
                } else {
                    $idU = $acc;
                }
                $lh->process_versioning
                    ($idU, $maxV, $class, $rec->{tax_id}, $auth, undef, $maxV);
                $similar{$type}{Other}{$idU} = $idV || $idU;
            }
            $translate{$auth}{U}{$type} = $idU;
            $translate{$auth}{V}{$type} = $idV;
            if ($gi && !$doBasic) {
                $gi = $lh->process_gi($gi, $rec->{tax_id}, $class);
                if ($gi && $idV) {
                    # We have a GI number and a versioned accession - relate
                    $lh->set_edge( -name1 => $gi,
                                   -name2 => $idV,
                                   -type  => 'reliable',
                                   -auth  => $auth );
                    $lh->set_edge( -name1 => $idV,
                                   -name2 => $gi,
                                   -type  => 'reliable',
                                   -auth  => $auth );   
                }
            }
        }

        # Relate RNA and protein for both versioned and unversioned accs
        # The authority for both will need to match
        foreach my $auth (keys %translate) {
            foreach my $key (keys %{$translate{$auth}}) {
                my ($rna, $prot) = ($translate{$auth}{$key}{rna},
                                    $translate{$auth}{$key}{protein});
                $lh->set_edge( -name1 => $rna,
                               -name2 => $prot,
                               -type  => 'translate',
                               -auth  => $auth ) if ($rna && $prot); 
            }
        }
    }


    if ($similar{rna}) {
        if (!$similar{rna}{RefSeq} && $similar{rna}{Other}) {
            # The loci has RNA entries, but none of them are RefSeq
            foreach my $idU (keys %{$similar{rna}{Other}}) {
                $edges{'is a locus containing'}{$idU} =1;
            }
        }
    } elsif ($similar{protein}) {

        # The locus has proteins, but no RNA. This is very annoying.
        # If there is a protein, there should be an mRNA. However, many
        # entries appear to be in this state - in particular, a lot of
        # prokaryotes appear to list a genomic sequence, and the
        # protein.

        # I am making a special edge LOCUSPROT to capture these
        # protein assignments

        foreach my $class ('RefSeq', 'Other') {
            next unless ($similar{protein}{$class});
            foreach my $idU (keys %{$similar{protein}{$class}}) {
                $edges{'is a locus with protein'}{$idU} =1;
            }
            last;
        }
    }

    # Relate transcripts and gDNA to the locus
    foreach my $edge (keys %edges) {
        foreach my $id (keys %{$edges{$edge}}) {
            $lh->set_edge( -name1 => $locid,
                           -name2 => $id,
                           -type  => $edge ) if ($id);
        }
    }

    return unless ($doAlign);

    # Tie the 'other' protein and RNA sequences to RefSeq by
    # similarity.  First see if we have done so already - we want to
    # minimize calls to Fetch, and particularly to ClustalW, if we
    # have already aligned the Other sequences to RefSeq.

    foreach my $type ('rna','protein') {
        next unless ($similar{$type});
        unless ($similar{$type}{RefSeq} && $similar{$type}{Other}) {
            # Can not compare unless we have members of both RefSeq and Other
            if (!$similar{$type}{RefSeq} && $similar{$type}{Other}) {
                # Only contains non-RefSeq
                
            }
            delete $similar{$type};
            next;
        }
        my @rs = values %{$similar{$type}{RefSeq}};
        my @os = values %{$similar{$type}{Other}};
        my $edges = $mt->get_edge_dump
            ( -name1    => \@rs,
              -name2    => \@os,
              -keeptype => ['SAMEAS','CONTAINS','SIMILAR'],
              -keepauth => ['tilfordc','ClustalW'],
              -return   => 'object array', );
        # Turn edges into nodes into names into hash:
        my %names = map {$_ => 1} map {$_->name} map {$_->nodes} @{$edges};
        foreach my $name (keys %names) {
            $name =~ s/\.\d+$//;
            # Remove from the Other list any entry that has already been
            # matched to a refseq
            delete $similar{$type}{Other}{$name};
        }
        my @others = keys %{$similar{$type}{Other}};
        if ($#others < 0) {
            # All the other sequences are already matched to RefSeq
            delete $similar{$type};
        }
    }
    # Ok, now try to align any sequences that survived the above filter:
    my $done = &compare( $proto_rec, \%similar);
}

sub compare {
    my ($rec, $data) = @_;
    my $done = 0;
    foreach my $type ('rna','protein') {
        next unless ($data->{$type} );
        my ($refgrp, $othgrp);
        if ($type eq 'rna') {
            $fcw->moltype('rna');
            ($refgrp, $othgrp) = ( [ 'REFSEQN' ] , [ 'GCGGB' ] );
        } else {
            $fcw->moltype('protein');
            ($refgrp, $othgrp) = ( [ 'REFSEQP' ] , [ 'GCGGB', 'GCGPROT' ] );
        }
        my %data;
        foreach my $key ('RefSeq','Other') {
            my @seqs = $fetch->fetch
                ( -anyid     => [ keys %{$data->{$type}{$key}} ],
                  -seq_group => $seqgrp->{$type}{$key},
                  -clear     => 1);
            last if ($#seqs < 0);
            foreach my $bs (@seqs) {
                my $di  = $bs->display_id;
                # Only consider versioned accessions
                next unless ($di =~ /\.\d+$/);
                push @{$data{$key}}, $bs;
            }
        }
        next unless (exists $data{RefSeq} && exists $data{Other});

        my @matches = $fcw->best_group_match
            ( -query   => $data{Other},
              -subject => $data{RefSeq} );

        foreach my $match (@matches) {
            my ($qry, $sbj) = ($match->{query}, $match->{subject});
            if ($sbj) {
                $lh->set_edge( -name1 => $qry->display_id,
                               -name2 => $sbj->display_id,
                               -type  => $match->{edge},
                               -tags  => $match->{tags},
                               -auth  => $match->{auth}, );
            } else {
                # No hits to this query
                $lh->set_edge( -name1 => $qry->display_id,
                               -name2 => $rec->{locid},
                               -type  => 'isnot',
                               -tags  => $match->{tags},
                               -auth  => $match->{auth}, );
            }
        }
    }
}

# Need to track down (was in LocusLink, where in Gene??
# EC Numbers?
# Orthologues?
# Protein domains?

sub msg {
    my ($obj, $msg, $detail) = @_;
    my $locid = $obj->{locid} || '++';
    $detail ||= "";
    my $txt = sprintf(" -- %10s : %10s : %s : %s\n", $locid, $doing || 'MAIN', 
                      $msg, substr($detail,0,75));
    print $logfh $txt if ($txt);
    return if ($locid eq '++');
    $uncertain->{$msg} ||= {};
    $uncertain->{$msg}{$detail}++;
}

sub _make_list {
    my ($string) = @_;
    return () unless ($string);
    $string =~ s/\s+\|\s+/\|/g; # Remove spaces around delimiter
    return &strip_null( split(/\|/, $string ) );
}

sub strip_null {
    my @array = @_;
    for my $i (0..$#array) {
        my $val = $array[$i];
        next if (!$val);
        if ($val eq '-' || $val eq '.' || $val eq 'none') {
            $array[$i] = undef;
        }
    }
    return @array;
}

sub _open_file {
    my ($req, $cols, $isclust) = @_;
    &_close_file if ($filedat);
    die "No columns specified" unless ($cols || $#{$cols} == 0);
    
    my $file = $req;
    if (!-e $file) {
        unless ($file =~ /^\//) {
            $file = $args->{DIR} . '/' . $file;
        }
        if (!-e $file && $file !~ /\.gz/) {
            $file .= ".gz";
        }
    }
    unless (-e $file) {
        die "I could not locate '$req'\n  ";
    }

    &msg({  }, 'Reading File', $req) if (!$forknum || !$subfork);
    my ($fh, $isgz, $count);
    if ($file =~ /\.gz$/) {
	open(GZIP, "gunzip -c $file|") || 
            die "Failed to establish gunzip pipe for '$file' ($req)";
        $fh = *GZIP;
        $isgz = 1;
        $count = `gunzip -c $file | wc -l`;
    } else {
	open(FILE, "<$file") || die "Failed to open '$file' ($req):\n  $!\n  ";
        $fh = *FILE;
        $count = `wc -l $file`;
    }
    if ($count =~ /(\d+)/) {
        $count = $1;
    } else {
        $count = 0;
    }
    $filedat = {
        fh   => $fh,
        file => $file,
        cols => $cols,
        gz   => $isgz,
        num  => 0,
        clust => $isclust,
        count => $count,
        start => time,
        clustcount => 0,
    };
    # Be sure to note a zero for all children when we start
    &_write_record_progress;
}

sub _next_record {
    # $is_single is true if each row is processed seperately
    my $is_single = !$filedat->{clust};
    if ($limit && $is_single && $filedat->{num} >= $limit + $prior) {
        # 1. The user has requested a limit
        # 2. This file is not having rows clustered
        # 3. We have analyzed a limits worth of data
        # Close file and return undef to indicate termination
        return undef;
    }
    my $fh   = $filedat->{fh};
    my @cols = @{$filedat->{cols}};
    my %hash;
    while (1) {
        my $line = <$fh>;
        return undef unless ($line);
        # GRR. Some files have comments
        next if ($line =~ /^\#/);
        chomp $line;
        my @row = split("\t", $line);
        for my $i (0..$#cols) {
            $row[$i] ||= '';
            my $cell = $row[$i] eq '-' ? '' : $row[$i];
            $hash{ $cols[$i] } = $cell;
        }
        if ($matchtaxa) {
            my $key = $hash{tax_id};
            if (!$key && $hash{ugid}) {
                $key = $hash{ugid};
                $key =~ s/\.\d+//;
            }
            if ($key && !$matchtaxa->{$key}) {
                # This entry does not match a requested taxa
                unless (++$filedat->{skiptaxa} % 500000 || $tnum) {
                    &msg({}, "Rejected taxa", sprintf
                         ("%.3fM",$filedat->{skiptaxa}/1000000));
                }
                next;
            }
        }
        
        if ($matchloci && (!$hash{locid} || !$matchloci->{$hash{locid}})) {
            # This entry does not match a requested locus, or has no locus
            unless (++$filedat->{skiploci} % 500000 || $tnum) {
                    &msg({}, "Rejected loci", sprintf
                         ("%.3fM",$filedat->{skiploci}/1000000));
            }
            next;
        }
        $filedat->{num}++;
        if ($is_single) {
            # 1. This file is not having rows clustered
            if ($history{$doing} && $history{$doing} >= $filedat->{num}) {
                # 2. This record was captured by a prior analysis
                next;
            }
            if ($forknum && 
                (($filedat->{num}-1) % ($forknum + 1)) != $subfork) {
                # 2. This task has multiple subforks
                # 3. The modulus of this line does not match the subfork
                # This line is not assigned to this task
                next;
            }
        }
        last;
    }

    # Standard safety checks and tweaks
    foreach my $key ('locid', 'depid') {
        if (my $lid = $hash{$key}) {
            if ($lid =~ /^\d+$/) {
                $hash{$key} = "LOC$lid";
            } else {
                &msg(\%hash, "LocusID ($key) malformed", $lid);
                $hash{$key} = '';
            }
        }
    }

    if ($is_single && $history{$doing}) {
        &msg({}, "Skipped previously analyzed records", $history{$doing})
            if (!$forknum || !$subfork);
        $history{$doing} = 0;
    }


    if (exists $hash{pmids}) {
        # Not all pubmed IDs are lists, but we will standardize them as such
        my @list;
        foreach my $pmid (split(/[\|\,]/, $hash{pmids} || '')) {
            if ($pmid =~ /^\d+$/) {
                push @list, "PMID:$pmid";
            } else {
                &msg(\%hash, "PubMed ID malformed", $pmid);
            }
        }
        $hash{pmids} = \@list;
    }
    if (exists $hash{xrefs} && $doxrefs) {
        my @list;
        my $taxid = exists $hash{tax_id} ? $hash{tax_id} : undef;
        foreach my $xref (split(/\|/, $hash{xrefs} || '')) {
            if ($xref =~ /^([^\:]+)\:(.+)$/) {
                my ($db, $val) = ($1, $2);
                my $ucdb = uc($db);
                my ($edge, $useval, $settax, $setclass);
                if ($ucdb eq 'MIM') {
                    if ($val =~ /^\d+$/) {
                        ($edge, $useval) = ('has attribute', $xref);
                    } else {
                        &msg(\%hash, "OMIM ID malformed", $xref);
                    }
                } elsif ($db eq 'LocusID') {
                    # What the heck is this here for???
                    
                } elsif ($db eq 'AceView/WormGenes') {
                    # http://www.wormgenes.org/
                    # Can not get it to make sense

                } elsif ($ucdb eq 'ENSEMBL') {
                    if ($doing eq 'Info') {
                        if ($val =~ /^ENS[A-Z]{0,4}G\d+$/) {
                            ($edge, $useval) = ('is the same as', $val);
                        } else {
                            &msg(\%hash, "Ensembl gene ID malformed", $val);
                        }
                    } else {
                        &msg(\%hash, "No logic for handling $db in $doing", $xref);
                    }
                } elsif ($db eq 'WormBase') {
                    # http://ws120.wormbase.org/db/gene/gene?name=Y65B4A.3
                    ($edge, $useval, $settax, $setclass) =
                        ('is a reliable alias for', $val, 1, 'Wormbase');
                } elsif ($db eq 'UniProtKB/Swiss-Prot') {
                    ($edge, $useval) = ('is a reliable alias for', $val);
                } elsif ($db eq 'RATMAP') {
                    # http://ratmap.gen.gu.se/ShowSingleLocus.htm?accno=92
                    if ($val =~ /^\d+$/) {
                        ($edge, $useval, $settax, $setclass) =
                            ('RELIABLE', $xref, 1, 'RatMap');
                    } else {
                        &msg(\%hash, "RatMap ID malformed", $val);
                    }
                } elsif ($db eq 'RGD') {
                    # http://ratmap.gen.gu.se/ShowSingleLocus.htm?accno=43
                    if ($val =~ /^\d+$/) {
                        ($edge, $useval, $settax, $setclass) =
                            ('RELIABLE', $xref, 1, 'RGD');
                    } else {
                        &msg(\%hash, "RGD ID malformed", $val);
                    }
                } elsif ($db eq 'MGI') {
                    # http://www.informatics.jax.org/searches/accession_report.cgi?id=MGI:87866
                    if ($val =~ /^\d+$/) {
                        ($edge, $useval, $settax, $setclass) =
                            ('RELIABLE', $xref, 1, 'MGI');
                    } else {
                        &msg(\%hash, "MGI ID malformed", $val);
                    }
                } elsif ($db eq 'MGD') {
                    # http://www.informatics.jax.org/
                    # I could not find matches to any of these -
                    # just a handful were present
                } elsif ($ucdb eq 'FLYBASE') {
                    # flybase.bio.indiana.edu/.bin/fbidq.html?FBgn0040373
                    if ($val =~ /^FBgn(\d{7})$/i) {
                        ($edge, $useval, $settax, $setclass) =
                            ('RELIABLE', "FBgn".$1, 1, 'Flybase');
                    } else {
                        &msg(\%hash, "FlyBase ID malformed", $val);
                    }
                } elsif ($db eq 'ZFIN') {
                    # http://zfin.org/cgi-bin/webdriver?MIval=aa-markerview.apg&OID=ZDB-GENE-011205-36
                    if ($val =~ /^ZDB\-/) {
                        ($edge, $useval, $settax) =
                            ('RELIABLE', $val, 1);
                    } else {
                        &msg(\%hash, "ZFIN ID malformed", $val);
                    }
                } elsif ($db eq 'HGNC') {
                    # These should be grabbed when symbols are parsed
                } elsif ($db eq 'ECOCYC') {
                    if ($val =~ /^(EG|G|M|G0\-)\d+$/) {
                        ($edge, $useval, $settax, $setclass) =
                            ('RELIABLE', "$db:$val", 1, 'locus');
                    } else {
                        &msg(\%hash, "Unknown EcoCyc ID", $val);
                    }
                } elsif ($db eq 'IMGT/GENE-DB') {
                    # http://imgt.cines.fr/
                    # international ImMunoGeneTics information system
                    # Not sure what to do with it
                } elsif ($db eq 'SGD') {
                    # db.yeastgenome.org/cgi-bin/locus.pl?locus=S000000538
                    if ($val =~ /^S\d{9}$/) {
                        ($edge, $useval, $settax, $setclass) =
                            ('RELIABLE', $val, 1, 'SGD');
                    } else {
                        &msg(\%hash, "SGD ID malformed", $val);
                    }
                    # http://imgt.cines.fr/
                    # international ImMunoGeneTics information system
                } elsif ($db eq 'HPRD') {
                    # Human Protein Reference Database
                    # Obnoxiously insist on licensing, but no authentication
                } else {
                    &msg(\%hash, "Unknown XREF DB", $db)
                        unless ($ignoreXref{$ucdb});
                }
                if ($edge) {
                    push @list, [ $useval, $edge ];
                    $lh->set_class($useval, $setclass) if ($setclass);
                    $lh->set_taxa($useval, $taxid) if ($settax);
                }
            } else {
                &msg(\%hash, "Unknown dbXref", $xref);
            }
        }
        $hash{xrefs} = \@list;
    }
    my $ops = $lh->operations;
    if ($ops >= $cache) {
        $filedat->{rows} += $ops;
        my $lines = $filedat->{num};
        my $mod   = $forknum + 1;
        my $txt = sprintf("%4.3fk Lines, %5.3fk DB Rows",
                          $lines/1000, $mod * $filedat->{rows}/1000);
        if (my $count = $filedat->{count}) {
            my $frac = $lines / $count;
            my $elapsed = (time - $filedat->{start});
            my $remain = $elapsed * (1 - $frac) / $frac;
            $txt .= sprintf(", %2.2f%% [%.2f hr remain]", 
                            $frac * 100, $remain / 3600);
        }
        # Show the message if there is only a single subfork, or if this is
        # the first subfork
        &msg( {}, "Progress", $txt) if (!$forknum || !$subfork);
        $lh->write;
        # processes steadily grow over time, particularly 'info'
        # I suspect it may be cached taxa entries
        $mt->clear_cache('seqnames', 'taxa');
        &_write_record_progress;
    }
    return \%hash;
}

sub _write_record_progress {
    # We will note progress only when we are actually writing to DB,
    # and when we are processing all entries
    return if ($tm || $matchtaxa || $matchloci);
    # Record the progress we have made
    my $hfile = sprintf("%s/%s.%02d", $hist, $doing, $subfork + 1);
    # Use clustered counts if we are clustering, otherwise use line counts:
    my $num   = $filedat->{clust} ? $filedat->{clustcount} : $filedat->{num};
    open(HISTFILE, ">$hfile") ||
        die "Failed to write history to '$hfile':\n  $!\n  ";
    print HISTFILE join("\t", $doing, $num, `date`);
    close HISTFILE;
}

sub _close_file {
    return unless ($filedat);
    my $fh = $filedat->{fh};
    close $fh;
    unless ($tnum) {
        &msg({}, "Rejected loci", sprintf
             ("%.3fM",$filedat->{skiploci}/1000000)) if ($filedat->{skiploci});
        &msg({}, "Rejected taxa", sprintf
             ("%.3fM",$filedat->{skiptaxa}/1000000)) if ($filedat->{skiptaxa});
    }
    undef $filedat;
}

sub _init_mt {
    return if ($lh);
    $lh = BMS::MapTracker::LoadHelper->new
        ( -username => 'LocusLink',
          -userdesc => 'Data specified by NCBI Locus Link file',
          -basedir  => $based,
          -loadtoken => $args->{LOADTOKEN},
          -carpfile => '>>' . $args->{ERRORS},
          -testmode => $tm,
          -testfile => $testfile ? ">>$testfile" : undef,
          -dumpsql  => $args->{DUMPSQL});
   # $lh->redirect( -stream => 'TEST',
   #                -fh     => *STDOUT );
    if (my $file = $args->{LOG}) {
        open(LOG, ">>$file") || 
            die "Could not append to log file '$file'\n  $!\n  ";
        $logfh = *LOG;
    }
    $mt = $lh->tracker;
    $seqgrp = {
        rna => {
            RefSeq => [ 'REFSEQN' ],
            Other  => [ 'GCGGB' ],
        },
        protein => {
            RefSeq => [ 'REFSEQP' ],
            Other  => [ 'GCGGB', 'GCGPROT' ],        
        },
    };
    
    $type_of_gene = {
        'unknown'        => '',
        'tRNA'           => 'Gene',
        'rRNA'           => 'Gene',
        'snRNA'          => 'Gene',
        'scRNA'          => 'Gene',
        'snoRNA'         => 'Gene',
        'protein-coding' => 'Gene',
        'pseudo'         => 'Pseudogene',
        'transposon'     => '',
        'miscRNA'        => 'Gene',
        'other'          => '',
    };

    if (my $txreq = $args->val(qw(taxa species))) {
        my @hits;
        $matchtaxa = {};
        my @reqs = ref($txreq) ? @{$txreq} : split(/[\,]+/, $txreq);
        if ($#reqs == 0 && lc($reqs[0]) eq 'common') {
            @reqs = @commonSpecs;
        }
        # cat /work5/tilfordc/gene/DATA/gene2unigene | awk '{ print $2 }' | sed -r "s/\..+//" | sort | uniq | grep -v GeneID | awk ' {print "\"\" => \""$1"\"," } '
        # Thanks to Stefan for pointing out uniq to me
        my $ugTokens = {
            "" => "Aae",
            "" => "Aga",
            "" => "Ame",
            "" => "Aps",
            "" => "At",
            "" => "Bfl",
            "" => "Bmo",
            "Bos taurus" => "Bt",
            "Caenorhabditis elegans" => "Cel",
            "Canis familiaris" => "Cfa",
            "Canis lupus familiaris" => "Cfa",
            "" => "Cin",
            "" => "Cpi",
            "" => "Cre",
            "Drosophila melanogaster" => "Dm",
            "Danio rerio" => "Dr",
            "" => "Dsi",
            "Equus caballus" => "Eca",
            "" => "Fne",
            "" => "Gac",
            "Gallus gallus" => "Gga",
            "" => "Gma",
            "" => "Hma",
            "Homo sapiens" => "Hs",
            "" => "Hv",
            "" => "Ipu",
            "" => "Isc",
            "" => "Les",
            "" => "Lja",
            "Macaca fascicularis" => "Mfa",
            "Monodelphis domestica" => "Mdm",
            "Meleagris gallopavo" => "Mga",
            "" => "Mgr",
            "Mus musculus" => "Mm",
            "Macaca mulatta" => "Mmu",
            "" => "Ncr",
            "" => "Nta",
            "" => "Nve",
            "" => "Nvi",
            "Ornithorhynchus anatinus" => "Oan",
            "Ovis aries" => "Oar",
            "Oryctolagus cuniculus" => "Ocu",
            "" => "Ola",
            "" => "Omy",
            "" => "Os",
            "Pongo abelii" => "Pab",
            "Papio anubis" => "Pan",
            "" => "Ppa",
            "" => "Psi",
            "" => "Ptc",
            "" => "Pte",
            "" => "Pth",
            "Peromyscus maniculatus" => "Pmn",
            "Rattus norvegicus" => "Rn",
            "" => "Rra",
            "" => "Rsa",
            "" => "Sbi",
            "" => "Sma",
            "" => "Smo",
            "" => "Spu",
            "" => "Ssa",
            "Sus scrofa" => "Ssc",
            "Xenopus (Silurana) tropicalis" => "Str",
            "Silurana tropicalis" => "Str",
            "Xenopus tropicalis" => "Str",
            "" => "Stu",
            "" => "Ta",
            "" => "Tca",
            "" => "Tgo",
            "" => "Tgu",
            "" => "Tru",
            "Trichosurus vulpecula" => "Tvu",
            "" => "Tth",
            "" => "Vca",
            "" => "Vvi",
            "" => "Xl",
            "" => "Zm",
        };
        foreach my $tr (@reqs) {
            foreach my $taxobj ($mt->get_taxa($tr)) {
                my $ttxt = $taxobj->to_text || '';
                $ttxt =~ s/\n//g;
                push @hits, $ttxt;
                $matchtaxa->{ $taxobj->id } = 1;
                if (my $ug = $ugTokens->{$taxobj->name}) {
                    $matchtaxa->{ $ug } = 1;
                    $hits[-1] .= " ($ug)";
                }
            }
        }
        
        unless ($tnum) {
            my $mtxt = "Use only taxae: " . join(', ', @hits);
            &msg({  }, 'Settings', $mtxt);
        }
    }
    if (my $lreq = $args->val(qw(loci locus))) {
        my @good;
        foreach my $req (split(/[\s\,]+/,uc($lreq))) {
            if ($req =~/^(LOC)?(\d+)$/) {
            }
            push @good, $2;
        }
        if ($#good > -1) {
            $matchloci = { map { $_ => 1 } @good };
            unless ($tnum) {
                my $mtxt = "Use only loci: " . join(', ', @good);
                &msg({  }, 'Settings', $mtxt);
            }
        }
    }
}

sub _finish {
    $lh->write;
    &_write_record_progress;
    &_close_file;
    if ($based && !$tm && $doload) {
        &msg({  }, 'DB Write' . ($forknum ? ' #'.($subfork+1): ''), $based);
        $lh->process_ready();
    }
    my $rn    = $lh->rows_written;
    my $fstat = ($rn > 500000) ?
        sprintf("%.3f Million DB Rows",$rn / 1000000) :
        sprintf("%.3f Thousand DB Rows",$rn / 1000);
    &msg({  }, 'Finished' . ($forknum ? ' #'.($subfork+1): ''), $fstat);
    my @errors = keys %{$uncertain};
    close $logfh if ($logfh);
    return if ($#errors < 0);
    &wMsg("$doing: some errors occured:");
    foreach my $msg (@errors) {
        &wMsg($msg);
        my @details = keys %{$uncertain->{$msg}};
        if ($#details > 100) {
            &wMsg(sprintf("  %d distinct entries", $#details + 1));
            next;
        }
        foreach my $det (@details) {
            &wMsg(sprintf("   %s : %d", $det, $uncertain->{$msg}{$det}));
        }
    }
}

sub _kill_edge_auths {
    my ($name1, $name2, $type) = @_;
    foreach my $auth ('LocusLink') {
        $lh->kill_edge( -name1 => $name1,
                        -name2 => $name2,
                        -type  => $type,
                        -auth  => $auth );
    }
}

sub _kill_class_auths {
    my ($name) = @_;
    foreach my $auth ('LocusLink') {
        $lh->kill_class($name, undef, $auth);
    }
}

sub deprecate_residual_locs {
    $doing   = "Deprecation";
    $prior   = 0;
    $doxrefs = 0;
    &_init_mt();
    my $locs = &get_all_locids();
    &wMsg( `date` );
    &wMsg("  Removing current IDs from the list","");
    my @cols = qw( tax_id locid symbol loctag alias_symbols xrefs chr chr_band
                   desc gene_type sym_is_auth name_is_auth sym_status);
    &_open_file('gene_info', \@cols);
    while (my $record = &_next_record) {
        delete $locs->{ $record->{locid} };
    }
    &wMsg(`date`);
    &wMsg("  Removing known deprecated IDs from the list","");
    @cols = qw( tax_id locid depid depsym );
    &_open_file('gene_history', \@cols);
    while (my $record = &_next_record) {
        delete $locs->{ $record->{depid} };
        delete $locs->{ $record->{locid} };
    }
    my @orphans = sort keys %{$locs};
    &wMsg(`date`);
    &wMsg("  Inspecting " . ($#orphans+1) . " orphan loci","");
    my $deped = 0;
    foreach my $locid (@orphans) {
        my $seq = $mt->get_seq('#NONE#'.$locid);
        next unless ($seq);
        my $sname = $seq->name;
        next unless ($sname =~ /^LOC\d+$/);
        if ($seq->is_class('deprecated')) {
            # Already deprecated, skip
            next;
        }
        $lh->set_class( $seq->namespace_name, 'Deprecated', 'tilfordc');
        $lh->set_edge( -name1 => $seq->namespace_name,
                       -name2 => 'Unknown Deprecated Locus',
                       -type  => 'deprecatedfor',
                       -auth  => 'tilfordc', );
        $deped++
    }
    &wMsg(`date`);
    &wMsg("  Finished - $deped entries deprecated","");
    $lh->write();
    exit;
}

sub get_all_locids {
    &wMsg(`date`);
    &wMsg("  Fetching all loci from MapTracker");
    my $sql = "SELECT s.seqname FROM seqname s";
    $sql .= " WHERE upper(s.seqname) LIKE 'LOC%'";
    $sql .= " AND s.space_id = 1";
    $sql .= " LIMIT $limit" if ($limit);
    $mt->_showSQL($sql, "Find LocusLink IDs", -1);
    my $rows = $mt->dbh->get_all_rows($sql);
    my %locs;
    my $count = 0;
    foreach my $row (@{$rows}) {
        my $name = $row->[0];
        if ($name =~ /^LOC(\d+)$/) {
            $locs{uc($name)}++;
            $count++;
        }
    }
    &wMsg("  Found $count","");
    return \%locs;
}

sub read_interact {
    &_init_mt();
    my @cols = qw( tax_id locid accn1 name1 keyphrase tax2 id2 idtype accn2 name2 complex comptype compname pmids modified generif intid intype);
    &_open_file('../GeneRIF/interactions', \@cols, 'clustered');
    my @locus;
    while (my $record = &_next_record) {
        next unless ($record->{locid});
        if ($#locus > -1 && $locus[0]{locid} ne $record->{locid}) {
            # We have started parsing a new locus
            &process_interact( \@locus );
            @locus = (); $filedat->{clustcount}++;
            if ($limit && $filedat->{clustcount} >= $limit + $prior) {
                last;
            }
        }
        push @locus, $record;
    }
    &process_interact( \@locus );
}

sub process_interact {
    my ($list) = @_;
    return if ($#{$list} < 0);
    if ($forknum && ($filedat->{clustcount} % ($forknum+1)) != $subfork) {
        # 1. This task has multiple subforks
        # 2. The modulus of this cluster does not match the subfork
        # This cluster is not assigned to this task
        return;
    }

    my $locid = $list->[0]{locid};
    if ($history{$doing} && $history{$doing} >= $filedat->{clustcount}) {
        # This record was captured by a prior analysis
        return;
    }
    if ($history{$doing}) {
        &msg({}, "Skipped previously analyzed records", $history{$doing})
            if (!$forknum || !$subfork);
        $history{$doing} = 0;
    }
    foreach my $rec (@{$list}) {
        my @tags;
        if (my $gr = $rec->{generif}) {
            push @tags, [ 'GeneRIF', "#FreeText#$gr", undef ];
        }
        foreach my $pmid (@{$rec->{pmids}}) {
            push @tags, ['Referenced In', $pmid, undef];
        }
        my @pairs;
        my ($a1, $a2) = ($rec->{accn1}, $rec->{accn2});
        if ($a1 && $a2) {
            my @pair;
            foreach my $id ($a1, $a2) {
                if ($id =~ /\.\d+$/) {
                    push @tags, ['Sequence Version', $id, undef];
                    $id =~ s/\.\d+$//;
                }
                push @pair, $id;
            }
            push @pairs, \@pair;
        }
        if ($rec->{idtype} && $rec->{idtype} eq 'GeneID') {
            my $oid = $rec->{id2};
            if ($oid && $oid =~ /^\d+$/) {
                push @pairs, [ $locid, "LOC$oid"];
            } else {
                &msg($rec, "Second LocusID malformed", $locid);
            }
        }
        foreach my $pair (@pairs) {
            my ($acc1, $acc2) = @{$pair};
            if ($acc1 && $acc2) {
                $lh->set_edge( -name1 => $acc1,
                               -name2 => $acc2,
                               -type  => 'PHYSICAL',
                               -tags  => \@tags );
            }
        }
    }
}


sub set_descriptions {
    &_init_mt();
    my $descs = {
        "Locus Tag" => "Usually associated with a gene symbol, the locus tag is typically a more reliable accession indicating the gene 'object' that a locus is associated with.",

        "Authorized Nomenclature" => "A flag used for gene symbols, indicating if the symbol is officially sanctioned, an interim (temporary) symbol, or an unofficial alias.",

        "#META_TAGS#Official" => "The gene symbol shown is the official gene symbol for the locus.",

        "#META_TAGS#Interim" => "The gene symbol shown is temporary, and may change in the future when an official symbol is finally assigned.",

        "Negated Edge" => "IMPORTANT! This entry indicates that the edge is disputed by at least one other authority. You should attempt to scrutinize evidence for and against the relevant data assignment.",

        "GO Qualifier" => "GeneOntology will sometimes qualify the assignment of a term to a locus or protein, the GO Qualifier provides more information on the details of the assignment.",

        "GeneRIF" => "NCBI Reference Into Function, a very brief description of an observed biological fact or event. Almost always associated with one or more PubMed references.",

        "Sequence Version" => "If a measurement or assay was performed using a specific version of a sequence, the versioned identifier is indicated with this tag.",

        "" => "",

        "" => "",

    };
    while (my ($tag, $desc) = each %{$descs}) {
        next unless ($tag && $desc);
        $tag = "#META_TAGS#$tag" unless ($tag =~ /^\#/);
        $lh->set_edge( -name1 => $tag,
                       -name2 => "#FREETEXT#$desc",
                       -type  => "SHORTFOR");
    }
    $lh->set_class($allLoci, "Group");
    $lh->write();
}
