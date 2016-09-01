#!/stf/biobin/perl -w

BEGIN {
    # Allows usage of beta modules to be tested:
    my $prog = $0; my $dir = `pwd`;
    if ($prog =~ /working/ || $dir =~ /working/) {
	warn "\n\n *** This is Beta Software ***\n\n";
	require lib;
	import lib '/stf/biocgi/tilfordc/perllib';
    }
   # require lib;
   # import lib '/stf/biocgi/tilfordc/released/Bio/SeqIO';
}

use strict;
use BMS::MapTracker::LoadHelper;
use BMS::TableReader;
use BMS::BmsArgumentParser;
use BMS::ForkCritter;

my $args = BMS::BmsArgumentParser->new
    ( -dir      => '/work5/tilfordc/WGET/refseq_catalog',
      -testmode => 1,
      -nocgi    => 1,
      -cache    => 20000,
      -progress => 60,
      -verbose  => 1,
      -basedir  => "Catalog",
      );

$args->shell_coloring();

my (%modes, $fc, @rowGroup, $lh, $mt, $summary);

my $srcdir     = $args->val(qw(dir));
my $limit      = $args->val(qw(limit lim)) || 0;
my $prog       = $args->val(qw(prog progress)) || 0;
my $vb         = $args->val(qw(vb verbose));
my $fork       = $args->val(qw(forknum fork)) || 1;
my $testfile   = $args->val(qw(testfile));
my $tm         = $testfile ? 1 : $args->val(qw(tm testmode));
my $baseDir    = $args->val(qw(loaddir basedir));
my $cache      = $args->val(qw(cache));
my $noVersion  = "RefSeqVersionless.tsv";
my $iadef      = 'is a deprecated entry for';
my $depto      = "RefSeqDeprecatedTo.tsv";
my $keepOdd    = $args->val(qw(keepodd));
my $clobber    = $args->val('clobber');
# These are the 'normal' IDs we want to get
my $notOdd     = '^(NM|NR|XM|XR|NP|XP|NC|NT)_';
# 'Odd' IDs include formats like:
# WP_000003004.1
# YP_003329477.1
# NZ_CAEI00000000.1
# NW_006256912.1
# AC_000188.1
# AP_004907.1
# NG_017190.1
# NS_000188.1
# Odd IDs will only be kept if -keepodd is true

if ($args->val(qw(wget mirror))) {
    &mirror();
}


my $releases = &files;
if (my $df = $args->val('depto')) {
    &process_to($df);
} else {
    &process();
}
$args->msg("Finished.");

sub process {
    $summary    = "RefSeqCatalogDeprecations.tsv";
    my $mod;
    $mod = "-LIMIT" if ($limit);
    my $sortFile;
    if ($sortFile = $args->val(qw(source))) {
        # User provided file
        $mod ||= "";
        $mod .= "-USER";
    } else {
        $sortFile = "$summary.sorted";
        if (-s $sortFile && !$clobber) {
            $args->msg("[<]", "Using existing sorted file", $sortFile,
                       "Pass -clobber to recompute");
        } else {
            &make_summary_file();
            my $cmd = "sort -S 10G '$summary' > '$sortFile'";
            $args->msg("[-]","Sorting summary information", $cmd);
            system($cmd);
        }
    }
    $fc = BMS::ForkCritter->new
        ( -init_meth   => \&initialize,
          -finish_meth => \&finish,
          -method      => \&parse_group,
          -progress    => $prog,
          -exitcode    => 42,
          -limit       => $limit,
          -verbose     => $vb, );
    $fc->group_method( \&group_entries );
    $fc->input_type( 'tsv' );
    $fc->input($sortFile);
    my $chain = "RefSeqDeprecatedChains.tsv";

    map { $_ .= $mod } ($depto, $chain) if ($mod);
    $fc->output_file( 'depto', $depto );
    $fc->output_file( 'chain', $chain );
    $fc->output_file( 'TestFile', $testfile) if ($testfile);
    $args->msg("Parsing and loading MapTracker", $sortFile);
    if (my $failed = $fc->execute( $fork )) {
        $args->err("$failed children failed to execute properly!");
    }
    my @files;
    push @files, ("Deprecated To Data: $depto") if (-s $depto);
    push @files, ("Deprecation Chains: $chain") if (-s $chain);
    push @files, ("Loader TestFile   : $testfile") if ($testfile);
    $args->msg("[FILE]", @files) if ($#files != -1);
}

sub process_to {
    my $depto = shift;
    $args->death("No DeprecatedTo file", $depto) unless (-s $depto);
    open(DEPTO, "<$depto") || $args->death
        ("Failed to read DeprecatedTo file", $depto, $!);
    $args->msg("[<]","Reading DeprecatedTo file", $depto);
    my %hash;
    while (<DEPTO>) {
        s/[\n\r]+$//;
        my ($from, $to) = split(/\t/);
        if (my $prior = $hash{$from}) {
            unless ($prior eq $to) {
                $args->msg("[!!]", "$from deprecates to both $prior and $to");
            }
        } else {
            $hash{$from} = $to;
        }
    }
    close DEPTO;
    my $changes = -1;
    my $lvl = 0;
    while ($changes) {
        $changes = 0;
        $lvl++;
        foreach my $from (keys %hash) {
            next unless (exists $hash{$from});
            my $to = $hash{$from};
            if ($to eq $from) {
                $args->msg("[!]","$from got deprecated to itself!");
                delete $hash{$from};
            } elsif (exists $hash{$to}) {
                my $toto = $hash{$to};
                # $args->msg("[+]", "$from -> $to -> $toto");
                $hash{$from} = $toto;
                $changes++;
            }
        }
        $args->msg("[LVL $lvl]","$changes tertiary remappings");
    }
    &initialize();
    foreach my $from (sort keys %hash) {
        my $to = $hash{$from};
        if ($from eq $to) {
            $args->msg("[!]","$from got deprecated to itself!");
            next;
        }
        &set_deprecated($from, $to);
    }
    $lh->write();
}

sub parse_group {
    my $info = shift;
    # return unless ($info);
    my $acc  = $info->{id};
    my $num  = $info->{num};
    my ($release, $vers, $state, $dest) = @{$info->{data}[0]};
    my $accv = $vers ? "$acc.$vers" : undef;
    my @ids = ($acc);
    push @ids, $accv if ($accv);
    if ($state eq 'CURRENT') {
        $lh->set_class($acc, 'unversioned');
        $lh->set_class($accv, 'versioned');
        &set_live( @ids );
    } else {
        $lh->set_class($accv, 'DEPRECATED');
        &set_deprecated($acc, $dest);
        if ($dest) {
            $fc->write_output('depto', join("\t", $acc, $dest)."\n");
        }
    }
    unless ($num == 1) {
        my @row = map { sprintf("%d=%s", $_->[0], $_->[2]) } @{$info->{data}};
        $fc->write_output('chain', join("\t", $num, $acc, @row)."\n");
    }
    $lh->write_threshold_quick($cache);
}

sub set_live {
    foreach my $acc (@_) {
        $lh->kill_class($acc, 'DEPRECATED', 0, 'NullOK');
        $lh->kill_edge( -name1    => $acc,
                        -auth     => 0,
                        -override => 'nullok',
                        -type     => $iadef, );
    }
}

sub set_deprecated {
    my ($acc, $dest) = @_;
    $lh->set_class($acc, 'DEPRECATED');
    $lh->kill_edge( -name1 => $acc,
                    -auth  => 0,
                    -override => 'nullok',
                    -type  => $iadef);
    if ($dest) {
        $lh->set_edge( -name1 => $acc,
                       -name2 => $dest,
                       -type  => $iadef );
    }
}

sub group_entries {
    my $row = shift;
    # Finish when we run out of data:
    return &_grouped_rows() unless ($row);
    if ($#rowGroup == -1 || $rowGroup[0][0] eq $row->[0]) {
        # Either a new group, or the ID is the same as the current group
        # Extend the group
        push @rowGroup, $row;
        return undef;
    }
    # A different ID
    # Prepare the prior one for return:
    my $rv = &_grouped_rows();
    # ... and start the new group:
    push @rowGroup, $row;
    return $rv;
}

sub _grouped_rows {
    return undef if ($#rowGroup == -1);
    my $rv = {
        num => $#rowGroup + 1,
        id  => $rowGroup[0][0],
    };
    # Shift off the ID from each row:
    map { shift @{$_} } @rowGroup;
    # Sort the 
    $rv->{data} = [ sort { $b->[0] <=> $a->[0] } @rowGroup ];
    @rowGroup = ();
    return $rv;
}

sub make_summary_file {
    if (-s $summary && !$clobber) {
        $args->msg("[<]","Using existing summary file", $summary, 
                   "Set -clobber to recompute");
        return;
    }
    # $summary      .= "-LIMIT" if ($limit);
    my @vnum = sort { $b <=> $a } keys %{$releases};
    $args->msg("Identified ".scalar(@vnum)." releases", join(' ', @vnum));
    my $now = $vnum[0];
    unlink($summary);
    open(SUMFILE, ">$summary") || $args->death
        ("Failed to write summary file", $summary, $!);
    open(NOVERS, ">$noVersion") || $args->death
        ("Failed to write versionless file", $noVersion, $!);
    if ($keepOdd) {
        $args->msg("[+]","Keeping all IDs");
    } else {
        $args->msg("[-]","Only keeping IDs starting with $notOdd");
    }
    &set_current($now);
    map { &set_dep_tsv( $_ ) } @vnum;
    close SUMFILE;
    close NOVERS;
    $args->msg("[FILE]","Generated summary file", $summary);
    if (-s $noVersion) {
        $args->msg("[FILE]","Generated versionless file", $noVersion);
    } else {
        unlink($noVersion);
        $args->msg("All accessions were versioned");
    }
    &report_modes("Deprecation modes across all releases");
}

sub report_modes {
    my $head = shift;
    my @seen = sort { $modes{$b} <=> $modes{$a} } keys %modes;
    $args->msg($head, map { sprintf("%10d %s", $modes{$_}, $_) } @seen);
    %modes = ();
}

sub set_current {
    my $v = shift;
    $args->death("No current version provided") unless ($v);
    my $catalog = $releases->{$v}{catalog};
    if (!$catalog) {
        $args->msg("[!!]", "No catalog file found for current version $v");
        return;
    } elsif (! -s $catalog) {
        $args->msg("[!!]", "No catalog file is empty", $catalog);
        return;
    }
    $args->msg("[<]","Processing current catalog, version $v", $catalog);
    my $vtok = sprintf("%03d", $v);
    my $tr = BMS::TableReader->new
        ( -limit  => 0,
          -format => 'tsv' );
    $tr->input( $catalog );
    $tr->select_sheet(1);
    while (my $row = $tr->next_row()) {
        my $id = $row->[2];
        if ($id) {
            next if (!$keepOdd && $id !~ /$notOdd/);
            my $vnum = 0;
            if ($id =~ /^(\S+)\.(\d+)$/) {
                ($id, $vnum) = ($1, $2);
            } else {
                print NOVERS "$id\t$vtok\n";
            }
            print SUMFILE "$id\t$vtok\t$vnum\tCURRENT\n";
        }
    }
}

sub set_dep_tsv {
    my $v = shift;
    return unless ($v);
    my $rr = $releases->{$v}{'removed-records'};
    return unless ($rr && -s $rr);
    my $short = $rr;
    $short =~ s/.+\///;
    $args->msg("[<]","Processing removed records from $v : $short");
    my $vtok = sprintf("%03d", $v);
    my $tr = BMS::TableReader->new
        ( -limit  => 0,
          -format => 'tsv' );
    $tr->input( $rr );
    $tr->select_sheet(1);
    while (my $row = $tr->next_row()) {
        my $id = $row->[2];
        next unless ($id);
        next if (!$keepOdd && $id !~ /$notOdd/);
        my $vnum = 0;
        if ($id =~ /^(\S+)\.(\d+)$/) {
            ($id, $vnum) = ($1, $2);
        } else {
            print NOVERS "$id\t$vtok\n";
        }
        my $what = $row->[7] || "unknown";
        # at least one double space in: 'release 6  by'
        $what =~ s/\s+/ /g;
        $what =~ s/\s+$//;
        $what =~ s/^\s+//;
        my $live = "";
        if ($what =~ /(replaced by) (.+)/) {
            $what = $1;
            $live = $2;
        }
        $modes{$what}++;
        print SUMFILE "$id\t$vtok\t$vnum\t$what\t$live\n" if ($id);
    }
}

sub files {
    my %releases;
    foreach my $file ($args->read_dir( -dir => $srcdir,
                                       -recurse => 1,
                                       -keep => '\.gz$' )) {
       # warn $file;
        if ($file =~ /release(\d+)\.(catalog|removed-records)\.gz$/i) {
            my ($v, $type) = ($1, $2);
            my $targ = $releases{$v} ||= {};
            my $tt   = $targ->{$type};
            if ($tt && $tt ne $file) {
                # There are two files of the same type and version
                if ($file !~ /\/archive\//) {
                    # The second one is not archived
                    if ($tt =~ /\/archive\//) {
                        # The currently held one is in the archived directory
                        # That is the one we should use - do nothing
                        # The other file is likely leftover from an old wget
                    } else {
                        # Huh.
                        $args->msg("[!!]", "Multiple non-archived $type files for $v",
                                   $file, $tt);
                    }
                } else {
                    # The second file is in the archive
                    if ($tt =~ /\/archive\//) {
                        $args->msg("[!!]", "Multiple archived $type files for $v", 
                                   $file, $tt);
                    } else {
                        # The second file is archived, first is not
                        # We should use the second file instead
                        $targ->{$type} = $file;
                    }
                }
            } else {
                $targ->{$type} = $file;
            }
        }
    }
    return \%releases;
}

sub mirror {
    my $ftp = "ftp://ftp.ncbi.nlm.nih.gov/refseq/release/release-catalog";
    my $cmd = "wget -t 45 -P $srcdir --no-parent -r -l 1 -N";
    system("$cmd -A '*.catalog.gz' $ftp/");
    system("$cmd -A '*.removed-records.gz' $ftp/");
    system("$cmd -A '*.removed-records.gz' $ftp/archive");
}

sub initialize {
    $lh = BMS::MapTracker::LoadHelper->new
        ( -username => 'RefSeq',
          -loaddir  => $baseDir,
          -testfile => $fc ? undef : $testfile,
          -testmode => $tm,
          );
    $mt = $lh->tracker();
    if ($fc) {
        if (my $fh = $fc->output_fh('TestFile')) {
            $lh->redirect( -stream => 'TEST', -fh => $fh );
        }
    }
}

sub finish {
    $lh->write();
    if ($baseDir && !$tm) {
        if ($lh->process_ready()) {
            sleep(300);
            $lh->process_ready();
        }
    }
}
