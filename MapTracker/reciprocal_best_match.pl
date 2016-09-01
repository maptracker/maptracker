#!/stf/biobin/perl -w

my $isBeta = 0;
BEGIN {
    # Allows usage of beta modules to be tested:
    my $progDir = join(' ', $0, `pwd`);
    if ($progDir =~ /(working|perllib)/) {
        $isBeta = 1;
	require lib;
	import lib '/stf/biocgi/tilfordc/perllib';
    }
    require lib;
    import lib '/stf/biocgi/tilfordc/released/Bio/SeqIO';
}

my $VERSION = ' $Id$ ';

=head1 Usage

In the examples below, nothing will happen until '-trial 0' is
specified. Run first in trial mode to make sure the program will be
doing what you desire.

=head2 Halted Jobs

Re-execute jobs flagged as halted. Halted jobs occur when it looks
like a large number of edges are being deleted. This can occur when
the blast databases are only partly dumped.

  reciprocal_best_match.pl -testmode 0 -unhalt -trial 0 -fork 20

=cut

use strict;
use BMS::MapTracker::LoadHelper;
use BMS::MapTracker;
use BMS::BmsArgumentParser;
use BMS::FriendlyClustalw;
use BMS::ForkCritter;
use Bio::DB::Fasta;
use Bio::PrimarySeq;
use BMS::SequenceLibraryFinder;

$ENV{BLASTFILTER} = '/stf/biolib/filter';
$ENV{BLASTDB}     = '/gcgblast';
$ENV{BLASTMAT}    = '/stf/biolib/matrix';

my $args = BMS::BmsArgumentParser->new
    ( -nocgi     => 1,
      -h         => undef,
      -db        => "",
      -testmode  => 1,
      -e         => 1e-10,
      -fork      => 1,
      -limit     => 0,
      -numalign  => 10,
      -verbose   => 1,
      -shadow    => 50,
      -progress  => 180,
      -cache     => 200,
      -nocgi     => 1,
      -workdir   => '/stf/biocgi/ReciprocalBestMatch',
      -messages  => "ReciprocalBestHit_Messages.txt",
      -algorithm => 'needle',
      -type      => '',
      -debug     => 0,
      -eskim     => 5,
      -killa     => 10,
      -age       => 0,
      -basedir   => 'RBM',
      -localdir  => '/scratch/RBM_Local/',
      -dbdir     => '/gcgblast',
      -taxa      => '',
      -worklocal => 1,
      -maxdb     => 0,
      );

$args->ignore_error("IO.pm line 505");
$args->shell_coloring();

# die $args->to_text();

# die &append_null_subjects( '/stf/biocgi/ReciprocalBestMatch/refseq_mouse_nuc_vs_refseq_human_nuc.blast', '/gcgblast/refseq_human_nuc');

my $workDir   = $args->{WORKDIR}; $workDir =~ s/\/+$//;
system("mkdir -p '$workDir'");
chmod(0777, $workDir);
my $startT    = time;
my $slf       = BMS::SequenceLibraryFinder->new();
my $tm        = $args->val(qw(tm testmode));
my $testfile  = $args->{TESTFILE};
my $workLocal = $args->{WORKLOCAL};
my $locDir    = $args->{LOCALDIR};
my $age       = $args->{AGE};
my $vb        = $args->val(qw(vb verbose)) || 0;
my $trial     = $args->val(qw(trial istrial));
$vb           = $trial unless (defined $vb);
my $dbvb      = $args->{DEBUG};
my $forkProg  = $args->{PROGRESS} || 0;

my $min       = $args->val(qw(min mincov));
my $lhcache   = $args->{CACHE};
my $numalign  = $args->{NUMALIGN};
my $killa     = $args->{KILLA};
my $mode      = lc($args->{MODE} || '');
my $limit     = $args->val('limit');
my $baseDir   = $args->val(qw(loaddir basedir));
my $alg       = $args->val(qw(method algorithm alg));
my $fileReq   = $args->val(qw(file fasta));
my $timeCache = 15 * 60; # Fifteen minutes
my $forkNum   = $args->{FORK};
my $doOldest  = $args->{OLDEST};
my $clobber   = $args->{CLOBBER};
my $unHalt    = $args->val(qw(unhalt)) ? 1 : 0;
my $reuse     = $clobber ? 0 : $unHalt ? 1 : $args->val(qw(reuse)) ? 1 : 0;
my $allowUnv  = $args->{UNVERSIONED} ? 1 : 0;
my $newOnly   = $args->val('newonly');
my $reloadRBM = $args->val(qw(reloadrbm));
my $overRide  = $unHalt ? 1 : $args->val(qw(forceload));
my $checkSfx  = lc($args->val(qw(suffix)) || 'rbm');

my %blastWord = ( blastn => 11, megablast => 28 );

if (!$vb) {
    $args->msg_callback(0);
} elsif ($vb =~ /terse/) {
    $forkProg = 0;
    my $oldCB = $args->msg_callback();
    $args->msg_callback(sub {
        my ($obj, $lines) = @_;
        return if (!$lines || $#{$lines} == -1);
        return unless ($lines->[0] =~ /DB|ERROR|HALT/);
        return &{$oldCB}(@_);
    });
}

my $okSfx = join('|', qw(blast align rbm old));
unless ($checkSfx =~ /^$okSfx$/) {
    $args->death("You have requested that age checks look for suffix '$checkSfx'", "The only allowed suffices are $okSfx");
}

$args->msg("[BETA]","This code is beta software") if ($isBeta);

$mode .= 'xeno' if ($args->{XENO});
my $doXeno    = $mode =~ /(xeno|both|orth)/ ? 1 : 0;
my $doIntg    = ($mode =~ /int/ || $mode !~ /xeno/) ? 1 : 0;

my %nice = ( rna => 'RNA',
             protein => 'Protein',
             refseq  => 'RefSeq',
             ensembl => 'Ensembl',
             ipi     => 'IPI',
             swissprot => 'SwissProt', );
             
my @matchClass = ('Non-Reciprocal Match',
                  'Reciprocal Sub-optimal Match',
                  'Reciprocal Best Match');

my $mainClearAuths = [ 'ClustalW', 'tilfordc', 'Needleman-Wunsch', 'Stretcher' ];
my (@files, @msg, $seqUnits);

if (my $file = $args->{SIMPLIFY}) {
    my $num = &simplify_file($file);
    $args->msg("[Simplify] $num lines written to $file");
    exit;
}

push @msg, ['Reprocessing', 'Prior intermediate analyses will '.
            ($clobber ? 'always be overwritten' :
             $reuse ? 'always be reused' : 'be reused if newer than input')];

push @msg, ['Test Mode', 'Database will not be altered'] if ($tm);
push @msg, ['Reload RBM', 'Prior .rbm files will be reloaded'] if ($reloadRBM);
push @msg, ['Test File', "Testing output to $testfile"] if ($tm && $testfile);
push @msg, ['Force Load', "Results will be loaded even if there are many deletions"] if ($overRide);
push @msg, ['Forking', "$forkNum processes will be used for each search"],
    if ($forkNum && $forkNum > 1);

my $doOnly;
my @explicitIDs = split(/[\n\r\s\,]+/, $args->{ID} || $args->{IDS} || '');
if (my $file = $args->{IDFILE}) {
    my $isErr = $file =~ /\.err$/ ? 1 : 0;
    open(IDF, "<$file") || $args->death("Failed to read idfile", $file, $!);
    while (<IDF>) {
        s/[\n\r]+$//;
        if ($isErr) {
            if (/^\[x\] (\S+)/) { push @explicitIDs, $1 }
        } else {
            my ($id) = split(/[\s\t]+/);
            push @explicitIDs, $id if ($id);
        }
    }
    close IDF;
}
if ($#explicitIDs > -1) {
    $doOnly = {};
    foreach my $id (@explicitIDs) {
        $id =~ s/^[^\:]+\://;
        $id =~ s/\.\d+$//;
        next unless ($id);
        $doOnly->{uc($id)} = 1;
    }
    my @reqs = keys %{$doOnly};
    $args->death("Request to analyze explicit set of IDs found no identifiers")
        if ($#reqs < 0);
    $limit ||= $#reqs + 1;
    push @msg, ["Explicit ID Request", ($#reqs+1)." IDs provided"];
}

my (@pairs, @singleSets);
if ($unHalt) {
    my @files = split(/[\n\r]+/, `ls -1 $workDir/*-HALTED`);
    if ($#files == -1) {
        push @msg, ["Redo Halt","No halted files found"];
    } else {
        my @errs;
        foreach my $file (@files) {
            my ($pair, $err) = &pair_for_file( $file );
            if ($pair) {
                push @pairs, $pair;
                $pair->{w} = "Halted file";
            }
            push @errs, @{$err};
        }
        if ($#errs != -1) {
            $args->err("Problems encountered identifying halted pairs",
                       @errs);
        }
        push @msg, ["Redo Halt","Re-running ".scalar(@pairs)." halted pairs"];
    }
} elsif ($fileReq) {
    my @paths;
    my $isDir = 0;
    if (my $fref = ref($fileReq)) {
        if ($fref eq 'ARRAY') {
            @paths = @{$fileReq};
        } else {
            $args->death("Not sure what to do with -file $fileReq");
        }
    } elsif (-d $fileReq) {
        $isDir = 1;
        # The user passed a directory
        opendir(TMPDIR, $fileReq) || $args->death
            ("Failed to read contents of directory", $fileReq, $!);
        foreach my $file (readdir TMPDIR) {
            next if ($file =~ /^\./);
            my $path = "$fileReq/$file";
            next unless (-f $path);
            # Verify that the file is a fasta file
            my $check = `head -n2 $path`;
            next unless ($check =~ /^\>/);
            push @paths, $path;
        }
        closedir TMPDIR;
    } else {
        @paths = split(/[\n\r\,\s]+/, $fileReq);
    }
    my ($taxa, $ns) = ([],[]);
    foreach my $tr (split(/[\n\r\t\,]+/,$args->{SPECIES} || $args->{TAXA})) {
        my ($t) = $slf->stnd_taxa($tr);
        push @{$taxa}, $t;
    }
    foreach my $nr ($slf->stnd_list($args->{NS})) {
        my ($n) = $slf->stnd_ns($nr);
        push @{$ns}, $n;
    }

    my @dbDirs;
    if (my $req = $args->val(qw(dbdir dbdirs))) {
        my @list = ref($req) ? @{$req} : split(/\s*[\,]\s*/, $req);
        foreach my $d (@list) {
            $d =~ s/\/+$//;
            push @dbDirs, $d if (-d $d);
        }
    }
    for my $p (0..$#paths) {
        my $path = $paths[$p];
        next unless ($path);
        my @tf = ($path, map { "$_/$path" } @dbDirs);
        foreach my $p (@tf) {
            if (-e $p) {
                $path = $p;
                last;
            }
        }
        unless (-f $path && -s $path) {
            &err("Invalid file", $path);
            next;
        }
        
        my ($head, $seq) = split(/[\n\r]+/,`head -n4 $path`);
        unless ($head =~ /^\>/) {
            &err("Invalid fasta file", $path, "First line: $head");
            next;
        }
        my $chars = $seq;
        $chars =~ s/^\S//;
        unless ($chars) {
            &err("Invalid fasta file", $path, "Second line: $seq");
            next;
        }
        my %cCount;
        my @c = split('', $chars);
        map { $cCount{ uc($_) }++ } @c;
        my $nucs = 0; map { $nucs += $cCount{$_} || 0 } qw(A C T G U N);
        my $type = ($nucs / ($#c+1)) > 0.9 ? 'dna' : 'protein';

        my ($pTax, $pNs, $pType) = $slf->params_for_file( $path );
        $pTax = $taxa->[$p] if ($taxa->[$p]);
        $pTax ||= 'Unknown';
        $pNs = $ns->[$p] if ($ns->[$p]);
        $pNs ||= 'Unknown';
        
        push @files, { path => $path,
                       type => $type,
                       taxa => $pTax,
                       ns   => $pNs, };
    }
    push @msg, ['User-specified file list', ($#files+1). ' files'];
} else {
    my $taxa = $slf->stnd_taxa($args->val(qw(species taxa)));
    my $ns   = $slf->stnd_namespace($args->{NS});
    my @molR = $slf->stnd_moltype($args->val(qw(moltype type)));
    my @mol;
    foreach my $mot (@molR) {
        if ($mot =~ /^(protein|rna)$/) {
            push @mol, $mot;
        } else {
            &err("Ignoring molecule type", $mot);
        }
    }
    $ns   = undef if ($#{$ns}   == -1);
    if ($#{$taxa} == -1) {
        # Use all taxa if none defined
        $taxa = undef;
    } else {
        # Otherwise make sure the versus taxa are included as well.
        $taxa = $slf->stnd_taxa($taxa, $args->{VERSUS});
    }

    # @ns  = $slf->all_namespaces() if ($#ns == -1);
    @mol = ('protein','rna') if ($#mol == -1);
    my $found = $slf->get_libraries( -taxa => $taxa,
                                     -best => 0,
                                     -type => \@mol,
                                     -ns   => $ns );
    unless ($ns) {
        # Remove affy hits
        foreach my $path (keys %{$found}) {
            delete $found->{$path} if ($found->{$path}[1] eq 'affy');
        }
    }
    my @fnum = keys %{$found};
    if ($#fnum == -1) {
        $args->death("No sequence files were identified with your request");
    }

    my %nss;
    while (my ($path, $dat) = each %{$found}) {
        my ($tx, $ns, $type) = @{$dat};
        unless ($tx) {
            $args->msg("[!!]","Can not determine taxa from file path", $path);
            next;
        }
        push @files, { path => $path,
                       type => $type,
                       taxa => $tx, 
                       ns   => $ns };
        $nss{$ns}++;
    }
    my @nsNum = keys %nss;
    my $cmode;
    if ($doXeno && $doIntg) {
        $cmode = "Full analysis: Both Orthologue and Integration searches";
    } elsif ($doXeno) {
        $cmode = "Different taxae = Orthologue Identification";
    } else {
        $cmode = "Different namespaces = Data Integration";
    }
    push @msg, ["Comparison mode", $cmode];
}
map { $_->{original} = $_->{path} } @files;

my %struct;
if ($workLocal) {
    push @msg, ["Local Copies","Searching is being performed on local copies"];
    $locDir ||= '/scratch/RBM_Local/';
    $locDir =~ s/\/+$//;
    $locDir .= "/$$";
    unless ($trial) {
        my @bits = split(/\/+/, $locDir);
        my $path = "";
        foreach my $bit (@bits) {
            next unless ($bit);
            $path .= "/$bit";
            next if (-d $path);
            mkdir($path);
            chmod(0777, $path);
            $args->death("Failed to create local file directory",$path)
                unless (-d $path);
        }
        system("rm -rf $path/*");
    }
    push @msg, ["Local Directory", $locDir];
    my @toChange = @files;
    push @toChange, map { $_->{q}, $_->{s} } @pairs;

    foreach my $dat (@toChange) {
        my $path = $dat->{original};
        my @bits = split(/\/+/, $path);
        $dat->{path} = "$locDir/$bits[-1]";
    }
}
foreach my $dat (@files) {
    push @{$struct{$dat->{type}}}, $dat;
}

my %required;
foreach my $tag (qw(taxa ns type)) {
    my $req = $args->{uc("REQ$tag")};
    next unless ($req);
    my @vals = 
        ($tag eq 'taxa') ? $slf->stnd_taxa( $req ) :
        ($tag eq 'type') ? $slf->stnd_moltype( $req ) :
        ($tag eq 'ns')   ? $slf->stnd_namespace( $req ) : ();
    next if ($#vals == -1);
    $required{$tag} = { map { $_ => 1 } @vals };
    push @msg, ["Pair Requirement", "At least one with $tag of ".
                join(" or ", map { $nice{$_} || $_ } @vals)];
}

while (my ($type, $dat) = each %struct) {
    my %sets;
    if ($fileReq) {
        $sets{"User Database List"} = $dat;
    } else {
        map { push @{$sets{$_->{ns}}},   $_ } @{$dat} if ($doXeno);
        map { push @{$sets{$_->{taxa}}}, $_ } @{$dat} if ($doIntg);
    }
    while (my ($what, $set) = each %sets) {
        if ($#{$set} == 0) {
            my $sing = $set->[0];
            push @singleSets, sprintf("%s %s %s", map { $nice{$_} || $_ } 
                                      map { $set->[0]{$_} } qw(taxa ns type));
            next;
        }
        for my $s1 (0..($#{$set}-1)) {
            my $qset = $set->[$s1];
            for my $s2 (($s1+1)..$#{$set}) {
                my $sset = $set->[$s2];
                my $ok = 1;
                while (my ($tag, $hash) = each %required) {
                    $ok = 0 unless ($hash->{ $qset->{$tag} } ||
                                    $hash->{ $sset->{$tag} } );
                }
                next unless ($ok);
                push @pairs, { q => $qset,
                               s => $sset,
                               t => $type, 
                               w => $what };
            }
        }
    }
}


$args->msg("Found ".sprintf("%d database%s", $#singleSets + 1,
                      $#singleSets == 0 ? '' : 's').
     " lacking partners", @singleSets,' ') unless ($#singleSets == -1);

if (my $tx = $args->{VERSUS}) {
    # Only do pairs against one or more specific species
    my @vs = $slf->stnd_taxa($tx);
    my %ok = map { $_ => 1 } @vs;
    my @keep;
    foreach my $pair (@pairs) {
        my $isOk = 0;
        map { $isOk++ if ($ok{ $pair->{$_}{taxa} }) } qw(q s);
        push @keep , $pair if ($isOk);
    }
    if ($#keep < $#pairs) {
        push @msg, ["Fixed 'Versus' species set", join(" | ", sort keys %ok) .
                    sprintf("  (Kept %d, Excluded %d)\n", $#keep + 1, 
                            $#pairs - $#keep)];
        @pairs = @keep;
    }
}

my $isPartial = ($limit || $doOnly) ? 1 : 0;
my $useLargeForQuery = ($args->{LARGEQUERY}) ? 1 : 0;
push @msg, ["Query Choice", ($useLargeForQuery ? 'Larger' : 'Smaller').
            " database will be used as the query"];
push @msg, ["Partial Search", $limit ? "Limit of $limit" : $doOnly ?
            "Explicit list of IDs provided" : "?? Not sure why!! ??"]
    if ($isPartial);

foreach my $pair (@pairs) {
    my ($q, $s) = ($pair->{q}, $pair->{s});
    map { $_->{sz} ||= -s $_->{original} } ($q, $s);
    $pair->{szsz} = $q->{sz} * $s->{sz};
    my $queryIsSmall = ($q->{sz} < $s->{sz}) ? 1 : 0;
    if ($queryIsSmall == $useLargeForQuery) {
        # We need to use the other DB as the query
        ($q, $s) = ($s, $q);
        $pair->{q} = $q;
        $pair->{s} = $s;
    }
    
    foreach my $db ($q, $s) {
        my @bs = split(/\//, $db->{original});
        my $bit = $bs[-1];
        $bit =~ s/\..{1,6}//;
        $bit =~ s/[\s_]+/_/;
        $db->{fname} = $bit;
    }
    # Sorted by cleaned-up name:
    my ($d1, $d2) = sort { lc($a->{fname}) cmp lc($b->{fname}) } ($q, $s);
    my (@com, @diff);
    foreach my $key ('taxa','ns','type') {
        my ($t1, $t2) = map { $nice{$_} || $_ } map { $_->{$key} } ($d1, $d2);
        next unless ($t1 && $t2);
        if ($t1 eq $t2) {
            push @com, $t1;
        } else {
            push @{$diff[0]}, $t1;
            push @{$diff[1]}, $t2;
        }
    }

    my @bits;
    unless ($#com == -1) {
        push @bits, join('-', @com);
    }
    unless ($#diff == -1) {
        push @bits, sprintf("%s-VS-%s", 
                            join("+", @{$diff[0]}), join("+", @{$diff[1]}));
    }
    if ($#bits == -1) {
        push @bits, sprintf("%s-VS-%s", $d1->{fname}, $d2->{fname});
    }
    unshift @bits, ($isPartial ? "Partial-" : $tm ? "TestMode-" : "" ) . "RBM";
    

    my $pairTag = join('.', @bits);
    $pairTag    =~ s/\s+/_/g;
    $pair->{tag} = $pairTag;
}

my $notDone = 999999;
my $maxSize = 0;
if ($doOldest || $newOnly) {
    # Churn mechansism - find the oldest search(es), and just do them.
    $doOldest ||= 9999;
    if ($doOldest =~ /^\d+$/) {
        # Request just to do the N oldest pairs - leave as is
    } elsif ($doOldest =~ /^(\d+)\s*Mb/i) {
        # Request to get the oldest pairs up to a square-Mb limit
        $doOldest = 0;
        $maxSize  = $1 * 1000000 * 1000000;
    } else {
        # Just do the single oldest pair
        $doOldest = 1;
    }
    my (@sorted, @halted);
    foreach my $pair (@pairs) {
        # warn $args->branch($pair) if ($pair->{tag} =~ /RefSeq-Protein.Bos_taurus-VS-Drosophila_melanogaster/);
        my $file    = sprintf("%s/%s.%s", $workDir, $pair->{tag}, $checkSfx);
        my $fileAge;
        if (-e $file) {
            $fileAge = -M $file unless ($newOnly);
        } else {
            $fileAge = $notDone;
        }
        next unless (defined $fileAge);
        if (&use_existing_file($file,$pair->{s},$pair->{q})) {
            if (my $why = &is_halted($pair)) {
                push @halted, $pair->{tag};
            }
            next;
        }
        
        push @sorted, [$fileAge, $pair];
        $pair->{age} = $fileAge;
    }
    @sorted = sort { $b->[0] <=> $a->[0] ||
                         $a->[1]{szsz} <=> $b->[1]{szsz} } @sorted;
    @sorted = splice(@sorted, 0, $doOldest) if ($doOldest);
    @pairs = ();
    unless ($#halted == -1) {
        $args->msg("[HALT]", "Some pairs have been halted", sort @halted);
    }
    if ($#sorted == -1) {
        push @msg, ["No Action",
                    "No pairs were found matching your time criteria"];
    } else {
        if ($newOnly) {
            push @msg, ["New Searches Only",
                        "Only running searches not yet performed"];
        } else {
            push @msg, ["Update Oldest", sprintf
                        ("Only updating the %d oldest search%s", $#sorted + 1,
                         $#sorted == 0 ? '' : 'es')] if ($doOldest);
        }
        while (my $dat = shift @sorted) {
            my ($oldage, $pair) = @{$dat};
            $pair->{why} = ($oldage == $notDone) ? 'New Search' : sprintf
                ("%.1f days old", $oldage);
            push @pairs, $pair;
        }
    }
} elsif ($age) {
    my (@keep, @done);
    my %reason;
    my $ageReason = sprintf("completed in past %.2f days", $age);
    foreach my $pair (@pairs) {
        my $rfile = sprintf("%s/%s.%s", $workDir, $pair->{tag}, $checkSfx);
        my $process = 0;
        if (-e $rfile) {
            my $fage = $pair->{age} = -M $rfile;
            if ($fage >= $age) {
                # Output file exists and is OLDER than the age limit
                # If the input is newer than the file, we will process
                if (&use_existing_file( $rfile, $pair->{q}, $pair->{s})) {
                    # No need to update
                    $reason{"with results newer than source files"}++;
                } else {
                    $process = 1;
                }
            } else {
                $reason{$ageReason}++;
            }
        } else {
            # pair has not been processed
            $pair->{age} = $notDone;
            $process = 1;
        }
        if ($process) {
            push @keep, $pair;
        } else {
            push @done, $pair;
        }
    }
    foreach my $reas (sort keys %reason) {
        my $n = $reason{$reas};
        push @msg, ["Refresh Only", sprintf("Ignoring %d job%s %s", $n,
                                            $n == 1 ? '' : 's', $reas)];
    }
    @pairs = sort { $b->{age} <=> $a->{age}  ||
                        $a->{szsz} <=> $b->{szsz} } @keep;
    foreach my $pair (@pairs) {
        my $oldage = $pair->{age};
        $pair->{why} = ($oldage == $notDone) ? 'New Search' : sprintf
            ("%.1f days old", $oldage);
    }
} elsif (my $minAge = $args->val(qw(minage))) {
    my (@keep, @done);
    foreach my $pair (@pairs) {
        my $rfile = sprintf("%s/%s.%s", $workDir, $pair->{tag}, $checkSfx);
        my $process = 0;
        if (-e $rfile) {
            my $fage = $pair->{age} = -M $rfile;
            if ($fage <= $minAge) {
                # Output file exists and is YOUNGER than the age limit
                $process = 1;
            }
        } else {
            # pair has not been processed
            $pair->{age} = 0;
            $process = 1;
        }
        if ($process) {
            push @keep, $pair;
        } else {
            push @done, $pair;
        }
    }
    push @msg, ["Recalculate Recent", sprintf
                ("Ignoring %d job%s completed after the past %.2f days",
                 $#done + 1, $#done == 0 ? '' : 's', $minAge)];
    @pairs = sort { $a->{age} <=> $b->{age}  ||
                        $a->{szsz} <=> $b->{szsz} } @keep;
    foreach my $pair (@pairs) {
        my $oldage = $pair->{age};
        $pair->{why} = ($oldage == 0) ? 'New Search' : sprintf
            ("%.1f days old", $oldage);
    }
}


if ($#pairs == -1) {
    push @msg,("", ["Nothing to Do",
                    "Current pair selection criteria generate no searches"]);
    &dump_msg();
    exit;
}

my %allDBs = map { $_->{q}{original} => $_->{q}, 
                   $_->{s}{original} => $_->{s} } @pairs;
my @dbCount = sort keys %allDBs;
my @sumBits = (sprintf("Preparing %d databases for searching...", 
                       $#dbCount + 1));
my @sumWid = (25, 8, 4, 4, 10);
my $sumFrm = join(' ', map { '%'.$_.'s' } @sumWid);
push @sumBits, (sprintf($sumFrm, "File", "Mb", "Age", "Type", "Prefix"),
                sprintf($sumFrm, map { '-' x $_ } @sumWid));
foreach my $path (@dbCount) {
    my $dat    = $allDBs{$path};
    my $db     = $dat->{original};
    my @bits   = split(/\//, $path);
    $dat->{short} = $bits[-1];

    my $prfx = '';
    foreach my $line (split(/[\n\r]+/, `head $db`)) {
        if ($line =~ /^>(\S+)/) {
            my $id = $1;
            if ($id =~ /^([^\:]+\:)\S+/) {
                # The ids in this file have a prefix on them
                $prfx = $1;
            }
            last;
        }
    }
    my $type = $dat->{type};
    if ($type =~ /pr/i) {
        $type = "prot";
        $seqUnits = 'aa';
    } else {
        $seqUnits = 'bp';
    }
    $dat->{prfx} = $prfx;
    my $sz = int(0.5 + $dat->{sz} / 1000) / 1000;
    my $age = int(0.5 + (-M $path));
    my $u  = 'd';
    if ($age > 365) {
        $age = sprintf("%.1f", $age / 365);
        $u = 'y';
    }
    $age .= $u;
    push @sumBits, sprintf($sumFrm, $dat->{short},$sz,$age,$type,$prfx || '');
}
$args->msg(@sumBits);

if ($args->val('biggest')) {
    # Sort biggest DBs to front
    @pairs = sort { &_pairSize($b) <=> &_pairSize($a) } @pairs;
    push @msg, ["Prioritize Big", "Analyzing largest DB pairs first"];
} elsif ($doOldest || $age) {
    # Sort by age
    @pairs = sort { ($b->{age} || 0) <=> ($a->{age} || 0) } @pairs;
} else {
    # Sort smaller database pairings to the front
    @pairs = sort { &_pairSize($a) <=> &_pairSize($b) } @pairs;
}
if (my $maxDB = $args->val('maxdb')) {
    push @msg, ["Limit Search", sprintf
                ("Running at most %d searches", $maxDB)];
    @pairs = splice(@pairs, 0, $maxDB)
}

if ($maxSize && $#pairs != -1) {
    my $totSize = 0;
    my @kept;
    foreach my $pair (@pairs) {
        $totSize += $pair->{szsz};
        last if ($maxSize && $totSize >= $maxSize);
        push @kept, $pair;
    }
    if ($#kept == -1) {
        push @msg, ["Size Limit", "All DBs removed due to your size limit"];
    } elsif (my $rem = $#pairs - $#kept) {
        push @msg, ["Size Limit", sprintf
                    ("Removed %d DBs to stay within size limit", $rem)];
        @pairs = @kept;
    }
}
foreach my $pair (@pairs) {
    push @msg, ["Update", sprintf("%s [%s]", $pair->{tag}, $pair->{why})];
}

my ($keepAcc);
my $blastFilters   = [];
my $blastSkimmers  = [];
my $clustalFilters = [];
if (defined $args->{E}) {
    push @{$blastFilters}, "all.e <= " . $args->{E};
    push @msg, ['Maximum Blast e-value', $args->{E}];
}

if ($min) {
    $min *= 100 if ($min <= 1);
    # We are NOT going to filter at the search level by percent match
    # This allows the saved search result files to be quickly re-filtered
    # push @{$blastFilters}, "all.match_perc >= $min";
    # push @{$clustalFilters}, "all.average_perc_match >= $min";
    push @msg, ['Minimum alignment percent match', $min];
} elsif (defined $min) {
    push @msg, ['Minimum alignment percent match', 'NONE'];
}

if (my $skimE = $args->{ESKIM}) {
    # Only keep blast hits within this order of magnitude of top E-value
    my $val = int(10 ** $skimE);
    push @{$blastSkimmers}, "all.e $val";
    push @msg, ['Skimming blast hits within LOD', $skimE];
}
if (my $areq = $args->{ACC} || $args->{ACCESSION} || $args->{ACCS}) {
    $keepAcc = {};
    map { s/\.\d+$//; s/^[^\:]+\://; $keepAcc->{$_} = 1} split(/[\t\,\s]+/, uc($areq));
}

push @msg, ['Trial Run', 'No analysis - only list of DBs to process'] 
    if ($trial);

my $sqMb = 0;
my $MbMb = 10 ** 12;
map { $sqMb += $_->{q}{sz} * $_->{s}{sz} / $MbMb } @pairs;
push @msg, ['Total Search space',  sprintf("%d Mb*Mb (overall file size)",
                                           $sqMb) ];


my $stopTime = 0;
if (my $hr = $args->val(qw(runhour runhours))) {
    $stopTime = time + int( 60 * 60 * $hr );
    push @msg, ['Maximum Run Time',  sprintf("%.2f Hours", $hr) ];
}

unless ($reuse || $clobber) {
    my @keep;
    foreach my $pair (@pairs) {
        my $rfile = sprintf("%s/%s.%s", $workDir, $pair->{tag}, $checkSfx);
        push @keep, $pair
            unless (&use_existing_file($rfile,$pair->{s},$pair->{q}));
    }
    unless ($#pairs == $#keep) {
        if ($#pairs != -1 && $#keep == -1) {
            $args->msg("[-]","All ".scalar(@pairs)." search pairs were removed");
        }
        @pairs = @keep;
    }
}

my @mbits = (sprintf("%d pair%s will be compared [Size in Mb]", 
                     $#pairs+1, $#pairs == 0 ? '' : 's'));
my ($longQ) = sort { $b <=> $a } map { length($_->{q}{short}) } @pairs;
$longQ ||= 10;
my $pFrm = "%${longQ}s [%7.3f]+[%7.3f] %s";
foreach my $pair (@pairs) {
    my ($q, $s) = ($pair->{q}, $pair->{s});
    push @mbits, sprintf($pFrm, $q->{short}, 
                         $q->{sz} / 1000000, 
                         $s->{sz} / 1000000,$s->{short});
}
push @mbits, " ";
$args->msg(@mbits);
&dump_msg();

my ($fc, $lh, $mt, $fcw, $sdb, $qdb, $dbfS, $dbfQ, $blastDB );
my ($prog, $blastSize, $useMin, %fileData, %okAuths, %seenEID);

exit if ($trial);

$reloadRBM = 0;

# Run the searches...
my $pairNum = 0;
for my $p (0..$#pairs) {
    $pairNum = $p;
    &SEARCH( $pairs[$p] );
    &clean_files( $pairs[$p] );
    if ($stopTime && time >= $stopTime) {
        $args->msg( sprintf("Run halted %.2f hours after stop time",
                      (time - $stopTime) / (60 * 60)));
        last;
    }
}

my $elaps = time - $startT;
my @units = (['second', 60], ['minute', 60], ['hour', 24], ['day']);
while ($#units > 0 && $elaps > $units[0][1]) {
    my $disc = shift @units;
    $elaps /= $disc->[1];
}
$args->msg(sprintf("All searches finished - %.1f %ss", $elaps,
                   $units[0][0], $elaps == 1 ? '' : 's'),"");

# Final purge for all local files
system("rm -rf $locDir");

sub SEARCH {
    my ($pair) = @_;
    $args->msg(sprintf("Searching Pair %d of %d: %s vs %s", $pairNum+1, 
                       $#pairs + 1, $pair->{q}{short}, $pair->{s}{short}),
               $pair->{tag});
    $prog      = ($pair->{t} eq 'protein') ? 'blastp' : 'blastn';
    $useMin    = $min;
    unless (defined $useMin) {
        if ($pair->{s}{taxa} ne $pair->{q}{taxa}) {
            # Cross species xeno search
            $useMin = $prog eq 'blastn' ? 30 : 20;
        } else {
            $useMin = $prog eq 'blastn' ? 90 : 95;
        }
    }
    $blastSize = $blastWord{$prog} || 3;
    ($qdb, $sdb) = ($pair->{q}, $pair->{s});

    &find_and_load( $pair );
#    if (my $mnum = &simplify_file($msgfile)) {
#        warn "     [$mnum] $msgfile\n" if ($vb);
#    }
    
}

sub _pairSize {
    my $pair = shift;
    if ($pair) {
        return $pair->{q}{sz} * $pair->{s}{sz};
    } else {
        return 0;
    }
}

sub verify_db_integrity {
    my ($db, $odb) = @_;
    my $path = $db->{path};
    my $fdat = $fileData{$path};
    if ($fdat) {
        $fdat->{mod} = -M $path unless ($fdat->{mod});
        my $err = &files_have_changed( $db);
        return "File changed on disk: $err" if ($err);
    } else {
        # First time seeing this file
        $fdat = $fileData{$path} = $db;
        if ($workLocal) {
            # We are using locally copied databases
            # We need to copy files over
            my $src    = $fdat->{original};
            my @files  = split(/[\n\r]+/, `ls -1 $src.*`);
            my @toCopy = ($src);
            foreach my $file (@files) {
                if ($file =~ /\Q$src\E.(index|nal|count)$/ ||
                    $file =~ /^\Q$src\E(\.\d+)?\.(n|p)(hr|in|sd|si|sq|al)$/) {
                    push @toCopy, $file;
                }
            }
            my @copied;
            foreach my $file (@toCopy) {
                my @bits = split(/\/+/, $file);
                my $targ = "$locDir/$bits[-1]";
                system("cp --preserve=timestamps $file $targ");
                chmod(0777, $targ);
                push @copied, $targ;
            }
            unless ($db->{path} eq $copied[0]) {
                $args->death("Local copy of DB does not match original",
                             "$fdat->{path} != $copied[0]");
            }
            $db->{copied} = \@copied;
        }
        $fdat->{mod} = -M $path;
    }
    my $fdb = Bio::DB::Fasta->new( $path, -debug => $vb ? 0 : 0); undef $fdb;
    # Make sure httpd can re-write this file, if needed
    chmod(0666, "$db.index");

    if ($odb) {
        # We also want to verify that two DBs are still kosher together
        my $orv = &verify_db_integrity( $odb );
        return $orv if ($orv);
        my @paths   = sort map { $_->{path} } ($db, $odb);
        my $key     = join("\t", @paths);
        my ($newer) = sort { $a <=> $b } map { -M $_ } @paths;
        if (my $pd = $fileData{$key}) {
            return "Paired files have changed" if ($pd->{mod} > $newer);
        } else {
            $fileData{$key} = {
                pair => \@paths,
                key  => $key,
                mod  => $newer,
            };
        }
    }
    return 0;
}

sub files_have_changed {
    my @changed;
    foreach my $db (@_) {
        next unless ($db);
        push @changed, $db->{path} unless ($db->{mod} == -M $db->{path});
    }
    return join(" + ", @changed);
}

sub clean_files {
    my ($pair, $force) = @_;
    map { delete $_->{mod} } ($pair->{q}, $pair->{s});
    return unless ($workLocal);
    # Are there any local files that we no longer need?
    my %needed;
    unless ($force) {
        for my $p (($pairNum+1)..$#pairs) {
            my $pp = $pairs[$p];
            map { $needed{ $_->{path} }++ } ($pp->{q}, $pp->{s});
        }
    }
    foreach my $db ($pair->{q}, $pair->{s}) {
        my $path = $db->{path};
        next if ($needed{ $path });
        system("rm -f $path; rm -f $path.*") if (-e $path);
    }
}

sub find_and_load {
    my ($pair) = @_;
    my $rbmFile = &find_rbm( $pair );
    return 0 unless ($rbmFile);

    my %allIDs;
    open(FILE, "<$rbmFile") || $args->death
        ("Failed to read RBM file",$rbmFile,$!);
    my $count = 0;
    my $loadLimit = $args->{LOADLIMIT};
    my @setParams;
    my %setting;
    while (<FILE>) {
        s/[\n\r]+$//;
        my ($qtxt, $t, $stxt, $ptxt) = split(/\t/);
        my @vers = ( [$qtxt ? split(/\,/, $qtxt) : () ],
                     [$stxt ? split(/\,/, $stxt) : () ] );
        foreach my $v (0..$#vers) {
            foreach my $vid (@{$vers[$v]}) {
                my $uid = $vid; $uid =~ s/\.\d+$//;
                $allIDs{$v}{$uid}++;
            }
        }
        next unless ($t eq 'RBM');
        my %params = map { split('=') } split(/ \| /, $ptxt || "");
        my $percID = $params{id} || 0;
        next if ($useMin && $percID < $useMin);
        my $rTag = "\#Meta_Values\#".join
            (' : ', sort { $a <=> $b } map { $#{$_} + 1 } @vers);
        my @base = ( ["Average Percent ID", undef, $params{id} ],
                     ["Average Percent Similarity", undef, $params{similar} ],
                     ["Reciprocal Best Match", $rTag, undef] );
        foreach my $vq (@{$vers[0]}) {
            my $uq = $vq; $uq =~ s/\.\d+$//;
            foreach my $vs (@{$vers[1]}) {
                my $us = $vs; $us =~ s/\.\d+$//;
                my @tags = @base;
                push @tags, ( ["Sequence Version", $vq, undef],
                              ["Sequence Version", $vs, undef], );
                push @setParams, [ -name1 => $uq,
                                   -name2 => $us,
                                   -type  => 'is similar to',
                                   -tags  => \@tags,
                                   -auth  => $params{auth} ];
                my @tok = sort ($uq, $us);
                map { s/^\#.+\#// } @tok;
                $setting{$tok[0]}{$tok[1]} = 1;
            }
        }
        $count++;
        last if ($loadLimit && $count >= $loadLimit);
    }
    close FILE;

    my $oldFile = &get_old_edge_file( $pair, \%allIDs );
    return 0 unless ($oldFile);

    my $lh = &new_load_helper();
    my $setCount = $#setParams + 1;
    my $remCount = 0;
    my %actions;
    if (-s $oldFile) {
        # There is at least one old edge we need to remove
        open(OLD, "<$oldFile") || $args->death
            ("Failed to read OldEdge file", $oldFile, $!);
        while (<OLD>) {
            chomp;
            my ($n1, $n2, $t, $kauth) = split(/\t/);
            $remCount++;
            $lh->kill_edge( -name1 => $n1,
                            -name2 => $n2,
                            -type  => $t,
                            -auth  => $kauth );
            my @tok = sort ($n1, $n2);
            map { s/^\#.+\#// } @tok;
            if ($setting{$tok[0]}{$tok[1]}) {
                $setting{$tok[0]}{$tok[1]} = -1;
                $actions{Update}++;
            } else {
                $actions{Delete}++;
                $setting{$tok[0]}{$tok[1]} = 0;
            }
        }
        close OLD;
    } else {
        $args->msg("No old file found");
    }
    while (my ($n1, $n2s) = each %setting) {
        while (my ($n2, $stat) = each %{$n2s}) {
            $actions{Create}++ if ($stat == 1);
        }
    }
    if (0) {
        # Print list of paired sequences
        foreach my $n1 (sort keys %setting) {
            foreach my $n2 (sort keys %{$setting{$n1}}) {
                print join("\t", $n1, $n2, $setting{$n1}{$n2})."\n";
            }
        }
    }
    map { $actions{$_} ||= 0 } qw(Create Update Delete);
    my @bits = map { "$_ $actions{$_}" } sort keys %actions;
    $args->msg("  [DB]", "$pair->{q}{short} vs. $pair->{s}{short} : ".
               join(", ", @bits));
    my ($toSet, $toKill) = 
        ($actions{Update} + $actions{Create}, $actions{Delete});
    my $haltFile = $rbmFile . "-HALTED";
    my $failFile = $rbmFile . "-FAILED";
    if ($toKill) {
        # We are deleting rows. Make sure we're not slaughtering the DB
        my $dangerous;
        if ($toSet) {
            # Too many changes in Ensembl - going from 10% for large sets
            # to 50% overall
            $dangerous = ($toKill > 100 && $toKill / $toSet > 0.5) ||
                ($toKill / $toSet > 0.5);
        } else {
            $dangerous = $toKill > 10 ? 1 : 0;
            
        }
        if (!$overRide && $dangerous) {
            my @msg =
                ("Halting load of database.", 
                 "$pair->{q}{short} vs. $pair->{s}{short}",
                 "The total number of similarities being deleted ($toKill) is".
                 " large compared to the total number being set ($toSet)",
                 $oldFile, $rbmFile,
                 "Specify -unhalt to load these data");
            if (open(HF, ">$haltFile")) {
                print HF join("\n", @msg, `date`);
                close HF;
            } else {
                $args->err("Failed to write halt file", $haltFile, $!);
            }
            $args->msg("[HALT]",@msg, $haltFile);
            $lh->clear_data();
            return;
        }
    }
    unlink($haltFile) if (-e $haltFile);
    my $failed = 0;
    foreach my $params (@setParams) {
        $failed++ unless ($lh->set_edge( @{$params} ) );
    }
    if ($failed) {
        my @msg = ("$failed of ".scalar(@setParams).
                   " edges failed to be set while loading data", $rbmFile);
        if (open(FF, ">$failFile")) {
            print FF join("\n", @msg, `date`);
            close FF;
        } else {
            $args->err("Failed to write fail file", $failFile, $!);
        }
        $args->msg(@msg, $failFile);
        $lh->clear_data();
        return;
    }
    unlink($failFile) if (-e $failFile);
    $lh->write();
    return $setCount + $remCount;
}

sub get_old_edge_file {
    my ($pair, $allIDs) = @_;
    my $oldFile = sprintf("%s/%s.%s", $workDir, $pair->{tag}, 'old');

    return $oldFile if (!$isPartial &&
                        &use_existing_file($oldFile, $qdb, $sdb));

    $args->msg("  [-]","Identifying old edges to clear");
    # Find edges we need to clear
    delete $allIDs->{''};
    my @refs = ($sdb, $qdb); # The opposing database
    my $purgeOld = 1;
    %okAuths = map { $_ => 1 } @{$mainClearAuths};
    my @killArgs;
    while (my ($ind, $hash) = each %{$allIDs}) {
        my $ref = $refs[ $ind ];
        my ($rns, $rtype, $rtax) = map { $ref->{$_} } qw(ns type taxa);
        my @base = ( -keeptype  => [ 'similar','sameas','contains' ],
                     -keepclass => [ $rns, $rtype ],
                     -keeptaxa  => $rtax,
                     -keepauth  => $mainClearAuths,
                     -return    => 'object array', );
        foreach my $id (sort keys %{$hash}) {
            my @dumps;
            push @dumps, [ -name => $id,     @base ];
            push @dumps, [ -name => "$id.%", @base ] if ($purgeOld);
            push @killArgs, [ $id, $rtax, $rns, $rtype, \@dumps ];
        }
    }

    %seenEID = ();

    $fc ||= &new_fork_critter();
    $fc->reset();
    $fc->unstable_input( 0 );
    $fc->input_type( 'array' );
    $fc->last_item_method( sub {
        return $_[0] ? sprintf("%s -> [%s %s %s]", @{$_[0]}) : '?';
    } );
    $fc->input( \@killArgs );
    $fc->method( \&find_old_edges );
    $fc->init_method( \&mt_init );
    $fc->finish_method( 0 );
    $fc->output_file('Old', $oldFile);
    $fc->skip_record_method( 0 );
    $fc->limit(0);

    my $failed = $fc->execute( $forkNum );
    $fc->last_item_method( 0 );

    if ($failed) {
        unlink($oldFile);
        $args->death("$failed children failed to fork properly");
    }
    $args->msg("    [F]", "$oldFile");
    if ( -e $oldFile) {
        if (open(OLDF, "<$oldFile")) {
            my %uniq;
            my $orig = 0;
            while (<OLDF>) {
                s/[\n\r]+$//;
                my ($n1, $n2, $t, $kauth) = split(/\t/);
                $uniq{$n1}{$n2}{$t}{$kauth} = 1;
                $orig++;
            }
            close OLDF;
            if (open(OLDF, ">$oldFile")) {
                my $cons = 0;
                foreach my $n1 (sort keys %uniq) {
                    foreach my $n2 (sort keys %{$uniq{$n1}}) {
                        while (my ($t, $auths) = each %{$uniq{$n1}{$n2}}) {
                            foreach my $kauth (keys %{$auths}) {
                                print OLDF join("\t", $n1,$n2,$t,$kauth)."\n";
                                $cons++;
                            }
                        }
                    }
                }
                close OLDF;
                if (my $rem = $orig - $cons) {
                    # $args->msg("      [*]","Consolidated $rem lines [$orig - $cons]");
                }
            } else {
                $args->death("Failed to write old edge file for consolidation",
                             $oldFile, $!);
            }
        } else {
            $args->death("Failed to read old edge file for consolidation",
                         $oldFile, $!);
        }
    } else {
        system("touch $oldFile");
    }
    return $oldFile;
}

sub find_old_edges {
    my $info = shift;
    my ($id, $rtax, $rns, $rtype, $dumps) = @{$info};
    my @rows;
    foreach my $eparams (@{$dumps}) {
        my $edgeset = $mt->get_edge_dump( @{$eparams} );
        foreach my $edge (@{$edgeset}) {
            my $eid = $edge->id;
            next if ($seenEID{$eid}++);
            my ($oseq, $qseq)  = $edge->seqs;
            ($oseq, $qseq) = ($qseq, $oseq)
                unless ($qseq->name =~ /^$id/i);
            next unless ($qseq->name =~ /^$id/i);
            # Make sure we are removing only appropriate entries
            next unless ($oseq->is_class($rns) &&
                         $oseq->is_class($rtype) );
            my ($n1, $n2) = map { $_->namespace_name } $edge->seqs;
            my $t         = $edge->reads;
            foreach my $kauth ( $edge->each_authority_name ) {
                next unless ($okAuths{$kauth});
                push @rows, [$n1, $n2, $t, $kauth ];
            }
        }
    }
    return if ($#rows == -1);
    $fc->write_output('Old', join('', map { join("\t", @{$_})."\n"} @rows));
}

sub is_halted {
    my $pair = shift;
    my $haltFile = sprintf
        ("%s/%s.%s-HALTED", $workDir, $pair->{tag}, 'rbm');
    return "" unless (-s $haltFile);
}

sub find_rbm {
    my ($pair) = @_;
    my $rbmFile = sprintf("%s/%s.%s", $workDir, $pair->{tag}, 'rbm');

    return $rbmFile if (!$isPartial &&
                        &use_existing_file($rbmFile, $qdb, $sdb));

    my $alnFile = &get_pairwise_file( $pair );
    return 0 unless ($alnFile);

    $args->msg("  [R]","Finding reciprocal matches");

    open(FILE, "<$alnFile") || $args->death
        ("Failed to read alignment file", $alnFile, $!);
    my @keyFields = qw(id match similar);
    my $keyFrm = join('-', map { '%010.2f' } @keyFields);
    my (%pairs, %queries, %nullIDs);
    my $goodCount = 0;
    my $errCount = 0;
    while(<FILE>) {
        s/[\n\r]+$//;
        my ($q, $t, $s, $ptxt) = split(/\t/);
        map { s/^[^:]+:// if ($_) } ($q, $s);
        unless ($q && $s) {
            $nullIDs{$q} = 1 if ($q);
            $nullIDs{$s} = 3 if ($s);
            next;
        }
        if ($q eq $s) {
            &err("Self alignment in file", $q);
            next;
        }
        $errCount++ if ($t eq 'ERR');
        next unless ($t eq 'HIT');
        $goodCount++;
        my %params = map { split('=') } split(/ \| /, $ptxt || "");
        $params{key} = sprintf($keyFrm, map {$params{$_} || 0} @keyFields);
        $params{txt} = $ptxt;
        if ($pairs{$q}{$s}) {
            &err("Duplicate results in file", "$q vs $s");
            next;
        }
        $pairs{$q}{$s} = $pairs{$s}{$q} = \%params;
        $queries{$q}++;
    }
    close FILE;

    my $totCount = $goodCount + $errCount;
    my $errFrac  = $errCount / ($totCount || 1);
    $args->death("Too many errors ($errCount / $totCount) found in RBM file",
                 $alnFile) if ($errFrac > 0.001 && $errCount > 5);

    # Organize hits for each key
    foreach my $id (keys %pairs) {
        my @hits;
        while (my ($oid, $hash) = each %{$pairs{$id}}) {
            push @hits, [$oid, $hash];
        }
        my @sorted = sort { $b->[1]{key} cmp $a->[1]{key} ||
                            $a->[0] cmp $b->[0] } @hits;

        # Find all best hits
        my $bestHit = shift @sorted;
        my $bestKey = $bestHit->[1]{key};
        $pairs{$id} = [ [$bestHit] ];
        while (my $hit = shift @sorted) {
            if ($hit->[1]{key} eq $bestKey) {
                push @{$pairs{$id}[0]}, $hit;
            } else {
                unshift @sorted, $hit;
                last;
            }
        }
        # Put the remaining non-best hits in the second array position
        $pairs{$id}[1] = \@sorted;
    }

    # Now find reciprocity
    open(RBM, ">$rbmFile") || $args->death
        ("Failed to write RBM file", $rbmFile, $!);
    my @queries = sort keys %queries;
    my %captured;
    my $count = 0;
    foreach my $qid (@queries) {
        next if ($captured{$qid});
        my ($q, $s, $failed) = &get_reciprocal_matches( $qid, \%pairs );
        my $ptxt = $pairs{$qid}[0][0][1]{txt};
        my $type = 'RBM';
        if ($failed) {
            $type = 'FAIL';
            $ptxt .= " | reject=".join(',', @{$failed});
        } else {
            $ptxt .= " | cluster=".($#{$q} + $#{$s} + 2);
            map { $captured{$_}++ } (@{$q});
        }
        print RBM join("\t", join(',',@{$q}),$type,join(',',@{$s}),$ptxt)."\n";
        map { delete $nullIDs{$_} } (@{$s}, @{$q});
        $count++;
    }
    foreach my $id (sort keys %nullIDs) {
        my @row = ("","NULL","");
        $row[ $nullIDs{$id} - 1 ] = $id;
        print RBM join("\t", @row)."\n";
    }
    close RBM;
    $args->msg("    [F]", "$rbmFile");
    return $rbmFile;
}

sub get_reciprocal_matches {
    my ($req, $data) = @_;
    my @seen   = ( {}, {} ); # 0=Q 1=S
    my @stack = ([$req, 0]); # 0=ID 1=index (Q/S)
    while (my $dat = shift @stack) {
        # Recursively expand to get the network of best matched IDs
        my ($req, $ind) = @{$dat};
        next if ($seen[$ind]{$req}++); # Already processesed this ID
        my $oind = $ind ? 0 : 1;
        push @stack, map { [ $_, $oind ] } &get_matches($req, $data);
    }
    # Find all unique IDs in both Query and Subject
    my @ids  = map { [ sort keys %{$_} ] } @seen;

    # Now find all tags generated for each individual ID
    my %observed;
    map { 
        my $tag = &arr2tag( &get_matches($_,$data));
        push @{$observed{$tag}}, $_;
    } map { @{$_} } @ids;

    # If the sets are reciprocal, there should only be two tags,
    # represented by the sets themselves. Remove those tags from those
    # observed, and see what is left
    map { delete $observed{ &arr2tag( @{$_} ) }} @ids;
    my @evil = map { @{$_} } values %observed;
    return (@ids, $#evil == -1 ? undef : \@evil);
}

sub get_matches {
    # For a given ID, just gets the top hits from the other database
    my ($id, $data) = @_;
    my $od = $data->{$id};
    return $od ? map { $_->[0] } @{$od->[0]} : ();
}

sub arr2tag {
    return join("\t", sort @_) || "";
}

sub get_pairwise_file {
    my ($pair)  = @_;
    my $alnFile = sprintf("%s/%s.%s", $workDir, $pair->{tag}, 'align');

    return $alnFile if (!$isPartial &&
                        &use_existing_file($alnFile, $qdb, $sdb));

    if (my $err = &verify_db_integrity($qdb, $sdb)) {
        &err("Can not perform alignment", $err);
        return ();
    }

    # We need to make a new file, for that we need the blast files
    my @hitfiles = &get_blast_files( $pair );
    return undef if ($#hitfiles == -1);

    $args->msg("  [P]","Generating pairwise alignments");
    # Ok, blast searches are done. Now find pairwise alignments to do
    my (%hits, %nulls);
    for my $h (0..$#hitfiles) {
        my $hf = $hitfiles[$h];
        open(HITS, "<$hf") || $args->death
            ("Failed to read hitfile", $hf, $!);
        while (<HITS>) {
            s/[\n\r]+$//;
            my ($q, $t, $s) = split(/\t/);
            next unless ($t);
            ($q, $s) = ($s, $q) if ($h);
            if ($t eq 'HIT') {
                $hits{$q}{$s}++;
            } else {
                $nulls{$q} = 0 if ($q);
                $nulls{$s} = 2 if ($s);
            }
        }
    }

    my (@seqPairs, %uniqS);
    my @queries = sort keys %hits;
    foreach my $q (@queries) {
        delete $nulls{$q};
        foreach my $s (sort keys %{$hits{$q}}) {
            $uniqS{$s}++;
            push @seqPairs, [ $q, $s ];
        }
    }
    my @subs = keys %uniqS;
    map { delete $nulls{$_} } @subs;
    my @nRows;
    while (my ($id, $indx) = each %nulls) {
        my @row = ("","NULL", "");
        $row[$indx] = $id;
        push @nRows, join("\t", @row)."\n";
    }

    $args->msg("    [O]", sprintf
               ("%d candidate pairs from %d queries and %d subjects".
                ", %d are orphans", $#seqPairs + 1, $#queries + 1, 
                $#subs + 1, $#nRows + 1));

    $fc ||= &new_fork_critter();
    $fc->reset();
    $fc->unstable_input( 0 );
    $fc->input_type( 'array' );
    $fc->last_item_method( sub {
        return $_[0] ? join(' vs ', @{$_[0]}) : '?';
    } );
    $fc->input( \@seqPairs );
    $fc->method( \&align_sequence );
    $fc->init_method( \&fcwmt_init );
    $fc->finish_method( 0 );
    $fc->output_file('Align', $alnFile);
    $fc->skip_record_method( 0 );
    $fc->limit(0);

    my $failed = $fc->execute( $forkNum );
    $fc->last_item_method( 0 );
    if ($failed) {
        unlink($alnFile);
        $args->death("$failed children failed to fork");
    }
    unless ($#nRows == -1) {
        my $nTxt = join('', sort @nRows);
        open (FILE, ">>$alnFile") || $args->death
            ("Failed to append to alignment file", $alnFile, $!);
        print FILE $nTxt;
        close FILE;
    }
    $args->msg("    [F]", "$alnFile");
    
    return $alnFile;
}

sub align_sequence {
    my ($pair) = shift;
    my @names;
    my ($qname, $sname) = @names = @{$pair};
    foreach my $name (@names) {
        $name =~ s/^[^:]+://;
        $name =~ s/\.\d+$//;
        my $seq = $mt->get_seq( -id => $name, -nocreate => 1 );
        if ($seq && $seq->is_class('deprecated')) {
            $fc->write_output('Align', "$qname\tDEPRECATED\t$sname\n");
            return;
        }
    }
    my $qseq  = $dbfQ->get_Seq_by_id( $qname );
    my $sseq  = $dbfS->get_Seq_by_id( $sname );
    if (my $mod = &files_have_changed( $qdb, $sdb )) {
        $args->death("Databases changed on disk during search!", $mod, 
                     $qdb, $sdb);
    }
    unless ($qseq && $sseq) {
        my @errs;
        push @errs, "query - '$qname' ($qdb->{path}) vs $sname" unless ($qseq);
        push @errs, "subject - '$sname' ($sdb->{path}) vs $qname" unless ($sseq);
        $args->death("Failed to recover sequences for alignment",@errs);
    }
    if ($killa && $sdb->{type} eq 'rna') {
        # Trim poly-A
        foreach my $bs ($qseq, $sseq) {
            my $sd = $bs->seq();
            $sd =~ s/a{$killa,}$//i;
            $bs->seq($sd);
        }
    }
    my ($qlen, $slen) = map { $_->length } ($qseq, $sseq);
    
    my @line = ($qname, 'HIT', $sname);
    # Stats cols:
    # overall_perc_span | match

    my $size = sqrt($qlen * $slen);
    my $alg  = ($size < $fcw->{CAT_THRESH}) ? 'needle' : 'stretcher';
    my %params  = ('time' => time,
                   auth   => $alg,
                   lenQ   => $qlen,
                   lenS   => $slen,);

    $fcw->algorithm($alg);
    my ($stats) = $fcw->align($qseq, $sseq);
    $params{time} = time - $params{time};

    my $astat = $stats ? $stats->{ALL} : undef;
    if ($astat) {
        $params{auth}    = $astat->{auth};
        $params{id}      = $astat->{average_perc_match};
        $params{match}   = $astat->{match};
        $params{score}   = $astat->{score};
        $params{similar} = $astat->{average_perc_similar};
        if (!$params{id}) {
            $line[1] = 'ERR';
            $params{FAIL} = "Alignment Percent ID not found";
        } elsif (!$params{similar}) {
            $line[1] = 'ERR';
            $params{FAIL} = "Alignment Percent Similar not found";
        }
    } else {
        $line[1] = 'ERR';
        $params{FAIL} = "Failed to get alignment statistics";
    }
    my @p;
    foreach my $param (sort keys %params) {
        my $val = $params{$param};
        push @p, "$param=$val" if (defined $val);
    }
    push @line, join(' | ', @p);

    $fc->write_output('Align', join("\t", @line)."\n");
}

sub get_blast_files {
    my ( $pair ) = @_;
    my @hitfiles;
    my @searches = ([$qdb, $sdb]);
    if ($isPartial) {
        # If we are doing a full search, then using a single database
        # should provide all relevant pairs.  Otherwise, we will need to
        # perform a focused back-search using the subject database.
        push @searches, [$sdb, $qdb];
    }

    for my $sn (0..$#searches) {
        my ($q, $s) = @{$searches[$sn]};
        my $hf = sprintf("%s/%s.%s%s", $workDir, $pair->{tag}, 'blast',
                         $sn ? $sn + 1 : '');
        push @hitfiles, $hf;
        next if (!$isPartial &&
                 &use_existing_file($hf, $qdb, $sdb));
        if (my $err = &verify_db_integrity($qdb, $sdb)) {
            &err("Can not perform blast", $err);
            return ();
        }

        if (my $mod = &files_have_changed( $qdb, $sdb )) {
            $args->death("Databases changed on disk during search!", $mod, 
                         $qdb, $sdb);
        }
        $blastDB = $s->{path};
        $args->msg("  [B]",sprintf("%s vs. %s", $q->{short}, $s->{short}));
        # It is possible that the file has changed since initial pre-calc
        { my $fdb = Bio::DB::Fasta->new( $blastDB ); undef $fdb }

        $fc ||= &new_fork_critter();

        $fc->reset();
        $fc->unstable_input( 1 );
        $fc->input_type( 'seq' );
        $fc->input( $q->{path} );
        $fc->method( \&blast_sequence );
        $fc->skip_record_method( \&skip_seq );
        $fc->init_method( \&fcw_init );
        $fc->finish_method( 0 );
        $fc->output_file('Hits', $hf);

        if ($isPartial) {
            if ($sn) {
                # This is the subject. We need to constrain our search
                # to sequences hit by the query
                my $hf = $hitfiles[0];
                open(HITS, "<$hf") || $args->death
                    ("Failed to read hitfile", $hf, $!);
                my %subs;
                while (<HITS>) {
                    chomp;
                    my ($q, $ty, $s) = split(/\t/);
                    $subs{$s}++ if ($ty eq 'HIT');
                }
                close HITS;
                if ($dbvb) {
                    my @uniq = keys %subs;
                    $args->msg(sprintf
                               ("  [PARTIAL]","Considering only %d subjects on reverse search",
                                $#uniq + 1));
                }
                $fc->limit(0);
                $fc->skip_record_method( sub {
                    my ($seq) = @_;
                    return $subs{ $seq->display_id } ? 0 : 1;
                } );
            } else {
                $fc->limit($limit || 0);
            }
        }
        my $failed = $fc->execute( $forkNum );
        if ($failed) {
            unlink($hitfiles[-1]);
            $args->death("$failed children failed on fork");
        }
        $args->msg("    [F]", "$hitfiles[-1]");
        $fc->skip_record_method( 0 );
    }

    # If we have only searched one database, scan the output, and
    # identify entries in subject that are absent
    &append_null_subjects( $hitfiles[0], $sdb->{path} ) if ($#searches == 0);
    return @hitfiles;
}

sub blast_sequence {
    my ($seq) = @_;
    my $qname = $seq->display_id;
    if ($blastSize && $seq->length < 2 * $blastSize) {
        $fc->write_output('Hits', "$qname\tSHORT\n");
        return;
    }
    my ($allhits, $toss) = $fcw->top_blast
        ( -program  => $prog,
          -database => $blastDB,
          -filters  => $blastFilters,
          -skimmers => $blastSkimmers,
          -numalign => $numalign,
          -e        => 0.1,
          -query    => $seq, );
    my $hits  = $allhits->{$qname};
    my @lines;
    if ($hits) {
        foreach my $hit (@{$hits}) {
            my $stat = $hit->{astat};
            push @lines, [ $qname, 'HIT', $hit->{subject}, 
                           $stat->{e}, $stat->{match_perc} ]; 
        }
    } elsif ($toss) {
        my ($hit) = sort {
            $a->{astat}{e} <=> $b->{astat}{e} ||
                $b->{astat}{match_perc} <=> $a->{astat}{match_perc}
        } @{$toss};
        if ($hit) {
            my $stat = $hit->{astat};
            push @lines, [ $qname, 'MISS', $hit->{subject}, 
                           $stat->{e}, $stat->{match_perc} ]; 
        } else{
            push @lines, [ $qname, 'NULL' ];
        }
    } else {
        return;
    }
    $fc->write_output('Hits', join('', map { join("\t", @{$_})."\n" } @lines));
}

sub append_null_subjects {
    my ($hf, $fasta) = @_;
    return 0 unless ($hf && $fasta);

    open(FILE, "<$hf") || $args->death
        ("Failed to read hitfile for null object identification", $hf, $!);
    my %noted;
    while (<FILE>) {
        s/[\n\r]+$//;
        my @bits = split(/\t/);
        $noted{$bits[2]}++ if ($bits[2]);
    }
    close FILE;
    
    my @null;
    open(FILE, "<$fasta") || $args->death
        ("Failed to parse fasta file", $fasta, $!);
    while (<FILE>) {
        if (/^\>(\S+)/) { push @null, $1 unless ($noted{$1}); }
    }
    close FILE;
    # Now append null entries to hitfile;
    my $nullTxt = join("", map { "\tNULL\t$_\n" } @null);
    open(FILE, ">>$hf") || $args->death
        ("Failed to append hitfile", $hf, $!);
    print FILE $nullTxt;
    close FILE;

    # The single hitfile should now contain at least one reference
    # to each sequence from both databases.
    return $#null + 1;
}

sub use_existing_file {
    my $file = shift;
    my $rv   = 0;
    if (!($file && -e $file)) {
        $rv = 0;
    } elsif ($reuse) {
        $rv = 1;
    } elsif ($clobber) {
        $rv = 0;
    } else {
        $rv = &first_file_newer( $file, @_);
    }
    return $rv;
}

sub first_file_newer {
    my $file = shift;
    return 0 unless ($file && -e $file);
    return 0 if ($reloadRBM && $file =~ /\.rbm$/);
    my $fAge = -M $file;
    my @oAges;
    foreach my $other (@_) {
        my $path = ref($other) ? $other->{original} : $other;
        push @oAges, -M $path;
    }
    my ($cmpAge) = sort { $a <=> $b } @oAges;
    return ($fAge < $cmpAge) ? 1 : 0;
}


sub simplify_file {
    # Remove duplicate rows from messages
    my ($fname) = @_;
    return 0 unless (-e $fname && -s $fname);
    $args->msg("Simplifying file", $fname);
    my $sfile  = "$fname.sort";
    system("sort $fname > $sfile");
    open(SFILE, "<$sfile") || $args->death
        ("Failed to read file for simplification", $sfile, $!);
    open(MFILE, ">$fname") || $args->death
        ("Failed to write file for simplification", $fname, $!);
    my $prior = "";
    my $lines = 0;
    while (<SFILE>) {
        next if ($_ eq $prior);
        $prior = $_;
        print MFILE $_;
        $lines++;
    }
    close MFILE;
    close SFILE;
    $args->msg("     [F]", "$fname");
    unlink($sfile);
    return $lines;
}

=head2 clear_by_database

 Title   : 
 Usage   : &clear_by_database( $queryID, $querySQLid );
 Function: Kills prior edges associated with the sequence

This method will kill any edges associated with the query ID that also
link to an ID known to be in the other database. It should be used
when one or both of the database being used can not be neatly defined
as a combination of Species + Namespace + Sequence Type; if all three
of those parameters are known, then clear_by_namespaces() should be
used instead.

The queryID is the actual accession/ID of the sequence in
question. querySQLid, by contrast, is what will be used to search for
candidate edges. The SQL ID could be the same as the query ID, or it
might be a wildcarded variant to allow capture of related IDs (for
example, Query = NM_001234.2, SQL = NM_001234.%).

=cut

sub clear_by_database {
    my ($qid, $sqlVal, $filter) = @_;

    # No mechanism to deal with subject clearing when clearing by DB...
    return if ($filter);

    my $existing = $lh->tracker->get_edge_dump
        ( -name      => $sqlVal,
          -keeptype  => ['similar','sameas','contains'],
          -keepauth  => $mainClearAuths,
          -filter    => $filter,
          # -dumpsql   => 5,
          -return    => 'object array', );
    foreach my $edge (@{$existing}) {
        my ($oseq, $qseq)  = $edge->seqs;
        ($oseq, $qseq) = ($qseq, $oseq) unless ($qseq->name =~ /^$qid/i);
        next unless ($qseq->name =~ /^$qid/i);
        my $sid       = $oseq->name;
        $sid          =~ s/\.\d+$//;

        $args->death("I need to re-write the code for clearing entries by database specifier!");

        my ($n1, $n2) = map { $_->namespace_name } $edge->seqs;
        my $t         = $edge->reads;
        my %found     = map { $_ => 1 } $edge->each_authority_name();
        foreach my $kauth (@{$mainClearAuths}) {
            next unless ($found{$kauth});
            $lh->kill_edge( -name1 => $n1,
                            -name2 => $n2,
                            -type  => $t,
                            -auth  => $kauth );
        }
    }
}

sub dump_msg {
    my @mbits;
    my ($longM) = sort { $b <=> $a } map { ref($_) ? length($_->[0]) :0} @msg;
    my $mFrm    = "%${longM}s: %s";
    foreach my $dat (@msg, @_) {
        push @mbits, ref($dat) ? sprintf($mFrm, @{$dat}) : $dat;
    }
    $args->msg(@mbits);
    @msg = ();
}

sub err {
    return if ($#_ == -1);
    $args->msg("[!]", @_);
}

sub skip_seq {
    my ($seq) = @_;
    if ($doOnly) {
        my $id = uc($seq->display_id);
        $id    =~ s/^[^\:]+\://;
        $id    =~ s/\.\d+$//;
        return 1 unless ($doOnly->{$id});
    }
    return 0;
}

sub new_fork_critter() {
    my $fc = BMS::ForkCritter->new( -progress => $forkProg );
    return $fc;
}

sub new_load_helper() {
    my $lh = BMS::MapTracker::LoadHelper->new
        ( -username => 'clustalw',
          -testfile => $testfile,
          -testmode => $tm, );

    if ($baseDir) {
        $lh->directory( $baseDir );
        $lh->load_token('RBM');
    }
    return $lh;
}

sub fcw_init {
    $fcw   = BMS::FriendlyClustalw->new( -alg => $alg );
    $dbfS  = Bio::DB::Fasta->new( $sdb->{path} );
    $dbfQ  = Bio::DB::Fasta->new( $qdb->{path} );
    my $type = $qdb->{type} eq $sdb->{type} ? $qdb->{type} : '';
    my $ft   = $fcw->moltype($type);
    # For nucleotides, it takes about a minute for each 4000 bp
    $fcw->{CAT_THRESH} = ($ft eq 'PROTEIN') ? 8000 : 15000;
    $fcw->{FORK_CHILD} = $fc->child();
}

sub mt_init {
    $mt = BMS::MapTracker->new( -username => 'tilfordc' );
}

sub fcwmt_init {
    &fcw_init();
    &mt_init();
}

sub lh_init {
    $lh = &new_load_helper()
}

sub pair_for_file {
    my $file = shift;
    $file =~ s/.+\///; # Remove leading dir path
    $file =~ s/\.[^\.]+$//; # Remove suffix
    $file =~ s/^RBM\.//;    # Remove prefix
    my @bits   = split(/\./, $file);
    my @common = split(/\-/, shift @bits); # Get parameters common to both
    my @uniq   = split(/-VS-/, join('.', @bits)); # Get unique parameters
    my (@pair, @errs, $pObj);
    foreach my $u (@uniq) {
        my @params = (@common, $u);
        my $found = $slf->get_libraries( -jumble => \@params,
                                         -best => 1 );
        my @paths = keys %{$found};
        if ($#paths == -1) {
            push @errs, "No files found for ".join(' + ', @params);
        } elsif ($#paths == 0) {
            my $path = $paths[0];
            my $dat  = $found->{$path};
            my $sht  = $path; $sht =~ s/.+\///;
            my ($tx, $ns, $type) = @{$dat};
            push @pair, {
                path     => $path,
                original => $path,
                short    => $sht,
                type     => $type,
                taxa     => $tx,
                ns       => $ns,
            };
        } else {
            push @errs, "Multiple files found for ".join(' + ', @params).
                " : ". join(', ', @paths);
        }
    }
    if ($#pair == 1) {
        if ($pair[0]{type} eq $pair[1]{type}) {
            $pObj = { q => $pair[0],
                      s => $pair[1],
                      t => $pair[0]{type},
                  };
        } else {
            push @errs, "Molecule type mismatch for ".join('.', @common)."-".
                join("-vs-", @uniq)." : ".join(', ', map { $_->{path} } @pair);
        }
    }
    return wantarray ? ($pObj, \@errs) : $pObj;
}
