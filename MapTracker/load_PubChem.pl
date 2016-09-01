#!/stf/biobin/perl -w

# $Id$ 

# Failed to generate canonical key        CID:8706390


BEGIN {
    # Needed to make my libraries available to Perl64:
    # use lib '/stf/biocgi/tilfordc/released';
    # Allows usage of beta modules to be tested:
    my $prog = $0; my $dir = `pwd`;
    if ($prog =~ /working/ || $dir =~ /working/) {
	warn "\n\n *** This is Beta Software ***\n\n";
	require lib;
	import lib '/stf/biocgi/tilfordc/perllib';
    }
    $| = 1;
    print '';
}

use strict;
use BMS::CommonCAT;
use BMS::MapTracker::LoadHelper;
use BMS::FriendlySAX;
use BMS::MapTracker::AccessDenorm;
use BMS::MapTracker::SciTegicSmiles;
use BMS::ForkCritter;
use BMS::BmsArgumentParser;

my $args = BMS::BmsArgumentParser->new
    ( -cache    => 20000,
      -nocgi    => $ENV{'HTTP_HOST'} ? 0 : 1,
      -dir      => '',
      -nocgi    => 1,
      -limit    => 0,
      -testmode => 'hush',
      -wgetcmd  => "/work5/tilfordc/WGET/pubchem_cmd.sh",
      -basedir  => "/work5/tilfordc/maptracker/PubChem",
      -trimdir  => "/work5/tilfordc/WGET/ParedPubChem",
      -age      => 7,
      -forkfile => 1,
      -progress => 300,
      -errormail  => 'charles.tilford@bms.com',
      -verbose  => 1, );

my $nocgi     = $args->val(qw(nocgi));

$args->debug->skip_key([qw(PARENT)]);

my %ignoreProp = map { $_ => 1 }
('Compound', 'Count', 'Molecular Weight', 'Fingerprint', 'Compound Complexity', 'Log P', 'Mass', 'Molecular Weight', 'Topological', 'Weight', 'IUPAC Name : Allowed', 'SMILES : Canonical', 'IUPAC Name : CAS-like Style', 'IUPAC Name : Systematic', 'IUPAC Name : Traditional', 'Compound', '', '');

my $paredToken  = "-Pared";
my $escapeCodes = {
    '#8242' => "'",
    '#8710' => "", # Product
    '#8722' => '-',
    '#8804' => '<=',
    '#8805' => '>=',
    '#916'  => "Delta",
    '#9702' => '', # Bullet
    'amp'   => '&',
    'apos'  => "'",
    'gt'    => '>',
    'lt'    => '<',
    'quot'  => '"',
};

my $authMap = {
    'Emory University Molecular Libraries Screening Center'     => 'MLSCN',
    'The Scripps Research Institute Molecular Screening Center' => 'Scripps',
    'NINDS Approved Drug Screening Program'   => 'NIH',
    'Prous Science Drugs of the Future'       => 'Prous',
    'San Diego Center for Chemical Genomics'  => 'SDCCG',
    'Shanghai Institute of Organic Chemistry' => 'PDBbind',
    'CambridgeSoft Corporation'      => 'CambridgeSoft',
    'ChemExper Chemical Directory'   => 'ChemExper',
    'NIST Chemistry WebBook'         => 'NIST',
    'Nature Chemical Biology'        => 'Nature Publishing',
    'Structural Genomics Consortium' => 'SGC',
    'ASINEX'           => 'ASINEX',
    'Ambinter'         => 'Ambinter',
    'Aronis'           => 'ArONIS',
    'BindingDB'        => 'BindingDB',
    'ChEBI'            => 'ChEBI',
    'ChemBank'         => 'ChemBank',
    'ChemBlock'        => 'ChemBlock',
    'ChemBridge'       => 'ChemBridge',
    'ChemDB'           => 'ChemDB',
    'ChemIDplus'       => 'NIH',
    'ChemSpider'       => 'ChemSpider',
    'DTP/NCI'          => 'NIH',
    'DiscoveryGate'    => 'MDL',
    'DrugBank'         => 'DrugBank',
    'EPA DSSTox'       => 'EPA',
    'Exchemistry'      => 'Exclusive Chemistry',
    'KEGG'             => 'KEGG',
    'KUMGM'            => 'KUMGM',
    'LipidMAPS'        => 'LipidMAPS',
    'MLSMR'            => 'NIH',
    'MMDB'             => 'NCBI',
    'MOLI'             => 'NIH',
    'MTDP'             => 'NIH',
    'NCGC'             => 'NIH',
    'NIAID'            => 'NIH',
    'NIST'             => 'NIST',
    'NMMLSC'           => 'MLSCN',
    'NMRShiftDB'       => 'NMRShiftDB',
    'PDSP'             => 'NIH',
    'R&D Chemicals'    => 'R&D Chemicals',
    'SMID'             => 'SMID',
    'Sigma-Aldrich'    => 'Sigma-Aldrich',
    'Specs'            => 'Specs',
    'Thomson Pharma'   => 'Thomson Scientific',
    'UPCMLD'           => 'UPCMLD',
    'ZINC'             => 'ZINC',
    'ncbi.nlm.nih.gov' => 'NCBI',
    'nist.gov'         => 'NIST',
    'openeye.com'      => 'OpenEye',
    'xPharm'           => 'xPharm',

    'BioCyc' => 'BioCyc',
    '' => '',
    '' => '',
    '' => '',
    '' => '',

};

my $actions = {
    'InChI : UNK' => ['alias', '#InChI#', 'InChI', 1 ],
    'IUPAC Name : Preferred' => ['alias', '#FreeText#', 'IUPAC'],
    'SMILES : Isomeric' => [],
    'Molecular Formula : UNK' => [],
};

my $idStack = {
    'PC-Substance' => ['PC-Substance_sid', 'PC-ID','PC-ID_id'],
    'PC-Compound'  => ["PC-Compound_id", "PC-CompoundType", 
                       "PC-CompoundType_id", "PC-CompoundType_id_cid"],
};
my $idPrfx = {
    'PC-Substance' => "SID:",
    'PC-Compound'  => "CID:",
};

my $limit      = $args->{LIMIT};
my $fileMod    = $args->{FILEMOD} ? "_$$" : '';
my $testFile   = $args->{TESTFILE};
my $tm         = $args->{TESTMODE};
my $cache      = $args->{CACHE};
my $vb         = $args->{VERBOSE};
my $mode       = $args->{MODE};
my $prog       = $args->{PROGRESS};
my $smiF       = $args->{FASTSMILES};
my $dmpF       = $args->val(qw(dump dumpfile));
my $basedir    = $args->val(qw(basedir loaddir));
my $doSum      = $args->{SUMMARIZE};
my $doTrim     = $args->val(qw(parexml trimxml)) || 0;
my $reProc     = $args->val(qw(reproc reprocess)); # Whole files
my $reDo       = $args->val(qw(redo)); # Individual entries
my $isTrial    = $args->val(qw(trial istrial));
my $forkByFile = $args->{FORKFILE};

if ($args->val(qw(wget update wgetonly updateonly))) {
    my $cmd = $args->{WGETCMD};
    warn "WGET: Mirroring with $cmd ... " . `date`;
    system($cmd);
    warn "  Done. " .`date`;
    exit if ($args->val(qw(wgetonly updateonly)));
} else {
    warn "\n *** USING EXISTING DATA - NO UPDATE! ***\n     Use -wget if you wish to update mirrored data.\n\n";
}


unless ($testFile || defined $testFile) {
    $testFile = "PubChem_Test_Loader_Output$fileMod.txt";
}


$smiF = "PubChem_SMILES$fileMod.smi" if ($smiF && $smiF eq '1');

my ($prfx, %issues, $shortFile, $lh, $sts, $simpMeth, $fileGlobals, $trimTags );
my ($lhParent, $mtParent, $dbhParent);

my $exitCode  = 42;

my $fc = BMS::ForkCritter->new
    ( -limit       => $limit,
      -initmeth    => \&initialize,
      -finishmeth  => \&finalize,
      -exitcode    => $exitCode,
      -progress    => $prog,
      -verbose     => $vb );

my %stndFiles = ( Issues   => "PubChem_Issues$fileMod.txt",
                  Messages => "PubChem_Messages$fileMod.txt",
                  TestFile => $tm ? $testFile : '',
                  SMILES   => $smiF, );
if ($smiF) {
    %stndFiles    = ('Smiles Messages' => "Smiles_Conversion_Messages$fileMod.txt" );
} elsif ($doSum || $doTrim) {
    %stndFiles = ( Issues   => "PubChem_Issues$fileMod.txt",
                   Messages => "PubChem_Messages$fileMod.txt");
    if ($doSum) {
        $stndFiles{Summary}  = "PubChem_Summary$fileMod.txt";
    } else {
        $stndFiles{Trimmed}  = "PubChem_Trimmed$fileMod.txt";
    }
}

if (my $filt = $args->{FILTER} || $args->{CANON}) {
    $fc->reset();
    $fc->input_type( 'tsv' );
    $fc->input( $filt );
    $fc->progress(60);
    my $output;
    if ($args->{FILTER}) {
        $output = $args->{OUTPUT} || "Filtered_SMILES$fileMod.smi";
        $fc->method( \&fast_filter_smiles );
        unlink($output);
        $fc->output_file( 'OUT', "$output");
    } else {
        $fc->method( \&fast_canon_smiles );
    }
    my $failed = $fc->execute( $args->{FORK} || 1 );
    if ($failed) {
        warn "$failed processes did not execute properly\n";
    }
    warn "\nFiltering finished.\n";
    warn "Output at:\n  $output\n\n" if ($output);
    exit;
}

unless ($isTrial) {
    map { unlink($_) if ($_) } values %stndFiles;
}

my $rootDir = "/work5/tilfordc/pubchem";
my $block   = 25000;
my @tasks;

my $bioAssayCount = 0;
if (my $file = $args->{FILE}) {
    unless ($file =~ /\//) {
        if ($file =~ /^(\d+)(\Q$paredToken\E)?(\.xml(.gz)?)?$/) {
            my $num = $1;
            foreach my $tok ($paredToken, '') {
                my $try = sprintf
                    ("%s/Bioassay/XML/%s%s.xml.gz", $rootDir, $num, $tok);
                next unless (-s $try);
                $file = $try;
                last;
            }
        } elsif ($file =~ /(Compound|Substance)/) {
            my $type = $1;
            my ($start, $end);
            if ($file =~ /(\d{9})[-_](\d{9})/) {
                ($start, $end) = ($1, $2);
            } elsif ($file =~ /(\d{9})/) {
                $start = $1;
                $end   = $start + $block - 1;
            }
            if ($start) {
                foreach my $tok ($paredToken, '') {
                    my $try = sprintf
                        ("%s/%s/CURRENT-Full/XML/%s_%09d_%09d%s.xml.gz",
                         $rootDir, $type, $type, $start, $end, $tok);
                    next unless (-s $try);
                    $file = $try;
                    last;
                }
            }
        } elsif ($file =~ /Bioassay/) {
        }
    }
    unless (-s $file) {
        $args->msg("[ERR]", "Failed to find requested file", $file);
        exit;
    }
    @tasks = ($file);
} elsif (my $dir = $args->{DIR}) {
    $dir = "$rootDir/$dir" unless ($dir =~ /^\//);
    push @tasks, &parse_dir($dir);
} else {
    foreach my $sdir qw(Compound Substance Bioassay) {
        push @tasks, &parse_dir("$rootDir/$sdir");
    }
}

if ($isTrial) {
    warn "Trial mode. Tasks identified:\n".
        join('', map { "  $_\n" } @tasks)."\n";
} elsif ($doTrim =~ /first/i) {
    &trim_xml_files( \@tasks );
} elsif ($smiF) {
    my $fastFile  = "Smiles_Conversion_Messages$fileMod.txt";
    $fc->reset();
    $fc->input_type( 'array' );
    $fc->input( \@tasks );
    $fc->method( \&fast_extract_smiles );
    $fc->output_file( 'SMSG', "$fastFile");
    if ($dmpF) {
        $stndFiles{"SMILES Dump"} = $dmpF;
        $fc->output_file( 'DUMP', "$dmpF");
    }
    my $failed = $fc->execute( $args->{FORK} || 1 );
    if ($failed) {
        $issues{"FAILED: SMILES dump"} = $failed;
        warn "$failed processes did not execute properly\n";
    }
} elsif ($forkByFile) {
    warn "\nForking by individual file\n" if ($vb);
    $fc->reset();
    $fc->limit(0);
    $fc->input_type( 'array' );
    $fc->input( \@tasks );
    $fc->method( \&parse_xml );
    &set_fork_output();
    my $failed = $fc->execute( $args->{FORK} || 1 );
    if ($failed) {
        $issues{"FAILED: SMILES dump"} = $failed;
        warn "$failed processes did not execute properly\n";
    }
    
} else {
    warn "\nForking by XML record\n" if ($vb);
    map { &parse_xml($_) } @tasks;    
}

# warn $lh->showbench if ($vb);

if ($stndFiles{Issues} && -s $stndFiles{Issues}) {
    my $f = $stndFiles{Issues};
    if (open(FILE, "<$f")) {
        while (<FILE>) {
            chomp;
            my ($k, $v) = split(/\t/);
            $issues{$k} += $v;
        }
        close FILE;
    } else {
        warn "Failed to read $f:\n $!\n  ";
    }
}
{
    my $msg = "";
    my $sum = "";
    foreach my $key (sort keys %issues) {
        if (my $num = $issues{$key}) {
            $msg .= sprintf(" %30s : %d\n", $key, $num);
            $sum .= "$key\t$num\n";
        }
    }
    my $f = $stndFiles{Issues};
    if ($sum && $f) {
        if (open(FILE, ">$f")) {
            print FILE $sum;
            close FILE;
        } else {
            warn "Failed to write $f:\n $!\n  " if ($f);            
        }
    }
    print "Process Messages:\n$msg\n" if ($msg);
}

my $ftxt = "";
undef $lh;
while (my ($fk, $f) = each %stndFiles) {
    # warn `ls -lh $f`;
    next unless ($isTrial || ($f && (-s $f)));
    $ftxt .= sprintf(" %20s: %s\n", $fk, $f);
}

warn "\nGenerated files:\n$ftxt" if ($ftxt);

if ($basedir && !$tm && !$isTrial) {
    warn "\nLoading database from $basedir\n";
    &initialize('par');
    $lh->process_ready()
}

warn "\nAll processing complete\n\n";


sub parse_dir {
    my @stack = @_;
    my %fileHash;
    my @bloated;
    while (my $dir = shift @stack) {
        if (opendir(DIR, $dir)) {
            $issues{"Directories parsed"}++;
            foreach my $file (readdir DIR) {
                my $path = "$dir/$file";
                if ($file =~ /\.xml(\.gz)?/) {
                    next if ($path =~ /\.(data|descr)\./);
                    my $key = $file;
                    $key =~ s/\..+//;
                    $key =~ s/\Q$paredToken\E//;
                    # Keep this path only if the file has not been seen before
                    # or if we have not captured the pared file yet.
                    my $prior = $fileHash{$key} || "";
                    if ($prior =~ /\Q$paredToken\E/ &&
                        $file  !~ /\Q$paredToken\E/) {
                        # We already have the pared version
                        push @bloated, [$path, $prior];
                    } else {
                        $fileHash{$key} = $path;
                        if ($prior && $prior !~ /\Q$paredToken\E/ &&
                            $file =~ /\Q$paredToken\E/) {
                            push @bloated, [$prior, $path];
                        }
                    }
                } elsif (-d $path) {
                    push @stack, $path unless ($file =~ /^\.+$/);
                }
            }
        } else {
            warn "Failed to read directory $dir:\n  $!\n  ";
            $issues{"Directory read failure"}++;
        }
    }
    my @files = sort values %fileHash;
    my $fnum = $#files + 1;
    $issues{"Files found"} = $fnum;
    my @msg = ("Found $fnum PubChem XML files within:", @_);
    my $bloat = 0;
    foreach my $bl (@bloated) {
        my ($of, $nf) = @{$bl};
        my $os = -s $of;
        my $ns = -s $nf;
        if ($ns > $os) {
            push @msg, "$nf is not smaller!";
        } else {
            $bloat += $os - $ns;
        }
    }
    if ($bloat) {
        push @msg, sprintf("A total of %.3f Gb of bloated older files exist",
                           $bloat / 1000000000);
    }
    $args->msg(@msg) if ($vb);
    return unless ($fnum);

    my $start = $args->{START} || 1;
    if ($start =~ /_(\d+)_(\d+)\./) {
        my $num = $2 / 25000;
        if ($num == int($num)) {
            $start = $num;
        } else {
            die "I am not sure how to interpret -start $start\n  ";
        }
    }
    if ($start > $fnum) {
        warn "  Request to analyze only after file $start results in directory being ignored\n";
        return;
    }
    my $end = $args->{END} || $fnum;
    $end = $fnum if ($end > $fnum);
    my $step = $args->{STEP} || 1;

    my @tasks;
    for (my $i = $start; $i <= $end; $i += $step) {
        push @tasks, $files[$i-1];
    }
    return @tasks;
}

sub trim_xml_files {
    my $tasks = shift;
    $args->msg("Paring down ".($#{$tasks} + 1)." XML files", "FIRST PASS");
    my $mb = 1000 * 1000;
    my $num = 0;
    foreach my $file (@{$tasks}) {
        next if ($file =~ /\Q$paredToken\E/);
        my $pFile;
        if ($file =~ /^(.+)(.xml.+)$/) {
            $pFile = join('', $1, $paredToken, $2);
            $pFile =~ s/\.gz$//;
        } else {
            next;
        }
        &trim_xml( $file, $pFile);
        system("gzip $pFile");
        $pFile .= ".gz";
        my $os = -s $file;
        my $ns = -s $pFile;
        my $short = $file;
        $short =~ s/.+\///;
        $num++;
        $args->msg(sprintf("[%3d]", $num), sprintf
                   ("%s: %.3f -> %.3fMb = %.1f%%", $short, 
                    $os / $mb, $ns / $mb, 100 * $ns / $os));
        last if ($limit && $num >= $limit);
    }
    $args->msg("Finished simplification");
    exit;
}

sub trim_xml {
    my ($file, $pFile) = @_;
    
    if ($file =~ /\.gz/) {
        open(READ, "gunzip -c $file|") || $args->death
            ("Failed to read gzip file", $file, $!);
    } else {
        open(READ, "<$file") || $args->death
            ("Failed to read file", $file, $!);
    }
    my $tt = &trim_tags();
    open(OUT, ">$pFile") || $args->death
        ("Failed to write output file", $pFile, $!);
    while (<READ>) {
        if (/^\s*<\/?(\S+?)[\s>]/) {
            next if ($tt->{$1});
        }
        print OUT $_;
    }
    close READ;
    close OUT;
}

sub parse_xml {
    my ($file) = @_;
    my @bits = split(/\//, $file);
    $shortFile = $bits[-1];
    my $isPared = 0;
    if ($shortFile =~ /\Q$paredToken\E/) {
        $shortFile =~ s/\Q$paredToken\E//;
        $isPared = 1;
    }
    # Keep this path only if the file has 
    if ($shortFile =~ /(.+)\.gz$/) {
        $shortFile = $1;
        if ($shortFile=~ /^(Substance|Compound)_/) {
            my $sc = $1;
            $prfx  = "PC-$sc";
        } elsif ($shortFile =~ /^(\d+)\.xml$/) {
            $prfx = "PC-AssayResults";
        } else {
            die "Failed to determine XML prefix from '$shortFile'\n  ";
        }
    } else {
        open(FILE, $file) || die "Failed to read $file:\n  $!\n  ";
        <FILE>;
        my $root = <FILE>;
        close FILE;
        chomp($root);
        if ($root =~ /^\<(\S+)s$/) {
            $prfx = $1;
        } else {
            die "Failed to determine XML prefix from '$root'\n  ";
        }
    }
    my $plainShort = $shortFile;
    $shortFile = "#File#$shortFile";
    
    my ($task, $meth);
    if ($prfx eq 'PC-Compound') {
        $meth = \&parse_cmpd;
        $task = "Parsed Compound Data";
    } elsif ($prfx eq 'PC-Substance') {
        $meth = \&parse_subs;
        $task = "Parsed Substance Data";
    } elsif ($prfx eq 'PC-AssayResults') {
        $meth = \&parse_bio;
        $task = "Parsed Bioassay Data";
        &bio_initialize( $file );
        return if ($args->{HEADONLY});
    } else {
        die "No method established for $prfx\n  ";
    }

    my $targTag = $prfx;
    my @skip = ("PC-Compound_atoms","PC-Compound_bonds",
                "PC-Compound_coords","PC-Compound_stereo",
                "PC-Compound_count", "PC-Compound_charge");

    # Not sure about these:
    # push @skip, ("PC-Substance_comment");

    my @saxArgs = ( -tag      => $targTag,
                    -textmeth => \&xml_text_method,
                    -skip     => \@skip,);

    my $status = "";
    if ($doSum || $doTrim) {
        if ($doSum) {
            $meth     = \&summarize;
            $status   = "Summarizing XML File";
        } else {
            $meth     = \&trimerize;
            $status   = "Trimming XML File";
        }
        $simpMeth = \&simplify;
        if ($prfx eq 'PC-Substance') {
            $simpMeth = \&simplify_subs;
        } elsif ($prfx eq 'PC-AssayResults') {
            $simpMeth = \&simplify_bios;
        }
        undef $task;
    }

    $lhParent  ||= $lh || &get_sts('parent')->loader();
    $mtParent  ||= $lhParent->tracker();
    $dbhParent ||= $mtParent->dbi->dbh();

    my $allDone;
    if ($task) {
        $task = "#FreeText#$task";
        my $edges = $mtParent->get_edge_dump
            ( -name1 => $task,
              -name2 => $shortFile,
              -type  =>  'has member' );
        if ($#{$edges} != -1) {
            if ($reProc) {
                $status = "XML File Re-processed";
            } else {
                $status = "XML File Previously Processed";
                $allDone = 1;
            }
        } else {
            $status = "XML File First Pass";
        }
    }

    if ($forkByFile) {
        
        &msg($status, $plainShort);
    }
    $args->msg("[+]",sprintf("%s : %s%s",$status, $plainShort, $isPared ? " (pared)" : "")) if ($vb);
    return if ($allDone);

    my $failed;
    if ($forkByFile) {
        # This file should be analyzed by one process
        eval {
            BMS::FriendlySAX->new
                ( -file    => $file,
                  -limit   => $limit,
                  -verbose => 0,
                  -method  => $meth,
                  @saxArgs
                  );
        };
        if ($@ && $@ !~ /user limit/i && $@ ne "\n") {
            die "FriendlySAX error:\n  '$@'\n  ";
        }
        &msg("Parse Complete", $plainShort);
    } else {
        $fc->reset();
        $fc->input_type( 'sax ');
        $fc->input_args( \@saxArgs );
        &set_fork_output();
        $fc->input( $file );
        $fc->method( $meth );
        $dbhParent->{InactiveDestroy} = 1;
        my $failed = $fc->execute( $args->{FORK} || 1 );
        $dbhParent->{InactiveDestroy} = 0;
        if ($failed) {
            $issues{"FAILED: $plainShort"} = $failed;
            warn "$failed processes did not execute properly\n";
        }
    }
    unless ($failed || $limit || $doSum || $doTrim) {
        if ($task) {
            $lhParent->set_edge( -name1 => $task,
                                 -name2 => $shortFile,
                                 -type  => 'has member' );
            $lhParent->write();
        }
    }
}

sub set_fork_output {
    while (my ($fk, $f) = each %stndFiles) {
        next unless ($f);
        $fc->output_file( $fk, ">>$f");
    }
    
}

sub fast_canon_smiles {
    my ($row) = @_;
    my $smi = $row->[0]; $smi =~ s/[\n\r]+$//;
    push @{$sts->{PC_CACHE}}, $smi;
    &act_on_smiles_cache(100);
}

sub fast_filter_smiles {
    my ($row) = @_;
    my $smi = $row->[0]; $smi =~ s/[\n\r]+$//;
    push @{$sts->{PC_CACHE}}, $smi;
    &act_on_smiles_cache(100);
}

sub fast_extract_smiles {
    my ($file, $dumpFile) = @_;
    # Nov 2011 - I think only the compound files have SMILES in them
    return unless ($file =~ /Compound/);
    $dumpFile ||= "SMILES_dump_$$.smi";
    die "SMILES dump file not defined\n  " unless ($dumpFile);
    my $cmd = "gunzip -c $file | egrep -A15 '<PC-Urn_name>Isomeric</PC-Urn_name>' | grep '<PC-InfoData_value_sval>' | egrep -o '>(.+)<' | egrep -o '[^\>\<]+'";
    $cmd .= " | head -n$limit" if ($limit);
    $cmd .= " > $dumpFile";
    $fc->write_output( 'SMSG', "Parsing $file\n".
                       "  Grepping SMILES to $dumpFile - ".`date`);
    my @bits = split(/\//, $file);
    $shortFile = $bits[-1];
    $sts->msg($shortFile);
    system($cmd);
    die "  Command failed to generate SMILES file:\n  CMD: $cmd\n  "
        unless (-e $dumpFile && -s $dumpFile);
    
    if ($dmpF) {
        my $smiles = `cat $dumpFile`;
        $fc->write_output( 'DUMP', $smiles);
        return;
    }

    $fc->write_output( 'SMSG', "  Registering SMILES - ".`date` );
    my @smis;
    if (open(SMI, "<$dumpFile")) {
        while (<SMI>) {
            s/[\n\r]$//;
            push @smis, "#SMILES#$_";
        }
        close SMI;
    } else {
        $sts->msg("Failed to read SMILES dump", $dumpFile, $!);
        die "Fatal!";
    }
    $lh->tracker->bulk_seq2id( @smis );

    $fc->write_output( 'SMSG', "  Canonicalizing SMILES - ".`date` );
    my $res = $sts->canonical( \@smis );
    $sts->loader->write();

    map { $issues{ $_->[3] || '-Unk-'}++ } @{$res} if ($res);
    my $msg = "  Finished - ".`date`;
    while (my ($txt, $cnt) = each %issues) {
        $msg .= sprintf("  %20s : %6d\n", $txt, $cnt);
    }
    $fc->write_output( 'SMSG', "$msg\n");
    unlink( $dumpFile );
    $issues{"File scanned for SMILES"}++;
}

sub summarize {
    my ($record) = @_;
    unless ($args->{NOSIMP}) {
        &{$simpMeth}($record);
    }
    $fc->write_output( 'Summary',
                       "\n---\n".BMS::FriendlySAX::node_to_text( $record ));
}

sub trimerize {
    my ($record) = @_;
    &{$simpMeth}($record);
    $fc->write_output( 'Trimmed', BMS::FriendlySAX::node_to_xml( $record ));
}

=pod A word about simplifying

The simplify() and simplify_subs() methods perform two tasks - they
extract needed information, and they also rearrange the XML structure
into somthing much more compact. The later process is unnecessary for
data extraction, but it greatly aids in helping a human to comprehend
the data (when used in conjunction with the summarize() method).

=cut

sub simplify {
    my ($record) = @_;

    $record->{ATTR}{id} = '';
    my $tagName = $record->{NAME};
    my @stack   = @{$idStack->{$tagName}};
    my $idp     = $idPrfx->{$tagName};
    my @found   = &extract_tree_text( $record, @stack );
    my @ids;
    foreach my $val (@found) {
        if ($val =~ /^\d+$/) {
            push @ids, $idp.$val;
        } else {
            &msg("Unusual ID",$val);
        }
    }
    my $id = '-UNK-';
    if ($#ids == 0) {
        # Unique ID found
        $id = $record->{ATTR}{id} = $ids[0];
        my ($pci) =  &extract_tree ( $record, $stack[0]);
        $pci->{KIDS}  = [];
        $pci->{BYTAG} = {};
    }

    my @infos = &extract_tree
        ( $record, "PC-Compound_props","PC-InfoData");
    foreach my $prop ( &extract_tree( $record, "PC-Compound_props")) {
        my @keep;
        foreach my $info (&extract_tree($prop, "PC-InfoData")) {
            my @urns   =  &extract_tree
                ( $info, "PC-InfoData_urn","PC-Urn");
            my ($name, $label) = ('UNK','UNK');
            if ($#urns == 0) {
                my $urn = $urns[0];
                my @labels = &extract_tree_text( $urn, "PC-Urn_label");
                if ($#labels == 0) {
                    $label = $labels[0];
                } else {
                    &msg("Non-unique info labels", $id);
                }
                next if ($ignoreProp{$label});
                my @names = &extract_tree_text( $urn, "PC-Urn_name");
                if ($#names == 0) {
                    $name = $names[0];
                } else {
                    # print "\n--->>\n".BMS::FriendlySAX::node_to_text( $urn );
                    &msg("Non-unique info names for label", $id, $label)
                        unless ($label eq 'InChI' ||
                                $label eq 'Molecular Formula');
                }
                next if ($ignoreProp{"$label : $name"});
               
                my @auths = &extract_tree_text( $urn, "PC-Urn_source");
                if ($#auths == 0) {
                    $info->{ATTR}{auth} = $auths[0];
                } else {
                    &msg("Non-unique info authorities", $id, $label,$name);
                }
                
            } else {
                &msg("Non-unique URN for info", $id);
            }
            push @keep, $info;
            my @vals;
            foreach my $vd (@{$info->{BYTAG}{'PC-InfoData_value'}}) {
                my $kids = $vd->{KIDS};
                if ($#{$kids} != 0) {
                    &msg("Non-unique value nodes", $id, $label,$name);
                    next;
                }
                push @vals, $kids->[0]{TEXT};
            }
            if ($#vals == 0) {
                $info->{TEXT} = $vals[0];
            } else {
                $info->{TEXT} = "";
                &msg("Non-unique values", $id, $label,$name);
            }
            $info->{ATTR}{name}  = $name;
            $info->{ATTR}{label} = $label;
            $info->{KIDS}  = [];
            $info->{BYTAG} = {};
        }
        $prop->{KIDS} = \@keep;
        $prop->{BYTAG}{"PC-InfoData"} = \@keep;
    }
}

sub simplify_subs {
    my ($record) = @_;
    &simplify($record);
    my $sid    = $record->{ATTR}{id};

    # Get xrefs
    my @refNodes = &extract_tree( $record, "PC-Substance_xref");
    unless ($#refNodes == -1) {
        my @refs = &extract_tree
            ( $record, "PC-Substance_xref","PC-XRefData");
        map { $_->{KIDS} = []; $_->{BYTAG} = {} } @refNodes;
        my $dest = $refNodes[-1]{KIDS};
        foreach my $refDat (@refs) {
            foreach my $ref (@{$refDat->{KIDS} || []}) {
                if ($ref->{NAME} =~ /PC-XRefData_(\S+)/) {
                    my $type = $1;
                    my $txt  = $ref->{TEXT};
                    push @{$dest}, {
                        ATTR => { type => $type },
                        NAME => 'XREF',
                        TEXT => $txt,
                        KIDS => [],
                    };
                    if ($type eq 'sburl' && $txt !~ /(listinput\.aspx)/) {
                        push @{$record->{urls}}, $txt;
                    }
                }
            }
        }
    }

    # Get the substance authority
    my @src    = &extract_tree( $record, "PC-Substance_source",);
    my @tracks = &extract_tree
        ( $record, "PC-Substance_source","PC-Source","PC-Source_db",
          "PC-DBTracking");
    if ($#tracks == -1 ) {
        $record->{ATTR}{ERROR} = "No Tracking";
        &msg("No tracking source found", $sid);
        return;
    } elsif ($#tracks != 0) {
        $record->{ATTR}{ERROR} = "Multiple Tracking";
        &msg("Multiple tracking soruces", $sid,($#tracks+1));
        return;
    }
    my $track = $tracks[0];
    
    my @auths = &extract_tree_text( $track, "PC-DBTracking_name");
    if ($#auths == -1 ) {
        $record->{ATTR}{ERROR} = "No Authority";
        &msg("No tracking authority", $sid);
        return;
    } elsif ($#auths != 0) {
        $record->{ATTR}{ERROR} = "Multile Authority";
        &msg("Multiple tracking authorities",$sid, join(',', @auths));
        return;
    }
    map { $_->{KIDS} = []; $_->{BYTAG} = {} } @src;


    # Find the compound ID(s) in this substance
    my @ctypes = &extract_tree
        ( $record, "PC-Substance_compound", "PC-Compounds", "PC-Compound",
          "PC-Compound_id", "PC-CompoundType", );
    my (@cids, @cidNodes);
    foreach my $ct (@ctypes) {
        my @tt  = @{$ct->{BYTAG}{'PC-CompoundType_type'} || []};
        my $typ = $#tt == 0 ? $tt[0]{ATTR}{value} : 'unknown';
        my @ids = &extract_tree_text
            ( $ct, "PC-CompoundType_id", "PC-CompoundType_id_cid");
        push @cids, @ids if ($typ eq 'standardized');
        push @cidNodes, {
            NAME => $typ,
            TEXT => join(' + ', @ids),
            KIDS => [],
        } unless ($#ids == -1);
    }
    my ($scRemap) = &extract_tree( $record, "PC-Substance_compound" );
    $scRemap->{KIDS} = \@cidNodes if ($scRemap);
    
    if ($#cids == -1) {
        &msg("No CID for substance", $sid);
        $record->{ATTR}{ERROR} = "No CID";
        # print BMS::FriendlySAX::node_to_text( $record );
       return;
    }
    $record->{ATTR}{cids} = join(' ', map { "CID:$_" } @cids);

    my @syns = &extract_tree_text
        ( $track, "PC-DBTracking_source-id","Object-id","Object-id_str");
    push @syns, &extract_tree_text
        ( $record, "PC-Substance_synonyms","PC-Substance_synonyms_E");

    my %uniq = map { $_ => 1 } @syns;
    my @synNodes = &extract_tree( $record, "PC-Substance_synonyms");
    @syns = ();
    foreach my $syn (sort keys %uniq) {
        next unless ($syn);
        # Collapse white space to single spaces
        $syn =~ s/[\n\r\t\s]+/ /g;
        # Remove some weird extras from end
        $syn =~ s/\s+(\(natural\))\s*$//i;
        # Trim lead/trail whitespace
        $syn =~ s/^\s+//; $syn =~ s/\s+$//;
        # Ignore pure digits
        next if ($syn =~ /^\d+$/);
        push @syns, $syn;
    }

    map { $_->{KIDS} = []; $_->{BYTAG} = {} } (@src, @synNodes);
    my $reMap = $#synNodes == -1 ? $src[0] : $synNodes[0];
    $reMap->{KIDS} = [ map { {
        NAME => 'Synonym',
        TEXT => $_,
        KIDS => [],
    } } @syns ];

    $record->{synonyms}   = \@syns;
    $record->{ATTR}{auth} = $auths[0];
}

sub simplify_bios {
    my ($record) = @_;
    my @sids = &extract_tree_text( $record, "PC-AssayResults_sid");
    my $aid  = $fileGlobals->{id};
    $record->{ATTR}{aid} = $aid;
    if ($#sids == -1) {
        &msg("No SID found for results", $aid);
        return;
    }
    my $sid = join(' ', map {"SID:$_"} @sids);
    if ($#sids > 0) {
        &msg("Multiple SIDs", $aid, $sid);
    }
    $record->{ATTR}{sid} = $sid;

    my @ranks = &extract_tree_text( $record, "PC-AssayResults_rank");
    if ($#ranks == 0) {
        $record->{ATTR}{rank} = $ranks[0];
    }

    my @outs = &extract_tree( $record, "PC-AssayResults_outcome");
    if ($#outs == 0) {
        $record->{ATTR}{outcome} = $outs[0]{ATTR}{value};
    } elsif ($#outs > 0) {
        &msg("Multiple Outcomes", $aid,
             join(', ', map { $_->{ATTR}{value} } @outs));
    }
    
    my @dRoot = &extract_tree( $record, "PC-AssayResults_data");
    if ($#dRoot == -1) {
        &msg("No data", $aid, $sid);
        return;
    }
    if ($#dRoot > 0) {
        &msg("Multiple data sections", $aid, $sid);
        return;
    }
    my @data = &extract_tree( $dRoot[0], "PC-AssayData");
    my @extracted;
    my $exTag = 'Data';
    foreach my $datum (@data) {
        my @tids = &extract_tree_text( $datum, "PC-AssayData_tid");
        # Require a unique tid:
        next unless ($#tids == 0);
        my $tid = $tids[0];
        my $tdat = $fileGlobals->{types}[$tid];
        my $name = $tdat->{name};
        unless ($name) {
            &msg("Failed to recover name for AssayData", $aid, $sid, $tid);
            next;
        }
        my @vals = &extract_tree( $datum, "PC-AssayData_value");
        if ($#vals != 0) {
            &msg("Non-unique value for datum", $aid, $sid, $name);
            next;
        } elsif ($#{$vals[0]{KIDS}} != 0) {
            &msg("Non-unique sub-value for datum", $aid, $sid, $name);
            next;
        }
        my %attr = ( name => $name );
        $attr{unit} = $tdat->{unit} if (defined $tdat->{unit});
        push @extracted, {
            NAME => $exTag,
            ATTR => \%attr,
            TEXT => $vals[0]{KIDS}[0]{TEXT},
            KIDS => [],
        };
    }
    &strip_child_nodes
        ($record, "PC-AssayResults_sid", "PC-AssayResults_outcome", 
         "PC-AssayResults_data","PC-AssayResults_date","PC-AssayResults_rank");
    push @{$record->{KIDS}}, @extracted;
    $record->{BYTAG}{Data} = \@extracted;
}

sub parse_bio {
    my ($record) = @_;
    &simplify_bios($record);
}

sub parse_cmpd {
    my ($record) = @_;
    &simplify($record);
    my $cid = $record->{ATTR}{id};
    unless ($cid) {
        &msg("Failed to find CID");
        return;
    }
    
    my @check = $sts->term_to_canonical( $cid );
    if ($#check == -1) {
        $issues{"Processing novel CID"}++;
    } elsif ($reDo) {
        $issues{"Reprocessing CID"}++;
    } else {
        $issues{"CID already processed"}++;
        return;
    }

    $lh->set_class($cid, 'PubChem');
    my @props = &extract_tree( $record, "PC-Compound_props", 'PC-InfoData');

    my $tagval = &info_data(\@props);
    my $chemKey;
    if (my $smis = $tagval->{'SMILES : Isomeric'}) {
        my @vals = keys %{$smis};
        if ($#vals == 0) {
            my $canSmi = $sts->canonical( $vals[0] );
            if ($canSmi && $#{$canSmi} == 0) {
                $chemKey = "#SMILES#" . $canSmi->[0][0];
            } else {
                &msg("Failed to generate canonical key", $cid);
                return;
            }
        } else {
            &msg("non-unique SMILES entries", $cid, $#vals+1);
            return;
        }
    } else {
        &msg("No Isomeric SMILES defined", $cid);
        return;
    }
    
    $lh->set_edge( -name1 => $chemKey,
                   -name2 => $cid,
                   type   => 'is a reliable alias for' );

    while (my ($tag, $vals) = each %{$tagval}) {
        my $actdat = $actions->{$tag};
        unless ($actdat) {
            $issues{"Ignoring tag: $tag"}++;
            print "\n---\n".BMS::FriendlySAX::node_to_text( $record );
            next;
        }

        my ($act, $ns, $var1, $var2) = @{$actdat};
        next unless ($act);
        $ns ||= '';
        # Normalize namespaces and authority names

        my %values;
        while (my ($val, $auths) = each %{$vals}) {
            my @auths;
            foreach my $rawAuth (keys %{$auths}) {
                my $auth = $authMap->{$rawAuth};
                if ($auth) {
                    push @auths, $auth;
                } else {
                    &msg("Unknown authority", $cid, $rawAuth);
                }
            }
            $values{$ns.$val} = \@auths;
        }
        if ($act eq 'alias') {
            if ($var1) {
                while (my ($id, $auths) = each %values) {
                    map { $lh->set_class($id, $var1, $_) } @{$auths};
                }
            }
            while (my ($id, $auths) = each %values) {
                map { $lh->set_edge( -name1 => $chemKey,
                                     -name2 => $id,
                                     -type  => 'is a reliable alias for',
                                     -auth  => $_ ) } @{$auths};
            }
            
        }
    }
    $lh->set_edge( -name1 => $chemKey,
                   -name2 => $shortFile,
                   -type  => 'is located within' );
    $lh->write_threshold_quick( 500 );
    # print BMS::FriendlySAX::node_to_text( $record );
}

sub parse_subs {
    my ($record) = @_;
    &simplify_subs($record);
    # print BMS::FriendlySAX::node_to_text( $record );
    my $sid     = $record->{ATTR}{id};
    unless ($sid) {
        &msg("Failed to find SID");
        return;
    }
    $lh->set_class($sid,"PUBCHEM");

    my $authRaw = $record->{ATTR}{auth};
    return unless ($authRaw);

    my $auth = $authMap->{$authRaw};
    unless ($auth) {
        &msg("Unknown substance authority", $sid, $authRaw);
        $auth = "Other PubChem";
    }

    my @cids = split(' ', $record->{ATTR}{cids});
    return if ($#cids == -1);

    my @chemKeys;
    foreach my $cid (@cids) {
        my @smis = $sts->term_to_canonical( $cid );
        if ($#smis == -1 ) {
            &msg("No SMILES found via SID-CID", $sid, $cid);
            return;
        } elsif ($#smis != 0) {
            &msg("Multiple SMILES via SID-CID", $sid, $cid, @smis);
            return;
        }
        push @chemKeys, $smis[0];
    }
    my $priKey = ($#chemKeys == 0) ? $chemKeys[0] : undef;

    my @aliases = ();
    my @syns    = @{$record->{synonyms}};
    foreach my $syn (@syns) {
        if ($syn =~ /^(EINECS|CCRIS|HSDB)\s*(\d+)$/) {
            # Well-formed accession
            push @aliases, "$1:$2";
        } elsif ($syn =~ /^(AIDS)\-?(\d+)/) {
            # Well-formed with dash
            push @aliases, "$1-$2";
        } elsif ($syn =~ /(\S+)\s+\(Beilstein Handbook Reference\)/) {
            push @aliases, "Beilstein:$1";
        } elsif ($syn =~ /\-NH2$/ || $syn =~ /\-NH2\;/) {
            # Peptide chain
            $lh->set_class($priKey, 'Protein', $auth) if ($priKey);
        } else {
            push @aliases, "#FreeText#$syn";
        }
    }

    if ($priKey) {
        $lh->set_edge
            ( -name1 => $priKey,
              -name2 => $sid,
              -type  => "is a reliable alias for" );
        map { $lh->set_edge
                ( -name1 => $priKey,
                  -name2 => $_,
                  -type  => "is a reliable alias for",
                  -auth  => $auth, ) } @aliases;
        if (my $urls = $record->{urls}) {
            my @url = map { "#File#$_" } @{$urls};
            map { $lh->set_class($_, 'HyperLink', $auth);
                  $lh->set_edge
                      ( -name1 => $priKey,
                        -name2 => $_,
                        -type  => "is referenced in",
                        -auth  => $auth, ) } @url;
        }
    }
    map {
        $lh->set_edge( -name1 => $_,
                       -name2 => $shortFile,
                       -type  => 'is located within' ) } @chemKeys;

    $lh->write_threshold_quick( 500 );
}

sub extract_tree {
    my $seed  = shift;
    my @stack = ($seed);
    while (my $name = shift @_) {
        my @extracted;
        foreach my $node (@stack) {
            push @extracted, @{$node->{BYTAG}{$name} ||[]};
        }
        @stack = @extracted;
        last if ($#stack == -1);
    }
    return @stack;
}

sub extract_tree_text {
    return map { $_->{TEXT} } &extract_tree( @_ );
}

sub xml_text_method {
    my ($arr) = @_;
    # Join with no spaces
    my $txt = join('', @{$arr});
    # Remove leading and trailing whitespace:
    $txt =~ s/\s+$//;
    $txt =~ s/^\s+//;
    # Replace XML escape codes with literals
    while ($txt =~ /\&([^\;]{1,6})\;/) {
        my $code = $1;
        my $old  = '&'.$code.';';
        my $rep  = $escapeCodes->{$code};
        unless (defined $rep ) {
            $rep = '-UNK-';
            &msg("Unknown HTML code", $code);
        }
        $txt =~ s/\Q$old\E/$rep/g;
    }
    return $txt;
}

sub info_data {
    my ($arr) = @_;
    my %rv;
    my @meta = qw(label name auth);
    foreach my $dat (@{$arr}) {
        # print BMS::FriendlySAX::node_to_text( $dat );
        my ($label, $name, $auth) = map { $dat->{ATTR}{$_} } @meta;
        # warn "($label, $name, $auth)";
        $rv{"$label : $name"}{ $dat->{TEXT} }{$auth} = 1;
    }
    return \%rv;
}

sub initialize {
    # Set global $lh and $sts
    %issues = ();
    return if ($lh);
    $sts = &get_sts( @_ );
    $lh  = $sts->loader();
    
}

sub bio_initialize {
    my ($file) = @_;
    $fileGlobals = undef;
    my @skip = ("PC-AssayDescription_description", "PC-AssayDescription_protocol", "PC-AssayDescription_comment");
    eval {
        BMS::FriendlySAX->new
            ( -file    => $file,
              -limit   => 1,
              -verbose => 0,
              -method  => \&init_assay,
              -tag     => 'PC-AssayDescription',
              -textmeth => \&xml_text_method,
              -skip    => \@skip,
              );
      };
    die "Failed to find distinct assay information in file"
        unless ($fileGlobals);
    die "Error extracting assay information"
        if ($fileGlobals->{error});
    if ($vb > 1) { 
        $args->branch($fileGlobals);
        my $frm = "%3s %8s %8s %-20s %s\n";
        warn "Type deffinitions for $fileGlobals->{id}\n".
            sprintf($frm, "ID", "Type", "Unit", "Name", "Description");
        foreach my $type (@{$fileGlobals->{types}}) {
            next unless ($type);
            warn sprintf($frm, (map { defined $type->{$_} ? $type->{$_} : '' } qw(id type unit name)),  substr($type->{desc} || '',0,40));
        }
        warn "\n";
    }
    # die "WORKING";
}

sub init_assay {
    my ($record) = @_;
    if ($fileGlobals) {
        &msg("Multiple ".$record->{NAME}." records in file", 
             $fileGlobals->{id});
        $fileGlobals->{error} = 1;
        return;
    }
    $fileGlobals = {
        types => [],
    };
    my $aid;
    my @ids = &extract_tree_text
        ($record, "PC-AssayDescription_aid","PC-ID", "PC-ID_id");
    if ($#ids == -1) {
        &msg("Failed to find BioAssay ID");
        $fileGlobals->{error} = 1;
        return;
    } elsif ($#ids > 0) {
        &msg("Multiple BioAssay IDs", join(',', @ids));
        $fileGlobals->{error} = 1;
        return;
    } elsif ($ids[0] =~ /^\d+$/) {
        $aid = "AID:$ids[0]";
        $fileGlobals->{id} = $aid;
    } else {
        &msg("Non-numeric BioAssay ID", $ids[0]);
        $fileGlobals->{error} = 1;
        return;
    }
    my @auths = &extract_tree_text
        ($record, "PC-AssayDescription_aid-source", "PC-Source", "PC-Source_db", "PC-DBTracking", "PC-DBTracking_name");
    if ($#auths == -1) {
        &msg("No authority found for BioAssay", $aid);
    } else {
        &msg("Multiple authorities found for BioAssay", 
             $aid, join(', ', @auths)) if ($#auths > 0);
        $fileGlobals->{auth} = $auths[0];
    }

    my @meths = &extract_tree
        ($record, "PC-AssayDescription_activity-outcome-method");
    if ($#meths == 0) {
        $fileGlobals->{method} = $meths[0]{ATTR}{value};
    } elsif ($#meths > 0) {
        &msg("Multiple methods found for BioAssay", 
             $aid, join(', ', map { $_->{ATTR}{value} } @meths));
    }

    my @descs = &extract_tree_text
        ($record, "PC-AssayDescription_name");
    $fileGlobals->{desc} = $descs[0];

    # Get the type deffinitions
    my @typeRoots = &extract_tree( $record, "PC-AssayDescription_results");
    if ($#typeRoots != 0) {
        &msg("Non-unique type section", $aid);
        $fileGlobals->{error} = 1;
        return;
    }
    my $root  = $typeRoots[0];
    my @types = &extract_tree( $root, "PC-ResultType");
    foreach my $type (@types) {
        my @tids = &extract_tree_text($type, "PC-ResultType_tid");
        if ($#tids != 0) {
            &msg("Non-unique IDs for BioAssay", $aid, 
                 join(',', @tids) || 'None');
            $fileGlobals->{error} = 1;
            return;
        }
        my $tid = $tids[0];
        my @names = &extract_tree_text($type, "PC-ResultType_name");
        if ($#names != 0) {
            &msg("Non-unique Names for BioAssay type", $aid, "Type ID $tid",
                 join(',', @names) || 'None');
        }
        my $name = $names[0] || "Field $tid";
        my $tobj = $fileGlobals->{types}[$tid] = {
            id   => $tid,
            name => $name,
        };
        my @units = &extract_tree($type, "PC-ResultType_unit");
        if ($#units == 0) {
            $tobj->{unit} = $units[0]{ATTR}{value};
        } elsif ($#units > 0) {
            &msg("Multiple units for column type",
                 $aid, $tid, join(', ', @units));
        }
        my @scalar = &extract_tree($type, "PC-ResultType_type");
        if ($#scalar == 0) {
            $tobj->{type} = $scalar[0]{ATTR}{value};
        } elsif ($#scalar > 0) {
            &msg("Multiple types for column type",
                 $aid, $tid, join(', ', @scalar));
        }
        my @descs = &extract_tree_text($type, "PC-ResultType_description","PC-ResultType_description_E");
        $tobj->{desc} = join(' ', @descs) if ($#descs != -1);
        $issues{"Field: $name"}++;
    }

    my @xrefs = &extract_tree( $record, "PC-AssayDescription_xref","PC-AnnotatedXRef","PC-AnnotatedXRef_xref","PC-XRefData");
    foreach my $xref (@xrefs) {
        foreach my $kid (@{$xref->{KIDS}}) {
            if ($kid->{NAME} =~ /^PC-XRefData_(\S+)$/) {
                my $xname = $1;
                push @{$fileGlobals->{xrefs}{$xname}}, $kid->{TEXT};
                $issues{"XREF: $xname"}++;
            }
        }
    }

    my @targs = &extract_tree( $record, "PC-AssayDescription_target","PC-AssayTargetInfo");
    foreach my $targ (@targs) {
        my @names = &extract_tree_text($targ, "PC-AssayTargetInfo_name");
        my $name = $names[0] || 'Unknown';
        my @pids = &extract_tree_text($targ, "PC-AssayTargetInfo_mol-id");
        if ($#pids == -1) {
            &msg("No ID found for target", $aid, $name);
            $fileGlobals->{error} = 1;
            return;
        } elsif ($#pids > 0) {
            &msg("Multiple IDs found for target", $aid, $name,
                 join(', ', @pids));
            $fileGlobals->{error} = 1;
            return;
        }
        my $gi = $pids[0];
        my @types = &extract_tree($targ, "PC-AssayTargetInfo_molecule-type");
        my $type;
        if ($#types == -1) {
            $type = "unknown";
        } elsif ($#types > 0) {
            &msg("Multiple types found for target", $aid, $name, 
                 join(', ', @types));
            $fileGlobals->{error} = 1;
            return;
        } else {
            $type = $types[0]{ATTR}{value};
        }
        push @{$fileGlobals->{targets}}, {
            name => $name,
            gi   => $gi,
        };
    }
    # Clear the node for easier visualization of residual nodes in this branch
    &strip_child_nodes($record, "PC-AssayDescription_aid","PC-AssayDescription_aid-source","PC-AssayDescription_revision","PC-AssayDescription_results", "PC-AssayDescription_target", "PC-AssayDescription_xref", "PC-AssayDescription_activity-outcome-method", "PC-AssayDescription_name");

    print BMS::FriendlySAX::node_to_text( $record )
        if ($#{$record->{KIDS}} > -1);
    # warn $args->branch($fileGlobals);
}

sub strip_child_nodes {
    my $record = shift;
    my %toStrip;
    foreach my $tag (@_) {
        $toStrip{$tag} = 1;
        delete $record->{BYTAG}{$tag};
    }
    my @keep;
    foreach my $kid (@{$record->{KIDS}}) {
        push @keep, $kid unless ($toStrip{$kid->{NAME}});
    }
    $record->{KIDS} = \@keep;
}

sub get_sts {
    # Non-globals
    my ($isParent) = @_;
    my $lh = BMS::MapTracker::LoadHelper->new
        ( -username => 'PubChem',
          -basedir   => $basedir,
          -loadtoken => "PubChem",
          -testmode => $tm, );
    if ($isParent) {
        if ($testFile && $tm) {
            $lh->redirect( -stream => 'TEST', -file => ">>$testFile" );
        }
    } else {
        if (my $fh = $fc->output_fh('TestFile')) {
            $lh->redirect( -stream => 'TEST', -fh => $fh );
        }
        if (my $fh = $fc->output_fh('SMSG')) {
            select((select($fh), $| = 1)[$[]); # Bob's call to autoflush
        }
    }

    my $sts = BMS::MapTracker::SciTegicSmiles->new( -loader => $lh );
    $sts->load_cache(500);
    $sts->{PC_CACHE} = [];
    return $sts;
}

sub finalize {
    &act_on_smiles_cache() if ($sts);
    if ($lh) {
        $lh->write();
        $issues{"Rows written"} += $lh->rows_written();
    } else {
        warn &stack_trace("LoadHelper object has expired",
                          "  Some data may not have been written!!");
    }
    my $msg = "";
    &msg("Completed tasks", $shortFile) if ($shortFile && !$forkByFile);
    foreach my $key (sort keys %issues) {
        $msg .= join("\t", $key, $issues{$key})."\n";
    }
    if ($msg) {
        if ($fc) {
            $fc->write_output('Issues', $msg);
        } else {
            warn "ForkCritter object is undef!\n$msg\n";
        }
    }
    exit $exitCode;
}

sub msg {
    my @bits = @_;
    my $pri = $bits[0];
    return unless ($pri);
    $issues{$pri}++;
    my $msg = join("\t", @bits);
    $fc->write_output('Messages', "$msg\n");
}

sub stack_trace {
    my $msg = join("\n", @_);
    $msg = $msg ? "$msg\n" : "";
    my @history;
    my $hist = 2;
    while (1) {
        my ($pack, $file, $j4, $subname) = caller($hist);
        last unless ($subname);
        my ($j1, $j2, $line) = caller($hist-1);
        push @history, sprintf("  %50s : %d\n", $subname, $line);
        $hist++;
    }
    return $msg . (join('', @history) || '-No stack trace-');
}

sub act_on_smiles_cache {
    my $limit = shift;
    my $cache = $sts->{PC_CACHE};
    return if (!$cache || $#{$cache} == -1 ||
               ($limit && $#{$cache} + 1 < $limit));
    if ($args->{PRELOAD}) {
        my @nsIDs = map { "#SMILES#$_" } @{$cache};
        $sts->tracker->bulk_seq2id( @nsIDs );
    } elsif ( $args->{FILTER}  ) {
        my ($done, $needed) = $sts->db_canonical( $cache );
        if ($needed && $#{$needed} != -1) {
            $fc->write_output( 'OUT', join('', map {"$_\n"} @{$needed}));
        }
    } else {
        $sts->canonical( $cache );
    }
    $sts->{PC_CACHE} = [];
}

sub trim_tags {
    return $trimTags if ($trimTags);
    my $tags = <<TAGTEXT;
PC-Compound_atoms
PC-Atoms
PC-Atoms_aid
PC-Atoms_aid_E
PC-Atoms_element
PC-Element
PC-Compound_bonds
PC-Bonds
PC-Bonds_aid1
PC-Bonds_aid1_E
PC-Bonds_aid2
PC-Bonds_aid2_E
PC-Bonds_order
PC-BondType
PC-Compound_coords
PC-Coordinates
PC-Coordinates_type
PC-CoordinateType
PC-Coordinates_aid
PC-Coordinates_aid_E
PC-Coordinates_conformers
PC-Conformer
PC-Conformer_x
PC-Conformer_x_E
PC-Conformer_y
PC-Conformer_y_E
PC-Conformer_style
PC-DrawAnnotations
PC-DrawAnnotations_annotation
PC-BondAnnotation
PC-DrawAnnotations_aid1
PC-DrawAnnotations_aid1_E
PC-DrawAnnotations_aid2
PC-DrawAnnotations_aid2_E

PC-Atoms_charge
PC-AtomInt
PC-AtomInt_aid
PC-AtomInt_value

PC-Atoms_isotope

PC-Urn_release
PC-Urn_datatype
PC-UrnDataType
PC-Urn_version
PC-Urn_software

PC-Urn_implementation

PC-Compound_count
PC-Count
PC-Count_heavy-atom
PC-Count_atom-chiral
PC-Count_atom-chiral-def
PC-Count_atom-chiral-undef
PC-Count_bond-chiral
PC-Count_bond-chiral-def
PC-Count_bond-chiral-undef
PC-Count_isotope-atom
PC-Count_covalent-unit
PC-Count_tautomers

PC-Compound_stereo
PC-StereoCenter
PC-StereoCenter_tetrahedral
PC-StereoTetrahedral
PC-StereoTetrahedral_center
PC-StereoTetrahedral_above
PC-StereoTetrahedral_top
PC-StereoTetrahedral_bottom
PC-StereoTetrahedral_parity
PC-StereoTetrahedral_below
PC-StereoTetrahedral_type

PC-StereoCenter_planar
PC-StereoPlanar
PC-StereoPlanar_left
PC-StereoPlanar_ltop
PC-StereoPlanar_lbottom
PC-StereoPlanar_right
PC-StereoPlanar_rtop
PC-StereoPlanar_rbottom
PC-StereoPlanar_parity
PC-StereoPlanar_type

PC-Atoms_radical
PC-AtomRadical
PC-AtomRadical_aid
PC-AtomRadical_type

PC-Atoms_label
PC-AtomString
PC-AtomString_aid
PC-AtomString_value



TAGTEXT

    $trimTags = {};
    foreach my $tag (split(/[\n\r\s]+/, $tags)) {
        $trimTags->{$tag} = 1 if ($tag);
    }
    return $trimTags;
}


=head3 Unusual files

Based on percent 'pruned'

 2118 PubChem XML files

 Compound_000075001_000100000.xml.gz 39.372 -> 7.507Mb = 19.1%
 Compound_002925001_002950000.xml.gz 68.792 -> 10.304Mb = 15.0%
 Compound_003175001_003200000.xml.gz 85.219 -> 7.754Mb = 9.1%
 Compound_006625001_006650000.xml.gz 111.431 -> 8.903Mb = 8.0%
 Compound_009625001_009650000.xml.gz 73.274 -> 11.244Mb = 15.3%
 Compound_012200001_012225000.xml.gz 6.013 -> 1.086Mb = 18.1%
 Compound_012225001_012250000.xml.gz 4.944 -> 0.912Mb = 18.4%
 Compound_045075001_045100000.xml.gz 8.506 -> 2.088Mb = 24.5%
 Compound_049550001_049575000.xml.gz 0.006 -> 0.002Mb = 29.3%


=cut
