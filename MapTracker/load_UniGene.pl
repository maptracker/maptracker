#!/stf/sys64/bin/perl -w

BEGIN {
    # Allows usage of beta modules to be tested:
    my $prog = $0; my $dir = `pwd`;
    if ($prog =~ /working/ || $dir =~ /working/) {
	warn "\n\n *** This is Beta Software ***\n\n";
	require lib;
	import lib '/stf/biocgi/tilfordc/perllib';
    }
    require lib;
    import lib '/stf/biocgi/tilfordc/released/Bio/SeqIO';
}

# Getting most everything:
#    Using process_refseq, process_versioning, and process_gi on accessions
#    Doing all Tissue calculations
#
# 710011 UniGene IDs generated 282.376 million MapTracker rows (397 per ID)

# If we limit sequence accessions to just:
#   Setting accession class and taxa for versioned and unversioned acc
#   Tying versioned accession to UniGene ID
#   Tie the versioned accession to the unversioned accession
#   ( a total of 6 rows per accession )
# ... then we get:
#
# 

# Tissue distribution data contributes 1.632 million rows

# There are about 16 million accessions
# Sequence class counts:
#                   EST = 15,417,720
#                   HTC =    192,243
#                 MODEL =     54,138
#                  MRNA =    454,794


=pod Multiplicities

Locus Link ID with multiple UniGene clusters:

 LOC75552 - Mm.151485 Mm.432664 Mm.455809 (pared down to 1 later)

 LOC10052 - Hs.659160 Hs.660494

=cut

my $VERSION = 
    ' $Id$ ';

use strict;
use BMS::MapTracker::LoadHelper;
use BMS::ForkCritter;
use BMS::SequenceLibraryFinder;
use BMS::ArgumentParser;

my $markupClasses = { map { uc($_) => 1 } qw(mRNA EST HTC) };


my $args = BMS::ArgumentParser->new
    ( -path     => '/work5/tilfordc/unigene',
      -nocgi    => 1,
      -testmode => 1,
      -h        => undef,
      -help     => undef,
      -clean    => 0,
      -limit    => 0,
      -progress => 180,
      -basedir  => 'UniGene',      
      -species  => '',
      -cache    => 20000,
      -paramalias => {
          tempdir    => [qw(tmpdir)],
          limit      => [qw(lim)],
          help       => [qw(h)],
          species    => [qw(taxa taxae)],
          xxxx => [qw()],
          xxxx => [qw()],
          xxxx => [qw()],
          xxxx => [qw()],
          xxxx => [qw()],
          xxxx => [qw()],
          xxxx => [qw()],
      });

$args->shell_coloring();

if ($args->val('help')) {
    print "\n\n"; system("pod2text $0"); print "\n\n";
    exit;
}
my ($mt, $lh, $tissues, $acccount);
my $limit    = $args->val('limit') || 0;
my $testfile = $args->val('testfile');
my $tm       = $args->val('testmode');
$tm          = $testfile ? 1 : 0 unless (defined $tm);
my $cache    = $args->val('cache');
my $clean    = $args->{CLEAN};
my $path     = $args->{PATH};
my $errf     = 'Unigene_Messages.txt';
my $keeps    = undef;
my $info     = {};
my $baseDir  = $args->{BASEDIR};

unlink($testfile) if ($testfile && -e $testfile);

if (my $tx = $args->val('species')) {
    my $slf  = BMS::SequenceLibraryFinder->new();
    my @taxa = $slf->stnd_taxa($tx);
    $args->death("I could not parse species from -taxa '$tx'")
        if ($#taxa == -1);
    $keeps = { map { $_ => 1 } @taxa };
}
$args->msg("[!]","Testmode is on") if ($tm);

if (my $upd = $args->val(qw(wget update))) {
    my $cmd = "/work5/tilfordc/WGET/unigene_wget.sh";
    $args->msg("[+]","WGET mirror requested", $cmd, `date`);
    system($cmd);
    $args->msg("[-]","Done", `date`);
    if ($upd =~ /only/i) {
        $args->msg("[!]", "Request to ONLY perform update");
        exit;
    }
} else {
    $args->msg("[!!]","USING EXISTING DATA - NO UPDATE!",
               "Use -wget if you wish to update mirrored data.");
}


my $fc = BMS::ForkCritter->new
    ( -init_meth   => \&initialize,
      -finish_meth => \&finish,
      -limit       => $limit,
      -progress    => $args->{PROGRESS},
      -verbose     => $args->{VERBOSE} );


&parse_dir;
$args->msg("[>]","Test file written", $testfile) if ($testfile);
if ($baseDir && $lh && !$tm) {
    $args->msg("[-]","Loading base directory $baseDir");
    $lh->process_ready();
}
$args->msg("All Operations Finished", `date`);

sub parse_dir {
    my %species;
    opendir(TMPDIR, $path)
        || die "Failed to read contents of '$path':  $!\n  ";
    foreach my $d (readdir TMPDIR) {
        next if ($d eq '.' || $d eq '..');
        $d = "$path/$d";
        next unless (-d $d);
        opendir(TMPSUBDIR, $d)
            || die "Failed to read contents of '$d':  $!\n  ";
        foreach my $file (readdir TMPSUBDIR) {
            if ($file =~ /^([A-Z][a-z]+)\.([a-z]+)/) {
                $species{$1}{$2} = "$d/$file";
            }
        }
        closedir TMPSUBDIR;
    }
    closedir TMPDIR;
    
    my @allspec = sort keys %species;
    foreach my $spec (@allspec) {
        my $ifile = $species{$spec}{info};
        unless ($ifile) {
            die "No info file for $spec\n  ";
            next;
        }
        my $fh;
        open($fh, "<$ifile") || $args->death
            ("Failed to read file", $ifile, $!);
        my $header = <$fh>;
        chomp $header;
        close $fh;
        if ($header =~ /Build\s*\#(\d+)\s+(.+)$/) {
            my ($bld, $sn) = ($1, $2);
            $info->{$spec} = {
                build   => $bld,
                species => $sn,
            };
            if ($keeps && !$keeps->{$sn}) {
                $species{$spec}{SKIP} = 1;
            } else {
                $args->msg("[+]", sprintf
                           ("  %3s [%4d] : %s", $spec, $bld, $sn));
            }
        } else {
            die "Failed to parse $spec info '$header'\n  ";
        }
    }
    &fork_history(\%species) unless ($args->{CLEAN});
    &fork_unigene(\%species) unless ($args->{HISTONLY});
}

sub fork_history {
    my ($species) = @_;
    my @basic;
    foreach my $spec (sort keys %{$species}) {
        my $file = $species->{$spec}{retired};
        next if (!$file || $species->{$spec}{SKIP});
        push @basic, [$spec, $file];
    }

    my $bnum = $#basic + 1;
    return unless ($bnum);
    $args->msg("[*]","Analyzing history data in $bnum files");
    $fc->reset();
    $fc->method( \&analyze_history );
    $fc->skip_record_method( \&history_skip_meth );
    $fc->input_type( 'tab groupby_0' );
    $fc->output_file( 'TestFile', ">>$testfile" ) if ($testfile);

    $fc->doall(1);
    $fc->total_fork( $bnum );
    foreach my $dat (@basic) {
        my ($spec, $file) = @{$dat};
        $args->msg("[<]", sprintf("  %4s : %s", $spec, $file));
        $fc->input( $file, 'tab groupby_0');
        $fc->fork;
    }
    $fc->wait();
    $args->msg("Assembling child output...");
    $fc->join_files();
}

sub fork_unigene {
    my ($species) = @_;
    my @basic;
    foreach my $spec (sort keys %{$species}) {
        my $file = $species->{$spec}{data};
        next if (!$file || $species->{$spec}{SKIP});
        push @basic, [$spec, $file];
    }

    my $bnum = $#basic + 1;
    return unless ($bnum);
    $args->msg("[*]","Analyzing basic data in $bnum files");
    $fc->reset();
    my $meth    = \&analyze_record;
    my $tisfile = 'Unigene_Tissues_raw.txt';
    if ($args->{CLEAN}) {
        $meth = \&clean_locus_membership;
    } else {
        $fc->output_file('tissue', $tisfile);
    }
    $fc->method( $meth );
    $fc->skip_record_method( 0 );
    $fc->input_type( 'basic' );

    $fc->output_file('error', $errf || *STDERR);
    $fc->output_file( 'TestFile', ">>$testfile" ) if ($testfile);
    $fc->doall(1);
    $fc->total_fork( $bnum );
    $fc->next_record_method( \&next_unigene );
    foreach my $dat (@basic) {
        my ($spec, $file) = @{$dat};
        $args->msg("[<]", sprintf("  %4s : %s", $spec, $file));
        $fc->input( $file, 'basic');
        $fc->fork;
    }
    $fc->wait();
    $args->msg("[+]", "Assembling child output...");
    $fc->join_files();
    &process_tissues($tisfile) unless ($args->{CLEAN});
}

sub next_unigene {
    my $childfc = shift;
    my $fh      = $childfc->{IO};
    my $record  = {};
    while (<$fh>) {
        chomp;
        if ($_ eq '//') {
            $childfc->last_item( $record->{ID} );
            return $record;
        }
        my @bits = split(/\s+/, $_);
        if (my $tag = shift @bits) {
            my $val = join(' ', @bits);
            $record->{$tag} ||= [];
            push @{$record->{$tag}}, $val;
        }
    }
    return undef;
}

sub clean_locus_membership {
    my ($record) = @_;
    if (!$record->{ID} || $#{$record->{ID}} != 0) {
        &msg($record, "Single ID not found");
        return;
    }
    my $ugID = $record->{ID}[0];
    $lh->kill_class($ugID, 'deprecated');
    $lh->kill_edge( -name2 => $ugID,
                    -auth  => 'LocusLink',
                    # -auth  => 0,
                    # -override => 1,
                    -type  => 'memberof',);

    if ($record->{LOCUSLINK} && $#{$record->{LOCUSLINK}} == 0) {
        my $lnum = $record->{LOCUSLINK}[0];
        if ($lnum =~ /^\d+$/) {
            my $locid = "LOC$lnum";
            $lh->set_edge( -name1 => $locid,
                           -name2 => $ugID,
                           -type  => 'memberof',
                           -auth  => 'locuslink');
        }
    }
    $lh->write_threshold_quick($cache);
}

sub analyze_record {
    my ($record) = @_;
    if (!$record->{ID} || $#{$record->{ID}} != 0) {
        &msg($record, "Single ID not found");
        return;
    }
    my $ugID = $record->{ID}[0];
    my $tid = $record->{species} = &species_for_id( $ugID);
    unless ( $tid ) {
        return;
    }

    $lh->set_taxa($ugID, $tid);
    $lh->set_class($ugID, 'cluster');
    $lh->set_class($ugID, 'unigene');
    $lh->kill_class($ugID, 'deprecated');
    &set_gene_info( $record );
    &note_tissues( $record );

    $lh->kill_edge( -name1 => $ugID,
                    -type  => 'is a cluster with sequence' );
    foreach my $sd (&tag_to_hash($record, 'SEQUENCE')) {
        my $acc    = $sd->{ACC};
        # EST, HTC, Model, mRNA :
        my $class  = uc($sd->{SEQTYPE});
        
        # Annotate mRNAs that are not RefSeq - by doing so, we leave
        # annotation of RefSeq to either LocusLink or RefSeq itself
        # (more reliable authorities for the task), and we avoid
        # adding data for the tons of ESTs associated with UniGene
        my $markup = ($markupClasses->{$class} && 
                      $acc !~ /^[NX][MR]_/) ? 1 : 0;
#        $lh->set_class($acc, $class, 'NCBI')
#            if ($class eq 'EST' || ($class eq 'MRNA' && !$markup));
        my ($idU, $idV, $vnum) = ($acc);

        if ($acc =~ /^([^\.]{6,})\.(\d+)/) {
            # This looks like a nicely versioned accession
            ($idU, $idV, $vnum) = ($1, $acc, $2);
            # Link versioned to unversioned ID:
            if ($markup) {
                $lh->process_versioning($idU, $vnum, $class, $tid, 'ncbi');
            } else {
                # At least tie the unversioned ID to the versioned one.
                # No, we will skip - this should be covered by RefSeq
                #$lh->set_edge( -name1 => $idU,
                #               -name2 => $idV,
                #               -type  => 'unversioned',
                #               -auth  => 'ncbi');
            }
        } else {
            &msg($record, "Ugly accession $acc");
            next;
        }
        # I was setting the edge to the versioned sequence. Because UniGene
        # updates the clusters with some regularity, I have decided to 
        # go ahead and point to the unversioned IDs.
        $lh->set_edge( -name1 => $ugID,
                       -name2 => $idU,
                       -type  => 'is a cluster with sequence' );

        if (1) {
            # GenAcc has safeties in place that prevent transition through
            # nodes that do not have a common unique taxa. If we do not
            # set the taxa of the members, then they can not be used by GenAcc

            # $lh->set_taxa($idU, $tid);
        }

        if ($sd->{NID} && $markup && $class eq 'mRNA') {
            # Add GI number, but only for mRNA entries
            my $gi = $lh->process_gi($sd->{NID}, $tid, $class);
            if ($gi) {
                $lh->set_edge( -name1 => $gi,
                               -name2 => $idV,
                               -type  => 'reliable', 
                               -auth  => 'ncbi');
                $lh->set_edge( -name1 => $idV,
                               -name2 => $gi,
                               -type  => 'reliable', 
                               -auth  => 'ncbi');
            }
        }
    }

    
    $lh->kill_edge( -name2 => $ugID,
                    -type  => 'memberof',
                    -auth  => 'locuslink');

    if ($record->{LOCUSLINK} && $#{$record->{LOCUSLINK}} == 0) {
        # $lh->write(); die $debug->branch($record);
        my $lnum = $record->{LOCUSLINK}[0];
        if ($lnum =~ /^\d+$/) {
            my $locid = "LOC$lnum";
            $lh->set_edge( -name1 => $locid,
                           -name2 => $ugID,
                           -type  => 'memberof',
                           -auth  => 'locuslink');
        }
    }
    $lh->write_threshold_quick($cache);
}

sub history_skip_meth {
    my ($set)   = @_;
    my $oldID   = $set->[0][0];
    my $isalive = 0;
    my (%newIDs, @to_remove);
    while (my $record = shift @{$set}) {
        my ($priorID, $currentID, $seqID, $gbID) = @{$record};
        # Skip some of the commentary at start of file:
        next unless ($gbID); 
        if ($priorID eq $currentID) {
            # If the two IDs are the same, then there was no change
            $isalive = 1;
        } else {
            push @to_remove, $gbID;
            $newIDs{$currentID}++ unless ($currentID =~ /\.0$/);
        }
    }
    # Nothing needs to be updated:
    return 1 if ($#to_remove == -1);

    push @{$set}, [$oldID, $isalive, \@to_remove, \%newIDs ];
    return 0;
}

sub analyze_history {
    my ($set) = @_;
    return if (!$set || $#{$set} == -1);
    my ($oldID, $isalive, $to_remove, $newIDs ) = @{$set->[-1]};

    if ($isalive) {
        # The old ID is still active, we just need to remove some entries
        foreach my $gbID (@{$to_remove}) {
            $lh->kill_edge( -name1 => $oldID,
                            -name2 => $gbID,
                            -type  => 'is a cluster with sequence', );
        }
    } else {
        # The entire ID has been retired
        $lh->set_class($oldID, 'deprecated');
        $lh->kill_edge( -name1 => $oldID,
                        -type  => 'is a cluster with sequence', );
        foreach my $newID (keys %{$newIDs}) {
            $lh->set_edge( -name1 => $oldID,
                           -name2 => $newID,
                           -type  => 'is a deprecated entry for' );
        }
    }
}

sub species_for_id {
    my ($id) = @_;
    my $spec = "";
    if ($id =~ /^([A-Z][a-z]+)\./) {
        my $tok = $1;
        unless ($spec = $info->{$tok}{species}) {
            $args->msg_once("[!!]",
                            "No species available for UniGene token $tok");
        }
    }
    return $spec;
}

sub note_tissues {
    my ($record) = @_;
    return unless ($record->{EXPRESS});
    my $ugID = $record->{ID}[0];    
    # Record the tissues that ESTs were derived from
    foreach my $line (@{$record->{EXPRESS}}) {
        # GRRR. They changed tokens!
        # foreach my $tissue (split(/ \; /, lc($line))) {
        foreach my $tissue (split(/\s*\|\s*/, lc($line))) {
            # Ignore fluffy terms:
            next if ($tissue =~ /other/  || $tissue =~ /mixed/ || 
                     $tissue =~ /pooled/ || $tissue =~ /mixture/ ||
                     $tissue =~ /whole/  || $tissue =~ /unknown/ ||
                     $tissue =~ /entire/ || $tissue =~ /multi\-/ ||
                     $tissue =~ / uncharacterized / || 
                     $tissue =~ / different / || 
                     $tissue =~ / of /        ||
                     $tissue =~ / or /        ||
                     $tissue =~ / from /      ||
                     $tissue =~ / without /   ||
                     $tissue eq 'adult');
            # Do not consider mixtures:
            next if ($tissue =~ /\,/ || $tissue =~ / and /);
            # Delete parenthetical comments
            $tissue =~ s/\([^\)]+\)//g;
            # Do not bother with weird identifiers:
            next if ($tissue =~ /[^a-z0-9_\.\- ]/);
            # Clean up spaces:
            $tissue =~ s/\s+$//;  $tissue =~ s/^\s+//; 
            $tissue =~ s/\s+/ /g; $tissue =~ s/\_/ /g;
            # Ignore long descriptions:
            my $textlen = length($tissue);
            if ($textlen > 25) {
                # Do not keep long descriptions unless 3 words or less
                my @words = split(/\s/, $tissue);
                next if ($#words > 2);
            }
            # Run away from small words:
            next if ($textlen < 3);
            $tissues->{$tissue} ||= {};
            $tissues->{$tissue}{$ugID} = 1;
        }
    }
}

sub process_tissues {
    my ($file) = @_;
    $args->msg("Recording Tissue distributions", $file);
    my %tissues;
    open(TISS, "<$file") || die "Failed to read '$file':\n  $!\n  ";
    while (<TISS>) {
        chomp;
        my ($tis, $id) = split(/\t/, $_);
        $tissues{$tis}{$id} = 1;
    }
    close TISS;
    my @tissues = sort { length($a) <=> length($b) ||
                             $a cmp $b } keys %tissues;
    foreach my $tissue (@tissues) {
        next unless ($tissues{$tissue});
        my @plurals = ($tissue . "s");
        if ($tissue =~ /y$/) {
            # extremities
            my $plural = $tissue; chop $plural; $plural .= 'ies';
            push @plurals, $plural;
        } elsif ($tissue =~ /a$/) {
            # papillae ->  papilla
            push @plurals, $tissue . "e";
        } elsif ($tissue =~ /us$/) {
            # uteri -> uterus
            my $plural = $tissue; chop $plural; chop $plural; $plural .= 'i';
            push @plurals, $plural;
        }
        if ($tissue =~ /s$/) {
            push @plurals, $tissue . "es";
        }
        foreach my $plural (@plurals) {
            if ($tissues{$plural}) {
                foreach my $ugID (keys %{$tissues{$plural}}) {
                    $tissues{$tissue}{$ugID} = 1;
                }
                delete $tissues{$plural};
            }
        }
    }

    &initialize('nofork');
    my $tfile = "Unigene_Tissues.txt";
    open(TFILE, ">$tfile") || die "Could not write to '$tfile':\n$! ";
    $args->msg("[<]", "Tissue summary in $tfile");
    print TFILE "Tissue\tCount\tSpecies\n";
    my $purgeOld = 1;
    foreach my $tissue (@tissues) {
        next unless ($tissues{$tissue});
        my @ids = sort keys %{$tissues{$tissue}};
        # Capitalize first letter
        substr($tissue, 0, 1) = uc(substr($tissue, 0, 1));
        my $mtTissue = "#FreeText#$tissue";
        $lh->set_class($mtTissue, 'organ');
        $lh->kill_class($tissue, 'organ') if ($purgeOld);
        my %spec;
        foreach my $ugID (@ids) {
            my $sp = &species_for_id($ugID);
            $spec{$sp}++ if ($sp);
            $lh->set_edge( -name1 => $ugID,
                           -name2 => $mtTissue,
                           -type  => 'derivedfrom');
            $lh->kill_edge( -name1 => $ugID,
                            -name2 => $tissue,
                            -type  => 'derivedfrom') if ($purgeOld);
        }
        my @specs = sort keys %spec;
        foreach my $sname (@specs) {
            $lh->set_taxa($mtTissue, $sname);
            $lh->kill_taxa($tissue, $sname) if ($purgeOld);
        }
        printf(TFILE "%s\t%d\t%s\n", $tissue, $#ids + 1, join(",", @specs));
    }
    close TFILE;
    &finish;
}

sub set_gene_info {
    my ($record) = @_;
    return if (!$record->{ID} || $#{$record->{ID}} != 0);
    my $ugID = $record->{ID}[0];
    my $tid  = $record->{species};
    if ($record->{TITLE} && $#{$record->{TITLE}} == 0) {
        my $title = $record->{TITLE}[0];
        if ($title) {
            $title = '#FREETEXT#' . $title;
            $lh->set_edge( -name1 => $ugID,
                           -name2 => $title, 
                           -type  => 'SHORTFOR');
            $lh->set_class($title, 'text');
        }
    }
    if ($record->{GENE}) {
        foreach my $gene (@{$record->{GENE}}) {
            $gene = '#GENESYMBOLS#' . $gene; # CASE SENSITIVE GENE SYMBOL
            $lh->set_edge( -name1 => $ugID,
                           -name2 => $gene,
                           -type  => 'RELIABLE');
            $lh->set_class( $gene, 'GENESYMBOL');
            $lh->set_taxa($gene, $tid);
        }
    }
}

sub tag_to_hash {
    my ($record, $pritag, $sep) = @_;
    my @values;
    return @values unless ($record->{$pritag});
    foreach my $line (@{$record->{$pritag}}) {
        my @parts = split(/\; /, $line);
        my %hash;
        foreach my $part (@parts) {
            my @bits = split(/\=/, $part);
            unless ($#bits == 1) {
                &msg($record, "Unexpected '=' content in '$part'", [$pritag]);
                return ();
            }
            my ($tag, $val) = @bits;
            if ($hash{$tag}) {
                &msg($record, "Multiple '$tag' values", [$pritag]);
                return ();
            }
            $hash{$tag} = $val;
        }
        push @values, \%hash;
    }
    return @values;
}

sub msg {
    my ($record, $txt, $detail) = @_;
    my $ugID = $record->{ID} ? join(" / ", @{$record->{ID}}) : "UNK";
    my $string = sprintf("  [%8s] %s\n", $ugID, $txt);
    if ($detail) {
        foreach my $key (@{$detail}) {
            $string .= sprintf("     %s:\n", $key);
            if ($record->{$key}) {
                foreach my $val (@{$record->{$key}}) {
                    $string .= sprintf("        %s:\n", $val);
                }
            } else {
                $string .= sprintf("        %s:\n", 'No entries!');
            }
        }
    }
    $fc->write_output('error', $string);
}

sub initialize {
    my $nofork = 1;
    $lh = BMS::MapTracker::LoadHelper->new
        ( -username => 'unigene',
          -userdesc => 'Data specified by NCBI Locus Link file',
          -testmode => $tm, );
    $lh->directory( $baseDir ) if ($baseDir);

    $mt = $lh->tracker;
    if ($nofork) {
        $lh->redirect( -stream => 'TEST', -file => ">>$testfile" )
            if ($testfile);
    } elsif ($fc) {
        if (my $fh = $fc->output_fh('TestFile')) {
            $lh->redirect( -stream => 'TEST', -fh => $fh );
        }
    } elsif ($testfile) {
        $lh->redirect( -stream => 'TEST', -file => ">>$testfile" );
    }

    my $gsns = $mt->make_namespace
        ( -name => "GeneSymbols",
          -desc => "Gene symbols, both official and otherwise",
          -sensitive => 1 );

    my $textns = $mt->make_namespace
        ( -name => "FreeText",
          -desc => "Free text information that is not expected to be ".
          "a reliable connection between other names",
          -sensitive => 0 );

    $tissues  = {};
    $acccount = {};
#    $pbg = BMS::MapTracker::PopulateByGenbank->new( -lh => $lh );
}

sub finish {
    $lh->write();
    foreach my $tissue (sort keys %{ $tissues }) {
        foreach my $ugID (sort keys %{$tissues->{$tissue}}) {
            $fc->write_output('tissue', "$tissue\t$ugID\n");
        }
    }
}
