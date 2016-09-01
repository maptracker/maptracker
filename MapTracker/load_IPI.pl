#!/stf/biobin/perl -w

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
    $| = 1;
    print '';
}

my $VERSION = ' $Id$ ';

=head1 SYNOPSIS

 load_IPI.pl -path /the/ipi/directory

 Analyzes the data provided by IPI (International Protein Index) and
 updates MapTracker with the information found within. The program
 also produces a modified Swiss-Prot file that can be loaded into
 SeqStore. The modification shuffles the version numbers around to
 allow unversioned and versioned accessions to be placed in the
 appropriate location.

 Currently all information is culled from the Swiss-Prot files
 provided by IPI. There are other files, but the information in them
 appears to be redundant to the SP files.

 The IPI website is at:

 http://www.ebi.ac.uk/IPI/IPIhelp.html

 The FTP site is:

 ftp://ftp.ebi.ac.uk/pub/databases/IPI/

=head1 OPTIONS

     -path Required. Default '/work5/tilfordc/ipi'. The directory
           that data should be read from. This is the folder that
           contains files of the format 'ipi.HUMAN.dat.gz',
           'ipi.HUMAN.IPC.gz', etc. The program is capable of reading
           the gzipped files.

 -testmode Default 1. If true, then no data will get written to the
           database. If any true value other than 'quiet', then the
           data that *would* have been written to the DB will be
           displayed as pseudo table dumps. Set to zero to write the
           data to MapTracker.

     -help Display this information. -h works also.

=cut

use strict;
use BMS::MapTracker::LoadHelper;
use BMS::BmsArgumentParser;
use BMS::ForkCritter;

my $code2spec = {
    ARATH => 'Arabidopsis thaliana',
    BOVIN => 'Bos taurus',
    BRARE => 'Brachydanio rerio',
    CHICK => 'Gallus gallus',
    DANRE => 'Danio rerio',
    HUMAN => 'Homo sapiens',
    MOUSE => 'Mus musculus',
    RAT   => 'Rattus norvegicus',
};


my $args = BMS::BmsArgumentParser->new
    ( -nocgi    => $ENV{'HTTP_HOST'} ? 0 : 1,
      -cache    => 20000,
      -dir      => "/work5/tilfordc/ipi",
      -dblink   => "IPI_Skipped_DBLinks.txt",
      -error    => "IPI_Parse_Errors.txt",
      -mode     => 'history data',
      -wgetcmd  => '/work5/tilfordc/WGET/ipi_cmd.sh',
      -depfile  => 'IPI_Deprecations.txt',
      -testmode => 1,
      -limit    => 0,
      -verbose  => 1,
      -progress => 180,
      -fork     => 1,
      );

# $args->ignore_error("Bio::Seq::accession is deprecated, use accession_number() instead");
my @edge_kill = ('is a reliable alias for', 'is reliably aliased by', 'is a cluster with sequence', 'has feature', 'has attribute', 'is a member of');

my $dbLinks = {

    # CLUSTER # Proteins we want to associate with IPI

    refseq_validated => {
        regexp    => '^[XNAY]P_(\d{6}|\d{9})$',
        cluster   => 1,
    },
    ensembl => {
        auth   => 'ensembl',
        regexp => '^ENS[A-Z]*P\d+$',
        # class  => ['protein', 'ensembl'],
        cluster => 1,
    },
    vega => {
        auth   => 'ensembl',
        regexp => '^OTT[A-Z]*P\d+$',
        class  => ['protein', 'ensembl'],
        cluster => 1,
    },
    uniprot => {
        auth      => 'uniprot',
        cluster   => 1,
    },
    mgi => {
        regexp    => '^MGI\:\d+$',
        auth      => 'mgi',
        cluster   => 1,
        symbolize => 1,
    },
    rgd => {
        regexp    => '^\d+$',
        auth      => 'rgd',
        symbolize => 1,
        edges     => [{  -name1 => '__IPI__',
                         -name2 => 'RGD:__PID__',
                         -type  => 'is a cluster with sequence' } ],
    },
    ccds => {
        url       => "http://www.ncbi.nlm.nih.gov/CCDS/",
        auth      => 'ncbi',
        cluster   => 1,
        class     => 'ccds',
    },
    uniparc => {
        regexp    => '^UPI[A-F0-9]+$',
        auth      => 'uniprot',
        class     => 'uniparc',
        cluster   => 1,
    },

    # DOMIANS # Features that describe motifs in the protein

    panther => {
        regexp    => '^PTHR\d+(\:SF\d+)?$',
        url       => "http://www.pantherdb.org/",
        domain    => 1,
        class     => 'panther',
        auth      => 'abi',
    },
    epd => {
        regexp    => '^EP\d+$',
        url       => "http://www.epd.isb-sib.ch/",
        domain    => 1,
        auth      => 'sib',
        class     => 'epd',
    },
    pirsf => {
        regexp    => '^PIRSF\d{6}$',
        url       => "http://pir.georgetown.edu/pirwww/dbinfo/pirsf.shtml",
        domain    => 1,
        auth      => 'pir',
        class     => 'pirsf',
    },
    interpro => { auth => 'interpro', domain => 1, },
    superfamily => { 
        auth => 'superfamily', domain => 1, class => 'superfamily',
    },
    transfac => { auth => 'biobase', domain => 1, },
    tigrfams => { auth => 'tigr', domain => 1, class => 'tigrfam' },
    pir      => { auth => 'pir', domain => 1 },

    # OTHER #

    hgnc => {
        symbolize => 1,
    },

    # IGNORE # DB_Links that we are not capturing at this time

    'h-invdb' => { ignore => "These are RNAs" },
    gene3d    => { ignore => "Maybe interesting - feature or ontology?" },
    genew     => { ignore => "Deal with from HGNC" },
    hugo      => { ignore => "Deal with from HGNC" },
    ignoreme  => { ignore => "" },
    locuslink => { ignore => "Deal with these associations with LL" },
    rzpd      => { ignore => "cDNA clones - not sure what to do with them" },
    unigene   => { ignore => "Do not associate with IPI" },
    utrdb     => { ignore => "Deal with from UTR DB itself" },
    zfin      => { ignore => "?" },
    trome     => { ignore => "Only web references are publications" },
    pathosign => { ignore => "Could not even get their site to work" },
    cleanex   => {
        url       => "http://www.cleanex.isb-sib.ch/",
        ignore    => "Gene expression database, does not seem interesting",
    },
};

# $dbLinks = {}; warn "FOO! Clobbered DB Links";

my $dbAlias = {
    'UniProtKB/Swiss-Prot' => 'uniprot',
    'UniProtKB/TrEMBL'     => 'uniprot',
    'swiss-prot'           => 'uniprot',
    ensembl_havana         => 'ensembl',
    mgd                    => 'mgi',
    pfam                   => 'interpro',
    prints                 => 'interpro',
    prodom                 => 'interpro',
    prosite                => 'interpro',
    refseq_inferred        => 'refseq_validated',
    refseq_model           => 'refseq_validated',
    refseq_predicted       => 'refseq_validated',
    refseq_provisional     => 'refseq_validated',
    refseq_reviewed        => 'refseq_validated',
    refseq_unknown_status  => 'refseq_validated',
    smart                  => 'interpro',
    trembl                 => 'uniprot',
};
while (my ($id, $alias) = each %{$dbAlias}) {
    $dbLinks->{lc($id)} = $dbLinks->{lc($alias)};
}

my $dir     = $args->{PATH} || $args->{DIRECTORY} || $args->{DIR}; 
my $vb      = $args->{VERBOSE};
my $tm      = $args->{TESTMODE};
my $limit   = $args->{LIMIT};
my $cache   = $args->{CACHE};
my $prog    = $args->{PROGRESS};
my $dblinkf = $args->{DBLINK};
my $mode    = lc($args->{MODE} || '');
$mode      .= 'history' if ($args->{HISTORY});
my $ft      = "#FreeText#";

my ($fc, $lh, $mt, $taxa, $keepTax, , $keepAcc, $writer, $novel, $only);

$dir =~ s/\/$//;

$args->msg_callback(0) unless ($vb);

if ($args->val(qw(update wget))) {
    my $wcmd = $args->{WGETCMD};
    $args->msg("Mirroring IPI via FTP", $wcmd, `date`);
    my $st = time;
    system($wcmd);
    my $tt = time - $st;
    unless ($tt > 20) {
        $args->death("Apparently failed to mirror IPI data",
                     $wcmd, "Took $tt seconds, should take ~5 minutes");
    }
    $args->msg("Finished", `date`);
} else {
    $args->msg("Using previously mirrored IPI data");
}

if (my $treq = $args->{SPECIES} || $args->{TAXA}) {
    my $mt = BMS::MapTracker->new();
    $keepTax = {};
    foreach my $t (split(/[\t\,]+/, $treq)) {
        my @tax = $mt->get_taxa($t);
        if ($#tax == 0) {
            my $tname = $tax[0]->name;
            $keepTax->{$tname}++;
        }
    }
    $args->msg("Keeping only specific taxae", sort keys %{$keepTax});
}
if (my $areq = $args->{ACC} || $args->{ACCESSION} || $args->{ACCS}) {
    $keepAcc = {};
    map { $keepAcc->{$_} = 1} split(/[\t\,\s]+/, uc($areq));
}
if (my $o = lc($args->{ONLY} || $args->{DOONLY} || '')) {
    $only = {};
    $only->{clear} = 1 if ($o =~ /clear/);
    $only->{fasta} = 1 if ($o =~ /fasta/ || $o =~ /seq/);
    $only->{build} = 1 if ($o =~ /build/);
    $only->{basic} = 1 if ($o =~ /basic/);
    $only->{links} = 1 if ($o =~ /link/);
    $only = { hacks => 1 } if ($o =~ /hack/);
    $dblinkf = "" unless ($only->{links});
}
$dblinkf = "" unless ($mode =~ /dat/i);

$fc = BMS::ForkCritter->new
    ( -limit       => $limit,
      -init_meth   => \&initialize,
      -finish_meth => \&finalize,
      -progress    => $prog,
      -verbose     => $vb );
my %output = ( TestFile => $args->{TESTFILE},
               Error    => $args->{ERR} || $args->{ERROR},
               DBLinks  => $dblinkf );
$output{Deprecation} = $args->{DEPFILE} if ($mode =~ /hist/);
while (my ($tok, $path) = each %output) {
    if ($path) {
        unlink($path);
        $args->msg("[>]",sprintf("%20s : %s", $tok, $path)) if ($vb);
    }
}


&parse_directory();

while (my ($tok, $path) = each %output) {
    if ($path && -s $path) {
        $args->msg("[>]",sprintf("%20s : %s", $tok, $path)) if ($vb);
    }
}


sub parse_directory {
    opendir(TMPDIR, $dir)
        || die "Failed to read contents of '$dir':  $!\n  ";
    my %targets;
    foreach my $file (readdir TMPDIR) {
        if ($file =~ /^ipi\.([A-Z]+)\.([^\.]+)/) {
            my ($code, $type) = ($1, $2);
            $taxa = $code2spec->{$code} || '';
            unless ($taxa) {
                $args->err("NO_STACK",
                           "Unable to parse file of unknown species",
                           $file);
                next;
            }
            next if ($keepTax && !$keepTax->{$taxa});
            push @{$targets{$type}}, "$dir/$file";
        }
    }
    closedir TMPDIR;

    my @types;
    push @types, 'history' if ($mode =~ /hist/);
    push @types, 'dat'     if ($mode =~ /dat/);
    foreach my $type (@types) {
        if ($type eq 'dat') {
            $fc->input_type('seq ipi');
            $fc->method(\&parse_uniprot);
        } elsif ($type eq 'history') {
            $fc->input_type('tsv head');
            $fc->method(\&parse_history);
        }
        foreach my $input (sort @{$targets{$type}}) {
            $args->msg("Parsing $input") if ($vb);
            $fc->input($input);
            while (my ($tok, $path) = each %output) {
                $fc->output_file( $tok, ">>$path" ) if ($path);
            }
            if (my $failed = $fc->execute( $args->{FORK} )) {
                $args->death("$failed processes did not execute properly");
            }
        }
    }
}

sub parse_history {
    my ($row) = @_;
    my ($oldid, $created, $deleted, $newid, $com) = map
    { $_ eq '-' ? '' : $_ } @{$row};
    return unless ($oldid);
    return if ($keepAcc && !($keepAcc->{$oldid} || $keepAcc->{$newid || ''}));

    $lh->write_threshold_quick( $cache );
    if ($deleted) {
        # This entry is now deprecated
        $lh->set_class($oldid, 'DEPRECATED');
        $lh->kill_edge( -name1 => $oldid,
                        -type  => 'is a member of', );
        $lh->kill_edge( -name1 => $oldid,
                        -type  => 'is a cluster with sequence', );
        # Make sure it has meta classes assigned to it
        $lh->set_class($oldid, 'CLUSTER');
        $lh->set_class($oldid, 'IPI');
        $fc->write_output('Deprecation', 
                          join("\t", 'IPI', 'Deprecated', $oldid,'')."\n")
            unless ($newid);
    } else {
        # The entry is NOT deprecated.
        $lh->kill_class($oldid, 'DEPRECATED');
    }
    return unless ($newid);

    my @tags;
    if ($deleted =~ /^v(\d+\.\d+)$/) {
        push @tags, [ 'IPI Deprecated Build', $taxa, $1 ];
    }
    if ($com) {
        $com =~ s/ \(P\)//;
        push @tags, [ 'Reason', "#META_VALUES#$com" ];
    }
    $fc->write_output('Deprecation', 
                      join("\t", 'IPI', 'DeprecatedFor', $oldid, $newid)."\n");
    $lh->set_edge( -name1 => $oldid, 
                   -name2 => $newid, 
                   -type  => 'is a deprecated entry for',
                   -tags  => \@tags, );
}

sub msg {
    my $txt = join("\t", @_)."\n";
    $fc->write_output('Error', $txt);
}

sub parse_uniprot {
    my ($seq) = @_;
    &get_taxid($seq);
    my ($ipiU, $ipiV) = ($seq->accession_number(), $seq->display_id);
    my $vnum;

    # Verify the nomenclature as looking reasonable:
    if ($ipiU =~ /^(\S+)\.\d+$/) {
        # Hmm. The unversioned looks like it is versioned
        # This is how IPI used to format their data
        $ipiV = $ipiU;
        $ipiU = $1;
    }
    if ($ipiV !~ /\.\d+$/) {
        # Hmm. $ipiV does not look versioned. We will not use it
        $ipiV = undef;
    } elsif ($ipiV =~ /^\Q$ipiU\E\.(\d+)$/) {
        $vnum = $1;
    } else {
        &msg($ipiU, "Versioned ID mismatch", $ipiV);
        return;
    }
    unless ($ipiU =~ /^IPI\d+$/) {
        &msg($ipiU, "Malformed ID");
        return;
    }
    return if ($keepAcc && !$keepAcc->{$ipiU});

    # Rearrange the accessions to match genbank standard
    $seq->accession($ipiU);
    if ($vnum) {
        $seq->display_id($ipiU . '.' . $vnum);
        $seq->seq_version($vnum);
    }
    &clear_old( $seq )       if (!$only || $only->{clear});
    &make_new_sp( $seq )     if (!$only || $only->{fasta});
    &process_builds( $seq )  if (!$only || $only->{build});
    &process_basic( $seq )   if (!$only || $only->{basic});
    &process_dblinks( $seq ) if (!$only || $only->{links});
    &hacks( $seq )           if ($only->{hacks});

    # Delete old associations that used 'sameas':
    # &clean_dat_seq($ipiU);
    $lh->write_threshold_quick( $cache );
}

sub clear_old {
    my ($seq) = @_;
    my $ipiU  = $seq->accession;
    $lh->kill_class( $ipiU, 'Deprecated');
    map { $lh->kill_edge( -name1 => $ipiU,
                          -type  => $_ ) } @edge_kill;
}

sub make_new_sp {
    # Make new Swiss-Prot files for Jansen
    my ($seq) = @_;
    return unless ($writer);
    unless ($seq->seq_version) {
        &msg( $seq->display_id, "Can't make SP file for unversioned ID");
        return;
    }
    # Write out the sequence to a file
    $writer->write_seq($seq);
}

sub process_basic {
    # Associate IPI numbers with each other - versioned to unversioned,
    # unversioned to deprecated past versions:
    my ($seq) = @_;
    my $ipiU  = $seq->accession;
    my $vnum  = $seq->seq_version;
    my $ipiV  = $vnum ? "$ipiU.$vnum" : undef;
    my $taxid = $seq->{TAXID};
    if ($ipiV) {
        $lh->set_class($ipiV, 'versioned');
        $lh->set_edge( -name1 => $ipiU, 
                       -name2 => $ipiV, 
                       -type  => 'is an unversioned accession of');
        if ( my $length = $seq->length) {
            $lh->set_length($ipiV, $length);
        }
    }
    $lh->set_class($ipiU, 'unversioned');
    if (my $desc = $seq->desc) {
        $lh->set_edge( -name1 => $ipiU,
                       -name2 => "$ft$desc",
                       -type  => 'shortfor' );
    }
    foreach my $term ($ipiU, $ipiV) {
        next unless ($term);
        $lh->set_class($term, 'ipi');
        $lh->set_class($term, 'protein');
        $lh->set_class($term, 'cluster');
        $lh->set_taxa($term, $taxid) if ($taxid);
    }

    # Associate with deprecated IPI identifiers
    foreach my $sacc ($seq->get_secondary_accessions) {
        $lh->set_class($sacc, 'DEPRECATED');
        $lh->set_class($sacc, 'CLUSTER');
        $lh->set_class($sacc, 'IPI');
        $lh->set_edge( -name1 => $sacc, 
                       -name2 => $ipiU, 
                       -type  => 'is a deprecated entry for');
        $lh->set_taxa($sacc, $taxid) if ($taxid);

        # Decided not to kill old edges - ENSEMBL IDs from the same
        # gene or transcript have no good mechanism to relate to each
        # other once they are altered, so we will leave the old links
        # in place in order to have SOME sort of association...

    }
}

sub process_builds {
    # Associate the IPI number with the release version
    my ($seq) = @_;
    my $ipiU  = $seq->accession;
    my $tname = $seq->{TAXNAME};
    my $build = "$tname IPI";
    my $annot = $seq->annotation();
    my @create = map {$_->display_text} $annot->get_Annotations("seq_create");
    my @update = map {$_->display_text} $annot->get_Annotations("seq_update");
    my @tags;
    if ($#create == 0) {
        push @tags, ["IPI Version History",
                     "#META_VALUES#Created", $create[0]];
    } elsif ($#create == -1) {
        &msg($ipiU, "Failed to find creation build");
    } else {
        &msg($ipiU, "Multiple creation builds found", @create);
    }
    foreach my $upd (@update) {
        push @tags, ["IPI Version History",
                     "#META_VALUES#Updated", $upd ];
    }
    $lh->set_edge( -name1 => $ipiU, 
                   -name2 => $build, 
                   -tags  => \@tags,
                   -type  => 'is a member of');
}

sub set_parent_build_objects {
    my ($build, $taxid, $set) = @_;
    $lh->set_class($build, 'IPIBUILD');
    $lh->set_class($build, 'versioned');
    $lh->set_taxa($build, $taxid) if ($taxid);
    $lh->set_edge( -name1 => $set,
                   -name2 => $build, 
                   -type  => 'unversioned');

    $lh->set_class($set, 'IPIBUILD');
    $lh->set_class($set, 'unversioned');
    if ($taxid) {
        $lh->set_taxa($set, $taxid);
    }
}

sub process_dblinks {
    my ($seq) = @_;
    # Associate the unversioned IPI with proteins, domains and ontologies:
    foreach my $dblink ( $seq->annotation->get_Annotations('dblink') ) {
        &process_dat_dblink( $seq, $dblink );
    }
}

sub process_dat_dblink {
    # This is where the IPI ID gets associated with a variety of
    # other protein identifiers, and ontology assignments
    my ($seq, $dblink) = @_;
    my $ipiU  = $seq->accession;
    my $taxid = $seq->{TAXID};

    my ($db, $pid, $oid, $com) = 
        map { defined $_ && $_ ne '-' ? $_ : '' }
    ( $dblink->database, $dblink->primary_id, 
      $dblink->optional_id, $dblink->comment );
    my $dset = $dbLinks->{ lc($db || '') };
    if (!$dset || $dset->{ignore}) {
        my ($keyA, $keyB) = $dset ? 
            ("Ignored DB Links", "$db : ".$dset->{ignore}) :
            ("New DB Links", $db);
        my $arr = $novel->{$keyA}{$keyB} ||= [];
        if ($#{$arr} + 1 < 3) {
            push @{$arr}, sprintf("%s [%s] %s", map { $_ || 'N/A' }
                                  ($pid, $oid, $com));
        }
        return;
    }

    if (my $check = $dset->{regexp}) {
        unless ($pid =~ /$check/) {
            &msg($ipiU, "Malformed DB Link ID", $db, $pid);
            return;
        }
    }
    my $dauth = $dset->{auth};
    my $map = {
        __IPI__  => $ipiU,
        __PID__  => $pid,
        __OID__  => $oid,
        __AUTH__ => $dauth,
    };
    my @edges = @{$dset->{edges} || []};
    # Some common edge assignments
    push @edges, ( {  -name1 => '__IPI__',
                      -name2 => '__PID__',
                      -type  => 'is a cluster with sequence' })
        if ($dset->{cluster});

    push @edges, ( {  -name1 => '__IPI__',
                      -name2 => '#GeneSymbols#__OID__',
                      -type  => 'is a reliable alias for' })
        if ($dset->{symbolize} && $oid ne $pid);

    push @edges, ( {  -name1 => '__IPI__',
                      -name2 => '__PID__',
                      -type  => 'has feature' })
        if ($dset->{domain});

    if (my $dclass = $dset->{class}) {
        my @list = ref($dclass) ? @{$dclass} : ($dclass);
        map { $lh->set_class($pid, $_, $dauth) } @list;
    }

    foreach my $eargs (@edges) {
        my @params;
        my $doIt = 1;
        while ( my ($key, $val) = each %{$eargs}) {
            while ($val =~ /(__[A-Z]+__)/) {
                my $tok = $1;
                my $rep = $map->{$tok} || '';
                $val =~ s/$tok/$rep/g;
                $doIt = 0 unless ($val);
            }
            push @params, ($key, $val);
        }
        $lh->set_edge( @params ) if ($doIt);
    }
}

sub get_taxid {
    my ($seq) = @_;
    my $spec = $seq->species;
    return undef unless ($spec);
    my $tax;
    if (my $id = $spec->ncbi_taxid) {
        # that was easy!
        ($tax) = $mt->get_taxa($id);
    } elsif ( my $bin  = $spec->binomial) {
        my @taxas = $mt->get_taxa( $bin );
        $tax = $taxas[0] if ($#taxas == 0);
    }
    if ($tax) {
        $seq->{TAXNAME} = $tax->name;
        $seq->{TAXID}   = $tax->id;
    }
    return $seq->{TAXID};
}

sub initialize {
    $lh = BMS::MapTracker::LoadHelper->new
        ( -username => 'IPI', 
          -testmode => $tm,);
    if (my $fh = $fc->output_fh('TESTFILE')) {
        $lh->redirect( -stream => 'TEST', -fh => $fh );
    }
    $mt = $lh->tracker;
    $novel = {};
}

sub finalize {
    $lh->write;
    # warn sprintf("child %d count %d", $fc->child,$fc->total_fork);
    if ($dblinkf) {
        foreach my $type (sort keys %{$novel}) {
            my $txt = sprintf("%s [%d]:\n", $type, $fc->child);
            foreach my $id (sort keys %{$novel->{$type}}) {
                $txt .= "  $id\n";
                map { $txt .= "    $_\n" } @{$novel->{$type}{$id}};
            }
            $fc->write_output('dblinks', $txt);
        }
    }
}

sub hacks {
    my ($seq) = @_;
    my $ipiU  = $seq->accession;
    my $vnum  = $seq->seq_version;
    my $ipiV  = $vnum ? "$ipiU.$vnum" : undef;
    foreach my $term ($ipiU, $ipiV) {
        next unless ($term);
        $lh->set_class($term, 'protein');
    }
}
