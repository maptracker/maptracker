#!/stf/biobin/perl -w

BEGIN {
    # Allows usage of beta modules to be tested:
    my $prog = $0; my $dir = `pwd`;
    if ($prog =~ /working/ || $dir =~ /working/) {
	warn "\n\n *** This is Beta Software ***\n\n";
	require lib;
	import lib '/stf/biocgi/tilfordc/perllib';
    }    
}

my $VERSION = ' $Id$ ';

use strict;
use BMS::MapTracker::LoadHelper;
use BMS::FriendlySAX;
use BMS::ForkCritter;
use BMS::BmsArgumentParser;
#use Devel::Cycle;
#use Devel::Peek;

my $args = BMS::BmsArgumentParser->new
    ( -cache    => 10000,
      -mapdir   => undef,
      -limit    => 0,
      -nocgi    => 1,
      -testmode => 1,
      -directory => "/work5/tilfordc/uniprot/mirror",
      -outdir    => "/work5/tilfordc/uniprot",
      # -wget     => "/work5/tilfordc/WGET/homologene_cmd.sh",
      -verbose  => 1,
      -mode     => 'load basic kw pub onto com ',
      -basedir  => 'UniProt',
      -source   => "SP Trembl",
      -paramalias => {
          strand    => [qw(str)],
          limit     => [qw(lim)],
          verbose   => [qw(vb)],
          fork      => [qw(forknum numfork)],
          wget      => [qw(update)],
          testmode  => [qw(tm)],
          purge     => [qw(clean)],
          basedir   => [qw(loaddir)],
          xxxx => [qw()],
          xxxx => [qw()],
          xxxx => [qw()],
      });

$args->shell_coloring();

my $mfilt = "TAG = '#META_TAGS#Sequence Version' AND VAL = ";
my ($sec,$min,$hour,$mday,$mon,$year) = localtime(time); $year += 1900; $mon++;
my ($fc, $lh, $mtclass, $tnames, $keepTax, $keepAcc, %hackStack);

my $uniRE   = '^([OPQ]\d[A-Z\d]{3}\d|[A-NR-Z]\d[A-Z][A-Z\d]{2}\d)$';
my $dir     = $args->{DIRECTORY} || '.';  $dir =~ s/\/$//;
my $odir    = $args->{OUTDIR}    || $dir; $odir =~ s/\/$//;
my $limit   = $args->{LIMIT};
my $vb      = $args->val('verbose');
my $tm      = $args->val('testmode');
my $cache   = $args->{CACHE};
my $prog    = $args->{PROGRESS};
my $purge   = $args->val('purge');
my $source  = lc($args->val('source') || '');
my $mode    = lc($args->val('mode') || '');
my $basedir = $args->val('basedir');
my $dprefix = sprintf("%s/%4d_%02d_%02d", $odir, $year, $mon, $mday);

if ($args->val('wget')) {
    my $cmd = "/work5/tilfordc/WGET/uniprot_cmd.sh";
    $args->msg( "Mirroring via $cmd ... ".`date`) if ($vb);
    my $st = time;
    system($cmd);
    my $tt = time - $st;
    unless ($tt > 20) {
        $args->death("Apparently failed to mirror Uniprot data",
                     $cmd, "Took $tt seconds, should take ~4 minutes");
    }
    $args->msg( "  Done ".`date`) if ($vb);
} else {
    $args->msg("Using previously mirrored data (-update 0)") if ($vb);
    
}
$args->ignore_error("A code closure was detected");
# die;

my %what;
if ($mode =~ /dep/) {
    $mode = "Deprecations";
} elsif ($mode =~ /(kw|keywords?)\s*ont/) {
    $mode = "Keywords";
} elsif ($mode =~ /spec/ || $mode =~ /tax/) {
    $mode = "Taxa";
} elsif ($mode =~ /clean/ || $mode =~ /purge/) {
    $mode = "Purge";
} else {
    $what{basic} = 1 if ($mode =~ /basic/);
    $what{kw}    = 1 if ($mode =~ /(key|kw)/);
    $what{pub}   = 1 if ($mode =~ /pub/);
    $what{onto}  = 1 if ($mode =~ /onto/);
    $what{com}   = 1 if ($mode =~ /com/);
    $mode = "Load";
}

$args->msg( "Parsing mode: $mode" ) if ($vb);

if (my $treq = $args->val(qw(species taxa))) {
    my $mt = BMS::MapTracker->new();
    $keepTax = {};
    foreach my $t (split(/[\t\,]+/, $treq)) {
        my @tax = $mt->get_taxa($t);
        if ($#tax == 0) {
            my $tname = $tax[0]->name;
            $keepTax->{$tname}++;
            $args->msg("  Keeping species $tname");
        }
    }
}

if (my $areq = $args->{ACC} || $args->{ACCESSION} || $args->{ACCS}) {
    $keepAcc = {};
    map { $keepAcc->{$_} = 1} split(/[\t\,\s]+/, uc($areq));
}
my %output    = ( Messages => $dprefix . "_UniProt_Parse_Messages.txt");
my %mtClasses = ( sprot => 'Swiss-Prot', trembl => 'TrEMBL');

my $xrefDbs = {
    InterPro => '^IPR\d+$',
    Pfam     => '^PF\d+$',
    PROSITE  => '^PS\d+$',
    SMART    => '^SM\d+$',
    PRINTS   => '^PR\d+$',
    ProDom   => '^PD\d+$',
    PIRSF    => '^PIRSF\d+$',
    TIGRFAMs => '^TIGR\d+$',
    PANTHER  => '^PTHR\d+',
    MEROPS   => '^[A-Z]\d{2}\.(\d{3}|[A-Z]\d{2})$',
    HAMAP    => '^MF_\d+(_A|_B)?$',
    EchoBASE => '^EB\d+$',
    EcoGene  => '^EG\d+$',
};

&ACT_ON_ARGS();

sub ACT_ON_ARGS {
    if ($mode eq 'Keywords') {
        &PARSE_KEYWORDS();
        return;
    }

    $fc = BMS::ForkCritter->new
        ( -inputtype   => 'sax',
          -inputargs   => [ -tag => 'entry' ],
          -limit       => $limit,
          -progress    => $prog,
          -verbose     => $vb );
    $fc->init_method( \&initialize );
    $fc->finish_method( \&finalize );

    if ($mode eq 'Deprecations') {
        $fc->method( \&find_deprecations );
        %output = ( Deprecations => $dprefix . "_UniProt_Deprecations.txt" );
    } elsif ($mode eq 'Taxa') {
        $fc->method( \&make_taxa_sets );
        $output{"TaxSet"} = sprintf("%s_UniProt_taxa_sets.txt", $dprefix);
        $output{"Messages"} = sprintf("%s_UniProt_taxa_msg.txt", $dprefix);
        $source = "sptr";
    } elsif ($mode eq 'Purge') {
        $fc->method( \&purge );
    } else {
        $fc->method( \&parse );
    }

    if ($fc->finish_method) {
        foreach my $key ('TestFile') {
            my $file = $args->{uc($key)};
            next unless ($file);
            $output{$key} = "$file";
        }
    }

    while (my ($tag, $path) = each %output) {
        unlink($path);
    }

    my (@sources, @setFiles);
    push @sources, 'sprot' if ($source =~ /(sp|swiss|uni)/);
    push @sources, 'trembl' if ($source =~ /tr/);

    foreach my $type (@sources) {
        my $path  = sprintf("%s/uniprot_%s.xml.gz", $dir, $type);
        $mtclass  = $mtClasses{$type};
        &PARSE_MAIN($path);
    }
    if ($mode eq 'Deprecations') {
        my $outf = $output{Deprecations};
        my %done;
        if ($outf) {
            if (-e $outf) {
                # Determine what is already DeprecatedFor:
                open(DONE, "<$outf") || die "Failed to read $outf :\n  $!\n  ";
                while (<DONE>) {
                    chomp;
                    my ($d, $acc) = split(/\t/);
                    $done{$acc}++;
                }
                close DONE;
            } else {
                $args->msg("[-]", "Earlier deprecation file not present",
                           $outf);
            }
            # Now scan the delacc* files:
        } else {
            $args->err("No ouput has been defined for deprecations");
        }
        foreach my $type (@sources) {
            $mtclass  = $mtClasses{$type};
            &DELETED_ACCESSIONS( $type, $outf, \%done );
        }
    }
    $mtclass = "";
    &build_taxa_sets() if ($mode eq 'Taxa');
    if ($vb) {
        my @bits = ("Output:");
        foreach my $key (sort keys %output) {
            my $path =  $output{$key};
            next unless (-e $path && -s $path);
            push @bits, sprintf("%20s : %s\n", $key,$path);
        }
        $args->msg( @bits );
    }
}

sub DELETED_ACCESSIONS {
    my ($type, $outf, $done) = @_;
    my $tok;
    if ($type eq 'sprot') {
        $tok = 'sp';
    } elsif ($type eq 'trembl') {
        $tok = 'tr';
    } else {
        die "Can not find deleted accessions for type '$type'\n  ";
    }
    my $path = sprintf("%s/docs/delac_%s.txt", $dir, $tok);
    die "The file $path does not exist\n  " unless (-e $path);

    $lh = BMS::MapTracker::LoadHelper->new
        ( -username => 'UniProt',
          -basedir  => $basedir,
          -testmode => $tm,);
    if (my $tf = $args->{TESTFILE}) {
        $lh->redirect( -stream => 'TEST', -file => ">>$tf");
    }

    $args->msg( "Deprecating $path") if ($vb);
    my $count = 0;
    open (OUT, ">>$outf") || die "Can not append to $outf :\n  $!\n  ";
    open (IN, "<$path") || die "Can not read from $path :\n  $!\n  ";
    my %both;
    while (<IN>) {
        s/[\n\r]+$//;
        my $acc = $_;
        next unless ($acc =~ /$uniRE/);
        if ($done->{$acc}) {
            # Already DeprecatedFor
            $both{$acc}++;
            next;
        }
        &flag_deprecated( $acc );
        print OUT "UniProt\tDeprecated\t$acc\t\n";
        $lh->write_threshold_quick( $cache );
        $count++;
        last if ($limit && $count >= $limit);
    }
    close IN;
    close OUT;
    $lh->write();
    undef $lh;
    $args->msg("  $count accessions deprecated") if ($vb);
    my @dd = sort keys %both;
    if ($#dd > -1) {
        $args->msg("Some accessions were both Deprecated and Deprecated For:",
                   @dd);
    }
}

sub PARSE_MAIN {
    my ($path) = @_;
    unless (-e $path) {
        $args->err("Failed to find XML file", $path);
        return;
    }
    $args->msg("Parsing $path") if ($vb);

    $fc->reset();
    $fc->input($path);
    while (my ($tag, $path) = each %output) {
        $fc->output_file( $tag, ">>$path" );
    }
    my $failed = $fc->execute( $args->{FORK} );
    if ($failed) {
        die "$failed processes did not execute properly\n";
    }
}

sub PARSE_KEYWORDS {
    my $path  = sprintf("%s/docs/keydef.xml.gz", $dir);
    unless (-e $path) {
        $args->err("Failed to find keyword deffinitions file",
                   $path);
        return;
    }
    my $textmeth = sub {
        my ($arr) = @_;
        # Remove leading and trailing whitespace:
        map { s/\s+$//; s/^\s+//; } @{$arr};
        # Keep all bits that are not empty:
        my @lines; map { push @lines, $_ if ($_) } @{$arr};
        # Join with spaces
        return join(' ', @lines);
    };

    %output = ( Messages => $dprefix . "_UniProt_Keyword_Messages.txt");
    $fc = BMS::ForkCritter->new
        ( -inputtype   => 'sax',
          -inputargs   => [ -tag => 'keyword', -textmeth => $textmeth ],
          -limit       => $limit,
          -progress    => $prog,
          -verbose     => $vb );

    $fc->input($path);
    $fc->init_method( \&initialize );
    $fc->method( \&parse_keywords );
    $fc->finish_method( \&finalize );
    my $failed = $fc->execute( $args->{FORK} );
    if ($failed) {
        die "$failed processes did not execute properly\n";
    }
}

sub parse_keywords {
    my ($hash) = @_;
    my $id = $hash->{ATTR}{id} || '';
    unless ($id =~ /^KW\-\d{4}$/) {
        &msg("Malformed keyword accession", $id);
        return;
    }
    $lh->set_class($id, 'keyword');
    foreach my $name (&c_l($hash, 'name')) {
        $lh->set_edge( -name1 => $id,
                       -name2 => "#FreeText#$name",
                       -type  => 'is a shorter term for' );
    }
    foreach my $desc (&c_l($hash, 'description')) {
        $lh->set_edge( -name1 => $id,
                       -name2 => "#FreeText#$desc",
                       -type  => 'has comment' );
    }
    
    $lh->kill_edge( -name1 => $id,
                    -type  => 'is mapped to' );
    
    $lh->kill_edge( -name1 => $id,
                    -auth  => 'swiss-prot',
                    -type  => 'is mapped to' ) if ($purge);
    foreach my $go (@{$hash->{BYTAG}{go} || []}) {
        my $gid = $go->{ATTR}{id} || '';
        if ($gid =~ /^GO\:\d{7}$/) {
            $lh->set_edge( -name1 => $id,
                           -name2 => $gid,
                           -type  => 'is mapped to' );            
        } else {
            &msg("Malformed keyword GO ID", $id, $gid);
        }
    }
}

sub make_taxa_sets {
    my ($hash) = @_;
    my @accs = &c_l( $hash, 'accession');
    if ($#accs < 0) {
        &msg("No accession found");
        return;
    }
    my $acc = shift @accs;
    return undef unless (&set_taxa( $hash ));
    map {$fc->write_output('TaxSet', "$_\t$acc\t$mtclass\n")} @{$hash->{taxa}};
}

sub build_taxa_sets {
    my $path = $output{"TaxSet"} || '-UNDEFINED-';
    unless (-e $path) {
        $args->err( "Failed to find Taxa Set file", $path);
        return;
    }
    my $fsize = (-s $path);
    my $tpath = "/scratch/uniprot_taxa_sort";
    $args->msg( "Sorting taxa data... ".`date`) if ($vb);
    system("sort $path > $tpath");
    my $tsize = (-s $tpath);
    unless ($fsize == $tsize) {
        die "Sorted taxa set should be $fsize bytes but is $tsize\n  ";
    }
    $args->msg( "Copying taxa data... ".`date`) if ($vb);
    system("cp $tpath $path");
    $tsize = (-s $path);
    unless ($fsize == $tsize) {
        die "Copied sorted taxa set should be $fsize bytes but is $tsize\n  ";
    }

    my $lht = BMS::MapTracker::LoadHelper->new
        ( -username => 'UniProt', 
          -testmode => $tm,);
    if (my $tf = $args->{TESTFILE}) {
        $lht->redirect( -stream => 'TEST', -file => ">>$tf");
    }
    my $nowTax = '';
    my $data   = {};
    my %taxae;
    $args->msg( "Parsing taxa data... ".`date`) if ($vb);
    open (SETTAX, "<$path") || die "Failed to read '$path':\n  $!\n  ";
    while (<SETTAX>) {
        chomp;
        my ($taxa, $acc, $type) = split(/\t/);
        if ($taxa ne $nowTax) {
            $taxae{$nowTax} = &_write_tax_set($data, $nowTax, $lht);
            $nowTax = $taxa;
            $data = {};
        }
        map { $data->{$_}{$acc}++ } ($type, 'UniProt');
    }
    close SETTAX;
    $taxae{$nowTax} = &_write_tax_set($data, $nowTax, $lht);
    $lht->write();

=pod

    my $mt = $lht->tracker;
    foreach my $taxa (keys %taxae) {
        # We could build a taxa hierarchy here - don't want to do it now
    }

=cut

    $args->msg( "Sorted taxa data:",  $path) if ($vb);
}

sub _write_tax_set {
    my ($data, $taxa, $lht) = @_;
    while (my ($type, $accs) = each %{$data}) {
        my $set = "$taxa $type";
        $lht->set_class($set, 'Group');
        $lht->set_taxa($set, $taxa);
        # Clear group membership if no limit, or if we are in testmode:
        $lht->kill_edge( -name2 => $set,
                         -type  => 'is a member of' )
            if ($tm || !$limit);
        foreach my $acc (keys %{$accs}) {
            $lht->set_edge( -name1 => $acc,
                            -name2 => $set,
                            -type  => 'is a member of' );
        }
    }
    $lht->write_threshold_quick( $cache );
    return [ sort keys %{$data} ];
}

sub find_deprecations {
    my ($hash) = @_;
    my @accs = &c_l( $hash, 'accession');
    return if ($#accs < 1);
    my $acc = shift @accs;
    map { $fc->write_output('Deprecations',"UniProt\tDeprecatedFor\t$_\t$acc\n");
          &flag_deprecated( $_, $acc );
      } @accs;
    $lh->write_threshold_quick( $cache );
}

sub flag_deprecated {
    my ($acc, $liveAcc) = @_;
    $lh->set_class($acc, 'Deprecated');
    $lh->kill_edge( -name2 => $acc,
                    -type  => 'is a deprecated entry for');
    if ($liveAcc) {
        $lh->kill_edge( -name2 => $liveAcc,
                        -type  => 'is a deprecated entry for');
        $lh->set_edge( -name1 => $acc,
                       -name2 => $liveAcc,
                       -type  => 'is a deprecated entry for');
    }
    &set_universal( $acc );
}

sub purge {
    my ($hash) = @_;
    my @accs = &c_l( $hash, 'accession');
    if ($#accs < 0) {
        &msg("No accession found");
        return;
    }
    my $acc = shift @accs;
    
    my @killEdge = (
                    [ 'SIB',        'has attribute' ],
                    [ 'SIB',        'is a reliable alias for' ],
                    [ 'SIB',        'is a shorter term for' ],
                    [ 'SIB',        'is an alias for' ],
                    [ 'SIB',        'is an unversioned accession of'],
                    [ 'SIB',        'is referenced in' ],
                    [ 'SIB',        'is reliably aliased by' ],
                    [ 'Swiss-Prot', 'has attribute' ],
                    [ 'Swiss-Prot', 'is a member of' ],
                    [ 'Swiss-Prot', 'is a reliable alias for' ],
                    [ 'Swiss-Prot', 'is attributed to' ],
                    [ 'Swiss-Prot', 'is reliably aliased by' ],
                    [ 'tilfordc',   'is an unversioned accession of'],
                    );

    my %killClass = ( protein     => ['sib','swiss-prot', 'ncbi', 'locuslink'],
                      swissprot   => ['sib','swiss-prot'],
                      deprecated  => ['swiss-prot'],
                      sptr        => ['sib'],
                      unversioned => ['ncbi','sib', 'tilfordc'],
                      trembl      => ['sib'] );
    my @taxKill = qw(NCBI LocusLink SIB IPI Swiss-Prot);


    # There are also evidence codes recorded as authorities, ie GO_IEA
    foreach my $dat (@killEdge) {
        $lh->kill_edge( -name1 => $acc,
                        -auth  => $dat->[0],
                        -type  => $dat->[1], );
    }
    while (my ($class, $auths) = each %killClass) {
        map { $lh->kill_class( $acc, $class, $_ ) } @{$auths};
    }
    map { $lh->kill_taxa($acc, undef, $_) } @taxKill if ($#taxKill > -1);
    $lh->write_threshold_quick( $cache );
}

# <gene>
# <organism>
# <reference>
# <comment>
# <dbReference>
# <>

sub parse {
    my ($hash) = @_;


    return unless (&check_required( $hash ));
    # &hack_clean_description_case( $hash); return;

    &set_basic( $hash )    if ($what{basic});
    &set_keywords( $hash ) if ($what{kw});
    &set_pubs($hash)       if ($what{pub});
    &set_onto($hash)       if ($what{onto});
    &set_comments($hash)   if ($what{com});
    if (my $accV = $hash->{accv}) {
        # Add an edge to indicate that we've analyzed this sequence
        $lh->set_edge( -name1 => $hash->{acc},
                       -name2 => 'Uniprot Loader',
                       -tags  => [['Sequence Version', $accV, undef]],
                       -type  => 'was assayed with', );
    }
    # print BMS::FriendlySAX::node_to_text( $hash );
    $lh->write_threshold_quick( $cache );
}

sub check_required {
    my ($hash) = @_;
    my @accs = &c_l( $hash, 'accession');
    if ($#accs < 0) {
        &msg("No accession found");
        return undef;
    }
    my $acc = shift @accs;
    # If the user wants only specific accessions, return if no match:
    return undef if ($keepAcc && !$keepAcc->{uc($acc)});
    $hash->{acc} = $acc;
    $hash->{dep} = \@accs;
    # Require that the taxa(e) be identified:
    return undef unless (&set_taxa( $hash ));
    # Return the accession if everything checks out
    return $acc;
}


sub set_basic {
    my ($hash) = @_;
    my $acc = $hash->{acc};
    my $dep = $hash->{dep};
    $lh->set_class($acc, 'unversioned');
    my ($accV, $len) = &set_seq( $hash );
    if ($accV) {
        $lh->set_length($accV, $len);
        $lh->set_class($accV, 'versioned');
        $lh->set_edge( -name1 => $acc,
                       -name2 => $accV,
                       -type => 'is an unversioned accession of' );
    }

    map { &flag_deprecated( $_, $acc ) } @{$dep};
    my $name = &c_s_v($hash, 'name');
    my $taxa = $hash->{taxa};
    if ( $name ) {
        foreach my $type ('is a reliable alias for','is reliably aliased by') {
            $lh->set_edge( -name1 => $name,
                           -name2 => $acc,
                           -type => $type);
            foreach my $oacc (@{$dep}) {
                # dis-associate the name with the old accessions
                $lh->kill_edge( -name1 => $name,
                                -name2 => $oacc,
                                -type => $type);
            }
        }
    }
    map { &set_universal( $_, $taxa) } ($acc, $accV, $name, @{$dep});

    if (my $prots = $hash->{BYTAG}{protein}) {
        if ($#{$prots} > 0) {
            &msg("Multiple protein tags", $acc);
            return;
        }
        my @names = &c_l($prots->[0], 'name');
        if ($#names > -1) {
            my @oknames;
            # Can we find descriptions that are more than a single word?
            map { push @oknames, $_ if ($_ =~ / /) } @names;
            if ($#oknames < 0 ) {
                # Only single words, keep the longest:
                my ($longest) = sort { length($b) <=> length($a) } @names;
                @oknames = ($longest);
            }
            foreach my $desc (@oknames) {
                next if ($desc =~ /^\d+$/ || $desc eq 'NA');
                $desc = "#FreeText#$desc";
                $lh->set_edge( -name1 => $acc,
                               -name2 => $desc,
                               -type  => 'is a shorter term for' );
                $lh->set_class($desc, 'text');
            }
        }
    }
    my $xref = &dbxrefs( $hash, 1 );
    my %external;
    foreach my $type ('RefSeq', 'Ensembl') {
        if (my $pHash = $xref->{$type}) {
            while (my ($oid, $pHash) = each %{$pHash}) {
                if (my $psi = $pHash->{'protein sequence ID'}) {
                    # Ensembl stuff is structured at the transcript level
                    # Have to extract the protein ID as a property
                    my @ids = keys %{$psi};
                    if ($#ids == 0 && 
                        $ids[0] =~ /^ENS[A-Z]{0,3}\d+(\.\d+)?$/) {
                        $oid = $ids[0];
                    }
                }
                $oid =~ s/\.\d+$//;
                my $qid = $acc;
                if (my $mh = $pHash->{molecule}) {
                    my @mols = keys %{$mh};
                    if ($#mols == 0 && $mols[0] =~ /^\Q$acc\E\-\d+$/) {
                        # <molecule> is indicating a specific form, eg:
                        # NP_002724 = P54619-1
                        $qid = $mols[0];
                    }
                }
                $external{$oid} = $qid;
            }
        }
    }
    
    my %qids;
    while (my ($oid, $qid) = each %external) {
        $lh->set_edge( -name1 => $oid,
                       -name2 => $qid,
                       -type  => 'is similar to' );
        $qids{$qid}++;
    }
    map { $lh->kill_edge( -name1 => $_,
                          -type  => 'is similar to' ) } keys %qids;
}

sub dbxrefs {
    my ($node, $withProp) = @_;
    my %xrefs;
    my $refs = $node->{BYTAG}{dbReference} || [];
    foreach my $ref (@{$refs}) {
        my ($type, $id) = map { $ref->{ATTR}{$_} } qw(type id);
        next unless ($type && $id);
        my $props = $ref->{BYTAG}{property} || [];
        $xrefs{$type}{$id} ||= {};
        foreach my $prop (@{$props}) {
            my ($ptype, $val) = map { $prop->{ATTR}{$_} } qw(type value);
            $xrefs{$type}{$id}{$ptype}{$val}++ if ($ptype && $val);
        }
        # Need to accomodate <molecule> tags!
    }
    unless ($withProp) {
        while (my ($type, $xhash) = each %xrefs) {
            $xrefs{$type} = [ sort keys %{$xhash} ];
        }
    }
    return \%xrefs;
}

sub set_taxa {
    my ($hash) = @_;
    my $specs = $hash->{BYTAG}{organism} || [];
    my %names;
    foreach my $spec (@{$specs}) {
        # The names are not trustworthy!!!
        my $xrefs = &dbxrefs($spec);
        map { my $tn = &get_taxa_name($_); 
              $names{$tn}++ if ($tn) } @{$xrefs->{'NCBI Taxonomy'} || []};
    }
    $hash->{taxa} = [ keys %names ];
    if ($#{$hash->{taxa}} < 0) {
        &msg("No species defined", $hash->{acc});
        return undef;
    }
    return 1 unless ($keepTax);
    for my $i (0..$#{$hash->{taxa}}) {
        return 1 if ($keepTax->{ $hash->{taxa}[$i] });
    }
    return 0;
}

sub get_taxa_name {
    my ($tid) = @_;
    unless (defined $tnames->{$tid}) {
        if ($tid =~ /^\d+$/) {
            my ($taxa) = $lh->tracker->get_taxa($tid);
            $tnames->{$tid} = $taxa->id if ($taxa && $taxa->id);
        }
        $tnames->{$tid} ||= "";
    }
    return $tnames->{$tid};
}

sub set_comments {
    my ($hash) = @_;
    my $acc  = $hash->{acc};
    my $coms = $hash->{BYTAG}{comment};
    return unless ($coms);

    my @taxa = @{$hash->{taxa}};
    map { $lh->kill_edge( -name1 => $acc,
                          -type  => $_ ) }
    ('has comment', 'has member', 'physically interacts with');
    foreach my $com (@{$coms}) {
        my $tags   = &get_evidence( $hash, $com );
        my @texts  = (&c_l($com,'note'), &c_l($com,'text'));
        foreach my $attr qw(Type Status) {
            my $val = $com->{ATTR}{lc($attr)};
            push @{$tags}, [ $attr, "#META_VALUES#$val" ] if ($val);
        }
        foreach my $txt (@texts) {
            $lh->set_edge( -name1 => $acc,
                           -name2 => "#FreeText#$txt",
                           -type  => 'has comment',
                           -tags  => $tags );
        }
        foreach my $iso ( @{$com->{BYTAG}{isoform} || []} ) {
            # Even isoforms have deprecated IDs:
            my @deps = &c_l($iso,'id');
            my $iacc = shift @deps;
            map { &flag_deprecated( $_, $iacc) } @deps;
            &set_universal( $iacc );
            my @tags;
            map { push @tags,['Note',"#FreeText#$_",undef] } &c_l($iso,'note');
            $lh->set_edge( -name1 => $acc,
                           -name2 => $iacc,
                           -tags  => \@tags,
                           -type  => 'has member' );
            map { $lh->set_edge
                      ( -name1 => $iacc,
                        -name2 => "#FreeText#$_ Isoform",
                        -type  => 'is a shorter term for' )} &c_l($iso,'name');
        }
        if (my $ints = $com->{BYTAG}{interactant}) {
            my @tags;
            my $odiff = &c_s_v( $com, 'organismsDiffer');
            my $exnum = &c_s_v( $com, 'experiments');
            push @tags, [ 'Number of experiments', undef, $exnum ] if ($exnum);
            my @others;
            foreach my $int ( @{$ints} ) {
                my $other = &c_s_v( $int, 'id' );
                push @others, $other if ($other);
            }
            foreach my $oacc (@others) {
                $lh->set_edge( -name1 => $oacc,
                               -name2 => $acc,
                               -tags  => \@tags,
                               -type  => 'physically interacts with' );
            }
        }
    }
}

sub set_universal {
    my ($acc, $taxa) = @_;
    return unless ($acc);
    $lh->set_class($acc, 'protein');
    if ($mtclass) {
        $lh->set_class($acc, $mtclass);
    } else {
        $lh->set_class($acc, 'UniProt');
    }
    map { $lh->set_taxa($acc, $_) } @{$taxa} if ($taxa);
}

# <sequence>
sub set_seq {
    my ($hash) = @_;
    my $sns = $hash->{BYTAG}{sequence};
    my $acc = $hash->{acc};
    unless ($sns) {
        &msg("No sequence", $acc);
        return undef;
    }
    my $sn   = $sns->[0];
    my $data = $sn->{TEXT};
    my $vnum = $sn->{ATTR}{version};
    unless ($vnum) {
        &msg("No sequence version", $acc);
        return undef;
    }
    my $accV = "$acc.$vnum";
    my $len = length($data);
    if ($sn->{ATTR}{length} && $sn->{ATTR}{length} != $len) {
        &msg("Sequence length error", $acc, "$len != " . $sn->{ATTR}{length});
        return undef;
    }
    $hash->{accv} = $accV;
    return ($accV, $len);
}

sub evidence_to_pubmed {
    
}

sub set_pubs {
    my ($hash) = @_;
    my $acc  = $hash->{acc};
    my $refs = $hash->{BYTAG}{reference} || [];
    foreach my $ref (@{$refs}) {
        my $cits = $ref->{BYTAG}{citation} || [];
        foreach my $cit (@{$cits}) {
            my $dbxr  = &dbxrefs( $cit );
            my @pmids = @{$dbxr->{PubMed} || []};
            if ($#pmids == 0 && $pmids[0] =~ /^\d+$/) {
                my $pmid  = "PMID:$pmids[0]";
                $lh->set_edge( -name1 => $acc,
                               -name2 => $pmid,
                               -type  => 'is referenced in' );
                #if (my $title = &c_s_v($cit, 'title')) {
                #    $title =~ s/\.$//; # Trailing period
                #    $title =~ s/\s+/ /g; # space runs
                #    $lh->set_edge( -name1 => $pmid,
                #                   -name2 => "#FreeText#$title",
                #                   -auth  => 'pubmed',
                #                   -type  => 'is a shorter term for' );
                #}
            }
        }
    }
}

sub set_onto {
    my ($hash) = @_;
    my $acc   = $hash->{acc};
    my $xrefs = &dbxrefs($hash, 1);
    $lh->kill_edge( -name1 => $acc,
                    -type  => 'has attribute' );
    while (my ($goid, $props) = each %{$xrefs->{GO} || {}}) {
        my @tags;
        foreach my $ectxt (keys %{$props->{evidence} || {}}) {
            # $ec =~ s/\.//g; # 2014 - not sure what this used to do?
            if ($ectxt =~ /^([A-Z]+):(.+)/) {
                my ($ec, $src) = ($1, $2);
                push @tags, [ 'GO Evidence', "#Evidence_Codes#$ec" ];
            }
        }
        $lh->set_edge( -name1 => $acc,
                       -name2 => $goid,
                       -tags  => \@tags,
                       -type  => 'has attribute' );
    }
    foreach my $id (keys %{$xrefs->{EC} || {}}) {
        if ($id =~ /^EC (.+)$/) {
            $lh->set_edge( -name1 => $acc,
                           -name2 => "EC:$1",
                           -type  => 'has attribute' );
        } 
    }
    foreach my $id (keys %{$xrefs->{UniGene} || {}}) {
        if ($id =~ /^[A-Z][a-z]\.\d+$/) {
            $lh->set_edge( -name1 => $id,
                           -name2 => $acc,
                           -type  => 'is a cluster with sequence' );
        }
    }
    foreach my $db qw(MIM) {
        foreach my $id (keys %{$xrefs->{$db} || {}}) {
            if ($id =~ /\d+$/) {
                $lh->set_edge( -name1 => $acc,
                               -name2 => "$db:$id",
                               -type  => 'has attribute' );
            }
        }
    }
    
    while (my ($db, $re) = each %{$xrefDbs}) {
        foreach my $id (keys %{$xrefs->{$db} || {}}) {
            if ($id =~ /$re/) {
                $lh->set_edge( -name1 => $acc,
                               -name2 => $id,
                               -type  => 'has attribute' );
            } else {
                &msg("Malformed $db ID", $acc, $id);
            }
        }
    }
}

# <keyword>
sub set_keywords {
    my ($hash) = @_;
    my $kws = $hash->{BYTAG}{keyword};
    return unless ($kws);

    my $acc = $hash->{acc};

    foreach my $kwn (@{$kws}) {
        my $kw = $kwn->{ATTR}{id} || "";
        if ($kw =~ /^KW-\d+$/) {
            $lh->set_edge( -name1 => $acc,
                           -name2 => $kw,
                           -type  => 'has attribute',
                           -tags  => &get_evidence($hash, $kwn) );
        } elsif (!$kw) {
            &msg("No keyword ID", $acc);
        } else {
            &msg("Malformed keyword", $acc, $kw);
        }
    }
}

# <evidence>
sub parse_evidence {
    my ($hash) = @_;
    return $hash->{EVID} if ($hash->{EVID});
    my $evs = $hash->{BYTAG}{evidence} || [];
    my %evid;
    foreach my $ev (@{$evs}) {
        my ($cat, $type, $attr, $key) = 
            map { $ev->{ATTR}{$_} } qw(category type attribute key);
        next unless ($key && $attr);
        my @attrs = split(/\s*\,\s*/, $attr);
        my $tag;
        if ($cat eq 'import') {
            $tag = "Imported from $type";
        } elsif ($cat eq 'curator') {
            if ($type eq 'Similarity') {
                $tag = "Similar to";
            } elsif ($type =~ /^(Experimental|Opinion|Literature)$/) {
                $tag = "Referenced In";
            } elsif ($type eq 'Common knowledge') {
                # Whaaaa??? Must be an election year...
                # If it is 'common', surely you can find a reference...
                next;
            }
        } elsif ($cat eq 'program') {
            if ($type eq 'Rulebase') {
                $tag = "Rulebase rule";
            } else {
                $tag = "Automated processing by $type";
                @attrs = ( $attr );
            }
        }
        if ($tag) {
            foreach my $val (@attrs) {
                next if (!$val || $val eq '-');
                $val = "PMID:$1" if ($val =~ /^PubMed=(\d+)/i);
                push @{$evid{$key}}, [ $tag, $val ];
            }
        } else {
            &msg("Unknown evidence set", $hash->{acc}, 
                 $cat, $type, $attr, $key);
            # die BMS::FriendlySAX::node_to_text( $hash);
        }
    }
    return $hash->{EVID} = \%evid;
}

sub get_evidence {
    my ($hash, $node) = @_;
    my $ev = &parse_evidence( $hash );
    my @rv;
    foreach my $key (split(/\s+/, $node->{ATTR}{evidence} || '')) {
        push @rv, @{$ev->{$key || 'UNK'} || []};
    }
    return \@rv;
}

sub get_location {
    my ($node) = @_;
    my @locs;
    foreach my $ln (@{$node->{BYTAG}{location} || []}) {
        my ($start, $end);
        if (my $point = $ln->{BYTAG}{position}) {
            $start = $end = $point->[0]{ATTR}{position} if ($#{$point} == 0);
        } elsif (my $bg = $ln->{BYTAG}{begin}) {
            if (my $en = $ln->{BYTAG}{end}) {
                ($start, $end) = ($bg->[0]{ATTR}{position}, 
                                  $en->[0]{ATTR}{position})
                    if ($#{$bg} == 0 && $#{$en} == 0);
            }
        }
        if ($start && $end && $start <= $end) {
            push @locs, Bio::Location::Simple->new
                ( -start => $start,
                  -end   => $end,
                  -strand => 1);
        } else {
            $start ||= '?';
            $end   ||= '?';
            my $txt = BMS::FriendlySAX::node_to_text( $ln );
            $txt =~ s/\s*\n\s*/ \| /g;
            &msg("Malformed location", "$start,$end", $txt);
            return;
        }
    }
    return ($#locs == 0) ? $locs[0] : Bio::Location::Split->new
        ( -locations => \@locs );
}

# - # - # - # - # - # - # - # - # - # - # - # - # - # - # - # - # - #

sub c_s_v {
    # = Child Simple Value
    my ($node, $child) = @_;
    my $list = $node->{BYTAG}{$child};
    return undef if (!$list || $#{$list} != 0);
    return $list->[0]{TEXT};
}

sub c_l {
    # = Child List
    my ($node, $child) = @_;
    my $list = $node->{BYTAG}{$child} || [];
    return ( map { $_->{TEXT} } @{$list} );
}

sub msg {
    my $txt = join("\t", @_);
    $args->msg( $txt ) if ($vb);
    $fc->write_output('Messages', "$txt\n");
}


sub initialize {
    $lh = BMS::MapTracker::LoadHelper->new
        ( -username => 'UniProt', 
          -basedir  => $basedir,
          -testmode => $tm,);
    if (my $fh = $fc->output_fh('TESTFILE')) {
        $lh->redirect( -stream => 'TEST', -fh => $fh );
    }
    $tnames = {};
}

sub finalize {


    # find_cycle( { ForkCritter => $fc, LoadHelper => $lh, });




    $lh->write;
    if (0) {
        $args->msg( "Altering FreeText case for descriptions");
        my $mt = $lh->tracker;
        my $get  =$mt->dbi->prepare
            ( "SELECT seqname FROM seqname".
              " WHERE upper(seqname) = upper(?) AND space_id = 3");
        my $sth  = $mt->dbi->prepare
            ( "UPDATE seqname SET seqname = ?".
              " WHERE upper(seqname) = upper(?) AND space_id = 3");
        foreach my $name (sort keys %hackStack) {
            my $dbname = $get->get_single_value($name);
            next if (!$dbname || $dbname eq $name);
            $args->msg( sprintf("  %50s : %s\n", $name, $dbname));
            $sth->execute($name, $name) unless ($tm);
        }
    }
    if ($basedir) {
        $args->msg("Loading $basedir...") if ($vb && !$tm && $fc->child == 1);
        $lh->process_ready();
    }
}

sub hack_clean_description_case {
    my ($hash) = @_;
    foreach my $prot (@{$hash->{BYTAG}{protein} || []}) {
        my @names = &c_l($prot, 'name');
        foreach my $name (@names) {
            $hackStack{$name}++;
        }
    }
}
