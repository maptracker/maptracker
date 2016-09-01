#!/stf/biobin/perl -w
# $Id$ 

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

#  -testmode 0 -verbose 0 -email tilfordc@bms.com

use strict;
use BMS::ArgumentParser;
use BMS::MapTracker::LoadHelper;
use BMS::FriendlySAX;
use BMS::MapTracker::AccessDenorm;

my %ignoreEvidence = map { uc($_) => 1 }
qw(UNIPROT MGI RGD FB TR SGD CGD_REF SGD_REF DDB SP BIOSIS EMBL
   TIGR_REF TIGR_TBA1);

my %ignoreTerms = map { uc($_) => 1 }
qw(AZ CGD CGN DDB EBI FB GO GOA GR GeneDB_Pfalciparum GeneDB_spombe 
   MGD MGI PAMGO RGD SP SANGER TAIR TGD TIGR TRAIT UM-BBD 
   UM-BBD_pathwayID VIDA WB ZFIN HTPP);

my %synrefs = map { uc($_) => 1 }
qw(ISBN HTTP EC PMID xxxxGO);

my $happyDatabases = {
    map { $_ => 1 } qw(SGD UNIPROT)
};

my $userID = {
    'AGRICOLA_BIB'       => 'AGRICOLA',
    'AGRICOLA_IND'       => 'AGRICOLA',
    'AGRICOLA_NAL'       => 'AGRICOLA',
    'BIOCYC'             => 'biocyc',
    'BIOSIS'             => 'Derwent',
    'CGD'                => 'CGD',
    'CGDID'              => 'CGD',
    'CGD_LOCUS'          => 'CGD',
    'CGD_REF'            => 'CGD',
    'CHEBI'              => 'ChEBI',
    'CYGD'               => 'cygd',
    'DATE'               => '',
    'DDB'                => 'DictyBase',
    'DDB_GENE_NAME'      => 'DictyBase',
    'EBI'                => 'EBI',
    'EC'                 => 'enzyme commission',
    'EMBL'               => 'EMBL',
    'ENSEMBL'            => 'EBI',
    'FB'                 => 'flybase',
    'FLYBASE'            => 'flybase',
    'GENEDB_GMORSITANS'  => 'Sanger Institute',
    'GENEDB_LMAJOR'      => 'Sanger Institute',
    'GENEDB_PFALCIPARUM' => 'Sanger Institute',
    'GENEDB_SPOMBE'      => 'Sanger Institute',
    'GENEDB_TBRUCEI'     => 'Sanger Institute',
    'GO'                 => 'GeneOntology',
    'GOA'                => 'EBI',
    'GOC'                => 'GeneOntology',
    'GO_REF'             => 'GeneOntology',
    'GR'                 => 'Gramene',
    'GRAMENE.PROTEIN'    => 'Gramene',
    'GR_PROTEIN'         => 'Gramene',
    'GR_REF'             => 'Gramene',
    'H-INVDB'            => 'H-InvDB',
    'H-INVDB_CDNA'       => 'H-InvDB',
    'H-INVDB_LOCUS'      => 'H-InvDB',
    'HGNC'               => 'HGNC',
    'INRA'               => 'inra/cnrs',
    'INTERPRO'           => 'interpro',
    'ISBN'               => 'ISBN',
    'MGI'                => 'MGI',
    'NCBI'               => 'NCBI',
    'NCBI_GI'            => 'NCBI',
    'PFAM'               => 'PFAM',
    'PMID'               => 'PubMed',
    'RGD'                => 'RGD',
    'RILEY'              => 'riley',
    'SGD'                => 'SGD',
    'SGD_REF'            => 'SGD',
    'SIB'                => 'SIB',
    'SP'                 => 'SIB',
    'SWISS-PROT'         => 'SIB',
    'SWISSPROT'          => 'SIB',
    'TAIR'               => '',
    'TIGR'               => 'TIGR',
    'TIGR_ATH1'          => 'TIGR',
    'TIGR_CMR'           => 'TIGR',
    'TIGR_REF'           => 'TIGR',
    'TIGR_TBA1'          => 'TIGR',
    'TR'                 => 'SIB',
    'UMBER'              => 'UMBER',
    'UNIPROT'            => 'UniProt',
    'WB'                 => 'WormBase',
    'ZFIN'               => 'ZFIN',
    'TAIR'               => 'TAIR',
    ''                   => '',
};


# http://wiki.geneontology.org/index.php/Relation_composition#Updates_to_relations_involving_gene_products.2C_April_2011

my $standardizeDirection = {
    is_a         => 'is_a',
    has_subclass => 'is_a',
    part_of      => 'part_of',
    has_part     => 'part_of',
    negatively_regulates => 'negatively_regulates',
    positively_regulates => 'positively_regulates',
    regulates            => 'regulates',
    # capable_of ??
    # annotated_to ??
};
my @allowedDirs = keys %{$standardizeDirection};

my ($lastTime);

my $args = BMS::ArgumentParser->new
    ( -cache    => 20000,
      -nocgi       => $ENV{'HTTP_HOST'} ? 0 : 1,
      -dir      => '/work5/tilfordc/go',
      -update   => '/work5/tilfordc/WGET/go_cmd.sh',
      -noupdate => 0,
      # -nocgi    => 1,
      -limit    => 0,
      -testmode => 1,
      -testfile => "GO_Loader_Test_Data.txt",
      -noassoc  => 0,
      -noterm   => 0,
      -age      => 7,
      -basedir   => "GO",
      -errormail => 'charles.tilford@bms.com',
      -mode     => "Update MapTracker",
      -verbose  => 1, );

my $nocgi = $args->val(qw(nocgi));
my $limit = $args->{LIMIT};
my $tm    = $args->{TESTMODE};
my $tf    = $args->{TESTFILE};

my $cache = $args->{CACHE};
my $vb    = $args->val(qw(vb verbose)) || 0;
my $mode  = $args->{MODE};
my $baseDir = $args->val(qw(loaddir basedir));
my $doDump  = $args->val(qw(dump dodump));

if ($nocgi) {
    $args->shell_coloring();
} else {
    $args->set_mime( );
}
$args->msg_callback(0, 'global') unless ($vb);

my $lh  = BMS::MapTracker::LoadHelper->new
    ( -username => 'GeneOntology',
      -testfile => $tf,
      -testmode => $tm,
      -basedir  => $baseDir,
      );

my $mt = $lh->tracker;

my %collated;
#my $dhash = &getdate();
#my $dtag  = sprintf
#    ("%d_%s_%02d", $dhash->{year}, $dhash->{mon}, $dhash->{day} );

my $dtag = `date '+%Y_%m_%d'`; $dtag =~ s/[\n\r]+$//;
if ($mode =~ /genacc/i) {
    &update_genacc();
} else {
    &update_maptracker();
    if ($baseDir && !$tm) {
        if ($args->val(qw(noload))) {
            $args->msg("Complete. You now need to manually load the directory",
                       $baseDir);
        } else {
            $args->msg("Loading $baseDir");
            $lh->process_ready();
            $args->msg("Loading done");
        }
    }
}

sub update_genacc {
    my $msgfile = 
        "/work5/tilfordc/parse_reports/$dtag-DenormalizeGeneOntology.msg";

    open(MSGF, ">$msgfile") 
        || $args->death("Failed to write output",$msgfile,$!);

    $args->msg("Messages to $msgfile");
    &msg(`date`);
    &msg("Updating GENACC for GeneOntology terms: tables ".
         "conversion, description, parentage\n");
    &msg( $args->to_text() );


    my $ad = BMS::MapTracker::AccessDenorm->new
        ( -tracker => $mt );
    $ad->age( $args->{AGE} );
    my @all_go = $ad->convert
        ( -id => 'GeneOntology Term', -ns1 => 'NS', -ns2 => 'GO', -age => 0 );
    my $numfound = sprintf("\nIdentified %d discrete GO terms in MapTracker",
                           $#all_go + 1);
    my @sets = $ad->get_all_go_subsets();    
    &msg("\nSet membership updated for " . join(', ', @sets));
    &msg("  AccessDenorm::convert".
         "( -id => \$setName, -ns1 => 'SET', -ns2 => 'GO' )");
    &msg("  AccessDenorm::convert".
         "( -id => \$goid, -ns1 => 'GO', -ns2 => 'SET')");

    &msg( $numfound );
    $args->msg($numfound);
    &msg("  AccessDenorm::convert".
         "( -id => 'GeneOntology Term', -ns1 => 'NS', -ns2 => 'GO')");
    my $last = time;
    for my $i (0..$#all_go) {
        my $go = $all_go[$i];
        $ad->description( -id => $go, -ns => 'GO' );
        $ad->all_parents( -id => $go, -ns => 'GO' );
        if (time - $last > 60) {
            $args->msg("  ","Parsed ".($i+1)." ". $go);
            $last = time;
        }
    }

    &msg("Updated GO descriptions");
    &msg("  AccessDenorm::description( \$goid, 'GO' )");
    &msg("Updated GO parentage");
    &msg("  AccessDenorm::all_parents( \$goid, 'GO' )");
    warn "\nDone conversions, updating set membership\n" if ($vb);

    &msg(`date`);
    close MSGF;
    warn "Finished\n\n" if ($vb);
    my $msg = "GeneOntology data has been denormalized to GENACC.\n".
        "Report - $msgfile";
    &notify( $args->{EMAIL}, $msg . "\n" . $args->to_text() );
}

sub update_maptracker {
    my $msgfile = "/work5/tilfordc/parse_reports/$dtag-LoadGeneOntology.msg";

    open(MSGF, ">$msgfile") 
        || die "Failed to write output to '$msgfile':\n $!\n ";

    $mt->make_namespace
        ( -name => "OBO_REL",
          -desc => "OBO Relationship types, used by GeneOntology and others",
          -sensitive => 0 );


    warn "Messages to $msgfile\n" if ($vb);
    &msg(`date`);
    &msg( $args->to_text() );

    if ($tm && $tf) {
        my @msg = ("Test output redirected:", "less -S $tf");
        $args->msg(@msg) if ($vb <= 1);
        &msg(join("\n", @msg));
    }


    &parse( $args->{DIR}, $args->{UPDATE});
    &finalize();

    &msg(`date`);
    close MSGF;
    warn "Finished\n\n" if ($vb);
    my $msg = "GeneOntology data has been parsed.\nReport - $msgfile\n".
        $args->to_text();
    &notify( $args->{EMAIL}, $msg );
}

sub notify {
    my ($email, $msg) = @_;
    return unless ($email);
    my $cmd = qq(| Mail -s 'Parse Report : GeneOntology' $email);
    open (MAIL, $cmd) || die "Could not send mail to $email:\n$!";
    print MAIL `date` . "\n";
    print MAIL $msg if ($msg);
    close MAIL;
}

sub parse {
    my ($dir, $update) = @_;
    unless ($args->val(qw(nowget noupdate))) {
        $args->msg("Updating local GO data via $update") if ($vb);
        system($update);
        warn "  Done\n" if ($vb);
    }
    my %files = &find_files($dir);
    foreach my $file (sort keys %files) {
        my $info = $files{$file};
        my $data = $info->{data};
        if ($data eq 'assocdb') {
            &parse_associations( $file );
        } elsif ($data eq 'termdb') {
            &parse_terms( $file );
        } else {
            &msg("ALERT: Unknown GO file '$file'");
        }
    }
}

sub find_files {
    my ($dir) = @_;
    my %files;
    return %files unless (-d $dir);
    opendir(DIR, $dir) || die "Failed to open directory '$dir':\n  $!\n  ";
    foreach my $file (readdir DIR) {
        if ($file =~ /^go_(\d+)-([a-z]+)\.(rdf|obo)-xml\.gz$/) {
            my ($date, $data, $type) = ($1, $2, $3);
            push @{$files{$data}}, [ $file, $date, $type ];
        }
    }
    closedir DIR;
    my %newest;
    while (my ($data, $arr) = each %files) {
        my @sorted = sort { $b->[1] <=> $a->[1] } @{$arr};
        my ($file, $date, $type) = @{$sorted[0]};
        my $full = "$dir/$file";
        $newest{$full} = { data => $data,
                           date => $date,
                           type => $type };
    }
    # die $args->branch(\%newest);
    return %newest;
}

sub parse_associations {
    return if ($args->val(qw(noassoc skipassoc)));
    my ($file) = @_;
    &msg("Parsing $file");
    $lastTime = time;
    eval {
        my $fs = BMS::FriendlySAX->new
            ( -file   => $file,
              -tag    => 'go:term',
              -skip   => 'xxxx',
              -limit  => $limit,
              -final  => \&finalize,
              -method => \&parse_go_assoc,  );
    };
}

sub parse_go_assoc {
    my ($hash) = @_;
    if ($doDump) {
        print BMS::FriendlySAX::node_to_text( $hash );
        return;
    }
    my $idnodes = $hash->{BYTAG}{'go:accession'};
    return unless ($idnodes);
    my $id = $idnodes->[0]{TEXT};
    unless ($id =~ /^GO\:\d{7}$/) {
        &msg("ERROR: $id is not a recognized GO ID");
        return;
    }
    my $accnodes = $hash->{BYTAG}{'go:association'} || [];
    my %orphaned;
    my $total = $#{$accnodes} + 1;
    my $lost  = 0;
    foreach my $accn (@{$accnodes}) {
        if (time - $lastTime > 60) {
            $args->msg("  ","Parsing $id vs $accn");
            $lastTime = time;
        }
        my %tags;
        my $evnodes = $accn->{BYTAG}{'go:evidence'} || [];
        foreach my $evn (@{$evnodes}) {
            my $ec = $evn->{ATTR}{evidence_code};
            my $xnodes = $evn->{BYTAG}{'go:dbxref'} || [];
            foreach my $xn (@{$xnodes}) {
                my ($db, $ref) = &process_xref( $xn );
                next unless ($db && $ref);
                my $syn = "$db:$ref";
                my $ucdb = uc($db);
                my $auth = $userID->{$ucdb};
                unless ($auth) {
                    push @{$collated{$db}{'Unknown authority'}}, $ref
                        unless (defined $auth);
                    next;
                }
                my $tag;
                if (($ucdb eq 'FB'   && $ref =~ /FBrf\d+/)          ||
                    ($ucdb eq 'WB'   && $ref =~ /WBPaper\d+/)       ||
                    ($ucdb eq 'ZFIN' && $ref =~ /ZDB-PUB-\d+/)      ||
                    ($ucdb eq 'TAIR' && $ref =~ /^IPR\d+$/)         ||
                    ($ucdb eq 'PFAM' && $ref =~ /^PF\d+$/)          ||
                    ($ucdb eq 'AGRICOLA_IND' && $ref =~ /^IND\d+$/) ||
                    ($ucdb eq 'INTERPRO' && $ref =~ /^IPR\d+$/)
                    ) {
                    # Use the reference as is:
                    $tag = [ 'Referenced In', $ref ];
                } elsif ( $ref =~ /^\d+$/ &&
                          ( $ucdb eq 'PMID'  ||
                            $ucdb eq 'ISBN'  ||
                            $ucdb eq 'GR_REF' )
                          ) {
                    # The reference should be synthetic db:id
                    $tag = [ 'Referenced In', $syn ];
                } elsif ($ignoreEvidence{ $ucdb }) {
                    # Not sure why these are here...

                    # These entries contribute an authority to assign
                    # to the evidence code, but no other tags.

                } elsif (($ucdb eq 'GO_REF' && $ref eq 'nd') ||
                         ($ucdb eq 'GOC'    && $ref =~ /^unpub/) ) {
                    # Useless data - but keep the authority all the same
                } elsif ($ref =~ /^[A-Z][A-Z\d]{4}\d$/ &&
                          ( $ucdb =~ /^SWISS/ ||
                            $ucdb eq 'GRAMENE.PROTEIN') 
                         ){
                    # Swiss-prot ID
                    # What should I do with these?
                    #$tag = [ 'Referenced In', $ref ];
                } else {
                    push @{$collated{$db}{'DB for evidence'}}, $ref;
                }
                push @{$tags{$auth}}, $tag if ($tag);
                push @{$tags{$auth}}, 
                [ 'Go Evidence', '#Evidence_Codes#'. $ec ] if ($ec);
            }
        }

        my $genes   = $accn->{BYTAG}{'go:gene_product'} || [];
        my @targets;
        foreach my $gn (@{$genes}) {
            my $xnodes = $gn->{BYTAG}{'go:dbxref'} || [];
            foreach my $xn (@{$xnodes}) {
                my ($db, $ref) = &process_xref( $xn );
                next unless ($db && $ref);
                if ($ref =~ /^LOC\d+$/) {
                    push @{$collated{$db}{'Forbidding LocusLink assignments'}}, $ref;
                    next;
                }
                my $syn = "$db:$ref";
                my $ucdb = uc($db);
                if ($happyDatabases->{$ucdb}) {
                    # Always use these references as-is
                    push @targets, $ref;
                } elsif (($ucdb eq 'FB'   && $ref =~ /FBgn\d+/      ) ||
                         ($ucdb eq 'ZFIN' && $ref =~ /ZDB-GENE-\d+/ ) ||
                         ($ucdb eq 'MGI'  && $ref =~ /^MGI:\d+$/    ) ||
                         ($ucdb eq 'RGD'  && $ref =~ /^RGD:\d+$/    ) ||
                         ($ucdb eq 'CGD'  && $ref =~ /^CA[LF]\d+$/  ) ||
                         ($ucdb eq 'DDB'  && $ref =~ /^DDB\d+$/     ) ||
                         ($ucdb eq 'ASPGD'     && $ref =~ /^ASPL\d+$/   ) ||
                         ($ucdb eq 'TIGR_ATH1' && $ref =~ /^At\d+g\d+$/ ) ||
                         ($ucdb eq 'GR'  && $ref =~ /^[A-Z][A-Z0-9]{4}\d$/)){
                    # Use these references as-is, provided they have an
                    # expected format
                    push @targets, $ref;
                } elsif ( $ucdb eq 'WB' && 
                          ( $ref =~ /WBGene\d+/ || $ref =~ /^CE\d+$/)
                          ) {
                    # More complex IDs
                    push @targets, $ref;
                } elsif ( $ucdb =~ /^GENEDB_/ ||
                          $ucdb =~ /^TIGR_/ ) {
                    # Entities that are captured as gene symbols
                    # (even though they do not look like it)
                    push @targets, "#GeneSymbols#$ref";
                } else {
                    push @{$collated{$db}{'DB for gene product'}}, $ref;
                }
            }
        }
        my @auths = sort keys %tags;
         $lost++ if ($#auths < 0);

        while (my ($auth, $tagarr) = each %tags) {
            foreach my $targ (@targets) {
                $lh->set_edge( -name1 => $id,
                               -name2 => $targ,
                               -type  => 'is attributed to',
                               -auth  => $auth,
                               -tags  => $tagarr, );
            }
        }
    }
    if ($total && $lost) {
        my $nns  = $hash->{BYTAG}{'go:name'};
        my $name = $nns ? $nns->[0]{TEXT} : 'unknown';
        my $msg  = sprintf(" Lost %5.1f%% [%d of %d] assignments for %s : %s",
                           100 * $lost / $total, $lost, $total, $id, $name);
        &msg($msg);
    }
    
    $lh->write_threshold_quick( $cache );
}

sub process_xref {
    my ($xn) = @_;
    my ($db, $ref) = ( $xn->{BYTAG}{'go:database_symbol'},
                       $xn->{BYTAG}{'go:reference'} );
    $db  = $db  ? $db->[0]{TEXT} : '';
    $ref = $ref ? $ref->[0]{TEXT} : '';
    return ($db, $ref);
}

sub parse_terms {
    return if ($args->val(qw(noterm noterms noonto noontology)));
    my ($file) = @_;
    &msg("Parsing $file");
    eval {
        my $fs = BMS::FriendlySAX->new
            ( -file   => $file,
              -tag    => 'term',
              -skip   => 'xxxx',
              -limit  => $limit,
              -final  => \&finalize,
              -method => \&parse_go_term,  );
    };
}

sub parse_go_term {
    my ($hash) = @_;
    my $idnodes = $hash->{BYTAG}{id};
    return unless ($idnodes);
    my $id = $idnodes->[0]{TEXT};
    unless ($id =~ /^GO\:\d{7}$/) {
        &msg("ERROR: $id is not a recognized GO ID");
        return;
    }
    $lh->set_class( $id, 'go');
    $lh->kill_edge( -node1 => $id,
                    -type  => "is reliably aliased by",);

    # Identify alternate IDs
    my $aids = $hash->{BYTAG}{alt_id} || [];
    foreach my $anode (@{$aids}) {
        my $aid = $anode->{TEXT};
        unless ($aid =~ /^GO\:\d{7}$/) {
            next;
        }
        $lh->set_class( $aid, 'go');
        $lh->set_edge( -node1 => $id,
                       -node2 => $aid,
                       -type  => "is reliably aliased by",
                       );
    }

    # Identify synonyms
    my $synodes   = $hash->{BYTAG}{synonym} || [];
    my %shash;
    foreach my $synode (@{$synodes}) {
        my $scope = '#FreeText#' . ($synode->{ATTR}{scope} || 'unknown');
        my $txtnodes  = $synode->{BYTAG}{synonym_text};
        foreach my $tnode (@{$txtnodes}) {
            my $txt = '#FreeText#' . $tnode->{TEXT};
            $shash{$txt} = $scope;
        }
    }
    
    
    # Set the name
    my $namenodes = $hash->{BYTAG}{name};
    if ($namenodes) {
        $lh->kill_edge( -name1 => $id,
                        -type  => 'is a shorter term for' );
        foreach my $nnode (@{$namenodes}) {
            my $name = '#FreeText#' . $nnode->{TEXT};
            $lh->set_edge( -name1 => $id,
                           -name2 => $name,
                           -type  => 'is a shorter term for' );
            $lh->kill_edge( -name2 => $name,
                            -type  => 'is a lexical variant of', );
            while (my ($syn, $scope) = each %shash) {
                next if (uc($syn) eq uc($name));
                $lh->set_edge( -name1 => $syn,
                               -name2 => $name,
                               -type  => 'is a lexical variant of',
                               -tags  => [[ 'Scope', $scope, undef ]]);
            }
        }
    }

    # Set more detailed descriptions:
    my $defnodes = $hash->{BYTAG}{def};
    if ($defnodes) {
        foreach my $defnode (@{$defnodes}) {
            my $descnodes = $defnode->{BYTAG}{defstr} || [];
            my $xnodes    = $defnode->{BYTAG}{dbxref} || [];
            my @tags;
            foreach my $xnode (@{$xnodes}) {
                my ($db, $acc) = ( $xnode->{BYTAG}{dbname}, 
                                   $xnode->{BYTAG}{acc});
                next unless ($db && $acc);
                ($db, $acc) = ($db->[0]{TEXT}, $acc->[0]{TEXT});
                my $ucdb = uc($db);
                if ($synrefs{$ucdb}) {
                    my $syn = "$db:$acc";
                    push @tags,  [ 'Referenced In', $syn, undef ];
                    if ($ucdb eq 'HTTP') {
                        $lh->set_class($syn, 'hyperlink');
                    }
                } else {
                    push @{$collated{$db}{'Term description xref'}}, $acc
                        unless ($ignoreTerms{$ucdb})
                }
            }
            foreach my $descnode (@{$descnodes}) {
                my $desc = '#FREETEXT#' . $descnode->{TEXT};
                $lh->set_edge( -name1 => $id,
                               -name2 => $desc,
                               -type  => 'has comment',
                               -tags  => \@tags );
            }
        }
    }

    # Assign to subsets
    my $setnodes   = $hash->{BYTAG}{subset} || [];
    $lh->kill_edge( -name1 => $id,
                    -type  => 'is a member of' );
    foreach my $setn (@{$setnodes}) {
        my $set = $setn->{TEXT};
        $lh->set_edge( -name1 => $id,
                       -name2 => $set,
                       -type  => 'is a member of' );
    }

    # Map to other ontologies
    my $xontos   = $hash->{BYTAG}{xref_analog} || [];
    foreach my $xo (@{$xontos}) {
        my ($db, $acc) = ( $xo->{BYTAG}{dbname}, 
                           $xo->{BYTAG}{acc});
        next unless ($db && $acc);
        ($db, $acc) = ($db->[0]{TEXT}, $acc->[0]{TEXT});
        my $other = "$db:$acc";
        if (uc($db) eq 'WIKIPEDIA') {
            $other = "#Wikipedia#$acc";
        }
        $lh->set_edge( -name1 => $id,
                       -name2 => $other,
                       -type  => 'is mapped to',);
    }
    

    # Build hierarchy

    # CAREFUL - we are allowing bi-directional edges to go in. It
    # appears that GO is pretty conservative, and always has the edges
    # in a child of direction. If that changes, however, the following
    # kill_edge() could delete entries set by other IDs.

    $lh->kill_edge( -name1 => $id,
                    -type  => 'is a child of');
    $lh->kill_edge( -name1 => $id,
                    -type  => 'is a parent of');

    my %parentage;
    if (my $isas = $hash->{BYTAG}{is_a}) {
        foreach my $isa (@{$isas}) {
            $parentage{ $isa->{TEXT} } = 'is_a';
        }
    }
    my $rels = $hash->{BYTAG}{relationship} || [];
    foreach my $rel (@{$rels}) {
        my ($type, $to) = ( $rel->{BYTAG}{type}, $rel->{BYTAG}{to});
        next unless ($type && $to);
        $parentage{ $to->[0]{TEXT} } = $type->[0]{TEXT};
    }
    my @oids = sort keys %parentage;
    if ($#oids > -1) {

        foreach my $oid (@oids) {
            my $type = $parentage{$oid};
            my $dir  = $standardizeDirection->{ $type };
            unless ($dir) {
                push @{$collated{$type}{'Unknown relationship'}}, $id;
                next;
            }
            my ($first, $second) = ($dir eq $type) ? ($id, $oid) : ($oid, $id);
            $lh->set_edge( -name1 => $first,
                           -name2 => $second,
                           -type  => 'is a child of',
                           -tags  => [ ['GO Type', "#OBO_REL#$dir" ] ] );
        }
    }

    # Scan for comments (may need for deprecation)
    my @comments;
    my $comnodes = $hash->{BYTAG}{comment} || [];
    foreach my $cn (@{$comnodes}) {
        my $txt = $cn->{TEXT};
        $txt =~ s/\\\:/:/g;
        push @comments, $txt;
    }
    my $obso = $hash->{BYTAG}{is_obsolete};
    if ($obso && $obso->[0]{TEXT} eq '1') {
        # This term is obsolete
        $lh->set_class($id, 'deprecated');
        my @toupd;
        for my $i (0..$#comments) {
            if ($comments[$i] =~ /^(.+)\. To update annotations, (.+)$/) {
                $comments[$i] = $1;
                push @toupd, $2;
            }
        }
        my @tags;
        my %oids;
        foreach my $tupd (@toupd) {
            substr($tupd, 0, 1) = uc(substr($tupd, 0, 1));
            push @tags, [ 'To Update', $tupd, undef ];
            while ($tupd =~ /(GO\:\d{7})/) {
                my $oid = $1;
                $oids{ $oid } = 1;
                $tupd =~ s/\Q$oid\E//g;
            }
        }
        foreach my $oid (keys %oids) {
            $lh->set_edge( -name1 => $id,
                           -name2 => $oid,
                           -type  => 'is a deprecated entry for',
                           -tags  => \@tags );
        }
    } else {
        $lh->kill_class($id, 'deprecated');
    }

    # Add comments:
    foreach my $com (@comments) {
        $lh->set_edge( -name1 => $id,
                       -name2 => '#FREETEXT#' . $com,
                       -type  => 'has comment', );
    }
}

sub msg {
    my ($msg) = @_;
    print MSGF "$msg\n";
    $args->msg($msg) if ($vb > 1);
}

sub finalize {
    $lh->write();
    my @probs = sort keys %collated;
    if ($#probs > -1) {
        my $msg = "ALERT: Some identifiers were not recognized:\n";
        foreach my $id (@probs) {
            $msg .= "  $id:\n";
            foreach my $desc (sort keys %{$collated{$id}}) {
                my $arr    = $collated{$id}{$desc};
                my $tail   = $#{$arr};
                my @sample = $arr->[0];
                push @sample, $arr->[ int($tail / 2) ] if ($tail > 1);
                push @sample, $arr->[$tail] if ($tail > 0);
                my $examp = join(", ", @sample);
                $msg .= sprintf
                    ("    %s [%d] %s\n", $desc, $tail+1, $examp);
            }
        }
        &msg($msg);
    }
    %collated = ();

    $lh->process_ready() if (!$tm && $args->{COMMIT});

    
}


=pod

    <id>GO:0000074</id>

  <term>
    <id>GO:0000001</id>
    <name>mitochondrion inheritance</name>
    <namespace>biological_process</namespace>
    <def>
      <defstr>The distribution of mitochondria, including the mitochondrial genome, into daughter cells after mitosis or meiosis, mediated by interactions between mitochondria and the cytoskeleton.      <dbxref>
        <acc>10873824</acc>
        <dbname>PMID</dbname>
      </dbxref>
      <dbxref>
        <acc>11389764</acc>
        <dbname>PMID</dbname>
      </dbxref>
      <dbxref>
        <acc>mcc</acc>
        <dbname>SGD</dbname>
      </dbxref>
    </def>
    <is_a>GO:0048308</is_a>
    <is_a>GO:0048311</is_a>
  </term>

=cut
