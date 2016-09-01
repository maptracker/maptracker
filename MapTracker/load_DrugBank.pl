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

use strict;
use BMS::BmsArgumentParser;
use BMS::MapTracker::LoadHelper;
use BMS::FriendlySAX;
use BMS::MapTracker::AccessDenorm;
use BMS::MapTracker::SciTegicSmiles;
use BMS::ForkCritter;
use BMS::Utilities::ReportingUtilities;

my $args = BMS::BmsArgumentParser->new
    ( -cache    => 20000,
      -nocgi    => $ENV{'HTTP_HOST'} ? 0 : 1,
      -dir      => '',
      -nocgi    => 1,
      -limit    => 0,
      -testmode => 'hush',
      -wgetcmd  => "/work5/tilfordc/WGET/drugbank_cmd.sh",
      -datadir  => "/work5/tilfordc/WGET/www.drugbank.ca/public/downloads/current/",
      -basedir  => "DrugBank",
      -XXXtestfile => "DrugBank_Test_Loader_Output.txt",
      -age      => 7,
      -forkfile => 1,
      -progress => 300,
      -errormail  => 'charles.tilford@bms.com',
      -verbose  => 1, );


my $nocgi    = $args->val(qw(nocgi));
my $tm       = $args->{TESTMODE};
my $testFile = $args->{TESTFILE};
my $limit    = $args->{LIMIT};
my $keepType = lc($args->{TYPE} || '');
my $skipType = defined $args->{SKIPTYPE} ? lc($args->{SKIPTYPE}) : 'Biotech';
my $basedir  = $args->val(qw(loaddir basedir));
my $residual = $args->{RESIDUAL};
my $vb       = $args->{VERBOSE};
my $ru       = BMS::Utilities::ReportingUtilities->new();

$args->msg_callback( sub {} ) unless ($vb);
if ($nocgi) {
    $args->shell_coloring();  
} else {
    $args->set_mime( );
}
if (my $upd = $args->val(qw(wget update))) {
    my $cmd = $args->{WGETCMD};
    $args->death("The DrugBank website is no longer easily accessible by wget",
                 "Try looking at the below link to mannually download new data",
                 "");
    $args->msg("[".&nice_dt()."]","Mirroring via wget", $cmd);
    system($cmd);
    $args->msg("[".&nice_dt()."]","Done.");
    if ($upd =~ /only/i) {
        $args->msg("Finished");
        exit;
    }
} else {
    $args->msg("[!!]", "USING EXISTING DATA - NO UPDATE!",
               "Use -wget if you wish to update mirrored data.");
}
my %nullVal = map { $_ => 1 } ('Not Available');

my %simpleTag = map { $_ => 1 } qw
    (CAS_Registry_Number Chemical_Formula Primary_Accession_No
     Smiles_String_isomeric Chemical_IUPAC_Name HPRD_ID
     Creation_Date Generic_Name InChI_Identifier HGNC_ID
     KEGG_Compound_ID PharmGKB_ID PubChem_Compound_ID PubChem_Substance_ID Name
     State Update_Date LIMS_Drug_ID GenAtlas_ID PDB_ID
     GenBank_ID_Gene GenBank_ID_Protein Gene_Name SwissProt_ID ChEBI_ID KEGG_Drug_ID HET_ID);

my %skipTag = map {$_ => 1 } qw
    (GO_Classification Gene_Sequence Cellular_Location Chromosome_Location
     Essentiality General_Function SNPs Specific_Function Transmembrane_Regions
     Half_Life Experimental_LogP_Hydrophobicity Molecular_Weight_Avg Signals
     Molecular_Weight_Mono Predicted_LogP_Hydrophobicity General_References
     Smiles_String_canonical Predicted_Water_Solubility Molecular_Weight
     SwissProt_Name Synonyms Theoretical_pI Number_of_Residues GeneCard_ID
     Locus Pfam_Domain_Function Melting_Point Experimental_Water_Solubility
     Pathway Phase_1_Metabolizing_Enzyme_Sequence State Chemical_Formula 
     PDB_Experimental_ID Experimental_Logs pKa_Isoelectric_Point 
     Phase_1_Metabolizing_Enzyme Organisms_Affected Protein_Binding);

# NCC skip tags
map { $skipTag{$_} = 1 } qw
(NCC_ALIQUOT_CONC NCC_ALIQUOT_CONC_UNIT NCC_ALIQUOT_SOLVENT NCC_ALIQUOT_VOLUME NCC_ALIQUOT_VOLUME_UNIT NCC_ALIQUOT_WELL_ID NCC_ALOGP NCC_PARENT_AMW NCC_PARENT_EMW NCC_PURITY_DATE NCC_SAMPLE_PURITY NCC_SSSR NCC_STRUCTURE_REAL_AMW NCC_STRUCTURE_REAL_MF NCC_TPSA PUBCHEM_CID PUBCHEM_SID NCC_NUM_HDONORS NCC_NUM_HACCEPTORS NCC_ANALYTICAL_PURITY_METHOD NCC_NUM_ROTATABLEBONDS);

my %multipleTag = map { $_ => 1 } qw
(NCC_STRUCTURE_SYNONYMS NCC_ISM);

$skipTag{Protein_Sequence} = 1;
$skipTag{Dosage_Forms} = 1;

# Accessions that we should probably get from elsewhere:
map { $skipTag{$_} = 1 } qw
    ( PubChem_Compound_ID
      PubChem_Substance_ID
      Chemical_IUPAC_Name
      InChI_Identifier );
      # CAS_Registry_Number

my $escapeCodes = {
    '#8242'  => "'",
    '#8722'  => '-',
    'amp'    => '&',
    'apos'   => "'",
    'gt'     => '>',
    'lt'     => '<',
    'quot'   => '"',
    'micro'  => 'u',
    'plusmn' => '+/-',
    'minus'  => '-',
    'mdash'  => '-',
    'ndash'  => '-',
    'mu'     => 'u',
    'le'     => '<=',
    'ge'     => '>=',
    'laquo'  => '<<',
    'raquo'  => '>>',
    'deg'    => 'degrees',
    'rsquo'  => "'",
    'lsquo'  => "'",
    'prime'  => "'",
    'nbsp'   => " ",
    'infin'  => 'Infinite',
    'ldquo'  => '"',
    'rdquo'  => '"',
    'szlig'  => 'beta',
    'ouml'   => 'oe',
    'acirc'  => 'a',
    'iacute' => 'i',
    'times'  => 'x',
    'hellip' => '...',
    'trade'  => '(TM)',

    'alpha'  => 'alpha',
    'beta'   => 'beta',
    'gamma'  => 'gamma',
    'delta'  => 'delta',
    'kappa'  => 'kappa',
    'omega'  => 'Omega',

    'radic'  => '', # Radical sign
    'reg'    => '(R)', # Registered sign
};



my $parGroup;
my @dTags    = qw(Creation_Date Update_Date);
my @cTags    = qw(Absorption Biotransformation Toxicity Pharmacology Indication Description Mechanism_Of_Action);

my %parentNodes;


my ($sts, $lh);
my $user = $args->{USER};

my $datDir = "/work5/tilfordc/WGET/DrugBank/public/downloads/current";

if (my $sdf = $args->{SDF}) {
    my $meth;
    if ($sdf =~ /NCC/) {
        $meth = \&parse_ncc;
        $user ||= 'NIH';
        $parGroup = "#FreeText#NIH Clinical Collection";
    }
    $sts  = &get_sts();
    $lh   = $sts->loader;
    &parse_sdf($sdf, $meth)
} else {
    $user = 'DrugBank';
    $sts  = &get_sts();
    $lh   = $sts->loader;
    $parGroup = "#FreeText#DrugBank";
    my $dcf = "$datDir/drugcards.txt";
    unless (-e $dcf) {
        my $z = "$datDir/drugcards.zip";
        system("unzip -d $datDir $z") if (-e $z);
    }
    &parse_drugcards( $dcf );
}

$lh->set_class($parGroup, 'Group') if ($parGroup);

while (my ($kid, $par) = each %parentNodes) {
    map { $lh->set_class("#FreeText#$_", 'Group') } ($kid, $par);
    $lh->set_edge( -name1 => "#FreeText#$kid",
                   -name2 => "#FreeText#$par",
                   -type  => 'is a child of' );
}

$lh->write();

if ($basedir && !$tm) {
    $args->msg("Loading database from $basedir");
    $lh->process_ready()
}

$args->msg("[".&nice_dt()."]","All Finished");

sub parse_drugcards {
    my ($file) = @_;
    $args->msg("Parsing DrugCards", $file);
    open(FILE, "<$file") || 
        $args->death("Failed to read DrugCard File", $file, $!);
    my $record = {};
    my $count  = 0;
    my ($tag, @vals) = ('');
    while (<FILE>) {
        s/[\n\r]+//;
        next if (/^\s*$/);
        if (/^\#END_DRUGCARD (\S+)/) {
            $count += &parse_record($record, $1);
            $record = {};
            $tag    = '';
            @vals   = ();
            last if ($limit && $count >= $limit);
        } elsif (/\# (\S+)\:/) {
            if ($#vals != -1 && $tag && !$skipTag{$tag}) {
                if ($tag =~ /Drug_Target_(\d+)_(\S+)/) {
                    my ($ind, $subTag) = ($1, $2);
                    unless ($skipTag{$subTag}) {
                        my $subRec = $record->{Target}[$ind-1] ||= {};
                        if ($simpleTag{$subTag}) {
                            if ($#vals == 0) {
                                $subRec->{$subTag}= $vals[0];
                            } else {
                                $args->msg("Multiple values for $subTag", 
                                           join(' + ', @vals));
                            }
                        } else {
                            $subRec->{$subTag} = [ @vals ];
                        }
                    }
                } elsif ($tag =~ /^(\S+)_Link$/) {
                    my ($subTag) = ($1);
                    if ($#vals == 0) {
                        $record->{Links}{$subTag} = $vals[0];
                    } else {
                        $args->msg("Multiple links for $subTag", @vals); 
                    }
                } elsif ($simpleTag{$tag}) {
                    if ($#vals == 0) {
                        $record->{$tag} = $vals[0];
                    } else {
                        $args->msg("Multiple values for $tag", @vals); 
                    }
                } else {
                    $record->{$tag} = [ @vals ] ;
                }
            }
            $tag  = $1;
            @vals = ();
        } elsif (! $nullVal{$_} && $tag) {
            while (/\&([a-z]+)\;/) {
                my $code = $1;
                my $unesc = $escapeCodes->{$code};
                unless (defined $unesc) {
                    $args->msg("Unknown escape code: $code");
                    $unesc = 'UNK';
                }
                s/\&$code\;/$unesc/g;
            }
            while (/(\<\/?[a-z]+\/?\>)/) {
                my $html = $1;
                my $rep  = ($html eq '<br/>') ? ' ' : '';
                s/\Q$html\E/$rep/;
            }
            push @vals, $_;
        }
    }
    close FILE;
    $args->msg("Parsed $count cards");
}

sub parse_sdf {
    my ($file, $meth) = @_;
    $args->msg("[".&nice_dt()."]", "Parsing SDF file", $file);
    open(FILE, "<$file") || die "Failed to read '$file':\n  $!\n  ";
    my $record = {};
    my $count  = 0;
    my ($tag, @vals) = ('');
    while (<FILE>) {
        s/[\n\r]+//;
        next if (/^\s*$/);
        if (/^\${4}$/) {
            $count += &{$meth}($record);
            $record = {};
            $tag    = '';
            @vals   = ();
            last if ($limit && $count >= $limit);
        } elsif (/^\> \<(\S+)\>$/) {
            if ($#vals != -1 && $tag && !$skipTag{$tag}) {
                if ($multipleTag{$tag}) {
                    $record->{$tag} = [ @vals ] ;
                } else {
                    if ($#vals == 0) {
                        $record->{$tag} = $vals[0];
                    } else {
                        $args->msg("Multiple values for $tag", @vals); 
                    }
                }
            }
            $tag  = $1;
            @vals = ();
        } elsif (! $nullVal{$_} && $tag) {
            while (/\&([a-z]+)\;/) {
                my $code = $1;
                my $unesc = $escapeCodes->{$code};
                unless (defined $unesc) {
                    $args->msg("Unknown escape code: $code");
                    $unesc = 'UNK';
                }
                s/\&$code\;/$unesc/g;
            }
            while (/(\<\/?[a-z]+\/?\>)/) {
                my $html = $1;
                my $rep  = ($html eq '<br/>') ? ' ' : '';
                s/\Q$html\E/$rep/;
            }
            push @vals, $_;
        }
    }
    close FILE;
    $args->msg("  ", "Parsed $count records");
}

sub parse_ncc {
    my ($record) = @_;

    my $id = &extract($record, 'NCC_STRUCTURE_ID');
    unless ($id) {
        $args->msg("Record without ID");
        return 0;
    }

    my $smi = &extract($record, 'NCC_ISM');
    unless ($smi && $#{$smi} != -1) {
        $args->msg("No SMILES entry", $id);
        return 0 ;
    }

    $smi = join('', @{$smi});

    if ($args->{SMILES}) {
        print "$smi $id\n";
        return 1;
    }
    my $canSmi = $sts->canonical( $smi);
    if (!$canSmi || $#{$canSmi} == -1) {
        $args->msg("Failed to generate canonical SMILES", $id, $smi);
        return 0;
    } elsif ($#{$canSmi} > 0) {
        $args->msg("Multiple canonical SMILES", "$id = $smi", map { $_->[0] } @{$canSmi});
        return 0;
    }
    my $chemKey = "#SMILES#".$canSmi->[0][0];

    $lh->set_class($id, 'NCC');
    my @aliases = ( $id );

    if (my $samp = &extract($record, 'NCC_SAMPLE_ID')) {
        if ($samp =~ /^SAM\d+$/) {
            $lh->set_class($samp, 'NCC');
            push @aliases, $samp;
        } else {
            $args->msg("Unusual sample ID", $samp, $id);
        }
    }


    my @memTags;
    $lh->set_edge( -name1 => $chemKey,
                   -name2 => $parGroup,
                   -type  => 'is a member of',
                   -tags  => \@memTags );

    my $alis = &extract($record, 'NCC_STRUCTURE_SYNONYMS') || [];
    foreach my $ali (@{$alis}) {
        if ($ali =~ /^CPD\d+$/) {
            $lh->set_class($ali, 'NCC');
        } else {
            $ali = "#FreeText#$ali";
        }
        push @aliases, $ali;
    }

    foreach my $ali (@aliases) {
        $lh->set_edge( -name1 => $chemKey,
                       -name2 => $ali,
                       -type  => 'is a reliable alias for' );
        $lh->set_class($ali, 'Chemical') if ($ali =~ /^\#FreeText\#/i);
    }

    if (my $sup = &extract($record, 'NCC_SAMPLE_SUPPLIER')) {
        my $vend = "#FreeText#$sup";
        $lh->set_class($vend, 'COMPANY');
        $lh->add_edge
            ( -name1 => $vend,
              -name2 => $chemKey,
              -type  => 'sells' );
        
    }


    # $debug->branch($record);
    $lh->write_threshold_quick( 500 );
    return 1;
}

sub parse_record {
    my ($record, $idCheck) = @_;
    my $id = &extract($record, 'Primary_Accession_No');
    unless ($id eq $idCheck) {
        $args->msg("Failed to match ID to record end", $id, $idCheck);
        return 0;
    }
    my (%types, %groups);
    foreach my $cat qw(Category Type) {
        my $par = "DrugBank $cat";
        $parentNodes{"DrugBank $cat"} = "DrugBank";
        if (my $val = &extract($record, "Drug_$cat")) {
            my @vals = ref($val) ? @{$val} : split(/\s*\;\s*/, $val);
            foreach my $typ (@vals) {
                next unless ($typ);
                my $group = "DrugBank $typ";
                $parentNodes{$group} = "DrugBank $cat";
                $types{lc($typ)} = 1;
                $groups{$group}  = 1;
            }
        }
    }
    return 0 if ($keepType && !$types{$keepType});
    return 0 if ($skipType && $types{$skipType});

    my $chemKey;
    if (my $smi = &extract($record, 'Smiles_String_isomeric')) {
        if ($args->{SMILES}) {
            print "$smi $id\n";
            return 1;
        }
        my $canSmi = $sts->canonical( $smi);
        if (!$canSmi || $#{$canSmi} == -1) {
            $args->msg("Failed to generate canonical SMILES", $id, $smi);
            return 0;
        } elsif ($#{$canSmi} > 0) {
            $args->msg("Multiple canonical SMILES", "$id = $smi", map { $_->[0] } @{$canSmi});
            return 0;
        }
        $chemKey = "#SMILES#".$canSmi->[0][0];
    } elsif (my $seqLines = &extract($record, 'Chemical_Structure')) {
        my $head  = shift @{$seqLines};
        if ($head =~ /^>$id/) {
            my $sd = "";
            foreach my $line (@{$seqLines}) {
                next unless ($line);
                if ($line =~ /^\>/) {
                    $args->msg("[!!]", "Multiple fasta sequences for $id");
                    $sd = "";
                    last;
                }
                $line =~ s/\s+//;
                $sd .= $line;
            }
            $chemKey = $sd ? "#Sequence#$sd" : undef;
        } elsif ($head =~ /^>(.+)/) {
            $args->msg("[!!]","Ignoring non-standard sequence ID", $1);
        }
    }
    next if ($args->{SMILES});
    unless ($chemKey) {
        $args->msg('[X]',"No chemical key extracted for $id");
        return 0;
    }

    $lh->set_class($id, 'DrugBank');
    my @aliases = ( $id );

    foreach my $tag qw(Generic_Name Secondary_Accession_No Synonyms) {
        if (my $val = &extract( $record, $tag)) {
            push @aliases, map { "#FreeText#$_" } ref($val) ? 
                @{$val} : split(/\s*\;\s*/, $val);
        }
    }

    if (my $bns = &extract( $record, 'Brand_Names') ) {
        my @names = ref($bns) ? @{$bns} : split(/\s*\;\s*/, $bns);
        foreach my $bn (@names) {
            my ($name, $vend) = ($bn);
            if ($name =~ /(.+)\s+\((.+)\)\s*/) {
                ($name, $vend) = ($1, $2);
            }
            $name = "#FreeText#$name";
            push @aliases, $name;
            if ($vend) {
                $vend = "#FreeText#$vend";
                $lh->set_class($vend,'Company');
                $lh->set_edge( -name1 => $vend,
                               -name2 => $name,
                               -type  => 'is the owner of' );
            }
        }
    }

    my $refs = &extract( $record, 'Drug_Reference') || [];

    foreach my $targ (@{$record->{Target} || []}) {
        my $trefs = &extract( $targ, 'Drug_References') || [];
        push @{$refs}, @{$trefs};
        my $prot = &extract( $targ, 'SwissProt_ID');
        unless ($prot) {
            my $name = $targ->{Name} || '-Unknown-';
            unless ($name =~ /(RNA|DNA)/) {
                $args->msg("No SwissProt ID", "$id - $name");
            }
            next;
        }
        $lh->set_edge( -name1 => $chemKey,
                       -name2 => $prot,
                       -type  => 'was assayed against' );
    }
    delete $record->{Target};

    if (my $cas = $record->{CAS_Registry_Number}) {
        if ($cas =~ /^\d[\d-]+\d$/) {
            $lh->set_edge( -name1 => $chemKey,
                           -name2 => "CAS:$cas",
                           -type  => 'is a reliable alias for' );
        }
    }

    foreach my $ref (@{$refs}) {
        if ($ref =~ /^(\d+)/) {
            $lh->set_edge( -name1 => $chemKey,
                           -name2 => "PMID:$1",
                           -type  => 'is referenced in' );
            
        }
    }

    if (my $metab = &extract($record, 'Phase_1_Enzyme_SwissProt_ID')) {
        foreach my $sp (@{$metab}) {
            next if (!$sp || $sp =~ /\//);
                $lh->set_edge( -name1 => $chemKey,
                               -name2 => $sp,
                               -type  => 'is a substrate for' );
            
        }
    }

    my @coms;
    foreach my $tag (@cTags) {
        my $vals = &extract( $record, $tag);
        next unless ($vals);
        my $ttag = $tag;
        if ($ttag eq 'Description') {
            $ttag = "";
        } else {
            $ttag =~ s/_/ /g;
            $ttag .= ": ";
        }
        push @coms, map { $ttag.$_ } @{$vals};
    }

    my $links = &extract($record, 'Links') || {};
    while (my ($ltype, $link) = each %{$links}) {
        $link = "#File#$link";
        $lh->set_class($link, 'HyperLink');
        $lh->set_edge( -name1 => $chemKey,
                       -name2 => $link,
                       -type  => 'is referenced in' );
    }

    my @memTags;
    foreach my $tag (@dTags) {
        my $val = &extract( $record, $tag);
        next unless ($val);
        my $ttag = $tag;
        $ttag =~ s/_/ /g;
        if ($val =~ /^(\S+)/) { $val = $1 };
        push @memTags, [$ttag, $val, undef];
    }

    $lh->set_edge( -name1 => $chemKey,
                   -name2 => $parGroup,
                   -type  => 'is a member of',
                   -tags  => \@memTags );

    foreach my $group (keys %groups) {
        $lh->set_edge( -name1 => $chemKey,
                       -name2 => "#FreeText#$group",
                       -type  => 'is a member of', );
    }

    foreach my $ali (@aliases) {
        $lh->set_edge( -name1 => $chemKey,
                       -name2 => $ali,
                       -type  => 'is a reliable alias for' );
        $lh->set_class($ali, 'Chemical') if ($ali =~ /^\#FreeText\#/i);
    }

    map { $lh->set_edge( -name1 => $chemKey,
                         -name2 => "#FreeText#$_",
                         -type  => 'has comment' ) } @coms;


    $args->msg("[Residual]", $id, $args->branch($record)) if ($residual);
    $lh->write_threshold_quick( 500 );
    return 1;
}

sub extract {
    my ($rec, $tag) = @_;
    my $rv = $rec->{$tag};
    delete $rec->{$tag};
    return $rv;
}

sub get_sts {
    # Non-globals

    my $lh = BMS::MapTracker::LoadHelper->new
        ( -username  => $user,
          -basedir   => $basedir,
          -loadtoken => $user,
          -testmode  => $tm, );
    if ($testFile && $tm) {
        $lh->redirect( -stream => 'TEST', -file => ">>$testFile" );
    }

    my $sts = BMS::MapTracker::SciTegicSmiles->new( -loader => $lh );
    $sts->load_cache(500);
    $sts->{PC_CACHE} = [];
    return $sts;
}

sub nice_dt { return $ru->getdate("DAY MON TIME"); }
