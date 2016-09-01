#!/stf/biobin/perl -w

BEGIN {
    # Needed to make my libraries available to Perl64:
    # use lib '/stf/biocgi/tilfordc/released';
    use lib '/stf/biocgi/tilfordc/patch_lib';
    # Allows usage of beta modules to be tested:
    my $prog = $0; my $dir = `pwd`;
    if ($prog =~ /working/ || $dir =~ /working/) {
        require lib;
        import lib '/stf/biocgi/tilfordc/perllib';
    }
    $| = 1;
    print '';
}

use strict;
use BMS::FriendlyDBI;
use BMS::MapTracker::LoadHelper;
use BMS::MapTracker::SciTegicSmiles;
use BMS::BmsArgumentParser;
use BMS::ForkCritter;

my $args = BMS::BmsArgumentParser->new
    ( -nocgi     => $ENV{HTTP_HOST} ? 0 : 1,
      -testmode  => 1,
      -ageall    => '20 May 2011',
      -dir       => '/home/tilfordc/chembl',
      -help      => undef,
      -clean     => 0,
      -limit     => 0,
      -progress  => 180,
      -cache     => 1000,
      -benchmark => 0,
      -basedir   => 'ChEMBL',
      -verbose   => 1 );

$args->shell_coloring();

my ( $lh, $mt, $dbh, $sts, $fc, $mode, $meth, %doneNote, %errs, %tally, $cntr,
     @objStack, $chemblFiles, $chemblVersion, $smiCache, %predicateCount );
my ($paLU, $atLU, $srcLU, $pmidLU);

my $baseDir   = $args->val(qw(loaddir basedir));
my $limit     = $args->val(qw(limit));
my $forknum   = $args->val(qw(fork)) || 1;
my $progTime  = $args->val(qw(progress)) || 300;
my $testFile  = $args->val(qw(testfile));
my $nocgi     = $args->val(qw(nocgi));
my $tm        = $testFile ? 1 : $args->val(qw(tm testmode));
my $vb        = $args->val(qw(vb verbose));
my $wombat    = "#None#WOMBAT";
my $ftns      = "#FreeText#";
my $wpns      = "#Wikipedia#";
my $dir       = $args->val(qw(dir));
my $cache     = $args->val(qw(cache));

my $user      = "ChEMBL";
my $chembl    = "#None#ChEMBL";
my $turtQuote = "TurtleQuote";
my $quoteChar = "TurtQuotChar";

my $canCmd    = "/usr/bin/nice -n19 /stf/biocgi/tilfordc/working/maptracker/MapTracker/canonicalize_smiles_file.pl";

my $remap = {
    'skos:prefLabel'     => 'LabelPref',
    'rdfs:label'         => 'HumanLabel', # These are human readable attributes
    'skos:altLabel'      => 'LabelAlt',

    'cco:substanceType'  => 'SubstanceType',
    'canonical_smiles'   => 'SMILES',
    'molformula'         => 'MolecularFormula',
    'full_molformula'    => 'MolecularFormula',
    'standard_inchi'     => 'InChI',
    'standard_inchi_key' => 'InChIKey',
    'owl:sameAs'         => 'SameAs',
    'cco:hasDocument'    => 'HasDocument',
    'rdfs:subClassOf'    => 'SubClass',
    'cco:highestDevelopmentPhase' => 'HighestDevPhase',
    'cco:moleculeXref'   => 'XrefMol',
    'bibo:pmid'          => 'PubMedUrl',

    'cco:assayType'      => 'AssayType',
    'cco:hasActivity'    => 'Activities',
    'cco:hasTarget'      => 'Targets',
    'cco:targetConfScore' => 'TargetConfidenceScore',
    'cco:targetConfDesc' => 'TargetConfidence',
    'dcterms:description' => 'Description',
    'cco:organismName'   => 'Species',
    'cco:taxonomy'       => 'Taxon',
    'cco:targetCmptXref' => 'XREFs',
    'cco:hasTargetComponent' => 'TargetComponents',
    'cco:targetType'     => 'TargetType',
    'dcterms:title'      => 'Title',
    'cco:hasMolecule'    => 'HasMolecule',
    'cco:standardRelation' => 'ActRel',
    'cco:standardType'     => 'ActType',
    'cco:standardUnits'    => 'ActUnits',
    'cco:standardValue'    => 'ActValue',
    'cco:activityComment' => 'Comment',
    'cco:dataValidityComment' => 'ValidityComment',
    'cco:dataValidityIssue' => 'ValidityIssue',
    'cco:potentialDuplicate' => 'PotentialDuplicate',
    'foaf:depiction'         => 'ImageURL',
    'mw_monoisotopic' => 'MolecularWeight',
    '' => '',
    '' => '',

    'cheminf:SIO_000008' => '',
    'bao:BAO_0000208' => '',
    'cco:publishedRelation' => '',
    'cco:publishedType' => '',
    'cco:publishedUnits' => '',
    'cco:publishedValue' => '',
    'cco:hasUnitOnto' => '',
    'cco:hasQUDT' => '',
    'cco:pChembl' => '',
    '' => '',
    '' => '',
    '' => '',
    '' => '',
};

my $molClass = {
    'Small molecule' => 'CHEMICAL',
    'Protein' => 'PROTEIN',
    'Enzyme' => '',
};

my $actSkipTxt = {
    (map { $_ => 1 } qw(ActRel ActType ActUnits ActValue HasMolecule Label) )
};

my $ignoredNS = {
    (map { $_ => 1 } qw(identifiers.org/taxonomy www.ncbi.nlm.nih.gov/Taxonomy) )
};


# Stuff I am not interested in:
map { $remap->{$_} = "" } qw 
    (cco:taxonomy cco:proteinSequence acd_logd acd_logp acd_most_apka acd_most_bpka alogp full_mwt hba hbd med_chem_friendly molecular_species num_ro5_violations mw_freebase psa ro3_pass rtb aromatic_rings heavy_atoms );


my $skipPred = {
    map { $_ => 1 } qw(HasDocument SameAs SubClass a)
};

if (my $v = $args->val('getvers')){
    &get_chembl_files( $v );
}
my $smiCacheMethod = \&act_on_cache;
if (my $path = $args->val('smilesfile')) {
    $smiCacheMethod = \&cache_to_file;
    &smiles_lookup( $path );
    exit;
}

&hack();
unless ($args->val(qw(loaddata))) { 
    &act_data();
} else {
    &chem_data();
}
&report_tally();

exit;


&initialize();
&get_assays();
&finalize();

foreach my $err (sort keys %errs) {
    my %u = map { $_ => 1 } @{$errs{$err}};
    &err( $err, sort keys %u);
}

sub report_tally {
    print &tally_text();
    %tally = ();
}

sub tally_text {
    my @tt = sort keys %tally;
    my $msg = "";
    foreach my $t (@tt) {
        if ($t =~ /^\#(.+)/) {
            $msg .= "$1\n";
            my @st = sort { uc($b) cmp uc($a)} keys %{$tally{$t}};
            foreach my $s (@st) {
                $msg .= sprintf("  %s : %s\n", $s, $tally{$t}{$s});
            }
            
        } else {
            $msg .= "$t\n";
            my @st = sort { $tally{$t}{$b} <=> $tally{$t}{$a} ||
                                uc($a) cmp uc($b)} keys %{$tally{$t}};
            foreach my $s (@st) {
                $msg .= sprintf("  %4d %s\n", $tally{$t}{$s}, $s);
            }
        }
    }
    return $msg;
}

sub tally_to_file {
    my ($file) = @_;
    return unless ($file);
    $file .= ".tally";
    open(TALLY,">$file") || $args->death
        ("Failed to write tally", $file, $!);
    print TALLY &tally_text();
    close TALLY;
    %tally = ();
}

sub act_data {
    my $act2T = &assay_lookup();
    my $id2pm = &pubmed_lookup();
    my $smiMT = &smiles_lookup();
    &init_lh();
    $args->msg("[<]","Loading activity data");
    my $fh    = &chembl_fh( 'activity' );
    my $num   = 0;
    my $lTime = time;
    my %ontos;
    my $issues = sprintf("%s/bms_%d_activity_issues.txt", $dir, $chemblVersion);
    $issues .= "-LIMIT" if ($limit);
    my @molars = qw(M mM uM nM);
    my $useful = {
        IC50       => \@molars,
        Kd         => \@molars,
        Ki         => \@molars, 
        EC50       => \@molars, 
        Inhibition => ['%'],

#        LC50       => \@molars,
#        MIC        => \@molars, 
#        AC50       => \@molars, 
#        ED50       => \@molars,
#        GI50       => \@molars,
    };
    foreach my $type (keys %{$useful}) {
        my %u = map { $_ => 1 } @{$useful->{$type}};
        $useful->{$type} = \%u;
    }
    my $etLU = {
        'Ki'           => 'inhibits',
        '% Inhibition' => 'inhibits',
        'Log Ki'       => 'inhibits',
        'ID50'         => '', # Infectious Dose
        'MIC'          => 'inhibits',
        'Ratio Ki'     => 'inhibits',
        'Inhibition'   => 'inhibits',
        'Inhibition zone' => 'inhibits',
        '% Inhibition rate' => 'inhibits',
        '' => '',
        '' => '',
        '' => '',
        '' => '',
        '' => '',
        '' => '',
        '' => '',
    };
    my $prePerc = {
        ( map { $_ => 1 } ("Inhibition")),
    };
    for (my $inhib = 5; $inhib <= 100; $inhib += 5) {
        # MIC = Minimum inhibitory concentration
        # GI  = Growth Inhibition
        # EII = Enzyme inhibition index
        foreach my $frm ('IC%d', 'EII%d', 'GI%d', 'logGI%d',
                         'MIC%d', 'IC%d ratio',
                         'Ratio IC%d') {
            $etLU->{sprintf($frm, $inhib)} = 'inhibits';
        }
    }
    my $milli = 10 ** 3;
    my $micro = 10 ** 6;
    my $nano  = 10 ** 9;
    my $unitMap = {
        "nM" => sub { return ('M', shift() / $nano) },
        "mM" => sub { return ('M', shift() / $milli) },
    };
    my $unitLess = {
        (map { uc($_) => 1 } qw(CFU pD2 Resistance Selectivity Survived Cures Slope)),
        (map { uc($_) => 1 } ("Hill coefficient", "Survivors at day 30")),
    };
    my $nullVals = {
        (map { uc($_) => 1 } ("Not Determined", "Not Tested", "No data", "NT", "Not Evaluated", "Not screened", "Not recorded", "ND", "NA", "N/A"))
    };

    my $notVals = {
        (map { uc($_) => 1 } ("Not Active", "Inactive", "No activity", "No inhibition", "Negative", "Not significant", "Nonpotent","no significant activity","Not detected","Not detectable"))
    };

    while (my $data = &next_turtle($fh, 'chembl_activity')) {
        # die $args->branch($data);
        my $chid    = $data->{subj};
        my $preds   = $data->{preds};
        my $type    = $preds->{ActType}  ? $preds->{ActType}[0]  || "" : "";
        my $units   = $preds->{ActUnits} ? $preds->{ActUnits}[0] || "" : "";
        my $val     = $preds->{ActValue} ? $preds->{ActValue}[0] || "" : "";
        my $rel     = $preds->{ActRel}   ? $preds->{ActRel}[0]   || "" : "";
        my $tdat;
        if ($chid =~ /^CHEMBL_ACT_(\d+)$/) {
            $tdat = $act2T->{$1};
        }
        unless ($tdat) {
            $tally{"Assay Utility"}{"Not useful: No link to assay"}++;
            $tally{"Processing Status"}{"Not Useful"}++;
            next;
        }

        my ($smiID, $utilityIssue);
        my $targ = $tdat->[3];
        if ($#{$targ} == 0) {
            $targ = $targ->[0];
        } elsif ($#{$targ} == -1) {
            $utilityIssue ||= "Not useful: No target";
            $targ = "";
        } elsif ($#{$targ} != 0) {
            $utilityIssue ||= "Not useful: Multiple targets";
            $targ = "";
        }

        my @hms  = @{$preds->{HasMolecule} || []};
        if ($hms[0] && $hms[0] =~ /^chembl_molecule:(CHEMBL\d+)$/) {
            $smiID = $smiMT->{ $1 };
        }
        unless ($smiID) {
            $utilityIssue ||= "Not useful: No compound";
        }

        if ($useful->{$type}{$units} && $targ) {
            # We should be good to record data!
        } elsif ($type) {
            $utilityIssue ||= "Not useful: Unhelpful assay";
        } else {
            $utilityIssue ||= "Not useful: No assay defined";
        }
        

        $tally{"Unit Assignments"}{$type}{$units ? $units : 'Unitless'}++;

        my @tags;
        push @tags, map { ["Referenced In", $_, undef] } @{$tdat->[2]};

        my ($com) = @{$preds->{Comment} || []};
        if ($com) {
            if ($nullVals->{uc($com)} || $notVals->{uc($com)}) {
                push @tags, ["Dissent", $ftns.$com, undef];
                $com = "";
            }
        } else {
            $com = "";
        }
        if ($type eq 'Activity' && $val eq '' && $com) {
            push @tags, ["Effect", $ftns.$com, undef ];
            $tally{"Activity Values"}{$com}++;
        } elsif ($com) {
            push @tags, ["Comment", $ftns.$com, undef ];
        }
        if ($preds->{ValidityIssue}) {
            my $vi = join(";", @{$preds->{ValidityComment} || []}) ||
                "Unspecified Validity Concerns";
            push @tags, ["Dissent", $ftns.$vi, undef];
        }
        foreach my $docid (@{$preds->{HasDocument} || []}) {
            if ($docid =~ /^chembl_document:(.+)$/) {
                if (my $pmid = $id2pm->{$1}) {
                    push @tags, ['Referenced In', $pmid, undef];
                }
            }
        }
        if ($units eq '%' && $prePerc->{$type}) {
            $type = "$units $type";
        }
        my $edgeType = $etLU->{$type};
        unless ($edgeType) {
            $edgeType = 'was assayed against';
            $tally{"Generic 'Assayed' Type"}{$type}++;
        }
        if (defined $val) {
            if ($val eq '') {
                $val = undef;
            } elsif (my $cb = $unitMap->{$units}) {
                ($units, $val) = &{$cb}($val);
            }
        }
        
        if ($utilityIssue || !$rel || $rel ne '=') {
            # There was a problem, or the assay relationship is not "equals"
            $utilityIssue ||= "Relation ".($rel || 'unknown');
            my $metric = "$type";
            $metric .= " $rel" if ($rel && $rel ne '=');
            $metric .= " [$units]" if ($units);
            push @tags, ["Result", $ftns.$metric, $val ] if (defined $val );
        } elsif ($type && defined $val) {
            # We are comfortable with recording the details of this assay
            push @tags, [$type, $units ? "#Unit#$units" : undef, $val];
        }


        $tally{"Assay Utility"}{$utilityIssue || $type}++;
        print &act2txt($data, $targ, $smiID)."\n";
        if ($targ && $smiID) {
            $lh->set_edge( -name1 => $smiID,
                           -name2 => $targ,
                           -type  => $edgeType,
                           -tags  => \@tags );
            $tally{"Processing Status"}{"Data Recorded"}++;
            if ($targ =~ /^CHEMBL/) {
                $utilityIssue ||= "Non-protein target: $type";
            }
        } else {
            $tally{"Processing Status"}{"Unrecordable"}++;
        }
        $lh->write_threshold_quick($cache);
        $num++;
        last if ($limit && $num >= $limit);
        if ($progTime && time - $lTime >= $progTime) {
            $lTime = time;
            $args->msg("[$num]", "$chid");
        }
    }
    my $uah = $tally{"#Unit Assignments"} ||= {};
    while (my ($k, $v) = each %{$tally{"Unit Assignments"} || {}}) {
        if ($v->{Unitless}) {
            my @vs = ('Unitless');
            my %h = %{$v};
            delete $h{Unitless};
            push @vs, sort { $v->{$b} <=> $v->{$a} } keys %h;
            my $tot = 0; map { $tot += $v->{$_} } @vs;
            my $sum = sprintf("%d%% %s", 100 * $v->{Unitless} / $tot, 
                              join(', ', map { $_.'='.$v->{$_}} @vs));
            $uah->{sprintf("%7d %s", $tot, $k)} = $sum;
        } else {
            delete $uah->{$k};
        }
    }
    delete $tally{"Unit Assignments"};
    $lh->write();
    my $tallyFile = sprintf("%s/bms_%d_Activities", $dir, $chemblVersion);
    $tallyFile .= '-LIMIT' if ($limit);
    &tally_to_file($tallyFile);
    $args->msg("[+]", "Finished processing $num activities", 
               "$tallyFile.tally");
}

sub chem_data {
    my $id2pm = &pubmed_lookup();
    my $smiMT = &smiles_lookup();
    my $fh    = &chembl_fh( 'molecule' );
    my $num   = 0;
    &init_lh();
    my (@noSmis, %hdps);
    my $lTime = time;
    $args->msg("[<]","Loading molecule data");
    while (my $data = &next_turtle($fh, 'chembl_molecule')) {
        my $id    = $data->{subj};
        my $preds = $data->{preds};
        next unless ($id);
        my $urlRef;
        unless ($id =~ /^CHEMBL\d+$/) {
            my $rescue;
            my $url = $data->{url} || "";
            if ($url eq 'http://en.wikipedia.org/wiki') {
                if (my $hl = $preds->{HumanLabel}) {
                    if ($hl->[0] && $hl->[0] =~ 
                        /^(CHEMBL\d+) Wikipedia Reference/) {
                        $rescue = $1;
                        $urlRef = "$url/$id";
                    }
                }
            }
            if ($rescue) {
                $id = $rescue;
            } else {
                #die $args->branch($data);
                &err("Unexpected ID '$id'");
                next;
            }
        }
        my $mtid = $smiMT->{ $id };
        my @types = @{$preds->{SubstanceType} || []};
        unless ($mtid) {
            push @noSmis, join("\t", $id, join('/', @types) || "?");
            next;
        }

        $lh->set_edge( -name1 => $mtid,
                       -name2 => $urlRef,
                       -type  => 'is referenced in', ) if ($urlRef);

        $lh->set_edge( -name1 => $mtid,
                       -name2 => $id,
                       -type  => 'is a reliable alias for' );
        foreach my $clsRaw (@types) {
            $tally{"SubstanceType"}{$clsRaw}++;
            if (my $cls = $molClass->{ $clsRaw }) {
                $lh->set_class($mtid, $cls);
            } else {
                $args->msg_once("Unrecognized molecule type '$clsRaw'");
            }
        }

        foreach my $key (qw(LabelAlt)) {
            foreach my $val (@{$preds->{$key} || []}) {
                next if (!$val || $val =~ /^\d+$/ || lc($val) eq 'na');
                $lh->set_edge( -name1 => $mtid,
                               -name2 => $ftns . $val,
                               -type  => 'is a reliable alias for' );
            }
        }

        foreach my $docid (@{$preds->{HasDocument} || []}) {
            if ($docid =~ /^chembl_document:(.+)$/) {
                if (my $pmid = $id2pm->{$1}) {
                    $lh->set_edge( -name1 => $mtid,
                                   -name2 => $pmid,
                                   -type  => 'is referenced in' );
                }
            }
        }

        my $hdp = $preds->{HighestDevPhase};
        if ($hdp && $#{$hdp} == 0) {
            my $set = "ChEMBL Highest Development Phase ". $hdp->[0];
            $hdps{$set}++;
            $tally{DevelopmentPhase}{$set}++;
            $lh->set_edge( -name1 => $mtid,
                           -name2 => $set,
                           -type  => 'is a member of' );
        }

        foreach my $xref (@{$preds->{XrefMol} || []}) {
            if ($xref =~ /^http:\/\/[a-z]+\.wikipedia\.org\/wiki\/(\S+)$/) {
                my $mtwp = $wpns . $1;
                $lh->set_class($mtwp, 'WIKIPEDIA');
                $lh->set_edge( -name1 => $mtwp,
                               -name2 => $mtid,
                               -type  => 'contains a reference to' );
            } elsif ($xref =~ /^http/) {
                $lh->set_class($xref, 'HYPERLINK');
                $lh->set_edge( -name1 => $xref,
                               -name2 => $mtid,
                               -type  => 'contains a reference to' );
            } else {
                 $args->msg_once("Unrecognized XREF '$xref'");
            }
        }

        $lh->write_threshold_quick($cache);
        # print &_data2txt($data)."\n";
        $num++;
        last if ($limit && $num >= $limit);
        if ($progTime && time - $lTime >= $progTime) {
            $lTime = time;
            $args->msg("[$num]", "$id $mtid");
        }
    }
    my $hdpTxt = "Development Phases:\n";
    foreach my $set (sort keys %hdps) {
        $lh->set_class($set, 'GROUP');
    }
    $lh->write();
    my $noSmi = sprintf("%s/bms_%d_NoSmiles.txt", $dir, $chemblVersion);
    $noSmi .= "-LIMIT" if ($limit);
    open(NOSMI, ">$noSmi") || $args->death
        ("Failed to write NoSmiles", $noSmi, $!);
    print NOSMI join("\n", @noSmis)."\n";
    close NOSMI;
    $args->msg("[FILE]",$noSmi);
    my $tallyFile = sprintf("%s/bms_%d_Smiles", $dir, $chemblVersion);
    &tally_to_file($tallyFile);
}

sub targ_lookup {
    &chembl_file();
    my $lookup = sprintf("%s/bms_%d_target_lookup.txt", $dir, $chemblVersion);
    unless (-s $lookup) {
        &init_lh();
        my $typeLU = {
            "SINGLE PROTEIN"              => "Protein",
            "ORGANISM"                    => "Organism",
            "CELL-LINE"                   => "Cell Line",
            "TISSUE"                      => "Tissue",
            "PROTEIN FAMILY"              => "Protein Family",
            "PROTEIN COMPLEX"             => "Complex",
            "SELECTIVITY GROUP"           => "Group",
            "SUBCELLULAR"                 => "Organelle",
            "PROTEIN COMPLEX GROUP"       => "Complex",
            "UNKNOWN"                     => "Unknown",
            "NUCLEIC-ACID"                => "Nucleic Acid",
            "PROTEIN-PROTEIN INTERACTION" => "Protein-Protein Interaction",
            "CHIMERIC PROTEIN"            => "Protein",
            "PHENOTYPE"                   => "Phenotype",
            "UNCHECKED"                   => "Unknown",
            "ADMET"                       => "",
        };
        my $tcl = &targ_comp_lookup();
        $args->msg("Parsing target data");
        my $fh    = &chembl_fh( 'target' );
        my $num   = 0;
        open(LOOKFH, ">$lookup") || $args->death
            ("Can not write target lookup", $lookup, $!);
        while (my $data = &next_turtle($fh, 'chembl_target')) {
            my $chid = $data->{subj};
            my $preds = $data->{preds};
            my @tt   = @{$preds->{TargetType} || []};
            map { $tally{TargetType}{$_}++ } @tt;
            if ($#tt != 0) {
                $args->death("Non-singular target type", $chid, @tt);
            }
            my $class = $typeLU->{$tt[0] || ""};
            next unless ($class);
            $lh->set_class($chid, $class);
            map { $tally{TargetType}{$_}++ } @tt;

            foreach my $d (@{$preds->{Title} || []}) {
                $lh->set_edge( -name1 => $chid,
                               -name2 => "$ftns$d",
                               -type  => 'is a shorter term for' )
                    unless (!$d || $d =~ /^\d+$/);
            }
            my @targs;
            foreach my $tc (@{$preds->{TargetComponents}}) {
                if ($tc =~ /^chembl_target_cmpt:(\S+)$/) {
                    push @targs, @{$tcl->{$1} || []};
                }
            }
            map { $lh->set_edge( -name1 => $chid,
                                 -name2 => $_,
                                 -type  => 'has member' ) } @targs;
            my $mapKey = $chid;
            if ($#targs == -1) {
                $tally{"Zero Target Types"}{$class}++;
            } elsif ($#targs == 0) {
                $mapKey = $targs[0];
                $tally{"Single Target Types"}{$class}++;
            } else {
                $tally{"Multipe Target Types"}{$class}++;
            }
            print LOOKFH "$chid\t$mapKey\n";
            $num++;
            $lh->write_threshold_quick($cache);
            # die if ($num >= 20);
        }
        close LOOKFH;
        $lh->write();
        $args->msg("[+]","Recovered $num target entries");
        &tally_to_file($lookup);
    }
    open(SLOOK, "<$lookup") || $args->death
        ("Failed to read assay lookup", $lookup);
    my %hash;
    my $num = 0;
    while (<SLOOK>) {
        s/[\n\r]+$//;
        my ($id, $mtid) = split(/\t/);
        $hash{$id} = $mtid;
        $num++;
    }
    $args->msg("[<]","Recovered $num target mappings", $lookup);
    return \%hash;
    
}

sub targ_comp_lookup {
    &chembl_file();
    my $lookup = sprintf("%s/bms_%d_targetComponent_lookup.txt",
                         $dir, $chemblVersion);
    unless (-s $lookup) {
        $args->msg("Parsing target component data");
        my $lookErr = sprintf("%s/bms_%d_targetComponent_issues.txt",
                              $dir, $chemblVersion);
        my $lookDet = sprintf("%s/bms_%d_targetComponent_issue_details.txt",
                              $dir, $chemblVersion);
        my $fh    = &chembl_fh( 'targetcmpt' );
        my $num   = 0;
        open(LOOKFH, ">$lookup") || $args->death
            ("Can not write target component lookup", $lookup, $!);
        open(LOOKERR, ">$lookErr") || $args->death
            ("Can not write target component error file", $lookErr, $!);
        open(LOOKDET, ">$lookDet") || $args->death
            ("Can not write target component details file", $lookDet, $!);
        my @priority = ('uniprot', 'ENSP', 'ENSG', 'ENST', 'EC');
        my %ignoredHTTP;
        while (my $data = &next_turtle($fh, 'chembl_target_cmpt')) {
            # die $args->branch($data);
            my %xrefs;
            if (my $xr = $data->{preds}{XREFs}) {
                foreach my $x (@{$xr}) {
                    if ($x =~ /^http.+\/ensembl\/(ENS[A-Z]*([GPT])\d+)$/) {
                        my ($i, $t) = ($1, $2);
                        push @{$xrefs{"ENS$t"}}, $i;
                        $tally{TargetNamespace}{"ENS$t"}++;
                        next;
                    }
                    if ($x =~ /^http.+\/ec-code\/([^\/]+)$/) {
                        push @{$xrefs{"EC"}}, "EC:$1";
                        $tally{TargetNamespace}{"EC"}++;
                        next;
                    }
                    if ($x =~ /^https?:\/\/(.+)/) {
                        my $url = $1;
                        if ($url =~ /^(identifiers.org\/[^\/]+)/) {
                            $url = $1;
                        } else {
                            $url =~ s/\/.+//;
                        }
                        $ignoredHTTP{$url}++;
                        next;
                    }
                    if ($x =~ /^([^:]+):(.+)$/) {
                        push @{$xrefs{$1}}, $2;
                        $tally{TargetNamespace}{$1}++;
                    }
                }
                delete $data->{preds}{XREFs};
            }
            my $val = "";
            for my $p (0..$#priority) {
                if (my $found = $xrefs{$priority[$p]}) {
                    my %u = map { $_ => undef } @{$found};
                    $val = join("\t", sort keys %u);
                    last;
                }
            }
            my $subj = $data->{subj};
            if (!$val) {
                print LOOKERR "$subj\tNo target IDs\n";
                print LOOKDET $args->branch($data)."\n";
            } elsif ($val =~ /\t/) {
                print LOOKERR join("\t", $subj, "Multiple target IDs", $val)."\n";
            }
            $val = "\t$val" if ($val);
            print LOOKFH "$subj$val\n";
            $num++;
        }
        close LOOKFH;
        my @skip = sort { $ignoredHTTP{$b} <=> 
                              $ignoredHTTP{$a} } keys %ignoredHTTP;
        printf( LOOKERR "\n############\n\n%d classes of URIs skipped\n\n",
                $#skip + 1);
        foreach my $url (@skip) {
            print LOOKERR "$url\t$ignoredHTTP{$url}\n";
        }
        close LOOKERR;
        close LOOKDET;
        
        $args->msg("[+]","Processed $num target component entries", 
                   $lookup, $lookErr, $lookDet );
        &tally_to_file($lookup);
    }
    open(SLOOK, "<$lookup") || $args->death
        ("Failed to read target component lookup", $lookup);
    my %hash;
    my $num = 0;
    while (<SLOOK>) {
        s/[\n\r]+$//;
        my @targs = split(/\t/);
        my $id = shift @targs;
        next if ($#targs == -1);
        $num++;
        $hash{$id} = \@targs;
    }
    $args->msg("[<]","Recovered $num target component mappings", $lookup);
    return \%hash;
}

sub assay_lookup {
    &chembl_file();
    my $lookup = sprintf("%s/bms_%d_assay_lookup.txt", $dir, $chemblVersion);
    unless (-s $lookup) {
        my $issues = sprintf("%s/bms_%d_assay_issues.txt", $dir, $chemblVersion);
        &init_lh();
        # https://www.ebi.ac.uk/chembldb/index.php/faq#faq24
        my $confKey = {
            0 => 'Target assignment has yet to be curated',
            1 => 'Target assigned is non-molecular',
            3 => 'Target assigned is molecular non-protein target',
            4 => 'Multiple homologous protein targets may be assigned',
            5 => 'Multiple direct protein targets may be assigned',
            6 => 'Homologous protein complex subunits assigned',
            7 => 'Direct protein complex subunits assigned',
            8 => 'Homologous single protein target assigned',
            9 => 'Direct single protein target assigned',
        };
        my $blacklist = {
            CHEMBL612545 => "Unchecked / UnclassifiedTarget",
        };
        my $tl    = &targ_lookup();
        my $id2pm = &pubmed_lookup();
        $args->msg("Parsing assay data");
        my $fh    = &chembl_fh( 'assay' );
        my $num   = 0;
        open(LOOKFH, ">$lookup") || $args->death
            ("Can not write assay lookup", $lookup, $!);
        print LOOKFH join("\t", qw(AssayID Type References Targets ActivityIDs))."\n";
        open(ISSUES, ">$issues") || $args->death
            ("Can not write assay issues", $issues, $!);
        while (my $data = &next_turtle($fh, 'chembl_assay')) {
            my $chid = $data->{subj};
            my @row = ($chid);
            $lh->set_class($chid, 'Protocol');
            my $preds = $data->{preds};
            foreach my $txtxt (@{$preds->{Taxon} || []}) {
                if ($txtxt =~ /ncbitax:(\d+)/) {
                    $lh->set_taxa($chid, $1);
                }
            }
            foreach my $d (@{$preds->{Description} || []}) {
                $lh->set_edge( -name1 => $chid,
                               -name2 => "$ftns$d",
                               -type  => 'is a shorter term for' )
                    unless (!$d || $d =~ /^\d+$/);
            }

            my @tags;
            my @at = @{$preds->{AssayType} || []};
            if ($#at == 0) {
                push @row, $at[0];
            } else {
                &err("Non-unique assay types", $chid, @at);
                push @row, "";
            }
            my @pmids;
            foreach my $docid (@{$preds->{HasDocument} || []}) {
                if ($docid =~ /^chembl_document:(.+)$/) {
                    if (my $pmid = $id2pm->{$1}) {
                        push @pmids, $pmid;
                        $lh->set_edge( -name1 => $chid,
                                       -name2 => $pmid,
                                       -type  => 'is referenced in' );
                    }
                }
            }
            push @row, join(",", @pmids) || "";

            foreach my $conf (@{$preds->{TargetConfidence} || []}) {
                push @tags, ['Target Confidence', $ftns . $conf, undef];
                $tally{TargetConfidence}{$conf}++;
            }

            my (@targs, @issues);
            foreach my $targ (@{$preds->{Targets} || []}) {
                if ($targ =~ /^chembl_target:(CHEMBL\d+)$/) {
                    if (my $ct = $tl->{$1}) {
                        if (exists $blacklist->{$ct} && $blacklist->{$ct}) {
                            push @issues, $blacklist->{$ct};
                        } else {
                            push @targs, $ct;
                        }
                    }
                }
            }
            push @row, join(",", @targs) || "";

            if ($#targs == -1) {
                print ISSUES join("\t", $chid,"No Valid targets",@issues)."\n";
            } elsif ($#targs != 0) {
                print ISSUES join("\t",$chid,"Non-unique targets",@targs)."\n";
            }
            foreach my $targ (@targs) {
                $lh->set_edge( -name1 => $chid,
                               -name2 => $targ,
                               -type  => 'has member',
                               -tags  => \@tags );
            }
            my @acts;
            foreach my $act (@{$preds->{Activities} || []}) {
                if ($act =~ /chembl_activity:CHEMBL_ACT_(\d+)$/) {
                    push @acts, $1;
                }
            }
            push @row, join(",", @acts) || "";
            print LOOKFH join("\t", @row)."\n";
            
            $lh->write_threshold_quick($cache);
            $num++;
            # last if ($num >= 10000);
        }
        $lh->write();
        close LOOKFH;
        close ISSUES;
        $args->msg("[+]","Recovered $num Assay entries", $issues);
        &tally_to_file($lookup);
    }
    open(SLOOK, "<$lookup") || $args->death
        ("Failed to read assay lookup", $lookup);
    <SLOOK>;
    my %hash;
    my $num = 0;
    my $assnum = 0;
    while (<SLOOK>) {
        s/[\n\r]+$//;
        my ($chid, $type, $refs, $targs, $acts) = split(/\t/);
        my $info = [ $chid, $type, 
                     [split(/\,/, $refs)],
                     [split(/\,/, $targs)] ];
        foreach my $act (split(/\,/, $acts)) {
            $hash{$act} = $info;
            $num++;
        }
        $assnum++;
     }
    $args->msg("[<]","Recovered $num activity lookups in $assnum Assays", $lookup);
    return \%hash;
}

sub act2txt {
    my ($data, $targ, $smi) = @_;
    my $preds = $data->{preds} || {};
    my $txt = $data->{subj};
    $txt .= " $targ" if ($targ);
    if (my $mol = $smi || $preds->{HasMolecule}[0]) {
        $txt .= " vs. $mol";
    }
    $txt .= "\n";
    $txt .= "   ";
    if (my $type = $preds->{ActType}[0]) {
        $txt .= "$type";
        if (defined $preds->{ActValue}[0]) {
            $txt .= " ". ($preds->{ActRel}[0] || "?=?"). " ".
                $preds->{ActValue}[0];
        }
        if (my $u = $preds->{ActUnits}[0]) {
            $txt .= " $u";
        }
    } else {
        $txt .= "?No type?";
    }
    $txt .= "\n";
    foreach my $pred (sort keys %{$data->{preds} || {}}) {
        next if ($actSkipTxt->{$pred});
        my @vals = @{$data->{preds}{$pred}};
        next if ($skipPred->{$pred} || $#vals == -1);
        $txt .= "  $pred : " .join(' // ', @vals). "\n";
    }
    return $txt;
}

sub _data2txt {
    my $data = shift;
    my $txt  = $data->{subj} || "";
    if ($txt) {
        if (my $ns = $data->{ns}) { $txt .= " [$ns]"; }
        $txt .= "\n";
        foreach my $pred (sort keys %{$data->{preds} || {}}) {
            my @vals = @{$data->{preds}{$pred}};
            next if ($skipPred->{$pred} || $#vals == -1);
            $txt .= "  $pred\n";
            $txt .= join('', map { "    $_\n" } @vals);
        }
    }
    return $txt;
}

sub smiles_lookup {
    my ($smiFile) = @_;
    &chembl_file();
    my $lookup = $smiFile || 
        sprintf("%s/bms_%d_smiles_lookup.txt", $dir, $chemblVersion);
    my $cacheSize = $args->val(qw(smilescache)) || 5000;
    $cacheSize -= 2;
    my $num = 0;
    unless (-s $lookup) {
        $args->msg("Parsing SMILES from molecule file");
        my $sts   = &get_sts();
        $sts->report_mtids(1);
        my $fh    = &chembl_fh( 'molecule' );
        $cntr     = 0;
        my $lines = 0;
        $smiCache = [];
        my $csmiFH;
        open($csmiFH, ">$lookup") || $args->death
            ("Can not write SMILES lookup", $lookup, $!);
        while (my $data = &next_turtle($fh, 'chembl_molecule')) {
            $lines++;
            # $args->branch($data);
            if (my $smiArr = $data->{preds}{SMILES}) {
                my $id = $data->{subj};
                if ($#{$smiArr} == 0) {
                    $num++;
                    push @{$smiCache},  "$smiArr->[0] $id";
                    &{$smiCacheMethod}( $sts, $csmiFH )
                        if ($#{$smiCache} > $cacheSize);
                    $cntr++;
                } else {
                    &err("Weird SMILES for $id", @{$smiArr});
                }
            }
        }
        &{$smiCacheMethod}( $sts, $csmiFH ) if ($#{$smiCache} != -1);
        close $csmiFH;
        $args->msg("Recovered $cntr SMILES entries");
    }
    my %hash;
    unless ($smiFile) {
        open(SLOOK, "<$lookup") || $args->death
            ("Failed to read SMILES lookup", $lookup);
        $num = 0;
        while (<SLOOK>) {
            s/[\n\r]+$//;
            my ($id, $mtid) = split(/\t/);
            $hash{$id} = $mtid;
            $num++;
        }
    }
    $args->msg("[<]","Recovered $num SMILES assignments", $lookup);
    return \%hash;
}

sub cache_to_file {
    my ($sts, $fh) = @_;
    return if ($#{$smiCache} == -1);
    print $fh join("\n", @{$smiCache})."\n";
    $args->msg("[$cntr]", $smiCache->[-1]);
    $smiCache = [];
}

sub act_on_cache {
    my ($sts, $fh) = @_;
    my $num = $#{$smiCache} + 1;
    return unless ($num);
    my $rv = $sts->canonical( $smiCache );
    foreach my $row (@{$rv}) {
        my ($mtid, $smi, $id) = @{$row};
        # warn "($mtid, $smi, $id)\n";
        if ($mtid) {
            print $fh "$id\t$mtid\n";
        } else {
            &err("$smi $id Failed");
        }
    }
    $args->msg("[$cntr]", $smiCache->[-1]);
    
    $smiCache = [];
}

sub pubmed_lookup {
    &chembl_file();
    my $lookup = sprintf
        ("%s/bms_%d_pubmed_lookup.txt", $dir, $chemblVersion);
    unless (-s $lookup) {
        $args->msg("Parsing PubMed from document file");
        my $fh    = &chembl_fh( 'document' );
        $cntr     = 0;
        my $pmFH;
        open($pmFH, ">$lookup") || $args->death
            ("Can not write PubMed lookup", $lookup, $!);
        while (my $data = &next_turtle($fh, 'chembl_document')) {
            if (my $pmArr = $data->{preds}{PubMedUrl}) {
                my $id = $data->{subj};
                if ($#{$pmArr} == 0 && $pmArr->[0] =~ 
                    /^http:\/\/identifiers\.org\/pubmed\/(\d+)$/) {
                    print $pmFH "$id\tPMID:$1\n";
                    $cntr++;
                } else {
                    &err("Weird PubMed for $id", @{$pmArr});
                }
            }
        }
        close $pmFH;
        $args->msg("Recovered $cntr PubMed entries");
    }
    open(SLOOK, "<$lookup") || $args->death
        ("Failed to read PubMed lookup", $lookup);
    my %hash;
    my $num = 0;
    while (<SLOOK>) {
        s/[\n\r]+$//;
        my ($id, $pmid) = split(/\t/);
        $hash{$id} = $pmid;
        $num++;
    }
    $args->msg("[<]","Recovered $num PubMed lookups", $lookup);
    return \%hash;
}

=pod Target types

 SELECT target_type, count(target_type) FROM TARGET_DICTIONARY
     group by target_type

TARGET_TYPE      #
  UNCHECKED      1
  ADMET          1
  NUCLEIC-ACID  10
  UNKNOWN       20
  SUBCELLULAR   35
  TISSUE       200
  CELL-LINE   1198
  ORGANISM    1460
  PROTEIN     4568

=cut

sub err {
    $args->msg("[ERR]", @_);
}

sub init_lh {
    return $lh if ($lh);
    $lh = BMS::MapTracker::LoadHelper->new
        ( -username  => 'ChEMBL',
          -userdesc  => 'The ChEMBL bioactive drug database',
          -loadtoken => 'ChEMBL',
          -basedir   => $baseDir,
          -testfile  => $testFile,
          -testmode  => $tm, );
    $mt = $lh->tracker;
    $mt->{DUMPSQL} = $args->{DUMPSQL};
    my $uns = $mt->make_namespace( -name => "Unit",
                                   -desc => "Unit specifications, such as kg or M",
                                   -sensitive => 0);
    
    return $lh;
}

sub finalize {
    # &process_bulk();
    $lh->write;
    if ($baseDir && !$tm) {
        $args->msg("Loading maptracker directory", $baseDir);
        $lh->process_ready( -benchmark => $args->{BENCHMARK} );
    }
}

sub get_chembl_files {
    my $vers = shift;
    my $base = "ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBL-RDF/latest";
    unless ($vers) {
        $args->msg("[!]","Sorry, automatic ChEMBL version detection not yet implemented","Go here to find the version to use (include decimal point):",$base);
        exit;
    }
    my @shorts = qw(activity assay document molecule target targetcmpt);
    foreach my $short (@shorts) {
        my $file = sprintf("chembl_%s_%s.ttl.gz", $vers, $short);
        my $url  = "$base/$file";
        my $path = "$dir/$file";
        if (-s $path) {
            $args->msg("[-]", "Skipping $short, already exists");
            next;
        }
        my $cmd = sprintf("wget -P '%s' -nd %s", $dir, $url);
        $args->msg("[>]", $url, $path, $cmd);
        system($cmd);
    }
    $args->msg("[+]","Version $vers has been mirrored", $dir);
    exit;
}

sub chembl_file {
    unless ($chemblFiles) {
        my %vers;
        foreach my $file ($args->read_dir( -dir => $dir,
                                           -keep => '(\.ttl|\.ttl\.gz)$' )) {
            if ($file =~ /\/chembl_(\d+(\.\d+)?)_(.+).ttl(.gz)?$/) {
                $vers{$1}{lc($3)} = $file;
            }
        }
        ($chemblVersion) = sort { $b <=> $a } keys %vers;
        $args->death("No chembl files recognized in directory", $dir)
            unless ($chemblVersion);
        $args->msg("[+]", "Using Version $chemblVersion ChEMBL files", $dir);
        $chemblFiles = $vers{$chemblVersion};
    }
    my $req = shift;
    return "" unless ($req);
    my $file = $chemblFiles->{lc($req || "")};
    $args->death("File '$req' not found for version $chemblVersion", $dir)
        unless ($file);
    return $file;
}

sub chembl_fh {
    my $file = &chembl_file(@_);
    $args->death("Failed to get file handle", $_[0]) unless ($file);
    my ($fh, $ftype);
    $args->ignore_error('Inappropriate ioctl for device');
    undef $!;
    undef $@;
    if ($file =~ /\.gz$/) {
        $file =~ s/^[\<\>]+//;
        $ftype = 'gz';
        $args->ignore_error('Illegal seek');
        open($fh, "gunzip -c $file |");
        $args->ignore_error('Illegal seek', 'StopIgnoring');
    } else {
        unless ($file =~ /^[\<\>]/) {
            $file = "<$file";
        }
        $ftype  = '';
        open($fh, $file);
    }
    if (!$fh || ($! && $! ne 'Illegal seek')) {
        if ($fh) {
            $args->err
                ("Failed to recover file handle glob", $file,
                 $! ? '$! = '.$! : undef, $@ ? '$@ = '.$@ : undef);
        } else {
            $args->death
                ("Failed to open file handle", $file,
                 $! ? '$! = '.$! : undef, $@ ? '$@ = '.$@ : undef);
        }
    }
    return $fh;
}

sub next_turtle {
    # http://en.wikipedia.org/wiki/Turtle_%28syntax%29
    my ($fh, $nsReq) = @_;
    my $rv;
    while (my $newRv = &_next_turtle_subject( $fh )) {
        if ($rv) {
            # We have already started building an object
            if ($rv->{subj} ne $newRv->{subj}) {
                # Record with different subject
                if ($nsReq && $newRv->{ns} && $newRv->{ns} eq $nsReq) {
                    # This is the next object, push on stack for later use
                    #$args->msg("[!!]", "Reserved:",$args->branch($newRv));
                    push @objStack, $newRv;
                    # Exit loop
                    last;
                } else {
                    # Just ignore it
                    #$args->msg("[-]","Ignoring record", $args->branch($newRv));
                }
            }
        } else {
            # We are trying to build an object
            if ($nsReq && (!$newRv->{ns} || $newRv->{ns} ne $nsReq)) {
                #$args->msg("[-]","Skipping record", $args->branch($newRv));
                next;
            }
            # First recovered record, use it to set the return value
            # and continue the loop
            $rv = $newRv;
            next;
        }
        # We need to add this data to our previous object
        
        while (my ($pred, $oArr) = each %{$newRv->{preds} || {}}) {
            push @{$rv->{preds}{$pred}}, @{$oArr};
            $tally{Predicates}{$pred} += $#{$oArr} + 1;
        }
        if (my $errs = $newRv->{ERR}) {
            push @{$rv->{ERR}}, @{$errs};
        }
        # continue loop

    }
    
    return $rv;
}



sub _next_turtle_subject {
    my $fh = shift;
    my $line = "";
    if ($#objStack != -1) {
        return shift @objStack;
    }
    while (<$fh>) {
        s/[\n\r]+$//;
        next if (/^\s*$/);
        s/\P{IsASCII}//g;
        next if (/^\@prefix/);
        $line .= $_;
        last if ($line =~ /\.\s*$/);
    }
    return undef unless ($line);
    my $raw = $line;
    my $lastDot;
    if ($line =~ /\s*\.\s*$/) {
        $line =~ s/\s*\.\s*$//;
        $lastDot = 1;
    }
    my %quotes;
    my $qnum = 0;
    $line =~ s/\\\\\\\"/$quoteChar/g;
    while ($line =~ /(\"([^\"]*)\")/ ||
           $line =~ /(<([^>]*)>)/) {
        my ($in, $out) = ($1, $2);
        $out =~ s/$quoteChar/\"/g;
        my $qt = $turtQuote . ++$qnum;
        $quotes{$qt} = $out;
        $line =~ s/\Q$in\E/$qt/g;
    }
    $line =~ s/$quoteChar/\"/g;
    my $rv = { ns => "" };
    my $subj;
    if ($line =~ /^(\S+)\s+(.+)/) {
        ($subj, $line) = ($1, $2);
        $subj =~ s/\^\^.+$//;
        $subj = $quotes{$subj} if (defined $quotes{$subj});
    } else {
        &err("Could not find subject in line", $line);
        push @{$rv->{ERR}}, "No subject found";
        return $rv;
    }
    
    my ($url, $hash) = ("","");
    if ($subj =~ /^(http.+)\/([^\/]+)$/) {
        # die $line if ($subj =~ /wikipedia.org/);
        $rv->{url} = $1;
        $subj = $2;
        if ($subj =~ /^(.+)\#(.+)$/) {
            # The hash is actually modifying predicates that follow
            $hash = $2;
            $subj = $1;
        }
    }
    if ($subj =~ /^([^:]+):(.+)$/) {
        $subj = $2;
        $rv->{ns} = $1;
    }
    if (! $rv->{ns} && $rv->{url} =~
        /^https?:\/\/(identifiers.org\/[^\/]+|www.ncbi.nlm.nih.gov\/Taxonomy)/){
        $rv->{ns} ||= $1;
    }
    $rv->{subj} = $subj;

    my @predObjs = split(/\s*\;\s*/, $line);
    # $args->msg("[PredObj]", @predObjs);
    if ($predObjs[0] =~ /^a /) {
        # The 'a' predicate does not seem useful
        shift @predObjs;
        # The hash appears to be the informative predicate in these cases
        $predObjs[0] =~ s/^\S+/$hash/ if ($hash);
    }
    foreach my $po ( @predObjs ) {
        my ($pred, $ol);
        if ($po =~ /^(\S+)\s+(.+)/) {
            ($pred, $ol) = ($1, $2);
            $pred =~ s/\^\^.+$//;
            $pred = $quotes{$pred} if (defined $quotes{$pred});
            if (defined $remap->{$pred}) {
                $pred =  $remap->{$pred};
                next unless ($pred);
            }
        } else {
            &err("Could not parse predicate/object", $po,$raw);
            push @{$rv->{ERR}}, "Unparsed PO : $po";
            next;
        }
        my $targ = $rv->{preds}{$pred} ||= [];
        foreach my $obj (split(/\s*\,\s*/, $ol)) {
            $obj =~ s/\^\^.+$//;
            $obj = $quotes{$obj} if (defined $quotes{$obj});
            $obj =~ s/\\\\/\\/g;
            push @{$targ}, $obj;
        }
    }
    # $args->msg("[SUBJ]", $subj);
    return $rv;
}

sub get_sts {
    # Non-globals
    my ($isParent) = @_;
    my $lh = BMS::MapTracker::LoadHelper->new
        ( -username  => $user,
          -basedir   => $baseDir,
          -loadtoken => "SMILES",
          -testmode => 0, );

    # The cache being passed is the number of database operations to batch
    # before writing a load file to disk
    my $sts = BMS::MapTracker::SciTegicSmiles->new
        ( -loader  => $lh,
          -cache   => 5000,
          -verbose => 1, );
    # Renew connection after 5 hours
    $sts->soap_life( 60 * 60 * 5);
    $sts->{PC_CACHE} = [];
    return $sts;
}

sub hack {
    return;
    my $fh;
    open($fh, "</stf/biocgi/tilfordc/working/foo.ttl");
    while (my $data = &next_turtle($fh, 'chembl_assay')) {
        print &_data2txt($data)."\n";
    }
    die;
}
