#!/stf/biobin/perl -w

my $isBeta;
BEGIN {
    # Needed to make my libraries available to Perl64:
    # use lib '/stf/biocgi/tilfordc/released';
    # use lib '/apps/sys/perl/lib/site_perl/5.12.0/';
    # Make sure 5.10 libraries are moved to end of INC:
    my (@incOther ,@inc510);
    map { if (/5\.10/) { push @inc510, $_ } else { push @incOther, $_ } } @INC;
    @INC = (@incOther ,@inc510);
    use lib '/stf/biocgi/tilfordc/patch_lib';
    # Allows usage of beta modules to be tested:
    my $progDir = join(' ', $0, `pwd`);
    if ($progDir =~ /(working|perllib)/) {
        require lib;
        import lib '/stf/biocgi/tilfordc/perllib';
        $isBeta = 1;
    } else {
        $isBeta = 0;
    }
    $| = 1;
    print '';
    # print "Content-type: text/plain\n\n";
}

my $VERSION = 
    ' $Id$ ';

use strict;
use BMS::Utilities;
use BMS::MapTracker::AccessDenorm;
use BMS::TableReader;
use BMS::LiterateTSV;
use BMS::ExcelHelper;
use BMS::FlatOntology;
use BMS::ForkCritter;
use BMS::FriendlyGraph;
use BMS::BmsArgumentParser;
use BMS::Utilities::Serialize;
use BMS::Utilities::ColorUtilities;

use Bio::SeqIO;
use Data::Dumper;
use GD;

my $safe      = '1 Jan 2015';
my $taxSafe   = '1 Jan 2015';
my $log10     = 1 / log(10);
my $log2      = 1 / log(2);

# map { &sig_fig($_) } (0.00000125314117494142, 1,5, 15, 115, 1115, 11115, 0.11151, 0.0111111, 0.0000011111); exit;

srand( time() ^ ($$ + ($$<<15)) );
my $args = BMS::BmsArgumentParser->new
    ( -nocgi      => $ENV{HTTP_HOST} ? 0 : 1,
      -mode       => 'convert',
      -minimize   => 1,
      -assurenull => 1,
      -fastSort   => 10000,
      -errormail  => 'charles.tilford@bms.com',
      -tmpdir     => "/stf/biohtml/tmp",
      -tiddlywiki => 'GenAcc',
      -paramalias => {
          tempdir    => [qw(tmpdir)],
          setup      => [qw(norun)],
          qrysrc     => [qw(querysrc querysource addqrysrc)],
          addsym     => [qw(addsymbol symbol)],
          symin      => [qw(symbolin addsymin)],
          addseq     => [qw(addsequence seqout outputseq outputsequence)],
          adddesc    => [qw(desc description descout adddescr)],
          descin     => [qw(adddescin)],
          sortchr    => [qw(chrsort sortchr fullchr)],
          addtax     => [qw(addspecies addtaxa)],
          addlink    => [qw(addlinks addurl addurls)],
          addinv     => [qw(inventory bmsinv wetinv)],
          dryinv     => [qw(dryinventory)],
          adddate    => [qw(update)],
          taxin      => [qw(speciesin)],
          subbad     => [qw(subhowbad howsubbad)],
          nullscore  => [qw(nullsc nullval nullvalue)],
          nearest    => [qw(keepnearest keepclosest)],
          keepdup    => [qw(keepduplicate keepduplicates keepdups)],
          nonskids   => [qw(nonschildren)],
          cleancol   => [qw(cleancols)],
          clustjoin  => [qw(clusterjoiner)],
          assmiles   => [qw(showsmile showsmi showsmiles assmile)],
          integerns  => [qw(numericns numberns numericid numberid)],
          int        => [qw(intersection intersect)],
          taxid      => [qw(ncbitax taxids usetaxid)],
          hasheader  => [qw(has_header)],
          requiremt  => [qw(requiremaptracker)],
          cols       => [qw(cols custcol custcols col columns)],
          asis       => [qw(nostandard)],
          explain    => [qw(explainsql)],
          xxxx => [qw()],
          xxxx => [qw()],
          xxxx => [qw()],
          xxxx => [qw()],
          xxxx => [qw()],
          xxxx => [qw()],
      });


my $nocgi     = $args->val(qw(nocgi));
my @preErrs;
unless ($nocgi) {
    $SIG{__WARN__} = sub {
        my ($txt) = @_;
        my $stk = $args->stack_trace();
        push @preErrs, ($txt, $stk);
    };
}

my $noSafeAge = $args->val(qw(nosafeage));
my $doWarn    = $args->val(qw(warn));
my $scrollSz  = $args->val(qw(scrollsize));
my $tmpdir    = $args->val('tempdir');
my $noRun     = $args->val('setup');
my $useOracle = $args->val( qw(useoracle));

$args->write_log();
$args->debug->skip_key([qw(eByN graph edges DB_CURRENT vals)]);
$args->ignore_error('user requested cancel of current operation');
$args->ignore_error('Bio/Root/IO.pm line 543');

my @adArgs = ( -noenv     => $args->val(qw(noenv noenvironment)),
               # -rebuild   => 1,
               -deepdive  => $args->val('deepdive'),
               -pghost    => $args->val('pghost'),
               -requiremt => $args->val('requiremt'),
               -oracle    => $useOracle, );

my $ad = &getAccessDenorm();
if ($args->{MAKEDATABASE}) {
    $args->msg("Creating database tables", `date`);
    $ad->{DBH}->make_all();
    $args->msg("Finished", `date`);
    exit;
}
# die $args->to_text();

my $selfLink  = ($0 =~ /working/) ? "http://bioinformatics.bms.com/biocgi/tilfordc/working/maptracker/MapTracker/genacc_service.pl" : "http://bioinformatics.bms.com/biocgi/genacc_service.pl";
my @defFmts =
    ( [ 'Full HTML Structured', 'Structured HTML' ],
      [ 'Full HTML', 'HTML Table' ],
      [ 'Full Grid HTML', 'HTML Grid' ],
      [ 'Excel', 'Excel Workbook' ],
      [ 'Excel Grid', 'Excel Grid' ],
      [ 'Described Excel', 'Excel + Description' ],
      [ 'List', 'Simple List' ],
      [ 'List Ranked', 'Ranked List' ],
      [ 'Full HTML OntologyReport', 'Ontology Report' ],
      [ 'TSV', 'Tab Table' ],
      [ 'CSV', 'Comma Separated Value' ],
      [ 'Set', 'SetFile Format' ],
      [ 'MatrixMarket', 'MatrixMarket' ],
      [ 'TiddlyWiki', 'TiddlyWiki' ],
      [ 'JSON', 'JavaScript Object' ],
      [ 'Perl', 'Perl Object' ],
      [ 'BED', 'BED Genome Coordiantes'],
      [ 'CommandLine', 'Show Command Line' ],
);

my @inFmts = 
    ( ['', 'Guess for me'],
      ['tsv','TSV'],
      ['csv','CSV'],
      ['fasta','Fasta'],
      ['rich','Rich / SetFile'],
      ['xls','Excel'],
      ['xlsx','Excel XML'],
      );

my $simpleEnglishGO = {
    'GO:0003707' => 'NHR',
    'GO:0004672' => 'Kinase',
    'GO:0004721' => 'Phosphatase',
    'GO:0004930' => 'GPCR',
    'GO:0005216' => 'Ion Channel',
    'GO:0005615' => 'Secreted',
    'GO:0009986' => 'Cell Surface',
    'GO:0003824' => 'Enzyme',
    'GO:0005887' => 'Membrane-bound',
    'GO:0016298' => 'Lipase',
    'GO:0006986' => 'Heat Shock',
    'GO:0008233' => 'Protease',
    'GO:0031012' => 'ECM',
    'GO:0005576' => 'Extracellular',
    'GO:0003774' => 'Motor',
    'GO:0005515' => 'Protein Binding',
    'GO:0005215' => 'Transporter',
    'GO:0045298' => 'Tubulin',
    'GO:0004888' => 'TMR',
    'GO:0099600' => 'TMR',
    'GO:0005886' => 'Cell Membrane',
    'GO:0009897' => 'External Membrane',
    'GO:0016651' => 'Oxidoreductase',
    'GO:0003700' => 'Transcription Factor',
    'GO:0004842' => 'Ubiquitin Ligase',
    'GO:0016032' => 'Virulence',
};
my $simpleEnglishEC = {
    IDA => "Platinum class. Direct assay EXPERIMENTAL data are present to support the finding. You may want to look more closely at the particular assay used",
    IPI => "Platinum class. Physical interaction EXPERIMENTAL data support the assignment. GO warns that it can be difficult to reliably determine if the interaction is truly direct.",
    IMP => "Platinum class. EXPERIMENTAL observation of the mutant phenotype supports this assignment.",
    IGI => "Platinum class. Genetic EXPERIMENTAL evidence is supporting the assignment. Note of course that genetic alterations and phenotypes are often horrifically misinterpreted.",

    EXP => "Gold class. EXPERIMENTAL DATA is supporting the finding, though the specific kind of experiment has not been indicated",
    IEP => "Gold class. EXPERIMENTAL expression data support the assignment. GO is somewhat suspicious of this category, as expression data are easy to misanalyze.",

    TAS => "Gold class. A STATEMENT in a publication supports the finding. No experimental data has been noted, however.",

    ISO => "Silver class. SEQUENCE orthology is being used to bring annotations from another species to this one. If the orthology is done well, and the similarity is high, this is generally reliable. Otherwise...",
    
    ISA => "Bronze class. SEQUENCE alignment data are supporting the annotation. This can be useful in some cases, but it can also be irrelevant.",
    ISM => "Silver class. SEQUENCE models are used as evidence. Good models can be reliable, bad models can generate lots of false positives",

    ISS => "Bronze class. Some kind of SEQUENCE analysis is being used as evidence. ISA, ISO and ISM are more specific versions of this code. Sometimes this will be a reliable method for some genes and ontologies, not so much with others. Consider such evidence cautiously",

    IGC => "Bronze class. SEQUENCE evidence in the form of where the gene lives on the genome. In some cases this can be rock-solid evidence, in others it might be circumstantial.",

    IBA => "Bronze class. PHYLOGENETIC evidence based on inferred function of the ancestral gene. Sketchy.",
    IBD => "Bronze class. PHYLOGENETIC evidence based on inferred function of a descendent gene. Sketchy.",
    IRD => "Bronze class. PHYLOGENETIC evidence based on divergence. REALLY sketchy.",
    
    IKR => "Bronze class. SEQUENCE analysis shows that the gene has mutations that make an assignment IMPOSSIBLE. If you see this code you should investigate further",
    
    RCA => "Bronze class. Human reviewed ALGORITHMS support the assignment. Presumably a bit better than IEA",
    
    

    NAS => "Tin class. Someone, somewhere, made a STATEMENT to support the assignment. We just can't remember who. Or maybe it was a conversation at the caffeteria, so I can't give you a link. Hey, just trust us!",

    IEA => "Tin class. The most common evidence, and unfortunately one of the weakest. An ALGORITHM has been used to infer the finding. Assume a high false positive rate. Also this is the ONLY evidence code that is not set by a human curator.",
    IC => "Tin class. The GO curator is making a STATEMENT themselves. They don't have time to write a publication to support this evidence, but they comment heavily about THIS VERY TOPIC on Twitter and their blogs. Or something. Sketchy.",

    NR => "Sawdust class. Yeah, we have no idea why this annotation is here. I'm sure there was a good reason, but we can't find it",
    ND => "Sawdust class. At some point, somebody recorded the associtation. But there is NO EVIDENCE to support it.",
    E => "Sawdust class. Like 'P', this is an ancient evidence code that is mostly found on cave paintings and velociraptor bones. If the annotators have not been able to find further evidence then you can safely ignore it.",
    P => "Sawdust class. Like 'E', this is an old no-longer-used evidence code. If you see it, it means nothing better was found. The code should generally be ignored.",
    ECO => "Sawdust class. UNKNOWN. 'ECO' generally means 'Evidence Code Ontology', but it does not make sense to be used by itself. These are probably misannotations.",
};

my @edgeCols = qw(cccccc 000000 ff0000 d73027
                  f46d43 fdae61 fee08b eeee00
                  cccc00 668800 339900 33ff00 );

&onebox($args->{QUERY}) if ($args->{ONEBOXNAME});

my @popularIntersections =
    ("AAD HG_U219", "AAD HG_U133A", "AAD HT_HG_U133A", "AAD MOE430A", "AAD MOE430B", "TAX Homo sapiens", "TAX Mus musculus", "TAX Rattus norvegicus");

my $urlJoin   = '|';
my $clustJoin = $args->val('clustjoin') || ' // ';
my $age       = $args->val(qw(age ageall allage)) || $safe;
my $cAge      = $args->val(qw(cloudage ageall allage)) || $safe;;
my $dumpsql   = $args->val(qw(dumpsql sqldump));
my $slowsql   = $args->val(qw(slowsql));
my $fmt       = $args->val(qw(format fmt)) ||
    ($nocgi ? 'tsv' : 'Full HTML');
$fmt          = $fmt->[-1] if (ref($fmt));
my $mode      = lc($args->val(qw(action mode)) || 'convert');
my $freq      = $args->val(qw(status));
my $filter    = $ad->list_from_request( $args->val(qw(filter)) );
my $doStnd    = $args->val(qw(standardize));
my $quiet     = $args->val(qw(quiet));
my $vb        = $quiet ? 0 : $args->val(qw(vb verbose));
$vb           = defined $vb ? $vb : $nocgi ? 1 : $doWarn;
my $doBench   = $args->val(qw(benchmark showbench dobench bench)) || 0;
$doBench     /= 100 if ($doBench && $doBench =~ /^\d+$/ && $doBench >= 1);
my $intersect = $args->val('int');
my $intNS     = $args->val(qw(intns));
my $numberNS  = $args->val('integerns');
my $setReq    = $args->val(qw(set sets));
my $limit     = $args->val(qw(limit));
my $cloudType = $args->val(qw(cloudtype)) || "";
my $sortKey   = $args->val(qw(sort order orderby sortby));
my $doReq     = $args->val(qw(directonly)) || "";
my $rootPar   = $args->val(qw(root));
my $maxDist   = $args->val(qw(distance dist range));
$maxDist      = 1000000 unless defined ($maxDist);
my $fastSort  = $args->val(qw(fastsort));
my $gaClass   = $args->val(qw(CLASS)) || 'gatab';
my $showSMI   = lc($args->val('assmiles') || "");
my $showInchi = $args->val(qw(inchi showinchi)) || ($showSMI =~ /inchi/)
    ? 1 : 0;
my $useTaxId  = $args->val('taxid');
my $nullRows  = $args->val(qw(nullrows needed missing nullonly));
my $asn       = $args->{ASSURENULL};
my $kn        = $nullRows || $args->val(qw(keepnull)) || 0;
my $doPretty  = $args->val(qw(dopretty prettyprint pretty));
my $precise   = $args->val(qw(precise));
my $authReq   = $args->val(qw(auth authority ec)) || "";
my $hideScore = $args->val('hidescore')   ? 1 : 0;
my $addEc     = $args->val('addec')  ? 1 : 0;
if (my $na = $args->val(qw(noauth noauthority notauth))) {
    my @old = !$authReq ? () : ref($authReq) ? @{$authReq} :
        split(/[\n\r\t\,]/, $authReq);
    my @not = ref($na) ? @{$na} : split(/[\n\r\t\,]/, $na);
    $authReq = [ @old, map { "!$_" } @not ];
}
my $igCase    = $args->val(qw(ignorecase));
if (my $igText = $args->val(qw(ignore)) || "") {
    $igCase = 1 if (!defined $igCase && $igText =~ /case/i);
}
my $showToken = $args->val(qw(showtoken usetoken));
my $ltnsupd   = $args->val(qw(ltupdate));
my $addLinks  = $args->val(qw(link links));
my $cxDebug   = $args->val(qw(cxdebug debug));
my $parPid    = $$;
my $isSet     = $args->{ISSET};
my $isText    = $args->{ISTEXT};
my $oraTable  = $args->val(qw(table oracletable tablename));
my $cleanChr  = $args->val(qw(cleanchr simplechr)) ? 1 : 0;
my $extrRE    = $args->val(qw(extractid));
my $cleanQry  = $args->val(qw(cleanqry cleanquery isdirty dirty)) || "";
$cleanQry   ||= 'Integers' if ($numberNS);
$cleanQry     = $cleanQry =~ /dirt/i || ($cleanQry eq '1' || $extrRE) ?
    'Dirty' : $cleanQry =~ /int/i ? 'Integers' : 'Clean';
my $isDirty   = $cleanQry eq 'Dirty' ? 1 : 0;
my $outFile   = $args->val(qw(output outfile));
my $nullSc    = $args->val('nullscore');
$nullSc       =~ s/^\s+// if (defined $nullSc);
my $isTable   = ($isSet || $isText) ? ($isSet || $isText) : 
    $args->val(qw(colname colnum excelcol istable));
my $hasHeader = $isText || $args->val('hasheader');
my $bestOnly  = $args->val(qw(best bestonly keepbest));
my $tiFormat  = undef;
my $classCol  = $args->val(qw(classcol colclass));
my $abbrTaxa  = $args->val(qw(shortspecies shorttaxa));
my $splitter  = $args->val(qw(splitter));
unless (defined $splitter) {
    $splitter = '\n\r\t';
    $splitter    .= '\s' unless (($nocgi && !$args->val('splitspace')) ||
                                 $args->val('keepspace'));
    $splitter    .= '\,' unless ($args->val('keepcomma'));
}
my $concatGo  = $args->val(qw(concatgo));
my $noSc      = $args->val(qw(nosc noscore))   ? 1 : 0;
my $noAu      = $args->val(qw(noauth)) ? 1 : 0;
my $scramble  = $args->val(qw(scramble randomize random shuffle));
my $ns2       = $args->val(qw(ns2 ons nsout ns_out));
my $inFormat  = $args->val(qw(inputformat formatin informat));
my @idKeys    = qw(queries id ids query term terms idlist idpath idfile
                   idfasta idquery idsql);
my $ns1arg    = $args->val(qw(ns1 ns nsin ns_in namespace qns ans)) || "";
my @ns1Arr    = ref($ns1arg) ? @{$ns1arg} : split(/\s*\,\s*/, $ns1arg);
my $fork      = $args->{FORK};
my $doPop     = $args->val(qw(populate)) || (!$fork ? $scramble : undef);
$fork       ||= $doPop;
my $oracleStart = $args->val(qw(orastart oraclestart));
my $maxRep    = $args->val(qw(maxrep maximumrepresentation));
my $minRep    = $args->val(qw(minrep minimumrepresentation));
my $reqRoot   = $args->val(qw(requireroot));
my $versus    = $args->val(qw(versus));
my $linkWP    = $args->val(qw(linkwikipedia linkwiki linkwp));
my $nsMemReq  = $args->val(qw(nsmembers nsmem));
my $allowDup  = $args->val(qw(allowduplicates allowduplicate allowdup));
my $doVers    = $args->val(qw(noversion novers)) ? 0 : 1;
my $clearnum  = $args->val(qw(clearnum clearnumber));
my $bpsOpts   = 'short' . ($args->val(qw(nosymtok)) ? '' : ' warn poor');
my $exSql     = $args->val('explain');

# Fold change min/max values for color scale
my $maxFC = 6;

if (my $addC  = $args->val(qw(addclass))) { $gaClass .= " $addC"; }
my (@queries, @uniqQ, %seen,  @preMsg, $fc, $primaryChild, $metaCols,
    %usedEC, $keepEC, $tossedGos, $tossedSets, @usedGos, $mustFlag,
    %extraDone, @css, @errors, %parCache, $crsSTH, @clouds, %randFlags,
    %specialCol, $benchFile, $mimeSet, $ltsv, $unusedCustSet,
    $unrequestedColumns, $userHeader, $compareLists, %ehColCB);


my $customSets = &parse_custom_sets('customset');
my $rejectSets = &parse_custom_sets('rejectset', "\t");
if ($rejectSets) {
    foreach my $id (keys %{$rejectSets}) {
        $rejectSets->{$id} = [ split("\t", $rejectSets->{$id} ) ];
    }
}
if (my $kcr = $args->val(qw(keepec))) {
    foreach my $k ($ad->list_from_request([split(/[\,\s]+/,uc($kcr))])) {
        if ($k =~ /^[A-Z]{2,4}$/) {
            $keepEC ||= {};
            $keepEC->{$k} = 1;
        } else {
            $args->msg_once("Unusual Evidence Code request -keepec '$k'")
                if ($k);
        }
    }
}
my $metaSets = &parse_custom_sets('metaset', undef, 1);
if ($metaSets) {
    my %names = map { $_ || "MetaData" => 1 } map { keys %{$_} } values %{$metaSets};
    $metaCols = [sort keys %names];
    $metaCols = undef if ($#{$metaCols} == -1);
}

my $flagReq = $args->val('mustflag');
if ($flagReq) {
    unless ($flagReq eq '1') {
        # We want flagged entries, but only for particular terms
        $mustFlag = { map { $_ => 1 } split(/\s*\,\s*/, lc($flagReq)) };
        delete $mustFlag->{""};
    }
}
my $mustToss = $args->val('musttoss');
if ($mustToss) {
    $mustToss = { map { $_ => 1 } split(/\s*\,\s*/, uc($mustToss)) };
}


if ($nsMemReq) {
    # User wants all members for a namespace
    if (my $id = $ad->namespace_name($nsMemReq)) {
        $mode   = 'convert';
        $ns2    = $id;
        @ns1Arr = ('NS');
        @idKeys = ();
        push @queries, $id;
        push @uniqQ,   $id;
    } else {
        $args->err("Did not recognize Namespace membership request for '$nsMemReq'");
    }
}

my @goodNs1;
foreach my $ns1req (@ns1Arr) {
    foreach my $ns1r (split(/\s+\,\s+/, $ns1req)) {
        if (my $ns = $ad->namespace_token($ns1r)) {
            push @goodNs1, $ns;
        }
    }
}

my $oneNs1 = $#goodNs1 == 0 ? 1 : 0;
my $ns1    = $oneNs1 ? $goodNs1[0] : $#goodNs1 == -1 ? '' : \@goodNs1;
my $ser    = BMS::Utilities::Serialize->new();

push @adArgs, ( -age => $age,
                -cloudage => $cAge );

# Huh. If you make a DBI connection, then a statement handle, then let the
# DBI object go out of scope, the STH will still work, but you can no longer
# recover the DBI with {Database}. @globals being used to solve issue.
my @globals;

# IF YOU ADD NEW COLUMNS, BE SURE TO UPDATE
# *BOTH* THE POD DOCUMENTATION AT THE END OF THE CODE,
# AND THE PARSING LOGIC IN _standardize_column()

my $hdata = {
    'format'   => {
        label => "Format",
        ToDo  => "What is this?",
    },
    age        => {
        label => "Data Age",
        mode => 'numeric',
        desc => 'The age, in days, since the data were last refreshed in GenAcc',
    },
    auth       => {
        label => "Authority",
        mode => 'character',
        desc => "The entities associated with the result. If multiple steps were taken to reach the output they will be separated with ' < '. Multiple authorities in one step are separated with ' + '",
    },
    chemtype   => {
        label => "Assay",
        mode => "character",
        desc => "The type of chemical assay reported, eg Ki or IC50"
    },
    chemunit   => {
        label => "Result",
        mode => "character",
        desc => "Chemical assay value plus unit, for example '1.3uM' or '43%'"
    },
    child      => {
        label => "Child Term",
        mode => "character",
        desc => "A child ontology term, more specific that the related parent term"
    },
    com_link   => {
        label => "Detailed Link Description",
        ToDo  => "What is this?",
    },
    clust_size => {
        label => "Cluster Size",
        mode => "integer",
        desc => "The number of nodes present in a cluster",
    },
    cust_set    => {
        label => "Custom Set",
        mode  => "character",
        desc  => "One or more sets optionally defined by the user. The column will be populated with a comma-concatenated list of all matching sets",
    },
    depth      => {
        label => "Depth",
        mode  => "integer",
        desc  => "The number of edges this node is from the root node in the hierarchy",
    },
    desc_in    => {
        label => "Input Description",
        mode => "character",
        desc => "A human-readable description of the input node"
    },
    desc_link  => {
        label => "Link Description",
        ToDo  => "What is this?",
    },
    desc_out   => {
        label => "Output Description",
        mode => "character",
        desc => "A human-readable description of the output node"
    },
    distance   => {
        label => "Distance",
        mode  => "integer",
        desc  => "The distance, in base pairs, this object is from the query. Zero indicates an overlap, negative values are bases 'to the left', positive are bases 'to the right'",
    },
    dryinv     => {
        label => "BMS Stock (mg)",
        mode  => "numeric",
        desc  => "Indicates the amount (miligrams) of dry inventory at BMS according to CRS",
    },
    end_in     => {
        label => "Input End",
        mode  => "numeric",
        desc  => "The end coordinate for the input node",
    },
    end_out    => {
        label => "Output End",
        mode  => "numeric",
        desc  => "The end coordinate for the output node",
    },
    go_class   => {
        label => "GeneOntology Classes",
        mode  => "character",
        desc  => "Zero or more GO IDs assigned to the output node",
    },
    howbad     => {
        label => "HowBad",
        mode  => "numeric",
        desc  => "How much worse the alignment score for this location is compared to the best alignment found for the query, in percentage points. A value of zero indicates this is the best possible alignment score."
    },
    inventory  => {
        label => "BMS Stock (ul)",
        mode  => "numeric",
        desc  => "Indicates the amount (microliters) of wet inventory at BMS according to CRS",
    },
    specificity => {
        label => "Specificity",
        mode  => "integer",
        desc  => "The count of Homologene clusters hit by the compound. The higher the number, the less specific the compound appears to be (hits more gene targets)",
    },
    len_in     => {
        label => "Input Length",
        mode  => "numeric",
        desc  => "The sequence length of the query (bp or aa)",
    },
    len_out    => {
        label => "Output Length",
        mode  => "numeric",
        desc  => "The sequence length of the output term (bp or aa)",
    },
    links      => {
        label => "Hyperlinks",
        mode  => "character",
        desc  => "Zero or more potentially useful hyperlinks related to the output terms",
    },
    loc_in     => {
        label => "Input Location String",
        mode  => "character",
        desc  => "A GenBank feature table string describing the HSP boundaries for the input sequence. TRUNCATED TO 4000 CHARACTERS.",
    },
    loc_out    => {
        label => "Output Location String",
        mode  => "character",
        desc  => "A GenBank feature table string describing the HSP boundaries for the output sequence. TRUNCATED TO 4000 CHARACTERS.",
    },
    matched    => {
        label => "Matched",
        mode  => "numeric",
        desc  => "A value between 0 and 1 describing the confidence of the information shown in the row, with zero being 'certainly wrong' and one being 'fully confident'. These values may be semi-arbitrary, or may reflect a specific measurement (like percent identity, or fraction of probes matching). A value of -1 may be reported for some query parameters, which should be interpreted as NA",
    },
    metasets => {
        label => "MetaSetData",
        mode  => "character",
        desc  => "User-supplied metadata tied to output values",
    },
    matched_in => {
        label => "Query Matched",
        mode  => "numeric",
        desc  => "For mapping overlaps, the fraction of the query that matches the shared sequence",
    },
    nicescore  => {
        label => "Nice Score",
        mode  => "numeric",
        desc  => "The normalize 'matched' score (0-1) denormalized back to a human-interpretable chemical assay value, based on the specific assay reported (eg Ki values will be 'M', percent inhibition will be '%'). Units may be shown in another column."
    },
    notes      => {
        label => "Notes",
        ToDo  => "What is this?",
    },
    ns         => {
        label => "Namespace Token",
        mode  => "factor",
        desc  => "A short token representing the namespace name, such as 'BAPS' for 'BrainArray Probe Set'",
        ToDo  => "Sure about this?",
    },
    ns_between => {
        label => "Internal NS Tokens",
        mode  => "character",
        desc  => "A list of zero or more namespace tokens that occur between the input and output, separated by '<'",
    },
    ns_in      => {
        label => "Input Namespace",
        mode  => "factor",
        desc  => "The namespace of the input node. A namespace is a broad category of identifiers, such as 'Ensembl Gene' or 'Affy ProbeSet'",
    },
    ns_out     => {
        label => "Output Namespace",
        mode  => "factor",
        desc  => "The namespace of the input node. A namespace is a broad category of identifiers, such as 'RefSeq RNA' or 'PubMed ID'",
    },
    nsn        => {
        label => "Namespace Name",
        mode  => "character",
        desc  => "The full namespace name, such as 'GeneOntology Term' or 'MSigDB Gene Set'",
        ToDo  => "Sure about this?",
    },
    parent     => {
        label => "Parent Term",
        mode => "character",
        desc => "A parent ontology term, more generic that the related child term"
    },
    pubmed     => {
        label => "PubMed",
        mode => "character",
        desc => "One or more PubMed literature accessions"
    },
    qry_src    => {
        label => "Query Source",
        mode => "character",
        desc => "When reporting mapping overlaps, this column defines the start and end coordinates of the query sequence"
    },
    rank       => {
        label => "Rank",
        mode  => "integer",
        desc  => "Used when reporting regular expressions used to match accessions to namespaces. A lower rank indicates a more reliable RegExp"
    },
    regexp     => {
        label => "Regular Expression",
        mode  => "character",
        desc  => "A RegExp used to match accessions to a namespace"
    },
    rel        => {
        label => "Relation",
        ToDo  => "What is this?",
    },
    set        => {
        label => "Set Membership",
        mode  => "character",
        desc  => "Zero or more sets that the output node belongs to. Commonly used to report Affy array designs for probe sets",
    },
    start_in   => {
        label => "Input Start",
        mode  => "numeric",
        desc  => "The start coordinate for the input node",
    },
    start_out  => {
        label => "Output Start",
        mode  => "numeric",
        desc  => "The start coordinate for the output node",
    },
    strand     => {
        label => "Strand",
        mode  => "integer",
        desc  => "The relative orientation between input and output sequences. -1 indicates a reverse orientation",
    },
    subset     => {
        label => "Ontology Subset",
        mode  => "character",
        desc  => "The high-level ontological subset for an ontology term, like 'molecular_function'",
    },
    seq_out    => {
        label => "Output Sequence",
        mode  => "character",
        desc  => "The biological sequence (ala 'AACCATGA...') for the output node",
    },
    sortchr    => {
        label => "Sortable Location",
        mode  => "character",
        desc  => "A character sort-friendly chromosomal identifier, like '04'",
    },
    sym_in     => {
        label => "Input Symbol",
        mode  => "character",
        desc  => "The gene symbol for the input node. Attempts are made to pick the 'best' symbol if more than one are present",
    },
    sym_out    => {
        label => "Symbol",
        mode  => "character",
        desc  => "The gene symbol for the output node. Attempts are made to pick the 'best' symbol if more than one are present",
   },
    tax_in     => {
        label => $abbrTaxa ? "GsIn" : "Input Species",
        mode  => "character",
        desc  => "The formal scientific species name of the input node.",
    },
    tax_out    => {
        label => $abbrTaxa ? "Gs" : "Species",
        mode  => "character",
        desc  => "The formal scientific species name of the output node.",
   },
    term_between => {
        label => "Intermediate Term",
        mode  => "character",
        desc  => "A chain of zero or more nodes that occur in the conversion chain between input and output. Generally not avaialable in cached data",
    },
    term_in    => {
        label => "Input Term",
        mode  => "character",
        desc  => "The input (query) term, belonging to the namespace reported by ns_in. Typically an accession from a database, like 'rs1234', 'LOC859' or 'GO:1903169'",
    },
    term_out   => {
        label => "Output Term",
        mode  => "character",
        desc  => "The output (result) term, belonging to the namespace reported by ns_out. Typically an accession from a database, like '1001_at', 'ENSG00000182533' or 'PMID:11532985'",
    },
    term_share => {
        label => "Shared Term",
        mode  => "character",
        desc  => "For mapping overlaps, this is the shared/common sequence that both input and output are aligned to. It will typically be a fully-specified genomic segment, like 'homo_sapiens.chromosome.3.GRCh37'",
    },
    updated    => {
        label => "Date Updated",
        mode  => "date('%Y-%m-%d %H:%M:%S')",
        desc  => "The date on which the reported fact was stored/refreshed in the database",
   },
    vers_in    => {
        label => "Input Version",
        mode  => "character",
        desc  => "The sequence version number of the input node. For nucleotides and proteins will generally be an integer. For whole chromosomes will be a build, like 'NCBIM37'",
   },
    vers_out   => {
        label => "Output Version",
        mode  => "character",
        desc  => "The sequence version number of the output node. For nucleotides and proteins will generally be an integer. For whole chromosomes will be a build, like 'Rnor_5'",
    },
};

my $stndColumns = { %{$hdata} };


$specialCol{flag_go}   = $args->{FLAGGO};

# $args->msg_callback(0) unless ($doWarn);

my $minScore = $args->val(qw(min matched score minsc));
my $howBad   = $args->val(qw(howbad));
my $defFlagGo = "GO:0003707,GO:0004672,GO:0004721,GO:0004842,GO:0004888,GO:0004930,GO:0005216,GO:0005576,GO:0006986,GO:0008233,GO:0016651,GO:0031012,GO:0003824,GO:0016298,GO:0005887,GO:0003774,GO:0005515,GO:0003700,GO:0005215,GO:0045298,GO:0016032";

my $colText = $args->val('cols');
my $noCol   = $args->val('skipcol');
my $colReq  = $colText ||= "";
if ($colReq) {
    if (ref($colReq)) {
        $colReq = $colText = join(",", @{$colReq});
    }
    $colReq = [map { &_standardize_column($_) } split(/[\,\n\r]+/, $colReq)];
    my %r = map { $_ => 1 } @{$colReq};
    map { $specialCol{$_} = $r{$_} }
    qw(desc_out desc_in links sym_out sym_in tax_out tax_in set updated
       inventory dryinv specificity seq_out sortchr len_in len_out qry_src);
    for my $ci (0..$#{$colReq}) {
        my $cr = $colReq->[$ci];
        if ($cr =~ /^GO\:/) {
            $specialCol{flag_go} ||= '';
            $specialCol{flag_go} .= " $cr ";
        } elsif ($cr =~ /^\#/) {
            my ($id, $ns, $int, $intns) = &_id_and_ns( $cr );
            if ($ns) {
                $specialCol{convert}{$cr} = {
                    -ns2   => $ns,
                    -int   => $int,
                    -intns => $intns,
                    -min   => $minScore,
                };
            } else {
                $args->msg("Failed to interpret conversion column request",
                           $cr);
            }
        }
    }
    if ($r{go_class}) {
        $specialCol{flag_go} ||= $defFlagGo;
        $concatGo ||= 1;
    }
}

my ($qryCol, $objCol) = ('term_in', 'term_out');
#$specialCol{updated}   ||= $args->{UPDATE} || $args->{ADDDATE};
#$specialCol{inventory} ||= $args->val(qw(inventory bmsinv));
#$specialCol{dryinv}    ||= $args->{DRYINVENTORY};

my $colLinks = {};
foreach my $arg (keys %{$args}) {
    if ($arg =~ /^(LINK_?COL|COL_?LINK)_(\S+)/) {
        my $col = &_standardize_column($2);
        unless ($col) {
            push @preErrs, "Can not make link for unknown column $1";
            next;
        }
        my $val = $args->{$arg};
        $val =~ s/^\s+//; $val =~ s/\s+$//;
        if ($val !~ /\</) {
            # The link is not fully constructed
            if ($val !~ /(href|src)=/i) {
                # The link is just a url
                $val = "href='$val'";
            }
            if ($val =~ /src=/i) {
                # Treat this as an image request
                $val = "<img $val />";
            } else {
                $val = "<a $val>";
            }
            $val .= "__${col}__</a>" if ($val =~ /^\<a /);
        }
        my $chk = $val;
        while ($chk =~ /__(\S+?)__/) {
            my $rep  = $1;
            my $subc = &_standardize_column($rep);
            unless ($subc) {
                push @preErrs, "Column link defines unknown column '$rep'";
                $val = '';
                last;
            }
            $val =~ s/__\Q$rep\E__/__${subc}__/g;
            $chk =~ s/__\Q$rep\E__/X/g;
        }
        $colLinks->{$col} = $val if ($val);
    }
}


if ($freq && 
    $freq =~ /^(\!?)(dep|deprecated|dead|self|live|status|use?able)$/i) {
    my ($ft, $fw) = ($1, $2);
    $mode  = 'convert' unless ($mode eq 'simple');
    $ns2   = 'RS';
    $kn    = 1;
    if ($fw =~ /status/i) {
        $freq = 'status';
    } elsif ($fw =~ /use?able/i) {
        $freq = 'usable';
    } else {
        if ($fw =~ /(self|live)/) {
            $freq = ($ft ? '!' : '') . 'self';
        } else {
            $freq = ($ft ? '' : '!') . 'self';
        }
    }
}

my $specialValues = {
    allgo => [qw(GO:0003674 GO:0005575 GO:0008150)],
    models => ['Bos taurus','Caenorhabditis elegans','Canis lupus familiaris','Danio rerio','Drosophila melanogaster','Gallus gallus','Homo sapiens','Macaca mulatta','Monodelphis domestica','Mus musculus','Oryctolagus cuniculus','Pan troglodytes','Rattus norvegicus','Sus scrofa','Felis cattus'],
};

my $savedTable = $args->val(qw(savetable savedtable keeptable)) ? [] : undef;
my $intNSfrm   = $ad->integer_format( $numberNS || $ns1);
if ($cleanQry eq 'Integers' && !$intNSfrm) {
    my @msg = ("You have indicated that your queries are integers");
    if (my $numNsTok = $numberNS || $ns1) {
        push @msg, "However, it is not possible to use integers to generate proper IDs using the '" .$ad->namespace_name($numNsTok)."' namespace";
    } else {
        push @msg, "If you wish to use this option, you must also specify the Query Namespace (-ns1), or if you are using mixed namespaces, use -integerns to specify the namespace to use when an integer is encountered";
    }
    push @msg, "Integer IDs can be used with the following namespaces:";
    push @msg, sort map { $ad->namespace_name($_) 
                          } $ad->allowed_integer_namespaces();
    push @preErrs, @msg;
}
if ($savedTable && !$isTable) {
    push @preMsg, "Assuming input from column 1";
    $isTable = 1;
}
$isTable ||= "";

# IF YOU ADD NEW FORMATS, BE SURE TO UPDATE
# THE POD DOCUMENTATION AT THE END OF THE CODE!

my %stndFmt = ( 
    ambig    => 'Ambig',
    atom     => 'Atom',
    bed      => 'BED',
    canvas   => 'CanvasXpress',
    chr      => 'ChrOnly',
    clust    => 'Cluster',
    col      => 'List',
    command  => 'CommandLine',
    cx       => 'CanvasXpress',
    dad      => 'DAD',
    datatable => 'DataTable',
    desc     => 'Described',
    distinct => 'Distinct',
    excel    => 'Excel',
    extgrid  => 'ExtJSgrid',
    fasta    => 'Fasta',
    fetcher  => 'GenomeFetcher',
    full     => 'Full',
    genbank  => 'Genbank',
    gml      => 'GML',
    google   => 'DataTable',
    grep     => 'RegExp',
    grid     => 'Grid',
    gsea     => 'GSEA',
    head     => 'Header',
    html     => 'HTML',
    image    => 'Image',
    img      => 'Image',
    json     => 'JSON',
    jsonp    => 'JSON',
    leaf     => 'Leaves',
    leav     => 'Leaves',
    link     => 'Links',
    list     => 'List',
    matrix   => 'MatrixMarket',
    meta     => 'MetaSet',
    nicechem => 'NiceChem',
    nohead   => 'NoHeader',
    nohit    => 'NoHits',
    nsname   => 'NamespaceName',
    nstok    => 'NamespaceToken',
    null     => 'Null',
    onto     => 'OntologyReport',
    oracle   => 'Oracle',
    perc     => 'AsPercent',
    round    => 'Rounded',
    perl     => 'Perl',
    pict     => 'Image',
    png      => 'Image',
    rank     => 'Ranked',
    regexp   => 'RegExp',
    rich     => 'Rich',
    seq      => 'Fasta',
    set      => 'SetFile',
    sheet    => 'Excel',
    snp      => 'Ambig',
    string   => 'String',
    struct   => 'Structured',
    sym      => 'Symbol',
    table    => 'Table',
    text     => 'Text',
    tiddly   => 'TiddlyWiki',
    tsv      => 'TSV',
    literate => 'LiterateTSV',
    ltsv     => 'LiterateTSV',
    integrated => 'Integrated',
    csv      => 'CSV',
    tw       => 'TiddlyWiki',
    txt      => 'Text',
    unique   => 'Distinct',
    xgml     => 'XGMML',
    xgmml    => 'XGMML',
    xls      => 'Excel',
    xlsx     => 'Excel',
    );

my @primaryFormat   = qw(Oracle Excel Fasta Genbank HTML Image XGMML GML CSV Text TSV List MetaSet SetFile OntologyReport Structured Perl String RegExp TiddlyWiki);
my @jsonFormats     = qw(DataTable ExtJSgrid DAD JSON);
my @secondaryFormat = (qw(Atom), @jsonFormats);

my %fmtFlags;
while (my ($alias, $flag) = each %stndFmt) {
    if ($fmt =~ /$alias/i) {
        $fmtFlags{$flag} = 1;
    }
}

my %formatArgMap =
    ( Cluster        => [ qw(cluster clusterin) ],
      Distinct       => [ qw(distinct unique) ],
      Null           => [ qw(populate null) ],
      NoHits         => [ qw(nohit nohits) ],
      NamespaceName  => [ qw(asname nsname) ],
      NamespaceToken => [ qw(astoken nstoken) ],
      Leaves         => [ qw(leafonly leavesonly onlyleaves onlyleaf) ],
      GenomeFetcher  => [ qw(genomefetcher) ],
      NoHeader       => [ qw(noheader skipheader nohead skiphead) ],
      NiceChem       => [ qw(nicechem cleanchem) ],
      AsPercent      => [ qw(aspercent usepercent asperc) ],
      Rounded        => [ qw(round roundpercent rounded) ],);
while (my ($flag, $arga) = each %formatArgMap) {
    next if (exists $fmtFlags{$flag} && defined $fmtFlags{$flag});
    $fmtFlags{$flag} = 1 if ($args->val(@{$arga}));
}

$fmtFlags{AsPercent} ||= $fmtFlags{Rounded};
if ($fmtFlags{LiterateTSV}) {
    $fmtFlags{TSV} ||= 1;
    $fmtFlags{Header} ||= 1;
}
if ($fmtFlags{MatrixMarket}) {
    $nullSc = -1;
}

my $formatColMap = {
    chemtype    => 'NiceChem',
    nicescore   => 'NiceChem',
    pubmed      => 'PubMed',
};
foreach my $col (@{$colReq || []}) {
    if (my $flag = $formatColMap->{$col}) {
        $fmtFlags{$flag} ||= 1;
    }
}

map { delete $fmtFlags{ $_->[0] } if ($fmtFlags{ $_->[1] }) } 
( [ 'Grid', 'ExtJSgrid' ] );
$fmt = join(' ', sort {lc($a) cmp lc($b)} keys %fmtFlags);
my $jsonType;
foreach my $fk (@jsonFormats) {
    if ($fmtFlags{$fk}) {
        $jsonType = $fk;
        $fmtFlags{HTML} = ($fk =~ /^DAD$/) ? 1 : 0;
        last;
    }
}
my $doClust = $fmtFlags{Cluster};

my %seenVals; my $valCount = 0;
@idKeys = () if ($noRun);
my $considered = 0;
foreach my $rkey (@idKeys) {
    my $kreq = $args->val($rkey);
    next unless ($kreq);
    my $kref = ref($kreq);
    my $ids;
    if (!$kref) {
        $ids = [ split(/[$splitter]+/, $kreq) ];
    } elsif ($kref eq 'Fh') {
        $ids = [ $kreq ];
    } elsif ($kref eq 'ARRAY') {
        $ids = $kreq;
    } else {
        $args->err("Not sure how to cope with IDs in '$kref' format");
        next;
    }
    for my $r (0..$#{$ids}) {
        my $req = $ids->[$r];
        my $reqSaved = 0;
        unless ($kref eq 'Fh') {
            $req =~ s/^\s+//; $req =~ s/\s+$//;
        }
        next unless ($req);
        $considered++;
        if ($rkey =~ /(sql)/i) {
            
        } elsif ($rkey =~ /(path|list|file|fasta)/i || $req =~ /^\//) {
            unless ($kref eq 'Fh' || -e $req) {
                push @preErrs, "The file path '$req' does not exist";
                $args->msg("[!!]", @preErrs);
                die $args->to_text();
                next;
            }
            my @list;
            my $tr     = BMS::TableReader->new();
            my $format;
            if ($inFormat) {
                $format = $tr->format( $inFormat, 'NonFatal' );
                $args->msg("[!]", "Failed to understand input format '$inFormat'") unless ($format);
            }
            $format ||= $tr->format_from_file_name($req . "");
            if ($format) {
                my $eColReq = $isTable || 0;
                if (!$eColReq && $format =~ /^(rich|fasta|list)$/) {
                    $eColReq = 1; 
                }
                my @colReqs = ref($eColReq) ? @{$eColReq} : ($eColReq);
                my (@textColumns, @numericColumns);
                foreach my $c1 (@colReqs) {
                    foreach my $c (split(/\s*[\t\n\r\,]\s*/, $c1)) {
                        next unless (defined $c && $c ne "");
                        $c =~ s/^\s+//; $c =~ s/\s+$//;
                        if ($c =~ /^\d+$/) {
                            push @numericColumns, $c;
                        } else {
                            push @textColumns, $c;
                        }
                    }
                }
                # $my $colNum = $eColReq =~ /^\d+$/ ? $eColReq : 0;
                $tr->has_header($hasHeader || ($#textColumns == -1 ? 0 : 1));
                $tr->limit( $limit * 2) if ($limit && !$scramble);
                $tr->format($format);
                $tr->input($req);
                my @sheets = $tr->each_sheet;
                if ($#textColumns == -1 && $#numericColumns == -1) {
                    if ($#sheets == 0) {
                        $tr->select_sheet($sheets[0]);
                        my @head = $tr->header();
                        push @numericColumns, 1 if ($#head == 0);
                    }
                }
                if ($#textColumns == -1 && $#numericColumns == -1) {
                    push @preErrs, 
                    ("When using tabular $format data as input you need to indicate the column to use with -colname (name or number)",
                     "Available columns in your input are:");
                    foreach my $sheet (@sheets) {
                        $tr->select_sheet($sheet);
                        my $sn = $tr->sheet_name();
                        my @sHead = $tr->header();
                        push @preErrs, "Sheet $sn", map { "  ".($_ + 1)." : ".($sHead[$_] || "") } (0..$#sHead);
                    }
                    next;
                }
                if (my $sreq = $args->val(qw(sheet excelsheet))) {
                    my $sheet = $tr->sheet($sreq);
                    unless ($sheet) {
                        push @preErrs, "Could not find worksheet '$sreq' in $req";
                        next;
                    }
                    @sheets = ($sheet);
                }
                if ($savedTable && $#sheets > 0) {
                    push @preMsg, "Multiple worksheets are present, but you requested to -savetable.", "Only the first worksheet will be kept. If you wish to use another, specifiy -sheet sheetName";
                    @sheets = ($sheets[0]);
                }

                push @preMsg, "Parsing $format file $req";
                my $checkLimit  = $limit && !$scramble ? 1 : 0;
                my $trackNumber = $savedTable || $checkLimit ? 1 : 0;
                my $loadVerbose = $args->val(qw(loadverbose));
                my $isFasta     = ($rkey =~ /fasta/i) ? 1 : 0;
                my $keepLftCol  = ($ns1 && $ns1 eq 'APS') ? 1 : 0;

                for my $sn (0..$#sheets) {
                    my $sheet = $sheets[$sn];
                    $tr->select_sheet($sheet);
                    if ($savedTable && $tr->has_header()) {
                        my @sHead = $tr->header();
                        push @{$savedTable}, \@sHead;
                        $userHeader ||= \@sHead;
                        $reqSaved++;
                    }

                    my %colHash = map { $_ => 1 } @numericColumns;
                    foreach my $cn (@textColumns) {
                        if (my $num = $tr->column_name_to_number($cn)) {
                            $colHash{ $num } = 1;
                        } else {
                            push @preErrs,"No columns matching '$cn' in ".
                                $tr->sheet_name();
                        }
                    }
                    my @colInds = sort { $a <=> $b } keys %colHash;
                    if ($#colInds == -1) {
                        next;
                    }
                    
                    push @preMsg, sprintf
                        ("Extracting column%s %s from %s",
                         $#colInds == 0 ? '' : 's', join
                         (",", @colInds), $tr->sheet_name())
                        if ($doWarn);
                    map { $_-- } @colInds;
                    while (my $row = $tr->next_clean_row()) {
                        my $c = $#{$row};
                        my @noN;
                        foreach my $i (@colInds) {
                            next if ($i > $c);
                            my $rowV = $row->[$i] || "";
                            my @vals = $isDirty ? $ad->extract_ids
                                ($rowV, $ns1, $extrRE)
                                : split(/[$splitter]+/, $rowV);
                            map { push @noN, $_ if ($_) } @vals;
                        }

                        if ($isFasta) {
                            for my $n (0..$#noN) {
                                if ($noN[$n] =~ /^([^:]+):(.+)/) {
                                    $noN[$n] = $keepLftCol ? $1 : $2;
                                }
                            }
                        }
                        push @list, @noN;
                        next unless ($trackNumber);
                        map {$valCount++ unless ($seenVals{$_}++)} @noN;
                        next unless ($savedTable);
                        my %stnd;
                        foreach my $val (@noN) {
                            my $std = &clean_query($val, $ns1);
                            $stnd{$std} = 1 if ($std);
                        }
                        @noN       = sort keys %stnd;
                        $reqSaved += $#noN + 1;
                        my @sTr    = @{$row};
                        if ($doClust) {
                            push @{$savedTable}, [ @sTr, \@noN ];
                        } elsif ($#noN == -1) {
                            # No input - we should preserve the row anyway
                            push @{$savedTable}, [ @sTr, '' ];
                        } else {
                            push @{$savedTable}, map { [ @sTr, $_ ] } @noN;
                        }
                        
                        last if ($checkLimit && $valCount >= $limit);
                        $args->msg
                            (sprintf("[LOAD %.03fk]", $#list/1000), $list[-1]) 
                            if ($loadVerbose && ! ($#list % $loadVerbose));
                    }
                    last if ($checkLimit && $valCount >= $limit);
                }
                # TableReader can sometimes discover a header:
                $hasHeader ||= $tr->has_header();
                $req = \@list;
            } else {
                my $isFasta    = ($rkey =~ /fasta/i) ? 1 : 0;
                my $keepLftCol = ($ns1 && $ns1 eq 'APS') ? 1 : 0;
                my $getIndex   = $isTable ? $isTable - 1 : 0;
                push @preMsg, "Reading input from $req";
                my $rowSplit = '\t';
                if ($isText) {
                    $rowSplit = '\s+\|\s+';
                }
                if (open(VALS, "<$req")) {
                    if ($hasHeader) {
                        my $fHead = <VALS>;
                        if ($isText) {
                            $fHead = <VALS>;
                            $fHead =~ s/[\n\r]+$//;
                            $fHead =~ s/^\|\s+//; $fHead =~ s/\s+\|$//;
                        }
                        if ($savedTable) {
                            $fHead =~ s/[\n\r]+$//;
                            push @{$savedTable}, [ split(/$rowSplit/, $fHead) ];
                            $userHeader ||= [ @{$savedTable->[-1]} ];
                            $reqSaved++;
                        }
                    }
                    while (<VALS>) {
                        s/[\n\r]+$//;
                        my @row;
                        if ($isSet) {
                            next if (/^\#/);
                        }
                        if ($isFasta) {
                            # This is being handled by TableReader now
                            $args->death("Should not get to this point!");
                            if (/^\>(\S+)\s+(.+)/ || /^\>(\S+)/) {
                                my ($id, $desc) = ($1, $2);
                                if ($id =~ /^([^:]+):(.+)/) {
                                    $id = $keepLftCol ? $1 : $2;
                                }
                                if ($id =~ /[^\|]+\|(.+)\|/) {
                                    # NCBI pipe-separated IDs
                                    $id = $1;
                                }
                                @row = ($id, $desc);
                            } else {
                                next;
                            }
                        } elsif ($isText) {
                            next if (/^\+/);
                            s/^\|\s+//; s/\s+\|$//;
                            @row = split(/\s+\|\s+/);
                        } elsif ($isTable) {
                            @row = split(/\t/);
                        } else {
                            @row = ($_);
                        }
                        my @vals = ($row[$getIndex] || '');
                        @vals = $ad->extract_ids(\@vals, $ns1, $extrRE)
                            if ($isDirty);
                        my @noN;
                        foreach my $val (@vals) {
                            if ($intNSfrm && $val =~ /^\d+$/) {
                                $val = sprintf($intNSfrm, $val);
                            }
                            push @noN, $val if ($val);
                        }
                        push @list, @noN;
                        if ($savedTable) {
                            my %stnd;
                            foreach my $val (@noN) {
                                my $std = &clean_query($val, $ns1);
                                $stnd{$std} = 1;
                            }
                            @noN       = sort keys %stnd;
                            $reqSaved += $#noN + 1;
                            if ($doClust) {
                                push @{$savedTable}, [ @row, \@noN ];
                            } elsif ($#noN == -1) {
                                # No input - we should preserve the row anyway
                                push @{$savedTable}, [ @row, '' ];
                            } else {
                                push @{$savedTable}, map { [ @row, $_ ] } @noN;
                            }
                        }
                        if ($limit && !$scramble) {
                            map {$valCount++ unless ($seenVals{$_}++)} @noN;
                        }
                        last if ($limit && $valCount >= $limit);
                    }
                    close VALS;
                    $req = \@list;
                } else {
                    push @preErrs, "Failed to read '$req':  $!";
                    next;
                }
            }
        } elsif ($isDirty) {
            $req = $ad->extract_ids( $req, $ns1, $extrRE );
            # push @{$savedTable}, [$req] if ($savedTable);
        }
        my $vals;
        if (my $r = ref($req)) {
            if ($r eq 'HASH') {
                $vals = [ keys %{$req} ];
            } elsif ($r eq 'ARRAY') {
                $vals = $req;
            } else {
                push @preErrs, "Unable to get values from data structure '$r'";
                $vals = [];
            }
        } elsif (my $spec = $specialValues->{lc($req)}) {
            $vals = $spec;
        } else {
            $vals = [$req];
        }
        my $shouldSave = $savedTable && !$reqSaved;
        for my $v (0..$#{$vals}) {
            my $val = $vals->[$v];
            next unless ($val);
            last if ($limit && !$scramble && $#uniqQ + 1 >= $limit);
            if ($intNSfrm && $val =~ /^\d+$/) {
                $val = sprintf($intNSfrm, $val);
            } elsif ($doStnd || $shouldSave) {
                #$val = &clean_query( $val, $ns1 );
                my $gns = $oneNs1 ? $ns1 : $ad->guess_namespace($val);
                ($val)  = $ad->standardize_id($val, $gns);
                $val  ||= "";
            }
            push @{$savedTable}, [$vals->[$v], $val] if ($shouldSave);
            push @queries, $val;
            push @uniqQ, $val unless ($seen{$val}++);
        }
    }
}



my $sourceID;
if ($args->{EXPAND} && $ns1) {
    if (!$oneNs1) {
        $args->err("I can not -expand with multiple namespaces: ".
                   join('|', @{$ns1}));
    } elsif ($ad->is_namespace($ns1, 'SMI', 'SEQ')) {
        $sourceID = {};
        for my $q (0..$#uniqQ) {
            my $id = $uniqQ[$q];
            $sourceID->{$id} = $id;
            foreach my $rel ($ad->all_children( $id, $ns1, undef, $doWarn),
                             $ad->all_parents( -id => $id, -ns => $ns1,
                                               -warn => $doWarn) ) {
                $sourceID->{$rel} ||= $id;
            }
        }
        @uniqQ = keys %{$sourceID};
    } else {
        $args->err("I do not know how to expand namespace $ns1");
    }
}


if ($scramble) {
    @uniqQ = sort { rand() <=> 0.5 } @uniqQ;
    @uniqQ = splice(@uniqQ, 0, $limit) if ($limit);
    @queries = @uniqQ;
    push @preMsg, "Query list order randomized";
}

if ($ns1 && $ns1 eq 'TAX' && $#uniqQ == 0 && $uniqQ[0] =~ /^common$/i) {
    @queries = @uniqQ = ('Bos taurus', 'Canis lupus familiaris', 'Gallus gallus', 'Homo sapiens', 'Macaca mulatta', 'Macaca fascicularis', 'Mus musculus', 'Pan troglodytes', 'Rattus norvegicus', 'Hepatitis C virus');
    push @preMsg, "Taxonomy request for 'common' returns ".scalar(@uniqQ)." taxae";
}

my $fo  = BMS::FlatOntology->new();

$mode   = 'churn' if ($args->{CHURN});
my $cgiMode = "";
my $act = 0;
if ($mode =~ /(ns|namespace)/) {
    $mode = 'ns';
    $cgiMode = "NamespaceList";
} elsif ($mode =~ /(onto[a-z_]*report)/ || $fmt =~ /(onto[a-z_]*report)/i) {
    my $chk = lc("$mode $fmt");
    $mode = !$mode ? 'convert'
        : $mode =~ /simp/ ? 'simple'
        : $mode =~ /par/ ? 'parents'
        : $mode =~ /child|kid/ ? 'children'
        : 'convert';
    $cgiMode = "Conversion";
    $fmt  = 'html ontoreport';
    $fmt .= ' full' if ($chk =~ /full/);
    $fmt .= ' dad' if ($chk =~ /dad/);
    if ($chk =~ /tidy/) {
        $fo->param('Ignore Unused', 1);
        $fo->param('Root Header', 'clean');
        $fo->param('No Popup', 1);
    } elsif ($chk =~ /no\s*pop/) {
        $fo->param('No Popup', 1);
    }
} elsif ($mode =~ /(integer)/) {
    $mode = 'integer';
    $act  = -1;
} elsif ($mode =~ /(regexp|express)/) {
    $mode = 'regexp';
    $act  = -1;
} elsif ($mode =~ /(assign|onto)/) {
    $mode = 'assign';
    $cgiMode = "Assignments";
} elsif ($mode =~ /(desc)/) {
    $mode = 'desc';
    $cgiMode = "Description";
} elsif ($mode =~ /(cloud)/) {
    $mode = 'cloud';
    $cgiMode = "Clouds";
} elsif ($mode =~ /(child|kid)/) {
    $mode = 'children';
} elsif ($mode =~ /(map|coord)/) {
    $mode = 'map';
} elsif ($mode =~ /(overlap)/) {
    $mode = 'overlap';
} elsif ($mode =~ /(par)/) {
    $rootPar = 1 if ($mode =~ /(root)/);
    $mode = 'parents';
} elsif ($mode =~ /(simpl|basic)/) {
    $mode = 'simple';
    $cgiMode = "SimpleAnnotation";
} elsif ($mode =~ /(url|link|href)/) {
    $mode = 'link';
} elsif ($mode =~ /(churn)/) {
    $mode = 'churn';
} elsif ($mode =~ /(look|text|txt)/) {
    $mode = 'lookup';
    if (my $tdat = $args->val(qw(text desc descr description))) {
        @queries = ref($tdat) ? @{$tdat} : ($tdat);
    }
    $cgiMode = "TextLookup";
} else {
    $mode = 'convert';
    $cgiMode = "Conversion";
}

if ($setReq) {
    @uniqQ = @queries = split(/\s*[\n\r\t\,]+\s*/, $setReq);
    $setReq = [ @uniqQ ];
    $ns1 = 'SET';
}

if ($mode eq 'ns') {
    $ns1 ||= 'NS';
    $act = -1;
} elsif ($mode eq 'churn') {
    $act = -1;
    $noSafeAge = 1;
} elsif ($#queries != -1) {
    $act = 1;
} elsif ($ltnsupd) {
    $act = 1;
    $mode = 'convert';
}


if ($jsonType) {
    $doBench        = 0;
    $vb             = 0;
}

# There are multiple ways to specify special columns, make sure we catch all:
$specialCol{sym_out}  ||= $fmtFlags{Symbol};
$specialCol{seq_out}  ||= $fmtFlags{Sequence};
$specialCol{tax_out}  ||= $fmtFlags{Species};
$specialCol{desc_out} ||= $fmtFlags{Described};
$specialCol{links}    ||= $fmtFlags{Links};
# $specialCol{sortchr}  ||= $fmtFlags{SortChr};
my %specialArgMap =
    ( sym_out     => 'addsym',
      sym_in      => 'symin',
      set         => 'addset',
      seq_out     => 'addseq',
      desc_out    => 'adddesc',
      desc_in     => 'descin',
      sortchr     => 'sortchr',
      tax_out     => 'addtax',
      links       => 'addlink',
      inventory   => 'addinv',
      dryinv      => 'dryinv',
      specificity => 'specificity',
      chemdetail  => 'chemdetail',
      updated     => 'update adddate',
      qry_src     => 'qrysrc',
      tax_in      => 'taxin', );
while (my ($sc, $arga) = each %specialArgMap) {
    next if ($specialCol{$sc});
    $specialCol{$sc} ||= $args->val($arga);
}
$specialCol{desc_out} = 0 if ($mode =~ /^(lookup|assign|desc)$/);

$sortKey = 'term_out' if ($fmtFlags{Cluster} && !$sortKey);

push @css, "http://bioinformatics.bms.com/biohtml/css/genaccService.css"
    if ($gaClass && $gaClass =~ /gatab/);

push @css, "http://bioinformatics.bms.com/biohtml/css/flatontology.css"
    if ($fmtFlags{OntologyReport} || $fmtFlags{Structured});

$args->url_callback( sub {
    my $path = shift;
    return undef unless (defined $path);
    if ($path =~ /^\/stf\/(.+)/) {
        return "/$1";
    } elsif ($path =~ /^\/home\/(.+)\/public_html\/(.+)/) {
        return "/~$1/$2";
    } elsif ($path =~ /^\/home\/(tilfordc\/people\/.+)/) {
        return "http://bioinformatics.bms.com/~$1";
    } elsif ($path =~ /^\/docs\/(.+)/) {
        return "http://biodocs.pri.bms.com/$1";
    }
    if ($path =~ /^\/stf(.+)/) { return $1; }
    return undef;
});

my $targNs  = [];
$targNs     = ref($ns2) ? $ns2 : [ split(/\s*[\,\t\n\r]+\s*/, $ns2) ] if ($ns2);

# printf("<pre>%s</pre>", $args->to_text());die;
if (!$act || (!$nocgi && $args->{SHOWOPTS}) || $noRun) {
    if ($nocgi) {
        my $pnum = 0;
        map { $pnum++ } $args->all_keys( -nodefault => 1 );
        &show_help() if ($pnum <= 2 && !$quiet);
    } else {
        &HTML_INTERFACE() unless ($args->{NOOPTS});
    }
    unless ($#preErrs == -1) {
        &set_mime();
        $args->err("Errors parsing input:", @preErrs);
    }
    # &preprint($args->to_text());
    # &prebranch(\@queries);
    &finish_up;
}

if (my $tab = $args->{ORACLE}) {
    $fmtFlags{Oracle} = 1;
    $oraTable ||= $tab;
}

$args->write_log();

if ($nocgi) {
    $args->shell_coloring( );
} else {
    my $mime = 'plain';
    if ($fmtFlags{Atom}) {
        $mime = 'xml';
    } elsif ($jsonType) {
        $mime = 'application/json';
    } elsif ($fmtFlags{HTML}) {
        $mime = 'html';
    } elsif ($fmtFlags{Excel}) {
        $mime = '';
    }
    if ($mime) {
        $args->_stop_intercept();
        &set_mime( $mime );
    }
    if ($fmtFlags{Atom} || $jsonType) {
        $SIG{__WARN__} = sub { print STDERR join("\n", @_) };
    }
}

if ($args->{HELP} || $args->{H}) {
    &show_help();
    &finish_up if ($nocgi);
}    

# Some excel formatting paramteters:
my $specMax   = 20;
my $colScale  = 10;
my $usingPerc = ($fmtFlags{AsPercent} ||
                 $mode eq 'map' || $mode eq 'overlap') ? 1 : 0;

if ($fmtFlags{Atom}) {
    $tiFormat = "yyyy-mm-dd hh24:mi:ss";
    %specialCol = ( desc_out => 1, updated => 1 );
} elsif ($fmtFlags{SetFile} || $fmtFlags{GSEA}) {
    my $useNS = ($mode eq 'lookup' || $mode eq 'simple') ? $ns1 : $ns2;
    $ns2 = $ad->namespace_name($useNS);
    if ($mode eq 'cloud') {
        ($qryCol, $objCol) = ('term_out', 'term_in');
    } else {
        push @preErrs, "If you specify SetFile format you must specify a single value for -ns2" unless ($ns2 || $mode =~ /^map|overlap$/);
    }
    my $doSymbol = $ad->is_namespace($ns2, "AP","AR","AL") ? 1 : 0;
    $fmtFlags{TSV} = 1;
    $fmtFlags{Distinct} = 1;
    %specialCol = ( desc_out => 1 );
    $specialCol{sym_out} = 1 unless ($fmtFlags{GSEA});
    $fmtFlags{SetFile} = 1;
}

$args->err(@preErrs) unless ($#preErrs == -1);

$args->msg(@preMsg)  if ($#preMsg != -1 && $vb);

my $ldap    = $ENV{'REMOTE_USER'} || $ENV{'LDAP_USER'} || $ENV{'USER'} || $ENV{'LOGNAME'} || 0;
my $cc      = $args->val('cleancol');
my $doBuild = $args->val(qw(rebuild));
my $redo    = $doBuild || $args->{REDONULL};
$fork       = 0 if ($nullRows || ($fork && $fork == 1));
my $head    = $fmtFlags{Header} || $args->val(qw(header head)) || 
    (($fmtFlags{Text} || $fmtFlags{Excel} || $fmtFlags{TiddlyWiki} ||
      (!$nocgi && !$fmtFlags{List}) || $hasHeader) ? 1 : 0);
$head       = [] if ($head && $head eq '1');
$head       = 0 if ($fmtFlags{NoHeader});

my $commonColumns;
if ($cc) {
    if (ref($cc)) {
        $cc = 0;
    } elsif ($cc =~ /auto/i) {
        if ($fmtFlags{Text} || $fmtFlags{TiddlyWiki}) {
            $cc = 'common';
        } else {
            $cc = 0;
        }
    }
    $commonColumns = $cc =~ /share|common/i ? [] : undef;
}

my @comArgs;

$fo->param('minimize', $args->{MINIMIZE});
$fo->param_method('desc',    \&desc_for_node);
$fo->param_method('parents', \&parents_for_node);
$fo->param_method('detail', \&details_for_node);

# Debugging flags
$ad->{TRACE}     = $args->{TRACE};
$ad->{CLOUDWARN} = $args->{CLOUDWARN};

$age  = $ad->age( $age );
$cAge = $ad->cloud_age( $cAge );
if ($vb) {
    my %ages;
    push @{$ages{$age}}, "Basic Age";
    push @{$ages{$cAge}}, "Cloud Age";
    my @units = qw(day hour minute second);
    my @steps = (24,60,60);
    foreach my $ag (sort { $a <=> $b } keys %ages) {
        my $what = join(" + ", @{$ages{$ag}});
        unless ($ag) {
            $args->msg("No age filter for $what") if ($doWarn);
            next;
        }
        my $i = 0;
        while ($#units > $i && $ag < 1) {
            $ag *= $steps[$i];
            $i++;
        }
        $args->msg(sprintf("Age limit of %.2f %s%s for %s", $ag, $units[$i],
                           $ag == 1 ? '' : 's', $what)) if ($doWarn);
    }
}

if (my $treq = $args->{TAXA} || $args->{SPECIES}) {
    if (my $mt = $ad->tracker) {
        my @tax = $mt->get_species($treq);
        if ($#tax == 0) {
            $intersect = $tax[0]->name;
            $intNS     = 'TAX';
        } elsif ($#tax == -1) {
            $args->err("Failed to find any taxae for '$treq'");
        } else {
            $args->err("Ignoring multiple taxae for '$treq':",
                       map { $_->name } @tax);
        }
    } else {
        $args->err("MapTracker is not available, can not apply taxa filter");
    }
} elsif (my $pi = $args->{POPULARINT}) {
    if ($pi =~ /^(\S+)\s+(.+)$/) {
        ($intNS, $intersect) = ($1, $2);
    } else {
        $args->err("Could not interpret popular intersection '$pi'");
    }
}


my $genomeBuild = $args->val(qw(build));
if ($genomeBuild && $genomeBuild =~ /^\d+$/) {
    # Treat integer genome builds as human build numbers
    $genomeBuild = $genomeBuild < 37 ? "NCBI$genomeBuild" : "GRCh$genomeBuild";
}

if ($fmtFlags{BED} && $mode ne 'map') {
    $args->msg('[!]', "BED format is only relevant in Map mode - no action");
    @queries = ();
} elsif ($mode eq 'convert') {
    @comArgs = ( 
                 # -bestonly     => $bestOnly, # do after fork if needed
                 -ageonce      => $args->{AGEONCE},
                 -assurenull   => $asn,
                 -auth         => $authReq,
                 -directonly   => $doReq,
                 -dumpsql      => $dumpsql,
                 -explainsql   => $exSql,
                 -ignorecase   => $igCase,
                 -intersection => $intersect,
                 -intns        => $intNS,
                 -keepnull     => $kn,
                 -links        => $addLinks,
                 -min          => $minScore,
                 -nolist       => $args->{NOLIST},
                 -nonull       => $args->{NONULL},
                 -nullrows     => $nullRows,
                 -nullscore    => $nullSc,
                 -progress     => $args->{CONVERTPROGRESS},
                 -redonull     => $redo,
                 -uselist      => $args->val(qw(uselist forcelist)),
                 -warn         => $doWarn,
                 );
} elsif ($mode eq 'map' || $mode eq 'overlap') {
    @comArgs = ( -build        => $genomeBuild,
                 -bestbuild    => $args->val(qw(bestbuild)),
                 # -limit        => $limit,
                 -warn         => $doWarn,
                 -current      => $args->{CURRENT},
                 -update       => $args->{UPDATEMAP},
                 -distance     => $maxDist,
                 -slowsql      => $slowsql,
                 -min          => $minScore,
                 -howbad       => $howBad,
                 -submin       => $args->val('submin'),
                 -subbad       => $args->val('subbad'),
                 -nonull       => $kn ? 0 : 1,
                 -nullrows     => $nullRows,
                 -dumpsql      => $dumpsql, );
    if ($mode eq 'overlap') {
        push @comArgs, ( -ns2      => $ns2,
                         -nonskids => $args->val('nonskids'),
                         -keep     => $args->val('keep'),
                         -toss     => $args->val('toss'),
                         -min      => $minScore);
    } elsif ($fmtFlags{BED}) {
        $cleanChr = 0;
 
    }
} elsif ($mode eq 'assign') {
    @comArgs = ( -min          => $minScore,
                 -ageonce      => $args->{AGEONCE},
                 -warn         => $doWarn,
                 -ec           => $authReq,
                 -parentage    => $args->{PARENTAGE}, );
} elsif ($mode eq 'desc') {
    @comArgs = ( -redonull     => $redo,
                 -warn         => $doWarn, 
                 -dumpsql      => $dumpsql);
} elsif ($mode eq 'churn') {
    @comArgs = ( -ns1          => $ns1,
                 -null         => $nullRows || $kn || $redo || $args->{NULL},
                 -id           => $#queries == 0 ? $queries[0] : undef,
                 -verbose      => $vb, 
                 -limit        => $limit,
                 -dumpsql      => $dumpsql,
                 -discard      => $fmtFlags{Null},
                 -description  => $args->{DESCRIPTION},
                 -warn         => $doWarn,
                 -time         => $args->{TIME},
                 -currentfile  => "/tmp/currentChurn.txt",
                 );
}
push @comArgs, ( -age => $age,
                 -cloudage => $cAge );

my $rNS;
if ($oneNs1) {
    unless ($mode eq 'churn') {
        my $nsOrig = $ns1;
        $ns1 = $ad->namespace_token($ns1);
        $args->err("The namespace -ns1 '$nsOrig' was not understood")
            unless ($ns1);
    }
} elsif ($mode !~ /^(simple|lookup)$/) {
    # Input namespace was not defined
    my $tot = $#uniqQ + 1;
    my $nss = $ns1;
    if ($tot) {
        # Guess the namespaces
        $ns1 = {}; # Entries clusterd by namespace
        $rNS = {}; # Reverse lookup (id -> NS)
        $args->msg("Guessing namespace for $tot requests") if ($doWarn);
        my (%gns, @residual);
        for my $q (0..$#uniqQ) {
            my $id = $uniqQ[$q];
            my ($std, $g) = &clean_query($id, $nss);
            push @{$ns1->{$g}}, $id;
            $rNS->{$std} = $rNS->{$id} = $g;
            $uniqQ[$q]   = $std;
        }
    }
}

my ($rows, @corder, $forkDesc);
if ($fork) {
    if ($fork > 20) {
        if ($ldap eq 'tilfordc') {
            # $args->msg('[ADMIN]', "Running at high fork level $fork");
        } elsif ($ldap eq 'riosca') {
            $args->msg('[ADMIN]', $fork > 40 ? "Bad Carlos. Run no more than 40 forks" : "Forking allowed at $fork. Please behave");
        } else {
            $args->death("[NO]", "You have requested excessive fork level $fork", "Too many forks cause our systems to crash. This has been demonstrated empirically.","Please talk to Charles if you have a business need for more processes");
        }
    }
    if (my $forkFile = &fork_it($fork)) {
        if (!-e $forkFile) {
            # $args->msg("[!]", "Forkfile not found", $forkFile);
        } elsif (open(FORKF, "<$forkFile")) {
            $rows = [];
            while (<FORKF>) {
                my @row = map { defined $_ ? $_ :  "" } split(/\t/, $_);
                # For some reason, if I tried to strip the newline from the
                # end of the line first, then split would completely ignore
                # a run of trailing tabs. 
                $row[-1] =~ s/[\n\r]+$//;
                push @{$rows}, \@row;
            }
            close FORKF;
            $args->msg(($#{$rows} + 1)." rows read from fork file", $forkFile)
                if ($vb);
            unlink($forkFile);
            if ($bestOnly) {
                # Filter best rows after forked results have been collated:
                $rows = $ad->best_only( $rows, $bestOnly, 3, [5,6,1], $nullSc);
            }
            my $descFile = $forkFile . ".desc";
            if (-e $descFile) {
                if (open(DESCF, "<$descFile")) {
                    $forkDesc = {};
                    while (<DESCF>) {
                        s/[\n\r]+$//;
                        my ($id, $ns, $desc) = split(/\t/);
                        $forkDesc->{$id}{$ns} ||= $desc || "";
                    }
                    close DESCF;
                    unlink($descFile);
                } else {
                    $args->msg("Failed to read forked descriptions",
                               $descFile, $!);
                }
            }
        } else {
            $args->err("Failed to read forkFile output", $forkFile, $!);
        }
    }
    #$args->msg("Forking benchmarks", $ad->showbench
    #           ( -minfrac => $doBench)) if ($doBench);
    if ($doPop) {
        $args->msg("Forking complete") if ($doWarn);
        &finish_up;
    }
    $ad = &getAccessDenorm();
}

$primaryChild = 1;

$ad->bench_start('Initialize');
my $outFH = &output_fh;

my ($paramFailed, @passedParams);

my $jsonp = $args->val(qw(callback jsonp));

$ad->process_url_list
    ( $args->{URLS} || "/stf/biohtml/stylesheets/GenAccServiceURLs.txt" ) ;
if ($fmtFlags{DAD}) {
    &start_DAD;
} elsif ($jsonType) {
    if ($fmtFlags{JSON}) {
        my $prfx = ($jsonp ? $jsonp."(" : "" ). "{";
        print $outFH $prfx;
    }
} elsif ($fmtFlags{HTML}) {
    if ($fmtFlags{Full}) {
        &standard_head;
        print $outFH &outstyles;
        if ($fmtFlags{CanvasXpress}) {
            my $hxtra = "";
            my $url = "http://xpress.pri.bms.com/JAVASCRIPT/canvas/js/canvasXpress.min.js";
            my $sty = $url; $sty =~ s/\/js\/[^\/]+$/\/css\/canvasXpress.css/;
            if ($cxDebug) {
                $url =~ s/\.min\./\.debug\./;
                $hxtra .= "  <script type='text/javascript' src='http://xpress.pri.bms.com/JAVASCRIPT/canvas/js/canvasXpress.public.min.js'></script>\n";
            }
            my $ieKludge = $url;
            $ieKludge =~ s/\/[^\/]+$//;
            print $outFH $hxtra . <<EOF;
  <!--[if IE]>
    <script type='text/javascript' src='$ieKludge/excanvas.js'></script>
    <script type='text/javascript' src='$ieKludge/extext.js'></script>
  <![endif]-->
  <link rel="stylesheet" href="$sty" type="text/css" />
  <script type='text/javascript' src='$url'></script>
EOF

        }
        print $outFH "\n</head><body>\n";
    }
    $linkWP = 'link' unless (defined $linkWP);
}

my $warnCount = 0;
my $warnSize  = $args->val(qw(warnsize)) || 1000;
my ($idxIn, $idxOut, ) = (5,0);
$ad->bench_end('Initialize');


if ($fmtFlags{CommandLine}) {
    # User just wants to see command line command
    $rows = [[ "genacc_service.pl ".$args->command_line( -nodef => 1) ]];
    my $cn  = $sortKey = $objCol = "command_line";
    @corder = ($cn);
    $hdata->{$cn} = {
        label => "Command Line",
    };
    my @fs = keys %fmtFlags;
    $fmtFlags{List} = 1 if ($#fs < 1);
} elsif ($ltnsupd) {
    @corder = qw(term_out ns_out auth matched ns_between term_in ns_in);
    $rows = $ad->update_EVERYTHING_to_LT( $ltnsupd, $age, $doWarn);
} elsif ($mode eq 'simple') {
    $ad->bench_start('Simple Mode');
    my %seen;
    $rows = [];
    my $noStnd = $args->val('asis');
    my $msg    = $noStnd ? '' : " and standardized";
    my $keepOrig = $args->val('keeporiginal');
    foreach my $req (@queries) {
        my $id = $req;
        my $ns;
        if ($noStnd) {
            $ns = $ns1 ? $ns1 : $ad->guess_namespace($req);
        } else {
            ($id, $ns) = &clean_query($req, $ns1);
        }
        next if ($fmtFlags{Distinct} && $seen{$id}++);
        my @row = ($id, $ns);
        push @row, $req if ($keepOrig);
        push @{$rows}, \@row;
        printf(STDERR "%s%30s [%4s] => Extracted$msg", 
               ++$warnCount % $warnSize ?
               "\r" : "\n", $id, $ns) if ($doWarn);
    }
    my %nss;
    map { $_->[1] = $nss{$_->[1]} ||= $ad->namespace_name($_->[1]) } @{$rows};
    $idxOut = 0;
    $idxIn  = $keepOrig ? 2 : 0;
    ($qryCol, $objCol) = $keepOrig ? ('term_in', 'term_out') : ('', 'term_in');
    $sortKey = $objCol unless ($sortKey);
    
    @corder = $keepOrig ? qw(term_out ns_in term_in) : qw(term_in ns_in);
    $ad->bench_end('Simple Mode');
} elsif ($mode eq 'ns') {
    $ad->bench_start('Namespace Mode');
    ($idxIn, $idxOut) = (-1,0);
    my $known = $ad->known_conversions('methName');
    my @nsin;
    if ($ns1 ne 'NS') {
        my $nreq = $ad->namespace_token($ns1);
        push @nsin, $nreq if ($nreq);
        $ns1 = 'NS';
    } else {
        map { push @nsin, $_ if $_ } map { $ad->namespace_token($_) } @queries;
    }
    my $n2filt = ($#{$targNs} == -1) ? undef :
    { map { $ad->namespace_token($ns2) || '' => 1 } @{$targNs} };
    my @names  = $ad->all_namespace_names();
    my @toks   = map { $ad->namespace_token($_) } @names;
    @nsin      = @toks if ($#nsin < 0); # Use all namespaces
    
    if ($fmtFlags{Structured}) {
        my %struct;
        foreach my $n1 (@nsin) {
            my $ns2s = $known->{$n1};
            my $n1n = $ad->namespace_name( $n1 );
            map { $struct{ $n1n }{'Known Mappings'}{$ad->namespace_name($_)} =
                  { 1 => {$ns2s->{$_} => 1 }} if (!$n2filt || $n2filt->{$_})
              } keys %{$ns2s};
        }
        &format_struct(\%struct);
    }

    my %nsouts = map { $_ => 1 } map { keys %{$known->{$_} || {}} } @nsin;
    @toks = sort { lc($ad->namespace_name($a)) cmp
                       lc($ad->namespace_name($b)) } keys %nsouts;
    @toks = sort keys %{$n2filt} if ($n2filt);

    $head = [ { label => 'Source NameSpace',
                desc => 'The starting point in a conversion step' },
              map { { label => $_, desc => "A destination namespace for a conversion" } } @toks ];
    $hdata = undef;
    $rows = [];
    my $tfrm = $fmtFlags{TiddlyWiki} ? ' @@%s %s@@ ' : 
        $fmtFlags{Text} ? '%4s %-4s' : '%s %s';
    foreach my $n1 (@nsin) {
        my $name = $ad->namespace_name($n1);
        my @row  = ( "$name [$n1]");
        my $num  = 0;
        foreach my $n2 (@toks) {
            my $tag = '';
            if (my $meth = $known->{$n1}{$n2}) {
                $tag = sprintf($tfrm,$n1,$n2);
                if ($args->{ADDFUNC} || $args->{ADDMETH}) {
                    $meth =~ s/\(.+?\)/\(\)/g;
                    $tag .= " $meth";
                }
                $num++;
            }
            push @row, $tag;
        }
        push @{$rows}, \@row if ($num);
    }
    if ($#{$head} > 2 && $#{$rows} == 0) {
        # Transpose the table
        my @trans;
        for my $i (1..$#{$rows->[0]}) {
            my $n2   = $head->[$i]{label};
            my $name = $ad->namespace_name($n2);
            push @trans, [ "$name [$n2]", $rows->[0][$i] ];
        }
        my $sns  = 'UNK';
        if ($rows->[0][0] =~ /\[(.+)\]/) { $sns = $1 }
        $head = [ {label => "Target Namespace"}, { label => $sns } ];
        $rows = \@trans;
    }
    if ($fmtFlags{List}) {
        $rows = [ map { [$ad->namespace_name($_)] } keys %{$known} ];
        $head = [ {label => 'Source Namespace'} ];
        @corder = qw(term_out);
    }
    $ad->bench_end('Namespace Mode');
} elsif ($fmtFlags{OntologyReport} && $ns1 && $ns1 eq 'GO' &&
         $ns2 && $ns2 eq 'GO') {
    $ad->bench_start('Ontology Mode');
    $rows = [map { [$_,$_,1,''] } @uniqQ];
    @corder = qw(term_out term_in matched auth);
    $ad->bench_end('Ontology Mode');
} elsif ($mode eq 'churn') {
    $ad->bench_start('Churning');
    my $what = lc($args->{CHURN} || 'conversions');
    if ($what =~ /desc/) {
        $rows = $ad->churn_descriptions
            ( @comArgs,
              -ignoredep => $args->{IGNOREDEP},
              );
        @corder = qw(term_in ns_in updated);
    } else {
        $rows = $ad->churn_conversions
            ( @comArgs,
              -ns2 => $ns2, );
        @corder = qw(term_in ns_in ns_out updated);
    }
    $ad->bench_end('Churning');
} elsif ($mode eq 'convert') {
    $ad->bench_start('Convert Mode');
    # $best only added here to not cause potential problems with forking
    my @xtra = ( -adddate    => $specialCol{updated},
                 -bestonly   => $bestOnly,
                 -timeformat => $tiFormat, );
    if ($nullRows) {
        @corder = qw(term_out);
        ($idxIn, $idxOut) = (0,0);
        $hdata->{term_out}{label} = "Missing Data";
        $hdata->{term_out}{desc}  = "These are query entries that did NOT have any output terms mapped to them, within your other query criteria. This filter is triggered by the -nullonly option.";
    } elsif ($fmtFlags{List} && !$fmtFlags{NoHits}) {
        @corder = qw(term_out);
        @xtra = ( -cols     => ['term_out'],
                  -bestonly => $bestOnly,
                  );
    } else {
        @corder = qw(term_out ns_out auth matched ns_between term_in ns_in);
        push @corder, 'updated' if ($specialCol{updated});
    }
    &add_column_dependencies( \@corder );
    my %hasCol = map { $corder[$_] => $_ + 1 } (0..$#corder);
    my $skipSort = $hasCol{&_standardize_column($sortKey || 'term_out')} ? 1:0;
    if ($rows) {
        # Captured during forking
    } elsif (!$ns2) {
        $args->msg("[!!]","Please choose an Output Namespace (-ns2) for conversions");
        &HTML_INTERFACE() unless ($nocgi);
        &finish_up;
    } elsif ($ns1 && ref($ns1)) {
        $rows = [];
        while (my ($n1, $list) = each %{$ns1}) {
            my $targs = &_targ_ns($targNs, $n1);
            foreach my $n2 (@{$targs}) {
                $args->msg(sprintf
                           (" %d entr%s [%s] -> [%s]", $#{$list} + 1,
                            $#{$list} == 0 ? 'y' : 'ies', 
                            $ad->namespace_token($n1),
                            $ad->namespace_token($n2))) if ($doWarn);
                my @xtraXtra;
                my $isChrMap = $ad->namespace_token($n2) eq 'CHR' ? 1 : 0;
                if ($isChrMap && $genomeBuild) {
                    push @xtraXtra,( -auth => $authReq ?
                                     "$authReq $genomeBuild" : $genomeBuild );
                }
                my $r = $ad->convert( -id  => $list,
                                      -ns1 => $n1,
                                      -ns2 => $n2,
                                      @xtra,
                                      @comArgs,
                                      @xtraXtra );
                next unless ($r);
                if ($isChrMap && $cleanChr && $hasCol{term_out}) {
                    my $ind = $hasCol{term_out} - 1;
                    for my $i (0..$#{$r}) {
                        if (my $chr = $r->[$i][$ind]) {
                            if ($chr =~ /^[^\.]+\.[^\.]+\.(.+)\.[^\.]+\:(.+)$/) {
                                $r->[$i][$ind] = "$1:$2";
                            }
                        }
                    }
                }
                $r = [ map { [ $_ ] } @{$r} ] if ($nullRows);
                push @{$rows}, $skipSort ? @{$r} : &fancySort($r, 0);
            }
        }
    } else {
        $rows = [];
        my $targs = &_targ_ns($targNs, $ns1);
        foreach my $n2 (@{$targs}) {
            $args->msg(sprintf
                       (" %d entr%s [%s] -> [%s]", $#uniqQ + 1,
                        $#uniqQ == 0 ? 'y' : 'ies', $ad->namespace_token($ns1),
                        $ad->namespace_token($n2))) if ($doWarn);
            my $r = $ad->convert( -id  => \@uniqQ,
                                  -ns1 => $ns1,
                                  -ns2 => $n2,
                                  @xtra,
                                  @comArgs );
            next unless ($r);
            $r = [ map { [ $_ ] } @{$r} ] if ($nullRows);
            push @{$rows}, $skipSort ? @{$r} : &fancySort($r, 0);
        }
    }
    if ($#{$rows} != -1 && $#corder < $#{$rows->[0]}) {
        # We need to pop off the 'member' row
        map { pop @{$_} } @{$rows};
    }
    $ad->bench_end('Convert Mode');
} elsif ($mode eq 'overlap') {
    @corder = qw(term_in term_out term_share matched_in matched strand distance start_in end_in start_out end_out sub_vers howbad);
    push @corder, 'qry_src' if ($specialCol{qry_src});
    $hdata->{matched}    = {
        label => "Output Score",
    };
    $hdata->{matched_in} = {
        label => "Input Score",
    };
    $hdata->{sub_vers}   = {
        label => "Build",
    };
    
    unless ($rows) {
        $ad->bench_start('Overlap Mode');
        ($idxIn, $idxOut) = (0,1);
        my ($okInt, $nsInt) = &overlap_int_params();

        my $hash    = ($ns1 && ref($ns1)) ? $ns1 : { $ns1 || "" => \@uniqQ };
        my %idHash;
        while (my ($n1, $list) = each %{$hash}) {
            map { push @{$idHash{$_}}, $n1 } @{$list};
        }
        while (my ($qid, $narr) = each %idHash) {
            push @{$rows}, &get_overlaps( $qid, $narr, $okInt, $nsInt );
        }
        $ad->bench_end('Overlap Mode');
    }
} elsif ($mode eq 'map') {
    $ad->bench_start('Map Mode');
    ($qryCol, $objCol) = ('term_in', 'term_out');
    @corder = map { &_standardize_column($_) } $ad->dbh->column_order('mapping');

 # qw(term_in term_out matched auth strand start_in end_in start_out end_out vers_in vers_out set tax_out ns_in ns_out updated loc_in loc_out len_in howbad);
    ($idxIn, $idxOut) = (0,1);
    if ($rows) {
        # Data already recovered from forking
    } elsif ($ns1 && ref($ns1)) {
        $rows = [];
        while (my ($n1, $list) = each %{$ns1}) {
            my $r = $ad->mappings( -id => $list,
                                   -ns => $n1,
                                   @comArgs );
            push @{$rows}, @{$r};
        }
    } else {
        $rows = $ad->mappings( -id => \@uniqQ,
                               -ns => $ns1,
                               @comArgs );
    }
    $ad->bench_end('Map Mode');
} elsif ($mode eq 'assign') {
    $ad->bench_start('Assignment Mode');
    ($idxIn, $idxOut) = (0,1);
    if ($ns1 && ref($ns1)) {
        $rows = [];
        while (my ($n1, $list) = each %{$ns1}) {
            my $r = $ad->assignments( -id  => \@uniqQ,
                                      -ans => $n1,
                                      -ons => $ns2,
                                      @comArgs);
            push @{$rows}, @{$r};
        }
    } else {
        $rows = $ad->assignments( -id  => \@uniqQ,
                                  -ans => $ns1,
                                  -ons => $ns2,
                                  @comArgs, );
    }
    @corder = qw(term_in term_out auth matched ns_in ns_out subset desc_in desc_out parent age);
    map { $_->[-1] = int(0.5 + 10 * $_->[-1])/10 if ($_->[-1]) } @{$rows};
    $ad->bench_end('Assignment Mode');
} elsif ($mode eq 'children' || $mode eq 'parents') {
    $ad->bench_start('Genealogy Mode');
    $rows = [];
    my $nstruct = $ns1;
    if (!$ns1) {
        $nstruct = { '' => \@uniqQ };
    } elsif (!ref($ns1)) {
        $nstruct = { $ns1 => \@uniqQ };
    }
    my ($isKid, $itd, $td);
    ($qryCol, $objCol, $td, $itd, $isKid, $idxIn, $idxOut) = 
        ($mode eq 'children') ?
        ('parent', 'child', 'Child',  'Parent', 1,0,1) :
        ('child', 'parent', 'Parent', 'Child',  0,1,0);
    $sortKey ||= ($mode eq 'children') ? 'child' : 'parent';
    my $minDep = $args->val(qw(mindepth depth level));
    my $maxDep = $args->val(qw(maxdepth depth level));
    while (my ($n1, $list) = each %{$nstruct}) {
        my $nn = $ad->namespace_name($n1);
        foreach my $node (@{$list}) {
            my ($id) = $ad->standardize_id($node, $n1);
            if ($isKid) {
                my $data = $ad->all_children($node,$n1, undef, $doWarn);
                while (my ($kid, $ddA) = each %{$data}) {
                    for (my $di = 0; $di < $#{$ddA}; $di += 2) {
                        my ($depth, $rels) = ($ddA->[$di], $ddA->[$di+1]);
                        next if ($minDep && $depth < $minDep);
                        next if ($maxDep && $depth > $maxDep);
                        push @{$rows}, [ $kid, $id, $depth, $nn, join(',', @{$rels}) ];
                    }
                }
            } elsif ($rootPar) {
                my @roots = $ad->root_parent( $node, $n1 );
                push @{$rows}, map { [ $_, $id, 'root', $nn ] } @roots;
            } else {
                my $data = $ad->all_parents
                    ( -id => $node, -ns => $n1, -warn => $doWarn);
                while (my ($par, $ddA) = each %{$data}) {
                    for (my $di = 0; $di < $#{$ddA}; $di += 2) {
                        my ($depth, $rels) = ($ddA->[$di], $ddA->[$di+1]);
                        next if ($minDep && $depth < $minDep);
                        next if ($maxDep && $depth > $maxDep);
                        push @{$rows}, [ $par, $id, $depth, $nn, join(',', @{$rels}) ];
                    }
                }
            }
        }
    }
    $hdata->{desc_out} = {
        label => "$td Description",
    };
    $hdata->{desc_in}  = {
        label => "$itd Description",
    };
    ($idxIn, $idxOut) = (1,0);
    @corder = $isKid ? qw(child parent) : qw(parent child);
    push @corder, qw(depth ns_out rel);
    $ad->bench_end('Genealogy Mode');
} elsif ($mode eq 'desc') {
    $ad->bench_start('Description Mode');
    ($idxIn, $idxOut) = (1,0);
    unless ($rows) {
        $rows = [];
        my (%byNS, %lu);
        map { push @{$byNS{($rNS ? $rNS->{$_} : $ns1) || ""}}, $_ } @uniqQ;
        while (my ($ns, $ids) = each %byNS) {
            my $descs = $ad->bulk_description( -ns => $ns, -ids => $ids,
                                               @comArgs );
            for my $i (0..$#{$ids}) {
                push @{$rows}, [$descs->[$i], $ids->[$i] ];
            }
        }
        #foreach my $id (@uniqQ) {
        #    my $ns   = $rNS ? $rNS->{$id} : $ns1;
        #    my $desc =  &_get_desc($id, $ns);
        #    push @{$rows}, [$desc, $id];
        #}
    }
    @corder = qw(desc_in term_in);
    ($qryCol, $objCol) = ('', 'term_in');
    $ad->bench_end('Description Mode');
} elsif ($mode eq 'lookup') {
    $ad->bench_start('Lookup Mode');
    ($idxIn, $idxOut) = (2,0);
    $rows = $ad->description_lookup
        ( -desc => \@queries, -ns => $ns1arg, -limit => $limit,
          -int  => $intersect, -intns => $intNS, -dumpsql => $dumpsql,
          -split => $args->{SPLIT}, -join => $args->{JOIN});
    @corder = qw(term_out ns_out desc_in);
    delete $specialCol{desc_out};
    delete $specialCol{desc_in};
    if ($colReq) {
        for my $c (0..$#{$colReq}) {
            my $col = $colReq->[$c];
            if ($col eq 'term_in') {
                $colReq->[$c] = 'term_out';
            } elsif ($col eq 'ns_in') {
                $colReq->[$c] = 'ns_out';
            } elsif ($col eq 'desc_out') {
                $colReq->[$c] = 'desc_in';
            } 
        }
    }
    $ad->bench_end('Lookup Mode');    
} elsif ($mode eq 'cloud') {
    $ad->bench_start('Cloud Mode');
    my @sdat;
    my (@edges, @seeds);
    foreach my $id (@uniqQ) {
        my $ns = $rNS ? $rNS->{$id} : $ns1;
        if ($ad->is_namespace($ns, 'AP', 'AR', 'AL')) {
            push @seeds, [$id, $ns];
        } else {
            my @con;
            foreach my $cns (qw(AL LL)) {
                # We try both AL and the more specific LL
                # This is to avoid restrictions encountered in chains
                # from AR -> AL (which is considered canonical)
                push @con,   $ad->convert( -id => $id,
                                     -ashash => 1,
                                     -nonull => 1,
                                     -ns1 => $ns, 
                                     -ns2 => $cns );
            }
            my %uniq = map { $_->{term_out} => $_->{ns_out} } @con;
            push @seeds, map { [ $_, $uniq{$_} ] } keys %uniq;
            push @edges, @con;
        }
    }

    if ($#seeds != -1) {
        my %types;
        if ($args->val(qw(smallcloud))) {
            foreach my $seed (@seeds) {
                my $type = 'GeneCluster';
                my $ns   = $seed->[1];
                if ($ad->is_namespace($ns, 'AP')) {
                    $type = 'ProteinCluster';
                } elsif ($ad->is_namespace($ns, 'AR')) {
                    $type = 'TranscriptCluster';
                }
                push @{$types{$type}}, $seed;
            }
        } elsif ($cloudType) {
            my $type = &stnd_cloudType($cloudType);
            if ($type) {
                $types{$type} = \@seeds;
            } else {
                $args->err("Unknown cloud type '$cloudType'");
            }
        } else {
            $types{GeneCluster} = \@seeds;
        }
        while (my ($type, $seedArr) = each %types) {
            $args->msg(sprintf("Recovering %s clouds using %d nodes", 
                               $type, $#{$seedArr} + 1)) if ($doWarn);
            push @clouds, $ad->cached_clouds
                ( -seed   => \@seeds,
                  -type   => $type );
        }
        foreach my $cloud (@clouds) {
            push @edges, $cloud->edge_hash();
        }
        $cloudType = join(' + ', sort keys %types);
    }
    if ($args->{GENEONLY}) {
        # Collapse edges down to their genes
        my (%byNS, %geneLU);
        my @pairs = (['term_in','ns_in'], ['term_out','ns_out']);
        foreach my $edge (@edges) {
            foreach my $pair (@pairs) {
                my ($t,$n) = map { $edge->{$_} } @{$pair};
                $byNS{$n}{$t}++;
            }
        }
        while (my ($n, $th) = each %byNS) {
            next if ($ad->is_namespace($n, 'AL'));
            my $targ = ($n =~ /^Ensembl/) ? 'ENSG' : 'LL';
            my @tids = keys %{$th};
            my $lr = $ad->convert( -id => \@tids, -ns1 => $n, -ns2 => $targ,
                                   -cols => ['term_in', 'term_out','matched'],
                                   -nullscore => -1 );
            foreach my $row (@{$lr}) {
                my ($in, $out, $sc) = @{$row};
                if ($out) {
                    $sc = -1 unless (defined $sc);
                    $geneLU{$in}{$out} = $sc
                        if (!defined $geneLU{$in}{$out}
                            || $geneLU{$in}{$out} < $sc);
                } else {
                    $geneLU{$in}{''} = -1;
                }
            }
        }
        while (my ($in, $outh) = each %geneLU) {
            my ($best) = sort { $b <=> $a } values %{$outh};
            my @keep; map {
                push @keep, $_ if ($geneLU{$in}{$_} == $best);
            } keys %{$outh};
            $geneLU{$in} = [ $best, \@keep];
        }
        my @keepEdges;
        my @tn = qw(term_in term_out);
        foreach my $edge (@edges) {
            my ($n1, $n2) = map { $edge->{$_} } @tn;
            my ($m1, $m2) = map { $geneLU{ $_ } } ($n1, $n2);
            unless ($m1 || $m2) {
                # Both nodes are already genes
                push @keepEdges, $edge;
                next;
            }
            my @keep = ( { %{$edge} } );
            $keep[0]{auth} = "Collapse to Gene";
            $keep[0]{matched} = -1 unless (defined $keep[0]{matched});
            if ($m1) {
                my @km;
                my ($sc, $genes) = @{$m1};
                foreach my $k (@keep) {
                    foreach my $gene (@{$genes}) {
                        next unless ($gene);
                        my %ne = %{$k};
                        $ne{term_in} = $gene;
                        $ne{ns_in} = ($gene =~ /^LOC/) ? 
                            'LocusLink Gene' : 'Ensembl Gene';
                        $ne{matched} = ($sc < 0 || $ne{matched} < 0) ?
                            -1 : $sc * $ne{matched};
                        push @km, \%ne;
                    }
                }
                @keep = @km;
            }
            if ($m2) {
                my @km;
                my ($sc, $genes) = @{$m2};
                foreach my $k (@keep) {
                    foreach my $gene (@{$genes}) {
                        next unless ($gene);
                        my %ne = %{$k};
                        $ne{term_out} = $gene;
                        $ne{ns_out} = ($gene =~ /^LOC/) ? 
                            'LocusLink Gene' : 'Ensembl Gene';
                        $ne{matched} = ($sc < 0 || $ne{matched} < 0) ?
                            -1 : $sc * $ne{matched};
                        push @km, \%ne;
                    }
                }
                @keep = @km;
            }
            foreach  my $e (@keep) {
                push @keepEdges, $e unless ($e->{term_in} eq $e->{term_out});
            }
        }
        my %uniq;
        foreach my $edge (@keepEdges) {
            my ($n1,$n2) = sort ($edge->{term_in}, $edge->{term_out});
            $uniq{$n1}{$n2} = $edge if
                (!defined $uniq{$n1}{$n2}{matched} ||
                 $uniq{$n1}{$n2}{matched} < $edge->{matched});
        }
        @edges = ();
        foreach my $h (values %uniq) {
            push @edges, values %{$h};
        }
    }
    my $igAny = $args->val(qw(noany));
    my $igTr  = $args->val(qw(notrembl));
    if ($igAny || $igTr) {
        my @ks = qw(term_in term_out matched ns_in ns_out);
        my %isQuery = map { $_ => 1 } @uniqQ;
        my @keep;
        foreach my $edge (@edges) {
            my ($n1, $n2, $sc, $ns1, $ns2) = map { $edge->{$_} } @ks;
            next if ($igAny && 
                     (($ns1 =~ /^Any/ && !$isQuery{$n1}) ||
                      ($ns2 =~ /^Any/ && !$isQuery{$n2})));
            next if ($igTr && 
                     (($ns1 eq 'Trembl' && !$isQuery{$n1}) ||
                      ($ns2 eq 'Trembl' && !$isQuery{$n2})));
            push @keep, $edge;
        }
        @edges = @keep;
    }
    if ($fmtFlags{Table}) {
        # @rows = @edges;
    } elsif ($fmtFlags{HTML} && !($fmtFlags{CanvasXpress} || $fmtFlags{Table})) {
        my $gv = &graphviz(\@edges, \@uniqQ);
        if ($fmtFlags{DAD}) {
            print $outFH &esc_js( $gv );
            &end_JSON();
        } else {
            print $outFH "$gv</body></html>\n";
        }
        &finish_up;
    }
    ($idxIn, $idxOut) = (5,0);
    @corder = qw(term_out ns_out auth matched ns_between term_in ns_in);
    my @mapped;
    foreach my $edge (@edges) {
        push @mapped, [ map { $edge->{$_} } @corder ];
    }
    $rows = \@mapped;
    $ad->bench_end('Cloud Mode');
} elsif ($mode eq 'integer') {
    my %nss   = $ad->integer_namespaces();
    @corder   = qw(ns format);
    $rows     = [];
    foreach my $ns (sort keys %nss) {
        my $nfmt = $nss{$ns};
        push @{$rows}, [ $ns, $nfmt ];
    }
} elsif ($mode eq 'regexp') {
    my @REs = $ad->namespace_regexps();
    @corder   = qw(ns regexp nsn rank);
    $rows     = [];
    for my $r (0..$#REs) {
        my ($ns, $re) = @{$REs[$r]};
        my $nsn = $ad->namespace_name($ns);
        push @{$rows}, [ $ns, $re, $nsn, $r + 1 ];
    }
} elsif ($mode eq 'link') {
    $ad->bench_start('Link Mode');
    delete $specialCol{links};
    my $tasks = ($ns1 && ref($ns1)) ? $ns1 : { $ns1 || '' => \@uniqQ };
    if (my $kidns = $args->val(qw(childns kidns))) {
        my $kns   = $kidns eq '1' ? [] : [ split(/\s*\,\s*/, $kidns) ];
        my %kids;
        while (my ($n1, $list) = each %{$tasks}) {
            my $targs = &_targ_ns($kns, $n1);
            my $nsn   = $ad->namespace_name($n1);
            foreach my $n2 (@{$targs}) {
                my $rows = $ad->convert
                    ( -id => $list, -ns1 => $n1, -ns2 => $n2, -nonull => 1,
                      -cols => ['term_out','term_in'] );
                map { $kids{$n2}{$_->[0]}{$_->[1]} ||= $nsn } @{$rows};
            }
        }
        while (my ($n2, $hash) = each %kids) {
            while (my ($id, $from) = each %{$hash}) {
                push @{$tasks->{$n2} ||= []}, [$id, $from];
            }
        }
    }
    @corder   = qw(term_out ns_out links desc_link com_link term_in ns_in);
    $rows     = [];
    while (my ($lnkNs, $list) = each %{$tasks}) {
        my $lnkNsn   = $ad->namespace_name($lnkNs);
        foreach my $dat (@{$list}) {
            my ($id, $from);
            if (ref($dat)) {
                ($id, $from) = @{$dat};
            } else {
                my ($stnd) = $ad->standardize_id($dat, $ns1);
                ($id, $from) = ($stnd, {$stnd => $lnkNsn});
            }
            my $links = $ad->namespace_links_raw( $id, $lnkNs );
            ($id) = $ad->standardize_id( $id, $lnkNs);
            foreach my $dat (@{$links}) {
                my @baseRow = ($id, $lnkNsn, $dat->[2], $dat->[3], $dat->[6]);
                while (my ($srcId, $srcNs) = each %{$from}) {
                    push @{$rows}, [@baseRow, $srcId, $srcNs];
                }
            }
        }
    }
    $sortKey = 'term_in';
    ($idxIn, $idxOut) = (0,2);
    $ad->bench_end('Link Mode');
} else {
    $args->err("I do not know what to do with -mode '$mode'");
}


my $primaryCount = $#{$rows} + 1;

$args->msg("[+]", sprintf("Primary database queries recover %d row%s - %s",
                          $primaryCount, $primaryCount  == 1 ? '':'s', `date`))
    if ($doWarn);

if ($fmtFlags{NoHits}) {
    if (defined $idxOut) {
        $ad->bench_start('Find No Hits');
        my @nulls;
        if ($fmtFlags{List}) {
            map { push @nulls, [$_->[$idxIn]]
                      unless ($_->[$idxOut]) } @{$rows};
            $idxOut = $idxIn = 0;
        } else {
            map { push @nulls, $_ unless ($_->[$idxOut]) } @{$rows};
            $idxOut = $idxIn;
        }
        $rows = \@nulls;
        $ad->bench_end('Find No Hits');
    } else {
        $args->msg("[!!]", "Can not select null hits without knowing the index of the output column (programming error!)");
    }
}

if ($fmtFlags{SetFile}) {
    $sortKey  = $objCol;
    my $nsCol = $objCol;
    $nsCol    =~ s/term/ns/;
    # my $doSymbol = 'sym_out' : 'Null';
    $colReq   = $fmtFlags{GSEA} ? [$objCol, 'desc_out', $nsCol] :
        [$objCol, 'sym_out', 'desc_out', $nsCol];
}

my %cpos = map { $corder[$_] => $_ + 1} (0..$#corder);
my %goPos; # Will only be used if FlagGo is specified


# Only include unversioned BrainArray IDs:
my $sba = $args->val(qw(simplebrainarray));
# Strip BrainArray IDs down to their 'native' format:
my $cba = $args->val(qw(cleanbrainarray));

if ($sba || $cba) {
    my @inds;
    foreach my $col ('term_in', 'term_out') {
        if (my $ind = $cpos{$col}) {
            push @inds, $ind - 1;
        }
    }
    if ($#inds != -1) {
        my @simplified;
        foreach my $row (@{$rows}) {
            my $keep = 1;
            foreach my $ind (@inds) {
                my $v = $row->[$ind] || "";
                if ($v =~ /^BrAr:(.+)/) {
                    my $id = $1;
                    if ($sba && $id =~ /:/) {
                        # Ignore versioned IDs
                        $keep = 0;
                        next;
                    }
                    if ($cba) {
                        # Remove the MapTracker-specifc prefices
                        $id =~ s/.+://;
                        $id =~ s/^LOC//;
                        $row->[$ind] = $id;
                    }
                }
            }
            push @simplified, $row if ($keep);
        }
        $rows = \@simplified;
    }
}

if ($specialCol{len_in} || $specialCol{len_out}) {
    my @cbs;
    my %lenCache;
    my $lenMeth = sub {
        my $id = shift || "";
        unless (defined $lenCache{$id}) {
            my @bss = $ad->fetch_bioseq
                ( -id => $id, -format => 'fasta');
            my %lenH = map { $_->length() => 1 } @bss;
            my @lens = keys %lenH;
            $lenCache{$id} = $#lens == 0 ? $lens[0] || "" : "";
        }
        return $lenCache{$id};
    };
    if ($specialCol{len_in} && $cpos{term_in}) {
        my $lenInd = &get_or_add_column(\%cpos, 'len_in', \@corder);
        my $srcInd = $cpos{term_in} - 1;
        push @cbs, sub {
            my $row = shift;
            $row->[$lenInd] ||= &{$lenMeth}( $row->[$srcInd] );
        };
    }
    foreach my $row (@{$rows}) {
        foreach my $cb (@cbs) {
            &{$cb}( $row );
        }
    }
}

if ($args->val(qw(noself)) && $cpos{term_in} && $cpos{term_out}) {
    # Remove rows where term_in == term_out
    my ($i, $o) = map { $cpos{$_} - 1} qw(term_in term_out);
    my @kept;
    foreach my $row (@{$rows}) {
        my ($ti, $to) = ($row->[$i], $row->[$o]);
        push @kept, $row unless ($ti && $to && $ti eq $to);
    }
    $rows = \@kept;
}

if ($cleanChr && !$fmtFlags{Fasta}) {
    # Turn 'homo_sapiens.chromosome.8' into '8'
    my @inds;
    my @check = qw(term_share);
    push @check, 'term_out' if ($mode eq 'map' || $mode eq 'overlap');
    map { push @inds, $_ - 1 if ($_) } map { $cpos{$_} } @check;
    unless ($#inds == -1) {
        foreach my $row (@{$rows}) {
            foreach my $ind (@inds) {
                if ($row->[$ind] && 
                    $row->[$ind] =~ /^[^\.]+\.[^\.]+\.(.+)$/) {
                    $row->[$ind] = $1;
                }
            }
        }
    }
}

if ($filter && $#{$filter} != -1) {
    if ($objCol && $cpos{$objCol}) {
        my ($keep, $toss);
        foreach my $name (map { uc($_ || "") } @{$filter}) {
            next unless ($name);
            if ($name =~ /^\!(.+)/) {
                $toss ||= {};
                $toss->{$1} = 1;
            } else {
                $keep ||= {};
                $keep->{$name} = 1;
            }
        }
        my $ind = $cpos{$objCol} - 1;
        my @keep;
        if ($qryCol && $cpos{$qryCol} && 
            ($mode eq 'convert' || $mode eq 'simple')) {
            my $qind = $cpos{$qryCol} - 1;
            my %byQry;
            foreach my $row (@{$rows}) {
                my $val = $row->[$qind] || "";
                push @{$byQry{$val}}, $row;
            }
            foreach my $rowGroup (values %byQry) {
                my %vals;
                map { push @{$vals{uc($_->[$ind] || "")}}, $_ } @{$rowGroup};
                delete $vals{""};
                my @kept;
                while (my ($val, $group) = each %vals) {
                    if ($toss && $toss->{$val}) {
                        # discard everything
                        @kept = ();
                        last;
                    }
                    next if ($keep && !$keep->{$val});
                    push @kept, @{$group};
                }
                push @keep, @kept;
            }
        } else {
            # Just filter row-by-row
            foreach my $row (@{$rows}) {
                my $val = uc($row->[$ind] || "");
                next if ($keep && !$keep->{$val});
                next if ($toss && $toss->{$val});
                push @keep, $row;
            }
        }
        $rows = \@keep;
        $args->msg("All results were removed by your filter")
            if ($doWarn && $#keep == -1);
    } else {
        $args->msg("[ERR]","Can not apply your filter because the output has been configured to exclude the row being filtered");
    }
}

if ($fmtFlags{Leaves}) {
    my %clustered;
    my $n = $cpos{ns_out};
    foreach my $row (@{$rows}) {
        my ($pk, $sk) = ($row->[$idxIn] || "", $row->[$idxOut] || "");
        next unless ($sk && $pk);
        my $sns = $n ? $row->[$n-1] || "" : "";
        push @{$clustered{$pk}{$sns}{$sk}}, $row;
    }
    my %parCache;
    $rows = [];
    while (my ($pk, $nss) = each %clustered) {
        while (my ($sns, $sks) = each %{$nss}) {
            my @all  = keys %{$sks};
            foreach my $sk (@all) {
                my $pars = $parCache{$sns}{$sk} ||=
                    [ $ad->direct_parents($sk, $sns ) ];
                map { delete $sks->{$_} } @{$pars};
            }
            push @{$rows}, map { @{$_} } values %{$sks};
        }
    }
}

if ($fmtFlags{MetaSet}) {
    my ($i, $o) = map { $cpos{$_} } qw(term_in term_out);
    $args->death("Can not generate MetaSet format:",
                 "Need both input and output columns.") unless
                     (defined $i && defined $o);
    my $n = $cpos{ns_out};
    my $dt = `date`;
    $i--;
    $o--;
    print $outFH <<EOF;
# MetaSet format output from GenAcc
# Global parameters
//split=\\t
//date=$dt
# Each row represents one set
# Entries of form /key=val are parameters for that set
# All other entries are the set members

EOF

    my %groups;
    my %ns;
    foreach my $row (@{$rows}) {
        my ($in, $out) = ($row->[$i], $row->[$o]);
        next unless ($in && $out);
        $groups{$out}{$in}++;
        $ns{$out} ||= $row->[$n-1] if ($n && ! $ns{$out});
    }
    foreach my $out (&fancySort([keys %groups])) {
        my @list = &fancySort([keys %{$groups{$out}}]);
        my @meta = ("/name=$out");
        if (my $desc = &_get_desc($out, $ns{$out})) {
            push @meta, "/desc=$desc";
        }
        print $outFH join("\t", @meta, @list)."\n";
    }
    &finish_up;
}

if ($sourceID && $cpos{term_in}) {
    $ad->bench_start('Flag Expanded');
    my $ind1 = $cpos{term_in} - 1;
    my $auth = $cpos{auth} ? $cpos{auth} - 1 : undef;
    
    foreach my $row (@{$rows}) {
        my $now = $row->[$ind1];
        my $src = $sourceID->{$now};
        next if ($now eq $src);
        $row->[$ind1] = $src;
        $row->[$auth] .= " < Expanded" if (defined $auth);
    }
    $ad->bench_end('Flag Expanded');
}

if ($versus) {
    my ($tO) = ($cpos{$objCol});
    if ($tO) {
        $tO--;
        my %ok = map { $_ => 1 } $ad->list_from_request( $versus );
        my $allOk = join(', ', sort keys %ok);
        my @keep;
        my $discarded = 0;
        foreach my $row ( @{$rows}) {
            if ($ok{ $row->[$tO] || ""}) {
                push @keep, $row;
            } else {
                $discarded++;
            }
        }
        $rows = \@keep;
        $args->msg("$discarded results were excluded as not matching explicit -versus filter of $allOk") if ($vb);
    } else {
        $args->msg("[!!]", "Can not filter results by -versus list",
                   "Missing output term column");
    }
}

if ($maxRep || $minRep) {
    my ($tO, $tI) = ($cpos{$objCol}, $cpos{$qryCol});
    if ($tO && $tI) {
        $tO--; $tI--;
        my (%byInput, %byOutput, %discard);
        map { $byInput{ $_->[$tI] || "" } = 1;
              $byOutput{ $_->[$tO] || "" }{ $_->[$tI] || "" } = 1; } @{$rows};
        delete $byInput{""}; delete $byOutput{""};
        my @uniqIn = keys %byInput;
        my $numIn  = $#uniqIn + 1;
        my $maxOut = int(0.5 + $numIn * ($maxRep || 1));
        my $minOut = int($numIn * ($minRep || 0));
        while (my ($out, $ins) = each %byOutput) {
            delete $ins->{""};
            my @uniqIn = keys %{$ins};
            my $num    = $#uniqIn + 1;
            if ($num > $maxOut || $num < $minOut) {
                $discard{ $out } = 1;
            }
        }
        my @uniqOut   = keys %discard;
        my $numDisc   = $#uniqOut + 1;
        my $discarded = 0;
        my @keep;
        foreach my $row ( @{$rows}) {
            if ($discard{ $row->[$tO] || ""}) {
                $discarded++;
            } else {
                push @keep, $row;
            }
        }
        $rows = \@keep;
        $args->msg("$discarded results matching one of $numDisc output terms with query representation outside of $minOut-$maxOut (total $numIn queries) removed.") if ($vb);
    } else {
        $args->msg("[!!]", "Can not filter results by representation",
                   "Missing input and/or output term columns");
    }
}

if ($reqRoot) {
    my ($tO) = ($cpos{$objCol});
    if ($tO) {
        $tO--;
        my $nOn = $objCol; $nOn =~ s/term/ns/;
        my $n2 = $cpos{$nOn} ? $cpos{$nOn} -1 : undef;
        my %roots;
        if (-e $reqRoot) {
            my $tr = BMS::TableReader->new();
            $tr->input($reqRoot);
            foreach my $sheet ($tr->each_sheet()) {
                $tr->select_sheet($sheet);
                while (my $row = $tr->next_clean_row()) {
                    if (my $val = $row->[0]) { $roots{$val}++; }
                }
            }
        } else {
            map { $roots{$_ || ""}++ } $ad->list_from_request( $reqRoot );
        }
        delete $roots{""};
        my $allRoot = join(', ', sort keys %roots);
        my %kids;
        foreach my $id (keys %roots) {
            my $ns = $ad->guess_namespace($id);
            my @children = $ad->all_children( $id, $ns, undef, $doWarn );
            map { $kids{ $_ } = 1 } @children;
        }
        my @keep;
        my $tot = $#{$rows} +1;
        my $kept = 0;
        foreach my $row (@{$rows}) {
            if (my $id = $row->[$tO]) {
                if ($kids{$id}) {
                    push @keep, $row;
                    $kept++;
                }
            }
        }
        $rows = \@keep;
        $args->msg("Kept $kept of $tot results that shared a parent in $allRoot") if ($vb);
    } else {
        $args->msg("[!!]", "Can not filter results by root parents",
                   "Missing output term column");
    }
}

if ($freq && $cpos{term_out} ) {
    $ad->bench_start('Filtering');
    $args->msg("Using conversion to filter input list") if ($vb);
    my %status;
    my $lvtok = 'Live';
    my $dttok = 'DeprecatedTo';
    my $dptok = 'Deprecated';
    my $ddtok = 'OldDeprecated';
    my $altok = 'Alias';
    my $uktok = 'Unknown';
    if ($rNS) {
        # Names are already standardized
        map { $status{$uktok}{$_} ||= 
              { '' => $rNS->{$_} || 'Unknown'} } @uniqQ;
    } else {
        # Need to standardize capitalization
        my $nsname = $ad->namespace_name($ns1) || 'Unknown';
        foreach my $id (@uniqQ) {
            my ($std) = $ad->standardize_id($id, $ns1);
            $status{$uktok}{$std} ||= { '' => $nsname };
        }
    }

    if ($freq =~ /^(\!?)(self|status|usable)$/) {
        # Request for deprecation status
        my ($nkey, $what) = ($1 || '', $2);
        my @tot  = keys %{$status{$uktok}};
        my $mpos = $cpos{matched} - 1;
        my $npos = $cpos{ns_in} - 1;
        foreach my $row (@{$rows}) {
            my $tout = $row->[$idxOut];
            if ($tout) {
                my $tin   = $row->[$idxIn];
                my $match = $row->[$mpos];
                my $nsin  = $row->[$npos];
                if (!defined $match || $match eq '') {
                    # Undefined score = unknown ID
                    $status{$uktok}{$tin}{''} = $nsin;
                } elsif ($match == 1) {
                    if ($tin eq $tout) {
                        # Score 1 match to self = live ID
                        $status{$lvtok}{$tin}{$tout} = $nsin;
                    } else {
                        # Score 1 match to something else = deprecated to
                        $status{$dttok}{$tin}{$tout} = $nsin;
                    }
                } elsif ($match == 0) {
                    if ($tin eq $tout) {
                        # Score 0 match to self = fully deprecated
                        $status{$dptok}{$tin}{''} = $nsin;
                    } else {
                        # Score 0 match to other = deprecated to deprecated
                        $status{$ddtok}{$tin}{$tout} = $nsin;
                    }

                }
            }
        }
        $rows = [];
        my @skeys = ($lvtok);
        if ($nkey || $what eq 'status' || $what eq 'usable') {
            my @live = keys %{$status{$lvtok}};
            map { delete $status{$dttok}{$_};
                  delete $status{$dptok}{$_} } @live;
            if ($what eq 'usable') {
                push @skeys, $dttok;
            } else {
                my @dt   = keys %{$status{$dttok}};
                map { delete $status{$dptok}{$_} } (@dt);
                my @dep  = keys %{$status{$dptok}};
                map { delete $status{$uktok}{$_} } (@dt, @dep, @live);
                @skeys = () unless ($what eq 'status');
                push @skeys, ($dptok, $dttok, $uktok);
                push @skeys, $ddtok if ($args->val(qw(alldep)));
            }
        }
        my @bits;
        foreach my $stat (@skeys) {
            my @tins = keys %{$status{$stat} || {}};
            push @bits, ($#tins+1)." $stat" unless ($#tins == -1);
            foreach my $tin (@tins) {
                my $th = $status{$stat}{$tin};
                push @{$rows}, map {[$_, $stat, $tin, $th->{$_}]} keys %{$th};
            }
        }
        ($idxIn, $idxOut ) = (2,0);
        @corder = qw(term_out ns_out term_in ns_in);
        $hdata->{ns_out} = {
            label => "Status",
        };
        %cpos = map { $corder[$_] => $_ + 1} (0..$#corder);
        if ($vb) {
            $args->msg(sprintf("  Found %s entries out of %d provided",
                               join(', ', @bits) || 'no', $#tot + 1))
                if ($doWarn);
        }
    } else {
        # Request to filter against specific value(s)
        my $filt = $ad->process_filter_request( $freq );
        my @tot  = keys %{$status{$uktok}};
        foreach my $tag ('IN', 'NOT IN') {
            my $vals = $filt->{$tag};
            next unless ($vals);
            my $keeping = ($tag eq 'IN') ? 1 : 0;
            my (@keep, %rhash);
            my %vhash   = map { ($_ || 0) => 1 } @{$vals};
            my $selfRef = $vhash{self} ? 1 : 0;
            # Group the rows by input term:
            map { push @{$rhash{$_->[$idxIn]}}, $_ } @{$rows};
            my @input = keys %rhash;
            my $kept  = 0;
            foreach my $tin (@input) {
                # Keep track of input terms seen in the output
                delete $status{$uktok}{$tin};
                my $tRows = $rhash{$tin};
                my $filterFound = 0;
                for my $r (0..$#{$tRows}) {
                    my $o = $tRows->[$r][$idxOut] || 0;
                    if ($vhash{$o} || ($selfRef && $o eq $tin)) {
                        $filterFound = 1;
                        last;
                    }
                }
                if ($filterFound == $keeping) {
                    push @keep, @{$tRows};
                    $kept++;
                }
            }
            $rows = \@keep;
            unless ($keeping) {
                my @null = sort keys %{$status{$uktok}};
                # The filters were rejecting non-matches
                # We need to be sure to include queries that had zero rows
                my $nns  = $rNS ? '' : $ad->namespace_name($ns1);
                $kept   += $#null + 1;
                foreach my $id (@null) {
                    my $ns = $nns;
                    if ($ns) {
                        ($id) = $ad->standardize_id($id, $nns);
                    } else {
                        $ns = $rNS->{$id};
                    }
                    push @{$rows}, [ '', '', '', '', '', $id, $ns];
                }
            }
            $args->msg(sprintf("  %s %s : %d/%d survive\n", $keeping ? 
                               "Keeping" : "Rejecting", join(' or ', @{$vals}),
                               $kept, $#tot +1)) if ($vb);
        }
        unless ($args->{NOREMAP}) {
            my @remap;
            map { push @remap, [ $_->[5], $_->[6] ] } @{$rows};
            $rows = \@remap;
            ($idxIn, $idxOut, ) = (0,0);
            @corder = qw(term_out ns_out);
            %cpos   = map { $corder[$_] => $_ + 1} (0..$#corder);
            ($qryCol, $objCol) = ('term_in', 'term_out');
            $hdata->{term_out} = {
                label => "Input (Filtered)",
            };
            $hdata->{ns_out} = {
                label => "Input Namespace",
            };
        }
    }
    $ad->bench_end('Filtering');
}

if ( $specialCol{sym_out} ) {
    $ad->bench_start('Add Gene Symbols');
    # Add a gene symbol column
    my $tO  = $cpos{$objCol} - 1;
    my $nOn = $objCol; $nOn =~ s/term/ns/;
    my $nO  = $cpos{$nOn} ? $cpos{$nOn} -1 : undef;
    my $spO = &get_or_add_column(\%cpos, 'sym_out', \@corder);
    my $nI  = $cpos{ns_in}  ? $cpos{ns_in} -1 : undef;
    $args->msg("Adding output symbols to column $spO") if ($doWarn);

    my %requests;
    foreach my $row ( @{$rows}) {
        my $sym = "";
        if (my $t = $row->[$tO]) {
            my $tns = $ad->effective_namespace
                ( defined $nO ? $row->[$nO] : undef,
                  defined $nI ? $row->[$nI] : undef, $t );
            $requests{$t} = $tns;
        }
    }
    my $syms =  $ad->bulk_best_symbol
        ( -ids => \%requests, -warn => $doWarn, -short => 1,
          -explainsql => $exSql );
    
    foreach my $row ( @{$rows}) {
        my $sym = "";
        if (my $arr = $syms->{ $row->[$tO] || ""}) {
            $sym = $arr->[0] || "";
        }
        $row->[$spO] = $sym;
    }

    $ad->bench_end('Add Gene Symbols');
}

if ( $specialCol{sym_in} && $qryCol && $cpos{$qryCol}) {
    $ad->bench_start('Add Gene Symbols');
    # Add a gene symbol column
    my $tO  = $cpos{$qryCol} - 1;
    my $nOn = $qryCol; $nOn =~ s/term/ns/;
    my $nO  = $cpos{$nOn} ? $cpos{$nOn} -1 : undef;
    my $spO = &get_or_add_column(\%cpos, 'sym_in', \@corder);
    my $nI  = $cpos{ns_in}  ? $cpos{ns_in} -1 : undef;
    $args->msg("Adding input symbols to column ".($spO+1)) if ($doWarn);

    my (%requests, %uniqNS);
    foreach my $row ( @{$rows}) {
        if (my $t = $row->[$tO]) {
            my $tns = $ad->effective_namespace
                ( defined $nO ? $row->[$nO] : undef,
                  defined $nI ? $row->[$nI] : undef, $t );
            $requests{$t} = $tns;
            # push @{$uniqNS{$tns}}, $t;
        }
    }
    # $args->msg("[DEBUG]", "Symbol namespaces", ( map { sprintf("%s : %s", $_, join(",", splice(@{$uniqNS{$_}}, 0, 5)))} sort keys %uniqNS));
    
    my $syms =  $ad->bulk_best_symbol
        ( -ids => \%requests, -warn => $doWarn, -short => 1,
          -explainsql => $exSql );
    
    
    foreach my $row ( @{$rows}) {
        my $sym = "";
        if (my $arr = $syms->{ $row->[$tO] || ""}) {
            $sym = $arr->[0] || "";
        }
        $row->[$spO] = $sym;
    }
    $ad->bench_end('Add Gene Symbols');
}

foreach my $taxSC ('tax_in','tax_out') {
    # Add a species column
    next unless ($specialCol{$taxSC});
    my $oCol = $taxSC eq 'tax_in' ? $qryCol : $objCol;
    next unless ($oCol && $cpos{$oCol});
    $ad->bench_start('Add Species');
    my $tO  = $cpos{$oCol} - 1;
    my $nOn = $oCol; $nOn =~ s/term/ns/;
    my $nO  = $cpos{$nOn} ? $cpos{$nOn} -1 : undef;
    my $nI  = $cpos{ns_in}  ? $cpos{ns_in} -1 : undef;
    my $spO = &get_or_add_column(\%cpos, $taxSC, \@corder);

    my %byNS;
    # Organize output by namespace
    for my $r (0..$#{$rows}) {
        my $row = $rows->[$r];
        $row->[$spO] = "";
        if (my $t = $row->[$tO]) {
            my $tns = $ad->effective_namespace
                ( defined $nO ? $row->[$nO] : undef,
                  defined $nI ? $row->[$nI] : undef, $t );
            push @{$byNS{$tns}{$t}}, $r;
        }
    }
    # Lookup derived information in bulk
    while (my ($tns, $idhash) = each %byNS) {
        my @ids = keys %{$idhash};
        my $lu = $ad->convert
            ( -id => \@ids, -ns1 => $tns, -ns2 => 'TAX', -nonull => 1,
              -warn => $doWarn, -cols => ['term_in','term_out'] );
        my %mapped; map { $mapped{$_->[0]}{$_->[1]} = 1 } @{$lu};
        while (my ($t, $lus) = each %mapped) {
            # Map over results to specific rows
            my @vals = sort keys %{$lus};
            map { s/([a-z])[a-z]+/$1/ig;
                  s/\s+//g;} @vals if ($abbrTaxa);
            my $spec = join(', ', @vals);
            map { $rows->[$_][$spO] = $spec } @{$byNS{$tns}{$t}};
        }
    }
    $ad->bench_end('Add Species');
}

if ($showToken) {
    my @tokCols;
    my @try = qw(ns_in ns_between);
    push @try, 'ns_out' unless ($freq);
    my %isToken = map { $_ => 1 } @try;
    map { push @tokCols, $_ if ($isToken{$corder[$_]}) } (0..$#corder);
    unless ($#tokCols == -1) {
        foreach my $row ( @{$rows}) {
            foreach my $ind (@tokCols) {
                if (my $nsnD = $row->[$ind]) {
                    my @toks;
                    foreach my $nsn (split(/\s+\<\s+/, $nsnD)) {
                        if (my $tok = $ad->namespace_token($nsn)) {
                            push @toks, $tok;
                        } else {
                            push @toks, $nsn;
                        }
                    }
                    $row->[$ind] = join(' < ', @toks);
                }
            }
        }
    }
}

if ( $specialCol{desc_out} && $cpos{$objCol}) {
    $ad->bench_start('Add Description');
    # Add a description column
    my $tO = $cpos{$objCol} - 1;
    my $nOn = $objCol; 
    $nOn    =~ s/term/ns/;
    my $nO  = $cpos{$nOn} ? $cpos{$nOn} -1 : undef;
    my $nI  = $cpos{ns_in}  ? $cpos{ns_in} -1 : undef;
    my $spO = &get_or_add_column(\%cpos, 'desc_out', \@corder);
    $hdata->{desc_out} = $hdata->{desc_in} if ($objCol eq 'term_in');
    my (%byNS, @toDo, %descs);
    for my $i (0..$#{$rows}) {
        my $row = $rows->[$i];
        if (my $t = $row->[$tO]) {
            my $tns = $ad->effective_namespace
                ( defined $nO ? $row->[$nO] : undef,
                  defined $nI ? $row->[$nI] : undef, $t );
            push @{$byNS{$tns}}, $t;
            push @toDo, [$i, $t, $tns];
        }
    }
    while (my ($tns, $ids) = each %byNS) {
        $descs{$tns} = $ad->bulk_description
            ( -ns => $tns, -ids => $ids, -ashash => 1, @comArgs );
    }
    foreach my $td (@toDo) {
        my ($i, $t, $tns) = @{$td};
        $rows->[$i][$spO] = $descs{$tns}{$t};
    }
    $ad->bench_end('Add Description');
}

if ( $specialCol{desc_in} && $cpos{$qryCol}) {
    $ad->bench_start('Add Description');
    # Add a description column
    my $tO = $cpos{$qryCol} - 1;
    my $nOn = $qryCol; 
    $nOn    =~ s/term/ns/;
    my $nO  = $cpos{$nOn} ? $cpos{$nOn} -1 : undef;
    my $nI  = $cpos{ns_in}  ? $cpos{ns_in} -1 : undef;
    my $spO = &get_or_add_column(\%cpos, 'desc_in', \@corder);
    $hdata->{desc_in} = {
        label => "Input Description",
    }  if ($qryCol eq 'term_in');
    my %cache;
    foreach my $row ( @{$rows}) {
        my $desc = "";
        if (my $t = $row->[$tO]) {
            my $tns = $ad->effective_namespace
                ( defined $nO ? $row->[$nO] : undef,
                  defined $nI ? $row->[$nI] : undef, $t );
            unless (defined $cache{$tns}{$t}) {
                $cache{$tns}{$t} = &_get_desc( $t, $tns );
            }
            $desc = $cache{$tns}{$t};
        }
        $row->[$spO] = $desc;
    }
    $ad->bench_end('Add Description');
}

if ($specialCol{sortchr}) {
    my (@nums, @char, @cols);
    if (my $pos = $cpos{term_out}) {
        my $w = 3;
        push @nums, "%0${w}d";
        push @char, "%${w}s";
        push @cols, $pos - 1;
    }
    foreach my $cn ('start_out','end_out') {
        if (my $pos = $cpos{$cn}) {
            my $w = 12;
            push @nums, "%0${w}d";
            push @char, "%${w}s";
            push @cols, $pos - 1;
        }
    }
    my $spO = &get_or_add_column(\%cpos, 'sortchr', \@corder);
    if ($fmt) {
        foreach my $row ( @{$rows}) {
            my @bits;
            for my $i (0..$#cols) {
                my $val = $row->[$cols[$i]];
                if (!defined $val) {
                    push @bits, "";
                } elsif ($val =~ /^\d+/) {
                    push @bits, sprintf($nums[$i], $val);
                } else {
                    push @bits, sprintf($char[$i], $val);
                }
            }
            $row->[$spO] = join('-', @bits);
        }
    }
}

if ($specialCol{links}) {
    $ad->bench_start('Add Hyperlinks');
    # Add a column of hyperlinks
    my $tO = $cpos{$objCol} - 1;
    my $n1 = $cpos{ns_in} ? $cpos{ns_in} -1 : undef;
    my $nOn = $objCol; $nOn =~ s/term/ns/;
    my $n2 = $cpos{$nOn} ? $cpos{$nOn} -1 : undef;
    my $spO = &get_or_add_column(\%cpos, 'links', \@corder);
    foreach my $row ( @{$rows}) {
        my $links = "";
        if (my $t = $row->[$tO]) {
            $links = $ad->namespace_links($t, defined $n2 ? $row->[$n2] : '', defined $n1 ? $row->[$n1] : '', $urlJoin);
            unless ($fmtFlags{HTML}) {
                my @urls;
                while ($links =~ /href=[\"\']([^\"\']+)[\"\']/) {
                    my $h = $1;
                    $links =~ s/\Q$h\E//g;
                    push @urls, $h;
                }
                # $links =~ s/\<.+?\>//g 
                $links = join(', ', @urls);
            }
        }
        $row->[$spO] = $links;
    }
    $ad->bench_end('Add Hyperlinks');
}

if ($specialCol{set}) {
    $ad->bench_start('Add Set Membership');
    # Add a column of set membership
    my $tO = $cpos{$objCol} - 1;
    my $n1 = $cpos{ns_in}  ? $cpos{ns_in} -1 : undef;
    my $nOn = $objCol; $nOn =~ s/term/ns/;
    my $n2 = $cpos{$nOn} ? $cpos{$nOn} -1 : undef;
    my $spO = &get_or_add_column(\%cpos, 'set', \@corder);
    my $setFilter = $setReq ? { map { lc($_) => 1 } @{$setReq} } : undef;
    # Organize terms by namespace
    my %byNS;
    foreach my $row ( @{$rows}) {
        if (my $t = $row->[$tO]) {
            my $tns = $ad->effective_namespace
                ( defined $n2 ? $row->[$n2] : undef,
                  defined $n1 ? $row->[$n1] : undef, $t );
            $byNS{$tns || ""}{$t} = 1;
        }
    }
    # Get sets in bulk
    my %id2set;
    while (my ($tns, $tHash) = each %byNS) {
        my @ids = keys %{$tHash};
        my $rows = $ad->convert
            ( -id => \@ids, -ns1 => $tns, -cols => ['term_in', 'term_out' ],
              -ns2 => 'set', -warn => $doWarn );
        foreach my $row (@{$rows}) {
            if (my $t = $row->[0]) {
                if (my $set = $row->[1]) {
                    $id2set{$t}{$set} = 1;
                }
            }
        }
    }
    # Pivot id2set from non-redundant hash to concatenated string
    foreach my $t (keys %id2set) {
        my @sets = keys %{$id2set{$t}};
        if ($setFilter) {
            my @keep;
            map { push @keep, $_ if ($setFilter->{lc($_)}) } @sets;
            @sets = @keep;
        }
        $id2set{$t} = join(',', sort @sets);
    }
    # Add sets to appropriate column
    foreach my $row ( @{$rows}) {
        $row->[$spO] = $id2set{ $row->[$tO] || "" } || "";
    }
    $ad->bench_end('Add Set Membership');
}

if ($specialCol{inventory} || $specialCol{dryinv}) {
    $ad->bench_start('Get BMS Stock');
    # Add a column of BMS inventory stocks
    my $tO  = $cpos{$objCol} - 1;
    my $nOn = $objCol; $nOn =~ s/term/ns/;
    my $n1  = $cpos{ns_in} ? $cpos{ns_in} -1 : undef;
    my $n2  = $cpos{$nOn} ? $cpos{$nOn} -1 : undef;
    my $spWet = &get_or_add_column(\%cpos, 'inventory', \@corder);
    my $spDry = &get_or_add_column(\%cpos, 'dryinv', \@corder);
    my $sts = $ad->sts();
    my $sth = &crs_stock_sth();
    my $sumMethod = sub {
        my $stocks = shift;
        return undef if ($#{$stocks} == -1);
        my $sum = 0;
        map { $sum += $_ } @{$stocks};
        return $sum;
    };
    my $maxMethod = sub {
        my $stocks = shift;
        my ($max) = sort { $b <=> $a } @{$stocks};
        return $max;
    };
    my $minToMaxMethod = sub {
        my $stocks = shift;
        my @rng = sort { $a <=> $b } @{$stocks};
        my $rv = $rng[0];
        $rv .= ' - '. $rng[-1] unless ($rng[0] == $rng[-1]);
        return $rv;
    };
    my $method = $sumMethod; # minToMaxMethod;
    
    foreach my $row ( @{$rows}) {
        my $t = $row->[$tO];
        next unless ($t);
        my $tns = $ad->effective_namespace
            ( defined $n2 ? $row->[$n2] : undef,
              defined $n1 ? $row->[$n1] : undef, $t );
        next unless ($ad->is_namespace($tns, 'AC'));
        my @bmsIDs = ($tns eq 'BMSC') ? ($t) :
            $ad->convert( -id => $t, -ns1 => $tns, -ns2 => 'BMSC' );
        next if ($#bmsIDs == -1);
        my @stocks;
        foreach my $q ($sts->diversify_bms_ids(@bmsIDs)) {
            $sth->execute($q);
            my $brows = $sth->fetchall_arrayref();
            foreach my $row (@{$brows}) {
                my ($lot, $amntWet, $amntDry) = @{$row};
                push @{$stocks[$spWet]}, $amntWet
                    if (defined $spWet && defined $amntWet);
                push @{$stocks[$spDry]}, $amntDry
                    if (defined $spDry && defined $amntDry);
            }
        }
        foreach my $ind ($spWet, $spDry) {
            next unless (defined $ind && $stocks[$ind]);
            $row->[$ind] = &{$method}( $stocks[$ind] );
            #my @stocks = sort { $a <=> $b } @{$stocks[$ind]};
            #$row->[$ind] = $stocks[0];
            #$row->[$ind] .= ' - '.$stocks[-1]
            #    unless ($stocks[0] == $stocks[-1]);
        }
    }
    $ad->bench_end('Get BMS Stock');
}

if ($specialCol{specificity}) {
    $ad->bench_start('Get specificity');
    # Add a column of gene specificity
    my $tO  = $cpos{$objCol} - 1;
    my $nOn = $objCol; $nOn =~ s/term/ns/;
    my $n1  = $cpos{ns_in} ? $cpos{ns_in} -1 : undef;
    my $n2  = $cpos{$nOn} ? $cpos{$nOn} -1 : undef;
    my $spSpc = &get_or_add_column(\%cpos, 'specificity', \@corder);
    foreach my $row ( @{$rows}) {
        my $t = $row->[$tO];
        next unless ($t);
        my $tns = $ad->effective_namespace
            ( defined $n2 ? $row->[$n2] : undef,
              defined $n1 ? $row->[$n1] : undef, $t );
        my @hgs = $ad->convert
            ( -id => $t, -ns1 => $tns, -ns2 => 'HG',  
              -warn => $doWarn, -min => $minScore );
        $row->[$spSpc] = $#hgs + 1;
    }
    $ad->bench_end('Get specificity');
}

if ($specialCol{chemdetail}) {
    $ad->bench_start('Extract chemical details');
    my $sci = $cpos{matched};
    my $aui = $cpos{auth};
    if ($sci && $aui) {
        $sci--;
        $aui--;
        my $lastCol = $#corder + 1;
        my $oldCol  = $lastCol;
        foreach my $row ( @{$rows}) {
            my $sc = $row->[$sci];
            next unless defined ($sc);
            my $au = $row->[$aui];
            my ($val, $assay) = $ad->score_to_chem_value($sc, $au);
            next unless ($assay);
            my $vi = ($cpos{$assay} ||= ++$lastCol) - 1;
            $row->[$vi] = $val;
        }
        foreach my $col (keys %cpos) {
            my $num = $cpos{$col};
            next unless($num > $oldCol);
            my $lcol = lc($col);
            $corder[$num-1] = $lcol;
            $cpos{$lcol} = $num;
            delete $cpos{$col};
            $hdata->{$lcol} = {
                label => $col,
            };
            push @{$colReq}, $col if ($colReq);
        }
    } else {
        $args->msg("Can not add chemical assay details without matched and authority columns");
    }
    $ad->bench_end('Extract chemical details');
}

if ($specialCol{seq_out}) {
    $ad->bench_start('Add Sequence');
    my $tO  = $cpos{$objCol} - 1;
    my $spO = &get_or_add_column(\%cpos, 'seq_out', \@corder);
    my @newRows;
    foreach my $row ( @{$rows}) {
        my @seqs;
        if (my $id = $row->[$tO]) {
            my @bss = $ad->fetch_bioseq
                ( -id => $id, -format => 'fasta');
            my %sd = map { uc($_) => $_ } map { $_->seq() } @bss;
            @seqs = values %sd;
        }
        @seqs = ("") if ($#seqs == -1);
        foreach my $seq (@seqs) {
            my @r = @{$row};
            $r[$spO] = $seq;
            push @newRows, \@r;
        }
    }
    $rows = \@newRows;
    $ad->bench_start('Add Sequence');
}

if (my $fg = $specialCol{flag_go}) {
    $rows = &flag_go($rows, $fg);
}

&add_custom_sets( $rows );

# gas -warn -mode simple -cols 'termin,sym,#APS#[#AAD#HT_MG_430A],#APS#[#AAD#HT_MG_430A_LL],#APS#[#AAD#HT_MG_430A_LLBL],#APS#[#AAD#HT_MG_430A_RSRBL],#APS#[#AAD#HT_MG_430A_ENSTBL],#APS#[#AAD#HT_MG_430A_ENSEBL],desc' -id LOC67220,LOC676827,LOC24084

if (my $con = $specialCol{convert}) {
    my $tO  = $cpos{$objCol} - 1;
    my $nOn = $objCol; $nOn =~ s/term/ns/;
    my $n2  = $cpos{$nOn} ? $cpos{$nOn} -1 : undef;

    my %objs;
    foreach my $row (@{$rows}) {
        my ($t, $n) = ($row->[$tO], $n2 ? $row->[$n2] : undef);
        $objs{$n || ""}{$t} = 1 if ($t);
    }
    my @onss = sort keys %objs;
    foreach my $ons (@onss) {
        delete $objs{$ons}{""};
        my @uniq = keys %{$objs{$ons}};
        if ($#uniq == -1) {
            delete $objs{$ons};
        } else {
            $objs{$ons} = \@uniq;
        }
    }

    foreach my $ctxt (sort keys %{$con}) {
        my $op = &get_or_add_column(\%cpos, $ctxt, \@corder);
        my $cArgs = $con->{$ctxt};
        my ($id, $ns, $int, $intns) = &_id_and_ns( $ctxt );
        my %mapped;
        foreach my $ons (@onss) {
            my $orows = $ad->convert
                ( -id => $objs{$ons}, -ns1 => $ons, -warn => $doWarn,
                  %{$cArgs}, -dumpsql => $dumpsql || 1,
                  -nonull => 1, -nullscore => -1,
                  -nolist => $args->{NOLIST},
                  -uselist => $args->{USELIST},
                  -cols => ['term_in', 'term_out' ]);
            foreach my $orow (@{$orows}) {
                my ($i, $o) = @{$orow};
                $mapped{$i}{$o} = 1;
            }
        }
        foreach my $row (@{$rows}) {
            if (my $ti = $row->[$tO]) {
                if (my $h = $mapped{$ti}) {
                    $row->[$op] = join(',', sort keys %{$h});
                }
            }
        }
    }
}

if ($fmtFlags{Null}) {
    &finish_up;
}

my $isListFormat = $fmtFlags{List} || $fmtFlags{String} || $fmtFlags{RegExp};

my ($gridPreCols, $gridPreRows) = (0,0);
if ($fmtFlags{Grid}) {
    my (%colData, %rowData, %colMembers);
    my ($tO, $tI) = ($cpos{$objCol}, $cpos{$qryCol});
    unless ($tO && $tI) {
        $args->death("Can not complete Grid formatting",
                     "Both the query and subject (term_in and term_out usually) must be available in results");
    }
    my ($dO, $dI, $sc, $au, $symI, $symO) = map { $cpos{$_} } 
    qw(desc_out desc_in matched auth sym_in sym_out);
    map { $_-- } ($tO, $tI);
    foreach my $row (@{$rows}) {
        my $i = $row->[$tI];
        next unless ($i);
        $rowData{$i} ||= { term_in => $i };
        $rowData{$i}{desc_in} ||= $row->[$dI - 1] if ($dI);
        $rowData{$i}{sym_in}  ||= $row->[$symI - 1] if ($symI);
        if (my $o = $row->[$tO]) {
            $colData{$o} ||= { term_out => $o };
            $colData{$o}{desc_out} ||= $row->[$dO - 1] if ($dO);
            my $targ = $rowData{$i}{$o} ||= [undef, {}];
            my ($score, $auth);
            if ($sc) {
                # We have a score column, capture it
                $score = $row->[$sc - 1];
                $score = -1 unless (defined $score);
            }
            if ($au) {
                # We have an authority column, capture it
                $auth = $row->[$au - 1] || "";
                # Use only the "first" author in the chain
                $auth =~ s/\s+\<.+//;
                if ($noAu || !$addEc) {
                    # We need to trim up the authority
                    my $ec;
                    if ($auth =~ /(.+)\s+\[([^\]]+)\]/) {
                        ($auth, $ec) = ($1, $2);
                    }
                    if ($noAu) {
                        # Just use the evidence code
                        $auth = $ec || "";
                    }
                }
            }
            $colMembers{$o}{$i} = 1;
            unless (defined $score) {
                # No score column, just note all authorities
                $targ->[1]{$auth}++ if (defined $auth);
                next;
            }
            if (!defined $targ->[0] || $targ->[0] < $score) {
                # We found a better score, reset the data
                $targ = $rowData{$i}{$o} = [ $score, {} ];
            }
            $targ->[1]{$auth}++ if (defined $auth);
        }
    }
    $rows = [];
    @corder = ('term_in');
    push @corder, ('desc_in') if ($dI);
    push @corder, ('sym_in') if ($symI);
    my @outs = keys %colData;
    if (1) {
        while (my ($o, $ins) = each %colMembers) {
            my @uniq = keys %{$ins};
            $colMembers{$o} = $#uniq + 1;
        }
        @outs = sort {$colMembers{$b} || 0 <=> $colMembers{$a} || 0} @outs;
    } else {
        @outs = &fancySort(\@outs);
    }
    my @ins  = &fancySort([keys %rowData]);
    if ($dO) {
        my @outDesc = map { "" } @corder;
        $outDesc[0] = "Output Description";
        foreach my $o (@outs) {
            push @outDesc, $colData{$o}{desc_out} || "";
        }
        push @{$rows}, \@outDesc;
    }
    $gridPreRows = $#{$rows};
    $gridPreCols = $#corder;

    push @corder, @outs;
    map { $hdata->{$_} = { label => $_ } } @outs;
    %cpos = map { $corder[$_] => $_ + 1} (0..$#corder);
    my %inds = map { $_ => $cpos{$_} - 1 } keys %cpos;
    foreach my $q (@ins) {
        my @row;
        while (my ($key, $val) = each %{$rowData{$q}}) {
            if (ref($val)) {
                # This is a primary cell
                my ($score, $aHash) = @{$val};
                $val = $score;
                if (!defined $val || !$noAu) {
                    my @auths = sort keys %{$aHash};
                    my $auth  = join(',', @auths) || 'X';
                    $val = defined $val ? "$val $auth" : $auth;
                }
            }
            $row[ $inds{$key} ] = $val;
        }
        map { $row[$_] = "" unless (defined $row[$_]) } (0..$#corder);
        push @{$rows}, \@row;
    }
    $colReq = undef;
    $sortKey = 'NO_SORT';
}

if ($colReq || $noCol || $metaCols) {
    $ad->bench_start('Prune Output Columns');
    my ($nmIn, $nmOut) = map { $corder[$_] } ($idxIn, $idxOut);
    my %indHash = map { $corder[$_] => $_ } (0..$#corder);
    my %skip    = map { &_standardize_column($_) => 1 }
    split(/[\s\,\n\r]+/, $noCol || '');
    my @cReq = $colReq ? @{$colReq} : @corder;
    @corder = ();
    foreach my $req (@cReq) {
        my $col = &_standardize_column( $req );
        next unless ($col && !$skip{$col});
        push @corder, $col unless ($savedTable && $col eq 'term_in');
    }
    &add_column_dependencies( \@corder );
    # Assure that term_in is at the front when working with a saved table:
    unshift @corder, 'term_in' if ($savedTable);
    if ($metaCols) {
        my %h = map {$corder[$_] => $_ + 1} (0..$#corder);
        if (my $i = $h{metasets}) {
            # See if it is numeric, if so, track min-max:
            my %minMax;
            foreach my $iH (values %{$metaSets}) {
                while (my ($n, $v) = each %{$iH}) {
                    if ($v =~ /^[\-\+]?(\d+|\d*\.\d+)$/) {
                        my $mm = $minMax{$n} ||= [$v,$v];
                        if ($v < $mm->[0]) {
                            $mm->[0] = $v;
                        } elsif ($v > $mm->[1]) {
                            $mm->[1] = $v;
                        }
                    }
                }
            }
            my @insMeta;
            for my $m (0..$#{$metaCols}) {
                my $l = $metaCols->[$m];
                my $n = 'metasets'.$m;
                $hdata->{$n} = {
                    label => $l,
                    mode  => "character",
                    desc  => "User-supplied metadata tied to output values",
                };
                # Callback may have been set under column label:
                $ehColCB{$n} = $ehColCB{$l};
                push @insMeta, $n;
                if (my $mm = $minMax{$l}) {
                    my $min = $mm->[0];
                    if (my $diff = $mm->[1] - $min) {
                        # Set the color based on (min..max) range
                        $ehColCB{$n} ||= sub {
                            my ($v) = @_;
                            return undef unless 
                                (defined $v && $v =~ /^[\-\+]?(\d+|\d*\.\d+)$/);
                            return 'col'.int
                                (0.5 + ($v - $min) * $colScale/$diff);
                        };

                     }
                }
            }
            splice(@corder, $i - 1, 1, @insMeta);
        }
    }

    my @keep    = map { $indHash{$_} } @corder;
    for my $i (0..$#{$rows}) {
        my @n; map { push @n, (defined $_) ? $rows->[$i][$_] : '' } @keep;
        $rows->[$i] = \@n;
    }
    %cpos = map { $corder[$_] => $_ + 1} (0..$#corder);
    ($idxIn,$idxOut) = map {$cpos{$_||""} ? $cpos{$_||""} - 1 : undef} ($nmIn,$nmOut);
    $ad->bench_end('Prune Output Columns');
}
&add_meta_sets( $rows );

if ($fmtFlags{NiceChem}) {
    &nice_chem($rows, $head, \%cpos, \@corder);
}
if ($fmtFlags{PubMed}) {
    &extract_pubmed($rows, $head, \%cpos, \@corder);
}


$sortKey  = &_standardize_column($sortKey || 'term_out');
my @sortInds;
if ($sortKey && $sortKey !~ /^(none|no)$/i) {
    my @kz = ref($sortKey) ? @{$sortKey} : ($sortKey);
    foreach my $k (@kz) {
        if (my $p = $cpos{$k}) {
            push @sortInds, $p - 1;
        }
    }
}

if ($#sortInds != -1 && ! $isListFormat) {
    $ad->bench_start('Sorting');
    # Need to implement multi column sorting
    my $skInd = $sortInds[0];
    $rows = &fancySort($rows, $skInd, $sortKey);
    if ($sortKey eq 'matched') {
        my @descending;
        foreach my $row (@{$rows}) {
            if (!defined $row->[$skInd] || $row->[$skInd] eq '') {
                # Make sure undefined goes at the end
                push @descending, $row;
            } else {
                unshift @descending, $row;
            }
        }
        $rows = \@descending;
        # $rows = [reverse @{$rows} ];
    }
    $ad->bench_end('Sorting');
}

&compare_list( $rows );
if ($fmtFlags{Oracle}) {
    map { delete $fmtFlags{$_} } 
    qw(HTML Atom Fasta Genbank JSON DAD SetFile OntologyReport Structured TiddlyWiki Text TSV LiterateTSV CSV Excel Image List String RegExp);
    $head = undef;
}

if ($fmtFlags{BED}) {
    # https://genome.ucsc.edu/FAQ/FAQformat.html#format1
    # Organize data by build and namespace
    my @problems;
    my @srcInds;
    # die $args->branch(\%cpos);
    my $oInd = $cpos{term_out};
    if ($oInd) { $oInd--; } else { push @problems, 'term_out'; }
    my $bInd = $cpos{vers_out};
    if ($bInd) { $bInd--; } else { push @problems, 'vers_out'; }
    my $n1Ind = $cpos{ns_in};
    my $ftInd = $cpos{loc_out};
    my $deInd = $cpos{desc_in};
    my $relNS = $args->val('related');
    my $doCol = $args->val('colorize');
    foreach my $col (qw(start_out end_out term_in matched strand)) {
        if (my $ind = $cpos{$col}) {
            push @srcInds, $ind - 1;
        } else {
            push @problems, $col;
        }
    }
    my %tracks;
    #  die $args->branch($row);
    if ($#problems == -1) {
        my $iInd = $cpos{term_in} - 1;
        my (%colors, %related);
        if ($relNS || $doCol) {
            # Get related objects. Need a better parameter name
            my %inH;
            foreach my $row (@{$rows}) {
                if (my $id = $row->[$iInd]) {
                    my $ns = $n1Ind ? $row->[$n1Ind - 1] || "" : "";
                    $inH{$ns}{$id} ||= 1;
                }
            }
            my $num = 0;
            if ($doCol) {
                my $cu = BMS::Utilities::ColorUtilities->new();
                while (my ($iNs, $idH) = each %inH) {
                    foreach my $id (keys %{$idH}) {
                        next if ($colors{$id});
                        $num++;
                        my $col = $cu->pastel_text_color( $id );
                        my @rgb = $cu->rgb_values( $col );
                        $colors{$id} = join(',', @rgb);
                    }
                }
            }
            if ($relNS) {
                my %relMap;
                while (my ($iNs, $idH) = each %inH) {
                    my $relRow = $ad->convert
                        ( -id => [keys %{$idH}], -ns1 => $iNs, 
                          -ns2 => $relNS, -nonull => 1,
                          -cols => ['term_in', 'term_out'], );
                    foreach my $rr (@{$relRow}) {
                        my ($in, $out) = @{$rr};
                        $relMap{uc($out)}{$in}++;
                    }
                    my $maps = $ad->mappings
                        ( -id => [keys %relMap], -ns1 => $relNS,
                          -build => $genomeBuild,
                          -cols => [qw(sub sub_vers sub_start sub_end qry score strand sub_ft)] );
                    foreach my $line (@{$maps}) {
                        next unless ($line->[0]);
                        $line->[10] = $relNS;
                        $line->[11] = \%tracks;
                        # $line has indicies 0..7
                        my @via = sort keys %{$relMap{ uc($line->[4]) }};
                        my $first = $via[0];
                        if ($num) {
                            if ($#via == 0) {
                                $line->[9] = $colors{ $first };
                            } else {
                                my $ind = int(0.5 + 255 * (1 - ($#via + 1) / $num));
                                $line->[9] = "$ind,$ind,$ind";
                            }
                        }
                        $line->[8] = "Derived from ".join(', ', @via)
                            if ($deInd);
                        &_bed_line( @{$line} );
                    }
                }
            }
            
        }
        my $col  = "0,0,128";
        for my $r (0..$#{$rows}) {
            my $row = $rows->[$r];
            my $id  = $row->[$iInd] || "";
            my $ns1 = $n1Ind ? $row->[$n1Ind - 1] : "";
            my $line = &_bed_line
                ( $row->[$oInd], $row->[$bInd], (map { $row->[$_] } @srcInds),
                  $ftInd ? $row->[$ftInd - 1] : undef,
                  $deInd ? $row->[$deInd - 1] : undef,
                  $colors{$id} || $col, $ns1, \%tracks );
         }
    } else {
        $args->msg("[!]", "Output does not have all columns needed for BED format","Missing: ".join(', ', @problems));
    }
    my $tFmt = 'track name="%s" description="%s" useScore="On"';
    $tFmt   .= ' type="bedDetail"' if ($deInd);
    $tFmt   .= ' itemRgb="On"' if ($doCol);
    $tFmt   .= "\n";
#    die $args->branch(\%tracks);
    foreach my $track (sort { $a->{name} cmp $b->{name} } values %tracks) {
        printf($outFH $tFmt, $track->{name}, $track->{desc});
        foreach my $row (@{$track->{rows}}) {
            print $outFH join("\t", @{$row})."\n";
        }
    }
    &finish_up;
}

if ($fmtFlags{HTML} && $args->{SHOWKEY}) {
    my $key = $ad->link_key();
    print $outFH $fmtFlags{DAD} ? &esc_js($key) : $key;
}

if ($fmtFlags{XGMML} || $fmtFlags{GML} || $fmtFlags{CanvasXpress}) {
    $ad->bench_start('Format Graph');
    my $graph = &_structure_node_data( $rows );

    $args->msg("Structuring Graph") if ($doWarn);
    $graph->remove_isolated_nodes if ($args->val(qw(nosingles nosingle)));
    
    my %byNS; map { 
        push @{$byNS{$_->attribute('ns') || ''}}, $_;
    } $graph->each_node();

    my $nometa = $args->val(qw(nometa));
    my $nodesc = $nometa || $args->val(qw(nodesc nodescription));
    while (my ($ns, $nodes) = each %byNS) {
        my $nameSfx = " [$ns]";
        my $isBio = 0;
        map { $isBio++ if ($ad->is_namespace($ns, $_)) } qw(AL AR AP);
        next if ($nometa);
        my @names = map { $_->attribute('label') } @{$nodes};
        # print join(" + ", @names)."<br />\n";
        unless ($nodesc) {
            $args->msg("Recovering Descriptions for $ns") if ($doWarn);
            foreach my $name (@names) {
                my $desc = &_get_desc( $name, $ns);
                $graph->set_node_attributes
                    ( $name.$nameSfx, description => $desc );
                # print "<pre>$name [$ns] = $desc</pre>" if ($ns eq 'LocusLink Gene');
            }
        }
        if ($isBio) {
            $args->msg("Recovering Gene Symbols for $ns") if ($doWarn);
            my $symHash = $ad->bulk_best_symbol
                ( -id    => \@names,
                  -ns    => $ns,
                  -trunc => 1,
                  -best  => 1,
                  -short => 1,
                  -explainsql => $exSql );
            while (my ($name, $syms) = each %{$symHash}) {
                $graph->set_node_attributes
                    ( $name.$nameSfx, symbol => join(",", @{$syms}) );
            }

            $args->msg("Recovering Species for $ns") if ($doWarn);
            my $taxRows = $ad->convert
                ( -id => \@names, -ns1 => $ns, -ns2 => 'tax', -nonull => 1,
                  -cols => ['term_in', 'term_out'], );
            foreach my $row (@{$taxRows}) {
                $graph->set_node_attributes
                    ( $row->[0].$nameSfx, taxa => $row->[1] );
            }
        }
    }
        
    my %isQry    = map { $_->[0] => 1 } map {
        [$ad->standardize_id($_)] } @uniqQ;
    my %specObj;
    if (my $cmp = $args->val(qw(compgraph))) {
        $args->msg("Comparing to another graph file", $cmp);
        open(CMP, "<$cmp") || $args->death
            ("Failed to read comparative graph", $cmp, $!);
        while (<CMP>) {
            s/[\n\r]+$//;
            if (/^\s*<node/) {
                if (/label=\'([^\']+)\'/) {
                    $specObj{$1} = 1;
                }
            }
        }
        close CMP;
    }
    $graph->attribute_generator('shape', 'Node', sub {
        my $node = shift;
        return 'roundrect'
            if ($isQry{$node->attribute('label') || $node->name()});
        my $ns   = $node->attribute('ns') || "";
        my $shape = 'circle';
        if ($ns eq 'LocusLink Gene' || 
            $ns eq 'RefSeq RNA' || $ns eq 'RefSeq Protein') {
            $shape = 'star';
        } elsif ($ns eq 'Ensembl Gene' || 
            $ns eq 'Ensembl Transcript' || $ns eq 'Ensembl Protein') {
            $shape = 'octagon';
        }
        return $shape;
    });
    $graph->attribute_generator('tooltip', 'Node', sub {
        my $node = shift;
        my @tbits;
        if (my $lab = $node->attribute('label'))  { push @tbits, $lab; }
        if (my $sym = $node->generated_attribute('symbol')) {
            push @tbits, "[$sym]"; }
        if (my $tax = $node->generated_attribute('taxa'))   { 
            push @tbits, "($tax)"; }
        return join(' ', @tbits);
    });
    $graph->attribute_generator('edges', 'Node', sub {
        return shift->edge_count;
    });
    my %roots;
    foreach my $ntok (qw(AL AP AR)) {
        foreach my $ctok ($ad->namespace_children( $ntok )) {
            $roots{$ctok} = $ntok;
            $roots{$ad->namespace_name($ctok)} = $ntok;
        }
    }
    $graph->attribute_generator('edge.lineStyle', 'Edge', sub {
        my $edge = shift;
        my ($n1, $n2) = ($edge->node1, $edge->node2);
        my ($ns1, $ns2) = map { $_->attribute('ns') } ($n1, $n2);
        return ($roots{$ns1 || ""} && $roots{$ns2 || ""} &&
                $roots{$ns1} eq $roots{$ns2}) ? 'LONG_DASH' : undef;
    });
    $graph->attribute_generator('taxa', 'Edge', sub {
        my $edge  = shift;
        my @unique;
        my @values = map { $_->generated_attribute('taxa') } $edge->nodes();
        foreach my $val (@values) {
            push @unique, $val if ($val && 
                                   ($#unique == -1 || $val ne $unique[-1]));
        }
        return join(' + ', @unique);
    });
    my (%symLab, %descLab);
    if (my $sl = $args->val(qw(symlab labsym labelsym symlabel))) {
        foreach my $ns (split(/\s*[\n\r\t\,]+\s*/, $sl)) {
            if (my $nsn = $ad->namespace_name($ns)) {
                $symLab{$nsn} = 1;
            }
        }
    }
    if (my $dl = $args->val(qw(desclab labdesc labeldesc desclabel))) {
        foreach my $ns (split(/\s*[\n\r\t\,]+\s*/, $dl)) {
            if (my $nsn = $ad->namespace_name($ns)) {
                $descLab{$nsn} = 1;
            }
        }
    }
    $graph->attribute_generator('label', 'Node', sub {
        my $node = shift;
        my $lab  = $node->attribute('label') || $node->name;
        my $ns   = $node->attribute('ns') || 'Unknown';
        if ($descLab{$ns}) {
            if (my $desc = $node->attribute('description')) {
                $lab = $desc;
            }
        } elsif ($symLab{$ns}) {
            if (my $sym = $node->attribute('symbol')) {
                $lab = $sym;
            }
        }
        return $lab;
    });

    $graph->attribute_generator('symbol', 'Edge', sub {
        my $edge  = shift;
        my @unique;
        my @values = map { $_->generated_attribute('symbol') } $edge->nodes();
        foreach my $val (@values) {
            push @unique, $val if ($val && 
                                   ($#unique == -1 || $val ne $unique[-1]));
        }
        return join(' + ', @unique);
    });
    my $nWidth      = 35;
    $graph->attribute_generator('score', 'Edge', sub {
        my $edge = shift;
        my $scores = $edge->attribute('scoreList');
        return undef unless ($scores);
        my @sorted = sort { $b->[0] <=> $a->[0] } @{$scores};
        return $sorted[0][0];
    });
    $graph->attribute_generator('auth', 'Edge', sub {
        my $edge = shift;
        my $scores = $edge->attribute('scoreList');
        return undef unless ($scores);
        my @sorted = sort { $b->[0] <=> $a->[0] } @{$scores};
        return $sorted[0][1];
    });

    my $numQry  = $#uniqQ + 1;
    my $gLabel  = $numQry > 1 ? "$numQry IDs" : join(' + ',@uniqQ);
    $gLabel    .= ' ' .($cloudType || "Network");
    $graph->attribute('label', $gLabel);
    $graph->attribute('stampLabel', 'true');
    $graph->attribute_type('graphics', 'XML');
    map { $graph->attribute_type($_, 'real') } ('score', 'edges');

    my @colMap      = ( AC  => '#00ffff', # compounds are cyan
                        AL  => '#00ff00', # genes     are green
                        AR  => '#cc0033', # RNA       are red
                        AP  => '#ff33ff', # Protein   are purple
                        ONT => '#ffff00', # Ontology  are yellow
                        );
    my $nodeColGen = sub {
        my $node = shift;
        my $col  = "#888888";
        my $lab  = $node->generated_attribute('label');
        if ($specObj{$lab}) {
            $col = "#000000";
        } elsif (my $ns   = $node->attribute('ns')) {
            for (my $ci = 0; $ci <= $#colMap; $ci += 2) {
                if ($ad->is_namespace($ns, $colMap[$ci])) {
                    $col = $colMap[$ci+1];
                    last;
                }
            }
        }
        return $col;
    };
    my $edgeColGen = sub {
        my $edge = shift;
        my $sc   = $edge->generated_attribute('score');
        my $ind  = defined $sc && $sc >= 0 ? 1+ int(10 * $sc) : 0;
        return '#' . $edgeCols[$ind];
    };
    if ($fmtFlags{CanvasXpress}) {
        $graph->attribute('showAnimation', '1');
        my @opts = ( -pretty => $doPretty, );
        if ($fmtFlags{HTML}) {
            my $canId = 'GAcanvasXpress';
            print "<canvas id='$canId' width='1024' height='768'></canvas>\n<script>\n";
            push @opts, ( -id => $canId );
        } else {
            push @opts, ( -bracket => 1 );
        }
        $graph->attribute_generator('fill', 'Node', sub {
            return &{$nodeColGen}(shift);
        });
        $graph->attribute_generator('fill', 'Edge', sub {
            return &{$edgeColGen}(shift);
        });
        print $outFH $graph->to_canvasXpress( @opts );
        if ($fmtFlags{HTML}) {
            print $outFH "</script>\n";
        }
    } elsif ($fmtFlags{GML}) {
        print $outFH $graph->to_gml( );
    } else {
        $graph->attribute_generator('graphics', 'Edge', sub {
            return {
                fill  => &{$edgeColGen}(shift),
                width => 4,
            };
        });

        $graph->attribute_generator('graphics', 'Node', sub {
            my $node = shift;
            my $col  = &{$nodeColGen}($node);
            my $lab  = $node->generated_attribute('label');
            my $dynWid = 10 + length($lab) * 7;
            my $grph = {
                w       => $dynWid,
                h       => $nWidth,
                x       => $node->attribute('x'),
                y       => $node->attribute('y'),
                type    => 'ellipse',
                outline => '#000033',
                fill    => $col,
            };
            return $grph;
        });
        $graph->layout( -type       => $args->{LAYOUT}      || 'grid',
                        -iterations => $args->{ITERATIONS}  || 50,
                        -wedges     => $args->{WEDGES}      || 10,
                        -forbid     => $nWidth,
                        -scale      => $args->{LAYOUTSCALE} || $nWidth * 4, );
        print $outFH $graph->to_xgmml();
    }
    $ad->bench_end('Format Graph');
    print STDERR $ad->showbench( $nocgi ? (-shell => 1) : (html => 1),
                                 -minfrac => $doBench,) if ($doBench);
    &finish_up;
}

if (0 && $fmtFlags{GML}) {
    $ad->bench_start('Format GML');
    my ($nodes, $edges) = &_structure_node_data( $rows );
    print $outFH "graph [\n";
    my @allNodes = sort { $a->{id} <=> $b->{id} } values %{$nodes};
    foreach my $nd (@allNodes) {
        print $outFH "  node [\n";
        foreach my $key (sort keys %{$nd}) {
            my $val = $nd->{$key};
            next if (!defined $val || $val eq "");
            $val = "\"$val\"" unless ($val =~ /^\d+$/);
            printf($outFH "    %s %s\n",$key, $val);;
        }
        if (my $ns = $nd->{ns}) {
            my $col = 
                $ad->is_namespace($ns, 'AL') ? '#336600' :
                $ad->is_namespace($ns, 'AR') ? '#cc0033' :
                $ad->is_namespace($ns, 'AP') ? '#ff33ff' : '#888888';
            my %params = ( fill => $col );
            print $outFH "    graphics [\n";
            foreach my $key (sort keys %params) {
                my $val = $params{$key};
                next if (!defined $val || $val eq "");
                $val = "\"$val\"" unless ($val =~ /^\d+$/);
                printf($outFH "      %s %s\n",$key, $val);;
            }
            print $outFH "    ]\n";
        }
        print $outFH "  ]\n";
    }
    my %i2n   = map { $_->{id} => $_->{label} } @allNodes;
    my %n2sym = map { $_->{label} => $_->{symbol} } @allNodes;
    foreach my $s ( sort keys %{$edges}) {
        foreach my $t (sort keys %{$edges->{$s}}) {
            my $edge = $edges->{$s}{$t};
            my ($sn1, $sn2) = map { $i2n{ $edge->{$_} } } qw(source target);
            my @lbits;
            my $sc = $edge->{score};
            if (my $sym = $n2sym{$sn1}) { push @lbits, "$sym"; }
            if ($sc && $sc >= 0) {
                push @lbits, sprintf("-%3d-", int(0.5 + 100 * $edge->{score}));
            } else {
                push @lbits, '-----';
            }
            if (my $sym = $n2sym{$sn2}) { push @lbits, "$sym"; }
            $edge->{label} = join(' ', @lbits);
            print $outFH "  edge [\n";
            foreach my $key (sort keys %{$edge}) {
                my $val = $edge->{$key};
                next if (!defined $val || $val eq "");
                $val = "\"$val\"" unless ($val =~ /^\d+$/);
                print $outFH "    $key $val\n";
            }
            my $ind   = defined $sc ? 1+ int(10 * $sc) : 0;
            my %params = ( fill => '#' . $edgeCols[$ind],
                           width => 2,);
            print $outFH "    graphics [\n";
            foreach my $pkey (sort keys %params) {
                my $val = $params{$pkey};
                next if (!defined $val || $val eq "");
                $val = "\"$val\"" unless ($val =~ /^\d+$/);
                printf($outFH "      %s %s\n",$pkey, $val);;
            }
            print $outFH "    ]\n";
            print $outFH "  ]\n";
        }
    }
    print $outFH "]\n";
    $ad->bench_end('Format GML');
    print STDERR "\n" if ($doWarn);
    $args->msg($ad->showbench( $nocgi ? (-shell => 1) : (html => 1),
                               -minfrac => $doBench)) if ($doBench);
    &finish_up;
} elsif ($fmtFlags{Atom}) {
    $ad->bench_start('Format Atom');
    my %feeds;
    my @srcCols    = qw(term_in ns_in term_out ns_out desc_out updated auth
                        links desc_link com_link);
    my %globalVals;
    if ($mode eq 'link') {
        $globalVals{auth}    = 'MapTracker';
        $globalVals{updated} = $ad->dbh->oracle_now();
    }
    my %inds = map { $_ => $cpos{$_} } @srcCols;
    my %absent;
    map { if ($inds{$_}) {
        $inds{$_}--;
    } else {
        $absent{$_} = 1;
    } } @srcCols;
    my $getVal = sub {
        my ($key, $row) = @_;
        my $rv = $globalVals{$key} || 
            ( $absent{$key} ? undef : $row->[ $inds{$key} ] );
        $rv = undef if (defined $rv && $rv eq '');
        return $rv;
    };
    my @indCB;
    foreach my $sc (@srcCols) {
        my $ind = $inds{$sc};
        if (defined $ind) {
            push @indCB, sub { my $row = shift; return $row->[$ind]; };
        } else {
            my $gv = $globalVals{$sc};
            push @indCB, sub { return $gv; };
        }
    }
    foreach my $row (@{$rows}) {
        my @vals = map { &{$_}( $row ) } @indCB;
        # my @vals = map { $globalVals{$_} || $row->[$inds{$_}] } @srcCols;
        # my ($in, $nns, $out, $ons, $desc, $dt, $auth) = @vals;
        my $in   = &{$getVal}('term_in', $row) || "Unknown Input";
        my $idat = $feeds{$in};
        unless ($idat) {
            my $nns  = &{$getVal}('ns_in', $row);
            $idat = $feeds{$in} = {
                id       => $in,
                ns       => $ad->namespace_token( $nns ),
                entries  => {},
                subtitle => &_get_desc( $in, $nns) || undef,
            };
        }
        if (my $out = &{$getVal}('term_out', $row)) {
            my $odat = $idat->{entries}{$out};
            unless ($odat) {
                my $ons  = &{$getVal}('ns_out', $row);
                my $desc = &{$getVal}('desc_out', $row) || 
                    $absent{'desc_out'} ? &_get_desc( $out, $ons ) : undef;
                $odat = $idat->{entries}{$out} = {
                    id       => $out,
                    ns       => $ad->namespace_token($ons),
                    summary  => $desc || undef,
                    updated  => &{$getVal}('updated', $row),
                };
            }
            my @auths = split(/\s+[\+\<]\s+/, &{$getVal}('auth', $row) ||'');
            if (my $url = &{$getVal}('links', $row)) {
                my $lnknm = &{$getVal}('desc_link', $row) || '';
                map { $odat->{links}{$lnknm}{$url}{$_} = 1 } @auths;
            } else {
                map {$odat->{auth}{$_} = 1} @auths;
            }
            
        }
    }
    my $rv = "<?xml version='1.0' encoding='utf-8'?>\n";
    my $aHead = "<feed xmlns='http://www.w3.org/2005/Atom' xml:lang='en'>\n";
    my @common = qw(id title updated);
    foreach my $id (sort keys %feeds) {
        my $dat = $feeds{$id};
        my @entries = sort { $a->{id} cmp $b->{id} } values %{$dat->{entries}};
        my ($useDate) = sort { $b->{updated} cmp $a->{updated} } @entries;
        $dat->{updated} = $useDate ? $useDate->{updated} : undef;
        $rv .= $aHead;
        $rv .= &atom_node($dat, 1);
        foreach my $entry (@entries) {
            if (my $lnks = $entry->{links}) {
                foreach my $lnknm (sort keys %{$lnks}) {
                    foreach my $url (sort keys %{$lnks->{$lnknm}}) {
                        my $ldat = {
                            url => $url,
                            ltok => $lnknm,
                            auth => $lnks->{$lnknm}{$url},
                            
                        };
                        map { $ldat->{$_} = $entry->{$_} } qw(id ns summary updated);
                        push @entries, $ldat;

                    }
                }
                next;
            }
            $rv .= " <entry>\n";
            $rv .= &atom_node($entry, 2);
            $rv .= " </entry>\n";
        }
        $rv .= "</feed>\n";
    }
    print $outFH $rv;
    $ad->bench_end('Format Atom');
    &finish();
    &finish_up;
}

if ($fmtFlags{Fasta} || $fmtFlags{Genbank}) {
    $ad->bench_start('Format Fasta');
    if ($fmtFlags{JSON}) {
        print $outFH " sequenceData: [\n";
    }
    my (%history, %errs, @ordered, %skipped, $snper);
    my $mt     = $ad->tracker;
    my $num    = 0;
    my $mtNs   = $ad->maptracker_namespace( $ns2 ) || "";
    my $isSeq  = uc($mtNs) eq '#SEQUENCE#' ? 1 : 0;
    my $sNsTok = $ad->namespace_token($ns2);
    my $noHB   = $args->{NOHITBY};
    if ($fmtFlags{Genbank}) {
        $history{IO} = Bio::SeqIO->new( -fh => $outFH, -format => 'genbank' );
    }
    if ($fmtFlags{Ambig}) {
        require BMS::SnpTracker::MapLoc::SnpProjector;
        $snper = BMS::SnpTracker::MapLoc::SnpProjector->new
            ( -build => $genomeBuild);
        my $mf = $args->val(qw(minfrac));
        $mf = 0.05 unless (defined $mf);
        $snper->param("minfrac",$mf);
        $snper->param("population", 'dbSNP Populations');
    }
    if ($mode eq 'map') {
        my ($in, $out, $ia, $istr, $s, $e, $b) = map { $cpos{$_} }
        qw(term_in term_out auth strand start_out end_out vers_out);
        if ($out && $s && $e && $b) {
            map { $_-- } ($out, $s, $e, $b);
            my %locs;
            foreach my $row (@{$rows}) {
                my $loc = sprintf("%s.%s:%d-%d", $row->[$out], $row->[$b], 
                                  $row->[$s], $row->[$e]);
                if ($istr) {
                    if (my $str = $row->[$istr-1]) {
                        $loc .= "[$str]";
                    }
                }
                my $targ = $locs{$loc} ||= {};
                if ($in) {
                    if (my $acc = $row->[$in-1]) {
                        $targ->{acc}{$acc}++;
                    }
                }
                if ($ia) {
                    if (my $auth = $row->[$ia-1]) {
                        $targ->{auth}{$auth}++;
                    }
                }
            }
            my @all = sort keys %locs;
            unless ($#all == -1) {
                my $outfile = sprintf("%s/GenomeFetcher-%d-%d.tsv",
                                      $tmpdir, time, $$);
                my $pfile = $outfile . ".param";
                open(PFILE, ">$pfile") || $args->death
                    ("Failed to create parameter file", $pfile, $!);
                print PFILE <<PF;
# Parameter file for Genome Fetcher
# Fasta output request from GenAcc

-format  => TSV
-lcflank => 1
PF


                if (my $flank = $args->val(qw(flank))) {
                    print PFILE "-flank => $flank\n";
                }
                print PFILE "-request => <<GAREQUEST;\n";
                foreach my $loc (@all) {
                    print PFILE $loc;
                    my @desc;
                    if (my $h = $locs{$loc}{acc}) {
                        push @desc, "From ".join(",", sort keys %{$h});
                    }
                    if (my $h = $locs{$loc}{auth}) {
                        push @desc, "Via ".join(",", sort keys %{$h});
                    }
                    print PFILE ' # '.join(' ', @desc) unless ($#desc == -1);
                    print PFILE "\n";
                }
                print PFILE "GAREQUEST\n";
                close PFILE;
                my $cmd = '/stf/biocgi/'.
                    ($isBeta ? 'tilfordc/working/scriptscat/' : '').
                    'genomeFetcher.pl';
                $cmd .= " -paramfile $pfile > $outfile";
                system($cmd);
                open(GFOUT, "<$outfile") || $args->death
                    ("Failed to read output", $outfile, $!);
                my @sds;
                while (<GFOUT>) {
                    s/[\n\r]+$//;
                    my ($id, $desc, $seq) = split(/\t/);
                    if ($cleanChr) {
                        if ($id =~ /^[^\.]+\.[^\.]+\.(.+)/) {
                            $id = $1;
                        } elsif ($id =~ /^[^_]+_[^_]+_(.+)\.(.+)_(\d+\:.+)$/) {
                            $id = "$1.$2$3";
                        }
                    }
                    push @sds, [$id, $desc, $seq, $seq];
                }
                close GFOUT;
                &process_fasta(\@sds, \%history, \%skipped);
            }
        } else {
            $args->msg("Fasta output for Map Mode must keep these columns:",
                       "term_out start_out end_out vers_out");
        }
    } elsif ($objCol ne 'term_out') {
        my $ind = $cpos{$objCol};
        if ($ind) {
            $ind--;
            foreach my $row (@{$rows}) {
                if (my $tout = $row->[$ind]) {
                    push @ordered, $tout unless ($history{uniq}{$tout});
                    $history{uniq}{$tout}{''}{1}{direct} = 1;
                }
            }
        } else {
            $args->death("Not enough information to recognize primary ID");
        }
        $sNsTok = $ad->namespace_token($ns1) if ($objCol eq 'term_in');
    } else {
        my @inds = map { $cpos{$_} } qw(term_in term_out matched auth);
        foreach my $row (@{$rows}) {
            my ($tin, $tout, $m, $auth) =
                map { defined $_ ? $row->[$_-1] : undef } @inds;
            $tin ||= "";
            if ($tout) {
                $auth ||= "Unknown";
                $m = -1 unless (defined $m);
                push @ordered, $tout unless ($history{uniq}{$tout});
                if ($noHB) {
                    $history{uniq}{$tout} ||= {};
                } else {
                    $history{uniq}{$tout}{$tin}{$m}{$auth} = 1;
                }
            } else {
                $errs{'No matches found'}{$tin} = 1;
            }
        }
    }

    if ($mode eq 'map') {
        # Already done
    } elsif ($sNsTok eq 'APRB') {
        while (my @chunk = splice(@ordered, 0, 1000)) {
            my @ids = $mt->bulk_seq2id( map { $mtNs . $_ } @chunk);
            my $prbs = $ad->convert
                ( -id => \@chunk, -ns1 => $sNsTok, -nonull => 1,
                  -ns2 => 'APS', -cols => ['term_in','term_out']);
            my $arrs = $ad->convert
                ( -id => \@chunk, -ns1 => $sNsTok, -nonull => 1,
                  -ns2 => 'AAD', -cols => ['term_in','term_out']);
            my %data;
            map { $data{$_->[0]}{probeset}{$_->[1]} = undef } @{$prbs};
            map { $data{$_->[0]}{array}{$_->[1]} = undef } @{$arrs};
            my @sds;
            for my $i (0..$#chunk) {
                my $seq = $chunk[$i];
                my @dbits;
                foreach my $tag (sort keys %{$data{$seq}}) {
                    push @dbits, "/$tag=".join
                        (",", sort keys %{$data{$seq}{$tag}});
                }
                push @sds, ["MTID:$ids[$i]", join(" ", @dbits), $seq, $seq];
            }
            &process_fasta(\@sds, \%history, \%skipped);
        }
    } elsif ($fmtFlags{Genbank}) {
        foreach my $tout (@ordered) {
            my @bss;
            if ($isSeq) {
                my $id   = $mt->get_seq($mtNs.$tout)->id;
                my $desc = &_get_desc( $tout );
                my $bs = Bio::PrimarySeq->new
                    ( -seq        => $tout,
                      -display_id => "MTID:$id",
                      -desc       => $desc, );
                push @bss, $bs;
            } else {
                @bss = $ad->fetch_bioseq
                    ( -id      => $tout, 
                      -version => $doVers,
                      -snper   => $snper,
                      -format  => 'genbank');
                if ($#bss == -1) {
                    $errs{'No sequence found'}{$tout} = 1;
                    next;
                }
            }
            &process_genbank(\@bss, $tout, \%history, \%skipped);
        }
    } else {
        my $sfmt = 'fasta';
        foreach my $tout (@ordered) {
            my @sds;
            if ($isSeq) {
                my $id   = $mt->get_seq($mtNs.$tout)->id;
                my $desc = &_get_desc( $tout );
                push @sds, [ "MTID:$id", $desc, $tout, $tout ];
            } else {
                my @bss = $ad->fetch_bioseq
                    ( -id      => $tout, 
                      -version => $doVers,
                      -snper   => $snper,
                      -format  => $sfmt);
                if ($#bss == -1) {
                    $errs{'No sequence found'}{$tout} = 1;
                    next;
                }
                foreach my $bs (@bss) {
                    my $bsid = $bs->display_id();
                    push @sds, [$bsid, $bs->desc(), uc($bs->seq), $tout];
                    #unless ($bsid =~ /^$tout/) {
                    #    $history{uniq}{$tout}{$tout}{1}{SeqStore} = 1;
                    #}
                }
            }
            &process_fasta(\@sds, \%history, \%skipped);
        }
    }

    if ($fmtFlags{JSON}) {
        print $outFH " ]\n";
    }
    if ($vb) {
        my @msg;
        my $msg = "";
        foreach my $err (sort keys %errs) {
            my @l = sort keys %{$errs{$err}};
            $msg .= sprintf("\n%s [%d]:\n", $err, $#l + 1);
            $msg .= join('', map { "  $_\n" } @l );
        }
        my @skn = sort keys %skipped;
        $args->msg("Some accessions were skipped as their sequence is already represented:", (map { "$_ = $skipped{$_}" } @skn), "If you wish to include these, use -allowduplicate") if ($#skn != -1);
    }
    &end_JSON() if ($jsonType);
    $ad->bench_end('Format Fasta');
    &finish();
    &finish_up;
}

if ($fmtFlags{SetFile}) {
    $ad->bench_start('Format Set File');
    print $outFH "# SetFile generated by GenAcc Service on ".`date`;
    my $label = "List of ";
    my $gsea = $fmtFlags{GSEA};
    if ($intersect) {
        ($intersect) = $ad->standardize_id($intersect, $intNS);
        $label .= "$intersect ";
    }
    my @src;
    my $snum = $#uniqQ + 1;
    if (my $sname = $args->val(qw(setname))) {
        push @src, $sname;
    } elsif ($snum < 6) {
        foreach my $id (@uniqQ) {
            my $gns  = $ns1 || $ad->guess_namespace($id);
            my $desc = &_get_desc( $id, $gns );
            push @src, $desc ? "$id ($desc)" : $id;
        }
    } else {
        push @src, "an explicit list of $snum IDs";
    }
    my $lns = $ns2;
    if ($mode eq 'cloud') {
        my $nOn = $objCol; $nOn =~ s/term/ns/;
        if (my $cp = $cpos{$nOn}) {
            $cp--;
            my %ns  = map { $_->[$cp] => 1 } @{$rows};
            my @allNs = keys %ns;
            if ($#allNs == 0) {
                $lns = $allNs[0];
            } else {
                $lns = "Unknown";
            }
        } else {
            $lns = "Unknown";
        }
        $label .= "$cloudType ";
    }
    $label .= "$lns objects derived from ".join(', ', @src);
    if ($gsea) {
        my $rs = $fmtFlags{GSEA} = join(' + ', @uniqQ);
        print $outFH <<OF;

# This file is designed to be used as a set specification for GSEA
# It contains all the members within '$rs', as recovered by GenAcc
# These members define the *Reference* set for a GSEA analysis.
# Check the end of the file for instructions on additional steps you
# must take to connect this set to other sets or ontologies.

OF

        print $outFH "SPLITTER:/\\t/\n";
        print $outFH join("\t", 'PARAM', 'REFRENCESET',  $rs)."\n";
        print $outFH join("\t", 'ALIAS', 'REFRENCESET',  $rs)."\n";
        my $hr = ("#" x 40)."\n";
        print $outFH "\n$hr";
        print $outFH join("\t", 'SET',  $rs)."\n";
        print $outFH "$hr\n";
        print $outFH join("\t", 'SETPARAM', 'NAME',  $rs)."\n";
        print $outFH join("\t", 'SETPARAM', 'NAMESPACE',
                          $ad->namespace_name($ns2))."\n"
                              if ($ns2 && !ref($ns2));
    } else {
        print $outFH "# NS: $lns\n";
        print $outFH "# Description: $label\n\n";
    }
    $ad->bench_end('Format Set File');
}

if ($fmtFlags{OntologyReport}) {
    $ad->bench_start('Format Ontology Report');
    my (%collected, @nodes);
    if ($mode eq 'simple') {
        if (my $in = $cpos{term_in}) {
            $in--;
            foreach my $row (@{$rows}) {
                my $tin = $row->[$in];
                $collected{$tin}{1}{"User Provided"}{$tin} = 1;
            }
        }
    } elsif ($mode eq 'children' || $mode eq 'parents') {
        my @inds;
        map { push @inds, $cpos{$_} - 1 if ($cpos{$_}) } qw(parent child);
        my %uniq;
        foreach my $row (@{$rows}) {
            foreach my $i (@inds) {
                if (my $node = $row->[$i]) {
                    $fo->node_param('class', $node, "");
                    $uniq{$node}++;
                }
            }
        }
        @nodes = keys %uniq;
        #$args->msg("Number of nodes = ".scalar(@nodes));
    } else {
        my @inds = map { $cpos{$_} - 1 } qw(term_in term_out matched auth);
        foreach my $row (@{$rows}) {
            my ($tin, $tout, $m, $auth) = map { $row->[$_] } @inds;
            next unless ($tout);
            $collected{$tout}{defined $m ? $m : -1}{$auth}{$tin} = 1;
        }
    }
    while (my ($node, $tdat) = each %collected) {
        my @scores;
        my $direct = 0;
        while (my ($m, $mdat) = each %{$tdat}) {
            my (%auths, %qrys);
            while (my ($auth, $qdat) = each %{$mdat}) {
                $auths{$auth}++;
                map {$qrys{$_}++} keys %{$qdat};
                $direct = 1 unless ($auth =~ /Inheritance/);
            }
            push @scores, [ $m < 0 ? $nullSc : $m, join(', ',sort keys %auths),
                            $#uniqQ < 1 ? undef : [sort keys %qrys]];
        }
        my $scClass = 'inh';
        if ($direct) {
            push @nodes, $node;
            $scClass = &class4score(\@scores) || 'm3';
        }
        $fo->node_param('class', $node, "m $scClass");
        $fo->node_param('stuff', $node, \@scores);
    }
    my $fpaths = $fo->build_paths( \@nodes );
    if ($jsonType && $jsonType ne 'DAD') {
        print $outFH ' "ontologyReport": {'.
            $ser->obj_to_json($fpaths, $doPretty)."}";
        &end_JSON();
        &finish_up;
    }
    my $phtml = $fo->paths2html( $fpaths);
    if ($fmtFlags{DAD}) {
        print $outFH &esc_js($phtml);
        &end_JSON();
    } else { 
        print $outFH $phtml;
    }
    $ad->bench_end('Format Ontology Report');
    &finish();
    &finish_up;
}

if ($fmtFlags{Structured}) {
    $ad->bench_start('Build Structure');
    my %struct;
    my ($aind, $nind, $mind) = ($cpos{auth}, $cpos{ns_out}, $cpos{matched});
    foreach my $row (@{$rows}) {
        my ($in, $out) = ($row->[$idxIn], $row->[$idxOut] || '');
        my ($auth, $n2, $m) = ( $aind ? $row->[$aind-1] || '' : '',
                                $nind ? $row->[$nind-1] || '' : '',
                                $mind ? $row->[$mind-1] : undef, );
        $struct{ $in }{$n2}{ $out }{ defined $m ? $m : -1}{ $auth }++;
    }
    $ad->bench_end('Build Structure');
    &format_struct(\%struct);
}

unless ($kn || $asn) {
    $ad->bench_start('Remove Null Output');
    my @populated;
    map { push @populated, $_ if (defined $_->[$idxOut]) } @{$rows};
    $rows = \@populated;
    $ad->bench_end('Remove Null Output');
}

if ($showSMI || $showInchi) {
    $ad->bench_start('Get SMILES');
    my $mt = $ad->tracker;
    my @cols = qw(term_in term_out);
    @cols = qw(parent child) if ($mode =~ /^(parents|children)$/);
    foreach my $col (@cols) {
        if (my $cin = $cpos{$col}) {
            $cin--;
            foreach my $row (@{$rows}) {
                my $val = $row->[$cin];
                if ($val && $val =~ /^MTID\:\d+$/) {
                    my $seq = $mt->get_seq($val);
                    if ($showInchi) {
                        my $struct = $ad->fast_edge_hash
                            ( -name      => $seq,
                              -keepclass => 'InChI',
                              -keeptype  => 'is a reliable alias for', );
                        my @inchis = sort keys %{$struct};
                        $row->[$cin] = $inchis[0] if ($#inchis == 0);
                    } else {
                        $row->[$cin] = $seq->name;
                    }
                }
            }
        }
    }
    $ad->bench_end('Get SMILES');
}

if ($useTaxId) {
    $ad->bench_start('Map taxa to TaxID');
    my $mt = $ad->tracker;
    my @useCols = qw(term_in term_out);
    @useCols = qw(parent child) if ($mode =~ /^(parents|children)$/);
    push @useCols, qw(tax_out tax_in);
    foreach my $col (@useCols) {
        if (my $cin = $cpos{$col}) {
            $cin--;
            foreach my $row (@{$rows}) {
                my $val = $row->[$cin];
                next unless ($val);
                my @tax = $mt->get_species($val);
                if ($#tax == 0) {
                    $row->[$cin] = $tax[0]->id;
                } elsif ($#tax != -1) {
                    $row->[$cin] = undef;
                }
            }
        }
    }
    $ad->bench_end('Map taxa to TaxID');    
}


if ($fmtFlags{HTML}) {
    $ad->bench_start('Escape HTML');
    # Escape cell contents
    #my @noEsc;
    #push @noEsc, '<a href' if ($linkWP && $linkWP =~ /link/i);
    for my $c (0..$#corder) {
        next if ($corder[$c] eq 'links');
        foreach my $row (@{$rows}) {
            if (my $val = $row->[$c]) {
                #my $noe = 0; map { $noe++ if ($val =~ /$_/) } @noEsc;
                $row->[$c] = &esc_html($val); # unless ($noe);
            }
        }
    }

    if (my $cin = $cpos{desc_out}) {
        $cin--;
        foreach my $row (@{$rows}) {
            if ($row->[$cin] && $row->[$cin] =~ /^(\[[^\]]+\])?\s*(\{[^\}]+\})?/) {
                my ($sym, $warn) = ($1, $2);
                if ($warn) {
                    my $rep  = "<span class='warn'>$warn</span>";
                    $row->[$cin] =~ s/\Q$warn\E/$rep/;
                }
                if ($sym) {
                    my $rep  = "<span class='sym'>$sym</span>";
                    $row->[$cin] =~ s/\Q$sym\E/$rep/;
                }
            }
        }
    }

    my @cls = keys %{$colLinks};
    my $cn  = 0; map { $cn++ if ($colLinks->{$_}) } @corder;
    if ($cn) {
        # We need to hyperlink some columns
        foreach my $col (@cls) {
            my $cin = $cpos{$col};
            next unless ($cin);
            my $tmp = $colLinks->{$col};
            foreach my $row (@{$rows}) {
                my $lnk = $tmp;
                while ($lnk =~ /__(\S+?)__/) {
                    my $src = $1;
                    my $sin = $cpos{$src};
                    if ($sin && defined $row->[$sin-1]) {
                        my $rep = $row->[$sin-1];
                        $lnk =~ s/__${src}__/$rep/g;
                    } else {
                        $lnk = undef;
                        last;
                    }
                }
                $row->[$cin-1] = $lnk if (defined $lnk);
            }
        }
    }
    $ad->bench_end('Escape HTML');
}

if ($linkWP) {
    $ad->bench_start('Link Wikipedia Terms');
    my @useCols;
    foreach my $col ( qw(term_in term_out wikipedia) ) {
        if (my $cin = $cpos{$col}) {
            $cin--;
            my $min;
            if ($col =~ /^term_(in|out)$/) {
                if ($min = $cpos{"ns_$1"}) {
                    $min--;
                }
            }
            push @useCols, [$cin, $min];
        }
    }
    my $doLink = $linkWP =~ /link/i ? 1 : 0;
    my $wpNs   = $ad->namespace_name('WP');
    foreach my $cdat (@useCols) {
        my ($cin, $min) = @{$cdat};
            foreach my $row (@{$rows}) {
                my $val = $row->[$cin];
                next unless ($val);
                if (defined $min) {
                    next unless ($row->[$min] && $row->[$min] eq $wpNs);
                } else {
                    my ($id, $seq) = $ad->standardize_id( $val, 'WP' );
                    next unless ($seq);
                }
                my $url = "http://en.wikipedia.org/wiki/$val";
                if ($doLink) {
                    $val =~ s/_/ /g;
                    $url = sprintf("<a href='%s'>%s</a>", $url, $val);
                }
                $row->[$cin] = $url;
            }
        
    }
    
    $ad->bench_end('Link Wikipedia Terms');
}

if ($savedTable && !$colReq) {
    unless ($specialCol{flag_go}) {
        if ($mode eq 'desc') {
            $colReq = ['desc_in'];
        } elsif ($freq) {
            $colReq = ['term_out','ns_out'];
        } else {
            $colReq = $#{$targNs} == 0 ? 
                ['term_out','matched','auth'] : 
                ['term_out', 'ns_out', 'matched','auth'];
        }
        foreach my $cn (keys %specialCol) {
            push @{$colReq}, $cn if ($specialCol{$cn});
        }
    }
}

if ($args->val('rawhead') && $hdata) {
    map { $hdata->{$_} = { label => $_ } } keys %{$hdata};
}

if ($head && $hdata) {
    # THIS APPEARS TO CLOBBER SOME $head ASSIGNMENTS PREVIOUSLY.
    $head = [];
    for my $c (0..$#corder) {
        my $raw = $corder[$c];
        my $h = $hdata->{$raw} || {
            label => "Column " . ($c+1),
            desc => "Anonymous column of uncertain provenance",
        };
        $h->{name} = $raw;
        push @{$head}, $h;
    }
}

if (!$fmtFlags{HTML} && $fmtFlags{Cluster}) {
    my $idx = $args->val(qw(clusterin)) ? $idxIn : $idxOut;
    if (defined $idx) {
        my $odx = ($idx == $idxIn) ? $idxOut : $idxIn;
        my (%ord, %clust, $oc);
        my $noNullCluster = $args->val(qw(nonullcluster nonullclust)) ? 1 : 0;
        my $csz = &get_or_add_column
            (\%cpos, 'clust_size', \@corder, $head, 'nonovel');
        foreach my $row (@{$rows}) {
            my $key = $row->[$idx];
            if (!defined $key || $key eq '') {
                if ($noNullCluster) {
                    $key = "NULL-".$noNullCluster++;
                } else {
                    $key = '';
                }
            }
            $ord{$key} ||= ++$oc;
            my $cl  = $clust{$key} ||= [ map { {} } @{$row} ];
            for my $i (0..$#{$row}) {
                $cl->[$i]{ defined $row->[$i] ? $row->[$i] : '' } = 1;
            }
        }
        $rows = [];
        foreach my $key (sort { $ord{$a} <=> $ord{$b} } keys %clust) {
            my @row;
            for my $i (0..$#{$clust{$key}}) {
                my @vals = keys %{$clust{$key}[$i]};
                @vals = &fancySort( \@vals, undef, undef, 'beQuiet' )
                    if ($#vals > 1);
                $row[$i] = join($clustJoin, @vals);
            }
            my %oCol =  %{$clust{$key}[$odx]};
            delete $oCol{""};
            my @u = keys %oCol;
            $row[$csz] = $#u + 1 if (defined $csz);
            push @{$rows}, \@row;
        }
    } else {
        $args->msg("[!!]","Can not cluster results, primary clustering column is absent");
    }
}

if ($cc && $#{$rows} > -1 && !$savedTable) {
    $ad->bench_start('Remove Null Columns');
    # Get rid of columns that lack data
    my $max = $#{$rows->[0]};
    my @nulls;
    for (my $c = $max; $c >= 0; $c--) {
        my $populated = 0;
        my %distinct;
        for my $r (0..$#{$rows}) {
            my $val = defined $rows->[$r][$c] ? $rows->[$r][$c] : '';
            if ($commonColumns) {
                $distinct{$val} = 1;
            } elsif ($val ne '') {
                # This column has data
                $populated = 1; last;
            }
        }
        if ($commonColumns) {
            my @uniq = keys %distinct;
            if ($#uniq == 0) {
                # All entries for this column are the same
                my $cn = $corder[$c] || "Column ".($c+1);
                push @{$commonColumns}, [$hdata->{$cn}{label} || $cn, $uniq[0]];
            } else {
                $populated = 1;
            }
        }
        push @nulls, $c unless ($populated);
    }
    # die $args->branch({ corder => \@corder, nulls => \@nulls});
    unless ($#nulls == -1) {
        # Adjust the column name and header arrays
        my $ind = $nulls[0];
        foreach my $arr (\@corder, $head) {
            next unless ($arr);
            $arr->[$ind] = "" if ($#{$arr} < $ind);
        }
    }
    # Some rows might be shared array references
    # We need to make distinct copies so splice() does not
    # act multiple times on the same array
    $rows = [ map { [ @{$_} ] } @{$rows} ];
    foreach my $c (@nulls) {
        map { splice(@{$_}, $c, 1)  } @{$rows};
        splice(@corder, $c, 1);
        splice(@{$head}, $c, 1) if ($head);
    }
    # If all rows are common remove them all (zero width now!):
    $rows = [] if ($#{$rows->[0]} == -1);
    %cpos = map { $corder[$_] => $_ + 1} (0..$#corder);
    $ad->bench_end('Remove Null Columns');
}

if ($savedTable) {
    $ad->bench_start('Merge Input with Output');
    # The user wants to keep their original input table, and append new data
    # Find the number of columns used by the input table
    my $colNm = $mode eq 'simple' || $mode eq 'desc' ? 'term_in' : $qryCol;
    my $cind = $cpos{$colNm};
    if ($cind) {
        $cind--;
    } else {
        $args->err("Failed to recover output column '$colNm' when preparing saved table");
    }
    my ($maxCol) = sort { $b <=> $a } map { $#{$_} } @{$savedTable};
    $maxCol--; # We've stuck a standardized ID on the end of the row
    if ($hasHeader && $#{$savedTable->[0]} > $maxCol) {
        # ... except for the header row
        $maxCol = $#{$savedTable->[0]};
    }
    # Update some column counting metrics:
    # shift @corder;
    my @original = map { ($userHeader ? $userHeader->[$_] : '') ||
                             'User_'.($_+1) } (0..$maxCol);
    if ($hasHeader) {
        for my $c (0..$#original) {
            if (my $h = $savedTable->[0][$c]) { $original[$c] = $h; }
        }
    }
    my @blankData = map { "" } @corder;
    my $baseIn    = $cpos{term_in};
    @corder = (@original, @corder);
    %cpos = map { $corder[$_] => $_ + 1} (0..$#corder);
    
    if ($head) {
        # We need to merge header rows
        # pop @{$head};
        my @uHead = map {{label => $original[$_], 
                          desc => sprintf("Column %d from input table", $_ + 1)}} (0..$#original);
        if ($hasHeader) {
            my $orig = shift @{$savedTable};
            for my $o (0..$#{$orig}) {
                if (my $hcell = $orig->[$o]) {
                    my $h = $hdata->{ $original[$o] } = {
                        label => $hcell,
                        desc => "Column imported from user query",
                    };
                    $uHead[$o] = $h;
                    # $original[$o] = $hcell;
                }
            }
        }
        $head = [ @uHead, @{$head} ];
    }

    # Organize output by query column
    my %lookup;
    my $igTok = $igCase ? "CASE-INSENSITIVE:" : "";
    foreach my $row (@{$rows}) {
        my $in = $row->[ $cind ] || "";
        push @{$lookup{$in}}, $row;
        # We use two keys when case is ignored so the actual case can
        # be matched when possible
        push @{$lookup{$igTok.uc($in)}}, $row if ($igTok);
    }
    $rows = [];
    my $sIndx = $isTable - 1;
    delete $fmtFlags{Cluster};
    foreach my $row (@{$savedTable}) {
        # Make sure that all rows from the saved table are same length
        my $stds = pop @{$row} || '';
        $stds    = [ $stds ] unless (ref($stds));
        $row->[$maxCol] = '' unless (defined $row->[$maxCol]);
        my @tRows;
        foreach my $std (@{$stds}) {
            my $targ = $lookup{ $std };
            $targ    = $lookup{$igTok.uc($std)} if (!$targ && $igTok);
            if ($targ) {
                push @tRows, @{$targ};
            }
        }
        if ($#tRows == -1) {
            # $args->err("No target row for '$std'");
            my @holder = @blankData;
            $holder[ $baseIn - 1]  = join(',', @{$stds}) if ($baseIn);
            push @tRows, \@holder ;
        }

        if ($doClust) {
            my @clust;
            foreach my $sRow (@tRows) {
                for my $s (0..$#{$sRow}) {
                    my $targ = $clust[$s] ||= {};
                    my $v    = $sRow->[$s];
                    $targ->{$v} = 1 if (defined $v && $v ne '');
                }
            }
            for my $s (0..$#clust) {
                my $v = join(',', sort keys %{$clust[$s]});
                $clust[$s] = defined $v ? $v : "";
            }
            @tRows = ( \@clust );
        }
        foreach my $sRow (@tRows) {
            push @{$rows}, [ @{$row}, @{$sRow} ];
        }
    }
    $ad->bench_end('Merge Input with Output');
}

$ad->bench_start('Map undef to space');
$rows = [ map { [ map { defined $_ ? $_ : ''} @{$_} ] } @{$rows} ];
$ad->bench_end('Map undef to space');

my $rank;
my $rankCG = 0;
if ($fmtFlags{Ranked}) {
    $ad->bench_start('Note rank order');
    $rank = {};
    for my $i (0..$#uniqQ) {
        my ($id) = $ad->standardize_id
            ($uniqQ[$i], $rNS ? $rNS->{$uniqQ[$i]} : $ns1 );
        push @{$rank->{$id}}, $i + 1;
    }
    while (my ($id, $list) = each %{$rank}) {
        $rank->{$id} = &rank_for_list( $list );
    }
    $ad->bench_start('Note rank order');
}

my @fonts = 
    (undef,gdTinyFont,gdSmallFont,gdMediumBoldFont,gdLargeFont,gdGiantFont);

my $lfmt   = "%s\n";
my $txtfmt = "";
my (@lines, $lfmtTwo);

if ($cpos{matched}) {
    if ($fmtFlags{AsPercent}) {
        $ad->bench_start('Fraction to Percent');
        my $in = $cpos{matched} - 1;
        # Show scores as percentages
        $head->[$in]{disp} = 'Percent' if ($head);
        if ($fmtFlags{Rounded}) {
            map { $_->[$in] = int(0.5 + 100 * $_->[$in])
                      if ($_->[$in]) } @{$rows};
        } else {
            map { $_->[$in] *= 100 if ($_->[$in]) } @{$rows};
        }
        $ad->bench_end('Fraction to Percent');
    }
}

if ($fmtFlags{NamespaceToken} || $fmtFlags{NamespaceName}) {
    my @inds;
    foreach my $cn (qw(ns ns_in ns_out)) {
        if (my $c = $cpos{$cn}) { push @inds, $c - 1; }
    }
    unless ($#inds == -1) {
        my $cb = $fmtFlags{NamespaceToken} ? 
            sub { return $ad->namespace_token(@_); } :
            sub { return $ad->namespace_name(@_); };
        foreach my $row (@{$rows}) {
            foreach my $i (@inds) {
                if (my $n = &{$cb}($row->[$i])) { $row->[$i] = $n; }
            }
        }
    }
}

if ($mode eq 'map' && $fmtFlags{ChrOnly} && $cpos{term_out}) {
    $ad->bench_start('Simplify Chromosome Names');
    my $in = $cpos{term_out} - 1;
    if ($head) {
        $head->[$in]{label} = 'Chr';
        $head->[$in]{desc} .= '. Shown as simplified chromosome name.';
    }
    map { $_->[$in] =~ s/^[^\.]+\.[^\.]+\.// if ($_->[$in]) } @{$rows};
    $ad->bench_end('Simplify Chromosome Names');
}

&prune_columns();

if ($fmtFlags{Oracle}) {
    # We will be breaking @lines up later, but for the moment standardize
    @lines = map { join("\t", @{$_}) } @{$rows};
} elsif ($fmtFlags{Rich}) {
    @lines = ("# Rich list format");
    if (my $ind = $cpos{$objCol}) {
        my $dcol = ($mode eq 'children' || $mode eq 'parents') ?
            'term_out' : $objCol;
        $dcol    =~ s/term/desc/;
        my $dind = $dcol eq $objCol ? undef : $cpos{$dcol};
        $ind--;
        my %seen;
        foreach my $row (@{$rows}) {
            if (my $id = $row->[$ind]) {
                next if ($seen{$id});
                my $desc = $dind ? $row->[$dind-1] : &_get_desc( $id );
                my $line = $id;
                $line .= " \# $desc" if ($desc);
                push @lines, $line;
            }
        }
    } else {
        my $msg = "Can not generate Rich List data without the object column being known";
        push @lines, "# $msg";
        $args->msg($msg);
    }
} elsif ($fmtFlags{Text}) {
    $ad->bench_start('Format Text');
    my @widths;

    my $btwn = $cpos{ns_between};
    my $lc = 0;
    if ($head) {
       map { push @{$widths[$_]}, length( $head->[$_]{label} ) } (0..$#{$head});
    }
    foreach my $row (@{$rows}) {
        if ($btwn && $lc) {
            my @toks = map { $ad->namespace_token($_) } 
            split(' < ', $row->[$btwn-1]);
            $row->[$btwn-1] = join(' < ', @toks);
        }
        map { $row->[$_] = '' unless (defined $row->[$_]);
              push @{$widths[$_]}, length( $row->[$_] ) } (0..$#{$row});
        $lc++;
    }
    $txtfmt = "|";
    foreach my $arr (@widths) {
        my ($sz) = sort {$b <=> $a} @{$arr};
        $txtfmt .= " %-${sz}s |";
    }
    @lines = map { sprintf($txtfmt, map { defined $_ ? $_ : "" } @{$_}) } @{$rows};
    $ad->bench_end('Format Text');
} elsif ($fmtFlags{MatrixMarket}) {
    # http://math.nist.gov/MatrixMarket/formats.html
    $ad->bench_start('Format MatrixMarket');
    my @inds = map { $cpos{$_} } qw(term_in term_out matched);
    my $ok = 0; map { $ok++ if (defined $_) } @inds;
    $args->death("Can not generate MatrixMarket format without all of:",
                 "term_in term_out matched") unless ($ok == 3);
    map { $_ -- } @inds;
    my ($rn, $cn, $tn, %rows, %cols, %matrix) = (0,0,0);
    # die $args->branch($rows);
    foreach my $row (@{$rows}) {
        my ($r, $c, $v) = map { $row->[$_] } @inds;
        if ($r && $c && defined $v) {
            my $ri = $rows{$r} ||= ++$rn;
            my $ci = $cols{$c} ||= ++$cn;
            # We will take the maximum score
            if (defined $matrix{$ri}{$ci}) {
                next if ($matrix{$ri}{$ci} >= $v);
            } else {
                # Tally total number of entries
                $tn++;
            }
            $matrix{$ri}{$ci} = $v;
        }
    }
    my $bar = "%-------------------------------------------\n";
    print $outFH "%%MatrixMarket matrix coordinate real general\n";
    print $outFH $bar;
    print $outFH "% Row Names\n";
    foreach my $r (sort { $rows{$a} <=> $rows{$b} } keys %rows) {
        printf($outFH "%% %d %s\n", $rows{$r}, $r);
    }
    print $outFH $bar;
    print $outFH "% Col Names\n";
    foreach my $c (sort { $cols{$a} <=> $cols{$b} } keys %cols) {
        printf($outFH "%% %d %s\n", $cols{$c}, $c);
    }
    print $outFH $bar;
    printf($outFH "  %d %d %d\n", $rn, $cn, $tn);
    foreach my $ri (sort { $a <=> $b } keys %matrix) {
        my $rmat = $matrix{$ri};
        foreach my $ci (sort { $a <=> $b } keys %{$rmat}) {
            push @lines, join(' ', $ri, $ci, $rmat->{$ci});
        }
    }
    $ad->bench_end('Format MatrixMarket');
} elsif ($fmtFlags{TSV} || $fmtFlags{CSV}) {
    $ad->bench_start('Format TSV');
    my $gsea = $fmtFlags{GSEA};
    if ($fmtFlags{SetFile}) {
        my @cols = ($objCol, 'sym_out', 'desc_out');
        if ($gsea) {
            print $outFH join("\t", 'SETPARAM', 'ID COUNT',  $#{$rows}+1)."\n";
            print $outFH "\n";
        }

        if ($savedTable) {
            # Keep user columns
            foreach my $cn (@corder) {
                push @cols, $cn unless ($stndColumns->{$cn});
            }
        }

        my @srcPos = map { $cpos{$_} } @cols;
        unless ($gsea) {
            for my $i (0..$#cols) {
                print $outFH "# COL".($i+1).": ".
                    ($hdata->{$cols[$i]}{label} || $cols[$i])."\n";
            }
        }
        my @remap;
        foreach my $row (@{$rows}) {
            my @rm = map { 
                !$srcPos[$_] ? "" : 
                    defined $row->[$srcPos[$_]-1] ? $row->[$srcPos[$_]-1] : ""
                } (0..$#srcPos);
            unshift @rm, 'DESC' if ($gsea);
            push @remap, \@rm;
        }
        $rows = \@remap;
    }
    if ($fmtFlags{CSV}) {
        foreach my $row (@{$rows}) {
            my @esc = @{$row};
            map { s/\\/\\\\/g; s/\"/\\\"/g; } @esc;
            push @lines, join(',', map { "\"$_\"" } @esc);
        }
    } else {
        @lines = map { join("\t", @{$_}) } @{$rows};
    }
    if ($gsea) {
        my $hr = ("#" x 40);
        push @lines, <<OF;

$hr
# YOUR ACTION NEEDED HERE
$hr

# You now need to connect your reference set to at least one other set
# If you want these other sets to have descriptions, you should use the DESC
# line, eg:

SET\tMy Colors
DESC\tBlue\tThe shade of the sky
DESC\tRed\tThink strawberries
DESC\tGreen\tFrogs, grass and sea-sick sailors

# To connect a set with your reference, use SET to indicate the reference,
# then TARGETSET to specify the analytic set, and finally CONNECT
# to make the links:

SET\t$gsea
TARGETSET\tMy Colors

CONNECT\t$rows->[0][1]\tBlue\tGreen

# CONNECT interprets the first entry as comming from 'SET', and ones
# that follow on the same line from 'TARGETSET'.
# If it is easier for you, you can connect in the other order:

TARGETSET\t$gsea
SET\tMy Colors

CONNECT\tBlue\t$rows->[0][1]
CONNECT\tGreen\t$rows->[0][1]

# Make sure to use tabs between entries on the same line
# Of course, make sure you delete (or comment out) the examples above, too

OF

    }
    $ad->bench_end('Format TSV');
} elsif ($fmtFlags{Excel}) {
    $ad->bench_start('Format Excel');
    my $path = $args->{OUTPUT};
    unless ($path) {
        my $file = "GenAccExcel_$parPid.xls";
        $path = "$tmpdir/$file";
    }
    my $defWidth = {
        matched    => 6,
        matched_in => 6,
        nicescore  => 8,
        howbad     => 6,
        strand     => 6,
        distance   => 10,
        start_in   => 10,
        start_out  => 10,
        end_in     => 10,
        end_out    => 10,
        sym_in     => 10,
        sym_out    => 10,
        taxa       => 15,
        term_in    => 20,
        term_out   => 20,
        desc_in    => 40,
        desc_out   => 40,
        term_share => 30,
        chemunit   => 8,
        assay      => 12,
        pubmed     => 15,
    };
    $defWidth->{term_share} = 6 if ($cleanChr);
    my $defForm = {
        matched    => 'cen',
        matched_in => 'cen',
        howbad     => 'cen',
        strand     => 'cen',
    };
    my @widths  = map { $defWidth->{$_} } @corder;
    my @baseFmt = map { $defForm->{$_} } @corder;
    my $eh = BMS::ExcelHelper->new( $path );
    $eh->sheet( -name    => 'GenAcc Results',
                -freeze  => $head ? 1 : 0,
                -width   => \@widths,
                -columns => $head ? [map { $_->{label} } @{$head}] : undef, );
    $eh->format( -name       => 'highlight',
                 -align      => 'center',
                 -color      => 'blue',
                 -background => 'yellow');
    if ($compareLists) {
        $ad->bench_start('Build list comparison worksheet');
        my $sn = 'Compare Lists';
        my @cl = sort { $a->{out} <=> $b->{out} } values %{$compareLists};
        my @sh = ("ID");
        my @w  = 15;
        push @sh, map { $_->{short} } @cl;
        push @w, map { 12 } @cl;
        push @sh, "Description";
        push @w, 50;
        $eh->sheet( -name    => $sn,
                    -cols    => \@sh,
                    -width   => \@w );
        my @vH = map { $_->{vals} } @cl;
        my %ids = map { uc($_) => $_ } map { $_->[1] } map { values %{$_} } @vH;
        my @uIDs = sort keys %ids;

        my (%byNS, %descs);
        map { push @{$byNS{$ad->guess_namespace($_)}}, $_ } @uIDs;
        while (my ($ns, $ids) = each %byNS) {
            $ad->bulk_description
                ( -ns => $ns, -ids => $ids, -ashash => \%descs, @comArgs );
        }
        
        foreach my $uid (@uIDs) {
            my $id = $ids{$uid};
            my @row = ($id);
            my @fmt = (undef);
            for my $j (0..$#vH) {
                my @hits = @{$vH[$j]{$uid}[2]};
                if ($#hits == -1) {
                    push @row, undef;
                    push @fmt, undef;
                } else {
                    push @row, join(',', @hits);
                    push @fmt, 'highlight';
                }
            }
            push @row, $descs{$uid};
            $eh->add_row($sn, \@row, \@fmt);
        }
        $ad->bench_end('Build list comparison worksheet');
    }
    my $usym  = $args->val('unusedsym');
    my $udesc = $args->val('unuseddesc');
    my $uns   = $args->val('unusedns');
    if ($unusedCustSet) {
        $ad->bench_start('Note unused custom set members');
        my $sn    = 'Unused Custom Set';

        my @ids = sort keys %{$unusedCustSet};
        my @hd = ('ID');
        my @wd = (12);
        my ($syms, $descs);
        if ($usym) {
            push @hd, 'Symbol'; push @wd, 12;
            $syms = $ad->bulk_best_symbol
                ( -ids => \@ids, -warn => $doWarn, -ns => $uns, -short => 1,
                  -explainsql => $exSql );
        }
        push @hd, "Set Membership";
        push @wd, 30;
        if ($udesc) { 
            push @hd, 'Description'; push @wd, 50;
            $descs = $ad->bulk_description
                ( -ns => $uns, -ids => \@ids, -ashash => 1, @comArgs );
        }
                        
        $eh->sheet( -name    => $sn,
                    -freeze  => 1,
                    -cols    => \@hd,
                    -width   => \@wd, );
        my @uRows;
        foreach my $id (@ids) {
            my @row = ($id);
            if ($syms) {
                my $sym = $syms->{$id} || [];
                push @row, $sym->[0] || "";
            }
            push @row, $unusedCustSet->{$id};
            push @row, $descs->{$id} if ($descs);
            push @uRows, \@row;
        }
        if ($syms) {
            @uRows = sort { uc($a->[1]) cmp uc($b->[1]) ||
                                $a->[0] cmp $b->[0] } @uRows;
        }
        foreach my $row (@uRows) {
            $eh->add_row($sn, $row);
        }
        $ad->bench_end('Note unused custom set members');
    }
    my $tossGoName = "Rejected IDs";
    
    $eh->sheet( -name    => 'Help',
                -width   => [ map { 15 } (0..10)], );
    $eh->format( -name       => 'cen',
                 -align      => 'center' );
    $eh->format( -name       => 'cenHead',
                 -bold       => 1,
                 -align      => 'center' );
    $eh->format( -name       => 'helpnote',
                 -bold       => 1,
                 -background => 'yellow',
                 -color      => 'blue' );

    my %hilights;
    foreach my $param ($args->each_param()) {
        if ($param =~ /^(hilight|hilite|color|format)_(.+?)(\.\.\d+)?$/i) {
            my ($match, $rank) = ($2,$3);
            if ($rank) {
                $rank =~ s/[^\d]+//g;
            }
            $rank ||= 99999;
            my $targ = $hilights{$match} ||= [ [], $match, $rank ];
            foreach my $val ($args->each_split_val('/\s*[\n\r\,]+\s*/',$param)) {
                if ($val =~ /^\s*(\-?(\S+))\s+=>\s+(.+)?\s*/) {
                    push @{$targ->[0]}, ('-'.($2), $3);
                }
            }
        }
    }
    my @texthl;
    foreach my $dat (sort { $a->[2] <=> $b->[2] } values %hilights) {
        my ($params, $match) = @{$dat};
        $eh->format( @{$params},
                     -name => $match);
        push @texthl, $match;
    }
    unless ($#texthl == -1) {
        $eh->add_row('Help', ["User defined formats for text matches:",'',''], ['helpnote','helpnote','helpnote']);
        map { $eh->add_row('Help', [$_],[$_] ) } @texthl;
        $eh->add_blank_row('Help');
    }

    my ($matchCol, @matchInds, %isFlagged);
    my @rankCols =qw(matched matched_in howbad clust_size);
    push @rankCols, 'distance' if ($maxDist);
    push @rankCols, 'specificity' if ($specialCol{specificity});
    foreach my $colname (@rankCols) {
        if (my $colNum = $cpos{$colname}) {
            push @matchInds, $colNum - 1;
        }
    }
    my %hiInds;
    for my $ind (0..$#corder) {
        if (my $cname = $corder[$ind]) {
            if ($cname =~ /^GO\:/) {
                $hiInds{$ind} = 'GO';
                push @matchInds, $ind;
                $isFlagged{$ind} = 1;
            } elsif ($cname =~ /^complist/) {
                $hiInds{$ind} = 'Compared List';
            }
        }
    }
    
    my %targs = map { $_ => 1 } @{$targNs};
    if ($targs{GO}) {
        # We have converted to GO terms
        # Tally them up
        my (%goOrder, @newGo, @cbs);
        if (my $tind = $cpos{term_out}) {
            $tind--;
            my $gnum = 0;
            map { $goOrder{ $_ } = ++$gnum } @usedGos;
            push @cbs, sub {
                if (my $term = shift->[$tind]) {
                    if ($term =~ /^GO:/ && !$goOrder{$term}) {
                        push @newGo, $term;
                        $goOrder{$term} = ++$gnum;
                    }
                }
            };
        }
        if (my $aind = $cpos{auth}) {
            $aind--;
            push @matchInds, $aind;
            $hiInds{$aind} = 'EC';
            push @cbs, sub {
                if (my $auth = shift->[$aind]) {
                    if ($auth =~ /^([A-Z]{2,3}) \[/i) {
                        my $ec = uc($1);
                        $usedEC{$ec}++;
                    }
                }
            };
        }
        unless ($#cbs == -1) {
            foreach my $row (@{$rows}) {
                map { &{$_}( $row ) } @cbs;
            }
        }
        push @usedGos, sort @newGo;
    }
    foreach my $cbCol (keys %ehColCB) {
        if (my $i = $cpos{$cbCol}) {
            push @matchInds, $i - 1;
        }
    }
    
    my @doHiLite = sort { $a <=> $b } keys %hiInds;
    my @uecs = sort { $usedEC{$b} <=> $usedEC{$a} || $a cmp $b } keys %usedEC;
    my $hasECs = $#uecs == -1 ? 0 : 1;
    my $seScores = {
        platinum => 1,
        gold     => .8,
        silver   => .6,
        bronze   => .4,
        tin      => 0.001,
        sawdust  => -1,
    };
    my $custColors = 0;
    unless ($#matchInds == -1) {
        my @cols;
        my @cHead = map { $hdata->{$_}{label} || $_ }
        map { $corder[$_] } @matchInds;
        $eh->add_row( 'Help', ["Color Markup (more-or-less 'good' to 'bad'):",'',''], ['helpnote','helpnote','helpnote'] );
        push @cHead, "EC Code" if ($hasECs);
        $eh->add_row( 'Help', \@cHead, [ map { 'cenHead' } @cHead]);
        
        my @undefSc = ('-1 or ?','-> Undefined, Unknown, etc.');
        my @undefFm = ('colNull','helpnote','helpnote');
        my $halfScale = 0.5 / $colScale;
        for (my $i = $colScale; $i >= 0; $i--) {
            my $fwd = int(255 * $i / $colScale);
            my $bak = 255 - $fwd;
            my $cc = $eh->set_custom_color(40 + $custColors++, $bak, $fwd, 0);
            my $name = "col$i";
            $eh->format( -name       => $name,
                         -align      => 'center',
                         -bg_color   => $cc );
            $eh->format( -name       => "Lft$name",
                         -bg_color   => $cc );
            $cols[$i+1] = $name;
            my $val = $i / $colScale;
            my (@hr, @hrFmt);
            foreach my $ind (@matchInds) {
                my $cN  = $corder[$ind];
                my ($min, $max) = map { int(0.5 + 1000 * $_) / 1000 }
                ($val - $halfScale, $val + $halfScale);
                $min = 0 if ($min < 0);
                $max = 1 if ($max > 1);
                if ($cN eq 'distance') {
                    ($max, $min) = map { $maxDist * (1 - $_ ) } ($min, $max);
                } elsif ($cN eq 'auth') {
                    &_ec_code_help_markup
                        (\@hr, \@hrFmt, $name, $seScores, $val, $halfScale);
                    $undefSc[$#hr] = "Sawdust";
                    $undefFm[$#hr] = 'colNull';
                    next;
                } elsif ($cN eq 'specificity') {
                    unless ($min) {
                        push @hr , $specMax . '+';
                        next;
                    }
                    ($max, $min) = map { $specMax * (1 - $_ ) } ($min, $max);
                    $min = 1 if ($min == 0);
                } elsif ($cN eq 'howbad') {
                    ($max, $min) = map { 100 * (1 - $_ ) } ($min, $max);
                } elsif ($usingPerc) {
                    map { $_ *= 100 } ($min, $max);
                }
                push @hr, $min eq $max ? $min : join('-', $min, $max);
                push @hrFmt, $name;
            }

            if ($hasECs) {
                &_ec_code_help_markup
                    (\@hr, \@hrFmt, $name, $seScores, $val,$halfScale);
                $undefSc[$#hr] = "Sawdust";
                $undefFm[$#hr] = 'colNull';
            }
            
            $eh->add_row('Help', \@hr, \@hrFmt);
        }
        # Fold change colors
        for (my $c = 0 - $maxFC; $c <= $maxFC; $c++) {
            next unless ($c);
            my $hex = 255 - int(255 * abs($c) / $maxFC);
            my @rgb = $c < 0 ? ($hex, $hex, 255) : (255, $hex, $hex);
            my $cc  = $c ? $eh->set_custom_color(40 + $custColors++, @rgb) :
                "";
            my $name = "FoldChange$c";
            # $args->msg("[-]", "$name = ".join(',',@rgb));
            $eh->format(  -bg_color   => $cc,
                          -name       => $name );
        }

        $eh->format( -name     => 'colNull',
                     -align    => 'center',
                     -bg_color => 'silver' );
        $eh->format( -name     => 'LftcolNull',
                     -bg_color => 'silver' );
        $eh->format( -name     => 'colZero',
                     -align    => 'center',
                     -bg_color => 'pink' );

        $eh->format( -name     => 'Clus0',
                     -align    => 'center',
                     -color    => 'white',
                     -bg_color => 'black' );

        $eh->format( -name     => 'Clus1',
                     -align    => 'center',
                     -bg_color => 'lime' );

        $eh->format( -name     => 'Clus2',
                     -align    => 'center',
                     -bg_color => 'yellow' );

        $eh->format( -name     => 'Clus3',
                     -align    => 'center',
                     -bg_color => 'red' );

        $matchCol = \@cols;
        $eh->add_row( 'Help', \@undefSc, \@undefFm);
        unless ($#uecs == -1) {
            my $msg = "Scored columns that ALSO include an Evidence Code will be colored by the *WORST OF* the EC class OR the score";
            $eh->add_row( 'Help', ['!!NOTE!!',$msg], [ 'helpnote','helpnote']);
            $eh->add_row( 'Help', ['!!NOTE!!',"Scores are not being shown explicitly, but have still contributed to cell coloration"], [ 'helpnote','helpnote']) if ($hideScore);
        }
    }
    my $ecScores;
    if ($compareLists) {
         $eh->add_blank_row('Help');
        
         $eh->add_row('Help', ["Comparison files"], ['helpnote','helpnote','helpnote']);
         my @cl = sort { $a->{out} <=> $b->{out} } values %{$compareLists};
         my @head = ("Header","Name");
         my @cnts = sort keys %{$cl[0]{counts}};
         push @head, @cnts;
         push @head, "File Path";
         $eh->add_row('Help', \@head, [ map {'helpnote'} @head]);
         foreach my $dat (@cl) {
             my @row = ($hdata->{ $dat->{tag} }{label}, $dat->{short});
             push @row, map { $dat->{counts}{$_} } @cnts;
             push @row, $dat->{path};
             $eh->add_row('Help', \@row, [ 'helpnote']);
         }
         map { $eh->add_row('Help', [$_]) }
         ("In the 'GenAcc Results' worksheet, the external lists will each have a column labeled per the 'Header' section above",
          "If a worksheet row matches a row from the comparison list, then an entry will be made",
          "If the comparison file included a second column, the value from that column will be entered",
          "Otherwise the row number(s) from the comparison file will be shown, so (92,411) means that the value was observed in rows 92 and 411 of the original file.",
          "In the 'Comparison Lists' worksheet, each comparison list will have its own column",
          "If a match was found to the worksheet, then the worksheet row numbers will be shown there (so '7,104' means that the match can be found in rows 7 and 104 on the 'GenAcc Results' worksheet)");
         
    }
    unless ($#uecs == -1) {
        $eh->add_blank_row('Help');
        
        $eh->add_row('Help', ["Gene Ontology Evidence Codes:",'','http://www.geneontology.org/GO.evidence.shtml'], ['helpnote','helpnote','helpnote']);
        $eh->add_row('Help', ["Code","Count", "Description","Interpretation"], ['helpnote','helpnote','helpnote','helpnote']);
        $ecScores = {};
        foreach my $ec (@uecs) {
            my $desc = $ad->description( -id => $ec, -ns => 'EC' );
            my $num  = $usedEC{$ec};
            my $simp = $simpleEnglishEC->{uc($ec)} || "";
            my $ecFmt = 'cenhead';
            if ($simp =~ /^(\S+) class/) {
                my $metal = lc($1);
                my $ecS = $seScores->{$metal};
                $ecS = -1 unless (defined $ecS);
                $ecScores->{$ec} = $ecS;
                $ecFmt = 'col'.($ecS == -1 ? 'Null' :
                                int(0.5 + $ecS * $colScale));
            }
            $eh->add_row('Help', [$ec, $num, $desc, $simp], 
                         [$ecFmt,'cen',"", ""] );
        }
    }
    unless ($#usedGos == -1) {
        $eh->add_blank_row('Help');
        
        $eh->add_row('Help', ["Gene Ontology Flags Used:","","",""], ['helpnote','helpnote','helpnote','helpnote']);
         $eh->add_row('Help', ["GO ID","Simple English","Official Name","Detailed Comment"], ['helpnote','helpnote','helpnote','helpnote']);
        foreach my $go (@usedGos) {
            my $desc = &_get_desc( $go, 'GO');
            my $simp = $simpleEnglishGO->{$go} || "";
            my ($com, $comid) = ("", 0);
            my $comHash = $ad->fast_edge_hash
                ( -name      => "#None#$go",
                  -keeptype  => "has comment", );
            while (my ($comTxt, $eidHash) = each %{$comHash}) {
                my ($eid) = sort { $b <=> $a } keys %{$eidHash};
                if ($eid > $comid) {
                    # This is a more recent comment
                    $com = $comTxt;
                    $comid = $eid;
                }
            }
            my $link = ["url", "http://amigo.geneontology.org/amigo/term/$go", $go];
            $eh->add_row_explicit('Help', [$link, $simp, $desc, $com], 
                                  ['cenhead',''] );
        }
    }

    if ($tossedGos || $tossedSets) {
        my @hd = ('ID');
        my @wd = (12);
 
        my @srcs = ($tossedGos, $tossedSets);
        my (@data, %uid, $syms, $descs, @tmi, %tf);
        my $totCols = 0;
        for my $s (0..$#srcs) {
            if (my $hash = $srcs[$s]) {
                # warn $args->branch( -maxany => 10, -ref => $hash);
                my @i = keys %{$hash};
                map { $uid{$_} = 1 } @i;
                my %u = map { $_ => 1 } map { keys %{$hash->{$_}} } @i;
                my @c = sort keys %u;
                my @h = $s ? @c : map {&_get_desc( $_, 'GO')} @c;
                push @data, {
                    col  => \@c,
                    head => \@h,
                    src  => $hash,
                };
                $totCols += $#h + 1;
            }
        }
        my @ids = sort keys %uid;
        my $cwid = 120 / ($totCols || 1);
        if ($cwid < 4) { $cwid = 4 } elsif ($cwid > 12) { $cwid = 12 }
        if ($usym) {
            push @hd, 'Symbol'; 
            push @wd, 12;
            $syms = $ad->bulk_best_symbol
                ( -ids => \@ids, -warn => $doWarn, -ns => $uns, -short => 1,
                  -explainsql => $exSql );
        }
        if ($customSets) {
            push @hd, "CustomSets";
            push @wd, 18;
        }
        foreach my $src (@data) {
            push @hd, @{$src->{head}};
            my $tgp = $src->{tgp} = {};
            foreach my $term (@{$src->{col}}) {
                push @wd, $cwid;
                push @tmi, $tgp->{$term} = $#wd;
                $tf{$#wd} = 1;
            }
        }
        if ($udesc) { 
            push @hd, 'Description';
            push @wd, 50;
            $descs = $ad->bulk_description
                ( -ns => $uns, -ids => \@ids, -ashash => 1, @comArgs );
        }
        @ids = sort @ids;
         
        $eh->sheet( -name    => $tossGoName,
                    -freeze  => 1,
                    -cols    => \@hd,
                    -width   => \@wd, );
        my @gr;
        foreach my $id (@ids) {
            my @row = ($id);
            if ($syms) {
                my $sym = $syms->{$id} || [];
                push @row, $sym->[0] || "";
            }
            push @row, $customSets->{uc($id)} if ($customSets);
            $row[$#hd] = $descs->{$id} if ($descs);
            foreach my $dat (@data) {
                &_go_hash_to_row( $dat->{src}, \@row, $id,
                                  $dat->{col}, $dat->{tgp});
            }
            push @gr, \@row;
        }
        if ($syms) {
            @gr = sort { uc($a->[1]) cmp uc($b->[1]) ||
                             $a->[0] cmp $b->[0] } @gr;
        }
        foreach my $row (@gr) {
            my $fmt = [];
            &_excel_score_format
                ( $row, $fmt, \@tmi, $ecScores, \%tf, {}, \@hd );
            $eh->add_row($tossGoName, $row, $fmt);
        }
    }

    $eh->add_blank_row('Help');
    $eh->add_row('Help', ["Program Execution Parameters","","",""],
                 ['helpnote','helpnote','helpnote','helpnote']);
    $eh->add_row('Help', ["Parameter","Default?","Value",""], 
                 ['helpnote','helpnote','helpnote','helpnote']);
    $eh->format( -name       => 'left',
                 -align      => 'left', );
    my $nd = '-NOT DEFINED-';
    foreach my $param (sort {lc($a) cmp lc($b) } $args->each_param()) {
        my $isDef = $args->is_default($param);
        my $val = $args->val($param);
        if (!defined $val) {
            $val = $nd;
        } elsif (my $r = ref($val)) {
            if ($r eq 'ARRAY') {
                $val = "ARRAY: ".join(',', map { !defined $_ ? $nd : ref($_) ? ref($_) : $_ } @{$val});
            } else {
                $val = "$r OBJECT";
            }
        }
        $eh->add_row('Help', [$param, $isDef ? 'Yes' : '', $val],
                     ['','cen','left']);
    }
    
    my $colorCells;
    if(my $mareq = $args->val(qw(color))) {
        my %data;
        foreach my $req (split(/\,/, $mareq)) {
            if ($req =~ /^([a-z12]+)[\: ](.+)[\: ]([a-z]+)$/) {
                my ($cn, $text, $color) = ($1, $2, $3);
                if (my $std = &_standardize_column($cn)) {
                    push @{$data{$std}}, $text;
                    $eh->format( -name     => $text,
                                 -bg_color => $color );
                } else {
                    $args->err("Could not understand color request for column '$cn'");
                }
            }
        }
        foreach my $cn (keys %data) {
            delete $data{$cn} unless ($cpos{$cn});
        }
        my @u = keys %data;
        if ($#u == -1) {
        } else {
            $colorCells = \%data;
        }
    }

    # These link primary columns to derivative columns
    my $sameButDifferent = {
        matched => [qw(chemunit nicescore)]
    };
    my %sbd;
    while (my ($same, $dArr) = each %{$sameButDifferent}) {
        if (my $sp = $cpos{$same}) {
            my @inds;
            foreach my $col (@{$dArr}) {
                if (my $dp = $cpos{$col}) {
                    push @inds, $dp - 1;
                }
            }
            $sbd{($sp-1)} = \@inds unless ($#inds == -1);
        }
    }
    
    for my $r (0..$#{$rows}) {
        my $row = $rows->[$r];
        my $fmt = [ @baseFmt ];
        foreach my $c (@doHiLite) {
            # Start GO columns off by being highlighted
            if ($row->[$c]) {
                $fmt->[$c] = "highlight";
            }
        }
        if ($fmtFlags{Grid} && $r > $gridPreRows ) {
            for my $c (0..$#{$row}) {
                my $fval;
                if ($c > $gridPreCols) {
                    my $val = $row->[$c];
                    $fval = "highlight" if (defined $val && $val ne "");
                }
                $fmt->[$c] = $fval;
            }
        } elsif ($#matchInds != -1) {
            &_excel_score_format( $row, $fmt, \@matchInds, $ecScores, 
                                  \%isFlagged, \%sbd );
        }
        if ($colorCells) {
            while (my ($cn, $arr) = each %{$colorCells}) {
                my $ind = $cpos{$cn} - 1;
                my $col;
                if (my $val = $row->[$ind]) {
                    for my $t (0..$#{$arr}) {
                        my $txt = $arr->[$t];
                        if ($val =~ /$txt/i) {
                            $col = $txt;
                            last;
                        }
                    }
                }
                $fmt->[$ind] ||= $col;
           }
        }
        if ($#texthl != -1) {
            for my $ind (0..$#{$row}) {
                next if ($fmt->[$ind]);
                my $v = $row->[$ind];
                next unless ($v);
                for my $t (0..$#texthl) {
                    next unless ($v =~ /$texthl[$t]/i);
                    $fmt->[$ind] = $texthl[$t];
                    last;
                }
            }
        }
        $eh->add_row( 'GenAcc Results', $row, $fmt);
    }

    

    $eh->close;
    if ($fmtFlags{JSON}) {
        print $outFH ' "excelFile": {'.
            $ser->obj_to_json($eh->file_path, $doPretty)."}";
        &end_JSON();
    } elsif ($nocgi) {
        $args->msg("Excel file generated", $eh->file_path)
            if ($vb || !$args->{OUTPUT});
    } else {
        my $url = $eh->file_path( );
        $url =~ s/\/stf/http:\/\/bioinformatics.bms.com/;
        if ($fmtFlags{DAD}) {
            print $outFH &esc_js($eh->html_summary);
            &end_JSON();
        } elsif ($url =~ /^http/) {
            $eh->url($url);
            if (1) {
                &set_mime( );
                &standard_head();
                print "<h1>Output</h1>\n";
                print "<p>Microsoft Excel is having problems auto-loading excel workbooks. Please click the link below to see your results if they fail to open automatically. If they fail to load in Excel, please right-click and save the file to your computer (e.g. to the desktop) and then open it from your computer.</p>\n";
                print $eh->html_summary;
                print "<iframe src='$url' style='display:none'></iframe>\n";
                # print "<script>window.setTimeout(function () { document.location = '$url'}, 200)</script>\n";
                &HTML_INTERFACE();
            } else {
                print $outFH "Location: $url\n\n";
                exit;
            }
        } else {
            &set_mime( );
            print $eh->html_summary;
        }
    }
    $ad->bench_end('Format Excel');
    &finish();
    &finish_up;
} elsif ($fmtFlags{Image}) {
    $ad->bench_start('Format Image');
    my $fInd = $args->{FONTSIZE} || 3;
    $fInd    = $#fonts if ($fInd > $#fonts);
    my $font = $fonts[$fInd];
    my $hT   = $font->height;
    my $wT   = $font->width;
    my $hpad = 4;
    my $cpad = int($fInd/1.5) || 1;
    my $iHd  = $head ? map { $_->{label}} @{$head} : [ map { '' } @{$rows->[0]} ];
    $iHd->[0] = '';
    my ($topMax)  = sort { $b <=> $a } map { length($_) } @{$iHd};
    my ($leftMax) = sort { $b <=> $a } map { length($rows->[$_][0]) } (0..$#{$rows});
    my $lmarg = $wT * $leftMax + $hpad;
    my $tmarg = $wT * $topMax + $hpad;
    my $step  = $cpad + $hT;
    my $cell  = $step - $cpad - 1;
    my $w     = $lmarg + $step * ($#{$iHd} + 1);
    my $h     = $tmarg + $step *  ($#{$rows} + 1);
    my $gph   = new GD::Image( $w, $h );
    my $white = $gph->colorAllocate(255,255,255 );
    my $black = $gph->colorAllocate(0,0,0 );
    my $blue  = $gph->colorAllocate(0,0,128 );
    if (1) {
        # Transparent image
        $gph->transparent( $white);
    } else {
        $gph->fill(0,0,$white);
    }
    $gph->interlaced('true');
    
    for my $i (0..$#{$iHd}) {
        my $txt = $iHd->[$i];
        next unless ($txt);
        my $x = $lmarg + ($i-1) * $step;
        $gph->stringUp( $font, $x, $tmarg - $hpad, $txt, $black );
    }
    for my $j (0..$#{$rows}) {
        my $row = $rows->[$j];
        my $txt = $row->[0];
        if ($txt) {
            my $len = $wT * length($txt);
            my $x   = $lmarg - $hpad - $len;
            $gph->string( $font, $x, $tmarg + $j * $step, $txt, $black );
        }
        for my $i (1..$#{$row}) {
            next unless ($row->[$i]);
            my ($x, $y) = ($lmarg + ($i-1) * $step, $tmarg + $j * $step);
            $gph->filledRectangle($x,$y,$x + $cell, $y + $cell,$blue)
        }
    }

    my $path = $args->{OUTPUT};
    unless ($path) {
        my $file = "Namespaces_$parPid.png";
        $path = "$tmpdir/$file";
    }
    open (FILE, ">$path") or $args->death("Failure to write image to file",
                                          $path, $!);
    print FILE $gph->png;
    close FILE;
    chmod(0777, $path);

    my $url = $path;
    $url =~ s/\/stf/http:\/\/bioinformatics.bms.com/;
    if ($fmtFlags{JSON}) {
        print $outFH ' "imgUrl": {'.$ser->obj_to_json($url, $doPretty)."}";
        &end_JSON();
    } elsif ($url =~ /^http/) {
        if ($fmtFlags{HTML}) {
            my $img = "<img src='$url' />\n";
            if ($fmtFlags{DAD}) {
                print $outFH &esc_js($img);
                &end_JSON();
            } else {
                print $outFH $img;
            }
        } else {
            $args->msg("Image generated", $path, $url) if ($vb);
        }
    }
    $ad->bench_end('Format Image');
    &finish();
    &finish_up;
} elsif ($isListFormat) {
    $ad->bench_start('Format List');
    if ($rank && $idxIn > -1) {
        my %hits;
        map { $hits{ $_->[$idxOut] }{ $_->[$idxIn] } = 1 } @{$rows};
        while (my ($out, $iHash) = each %hits) {
            my @ranks = map { $rank->{$_} } keys %{$iHash};
            $hits{ $out } = &rank_for_list( \@ranks );
        }
        @lines = sort { $hits{$a} <=> $hits{$b} } keys %hits;
    } else {
        my $ind;
        if ($args->{LISTINPUT}) {
            # User wants to generate list from 'input' column
            if ($ind = $cpos{$qryCol}) {
                # ... and we found it
            } elsif ($vb) {
                $args->err("Input column is not present for list generation");
            }
        } elsif ($ind = $cpos{$objCol}) {
            # Normal column being used for output
        } elsif ($vb) {
            if ($cpos{$qryCol}) {
                $args->err
                    ("Output column is not available to generate list output.",
                     "If you desire to generate output from input column, specify -listinput");
            } else {
                $args->err("No input or output columns preserved to make list from");
            }
        }
        if ($ind) {
            $ad->bench_start('Find Unique IDs');
            $ind--;
            #my %ids = map { $_->[$ind] => 1 } @{$rows};
            #@lines = &fancySort([keys %ids]);
            #my (%got, @uniq);
            #for my $r (0..$#{$rows}) {
            #    my $val = $rows->[$r][$ind];
            #    push @uniq, $val unless ($got{$val}++);
            #}
            my %seen;
            my @uniq = grep ! ($seen{$_}++ || $_ eq ""), map { $_->[$ind] } @{$rows};
            $ad->bench_end('Find Unique IDs');
            @lines = &fancySort(\@uniq);
        } else {
            @lines = ();
        }
    }
    if ($fmtFlags{String} || $fmtFlags{RegExp}) {
        $head = undef ;
    } else {
        @lines = map { "* $_" } @lines if ($fmtFlags{TiddlyWiki});
        $head = [ $head->[0] ] if ($head);
    }
    $ad->bench_end('Format List');
} elsif ($fmtFlags{TiddlyWiki}) {
    $ad->bench_start('Format TiddlyWiki');
    $lfmt = "|%s|\n";
    foreach my $row (@{$rows}) {
        map { s/\b([A-Z][a-z\-]+[A-Z][a-z\-]+\S+)/~$1/g; } @{$row};
        push @lines, join("|", @{$row});
        #map { $_ = "~$_" if (/^[A-Z][a-z\-]+[A-Z][a-z\-]+/) } @bits;
    }
    # @lines = map { join("|", @{$_}) } @{$rows};    
    $ad->bench_end('Format TiddlyWiki');
} elsif ($fmtFlags{HTML}) {
    $ad->bench_start('Format HTML');
    $lfmt = "%s\n";
    if ($classCol) {
        # The user wants each table column to be individually classed
        $lfmtTwo = [ map { " class='$_'" } @corder];
    } else {
        $lfmtTwo = [ map { "" } @corder];
    }
    my $cr = $#corder;
    my $tf = "<td%s>%s</td>";
    if ($fmtFlags{Cluster} && defined $idxOut) {
        # The user wants to cluster rows by the output term
        my $prior = 'xxx';
        my (@stack, @clustered);
        foreach my $row (@{$rows}) {
            my $key = $row->[$idxOut];
            if ($key eq $prior) {
                push @stack, $row;
                next;
            }
            push @lines, &_manage_cluster_stack( \@stack );
            @stack = ($row);
            $prior = $key;
        }
        push @lines, &_manage_cluster_stack( \@stack );
    } else {
        foreach my $row (@{$rows}) {
            push @lines, ' <tr>'.join
                ('', map { sprintf($tf, $lfmtTwo->[$_], $row->[$_]) } (0..$cr))."</tr>";
        }
    }
    $ad->bench_end('Format HTML');
}

if ($fmtFlags{Distinct}) {
    $ad->bench_start('Remove Duplicates');
    my %unique; my $rank = 0;
    
    map { $unique{$_} ||= ++$rank } @lines;
    @lines = sort { $unique{$a} <=> $unique{$b} } keys %unique;
    $ad->bench_end('Remove Duplicates');
}

if ($fmtFlags{LiterateTSV}) {
    $ltsv = BMS::LiterateTSV->new();
    $ltsv->integrated(1) if ($fmtFlags{Integrated});
    $ltsv->meta_format( $args->val('literateformat') );
    if ($outFile) {
        close $outFH;
        $ltsv->data_file($outFile);
    }
    my $secName = "GenAcc Output";
    $ltsv->add_section_info
            ( -name => $secName,
              -text => "Data are generated from a BMS-internal program called GenAcc, a graph following engine that connects metadata from various sources. The primary network is stored in MapTracker, a very large, normalized graph database storing scientific information and relationships");
    
    my $modeText = "Charles has not yet added a description for '$mode' mode. Please pester him to do so.";
    
    if ($mode eq 'convert') {
        $modeText = "The data represent a conversion from an 'input' to an 'output'. GenAcc will find direct - and in many cases indirect - linkages between your input and the desired output. Input is specified as one or more IDs / names / symbols etc, while output is specified as one or more 'namespaces'. Namespaces include categories like 'LocusLink ID', 'GeneOntology Term', 'InChI Chemical Structure', etc.";
        my $rowNum = $#{$rows} + 1;
        my $qNum = $#queries + 1;
        $ltsv->add_section_info
            ( -name => $secName, -tagval => {
                UniqueQueries => $#uniqQ + 1,
                TotalQueries => $qNum,
                OutputRows => $rowNum,
                RowsPerQuery => $qNum ? int(0.5 + 100 * $rowNum / $qNum) / 100 : undef,
              });

    } elsif ($mode eq 'map') {
        $modeText = "These data represent sequence map coordinates. Alignments are queried for all input terms, with start and end coordinates reported for both the query and any aligned subjects. In most cases the output terms will be chromosomes or contigs.";
        $ltsv->add_section_info
            ( -name => $secName, -tagval => { CleanChromosomes => "Chromosome names have been simplified, such that 'homo_sapiens.chromosome.3' will be reported simply as '3'" }) if ($cleanChr);
        $ltsv->add_section_info
            ( -name => $secName, -tagval => { GenomeBuild => $genomeBuild }) if ($genomeBuild);
        $ltsv->add_section_info
            ( -name => $secName, -tagval => { HowBad => $howBad ? "Only mappings within $howBad% of the best alignment for each query are reported" : "Only the best scoring alignments for each query is reported. Note that this does not guarantee unique mappings, if two subjects have identical top scores." }) if (defined $howBad);
        $hdata->{matched}{desc} = "The alignment score, as a percentage. This is generally the 'total percent identity' between subject and query, calculated as 100 * 2 * NumberMatchingPoisitions / (LengthOfQuery + LengthOfSubject)";
    }
    $ltsv->add_section_info
        ( -name => $secName, -text => $modeText);
    my %params;
    foreach my $param ($args->each_param()) {
        my $ptyp = $args->is_default($param) ?
            "Default Parameters" : "Custom Parameters";
        my $val = $args->val($param);
        if (!defined $val) {
            $val = "";
        } elsif (my $r = ref($val)) {
            if ($r eq 'ARRAY') {
                $val = sprintf("ARRAY[%s]", join(", ", map { defined $_ ? $_ : "" } @{$val}));
            }
        }
        $params{$ptyp}{$param} = $val;
    }
    foreach my $ptyp (sort keys %params) {
        $ltsv->add_section_info
            ( -name => $ptyp, -level => 5, tagval => $params{$ptyp});

    }

    my %order = ( num => 1, name => 2, label => 3, desc => 4 );
    my $cInc = 99;
    my $used = {};
    for my $i (0..$#{$head}) {
        my $h = $head->[$i];
        my $stnd = $ltsv->add_column($h->{name});
        $ltsv->col_desc( $stnd, $h->{desc} );
        $ltsv->col_mode( $stnd, $h->{mode} );
        $ltsv->col_meta( $stnd, "label", $h->{label} );
        map { $order{$_} ||= ++$cInc } sort keys %{$h};
    }
    my $msg = "LiterateTSV Metadata block - designed to be parsed by 'literatetsv' R package";
    my $colMeta = "";
    $ltsv->meta_description
        ([num   => "This is the column number in the data file",
          name  => "The column / field name as represented in the data frame",
          label => "A potentially more human-friendly field name for the column",
          desc  => "A human-readable description of the column contents",]);
    $ltsv->add_section_info( -name => "Benchmarks", -level => 4, -code => "",
                             -text =>  $ad->showbench( -minfrac => $doBench))
        if ($doBench);

    if (my $comReq = $args->val('comment')) {
        my $sec = "Project Comments";
        my @coms = ref($comReq) ? @{$comReq} : ($comReq);
        foreach my $com (@coms) {
            my $cd;
            if ($com =~ /^(```(\S+)[\s\n+])(.+)/) {
                $cd = $2;
                $com = $3;
            }
            next unless ($com);
            $ltsv->add_section_info( -name => $sec, -rank => 'first',
                                     -code => $cd, -text =>  $com);
        }
    }
    
    $ltsv->data( $rows );
    if ($outFile) {
        $ltsv->write();
        my @files = ($outFile);
        if (my $mf = $ltsv->meta_file()) { push @files, $mf; }
        $args->msg("[>]", "LiterateTSV file generated", @files) if ($vb);
    }
    exit;
}

if ($fmtFlags{Perl}) {
    my $obj = { format => 'Perl', header => [ map { $_->{label} } @{$head}],
                columns => \@corder, rows => $rows, params => $args };
    $Data::Dumper::Indent = 1;
    print $outFH Data::Dumper->Dump([$obj], ['data']);
    &finish_up;
}



if ($fmtFlags{DataTable}) {
    $ad->bench_start('Format DataTable');
    my @cols = map { { id => $_, label => $hdata->{$_}{label} } } @corder;
    print $outFH "{ \"cols\": ".$ser->obj_to_json(\@cols, $doPretty).",\n";
    print $outFH "  \"rows\": [\n";
    for my $r (0..$#{$rows}) {
        my @row = map { { v => $_ } } @{$rows->[$r]};
        print $outFH " {\"c\": ".$ser->obj_to_json(\@row, $doPretty)."}".
            ($r == $#{$rows} ? "" : ",")."\n";
    }
    print $outFH " ]\n}\n";
    $ad->bench_end('Format DataTable');
    exit;
} elsif ($fmtFlags{ExtJSgrid}) {
    $ad->bench_start('Format ExtJSgrid');
    my $dataKey  = $args->val('extdatakey')   || 'results';
    my $countKey = $args->val('extcountkey')  || 'rows';
    my $nameKey  = $args->val('extheaderkey') || 'header';
    my %head     = map { $_ => $hdata->{$_}{label} || $_ } @corder;
    print $outFH "{ \"dataSource\": 'GenAcc Service',\n".
        "  \"success\": true, \n".
        "  \"".$ser->esc_js($nameKey)."\": ".$ser->obj_to_json
        (\%head, $doPretty).",\n";
    print $outFH "  \"".$ser->esc_js($countKey)."\": ".($#{$rows}+1).",\n";
    print $outFH "  \"".$ser->esc_js($dataKey)."\": [\n";
    for my $r (0..$#{$rows}) {
        my %hash;
        for my $c (0..$#corder) {
            my $val = $rows->[$r][$c];
            $hash{$corder[$c]} = $val if (defined $val && $val ne "");
        }
        print $outFH "  ".$ser->obj_to_json(\%hash, $doPretty).
            ($r == $#{$rows} ? "" : ",")."\n";
    }
    print $outFH "]}\n";
    $ad->bench_end('Format ExtJSgrid');
    exit;
} elsif ($fmtFlags{JSON}) {
    $ad->bench_start('Format JSON');
    print $outFH " \"table\": {\n   \"columns\": ".
        $ser->obj_to_json(\@corder, $doPretty). ",\n   \"header\": ".
        $ser->obj_to_json([ map { $hdata->{$_}{label} || $_ } @corder], $doPretty).
        ",\n   \"body\": [\n";
    for my $r (0..$#{$rows}) {
        print $outFH ",\n" if ($r);
        print $outFH "   ".$ser->obj_to_json($rows->[$r], $doPretty);
    }
    print $outFH "\n  ]}";
    $ad->bench_end('Format JSON');
}

if ($head) {
    my @bHead = map { $_->{label} } @{$head};
    map { $_ = "" unless (defined $_) } @bHead;
    $ad->bench_start('Add Header');
    if ($fmtFlags{HTML}) {
        unshift @lines, ' <tr>'.join
            ('', map { sprintf("<th%s>%s</th>", $lfmtTwo->[$_] || "",
                               $bHead[$_] || "Column ".($_+1)) }
             (0..$#corder))."</tr>";
    } elsif ($fmtFlags{TiddlyWiki}) {
        for my $i (0..$#bHead) {
            my $cn = $bHead[$i];
            $cn    =~ s/\s+//g;
            $cn    = "[[$cn]]" unless ($cn =~ /[A-Z][a-z]+[A-Z]/);
            $bHead[$i] = $cn;
        }
        unshift @lines, "!".join("|!", @bHead);
    } elsif ($fmtFlags{Text}) {
        my $bar = $txtfmt;
        $bar =~ s/\|/\+/g;
        while ($bar =~ /\%\-?(\d+)s/) {
            my $num = $1;
            my $rep = '-' x $num;
            $bar =~ s/\%\-?${num}s/$rep/g;
        }
        unshift @lines, ($bar, sprintf($txtfmt, @bHead), $bar);
        push @lines, $bar;
    } elsif ($fmtFlags{CSV}) {
        unshift @lines, join(",", map { '"'.$_.'"' } @bHead) unless ($fmtFlags{JSON});
    } else {
        unshift @lines, join("\t", @bHead) unless ($fmtFlags{JSON});
    }
    $ad->bench_end('Add Header');
}

$ad->bench_start('Final Formatting');
if ($fmtFlags{RegExp}) {
    map { s/\./\\./g; } @lines;
    @lines = ("egrep '(".join('|', @lines).")'");
    
} elsif ($fmtFlags{String}) {
    my $joiner = $args->val(qw(joiner)) || ',';
    if (my $qt = $args->val(qw(quote))) {
        @lines = map { $qt.$_.$qt } @lines;
    }
    my $line = join($joiner, @lines);
    $line =~ s/\Q$joiner\E{2,}/$joiner/g;
    $line =~ s/^\Q$joiner\E//;
    $line =~ s/\Q$joiner\E$//;
    @lines = ($line);
} elsif ($fmtFlags{HTML}) {
    my $tstart = "<table class='$gaClass'><tbody>\n";
    print $outFH $fmtFlags{DAD} ? &esc_js($tstart) : $tstart;
}

if (my $preText = $args->val(qw(pretext))) {
    my @pre = ref($preText) ? @{$preText} : ($preText);
    map { print $outFH "# $_\n" if (defined $_) } @pre;
}

if ($fmtFlags{DAD}) {
    print $outFH &esc_js(join('', map {sprintf($lfmt, $_)} @lines));
} elsif ($fmtFlags{Oracle}) {
    if (my $tname = &write_to_oracle(\@lines)) {
        $args->msg("Data written to oracle table", $tname) if ($vb);
    }
} else {
    if ($fmtFlags{Text} && $#{$rows} == -1) {
        print $outFH "/no results returned/\n" unless ($primaryCount);
    } else {
        print $outFH join('', map {sprintf($lfmt, $_)} @lines);
    }
}

if ($fmtFlags{HTML}) {
    my $tend = "</tbody></table>\n";
    print $outFH $fmtFlags{DAD} ? &esc_js($tend) : $tend;
}

if (($fmtFlags{HTML} || $fmtFlags{TiddlyWiki}) && $args->{RECALCULATE}) {
    my $html = "";
    $html .= "<html>\n" if ($fmtFlags{TiddlyWiki});
    $html .= "<form action='http://bioinformatics.bms.com".$ENV{SCRIPT_NAME}.
        "' method='post'>\n";
    my %loc = %{$args};
    map { delete $loc{uc($_)} } qw(nocgi query queries id terms);
    $loc{QUERIES}  = join("\n", @uniqQ) || '';
    $loc{SHOWOPTS} = 1;
    foreach my $key (sort keys %loc) {
        my $val = $loc{$key};
        $val =~ s/\'/\\\'/g;
        $html .= sprintf("  <input type='hidden' name='%s' value='%s' />\n", 
                         $key, $val); 
    }
    $html .= "  <input type='submit' value='Recalculate' />\n";
    $html .= "</form>\n";
    $html .= "</html>\n" if ($fmtFlags{TiddlyWiki});
    print $outFH $fmtFlags{DAD} ? &esc_js($html) : $html;
}

if ($commonColumns && $#{$commonColumns} != -1) {
    if ($fmtFlags{Text}) {
        foreach my $dat (@{$commonColumns}) {
            my ($col, $val) = @{$dat};
            $val = '-Null-' unless (defined $val && $val ne '');
            printf($outFH " %24s: %s\n", $col, $val);
        }
    }
}

&end_JSON() if ($jsonType);
$ad->bench_end('Final Formatting');

&_close_out();
&finish();

sub _excel_score_format {
    my ($row, $fmt, $matchInds, $ecScores, $isFlagged, $sbd, $colOrd) = @_;
    $colOrd ||= \@corder;
    foreach my $matchInd (@{$matchInds}) {
        my $val = $row->[$matchInd] || "";
        my @vbits;
        foreach my $v (split(/\Q$clustJoin\E/, $val)) {
            push @vbits, $v if (defined $v && 
                                $v =~ /^[\-\+]?(\d+|\d*\.\d+)$/);
        }
        @vbits = sort { $a <=> $b } @vbits;
        my $vMax = $vbits[-1];
        my $vMin = $vbits[0];
        my $cN  = $colOrd->[$matchInd];
        my $colVal;
        my $col;
        my $lft = "";
        # $args->msg_once("$matchInd = $cN");
        if (my $cb = $ehColCB{$cN}) {
            # A custom formatting callback has been defined for this column
            $fmt->[$matchInd] = &{$cb}( $vMax );
        } elsif ($cN eq 'auth') {
            if ($val =~ /^([A-Z]{2,3}) \[/i) {
                # Evidence code
                my $ec = uc($1);
                if (my $ecs = $ecScores->{$ec}) {
                    if (defined $ecs) {
                        $colVal = $ecs == - 1 ? 'Null' :
                            int(0.5 + $ecs * $colScale);
                        $lft = "Lft";
                    }
                }
            }
        } elsif ($cN eq 'distance') {
            if (defined $vMin) {
                $colVal = int(0.5 + 10 - (10 * abs($vMin) / $maxDist));
            } else {
                $colVal = 'Null';
            }
        } elsif ($cN eq 'specificity') {
            if (!defined $vMin) {
                $colVal = 'Null';
            } elsif ($vMin eq '0') {
                $colVal = 'Zero';
            } elsif ($vMin ne '') {
                $colVal = 1 - $vMin / $specMax;
                $colVal = int(0.5 + $colVal * $colScale);
            }
        } elsif ($cN eq 'clust_size') {
            my $num = $vMin || 0;
            $num    = 3 if ($num > 3);
            $fmt->[$matchInd] = "Clus$num";
            next;
        } elsif (!defined $val || $val eq '' ||  $val eq '-1') {
            $colVal = $isFlagged->{$matchInd} ? undef : 'Null';
        } elsif ((defined $vMax && $vMax =~ /^(\-?(\d+|\d*\.\d+))$/) ||
                 $val  =~ /^(\-?(\d+|\d*\.\d+)) (.+)$/) {
            my ($num, $txt) = ($1, $3);
            if ($txt) {
                # Not a pure number
                if ($clearnum) {
                    $row->[$matchInd] = $txt;
                } else {
                    # Check and see if there is an evidence code
                    $txt =~ s/\s.+//;
                    if (my $ecs = $ecScores->{$txt}) {
                        $num = $ecs if ($num > $ecs);
                    }
                }
            }
            if ($hideScore) {
                # We can remove the score now
                $row->[$matchInd] = $txt;
            }
            $colVal = $num + 0;
            if ($cN eq 'howbad') {
                $colVal = (100 - $colVal) / 100;
            } elsif ($colVal > 1 && $usingPerc) {
                # Normalize values to 0-1
                $colVal /= 100;
            }
            $colVal = $colVal == -1 ? 'Null' :
                int(0.5 + $colVal * $colScale);
        } elsif ($cN =~ /^GO:/) {
            if ($val =~ /^([A-Z]{2,3}) \[/i) {
                # Evidence code
                my $ec = uc($1);
                if (my $ecs = $ecScores->{$ec}) {
                    if (defined $ecs) {
                        $colVal = $ecs == - 1 ? 'Null' :
                            int(0.5 + $ecs * $colScale);
                        $lft = "Lft";
                    }
                }
            }
        }
        if (defined $colVal) {
            if ($colVal =~ /^\-?\d+$/) {
                if ($colVal < 0) {
                    $colVal = 0;
                } elsif ($colVal > $colScale) {
                    $colVal = $colScale;
                }
            }
            my $col = "${lft}col${colVal}";
            $fmt->[$matchInd] = $col;
            if (my $sbdArr = $sbd->{$matchInd}) {
                map { $fmt->[$_] ||= $col } @{$sbdArr};
            }
        }

    }
}

sub _ec_code_help_markup {
    # Tosses is "metal" scores for evidence codes
    my ($hr, $hrfmt, $name, $seScores, $val, $halfScale) = @_;
    my $here;
    while (my ($metal, $sc) = each %{$seScores}) {
        next if ($sc > $val + $halfScale);
        next if ($sc < $val - $halfScale);
        $here = $metal;
        substr($here, 0, 1) = uc(substr($here, 0, 1) );
    }
    push @{$hr}, $here;
    push @{$hrfmt}, $name if ($here);
}

sub _bed_line {
    my ($chr, $bld, $s, $e, $in, $sc, $str, 
        $ft, $desc, $col, $ns1, $trackHash) = @_;
    return undef unless ($chr && $bld);
    my $trDesc = "";
    if ($chr =~ /(\S+)\.\S+\.(\S+)/) {
        ($trDesc, $chr) = ($1, $2);
    } else {
        
    }
    my @line = ("chr$chr");
    push @line, ($s, $e, $in, $sc, $str);
    $line[4] = $line[4] ? $line[4] * 10 : 0;
    $line[5] = !$line[5] || $line[5] > 0 ? '+' : '-';
    $line[1]--; # All coords are zero-indexed
    my $start = $line[6] = $line[1];
    $line[7] = $line[2];
    $line[8] = $col;
    if ($ft) {
        my (@sz,@st);
        foreach my $bit (split(',', $ft)) {
            if ($bit =~ /^(\d+)\.\.(\d+)$/) {
                my ($s, $e) = ($1, $2);
                push @sz, $e - $s + 1;
                push @st, $s - $start - 1;
            } elsif ($bit =~ /^\d+$/) {
                # Single base
                push @sz, 1;
                push @st, $bit - $start - 1;
            } else {
                $args->msg_once("[?]","Ignoring odd feature footprint: $bit");
                return undef;
            }
        }
        if (my $num = $#sz + 1) {
            $line[9] = $num;
            $line[10] = join(',', @sz);
            $line[11] = join(',', @st);
        }
    }
    if (defined $desc) {
        $line[12] = $line[3];
        $line[13] = $desc;
    }
    my $rv = \@line;
    if ($trackHash) {
        my $trkName = $bld;
        my $n1    = $ad->namespace_token( $ns1 );
        $trkName .= " $n1" if ($n1);
        my $trk   = $trackHash->{$trkName};
        unless ($trk) {
            $trDesc =~ s/_/ /g;
            substr($trDesc, 0, 1) = uc(substr($trDesc, 0, 1));
            $trDesc .= " Build $bld";
            $trk = $trackHash->{$trkName} = {
                name => $trkName,
                desc => $trDesc . ($n1 ? " ".$ad->namespace_name($n1) : ""),
                rows => [],
            };
        }
        push @{$trk->{rows}}, $rv;
    }
    return $rv;
}

sub parse_custom_sets {
    my ($param, $sep, $asVal) = @_;
    my ($setHash);
    foreach my $req ($args->each_split_val('/\s*[\n\r]+\s*/', $param)) {
        next unless ($req);
        my %setParam = (namecol => 1, idcol => 2, header => 0, sheet => 1);
        if ($req =~ /^\s*\{([^\}]+)\}\s*(.+?)\s*$/) {
            my $params = $1;
            $req = $2;
            foreach my $kv (split(/\s*,\s*/, $params)) {
                if ($kv =~ /^\s*(\S+)=(.+?)\s*$/) {
                    $setParam{lc($1)} = $2;
                } else {
                    
                }
            }
        }
        my $tr = BMS::TableReader->new();
        my $format = $tr->format_from_file_name($req);
        $tr->has_header( $setParam{header} );
        $tr->format($format);
        $tr->input($req);
        $args->msg("[<]","Parsing set file $req") if ($doWarn);
        $tr->select_sheet( $setParam{sheet} );
        my ($ii, $ni, $vi) = map { ($setParam{$_} || 0) - 1 } qw(idcol namecol valcol);
        $setHash ||= {};
        while (my $row = $tr->next_clean_row()) {
            if (my $id = $row->[$ii]) {
                my $name = $setParam{name} ? $setParam{name} : 
                    $ni == -1 ? "" : $row->[$ni];
                my $val = defined $setParam{val} ? $setParam{val} :
                    $vi == -1 ? 1 : $row->[$vi];
                $setHash->{uc($id)}{$name || "Un-named Set"} = $val;
            }
        }
        $asVal ||= 1 unless ($vi == -1);
        if (my $sch = $setParam{scheme}) {
            if (my $name = $setParam{name}) {
                if ($sch =~ /(fold|fc)/i) {
                    # Fold change formatting
                    $ehColCB{$name} = sub {
                        my ($v) = @_;
                        return undef unless 
                            (defined $v && $v =~ /^[\-\+]?(\d+|\d*\.\d+)$/);
                        # XPRESS values appear to not be logs
                        return undef if ($v > -1 && $v < 1);
                        my $c = $v < 0 ? 0 - 1 / $v : $v;
                        $c = int(log($c) * $log2);
                        if ($c < 0 - $maxFC) {
                            $c = 0 - $maxFC;
                        } elsif ($c > $maxFC) {
                            $c = $maxFC;
                        }
                        return "FoldChange$c";
                    };
                } else {
                    $args->msg("[?]","Unknown scheme '$sch' for custom set '$name'"); 
                }
            } else {
                $args->msg("[?]","Can not set column scheme for custom set without 'name' defined"); 
            }
        }
    }
    if ($setHash && !$asVal) {
        my @ids = keys %{$setHash};
        $sep ||= ",";
        foreach my $id (@ids) {
            $setHash->{$id} = join($sep, sort keys %{$setHash->{$id}});
        }
    }
    return $setHash;
}

sub prune_columns {
    # Some columns may have been added to support extra functions
    # They may not be requested by the user, though
    # These are globals:
    # $rows, $head, @corder, %cpos
    return unless ($unrequestedColumns);
    my @newOrder;
    my $newHead = [];
    my @inds;
    %cpos = ();
    for my $i (0..$#corder) {
        my $col = $corder[$i];
        next if ($unrequestedColumns->{$col});
        push @newOrder, $col;
        push @{$newHead}, $head->[$i] if ($head);
        push @inds, $i;
        $cpos{$col} = $#newOrder  + 1;
    }
    @corder = @newOrder;
    $head = $newHead if ($head);
    my @newRows;
    while (my $row = shift @{$rows}) {
        my @newRow = map { $row->[$_] } @inds;
        push @newRows, \@newRow;
    }
    $rows = \@newRows;
}

sub get_or_add_column {
    my ($cpos, $col, $corder, $head, $noNovel) = @_;
    my $ind = $cpos->{$col};
    unless ($ind) {
        # Column does not exist, add it, unless requested not to
        return undef if ($noNovel && $colReq);
        push @{$corder}, $col;
        $cpos->{$col} = $ind = $#{$corder} + 1;
        $head->[$ind - 1] = $hdata->{$col} if ($head);
    }
    # Return the 0-index value
    return $ind - 1;
}

sub extract_pubmed {
    my ($rows, $head, $cpos, $corder) = @_;
    $ad->bench_start('PubMed Format');
    my $auInd  = $cpos{auth} - 1;
    my $pmInd  = &get_or_add_column($cpos, 'pubmed', $corder, $head);
    foreach my $row (@{$rows}) {
        my $au = $row->[$auInd] || "";
        my $pmids;
        while ($au =~ /(PMID:\d+)/) {
            my $pm = $1;
            $pmids ||= {};
            $pmids->{$pm}++;
            $au =~ s/\Q$pm\E/ /g;
        }
        if ($pmids) {
            $row->[$pmInd] = join(',', sort keys %{$pmids});
        }
    }
    $ad->bench_end('PubMed Format');
}

sub nice_chem {
    my ($rows, $head, $cpos, $corder) = @_;
    $ad->bench_start('Nice ChemBio Format');
    my $l10f = sub {
        my $sc = shift;
        return '' if (!defined $sc || $sc eq '' || $sc < 0);
        return 10 ** (-10*$sc);
        return "1E-".(10*$sc);
    };
    my $perf = sub {
        my $sc = shift;
        return undef if (!defined $sc || $sc eq '' || $sc < 0);
        return 100 * $sc;
        # return sprintf("%d%%", int(0.5 + 100 * $sc));
    };
    my $lodf = sub {
        my $sc = shift;
        return '' if (!defined $sc || $sc eq '' || $sc < 0);
        return $sc * 10;
    };

    my $invf = sub {
        my $sc = shift;
        return undef if (!defined $sc || $sc eq '' || $sc < 0);
        return 100 * (1-$sc);
    };
    my $def = sub { return shift; };
    # my $spO = &get_or_add_column(\%cpos, $go, \@corder);
    
    my $expForm    = '';# 0.00e-0'; # '0.00E+00''##0.0E+0'
    my $valFormats = {
        Ki             => [ $l10f, 5,  'log',     'KI',        $expForm ],
        EC50           => [ $l10f, 5,  'log',     'EC50',      $expForm ],
        IC50           => [ $l10f, 5,  'log',     'IC50',      $expForm ],
        'Generic'      => [ $def, -1,  '',        '',       ],
        '%KD'          => [ $perf, 50, 'perc',    'PERCKD',    '0.0%' ],
        '% Inhibition' => [ $perf, 50, 'perc',    'PERCINHIB', '0.0%' ],
        '% Set'        => [ $perf, 50, 'perc',    'PERCSET',   '0.0%' ],
        'LOD'          => [ $lodf, 5,  'log',     'ODDSSET',   '0.00' ],
        'pV'           => [ $l10f, 5,  'log',     'PVSET',     $expForm ],
        '%Ctrl 1uM'    => [ $invf, 5,  'invperc', 'PERCCTRL',  '0.0%' ],
    };
    my %molars = map { $_ => 1 } qw(Ki EC50 IC50);
    my %percs  = map { $_ => 1 } ('%KD','% Inhibition','%Ctrl 1uM');
    my @molVal = qw(M mM uM nM pM);
    my $regExp = '\[('.join('|', sort keys %{$valFormats}).')\]';
    my $scInd  = $cpos{matched} - 1;
    my $auInd  = $cpos{auth} - 1;
    my $tyInd  = &get_or_add_column($cpos, 'chemtype', $corder, $head);
    my $niInd  = &get_or_add_column($cpos, 'nicescore', $corder, $head);
    my $unInd;
    if ($cpos->{chemunit}) {
        $unInd = &get_or_add_column($cpos, 'chemunit', $corder, $head);
    }
    my %sfFmt  = ( 2 => '%.2f', 1 => '%.1f' );
    foreach my $row (@{$rows}) {
        my $au = $row->[$auInd];
        if ($au && $au =~ /$regExp/) {
            my $type = $row->[$tyInd] = $1;
            my $tdat = $valFormats->{$type};
            next unless ($tdat);
            # $args->msg_once($type);
            if (my $cb = $tdat->[0]) {
                my $sc   = $row->[$scInd];
                my $ni   = $row->[$niInd] = &sig_fig( &{$cb}( $sc ) );
                if (defined $unInd) {
                    # Add a column with units
                    my $unit;
                    my $v;
                    if ($sc && $molars{$type}) {
                        my $u = 0;
                        $v = $ni;
                        while ($v < 1 && $u < $#molVal) {
                            $v *= 1000;
                            $u++;
                        }
                        $unit = $molVal[$u];
                    } elsif ($percs{$type}) {
                        $v = $ni;
                        $unit = "%";
                    }
                    if (defined $v && defined $unit) {
                        $row->[$unInd] = &sig_fig($v) . " $unit";
                    }
                }
            }
        }
        
    }
    $ad->bench_end('Nice ChemBio Format');
}

sub sig_fig {
    my ($v, $sf) = @_;
    return "" if (!defined $v || $v eq '');
    return 0 unless ($v);
    $sf ||= 3;
    $sf  -= 1;
    my $sgn = 1;
    if ($v < 0) {
        $sgn = -1;
        $v = abs($v);
    }
    my $lv  = int(log($v) * $log10);
    $lv-- if ($lv < 0);
    my $fac = 10 ** ($sf - $lv);
    $v = $sgn * int(0.5 + $fac * $v) / $fac;
    return $v;
}

sub compare_list {
    my ($rows) = @_;
    $ad->bench_start('Parse comparison list files');
    my $cnt = 0;
    foreach my $src ('in','out') {
        my $param = "compare$src";
        foreach my $val ($args->each_split_val($param)) {
            next unless ($val);
            unless (-s $val) {
                $args->msg_once("[?]","-$param file not found", $val);
                next;
            }
            my $srcCol = "term_$src";
            my $ind    = $cpos{$srcCol};
            unless ($ind) {
                $args->msg_once("[?]","-$param file can not be used, $srcCol not in output", $val);
                next;
            }
            if (open(COMP,"<$val")) {
                $args->msg("[<]","Loading file for comparison to $srcCol", $val);
                my $rn = 0;
                my %found;
                while (<COMP>) {
                    $rn++;
                    s/[\n\r]+//;
                    my ($id, $val) = split(/\t/);
                    next unless ($id);
                    $id =~ s/^\s+//;
                    $id =~ s/\s+$//;
                    next unless ($id);
                    my $trg = $found{uc($id)} ||= [$val, $id, []];
                    if (!$trg->[0] || ref($trg->[0])) {
                        $trg->[0] ||= [];
                        push @{$trg->[0]}, $rn;
                    }
                }
                close COMP;
                my @u = keys %found;
                my $snum = $#u + 1;
                unless ($snum) {
                    $args->msg_once("[?]","-$param file is empty", $val);
                    next;
                }
                $compareLists ||= {};
                my $tag = "complist".++$cnt;
                $hdata->{$tag} = {
                    label => "Compare $cnt",
                };
                my $short = $val; $short =~ s/.+\///;
                my $out = &get_or_add_column(\%cpos, $tag, \@corder);
                $compareLists->{$out} = {
                    path => $val,
                    short => $short,
                    tag   => $tag,
                    vals  => \%found,
                    num   => $snum,
                    src   => $srcCol,
                    in    => $ind - 1,
                    nin   => $cpos{"ns_$src"},
                    out   => $out,
                    miss  => {},
                    ns    => {},
                };
            } else {
                $args->msg_once("[?]","Failed to read -$param file", $val, $!);
                next;
            }
        }
    }
    return unless ($compareLists);
    foreach my $dat (values %{$compareLists}) {
        my ($in, $out) = ($dat->{in}, $dat->{out});
        my $vals = $dat->{vals};
        my $miss = $dat->{miss};
        my $nin  = $dat->{nin};
        my %uSrc;
        for my $i (0..$#{$rows}) {
            if (my $term = $rows->[$i][$in]) {
                my $uct  = uc($term);
                $uSrc{$uct} ||= $term;
                if (my $x = $vals->{$uct}) {
                    # Add the row number(s) of the source file to output
                    $rows->[$i][$out] = join(',', @{$x->[0]});
                    # Note the *excel row number* where it was seen:
                    push @{$x->[2]}, $i + 2;
                } else {
                    my $m = $miss->{$uct} ||= [ $i + 1, $term ];
                }
            }
        }
        # Calculate relative counts
        my @us     = keys %uSrc;
        my $sNum   = $#us + 1;
        my @um     = keys %{$miss};
        my $sUniq  = $#um + 1;
        my $shared = 0;
        my $fnum   = $dat->{num};
        map { $shared++ unless ($#{$_->[2]} == -1) } values %{$vals};
        $dat->{counts} = {
            File            => $fnum,
            FileUnique      => $fnum - $shared,
            Shared          => $shared,
            Worksheet       => $sNum,
            WorksheetUnique => $sUniq,
        };
    }
    $ad->bench_end('Parse comparison list files');
}

sub flag_go {
    $ad->bench_start('Add GO Membership');
    my ($rows, $fg) = @_;
    my $tO     = $cpos{$objCol} - 1;
    my $n1     = $cpos{ns_in}  ? $cpos{ns_in} -1 : undef;
    my $nOn    = $objCol; $nOn =~ s/term/ns/;
    my $n2     = $cpos{$nOn} ? $cpos{$nOn} -1 : undef;
    my $useAcc = $args->val(qw(usegoacc));
    my $flagged = ($flagReq || $mustToss) ? [] : undef;
    @usedGos = ();
    $fg = $defFlagGo if (!$fg || $fg eq '1');
    $fg =~ s/^\s+//; $fg =~ s/\s+$//;
    my (@goErrs, %goOrder, %reMap);
    my $gnum = 0;
    foreach my $goReq ( $ad->list_from_request([split(/[\,\s]+/,$fg)]) ) {
        next unless ($goReq);
        my ($go, $seq) = $ad->standardize_id($goReq);
        if ($seq) {
            unless ($goOrder{$go}) {
                push @usedGos, $go;
                $goOrder{$go} = ++$gnum;
            }
        } else {
            push @goErrs, $go;
        }
    }
    $args->err("Some of your GO membership requests appear to be malformed:",
               @goErrs) unless ($#goErrs == -1);
    return $rows if ($#usedGos == -1);

    $args->msg("Flagging ".scalar(@usedGos)." Gene Ontology columns")
        if ($doWarn);
    my @setCol = @usedGos;
    my $conCol = "go_class";
    if ($concatGo) {
        $concatGo = ';' if ($concatGo eq '1');
        my $conRep = " ";
        $conRep = "_" if ($concatGo eq $conRep);
        foreach my $go (@usedGos) {
            my $rm = $useAcc ? $go : &_get_desc( $go, 'GO');
            $rm =~ s/\Q$concatGo\E/$conRep/g;
            $reMap{$go} = $rm;
        }
        @setCol = ($conCol);
    }
    foreach my $go (@setCol) {
        my $colName = $go;
        # Use the GO description as a column name
        if ($colName eq $conCol) {
            $colName = "GeneOntology Classes";
            if ($keepEC) {
                $colName .= " ".join(',', sort keys %{$keepEC});
            }
        } elsif ($fmtFlags{Text} || $useAcc) {
            # Leave as-is
        } else {
            $colName = &_get_desc( $go, 'GO');
        }
        $hdata->{$go} = { label => $colName };

        my $spO = &get_or_add_column(\%cpos, $go, \@corder);
        $goPos{$go} = $spO;
        if ($colReq) {
            my %got = map { $_ => 1 } @{$colReq};
            push @{$colReq}, $go unless ($got{$go});
            # $colReq->[ $spO ] = $go;
        }
    }

    my %byNS;
    foreach my $row (@{$rows}) {
        my $t = $row->[$tO];
        next unless ($t);
        my $tns = $ad->effective_namespace
            ( defined $n2 ? $row->[$n2] : undef,
              defined $n1 ? $row->[$n1] : undef, $t );
        $byNS{$tns}{$t} = 1;
    }
    my (%goSc, %tossID);
    while (my ($tns, $termH) = each %byNS) {
        my @terms = keys %{$termH};

        my $minGo  = $args->val('mingo');
        my $goRows = $ad->convert
            ( -id => \@terms, -ns1 => $tns, -ns2 => 'go', -warn => $doWarn,
              -nonull => 1, -nullscore => -1, -min => $minGo,
              -cols => ['term_in', 'term_out', 'matched', 'auth' ]);
        foreach my $gr (@{$goRows}) {
            my ($t, $go, $sc, $auth) = @{$gr};
            if ($mustToss && $mustToss->{uc($go)}) {
                &_process_go_hash( \%tossID, $t, $go, $sc, $auth );
            } elsif ($goOrder{$go}) {
                &_process_go_hash( \%goSc, $t, $go, $sc, $auth );
            }
        }
    }
    my ($maxGoInd) = sort { $b <=> $a } values %goPos;
    foreach my $row (@{$rows}) {
        my $t = $row->[$tO] || "";
        my $gsc = $goSc{uc($t)};
        unless ($gsc) {
            $row->[$maxGoInd] ||= "";
            next;
        }
        my $tossIt = 0;
        if (my $tDat = $tossID{$t}) {
            # This was positively selected, AND set to be toss. We
            # will exclude it, but also make note so we can build a
            # summary sheet.
            $tossedGos ||= {};
            while (my ($go, $sa) = each %{$tDat}) {
                &_process_go_hash($tossedGos, $t, $go, @{$sa});
            }
            $tossIt++;
        }
        if ($rejectSets && $rejectSets->{uc($t)}) {
            $tossedSets ||= {};
            map { $tossedSets->{uc($t)}{$_} = 
                      [ 1, 'User' ] } @{$rejectSets->{uc($t)}};
            $tossIt++;
        }
        next if ($tossIt);
        
        if ($concatGo) {
            # We just need to concatenate the observed values
            my @vals = sort { $goOrder{$a} <=> $goOrder{$b} } keys %{$gsc};
            my $cat = join($concatGo, map { $reMap{$_} || $_ } @vals);
            $row->[$goPos{$conCol}] = $cat;
            if ($flagged) {
                if ($mustFlag) {
                    $cat = 0;
                    map { $cat += $mustFlag->{lc($_)} || 0 } @vals;
                }
                push @{$flagged}, $row if ($cat);
            }
        } else {
            my $hit = &_go_hash_to_row( \%goSc, $row, $t, \@usedGos, \%goPos);
            push @{$flagged}, $row if ($flagged && $hit);
        }
    }
    $ad->bench_end('Add GO Membership');
    return $flagged || $rows;
}

sub _process_go_hash {
    my ($goHash, $t, $go, $sc, $auth) = @_;
    $t = uc($t);
    my $gsc = $goHash->{$t}{$go};
    if ($gsc) {
        # The term is already noted. If the noted score is better, do nothing:
        return $gsc if ($gsc->[0] > $sc);
    }
    my $ec     = $auth || ""; 
    $ec        =~ s/\s+\<.+//; # Includes author
    my $ecOnly = $ec;
    $ecOnly    =~ s/\s+\[.+//;

    # If the term is already recorded, do not do anything if this EC
    # is not an improvement.
    return $gsc if ($gsc && $gsc->[0] == $sc && 
                    $ad->compare_evidence_codes( $gsc->[1], $ecOnly ) >= 0 );
            
    # If we only are keeping particular EC codes, skip if this ain't one:
    return undef if ($keepEC && ! $keepEC->{$ecOnly});
    return $goHash->{$t}{$go} = [ $sc, $noAu ? $ecOnly : $ec ];
}

sub _go_hash_to_row {
    my ($goHash, $row, $t, $goIDs, $goInds) = @_;
    my $gsc = $goHash->{uc($t)};
    return unless ($gsc);
    my $hit = 0;
    foreach my $go (@{$goIDs}) {
        my $val = "";
        if (my $dat = $gsc->{$go}) {
            my ($sc, $ec) = @{$dat};
            my @vb;
            unless ($noSc) {
                if ($fmtFlags{Rounded}) {
                    $sc = $sc < 0 ? $sc : int(0.5 + 100 * $sc);
                } elsif ($fmtFlags{AsPercent}) {
                    $sc *= 100;
                }
                push @vb, $sc;
            }
            if ($addEc && $ec) {
                push @vb, $ec;
                # Remove authority tags eg "TAS [EMBL]"
                my $cleanEC = uc($ec);
                $cleanEC =~ s/\s+.+//;
                $usedEC{$cleanEC}++;
            }
            $val = join(' ', @vb);
            $hit++ if (!$mustFlag || $mustFlag->{lc($go)});
        }
        $row->[$goInds->{$go}] = $val;
    }
    return $hit;
}

sub add_custom_sets {
    my ($rows) = @_;
    return unless ($customSets);
    $args->msg("Adding user-defined custom sets") if ($doWarn);
    $unusedCustSet = { map { $_ => $customSets->{$_} } keys %{$customSets} };
    my $csInd = &get_or_add_column(\%cpos, 'cust_set', \@corder);
    my $ind = $cpos{$objCol};
    $ind--;
    foreach my $row (@{$rows}) {
        my $val;
        if (my $id = $row->[$ind]) {
            if ($val = $customSets->{uc($id)}) {
                delete $unusedCustSet->{uc($id)};
            }
            # warn sprintf("%s = %s\n", $id, $val || "");
        }
        $row->[$csInd] = $val || "";
    }
    return $rows;
}

sub add_meta_sets {
    my ($rows) = @_;
    return if (!$metaCols || $#{$metaCols} == -1);
    $args->msg("Adding user-defined metadata") if ($doWarn);
    my %n2i;
    for my $m (0..$#{$metaCols}) {
        my $n = $metaCols->[$m];
        my $i = $cpos{'metasets'.$m};
        $n2i{$n} = $i - 1 if (defined $i);
    }
    my $ind = $cpos{$objCol};
    $ind--;
    foreach my $row (@{$rows}) {
        if (my $id = $row->[$ind]) {
            while (my ($n, $i) = each %n2i) {
                $row->[$i] = $metaSets->{uc($id)}{$n};
            }
        }
    }
}

sub process_genbank {
    my ($bss, $tout, $history, $skipped) = @_;
    my $uniq = $history->{uniq};
    $skipped ||= {};
    foreach my $bs (@{$bss}) {
        my $did = $bs->display_id();
        next if ($history->{name}{$did}++);
        my $seq = $bs->seq();
        if (my $os = $history->{seq}{$seq}) {
            unless ($allowDup) {
                $skipped->{$did} = $os;
                next;
            }
        }
        my $desc = $bs->desc();
        $history->{seq}{$seq} = $did;
        my $len  = length($seq);
        my $num  = ++$history->{count};
        my %hb;
        # Calculate the input terms that hit these
        foreach my $tin (sort keys %{$uniq->{$tout} || {}}) {
            unless ($tin) {
                next;
            }
            my ($m) = sort { ($b||0) <=> ($a||0)
                             } keys %{$uniq->{$tout}{$tin}};
            my @auths = $ad->simplify_authors
                (map {split(/ \< /, $_)} keys %{$uniq->{$tout}{$tin}{$m}} );
            my $sc = ($m == -1) ? 'N/A' : (int(0.5 + 1000 * $m) / 10).'%';
            $hb{$tin} = [ $sc, \@auths ];
            # $desc .= sprintf(" %s %s [%s]", $tin, $sc, join(',',@auths));
        }

        my $dxtra = "";
        while (my ($tin, $dat) = each %hb) {
            $dxtra .= sprintf(" %s %s [%s]", $tin, $dat->[0], 
                              join(',',@{$dat->[1]}));
        }
        $desc ||= "";
        if ($dxtra) {
            $desc .= " - " if ($desc);
            $desc .= "Hit by:$dxtra";
        }
        $bs->desc($desc);
        $history->{IO}->write_seq($bs);
    }
    return $skipped;
}

sub process_fasta {
    my ($reqs, $history, $skipped) = @_;
    my $uniq = $history->{uniq};
    $skipped ||= {};
    foreach my $sdat (@{$reqs}) {
        my ($did, $desc, $seq, $tout) = @{$sdat};
        next if ($history->{name}{$did}++);
        if (my $os = $history->{seq}{$seq}) {
            unless ($allowDup) {
                $skipped->{$did} = $os;
                next;
            }
        }
        $history->{seq}{$seq} = $did;
        my $len  = length($seq);
        my $num  = ++$history->{count};
        my %hb;
        # Calculate the input terms that hit these
        foreach my $tin (sort keys %{$uniq->{$tout} || {}}) {
            unless ($tin) {
                next;
            }
            my ($m) = sort { ($b||0) <=> ($a||0)
                             } keys %{$uniq->{$tout}{$tin}};
            $m = -1 if (!defined $m || $m eq '');
            my @auths = $ad->simplify_authors
                (map {split(/ \< /, $_)} keys %{$uniq->{$tout}{$tin}{$m}} );
            my $sc = ($m == -1) ? 'N/A' : (int(0.5 + 1000 * $m) / 10).'%';
            $hb{$tin} = [ $sc, \@auths ];
            # $desc .= sprintf(" %s %s [%s]", $tin, $sc, join(',',@auths));
        }
        if ($fmtFlags{JSON}) {
            print $outFH ",\n" unless ($num == 1);
            print $outFH "    ".$ser->obj_to_json({
                id => $did, desc  => $desc, seq => $seq, len => $len,
                hitBy => \%hb }, $doPretty);
            next;
        }

        my $dxtra = "";
        while (my ($tin, $dat) = each %hb) {
            $dxtra .= sprintf(" %s %s [%s]", $tin, $dat->[0], 
                              join(',',@{$dat->[1]}));
        }
        $desc ||= "";
        if ($dxtra) {
            $desc .= " - " if ($desc);
            $desc .= "Hit by:$dxtra";
        }
        
        my $blk = $args->val(qw(block chunk)) || 100;
        my $txt = sprintf(">%s %s\n", $did, $desc);
        for (my $l = 0; $l < $len; $l += $blk) {
            $txt .= substr($seq, $l, $blk)."\n";
        }
        print $outFH $fmtFlags{DAD} ? &esc_js($txt) : $txt;
    }
    return $skipped;
}

sub output_fh {
    $ad->bench_start;
    my $fh;
    if ($outFile) {
        my $hack = $outFile;
        my @maybe;
        while ($hack =~ /(__([^_].*?[^_])__)/) {
            my $rep = $1;
            push @maybe, $2;
            $hack =~ s/\Q$rep\E//g;
        }
        my $file  = $outFile;
        my $maxID = 3;
        foreach my $colm (@maybe) {
            my $col = &_standardize_column($colm);
            next unless ($col);
            my $rep;
            if ($col eq 'term_in') {
                if ($#uniqQ < $maxID) {
                    $rep = join('+', sort @uniqQ);
                } else {
                    $rep = sprintf("%s ID%s", $#uniqQ + 1, 
                                   $#uniqQ == 0 ? '' : 's');
                }
            } elsif ($col eq 'ns_in') {
                if (ref($ns1)) {
                    $rep = join('+', sort keys %{$ns1});
                } elsif ($ns1) {
                    $rep = $ns1;
                }
            }
            $rep ||= 'X';
            $file =~ s/__\Q$colm\E__/$rep/g;
        }
        $outFile = $file;
        open(OUTFH, ">$file") || $args->death("Failed to write to output",
                                              $file, $!);
        $fh = *OUTFH;
    } else {
        $fh = *STDOUT;
    }
    $ad->bench_end;
    return $fh;
}

sub finish {
    if ($benchFile) {
        $fc->write_output('bench', $ad->benchmarks_to_text());
    }
    print STDERR $ad->showbench( $nocgi ? (-shell => 1) : (html => 1),
                                 -minfrac => $doBench)
        if ($doBench && $primaryChild && $primaryChild > 0);
}

our %reservedNames = map { $_ => 1 } qw(if else repeat while function for in next break TRUE FALSE NULL Inf NaN NA NA_integer_ NA_real_ NA_complex_ NA_character_
);

sub finish_up {
    &_close_out();
    exit;
}

sub _close_out {
    return unless ($outFile && $outFH);
    close $outFH;
    my $sz = -e $outFile ? sprintf("%.3fMb", (-s $outFile) / 1000000)
        : 'Does not exist!';
    $args->msg("[OUTPUT]", "Output file - $sz", $outFile) if ($vb);
}

sub _structure_node_data {
    $ad->bench_start;
    my ($rows) = @_;
    $args->msg("Building Graph") if ($doWarn);
    my $graph  = BMS::FriendlyGraph->new();

    my ($n1, $n2, $s1, $s2, $au, $sc) = map { $cpos{$_} }
    qw(term_in term_out ns_in ns_out auth matched);

    foreach my $row (@{$rows}) {
        my $node1;
        if ($n1) {
            if (my $name1 = $row->[$n1-1]) {
                my $ns1   = $s1 ? $row->[$s1-1] : 'Unknown';
                $node1    = $graph->node("$name1 [$ns1]");
                $node1->attribute('label', $name1);
                $node1->attribute('ns', $ns1);
            }
        }
        if ($n2) {
            if (my $name2 = $row->[$n2-1]) {
                my $ns2  = $s2 ? $row->[$s2-1] : 'Unknown';
                my $node2 = $graph->node("$name2 [$ns2]");
                $node2->attribute('label', $name2);
                $node2->attribute('ns', $ns2);

                if ($node1) {
                    my $edge = $graph->edge($node1, $node2);
                    if ($sc || $au) {
                        my @data = ($sc ? $row->[$sc - 1] : undef,
                                    $au ? $row->[$au - 1] : undef );
                        $data[0] = -1 unless (defined $data[0]);
                        push @{$edge->{FOO_SCORE}}, \@data;
                    }
                }
            }
        }
    }
    foreach my $edge ($graph->each_edge) {
        if (my $list = $edge->{FOO_SCORE}) {
            $edge->attribute('scoreList', $list);
            delete $edge->{FOO_SCORE};
        }
    }

    $ad->bench_end;
    return $graph;
}

sub write_to_oracle {
    my ($lines) = @_;
    my $rows = [ map { [ split("\t", $_) ] } @{$lines} ];
    if (!$rows || $#{$rows} == -1) {
        $args->msg("No data selected, no changes made to Oracle") if ($vb);
        return undef;
    }
    $ad->bench_start;
    my $tname = lc($oraTable && $oraTable ne '1' ? $oraTable : 'temp');
    my $user  = lc($ldap || 'anon');
    $tname    =~ s/[^a-z0-9]+/_/g;
    my $oklen = 25;
    $oklen   -= length($user) + 1;
    if (length($tname) > $oklen) {
        $tname =~ s/[aeiou]+//g;
    }
    my $table = join('_', $ldap, $tname);
    $table    =~ s/[^a-z0-9]+/_/g;
    my $odbh  = BMS::FriendlyDBI->connect
        ("dbi:Oracle:", 'gaout/gaout@mtrkp1',
         undef, { RaiseError  => 0,
                  PrintError  => 0,
                  LongReadLen => 100000,
                  AutoCommit  => 1, },
         -errorfile => '/scratch/GAOUT.err',
         -adminmail => 'tilfordc@bms.com', );
    my $tdesc  = "GenAcc $mode output results generated on ".`date`;
    $tdesc =~ s/[\s\n\r]+$//;
    my @cols;
    my $index;
    for my $c (0..$#corder) {
        my $cname = $corder[$c];
        my $desc  = $hdata->{$cname}{label};
        my $type  = "varchar(100)";
        if ($cname =~ /^term_(.+)/) {
            my $ibase = $table . '_' .substr($1,0,1);
            $index ||= {};
            $index->{$ibase . "u"}{cols} = [ "upper($cname)" ];
            $index->{$ibase}{cols} = [ "$cname" ];
        } if ($cname =~ /^(desc|User)_/) {
            $type = "varchar(4000)";
        } elsif ($cname =~ /^(auth)$/) {
            $type = "varchar(500)";
        } elsif ($cname =~ /^(ns_between)$/) {
            $type = "varchar(1000)";
        } elsif ($cname =~ /^(matched|depth|inventory|specificity)$/) {
            $type = "number";
        } elsif ($cname =~ /^(updated)$/) {
            $type = "date";
        }
        push @cols, [$cname, $type, $desc];
    }
    
    $index = undef if ($args->val(qw(noindex)));
    my $schema = {
        name => $table,
        com  => $tdesc,
        index => $index,
        cols  => \@cols,
    };
    my $tinfo  = $odbh->table_info();
    my $makeit = 1;
    my @using  = @corder;
    if (my $tdat = $tinfo->{$table}) {
        # The table alread exists
        if ($args->val(qw(drop clobber truncate))) {
            $ad->bench_start("Drop Oracle Table");
            my $sth = $odbh->prepare("DROP TABLE $table");
            $sth->execute();
            delete $tinfo->{$table};
            $ad->bench_end("Drop Oracle Table");
        } else {
            $makeit = 0;
            my @lost;
            @using = ();
            my %inDB = map { $_ => 1 } @{$tdat->{order}};
            foreach my $cname (@corder) {
                if ($inDB{$cname}) {
                    push @using, $cname;
                } else {
                    push @lost, $cname;
                }
            }
            if ($#using == -1) {
                $args->err("None of your current output columns match those within the existing table $table", "No operations performed");
                $ad->bench_end;
                return undef;
            } elsif ($#lost != -1) {
                $args->err("Some output columns are not in the existing table and are being discarded: ", @lost);
            }
        }
    }
    if ($makeit) {
        $ad->bench_start("Create Oracle Table");
        $odbh->make_table( $table, $schema );
        $ad->bench_end("Create Oracle Table");
    }
    my $isql  = sprintf
        ("INSERT INTO %s ( %s ) VALUES ( %s )", $table, join(", ", @using),
         join(", ", map { '?' } @using));
    my $isth = $odbh->prepare( -sql => $isql,
                               -name => "Insert row into user table");
    my @indices = map { $cpos{$_} - 1 } @using;

    $ad->bench_start("Write Rows");
    foreach my $row (@{$rows}) {
        my @vals = map { $row->[$_] } @indices;
        $isth->execute(@vals);
    }
    $ad->bench_end("Write Rows");

    if ($index && !$args->val(qw(nostatistics nostats))) {
        $ad->bench_start("Compute Statistics");
        $odbh->statistics( -percent => 100 );
        $ad->bench_end("Compute Statistics");
    }

    $ad->bench_end;
    return $table;
}

sub go_match {
    my ($rows) = @_;
}

sub cached_parents {
    my ($id, $ns) = @_;
    return wantarray ? () : [] unless ($id);
    unless ($parCache{$ns}{$id}) {
        my @pars = $ad->all_parents( -id => $id, -ns => $ns, -warn => $doWarn );
        $parCache{$ns}{$id} = \@pars;
    }
    return wantarray ? @{$parCache{$ns}{$id}} : $parCache{$ns}{$id};
}

sub clean_query {
    my ($id, $ns) = @_;
    return wantarray ? ('', '') : '' unless ($id);
    $ad->benchstart;
    if ((!$ns || ref($ns)) && $id =~ /^\#([A-Z]{2,4})\#(.+)$/i) {
        # The ID has a namespace token prefixed to it ala #RSR#NM_001234
        ($ns, $id) = (uc($1), $2);
    }
    if ($id =~ /^\d+$/) {
        if ($intNSfrm) {
            $id = sprintf($intNSfrm, $id);
        }
        $ad->benchend;
        return wantarray ? ($id, $ns) : $id;
    }
    my ($pns, $std) = $ad->pick_namespace($id, $ns, ref($ns) ? 1 : 0);
    if ($pns eq 'UNK') {
        
    }
    $ad->benchend;
    return wantarray ? ($std || $id, $pns) : $std;
}

sub atom_node {
    $ad->bench_start;
    my ($node, $pad) = @_;
    my $ptxt = " " x $pad;
    my $xml  = "";
    if ($node->{updated}) {
        $node->{updated} =~ s/ /T/;
        $node->{updated} .= 'Z';
    }

    my $id    = $node->{id};
    my $ns    = $node->{ns};
    my $nsn   = $ad->namespace_name($ns);
    my $title = $id;
    $title   .= " [$nsn]" if ($nsn);
    my $link  = $node->{url};
    if ($link) {
        if (my $tok = $node->{ltok}) {
            $title .= " $tok";
            $id .= "/$tok";
        }
    } else {
        $link = "$selfLink?format=Atom&id=$id";
        $link .= "&ns=$ns" if ($ns);
    }
    $xml  .= "$ptxt<title>$title</title>\n";
    $xml  .= "$ptxt<id>http://bioinformatics.bms.com/accession/".
        ($ns || 'UNK')."/$id</id>\n";
    
    # Print if available
    foreach my $tag ('subtitle', 'summary') {
        if (my $val = $node->{$tag}) {
            $xml .= "$ptxt<$tag>".&esc_xml($val)."</$tag>\n";
        }
    }
    # Always print
    foreach my $tag ('updated') {
        my $val = $node->{$tag};
        $xml .= "$ptxt<$tag" .
            (defined $val ? ">".&esc_xml($val)."</$tag>" : " />")."\n";
    }
    if ($ns) {
        $xml .= "$ptxt<category term='$ns' label='$nsn' />\n"
    }
    $xml  .= "$ptxt<link href='".&esc_xml($link)."' />\n";
    if (my $auths = $node->{auth}) {
        map { $xml .= "  <author><name>".&esc_xml($_)."</name></author>\n" } sort keys %{$auths};
    }
    $ad->bench_end;
    return $xml;
}

sub _manage_cluster_stack {
    my ($stack) = @_;
    my $sn = $#{$stack};
    return () if ($sn == -1);
    $ad->bench_start;
    my $tf = "<td%s>%s</td>";
    my $cr = $#corder;
    my @toAdd;
    if ($sn == 0) {
        # This is just a single row
        push @toAdd, [ map { sprintf($tf, $lfmtTwo->[$_], $stack->[0][$_]) } (0..$cr) ];
    } else {
        my $rs = " rowspan='".($sn+1)."'";
        @toAdd = map { [] } (1..$sn);
        for my $c (0..$cr) {
            # Cycle through each column, check if values are unique or mixed:
            my %distinct = map { $_->[$c] => 1 } @{$stack};
            my @u        = keys %distinct;
            my $lfc      = $lfmtTwo->[$c];
            if ($#u == 0) {
                # This column has a single value in the cluster
                push @{$toAdd[0]}, sprintf($tf, $lfc . $rs, $u[0]);
                next;
            }
            # Each row gets a cell
            for my $s (0..$sn) {
                push @{$toAdd[$s]}, sprintf($tf, $lfc, $stack->[$s][$c]);
            }
        }
    }
    my @lines = map { " <tr>".join('', @{$_})."</tr>" } @toAdd;
    $ad->bench_end;
    return @lines;
}

sub err {
    if ($jsonType) {
        push @errors, @_;
    } else {
        map { warn "ERROR: $_\n" } @_;
    }
}

sub start_DAD {
    print $outFH '{ \'content\': "';
}

sub esc_js {
    my ($html) = @_;
    $html =~ s/\\/\\\\/g;
    $html =~ s/\"/\\\"/g;
    $html =~ s/[\n\r]/\\n/g;
    return $html;
}

sub end_JSON {
    my $jps = $jsonp ? ")" : "";
    if ($jsonType eq 'DataTable') {
        print $outFH "\n}$jps\n";
        return;
    }
    $ad->bench_start;
    my $dt   = `date`; chomp($dt);
    print $outFH '"' if ($jsonType eq 'DAD');
    my %query;
    foreach my $key ($args->all_keys( -nodef => 1 )) {
        $query{$key} = $args->val($key);
    }
    my %flags = ( objectType    => $jsonType,
                  dateGenerated => $dt,
                  queryParams   => \%query,
                  commandLine   => $args->command_line( -nodef => 1),
                  serverHost    => $ENV{HTTP_HOST},
                  serverQuery   => $ENV{QUERY_STRING},
                  serverUrl     => $ENV{SCRIPT_NAME} );
    $flags{include}    = \@css if ($#css != -1 && $jsonType eq 'DAD');
    $flags{serverArgs} = \@ARGV unless ($#ARGV == -1);
    $flags{error}      = join(' / ', @errors) unless ($#errors == -1);
    if ($VERSION =~ /\,v (\S+) /) { $flags{serviceVersion} = $1 };
    foreach my $key (sort keys %flags) {
        my $val = $flags{$key};
        next unless (defined $val && $val ne '');
        print $outFH ",\n  \"$key\": ".$ser->obj_to_json($val, $doPretty);
    }
    print $outFH "\n}$jps\n";
    $ad->bench_end;
}

sub perl2js  {
    my ($val, $expand) = @_;
    return 'null' unless (defined $val);
    my $ref = ref($val);
    if (!$ref) {
        unless ($val =~ /^\-?(\d+|\d*\.\d+)$/) {
            # $val =~ s/\"/\\\"/g;
            $val = '"' .&esc_js($val). '"';
        }
        return $val;
    }
    my ($pre, $pro) = ('','');
    if ($expand) {
        $pre = "\n" . (' ' x $expand);
        $expand++;
    }
    if ($ref eq 'ARRAY') {
        return $pre.'['.join
            (',', map { &perl2js($_, $expand) } @{$val}).']'.$pro;
    } elsif ($ref eq 'HASH') {
        my @parts;
        foreach my $key (sort keys %{$val}) {
            my $v = $val->{$key};
            push @parts, sprintf
                ("%s: %s", &perl2js($key), &perl2js($v, ($expand && ref($v)) ? $expand : 0));
        }
        return $pre.'{'.join(',', @parts).'}';
    } else {
        return "'Can not convert $ref object to JavaScript'";
    }
}

sub _standardize_column {
    my ($req) = @_;
    return '' unless ($req);
    if (my $r = ref($req)) {
        if ($r eq 'ARRAY') {
            my @rv = map { &_standardize_column( $_ ) } @{$req};
            return \@rv;
        }
        return '';
    }
    $ad->bench_start;
    my $reMap;
    if ($req =~ /(.+)\((.+)\)/) {
        ($req, $reMap) = ($1, $2);
    }
    my $rv = lc($req);
    if ($req =~ /^\#/) {
        # Conversion request
        my ($id, $ns, $int, $intns) = &_id_and_ns( $req );
        if ($ns) {
            $rv = "#$ns#";
            my $cname = $ns;
            if ($int) {
                $rv .= '['.($intns ? "#$intns#" : "") . $int . ']';
                $cname .= " [$int]";
            }
            $reMap ||= $cname;
        }
        $req = "";
    } else {
        $req = lc($req);
    }
    my ($x,$y);
    if ($req =~ /desc[a-z]*_?(out|2)/ ||
        $req =~ /ont.*desc/) {
        $rv = 'desc_out';
    } elsif ($req =~ /desc[a-z]*_?(link)/) {
        $rv = 'desc_link';
    } elsif ($req =~ /(specific)/) {
        $rv = 'specificity';
    } elsif ($req =~ /(tax|spec).*_?in/) {
        $rv = 'tax_in';
    } elsif ($req =~ /(tax|spec)/) {
        $rv = 'tax_out';
    } elsif ($req =~ /(match(?:ed)?|min|score)_?in/ ||
             $req =~ /(in|input)_?(match|min|score)/) {
        $rv = 'matched_in';
    } elsif ($req =~ /go_?class/) {
        $rv = 'go_class';
    } elsif ($req =~ /date/) {
        $rv = 'updated';
    } elsif ($req =~ /acc.*desc/) {
        $rv = 'desc_in';
    } elsif ($req =~ /sym.*_?in/) {
        $rv = 'sym_in';
    } elsif ($req =~ /(id|term)_?(mid|btwn|between)/ ) {
        $rv = 'term_between';
    } elsif ($req =~ /(id|term)_?(share|common)/ ||
             $req =~ /(shared?|common)_?(id|term)/) {
        $rv = 'term_share';
    } elsif ($req =~ /((id|term)_?(out|2)|ont)/ || $req eq 'sub') {
        $rv = $mode eq 'simple' ? 'term_in' : 'term_out';
    } elsif ($req =~ /(term|acc)/ || $req eq 'qry') {
        $rv = 'term_in';
    } elsif ($req =~ /ns_?(mid|btwn|between)/) {
        $rv = 'ns_between';
    } elsif ($req =~ /ns_?(out|2|sub)/ || $req =~ /(out|sub)_?ns/) {
        $rv = 'ns_out';
    } elsif ($req =~ /ns/) {
        $rv = 'ns_in';
    } elsif ($req =~ /(len|length)_?in|qry_?len/) {
        $rv = 'len_in';
    } elsif ($req =~ /(len|length)_?out|sub_?len/) {
        $rv = 'len_out';
    } elsif ($req =~ /(sortchr|chrsort|fullchr)/) {
        $rv = 'sortchr';
    } elsif ($req =~ /com_?link/) {
        $rv = 'com_link';
    } elsif ($req =~ /(link|url)/) {
        $rv = 'links';
    } elsif ($req =~ /desc_?(in|1)/) {
        $rv = 'desc_in';
    } elsif ($req =~ /desc/) {
        $rv = ($mode eq 'desc' || $mode eq 'lookup') ? 'desc_in' : 'desc_out';
    } elsif ($req =~ /(auth|evid|ec)/) {
        $rv = 'auth';
    } elsif ($req =~ /(nicescore)/) {
        $rv = 'nicescore';
    } elsif ($req =~ /comp.*(\d+)/) {
        $rv = 'complist'.$1;
    } elsif ($req =~ /(match|min|score)/) {
        $rv = 'matched';
    } elsif ($req =~ /(perc)/) {
        $rv = 'matched';
        $fmtFlags{AsPercent} = 1;
    } elsif ($req =~ /(child|kid)/) {
        $rv = 'child';
    } elsif ($req =~ /sym/) {
        $rv = 'sym_out';
    } elsif ($req =~ /(meta)/) {
        $rv = 'metasets';
    } elsif ($req =~ /set/) {
        $rv = $req =~ /cust/ ? 'cust_set' : 'set';
    } elsif ($req =~ /seq_?(out|2)/) {
        $rv = 'seq_out';
    } elsif ($req =~ /status/) {
        $rv = 'ns_out';
    } elsif ($req =~ /(pubmed|pmid)/) {
        $rv = 'pubmed';
    } elsif ($req =~ /bad/) {
        $rv = 'howbad';
    } elsif ($req =~ /par/) {
        $rv = 'parent';
    } elsif ($req =~ /clust/ && $req =~ /(size|num|len)/) {
        $rv = 'clust_size';
    } elsif ($req =~ /build/) {
        $rv = 'vers_out';
    } elsif ($req =~ /(depth|level|lvl)/) {
        $rv = 'depth';
    } elsif ($req =~ /(dist)/) {
        $rv = 'distance';
    #} elsif ($req =~ /(sub_?vers)/) {
    #    # Needed for overlap Build
    #    $rv = 'sub_vers';
    } elsif ($req =~ /(vers|loc|ft|end|start|begin|stop).*_?.*(in|out|1|2|qry|sub|query|ns)/) {
        ($x,$y) = ($1,$2);
    } elsif  ($req =~ /(in|out|1|2|qry|sub|query).*_?.*(vers|loc|ft|end|start|begin|stop|ns)/) {
        ($x,$y) = ($2,$1);
    } elsif ($req =~ /(str)/) {
        $rv = 'strand';
    } elsif ($req =~ /(chemty|assay)/) {
        $rv = 'chemtype';
    } elsif ($req =~ /(chemunit)/) {
        $rv = 'chemunit';
    } elsif ($req =~ /(qry|query)_?(src|source)/) {
        $rv = 'qry_src';
    } elsif ($req =~ /(inv|stock)/) {
        $rv = $req =~ /dry/ ? 'dryinv' : 'inventory';
    } elsif ($req =~ /^GO:\d{7}$/i) {
        $rv = uc($req);
    }
    if ($x && $y) {
        if ($x =~ /vers/) {
            $x = 'vers';
        } elsif ($x =~ /(loc|ft)/) {
            $x = 'loc';
        } elsif ($x =~ /(start|begin)/) {
            $x = 'start';
        } elsif ($x =~ /(end|stop)/) {
            $x = 'end';
        }
        if ($y =~ /(in|1|qry|query)/) {
            $y = 'in';
        } elsif ($y =~ /(out|2|sub)/) {
            $y = 'out';
        }
        $rv = $x.'_'.$y;
        if ($rv eq 'vers_out' && $mode =~ /(overlap)/) {
            $rv = 'sub_vers';
        } elsif ($rv eq 'sub_vers' && $mode =~ /(map)/) {
            $rv = 'vers_out';
        }
    }
    $rv = 'desc_in' if ($rv eq 'desc_out' && $mode eq 'desc');
    $hdata->{$rv}{label} = $reMap if ($reMap);
    $ad->bench_end;
    return $rv;
}


# Format : #NS#ID[#IntNS#IntID]
sub _id_and_ns {
    my ($txt) = @_;
    return () unless ($txt);
    my ($id, $ns, $int, $intns);
    if ($txt =~ /(.+)\s*\[([^\]]+)\]\s*$/) {
        $txt = $1;
        ($int, $intns) = &_id_and_ns($2);
    }
    if ($txt =~ /^\#+([^\#]+)\#+$/) {
        # Only namespace
        $ns = $ad->namespace_token($1);
    } elsif ($txt =~ /\#([^\#]+)\#+([^\#]+)$/) {
        # Both
        ($ns, $id) = ($ad->namespace_token($1), $2);
    } else {
        # ID only
        $id = $txt;
    }
    return ($id, $ns, $int, $intns);
}

sub _targ_ns {
    $ad->bench_start;
    my ($n2s, $n1) = @_;
    my $rv = [''];
    if ($#{$n2s} != -1) {
        $rv = $n2s;
    } elsif ($n1) {
        my $known = $ad->known_conversions();
        my $key   = $ad->namespace_token($n1);
        $rv       = [ sort keys %{$known->{$key}} ];
        push @{$rv}, '' if ($#{$rv} == -1);
    }
    if ($fmtFlags{OntologyReport}) {
        my @keep;
        foreach my $ns (@{$rv}) {
            push @keep, $ns if ($ad->is_namespace($ns, 'ONT'));
        }
        $rv = \@keep;
    }
    $ad->bench_end;
    return $rv;
}

sub format_struct {
    $ad->bench_start;
    my ($struct) = @_;
    my @parsed;
    my $getMet = {
        desc => { map { $_ => 1 } qw(AL AP APS AR BTFO CDD ENSG ENSP ENST GO IPI IUO LL NRDB PMID RS RSP RSR SP SPN TR TRC TRN UG UP UPN XONT) },
        tax  => { map { $_ => 1 } qw(ORTH) },
        link => { map { $_ => 1 } qw(AL AP APS AR BTFO CDD ENSG ENSP ENST GO IPI IUO LL NRDB ORTH PMID RS RSP RSR SP SPN SYM TR TRC TRN UG UP UPN) },
        onto => { map { $_ => 1 } qw(GO BTFO XONT) },
    };
    my @things = &fancySort([keys %{$struct}]);
    if ($fmtFlags{JSON}) {
        print $outFH " queryStructure: {\n";
        for my $i (0..$#things) {
            print $outFH ",\n" if ($i);
            my $in = $things[$i];
            print $outFH "   ".$ser->obj_to_json($in, $doPretty).": {\n";
            my @ns2s = sort keys %{$struct->{$in}};
            for my $j (0..$#ns2s) {
                print $outFH ",\n" if ($j);
                my $ns2  = $ns2s[$j];
                my $hash = $struct->{$in}{$ns2};
                my @outs = sort keys %{$hash};
                print $outFH "    ".$ser->obj_to_json($ns2, $doPretty).": {\n";
                for my $k (0..$#outs) {
                    print $outFH ",\n" if ($k);
                    my $out = $outs[$k];
                    my @scores;
                    while (my ($sc, $auths) = each %{$hash->{$out}}) {
                        push @scores, [$sc, [sort keys %{$auths}]];
                    }
                    print $outFH "     ".$ser->obj_to_json($out, $doPretty).
                        ":".$ser->obj_to_json(\@scores, $doPretty);
                }
                print $outFH "\n    }";
            }
            print $outFH "\n   }";
        }
        print $outFH "\n  }";
        $ad->bench_end;
        &end_JSON();
        &finish_up;
    }
    foreach my $in (@things) {
        my $intok = $rNS ? $rNS->{$in} : $oneNs1 ? $ad->namespace_token( $ns1)
            : $ad->guess_namespace($in);

        my $data = {
            id  => $in,
            res => 0,
            org => [],
        };
        push @parsed, $data;
        my @n2s = sort keys %{$struct->{$in}};
        if ($getMet->{desc}{$intok}) {
            $data->{desc} = &_get_desc( $in, $intok );
        }
        if ($#n2s == 0) {
            if ($mode eq 'desc') {
                my @descs = keys %{$struct->{$in}{''}};
                $data->{desc} = $descs[0] || 'No description found';
                $data->{res} = 1;
                next;
            } else {
                
                if ($n2s[0]) {
                    # Single namespace
                    $data->{idmod} = $n2s[0];
                }
            }
        }
        foreach my $n2 (@n2s) {
            my $hash = $struct->{$in}{$n2};
            my $ntok = $ad->namespace_token($n2);
            delete $hash->{''};
            my @hits;
            my @outs = &fancySort([keys %{$hash}]);
            foreach my $out (@outs) {
                my @stuff;
                foreach my $m (sort { $b <=> $a } keys %{$hash->{$out}}) {
                    my $auths = $hash->{$out}{$m};
                    delete $auths->{''};
                    $m = $nullSc if ($m < 0);
                    push @stuff, [$m, join(', ', sort keys %{$auths})];
                }
                my $meta;
                if ($getMet->{desc}{$ntok}) {
                    my $u2 = $ad->effective_namespace( $ntok, $intok, $out );
                    $meta = &_get_desc( $out, $u2 );
                } elsif ($getMet->{tax}{$ntok}) {
                    my $u2 = $ad->effective_namespace($ntok, $intok, $out);
                    my @taxae = $ad->convert
                        ( -id => $out, -ns1 => $u2, -ns2 => 'TAX', -age => $age );
                    $meta = join(', ', @taxae);
                }
                push @hits, [ $out, \@stuff, $meta ];
            }
            $data->{res} += $#hits + 1;
            push @{$data->{org}}, [ $n2, \@hits ];
        }
    }

    if ($fmtFlags{HTML}) {
        foreach my $data (@parsed) {
            my $id = $data->{id};
            my $intok = $rNS ? $rNS->{$id} : $ad->namespace_token($ns1);
            my $html  = "<span class='q'>$id ".$ad->namespace_links($id, $intok,undef,$urlJoin)."</span>\n";
            if (my $im = $data->{idmod}) {
                $html .= " : <span class='ns'>$im</span>\n";
            }
            if (my $desc = $data->{desc}) {
                $html .= " <span class='metd'>$desc</span>\n";
            }
            if ($data->{res} < 1) {
                $html .= " <span class='null'>no information found</span><br />\n";
                print $outFH $fmtFlags{DAD} ? &esc_js($html) : $html;
                next;
            }
            my $isMult = $#{$data->{org}} > 0 ? 1 : 0;
            
            $html .= "<ul>\n" if ($isMult);
            foreach my $odat (@{$data->{org}}) {
                my ($n2, $hits) = @{$odat};
                my $link;
                my $ntok = $ad->namespace_token($n2);
                my $u2   = $ad->effective_namespace( $ntok, $intok );
                if ($getMet->{link}{$ntok}) {
                    $link  = "genacc_service.pl?format=$fmt&ns1=$u2&id=";
                    $link  =~ s/ /+/g;
                }
                if ($isMult) {
                    $html .= " <li><span class='ns'>$n2</span>";
                    if ($#{$hits} < 0) {
                        $html .= " <span class='null'>no data</span></li>\n";
                        next;
                    }
                    
                }
                if ($#{$hits} > 0 && $#{$hits} < 30) {
                    my $urls = $ad->namespace_links
                        ( [map {$_->[0]} @{$hits}], $u2, $intok,$urlJoin);
                    $html .= " <i>All:</i> $urls" if ($urls);
                }
                if ($getMet->{onto}{$ntok}) {
                    $html .= &struct_onto($hits, $link, $n2, $intok);
                } else {
                    $html .= &struct_table($hits, $link, $u2, $intok);
                }
                $html .= "</li>" if ($isMult);
            }
            if ($isMult) {
                $html .= "</ul>\n";
            } else {
                $html .= "<br />\n";
            }
            print $outFH $fmtFlags{DAD} ? &esc_js($html) : $html;
        }
    }
    &end_JSON() if ($jsonType);
    $ad->bench_end;
    &finish();
    &finish_up;
}

sub esc_xml {
    my ($txt) = @_;
    return "" unless (defined $txt);
    $ad->bench_start;
    $txt =~ s/\&/&amp;/g;
    $txt =~ s/\>/\&gt;/g;
    $txt =~ s/\</&lt;/g;
    $txt =~ s/\'/&apos;/g;
    $ad->bench_end;
    return $txt;
}

sub esc_html {
    my ($txt) = @_;
    return "" unless (defined $txt);
    $ad->bench_start;
    $txt =~ s/\&/&amp;/g;
    $txt =~ s/\>/\&gt;/g;
    $txt =~ s/\</&lt;/g;
    $ad->bench_end;
    return $txt;
}

sub struct_onto {
    $ad->bench_start;
    my ($hits, $link, $ns) = @_;
    my @nodes;
    map { $fo->clear_node_param($_) } qw(stuff class detail);
    foreach my $hit (@{$hits}) {
        my ($node, $stuff, $meta) = @{$hit};
        push @nodes, $node;
        $fo->node_param('class', $node, 'm ' .(&class4score($stuff) || 'm3'));
        $fo->node_param('stuff', $node, $stuff);
    }
    my $fpaths = $fo->build_paths( \@nodes );
    my $html = $fo->paths2html( $fpaths);
    $ad->bench_end;
    return $html;
}

sub struct_table {
    $ad->bench_start;
    my ($hits, $link, $nsA, $nsB) = @_;
    my $html = " <table><tbody>\n";
    foreach my $hit (@{$hits}) {
        my ($id, $stuff, $meta) = @{$hit};
        my $pop = $ad->namespace_links($id, $nsA, $nsB,$urlJoin);
        my $show = $id;
        if ($showSMI && $nsA eq 'SMI' && $id =~ /^MTID/) {
            $show = $ad->tracker->get_seq($id)->name;
        }
        if ($link) {
            my $class = "";
            $id = sprintf("<a %shref='%s%s'>%s</a>",
                           $class, $link, $id, $show,);
        }
        $html .= "  <tr><td>$show</td><td>$pop</td><td>";
        $html .= &stuff2html( $stuff );
        $html .= "</td>";
        $html .= "<td class='metd'>$meta</td>" if ($meta);
        $html .= "</tr>\n";
    }
    $html .= " </tbody></table>\n";
    $ad->bench_end;
    return $html;
}

sub stuff2html {
    my ($stuff, $classes, $tag, $joiner) = @_;
    return '' unless ($stuff);
    $tag    ||= 'span';
    $joiner ||= ' ';
    my @bits;
    foreach my $s (@{$stuff}) {
        my ($m, $auths) = @{$s};
        my $html;
        my $c  = &class4score( $m ) || 'm3';
        $c    .= " $classes" if ($classes);
        my $sc = (defined $m) ? sprintf("%.3f", $m) : 'n/a';
        $html .= "<$tag class='$c'>$sc</$tag>";
        push @bits, sprintf("%s <$tag class='auth'>[%s]</$tag> ",
                            $html, $auths || 'UNK');
    }
    return join($joiner, @bits);
}

sub class4score {
    my ($m) = @_;
    ($m) = sort {$b <=> $a} map { defined $_->[0] ? $_->[0] : - 1 } @{$m} if
        (ref($m));
    return '' if (!defined $m || $m < 0);
    my $c = 0;
    if ($m < 0.5) {
        $c = 3;
    } elsif ($m < 0.8) {
        $c = 2;
    } elsif ($m < 0.95) {
        $c = 1;
    }
    return "m$c";
}

sub outstyles {
    my $html = "";
    $html .= join('', map 
                  {"<link type='text/css' rel='stylesheet' href='$_' />\n"} 
                  @css);
    $html .= $ad->namespace_url_styles();
    return $html;
}

sub HTML_INTERFACE {
    $ad->bench_start;
    &set_mime( );
    $ad        ||= &getAccessDenorm();
    my $known    = $ad->known_conversions();
    my @knownJS;
    my %allTargs = ( "''" => 1 );
    while (my ($n1, $two) = each %{$known}) {
        my @n2s = keys %{$two};
        push @knownJS, "$n1:{ ".join(", ", map { "$_:1" } ("''",@n2s)) ."}";
        map { $allTargs{$_} = 1 } @n2s;
    }
    push @knownJS, "'':{ ".join(", ", map { "$_:1" } keys %allTargs) ."}";
    my $kjs = join(",\n    ", @knownJS);
    &standard_head( *STDOUT );
    print "<!-- Styles -->\n";
    print &outstyles;

print <<EOF;
  <script type='text/javascript' src='/biohtml/javascript/configureForm.js'></script>

  <script>
    var knownMaps = { 
        $kjs };
    function changeNs1(x,y) {
        var src = document.getElementById( x );
        var trg = document.getElementById( y );
        if (!src || !trg) return;
        var ns1, ns2;
        if (y && y == 'intns') { ns2 = src.value; }
        else { ns1 = src.value; }
        var good = 0;
        for (var o=0; o < trg.options.length; o++) {
            var opt = trg.options[o];
            if (y && y == 'intns') { ns1 = opt.value; }
            else { ns2 = opt.value; }
            if (knownMaps[ns1] && knownMaps[ns1][ns2]) {
                opt.style.color = 'green';
                good++;
            } else {
                opt.style.color = '#dddddd';
            }
        }
        var rpt = document.getElementById( y+'ok' );
        if (rpt) rpt.innerHTML = good + " valid target" + (good == 1 ? '':'s');
        colTxt(x,y);
    }
    function colTxt(x,y) {
        var src = document.getElementById( x );
        var trg = document.getElementById( y );
        var txt = document.getElementById( y + 'txt' );
        if (!src || !trg || !txt) return;
        var ns1, ns2;
        if (y && y == 'intns') {ns1 = trg.value; ns2 = src.value; }
        else { ns1 = src.value; ns2 = trg.value;}
        if (knownMaps[ns1] && knownMaps[ns1][ns2]) {
            txt.style.color = '';
            txt.style.backgroundColor = '';
        } else {
            txt.style.color = 'red';
            txt.style.backgroundColor = 'orange';
        }
    }
    function setPopInt(frm) {
        var sind  = frm.selectedIndex;
        var sel   = frm.options[sind];
        var pival = sel.value;
        var arr   = pival.split(' ');
        var intns = arr.shift();
        var int   = arr.join(' ');
        var pii   = document.getElementById( 'intersection' );
        var piins = document.getElementById( 'intns' );
        if (pii && piins) {
            pii.value = int;
            piins.value = intns;
        }
    }
  </script>
 </head>
 <style>

 h3 { color: #F60; padding: 2px; border: #ddd solid 1px; }
 .conf { font-size: 0.8em; color: orange; background-color: navy; }
 .twhelp {
     background-color: #ff3;
     text-decoration: none;
     padding-left:  2px;
     padding-right: 2px;
     margin-left:   2px;
     margin-right:  2px;
     font-size:     smaller;
     font-weight:   bold;
   color:   #f00;
   display: inline;
 }
</style>
 <body bgcolor="white">
  <center><font size='+2' color='brick'><b>
   GenAcc Service Front End
  </b></font><br />
EOF

    print "<p style='color:red'>" . &help('BetaSoftware') . 
            "*** THIS VERSION IS BETA SOFTWARE ***</p>\n" if ($isBeta);
    print &help('SoftwareOverview','[Software Overview]');
    print "</center>";
    if (my $msg = $args->val(qw(formmsg))) {
        print $msg;
    }

    print "<form method='post' enctype='multipart/form-data'>\n";
    print "<table><tbody><tr><td valign='top'>";
    print &help('InputTerms'). "<b>Query terms:</b><br />".
        "<textarea rows='25' style='background-color:#cff' name='queries'>";
    print join("\n", @uniqQ) || '';
    print "</textarea><br />\n";
    print "<b>Queries from files ...</b></br>\n";
    my $spVal = $noRun ? ($args->val(qw(idlist idpath idfile idfasta)) || '')
        : '';
    print &help('RemoteFile').
        "<b>... By Server Path:</b><br />".
        "<input type='text' id='idpath' name='idpath' value='$spVal' /><br />\n";
    print &help('LocalFile').
        "<b>... Upload from PC:</b><br />".
        "<input type='file' size='8' style='width:8em;' name='idfile' /><br />\n";
    my $ttr = BMS::TableReader->new();
    my $stnd = $ttr->format($inFormat) || "";
    print &help('InputFormat').
        "<b>... Input format:</b><br /><select id='inputformat' name='inputformat'>\n";

    foreach my $ii (@inFmts) {
        my ($val, $tag) = @{$ii};
        printf("  <option value='%s'%s>%s</option>\n", $val, $val eq $stnd ?
               " selected='SELECTED'" : "",$tag);
    }
    print "</select><br />\n";
 print  &help('IsTable').
     "<b>... from table column: </b> ".
     "<input type='text' style='width:2em;' name='istable' value='$isTable' /><br />\n";
 print  &help('SaveTable').
     "<input type='checkbox' value='1' id='savetable' name='savetable' ".
     ($savedTable ? "checked='checked' " : '')."/> Merge results with input<br /><br />\n";
 

    print "</td><td valign='top'>";

    print "<input type='submit' style='background-color:lime; font-weight: bold; font-size: 1.5em;' value='Search' /><br />\n";
    print &help('AvailableFunctions')."<b>Action: </b><select id='mode' name='mode'>\n";
    foreach my $n (qw(Conversion Description Clouds NamespaceList TextLookup SimpleAnnotation Mapping)) {
        printf("  <option value='%s'%s>%s</option>\n", $n, ($cgiMode eq $n) ?
               " selected='SELECTED'" : "", $n);
    }
    print "</select><br />\n";

    print &help('OutputFormats')."<b>Output format: </b><select id='format' name='format'>\n";
    
    my $found = 0;
    foreach my $n (@defFmts) {
        my ($val, $tag) = @{$n};
        $found++ if ($fmt eq $val);
        printf("  <option value='%s'%s>%s</option>\n", $val, ($fmt eq $val) ?
               " selected='SELECTED'" : "", $tag);
    }
    unless ($found) {
        printf("  <option value='%s' SELECTED>%s</option>\n", $fmt, $fmt);
    }
    print "</select><br />\n";

    my @ns = ('', $ad->all_namespace_names());
    my $rns1 = $ad->namespace_name(($ns1 && ref($ns1)) ? '' : $ns1);
    my $rns2 = $ad->namespace_name($ns2);

    print &help('InputNameSpace').&namespace_html
        (\@ns, 'ns1', $rns1, "Query Namespace", "changeNs1('ns1','ns2')")."<br />\n";

 print "<input type='hidden' value='0' name='ignorespace' />\n";
    print &help('IgnoreCase') . sprintf
        ("<input type='checkbox' value='1' id='ignorecase' name='ignorecase' %s/> Ignore capitalization for case-sensitive queries (<i>eg gene symbols</i>)<br />\n",
         $igCase ? "checked='checked' " : '');

 print "<input type='hidden' value='0' name='keepspace' />\n";
 print &help('KeepSpaces') . sprintf
        ("<input type='checkbox' value='1' id='keepspace' name='keepspace' %s/> Preserve spaces in query terms<br />\n",
         $args->{KEEPSPACE} ? "checked='checked' " : '');

 print &help('CleanQueries') . "Queries are: ";
 foreach my $val (qw(Clean Dirty Integers)) {
     printf("<input type='radio' value='%s' id='%scleanquery' name='cleanquery' %s/>  %s",
         $val, $val, $val eq $cleanQry ? "checked='checked' " : '', $val);
 }
 print "<br />\n";

 print &help('OutputNameSpace').&namespace_html
     (\@ns, 'ns2', $targNs, "Output Namespace", "changeNs1('ns2','intns');colTxt('ns1','ns2')").
     "<span id='ns2ok' style='color:green;font-style:italic'></span><br />\n";

 print "<input type='hidden' value='0' name='assurenull' />\n";
 print &help('AssureNull') . sprintf
     ("<input type='checkbox' value='1' name='assurenull' id='assurenull' %s/> Include empty rows for input terms with no output<br />\n",
      $asn ? "checked='checked' " : '');

 # print "<input type='hidden' value='0' name='showsmiles' />\n";
 print &help('ShowSmiles') . "Compunds are shown as". sprintf
     (" <input type='radio' value='%s' id='mtcmpd' name='showsmiles' %s/>  %s",
      0, !$showSMI ? "checked='checked' " : '', 'MapTracker ID'). sprintf
      (" <input type='radio' value='%s' id='smicmpd' name='showsmiles' %s/>  %s",
       'smiles', $showSMI && !$showInchi ? "checked='checked' " : '', 'SMILES'). sprintf
       (" <input type='radio' value='%s' id='inchicmpd' name='showsmiles' %s/>  %s",
        'inchi', $showInchi ? "checked='checked' " : '', 'InChI')."<br />\n";
       

 print &help('AddDescription AddGeneSymbol AddSpecies AddLinks') .
     "Add additional columns describing output terms:<br />&nbsp;&nbsp;";
 
 print "<input type='hidden' value='0' name='adddesc' />\n";
 printf
     ("<input type='checkbox' value='1' id='adddesc' name='adddesc' %s/> Description\n", $args->{ADDDESC} ? "checked='checked' " : '');

 printf
     ("<input type='checkbox' value='1' id='addsym' name='addsym' %s/> Gene Symbol\n", $args->{ADDSYM} ? "checked='checked' " : '');

 printf
     ("<input type='checkbox' value='1' id='addtaxa' name='addtaxa' %s/> Species\n", $args->{ADDTAXA} ? "checked='checked' " : '');

 printf
     ("<input type='checkbox' value='1' id='addlink' name='addlink' %s/> Hyperlinks\n", $args->{ADDLINK} ? "checked='checked' " : '');
 printf
     ("<input type='checkbox' value='1' id='addseq' name='addseq' %s/> Sequence\n", $args->{ADDSEQ} ? "checked='checked' " : '');
 print "<br />\n";
 print "<input type='hidden' value='0' name='cleanchr' />\n";
    print &help('SimpleChromosomeNames') . sprintf
        ("<input type='checkbox' value='1' id='cleanchr' name='cleanchr' %s/> Use simplified chromosome names (remove species)<br />\n",
         $cleanChr ? "checked='checked' " : '');
    print &help('GenomeBuild') . 
        "<b>Genome Build: </b>";
    printf("<input type='text' value='%s' id='build' name='build' />",
           $args->{BUILD} || '');
 print " <i>Only needed when Mapping</i><br />\n";

    print &help('IntersectingQuery%20IntersectingNameSpace') . 
        "<b>Intersect With: </b>";
    printf("<input type='text' value='%s' id='intersection' name='intersection' />",
           $intersect || '');
    print &namespace_html(\@ns, 'intns', $intNS,'',"colTxt('ns2','intns')").
        "<span id='intnsok' style='color:green;font-style:italic'></span><br />\n";
    
    print &help('PopularIntersection') . 
        "<b>Popular Intersections: </b>";
    print "<select id='intnstxt' name='popularint' onchange='setPopInt(this)' >\n";
            print "  <option value=''></option>\n";
    foreach my $pi (@popularIntersections) {
        if ($pi =~ /^(\S+)\s*(.+)$/) {
            print "  <option value='$pi'>$2</option>\n";
        }
    }
    print "</select><br />\n";

    print &help('CustomColumns') . 
        "<b>Custom Column Order: </b>";
    printf("<input type='text' value='%s' name='custcol' id='custcol' style='width:40em;'/><br />",
           $colText || '');

    print &help('FlagGo') . 
        "<b>Highlight GO Classes: </b>";
    printf("<input type='text' value='%s' name='flaggo' id='flaggo' style='width:40em;'/><br />",
           $args->{FLAGGO} || '');

 print "<input type='hidden' value='0' name='addec' />\n";
    print &help('GoEvidence') . sprintf
        ("<input type='checkbox' value='1' name='addec' id='addec' %s/> Add evidence code to GO class score\n",
         $args->{ADDEC} ? "checked='checked' " : '');
    print &help('NoAuthority') . sprintf
        ("<input type='checkbox' value='1' name='noauth' id='noauth' %s/>&hellip; but not authority<br />\n",
         $args->{NOAUTH} ? "checked='checked' " : '');

    print &help('AgeFilter') . 
        "<b>Data Age: </b> ". sprintf
        ("<input type='text' value='%s' name='age' id='age' style='width:12em;'/><br />",
         defined $age ? $age : '');
    print &help('CloudAge') . 
        "<b>Cloud Age: </b> ". sprintf
        ("<input type='text' value='%s' name='cloudage' id='cloudage' style='width:12em;'/><br />",
         defined $cAge ? $cAge : '');
    
    print &help('Matched') . 
        "<b>Minimum Score: </b> ". sprintf
        ("<input type='text' value='%s' name='min' id='min' style='width:4em;'/> ",
         defined $minScore ? $minScore : '');
    print &help('FilterAuthor') . 
        "<b>Filter Authority: </b> ". sprintf
        ("<input type='text' value='%s' name='authority' id='authority' style='width:4em;'/><br />", $authReq);
    

    print "<input type='hidden' value='0' name='bestonly' />\n";
    print &help('BestOnly') . sprintf
        ("<input type='checkbox' value='1' name='bestonly' id='bestonly' %s/> Keep only the top scored matches for each query<br />\n",
         $bestOnly ? "checked='checked' " : '');
    print "<input type='hidden' value='0' name='aspercent' />\n";
    print &help('PercentScores') . sprintf
        ("<input type='checkbox' value='1' name='aspercent' id='aspercent' %s/> Represent scores as percentages (eg 98.93 rather than 0.9893)<br />\n",
         $fmtFlags{AsPercent} ? "checked='checked' " : '');

    print &help('SearchLimit') . 
        "<b>Limit Number of Results: </b> ". sprintf
        ("<input type='text' value='%s' name='limit' id='limit' style='width:4em;'/><br />",
         $limit || '');
    
    foreach my $nogui (qw(sort)) {
        # Some params lack a web UI, but may still be passed by URL
        my $val = $args->val($nogui);
        if (defined $val && !ref($val)) {
            print "<input type='hidden' value='$val' name='$nogui' />\n";
        }
    }

    print "<input type='submit' style='background-color:lime; font-weight: bold; font-size: 1.5em;' value='Search' />\n";
    
    print "</tr></tbody></table></form>\n";
    print "<script>changeNs1('ns1','ns2'); changeNs1('ns2','intns')</script>\n";
    print <<EOF;
<h3>Common Tasks - click a button to pre-configure the settings</h3>

 <button class='conf' onclick="cfConfigure( {
mode:         'Conversion',
format:       'Full HTML OntologyReport',
ns1:          '',
ns2:          'GO',
intersection: '',
intns:        '',
intnstxt:     '',
custcol:      '',
flaggo:       '',
addec:        0,
bestonly:     0,
ignorecase:   0,
limit:        '',
showsmiles:   0
} )">Gene Ontology Report</button> - Compact ontology tree for your queries<br />

 <button class='conf' onclick="cfConfigure( {
mode:         'Conversion',
format:       'Full HTML',
ns1:          '',
intersection: 'Homo sapiens',
ns2:          'LL',
intns:        'TAX',
custcol:      'termin(Query),termout(LocusLink),sym_out,tax_out,descr(Description)',
ignorecase:   1,
limit:        '',
bestonly:     1,
showsmiles:   0
} )">Gene Summary</button> - Distill your queries down to Loci with accompanying meta data<br />

 <button class='conf' onclick="cfConfigure( {
mode:         'SimpleAnnotation',
format:       'Excel',
addec:        1,
flaggo:       'GO:0003707,GO:0004672,GO:0004721,GO:0004842,GO:0004888,GO:0004930,GO:0005216,GO:0005576,GO:0006986,GO:0008233,GO:0016651,GO:0031012'
} )">Flag Target Class</button> /
<button class='conf' onclick="cfConfigure( {
mode:         'SimpleAnnotation',
format:       'Excel',
addec:        1,
flaggo:       'GO:0003707,GO:0004672,GO:0004721,GO:0004842,GO:0004888,GO:0004930,GO:0005216,GO:0005576,GO:0005615,GO:0031012,GO:0006986,GO:0008233,GO:0016651,GO:0003824,GO:0016298,GO:0005887,GO:0003774,GO:0005515,GO:0003700,GO:0005215,GO:0045298,GO:0016032'
} )">Extended Target Class</button> - Add Target Class GeneOntology Columns<br />

<button class='conf' onclick="cfConfigure( {
mode:         'SimpleAnnotation',
format:       'Excel',
ns1:          '',
addec:        1,
custcol:      'termin,sym,desc',
flaggo:       'GO:0005576,GO:0005886,GO:0005856,GO:0005737,GO:0005829,GO:0005768,GO:0005773,GO:0031982,GO:0005794,GO:0005783,GO:0005739,GO:0005634'
} )">Cellular compartment</button> - Add Sub-cellular localization GeneOntology Columns<br />

 <button class='conf' onclick="cfConfigure( {
mode:         'TextLookup',
format:       'Full HTML',
ns1:          '',
ns2:          '',
custcol:      '',
limit:        40
} )">Free Text Query</button> - Try to locate objects based on descriptive text. Focus on: 
 <button class='conf' onclick="cfConfigure( {
mode:         'TextLookup',
format:       'Full HTML',
ns1:          'AL',
ns2:          '',
custcol:      '',
limit:        40
} )">Genes</button>
 <button class='conf' onclick="cfConfigure( {
mode:         'TextLookup',
format:       'Full HTML',
ns1:          'ONT',
ns2:          '',
custcol:      '',
limit:        40
} )">Ontologies</button>
 <button class='conf' onclick="cfConfigure( {
mode:         'TextLookup',
format:       'Full HTML',
ns1:          'PMID',
ns2:          '',
custcol:      '',
limit:        40
} )">PubMed</button>

<br />

 <button class='conf' onclick="cfConfigure( {
mode:         'Conversion',
format:       'Full HTML',
ns1:          'AC',
ns2:          'SMI',
intersection: '',
intns:        '',
intnstxt:     '',
custcol:      'termin,matched,auth,termout,desc',
flaggo:       '',
addec:        0,
keepspace:    1,
showsmiles:   1
} )">Compound names to SMILES</button> - Find canonical SMILES compound descriptors for free-text chemical names<br />

 <button class='conf' onclick="cfConfigure( {
mode:         'Mapping',
format:       'Excel',
ns1:          '',
ns2:          '',
intersection: '',
intns:        '',
intnstxt:     '',
custcol:      'termin(Query),termout(Chr),startout(ChrStart),endout(ChrEnd),strand(Strand),matched(Score),build(GenomeBuild)',
flaggo:       '',
addec:        0,
keepspace:    0,
cleanchr:     1,
showsmiles:   0
} )">Genomic Mapping</button> - Find genomic locations for your queries<br />

<button class='conf' onclick="cfConfigure( {
mode:         'Conversion',
format:       'Full HTML',
ns1:          'SYM',
ns2:          'LL',
intersection: 'Homo sapiens',
intns:        'TAX',
intnstxt:     '',
custcol:      'term_in(Your Symbol),term_out(LocusLink),sym(Best Symbol),matched,desc(Locus Description)',
flaggo:       '',
addec:        0,
age:          '$safe',
cloudage:     '$safe',
min:          '',
bestonly:     1,
ignorecase:   1,
limit:        '',
assurenull:   1,
showsmiles:   0
} )">Symbol to Locus</button> - Map gene symbols to loci. Be sure to choose the correct species under "Intersect With"<br />


 <button class='conf' onclick="cfConfigure( {
mode:         'Conversion',
format:       'Full HTML Structured',
ns1:          '',
ns2:          '',
intersection: '',
intns:        '',
intnstxt:     '',
custcol:      '',
flaggo:       '',
addec:        0,
age:          '$safe',
cloudage:     '$safe',
min:          '',
bestonly:     0,
ignorecase:   0,
limit:        '',
assurenull:   1,
showsmiles:   0
} )">RESET</button> - Reset all parameters (except your queries) to default<br />

<h4>The presets will simply configure the interface for you, you will still need to click 'Search' to begin analysis</h4>

EOF

    print "</body></html>\n";
    $ad->bench_end;
}

sub prebranch {
    print "<pre>".$args->branch(@_)."</pre>\n";
}

sub preprint {
    print "<pre>".join("\t", @_)."</pre>\n";
}


sub namespace_html {
    $ad->bench_start;
    my ($list, $param, $chosen, $title, $func) = @_;
    my $txt = "";
    $txt  = sprintf("<b id='%stxt'>%s:</b> ",$param, $title) if ($title);
    my @choice = ref($chosen) ? @{$chosen} : ($chosen);
    # &prebranch(\@choice);
    if ($#choice > 0) {
        $txt .= "<br />\n";
        $txt .= sprintf("<textarea cols='40' rows='5' name='%s' id='%s'>",
                        $param, $param);
        $txt .=join("\n", map { $ad->namespace_name($_) } @choice);
        $txt .= "</textarea>\n";
    } else {
        $chosen = $ad->namespace_token($choice[0]);
        $txt .= sprintf("<select name='%s' id='%s'%s>\n", $param, $param, $func ?
                        " onchange=\"$func\"" : '');
        foreach my $n (@{$list}) {
            my $tok = $ad->namespace_token($n);
            my $show = $n || ($param eq 'ns1' ? 'Automatic (Guess from Query)' : 
                              '');
            $txt .= sprintf
                ("  <option value='%s'%s>%s</option>\n", $tok, 
                 $tok eq $chosen ? ' SELECTED' : '', $show );
        }
        $txt .= "</select>";
    }
    $ad->bench_end;
    return $txt;
}

sub rank_for_list {
    $ad->bench_start;
    my ($list) = @_;
    if ($rankCG) {
        # Take the average instance
        my $sum = 0; map { $sum += $_ } @{$list};
        return int(0.5 + ($sum / ($#{$list} + 1)));
    }
    # Use the first instance
    my ($rv) = sort {$a <=> $b} @{$list};
    $ad->bench_end;
    return $rv;
}

sub help { return $args->tiddly_link( @_); }

sub onebox {
    my ($q) = @_;
    &finish_up unless ($q);
    $ad     ||= &getAccessDenorm();
    print "Content-type: text/xml\n\n";
    print qq(<?xml version="1.0" encoding="UTF-8"?>\n<OneBoxResults>\n);
    my %hits;
    my %words;
    foreach my $word (split(/\s+/, $q)) {
        $words{uc($word)} = 1 if (length($word) > 3);
        my $ns = $ad->guess_namespace($word);
        $ns ||= $ad->guess_namespace_from_db($word, 20);
        $ns ||= $ad->guess_namespace_from_db(uc($word), 20);
        next unless ($ns);
        my ($id, $seq) = $ad->standardize_id($word, $ns);
        ($id, $seq) = $ad->standardize_id(uc($word), $ns) unless ($seq);
        $hits{$ad->namespace_name($ns)}{$id}++ if ($seq);
    }
    
    my @nss = sort keys %hits;
    map { $hits{$_} = [ sort keys %{$hits{$_}} ] } @nss;
    my $count = 0; map { $count += $#{$hits{$_}} + 1 } @nss;
    my $script = $ENV{SCRIPT_NAME} || "/biocgi/genacc_service.pl";
    my $host   = "bioinformatics.bms.com";
    printf("  <provider hits='%d'>MapTracker GenAcc Service</provider>\n",
           $count);
    print "  <title>\n";
    printf("    <urlText>%d ID%s recognized by MapTracker</urlText>\n", 
           $count, $count == 1 ? '' :'s');
    print "    <urlLink>http://$host/$script</urlLink>\n";

    print "  </title>\n";
    print "  <IMAGE_SOURCE>http://bioinformatics.bms.com/biohtml/images/Platter40.png</IMAGE_SOURCE>\n";
    my %esc = ( '>' => 'gt',
                '&' => 'amp',
                '<' => 'lt', );
                
    if ($#nss > -1) {
        foreach my $ns (@nss) {
            my $tok = $ad->namespace_token($ns);
            foreach my $id (@{$hits{$ns}}) {
                my $desc = &_get_desc( $id, $ns ) || '';
                $desc = substr($desc, 0, 80) . '...'
                    if (length($desc) > 80);
                my %data = ( Namespace => $ns,
                             ID => $id,
                             Description => $desc );
                print "  <MODULE_RESULT>\n";
                foreach my $name (&fancySort( [keys %data])) {
                    while (my ($in, $out) = each %esc) {
                        $data{$name} =~ s/\Q$in\E/&$out;/g;
                    }
                    my $val = $data{$name};
                    printf('    <Field name="%s">%s</Field>'."\n", $name, $val)
                        if ($val);
                }
                print "    <U>http://bioinformatics.bms.com/biocgi/tilfordc/working/maptracker/MapTracker/genacc_service.pl?ns=$tok&amp;format=fullhtmlstruct&amp;queries=$data{ID}</U>\n";
                print "  </MODULE_RESULT>\n";
            }
        }
    }
    print "</OneBoxResults>\n";
    &finish_up;
}

sub desc_for_node {
    my ($node, $tag, $fo) = @_;
    my $desc = &_get_desc( $node );
    return $desc || '';
}
sub parents_for_node {
    my ($node, $tag, $fo) = @_;
    my $ns = $ad->guess_namespace($node);
    my @pars;
    if ($ns =~/^(CDD|IPR|PMID)$/) {
        my $nsn = $ad->namespace_name($ns);
        @pars = ($nsn);
    } else {
        @pars = $ad->direct_genealogy($node, -1, $ns, $doWarn);
        #$args->msg("$node has ".scalar(@pars)." parents");
    }
    return \@pars;
}

sub details_for_node {
    my ($node, $tag, $fo) = @_;
    my $html = "";
    if (my $stuff = $fo->node_param('stuff', $node)) {
        foreach my $s (@{$stuff}) {
            my ($m, $auths, $src) = @{$s};
            my $c  = &class4score( $m ) || 'm3';
            my $sc = (defined $m) ? sprintf("%.3f", $m) : 'n/a';
            my @bits = ("","Score <b class='$c'>$sc</b>");
            if ($src && $#{$src} != -1) {
                my $stxt;
                my $num = $#{$src} + 1;
                if ($num > 5) {
                    $stxt = "$num queries";
                } else {
                    $stxt = join(', ', @{$src});
                }
                push @bits, "<i>Associated with $stxt</i>";
            }
            push @bits, "<i class='auth'>".join
                ("<br />", split(', ',$auths))."</i>";
            $html .= join("<br />", @bits);
        }
    }
    my $p = $fo->parents($node);
    my $c = $fo->children($node);
    my %lineage = ( Parents  => $p,
                    Children => $c, );
    foreach my $rt (sort keys %lineage) {
        my @rels = @{$lineage{$rt}};
        next if ($#rels < 0);
        $html .= "<div><b style='color:blue'>[$rt]</b>";
        foreach my $rel (@rels) {
            my $cl = $fo->node_param('class', $rel);
            my $rd = $fo->node_param('desc', $rel);
            $rel   = "<b class='$cl'>$rel</b>" if ($cl);
            $html .= "<br />$rel";
            $html .= " <i class='fo_desc'>$rd</i>" if ($rd);
        }
        $html .= "</div>";
    }
    return $html;
}

sub network {
    
}

# Color scheme from:
# http://www.personal.psu.edu/faculty/c/a/cab38/ColorBrewer/ColorBrewer.html
my $gvKey;
sub graphviz {
    $ad->bench_start;
    my ($edges, $ids) = @_;
    my $dot = "";
    my $qfrm = '  "%s" [color="#ccffff",peripheries="2",style="filled,setlinewidth(2)"];'."\n";
    my $nfrm = '  "%s" [fontcolor="%s"];'."\n";
    
    my $efrm = '  "%s" -- "%s" [color="#%s",style="setlinewidth(3),%s"];';
    my @ks = qw(term_in term_out matched ns_in ns_out);
    my %nss;
    # Record the edges
    my %usedCol;
    my $eDot = "";
    my $igAny = $args->{NOANY};
    my $igTr  = $args->{NOTREMBL};
    my %isQuery = map { $_ => 1 } @{$ids};

    foreach my $edge (@{$edges}) {
        my ($n1, $n2, $sc, $ns1, $ns2) = map { $edge->{$_} } @ks;
        $nss{$n1} = $ns1; 
        $nss{$n2} = $ns2;
        my $sty   = $ad->is_canonical($ns1, $ns2) ? 'solid' : 'dashed';
        my $ind   = defined $sc ? 1+ int(10 * $sc) : 0;
        $usedCol{$ind}++;
        $eDot .= sprintf($efrm,$n1, $n2, $edgeCols[$ind], $sty)."\n";
    }

    my %nodeDef;
    my %qprops = ( color => "#ccffff", peripheries => 2,
                   style => "filled,setlinewidth(2)" );
    # Note the query IDs
    foreach my $id (@{$ids}) {
        $nodeDef{$id} = { %qprops };
    }
    $dot .= "\n";
    $dot .= "$eDot\n";

    my @nsCols = ( ['AL', 'Gene',    { fontcolor => '#336600',
                                       fontsize  => 10,
                                       color     => '#336600', } ],
                   ['AR', 'RNA',     { fontcolor => '#cc0033' } ],
                   ['AP', 'Protein', { fontcolor => '#ff33ff' } ],
                   ['',   'Unknown', { fontcolor => '#000000' } ] );

    # Note all other IDs
    while (my ($id, $ns) = each %nss) {
        my $col;
        for my $c (0..2) {
            if ($ad->is_namespace($ns, $nsCols[$c][0])) {
                $col = $c;
                last;
            }
        }
        next unless (defined $col);
        my @xtra;
        if ($nsCols[$col][1] eq 'Gene' || $cloudType =~ /orth/i) {
            # Include a gene symbol
            my $hash = $ad->bulk_best_symbol
                ( -id    => $id,
                  -ns    => $ns,
                  -trunc => 1,
                  -best  => 1,
                  -short => 1,
                  -warn  => 1,
                  -explainsql => $exSql );
            my %uniq = map { $_ => 1 } map { @{$_} } values %{$hash};
            my $sym = join(',', sort keys %uniq);
            push @xtra, $sym if ($sym);
        }
        if ($cloudType =~ /orth/i) {
            my @taxa = $ad->convert
                ( -id => $id, -ns1 => $ns, -ns2 => 'TAX');
            if ($#taxa == 0) {
                my @bits = split(/\s+/, $taxa[0]);
                push @xtra, "(".join('', map {substr($_,0,1)} @bits).")";
            }
        }
        $nodeDef{$id}{label} = "$id\\n".join(' ', @xtra) unless ($#xtra == -1);
        while (my ($prop, $val) = each %{$nsCols[$col][2]}) {
            $nodeDef{$id}{$prop} ||= $val;
        }
    }

    # Make note of each node:
    foreach my $id (sort keys %nodeDef) {
        my @pbits;
        foreach my $prop (sort keys %{$nodeDef{$id}}) {
            push @pbits, sprintf('%s="%s"', $prop, $nodeDef{$id}{$prop});
        }
        $dot .= "  \"$id\"";
        $dot .= " [".join(',', @pbits)."]" unless ($#pbits == -1);
        $dot .= ";\n";
    }

    $dot = <<EOF;
graph seq_edges {
  // GraphViz specification - see:
  // http://www.graphviz.org/
  // for more information on GraphViz!

  // Default Global Parameters:
  edge [fontname="helvetica", fontsize=9, weight=2, style="setlinewidth(3)"];
  graph [maxiter=1000, overlap="false",splines="true",epsilon=0.0001,packmode="graph",start="regular"];
  node [fontname="helvetica", fontsize=8, height=0.15, shape="box", style="setlinewidth(2)"];

$dot}
EOF

    my $base  = "$tmpdir/GenAccCloud_$parPid";
    my $tfile = "$base.dot";
    my $pfile = "$base.png";

    open(TFILE, ">$tfile") ||
        $args->death("Failed to write GraphViz file", $tfile, $!);
    print TFILE $dot;
    close TFILE;
    chmod(0777, $tfile);

    system("/stf/sys/bin/neato -Tpng -o $pfile $tfile");
    chmod(0777, $pfile);
    my $url = $pfile;
    $url =~ s/\/stf/http:\/\/bioinformatics.bms.com/;

    my $html = "<img src='$url' />\n";
    my $gvKey = "<b>Nodes:</b>";
    foreach my $dat (@nsCols) {
        $gvKey .= sprintf(" <span style='padding:0em 1em;color:%s;border:solid black 2px'>%s</span>", $dat->[2]{fontcolor}, $dat->[1]);
    }
    my @ignore;
    push @ignore, 'generic' if ($igAny);
    push @ignore, 'TrEMBL' if ($igTr);
    $gvKey .= "\n<i>Ignoring ".join(' and ', @ignore)." identifiers</i>\n"
        unless ($#ignore == -1);
    unless ($paramFailed) {
        foreach my $pair (@passedParams) {
            my ($key, $val) = @{$pair};
            next if ($key =~ /^(ids|noany|notrembl)$/);
            $gvKey .= "  <input type='hidden' name='$key' value='$val' />\n";
        }
        $gvKey .= " <i><b>Another ID:</b></i>";
        $gvKey .= " <input style='background-color:#fc9' type='text' style='width:20em;' name='ids' />\n";
        $gvKey .= " <b>No AR</b>:<input type='checkbox' value='1' name='noany'".($igAny ? " checked='checked'":'')." />\n";
        $gvKey .= " <b>No TR</b>:<input type='checkbox' value='1' name='notrembl'".($igTr ? " checked='checked'":'')." />\n";
    }
    
    $gvKey .= "<br />\n<b>Edge Score:";
    my $done = 0;
    for my $ind (0..$#edgeCols) {
        next unless ($usedCol{$ind});
        my $sc = 'Unknown';
        if ($ind) {
            $sc = ($ind-1) * 10;
            $sc .= '-' . ($sc+9) unless ($sc == 100);
            $sc .= '%';
        }
        $gvKey .= ' |' if ($done++);
        $gvKey .= sprintf(" <span style='color:#%s'>%s</span>", $edgeCols[$ind], $sc);
    }
    $gvKey .= "</b>";
    if ($useOracle && $#clouds != -1 ) {
        $gvKey .= sprintf(" - <a target='_blank' href='http://otb.pri.bms.com/otb.lisp?func=btable&nfilter=1&server=mtrkp1&user=GENACC&table=CLOUD&pagesize=500&filtcol1=CLOUD_ID&filtmode1=IN&filtval1=%s'>%d Cloud%s in OTB</a>", join(' ', map { $_->cloud_id } @clouds), $#clouds + 1, $#clouds == 0 ? '' : 's');
    }
    $gvKey .= "<br />\n";
    $gvKey = "<form method='post'>$gvKey</form>" unless ($paramFailed);
    
    $ad->bench_end;
    return $gvKey . $html;
}


sub fancySort {
    my ($list, $ind, $metric, $shutUp) = @_;
    my $rn = $#{$list} + 1;
    my $msg = "$rn rows";
    $msg   .= " by $metric [$ind]" if ($metric);
    my $nullVal = "ZZZZZ";
    if ($fastSort && $rn >= $fastSort) {
        $ad->bench_start('FastSort');
        my @rv;
        $args->msg("Fast sorting $msg") if ($doWarn);
        if (defined $ind) {
            @rv = sort { ($a->[$ind] || $nullVal) cmp ($b->[$ind] || $nullVal) } @{$list};
        } else {
            @rv = sort { ($a || $nullVal) cmp ($b || $nullVal) } @{$list};
        }
        $ad->bench_end('FastSort');
        return wantarray ? @rv : \@rv;
    }
    $args->msg("Sorting $msg") if ($doWarn && !$shutUp);

    $ad->bench_start('Cluster');
    # Clustering allows regular expressions to be applied only once for
    # each unique sort term.
    my %cluster;
    if (defined $ind) {
        map { push @{$cluster{ defined $_->[$ind] ? $_->[$ind] : '' }}, $_ }
        @{$list};
    } else {
        map { push @{$cluster{ defined $_ ? $_ : '' }}, $_ } @{$list};
    }
    $ad->bench_end('Cluster');
    $ad->bench_start('Organize');
    # Organizing allows the three sort modes to be broken out into
    # separate structures.
    my (%byNum, %byBoth, %simple);
    while (my ($mem, $items) = each %cluster) {
        if ($mem =~ /^(\-?\d+|\-?\d*\.\d+)$/) {
            # Pure number
            push @{$byNum{$1}{""}}, @{$items};
        } elsif ($mem =~ /^([^\d]+?)(\d+)$/) {
            # Trailing numbers - sort by non-numeric, then numeric
            push @{$byBoth{uc($1)}{$2}{uc($mem)}}, @{$items};
        } elsif ($mem =~ /^(\d+)(.+?)/) {
            # Leading numbers - sort by number only
            push @{$byNum{$1}{$mem}}, @{$items};
        } else {
            $mem ||= $nullVal;
            push @{$byBoth{uc($mem)}{0}{uc($mem)}}, @{$items};
            # push @{$simple{$mem}}, @{$items};
        }
    }
    undef %cluster;
    $ad->bench_end('Organize');

    $ad->bench_start('Sort');
    # Sort each type individually
    # Doing this to try to limit the massive memory bloat seen when a
    # monolithic sort structure was used.
    my @rv;
    foreach my $num (sort { $a <=> $b } keys %byNum) {
        my $hash1 = $byNum{$num};
        foreach my $mem (sort {uc($a) cmp uc($b)} keys %{$hash1}) {
            push @rv, @{$hash1->{$mem}};
        }
    }
    undef %byNum;

    foreach my $alpha (sort {$a cmp $b} keys %byBoth) {
        my $hash1 = $byBoth{$alpha};
        foreach my $num (sort { $a <=> $b } keys %{$hash1}) {
            my $hash2 = $hash1->{$num};
            foreach my $mem (sort {$a cmp $b} keys %{$hash2}) {
                push @rv, @{$hash2->{$mem}};
            }
        }
    }
    undef %byBoth;

    foreach my $mem (sort {uc($a) cmp uc($b)} keys %simple) {
        push @rv, @{$simple{$mem}};
    }
    undef %simple; # Not needed, but will assoc benchmark time to undef here.
    $ad->bench_end('Sort');
    return wantarray ? @rv : \@rv;
}

sub standard_head {
    my $fh = shift || $outFH;
    $ad->bench_start;
    print $fh "<html>\n <head>\n";
    print $fh '  <link rel="shortcut icon" href="/biohtml/images/Platter.png">';
    my @params;
    my %using = map { uc($_) => $args->val($_) } $args->all_keys(-nodef => 1);
    $using{FORMAT} = $fmt  if ($using{FORMAT});
    $using{MODE}   = $mode if ($using{MODE});
    delete $using{NOCGI};
    if ($#queries <= 50) {
        map { delete $using{uc($_)} } @idKeys;
        $using{IDS} = join('%09', @queries);
    }
    my @pairs = map { [ lc($_), $using{$_} ] } keys %using;
    while (my $pair = shift @pairs) {
        my ($key, $val) = @{$pair};
        if (ref($val)) {
            if (ref($val) eq 'ARRAY') {
                # Map the array to a linear expansion
                push @pairs, map { [$key, $_] } @{$val};
            } elsif (uc($key) eq $key) {
                # Failed to map over the parameters
                $paramFailed = 1;
                $ad->bench_end;
                return;
            }
        } else {
            push @passedParams, [$key, $val];
            $val =~ s/\'/\%27/g;
            $val =~ s/\"/\%22/g;
            push @params, "$key=$val";
        }
    }
    my $title;
    my $num = $#uniqQ + 1;
    if ($num) {
        my $what;
        if ($num > 3) {
            my $ns = $ad->namespace_name($ns1) || 'ID';
            $what  = "$num ${ns}s";
        } else {
            $what = join(" + ", @uniqQ);
        }
        if ($mode eq 'convert') {
            $title = "Conversion of $what";
            my $ns = $ad->namespace_name($ns2);
            $title .= " to $ns" if ($ns);
        } elsif ($mode eq 'assign') {
            $title = "Assignments for $what";
            my $ns = $ad->namespace_name($ns2);
            $title .= " in $ns" if ($ns);
        } elsif ($mode eq 'cloud') {
            $title = (&stnd_cloudType($cloudType) || "Unknown").
                " cloud for $what";
        } elsif ($mode eq 'desc') {
            $title = "Descriptions for $what";
        }
    } else {
        $title = "GenAcc ID Conversion Home";
    }
    print $fh " <title>$title</title>\n" if ($title);
    print $fh " <script src='/biohtml/javascript/taggr.js'></script>\n";
    if ($#params == -1) {
        push @params, "home=1";
    }
    my $opts = join('&amp;', @params);
    print $fh " <meta name='url' content='$opts' />\n"
        if (length($opts) < 500);
    $ad->bench_end;
}

sub stnd_cloudType {
    my $type = shift;
    if ($type =~ /prot/i) {
        $type = "ProteinCluster";
    } elsif ($type =~ /(rna|trans)/i) {
        $type = "TranscriptCluster";
    } elsif ($type =~ /(gene|loc)/i) {
        $type = "GeneCluster";
    } elsif ($type =~ /(orth)/i) {
        $type = "OrthologCluster";
    } else {
        $type = "";
    }
    return $type;
}

sub get_overlaps {
    my ($qid, $narr, $okInt, $nsInt) = @_;
    my @locs;
    foreach my $n1 (@{$narr}) {
        push @locs, $ad->genomic_overlap( -id => $qid,
                                          -ns => $n1,
                                          @comArgs );
    }

    $ad->bench_start('Organize Loci');
    # Both the query and subject may be duplicated in a small space
    my %distances;
    my $added = 0;
    foreach my $loc (@locs) {
        my ($subj, $qdats, $sdats, $build, $sns) = @{$loc};
        # Calculate the 'nearest' distance
        my (@sRange, @qRange);
        foreach my $sd (@{$sdats}) {
            if ($precise) {
                foreach my $hsp (split(/\,/, $sd->[6])) {
                    if ($hsp =~ /^(\d+)\.\.(\d+)$/ ||
                        $hsp =~ /^(\d+)$/) {
                        push @sRange, [$sd, $1, $2 || $1];
                    } else {
                        $args->err("Failed to parse HSP '$hsp' from $sd->[6]", "$sd->[0] vs $qid");
                    }
                }
            } else {
                my ($sid, $ss, $se) = @{$sd};
                push @sRange, [$sd, $ss, $se];
            }
        }
        foreach my $qd (@{$qdats}) {
            if ($precise) {
                # Intersect at the HSP level
                foreach my $hsp (split(/\,/, $qd->[4])) {
                    if ($hsp =~ /^(\d+)\.\.(\d+)$/ ||
                        $hsp =~ /^(\d+)$/) {
                        push @qRange, [$qd, $1, $2 || $1];
                    } else {
                        $args->err("Failed to parse HSP '$hsp' from $qd->[4]", "$subj : $qid");
                    }
                }
            } else {
                # Intersect across the whole range
                my ($qs, $qe) = @{$qd};
                push @qRange, [$qd, $qs, $qe];
            }
        }
        foreach my $sD (@sRange) {
            my ($sd, $ss, $se) = @{$sD};
            my $sid = $sd->[0];
            my @passing;
            foreach my $qD (@qRange) {
                my ($qd, $qs, $qe) = @{$qD};
                my $dist = 0;
                if ($qs > $se) {
                    # The object is to the "left" of the query
                    $dist = $se - $qs;
                    # If we are generating precise distances, the object
                    # could still in fact be overlapping, just not on HSPs
                    # If so, make the value positive and put an explicit
                    # plus sign on it:
                    $dist = '+' . abs($dist) if ($qd->[0] < $se);
                } elsif ($ss > $qe) {
                    # The object is to the "right" of the query
                    $dist = $ss - $qe;
                    # As above, note internal non-overlapping HSPs with +
                    $dist = '+' . $dist if ($ss < $qd->[1]);
                }
                # Precise internal HSP calculations would not have been
                # applied in SQL, so we need to re-check here.
                next if ($precise && abs($dist) > $maxDist);
                push @passing, [$dist, $qd, $sd, $subj, $build, $qs, $qe];
            }
            if ($okInt) {
                # User wants the results to only be in certain intersecting
                # sets;
                my $sns  = $ns2 ? $ns2 : $ad->guess_namespace($sid);
                my @sets = $ad->cached_conversion
                    ( -id => $sid, -ns1 => $sns, -ns2 => $nsInt );
                my $isOk = 0;
                foreach my $set (@sets) {
                    if (exists $okInt->{$set} && $okInt->{$set}) {
                        $isOk = 1;
                        last;
                    }
                }
                next unless ($isOk);
            }
            next if ($#passing == -1);
            $added += $#passing + 1;
            push @{$distances{$sid}}, @passing;
        }
    }
    $ad->bench_end('Organize Loci');

    #warn $args->branch(\%distances);
    # If no positions were found at all, or were removed during
    # 'precise' filtration, then add an empty row, if requested
    my @rv;
    if ($kn && !$added) { 
        my @nullRow = ($qid, map { '' } (1..12));
        if ($specialCol{qry_src}) {
            my %qLocs;
            foreach my $loc (@locs) {
                my $subj = $loc->[0];
                if ($cleanChr && $subj =~ /^[^\.]+\.[^\.]+\.(.+)$/) {
                    $subj = $1;
                }
                my $sid = join('.', $subj, $loc->[3]);
                foreach my $qd (@{$loc->[1]}) {
                    $qLocs{ sprintf("%s:%d-%d", $sid,$qd->[0],$qd->[1]) } = 1;
                }
            }
            my @ql = sort keys %qLocs;
            push @ql, "" if ($#ql == -1);
            push @rv, map { [ @nullRow, $_ ] } @ql;
        } else {
            push @rv, \@nullRow;
        }
    } else {
        my @subIDs = keys %distances;
        unless ($args->val('keepdup')) {
            # We will only keep the closest entry for each subject
            foreach my $sid (@subIDs) {
                my $allDats = $distances{$sid};
                next unless ($#{$allDats} > 0);
                my ($closest) = sort {
                    abs($a->[0]) <=> abs($b->[0]) } @{$allDats};
                $distances{$sid} = [ $closest ];
            }
        }
        my $keepNear = $args->val('nearest');
        if ($keepNear && $#subIDs != -1) {
            $ad->bench_start('Filter for Nearest');
            # We are only keeping the nearest subjects
            my %byDist;
            foreach my $subHits (values %distances) {
                foreach my $dat (@{$subHits}) {
                    push @{$byDist{abs($dat->[0])}}, $dat;
                }
            }
            my @kept;
            my @sorted = sort { $a <=> $b } keys %byDist;
            my $nearest = shift @sorted;
            push @kept, @{$byDist{$nearest}};
            if ($nearest && $keepNear =~ /flank/i) {
                # The nearest object is not overlapping, and we have requested
                # that both flanks be recovered
                my %needed = ( '+' => 1, '-' => 1);
                map { delete $needed{ $_->[0] < 0 ? '-' : '+' } } @kept;
                my @need = keys %needed;
                if ($#need == -1) {
                    # Managed to get both left and right at identical distances
                } elsif ($#need == 1) {
                    $args->msg("[!]","-keepnearest 'flank' is failing to identify which side of query is still needed");
                } else {
                    my $needCB = $need[0] eq '+' ? sub {
                        my $dat = shift;
                        return $dat->[0] > 0 ? 1 : 0;
                    } : sub {
                        my $dat = shift;
                        return $dat->[0] < 0 ? 1 : 0;
                    };
                    while (my $dist = shift @sorted) {
                        my $found = 0;
                        foreach my $dat (@{$byDist{$dist}}) {
                            if (&{$needCB}( $dat )) {
                                # This is what we want
                                $found++;
                                push @kept, $dat;
                            }
                        }
                        last if ($found);
                    }
                }
            }
            %distances = ();
            foreach my $k (@kept) {
                push @{$distances{$k->[2][0]}}, $k;
            }
            $ad->bench_end('Filter for Nearest');
        }
        foreach my $subHits (values %distances) {
            foreach my $dat (@{$subHits}) {
                my ($dist, $qd, $sd, $subj, $build, $qss, $qse) = @{$dat};
                my ($sid, $ss, $se, $sstr, $ssc)    = @{$sd};
                my $howBad = $sd->[7];
                my ($qs, $qe, $qstr, $qsc)          = @{$qd};
                if ($cleanChr && $subj =~ /^[^\.]+\.[^\.]+\.(.+)$/) {
                    $subj = $1;
                }
                my @res = ($qid, $sid, $subj, $qsc, $ssc, 
                           $qstr * $sstr, $dist,
                           $qs, $qe, $ss, $se, $build, $howBad);
                if ($specialCol{qry_src}) {
                    # Want to also include the coordinates of the query
                    push @res, sprintf
                        ("%s.%s:%d-%d", $subj, $build, $qss, $qse);
                }
                push @rv, \@res;
            }
        }
    }
    return wantarray ? @rv : \@rv;
}

sub overlap_int_params {
    my $okInt = $ad->list_from_request( $intersect );
    my $nsInt;
    if ($#{$okInt} != -1) {
        # Guess namespace unless it has been provided:
        $nsInt = $intNS ? $intNS : $ad->guess_namespace( $okInt->[0] );
        if ($nsInt) {
            # Convert array to lookup hash:
            my @stnd;
            foreach my $id (@{$okInt}) {
                my $std = &clean_query( $id, $nsInt);
                if ($std) {
                    push @stnd, $std;
                } else {
                    $args->err("Failed to standardize putative '$nsInt' accession '$id'");
                }
            }
            $okInt = { map { $_ => 1 } @stnd };
        } else {
            $args->err("Failed to guess intersecting namespace from ".
                       join(" + ", @{$okInt}));
            $okInt = undef;
        }
    } else {
        $okInt = undef;
    }
    return ($okInt, $nsInt);
}

sub fork_it {
    return undef unless
        ($mode =~ /^(convert|assign|desc|children|map|overlap|parents|simple)$/);
    $ad->bench_start;
    $benchFile = $args->val(qw(benchfile));
    if ($benchFile || $doBench) {
        $benchFile ||= "GA-$$.benchmark";
        unlink($benchFile);
    }

    $fc = BMS::ForkCritter->new
        ( -init_meth   => \&initialize,
          -finish_meth => \&finish,
          -limit       => $args->{LIMIT},
          -progress    => $args->{PROGRESS},
          -verbose     => $args->{VERBOSE} );

    $fc->{BENCHFILE} = $benchFile;

    my $nss = {};
    if ($oneNs1) {
        $nss->{$ns1} = \@uniqQ;
    } elsif ($ns1) {
        $nss = $ns1;
    } else {
        $nss->{""} = \@uniqQ;
        $ns1 = "";
        # Namespace guessing can really slow down if it is done here
        # Code modified 2013-05-03 to do this on-the-fly while forking
        #foreach my $id (@uniqQ) {
        #    my $ns = $ad->guess_namespace($id) ||
        #        $ad->guess_namespace_from_db($id, 50) || '';
        #    push @{$nss->{$ns}}, $id if ($ns);
        #}
    }
    my ($fMeth, $forkFile, $descFile, @extraTasks);

    my $ffName = $args->val(qw(forkfile)) || 1;

    $args->msg(sprintf("%d forks against %d IDs", $fork, $#uniqQ + 1))
        if ($doWarn);
    if ($mode eq 'convert') {
        $fMeth = \&fork_convert;
        $forkFile = $ffName;
    } elsif ($mode eq 'assign') {
        $fMeth = \&fork_assignments;
    } elsif ($mode eq 'map') {
        $fMeth = \&fork_map;
        $forkFile = $ffName;
    } elsif ($mode eq 'overlap') {
        $fMeth = \&fork_overlap;
        ($fc->{AD_OKINT}, $fc->{AD_NSINT}) = &overlap_int_params();

        $forkFile = $ffName;
    } elsif ($mode eq 'desc') {
        $fMeth = \&fork_description;
        $forkFile = $ffName;
    } elsif ($mode eq 'children') {
        $fMeth = \&fork_children;
    } elsif ($mode eq 'parents') {
        $fMeth = \&fork_parents;
    } elsif ($mode eq 'simple') {
        $fMeth = \&fork_simple;
    } else {
        $args->death("No forking mechanism for mode '$mode'");
    }

    $forkFile = 0 if ($fmtFlags{Null} || $doPop);
    if ($forkFile) {
        $forkFile = "ForkedRows-$parPid.tsv" if ($forkFile eq '1');
        unlink($forkFile);
        $fc->method( sub {
            my $rows = &{$fMeth}( @_ );
            $fc->write_output('rowFile', join
                              ("\n", map { join("\t", map { defined $_ ? $_ : '' } @{$_}) } @{$rows})."\n")
                unless ($#{$rows} == -1);
        } );
        if ($specialCol{desc_in} || $specialCol{desc_out}) {
            $descFile = $forkFile . ".desc";
            unlink($descFile);
        }
    } else {
        $fc->method( $fMeth );
    }

    push @extraTasks, sub {
        my ($rv) = @_;
        map { &fork_extra_convert( $_, &{$fc->{AD_EFFNS}}( $_ ), 'SET') } @{$rv};
    } if ($specialCol{set});
    push @extraTasks, sub {
        my ($rv) = @_;
        map { &fork_extra_convert( $_, &{$fc->{AD_EFFNS}}( $_ ), 'HG') } @{$rv};
    } if ($specialCol{specificity});
    push @extraTasks, sub {
        my ($rv) = @_;
        map { &fork_extra_convert( $_, &{$fc->{AD_EFFNS}}( $_ ), 'GO') } @{$rv};
    } if ($specialCol{flag_go});
    push @extraTasks, sub {
        my ($rv) = @_;
        map { &fork_extra_convert( $_, &{$fc->{AD_EFFNS}}( $_ ), 'TAX') } @{$rv};
    } if ($specialCol{tax_out});
    push @extraTasks, sub {
        my ($rv) = @_;
        map { &fork_extra_convert( $_, &{$fc->{AD_EFFNS}}( $_ ), 'SYM') } @{$rv};
    } if ($specialCol{sym_out});
    push @extraTasks, sub {
        my ($rv) = @_;
        map { &fork_extra_description( $_, &{$fc->{AD_EFFNS}}( $_ )) } @{$rv};
    } if ($specialCol{desc_out});
    if (my $con = $specialCol{convert}) {
        push @extraTasks, sub {
            my ($rv) = @_;
            foreach my $cArgs (values %{$con}) {
                map { $ad->convert
                          ( -id => $_, -ns1 => &{$fc->{AD_EFFNS}}( $_ ),
                            -warn => $doWarn, %{$cArgs}, ) } @{$rv};
            }
        };
    }
    push @extraTasks, sub {
        my ($ignore, $id) = @_;
        &fork_extra_convert( $id, &{$fc->{AD_NS1}}( $id ), 'TAX' );
    } if ($specialCol{tax_in});
    push @extraTasks, sub {
        my ($ignore, $id) = @_;
         &fork_extra_convert( $id, &{$fc->{AD_NS1}}( $id ), 'SYM' );
    } if ($specialCol{sym_in});
    push @extraTasks, sub {
        my ($ignore, $id) = @_;
        &fork_extra_description( $id, &{$fc->{AD_NS1}}( $id ) );
    } if ($specialCol{desc_in});

    $fc->{extraTasks} = \@extraTasks;

    $fc->input_type( 'array' );
    foreach my $ns1 (sort keys %{$nss}) {
        $fc->{AD_NS1} = $ns1 ? sub { return $ns1; } : sub {
            my $id = shift;
            unless (defined $fc->{NS_CACHE}{$id}) {
                $fc->{NS_CACHE}{$id} = $fc->{AD_OBJ}->guess_namespace($id) ||
                    $fc->{AD_OBJ}->guess_namespace_from_db($id, 50) || '';
            }
            return $fc->{NS_CACHE}{$id};
        };
        my @targs = $ad->all_namespace_tokens if ($#{$targNs} == -1);
        foreach my $ns2 (@{$targNs}) {
            my $clean = $ad->namespace_token($ns2);
            if ($clean) {
                push @targs, $clean;
            } else {
                $args->err("Unknown target namespace", $ns2);
            }
        }
        my $tail;
        if ($mode eq 'convert') {
            #$fc->skip_record_method( \&skip_conversion );
            my @ok;
            foreach my $ns2 (@targs) {
                push @ok, $ns2 if ($ad->get_converter($ns1, $ns2));
            }
            @targs = @ok;
            $tail  = " -convert-> [%s]";
       } elsif ($mode eq 'assign') {
            my @ok;
            foreach my $ns2 (@targs) {
                push @ok, $ns2 if ($ad->get_assigner($ns2, $ns1));
            }
            @targs = @ok;
            $tail = " =assign=> [%s]";
        } elsif ($mode eq 'overlap') {
            @targs = ('');
            $tail  = " to Overlapping Mappings";
        } elsif ($mode eq 'map') {
            @targs = ('');
            $tail  = " to Genomic Mappings";
        } elsif ($mode eq 'desc') {
            @targs = ('');
            $tail  = " to Description";
        } elsif ($mode eq 'parents') {
            $tail  = " to Parents";
            @targs = ('');
        } elsif ($mode eq 'children') {
            $tail  = " to Children";
            @targs = ('');
        } elsif ($mode eq 'simple') {
            $tail  = " simple passthrough";
            @targs = ($ns1);
        }
        if ($#targs == -1) {
            my @ids = @{$nss->{$ns1}};
            $args->err("No valid targets found for source namespace", 
                       "NS1 = ".($ns1 || "Unknown Namespace").
                       ", NS2 = ".($ns2 || "Unknown Namespace").
                       ", Mode = $mode",
                       scalar(@ids)." ids, eg: ". join(",", splice(@ids,0,3)));
        }
        
       while ($#targs != -1) {
            my $ns2 = shift @targs;
            next unless (defined $ns2);
            my $preScreened = 0;
            if ($ns2 =~ /^TRUE (\S+)/) {
                $ns2 = $1;
                $preScreened = 1;
            }
            $fc->{AD_NS2}   = $ns2;
            if ($mode =~ /simple/) {
                $fc->{AD_EFFNS} = $fc->{AD_NS1};
            } elsif ($ns1 || !$ns2) {
                my $efns = $ad->effective_namespace( $ns2, $ns1 );
                $fc->{AD_EFFNS} = sub { return $efns; };
            } else {
                $fc->{AD_EFFNS} = sub {
                    my $id = shift;
                    unless (defined $fc->{EFFNS_CACHE}{$id}) {
                        my $ns1  = &{$fc->{AD_NS1}}( $id );
                        $fc->{EFFNS_CACHE}{$id} = 
                            $fc->{AD_OBJ}->effective_namespace
                            ( $fc->{AD_NS2}, $ns1 );
                    }
                    return $fc->{EFFNS_CACHE}{$id};
                };
            }
            my $list = $nss->{$ns1};
            if ($mode eq 'convert') {
                my @mdat =  $ad->_func_for_tokens($ns1, $ns2, );
                my $mname = $mdat[3];
                # Pre-screen to find IDs that require updates
                if (!$preScreened && $mname &&
                    $mname eq 'update_ONTOLOGY_to_OBJECT') {
                    $list = [];
                    my (%needed, %parents, %seen);
                    my @stack = map { $_ } @{$nss->{$ns1}};
                    foreach my $gNS (qw(AL AR AP)) {
                        if ($ad->is_namespace($ns2, $gNS)) {
                            $args->msg(" Preparing child terms with generic namespace $gNS") if ($vb);
                            unshift @targs, "TRUE $ns2";
                            $fc->{AD_NS2} = $ns2 = $gNS;
                            last;
                        }
                    }
                    while ($#stack != -1) {
                        $args->msg("[$ns1] => [$ns2]") if ($vb);
                        my $need;
                        if (0) {
                            # I am VERY confused why I put this here.
                            # It is slowing down forking A LOT
                            $need = $ad->convert
                                ( -id => \@stack, -ns1 => $ns1, -ns2 => $ns2,
                                  @comArgs, -nullrows => 1, -warn => 0,
                                  -dumpsql => 0 );
                        } else {
                            $need = [ @stack ];
                        }
                        @stack = ();
                        foreach my $id (@{$need}) {
                            next if ($needed{$id});
                            my @kids = $ad->direct_children
                                ( $id, $ns1, undef, $doWarn );
                            $needed{$id} = {};
                            foreach my $kid (@kids) {
                                push @stack, $kid unless ($needed{$kid});
                                $needed{$id}{$kid}  = 1;
                                $parents{$kid}{$id} = 1;
                            }
                        }
                    }
                    foreach my $id (keys %needed) {
                        map { delete $needed{$id}{$_} unless 
                                  ($needed{$_}) } keys %{$needed{$id}};
                    }

                    my @levels;
                    while (1) {
                        my @leaves;
                        my @need = keys %needed;
                        foreach my $id (@need) {
                            my @kids = keys %{$needed{$id}};
                            push @leaves, $id if ($#kids == -1);
                        }
                        last if ($#leaves == -1 );
                        push @levels, [ \@leaves ];

                        foreach my $leaf (@leaves) {
                            map { delete $needed{$_}{$leaf} } keys %{$parents{$leaf}};
                            delete $needed{$leaf};
                        }
                    }

                    my $lnum = $#levels + 1;
                    next unless ($lnum);
                    my $msg = "Ontology processed in $lnum levels:\n";
                    for my $l (0..$#levels) {
                        my $num  = $#{$levels[$l][0]} + 1;
                        my $lmsg = sprintf
                            ("  Level %d : %d entr%s\n",
                             $lnum - $l, $num, $num == 1 ? 'y' : 'ies');
                        $msg .= $lmsg;
                        $levels[$l][1] = $lmsg;
                    }
                    $args->msg($msg) if ($doWarn);

                    for my $l (0..$#levels) {
                        my ($leaves, $lmsg) = @{$levels[$l]};
                        $args->msg($lmsg) if ($doWarn);
                        my $num  = $#{$leaves} + 1;
                        my $fnum = ($fork > $num) ? $num : $fork;
                        $fc->reset();
                        $fc->{AD_STOREDESC} = 0;
                        $fc->output_file('bench', ">$benchFile")
                            if ($benchFile);
                        $fc->output_file( 'rowFile', "/dev/null" )
                            if ($forkFile);
                        $fc->input( $leaves );
                        $ad->fork_safe( 1 );
                        my $failed = $fc->execute( $fnum );
                        $ad->fork_unsafe();
                        $args->err("$failed children failed to finish")
                            if ($failed);
                        &add_forked_benchmarks( $benchFile, $fnum);
                    }
                    next;
                } elsif (! $args->{PREFILTER}) {
                    # Request not to pre-scan for needed rows. Sometimes is faster
                } else {
                    $list = $ad->convert
                        ( -id => $nss->{$ns1}, -ns1 => $ns1, -ns2 => $ns2,
                          @comArgs, -nullrows => 1,);
                }
            }
            $fc->reset();
            $fc->output_file( 'rowFile', ">>$forkFile" ) if ($forkFile);
            if ($descFile) {
                $fc->output_file( 'descFile', ">>$descFile" );
                $fc->{AD_STOREDESC} = 1;
            } else {
                $fc->{AD_STOREDESC} = 0;
            }

            $fc->output_file( 'bench', ">>$benchFile") if ($benchFile);
            $fc->input( $list );
            my $num  = $#{$list} + 1;
            my $fnum = ($fork > $num) ? $num : $fork;
            if ($doWarn) {
                my $msg = sprintf
                    (" %s [%s]$tail", $num ?
                     "Forking $num entries" : "Nothing to do for", $ns1, $ns2);
                my $diff = $#{$nss->{$ns1}} - $#{$list};
                $msg .= " ($diff already done)" if ($diff > 0);
                $args->msg($msg);
            }
            next unless ($num);
            $ad->fork_safe( 1 );
            my $failed = $fc->execute( $fnum );
            $ad->fork_unsafe();
            &add_forked_benchmarks( $benchFile, $fnum);
            $args->err("$failed children failed to finish") if ($failed);
        }
    }
    # touch $forkFile unless (-e $forkFile);
    $benchFile = "";
    $args->msg("Forking complete") if ($vb);
    $ad->bench_end;
    return $forkFile;
}

sub add_forked_benchmarks {
    my ($benchFile, $fnum) = @_;
    return unless ($benchFile && -s $benchFile);
    $ad->bench_start('Add Forked Benchmarks');
    if (open(BF, "<$benchFile")) {
        my $txt = "";
        while (<BF>) { $txt .= $_; }
        close BF;
        $ad->benchmarks_from_text($txt, $fnum);
    } else {
        $args->msg("[!!]","Failed to read forking benchmarks", $benchFile, $!);
    }
    unlink($benchFile);
    $ad->bench_end('Add Forked Benchmarks');
}

sub fork_convert {
    my $rv = $ad->convert
        ( -id  => $_[0], 
          -ns1 => &{$fc->{AD_NS1}}( $_[0] ), 
          -ns2 => $fc->{AD_NS2},
          @comArgs);
    &fork_extra([ map { $_->[0] } @{$rv}], $_[0]);
    if ($fc->{BENCHFILE} &&
        (!$fc->{BENCHTIME} || time - $fc->{BENCHTIME} > 15)) {
        my $bf = $fc->{BENCHFILE}."-Snapshot";
        if (open(BF, ">$bf")) {
            print BF $ad->showbench();
            close BF;
        } else {
            $args->err("Failed to write benchmarks to file while forking",
                       $bf, $!);
            $fc->{BENCHFILE} = 0;
        }
        $fc->{BENCHTIME} = time;
    }
    return $rv;
}

sub fork_assignments {
    my $rv = $ad->assignments
        ( -id  => $_[0], 
          -ans => &{$fc->{AD_NS1}}( $_[0] ), 
          -ons => $fc->{AD_NS2},
          -discard => 1,
          @comArgs);
    return $rv;
}

sub fork_overlap {
    my $rv = &get_overlaps
        ( $_[0], [&{$fc->{AD_NS1}}( $_[0] )],
          $fc->{AD_OKINT}, $fc->{AD_NSINT});
    return $rv;
}

sub fork_map {
    my $rv = $ad->mappings
        ( -id  => $_[0], 
          -ns1 => &{$fc->{AD_NS1}}( $_[0] ), 
          @comArgs);
    return $rv;
}

sub fork_description {
    my $desc = $ad->description
        ( -id  => $_[0], 
          -ns  => &{$fc->{AD_NS1}}( $_[0] ),
          @comArgs);
    return [[$desc, $_[0]]];
}

sub fork_children {
    my @rv = $ad->all_children( $_[0], &{$fc->{AD_NS1}}( $_[0] ), undef, $doWarn );
    &fork_extra(\@rv, $_[0]);
    return \@rv;
}

sub fork_parents {
    my @rv = $ad->all_parents( -id   => $_[0], 
                               -ns   => &{$fc->{AD_NS1}}( $_[0] ),
                               -warn => $doWarn );
    &fork_extra(\@rv, $_[0]);
    return \@rv;
}

sub fork_simple {
    my $id = $_[0];
    &fork_extra([ $id ], $id );
    return ([ $id ]);
}

sub fork_extra {
    my ($rv, $id) = @_;
    foreach my $callback (@{$fc->{extraTasks}}) {
        &{$callback}( $rv, $id );
    }
}

sub fork_extra_convert {
    my ($id, $ns1, $ns2) = @_;
    return if (!$id || $extraDone{$id}{$ns2}++);
    $ad->convert( -id => $id, -ns1 => $ns1, -ns2 => $ns2, -warn => $doWarn );
}
sub fork_extra_description {
    my ($id, $ns1) = @_;
    return unless ($id);
    return if ($extraDone{$id}{$ns1}{DESCR}++);
    my $desc = $ad->description( -id => $id, -ns => $ns1, -warn => $doWarn );
    $fc->write_output('descFile', join("\t", $id, $ns1, $desc || "")."\n")
        if ($fc->{AD_STOREDESC});
}

sub initialize {
    # This is now the forked child; re-make the $ad object
    $ad = $fc->{AD_OBJ} = &getAccessDenorm();
    $ad->clear_benchmarks();
    $ad->{TRACE}       = $args->{TRACE};
    $ad->{CLOUDWARN}   = $args->{CLOUDWARN};
    $primaryChild = $fc->child() % $fc->total_fork ? 0 : -1;
    if ($primaryChild) {
        # Show verbosity only for the first child
        $ad->{SCROLL_SIZE} = $scrollSz || 50;
    } else {
        # Non-primary children do not scroll
        $ad->{SCROLL_SIZE} = 999999;
 #       $doWarn  = 0;
 #       my %hash = (@comArgs);
 #       @comArgs = ();
 #       delete $hash{'-warn'};
 #       while (my ($key, $val) = each %hash) { push @comArgs, ($key, $val) };
    }

    # $ad->{SCROLL_SIZE} = 100 * int(0.99 + $fc->total_fork() / 5);

    $ad->set_specific_age(undef, 'TAX', $safe);
    $ad->age( $age );
    $ad->cloud_age( $cAge );
}

sub getAccessDenorm {
    # $args->msg("[DEBUG]","Instantiating AD object for PID=$$");
    my $ad = BMS::MapTracker::AccessDenorm->new(@adArgs);
    unless ($noSafeAge) {
        my $sa = $ad->set_specific_age(undef, 'TAX', $taxSafe);
        $args->msg("[v]","Taxa age set to ".int($sa)." days") if
            ($doWarn && !$randFlags{taxaAge}++);
        my $lta = 90;
        foreach my $pair (['LT','NS'], ['LT','UNK']) {
            $ad->set_specific_age(@{$pair}, $lta);
        }
        $args->msg("[v]","ListTracker MetaData age set to ".int($lta)." days") if
            ($doWarn && !$randFlags{ListTrackerAge}++);
    }
    $oracleStart ||= $ad->oracle_start();
    $ad->oracle_start( $oracleStart );
    # $ad->dbh->update_via_array_insert( 15 );
    $ad->{SCROLL_SIZE} = $scrollSz if ($scrollSz);
    return $ad;
}

sub get_oscp1_dbh {
    return BMS::FriendlyDBI->connect
        ("dbi:Oracle:", 'maptracker/mtreadonly@oscp1',
         undef, { RaiseError  => 0,
                  PrintError  => 0,
                  LongReadLen => 100000,
                  AutoCommit  => 1, },
         -errorfile => '/scratch/OSCP1.err',
         -adminmail => 'tilfordc@bms.com', );
}

sub crs_stock_sth {
    return $crsSTH if ($crsSTH);
    $ad->bench_start;
    my $dbh     = &get_oscp1_dbh();
    push @globals, $dbh;
    my $sql     = <<EOF;
SELECT psl.ext_sub_lot_id, ic.maxorderablesolutionamount, ic.maxorderabledryamount
  FROM tapif.crs_inv_compound ic, rsims.pcris_substance_lots psl
 WHERE ic.compoundid = ?
   AND psl.LOT_ID = ic.lotid
EOF

    $crsSTH = $dbh->prepare
        ( -name => "Get wet and dry inventory from CRS",
          -sql => $sql, );
    $ad->bench_end;
    return $crsSTH;
}

sub set_mime {
    return if ($nocgi || $mimeSet++);
    my $mime = shift;
    $args->set_mime( $mime ? (-mime     => $mime) : (),
                     -mail     => 'charles.tilford@bms.com',
                     -codeurl  => "http://bioinformatics.bms.com/biocgi/filePod.pl?module=_MODULE_&highlight=_LINE_&view=1#Line_LINE_",
                     -errordir => '/docs/hc/users/tilfordc/' );
}

sub add_column_dependencies {
    my ($corder) = @_;
    my %required;
    my $cnt = 0;
    if ($fmtFlags{NiceChem}) {
        map { $required{$_} ||= ++$cnt } qw(matched auth);
    }
    if ($fmtFlags{BED}) {
        map { $required{$_} ||= ++$cnt }
        qw(term_in ns_in matched strand
           term_out vers_out start_out end_out loc_out);
    }
    if ($fmtFlags{PubMed}) {
        map { $required{$_} ||= ++$cnt } qw(auth);
    }
    if ($fmtFlags{MatrixMarket}) {
        map { $required{$_} ||= ++$cnt } qw(term_in term_out matched);
    }
    if ($customSets) {
        map { $required{$_} ||= ++$cnt } ($objCol);
    }
    if ($metaCols) {
        map { $required{$_} ||= ++$cnt } ($objCol, 'metasets');
    }
    if ($bestOnly) {
        map { $required{$_} ||= ++$cnt } qw(term_in ns_in ns_out matched);
    }
    if ($addLinks) {
        map { $required{$_} ||= ++$cnt } qw(auth);
    }
    if ($cnt) {
        my @needed = sort { $required{$a} <=> $required{$b} } keys %required;
        foreach my $newCol ($ad->extend_array( $corder, \@needed )) {
            push @{$corder}, $newCol;
            $unrequestedColumns ||= {};
            $unrequestedColumns->{$newCol} = 1;
        }
    }
}

sub show_help {
    if ($nocgi) {
        print `pod2text $0`;
    } else {
        print `pod2html $0`;
    }
}

sub _get_desc {
    my ($id, $ns) = @_;
    return "" unless ($id);
    $ns = $ad->namespace_token($ns) if ($ns);
    my $desc;
    if ($forkDesc && $ns) {
        # Descriptions were generated during forking
        $desc = $forkDesc->{$id}{$ns};
    } else {
        # Might as well set up a cache
        $forkDesc = {};
    }
    unless (defined $desc) {
        $desc = $forkDesc->{$id}{$ns || ""} ||=
            $ad->description(-id => $id, -ns => $ns, @comArgs);
    }
    return $desc;
}


=head1 GenAcc Service

=head2 Usage

=head3 Command Line

  genacc_service.pl -param1 value1 -param2 value2 ...

=head3 URL

 genacc_service.pl?param1=value1&param2=value2 ...

 Stable URL: http://bioinformatics.bms.com/biocgi/genacc_service.pl
   Beta URL: http://bioinformatics.bms.com/biocgi/tilfordc/working/maptracker/MapTracker/genacc_service.pl

For both URL and command line, parameters are case-insensitive. On the
command line, parameters must be preceded with a dash, and a parameter
without a value will be assigned a value of 1.

=head3 Examples

Descriptions for assorted ids:

 genacc_service.pl -mode desc -id NM_001234,P31_5417,Q9H210,Hs.131138

Get LocusLink entries for a gene symbol:

 genacc_service.pl -id hoxb8 -ignorecase -ns1 sym -ns2 ll -cols term_in,term_out,desc -format tsv_head -min 0.9

Get protein sequences associated with a locus:

 genacc_service.pl -id loc859 -ns2 ap -format fasta > CAV3_protein.fa

Get a list of all recognized Affy array design names:

 genacc_service.pl -id AAD -ns2 aad -format list

Find all probesets in HG_U133A

 genacc_service.pl -id HG_U133A -ns1 AAD -ns2 aps -format list > U133probes.txt

Find all loci in HG_U133A with at least 80% match to probeset:

 genacc_service.pl -id HG_U133A -ns1 AAD -ns2 LL -min 0.8 -format list > U133loci.txt

Generate an excel spreadsheet of all human kinases (as per
GeneOntology with at least 90% confidence):

 genacc_service.pl -id GO:0004672 -ns2 LL -format excel -int 'Homo sapiens' -intns TAX -desc -min 0.9 -output HumanKinases.xls

Find up to 10 LocusLink genes that have the text "t-cell" AND the text
"killer" in their description:

 genacc_service.pl -mode lookup -ns ll -text 't-cell' -text 'killer' -limit 10

=head2 General Configuration

     -mode Default 'convert'. What you want the program to do. Can use
           the alias -action as well. Options are:

             'convert' - Transform your input into IDs in another namespace

              'assign' - Similar to convert, but performs a transitive
                         closure recovery of ontology terms assigned
                         to the input term.

                'desc' - Recover descriptions for your input

               'cloud' - Get cloud subgraphs for your input

            'children' - Get all direct and indirect children for input
                         terms organized in a hierarchy

           'namespace' - show available namespaces

          'ontoreport' - Similart to 'assign', but organizes the
                         ontology terms into a compact HTML structure.

              'simple' - The input is passed through as-is. This is
                         useful if you want to simply add additional
                         columns to your data (see the special columns
                         section below).

                'link' - Each input term will have hyperlinks created
                         for it that point to other resources related
                         to the term.

               'churn' - An administrative mode that causes old data
                         to be reculaulated.

              'lookup' - Allows free-text queries against the
                         description table. The query text is taken
                         from the -text parameter.

=head3 Data Age / Staleness

      -age Default 999. Maximum allowable age for data recovered for
           conversions, descriptions or assignments. If data stored in
           the database are older than this value, then the
           information will be freshly recalculated. Note that while
           this allows the most recent data to be recovered, it could
           also take a LONG time.

           Integers are treated as days. Date::Manip is used to parse
           this value, so you can also pass entries like "10minutes"
           or "4 days ago" or "7 July 2006"

 -cloudage Default 'safe'. Similar to -age, but applies to the
           recovery of clouds (subgraphs of related identifiers).

   -ageall A dual alias, if used, the value will be applied to both
           -age and -cloudage.

  -ageonce Default 0. If true, then age filters will only be applied
           to the first level of analysis. Many namespace conversions
           involve 1 or more recursive conversions to connect the
           input and output namespace. If -ageonce is true, then the
           recursive operations will use the default age (generally a
           large number). This can speed up operations, but can also
           generate unusual connections.

 -nosafeage Default 0 (except for churn mode, where it is forced to be
           true). If false (default), then namespace specific ages are
           set for some operations. Currently, this is just for
           conversions that return taxonomies, which are set to the
           'safe' age; this is because taxonomy assignments should be
           constant over time, and they are frequently calculated. If
           you desire your age constraints to be applied to all
           conversions, provide a true value for this parameter.

 -redonull Default 0. If true, then input terms that have no output
           terms will be recalculated. Conceptually similar to -age,
           but presumes that data passing the age filter but having no
           results may have had updated information provided.

=head3 Forking

     -fork Default 0. If true, then before the conversion occurs, the
           database will be checked relatively quickly to determine
           what de-novo calculations need to be performed. Forking
           (number of forks defined by -fork) will be used to populate
           the database more rapidly, and then the DB will be queried
           by a single process as normal, and your results
           returned. This option is useful for administrative
           pre-caching of data.

 -populate The same as fork, but will return no output (useful for
           pre-populating the database).

=head2 Specifying Input

=head3 Providing Input Terms

  -queries Your input term(s). Aliases -id, -ids, -query, -term, and
           -terms. If provided as a string, the string will be split
           on [\n\r\t\s\,]+ (see also -keepspace and -keepcomma)

   -idlist Aliases -idpath, -idfile. Alternative to -queries, a path
           to a file of identifiers. Assumes one identifier per row.

  -idfasta Similar to -idlist, but it should provide the path to a
           fasta file. The primary identifiers in the file will be
           stripped out (including removal of SeqStore container
           names; eg REFSEQP:NP_001234 will be extracted simply as
           NP_001234) and used as the query; the sequence data will
           simply be ignored.

=head3 Describing Input Terms

      -ns1 Input namespace. Aliases -ns, -namespace, -qns, -ans. If
           not provided, will be guessed from your input. If you know
           the namespace of your input, it is generally a good idea to
           specify this value. Conversely, if your input contains
           identifiers from many namespaces, leave it blank and let
           the program guess.

      -set Specify one or more (separate with comma, return or tab)
           set identifiers as the query input. Will force -ns1 to be
           'SET', and will over-ride other inputs provided! Further,
           if the 'set' column has been requested in output, then
           every output value will be tested to see which set it
           belongs to (if only a single set is provided, then this
           column will be relatively boring. If multiple sets were
           provided, the column can be used to determine differential
           membership of each object in the originating sets).

=head3 Describing Input Format

  -istable Used in conjuction with -idlist. Specifies that the
           provided file is a tab-delimited table, and that the IDs
           reside in the indicated column, so -idlist 3 will gather
           IDs from the third column. Note that providing a parameter
           without a value from the command line automatically sets
           the parameter to 1, so simply calling -istable will gather
           the first column. Aliases -colname, -colnum, -excelcol.

 -hasheader Used in conjuction with -idlist. If true, the first row of
            the data in -idlist will be discarded.

 -ignorecase Default 0. If true, then case-sensitive namespaces will
           be queried disregarding the case. For example, if you query
           with a gene symbol (-ns1 SYM), matches will normally be
           returned only for an exact case match, so a human symbol
           will not match data associated with the corresponding one
           from mouse. If this is undesirable, use -ignorecase.

 -cleanqry Default off. There are two recognized values:

           Dirty: Indicates that the query IDs are "not clean". Should
           be used if the query contains non-ID characters. If a
           namespace is provided (-ns1) this will limit the ID
           patterns that the program will search for. Otherwise it
           will search for all known patterns (which may introduce
           undesired false IDs, depending on how dirty the input is).

           Int: Indicates that the queries are integers. Integer IDs
           are shunned by MapTracker/GenAcc, since they are incredibly
           hard to namespace. However, they are frequently encountered
           "in the wild", so user input may be forced to deal with
           them. For example, MapTracker will record GI numbers as
           "gi12345", but usually these will be reported as just
           "12345".

           Integer IDs must have a single namespace provided, and not
           all namespaces can be reliably mapped from integers
           alone. The namespace can be passed via -ns1 or by
           -integerns.

           -cleanqry aliases are -cleanquery, -isdirty, -dirty

 -integerns Default false. If set to a namespace, will automatically
           set -ns1 to that value and set -cleanqry to 'Integer'.

 -keepspace Default 1 if -nocgi is set, otherwise 0. If true, then -id
            will NOT split string argments on spaces. This should be
            set to true if your input contains spaces within a single
            ID.

 -keepcomma Default 0. If true, then -id will NOT split on
            commas. Should be set to true if your input contains
            commas within a single ID.

 -scramble Default false. Normally input terms are analyzed in a
           consistent, sorted order. Setting -scramble to true will
           randomize the order they are analyzed in. This is primarily
           of utility when doing a forked analysis (for database
           population), as it can prevent duplicated effort.

 -standardize Default false. If true, then input terms will be
           standardized. This usually just involves capitalization
           changes (loc859 -> LOC859), but in other cases might cause
           more dramatic changes (HG-U133A -> HG_U133A). This step is
           usually not needed, but for 'simple' mode can be used to
           normalize the names of input.

=head2 Specifying Output

      -ns2 Output namespace. Alias -ons ('ontology
           namespace'). Defines the namespaces you wish to convert or
           assign your input to. If not provided, all available
           namespaces will be recovered.

   -filter Indicates that the term_out results should be filtered
           against specific values returned from a conversion or
           assignment. A leading exclamation point (which may need to
           be escaped in your shell) indicates that you want to find
           entries that do NOT match the value. For example:

           -ns1 ll -ns2 GO -filter '\!GO:0004984'

           ... indicates that you will be converting from LocusLink to
           GO, and you want to keep only entries that do not convert
           to GO:0004984 (olfactory receptors). Note that filtering is
           input term-centric; output results are clustered around
           input terms, and if the filter criteria match for any
           output term, the effect (retention or discarding) is
           applied to all rows returned for that input term.

           If you are using filter, it is assumed that you desire as
           output your filtered input list. If you in fact want the
           output generated by the conversion used to perform the
           filtering, specify -noremap.

   -status Will find deprecation status for your input terms. The
           following values will force a conversion calculation to
           "Reliable Synonym". These results are then used to find if
           your input is deprecated or not:

           'deprecated', 'dead' - Will return only rows where the
           input term is deprecated

           'live' - Will return only rows where the input is not deprecated

           'usable' - Returns live IDs, or IDs that are deprecated but
           have an indicated 'newer' term.

           'status' - Returns all rows regardless of deprecation state

           In all deprecation cases, the returned data will contain
           four columns: term_out, ns_out, term_in, ns_in. The last
           two describe the input terms as normal. ns_out now reflects
           the 'Status' of the input term, and will be either "Live",
           "Deprecated", "Unknown" or "DeprecatedTo". "Unknown"
           generally indicates that the input term was unrecognized in
           the context of the input namespace. term_out will be empty
           if the status is Deprecated. If the status is Live,
           term_out will be identical to term_in.  If status is
           DeprecatedTo, term_out will be the newer, live ID that
           replaces the input term.

           As an example, if you had a list of IDs and wanted to make
           sure that you were working with non-deprecated accessions,
           you could execute:

           -idlist myListOfTerms.txt -status usable -format list

           ... which will generate a non-redundant list of
           non-deprecated terms. Note that the authorities that set
           DeprecatedTo assignments may not be using criteria that you
           trust; if you want to be extra careful, use -status live to
           avoid external mapping of deprecations.

   -alldep When filtering for status, deprecations to other
           deprecations are normally ignored. If you specify -alldep,
           this will add another class 'OldDeprecated' to the output,
           showing deprecations to IDs that were then themselves also
           (presumably later) deprecated.

=head3 Filtering and Constraining Output

      -min Optional filter against the -matched column. Should be a
           value from 0 to 1. Any entries less than this value will be
           discarded. Aliases -matched, -score.

      -int Aliases -intersect and -intersection.
    -intns
           Used in Convert mode, defines an intersecting value that
           output terms should also match. -intns specifies the
           namespace of the intersecting term. Usually used to filter
           for set membership or species. For example:

           -ns1 sym -ns2 LL -int 'Homo sapiens' -intns TAX

           ... will perform a conversion from gene symbols to
           LocusLink, but only return loci that are also assigned to
           Taxonomy 'Homo sapiens'. Assigning an intersection will
           generally add additional time to the process. Note that the
           provided term is case-sensitive, so 'homo sapiens' will
           return zero rows.

     -auth Default undefined, aliases -authority, -ec. Currently used
           only for assignments, will cause results to be filtered by
           evidence code. So -ec TAS will only get Traceable Author
           Statements, while -ec '\!IEA' will reject "Infered from
           Electronic Evidence".

           Because '!' can cause irritations with some shells (and
           different irritations for different shells), -notauth can
           be used as an alias where the passed value will be prefixed
           with '!' for you.

 -keepnull Default 0. If true, then rows where the output term is
           undefined will be included in the results. Otherwise they
           will be discarded.

 -keepbest Default 0. If true, then for each input, only the best
           scoring output(s) will be kept. So if a query returns A
           (score 0.9), B (0.7) and C (0.9), only A and C will be
           reported. Please bear in mind that this filter is quite
           literal and my not do the "common sense" action; for
           example, when filtering Orthologue results it will only
           report the best species, not the best matched loci for each
           species. Aliases -best and -bestonly.

 -keepnearest Default 0. When recovering nearby genomic objects in
           overlap mode, will report only the closest object(s) for
           each query. More than one object may be returned if they
           have the same minimal distance; this is relatively common
           since frequently nearby objects actually overlap the query,
           and would then share a distance of zero. Alias -keepclosest

    -range Defualt 1000000 (1 Mb). For overlap mode, defines the
           distance to which near-but-not-overlapping objects should
           be recovered. Aliases -distance, -dist

   -noself Remove rows where the input and output terms are the same

=head2 Controlling Format

   -format Default TSV if -nocgi is true, otherwise "Full Structured
           HTML". Primary mechanism for controlling the
           format. Structured as a string with one or more keywords in
           it. For example, "tsv header desc" specifies TSV output
           format, that a header row should be included, and a
           description column added. The keywords can be in any order,
           and capittalization is ignored. Recognized keywords are:

               atom - Output will be in ATOM XML

                chr - Chromosome names will only be the chromosome;
                      that is, "homo_sapiens.chromosome.4" will become
                      just "4". This behavior can also be set using
                      the parameters -cleanchr or -simplechr.

            cluster - Only applicable to HTML tabular output. Rows
                      sharing the same term_out value will be grouped
                      (via rowspan) into the same location in the
                      table.

             canvas - CanvasXpress. Will generate JSON data suitable
                      for loading into a CanvasXpress pane. Alias -cx

                dad - "Dumb as Dirt" output will be used; a simple
                      JSON format where the primary output is a
                      pre-formatted HTML payload, but additional
                      metadata will be provided via JSON structuring.

          datatable - Output will be generated in Google DataTable
                     JSON format. The string 'google' will trigger the
                     same effect.

               desc - A description column should be added for the
                      most relevant output column. Can also be
                      effected with the -desc parameter

           distinct - duplicate rows should be removed

              excel - output should be written to an excel file. Use
                      the -output parameter if you wish to define a
                      specific file path.

            extgrid - create ExtJS Grid JSON format. Be careful that
                      you do not put a 't' in front, as you will
                      likely get 'text' output instead.

              fasta - A fasta file should be generated, using the
                      output terms as queries against the SeqStore
                      database.

               full - When html format is being used, including 'full'
                      will generate a fully-formed HTML web
                      page. Otherwise, the output will be just a
                      snippet of HTML code, designed to be embedded in
                      a larger page.

                gml - Create GML graph XML output (for cytoscape)

               grep - Geek mode format, will take all the found output
                      terms and generate a command line useful for
                      grepping those terms in text data. For example,
                      "-id NM_001234 -ns2 sym -format grep" makes:

                      egrep '(CAV3|LQT9|MGC126100|MGC126101|MGC126129|VIP21|VIP-21|LGMD1C)'

               grid - Transforms output into a 2D table with input
                      terms as row headers, output terms as column
                      headers, and fills in cells when an input was
                      connected to an output. The cell contents are by
                      default the score and authority of the
                      association. They can be altered with the
                      following parameters (pass individually, as
                      opposed to part of format string):

                      -nosc   : Leave out score
                      -addec  : Add Evidence Code, when available
                      -noauth : Leave out authority.

               head - Forces inclusion of a header row

               html - output should be structured with appropriate HTML
                      tags for display in a web browser.

              image - Only relevant for -mode namespace. Will generate
                      a PNG image showing a grid of allowed namespace
                      conversions.

               json - Will generate JSON text output, suitable for
                      being parsed by JavaScript programs. The JSON
                      object will include column header information,
                      and a 2D array of results.

             leaves - If ontology terms are being recovered,
                      specifying leaves (or leaf) in the format will
                      result in any internal nodes being discarded
                      (keeping only the 'outer-most' terms).

               link - will cause the output to include an additional
                      column of hyperlinks. The links will be relevant
                      to the output term in the row they reside in,
                      and will be formatted as HTML anchor (<a>) tags
                      if the overall format is HTML, otherwise will be
                      comma-separated URL strings.

               list - output should be reduced to the single, most
                      relevant output column. Alias 'col'.

               meta - Formats the output as a "MetaSet" text file. 

             nohead - Will prevent a header row from being generated

               null - Do not report output. Generally only useful for
                      database populating uses.

               onto - An HTML ontology report should be generated

             oracle - Output should be directed to an Oracle
                      table. See the -table argument for more
                      information.

               perc - Report matched values (scores) as percentages
                      (0-100), rather than ratios (0-1).

               perl - Uses Data::Dumper to output the results as a
                      Perl structure.

               rank - Indicates that the input terms are rank ordered,
                      and that the output should preserve that order.

               rich - Output will be in RichList format, which allows
                      comments and descriptions to be added to a 1-D
                      list. GenAcc Service is capable of reading these
                      lists as input.

                set - A set file should be generated. Set files are
                      simple text formats used by the SetMachine,
                      which allows set logic comparisons between
                      lists.

             string - Output format very similar to List, will force
                      headers to be off. Useful for AJAX calls.

             struct - HTML format normally produces tabular
                      output. Including struct will result in a
                      structuted tree (list) format for the results.

             symbol - Same effect as using the -symbol argument or the
                      'sym' column , any of which will add a gene
                      symbol column to the output.

               text - Generates human-readable (space-padded) text
                      output, suitable for viewing in a terminal
                      window. This is NOT the same as TSV. Alias txt.

             tiddly - Output will be formatted as a TiddlyWiki
                      table. Alias tw.

                tsv - Generates Tab-Separated Values output

              xgmml - Create XGMML graph XML output (for cytoscape)

           Note that the keywords are case-insensitive, and also will
           be recognized if any part of the word matches (so Ranked
           will match rank). 

   -header Default 1 for some formats. If true, then a header row will
           be added to output tables.

    -limit Default 0. If non-zero, then will limit the number of rows
           returned. If you are experimenting with settings, it is
           often wise to set a small limit (say 10) to make sure you
           are not flooded with results.

     -sort Default term_out. Defines the column that data should be
           sorted on.

=head3 Adding Extra Columns

All modes come with a default set of columns, but you can control
which columns are shown, the order they appear in, and add derived
columns as well.

     -cols Default undef. Allows the user to provide custom columns
           and/or custom order for tabular output. For example, -cols
           term_in,term_out will generate a table with just the two
           output columns. Useful for streamlining the output. Note
           that if you request an unknown column ("poodle") or a
           column that is not normally displayed for the mode you are
           using, it will be empty. Aliases -custcol, -custcols.

           See the "Column Specifiers" section below for column names.

  -adddesc Aliases -desc, -description. Default 0. If true, then a
           column containing the description of the output object will
           be added. Not needed for assignments, which have output
           descriptions by default.

  -adddesc Default 0. If true, then a column containing the
           description of the output object will be added. Not needed
           for assignments, which have output descriptions by
           default. Aliases -desc, -description, -descout, -adddescr;
           can also be set with 'desc' in the -format text; can be
           placed in a particular column with the -cols argument.

   -descin As above, but adds a description column for the input term. Alias -adddescin, can use

  -addtaxa Default 0. If true, then a column containing the
           description of the output object will be added. Not needed
           for assignments, which have output descriptions by
           default. Aliases -desc, -description, -descout, -adddescr;
           can also be set with 'desc' in the -format text; can be
           placed in a particular column with the -cols argument.

 -nullscore Default false. Normall null (undefined) scores are
           presented as nulls (or empty strings ""). You can use this
           parameter to pass any value you would like to use for null
           values. A useful value is "-1" (minus one), which allows
           numeric sorting on the score column.

  -skipcol Similar to -cols, but allows you to discard specific
           columns, while otherwise maintaining default columns and
           order. -skipcol auth,ns_out will discard those two columns
           (if they were present in the first place).

  -showkey Default 0. If true, for HTML output formats, will show a
           key detailing the nature of each hyperlink token.

 -cleancol Default 0. If true, then columns that have no data will be
           removed before generating output.

 -distinct If true, then will remove duplicate rows from the final
           output. Can also be defined with the -format flag.

    -nocgi Default is set by %ENV - if HTTP_HOST environment is set,
           then will be 0, otherwise 1. -nocgi will affect other
           default settings, particularly the default format.

   -output Optional file path to which output will be written. If not
           provided, output will either be directed to STDOUT, or to
           an automatically generated file path (for example, for
           file-neccesary formats such as excel). Alias -outfile.

 -showopts If true and -nocgi is false, will generate the HTML code
           needed to build a simple web GUI, providing a starting page
           to let the user set many of these parameters with a web
           form interface.

     -urls Specifies a file of URL specifications used for
           hyperlinking some HTML formats. Generally should be left alone.

 -minimize Used only with the Ontoreport format. Will make the
           displayed HTML more compact.

  -addfunc Alias -addmeth. When the mode is 'namespace', will add the
           function name for the code responsible for performing the
           conversion. Generally only of interest to geeks.

 -smallcloud Default 0. The default for mode 'cloud' is to render the
           GeneCluster(s) relevant to your queries. If -smallcloud is
           true, then a ProteinCluster or TranscriptCluster will be
           rendered if the query is a protein or transcript.

 -cloudtype Default Gene. Selects the type of cloud to display when
           -mode is cloud. Other values are Protein, Transcript or
           Orthologue.

 -fontsize Used only when generating a PNG representation of a -mode
           'namespace' table. Essentially defines how wide the columns
           and rows will be.

 -showsmiles Default 0. If false, then SMILES strings will be
           represented by their internal MapTracker ID format, eg
           MTID:66325809. When true, the full SMILES string will be
           used.

 -nullrows Default 0. If true, then the search will return only those
           IDs for which no output terms were found.

    -noany When drawing GraphViz graphs for -mode cloud, will leave
           out generic sequences (Any RNA, Any Protein). Tends to make
           more legible images.

 -notrembl Similar to -noany, but leaves out TrEMBL IDs.

   -class Default 'gatab'. The CSS class to use when printing out HTML tables.

 -classcol Default 0. If true, then every HTML table cell will be
           given a class corresponding to the column it came from (eg
           class='term_in' or class='descr'). Useful for column-level
           styling.

 -addclass Default undef. If defined, will add that value as a CSS
           class to HTML table output.

   -flaggo Default "". If not null, then the argument will be
           interpreted as one or more go terms (separated by
           whitespace or commas). Each GO term will add a new column
           to the table. If the output term in the row is directly or
           transitively assigned to that GO term, then the score will
           be entered into the column, with -1 representing an
           udefined score. An undefined cell value indicates that the
           GO term was not associated with the output term.

 -usegoacc Default false. If true, then use the go accession as column
           headers when using -flaggo. Otherwise, the description of
           the GO term will be used (except when text format is
           selected).

     -root When using 'parent' mode, will assure that the root parent
           is included in output. In some cases (for example GO) the
           ultimate root parent is generally not included.

 -savetable Default 0. If true, then the input will be taken to be a
           2-dimensional table (either because you are passing input
           with -istable, or because a 1-D list will be utilized as a
           single column table), and the output columns will be
           appended to the end of your input. The output should have
           the same number of rows as the input, multiple values per
           row will be concatenated with commas. This feature is beta,
           please let me know if you have questions or encounter
           problems.

    -table Default "temp". Must be used in conjuction with -mode
           Oracle. Used to define the oracle table name; the table
           will be your LDAP name combined with this value, so if you
           are 'dolanp' and you provide 'data', the data will be
           written to table dolanp_data.

           The table will have columns identical to those that would
           be produced with normal tabular output. If the table
           already exists, and the columns differ, then any output
           columns not in the existing Oracle table will be dropped.

           Tables can be found in:

           gaout/gaout@mtrkp1

           See the OTB:
           http://otb.pri.bms.com/otb.lisp?func=tables&server=mtrkp1&user=GAOUT

     -drop Default 0. If false, then output directed to -table in
           -mode Oracle will be appended to any existing data. If
           true, then an existing table will be dropped before being
           re-populated.

  -nostats When outputing to an Oracle table, the primary time spent
           will be an automatic computation of statistics for that
           table. If you do not need up-to-date statistics on the
           generated tables, set this parameter to true to skip this
           step. Note that queries on the table will likely perform
           poorly.

  -noindex By default the table generated by Oracle format output will
           indices added to term_in and term_out columns. If you do
           not wish this ot occur, set -noindex to a true value. This
           will also cause -nostats to be true as well.

=head2 Column Specifiers

These following names are recognized by parameters that take columns
as arguments (-sort, -custcols, -skipcol). [convert] indicates the
modes relevant to the column. The first value is the value used
internally by the program, other values are recognized
aliases. Underscores are always optional. Regular expressions allow
many more aliases than shown, only the most sensible are shown for
brevity.

            Input Term [all]: term_in, term, acc, term_1
           Output Term [almost all]: term_out, term_2, id_2, ont
       Input Namespace [all]: ns_in, ns_1, ns
      Output Namespace [almost all]: ns_out, ns_2
             Authority [almost all]: auth, evid, ec
               Matched [almost all]: matched, score, min
         Query Matched [overlap]: matched_in, score_in
    Internal Namespace [conversion, cloud]: ns_between, ns_mid, ns_btwn
    Output Description [almost all]: desc_out, desc, description
     Input Description [almost all]: desc_in
      Link Description [links]: desc_link
          Link Comment [links]: com_link
            Child Term [children]: child, children, kid
                Parent [children]: parent
                 Depth [children]: depth, level, lvl
              Distance [overlap]: distance
        BMS Stock (ul) [conversion]: inventory
            Hyperlinks [almost all]: links
              Data Age [almost all]: age
        Set Membership [all]: set
               Species [all]: tax_out, taxa, species
         Input Species [almost all]: tax_in, species_in
                Symbol [all]: sym_out, sym
          Input Symbol [almost all]: sym_in
         Internal Term [n/a]: term_between, term_btwn
          Input Length [maps]: len_in, length_in
         Input Version [maps]: vers_in, vers1
        Output Version [maps]: vers_out, vers2
 Input Location String [maps]: loc_in, ft_in
 Output Location String [maps]: loc_out, ft_out

=head3 Special Columns

The following columns are derivative columns that can be added to most
output tables. They will utilize the 'primary' output column of each
row as input. Generally this will be term_out, but in some cases (like
simple mode, where the input is left unchanged) they might be
term_in. 

In the list below, "'foo','bar' or -foobar" should be read as:

   "this column can be specified with either 'foo' or 'bar', which
    allows it to be put in a specific position using the -custcols
    parameter. Alternatively, setting the -foobar parameter to a true
    value will incorporate the column at the end of any standard
    output table."

  Inventory - 'inv','stock' or -inventory. This column will show the
              current maximum BMS wet inventory stock for a
              compound. It is not recommended to use with non-chemical
              primary terms.

     Symbol - 'sym' or -symbol, -addsymbol, -addsym. Shows the 'best'
              gene symbol for the output term. If you wish to apply
              this to the input term, use instead 'symin' or -symin.

    Species - 'tax','spec' or -addtaxa, -addspecies. Reports the
              scientific species name for the output
              term. Occasionally some identifiers have multiple
              species assigned to them (Homologene, older Swiss-Prot
              entries). Do not confuse with the -taxa parameter, which
              is used to provide a species filter (eg to filter
              results to only hits to Homo sapiens). To apply to the
              input term, use 'taxin' or -taxin.

      Links - 'link','url'. Will provide a set (possibly large) of
              hyperlinks relevant to the term.

        Set - 'set' or -addset. Will iterate all conceptual sets that
              the term is associated with. Note that the -set
              parameter will do the same, but will simultaneously
              define the queries being used.

 Description - 'desc' or -desc, -adddesc, -description. Description
              columns are provided by default for some querie modes
              (assignments, description), but can be added to other
              tables using these tokens. The parameters above apply to
              the output term, for the input term use 'descin' or
              -descin.

In addition, if a -custcol request contains an entry of format
GO:#######, then it will be interpreted as a GO membership request,
similar to -flaggo.

=head3 Linking / Formatting cells

If using a format with HTML content, you can provide arguments that
specify templates for embedding column contents in hyper links. The
general format is:

 -link_col_COLNAME http://example.com/foo.cgi?arg=__COLNAME__

... where COLNAME is any of the GenAcc recognized column tokens (see
"Column Specifiers" above), and __COLNAME__ (a column name flanked by
double underscores) is a GenAcc column token variable placeholder. As
an explicit example, if you provide:

 -link_col_termout http://example.com/__termout__/foo.cgi?val=__score__

... and you have a row where term_out = "HotDog" and matched = 0.934,
then the term_out cell of that row will be replaced with:

  <a href='http://example.com/HotDog/foo.cgi?val=0.934'>HotDog</a>

The template can be specified in a variety of ways:

=head4 Full HTML tag

If the template begins with a lessthan sign (<) then it is assumed that the template is complete and should not be altered in any way. This allows you to provide complex HTML tags, such as:

 <a href="javascript:show_value('__termout__','__auth__')">__termout__</a>

 <span class='__nsout__'>__termout__</span>

=head4 Image SRC tag

You can cause the cell to load an image from a remote source by passing a template starting with src=

  src='render_img.pl?id=__termout__'

This will be automatically converted to:

  <img src='render_img.pl?id=__termout__' />

=head4 Simple HREF

Any other argument will be interpreted as an HREF for an anchor, with the visible text of the anchor being the original column text. So if column term_out is provided the link template:

  foo.pl?in=__termin__&out=__termout__

... it will be converted to:

  <a href='foo.pl?in=__termin__&out=__termout__'>__term_out__</a>

=head2 Debugging

  -verbose Default 1 if -nocgi is true, otherwise 0. If true, a few
           more status messages will be displayed during operation.

     -warn Default 0. If true, then the program will be fairly chatty
           about what it is doing. Useful to see where it is spending
           time for long calculations.

    -trace Similar to -warn, adds recursion tracking when following
           chained conversions.

  -cloudwarn Similar to -warn, but provides details on cloud
           generation logic. For maximal fun, pass 'detail clean build
           delete' as the argument.

  -dumpsql Default 0. If set to a non-zero number, some SQL statements
           will be shown before they are executed. In general, 1 will
           show only 'important' statements, while 3 will flood the
           screen with nearly all statements being executed.

  -slowsql Default 0. If set to a non-zero number, SQL statements that
           take longer than this amount of time (seconds) will be
           printed to STDERR. Will be overridden by -dumpsql.

 -benchmark Default 0. If true will display benchmarking times at the
            end of program execution. If a non-1 integer, will limit
            benchmarks shown to those that consume a greater
            percentage of the passed value (eg, a value of 25 will
            only show methods that use 25% or more of elapsed time).

      -help Also -h. Show this help documentation. Can also be seen by
            running the program with no options (in command line mode).

    -quiet Default 0. If true, forces -verbose to 0 and prevents help
           being shown when null queries are presented. Mostly useful
           for machine-run scripts.

=head3 Useful queries

 select * from v_wait where minutes != 0;
 select * from newconversion limit 10;
 select * from newclouds limit 10;
 select * from newmaps limit 10;
 select * from newdescription limit 10;
 select * from newparentage limit 10;
 select * from newparents limit 10;
 select * from newchildren limit 10;
 select * from activity;

All on one line:

 select * from v_wait where minutes != 0; select * from newconversion limit 10; select * from newclouds limit 10; select * from newmaps limit 10; select term, ns, description from newdescription limit 10; select * from newparentage limit 10; select * from newparents limit 10; select * from newchildren limit 10; select * from activity ORDER BY type;

=cut
