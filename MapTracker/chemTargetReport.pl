#!/stf/biobin/perl -w

BEGIN {
    # Needed to make my libraries available to Perl64:
    # use lib '/stf/biocgi/tilfordc/released';
    use lib '/stf/biocgi/tilfordc/patch_lib';
    # Allows usage of beta modules to be tested:
    my $progDir = join(' ', $0, `pwd`);
    if ($progDir =~ /(working|perllib)/) {
        require lib;
        import lib '/stf/biocgi/tilfordc/perllib';
    }
    $| = 1;
    print '';
}

my $VERSION = 
    ' $Id$ ';

=head1 Usage

Just for convienence:

 alias ctr maptracker/MapTracker/chemTargetReport.pl

Simple single column input:

 ctr -file foo.xls -header 0 -colnum 1

Full spreadsheet:

 ctr -paramfile ~/people/wardwelj/basicChemReport.param \
    -file myInput.xls -colname Compound -output myOutput.xls

Load a new set:

 ctr -load mySet.list -makeset -set SetName -colnum 1

 ctr -load ~/people/wardwelj/CombinedLibrary2010.list \
     -makeset -set AnnotatedCompoundLibrary

=head2 Loading all underlying data

 ctr -load ~/people/wardwelj/CombinedLibrary2010.list 

=cut

use strict;
use BMS::MapTracker::AccessDenorm;
use BMS::MapTracker::SciTegicSmiles;
use BMS::TableReader;
use BMS::ExcelHelper;
use BMS::FriendlyGraph;
use BMS::BmsArgumentParser;
use BMS::Utilities::ReportingUtilities;
use BMS::MapTracker::GenAccService;
use BMS::FractionalFactorial;
use BMS::Utilities::Escape;

my $nodeColor = {
    SMI  => 'silver',
    LL   => 'lime',
    GO   => 'tan',
    SYM  => 'orange',
    BMSC => 'blue',
};

my %sc2col  = ( 1   => [ 4, 1, 'Green',        '#008000'],
                0.9 => [10, 1, 'Dark Green',   '#006400'],
                0.8 => [43, 1, 'Yellow Green', '#9ACD32'], 
                0.7 => [27, 1, 'Yellow',       '#FFFF00'],
                0.6 => [44, 1, 'Yellow Orange','#FFCC00'],
                0.5 => [45, 1, 'Orange',       '#FFA500'],
                0.4 => [46, 1, 'Dark Orange',  '#FF8C00'],
                0.3 => [ 3,43, 'Red',          '#FF0000'],
                0.2 => [ 9,43, 'Brick',        '#B22222'],
                0.1 => [29,43, 'Purple',       '#800080'],
                0   => [ 1, 2, 'Black',        '#000000'],
                -1  => [15,29, 'Gray',         '#808080'], );

my $specCol = {
    0 => 0,
    0.1 => 0.1,
    0.2 => 0.6,
    0.3 => 0.6,
    0.4 => 0.8,
    0.5 => 0.8,
    0.6 => 0.9,
    0.7 => 0.9,
    0.8 => 1,
    0.9 => 1,
    1   => 1,
};

my $safe  = '9 Sep 2009';
# my $picky = '4pm Oct 27 2010';
my $picky = '4 July 2013';
my $fPrfx = "CTR-Load";

my $args = BMS::BmsArgumentParser->new
    ( -nocgi     => $ENV{HTTP_HOST} ? 0 : 1,
      -taxa      => 'Homo sapiens',
      -algorithm => 'ClustalW',
      -numhits   => 5,
      -evalue    => 0.00001,
      -minp      => 0.1,
      -perc      => 90,
      -header    => 1,
      -primary   => 'Mus musculus',
      -keepmutant => 0,
      -verbose    => 1,
      -keepundef  => 1,
      -keepsmall  => 2,
      -ageall     => $picky,
      -age        => $picky,
      -singleok   => 1,
      -pvalset    => 1,
      -maxorth    => 10000,
      -fork       => 20,
      -seq        => '', );

$args->shell_coloring();

my $gname     = $args->val(qw(GRAPHNAME)) || "ChemBio6";
my $twName    = 'ChemTargReport';
my $isBeta    = ($0 =~ /working/ || `pwd` =~ /working/) ? 1 : 0;
my $graph     = BMS::FriendlyGraph->new();
my $searcher  = BMS::FriendlyGraph->new();
my $log10     = 1 / log(10);
my $nocgi     = $args->val(qw(NOCGI));
my $vb        = $args->val(qw(VERBOSE));
my $limit     = $args->val(qw(LIMIT)) || 0;
my $forkNum   = $args->val(qw(populate fork));
my $keepMut   = $args->val(qw(KEEPMUTANT));
my $dumpSql   = $args->val(qw(DUMPSQL));
my $explain   = $args->val(qw(explain)) ? 1 : 0;
my $doWarn    = $args->val(qw(WARN));
my $usePval   = $args->val(qw(setpval pvalset));
my $statTag   = $usePval ? 'pV' : 'LOD';
my $l10f      = \&log10fmt;
my $noNull    = $args->val(qw(KEEPUNDEF)) ? 0 : 1;
my $noClusOk  = $args->val(qw(SINGLEOK));
my $ifile     = $args->val(qw(path file input idfile idlist));
my $colNumRq  = $args->val(qw(COLUMN colnum));
my $poorOrth  = $args->val(qw(POORORTH)) ? 1 : 0;
my $tm        = $args->val(qw(testmode tm));
my $colNameRq = $args->val(qw(colname col cols));
my $sheetReq  = lc($args->val(qw(SHEET)) || "");
my $indSheeet = $args->val(qw(BYSHEET)) ? 1 : 0;
my $loadFile  = $args->val(qw(load loadfile));
my $captureC  = $args->val(qw(capture));
my $goSheet   = $args->val(qw(gosheet));
my $clobber   = $args->val(qw(clobber));
my $keepSmall = $args->val(qw(keepsmall));
my $cmpdSet   = $args->val(qw(set));
my $select    = $args->val(qw(select selection));
my $noStrip   = $args->val(qw(nostrip notrim noskim)) || 0;
my $cvColName = "CTR-CV";
$sheetReq     =~ s/[^a-z0-9]+/_/gi;
my $basePath  = $args->val(qw(xml output outfile)) ||
    "/stf/biohtml/tmp/ChemTargets_$$.xgmml";
$basePath =~ s/\.\S{3,7}$//;

my $esc = BMS::Utilities::Escape->new();

my %autoSets;
if ($captureC) {
    my @cols = ref($captureC) ? @{$captureC} : split(/[\n\r]/, $captureC);
    @cols = sort { $a cmp $b } @cols;
    my @good;
    $captureC = {};
    for my $i (0..$#cols) {
        my $c = $cols[$i];
        $c =~ s/^\s+//;
        $c =~ s/\s+$//;
        my ($min, $max) = (0, 100);
        my ($captype, $thresh);
        if ($c =~ /(.+?)\s+MakeSet\s+([<>])\s*([\+\-]?(\d+|\d*\.\d+))\s*$/) {
            # Request to automatically build a set
            ($c, $captype, $thresh) = ($1, $2, $3);
        }
        if ($c =~ /^(.+?)\s+\[\s*(\-?\d+)\s*to\s*(\-?\d+)\s*\]\s*$/) {
            # "Some Value [min to max]"
            ($c, $min, $max) = ($1, $2, $3);
        }
        unless ($c) {
            $args->err("-capture request '$cols[$i]' is null");
            next;
        }
        # warn "'$c' = [$min,$max] + ($captype, $thresh)";
        push @good, $c;
        my $inv = $min > $max ? 1 : 0;
        ($min, $max) = ($max, $min) if ($inv);
        my $rng = $max - $min;
        $captureC->{$c} = {
            invert => $inv,
            name   => $c,
            min    => $min,
            max    => $max,
            range  => $rng,
            capset => $captype,
            thresh => $thresh,
        };
        if ($captype) {
            $autoSets{$c} = {
                t => $captype,
                v => $thresh,
                h => {},
            };
        }
    }
    $captureC->{$cvColName} = {
        invert => 1,
        name   => "CV",
        min    => 0,
        max    => 1,
        range  => 1,
    };
    $captureC->{cols} = \@good;
}

my @typePref = ('Ki','IC50','EC50','% Inhibition','%Ctrl 1uM','Generic');

$graph->name($gname);
$searcher->name($gname);
map { $graph->attribute_type($_, 'real') } 
(qw(score invScore clusterSize smilesCount), @typePref);
$graph->attribute_type('Generic', 'string');
my $dbi = $graph->dbi;


if ($colNumRq && $colNumRq !~ /^\d+$/) {
    if ($colNameRq) {
        $args->msg("[!]", "Ignoring non-numeric -column request '$colNumRq' in favor of -colname '$colNameRq'");
    } else {
        $colNameRq = $colNumRq;
        $colNumRq = undef; 
    }
}

my $niceCol   = 
    $colNumRq ? "Column # $colNumRq" :
    $colNameRq ? $colNameRq : "";

my $ru        = BMS::Utilities::ReportingUtilities->new();
my $ad        = BMS::MapTracker::AccessDenorm->new
    ( -age      => $args->{AGE},
      -ageall   => $args->{AGEALL},
      -cloudage => $args->{CLOUDAGE} );
my $ageDays  = int(0.5 + 10 * $ad->age()) / 10;
$ad->{CLOUDWARN} = $args->{CLOUDWARN};
$ad->set_specific_age(undef, 'TAX', $safe);
my $mt        = $ad->tracker;

my (%targs, %clusters, %cl2sym, %cl2desc, %symbolCache, %goCache, %cl2loci,
    %cl2go, %descCache, $oTaxaTag, $userSelection, %links, @uSelect, %unknown, 
);

# die &cached_desc('XLG-143194');

&init();

my $edgeFilter = $args->val(qw(edgefilter));
unless (defined $edgeFilter) {
    $edgeFilter = <<EOF;
Ki           >= 0.5  OR
EC50         >= 0.5  OR
IC50         >= 0.5  OR
%KD          >= 0.5  OR
% Inhibition >= 0.5  OR
%Ctrl 1uM    >= 0.98 OR
Generic       = -1
EOF

}


my %helpNotes =
    ( "Source Data"         => $ifile,
      "Source Columns"      => $niceCol,
      "Source Sheets"       => $sheetReq ? $sheetReq : 'All available',
      'Mutant Targets'      => "Are ". ($keepMut ? 'kept' : 'excluded'),
      'Undefined Assays'    => "Are ". (!$noNull ? 'kept' : 'excluded'),
      'Poor Orthologues'    => "Are ".($poorOrth ? 'included' : 'discarded'),
      'Unclustered Targets' => "Are ". ($noClusOk ? 'kept' : 'excluded'),
      'FriendlyGraph DB'    => $gname,
      'Max Data Staleness'  => "$ageDays Days",
      );

my (%blackList, $whiteList);
if (my $blArr = &read_cmpd_list( $args->val(qw(blacklist)), 'Blacklist')) {
    map { $blackList{$_} = 1 } @{$blArr};
    my $num = $#{$blArr} + 1;
    &ts_message("Blacklisted a total of $num compounds");
    $helpNotes{Blacklist} = "$num compounds from ".$args->val(qw(blacklist));
}

if (my $wlArr = &read_cmpd_list( $args->val(qw(whitelist)), 'Whitelist')) {
    $whiteList = {};
    map { $whiteList->{$_} = 1 } @{$wlArr};
    my $num = $#{$wlArr} + 1;
    &ts_message("Whitelisted a total of $num compounds");
    $helpNotes{Whitelist} = "$num compounds from ".$args->val(qw(whitelist));
}

my @gasCom = ( -age      => $args->val(qw(ageall age)),
               -cloudage => $args->val(qw(ageall cloudage)),
               -ageall   => $args->val(qw(ageall)),
               -format   => 'tsv',
               -fork     => $forkNum,
               -verbose  => 0,
               -keepcomma => 1,
               -warn     => 0,
               -quiet    => 1,
               -scramble => 1);


if ($keepSmall) {
    $helpNotes{"Specific Genes"} = "Genes with ".($keepSmall == 1 ? "only a single compound" : "$keepSmall or fewer compounds"). " are considered 'significant' regardless of actual pValue";
}

my $revOps = {
    '<'  => '>',
    '<=' => '>=',
    '='  => '=',
    '=<' => '>=',
    '=>' => '<=',
    '>'  => '<',
    '>=' => '<=',
};

my $expForm    = '';# 0.00e-0'; # '0.00E+00''##0.0E+0'
my $percForm   = '0%';
my $valFormats = {
    Ki             => [ $l10f,         5,  'log',    'KI',        $expForm ],
    EC50           => [ $l10f,         5,  'log',    'EC50',      $expForm ],
    IC50           => [ $l10f,         5,  'log',    'IC50',      $expForm ],
    'Generic'      => [ \&defaultfmt, -1,     '',        '',       ],
    '%KD'          => [ \&percfmt,    50, 'perc',    'PERCKD',    '0.0%' ],
    '% Inhibition' => [ \&percfmt,    50, 'perc',    'PERCINHIB', '0.0%' ],
    '% Set'        => [ \&percfmt,    50, 'perc',    'PERCSET',   '0.0%' ],
    'LOD'          => [ \&lodfmt,      5,  'log',    'ODDSSET',   '0.00' ],
    'pV'           => [ $l10f,         5,  'log',    'PVSET',     $expForm ],
    '%Ctrl 1uM'    => [ \&invpercfmt,  5, 'invperc', 'PERCCTRL',  '0.0%' ],
};



# $dbi->extract_parenthetical_test($edgeFilter); die;

my @tcGO = qw(GO:0003707 GO:0004672 GO:0004721 GO:0004842 GO:0004888 GO:0004930 GO:0005216 GO:0005576 GO:0006986 GO:0008233 GO:0016651 GO:0031012);

my %goDesc;
foreach my $go (@tcGO) {
    $goDesc{$go} = $ad->description
        ( -id => $go, -ns1 => 'GO' ) || 'Unknown GO Term';
}

my %filters;
while (my ($vk, $vv) = each %{$valFormats}) {
    my ($func, $def, $type, $key) = @{$vv};
    next unless ($key);
    my $val = defined $args->{$key} ? $args->{$key} : $def;
    if ($val) {
        if ($val < 1) {
            # Already in fractional form
            $filters{$vk} = $val;
        } elsif ($type eq 'log') {
            $filters{$vk} = $val / 10;
        } elsif ($type eq 'perc') {
            $filters{$vk} = $val / 100;
        } elsif ($type eq 'invperc') {
            $filters{$vk} = (100 - $val) / 100;
        }
        if ($filters{$vk}) {
            if ($filters{$vk} > 1) {
                $filters{$vk} = 1;
            } elsif ($filters{$vk} <= 0) {
                delete $filters{$vk};
            }
        }
    }
}

$valFormats->{MapTracker} = [ \&mtfmt,    50, 'perc',    'MTSCORE', '0.00' ];
%filters = () if ($args->{NOFILTERS});

my $msgFH     = *STDOUT;
my $outFH     = *STDOUT;

$args->message_output( $msgFH );

&HTML_START();

my @defaultTaxa = ("Homo sapiens", "Mus musculus",
                   "Rattus norvegicus", "Canis lupus familiaris" );

my $treqs = $args->{PRIMARY} || "Homo sapiens";
$treqs = join(',', @defaultTaxa) if ($loadFile);
$treqs = [ split(/\s*[\t\n\r\,]+\s*/, $treqs) ] unless (ref($treqs));
my @primary;
foreach my $treq (@{$treqs}) {
    my @gt = map { $_->name } $mt->get_taxa($treq);
    if ($#gt == -1) {
        &msg( "Failed to find any taxa for '$treq'");
    } elsif ($#gt == 0) {
        push @primary, $gt[0];
    } else{ 
        &msg("Multiple taxae found for '$treq': ".join(", ", @gt));
    }
}
my %isPrimary = map { $_ => 1 } @primary;
$helpNotes{'Species Priority'} = join(' > ', @primary);

my $shouldBeCached = 1;
if ($args->val(qw(makeset))) {
    &make_set( &extract_cmpd_ids() );
} elsif (my $geneReq = $args->val(qw(gene cluster))) {
    $geneReq .= " Orthologues" if ($geneReq =~ /^LOC\d+$/);
    &ts_message("Creating report for target '$geneReq'");
    my $c2op =  $dbi->query_edges
        ( -desc     => "Find all compounds for a given gene",
          -graph    => $gname,
          -node     => $geneReq,
          -edgeattr => $edgeFilter,
          -nodeattr => "ns = BMSC",
          -explain  => $explain,
          -dumpsql  => $dumpSql );
    my $inSet  = &set_members( $cmpdSet );
    my %ids;
    foreach my $row (@{$c2op}) {
        my ($n1, $n2) = @{$row};
        my $cmpd = $n2 eq $geneReq ? $n1 : $n2;
        next unless ($inSet->{$cmpd});
        $ids{$cmpd}++;
    }
    &process_list( [ keys %ids ] );
} elsif ($loadFile) {
    &load_fg( &extract_cmpd_ids() );
} elsif (my $lidReqs = $args->val(qw(loadid loadids idload))) {
    my @ids = split(/[\ \,\n\r]+/, $lidReqs);
    &load_fg( \@ids );
} else {
    &process() unless ($#primary == -1);
}

&HTML_INTERFACE();

&msg( $ad->benchmark() ) if ($args->{BENCHMARK});

&HTML_END();

sub extract_cmpd_ids {
    my $tr = BMS::TableReader->new( -limit => $limit );
    $tr->has_header(1) if ($args->val(qw(header hasheader)));
    my $trFmt = $tr->format_from_file_name($loadFile);
    
    unless ($colNumRq || $colNameRq) {
        if ($trFmt eq 'list') {
            $colNumRq = 1;
        } else {
            $args->death("You must indicate the column(s) that contain compound IDs",
                         "Use either -colname (named) or -colnum (numbered)");
        }
    }
    unless ($tr->input($loadFile)) {
        $args->death("Failed to read input from '$loadFile'");
    }
    &msg("Reading IDs from ".$tr->input);
    my @ids;
    foreach my $sheet ($tr->each_sheet) {
        $tr->select_sheet($sheet);
        my $wsn = $tr->sheet_name($sheet);
        my @cnums;
        if ($colNumRq) {
            push @cnums, $colNumRq;
        } elsif ($colNameRq) {
            @cnums = $tr->column_name_to_number
                ( split(/[\,]/, $colNameRq) );
        }
        if ($#cnums == -1) {
            my @head = $tr->header();
            $args->msg("    [NOTE]","None of the columns on Worksheet $wsn are recognized as containing compound IDs:",
                       join(" + ", @head)) unless ($#head == -1);
            next;
        } elsif ($#cnums != 0) {
            $args->msg("  [ERR]","Multiple possible compound columns within $wsn", join('+', @cnums));
            next;
        }
        my $cn = $cnums[0] - 1;
        my @found;
        while (my $row = $tr->next_clean_row()) {
            if (my $id = $row->[$cn]) {
                push @found, $id;
            }
        }
        my %uniq = map {$_ => 1 } @found;
        @found = keys %uniq;
        push @ids, @found;
        &msg("[Sheet]", "$wsn : ".scalar(@found));
    }
    return \@ids;
}

sub process {
    if (my $req = $args->val(qw(id ids))) {
        return &process_list( $req);
    }
    my $tr = BMS::TableReader->new( -limit => $limit );
    $tr->has_header(1) if ($args->val(qw(header hasheader)));
    if (my $raw = $args->{RAW}) {
        die "No code for processing raw tables yet";
        $tr->has_header(0);
    } elsif ($ifile) {
        $tr->format_from_file_name($ifile);
        $tr->input($ifile);
    } elsif (my $cmpds = &get_selections(  )) {
        # Compounds were provided as specified sets
        return &process_data( $cmpds );
    } else {
        return;
    }
    &process_table($tr);
}

sub make_ontology {
    return unless ($nocgi);
    my ($cmpdIn) = @_;
    
}

sub make_set {
    return unless ($nocgi);
    my ($cmpdIn) = @_;
    my $cnum = $#{$cmpdIn} + 1;
    $args->death("You need to specify the set name with -set")
        unless ($cmpdSet);
    $args->death("Can not create a set without any compounds")
        unless ($cnum);
    my $setFile = "CTR-SetFile.tsv";
    unlink($setFile);
    $args->death("Could not remove prior set file", $setFile)
        if (-e $setFile);
    
    my $check = &cached_data
        ( -ids => $cmpdIn, -mode => 'simple', -standardize => 1, 
          -cols => 'term,ns', -output => $setFile);
    my %uniq = map { $_->[0] => 1 } @{$check};
    $cmpdIn = [ keys %uniq ];
    $cnum = $#{$cmpdIn} + 1;
    &ts_message("Creating set '$cmpdSet' with $cnum compounds", $setFile);

    $graph->database->unlock();
    my $node = $graph->node($cmpdSet);
    $node->set_attributes( ns => 'SET' );
    $node->write();
    foreach my $cmpd (@{$cmpdIn}) {
        my $edge = $graph->edge($cmpd, $cmpdSet);
        $edge->set_attributes( type => 'InSet' );
        $edge->write();
    }
    &ts_message("Finished");
}

sub load_fg {
    return unless ($nocgi);
    $shouldBeCached = 0;
    $graph->database->unlock();
    my ($cmpdIn) = @_;
    my %uniq = map {$_ => 1 } @{$cmpdIn};
    $cmpdIn = [ keys %uniq ];

    
    &ts_message("Processing ".($#{$cmpdIn} + 1)." BMS Compounds");
    &msg("  ", "Limiting to $limit") if ($limit && $limit < $#{$cmpdIn} + 1);

    my $ctrFile = "$fPrfx-Input-For-Load.list";
    my @ctrArgs = ( -ids => $cmpdIn, -mode => 'simple', -standardize => 1, 
                    -cols => 'term,ns', -output => $ctrFile);
    my $ctr     = &cached_data( @ctrArgs );
    unless ($clobber) {
        # If we are not already clobbering, we need to make sure that
        # the IDs we are using now are the same as those previously used
        my %need;
        my %prior = map { $_->[0] => 1 } @{$ctr};
        foreach my $in (@{$cmpdIn}) {
            $in =~ s/\s+//g;
            $need{$in}++;
            delete $prior{$in};
        }
        map { delete $need{$_->[0]} } @{$ctr};
        my @needed = keys %need;
        my @extra  = keys %prior;
        unless ($#needed == -1 && $#extra == -1) {
            # Previously stored data is not the same as what we have now
            $clobber = 1;
            system("rm -f $fPrfx-*.tsv");
            system("rm -f $fPrfx-*.tsv.param");
            $ctr = &cached_data( @ctrArgs );
        }
    }

    $ad->bench_start("Load input");
    my (%cmpds, %nstok);
    foreach my $ct (@{$ctr}) {
        my ($chem, $nsName) = @{$ct};
        my $ns = $nstok{$nsName || ""} ||= $ad->namespace_token($nsName);
        next unless ($ns eq 'BMSC');
        my $cdat = $cmpds{$chem} ||= &new_compound($chem, $ns);
    }
    $ad->bench_end("Load input");


    my @cdats = sort { $a->{id} cmp $b->{id} } values %cmpds;
    $args->death("No IDs... no point!?!") if ($#cdats == -1);

    &ts_message( ($#cdats + 1)." unique chemical IDs found" );
    if ($limit && $#cdats + 1 > $limit) {
        @cdats = splice(@cdats, 0, $limit);
        &ts_message( "  Truncated to $limit" );
    }
    my %byNS;
    map { push @{$byNS{$_->{ns}}}, $_ } @cdats;

    if ($args->{HACK}) {
        &ts_message( "Running fast hack" );
        for my $c (0..$#cdats) {
            my $cdat    = $cdats[$c];
            my $cmpd    = $cdat->{id};
            my $cns     = $cdat->{ns};
            my $node    = $graph->node($cmpd);
            $node->set_attributes( foo => 1);
            if ($tm) {
                print $node->to_text();
            } else {
                $node->write();
            }
        }
        &ts_message( "Finished" );
        return;
    }
    if ($args->val(qw(purge))) {
        # Clear all data around the compounds
        &msg("Clearing all data associated with input in ".$graph->name());
        foreach my $cdat (@{$byNS{BMSC} || []}) {
            my $node = $graph->node($cdat->{id});
            # $args->msg("[D]", $node->name());
            $node->delete('deep');
        }
    }

    $ad->bench_start("Expand input");
    # Map each entry to SMILES:
    while (my ($ns, $cds) = each %byNS) {
        if ($ns eq 'SMI') {
            map { $_->{smi} = [ $_->{id} ] } @{$cds};
            next;
        }
        my @cids = map { $_->{id} } @{$cds};
        my $smis = &cached_data
            ( -ids => \@cids, -ns1 => $ns, -ns2 => 'SMI',
              -directonly => 1,
              -cols   => 'termin,termout', -nonull => 1,
              -output => "$fPrfx-$ns-to-SMI.tsv");
        my %smap;
        foreach my $row (@{$smis}) {
            my ($cmpd, $smi) = @{$row};
            push @{$smap{$cmpd}}, $smi;
            my $edge = $graph->edge($cmpd, $smi);
            $edge->set_attributes( type => 'SMI', direct => 1 );
        }
        # map { $_->{smi} = $smap{ $_->{id} } || [] } @{$cds};
        my @mults;
        foreach my $cdat (@{$cds}) {
            my $smis = $smap{ $cdat->{id} } || [];
            $cdat->{smi} = $smis;
            if ($#{$smis} > 0) {
                push @mults, sprintf("%s : %s", $cdat->{id}, join(",", @{$smis}));
            }
        }
        unless ($#mults == -1) {
            $args->msg("[!!]", "Some query compounds have multiple SMILES designations", @mults);
        }

    }
    
    # Now expand each SMILES to related parents and children:
    my %expand = map { $_ => { $_ => 1 } } map { @{$_->{smi}} } @cdats;
    my @exIDs  = keys %expand;
    &ts_message(($#exIDs + 1)." direct SMILES found");
    my $smiPar = &cached_data
        ( -ids => \@exIDs, -ns1 => 'SMI', -mode => 'parent',
          -cols   => 'child,parent', -depth => 1,
          -output => "$fPrfx-SMI-Parents.tsv");
    my $smiKid = &cached_data
        ( -ids => \@exIDs, -ns1 => 'SMI', -mode => 'child',
          -cols   => 'parent,child', -depth => 1,
          -output => "$fPrfx-SMI-Children.tsv");
    foreach my $arr ($smiPar, $smiKid) {
        foreach my $pair (@{$arr}) {
            my ($smi, $relative) = @{$pair};
            $expand{$smi}{$relative} = 1;
        }
    }
    my %full;
    foreach my $smi (keys %expand) {
        my @relatives = keys %{$expand{$smi}};
        $expand{$smi} = \@relatives;
        map { $full{$_} = 1 } @relatives;
    }
    my @all  = keys %full;

    &ts_message(($#all + 1)." total SMILES found");
    if ($args->{SMIHACK}) {
        &ts_message( "Running fast SMILES hack" );
        for my $m (0..$#all) {
            my $mtid    = $all[$m];
            my $smi     = $mt->get_seq($mtid)->name;
            my $node    = $graph->node($mtid);
            $node->set_attributes( smiles => $smi );
            if ($tm) {
                print $node->to_text();
            } else {
                $node->write();
                &ts_message( "$mtid = $smi" ) unless ($m % 100);
            }
        }
        &ts_message( "Finished" );
        return;
    }


    $ad->bench_end("Expand input");

    my $smiDesc = &cached_data
        ( -ids => \@all, -ns1 => 'SMI', -mode => 'descr',
          -cols => 'term,desc', -output => "$fPrfx-SMI-Description.tsv");
    foreach my $row (@{$smiDesc}) {
        my ($mtid, $desc) = @{$row};
        $descCache{$mtid} = $desc;
        my $smi = $mt->get_seq($mtid)->name;
        $graph->set_node_attributes( $mtid,
                                     ns => 'SMI',
                                     smiles => $smi,
                                     desc   => $desc,
                                     type   => 'SMILES' );
    }
    
    $ad->bench_start("Find targets");
    # Get all direct targets of the compound
    my $trows = &cached_data
        ( -ids => \@all, -ns1 => 'SMI', -ns2 => 'AP',
          -directonly => 1, -nonull => 1, -nullscore  => -1,
          -cols => 'termin,termout,matched,auth',
          -output => "$fPrfx-SMI-AP.tsv");

    
    my (%tByNS, %smi2targ, %tags, %targHits, %filterCount);
    foreach my $row (@{$trows}) {
        my ($smi, $targ, $sc, $authTxt) = @{$row};
        my ($auth, $tag, $pmids) = &parse_author($authTxt);
        if ($targ =~ /^(.+)\.\d+$/) {
            # GRRRR. Loaded in versioned target IDs by mistake
            my $unv = $1;
            $targ = $unv if ($auth eq 'Ambit');
        }
        if (!$tag) {
            if ($auth eq 'Ambit') {
                # Some compounds were assayed only in concentrations OTHER
                # than 1 um (eg 10 um). These will appear as if they have
                # an undefined (-1) score, and will have no tag. Skip them
                next;
            } else {
                $tag = 'Generic';
            }
        }
        unless ($targs{$targ}) {
            my $ns = $ad->guess_namespace_careful( $targ, 'AP' );
            $targs{$targ} = {
                id  => $targ,
                ns  => $ns,
                smi => {},
            };
            # Organize the targets by namespace
            push @{$tByNS{$ns}}, $targ;
        }
        $targs{$targ}{smi}{$smi} = 1;
        push @{$smi2targ{$smi}{targ}}, [ $targ, $tag, $sc, $auth, $pmids ];
        push @{$targHits{$targ}{$tag}}, $sc;
        $tags{$tag}++;
    }
    my @allt    = sort keys %targs;
    my %needTag = %tags;
    my @alltags;
    foreach my $tag (@typePref) {
        if ($tags{$tag}) {
            push @alltags, $tag;
            delete $needTag{$tag};
        }
    }
    push @alltags, sort { uc($a) cmp uc($b) } keys %needTag;
    &ts_message(($#allt + 1)." total targets found");
    &ts_message(($#alltags + 1)." total tag types found");
    $ad->bench_end("Find targets");

    $ad->bench_start("Find loci");
    # Convert the targets to loci
    my (%t2l, %l2o);
    while (my ($ns, $ts) = each %tByNS) {
        my $targLocs = &cached_data
            ( -ids => $ts, -ns1 => $ns, -ns2 => 'LL', -nonull => 1,
              -output => "$fPrfx-$ns-LL.tsv", 
              -nullscore  => -1, -cols => 'termin,termout,score');
        my %tmaps;
        foreach my $trow (@{$targLocs}) {
            my ($targ, $ll, $sc) = @{$trow};
            $tmaps{$targ}{$sc}{$ll} = 1;
        }
        my $targMeta = &cached_data
            ( -ids => $ts, -ns1 => $ns, -mode => 'simple',
              -output => "$fPrfx-$ns-MetaData.tsv",
              -cols => 'term,sym,taxa,desc',);
        foreach my $mrow (@{$targMeta}) {
            my ($targ, $sym, $taxa, $desc) = @{$mrow};
            if (defined $targs{$targ}{taxa}) {
                $args->msg("[!!]","Multiple metadata rows for $targ");
            } else {
                $targs{$targ}{taxa} = $taxa || "";
                $targs{$targ}{sym}  = $sym  || "";
                $targs{$targ}{desc} = $desc || "";
                $symbolCache{$ns}{$targ} = $targs{$targ}{sym};
                $descCache{$targ} ||= $desc;
            }
        }
        foreach my $targ (@{$ts}) {
            my $tmap = $tmaps{$targ} || {};
            # Choose only best loci for each target
            my ($sc) = sort { $b <=> $a } keys %{$tmap};
            if (defined $sc) {
                my @locs   = sort keys %{$tmap->{$sc}};
                $t2l{$targ} = [ $sc, \@locs ];
                map { $l2o{$_} ||= [] } @locs;
                foreach my $loc (@locs) {
                    my $edge = $graph->edge($targ, $loc);
                    $edge->set_attributes( score => $sc, type => 'T2L' );
                }
            }
            my $key = $targ;
            
            unless (defined $targs{$key}{taxa}) {
                ($key) = $ad->standardize_id($targ, $ns);
                unless (defined $targs{$key}{taxa}) {
                    $args->msg("[!!]","No metadata found for $targ");
                    next;
                }
            }
            if (defined $targs{$key}{taxa}) {
                $graph->set_node_attributes
                    ( $targ,
                      ns   => $ns,
                      type => 'Target',
                      desc => $targs{$key}{desc},
                      sym  => $targs{$key}{sym},
                      taxa => $targs{$key}{taxa} );
            }
        }
    }


    my @dlocs = keys %l2o;
    &ts_message(($#dlocs + 1)." direct loci found");
    $ad->bench_end("Find loci");

    my ($clusterNames) = &find_orthologues
        ( \@dlocs, \%smi2targ, \%l2o, \%t2l );

    $ad->bench_start("Write Compound sheets");
    my $gmlID = 0;
    my %allCmpd; map { push @{$allCmpd{$_->{ns}}}, $_->{id} } @cdats;
    while (my ($cns, $list) = each %allCmpd) {
        my $dRows = &cached_data
            ( -id => $list, -ns1 => $cns, -mode => 'description',
              -cols   => 'term,desc', 
              -output => "$fPrfx-All$cns-Description.tsv");
        foreach my $row (@{$dRows}) {
            $descCache{$row->[0]} ||= $row->[1];
        }
    }
    
    for my $c (0..$#cdats) {
        my $cdat    = $cdats[$c];
        my $cmpd    = $cdat->{id};
        my $cns     = $cdat->{ns};
        my @dirSmi  = @{$cdat->{smi}};
        my %fsh     = map { $_ => 1 } map { @{$expand{$_}} } @dirSmi;
        my @fullSmi = sort keys %fsh;
        my $smiN    = ($#fullSmi + 1) || '';
        my $sDesc   = &cached_desc($cmpd, $cns);
        $graph->set_node_attributes
            ( $cmpd, 
              ns             => $cns,
              type           => "Compound",
              smilesCount    => $smiN,
              desc           => $sDesc );

        if ($#fullSmi == -1) {
            my $edge = $graph->edge($cmpd, "No Structure");
            $edge->set_attributes( type => 'Null');
            next;
        } else {
            my $node = $graph->node($cmpd);
            $node->delete_edge("No Structure");
        }
        my %clusterData;
        foreach my $smi (@fullSmi) {
            my $edge = $graph->edge($cmpd, $smi);
            $edge->set_attributes( type => 'SMI' );

            while (my ($cn, $cd) = each %{$smi2targ{$smi}{clus}}) {
                while (my ($key, $tagH) = each %{$cd}) {
                    my $h = $clusterData{$cn}{$key} ||= {};
                    while (my ($tag, $sc) = each %{$tagH}) {
                        if (!defined $h->{$tag} || $h->{$tag} < $sc) {
                            $h->{$tag} = $sc;
                        }
                    }
                }
            }
        }
        my @cns = sort sort_mixed keys %clusterData;

        if ($#cns == -1) {
            my $edge = $graph->edge($cmpd, "No Targets");
            $edge->set_attributes( type => 'Null');
            next;
        } else {
            my $node = $graph->node($cmpd);
            $node->delete_edge("No Targets");
        }

        foreach my $cn (@cns) {
            my $dd = $clusterData{$cn};
            my ($best, @eAttr);
            foreach my $key (@alltags) {
                my $sc = $dd->{tags}{$key};
                if (defined $sc) {
                    $best = $sc if (!defined $best || $sc > $best);
                    push @eAttr, ($key => $sc);
                }
            }
            while (my ($tx, $sc) = each %{$dd->{taxs}}) {
                push @eAttr, ($tx => $sc) if (defined $sc);
            }
            next unless (defined $best);
            my $nm = $clusterNames->{$cn} ||
                ($cn =~ /^\d+$/ ? "Orth Group $cn" : $cn);
            my $edge = $graph->edge($cmpd, $nm);
            $edge->set_attributes
                ( score    => $best,
                  type     => 'Activity',
                  @eAttr );
        }
    }
    $ad->bench_end("Write Compound sheets");
    
    if (my $xml = $args->val(qw(xml output outfile))) {
        unless ($xml =~ /\.xgmml$/) {
            $xml =~ s/\.[^\.]{3,7}$//;
            $xml .= '.xgmml';
        }

        $graph->attribute('label', "Chem Target Report");
        $graph->attribute('stampLabel', 'true');
        if ($args->{NOGO}) {
            foreach my $node ($graph->each_node) {
                my $ns = $node->attribute('ns');
                $graph->delete_node($node) if ($ns && $ns eq 'GO');
            }
        }
        if (open(XML, ">$xml")) {
            print XML $graph->to_xgmml;
            close XML;
            $args->msg('[XML]',"XGMML written to $xml");
        } else {
            $args->err("Failed to write XML document", $xml, $!);
        }
    } else {
        &ts_message("Writing graph to database");
        my %byType;
        map { push @{$byType{ $_->type() }}, $_ } $graph->all_objects();
        my $prog = 60;
        foreach my $type ( sort { $#{$byType{$a}} <=> $#{$byType{$b}} ||
                                      uc($a) cmp uc($b) } keys %byType) {
            my @objs = @{$byType{$type}};
            my $tNum = $#objs + 1;
            &ts_message("$tNum $type entries");
            my $ti = time;
            my $start = $ti;
            for my $o (0..$#objs) {
                $objs[$o]->write();
                next if (time - $ti < $prog);
                my $done = $o + 1;
                my @msg = ($done, $objs[$o]->name(), 
                           sprintf("%.1f%%", 100 * $done / $tNum));
                if (my $elap = time - $start) {
                    my $remain = ($tNum - $done) * $elap / (60 * $done);
                    push @msg, sprintf("%.1f minutes remain", $remain);
                }
                &ts_message(@msg);
                $ti = time;
            }
        }
        &ts_message("Finished Graph Write");
    }
}

sub parse_author {
    my ($txt, $cmpd, $targ) = @_;
    # [Author, AssayTag, PubMed array]
    my @rv = ($txt || "","", []);
    return @rv unless ($txt);
    if ($rv[0]  =~ /(.+) \[([^\]]+)\]/) {
        $rv[0] = $1;
        $rv[1] = $2;
    }
    if ($rv[0] =~ /(.+) (PMID:\d+)/) {
        $rv[0] = $1;
        $rv[2] = [ $2 ];
    } elsif ($rv[0] =~ /(.+) (\d+)xPubMed/) {
        $rv[0] = $1;
        # Query DB to find the actual PubMed IDs
        push @{$rv[2]}, $ad->all_pubmed_for_chembio
            ( -cmpd => $cmpd, -targ => $targ );
    }
    # print "<pre>FOO $txt = $rv[0] + $rv[1] + ".join(",", @{$rv[2]})."</pre>\n" unless ($rv[0] eq 'Ambit');
    return @rv;
}

sub find_orthologues {
    my ($dlocs, $smi2targ, $l2o, $t2l) = @_;
    return if (!$dlocs || $#{$dlocs} == -1);
    $ad->bench_start("Find orthologues");
    # Convert loci to Orthologues
    my $orows = &cached_data
        ( -id         => $dlocs, -ns1 => 'LL', -ns2 => 'ORTH',
          -min        => $poorOrth ? undef : 0.05,
          -nullscore  => -1, -cols => 'termin,termout,score',
          -output => "$fPrfx-LL-ORTH.tsv");
    my %l2taxa = map { $_->[0] ||= "" => "", 
                       $_->[1] ||= "" => "" } @{$orows};
    delete $l2taxa{""};

    my @fullLoc = keys %l2taxa;
    &ts_message(($#fullLoc + 1)." orthologous loci found");

    my $llMeta =  &cached_data
        ( -id => \@fullLoc, -ns1 => 'LL', -mode => 'simple',
          -cols   => 'term,sym,tax,desc',
          -output => "$fPrfx-LL-MetaData.tsv");
    my %loc = map { $_->[0] => 1 } @{$llMeta};

    &structure_go(\@fullLoc, 'LL');
    
    &ts_message("Organizing orthologous loci");
    &ts_message("GeneOntology data NOT being captured") unless ($goSheet);
    my (%tcount, %allGO);
    foreach my $trow (@{$llMeta}) {
        my ($ll, $sym, $tax, $desc) = @{$trow};
        $symbolCache{LL}{$ll} = $sym || "";
        $descCache{$ll} ||= $desc;
        $l2taxa{$ll}   = $tax;
        $tcount{$tax} += $l2o->{$ll} ? 1 : 0 if ($tax);
        $graph->set_node_attributes( $ll,
                                     ns   => "LL",
                                     type => 'Locus',
                                     desc => $desc,
                                     sym  => $sym,
                                     taxa => $tax );
        if ($goSheet) {
            my $gos = &go_for_obj( [$ll], 'LL' );
            foreach my $dat (@{$gos}) {
                my ($go, $sc, $ec, $auth) = @{$dat};
                my $edge = $graph->edge( $go, $ll );
                $edge->set_attributes( type  => 'GO',
                                       score => $sc,
                                       auth  => $auth,
                                       EC    => $ec );
                $allGO{$go}++;
            }
        }
    }
    if ($goSheet) {
        my @everyGO = keys %allGO;
        &ts_message(($#everyGO + 1)." GeneOntology terms");

        my $goDescRows =  &cached_data
            ( -id => \@everyGO, -ns1 => 'GO', -mode => 'description',
              -cols   => 'term,desc', -output => "$fPrfx-GO-Description.tsv");
        my $goParRows =  &cached_data
            ( -id => \@everyGO, -ns1 => 'GO', -mode => 'parents', -depth => 1,
              -cols   => 'child,parent', -output => "$fPrfx-GO-Parents.tsv");
        map { $descCache{$_->[0]} ||= $_->[1] } @{$goDescRows};
        my %goPar;  map { $goPar{$_->[0]}{$_->[1] || ""} = 1 } @{$goParRows};
        while (my ($go, $pars) = each %goPar) {
            delete $pars->{""};
            $goPar{$go} = [sort keys %{$pars}];
        }
        foreach my $go (@everyGO) {
            my $desc = &cached_desc($go, 'GO');
            my @pars = @{$goPar{$go} || []};
            foreach my $par (@pars) {
                my $edge = $graph->edge( $par, $go, 1 );
                $edge->set_attributes( type  => 'Child' );
            }
            $graph->set_node_attributes( $go,
                                         ns   => "GO",
                                         type => 'GO',
                                         root => $#pars == -1 ? 1 : undef,
                                         desc => $desc, );
        }
    }
    my @cntTx = sort { $tcount{$b} <=> $tcount{$a} ||
                           $a cmp $b } keys %tcount;
    my @taxae = @primary;
    my %gotTx = map { $_ => 1 } @primary;
    my $thits = 0; 
    foreach my $tx (@cntTx) { 
        if ($tcount{$tx}) {
            $thits++;
            push @taxae, $tx unless ($gotTx{$tx}++);
        }
    }
    &ts_message(sprintf("%d distinct taxae, %d with assays", 
                        $#cntTx + 1, $thits));
    $ad->bench_end("Find orthologues");

    $ad->bench_start("Cluster orthologues");
    my %inClust = ( '' => [ 0, -2, "" ] );
    my $cnum = 0;
    my @reuse;

    my %cleanOrth;
    foreach my $row (@{$orows}) {
        my ($l1, $l2, $sc) = @{$row};
        unless ($l2) {
            $cleanOrth{$l1} ||= {};
            next;
        }
        my ($la, $lb) = sort ($l1, $l2);
        $cleanOrth{$la}{$lb} = $sc if (!defined $cleanOrth{$la}{$lb} || 
                                       $cleanOrth{$la}{$lb} < $sc);
    }

    while (my ($lOne, $lhash) = each %cleanOrth) {
        my @lTwos = keys %{$lhash};
        if ($#lTwos == -1) {
            # Locus with no orthologues, it is in its own cluster
            my $cn = shift @reuse || ++$cnum;
            $inClust{$lOne} = [ $cn, -1, $lOne ];
            next;
        }
        foreach my $lTwo (@lTwos) {
            my $sc = $lhash->{$lTwo};
            my $edge = $graph->edge($lOne, $lTwo);
            $edge->set_attributes( score => $sc, type => 'L2L' );

            my ($d1, $d2) = map { $inClust{$_} } ($lOne, $lTwo);
            if (!$d1 && !$d2) {
                # Neither locus is in a cluster, make a new one
                my $cn = shift @reuse || ++$cnum;
                $inClust{$lOne} = [ $cn, $sc, $lTwo ];
                $inClust{$lTwo} = [ $cn, $sc, $lOne ];
            } elsif (!$d1) {
                # L2 in cluster, but not L1
                my $cn = $d2->[0];
                $inClust{$lOne} = [ $cn, $sc, $lTwo ];
            } elsif (!$d2) {
                # L1 in cluster, but not L2
                my $cn = $d1->[0];
                $inClust{$lTwo} = [ $cn, $sc, $lOne ];
            } elsif ($d1->[0] == $d2->[0]) {
                # Both already in same cluster
                # Update linkage with cluster if needed...
                $inClust{$lOne} = [ $d1->[0], $sc, $lTwo ] if ($d1->[1] < $sc);
                $inClust{$lTwo} = [ $d2->[0], $sc, $lOne ] if ($d2->[1] < $sc);
            } else {
                # Both loci are in differing inClust
                # Join the clusters together
                my ($c1, $c2) = sort { $a <=> $b } map { $_->[0] } ($d1, $d2);
                foreach my $ll (keys %inClust) {
                    my $cd = $inClust{$ll};
                    $cd->[0] = $c1 if ($cd->[0] == $c2);
                }
            }
        }
    }
    $ad->bench_end("Cluster orthologues");

    $ad->bench_start("Project compound to clusters");
    # Generate SMILES -> Cluster lookups
    while (my ($smi, $sdat) = each %{$smi2targ}) {
        my $cdat = $sdat->{clus} ||= {};
        my (%targH, %pubmed);
        foreach my $dat (@{$sdat->{targ}}) {
            my ($targ, $tag, $sc, $auth, $pmids) = @{$dat};
            push @{$targH{$targ}{tags}{$tag}}, $sc;
            map { $pubmed{$targ}{$_} = 1 } @{$pmids};
            $targH{$targ}{auth}{$auth} = 1;
            my $ldat = $t2l->{$targ};
            my @locs = (''); # Default null locus
            if (!$ldat || $#{$ldat->[1]} == -1) {
                # This target was not mapped to loci
                if ($noClusOk) {
                    # We are going to put it in it's own pseudo-locus
                    &record_singleton_cluster($targ, $cdat, $tag, $sc);
                    next;
                }
            } else {
                # At least one loci for the target
                @locs = @{$ldat->[1]};
            }
            foreach my $ll (@locs) {
                my $cl = $inClust{$ll} || $inClust{''};
                my $cn = $cl->[0];
                if (!$cn && $noClusOk) {
                    &record_singleton_cluster($ll, $cdat, $tag, $sc);
                    next;
                }
                my $cd = $cdat->{$cn} ||= {};
                my $tx = $l2taxa{$ll} || 'Unknown Species';
                # Make note of the locus
                # push @{$cd->{loc}{$tx}}, $ll;
                # Record the score for this tag in this cluster
                $cd->{tags}{$tag} = $sc
                    if (!defined $cd->{tags}{$tag} || $cd->{tags}{$tag} < $sc);
                $cd->{taxs}{$tx} = $sc
                    if (!defined $cd->{taxs}{$tx} || $cd->{taxs}{$tx} < $sc);
                # Likewise within the species
                $cd->{$tx}{$tag} = $sc
                    if (!defined $cd->{$tx}{$tag} || 
                        $cd->{$tx}{$tag} < $sc);
            }
        }
        while (my ($targ, $dat) = each %targH) {
            my $edge  = $graph->edge($targ, $smi);
            my $auths = join(",", sort keys %{$targH{$targ}{auth}});
            my @tags;
            while (my ($tag, $vals) = each %{$targH{$targ}{tags}}) {
                my ($val) = sort { $b <=> $a } @{$vals};
                push @tags, ( $tag, $val );
            }
            if (my $ph = $pubmed{$targ}) {
                my @ids = keys %{$ph};
                map { s/^PMID:// } @ids;
                push @tags, ("PubMed", "PMID:".
                             join(',', sort { $a <=> $b } @ids));
            }
            $edge->set_attributes( type => 'S2T',
                                   @tags,
                                   auth => $auths );
            # warn $edge->to_text();
        }
    }
    $ad->bench_end("Project compound to clusters");

    # Now flip clusters over
    while (my ($ll, $cd) = each %inClust) {
        my $tx = $l2taxa{$ll} || 'Unknown';
        my $cn = $cd->[0];
        push @{$clusters{$cn}{$tx}}, $ll;
    }
    my @cs = sort sort_mixed keys %clusters;
    &ts_message(($#cs + 1)." orthologue clusters");

    $ad->bench_start("Organize Clusters");
    my %clusterNames;
    foreach my $cn (@cs) {
        next unless ($cn);
        next unless ($cn =~ /^\d+$/);
        my $cd  = $clusters{$cn};
        my ($cNm, $sym, $stx, $sdesc);
        my $num = 0;
        my $ns  = $cn =~ /^\d+$/ ? 'LL' : undef;
        foreach my $tx (@taxae) {
            my $locs = $cd->{$tx};
            if ($locs) {
                $num += $#{$locs} + 1;
                $cNm ||= join(",", sort @{$locs})." Orthologues";
            } else {
                next;
            }
            unless ($sym) {
                $stx = $tx if ($sym = &best_sym($locs, $ns));
            }
            if ($sym && !$sdesc) {
                foreach my $ll (@{$locs}) {
                    $sdesc = &cached_desc($ll, $ns);
                    last if ($sdesc);
                }
            }
        }
        $clusterNames{$cn} = $cNm;

        $num = 0 unless ($ns);
        unless ($stx) {
            ($stx) = sort keys %{$cd};
            if ($stx) {
                $sym   = &best_sym($cd->{$stx}, $ns);
                $sdesc = &cached_desc($cd->{$stx}[0], $ns );
            }
        }
        my @aLL = map { @{$_ || []} } values %{$cd};
        foreach my $ll (@aLL) {
            if (my $edge = $graph->edge($cNm, $ll)) {
                $edge->set_attributes( type => "L2O" );
            } else {
                $args->msg("[ERR]", "Failed to get edge for '$cNm' vs '$ll'");
            }
        }
        if ($goSheet) {
            my $gos = &go_for_obj( \@aLL, 'LL' );
            foreach my $dat (@{$gos}) {
                my ($go, $sc, $ec, $auth) = @{$dat};
                warn "$go -- $cNm" unless ($go =~ /^GO/);
                my $edge = $graph->edge( $go, $cNm );
                $edge->set_attributes( type  => 'GO',
                                       score => $sc,
                                       auth  => $auth,
                                       EC    => $ec );
            }
        }

        $cl2sym{$cn}   = $sym;
        $cl2desc{$cn}  = $sdesc;

        $graph->set_node_attributes
            ($cNm,
             type           => "Cluster",
             clusterSize    => $num, 
             sym            => $sym,
             refTaxa        => $stx,
             desc           => $sdesc,
             ns             => 'SYM', );
    }
    $ad->bench_end("Organize Clusters");

    return (\%clusterNames);
}

sub ts_message {
    return unless ($vb);
    my $dt = $ru->getdate("0HR:0MIN:0SEC MON DAY");
    $args->msg("[$dt]", join(' | ', @_));
}

sub structure_go {
    return unless ($goSheet);
    my ($ids, $ns) = @_;
    my $gos = &cached_data
        ( -id => $ids, -ns1 => $ns, -ns2 => 'GO', -min => 0.1,
          -cols   => 'termin,termout,score,auth', -nonull => 1,
          -output => "$fPrfx-$ns-GO.tsv");
    my %struct;
    foreach my $gr (@{$gos}) {
        my ($id, $go, $sc, $auth) = @{$gr};
        next if ($struct{$id}{$go} && $struct{$id}{$go}[0] > $sc);
        my $ec = $auth || ""; $ec =~ s/\s+\<.+//;
        $struct{$id}{$go} = [$sc, $ec];
    }
    while (my ($id, $gdat) = each %struct) {
        my @vals;
        while (my ($go, $dat) = each %{$gdat}) {
            my ($sc, $ec, $auth) = @{$dat};
            if ($ec =~ /(\S+) \[(.+)\]/) {
                ($ec, $auth) = ($1, $2);
            }
            push @vals, [$go, $sc, $ec, $auth];
        }
        $goCache{$ns}{$id} = \@vals;
    }
}

sub go_for_obj {
    my ($ids, $ns) = @_;
    my %rvh;
    foreach my $id (@{$ids}) {
        foreach my $dat (@{$goCache{$ns}{$id} || []}) {
            my ($go, $sc, $ec, $auth) = @{$dat};
            next if ($rvh{$go} && $rvh{$go}[0] > $sc);
            $rvh{$go} = [$sc, $ec, $auth];
        }
    }
    my @rv;
    while (my ($go, $dat) = each %rvh) {
        my ($sc, $ec, $auth) = @{$dat};
        push @rv, [$go, $sc, $ec, $auth];
    }
    return wantarray ? @rv : \@rv;
}

sub new_compound {
    my ($chem, $ns) = @_;
    return {
        id   => $chem,
        ns   => $ns,
        # rows => [],
        smi  => [],
        data => {},
    };
}

sub process_list {
    my $text  = shift || "";
    my @ids   = ref($text) ? @{$text} : split(/\s*[\t\,\n\r]\s*/, $text);
    my $cmpds = {};

    map { &add_compound($_, $cmpds) } @ids;
    &get_selections( $cmpds );
    return &process_data( $cmpds );
}

sub add_compound {
    my ($id, $cmpds) = @_;
    my $ns     = $ad->guess_namespace_careful( $id, 'AC' );
    my ($chem) = $ad->standardize_id($id, $ns);
    unless ($chem) {
        $args->msg('[!]', "Failed to standardize compound", "$id [$ns]");
        return undef;
    }
    my $cdat = $cmpds->{$chem} ||= &new_compound($chem, $ns);
    return $cdat;
}

sub process_table {
    my ($tr) = @_;
    &ts_message("Processing ".$tr->input, "LoadData");
    &msg("Limiting to $limit", "LimitAnalysis") if ($limit);
    $ad->bench_start("Load input");
    my $cmpds = {};
    my $snum  = 0;
    $userSelection ||= {};

    foreach my $sheet ($tr->each_sheet) {
        $snum++;
        my $wsn = $tr->sheet_name($sheet);
        if ($sheetReq =~ /^\d+$/) {
            next unless ($snum == $sheetReq);
        } elsif ($sheetReq) {
            my $check = lc($wsn);
            $check =~ s/[^a-z0-9]+/_/gi;
            next unless ($sheetReq eq $check);
        }
        &msg("[Sheet] $wsn");
        my ($sObj, $sInd) = $tr->select_sheet($sheet);
        my (@cnums, $capCols);
        if ($colNumRq) {
            push @cnums, $colNumRq - 1;
        } elsif ($colNameRq) {
            @cnums = $tr->column_name_to_number
                ( split(/[\,]/, $colNameRq) );
        }
        if ($captureC) {
            foreach my $cname (@{$captureC->{cols}}) {
                my @n = $tr->column_name_to_number( $cname );
                if ($#n == -1) {
                    next;
                }
                $capCols ||= {};
                map { $capCols->{$cname}{$_} = 1 }  @n;
            }
            if ($capCols) {
                while (my ($cname, $nh) = each %{$capCols}) {
                    $capCols->{$cname} = [ map { $_ - 1 }
                                           sort { $a <=> $b } keys %{$nh} ];
                }
            }
        }
        my @selectNums;
        if ($select) {
            @selectNums = map { $_ - 1 } $tr->column_name_to_number( $select );
        }
        my $cn;
        if ($#cnums == -1) {
            my @head = $tr->header();
            my @def; map { push @def, $_ if ($_ && $_ !~ /^\s*$/) } @head;
            $args->msg("  [ERR]","Unable to find compound column '$niceCol' for $wsn",
                       join(" + ", @def)) unless ($#def == -1);
        } elsif ($#cnums != 0) {
            $args->msg("  [ERR]","Multiple possible compound columns within $wsn", join('+', @cnums));
            next;
        } else {
            $cn = $cnums[0] - 1;
        }
        #my (@rows, @widths);
        # die $args->branch({ cc => $capCols, as => \%autoSets});
        my $sheetSet = $autoSets{$wsn} ||= {
            t => '*',
            h => {},
        };
        $sheetSet = $sheetSet->{h};
        while (my $row = $tr->next_clean_row()) {
            my $c = $#{$row};
            next if ($c == -1);
            #push @widths, $c;
            #push @rows, $row;
            my $id = defined $cn ? $row->[$cn] : undef;
            if ($id) {
                my $cdat = &add_compound( $id, $cmpds );
                my $chem = $cdat->{id};
                $sheetSet->{$chem} = $chem;
                if ($capCols) {
                    while (my ($cname, $nums) = each %{$capCols}) {
                        my @vals;
                        foreach my $n (@{$nums}) {
                            my $v = $row->[$n];
                            next unless (defined $v);
                            push @vals, $v;
                            my $dat = $autoSets{$cname};
                            if ($dat && 
                                $v =~ /^\s*([\+\-]?(\d+|\d*\.\d+))\s*/) {
                                my $clean = $1;
                                my $thresh = $dat->{v};
                                if ($dat->{t} eq '>' && $clean > $thresh) {
                                    $dat->{h}{$chem} = $clean if
                                        (!defined $dat->{h}{$chem} ||
                                         $dat->{h}{$chem} < $clean);
                                } elsif ($dat->{t} eq '<' && $clean < $thresh) {
                                    $dat->{h}{$chem} = $clean if
                                        (!defined $dat->{h}{$chem} ||
                                         $dat->{h}{$chem} > $clean);
                                }
                            }
                        }
                        push @{$cdat->{data}{$cname}}, @vals;
                    }
                }
            }
            foreach my $n (@selectNums) {
                if (my $v = $row->[$n]) {
                    push @uSelect, $v;
                }
            }
            # push @{$cdat->{rows}}, $row;
        }

        # my ($w) = sort { $b <=> $a } @widths;
        #push @sheets, {
        #    name => $wsn,
        #    col  => $cn,
        #    rows => \@rows,
        #    cols => $w,
        #};
    }
    # &tidy_user_selection();
    $ad->bench_end("Load input");
    &get_selections( $cmpds );
    return &process_data( $cmpds );
}

sub get_selections {
    my $cmpds = shift;
    if (my $explicit = $args->val(qw(selectedids))) {
        my @expl = ref($explicit) ? @{$explicit} :
            split(/\s*[\n\r\t\,]+\s*/, $explicit);
        foreach my $ex (@expl) {
            push @uSelect, $ex unless ($ex =~ /^\s*$/);
        }
    }
    while (my ($cname, $dat) = each %autoSets) {
        my %h = %{$dat->{h}};
        my $t = $dat->{t};
        my @set;
        if ($t =~ />/) {
            @set = sort { $h{$b} <=> $h{$a} || $a cmp $b } keys %h;
        } elsif ($t =~ /</) {
            @set = sort { $h{$a} <=> $h{$b} || $a cmp $b } keys %h;
        } elsif ($t eq '*') {
            # Keep everyhing
            @set = sort keys %h;
        } else {
            $args->msg("[!]","Not sure how to sort values for test '$t'");
            @set = sort { $a cmp $b } keys %h;
        }
        next if ($#set == -1);
        push @uSelect, "SET : $cname";
        push @uSelect, @set;
    }

    return undef if ($#uSelect == -1);
    $cmpds ||= {};

    # Organize IDs into one or more sets
    my $setName = "User Selection";
    foreach my $id (@uSelect) {
        if ($id =~ /^SET\s*:\s*(.+?)\s*$/) {
            # Specifying a new set
            $setName = $1;
            next;
        }
        if (my $cdat = &add_compound( $id, $cmpds )) {
            # Make sure the compound is part of the overall set
            # $id =~ s/\s+//g;
            # Corrected ID name
            my $id = $cdat->{id};
            unless (&_cmpd_blackwhite($id)) {
                $userSelection->{$setName}{$id}++;
                $userSelection->{All}{$id}++;
            }
        }
    }

    my @sets = keys %{$userSelection};
    if ($#sets == 1) {
        # There is really only one set, and we also have "All" there.
        # Get rid of the other set, since we will need to reference "All"
        # in later code
        my %sh = map { $_ => 1 } @sets;
        delete $sh{All};
        map { delete $userSelection->{$_} } keys %sh;
    }
    
    my $fb = "/scratch/$fPrfx-$$-Normalize.tsv";
    system("rm -f $fb");
    my $bmsc = &cached_data
        ( -ids => [keys %{$userSelection->{All}}],
          -ns1 => 'BMSC', -mode => 'simple', -keeporiginal => 1,
          -cols => ['termin','termout'], -output => $fb);
    my %idMap;
    foreach my $row (@{$bmsc}) {
        my ($in, $out) = @{$row};
        $idMap{$in} ||= $out;
    }
    my %prob;
    foreach my $setName (keys %{$userSelection}) {
        my %uniq;
        foreach my $in (keys %{$userSelection->{$setName}}) {
            if (my $out = $idMap{$in}) {
                $uniq{$out}++;
            } else {
                $prob{$in}{$setName}++;
            }
        }
        $userSelection->{$setName} = [ sort keys %uniq ];
    }
    my @pa = sort keys %prob;
    $args->msg("[ERR]", "Some of your set choices could not be recognized",
               map { "$_ (".join(',', sort keys %{$prob{$_}}).")" } @pa)
        unless ($#pa == -1);
    my $all = $userSelection->{All};
    if ($#{$all} == -1) {
        $args->death("Could not find any BMS compound IDs in your capture request");
    } else {
        my @sets = keys %{$userSelection};
        &msg(($#{$all} + 1)." distinct BMS IDs set as selection in ".
             ($#sets+1)." distinct sets");
    }
    return $cmpds;
}

sub statistics {
    my $arr = shift;
    return () unless ($arr);
    my ($n, $sum, $sum2, @nonNumeric);
    foreach my $val (@{$arr}) {
        next unless (defined $val);
        $val =~ s/^\s+//; $val =~ s/\s+$//;
        next if ($val eq '');
        unless ($val =~ /^[\-\+]?(\d+|\d+\.\d+|\.\d+)$/) {
            push @nonNumeric, $val;
            next;
        }
        $n++;
        $sum  += $val;
        $sum2 += $val ** 2;
    }
    my ($avg, $stddev, $cv);
    if ($n) {
        $avg = $sum / $n;
        unless ($n < 2) {
            $stddev = sqrt( abs($n * $sum2 - $sum ** 2)  / ($n * ($n-1)) );
            $cv     = $avg ? int(0.5 + 1000 * abs($stddev / $avg))/1000 :
                $stddev ? undef : 0;
        }
    }
    return ($n, $avg, $stddev, $cv, \@nonNumeric);
}

sub captured_format {
    my ($val, $col) = @_;
    return undef unless ($captureC && $col && defined $val);
    my $dat = $captureC->{$col};
    return undef unless ($dat);
    my $inv  = $dat->{invert};
    my $rng  = $dat->{range};
    my $sc;
    if ($val < $dat->{min}) {
        $sc = $inv ? 2 : - 1;
    } elsif ($val > $dat->{max}) {
        $sc = $inv ? -1 : 2;
    } else {
        my $stnd = $inv ? $dat->{max} - $val : $val - $dat->{min};
        $sc = int(0.5 + 10 * $stnd / $rng) / 10;
        # warn "[$col] $val = $stnd = $sc\n";
    }
    return defined $sc ? "sc$sc".$col : undef;
}

sub add_captured_columns {
    my ($cmpds, $ids) = @_;
    my (@row, @frm);
    foreach my $cn (@{$captureC->{cols}}) {
        my @vals;
        foreach my $cmpd (@{$ids}) {
            # warn "['$cmpd', '$cn']\n";
            push @vals, @{$cmpds->{$cmpd}{data}{$cn} || []};
        }
        my ($n, $avg, $stddev, $cv, $nn) = &statistics( \@vals );
        push @row, ($avg, $cv );
        push @frm, (&captured_format($avg, $cn), 
                    &captured_format($cv, $cvColName));
    }
    return (\@row, \@frm);
}

sub process_data {
    my $cmpds = shift;
    my @cdats = sort { $a->{id} cmp $b->{id} } values %{$cmpds};
    &msg(sprintf("%d unique chemical IDs found", $#cdats + 1));
    return if ($#cdats == -1);


    $graph->attribute('label', "Chem Target Report");
    $graph->attribute('stampLabel', 'true');
    my @edgeCols = qw(cccccc 000000 ff0000 d73027
                      f46d43 fdae61 fee08b eeee00
                      cccc00 668800 339900 33ff00 );


    $graph->attribute_generator('graphics', 'Edge', sub {
        my $edge = shift;
        my $sc   = $edge->generated_attribute('score');
        my $ind  = defined $sc && $sc >= 0 ? 1+ int(10 * $sc) : 0;
        my $grph = {
            fill  => '#' . $edgeCols[$ind],
            width => 4,
        };
        return $grph;
    });

    $graph->attribute_generator('label', 'Node', sub {
        my $node = shift;
        my $ns   = $node->attribute('ns') || '';
        my $lab  = $ns eq 'SYM' ? $node->attribute('sym') : $node->name();
        return $lab || $node->name();
    });

    $graph->attribute_generator('color', 'Node', sub {
        my $node = shift;
        my $ns   = $node->attribute('ns') || '';
        if ($cmpdSet && $ns eq 'SYM') {
            # Color the gene by p-value
            if (my $ex = $node->attribute('LOD : Best')) {
                return '#330066' if ($ex > 0); # Under-enriched
                my $sc = $ex / -10;
                if ($sc > 1) {
                    $sc = 1;
                } elsif ($sc < 0) {
                    $sc = -1;
                }
                $sc = int(0.5 + 10 * $sc) / 10;
                if (my $dat = $sc2col{ $specCol->{$sc} }) {
                    return $dat->[3];
                }
                warn "LOD '$ex' has score '$sc' which is not recognized";
                return 'black';
            }
            return 'gray';
        }
        if ($ns eq 'SYM') {
            return 'green';
        } elsif ($ns eq 'BMSC') {
            return 'cyan';
        }
        return 'white';
    });

    my @filtExcel;
    if ($edgeFilter) {
        my @bits = split(/[\n\r]+/, $edgeFilter);
        foreach my $bit (@bits) {
            if ($bit =~ /(\S.*\S)\s+([\<\>\=]+)\s+([\d\-\+\.]+)\s+(.+)/ ||
                $bit =~ /(\S.*\S)\s+([\<\>\=]+)\s+([\d\-\+\.]+)$/) {
                my ($tag, $op, $v, $xtra) = ($1, $2, $3, $4 || "");
                my ($val, $fmt, $nOp, $plain) = &scValFrm($v, $tag, $op);
                $bit = join(" ", $tag, $nOp, $plain, $xtra);
                # $bit =~ s/\Q$rep\E/$nov/;
            }
            $bit =~ s/\s+/ /g;
            push @filtExcel, $bit;
        }
        $args->msg("Filtering edges from graph:", @filtExcel);
    }
    my @cids; # This will hold all the *recognized* compound IDs
    my (%compoundStyles);
    my $fgTextIds = $dbi->bulk_text2id( map { $_->{id} } @cdats );
    my $doExp = $explain || 0;
    while (my ($id, $tid) = each %{$fgTextIds}) {
        if ($tid) {
            push @cids, $id;
        } else {
            $unknown{$id}{"Chem Report DB"} = "Not in CR DB";
            my $idseq = $mt->get_seq(-defined => 1, 
                                     -nocreate => 1,-id => $id);
            if ($idseq) {
                my $edges = $mt->get_edge_dump
                    ( -name      => $id,
                      -type      => 'is reliably aliased by',
                      -keepclass => 'SciTegic SMILES',
                      -limit     => 1 );
                if ($#{$edges} == -1) {
                    $unknown{$id}{"SMILES Structure"} = "None found";
                    $compoundStyles{$id}{noSmiles} = 1;
                }
            } else {
                $unknown{$id}{"MapTracker"} = "Unknown";
                $compoundStyles{$id}{noSmiles} = 1;
            }
        }
    }
    &msg(sprintf("Analyzing %d known chemical IDs", $#cids + 1));

    # my @cids = map { $_->{id} } @cdats;
    my %needed = map { uc($_) => $_ } @cids;
    my $c2o =  $dbi->query_edges
        ( -desc     => "Find all gene activities for requests",
          -graph    => $gname,
          -node     => \@cids,
          -edgeattr => $edgeFilter,
          -nodeattr => "ns = SYM",
          -explain  => $explain,
          -dumpsql  => $dumpSql );
    foreach my $row (@{$c2o}) {
        my ($n1, $n2) = @{$row};
        $graph->edge($n1, $n2);
        map { delete $needed{uc($_)} } ($n1, $n2);
    }

    my @noSyms = values %needed;
    my %isCmpd = map { uc($_) => 1 } @noSyms;
    unless ($#noSyms == -1) {
        my $c2op =  $dbi->query_edges
            ( -desc     => "Find all non-gene activities for requests",
              -graph    => $gname,
              -node     => \@noSyms,
              -edgeattr => $edgeFilter,
              -nodeattr => "ns != SYM AND ns != SMI",
              -explain  => $explain,
              -dumpsql  => $dumpSql );
        foreach my $row (@{$c2op}) {
            my ($n1, $n2) = @{$row};
            my ($cmpd, $targ) = $isCmpd{uc($n1)} ? ($n1, $n2) : ($n2, $n1);
            unless (exists $isCmpd{uc($cmpd)}) {
                $args->msg("[ERR]","Unclear why edge {$cmpd -- $targ} was recovered in salvage",join(',', @noSyms));
                next;
            }
            my $ns = $ad->guess_namespace($targ);
            unless ($ad->is_namespace($ns, 'AP','AR')) {
                warn "Toss $targ [$ns] via $cmpd\n";
                next;
            }
            $graph->edge($n1, $n2);
            delete $needed{uc($cmpd)};
        }
    }

    $graph->read_all();
    $graph->remove_isolated_nodes() unless ($noStrip);

    my %nodeTypes = (Compound => [], Cluster => [], Target => [] );
    foreach my $node ($graph->each_node) {
        my $type = $node->attribute('type');
        push @{$nodeTypes{$type || ""}}, $node;
    }
    map { delete $needed{uc($_->name)} } @{$nodeTypes{Compound}};
    my %edgeTypes;
    foreach my $edge ($graph->each_edge) {
        my $type = $edge->attribute('type');
        push @{$edgeTypes{$type || ""}}, $edge;
    }

    my @allClus = sort { ($a->attribute('sym') || 'ZZZ') cmp 
                          ($b->attribute('sym') || 'ZZZ') }
    @{$nodeTypes{Cluster}};
    push @allClus,  sort { $a->name() cmp $b->name() }
    @{$nodeTypes{Target}};

    my %actTags;
    foreach my $edge (@{$edgeTypes{Activity}}) {
        map { $actTags{$_}++ } $edge->each_attribute;
    }
    my %activityTaxae;
    foreach my $tx (keys %actTags) {
        # Kinda lame, but assume the entry is a taxa if it has a space
        $activityTaxae{$tx} = $actTags{$tx}
        if ($tx =~ /^[A-Z][a-z]{2,} [a-z]{3,}/);
    }
    my @actTaxae = keys %activityTaxae;
    map { $isPrimary{$_} ||= 0 } @actTaxae;

    my (@tagTaxae, %otherTaxae);
    foreach my $tx (sort { $isPrimary{$b} <=> $isPrimary{$a} ||
                                $activityTaxae{$b} <=> $activityTaxae{$a} ||
                                $a cmp $b } @actTaxae) {
        if ($tx eq 'Canis lupus familiaris' ||
            $tx =~ /^[A-Z][a-z]{2,} [a-z]{3,}$/) {
            # 'well behaved' taxa
            push @tagTaxae, $tx;
        } else {
            $otherTaxae{$tx}++;
        }
    }
    foreach my $clus (@allClus) {
        my $taxa = $clus->attribute('taxa');
        next if (!$taxa || $activityTaxae{$taxa});
        $otherTaxae{$taxa}++;
    }
    my @uOther = sort keys %otherTaxae;
    $oTaxaTag = $#uOther == -1 ? "" : 
        sprintf("%d Other Taxa%s", $#uOther + 1, $#uOther == 0 ? '' : 'e');
    push @tagTaxae, $oTaxaTag if ($oTaxaTag);

    my ($clusHits, $inSet, $ff);
    if ($cmpdSet) {
        # The set is explicitly defined by a reference
        $inSet  = &set_members( $cmpdSet );
        my @got = keys %{$inSet};
        $userSelection ||= {
            All => [ @cids ],
        };
        &ts_message("Using Reference $cmpdSet (".($#got+1).")");
    } elsif ($userSelection) {
        # Use all available IDs from the workbook
        $cmpdSet = "UserSelection";
        $inSet = { map { $_ => 1 } @cids };
        &ts_message("Using all compounds in workbook as reference (".($#cids+1).")");
    }
    # isSelection will report if a compound ID is part of the users selection
    # and if it is valid within the set
    my (@hgdSets, %isSelection, %compoundCounts, %notok);
    if ($inSet) {
        $ff = BMS::FractionalFactorial->new();
        # All compounds in the FULL set:
        my @snum = keys %{$inSet};
        $compoundCounts{FullRefSet} = $#snum + 1;
        &ts_message("Finding all potential compound hits for clusters in set '$cmpdSet' ($compoundCounts{FullRefSet} members)");
        $args->death("Your designated set has no members.",
                     "Maybe you mis-spelled the set name?",
                     "Perhaps the set was never defined and loaded?")
            unless ($compoundCounts{FullRefSet});
        @hgdSets = sort keys %{$userSelection};
        while (my ($sname, $sarr) = each %{$userSelection}) {
            my @ok;
            foreach my $cid (@{$sarr}) {
                $compoundStyles{$cid}{uselect} = 1;
                if ($inSet->{$cid}) {
                    push @ok, $cid;
                    $isSelection{$sname}{$cid} = 1;
                } else {
                    $notok{$cid}{$sname} = 1;
                    $compoundStyles{$cid}{notinset} = 1;
                }
            }
            # All compounds in a SELECTION:
            $compoundCounts{$sname} = $#ok + 1;
        }
        my @nok = sort keys %notok;
        my $nnum = $#nok + 1;
        if ($nnum) {
            $args->msg("[!!]", "$nnum selection members are not in your main reference set", @nok);
            foreach my $n (@nok) {
                $unknown{$n}{"Not in Set"} = join
                    (' + ', sort keys %{$notok{$n}});
            }
        }
        my @cNames = map { $_->name } @allClus;
        my %isClus = map { uc($_) => 1 } @cNames;
        my $c2op =  $dbi->query_edges
            ( -desc     => "Find all hits on each cluster",
              -graph    => $gname,
              -node     => \@cNames,
              -edgeattr => $edgeFilter,
              -nodeattr => "ns = BMSC",
              -explain  => $explain,
              -dumpsql  => $dumpSql );
        $clusHits = {};
        foreach my $row (@{$c2op}) {
            my ($n1, $n2) = @{$row};
            my ($cmpd, $targ) = $isClus{uc($n2)} ? ($n1, $n2) : ($n2, $n1);
            next unless ($inSet->{$cmpd});
            $clusHits->{$targ}{FullRefSet}{$cmpd}++;
        }
        foreach my $targ (keys %{$clusHits}) {
            my @u = keys %{$clusHits->{$targ}{FullRefSet}};
            $clusHits->{$targ}{FullRefSet} = $#u + 1;
            foreach my $sname (@hgdSets) {
                my $count = 0;
                map { $count++ if ($isSelection{$sname}{$_}) } @u;
                $clusHits->{$targ}{$sname} = $count;
            }
        }
    }
    

    my @usedTags;
    map { push @usedTags, $_ if ($actTags{$_}) } @typePref;


    my @needIds = values %needed;
    @needIds = () if ($args->val(qw(skipcheck)));
    my %filtCount;
    unless ($#needIds == -1) {
        &ts_message("Checking ".scalar(@needIds)." target-less compounds");
        my %noTarg;
        
        # Find explicit nulls
        my $knownNulls = $dbi->query_edges
            ( -desc     => "Find explicit null annotations",
              -graph    => $gname,
              -node     => \@needIds,
              -edgeattr => "type = Null",
              -explain  => $explain,
              -dumpsql  => $dumpSql );
        my %recheck = map { $_ => 1 } @needIds;
        foreach my $row (@{$knownNulls}) {
            my ($cmpd, $why) = @{$row};
            ($cmpd, $why) = ($why, $cmpd) if ($cmpd =~ /^No /);
            if ($why eq 'No Targets') {
                $noTarg{$cmpd}++;
                $unknown{$cmpd}{"Unannotated"} = "No targets at all";
                
            } elsif ($why eq 'No Structure') {
                $unknown{$cmpd}{"Unannotated"} = "No structure defined";
            } else {
                $args->msg("[??]","Unrecognized null tag '$why'");
                next;
            }
        }
        
        foreach my $cmpd (keys %recheck) {
            my $check = $dbi->query_edges
                ( -desc     => "Find all activities for requests",
                  -graph    => $gname,
                  -node     => $cmpd,
                  -edgeattr => "type = Activity",
                  -dumpsql  => $dumpSql );
            if (my $num = $filtCount{$cmpd} = $#{$check} + 1) {
                my %acts;
                foreach my $row (@{$check}) {
                    my ($n1, $n2) = @{$row};
                    my $edge = $searcher->edge($n1, $n2);
                    $edge->read();
                    foreach my $key (@usedTags) {
                        my $val = $edge->attribute($key);
                        push @{$acts{$key}}, $val if (defined $val);
                    }
                }
                my @abits;
                foreach my $key (sort keys %acts) {
                    my ($val) = sort { $b <=> $a } @{$acts{$key}};
                    my ($sc, $fmt) = &scValFrm($val, $key);
                    push @abits, "$key=$sc";
                }
                $unknown{$cmpd}{"Target Filter"} = join("; ", @abits);
            } else {
                $noTarg{$cmpd}++;
                $unknown{$cmpd}{"Unannotated"} ||= "Unknown Reason";
            }
        }
        foreach my $cmpd (sort { $filtCount{$b} <=> $filtCount{$a} ||
                                     $a cmp $b } keys %filtCount) {
            my $node = $graph->node($cmpd);
            $node->read;
            push @{$nodeTypes{Compound}}, $node;
        }
        my @noArr = keys %noTarg;
        &ts_message(($#noArr + 1)." compounds confirmed to lack targets");
    }

    my $path = "$basePath.xlsx";

    my $eh = BMS::ExcelHelper->new( $path );
    $eh->format( -name       => 'cen',
                 -align      => 'center', );
    $eh->format( -name       => 'cenbold',
                 -bold       => 1,
                 -align      => 'center', );

    # User-selected IDs:
    $eh->format( -name       => 'uselect',
                 -bold       => 1,
                 -background => 'lime');
    
    # Not in set:
    $eh->format( -name       => 'notinset',
                 -bold       => 1,
                 -color      => 'pink' );

    # User selected, but not in set:
    $eh->format( -name       => 'notinset uselect',
                 -bold       => 1,
                 -color      => 'pink',
                 -background => 'lime');

    # Rows with missing SMILES data:
    $eh->format( -name       => 'noSmiles',
                 -color      => 'red' );

    # Null data, used to capture locus IDs with no information
    $eh->format( -name       => 'nulldata',
                 -align      => 'center',
                 -color      => 'silver' );


    $eh->format( -name       => 'noSmiles uselect',
                 -bold       => 1,
                 -color      => 'red',
                 -background => 'lime');

    $eh->format( -name       => 'hilight',
                 -background => 36 + 7,
                 -color      => 'red' );
    $eh->format( -name       => 'hilightcen',
                 -background => 36 + 7,
                 -align      => 'center',
                 -color      => 'red' );

    # Rows with no hits due to filtering:
    $eh->format( -name       => 'filtCmpd',
                 -background => 'white',
                 -color      => 'orange', );

    $eh->format( -name       => 'filtCmpd uselect',
                 -bold       => 1,
                 -color      => 'orange',
                 -background => 'lime');

    $eh->format( -name       => 'BoldRt',
                 -bold       => 1,
                 -align      => 'right');
    $eh->format( -name       => 'taxa',
                 -italic     => 1,
                 -color      => 'brown', );
    $eh->format( -name       => 'left',
                 -align      => 'left');

    # Some Help formats
    $eh->format( -name       => 'hhead',
                 -color      => 'yellow',
                 -background => 'blue',
                 -bold       => 1 );



    my (@capCols, @capWid);
    if ($captureC) {
        @capCols = map { $_, "CV $_" } @{$captureC->{cols}};
        @capWid  = map { (8, 4) } @{$captureC->{cols}};
    }
    my @bhead = ("Compound", "Cluster", "Sym");
    my @bwid  = (16, 16, 12);
    my @bctfrm  = (undef,undef, undef);
    if ($cmpdSet) {
        push @bhead, "$statTag All";
        push @bwid, 6;
        push @bctfrm, undef;
    }
    my @bdesc = ('Cluster Description', 'Compound Description' );
    my $bCapOffset = $#bhead + 1;
    push @bhead, (@capCols, 'Score', @usedTags, @tagTaxae, @bdesc);
    my %c2tN2I = map { $bhead[$_] => $_ } (0..$#bhead);

    push @bwid, @capWid;
    push @bwid, 6; # Score
    push @bwid, map { 8 }  @usedTags;
    push @bwid, map { 12 } @tagTaxae;
    push @bwid, map { 60 } @bdesc;
    
    my $ctws = $eh->sheet( -name    => 'Compound To Target',
                           -freeze  => 1,
                           -columns => \@bhead,
                           -format  => \@bctfrm,
                           -width   => \@bwid, );
    $ctws->freeze_panes(1, 3);

    my @clhead = ("Cluster", "Sym");
    my @clwid  = (20, 12, 6);
    if ($cmpdSet) {
        push @clhead, "\# in Set ($compoundCounts{FullRefSet})";
        foreach my $setName (@hgdSets) {
            push @clhead, ("\# $setName ($compoundCounts{$setName})", "$statTag $setName");
            push @clwid, (6, 6);
        }
    }
    push @clhead, @capCols;
    push @clwid, @capWid;
    push @clhead, ('Score', @usedTags, @tagTaxae, 
                   "\# Loci", "Compounds", "NS", "Description");
    push @clwid, (map { 8 } @usedTags, map { 12 } @tagTaxae, 
                  6, 10, 8, 60);
    my %clN2I = map { $clhead[$_] => $_ } (0..$#clhead);

    my $clSum = $eh->sheet
        ( -name    => 'Cluster Summary',
          -columns => \@clhead, 
          -format  => [undef,'center',undef,undef,'center'],
          -width   => \@clwid,
          );
    $clSum->freeze_panes(1, 2);

    my $cmpTaxHitTag = "Hit Loci";
    my @chead = ("Compound", @capCols, 'Score', @usedTags,
                 (map { "$_ $cmpTaxHitTag" } @tagTaxae),
                 "SMILES", "NS", "MTID", "# Var", "Variants", "Description");
    my %cN2I = map { $chead[$_] => $_ } (0..$#chead);
    my $cSum = $eh->sheet
        ( -name    => 'Compound Summary',
          -columns => \@chead, 
          # -format  => [undef,'center',undef,undef,'center'],
          -width   => [ 20, @capWid, 6, (map { 8 } @usedTags),
                        (map { 12 } @tagTaxae), 20, 8, 20, 6, 20, 60],
          );
    $cSum->freeze_panes(1, 1);

    # Hide non-primary taxae columns
    my %priTax = map { $_ => 1 } @primary;
    foreach my $tax (@tagTaxae) {
        next if ($priTax{$tax});
        if (my $ind = $c2tN2I{$tax}) {
            $ctws->set_column($ind, $ind, undef, undef, 1);
        }
        if (my $ind = $clN2I{$tax}) {
            $clSum->set_column($ind, $ind, undef, undef, 1);
        }
        if (my $ind = $cN2I{"$tax $cmpTaxHitTag"}) {
            $cSum->set_column($ind, $ind, undef, undef, 1);
        }
        if (my $ind = $cN2I{$tax}) {
            $cSum->set_column($ind, $ind, undef, undef, 1);
        }

    }

    # my @allEdges = $graph->each_edge( -edgefilter => "type == Activity");

    my @scScale = sort { $b <=> $a } keys %sc2col;
    my @scKeyRows;
    foreach my $sc (2, @scScale) {
        my @fparam;
        if ($sc == 2) {
            # Not relevant
        } else {
            my ($bg, $fg) = @{$sc2col{$sc}};
            @fparam = ( -name       => "sc$sc",
                        -background => 7 + $bg,
                        -align      => 'center',
                        -color      => 7 + $fg );
        }
        $eh->format( @fparam ) unless ($#fparam == -1);
        my $mtfrm = 'center';
        if (my $vf = $valFormats->{MapTracker}) {
            if (my $nform = $vf->[4]) {
                my $fn = join('', 'sc', $sc, 'MapTracker');
                $eh->format( @fparam,
                             -name => $fn,
                             -num_format => $nform );
                $mtfrm = $fn;
            }
        }
        my @frm = ($mtfrm);
        my @vals = ($sc == 2 ? undef : $sc);
        foreach my $tag (@usedTags) {
            if ($sc == 2) {
                push @frm, undef;
                push @vals, undef;
                next;
            }
            if (($sc == -1 && $tag ne 'Generic') ||
                ($sc != -1 && $tag eq 'Generic')) {
                push @frm, undef;
                push @vals, undef;
                next;
            }
            my $v = "?";
            my $fn = "sc$sc";
            if (my $vf = $valFormats->{$tag}) {
                $v = &{$vf->[0]}($sc);
                if (my $nform = $vf->[4]) {
                    $fn .= $tag;
                    $eh->format( @fparam,
                                 -name => $fn,
                                 -num_format => $nform );
                }
            }
            push @frm, $fn;
            push @vals, $v;
        }
        if ($cmpdSet) {
            my $tag = $statTag;
            my ($fn, $v);
            if ($sc >= 0 && $sc <= 1) {
                $fn = "sc$sc";
                if (my $vf = $valFormats->{$tag}) {
                    $v = &{$vf->[0]}($sc);
                    my @override;
                    my $scsc = $specCol->{$sc};
                    if (defined $scsc) {
                        my ($bg, $fg) = @{$sc2col{$scsc}};
                        @override = ( -background => 7 + $bg,
                                      -color      => 7 + $fg );
                    }
                    if (my $nform = $vf->[4]) {
                        $fn .= $tag;
                        $eh->format( @fparam,
                                     @override,
                                     -name => $fn,
                                     -num_format => $nform );
                        # Also add the Under-Enriched format
                        $eh->format( @fparam,
                                     @override,
                                     -color => 'pink',
                                     -name => $fn."UE",
                                     -num_format => $nform );
                    }
                }
            }
            push @frm, $fn;
            push @vals, $v;
        }
        if ($captureC) {
            # Color indices we are not currently using:
            my @cSlots = (39..42,47..52,54..56);
            foreach my $cn (@{$captureC->{cols}}, $cvColName) {
                my $dat = $captureC->{$cn};
                my $inv = $dat->{invert};
                my $fn  = "sc$sc".$cn;
                my @fp  = @fparam;
                my $v;
                if ($sc == 2) {
                    $v = $inv ? "< ".$dat->{min} : "> ".$dat->{max};
                    @fp = ( -align      => 'center',
                            -background => 'lime',
                            -bold       => 1,
                            -color      => 'red');
                    ($v, $fn) = (undef, undef) if ($cn eq $cvColName);
                } elsif ($sc == -1) {
                    $v = $inv ? "> ".$dat->{max} : "< ".$dat->{min};
                    @fp = ( -align      => 'center',
                            -background => 'black',
                            -bold       => 1,
                            -color      => 'red');
                    if ($cn eq $cvColName) {
                        $v = "> 100%";
                        push @fp, ( -num_format => $percForm );
                    }
                } else {
                    my $delta = $sc * $dat->{range};
                    if ($inv) {
                        $v = $dat->{max} - $delta;
                    } else {
                        $v = $dat->{min} + $delta;
                    }
                    if ($cn eq $cvColName) {
                        my $r  = $sc * 230;
                        my $ind = 7 + $cSlots[ $sc * 10 ];
                        my $cc = $eh->set_custom_color($ind, 255, $r, $r);
                        @fp = ( -num_format => $percForm,
                                -color      => $cc,
                                -align      => 'center' );
                        # warn "[$sc] = $fn\n";
                    }
                }
                $eh->format( @fp,
                             -name => $fn ) if ($fn);
                push @frm, $fn;
                push @vals, $v;
            }
        }
        push @scKeyRows, [\@vals, \@frm];
    }
    
    # Make sure our selected IDs are ok in the context of the set
    # If we have a selection, check that, otherwise check all compound nodes
    my @selectToCheck = $userSelection ? @{$userSelection->{All} || []} :
        ( map { $_->name } @{$nodeTypes{Compound} || []} );
    my @validSelect = @selectToCheck;
    
    my $totCompound = $#validSelect + 1;
    if ($cmpdSet) {
        my %ok;
        map { $ok{$_} = $_ if ($inSet->{$_}) } @validSelect;
        @validSelect = sort values %ok;
        my $toss = $#selectToCheck - $#validSelect;
        if ($toss) {
            $totCompound = $#validSelect + 1;
            $helpNotes{"Set: Picks Not in Set"} = $toss;
        }
        $helpNotes{"Set: Cmpd Total = HGD:m+n"} = $compoundCounts{FullRefSet};
        $helpNotes{"Set: Statistics"} = "Hypergeometric Distribution ".
            ($usePval ? "p-Values" : "single point odds");
        foreach my $sname (@hgdSets) {
            $helpNotes{"Set $sname: Name"} = $sname;
            $helpNotes{"Set $sname: Cmpd Picked = HGD:N"} = $compoundCounts{$sname};
        }
    }
    unless ($totCompound) {
        $args->death("No valid compounds are in your picked set!",
                     @selectToCheck);
    }
    
    &ts_message("Organizing spreadsheet");
    my %cRowLU;
    $ad->bench_start("Write C2T sheet");
    my $cmpD  = $c2tN2I{'Compound Description'};
    my $cluD  = $c2tN2I{'Cluster Description'};
    my $cluN  = $c2tN2I{'Cluster'};
    my $symI  = $c2tN2I{'Sym'};
    my $scrI  = $c2tN2I{'Score'};
    my %pivot;
    my $rowCount = 0;
    for my $c (0..$#{$nodeTypes{Compound}}) {
        my $cNode    = $nodeTypes{Compound}[$c];
        my $cmpd     = $cNode->name;
        my ($cNs, $smiN) = map { $cNode->attribute($_) } qw(ns smilesCount);
        my @base     = ($cmpd);
        $base[$cmpD] = &cmpd_desc($cNode);
        
        $compoundStyles{$cmpd}{uselect} = 1 if ($isSelection{All}{$cmpd});
        my @bFrm = (undef, undef, 'cen');

        if ($captureC->{cols}) {
            my ($cr, $cf) = &add_captured_columns( $cmpds, [$cmpd] );
            for my $i (0..$#{$cr}) {
                my $ri = $bCapOffset + $i;
                $base[ $ri ] = $cr->[$i];
                $bFrm[ $ri ] = $cf->[$i];
            }
        }
        if (!$smiN) {
            # Unable to map this compound identifier to any smiles
            $compoundStyles{$cmpd}{noSmiles} = 1;
            $base[1] =  "No Structure";
            $bFrm[1] = 'noSmiles';
        } elsif ($notok{$cmpd}) {
            $compoundStyles{$cmpd}{notinset} = 1;
        }
        $compoundStyles{$cmpd}{filtCmpd} = 1 if ($filtCount{$cmpd});
        $bFrm[0] = join(' ', sort keys %{$compoundStyles{$cmpd}});
        unless ($smiN) {
            $rowCount = $eh->add_row( $ctws, \@base, \@bFrm );
            next;
        }

        my @actEdges = $cNode->each_edge( -edgefilter => "type == Activity");
        my @rows;
        foreach my $edge (@actEdges) {
            my $clus = $edge->other_node($cNode);
            my $cTax = $clus->attribute('taxa');
            my $row = [ @base ];
            my $frm = [ @bFrm  ];
            my $cN  = $row->[ $cluN ] = $clus->name;
            my $sym = $row->[ $symI ] = $clus->attribute('sym') || "";
            if (!$sym && $cTax) {
                $row->[ $symI ]  = $cTax;
                $frm->[ $symI ] .= 'taxa';
            }
            $row->[ $cluD ] = $clus->attribute('desc');
            my $go = $goSheet ? &go_for_cluster($cN) : [];

            my @usedVals;
            foreach my $key (@usedTags) {
                my $ind = $c2tN2I{$key};
                next unless ($ind);
                my $val = $edge->attribute($key);
                next unless (defined $val);
                if (defined $val) {
                    push @usedVals, $val;
                    my ($sc, $fmt) = &scValFrm($val, $key);
                    $row->[$ind] = $sc;
                    $frm->[$ind] = $fmt;
                    push @{$pivot{"Compound Summary"}{$cmpd}{$key}}, $val;
                    push @{$pivot{"Cluster Summary"}{$cN}{$key}}, $val;
                    $pivot{"Cluster Summary"}{$cN}{COMPOUNDS}{$cmpd} = 1;
                    push @{$pivot{"Cluster Summary"}{$cN}{SCORES}}, $val;
                    map { push @{$pivot{GO}{$key}{$_}}, $val } @{$go};
                }
            }
            if ($scrI) {
                my ($best) = sort { $b <=> $a } @usedVals;
                my ($sc, $fmt) = &scValFrm($best, 'MapTracker');
                $row->[ $scrI ] = $sc || $best;
                $frm->[ $scrI ] = $fmt;
            }
            foreach my $tx (@tagTaxae) {
                my $ind = $c2tN2I{$tx};
                next unless ($ind);
                my ($val);
                if ($ind) {
                    if ($tx eq $oTaxaTag) {
                        # Junk drawer taxa value
                        ($val) = sort { $b <=> $a } @usedVals
                            if ($cTax && !$activityTaxae{$cTax});
                    } else {
                        $val = $edge->attribute($tx);
                    }
                }
                my $loci = &species_locus_for_cluster($cN, $tx);
                next if (!$loci || $#{$loci} == -1);
                my $allLoc = $row->[$ind] = join(",", @{$loci});
                $pivot{"Cluster Summary"}{$cN}{$tx} ||= [ $allLoc ];
                unless (defined $val) {
                    #$row->[$ind] = " "; # prevent neighbor cells from spilling
                    $frm->[$ind] = 'nulldata';
                    next;
                }
                my ($sc, $fmt) = &scValFrm($val, $tx);
                push @{$pivot{"Compound Summary"}{$cmpd}{"$tx $cmpTaxHitTag"}}, $val;
                push @{$pivot{"Cluster Summary"}{$cN}{"$tx $cmpTaxHitTag"}}, $val;
                $frm->[$ind] = $fmt;
            }
            push @rows, [$row, $frm];
        }
        foreach my $dat (sort { uc($a->[0][$symI]) cmp
                                    uc($b->[0][$symI]) } @rows) {
            my ($r, $f) = @{$dat};
            $rowCount = $eh->add_row( $ctws, $r, $f );
            push @{$cRowLU{$r->[$cluN] || ""}}, $rowCount - 1;
        }
        if ($#rows == -1) {
            my $row = [ @base ];
            my $frm = [ @bFrm  ];
            $row->[ $cluN ] = "No Targets";
            if (my $known = $filtCount{$cmpd}) {
                $row->[$cluN] = sprintf
                    ("%d filtered Target%s",$known, $known == 1 ? '' : 's');
                # map { $frm->[$_] .= 'filt' } (0..2);
            }
            $rowCount = $eh->add_row( $ctws, $row, $frm );
            $ctws->set_row($rowCount - 1, undef, undef, 1)
                if ($cmpdSet);
        }
    }
    my %styleCount;
    while (my ($cid, $sh) = each %compoundStyles) {
        my $sty = $compoundStyles{$cid} = join(' ', sort keys %{$sh});
        $styleCount{$sty}++;
    }
    $args->msg("Utilized compound styles", map { "$_ : $styleCount{$_}"}
               sort { $styleCount{$b} <=> $styleCount{$a} }
               keys %styleCount );
    $ad->bench_end("Write C2T sheet");

    if ($cmpdSet) {
        # Can not get this to work as advertised
#        my $ind = $c2tN2I{$statTag};
#        $ctws->autofilter(0, $ind, 20000, $ind); #$rowCount, $ind);
#        $ctws->filter_column($ind, 'p < 0.005');
#        $ctws->filter_column($ind, 'p < 0.001 and p >= 0');
#        $ctws->filter_column($ind, 'p < 0.0001 and p >= 0');
#        $ctws->filter_column($ind, 'p < 0.00001 and p >= 0');
    }

    &ts_message("Writing Compound Summary");
    $ad->bench_start("Write Compound Summary");
    my $cSmiInd = $cN2I{'SMILES'};
    my $cscrI   = $cN2I{'Score'};
    for my $c (0..$#{$nodeTypes{Compound}}) {
        my $cNode    = $nodeTypes{Compound}[$c];
        my $cmpd     = $cNode->name;
        unless ($cmpd) {
            $args->err("No name assigned to graph node?!?",
                       $args->branch($cNode));
            next;
        }
        my ($cNs, $smiN) = map { $cNode->attribute($_) } qw(ns smilesCount);

        # Populate the compound summary worksheet
        my (@dirSmi, @fullSmi, @struct);
        for my $i (0..1) {
            my $smiEdge  = $dbi->query_edges
                ( -graph     => $gname,
                  "-node".($i+1) => $cmpd,
                  -nodeparam => 'type = SMILES' );
            foreach my $row (@{$smiEdge}) {
                my ($n1, $n2, $gname) = @{$row};
                my $edge = $searcher->edge($n1, $n2);
                $edge->read();
                my $sN = $edge->other_node($cmpd);
                my $smi = $sN->name;
                if ($edge->attribute('direct')) {
                    push @dirSmi, $smi;
                    $sN->read;
                    if (my $st = $sN->attribute('smiles')) {
                        push @struct, $st;
                    }
                }
                push @fullSmi, $smi;
            }
        }

        my @row = ($cmpd);
        my @frm = ($compoundStyles{$cmpd} || undef);
        my $hash = $pivot{"Compound Summary"}{$cmpd} || {};
        if ($captureC->{cols}) {
            my ($cr, $cf) = &add_captured_columns( $cmpds, [$cmpd] );
            push @row, @{$cr};
            push @frm, @{$cf};
        }
        my @allVals;
        foreach my $key (@usedTags) {
            if (my $vals = $hash->{$key}) {
                my ($val) = sort {$b <=> $a} @{$vals};
                push @allVals, $val;
                my ($sc, $fmt) = &scValFrm($val, $key);
                my $ind = $cN2I{$key};
                if (defined $ind) {
                    $row[$ind] = $sc;
                    $frm[$ind] = $fmt;
                }
            }
        }
        if (1) {
            my ($best) = sort { $b <=> $a } @allVals;
            my ($sc, $fmt) = &scValFrm($best, 'MapTracker');
            $row[ $cscrI ] = $sc || $best;
            $frm[ $cscrI ] = $fmt;
        }
        foreach my $tx (@tagTaxae) {
            my $key = "$tx $cmpTaxHitTag";
            if (my $vals = $hash->{$key}) {
                my ($val) = sort {$b <=> $a} @{$vals};
                my ($ll, $fmt) = &scValFrm($val, $tx);
                $ll = $#{$vals} + 1;
                my $ind = $cN2I{$key};
                if (defined $ind) {
                    $row[$ind] = $ll;
                    $frm[$ind] = $fmt;
                }
            }
        }
        $row[$cSmiInd] = join(' ', @struct);
        push @row, ( $cNs, join(',', @dirSmi),
                    $smiN, join(',', @fullSmi), &cmpd_desc($cNode));
        $eh->add_row( 'Compound Summary', \@row, \@frm );
    }
    $ad->bench_end("Write Compound Summary");

    &ts_message("Writing Cluster Summary");
    $ad->bench_start("Write Cluster Summary");
    my @allClusRows;
    my (%pvSet, %cmpdNum, %bestLOD);
    # numCmpdInFullSet counts all available compounds (when a ref set is used):
    my $numCmpdInFullSet = $compoundCounts{FullRefSet};
    my $numLocInd = $clN2I{'# Loci'};
    foreach my $cNode (@allClus) {
        # for my $c (0..$#{$nodeTypes{Cluster}}) {
        # my $cNode    = $nodeTypes{Cluster}[$c];
        my $cN     = $cNode->name;
        my ($cDesc, $cNs, $lNum, $sym, $taxa) = map 
        { $cNode->attribute($_) } qw(desc ns clusterSize sym taxa);

        my $hash   = $pivot{"Cluster Summary"}{$cN} || {};
        my @cmpIDs = sort keys %{$hash->{COMPOUNDS}};
        # my $cmpNum = $#cmpIDs + 1;
        #if ($#userSelection != -1) {
        #my $cmpNum = 0;
        #map { $cmpNum++ if ($isSelection{All}{$_}) } @cmpIDs;
        #}
        
        $sym     ||= "";
        my @row    = ($cN, $sym);
        my @frm    = (undef, $sym =~ /\,/ ? 'hilightcen' : 'cen');
        if ($cmpdSet) {
            # numCmpdForTargInSet = the total number of compounds hitting this target
            # in the set defined by the user:
            my $numCmpdForTargInSet = $clusHits->{$cN}{FullRefSet} || 0;
            push @row, $numCmpdForTargInSet;
            push @frm, 'cen';
            my @bestP;
            foreach my $sname (@hgdSets) {
                # numCmpdForTargInSelection = the number of selected compounds
                # in this set that hit the target
                my $numCmpdForTargInSelection = $clusHits->{$cN}{$sname} || 0;
                # sizeOfSelection = the total number of compounds in 
                # the selected set
                my $sizeOfSelection = $compoundCounts{$sname} || 0; #cmpdNum
                push @row, $numCmpdForTargInSelection;
                push @frm, 'cen';
                my ($val, $fmt);
                if ($numCmpdInFullSet) {
                    # $ff->hypergeometric($i, $N, $n, $m);
                    # expect = the expected number of hits from a random choice
                    my $expect = $sizeOfSelection *
                        $numCmpdForTargInSet / $numCmpdInFullSet;
                    # isUnder = 1 if the actual numCmpdForTargInSelection 
                    my $isUnder = $numCmpdForTargInSelection < $expect ? 1 : 0;
                    # For odds, we will do a single point calculation
                    # For p-values, we will sum the odds for the observed
                    # result and all less-likely results:
                    my @nums = !$usePval ? ($numCmpdForTargInSelection) :
                        $isUnder ?
                        (0..$numCmpdForTargInSelection) :
                        ($numCmpdForTargInSelection..$numCmpdForTargInSet);
                    my $ex = 0;
                    # @hga = (N, n, m)
                    my @hga = ($sizeOfSelection, $numCmpdForTargInSet,
                               $numCmpdInFullSet - $numCmpdForTargInSet);
                    if ($#nums == -1) {
                    } else {
                        my $stat = 0;
                        foreach my $num (@nums) {
                            my $ex = $ff->hypergeometric($num, @hga);
                            if ($ex > 0) {
                                $args->err("Exponent exceeds 0 for HGD(".
                                           join(",", $num, @hga));
                            } elsif ($ex > -180) {
                                $stat += 10 ** $ex;
                            } else {
                                # die "CAN NOT EXP( $ex ) via HGD(".join(",", $num, @hga).")";
                            }
                        }
                        # warn "[$nums[0]..$nums[-1]] = $stat\n";
                        # Convert back to log form
                        $ex = ! $stat ? -999 : $stat >= 1 ? 0 :
                            log($stat) * $log10;
                        # warn "PVALUE $#nums => $stat => $ex\n";
                    }
                    # Then convert exponent to a form that looks like a
                    # GenAcc score (ie 0.0-1.0)
                    my $sc = $ex / -10;
                    if ($sc > 1) {
                        # $sc = 1;
                    } elsif ($sc < 0) {
                        $sc = -1;
                    }
                    ($val, $fmt) = &scValFrm($sc, $statTag);
                    if ($isUnder) {
                        # Underenrichment
                        $val *= -1;
                        $fmt .= "UE";
                        # Change sign of exponent (ie will be > 0)
                        $ex *= -1;
                    }
                    $pvSet{$cN}{$sname}   = $ex;
                    $cmpdNum{$cN}{$sname} = $numCmpdForTargInSelection;
                    push @bestP, [$ex, $val, $fmt];
                }
                push @row, $val;
                push @frm, $fmt;
            }
            my ($best) = sort { $a <=> $b } values %{$pvSet{$cN} || {}};
            $best = 900 unless (defined $best);
            $pvSet{$cN}{BestLOD} = $best;
            if (my $clRows = $cRowLU{$cN}) {
                # Update color on C2T worksheet
                my $ind = $c2tN2I{"$statTag All"};
                my ($best) = sort { $a->[0] <=> $b->[0] } @bestP;
                my ($ex, $val, $fmt) = @{$best || []};
                my ($wsf) = $eh->_map_formats($fmt);
                foreach my $rn (@{$clRows}) {
                    # Update the enrichment score on compound-to-target sheet
                    $ctws->write($rn, $ind, $val, $wsf);
                    # Hide the row if the gene is not significantly enriched:
                    $ctws->set_row($rn, undef, undef, 1)
                        if ($val < 0 || $val > 0.05);
                }
            }
        }
        if (!$sym && $taxa) {
            $row[1] = $sym = $taxa;
            $frm[1] = 'taxa';
        }
        if ($captureC->{cols}) {
            my ($cr, $cf) = &add_captured_columns( $cmpds, \@cmpIDs );
            push @row, @{$cr};
            push @frm, @{$cf};
        }
        my ($bestScore) = sort { $b <=> $a } @{$hash->{SCORES} || []};
        my ($bsc, $bfmt) = &scValFrm($bestScore, 'MapTracker');
        push @row, $bsc || $bestScore;
        push @frm, $bfmt;
        
        foreach my $key (@usedTags) {
            my ($sc, $fmt);
            if (my $vals = $hash->{$key}) {
                my ($val) = sort {$b <=> $a} @{$vals};
                ($sc, $fmt) = &scValFrm($val, $key);
            }
            push @row, $sc;
            push @frm, $fmt;
        }
        foreach my $tx (@tagTaxae) {
            my ($cmpdCount, $fmt);
            my $allLoc = $hash->{$tx};
            my $locFmt = $allLoc ? 'nulldata' : undef;
            if (my $vals = $hash->{"$tx $cmpTaxHitTag"}) {
                my ($val) = sort {$b <=> $a} @{$vals};
                ($cmpdCount, $fmt) = &scValFrm($val, $tx);
                $locFmt = $fmt;
                $cmpdCount = $#{$vals} + 1;
            }
            my ($lind, $cind) = ($clN2I{$tx}, $clN2I{"$tx $cmpTaxHitTag"});
            if (defined $lind) {
                $row[$lind] = $allLoc;
                $frm[$lind] = $locFmt;
            }
            if (defined $cind) {
                $row[$cind] = $cmpdCount;
                $frm[$cind] = $fmt;
            }
        }
        $row[$numLocInd] = $lNum;
        push @row, (join(',', @cmpIDs), $cNs, $cDesc);
        push @allClusRows, [ \@row, \@frm ];
    } 
    if ($cmpdSet) {
        my @sorter;
        # big sort values 'better'
        my %typeStrings;
        foreach my $r (@allClusRows) {
            my $cN  = $r->[0][0];
            my %pvs = %{$pvSet{$cN} ||= {}};
            my $ex  = $pvs{BestLOD};
            unless (defined $ex) {
                $ex     = 901;
            }
            push @sorter, [$ex, $r];
            # While we are here, also set graph attributes
            my @ks = keys %pvs;
            foreach my $k (@ks) {
                my $ex = $pvs{$k};
                delete $pvs{$k};
                next unless (defined $ex);
                my $pv  = sprintf("%.4g", 10 ** (0-abs($ex)));
                $ex     = sprintf("%.4f", $ex);
                my $tag = $k eq 'BestLOD' ? 'Best' : $k;
                $pvs{"LOD : $tag"} = $ex;
                $pvs{"Pval : $tag"}  = $pv;
            }
            map { $typeStrings{$_} = 1 } keys %pvs;
            $graph->set_node_attributes( $cN, %pvs );
        }
        map { $graph->attribute_type($_, 'real') } keys %typeStrings;

        @allClusRows = map { $_->[1] } sort 
        { $a->[0] <=> $b->[0] ||
              uc($a->[1][1] || "") cmp uc($b->[1][1] || "") } @sorter;
    }
    foreach my $rd (@allClusRows) {
        my ($r, $f) = @{$rd};
        $eh->add_row( 'Cluster Summary', $r, $f);
    }
    $ad->bench_end("Write Cluster Summary");


    $ad->bench_start("Write Edge TSV");

    $ad->bench_end("Write Edge TSV");


    my @gKeys = sort keys %{$pivot{GO} || {}};
    &ts_message("Writing GO Summaries") unless ($#gKeys == -1);
    foreach my $key (@gKeys) {
        my @goBins = map { 1 - ($_/10) } (0..10);
        @goBins = () if ($key eq 'Generic');
        push @goBins, -1;
        my $gbn = $#goBins;
        my $goSum = $eh->sheet
            ( -name    => "GO $key",
              -width   => [ 30, 12, map { 8 } (1..11)  ],
              );
        $goSum->freeze_panes(1, 1);
        my @goHead = ("GeneOntology", "ID");
        my (@gHfrm, @goFrm);
        foreach my $val (@goBins) {
            my ($sc, $fmt) = &scValFrm($val, $key);
            push @goHead, $sc;
            push @gHfrm, $fmt;
            ($sc, $fmt) = &scValFrm($val, 'Generic');
            push @goFrm, $fmt;
        }
        $eh->add_row( $goSum, \@goHead, [undef, undef, @gHfrm]);
        
        my @rows;
        my $gbn2 = $gbn + 2;
        while (my ($goid, $vals) = each %{$pivot{GO}{$key}}) {
            my @gr = (&desc_from_graph($goid), $goid, map { 0 } @goBins);
            foreach my $val (@{$vals}) {
                my $ind = $gbn2;
                if ($val >= 0) {
                    $ind = 12 - int($val * 10);
                }
                $gr[$ind]++;
            }
            push @rows, \@gr;
        }
        my @sorted = sort {
            for my $i (2..$gbn2) {
                if ($a->[$i] > $b->[$i]) {
                    return -1;
                } elsif ($a->[$i] < $b->[$i]) {
                    return 1;
                }
            }
            return $a->[0] cmp $b->[0];
        } @rows;
        foreach my $row (@sorted) {
            my @frm;
            for my $i (2..$gbn2) {
                if (my $num = $row->[$i]) {
                    $frm[$i] = $goFrm[$i-2];
                } else {
                    $row->[$i] = undef;
                }
            }
            $eh->add_row( $goSum, $row, \@frm);
        }
    }

    my %uHash; map { $uHash{$_}++ } map { keys %{$_} } values %unknown;
    my @uCols = sort { $uHash{$b} <=> $uHash{$a} } keys %uHash;
    unless ($#uCols == -1) {
        my $unk = $eh->sheet
            ( -name    => "Excluded IDs",
              -freeze  => 1,
              -columns => [ "ID", @uCols, 'Description' ],
              -width   => [ 30, (map { 20 } @uCols), 40 ],
              );
        $eh->format( -background => 'yellow',
                     -align      => 'center',
                     -name => 'Excluded', );
       foreach my $id (sort keys %unknown) {
            my @row = ($id);
            my @fmt = ($compoundStyles{$id});
            foreach my $u (@uCols) {
                my $detail = $unknown{$id}{$u};
                push @row, $detail;
                push @fmt, !$detail ? undef : 
                    $u eq 'Target Filter' ? 'filtCmpd' : 'Excluded';
            }
            push @row, &cached_desc($id);
            $eh->add_row( $unk, \@row, \@fmt );
        }
    }

    if ($inSet) {
        # my @picked = $#userSelection == -1 ? @cids : @userSelection;
        # &hgd_links( $inSet, \@picked, $edgeFilter, \@filtExcel, \%pvSet );
        &hgd_links( $inSet, $userSelection, $edgeFilter,
                    \@filtExcel, \%pvSet );
        &add_user_sets($userSelection, $cmpds, $eh);
    }

    &add_key($eh, \@scKeyRows, \@usedTags);
    &add_help($eh, \@filtExcel);
    my $url = $eh->file_path( );
    
    $url =~ s/\/stf/http:\/\/bioinformatics.bms.com/;
    if ($url =~ /^http/) {
        $eh->url($url);
    } else {
        $url = "";
    }
    if ($nocgi) {
        if ($vb) {
            &ts_message("Excel file written", $eh->file_path);
            # warn "   $url\n" if ($url);
        }
    } else {
        print $eh->html_summary
    }

    
    if ($noStrip) {
    } else {
        # Strip out poor confidence genes
        my $num      = 0;
        my $minP     = $args->val(qw(pvalue pval minp));
        my @stripMsg = ("Stripping genes with p-value > $minP");
        push @stripMsg, "Keeping genes with <= $keepSmall compounds"
            if ($keepSmall);
        foreach my $cN (keys %pvSet) {
            my $ex = $pvSet{$cN}{BestLOD};
            if (defined $ex && $ex < 0) {
                # We have an enriched (not underenriched) gene
                my $pv = 10 ** $ex;
                next if ($pv < $minP);
                my ($cNum) = sort { $a <=> $b } values %{$cmpdNum{$cN} || {}};
                next if ($keepSmall && ($cNum || 999) <= $keepSmall);
            }
            $graph->remove_node($cN);
            # warn "$cN pV = 10 ^ $ex, Num = $cmpdNum{$cN}\n";
            $num++;
        }
        &ts_message(@stripMsg, "Removed $num low confidence genes from XGMML");
        # Remove orphan nodes
        $graph->remove_isolated_nodes();
    }

    
    my $xml = "$basePath.xgmml";
    if (open(XML, ">$xml")) {
        print XML $graph->to_xgmml;
        close XML;
        &ts_message("XGMML written", $xml);
    } else {
        $args->err("Failed to create XGMML file");
    }

    $graph->attribute_generator('shape', 'Node', sub {
        my $node = shift;
        my $type = lc($node->generated_attribute('type') || "");
        if ($type eq 'compound') {
            return 'square';
        } elsif ($type eq 'cluster') {
            return 'oval';
        }
        return 'octagon';
    });

    $graph->attribute_generator('labelSize', 'Node', sub {
        return 1;
    });
    $graph->attribute_generator('width', 'Edge', sub {
        return 3;
    });

    $graph->attribute_generator('color', 'Edge', sub {
        my $edge = shift;
        my $sc   = $edge->generated_attribute('score');
        my $ind  = defined $sc && $sc >= 0 ? 1+ int(10 * $sc) : 0;
        return '#' . $edgeCols[$ind];
    });
    $graph->set_attributes
        (showNodeNameThreshold => 50,
         showAnimation         => 1 );
    
    my $txtF = "$basePath-FG.txt";
    if (open(TXT, ">$txtF")) {
        print TXT $graph->to_text_file();
        close TXT;
        $args->msg("FriendlyGraph Text File written", $txtF);
    } else {
        $args->err("Failed to create FriendlyGraph text file");
    }
    

    my $html = "$basePath.html";
    if (open(HTML, ">$html")) {
        my $cid = "CxNetwork";
        print HTML <<EOF;
<html>
 <head>
  <link rel="shortcut icon" href="/biohtml/images/AGCT_16x16.png">
  <title>ChemTarget Network Report</title>
  <script type='text/javascript' src='http://xpress.pri.bms.com/JAVASCRIPT/canvas/js/canvasXpress.min.js'></script>
 </head>
 <body>

<canvas id='$cid' width='1024' height='800'></canvas>
<script>

EOF

    print HTML $graph->to_canvasXpress
        ( -id => $cid,
          -pretty => 1, );

        print HTML <<EOF;
</script>
</body></html>
EOF

        close HTML;
        $args->msg("HTML Network written", $html);

    } else {
        $args->err("Failed to create HTML file");
    }
}

sub add_user_sets {
    my ($userSelection, $cmpds, $eh) = @_;
    my %l = %{$userSelection};
    delete $l{"All"};
    my @sets = sort keys %l;
    return if ($#sets == -1);
    my %compounds;
    my @head = "Compound";
    foreach my $setName (@sets) {
        my @cmpd = @{$l{$setName}};
        map { $compounds{$_}{$setName} = 1 } @cmpd;
        push @head, "$setName (".($#cmpd + 1).")";
    }
    push @head, 'Description';
    my $sheet = "User Sets";
    $eh->format( -name       => 'SetCol',
                 -bold       => 1,
                 -background => 'blue',
                 -color      => 'yellow',
                 -align      => 'center');
    $eh->sheet( -name    => $sheet,
                -freeze  => 1,
                -columns => \@head,
                -width   => [16, (map { 10 } @sets), 40], );
    foreach my $chem (sort keys %compounds) {
        my @row = ($chem);
        my @fmt = (undef);
        foreach my $set (@sets) {
            if ($compounds{$chem}{$set}) {
                push @row, 'X';
                push @fmt, 'SetCol';
            } else {
                push @row, undef;
                push @fmt, undef;
            }
        }
        my $cdat = &add_compound($chem, $cmpds);
        push @row, &cached_desc($cdat->{id}, $cdat->{ns});
        $eh->add_row( $sheet, \@row, \@fmt );
    }
}

sub hgd_links {
#     my ( $inSet, $pickedCmpds, $edgeFilter, $filt, $pvSet ) = @_;
    my ( $inSet, $userSelection, $edgeFilter, $filt, $pvSet ) = @_;
    my @setCmpds = keys %{$inSet};
    my $numCmpd  = $#setCmpds + 1;
    if (my $maxOrth = $args->val(qw(maxorth))) {
        if ($numCmpd > $maxOrth) {
            &ts_message("$numCmpd compounds exceeds upper limit -maxorth $maxOrth - skipping HGD link creation");
            return;
        }
    }
    &ts_message("Finding all orthologues possible for $numCmpd compounds in reference set");
    my $c2oArr =  $dbi->query_edges
        ( -desc     => "Find all gene activities for requests",
          -graph    => $gname,
          -node     => \@setCmpds,
          -edgeattr => $edgeFilter,
          -nodeattr => "ns = SYM",
          -explain  => $explain,
          -dumpsql  => $dumpSql );
    my (%c2o, %o2c);
    # o2c allows compounds to be recovered for a target (orthologue)
    # c2o is reverse, orths from compounds
    foreach my $row (@{$c2oArr}) {
        my ($n1, $n2) = @{$row};
        my ($cmpd, $orth) = $inSet->{$n1} ?
            ($n1, $n2) : $inSet->{$n2} ? ($n2, $n1) : ();
        unless ($orth) {
            $args->err("Failed to determine cmpd vs orth for ($n1, $n2)");
            next;
        }
        $c2o{$cmpd}{$orth}++;
        $o2c{$orth}{$cmpd}++;
    }
    my @hgdSets = sort keys %{$userSelection};
    my (%pickedOrth, %wasPicked);
    foreach my $sname (@hgdSets) {
        my @pickedCmpds = @{$userSelection->{$sname}};
        map { $wasPicked{$sname}{$_} = 1 } @pickedCmpds;
        map { $pickedOrth{$sname}{$_} = 1 }
        map { keys %{$c2o{$_} || {} } } @pickedCmpds;
    }
    my (%taxSets, %pickSet, %l2c);
    &ts_message("Preparing GSEA hyperlinks for ".scalar(@hgdSets).
                " user sets in ".scalar(@defaultTaxa)." taxae");
    while (my ($orth, $cH) = each %o2c) {
        my @cmpd = sort keys %{$cH};
        foreach my $tx (@defaultTaxa) {
            my $loci = &species_locus_for_cluster($orth, $tx) || [];
            map { $taxSets{$tx}{$_}++ } @{$loci};
            foreach my $sname (@hgdSets) {
                next unless ($pickedOrth{$sname}{$orth});
                # This orthologue group is in the user selection
                my $ex = $pvSet->{$orth}{$sname};
                $ex    = 904 unless (defined $ex);
                foreach my $ll (@{$loci}) {
                    $pickSet{$tx}{$sname}{$ll} = $ex
                        if (!defined $pickSet{$tx}{$sname}{$ll} ||
                            $pickSet{$tx}{$sname}{$ll} > $ex);
                    map { $l2c{$ll}{$sname}{$_} = $wasPicked{$_} || 0 } @cmpd;
                }
            }
        }
    }

    my $url = "http://bioinformatics.bms.com/biocgi/".
        (0 && $isBeta ? "tilfordc/working/maptracker/" : "" ).
        "hypergeometric_affy.pl?inputfilter=".$esc->esc_url("pValue <= 0.05").
        "&paramfile=";


    my $filtText = $filt ? "# Activity filters used:\n".
        join("", map { "# $_\n" } @{$filt})."\n" : "";
    

    my $comParam = <<PARAM;
# Parameter file for GSEA analysis of chemTarget.pl presumptive loci
$filt

# Set the namespace for the ids
-reference => LL

# Set analysis mode to full (ie not a ranked list)
-mode => GSEA

# Discard genes with no annotation
-removenull => 1



PARAM

    my $setDir = "/stf/biohtml/GSEA";
    foreach my $tx (sort keys %taxSets) {
        my $pHall = $pickSet{$tx}{ALL};
        my @pickedAll = sort { $pHall->{$a} <=> $pHall->{$b} } keys %{$pHall};
        my $pNum   = $#pickedAll + 1;
        next unless ($pNum);
        my $sFile = sprintf("%s/GSEA_%s LocusLink_LL_005.set", $setDir, $tx);
        unless (-e $sFile) {
            $args->msg("[?]", "Can not do GSEA for $tx without set file",
                       $sFile);
            next;
        }
        my @allLoc = sort keys %{$taxSets{$tx}};
        my $aNum   = $#allLoc + 1;
        &ts_message(sprintf("%s : %d loci out of %d", $tx, $pNum, $aNum));
        my $file = sprintf("%s-%s-GSEA", $basePath, $tx);
        $file =~ s/\s+/_/g;
        my $pickS  = $file;
        $file     .= ".param";
        $pickS    .= ".list";
        my $pickM  = "$pickS.meta";

        unless (open(SET, ">$pickS")) {
            $args->err("Failed to write picked gene set file", $pickS, $!);
            next;
        }
        my $meta = &cached_data
            ( -ids => \@pickedAll, -mode => 'simple',
              -cols => 'term,ns,sym,desc', -output => $pickM);
        my %mH;
        foreach my $row (@{$meta}) {
            my ($ll, $ns, $sym, $desc) = @{$row};
            $mH{$ll} = [$sym, $desc];
        }
        print SET "# Set File generated by chemTargetReport.pl on ".`date`."\n";

        print SET "# NOTE! Negative pValues represent under-enrichment\n";
        print SET "# List is ranked by most confident locus to least confident\n";
        print SET "# TAXA=$tx\n";
        print SET "# NAMESPACE=LL\n";
        my $shortFile = $ifile;
        $shortFile =~ s/.+\///;
        foreach my $sname (@hgdSets) {
            my $lname = "$tx $sname genes";
            $lname   .= " from $shortFile" if ($shortFile);
            print SET "\n# LIST - $lname\n";
            print SET "# Metadata columns:\n";
            print SET "# ".join("\t", "Query ID", "Symbol", "pValue", "CmpdHit", "CmpdTot", "CmpdHitNames")."\n";
            
            my @setData;
            my $pH     = $pickSet{$tx}{$sname};
            my @picked = sort { $pH->{$a} <=> $pH->{$b} } keys %{$pH};
            foreach my $ll (@picked) {
                my ($sym, $desc) = @{$mH{$ll} || []};
                my $llH    = $l2c{$ll}{$sname};
                my @llC    = sort { $llH->{$b} <=> $llH->{$a} 
                                    || $a cmp $b } keys %{$llH};
                my @hitC; map { push @hitC, $_ if ($llH->{$_}) } @llC;
                my $llCmpd = join(',', @hitC) || "";
                $llCmpd = "<a href='http://bioinformatics.bms.com/biocgi/chemBioHopper.pl?id=$llCmpd' target='_blank'>$llCmpd</a>" if ($llCmpd);
                my $exp = $pH->{$ll} || 0;
                # Do not consider loci that are underenriched:
                next if ($exp > 0);
                my $pV  = sprintf("%.4g", ($exp > 0 ? -1 : 1) * 
                                  10 ** (0-abs($exp)));
                push @setData, join
                    ("\t", $ll, $sym || "", $pV, $#hitC + 1, $#llC + 1, $llCmpd) . "\n";
            }
            print SET sprintf("\n# %d entries out of %d survive assignment confidence filtering:\n\n", $#setData + 1, $#picked + 1);
            print SET join('', @setData);
        }
        close SET;
        
        unless (open(PARAM, ">$file")) {
            $args->err("Failed to write parameter file", $file, $!);
            next;
        }
        print PARAM $comParam;

        print PARAM <<TAX;
# Data are for species $tx
# The 'file' contains an annotated list of the "picked set" of loci
# (based on the picked set of compounds)

# The 'setfile' contains all the ontologies needed for this species

# The 'restrict' parameter will restrict the set file to just those genes that
# *could* have been picked given our compound screening set, and the activity
# filters shown above

# In this example we have picked $pNum loci out of $aNum possible in screening set

TAX

        print PARAM "-jobnote  => Significant $tx genes from ChemTarget analysis";
        print PARAM " in $shortFile" if ($shortFile);
        print PARAM "\n\n";
        print PARAM "-setfile  => $sFile\n\n";
        print PARAM "-file     => $pickS\n\n";
        # print PARAM "-query    => <<PICK;\n".join("\n", @picked). "\nPICK\n\n";
        print PARAM "-restrict => <<SET;\n".join("\n", @allLoc). "\nSET\n\n";
        close PARAM;
        map { chmod(0666, $_) } ($file, $pickM, $pickS);
        $links{"$tx GSEA Analysis"} = $url.$file;
    }
}

sub read_cmpd_list {
    my $req = shift;
    return undef unless ($req);
    my $ctxt = shift || "Unspecified Input";
    my %found;
    if (-e $req) {
        my $tr = BMS::TableReader->new( );
        my $format = $tr->format_from_file_name( $req . "" ) || 'list';
        my $isList = $format =~ /list/i ? 1 : 0;
        $tr->format($format);
        $tr->has_header($isList ? 0 : 1);
        $tr->input($req);
        foreach my $sheet ($tr->each_sheet()) {
            $tr->select_sheet($sheet);
            my @head = $tr->header();
            my %h = map { uc($head[$_]) => $_ + 1 } (0..$#head);
            my $scol = $isList ? 1 : $h{'SUBSTANCE'};
            next unless ($scol);
            $scol--;
            while (my $row = $tr->next_clean_row()) {
                $found{ $row->[$scol] || "" } = 1;
            }
        }
    } else {
        my @arr = ref($req) ? @{$req} : ($req);
        foreach my $blr (@arr) {
            map { $found{ $_ || ""} = 1 } split(/\s*[\n\r\t\,]+\s*/, $blr);
        }
    }
    my %ok;
    foreach my $id (keys %found) {
        next unless ($id);
        my $ns     = $ad->guess_namespace_careful( $id, 'AC' );
        my ($chem) = $ad->standardize_id($id, $ns);
        unless ($chem) {
            $args->msg_once('[!]', "Failed to standardize compound",
                       "$id [$ns] - In $ctxt");
            next;
        }
        $ok{$chem} = 1;
    }
    my @good = sort keys %ok;
    if ($#good == -1) {
        $args->msg("Failed to find any compounds in $ctxt list",
                   $req);
        return undef;
    } else {
        return \@good;
    }
}

sub set_members {
    my $cmpdSet = shift;
    return undef unless ($cmpdSet);
    my $s2c =  $dbi->query_edges
        ( -desc     => "Find all compounds in a set",
          -graph    => $gname,
          -node     => $cmpdSet,
          -edgeattr => 'type = InSet',
          -nodeattr => "ns = BMSC",
          -explain  => $explain,
          -dumpsql  => $dumpSql );

    my %inSet;
    foreach my $row (@{$s2c}) {
        my ($n1, $n2) = @{$row};
        my $cmpd = $n2 eq $cmpdSet ? $n1 : $n2;
        unless (&_cmpd_blackwhite($cmpd, 'RefSet')) {
            $inSet{$cmpd} = 1;
        }
    }
    return \%inSet;
}

sub _cmpd_blackwhite {
    my $cmpd = shift || "";
    my $src  = shift || "";
    return 'null' unless ($cmpd);
    if ($blackList{$cmpd}) {
        $unknown{$cmpd}{"Blacklisted"} = "In Blacklist";
        return 'black';
    } elsif ($whiteList && !$whiteList->{$cmpd}) {
        $unknown{$cmpd}{"Not Whitelisted"} = "Not in Whitelist";
        return 'white';
    }
    return undef;
}

sub desc_from_graph {
    my $id = shift;
    unless (defined $descCache{$id}) {
        my $node = $graph->node($id);
        $node->read();
        $descCache{$id} = $node->attribute('desc') || "";
    }
    return $descCache{$id};
}

# *cached_description = \&cached_desc;
sub cached_desc {
    my ($id, $ns) = @_;
    unless (defined $descCache{$id}) {
        $ns ||= "";
        $args->msg("[?]", "Description not cached for $id [$ns]")
            if ($shouldBeCached);
        my $desc = $ad->description
            ( -id => $id, -ns => $ns ) || "";
        $desc = "?" if ($desc eq '{IllegalNamespace}');
        $descCache{$id} = $desc;
    }
    return $descCache{$id};
}

sub cmpd_desc {
    my $cNode = shift;
    my $name  = $cNode->name;
    # warn "$name = $descCache{$name}\n";
    return $descCache{$name} if (defined $descCache{$name});
    my $desc  = $cNode->attribute('desc');
    if (!$desc || $desc =~ /^\{/) {
        if (my $mtn = $mt->get_seq($name)) {
            my $nd = join(', ', map { $_->name } $mtn->desc());
            $nd .= " $desc" if ($desc && $nd);
            $desc = $nd if ($nd);
        }
        return $descCache{$name} = $desc || "";
    }

    # Take out random IDs
    my $cleaned = $desc;
    my @regExps = ('[A-Z ]{1,3}\-\d{3,6}', 
                   '(AUR|MFCD|DB|CPD)\d+','(SMDL)-\d+','(SID|CID|CMB)\:\d+');
    my $tot = 0;
    foreach my $re (@regExps) {
        my $iloop = 0;
        while ($cleaned =~ /^($re( \(\+(\d+)\))?)$/ ||
            $cleaned =~ /^($re( \(\+(\d+)\))?\, )/  ||
            $cleaned =~ /\, ($re( \(\+(\d+)\))?\, )/) {
            my $rep = $1;
            my $num = $4 || $3;
            $num = $num ? $num + 1 : 1;
            $tot += $num;
            $cleaned =~ s/\Q$rep\E//;
            if (++$iloop > 50) {
                $args->err("Potential infinite loop parsing description",
                           $desc);
                last;
            }
        }
    }
    if ($cleaned && $tot) {
        $desc = $cleaned . " (+$tot accessions)";
    }
    return $descCache{$name} = $desc;
}

sub species_locus_for_cluster {
    my ($cN, $tx) = @_;
    unless ($cl2loci{$cN}) {
        # We are using an object other than $graph to allow recovery of data
        # for loci not in the graph. This allows loci to be found for
        # GSEA via hgd_links
        my $there = $searcher->has_node($cN);
        my $cNode = $searcher->node($cN);
        my $dbi   = $searcher->dbi;
        $cNode->read() unless ($there);
        my $nType = $cNode->attribute('type') || "";
        if ($nType eq 'Cluster') {
            my %loci;
            for my $i (0..1) {
                # There is a performance cost to using "-node" rather
                # than -node1 and -node2 separately
                my $c2l =  $dbi->query_edges
                    ( -desc     => "Find all loci for a cluster (one-way)",
                      -graph    => $gname,
                      "-node".($i+1) => $cN,
                      -edgeattr => "type = L2O",
                      -explain  => $explain,
                      -dumpsql  => $dumpSql );
                my $oInd = $i ? 0 : 1;
                map { $loci{ $_->[$oInd] } = undef } @{$c2l}
            }
            foreach my $ll (keys %loci) {
                my $lNode = $searcher->node($ll);
                $lNode->read();
                push @{$cl2loci{$cN}{$lNode->attribute('taxa') || ""}}, $ll;
            }
        } else {
            my $tax = $cNode->attribute('taxa') || "";
            $cl2loci{$cN}{ $tax } = [ $cN ];
            # Clean up some of the longer taxae
            $tax =~ s/Human immunodeficiency virus/HIV/gi;
            $tax =~ s/Escherichia /E. /g;
            if ($tax =~ /(\(?(\S+) isolate\)?)/i) {
                my ($in, $out) = ($1, $2);
                $tax =~ s/\Q$in\E/$out/g;
            }
            $cl2loci{$cN}{ $oTaxaTag } = [$tax];
        }
        
    }
    return $cl2loci{$cN}{$tx} || [];
}

sub go_for_cluster {
    my ($cN) = @_;
    unless ($cl2go{$cN}) {
        my %go;
        for my $i (0..1) {
            # There is a performance cost to using "-node" rather
            # than -node1 and -node2 separately
            my $c2l =  $dbi->query_edges
                ( -desc     => "Find all GO for a cluster (one-way)",
                  -graph    => $gname,
                  "-node".($i+1) => $cN,
                  -edgeattr => "type = GO",
                  -dumpsql  => $dumpSql );
            my $oInd = $i ? 0 : 1;
            map { $go{ $_->[$oInd] } = undef } @{$c2l}
        }
        $cl2go{$cN} = [ sort keys %go ];
    }
    return $cl2go{$cN};
}

sub hide_non_primary {
    my ($ws, $cols, $taxae);
    return unless ($cols);
    my %colLu = (ref($cols) eq 'HASH') ? 
        %{$cols} : map { $cols->[$_] => $_ } (0..$#{$cols});
    foreach my $tax (@{$taxae}) {
        next if ($isPrimary{$tax});
        my $cn = $colLu{$tax};
        next unless (defined $cn);
        # Hide the column
        $ws->set_column($cn, $cn, undef, undef, 1);
    }
}

sub sort_mixed {
    if ($a =~ /^\d+$/ && $b =~ /^\d+/) {
        return $a <=> $b;
    } elsif ($a =~ /^\d+$/) {
        return -1;
    } elsif ($b =~ /^\d+$/) {
        return 1;
    } else {
        return $a cmp $b;
    }
}

sub record_singleton_cluster {
    my ($targ, $cdat, $tag, $sc) = @_;
    my $cd = $cdat->{$targ} ||= {};
    unless (defined $targs{$targ}{taxa}) {
        my ($tx) = $ad->convert
            ( -id => $targ, -ns1 => $targs{$targ}{ns},
              -ns2 => 'TAX', -warn => $doWarn );
            $targs{$targ}{taxa} = $tx || "";
    }
    my $tx = $targs{$targ}{taxa};
    $cd->{$tx}{$tag} = $sc
        if (!defined $cd->{$tx}{$tag} || 
            $cd->{$tx}{$tag} < $sc);
    $cd->{tags}{$tag} = $sc
        if (!defined $cd->{tags}{$tag} ||
            $cd->{tags}{$tag} < $sc);
    $clusters{$targ}{$tx} ||= [$targ];
}

sub best_sym {
    my ($ids, $ns) = @_;
    my $key = join("\t", sort @{$ids});
    unless (defined $symbolCache{$ns}{$key}) {
        my @syms;
        if ($#{$ids} == 0) {
            my $bps = $ad->best_possible_symbol
                ($ids->[0], $ns, 'short warn poor');
            push @syms, $bps if ($bps);
        } else {
            foreach my $id (@{$ids}) {
                my $bps = &best_sym([$id], $ns);
                push @syms, $bps if ($bps);
            }
        }
        $symbolCache{$ns}{$key} = join(',', sort @syms) || "";
    }
    return $symbolCache{$ns}{$key};
}

sub scFrm {
    my ($sc, $fmt) = &scValFrm( @_ );
    return $fmt;
}

sub scVal {
    my ($sc, $fmt) = &scValFrm( @_ );
    return $sc;
}

sub scValFrm {
    my ($sc, $mod, $op) = @_;
    my ($val, $fmt, $newop, $plain) = ($sc, undef, $op, $sc);
    if (defined $sc) {
        $fmt = "";
        if ($sc < 0) {
            $fmt = "sc-1";
            $val = undef;
        } elsif ($sc > 1) {
            $fmt = "sc1";
        } else {
            $fmt = "sc".(int(0.5 + 10 * $sc) / 10);
        }
        if (my $vf = $valFormats->{$mod || ''}) {
            my $exFrm = $vf->[4] || "";
            $fmt .= $mod if ($mod && $exFrm);
            if (my $cb = $vf->[0]) {
                $val = &{$cb}( $sc );
                $plain = $val;
                if ($exFrm =~ /\%$/) {
                    # Make plain results percentages where appropriate
                    $plain = sprintf("%d%%", int(0.5 + 100 * $val));
                } elsif ($mod eq 'Generic' && $val eq '?') {
                    $plain = 'Any Value';
                }
            }
            if ($op) {
                my $type = $vf->[2];
                if ($type eq 'log' || $type eq 'invperc') {
                    $newop = $revOps->{$op};
                    unless ($newop) {
                        $args->msg("[!!]","Can not find inverse operation for '$op'");
                        $op = "??";
                    }
                }
            }
        }
    }
    # warn "($sc, $mod, $op) = ($val, $fmt, $newop, $plain)\n";
    return ($val, $fmt, $newop, $plain);
}

sub help { return &TWH($twName, @_); }

sub log10fmt {
    my $sc = shift;
    return '' if (!defined $sc || $sc < 0);
    return 10 ** (-10*$sc);
    return "1E-".(10*$sc);
}

sub lodfmt {
    my $sc = shift;
    return '' if (!defined $sc || $sc < 0);
    return $sc * 10;
    # return "1E-".(10*$sc);
}


sub directfmt { return shift; }

sub defaultfmt {
    return "?";
}
sub percfmt {
    my $sc = shift;
    return undef if (!defined $sc || $sc < 0);
    return $sc; # We are setting percent format style in excel, leave as is
    # return sprintf("%d%%", int(0.5 + 100 * $sc));
}

sub mtfmt {
    my $sc = shift;
    return undef if (!defined $sc);
    return $sc;
}

sub invpercfmt {
    my $sc = shift;
    return undef if (!defined $sc || $sc < 0);
    return 1-$sc;
    # return sprintf("%d%%", int(0.5 + 100 * (1-$sc)));
}

sub init {
    $args->ignore_error("Argument \"#VALUE!\" isn't numeric in int", 'global');
    $args->ignore_error("wrapped in pack", 'global');
    $args->set_mime(  ) unless ($nocgi);
    $args->msg("Using FriendlyGraph database $gname");
}

sub HTML_INTERFACE {
    return if ($nocgi || $args->{NOOPTS});
}

sub prep_msg {
    my $msg = shift;
    $msg ||= "Empty Message";
    my @mbits;
    if ($nocgi) {
        $msg =~ s/\<[^\>]+\>//g;
    } elsif ($#_ != -1) {
        $msg = &help(@_).$msg;
    }
    return $msg;
}

sub msg {
    $args->msg( &prep_msg(@_) );
}

sub err {
    $args->err( &prep_msg(@_) );
}

sub add_key {
    my ($eh, $scr, $ut) = @_;
    my @skHead = ('MapTracker');
    push @skHead, @{$ut};
    push @skHead, $statTag if ($cmpdSet);
    push @skHead, (@{$captureC->{cols}}, "%CV") if ($captureC->{cols});

    $eh->sheet( -name    => 'Score Key',
                -freeze  => 1,
                -columns => [ @skHead ],
                -width   => [ map { 15 } @skHead ],
                );
    foreach my $dat ( @{$scr} ) {
        # warn $args->branch($dat);
        $eh->add_row( 'Score Key', @{$dat});
    }
    my @bar = map { 'hhead' } (1..10);
    my @btx = map { ' ' } (1..9);
    $eh->add_row('Score Key', [' ']);
    $eh->add_row('Score Key',['Other Markup', @btx], [ @bar ]);

    $eh->add_row('Score Key', ['  ']);

    $eh->add_row('Score Key', ['Cluster',undef,'Sym'],['cenbold', undef,'cenbold']);
    $eh->add_row('Score Key', ['LOC1111,LOC2222 Orthologues',undef,'ABC,XYZ','The orthologue cluster has two or more distinct genes in it'],[undef, undef,'hilight' ]);

    $eh->add_row('Score Key', ['   ']);
    $eh->add_row('Score Key', ['Compound'],['cenbold']);
    $eh->add_row('Score Key', ['BMS-123456',undef,undef,'The compound has targets associated with it, but your filters (see Help sheet) exclude all of them'],['filtCmpd', undef, ]);
    $eh->add_row('Score Key', ['BMS-123456',undef,undef,'The compound is one of those that you selected'],['uselect', undef, ]);
    $eh->add_row('Score Key', ['BMS-123456',undef,undef,'The compound does not have SMILES data and could not be standardized through other means'],['noSmiles', undef, ]);
    $eh->add_row('Score Key', ['BMS-123456',undef,undef,'One of your selections that was not standardized.'],['noSmiles uselect', undef, ]);

    $eh->add_row('Score Key', ['         ']);
    $eh->add_row('Score Key', ['Cluster',undef,'# Cmpd'],['cenbold', undef,'cenbold']);
    $eh->add_row('Score Key', ['LOC3333 Orthologues',undef,'4','The orthologue cluster has two or more distinct compounds hitting it'],[undef, undef,'hilight' ]);

    $eh->add_row('Score Key', ['     ']);
    $eh->add_row('Score Key', ['Cluster',undef,'pV Set'],['cenbold', undef,'cenbold']);
    $eh->add_row('Score Key', ['LOC3333 Orthologues',undef,'0.001','The cluster is UNDER-enriched for compounds'],[undef, undef,'sc0.3pV SetUE' ]);

    $eh->add_row('Score Key', ['            ']);
    $eh->add_row('Score Key', ['Homo sapiens',],['cenbold', undef,'cenbold']);
    $eh->add_row('Score Key', ['LOC1234',undef,undef,'A locus associated with the cluster for which no compound hits exist in your analysis. That is, the locus is part of the cluster, and is being reported so you have the locus ID for that species, but has no reported assay data for the target'],['nulldata', undef,undef ]);
 
    
}

sub add_help {
    my ($eh, $filt) = @_;

    $eh->sheet( -name    => 'Help',
                -width   => [ 20, 80 ], );

    $eh->format( -name       => 'hitem',
                 -valign     => 'top' );
    $eh->format( -name       => 'hitemNote',
                 -color      => 'orange',
                 -bold       => 1,
                 -valign     => 'top' );
    $eh->format( -name       => 'htext',
                 -text_wrap  => 1 );
    $eh->format( -name       => 'pre',
                 -font       => 'Courier',
                 -color      => 'brown' );

    my $now = `date`; $now =~ s/[\n\r]+$//;
    $helpNotes{'Prepared on'} = $now;
    $helpNotes{'Captured Columns'} = join(', ', @{$captureC->{cols}})
        if ($captureC && $captureC->{cols});
    foreach my $note (sort keys %helpNotes) {
        my $val = $helpNotes{$note};
        $eh->add_row('Help', [$note, $val],['hhead'])
            if (defined $val && $val ne "");
    }
    my @linkNames = sort keys %links;
    unless ($#linkNames == -1) {
        $eh->add_row('Help', ['           ']);
        $eh->add_row('Help',['External Links',' '],['hhead','hhead']);
        foreach my $n (@linkNames) {
            $eh->add_row_explicit('Help', [['url', $links{$n}, $n]]);
        }
    }
    if ($filt && $#{$filt} != -1) {
        $eh->add_row('Help', ['     ']);
        $eh->add_row('Help',['Potency Filters Applied',' '],['hhead','hhead']);
        map { $eh->add_row('Help', [$_], ['pre'] ) } @{$filt};
    }
    $eh->add_row('Help', []);
    $eh->add_row('Help', ['Worksheet','Description'],['hhead','hhead']);
    
    my @sheets = 
        ( [ 'Compound To Target', "Summarizes all chemical/biological pairs that pass your filters. Each row represents a highly distilled report of all data relevant to one compound and one orthologue group. The symbol(s) for the orthologue group will be reported, followed by a species breakdown of group members (LocusLink IDs), then by the 'best' score for each of the observed assay types. Coloration indicates the strength of the hit; no color indicates no data reported for that cell. Note that so long as at least one cell passes your filters, the best results for any other failing cells will also be indicated."],
          [ 'Compound Summary', "Brief summary of every compound provided in your input. The sheet will include entries that were ultimately suppressed by your filters."],
          # ['Target Summary', "Brief summary of every biological object hit by one or more of your compounds. The sheet will include entries that were ultimately suppressed by your filters."],
          ['Cluster Summary', "Listing of all Orthologue Clusters generated by the program. For each cluster, the loci within that cluster will be broken out by species. If you have allowed 'unclustered targets' to be kept, those will be listed at the bottom of the worksheet, identified by their name rather than a number. The sheet will include entries that were ultimately suppressed by your filters."],
          ['Excluded IDs' => "Lists compound IDs for which no targets are shown, and the reason why. 'Target Filters' show compounds with one or more targets, all of which were excluded by your filters. 'Unannotated' show known compounds that have no recorded targets. 'MapTracker' indicates that the compound is totally alien to the system (not in the most basic database). 'SMILES Structure' indicates that the compound ID is known, but has no structure associated with it."],
          ['Score Key' => 'A visual summary of the colors used to represent scores for different assay types, as well as a report of the filters used in this analysis, plus the total number of compound/target pairs that were excluded for each filter.'],
          ['Help' => 'This worksheet, with brief descriptions of all other worksheets and all the columns encountered within the workbook.'], );
    foreach my $sdat (@sheets) {
        $eh->add_row('Help', $sdat, ['hitem', 'htext']);
    }

    my $gTok = 'ZZZ';

    my $cols = {
        'Compound' => "The compound name as found in your original data",
        '# Loci' => "The number of distinct loci in a Cluster. In almost all cases this will be reporting the number of distinct species in a cluster, but it is possible for multiple loci to be clustered together (in the case of very similar family members)",
        '# Cmpd' => "The number of compounds in your data set that hit a cluster. Generally this will be 1, but if the cluster has multiple genes, or if you are reporting on compounds that hit multiple loci, it is possible that more than one compound could be selecting the cluster",
        'NS' => "The namespace of the object being described (like 'LocusLink Gene' or 'BMS Compound ID'). May be abbreviated.",
        "MTID" => "The internal MapTracker ID for an object. Represents a compact way to recover data for the object from many BMS systems",
        "SMILES" => "Structural form of the molecule in SMILES format. Many systems within and outside BMS can accommodate SMILES format as input",
        "# Var" => "The count of variant structures considered when querying one of your compounds. For example, in many cases the structures we use have ambiguous chirality; in such cases, the more specifically defined chiral forms will be included in the set of variants associated with that compound",
        "Variants" => "A list of all variants (in MapTracker ID format) considered for the compound. For example, in many cases the structures we use have ambiguous chirality; in such cases, the more specifically defined chiral forms will be included in the set of variants associated with that compound",
        "Description" => "A more-or-less human-readable description of the object being described",
        "C#" => "The orthologue cluster identifier, corresponds to the 'Cluster' column on the Cluster worksheet.",
        "LOD Set" => "The Hypergeometric distribution single point odds (NOT a p-value) of selecting the number of compounds for the indicated gene, given the compound set you have specified. Negative pink values indicate UNDER-enrichment. NOT multiple testing corrected.",
        "pV Set" => "The Hypergeometric distribution p-value for selecting the number of compounds for the indicated gene, given the compound set you have specified. Negative pink values indicate UNDER-enrichment. NOT multiple testing corrected.",
        "Cluster" => "The orthologue cluster identifier, sometimes represented as 'C#' for brevity. If the identifier is a number, it represents one or more LocusLink IDs that are 'the same gene' across one or more species. If it is a biological identifier (Swiss-Prot, GenBank, etc), it represents a target that could not be mapped into a LocusLink orthologue group (ie a lonely cluster of one). Note that such identifiers are generally poorly characterized, or from 'unusual' species.",
        "Sym" => "The gene symbol for the object being described. For distinct loci or proteins, it will be derived from the best-matched LocusLink gene. For orthologue clusters, it will be from the highest priority locus that has a symbol defined (species priority for this workbook is shown at the top of this page). If a cluster has multiple loci, the symbols will be joined with a comma and highlighted in red",
        "Symbol" => "The gene symbol for the object being described. For distinct loci or proteins, it will be derived from the best-matched LocusLink gene. For orthologue clusters, it will be from the highest priority locus that has a symbol defined (species priority for this workbook is shown at the top of this page).",
        "Target" => "A specific biological target for which chemistry hits were found. In the majority of cases this will be a Swiss-Prot ID",
        "Species" => "The scientific species name assigned to the object being described",
        "Loci" => "One or more LocusLink IDs associated with an object. Generally this will be a unique value, but in some cases there may be multiple loci found with equally good association scores; in such cases, all will be listed",
        "T->L" => "A score reflecting the quality of association between the target (often a protein) and LocusLink Gene. A value of 100 indicates perfect association, a value of 0 represents perfect NON-association, and undefined values mean that there is an association that lacks reliable quantitative information (potentially true, but unknown confidence",
        "C2T Rows" => "Row numbers in the 'Compound to Target' worksheet associated with this object. Note that if you have changed the sorting of that worksheet then of course these numbers will no longer be accurate",
        "AllHits" => "The TOTAL number of your input compounds that were found associated with this target, PRIOR to filtnnering.",
        "KeptHits" => "The number of hits from this target that are actually shown in the 'Compound to Target' worksheet (it is the count of row numbers shown in the 'C2T Rows' column)",
        "Size" => "The number of distinct LocusLink IDs present in an Orthologue cluster",
        "Sym Tax" => "The species ultimately used to determine the gene symbol (and description) for an Orthologue Cluster",
        "MapTracker" => "This is the internal MapTracker score. It is a normalized score that is either undefined (unknown / unspecified value, sometimes encoded by -1), or ranges between 0 and 1, where 1 is 'perfect' and 0 is 'explicitly not'. All other quantified scores are projected onto this normalized scale, as described in the 'Score Key' worksheet",

        $gTok.'Spec0     ' => '',
        $gTok.'Spec1a Species Notes' => "When Compound to Target data are reported, the targets are collapsed to orthologue groups. The view will include columns for individual species. The value in the cell will correspond to the LocusLink ID(s) representing that species within the orthologue group. If the cell is uncolored, it means that no assays were reported for that particular species. Coloration indicates that at least one assay was reported in the species; please note that this color will reflect the single most 'potent' value seen across all assays and all assay *types*. So if a species reports a Ki of 1e-7 M AND a %Ctrl 1uM of 0%, the color will be based on the %Ctrl 1uM.",
        $gTok.'Spec1b Species Alert' => "Note that initially only the 'primary' (as listed at the top of this sheet) species columns will be visible. The remaining will be hidden in the initial view.",
        $gTok.'Spec2 Homo sapiens' => 'Human loci (generally lots of information available)',
        $gTok.'Spec2 Mus musculus' => 'Mouse loci (generally a goodly amount of data)',
        $gTok.'Spec2 Rattus norvegicus' => 'Rat loci (moderate information available)',
        $gTok.'Spec2 Canis lupus familiaris' => 'Dog loci, aka Canis familiars (occasional information available)',
        $gTok.'Spec2 Bos taurus' => 'Cow loci (sporadic information available)',
        $gTok.'Spec3 Other species' => 'Occasionally other data are available for additional species; if the species name is unfamiliar, we recommend Wikipedia to look it up',

        $gTok.'Unit0      ' => '',
        $gTok.'Unit1 Assay Type Notes' => 'The system is capable of recognizing the classes of assays shown below. Please note that due to the data reduction used to make this workbook managable, the cell reporting an assay value will only show the most potent value observed for that row; less potent values may also be represented in the database.',
        $gTok.'Unit2 Ki' => "'Dissociation constant' = the affinity between the compound and the target (http://en.wikipedia.org/wiki/Dissociation_constant). Molar Units.",
        $gTok.'Unit2 IC50' => "'Half maximal inhibitory concentration' = how much of a particular substance is needed to inhibit a given biological process (http://en.wikipedia.org/wiki/IC50). Molar Units.",
        $gTok.'Unit2 EC50' => "'Half maximal effective concentration' = the concentration of a drug or antibody which induces a response halfway between the baseline and maximum (http://en.wikipedia.org/wiki/Ec50). Molar Units.",
        $gTok.'Unit3 % Inhibition' => "'Percent Inhibition'. Available from a variety of sources, larger values are more potent",
        $gTok.'Unit3 %Ctrl 1uM' => "Ambit screening value = 'Percentage of control at 1uM compound concentration'. For Ambit results, smaller percentages represent more potent compounds.",
        $gTok.'Unit4 Generic' => "The Generic 'assay type' reports unqualified assay information. That is, an authority has indicated that the compound has been assayed against the target, but does not indicate how the assay was performed (or does so in an unusual fashion such that the parsing program was unable to recognize a standard assay type). Generic values are almost always also unquantified; that is, there will be no numeric component to the assay as well. A common source of generic information is DrugBank.",
        $gTok.'Unit9       ' => '',

    };
    $eh->add_row('Help', ['  ']);
    $eh->add_row('Help', ['Column','Description'],['hhead','hhead']);
    foreach my $col (sort { uc($a) cmp uc($b) } keys %{$cols}) {
        my $cname = $col;
        if ($cname =~ /^$gTok\S+ (.+)/) { $cname = $1 }
        my $frm = 'hitem';
        $frm .= 'Note' if ($cname =~ /(Note|Alert)/);
        $eh->add_row('Help', [$cname, $cols->{$col}], [$frm, 'htext']);
    }
}

sub HTML_START {
    return if ($nocgi);

    print "<html><head>\n";
    print "  <link rel='shortcut icon' href='/biohtml/images/TinyGraph1.png'>\n";
    print "  <title>Query Chem Bio - Create subgraphs from Chem Bio Network</title>\n";
    print "  <style>\n".&stnd_styles()."</style>\n";

    print "</head><body>\n";
    print "<center>\n";
    print "<h1>Query Chem Bio Networks</h1>\n";
    if ($isBeta) {
        print "<span class='err'>*** THIS VERSION IS BETA SOFTWARE ***</span><br />\n";
    }
    # print &SHOW_CVS_VERSION($VERSION, 'html cvs' ) ."<br />\n";
    print "</center>\n";
}

sub stnd_styles {
    my $msgPad = "4px";
    my $css =  <<EOF;
.node { font-size:1.2em; font-weight: bold; }
h2    { color: navy; }
.code { color: #c90; font-family: monospace; }
.err  { background-color: red ! important; color: yellow ! important;
        font-weight: bold; }
.tab    { border-collapse: collapse; }
.tab th { background-color: #ffc; }
.tab caption { font-size: 1.2em; font-weight: bold;
               color: #360; background-color: #ff9; }
.tab th, .tab td { empty-cells: show; vertical-align: top;
                   border: #fc9 solid 1px; padding: 2px; }

.butt { background-color: #cfc; font-weight: bold; white-space: nowrap;
        border-bottom: #999 solid 1px; border-right: #999 solid 1px;
        padding: 0px 2px; margin: 4px; cursor: pointer; text-decoration: none;}
a.butt:hover, a.format:hover
{ border-top: #999 solid 1px; border-left: #999 solid 1px;
  border-bottom: none; border-right: none; background-color: #6f6; }
 .olddir { background-color: grey ! important; }

.input { color:#f63; font-weight:bold; }

.small { font-size: 0.7em }
.big { font-size: 1.3em }

 .msgh {
   clear: none;
   float: left;
     margin-top: $msgPad;
 }
 .msgb {
   float: left;
   color: #366;
     margin-top: $msgPad;
 }
 .twhelp {
     background-color: #ff9;
     text-decoration: none;
     padding-left:  2px;
     padding-right: 2px;
     margin-left:   2px;
     margin-right:  2px;
     font-size:     1.0em;
     font-weight:   bold;
   color:   #c30;
   display: inline;
 }
EOF

    while (my ($t, $c) = each %{$nodeColor}) {
        $css .= sprintf(" .%s { background-color: %s; }\n", $t, $c);
    }

    return $css;
}

sub HTML_END {
    return if ($nocgi);
    print "</body></html>\n";
}

sub cached_data {
    my $gas = BMS::MapTracker::GenAccService->new( @gasCom, @_ );
    $gas->use_beta( $isBeta );
    my $rows = $gas->cached_array( $clobber );
    # my %foo = (@gasCom, @_); die $args->branch(\%foo)."less ".$gas->val('output').".param\n\n";;
    return $rows;
}

