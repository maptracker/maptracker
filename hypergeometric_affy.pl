#!/stf/biobin/perl -w

BEGIN {
    # Needed to make my libraries available to Perl64:
    use lib '/apps/sys/perl/lib/site_perl/5.12.0/';
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

my $dumpfile = "/stf/biohtml/tmp/testLog.txt";
chmod(0666, $dumpfile) if (-e $dumpfile);

my $VERSION = 
    ' $Id$ ';
my $progVers = 0;
if ($VERSION =~ /\,v (\S+)/) { $progVers = $1 }

=head1 NAME

 AffyHyperGO - Perform set based hypergeometric distribution analysis

=head1 DESCRIPTION

 For a list of affymetrix probe sets (or other biological
 identifiers), finds associated ontology terms that appear to be
 over-represented in that list, compared to the overall represenation
 of each ontology term in the reference set as a whole.

=head1 Precalculation

Create a list of all primary objects in the set, for example:

  genacc_service.pl -id HG_U133A -ns1 aad -ns2 aps -format list > ids.list

Then run populating conversions on the relevant namespaces:

  genacc_service.pl -scramble -ageall '12 Aug 2009' -warn -ns2 aad,set,go,msigdb,pmid,btfo,cdd -populate 40 -idlist ids.list -ns1 aps 

=head1 To Do

Versioned RefSeq IDs (and presumably other sequence IDs) are not
accurately recognized (M. Healy).

Add option for "Show top N hits irregardless of score" (M. Healy)

=cut

use strict;
use BMS::ArgumentParser;
use BMS::MapTracker::Services;
use BMS::AffyHyperGO;
use BMS::TicketManager;
use LWP::UserAgent;

my $argvNum = $#ARGV + 1;
$argvNum    = 0 if ($argvNum == 1 && (!defined $ARGV[0] || $ARGV[0] eq ''));
# use Proc::Daemon;
# &log("$$ - Modules loaded [$argvNum]");

my $ldap = BMS::Utilities::BmsDatabaseEnvironment::ldap();
my $tDir = &ticketDirectory("/stf/biohtml/GSEA_Analysis");
$tDir .= "/".&ticketUser($ldap);
&ticketType('GSEA');
&ticketComs("This file is a 'ticket' describing a GSEA analysis",
            "It contains information used to execute the analysis, ".
            "as well as file paths to the resulting output.");

my @months = ('JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 
              'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC');


my $paramAlias = {
    query_path => [qw(input file listfile)],
    query      => [qw(PROBES PROBE PROBESET LIST IDS ID)],
    nonull     => [qw(removenull)],
    mode       => [qw(fullset)],
    array      => [qw(set)],
    ans        => [qw(transform)],
    int        => [qw(intersection)],
    sortdesc   => [qw(sortdown sortdn)],
    sortasc    => [qw(sortup)],
    pcutoff    => [qw(P PCUT PVAL PVALUE)],
    ecfilt     => [qw(ecfilter ec ecs)],
    eccustom   => [qw(eccust)],
    errormail  => [qw(errmail)],
    ontology   => [qw(ons ontologies)],
    redons     => [qw(rebuildns clobber)],
    comparelod => [qw(lodcomp)],
    aslod      => [qw(aslods)],
    applybonf  => [qw(docorrect)],
    xxxx       => [qw()],
    xxxx       => [qw()],
    xxxx       => [qw()],
};

my $ticket;
my $preArgs = BMS::ArgumentParser->new
    ( -nocgi => $argvNum || !$ENV{'HTTP_HOST'} || $ENV{FORCE_NOCGI} ? 1 : 0,
      -paramAlias => $paramAlias, );
# &protect_args($preArgs);

my $usingTicket = $preArgs->val(qw(ticket));
my $tickReq     = $preArgs->val(qw(background ticket status));
$ticket         = BMS::TicketManager->new( $tickReq ) if ($tickReq);
# die "$tickReq = ".$preArgs->branch($ticket);
if ($ticket) {
    my %okNotUpper = map { uc($_) => $_ } qw(setFile);
    foreach my $pair ($ticket->each_param) {
        my ($key, $val) = @{$pair};
        my $uckey = uc($key);
        next unless ($uckey eq $key || $okNotUpper{$uckey});
        $preArgs->set_val($uckey, $val) unless (defined $preArgs->val($uckey));
    }
}

my %pa   = $preArgs->export();
my $args = BMS::ArgumentParser->new
    ( -show_cvs_vers => 'html cvs',
      -age           => '30 aug 2015',
      -cloudage      => '30 aug 2015',
      -limit         => 30,
      -set           => '',
      -matched       => 80,
      -maxperc       => 50,
      -minrep        => 3,
      -query         => '',
      -mode          => 'auto',
      -bonfterm      => 1,
      -applybonf     => 1,
      -randtest      => 0,
      -randsize      => '',
      -randbonf      => 1,
      -pcutoff       => 0.05,
      -probsize      => 300,
      -ans           => '',
      -ecfilt        => '!ND !NR !E',
      -eccust        => '',
      -purgens       => 1,
      -nonull        => 0,
      -verbose       => 1,
      -fast          => 0,
      -pmode         => "Slow",
      -graphviz      => 1,
      -zmax          => 0,
      -output        => '',
      -simplify      => 1,
      -terminlist    => 1,
      -ignoreonto    => "PMID,XONT",
      -headers       => 0,
      -ismatrix      => 0,
      -showtree      => 1,
      -default       => 'standard',
      -urls          => "/stf/biohtml/stylesheets/GenAccServiceURLs.txt",      
      -sumnum        => 3,
      -background    => 0,
      -status        => 0,
      -tiddlywiki    => 'HyperGeometric',
      -errormail     => 'charles.tilford@bms.com',
      # %pa,
      -paramAlias    => $paramAlias,
      );

if ($ticket) {
    $args->manage_callback( 'DeathCallback', sub {
        my $self = shift;
        my $msg = $_[0] || [];
        $msg = $msg->[1] || "Unknown cause";
        $msg =~ s/^\s+//;
        $msg =~ s/[\n\r]+/ /g;
        $ticket->status("FatalError", $msg);
        $args->err(@_);
    }, 'global');
}

while (my ($key, $val) = each %pa) {
    $args->set_param($key, $val);
    $args->flag_as_default($key) if ($preArgs->is_default($key));
}

my $bgnd     = $args->val('background') ? 1 : 0;
my $nocgi    = $bgnd ? 1 : $args->val('nocgi') ? 1 : 0;
my $trueBgnd = ($bgnd && !$args->val('bgoverride')) ? $bgnd : 0;
srand( time() ^ ($$ + ($$<<15)) );
if ($nocgi) {
    $args->error_mail( $args->val(qw(errormail)));
    $args->intercept_errors() if ($bgnd);
    if (my $forcemime = $args->val(qw(setmime))) {
        $args->set_mime( -mime => $forcemime );
    }
    $args->shell_coloring( ) unless ($bgnd || $args->val('noshellcolor'));
} else {
    $args->set_mime( );
}

&protect_args($args);

my @failedListGuess;

my @valcols =
    ( [ 0.000001, 'red' ],
      [ 0.0001, 'green' ],
      [ 0.05, 'blue' ],
      [ 1, '#669999' ] );

my @ecfilt =
    ( [ '', 'No filters' ],
      [ '!ND !NR !E', 'Discard lame evidence (ND, NR, E)' ],
      [ '!ND !NR !E !IEA !P', 'Discard lame and electronic evidence (ND, NR, E, IEA, P)' ],
      [ 'TAS' , 'Only consider Traceable Author Statements'],
      );


my %tf2array = 
    (
     "1" => "HUMAN_Uniprot",
     "10" => "MG_U74A",
     "11" => "MG_U74AV2",
     "12" => "MG_U74B",
     "13" => "MG_U74BV2",
     "14" => "MG_U74C",
     "15" => "MG_U74CV2",
     "16" => "MOE430A",
     "17" => "MOE430B",
     "18" => "RAE230A",
     "19" => "RAE230B",
     "2" => "Saccharomyces_cerevisiae",
     "20" => "RG_U34A",
     "21" => "RG_U34B",
     "22" => "RG_U34C",
     "23" => "RN_U34",
     "3" => "HG_U133A",
     "4" => "HG_U133A_2",
     "5" => "HG_U133A_TAG",
     "6" => "HG_U133B",
     "7" => "HG_U133_PLUS_2",
     "8" => "HU35KSUBA",
     "9" => "HU6800",
     );


my %array2tf;
while (my ($aid, $aname) = each %tf2array) {
    $array2tf{$aname} = $aid;
}

my $hostUrl  = "http://bioinformatics.bms.com";
my $gasUrl   = "$hostUrl/biocgi/genacc_service.pl";
my $veryVB   = $args->val(qw(veryverbose)) ? 1 : 0;
my $vb       = $veryVB || $args->val(qw(verbose)) ? 1 : 0;
my $doWarn   = $args->val(qw(warn));
my $asCurve  = $args->val(qw(PROFILE));
my $ontoMem  = $args->val(qw(ONTOMEMBERS));
my $nonull   = $args->val(qw(nonull));
my $purgens  = $args->set_val('purgens', $args->val('purgens') ? 1 : 0);
my $til      = $args->val(qw(TERMINLIST));
my $mode     = $asCurve ? 'GSEA' : $args->val(qw(mode)) || '';
my $set      = &unParam('Set Name', qw(array)) || '';
my $transf   = $args->val('ans') || 0;
my $refer    = &unParam('Reference', qw(reference)) || '';
my $matched  = $args->val(qw(min matched)) || 0;
my $transMat = $matched;
my $maxperc  = $args->val(qw(MAXPERC));
my $minrep   = $args->val(qw(MINREP));
my $bonfgo   = $args->set_val('bonfterm', $args->val('bonfterm') ? 1 : 0);
my $bonflist = $args->set_val('bonflist', $args->val('bonflist') ? 1 : 0);
my $ontoReq  = $args->val(qw(ontoterms term terms));
my $dumpsql  = $args->val(qw(DUMPSQL)) || 0;
my $headNum  = $args->val(qw(headers header)) || 0;
my $isMatrix = $args->set_val('ismatrix', $args->val('ismatrix') ? 1 : 0);
my $restRef  = $args->val(qw(restrict restrictref)) || "";
my $ptable   = $args->val(qw(PROBTABLE));
my $idsForTerm = $args->val('IdsForTerm');
my $preCut   = $args->val(qw(precut));
my $doSimp   = $preCut ? 0 : $args->val(qw(SIMPLIFY));
my $randc    = $args->val(qw(RANDTEST)) || 0;
my $rands    = $args->val(qw(RANDSIZE));
my $selfRef  = $args->val(qw(SELFREF));
my $noExpand = $args->val(qw(noexpand));
my $isFast   = $args->set_val('fast', $args->val('fast') ? 1 : 0);
my $pMode    = lc($args->val('pmode') || "");
$pMode       = $args->set_val('pmode', $pMode =~ /lod/ ? 'LOD' :
                              $pMode =~ /slow/ ? 'Slow p-Value' :
                              'HyperQuick');
my $leafOnly = $args->val(qw(LEAFONLY));
my $format   = lc(&unParam('Format', qw(fmt format)) || '');
$format      = $nocgi ? 'tsv' : 'html' unless ($format);
my $dataMode = lc(&unParam('Data Mode', qw(datamode)) || '');
my $standSz  = lc(&unParam('Standard', qw(standard)) || 'Standard');
my $probSz   = $args->val(qw(PROBSIZE));
my $setFile  = &unParam('Set File', qw(setfile)) || '';
my $sffh     = $args->val(qw(SET_PC_FH)) || $args->val(qw(SET_PC));
my $doDebug  = $args->val(qw(DEBUG));
my $zmax     = $args->val(qw(ZMAX)) || 0;
my $noNot    = $args->val(qw(NONOT)) || 0;
my $clean    = $args->val(qw(CLEAN));
my $setDir   = $args->val('setdir') || "/stf/biohtml/GSEA";
my $tempDir  = $args->val(qw(tempdir tmpdir)) || "/stf/biohtml/tmp";
my $noRun    = $args->val(qw(NORUN));
my $isTemp   = $args->val(qw(TEMP));
my $priorRes = $args->val(qw(results));
my $exTerm   = $args->val(qw(exterm excludeterm)) || '';
my $listReq  = &unParam('List Number', qw(listnum)) || 0;
my $forceSet = $args->val(qw(forceset rebuildset));
my $isSetName = $args->val(qw(issetname));
my $forceTk  = $args->val(qw(forceticket));
my $ofile    = &unParam('Output', qw(output));
my $splitter = '\n\r\t\,' . ($args->val(qw(KEEPSPACE)) ? '' : '\s');
my $rsplitter = '\t\,' . ($args->val(qw(KEEPSPACE)) ? '' : '\s');
my $outputEstablished = 0;
my $ignoreIgnore = ($dataMode eq 'database') ? 0 : 1;
my $sortKeyDown  = $args->val(qw(sortdesc)) || "";
my $sortKeyUp    = $args->val(qw(sortasc)) || "";
my $usePerfect   = $args->val(qw(useperfectguess)) || 0;
my $keepDash     = $args->val('keepdash');

my $reqNs;
if (my $fs = $forceSet) {
    # Just collect some settings here to simplify force rebuild of setfiles.
    if ($fs =~ /(GSEA_(\S+)_([A-Z]{2,4})_(\d{3}|SLIM))/) {
        my ($of, $setName, $sns, $perc) = ($1, $2, $3, $4);
        $setFile ||= $of.".set";
        $fs        = $setName;
        $fs        =~ s/\-/ /g unless ($fs =~ /ercc/i || $keepDash);
        $reqNs     = $sns;
        $standSz   = $perc + 0 if ($perc =~ /^\d+/);
    } elsif ($fs =~ /\.list$/) {
        $setFile ||= "Custom-Set";
        $fs = "Custom";
    }

    $reqNs ||= $args->val(qw(forcens));

    # GSEA_Mus-musculus-LocusLink_LL_005.set
    my $fsPrfx = "ForceSet-${fs}-";
    $fsPrfx    =~ s/[\s\,\n\r]+/-/g;
    $ofile   ||= $fsPrfx. "Output.tsv";
    $forceTk ||= $fsPrfx. "Ticket.tick";
    unless ($setFile) {
        $setFile = $fsPrfx. "SetFile.set";
    }
    $set       = $fs;
    $dataMode  = 'db';
    # die "$fs -> $setFile [$transf] $standSz%";
}

if ($forceTk) {
    $args->msg("[?]","Existing ticket (".$ticket->ticket().") being overwritten by -forceticket $forceTk") if ($ticket);
    $ticket = BMS::TicketManager->new();
    $ticket->path($forceTk);
}

my @inputFilter;
if (my $ifReq = $args->val(qw(inputfilter))) {
    my @reqs = ref($ifReq) ? @{$ifReq} : ($ifReq);
    my $okOps = [ '>=' => '>=',
                  '<=' => '<=',
                  '=>' => '>=',
                  '=<' => '<=',
                  '>' => '>',
                  '<' => '<',
                  'ge' => '>=',
                  'le' => '<=',
                  'lt' => '<',
                  'gt' => '>',
                  'defined' => 'defined' ];
    foreach my $req (@reqs) {
        my ($l, $op, $r);
        for (my $o = 0; $o < $#{$okOps}; $o += 2) {
            my $chk = $okOps->[$o];
            if ($req =~ /^\s*(.+?)\s+(\!|not )?\Q$chk\E\s+(.+?)\s*$/i) {
                ($l, $op, $r) = ($1, $okOps->[$o+1], $3);
                $op = "!$op" if ($2);
                last;
            }
        }
        unless ($op) {
            $args->msg("[!!]",
                       "Failed to find logical operator in filter", "'$req'");
            next;
        }
        my $check = &_test_value(1, 2, $op);
        next unless (defined $check);
        push @inputFilter, {
            op => $op,
            l  => $l,
            r  => $r,
            txt => "[$l] $op [$r]",
        };
    }
}

if (0) {
    my $type = 
        $bgnd ? "Background Process" :
        $ENV{FORCE_NOCGI} ? 'Forced NOCGI' :
        $preArgs->{STATUS} ? "Status Check" : "Unknown";
    if ($type eq 'Status Check') {
        &log("$$ - $type");
    } else {
        my @logs = ("", "- " x 35, `date`,
                    "PID    = $$",
                    "User   = $ldap",
                    "Type   = $type",
                    "ARGV   = $argvNum",);
        push @logs, "Ticket = ".$ticket->path() if ($ticket);
        foreach my $key (sort keys %{$preArgs}) {
            push @logs, sprintf
                ("  %20s => %s", $key, defined $args->val($key) ?
                 $args->val($key) : '-undef-');
        }
        &log(@logs);
    }
}

$args->debug->skip_key
    ( ['dbi', 'TRACKER', 'CLASSES', '_gsf_tag_hash', 'BENCHMARKS',
       'END', 'START', 'NETWORK', 'COLLECTION', 'OTHERSETS' ] );

if ($exTerm) {
    $exTerm = [ split(/[$splitter]+/,$exTerm) ] unless (ref($exTerm));
    $exTerm = '' if ($#{$exTerm} == -1);
}

my %skip = ( Analysis => 1, Recent => 1 );
my @pbits = split(/\//, $0);
my $shortProg = $pbits[-1];

if ($selfRef || $mode =~ /gsea/i) {
    $mode = 'GSEA';
} elsif ($mode =~ /auto/i) {
    $mode = 'Auto';
} else {
    $mode = 'Full';
}

my ($resFH, $resFile);

if ($forceSet) {
    $dataMode = 'database';
} elsif ($dataMode =~ /file/i) {
    $dataMode = 'file';
} elsif ($sffh || $setFile) {
    # If there is a setfile, ignore everything else
    $dataMode = 'file';
} elsif ($dataMode =~ /(cust|db|database)/i) {
    $dataMode = 'database';
} elsif ($dataMode =~ /(stand)/i) {
    $dataMode = 'standard';
} else {
    $dataMode = 'standard'; 
}

if ($standSz =~ /\d+/) {
    # Standard percentage
} elsif ($standSz =~ /(insane|crazy)/) {
    $standSz = 'Insane';
} elsif ($standSz =~ /(exhaus|full)/) {
    $standSz = 'Exhaustive';
} elsif ($standSz =~ /(rapid)/) {
    $standSz = 'Rapid';
} else {
    $standSz = 'Standard';
}

my $pcut = $args->set_val('pcutoff', $asCurve ? 0 : 
                          $args->val('pcutoff') || 0);


$ontoReq ||= []; $ontoReq = ref($ontoReq) ? 
    $ontoReq : [ split(/\s*[\n\r\t\,]+\s*/, $ontoReq) ];
my $ecfilt  = uc($args->val('ecfilt') || '');
if ($ecfilt =~ /CUST/) {
    $ecfilt = uc($args->val(qw(eccustom)) || '');
}
$ecfilt =~ s/\![\n\r\s\t\,]+/\!/g;

# Assume a LOD score is being passed if the p-cutoff is greater than 1
$pcut = 10 ** (0-$pcut) if ($pcut > 1);

my ($mts, $mt, $setHTML, $sc, @requests);

$args->ignore_error("Failed to connect to MapTracker");
$args->ignore_error("dbname=maptracker");
eval {
    $mt = BMS::MapTracker->new( -dbadmin => 0 );
};
unless ($mt) {
    &err("MapTracker database off-line; some features will not be available",
         'NoMapTracker', 'TICK');
    $mt = 0;
}
my $ahg  = BMS::AffyHyperGO->new( -tracker => $mt, );
$ahg->web_output( $args->val('forceweb') );

my $ad;
eval { $ad = $ahg->denorm; };

my ($lastT, $startT, %sths);
my $smallSetWarn = 0;
my $progTime = 15;
if (my $sPath = &unParam('Set Path', qw(setpath))) {
    if (open(SPATH, "<$sPath")) {
        if ($set) {
            $args->msg("Replacing contents of set with those in file",
                       "$set > $sPath");
        }
        $set = [];
        while (<SPATH>) {
            s/[\n\r]+$//; s/^\s+//; s/\s+$//;
            my @row = split(/\s*[\t]+\s*/);
            if (my $id = $row[0]) { push @{$set}, $id; }
        }
        close SPATH;
    } else {
        $args->err("Failed to read file specifying set members", $sPath, $!);
    }
} elsif ($forceSet || $isSetName) {
    # Do not split when doing force set
} elsif ($set =~ /[$splitter]/) {
    my $gns = $ad->guess_namespace($set);
    unless ($gns eq 'SET') {
        my @list = split(/[$splitter]+/, $set);
        map { s/^\s+//; s/\s+$//; } @list;
        $set = \@list;
    }
}
unless (ref($set)) {
    # Normalize array names
    my @seqs = $mt->get_seq( -id => $set, -nocreate => 1,
                             -defined => 1);
    if ($#seqs == -1) {
        my $try = $set;
        if ($try =~ /humanht.(\d+).v(\d+)/i) {
            $try = "HumanHT-$1 V$2";
        } else {
            $try =~ s/\-/_/g;
        }
        @seqs = $mt->get_seq( -id => $try, -nocreate => 1,
                              -defined => 1);
        $set = $try unless ($#seqs == -1);
    }
}


# print "<pre>".$args->to_text()."</pre>";
# $args->write_log() unless ($args->val(qw(AJAXTIME BACKGROUND)));

my $button = "<input type='submit' style='font-size:larger;font-weight:bold;background-color:#9f9;color:#00f' value='Run Analysis' />";

my @iteropts =
    ( ['Auto','Automatic','Ranked GSEA performed if your query list has over half of the reference set in it'],
      ['Full','Single Sublist','Query list treated as a single unranked list'],
      ['GSEA','Gene Set Enrichment Analysis','Ranked list; All possible sublists analyzed'],
      );

my @refopts = (0, 'LocusLink Gene', 'Affy Probe Set',
               'RefSeq Protein', 'RefSeq RNA', );
my @mapOpts = qw(Best Average);
my $transMap = lc(&unParam('Trans Map', qw(transmap)) || '');
if ($transMap =~ /(average|avg)/) {
    $transMap = 'Average';
} else {
    $transMap = 'Best';
}

# for my $i (0..350) { warn sprintf("10 ** -%d = %s\n",$i,10 ** (0-$i));}die;

#&WRITE_LOG( -args     => $args,
#            -capture  => [ 'QUERY', 'MATCHED', 'TRANSFORM', 'MODE', 
#                           'FORMAT','ARRAY'], ) unless ($clean);



if (0) {
    $ahg->expectation
        ( -setsize    => $args->val(qw(SETSIZE)) || 22283,
          -classsize  => $args->val(qw(CLASSSIZE)) || 5,
          -size       => defined $args->val('SIZE') ? $args->val('SIZE') : 100,
          -iterations => $args->val(qw(ITER)) || 1000,
          );
    die;
}


if ($ad) {
    $ad->dbh->verbose(1);
    $ad->age( &unParam('GenAcc Age', qw(ageall allage age)) );
    $ad->cloud_age( &unParam('Cloud Age', qw(ageall allage cloudage)) );
    $ad->dbh->default_progress_delay(15);
    if ($set && !ref($set)) {
        ($set) = $ad->standardize_id($set, 'SET');
    }
    $ad->process_url_list( $args->val(qw(URLS)) );
}
$ahg->dumpsql( $dumpsql );


my $path = $ENV{SCRIPT_NAME} || $ENV{REQUEST_URI} || 
    $ENV{SCRIPT_FILENAME} || $ENV{HTTP_REFERER} || $0 || '';

my $isbeta = ($path =~ /working/ ) ? 1 : 0;

my $outfh  = *STDOUT;
my $statfh = *STDOUT;
my $fh     = $args->val(qw(QUERY_PC_FH QUERY_PC));
my $workFile = $ofile;
if ($workFile) {
    $workFile .= ".working";
    if (open(WORK, ">$workFile")) {
        print WORK $args->to_text();
        close WORK;
        chmod(0666, $workFile);
    } else {
        $args->err("Failed to write working file", $workFile, $!);
        $workFile = 0;
    }
}

my ($qfile, $query);
if ($forceSet) {
    $query = "BogusIdForForcingSetCreation";
} else {
    $qfile = $args->val('query_path');
    $query = $args->val('query');
}
$args->set_param('query', $query || "");
$args->set_param('file',  $qfile ||= "");

if ($format =~ /html/) {
    $format = $format =~ /sum/ ? 'HTML Summary' : 'HTML';
} elsif ($format =~ /tsv/) {
    $format = $format =~ /sum/ ? 'TSV Summary' : 'TSV';
} elsif ($format =~ /(text|txt)/) {
    $format = 'Text';
} elsif ($format =~ /(excel|work|book|sheet)/) {
    $format = 'Excel';
} else {
    &err("Unknown format '$format'", 'UnknownFormat', 'TICK');
}

my $fmtDat = {
    TSV            => [ 'tsv', 'tsvOutput' ],
    Excel          => [ 'xls', 'excelOutput' ],
    Text           => [ 'txt', 'textOutput' ],
    HTML           => [ 'html', 'htmlOutput' ],
    Benchmarks     => [ 'status', 'ticketStatus' ],
    'Set Details'  => [ '', 'setSummary' ],
    'Query List'   => [ '', 'FILE' ],
    'HTML Summary' => [ 'sum.html', 'summaryOutput' ],
};

my (@ignoreOntos, %igFlags, %ontoStat);
if ($ad) {
    @ignoreOntos = sort map 
    {$ad->namespace_name($_)} qw(BTFO CDD GO PMID MSIG);
    if (my $igo = $args->val(qw(IGNOREONTO))) {
        # These are specific requests to ignore an ontology
        my $hash = $ad->process_filter_request($igo);
        my @ok;
        while (my ($tag, $list) = each %{$hash}) {
            foreach my $tok (@{$list}) {
                if (my $name = $ad->namespace_name($tok)) {
                    push @ok, $tok;
                    $igFlags{$name} = '!';
                }
            }
        }
        $args->set_param('IGNOREONTO', join(',', sort @ok));
    }
    if ($ignoreIgnore) {
        # We are not in database mode, so ignore this setting
        $args->clear_val('IGNOREONTO');
    } else {
        # We should consider the ignore criteria as well
        %ontoStat = %igFlags;
    }
    if (my $ggo = $args->val('ontology')) {
        # Generic requests to keep or ignore ontologies
        my $hash = $ad->process_filter_request($ggo);
        while (my ($tag, $list) = each %{$hash}) {
            foreach my $tok (@{$list}) {
                if (my $name = $ad->namespace_name($tok)) {
                    my $flag = $tag eq 'IN' ? '' : '!';
                    $ontoStat{$name} = $igFlags{$name} = $flag;
                }
            }
        }
    }
}

my $otxt     = join("\t", map { $ontoStat{$_} . $_ } sort keys %ontoStat);
$ahg->verbose( $nocgi ? 1 : 'html');
$ahg->option('tsv meta cols', $args->val(qw(TSVMETA)));
$ahg->option('debug', $doDebug);
$ahg->option('bonferroni term', $bonfgo);
$ahg->option('bonferroni list', 0);

&HTMLSTART;
&HTMLMENU;

if ($ticket && $nocgi && $vb) {
    $ticket->status_callback( sub {
        my ($dt, $tag, $details) = @_;
        $details =~ s/\<[^\>]+\>//g;
        warn sprintf("  %s [%20s] %s\n", $dt, $tag, $details);
    });
}

if ($ticket) {
    if (defined $args->val(qw(JOBNOTE))) {
        $ticket->extend( { Notes => $args->val(qw(JOBNOTE)) } )
    }
}

my @userArgs = $args->all_keys( -nodefault => 1, -skip => [qw(h help)] );
if (my $file = $args->val('show')) {
    &show_file($file);
} elsif ($args->val(qw(status))) {
    &ticket_status( $ticket );
} elsif ($args->val(qw(rerun))) {
    $skip{Analysis} = 0;
} elsif (my $sr = $args->val('showref')) {
    print $ahg->stat_reference_html( -notoggle => ($sr =~ /hide/i) ? 0 : 1,
                                     -show => 1,);
} elsif (($nocgi && !$bgnd && $#userArgs == -1) || $args->val(qw(h help))) {
    if ($nocgi) {
        system("pod2text $0");
        print &recent();
    } else {
        print &helpHTML();
    }
} elsif ($args->val(qw(PREFS))) {
    &err("Preferences system has not been developed yet.");
    if ($nocgi) {
    } else {
    }
} elsif ($args->val(qw(popularity popular))) {
    &set_popularity();
} elsif (my $req = $args->val(qw(HISTORY))) {
    &ticket_history($req);
} elsif ($priorRes) {
    &show_results($priorRes);
} elsif ($usingTicket && !($noRun || $asCurve || $ontoMem)) {
    &ticket_summary($ticket);
} elsif ($noRun) {
    my ($msg, $tw) = 
        ("The settings used for ticket ".&tick_link($ticket).
         " have been used to configure the search options below. Make any changes you desire then 'Run Analysis'.", "NewAnalysis") if ($usingTicket);
    if ($msg) { &msg($msg, $tw); print "<p />\n" unless ($nocgi); }
} else {
    $skip{Analysis} = 0;
    $skip{Recent} = 0 unless ($bgnd);
}


if ($transf && $ad) {
    my $ns = $ad->namespace_name($transf);
    if ($ns) {
        $transf = $ns;
    } else {
        &err("I am unaware of a tranformation namespace called '$transf'",
             "TransformNameSpace", 'TICK');
        $transf = '';
    }
}

# If postcut is true, then the TSV file will be cut by simplification AFTER
# generating the full file, unless PRECUT has been specified
my $postCut = ($ofile && $format eq 'TSV Summary' && 
               $doSimp && !$args->val(qw(SUMMARIZE)) && !$preCut);

if ($refer && $ad) {
    my $ns = $ad->namespace_name($refer);
    if ($ns) {
        $refer = $ns;
        $ahg->reference($refer);
    } elsif ($refer =~ /cust/i) {
        &msg("Input is flagged as a custom namespace", "CustomInput");
        $refer = 'Custom';
    } else {
        &err("I do not know what the reference namespace '$refer' is",
             "ReferenceNameSpace", 'FATAL');
        $refer = '';
    }
}

my $inputCount = 0; map { $inputCount++ if ($_) } ($fh, $qfile, $query);
if ($inputCount) {
    $skip{Recent} = 1;
    &PARSEINPUT() unless ($skip{Analysis});
}

if ($doSimp && $doSimp ne '1') {
    # The user just wants to simplify a TSV file
    &SIMPLIFY( $doSimp, $ofile);
} elsif (!$skip{Analysis}) {
    &establish_output( ($bgnd || $isTemp) ? $ticket : undef, $ticket  );
    &ACT_ON_ARGS;
}

if ($doDebug) {
    warn $ahg->list_options;
}
if ($ofile) {
    if ($format =~ /html/i && $args->val(qw(FULLHTML))) {
        print $outfh "</body></html>";
    }
    close OFILE;
    chmod(0666, $ofile);
    unless ($bgnd || $nocgi) {
        if ($format =~ /html/i) {
            &show_file($ofile);
        } else {
            print "";
        }
    }
    &SIMPLIFY( $ofile ) if ($postCut);
    &msg("Output written to $ofile") if ($nocgi);
}

print &recent() unless ($skip{Recent});
&MAINFORM unless ($skip{MainForm});
if ($args->val(qw(benchmarks benchmark))) {
    &err($ahg->showbench(), 'BenchMarks');
}
&HTMLEND;

unlink($workFile) if ($workFile);

sub protect_args {
    my $params = shift;
    $params->xss_protect('all');
    map { $params->file_path_protect($_, '\.\.');
          $params->file_path_protect($_, '^\/(apps|bin|etc|lib|lib64|logs|lost\+found|mnt|opt|proc|root|scratch|sbin|selinux|sys|usr|var)') 
          } qw(query_path setfile setpath output forceset rebuildset set_pc status);
}

sub unarray {
    my ($val, $what) = @_;
    my $r = ref($val);
    return $val if (!$r);
    $args->death("$what has been passed as a $val reference",
                 "No logic for dealing with that!") unless ($r eq 'ARRAY');
    my %u; map { $u{uc($_ || "")} ||= $_ || ""} @{$val};
    delete $u{""};
    my @uv = values %u;
    return $uv[0] if ($#uv == 0);
    return undef if ($#uv == -1);
    $args->death("Multiple $what are specified, please use only one", @uv);
}

sub unParam {
    my ($what) = shift;
    return &unarray( $args->val(@_), $what );
}

sub show_file {
    my ($file) = @_;
    my $request = $file;
#    die $ticket;
    if (!-e $file) {
        $file = $ticket->param($file) if ($ticket && $ticket->param($file));
        unless ($file) {
            $args->err("Failed to find file", $file);
            return;
        }
    }
    if ($file =~ /\.list$/ || $_[0] eq 'FILE') {
        $ad->benchstart("Show query list");
        open(QFILE, "<$file") || &err
            ("Failed to read '$file':\n  $!", 'FileOpenFailure', 'FATAL');
        my @prelim = &_READ_LIST_FH( *QFILE );
        close QFILE;
        my @lists;
        map { push @lists, $_ if ($#{$_->{list} || []} != -1) } @prelim;
        my $fileUrl = &path2url($file, $file, 'small file butt');
        if ($listReq) {
            my $list = $lists[$listReq - 1];
            my $lns  = $list->{param}{NAMESPACE};
            &TRANSFORM_LIST
                ( $list, $transf, $transMat, $transMap,
                  $args->val(qw(int)), $args->val(qw(INTNS)), 'noMap')
                if ($transf && $transf ne $lns);
            my $desc;
            if ($ticket && $ticket->param('setFile')) {
                $desc = &rip_descriptions( $ticket->param('setFile') );
            }
            if ($nocgi) {
                
            } else {
                print "<table class='tab'>\n";
                printf("<caption class='large'>Details for <span class='sublist'>%s</span> (list %d)",
                       $list->{name} || 'Anonymous', $listReq);
                if ($ticket) {
                    print "<br />\nTicket:&nbsp;".&tick_link($ticket);
                } 
                print "<br />\nFile:&nbsp;" .
                    ($fileUrl ||"<span class='file'>$file</span>");
                if (my $tDesc = $list->{TransDesc}) {
                    print "<br />Transformation: $tDesc";
                }
                print "</caption>\n<tbody>\n";
                my $lok  = ($lns && $ad->namespace_is_linkable($lns)) ? 1 : 0;
                my $mdat = $list->{meta};
                my @meta = sort keys %{$mdat || {}};
                my @head = qw(Item);
                my $addRank;
                unless ($mdat && $mdat->{Rank}) {
                    $addRank = 1;
                    push @head, 'Rank';
                }
                push @head, 'Links' if ($lok);
                push @head, 'Description' if ($desc);
                print " <tr>\n".join
                    ('', map { "  <th>$_</th>\n" } (@head, @meta)) ." </tr>\n";
                my @idLists = ($list->{list}, $list->{filtered});
                for my $il (0..$#idLists) {
                    my $idL = $idLists[$il];
                    next if (!$idL || $#{$idL} == -1);
                    my $rCl = $il ? " class='filtered'" : '';
                    for my $i (0..$#{$idL}) {
                        my $item = $idL->[$i];
                        print " <tr$rCl>\n  <th>$item</th>\n";
                        my @row = ();
                        push @row, $i+1 if ($addRank);
                        push @row, $ad->namespace_links($item, $lns) if ($lok);
                        if ($desc) {
                            my $d = $desc->{uc($item)} || '';
                            if ($d =~ /(\{(ZERO|POOR|UNVAL|Some loci|Matches|Locus via)[^\}]+\})/) {
                                my $txt = $1;
                                my $cls = $txt =~ /(ZERO|POOR|UNVAL)/ ?
                                    'alert' : 'caution';
                                my $rep = "<span class='$cls'>$txt</span>";
                                $d =~ s/\Q$txt\E/$rep/;
                            }
                            if ($d =~ /^(\[[^\]]+\])/) {
                                my $txt = $1;
                                my $rep = "<span class='gene'>$txt</span>";
                                $d =~ s/\Q$txt\E/$rep/;
                            }
                            push @row, $d;
                        }
                        foreach my $m (@meta) {
                            my $val = $mdat->{$m}{$item};
                            push @row, defined $val ? $val : '';
                        }
                        print join('', map {"  <td>$_</td>\n"} @row);
                        print " </tr>\n";
                    }
                }
                print "</tbody></table>\n";
            }
        } else {
            if ($nocgi) {
                
            } else {
                my %seen;
                my @uparams = ( show => $file );
                if ($ticket && $ticket->param('FILE') eq $file) {
                    @uparams = ( ticket => &short_ticket_path($ticket),
                                 show   => 'FILE' );
                }
                map { $seen{$_} = 1 } map { keys %{$_->{param}} } @lists;
                my @params = sort keys %seen;
                print "<table class='tab'>\n";
                printf("<caption class='large'>Summary of %d query list%s",
                       $#lists + 1, $#lists == 0 ? '' : 's');
                print "<br />\nTicket:&nbsp;".&tick_link($ticket) if ($ticket);
                print "<br />\nFile:&nbsp;" .
                    ($fileUrl ||"<span class='file'>$file</span>");
                print "</caption>\n<tbody>\n";
                print " <tr>\n".join
                    ('', map { "  <th>$_</th>\n" }
                     ('List Name', '#', 'Links','Items', @params)) ." </tr>\n";
                my $num = 0;
                foreach my $list (@lists) {
                    my $items = $list->{list};
                    print " <tr>\n  <th>".($list->{name} || 'Anonymous')."</th>\n";
                    my @row = (++$num);
                    push @row, &selfUrl({@uparams, listnum => $num}, 'Show', 'butt');
                    push @row, $#{$items} + 1;
                    foreach my $param (@params) {
                        my $val = $list->{param}{$param};
                        $val = '' unless (defined $val);
                        push @row, $val;
                    }
                    print join('', map {"  <td>$_</td>\n"} @row);
                    print " </tr>\n";
                }
                print "</tbody></table>\n";
            }
        }
        $ad->benchend("Show query list");
        return;
    }
    my $tag;
    if ($file =~ /\.(txt|tsv)$/) {
        $tag = 'pre';
    } elsif ($file =~ /\.(xml)$/) {
        $tag = 'plaintext';
    }
    print "\n<$tag>" if ($tag);
    unless (-e $file) {
        # Can we regenerate?
        if ($request eq 'setSummary') {
            $sc = $ahg->get_dataset
                ( -file   => $ticket->param('setFile'),
                  -ticket => $ticket, );
            &_make_sc_summary_file( $sc, $file);
        }
    }
    if (-s $file) {
        print `cat $file`;
    } else {
        print "Summary data not found!\n";
    }
    print "</$tag>\n" if ($tag);
}

sub rip_descriptions {
    my ($file) = @_;
    my %desc;
    return \%desc unless ($file && -e $file);
    $ad->benchstart;
    open(RDESC, "<$file") || &err
        ("Failed to rip descriptions from '$file':\n  $!",
         'FileOpenFailure', 'FATAL');
    while (<RDESC>) {
        next unless (/^DESC/);
        chomp;
        my ($tok, $id, $d) = split("\t", $_);
        $desc{uc($id)} = $d;
    }
    close RDESC;
    $ad->benchend;
    return \%desc;
}

sub ticket_status {
    my ($ticket) = @_;
    unless ($ticket) {
        &task_failed("TaskFailed - Can not get ticket status for undefined ticket", "NoTicket");
         return;
    }
    my $status   = $ticket->status();
    my $tick     = $ticket->ticket();
    my $st       = "";
    my @rows;
    if ($#{$status} == -1) {
        @rows = [ 0,0,'ExecutionPending','Analysis has not yet begun'];
        $st = "<span class='hilite'>Waiting...</span><img src='/biohtml/images/Connection.gif' />";
    } else {
        my %stats = 
            ( TaskComplete => "<span class='large ref'>Completed</span>",
              ActionNeeded => "<span class='large alert'>Halted</span>",
              FatalError   => "<span class='large alert'>ProgramError</span>",
              TaskFailed   => "<span class='large alert'>Failed</span>",);
        $st = $stats{ $status->[-1][1] } || "<span class='large hilite'>Running</span><img src='/biohtml/images/Connection.gif' />";
        my $t1 = $#{$status} == -1 ? 0 : $status->[0][0];
        foreach my $s (@{$status}) {
            my ($ti, $tw, $detail) = @{$s};
            if ($tw eq 'TaskStart') {
                $t1  = $ti;
                @rows = () if ($clean && $clean eq '2');
                my ($sec,$min,$hour,$mday,$mon,$year) = localtime($ti);
                my $dt = sprintf("%4d-%s-%02d %02d:%02d:%02d",$year+1900,
                                 $months[$mon],$mday,$hour,$min,$sec);
                push @rows, [$dt, undef, $tw, $detail || ''];
                next;
            }
            $ti -= $t1;
            my $min = int($ti / 60);
            my $sec = $ti % 60;
            $detail ||= "";
            push @rows,  [$min, $sec, $tw, $detail];
        }
        unless ($rows[-1][2] =~ /(Complete|Finished|Done|Needed|Failed|ProgramError|FatalError)/) {
            if (my $ti = time - $status->[-1][0]) {
                my $min = int($ti / 60);
                my $sec = $ti % 60;
                push @rows,  [$min, $sec, "<img src='/biohtml/images/brainJar.gif' />", "<span class='busy'>Working on $rows[-1][2] ...</span>"];
            }
        }
    }
    if ($nocgi) {
        print "Status for Ticket $tick:\n";
    } else {
        print "<table pid='$$' class='tab'><caption>".&tick_link($ticket)."<br />\n".
            "Ticket status $st</caption><tbody>\n";
    }
    foreach my $row (@rows) {
        my ($min, $sec, $tw, $detail) = @{$row};
        if ($nocgi) {
            if (defined $sec) {
                printf(" %3d:%02d  %20s %s\n", $min, $sec, $tw, $detail);
            } else {
                printf(" %12s  %20s %s\n", $min, $tw, $detail);
            }
        } else {
            if (defined $sec) {
                printf("<tr><td class='time'>%d:%02d</td><td>%s</td><td>%s".
                       "</td></tr>\n", $min, $sec, ($tw && $tw !~ /^\</) ? 
                       &help($tw, $tw ) : $tw, $detail);
            } else {
                printf("<tr><td class='time'>%s</td><td>%s</td><td>%s".
                       "</td></tr>\n", $min, ($tw && $tw !~ /^\</) ? 
                       &help($tw, $tw ) : $tw, $detail);
            }
        }
    }
    print "</tbody></table>\n" unless ($nocgi);
    
    if ($st =~ 'Completed' && !$noRun) {
        my $rfile = $ticket->param('output');
        if ($rfile) {
            if (-e $rfile) {
                if ($nocgi) {
                    &msg("Output: $rfile");
                } elsif ($rfile =~ /\.xls$/) {
                    print &path2url($rfile, "View Excel Output",'larger butt')."<br />\n";
                    if (my $chk = &path2url($rfile, 'N/A')) {
                        print "<script>document.location = '$chk'</script>\n";
                    }
                } else {
                    print `cat $rfile`;
                }
            } else {
                &err("Output file '$rfile' does not exist", 
                     "FileNotFound", 'TICK');
            }
        } else {
            &err("No output file was defined", "FileNotFound", 'TICK');
        }
    } elsif ($st =~ /Failed/) {
        print "<h2>Your analysis failed!</h2>\n";
        my $reason = $rows[-1][3];
        print "<p class='alert'>$reason</p>\n";
        $reason =~ s/<[^>]+>//g;
        if ($reason =~ /No valid lists|Unable to find any Reference Sets/) {
            print "<p>GSEA could not recognize your list. It is possible that there is a typo in your list, in which case fixing the typo should help. However, it is also possible that you are providing genes/objects in a form that GSEA can not recognize, or can not reliably parse. The most common situation in this case is the use of gene symbols. It is <b>impossible</b> for software <i>alone</i> to reliably parse gene symbols - they're a mess. Software can provide suggested parsings, but a human will need to validate them.</p>\n";
            if ($ticket) {
                # warn $ticket->path();
                if (my $tfile = $ticket->param('FILE')) {
                    my $url = "$gasUrl?paramfile=/home/tilfordc/public_html/gseaListHelper.param&idlist=$tfile";
                    print "<p><img src='/biohtml/images/animYellowArrow.gif' /><a href='$url' class='butt' target='_blank'>This link</a> can be used to help you convert your list to LocusLink IDs, which is generally the prefered format for GSEA analysis. It will open a conversion tool that will generate a spreadsheet. You can then copy the LocusLink IDs out of the spreadsheet and use them for GSEA analysis. Please bear in mind that you may need to modify the preset values in the tool; guidance for this is provided in the link.</p>";
                }
            }
        }
    }
}

sub present_output {
    my ($path) = @_;
    if ($nocgi) {
        $args->msg("Results at $path");
    } elsif ($path =~ /\.html$/) {
        print `cat $path`;
    }
}

sub set_popularity {
    my $dir = &ticketDirectory();
    my $cmd = "grep setFile $dir/*/*/*/GSEA_*:??";
    my @used = split(/[\n\r]+/, `$cmd`);
    my %byTime;
    foreach my $line (@used) {
        if ($line =~ /GSEA_(\S+)_(\d{4}-\d{2}|SLIM).+:\s+setFile\s+.+(GSEA_.+\.set)/) {
            my ($user, $date, $file) = ($1, $2, $3);
            # Pseudo sets that should never have been used, or bugs:
            next if ($file =~ /(ListTracker|VIRTUAL|ARRAY)/); 
            $byTime{$date}{$file}++;
        }
    }
    my $rpt = "Used set files, sorted by date and popularity:\n";
    my @dates = $nocgi ? 
        sort { $a cmp $b } keys %byTime :
        sort { $b cmp $a } keys %byTime;
    foreach my $date (@dates) {
        $rpt .= ($nocgi ? $date : "<b>$date</b>")."\n";
        my $dh = $byTime{$date};
        my @files = sort { $dh->{$b} <=> $dh->{$a} || $a cmp $b } keys %{$dh};
        $rpt .= join("", map { sprintf("   %3d %s\n", $dh->{$_}, $_) } @files);
    }
    print $nocgi ? $rpt : "<pre>$rpt</pre>";
}

sub ticket_history {
    my ($req) = @_;
    # Get information about a user's history
    my ($hh, $html, $tickets) = &ticketHistory
        ( $req, &selfUrl( { history => '%s', clean => 1 })  );
    print "<div class='history'><h3>$hh</h3>\n";
    if ($#{$tickets} > -1) {
        my @hticks = map { BMS::TicketManager->new($_) } @{$tickets};
        $html .= &ticket_list_summary( \@hticks );

    }
    print "$html</div>\n";
}

sub ticket_list_summary {
    my ($list, $title) = @_;
    my $rv = "";
    my $lastd = '';
    my @rows;
    foreach my $htick ( @{$list}) {
        my $notes   = $htick->param('Notes') || '';
        my $setF    = $htick->param('setFile') || '';
        my $tick    = $htick->ticket;
        my ($d, $t) = $htick->date_time();
        if ($d eq $lastd) { $d = '' } else { $lastd = $d }
        my @row = ( $d, $nocgi ? $t : &tick_link($htick,$t) );
        my $raw = $htick->param('results');
        my $sz  = ($raw && -e $raw) ? -s $raw : 0;
        if ($sz) {
            my $hf = $htick->param('htmlOutput');
            $sz    = sprintf("%.3f Mb", $sz / 1000000);
            if (!$nocgi && $hf && -s $hf) {
                push @row, &selfUrl
                    ({status => &short_ticket_path($htick),
                      show => 'htmlOutput'}, $sz, 'butt');
                
            } else {
                push @row, $sz;
            }
        } else {
            my $abs = $htick->most_recent_status(1) || "";
            $abs = "Not Found" unless ($abs =~ /^(TaskFailed|ActionNeeded)/);
            $abs    = "<span class='alert'>$abs</span>" unless ($nocgi);
            push @row, $abs;
        }
        if ($nocgi) {
            $notes =~ s/\n/ \| /g;
        } else {
            $notes =~ s/\n/<br \/>/g;
        }
        if ($setF) {
            $setF =~ s/^.+\///;
            if ($setF =~ /^GSEA_(.+)_([A-Z]+)_(\d{3}|SLIM)\.set$/) {
                my ($name, $ns, $lvl) = ($1, $2, $3);
                $lvl .= '%' if ($lvl =~ /^\d+$/);
                $setF = sprintf("%s (%s %s)", $name, $ns, $lvl);
            }
        }
        push @row, $setF;
        push @row, $nocgi ? $notes : [$notes, 'note'];
        push @rows, [@row, $tick];
    }
    return $rv if ($#rows == -1);
    $title = sprintf("%d %s%s", $#rows + 1, $title || 'Job', $#rows == 0 ? '' : 's');
    if ($nocgi) {
        $rv .= "$title\n";
    } else {
        $rv .= "<table class='tab'><caption>$title</caption><tbody>\n";
        my @cols = ('Date', 'Summary', 'Results', 'Set', "Notes");
        $rv .= "  <tr>".join('', map {"<th>$_</th>"}@cols)."</tr>\n";
    }
    foreach my $row (@rows) {
        if ($nocgi) {
            my $tick  = pop @{$row};
            my $notes = pop @{$row};
            $rv .= sprintf("  %10s %8s - %-10s : %s\n", @{$row}, $tick);
            $rv .= sprintf("  %10s %8s   %s\n", "","",$notes) if ($notes);
        } else {
            my $tick  = pop @{$row};
            $rv .= "  <tr>".join('', map {
                ref($_) ? "<td class='$_->[1]'>$_->[0]</td>" : "<td>$_</td>";
            } @{$row})."</tr>\n";
        }
    }
    $rv .= "</tbody></table>\n" unless ($nocgi);
    return $rv;
}

sub recent {
    my $rec = "";
    return $rec unless ($ldap);
    my $wanted = $args->val(qw(mostrecent)) || 10;
    my @years  = map { "$tDir/$_" } split(/[\n\r]+/, `ls -1t $tDir`);
    my @found;
    foreach my $ydir (@years) {
        my @mons = map { "$ydir/$_" } split(/[\n\r]+/, `ls -1t $ydir`);
        foreach my $mdir (@mons) {
            my @tiks = split(/[\n\r]+/, `ls -1t $mdir/*.status`);
            foreach my $tick (@tiks) {
                $tick =~ s/\.status//;
                my $htick = BMS::TicketManager->new($tick);
                next unless ($htick);
                my $stat  = $htick->most_recent_status(1) || "";
                next if (!$stat || $stat eq 'TaskFailed');
                push @found, $htick;
                last if ($#found + 1 >= $wanted);
            }
            last if ($#found + 1 >= $wanted);
        }
        last if ($#found + 1 >= $wanted);
    }
    return "" if ($#found == -1);
    my $rv = &ticket_list_summary( \@found, "Most Recent Job" );
    unless ($nocgi) {
        $wanted *= 2;
        $rv .= "<a href='hypergeometric_affy.pl?mostrecent=$wanted'>Show $wanted</a><br />\n";
    }
    return $rv;
}

sub ticket_summary {
    my ($ticket) = @_;
    unless ($ticket) {
        &task_failed("TaskFailed - Can not get ticket status for undefined ticket", "NoTicket");
         return;
    }
    my $txt;
    my $tick = $ticket->ticket();
    my $tp   = &short_ticket_path( $ticket );
    my ($d, $t) = $ticket->date_time();
    my $hfrm = $nocgi ? "  %20s : %s\n" :
        " <tr><th>%s</th>\n  <td%s>%s</td>\n </tr>\n";
    my @rows = ( ['Date', "$d $t", 'time'],
                 ['User', $ticket->param('ticketUser') || 'Unknown' ],);
    my $notes = $ticket->param('Notes') || '';
    if ($nocgi) {
        $txt = "Details for Ticket $tick\n";
        push @rows, ['Notes', $notes] if ($notes);
    } else {
        my $det = &path2url($ticket->path, 'Details', 'mini butt');
        $txt = "<form><input type='hidden' name='ticket' value='$tp' />\n".
            "<table class='tab'><caption class='large'>Ticket Summary $det<br /><span class='tick'>$tick</span></caption><tbody>\n";
        push @rows, ["Notes<br /><input type='submit' class='mini butt' value='Update' />", "<textarea class='note' name='jobNote' cols='20' rows='6'>$notes</textarea>"];
    }
    my %hrows;
    my $res = $ticket->param('results');
    unless ($nocgi) {
        my @acts = ( ["New Analysis", { ticket => $tp, norun => 1} ]);
        push @acts, ["Reformat Results",
                     {ticket => $tp, results => 1, norun => 1} ]
                         if ($res && -e $res);
        foreach my $act (@acts) {
            my ($lab, $params) = @{$act};
            push @{$hrows{Actions}}, &selfUrl( $params, $lab, 'butt' );
        }
    }
    foreach my $hkey (sort keys %hrows) {
        push @rows, [$hkey, join("<br />\n", @{$hrows{$hkey}})];
    }
    push @rows, ['Pre-computed Output'];
    foreach my $fkey (sort keys %{$fmtDat}) {
        my $param = $fmtDat->{$fkey}[1];
        my $path  = $ticket->param($param);
        next unless ($path);
        if (! -e $path) {
            push @rows, [$fkey, 'Absent' ];
        } elsif ($nocgi) {
            push @rows, [ $fkey, $path ];
        } else {
            my $fval;
            my $cat = "Formatted Output";
            my $url = &path2url($path, 'Save', 'butt');
            my $txt = ""; #"<b>$fkey</b>: ";
            if ($fkey eq 'Benchmarks') {
                $txt  = "";
                $fval = &selfUrl({status => $tp, norun => 1});
                $cat  = "Benchmarks";
            } else {
                $fval = &selfUrl({show => $param, ticket => $tp});
            }
            $txt .= $url if ($url =~ /^\<a/);
            $txt .= "<a class='butt' href='$fval'>Show</a>" if ($fval);
            $txt .= "<span class='smalert'>Temporary File!</span>"
                if ($path =~ /tmp/);
            push @rows, [$fkey, $txt];
            # push @{$hrows{$cat}}, $txt;
        }
    }
    foreach my $row (@rows) {
        my ($c1, $c2, $c3, $c4) = 
            map { defined $row->[$_] ? $row->[$_] : "" } (0..3);
        if ($nocgi) {
            $c2 =~ s/\<[^\>]+\>//;
        } else {
            ($c2,$c3) = ($c3, $c2);
            $c2 = $c2 ? " class='$c2'" : "";
        }
        if ($row->[1]) {
            my $hr = sprintf($hfrm, $c1, $c2, $c3);
            $txt  .= $hr;
        } else {
            $txt .= $nocgi ? " $c1:\n" : "<tr><th colspan='2'>$c1</th></tr>\n";
        }
    }
    $txt .= "</tbody></table></form><p />\n" unless ($nocgi);
    print $txt;
}

sub show_results {
    my ($file) = @_;
    my $tick  = $ticket ? $ticket->ticket : '';
    if ($ticket && (!$priorRes || $priorRes eq '1')) {
        $priorRes = $ticket->param('results');
        &msg("Results taken from ticket ".&tick_link($ticket)) unless ($bgnd);
        if ($ofile && $args->is_default('output')) {
            # We are generating new output, but an old output file exists
            # Make sure we do not overwrite
            my $oldOut = $ofile;
            $ofile =~ s/\.[^\.]{2,6}$//;
            if ($format =~ /htm/i) {
                $ofile .= ".html";
            } elsif ($format =~ /(excel|xls)/i) {
                $ofile .= ".xls";
            } elsif ($format =~ /(tsv)/i) {
                $ofile .= ".tsv";
            }
        }
    }
    if ($noRun) {
        # Present dialogs for structuring output choices
        if ($nocgi) {
            &err("Do not use -norun if you wish your results to be formatted");
            return;
        }
        print "<form method='get' action='$shortProg'>\n";
        print "<input type='submit' style='font-size:larger;font-weight:bold;background-color:#9f9;color:#00f' value='Reformat' />";
        if ($ticket) {
            print "<input type='hidden' name='ticket' value='$tick' />\n";
            print "<input type='hidden' name='results' value='1' />\n";
        } else {
            print "<input type='hidden' name='results' value='$priorRes' />\n";
        }
        &RESULTSFORM();
        &RESULTS_EXTRA();
        print "</form>";
        $skip{MainForm} = 1;
    } elsif ($nocgi || $bgnd) {
        # Execute
        &establish_output( $bgnd ? $ticket : undef, $ticket );
        my $parser = BMS::SetCollection::Results->new( );
        my @res    = $parser->from_xml( $priorRes );
        if (0) {
            my %cols;
            map { $cols{ $_->collection } = $_->collection } @res;
            my @allColls = values %cols;
            my $hm = sub { my ($tw, $msg) = @_; return &help($tw, $msg); };
            # my $hm = sub { my ($msg,$tw) = @_; return &help($tw, $msg); };
            map { $_->help_method( $hm );
                  $set = $_->alias('ReferenceSet') } @allColls;
            &show_collection(@allColls);
            if ($#res < 3) {
                my $did = "ResultsLoad";
                print $outfh "<div id='$did' class='DynamicDiv' cantoggle='hide' menutitle='Loaded results summary'>\n";
                map { print $outfh $_->html_summary( $_ ) } @res;
                print $outfh "</div>\n<script>jsmtk_manual_load('$did')</script>\n";
            }
        }
        &show_notes();
        $ticket->status('TaskStart', "Reformat <span class='file'>$priorRes</span> as $format");
        &format_results( -results => \@res,
                         -ticket  => $ticket, );
        $ticket->status('TaskComplete', "Reformatting done");
    } else {
        my @params = ('-temp', '-clean', '-results', '-format', $format, '-pcut', $pcut);
        push @params, "-nonot" if ($noNot);
        push @params, ("-idsforterm", join("\t", split(/\s*(?:\s+\+\s+|[\n\r\t]+)\s*/,$idsForTerm)))
            if ($idsForTerm);
        push @params, ("-exterm", join(",", @{$exTerm})) if ($exTerm);
        $ticket->status('TaskStart', "Reformat <span class='file'>$priorRes</span> as $format");
        &bgnd_launch(\@params, $tick);
    }
}

sub bgnd_launch {
    my ($params, $tick) = @_;
    my $prog   = $0;
    my $uniqID = join('_', time, $$);
    my @cmds   = ($prog, '-nocgi', '-background', $tick, '-uniqid', $uniqID,
                  @{$params || []});
    my @quoted;
    for my $i (0..$#cmds) {
        my $bit = $cmds[$i];
        if ($i && $bit =~ / /) {
            $bit = ($bit =~ /\"/) ? "'$bit'" : "\"$bit\"";
        }
        $quoted[$i] = $bit;
    }
    my $cmd = join(' ', @quoted);
    my $kid;
    $ENV{FORCE_NOCGI} = "Force NOCGI";
    if ($kid = fork) {
        # Parent does not need to do anything
        # &log(join("\t", $tick, "$$ forks $kid", $cmd, &stack_trace));
    } elsif (defined $kid) {
        my $gkid = 0;
        delete $ENV{PERL5LIB};
        if (0) {
            my $redirFail = 0;
            open STDIN, '/dev/null'   || $redirFail++;
            open STDOUT, '>/dev/null' || $redirFail++;
            open STDERR, '>/dev/null' || $redirFail++;
            system("$cmd &"); # 2>&1 >>$dumpfile < /dev/null &");
        } else {
            close STDERR;
            close STDOUT;
            close STDIN;
            exec @cmds;
        }
        exit;
    } else {
        die "Failed to fork off background child process!\n  ";
    }
    print "\n<div id='status'>Starting analysis:<br />\n".
        "<span class='file'>$cmd</span></div>\n";
    print "<script>ajaxRecursiveOnLoad('$shortProg?status=$tick&clean=2',".
        "'status','(Running|Waiting|Pending)',5)</script>\n\n";
    return $cmd;
}

sub stack_trace {
    my @history;
    my $hist = 1;
    while (1) {
        my ($pack, $file, $j4, $subname) = caller($hist);
        last unless ($subname);
        my ($j1, $j2, $line) = caller($hist-1);
        push @history, sprintf("  %50s : %d", $subname, $line);
        $hist++;
    }
    return @history;
}

sub establish_output {
    my ($ticket, $tick2) = @_;
    return if ($outputEstablished++);

    if ($ticket || $isTemp) {
        my ($sfx, $otag) = @{$fmtDat->{$format} || []};
        if ($sfx) {
            $otag  ||= '';
            $sfx   ||= 'out';
            if ($isTemp) {
                $ofile ||= "$tempDir/".
                    join('_', $ldap, 'TEMP', time, $$).'.'. $sfx;
                $ticket->extend( { output => $ofile } ) if ($ticket);
            } else {
                $ofile ||= "$tempDir/".$ticket->ticket.'.'. $sfx;
                $ticket->extend( { output => $ofile,
                                   $otag  => $ofile,});
            }
        }
    }
    if ($ofile && $format ne 'Excel') {
        open( OFILE, ">$ofile") || &err
            ("Failed to send output to '$ofile':\n $!",
             'FileWriteFailure','FATAL');
        $outfh = *OFILE;
        if ($format =~ /html/i && $args->val(qw(FULLHTML))) {
            print $outfh "<html><head>";
            print $outfh $ahg->html_head( $args->val(qw(BETAJS)) );
            print $outfh $ad->namespace_url_styles();
            print $outfh "</head><body>";
        }
    }
    if (my $tickObj = $ticket || $tick2) {
        if ($ontoMem || $format =~ /html/i) {
            my $tick = $tickObj->ticket();
            my $base = $0;
            $base    =~ s/^\/stf//;
            print $outfh "<script>ticketUrl = '$base?ticket=$tick'</script>\n";
            #if ($bgnd || $isTemp) {
            #    my $url  = &selfUrl( { ticket => $tick  } );
            #    print $outfh "<p>$format results for ".&tick_link($ticket)."</p>";
            #}
        }
    }
}

sub PARSEINPUT {
    if ($forceSet) {
        unless ($reqNs) {
            my $cTax  = $set || "";
            my $xtra  = "";
            if ($cTax =~ /^(\S+ \S+)\s*(.*)/) { ($cTax, $xtra) = ($1, lc($2)) }
            my @taxae = $mt->get_taxa( $cTax );
            if ($#taxae == 0) {
                $set = $taxae[0]->name;
                if (!$xtra) {
                } elsif ($xtra =~ /ens/) {
                    $reqNs = 'ENSG';
                    $set .= " Ensembl Gene";
                } elsif ($xtra =~ /trc/) {
                    $reqNs = 'TRC';
                    $set .= " RNAi Consortium Reagent";
                }
                unless ($reqNs) {
                    $reqNs = 'LL';
                    $set  .= " LocusLink";
                }
                $forceSet = $set;
            } elsif ($set =~ /HumanHT/i) {
                $reqNs = 'ILMN';
            } elsif ($set =~ /agilent/i) {
                $reqNs = 'AGIL';
            } else {
                my @seqs = $mt->get_seq( -id => $set, -nocreate => 1,
                                         -defined => 1);
                if ($#seqs == -1) {
                    &err("Unable to find requested set '$set'","UnknownSet");
                    return;
                }
                my $seq = $seqs[0];
                if ($seq->is_class('Affy Array Design')) {
                    $reqNs = 'APS';
                } elsif ($seq->is_class('Illumina BeadArray')) {
                    $reqNs = 'ILMN';
                } else {
                    &err("I found requested set '$set', but I do not know how to figure out the namespace. This probably just means Charles needs to add a rule to set the namespace from this kind of set. In the interim, please explicitly pass the correct AccessDenorm namespace with -reference","UnknownSet");
                    return;
                    
                }
            }
        }
        push @requests, $query;
        return;
    }
    if ($inputCount > 1) {
        my $msg = "You have specified query input from more than one source. Please use a single source for your data, right now you have specified:\n";
        $msg .= "* A list of identifiers pasted into the text box\n"
            if ($query);
        $msg .= "* A file that you chose from your computer\n" if ($fh);
        $msg .= "* A path to a Unix file\n" if ($qfile);
        &err($msg,"MultipleInputSources", 'TICK BR');
        return;
    }

    if ($query) {
        my @check = split(/[$splitter]+/, $query);
        if ($#check == 0 && -e $check[0]) {
            # The user has provided a file in the free text field
            $args->set_val('file', $qfile = $check[0]);
            $args->set_val('query', $query = "");
        }
    }

    my $tfile;
    my @preliminary;
    my @coms = ('BMS Hypergeometric Distribution query list');
    if ($fh) {
        @preliminary = &_READ_LIST_FH( $fh );
        push @coms, "Provide from local file on user's machine";
        $tfile   = &LISTS_TO_FILE( \@preliminary, 'list', \@coms);
    } elsif ($query) {
        # Read list from pasted text box
        push @preliminary, {} unless ($isMatrix);
        my @rows = $isMatrix ? 
            split(/[\n\r]+/, $query) : split(/[$splitter]+/, $query);
        foreach my $row (@rows) {
            my @bits = split(/[$rsplitter]+/, $row);
            if ($isMatrix) {
                $preliminary[-1]{list} = \@bits;
                push @preliminary, {};
            } else {
                next unless ($bits[0]);
                push @{$preliminary[-1]{list}}, $bits[0];
            }
        }
        push @coms, "Provided by direct user text input";
        $tfile   = &LISTS_TO_FILE( \@preliminary, 'list', \@coms);
        $args->set_val('query', "");
    } elsif ($qfile && ($nocgi || $bgnd || $isTemp || $args->val(qw(USEKNN)))) {
        # Read lists from file
        $qfile =~ s/^\s+//; $qfile =~ s/\s+$//;
        $qfile = "$setDir/$qfile" if
            ($qfile !~ /\// && ! -e $qfile && -e "$setDir/$qfile");
        if (-e $qfile) {
            # Establish filehandle, read with next block
            open(QFILE, "<$qfile") || &err
                ("Failed to read '$qfile':\n  $!",
                 'FileOpenFailure', 'FATAL');
            push @preliminary, &_READ_LIST_FH( *QFILE );
            close QFILE;
            my @urlparams = ( show => $qfile );
            push @urlparams, ( ticket => &short_ticket_path( $ticket ) )
                if ($ticket);
            my $url = &selfUrl({@urlparams});
            my $fAge = -M $qfile; my $fUn = "days";
            if ($fAge > 360) {
                $fAge /= 356;
                $fUn = "years";
            }
            my $m = sprintf
                ("<i>Queries read from <a target='_blank' class='file butt' href='%s'>%s</a> (%.1f %s old)</i><br />",
                 $url, $qfile, $fAge, $fUn);
            &msg($m, "NetworkFile" );
        } else {
            &err("I failed to find a file at '$qfile'", 
                 'FileOpenFailure', 'TICK');
        }
    }
    if ($args->val(qw(USEKNN))) {
        if ($#preliminary != 0) {
            &err("Please provide only a single list of IDs to recover XPRESS KNN ranked lists", "XpressRankedLists", 'TICK');
            return;
        }
        my $xReq  = { name => 'XPRESS KNN ranked list request' };
        my $qlist = $xReq->{list} = $preliminary[0]{list};
        my ($ns, $nsErr) = &NAMESPACE_FOR_LIST
            ($qlist, $refer, undef, $xReq->{name});
        if ($nsErr) {
            if ($isTemp) {
                &err(@{$nsErr});
            } else {
                my $twXtra = $nsErr->[1] ? " ".$nsErr->[1] : "";
                &task_failed( $nsErr->[0], "TaskFailed".$twXtra);
            }
            return;
        }

        $xReq->{param}{NAMESPACE} = $ns;
        my $desired = $ad->namespace_name('APS');
        if ($ns ne $desired) {
            unless ($set) {
                &err("You are requesting XPRESS KNN ranked lists using non-Affy IDs. You need to define the Reference Set (Advanced List Options) to be the appropriate affy array design in order to gather the appropriate probe sets.", "XpressRankedLists", 'TICK');
                return;
            }
            &TRANSFORM_LIST($xReq, $desired, $transMat, $transMap, $set,'SET');
            my @coms =
                ("Mapping of $ns identifiers to Affy Probe Sets",
                 'Used to recover KNN lists from XPRESS',
                 "Note that rank is irrelevant in this file");
            my $mfile = $args->set_val('knnmap', &LISTS_TO_FILE
                                       ( [$xReq], 'knnmap', \@coms));
            my $url = &path2url($mfile,'temporary file');
            &msg("Your list has been mapped to Affy IDs for KNN recovery - a $url contains the mappings", "TemporaryFile");
        }
        my $probeList = $xReq->{list};
        &DETERMINE_SET( $xReq );
        return unless ($set);
        my $ua     = LWP::UserAgent->new();
        my $design = $set;
        $design    =~ s/HG_/HG-/;
        my %needed = map { uc($_) => $_ } @{$probeList};
        my @unique = sort values %needed;
        my $url    = "http://xpress.pri.bms.com/CGI/knn.cgi?".
            "database=AFFY&webservice=1&outputfile=1&between=1&".
            "design=$design&query=" . join('+', @unique);
        @preliminary = ();
        my $response = $ua->get($url, cookie => $ENV{HTTP_COOKIE});
        if ($response->is_success) {
            my @rows = split(/[\n\r]+/, $response->content);
            if ($rows[0] =~ /Software error/) {
                my $err = "";
                foreach my $row (@rows) {
                    if ($row =~ /pre\>.+/) {
                        last if ($err);
                        $err = $1;
                    } else {
                        $err .= $row if ($err);
                    }
                }
                &err("Request to XPRESS for KNN data returns an error:\n$err", 'FailedKNN','TICK');
            } else {
                foreach my $row (@rows) {
                    if ($row =~ /^\#\s+(\S+)\=(.+)/) {
                        my ($param, $val) = (uc($1), $2);
                        if ($param eq 'GENE') {
                            # Starting a new list
                            my $desc = $ad->description
                                ( -id => $val, -ns => 'APS') || '';
                            push @preliminary, {
                                param => {
                                    NAMESPACE  => 'Affy Probe Set',
                                    SET        => $set,
                                    MAX_ZSCORE => $zmax,
                                },
                                name => "KNN Ranked list for $val - $desc",
                            };
                            delete $needed{uc($val)};
                        }
                        $preliminary[-1]{param}{$param} = $val;
                        next;
                    }
                    my ($ps, $dist, $zscore) = split("\t", $row);
                    next unless ($ps && defined $zscore && $zscore ne '');
                    if ($ps =~ /^NN/) {

                    } else {
                        next if (defined $zmax && $zscore > $zmax);
                        push @{$preliminary[-1]{list}}, $ps;
                        $preliminary[-1]{meta}{'Z-Score'}{$ps} = $zscore;
                    }
                }
            }
        } else {
            die "Failed to recover KNN lists from XPRESS:\n".
                $response->status_line;
        }
        my @missing = values %needed;
        unless ($#missing == -1) {
            &err("Failed to find KNN lists for some probesets: ".
                 join(", ", sort @missing), 'MissingKNN', 'TICK');
            if ($#preliminary == -1) {
                &err("XPRESS refused to return any list data!\n".
                     "The URL used was:\n$url", 'FailedKNN', 'TICK');
                return;
            }
        }
        $qfile    = '';
        $isMatrix = 0;
        $mode     = $args->set_val('mode', 'GSEA');
        push @coms, "Lists provided by KNN - KNN seeds provided by user";
        $tfile    = &LISTS_TO_FILE( \@preliminary, 'list', \@coms);
        $args->set_val('useknn', 0);
    }

    if ($tfile) {
        my $url = &path2url($tfile,'temporary file');
        &msg("Your list has been written to a $url to make re-analysis easier", "TemporaryFile");
        $args->set_val('file', $qfile = $tfile);
    }

    unless ($trueBgnd || $isTemp) {
        unless ($ticket) {
            $ticket = BMS::TicketManager->new();
            $ticket->path($args->val(qw(ticketname)) || 'new');
        }
        if ($sffh && ref($sffh)) {
            # We need to write user uploads to a temp file
            my $setFile = sprintf("/stf/biohtml/tmp/%d-%s", time, $sffh);
            $setFile =~ s/\s+/_/g;
            open(TOUT, ">$setFile") || &err
                ("Failed to write to '$setFile'\n  $!",
                 'FileWriteFailure','FATAL');
            while (<$sffh>) {
                print TOUT $_;
            }
            close TOUT;
            map { $args->clear_val($_) } qw(SET_PC_FH SET_PC);
            $args->set_val('setfile', $setFile);
        }
        $args->set_val('file', $qfile);
        my @igList =
            qw(CLEAN RERUN STATUS TEMP BACKGROUND NOCGI REBUILD ISDEFAULT
               SHOW_CVS_VERS TICKET USEKNN DEBUG QUERY_PC QUERY_PC_FH JOBNOTE
               BENCHMARK SHOWBENCH pFiles blockQuote notPassed forceticket
               BGOVERRIDE
               PARAMFILE VALUEFILE argumentCase defaultValues isDefault);

        my %omit = map { uc($_) => 1 } @igList;
        foreach my $key ($args->all_keys()) {
            next if ($omit{uc($key)});
            $ticket->param(uc($key), $args->val($key));
        }
        $ticket->param('Notes', $args->val(qw(JOBNOTE)));
        $ticket->param('queryFile', $args->val(qw(FILE)));
        $ticket->write();
    }

    if ($bgnd || $isTemp) {
        # The current process should perform the actual execution
    } elsif (!$nocgi) {
        # Web requests lauch the processing in the background
        my $cmd = &bgnd_launch(["-format", $format], $ticket->ticket);
        $ticket->extend( { cmd => $cmd } );
        $inputCount = 0;
        return;
    }
    my @nonNull;
    foreach my $req (@preliminary) {
        my $list = $req->{list};
        next unless ($list && $#{$list} != -1);
        push @nonNull, $req;
    }
    if ($#nonNull == -1) {
        my $msg = "No lists were found in your request!";
        if ($isTemp) {
            &err($msg, "NoQueryLists");
        } else {
            my $isCommon = ($ldap =~ /^jacksod|tilfordc$/) ? 1 : 0;
            &task_failed($msg, undef, $isCommon);
        }
        return;
    }
    my $lcount  = 0;
    my $toShift = $isMatrix ? $headNum : 0;
    my $smsg    = sprintf("%d requested list%s being prepared", $#nonNull+1,
                          $#nonNull == 0 ? '' : 's');
    $ticket->status('TaskStart', $smsg) if ($ticket && !$isTemp);
    for my $l (0..$#nonNull) {
        my $req = $nonNull[$l];
        my $lnum = $req->{param}{LISTNUMBER} = $l + 1;
        next if ($listReq && $listReq != $lnum);

        my $list = $req->{list};
        $req->{headers} = [];
        for my $h (1..$toShift) {
            push @{$req->{headers}}, shift @{$list};
        }
        my $name = $req->{name} ||= join(" || ", @{$req->{headers}}) ||
            "Query List " . ++$lcount;
        $req->{name} =~ s/[\|\s]+$//;
        my $ns = $ad->namespace_name
            ($req->{param}{NS} || $req->{param}{NAMESPACE} || $refer);
        my $nsErr;
        ($ns, $nsErr) = $refer =~ /cust/i ? ('Custom') :
            &NAMESPACE_FOR_LIST($list, $ns, undef, $name);
        if ($nsErr && !$args->val(qw(IGNORENS))) {
            if ($isTemp) {
                &err(@{$nsErr});
            } else {
                $ticket->status($nsErr->[1] || 'UnknownNameSpace',$nsErr->[0])
                    if ($ticket);
            }
            next;
        }

        $req->{param}{NAMESPACE} = $ns;
        delete $req->{param}{NS};
        if ($transf && $transf ne $ns) {
            # We need to transform the query list to another namespace
            &TRANSFORM_LIST
                ( $req, $transf, $transMat, $transMap,
                  $args->val(qw(int)), $args->val(qw(intns)));
        }
        push @requests, $req;
    }
    my %seenNS = map { $_->{param}{NAMESPACE} => 1 } @requests;
    my @allNs  = keys %seenNS;
    if ($#allNs > 0) {
        &err("Your lists are using multiple analysis namespaces - ".
             "I found: ".join(", ", @allNs).
             ". Please chose a single analysis namespace and try again",
             "MultipleAnalysisNamespaces");
        &task_failed("Mixed namespaces") if ($ticket && !$isTemp);
        @requests = ();
        return;
    } else {
        $reqNs = $allNs[0];
    }
}

sub _READ_LIST {
    my ($req) = @_;
    my @lists;
    if (!$req) {
        $args->err("Could not read list file as no filename was passed!");
    } elsif ($req !~ /[\n\r]/ && -e $req) {
        open(CFILE, "<$req") || &err
            ("Failed to read list file '$req':\n  $!",
             'FileOpenFailure', 'FATAL');
        @lists = &_READ_LIST_FH(*CFILE);
        close CFILE;
    } else {
        @lists = ( { list => [ split(/\s*[$splitter]+\s*/, $req) ]} );
    }
    return @lists;
}

sub _READ_LIST_FH {
    $ahg->benchstart("Read List File");
    my ($fh) = @_;
    my @lists = ({});
    my $lastCom;
    my %ignoreMeta = map { $_ => 1 } ('Query ID', 'Query Rank');
    my $listSplit = '\s*\t\s*';
    while(<$fh>) {
        s/[\n\r]+$//;
        # Skip comment lines
        if (/^\#\s*(.+?)\s*$/) {
            $lastCom = $1;
            if ($lastCom =~ /^(\S+)\=(.+)/) {
                # Parameter deffinition
                $lists[-1]{param}{uc($1)} = $2;
            } elsif ($lastCom =~ /^LIST\s*$/ ||
                     $lastCom =~ /^LIST\s+-\s+(.+)$/) {
                push @lists, { name => $1 };
            } elsif ($lastCom =~ /^Query ID/) {
                # Specifying column headers
                my @meta_cols = split(/\s*\t\s*/, $lastCom);
                my @lu;
                for my $i (0..$#meta_cols) {
                    my $mc = $meta_cols[$i];
                    $lu[$i] = $mc unless ($ignoreMeta{$mc});
                }
                $lists[-1]{meta_pos} = \@lu if ($#lu != -1);
            }
            next;
        }
        my @bits = split(/$listSplit/);
        my $list = $lists[-1];
        if ($isMatrix) {
            $list->{list} = \@bits;
            push @lists, {};
        } else {
            my $id = shift @bits;
            next unless ($id);
            # Auto populate Rank
            my $num = $list->{meta}{Rank}{$id} = ++$list->{totalSize};
            if (my $lu = $list->{meta_pos}) {
                for my $i (0..$#bits) {
                    if (my $mkey = $lu->[$i + 1]) {
                        $list->{meta}{$mkey}{$id} = $bits[$i];
                    }
                }
            }
            if ($#inputFilter != -1) {
                my $reason;
                foreach my $ifd (@inputFilter) {
                    unless (&_apply_test($id, $list, $ifd)) {
                        $reason = $ifd->{txt};
                        last;
                    }
                }
                if ($reason) {
                    push @{$list->{filtered}}, $id;
                    $list->{meta}{Filtered}{$id} = $reason;
                    next;
                }
            }
            push @{$list->{list}}, $id;
        }
    }
    # Deal with quoted query IDs
    for my $ln (0..$#lists) {
        my $list = $lists[$ln];
        for my $l (0..$#{$list->{list} || []}) {
            if (my $id = $list->{list}[$l]) {
                if ($id =~ /^\s*\"(.+)\"\s*$/ ||
                    $id =~ /^\s*\'(.+)\'\s*$/) {
                    $list->{list}[$l] = $1;
                }
            }
        }
    }
    if ($sortKeyDown || $sortKeyUp) {
        my ($mkey, $dir) = $sortKeyDown ?
            ($sortKeyDown, 'Descending') : ($sortKeyUp, 'Ascending');
        my $keepNon = $args->val(qw(keepnon));
        for my $ln (0..$#lists) {
            my $list = $lists[$ln];
            my $meta = $list->{meta}{$mkey};
            unless ($meta) {
                next;
            }
            my (@sorter, @nonNum);
            foreach my $id (@{$list->{list}}) {
                my $val = $meta->{$id};
                unless (defined $val) {
                    push @nonNum, $id;
                    next;
                }
                $val =~ s/\s+//g;
                if ($val =~ /[^0-9\-\.Ee]/) {
                    push @nonNum, $id;
                    next;
                }
                $val += 0;
                push @sorter, [ $val, $id ];
            }
            my @ids;
            if ($dir eq 'Descending') {
                @ids = map { $_->[1] } sort { $b->[0] <=> $a->[0] } @sorter;
            } else {
                @ids = map { $_->[1] } sort { $a->[0] <=> $b->[0] } @sorter;
            }
            if ($keepNon) {
                push @ids, @nonNum;
            } else {
                push @{$list->{filtered}}, @nonNum;
                my $fr = "$mkey is non-numeric";
                map { $list->{meta}{Filtered}{$_} = $fr } @nonNum;
            }
            $list->{list} = \@ids;
            $list->{name} ||= sprintf("List %d", $ln + 1);
            $list->{name} .= " ($dir Sort on $mkey)";
        }
        
    }
    $ahg->benchend("Read List File");
    return @lists;
}

sub _apply_test {
    my ($id, $list, $ifd) = @_;
    my $meta = $list->{meta};
    return 0 unless ($meta);
    # The "left" value
    my $l = $ifd->{l};
    if (exists $meta->{$l} && $meta->{$l}) {
        # The left value should be taken from metadata
        $l = $meta->{$l}{$id};
    }    
    my $r = $ifd->{r};
    if (exists $meta->{$r} && $meta->{$r}) {
        # The left value should be taken from metadata
        $r = $meta->{$r}{$id};
    }    
    return &_test_value($l, $r, $ifd->{op});
}

sub _test_value {
    my ($l, $r, $op) = @_;
    unless ($op) {
        $args->err("Can not make test for '$l vs. $r' without operator");
        return undef;
    }
    # (False, True);
    my @rvs = (0, 1);
    if ($op =~ /^\!(.+)$/) {
        # 'not' test
        $op = $1;
        @rvs = (1, 0);
    }
    if ($op eq 'defined') {
        return defined $l ? $rvs[1] : $rvs[0];
    }
    return 0 unless (defined $l && defined $r);
    my $ri;
    if ($op eq '>') {
        $ri = $l > $r ? 1 : 0;
    } elsif ($op eq '<') {
        $ri = $l < $r ? 1 : 0;
    } elsif ($op eq '=' || $op eq '==') {
        $ri = $l == $r ? 1 : 0;
    } elsif ($op eq '>=' || $op eq '=>') {
        $ri = $l >= $r ? 1 : 0;
    } elsif ($op eq '<=' || $op eq '=<') {
        $ri = $l <= $r ? 1 : 0;
    } else {
        $args->err("No logic for running test '$l $op $r'");
        return undef;
    }
    return $rvs[$ri];
}

sub LISTS_TO_FILE {
    my ($lists, $ftok, $coms) = @_;
    my $file = "$tempDir/".
        ($ticket ? $ticket->ticket : "gsea_list_file_$$").'.'.$ftok;
    open(TMP, ">$file") || &err
        ("Failed to write file '$qfile'\n  $!",'FileWriteFailure','FATAL');
    $coms ||= [];
    my $lnum = $#{$lists} + 1;
    push @{$coms}, "This file contains a total of $lnum lists" if ($lnum > 1);
    push @{$coms}, `date`;
    map { print TMP "# $_\n" } @{$coms};

    my %unknown;
    for my $i (1..$lnum) {
        my $req   = $lists->[$i-1];
        my $name  = $req->{name} || "List $i";
        my $list  = $req->{list};
        my $ranks = $req->{rank} || {};
        my $src   = $req->{source};
        my @miss  = sort values %{$req->{orph} || {}};
        my $ns    = $req->{param}{NAMESPACE};
        print TMP "# LIST - $name\n";
        foreach my $param (sort keys %{$req->{param} || {}}) {
            my $val = $req->{param}{$param};
            print TMP "#   $param=$val\n" if (defined $val);
        }
        if ($req->{headers} && $#{$req->{headers}} != -1) {
            print TMP "#   HEADERS=". join
                ("\t", map { defined $_ ? $_ : '' } @{$req->{headers}})."\n";
        }
        map { print TMP "# $_\n" } @{$req->{com} || []};

        if ($#miss != -1) {
            print TMP "# Source terms that can not be assigned to $ns:\n";
            print TMP "#   ORPHANS=".join(", ", @miss)."\n";
        }
        my @header = ("Query ID", "Query Rank");
        push @header, ("Source ID", "Source Rank") if ($src);
        my $meta = $req->{meta};
        my @mkeys;
        if ($meta) {
            @mkeys = sort keys %{$meta};
            if ($#mkeys == -1) {
                $meta = undef;
            } else {
                push @header, @mkeys;
            }
        }
        my @unknown;
        print TMP "\n# " .join("\t", @header) ."\n";
        for my $l (0..$#{$list}) {
            my $req = $list->[$l];
            next unless ($req);
            my ($id) = $ad->standardize_id($req);
            unless ($id) {
                push @{$unknown{$req}}, $name;
                next;
            }
            my @row = ($id, $ranks->{$id} || $l + 1);
            push @row, @{$src->{$id} || []};
            if ($meta) {
                foreach my $mkey (@mkeys) {
                    my $val = $meta->{$mkey}{$id};
                    push @row, defined $val ? $val : '';
                }
            }
            print TMP join("\t", @row)."\n";
        }
        print TMP "\n" unless ($i == $lnum);
    }
    close TMP;
    chmod(0666, $file);
    my @unks = sort keys %unknown;
    unless ($#unks == -1) {
        $args->msg("Some IDs were not recognized and were removed from your list(s)", map {
            sprintf("%s in list%s %s", $_, $#{$unknown{$_}} == 0 ? '' : 's',
                    join(", ", @{$unknown{$_}})) } @unks);
    }
    return $file;
}

sub TRANSFORM_LIST {
    my ($req, $ns2, $min, $how, $int, $intns, $metaOnly) = @_;
    my $list  = $req->{list};
    my $mdat  = $req->{meta} ||= {};
    my $ns1   = $req->{param}{NAMESPACE};
    unless ($ns1) {
        my $err;
        ($ns1, $err) = &NAMESPACE_FOR_LIST( $list );
    }
    my $rows  = $ad->convert( -id  => $list, -ns1 => $ns1,
                              -ns2 => $ns2,  -min => $min,
                              -intersect => $int,  -intns => $intns );

    my %hits;
    for my $r (0..$#{$rows}) {
        my ($id1, $id2) = ($rows->[$r][5], $rows->[$r][0]);
        next unless ($id2);
        $hits{$id2}{uc($id1)} = $id1;
    }
    my (%rankLU, %orphan);
    map { my $id = uc($list->[$_]);
          $rankLU{$id}  ||= $_ + 1;
          $orphan{uc($id)}  = $id; } (0..$#{$list});
    $how ||= 'Best';
    my $doBest = ($how ne 'Best') ? 0 : 1;
    my $joiner = ', ';
    my @mtags = keys %{$mdat};

    my $tDesc = "From $ns1 to $ns2, resolution mode '$how'";
    $tDesc .= sprintf(", minimum score %d%%", $min) if ($min);
    if ($int) {
        $tDesc .= ", belonging to $int";
        $tDesc .= " [$intns]" if ($intns);
    }
    $req->{TransDesc} = $tDesc;
    my %via;
    while (my ($id2, $id1s) = each %hits) {
        map { $via{$_}{$id2} = 1 } values %{$id1s};
    }
    while (my ($id1, $id2H) = each %via) {
        $mdat->{TransformedTo}{$id1} = join($joiner, sort keys %{$id2H});
    }
    return if ($metaOnly);

    my @sorter;
    while (my ($id2, $id1s) = each %hits) {
        my @orig = sort {$rankLU{uc($a)} <=> $rankLU{uc($b)}} values %{$id1s};
        $mdat->{RawTargets}{$id2} = join($joiner, @orig);
        $mdat->{RawRanks}{$id2}   = join($joiner, map { $rankLU{uc($_)} } @orig);
        foreach my $mtag (@mtags) {
            my $mmdat = $mdat->{$mtag};
            my @vals = map {defined $_ ? $_ : '?'} map { $mmdat->{$_} } @orig;
            my %u = map { $_ => 1 } @vals;
            my @us = keys %u;
            @vals = @us if ($#us == 0);
            $mmdat->{$id2} = join($joiner, @vals);
        }
        map { delete $orphan{uc($_)} } @orig;
        my @ranks = map { $rankLU{uc($_)} } @orig;
        my $calcRank;
        if ($doBest) {
            ($calcRank) = sort { $a <=> $b } @ranks;
        } else {
            $calcRank = 0;
            map { $calcRank += $_ } @ranks;
            $calcRank /= ($#ranks + 1);
        }
        push @sorter, [$id2, $calcRank];
        $req->{rawCount}{$id2} = $#orig + 1;
        $req->{source}{$id2} = [ join(", ", @orig), join(", ", @ranks)];
    }
    $req->{param}{NAMESPACE} = $ns2;
    $req->{param}{SOURCE_NS} = $ns1;
    $req->{param}{MIN_MATCH} = $min;
    $req->{param}{INTERSECT} = $int;
    $req->{param}{TRANSMODE} = $how;
    $req->{param}{INTNS}     = $intns;
    $req->{orig}             = $list;
    $req->{orph}             = \%orphan;
    if ($#sorter == -1) {
        $req->{list}          = [];
        $req->{nullTransform} = 1;
        &msg("Transformation of list ".$req->{name}." from ".($#{$list}+1).
             " $ns1 entries yields zero $ns2 mappings", "EmptyQueryList");
        return;
    }
    @sorter = sort { $a->[1] <=> $b->[1] ||
                     $a->[0] cmp $b->[0] } @sorter;
    my @transList = map { $_->[0] } @sorter;
    my %ranks;
    $ranks{$sorter[0][0]} = $sorter[0][2] = 1;
    for my $i (1..$#sorter) {
        my $id2 = $sorter[$i][0];
        $ranks{$id2} = $sorter[$i][2] = $mdat->{Rank}{$id2} =
            ($sorter[$i][1] == $sorter[$i-1][1]) ? $sorter[$i-1][2] : $i + 1;
    }
    
    $req->{list}  = \@transList;
    $req->{rank}  = \%ranks;
}

sub NAMESPACE_FOR_LIST {
    $ad->benchstart('Guess Namespace');
    my ($list, $ns, $dbNum, $lname) = @_;
    return $ns if ($ns =~ /cust/i);
    my @samp;
    if ($#{$list} < 100) {
        @samp = @{$list};
    } else {
        # Take a random sample
        my @rand = sort { rand(1) <=> rand(1) } @{$list};
        @samp = splice(@rand, 0, 100);
    }
    my @guesses = map { $ad->namespace_name($_) } $ad->most_likely_namespace
        ( \@samp, $dbNum );
    while ($#guesses != -1 && $guesses[0] =~ /^Any /) {shift @guesses}
    my $guess = shift @guesses;
    my $err;
    $lname ||= 'list';
    if ($ns) {
        if ($guess && $ns ne $guess) {
            if ($ns eq 'BrainArray Probe Set' && $guess eq 'Affy Probe Set') {
                # The user is providing 'Native' BrainArray LocusLink IDs
                # which look like Affy IDs
            } else {
                $err = [ "Defined namespace $ns does not match predicted ".
                         "namespace $guess for $lname",'NameSpaceMismatch'];
            }
        }
        unshift @guesses, $guess;
        push @failedListGuess, [$lname, join('/', @guesses).
                                " does not match request of $ns", \@samp];
    } elsif ($guess) {
        $ns = $guess;
    } else {
        $err = ["Failed to guess namespace for $lname",
                'NameSpaceMismatch'];
        push @failedListGuess, [$lname, '?NS?', \@samp];
    }
    $ad->benchend('Guess Namespace');
    return ($ns, $err);
}

sub SIMPLIFY {
    my ($in, $out) = @_;
    if (-e $in) {
        $ahg->simplify_tsv( %{$args},
                            -file    => $in,
                            -input   => $in,
                            -output  => $out,
                            -pcutoff => $pcut );
    } else {
        &msg("I could not find '$in' to simplify", "FileOpenFailure");
    }
}

sub HTMLMENU {
    return if ($nocgi || $clean);
    print "<table ><tbody><tr style='vertical-align:top'>\n";
    print "<td><span class='larger' style='color:#390'>GSEA</span>&nbsp;<span class='mini'>$progVers</span></td>";
    print "<td >".&help('BetaSoftware', 'BETA', 'butt alert')."</td>" if ($isbeta);

    print "<td class='menu' id='histRoot'><a class='butt' href='".&selfUrl
        ({history => '/',clean => 1}).
        "' onclick='return cuteGet(this,\"histRoot\")'>My History</a></td>\n"
        unless ($args->val(qw(HISTORY)));

    # print "<td class='menu'>".&niceUrl(&selfUrl({prefs => 1}), 'My Preferences', 'butt', 1)."</td>\n" if ($ldap);

    print "<td class='menu' id='helpRoot'><a class='butt' href='".&selfUrl
        ({help => 1,clean => 1}).
            "' onclick='return cuteGet(this,\"helpRoot\")'>Help</a></td>\n";

    print "<td class='menu' id='refRoot'><a class='butt' href='".&selfUrl
        ( { showref => 'hide', clean => 1 }).
        "' onclick='return cuteGet(this,\"refRoot\")' >Stats Reference</a></td>\n";
    print "<td class='menu'>".&selfUrl(undef, 'New Analysis', 'butt').
        "</td>\n" if ($ticket);
    print "</tr></tbody></table>\n";
}

sub helpHTML {
    my %twls =
        (ov => &help('SoftwareOverview', "Program overview", 'butt'),
         fm => &help('OutputFormat', "Display formats", 'butt'),
         ts => &help('TicketSystem', "Using Tickets", 'butt'),);

    return <<EOF;
<div class='help'>
<span class='query'>Help Overview</span>

<p>GSEA uses an on-line help system. Many subjects are already linked on-screen by <span class='twhelp'>[?]</span> hyperlinks. In addition, you may find these broad topics helpful:</p>

<ul>
<li>$twls{ov} - General overview of the system</li>
<li>$twls{fm} - Description of the available output formats</li>
<li>$twls{ts} - The benefits of using tickets for tracking your searches</li>
</ul>

You can also <a href='mailto:Charles.Tilford\@bms.com'>e-mail me</a> or call (HPW x3213) with specific questions.

<p class='mini'>Please note that some help topics may be empty. This
is because researchers <u>rarely</u> use help documentation, which
takes a <b>lot</b> of work to write. However, if you request an empty
help topic, I will be notified automatically by email that there is an
interest in that subject. I put a <b>very</b> high priority on providing
user documenatation, and am happy to provide it. Please do not be
discouraged if you encounter such a message - you actually get brownie
points for taking time to read the documentation!</p> </div>

EOF

}

sub path2url {
    my ($path, $name, $class, $noTarg) = @_;
    $name ||= $path;
    my $url;
    if ($path =~ /\/stf\/((biohtml|biocgi)\/.+)/) {
        $url   = "http://bioinformatics.bms.com/$1";
    } elsif ($path =~ /\/home\/([^\/]+)\/public_html\/(\S+)$/) {
        $url   = "http://bioinformatics.bms.com/~$1/$2";
    } elsif ($path =~ /\/home\/tilfordc\/people\/(\S+)$/) {
        $url   = "http://bioinformatics.bms.com/~tilfordc/people/$1";
    } else {
        $class = $class ? "$class file" : 'file';
        return "<span class='$class'>$name</span>";
    }
    return $url ? $url : "" if ($name eq 'N/A');
    my $rv = "<a href='$url'";
    $rv   .= " class='$class'" if ($class);
    $rv   .= " target='_blank'" unless ($noTarg);
    $rv   .= ">$name</a>";
    return $rv;
}

sub niceUrl {
    my ($path, $name, $class, $noTarg) = @_;
    my $url;
    if ($path =~ /^(http|hypergeometric_affy\.pl)/) {
        $url = $path;
    } elsif ($path =~ /\/stf\/((biohtml|biocgi)\/.+)/) {
        $url = "http://bioinformatics.bms.com/$1";
    } else {
        $url = &path2url($path, $name, $class, $noTarg);
        return $url || $path;
    }
    unless ($name) {
        $name = $path;
        $name =~ s/\?.+//;
        if ($name =~ /\/([^\/]+)$/) { $name = $1 }
    }
    my $html = sprintf
        ("<a href='%s'%s%s>%s</a>", $url, $class ? " class='$class'" : "",
         $noTarg ? '' : " target='_blank'", $name);
    return $html;
}

sub selfUrl {
    my ($params, $name, $class) = @_;
    my $url = "hypergeometric_affy.pl";
    if ($params) {
        my @bits;
        while (my ($tag, $val) = each %{$params}) {
            push @bits, "$tag=$val";
        }
        $url = "$url?".join('&', @bits) if ($#bits > -1);
    }
    if ($name) {
        $class = $class? " class='$class'" : "";
        return "<a$class href='$url'>$name</a>";
    }
    return $url;
}

sub RESULTSFORM {
    return if ($nocgi || $clean);
    print &help('OutputFormat'). "Output format: <select name='format'>\n";
    foreach my $f ('HTML', 'Excel', 'TSV', 'HTML Summary', 'TSV Summary') {
        printf("  <option value='%s'%s>%s</option>\n", $f, 
               (lc($f) eq lc($format)) ? " SELECTED" : "", $f);
    }
    print "</select><br />\n";
    my $did = "NoIE";
    print "<div id='$did' class='DynamicDiv' cantoggle='hide' ".
        "menutitle='Please use Firefox!'>\n";
    print "<img src='/biohtml/images/IE-BadTime2.png' /><br />\n";
    print "<a class='butt' href='http://www.mozilla.org/en-US/firefox/new/'>Get FireFox</a> <span class='noteit'>(BMS allows, but does not support)</span>\n";
    print "</div><script>jsmtk_manual_load('$did')</script>\n";
    
}

sub pModeForm {
    return "" if ($nocgi || $clean);
    my $rv = &help('StatsMode'). "Statistical Method: <select name='pmode'>\n";
#    foreach my $f ('HyperQuick', 'Slow p-Value', 'LOD') {
    foreach my $f ('Slow p-Value', 'LOD') {
        $rv .= sprintf("  <option value='%s'%s>%s</option>\n", $f, 
                       (lc($f) eq lc($pMode)) ? " SELECTED" : "", $f);
    }
    $rv .= "</select><br />\n";
    return $rv;
}

sub RESULTS_EXTRA {
    return if ($nocgi || $clean);
    print &help('IgnoreUnderRepresentation').
        "<input type='checkbox' name='noNot' value='1' ".
        ($noNot ? "checked='checked' " : "") ."/>".
        " Disregard significant under representation<br />\n";
    print &help('pCutOff'). 
        "Discard p-values worse than ".
        "<input type='text' size='5' name='pcutoff' value='$pcut' /><br />\n";
    my $ex = $exTerm ? join(' ', @{$exTerm}) : '';
    print &help('ExcludeTerm'). 
        "Exclude ontology terms from output:<br />\n".
        "<input type='text' size='40' name='excludeterm' value='$ex' /><br />\n";
    print &help('IdsForTerm'). 
        "List all queries for the following terms:<br />\n";

    print "<textarea id='idsforterm' name='idsforterm' rows='10' cols='30'>";
    print "</textarea>\n";
    
}

sub MAINFORM {
    return if ($nocgi || $clean);
    my $did;
    
    print "<form method='post' action='$shortProg' enctype='multipart/form-data'>\n";
    print "<table class='tab'><tbody><tr><th class=''>";

    print &help('QueryListInput')."Define your query list";
    print "</th><th class=''>";
    print &help('DefineAnalysis')."Describe your analysis";
    print "</th></tr><tr><td>\n";

    print "<center>Choose <em>ONE</em> of three ways:</center><ol>\n";
    print "<li>".&help('LiteralList').
        "<b>Paste in a list of identifiers:</b><br />\n";
    print "<textarea id='queryta' name='query' rows='10' cols='30'>";
    print $args->val(qw(QUERY));
    print "</textarea></li>";

    print "<li>". &help('LocalFile').
        "<b>Load a list file from your PC:</b><br />\n".
        "<input type='file' name='query_pc' /></li>";
    print "<li>". &help('NetworkFile').
        "<b>Specify the Unix path of a file on a server:</b><br />\n".
        "<input type='text' size='30' name='query_path' value='$qfile' /></li>\n";
    print "</ol>";
    if ($setHTML) {
        print "<div class='alert'>";
        print $setHTML;
        print "$button</div>\n";
    }

    print &help('JobNotes').
        "<b>Optional notes for this analysis:</b><br />\n".
        "<input type='text' size='30' name='jobNote' value='' /><br />\n";
    
    print &help('SortList').
        "<b>Meta sort descending:</b> ".
        "<input type='text' size='9' name='sortdesc' value='$sortKeyDown' /><br />\n";
    print &help('SortList').
        "<b>Meta sort ascending:</b> ".
        "<input type='text' size='9' name='sortasc' value='$sortKeyUp' /><br />\n";
    

    print "</td><td>";
    print "<div style='width: 30em'>\n";
    print &help('AnalysisMode')."<b>Calculation Mode</b><ol>\n";
    foreach my $odat (@iteropts) {
        my ($val, $name, $desc) = @{$odat};
        my $mod = ($val eq $mode) ? " checked='checked'" : "";
        print "<li><input type='radio' name='mode' title='$desc' value='$val'$mod />".
            "<b>$name</b><br /><span class='noteit'>$desc</span></li>\n";
    }
    print "</ol>\n";

    print &help('SelfReferential'). 
        "<input type='checkbox' name='selfref' value='1' ".
        ($selfRef ? "checked='checked' " : "") ."/>".
        " Use the query list as the reference set<br />\n";
    print "<span class='noteit'>Use if your query list is ranked <em>and</em> represents the full population you want to analyze. Allows only a subset of a reference set to be analyzed.</span><br />";
    print "</div>\n";

    my $chng = "onchange=\"document.getElementById('%s').checked = 'checked'\"";
    my $chId = ""; my $chTxt = "";

    my $dsrid = "DsetRecovery";
    print "<div id='$dsrid' class='DynamicDiv' cantoggle='hide' ".
        "menutitle='Advanced: Dataset Recovery'>\n";
    print "<p class='smalert'>If (Confused) Then { Leave these alone! }</p>\n";
    print "<ul>\n";
    print &help('DataSet')."<b>Dataset recovery</b><br /><ol>\n";
    $chId  = "dmStandard";
    $chTxt = sprintf($chng, $chId);
    print "<li><input id='$chId' type='radio' name='datamode' value='standard' ".
        ($dataMode eq 'standard' ? "checked='checked' " : "") ."/>".
        &help('StandardDataSet'). "Standard Dataset</li><ul>\n";
    foreach my $sdsm (qw(Rapid Standard Exhaustive Insane)) {
        print "<li><input $chTxt type='radio' name='standard' value='$sdsm' ".
        ($standSz eq $sdsm ? "checked='checked' " : "") ."/>".
        " $sdsm</li>\n";
    }
    print "</ul>\n";

    $chId  = "dmFile";
    $chTxt = sprintf($chng, $chId);
    print "<li><input id='$chId' type='radio' name='datamode' value='file' ".
        ($dataMode eq 'file' ? "checked='checked' " : "") ."/>".
        &help('DataSetFile'). "Dataset from file</li>\n";
    print "<br />". &help('LocalFile').
        "Dataset file on your PC:<br />\n".
        "<input $chTxt type='file' name='set_pc' />\n";
    print "<br />". &help('NetworkFile').
        "Unix path to Dataset on server:<br />\n".
        "<input $chTxt type='text' size='30' name='setfile' value='$setFile' />";
    
    $chId  = "dmQuery";
    $chTxt = sprintf($chng, $chId);
    print "<li><input id='$chId' type='radio' name='datamode' value='database' ".
        ($dataMode eq 'database' ? " checked='checked'" : "") ."/>".
        &help('DatabaseDataSet'). "Dataset from custom database query</li>\n";
    
    $did = 'dbQuery';
    print "<div id='$did' class='DynamicDiv' cantoggle='hide' ".
        "menutitle='Database Query Parameters'><ul>\n";
    print "<li>".&help('MinimumMatch'). 
        "Discard any assignment with match score less than ".
        "<input $chTxt type='text' size='3' name='matched' value='$matched' /></li>\n";
    print "<li>".&help('EvidenceCodes'). 
        "<b>Filter Evidence Codes:</b>";
    my $ecmatch = 0;
    foreach my $ecd (@ecfilt) {
        my ($val, $d) = @{$ecd};
        my $m = "";
        if ($val eq $ecfilt) {
            $m = " checked='checked'";
            $ecmatch = 1;
        }
        print "<br /><input $chTxt type='radio' name='ecfilt' value='$val'$m /> $d\n";
    }
    print "<br /><input $chTxt type='radio' name='ecfilt' value='CUSTOM' ".
        ($ecmatch ? '' : "checked='checked'") ."/> Custom Filter: ".
        "<input $chTxt type='text' name='eccust' value='".($ecmatch ? '' : $ecfilt).
        "' size='20' />\n";
    print "</li>";
    print "<li>Discard ontology terms with represntation:<ul>";
    print "<li>". &help('MaximumRepresentation').
        "Greater than ".
        "<input $chTxt type='text' size='3' name='maxperc' value='$maxperc' />" .
        " (percent; 100% keeps all)</li>\n";
    print "<li>". &help('MinimumRepresentation').
        "Less than ".
        "<input $chTxt type='text' size='3' name='minrep' value='$minrep' />" .
        " (count; 1 keeps all)</li>\n";
    print "</ul></li>\n";
    print "<li>".&help('DefineOntologies')."<em style='background-color:red'>".
        "Ignore</em> the following Ontologies:";
    print "<input $chTxt type='hidden' name='ignoreonto' value='0' />\n";
    foreach my $oname (@ignoreOntos) {
        printf("<br /><input $chTxt type='checkbox' style='background-color:red' ".
               "name='ignoreonto' value='%s' %s/> %s ",
               $oname, $igFlags{$oname} ? " checked='checked'" : "", $oname);
    }
    print "</li>";
    print "<li>".&help('RestrictedOntology')."Use a restricted ontology:";
    print "<br/><textarea $chTxt name='ontoterms' rows='5' cols='20'>";
    print join("\n",@{$ontoReq});
    print "</textarea></li>";
    
    print "</ul>\n";
    print "</div>\n";
    print "</div>\n";
    print "<script>jsmtk_manual_load('$did'); jsmtk_manual_load('$dsrid')</script>\n";
    

    print "</ol>\n";

    print "</td></tr>\n";
    print "<tr><th colspan='2'>Customize your output:</th></tr>\n";
    print "<tr><td>\n";
    print &help('pCutOff'). 
        "Discard p-values worse than ".
        "<input type='text' size='5' name='pcutoff' value='$pcut' />\n";
    print "<input type='hidden' name='fast' value='0' />\n";
    print "<br />".&pModeForm();
    #print "<br />".&help('FastCalculation').
    #    "<input type='checkbox' name='fast' value='1' ".
    #    ($isFast ? " checked='checked'" : "") . " />".
    #    "Skip p-value calculation for faster execution\n";
    print "</td><td>\n";
    &RESULTSFORM;

    print "</td></tr>\n";
    print "<tr><th colspan='2'>$button</th></tr>\n";
    print "<tr><td>";

    $did = "advancedList";
    print "<div id='$did' class='DynamicDiv' cantoggle='hide' ".
        "menutitle='Advanced List Options'><ul>\n";
    unless ($setHTML) {
        print "<li>".&help('ReferenceSet').
            "<b>Reference Set:</b> ".
            "<input type='text' name='set' value='$set' size='15' /> ".
            "<ul><li><i>leave blank for automatic</i></li></ul></li>\n";
    }
    print "<li>".&help('TransformNameSpace').
        "<b>Unit of calculation:</b><br />\n";
    foreach my $val (@refopts) {
        my $mod = ($val eq $transf) ? " checked='checked'" : "";
        my $tag = $val || "Use as provided";
        $tag .= " <span class='smalert'>Recommended</span>" 
            if (!$val);
        print "<input type='radio' name='transform' value='$val'$mod /> ".
            "$tag<br />\n";
        print "<i>... or transform to:</i><br />" unless ($val);
    }
    
    print &help('TransformMode')."Multiple transformations will use:<ul>";
    foreach my $val (@mapOpts) {
        my $mod = ($val eq $transMap) ? " checked='checked'" : "";
        print "<li><input type='radio' name='transmap' value='$val'$mod />$val position</li>\n";
    }
    print "</ul>\n";
    print "<div id='pickqns' class='DynamicDiv' cantoggle='hide' ".
        "menutitle='Input Namespace' style='width:20em'>\n";

    print &help('QueryNamespace')."Use this section to define the <b>kind</b> of IDs you are providing. This will help the <b>Unit of Calculation</b> section perform the transformation.<br />\n";
    printf("<input type='radio' name='reference' value='%s'%s />%s".
           "<br />\n", '', $refer eq ''? " checked='checked'" : '', 
           "Automatic - <i>will work for the common, well-strucutred IDs</i>");
    my $sets = {
        "Generic Identifiers" => ["Use for unusual IDs (like GenBank RNA or Protein)", qw(AL AR AP)],
        "Poorly Structured" => ["IDs that are hard for a computer to recognize",qw(SYM)],
        "Partially Structured" => ["Can usually be guessed by 'Automatic', but are sometimes missed",qw(APS BAPS UP SP TR NRDB)],
        "Custom" => ["Use this for unusual identifiers (needs unusual data set)",qw(Custom)],
    };
    foreach my $head (sort keys %{$sets}) {
        print "<h3>$head</h3>\n";
        my @nss = @{$sets->{$head}};
        print "<i>".(shift @nss)."</i><br />\n";
        my @nsn = map {$ad->namespace_name($_) } @nss;
        for my $n (0..$#nsn) {
            my $ns = $nsn[$n] || $nss[$n];
            printf("<input type='radio' name='reference' value='%s'%s />%s".
                   "<br /.>\n",$ns, uc($ns) eq uc($refer) ? " checked='checked'" : '',
                   $ns);
        }
    }
    print "</div>\n";

    print "<div id='pickint' class='DynamicDiv' cantoggle='hide' ".
        "menutitle='Filter Transformation' style='width:20em'>\n";
    print "Sometimes when transforming your input to another namespace you need to restrict the transformation to members of a particular set (eg convert LocusLink to Probe Set, but keep only those from one chip design). Click on a 'popular' example or provide the set manually.<br />\n";
    my $intName = $args->val(qw(int)) || "";
    my $intNS   = $ad->namespace_token($args->val(qw(INTNS)));
    print &help('IntersectingSet').
        "<b>SET:</b>".
        "<input type='text' size='10' id='int' name='int' value='$intName' />\n";
    print &help('IntersectingNamespace').
        "<b>NS:</b>".
        "<input type='text' size='3' id='intns' name='intns' value='$intNS' /><br />\n";
    my @aad = qw(CANINE CANINE_2 HG_U133A HG_U219 HG_U133_PLUS_2 HG_U95A HG_U95AV2 HT_HG_U133A HT_HG_U133_PLUS_A HT_RAT230_2 MG_U74A MG_U74AV2 MOE430A MOUSE430A_2 MOUSE430_2 RAE230A RAE230B RAT230_2 RG_U34A RHESUS RN_U34 RT_U34 U133AAOFAV2);
    my @pop = ( ['TAX', 'Homo sapiens', 'Mus musculus', 'Rattus norvegicus'],
                ['AAD', @aad]);
    foreach my $setTarg (@pop) {
        my $ns = shift @{$setTarg};
        my $nsn = $ad->namespace_name($ns);
        print "<h3>Popular $nsn</h3>\n";
        print "<div style='font-size:0.7em'>\n";
        foreach my $val (@{$setTarg}) {
            printf("<a class='butt' ns='%s' title='%s' onclick='return setInt(this)'>%s</a>\n",
                   $ns, $val, $val);
        }
        print "</div>\n";
    }
    print <<EOF;
<script>
    function setInt(obj) {
        if (!obj) return false;
        document.getElementById('int').value = obj.innerHTML;
        document.getElementById('intns').value = obj.getAttribute('ns');
        return false;
    }
</script>

EOF

    print "</div>\n";

    
    print "<li>".&help('RestrictReference'). 
        " Use an explicitly restricted reference:<br />".
        "<textarea id='restrictref' name='restrictref' rows='3' cols='30'>".
        "$restRef</textarea></li>\n";

    print "<li>".&help('ListOfLists').
        "<input type='checkbox' name='ismatrix' value='1' ".
        ($isMatrix ? "checked='checked' " : "") ."/>".
        " Query is defining a List-of-Lists<ul>";
    print"<li>".
        "<input type='text' size='2' name='headers' value='$headNum'>".
        " columns are used as headers.</li></ul></li>\n";
    print "<input type='hidden' name='purgens' value='0' />\n";
    print "<li>".&help('NamespacePurging').
        "<input type='checkbox' name='purgens' value='1' ".
        ($purgens ? "checked='checked' " : "") ."/>".
        " Purge unusual IDs from list</li>\n";
    print "<input type='hidden' name='removenull' value='0' />\n";
    print "<li>".&help('IgnoreNullAccessions').
        "<input type='checkbox' name='removenull' value='1' ".
        ($nonull ? "checked='checked' " : "") ."/>".
        " Remove queries lacking ontology annotations</li>\n";
    print "<li>".&help('XpressRankedLists').
        "<input type='checkbox' name='useknn' value='1' ".
        ($args->val(qw(USEKNN)) ? "checked='checked' " : "") ."/>".
        " Use the list to extract KNN lists from XPRESS<ul>";
    print "<li>Maximum allowed Z-score ".
        "<input type='text' size='2' name='zmax' value='$zmax'>".
        "</li></ul></li>\n";

    print "</ul></div>\n<script>jsmtk_manual_load('$did'); jsmtk_manual_load('$did'); jsmtk_manual_load('pickint')</script>\n";

    print "</td><td>";

    $did = "advancedMode";
    print "<div id='$did' class='DynamicDiv' cantoggle='hide' ".
        "menutitle='Advanced Analysis Options'><ul>\n";
    print "<input type='hidden' name='bonfterm' value='0' />\n";
    print "<li>Multiple testing correction<ul>".
        "<li>".&help('BonferroniCorrectTerms'). 
        "<input type='checkbox' name='bonfterm' value='1' ".
        ($bonfgo ? "checked='checked' " : "") ."/>".
        " Bonferroni correction for number of ontology terms</li>\n";
    print "</ul>\n";
    print "<li>".&help('IgnoreUnderRepresentation').
        "<input type='checkbox' name='noNot' value='1' ".
        ($noNot ? "checked='checked' " : "") ."/>".
        " Disregard significant under representation</li>\n";
    print "</ul></div>\n<script>jsmtk_manual_load('$did')</script>\n";

    print "</td></tr>";
    print "</tbody></table>\n";

    $did = "advancedRandom";
    print "<div id='$did' class='DynamicDiv' cantoggle='hide' ".
        "menutitle='Additional Options'><ul>\n";
    print "<input type='hidden' name='showbad' value='0' />\n";
    print "<li>".&help('FilterReport').
        "<input type='checkbox' name='showbad' value='1' ".
        ($args->val(qw(SHOWBAD)) ? "checked='checked' " : "") ."/>".
        " Generate summary table of sub-optimal GO terms</li>\n";

#    print "<li>".&help('QuerySummary').
#        "<input type='checkbox' name='querysum' value='1' ".
#        ($args->val(qw(QUERYSUM)) ? "checked='checked' " : "") ."/>".
#        " Sumarize query list used</li>\n";
    print "<li>".&help('ProbabilitySummary').
        "Probability summary plot size (pixels) ".
        "<input type='text' size='4' name='probsize' value='$probSz'></li>\n";
    print "<li>".&help('RandomLists').
        "Include ".
        "<input type='text' size='3' name='randtest' value='$randc' />".
        " random lists of size ".
        "<input type='text' size='8' name='randsize' value='$rands' /></li>\n";
    print "<li>Debugging options<ul>\n";
    print "<li><input type='checkbox' name='dumpsql' value='1' ".
        ($dumpsql ? "checked='checked' " : "") ."/>".
        " Show SQL</li>\n";

    print "<li><input type='checkbox' name='benchmark' value='1' ".
        ($args->val(qw(BENCHMARK)) ? "checked='checked' " : "") ."/>".
        " Show program benchmark times</li>\n";
    print "</ul></li>\n";
    print "</div>\n<script>jsmtk_manual_load('$did')</script>\n";






#    print "<input type='checkbox' name='probtable' value='1' ".
#        ($args->val(qw(PROBTABLE)) ? "checked='checked' " : "") ."/>";
#    print " Calculate probability tables for each ontology<br />\n";

    $did = "examplePop";
    print "<div id='$did' class='DynamicDiv' cantoggle='hide' ".
        "menutitle='Pre-canned example analyses'><ol>\n";
    
    my $examp = {
        'Retroviral Protease Inhibitor'  => {
            file => '/stf/biohtml/examples/rv_protease.txt',
        },
        'Top Huntington hits from hdbase.org' => {
            file => '/stf/biohtml/examples/huntingtonLong.txt',
        },
        'KNN Top 25 for TPD52' => {
            file => '/stf/biohtml/examples/knn_tpd52.txt',
         },
        'KNN Top 25 for MMP2' => {
            file => '/stf/biohtml/examples/knn_mmp2.txt',
        },
        'ALL Relapse' => {
            file => '/stf/biohtml/examples/BeesleyRelapseSuppl.txt',
        },
        'BrainArray Example' => {
            file => '/stf/biohtml/examples/BrainArrayLists.txt',
        },
    };
    my %capD = map { $_ => 1 } qw(set);
    foreach my $desc (sort keys %{$examp}) {
        my (@ubits, @dbits);
        foreach my $key (sort keys %{$examp->{$desc}}) {
            my $val = $examp->{$desc}{$key};
            push @ubits, "$key=$val";
            next unless ($capD{$key});
            substr($key,0,1) = uc(substr($key, 0,1));
            push @dbits, "<b>$key:</b> $val";
        }
        push @ubits, "jobnote=Example - $desc";
        printf("<li><a href='hypergeometric_affy.pl?%s'>%s</a> %s</li>\n",
               join('&', @ubits), $desc, join(', ', @dbits));
    }
    print "</ul></div>\n<script>jsmtk_manual_load('$did')</script>\n";
    print "</form>\n";
    # print "<script> load_ontology = [ ]; </script>\n";
}

sub HTMLSTART {
    return if ($nocgi || $clean);
    print "<html pid='$$'><head>\n";
    print "  <title>Affymetrix-GO Hypergeometric Distribution</title>\n";
    print "  <link rel='shortcut icon' href='/biohtml/images/BlackBlackRed.png'>\n";
    print "  <script type='text/javascript' src='/biohtml/javascript/miniAjax.js'></script>\n";
    print $ahg->html_head( $args->val(qw(BETAJS)) );
    print $ad->namespace_url_styles();
    print "</head><body bgcolor='white'>\n";
}

sub HTMLEND {
    return if ($nocgi || $clean);
    if ($isbeta) {
        my $did = 'jsmtk_errors';
        print "<div id='$did' class='DynamicDiv' cantoggle='hide' menutitle='JSMTK Error Console'></div>\n<script>jsmtk_manual_load('$did')</script>\n";
#        print <<EOF;
#        
#        <script>
#           document.body.appendChild(jsmtk_error_list);
#        </script>
#EOF
    }
    print "</body></html>\n";
}

sub ACT_ON_ARGS {
    if (my $f = $args->val(qw(SUMMARIZE))) {
        print $outfh $ahg->summarize_tsv( -file   => $f,
                                          -format => $format );
        return;
    }

    # print $args->to_text();

    # Gene_Ontology biological_process molecular_function cellular_component
    # qw(GO:0003673 GO:0008150 GO:0003674 GO:0005575)

    # Add any random lists requested
    &RANDOM_LISTS( \@requests );
    # $ticket->status("DebugMessage2", "($args->val(qw(STATUS))) $#requests requests in $reqNs by $$ via".join(' + ', &stack_trace)) if ($ticket);
    if ($#requests == -1) {
        if ($inputCount && $ticket && !$isTemp) {
            my $err = "No valid lists available for analysis!";
            my $isCommon = 0;
            if ($ldap =~ /^jacksod|tilfordc$/) {
                $err .= " Oh, it's you.";
                $isCommon = 1;
            }
            &task_failed($err, undef, $isCommon);
        }
        return;
    }

    # I don't think I need originalns

    my @custArgs = ( -matched    => $matched,
                     -maxrep     => $maxperc,
                     -minrep     => $minrep,
                     -ecs        => $ecfilt,
                     -fork       => $args->val(qw(FORK)),
                     -redons     => $args->val('redons'),
                     -setname    => $args->val(qw(SETNAME)),
                     -refresh    => $args->val(qw(REFRESH)),
                     -dumpsql    => $args->val(qw(DUMPSQL)),
                     -ontosubset => $args->val('ontosubset'),
                     -originalns => undef,
                     -ontoterms  => $ontoReq,
                     -ontologies => $otxt,);

    # print $outfh $ahg->stat_reference_html() if ($format eq 'HTML');
    
    $set ||= $requests[0]{param}{SET};
    if ($dataMode eq 'file') {
        # A set is being provided in a file
        if ($sffh && $setFile) {
            # oops
            &err("The set file is being defined by both a local file on your PC and a path to a unix file; I am not sure which to use. Please select only one", "MultipleSetSources");
            return;
        }
        if ($setFile) {
            foreach my $swap (0,'_','-') {
                last if (-e $setFile);
                foreach my $dir (0, '/stf/biohtml/GSEA/') {
                    my $foo = $setFile;
                    $foo =~ s/\s+/$swap/g if ($swap);
                    $foo = "$dir/$foo" if ($dir);
                    if (-e $foo) {
                        $setFile = $foo;
                        last;
                    }
                }
            }
            unless (-e $setFile) {
                &err("Could not find set file '$setFile'. If you wish to generate this file from a database query, specify -datamode db",'FileOpenFailure', 'fatal');
            }
            $sc = $ahg->get_dataset
                ( -file   => $setFile,
                  -ticket => $ticket, );
        } else {
            my $qtag = join('_', 'SET', $ldap || 'user', $$). '.txt';
            $setFile = "/stf/biohtml/tmp/$qtag";
            $sc = $ahg->get_dataset
                ( -file   => $sffh,
                  -ticket => $ticket );
            $args->death("Failed to recover set file from file handle", $sffh)
                unless ($sc);
            $sc->comments("Set file read from local PC");
            $sc->write_data_file( -file   => $setFile,
                                  -ticket => $ticket);
            my $url = $nocgi ?
                $setFile : &path2url($setFile,'a temporary file');
            &msg("Your Set File has been written to $url to make re-analysis easier", "TemporaryFile");
        }
    } elsif ($dataMode eq 'database') {
        # Request to load specific data from database
        $sc = $ahg->get_dataset
            ( @custArgs,
              -set        => $set,
              -ans        => $reqNs,
              -ticket => $ticket );

        if ($setFile) {
            $sc->comments("Custom data set created on ".`date`);
            $sc->write_data_file( -file   => $setFile,
                                  -ticket => $ticket);
        }
        my $url = $nocgi ?
            $setFile : &path2url($setFile,'a temporary file');
        &msg("Your Set File has been written to $url", "TemporaryFile");
    } elsif (my $comp = $args->val(qw(EXPLICIT))) {
        $sc = $ahg->{ONTOSET} = BMS::SetCollection->new( -ticket => $ticket );
        my @comps = &_READ_LIST($comp);
        
        my $rs = $sc->set($set || "ReferenceSet");
        unless ($selfRef) {
            # We need to get set members
            my $ns = $refer || $requests[0]{param}{NAMESPACE};
            $sc->alias('ReferenceSet', $set) if ($set);
            if ($set && $ns) {
                &msg("Finding reference set members") if ($vb);
                my @members = $ad->convert( -id => $set, -ns1 => 'SET',
                                            -ns2 => $ns, -min => $transMat);
                $rs->obj_ids(\@members);
            } elsif (my $rreq = $args->val(qw(REFSET))) {
                my ($mref) = &_READ_LIST($rreq);
                $rs->obj_ids( $mref->{list} );
                $set ||= 'ReferenceSet';
            } elsif ($args->val(qw(NOREF))) {
                # Do nothing - The reference will be built by the
                # intersection of the query lists and the custom sets
            } else {
                $selfRef = 1;
                $mode = 'GSEA';
                &err("Caution: Insufficient information to determine reference set members. Forcing -selfref analysis (query list == reference set). Be sure that both -set (eg HG_U133A) and -reference (eg APS) are defined in order to calculate the full reference set", "ForcedSelfReference",'TICK');
            }
        }
        my $cset = $sc->set("Custom Set");
        for my $c (0..$#comps) {
            my $cd = $comps[$c];
            next unless ($cd->{list} && $#{$cd->{list}} != -1);
            my $id = $cd->{param}{NAME} || sprintf("CUST:%05d", $c+1);
            $cset->obj_param($id, 'desc', $cd->{param}{DESC} || "Custom Group ".($c+1));
            $cset->connect( $rs, $id, @{$cd->{list}} );
        }
    } else {
        # User wants to use default settings
        $sc = &get_default_set( );
    }

    unless ($sc) {
        return if ($bgnd);
        &task_failed("Failed to get SetCollection")
            if ($ticket && !$isTemp);
        if ($nocgi) {
            die "Failed to get SetCollection";
        } else {
            $args->msg("[ERROR]", "Failed to recover the SetCollection");
            return;
        }
    }

    my $scPath = $sc->param('FileLocation');
    if ($ticket && $scPath) {
        if (my $prior = $ticket->param('setFile')) {
            if ($prior eq $scPath) {
                my $ft = int(time - ((-M $scPath) * 24 * 60 * 60));
                my $pt = $ticket->param('setTime');
                my $delta = ($ft && $pt) ? ($ft - $pt) / 60: 0;
                if (abs($delta) > 5) {
                    my $u = 'minutes';
                    if (abs($delta) > 100) {
                        $delta /= 60;
                        $u = 'hours';
                        if (abs($delta) > 100) {
                            $delta /= 24;
                            $u = 'days';
                        }
                    }
                    $delta = int($delta * 10)/10;
                    &msg("Caution - the set file (<span class='file'>$scPath</span>) has a different modification time than the file originally used ($delta $u difference)", "DifferentSetFile", 'alert');
                }
            } else {
                &msg("Caution - current analysis is utilizing a set file (<span class='file'>$scPath</span>) that is different from the originally used file (<span class='file'>$prior</span>)", "DifferentSetFile", 'alert');
            }
        } else {
            my $ft = int(time - ((-M $scPath) * 24 * 60 * 60));
            $ticket->extend( { setFile => $scPath,
                               setTime => $ft } );
        }
    }

    my $rs = $sc->set('ReferenceSet');

    if ($otxt) {
        my %filter;
        map { push @{$filter{$ontoStat{$_}}}, $_ } keys %ontoStat;
        my %nss;
        foreach my $set ($rs->connected_sets) {
            my $ns = $ad->namespace_token($set->param('NameSpace'));
            push @{$nss{$ns}}, $set;
        }
        if (my $keep = $filter{''}) {
            while (my ($ns, $sets) = each %nss) {
                my $ok = 0;
                foreach my $kns (@{$keep}) {
                    $ok++ if ($ad->is_namespace($ns, $kns));
                }
                unless ($ok) {
                    map { $_->param('skip', 1) } @{$sets};
                }
            }
        } elsif (my $toss = $filter{'!'}) {
            while (my ($ns, $sets) = each %nss) {
                my $ok = 1;
                foreach my $kns (@{$toss}) {
                    $ok = 0 if ($ad->is_namespace($ns, $kns));
                }
                unless ($ok) {
                    map { $_->param('skip', 1) } @{$sets};
                }
            }
        }
    }


    if ($restRef) {
        my @comps = &_READ_LIST($restRef);
        if ($#comps > 0) {
            $args->msg("Only using first list in $restRef for -restrict");
        }
        my $comp = $comps[0];
        if ($comp && $comp->{list} && $#{$comp->{list}} != 0) {
            $rs->allowed_objects( $comp->{list} );
            my $full  = $rs->obj_count();
            my $allow = $rs->allowed_count();
            my $msg   = "Restricting reference to $allow out of $full objects";
            if ($ticket) {
                $ticket->status("RestrictReference", $msg);
            } else {
                $args->msg($msg);
            }
        } else {
            $args->err("List not found in -restrict $restRef");
        }
    }

    unless ($rs->allowed_count) {
        &task_failed("<span class='alert'>The reference set '<b>". 
                     $rs->name ."</b>' is completely empty!</span>")
            unless ($isTemp);
        return;
    }

    if ($leafOnly) {
        # The user only wants to analyze the leaves of the tree
        foreach my $setSet ($sc->each_set) {
            my $sname = $setSet->name;
            next unless ($sname =~ /(GeneOntology|Xpress|BMS TF)/);
            my @leaves;
            my $ns = "";
            foreach my $term ($setSet->each_object) {
                my @kids = $ad->direct_genealogy($term, 1, $ns);
                push @leaves, $term if ($#kids == -1);
            }
            $setSet->allowed_objects(\@leaves);
        }
    }
    
    $set ||= $rs->name;
    
    &show_collection($sc);
    &show_notes();

    if ($forceSet) {
        &msg("Set file for $forceSet has been generated at <span class='file'>$setFile</span>",
             "ForceSet");
        return;
    }

    unless ($isTemp || $asCurve || $ontoMem) {
        if ($ticket) {
            $resFile = "$tempDir/".$ticket->ticket.".xml";
            $ticket->extend( { results => $resFile } );
        } else {
            $resFile = sprintf("%s/GSEA-Temp-%d-%d.xml",$tempDir, time, $$);
        }
        open(RESF, ">$resFile") || &err
            ("Failed to write to '$resFile'\n  $!",'FileWriteFailure','FATAL');
        if ($ticket) {
            print RESF "<resultSet ticket='".$ticket->path()."'>\n";
        } else {
            print RESF "<resultSet>\n";
        }
        $resFH  = *RESF;
        &msg("Results will be saved to <span class='file'>$resFile</span>",
             "ResultsFile") unless ($bgnd);
    }

    my $lstart = time;
    my $done   = 0;
    map { $done += &DO_LIST( $_ ) } @requests;

    if ($resFH) {
        print RESF "</resultSet>\n";
        close RESF;
        chmod(0666, $resFile);
    }
    print $outfh "<hr />" if ($format =~ /html/i);
    if ($vb) {
        my $what = sprintf("%d list%s", $done, $done == 0 ? '' : 's');
        my $rnum = $#requests + 1;
        $what .= sprintf(" (%d failed)", $rnum - $done) if ($done < $rnum);
        my ($u, $un) = (time - $lstart, 'sec');
        ($u, $un) = ($u / 60, 'min') if ($u > 60);
        &msg(sprintf("%.1f %s to analyze %s", $u, $un, $what));
    }
    print $outfh "<p></p>" if ($format eq 'HTML');
    $ticket->status('TaskComplete', "Analysis done") if ($ticket && !$isTemp);
}

sub get_default_set {
    my ($maxPer, $dir, $file) = &_file_from_settings();
    my $defSet;
    return undef unless ($dir);
    my $sfile = $file ? "$dir/$file" : '';

    if ($sfile && -e $sfile && !$args->val(qw(REBUILD))) {
        # The file exists and we are not supposed to rebuild
        $defSet = $ahg->get_dataset( -file   => $sfile,
                                     -ticket => $ticket );
    } else {
        # Either the file is not there or we are rebuilding
        my @stand = ($maxPer > 100) ? ( -isinsane => 'Yes') :
            ( -matched    => 80,
              -minrep     => 3,
              -ec         => '!ND !NR !E', );
        $defSet = $ahg->get_dataset
            (  @stand,
               -originalns => undef,
               -maxrep     => $maxPer > 100 ? 100 : $maxPer,
               -minrep     => $minrep,
               -set        => $set,
               -redons     => $args->val('redons'),
               -ans        => $reqNs,
               -verbose    => $veryVB,
               -ticket     => $ticket,
               -ontosubset => $args->val('ontosubset'),
               -ontologies => $otxt,
               );
        if ($sfile) {
            my $refName = $defSet->param('ReferenceSet');
            $defSet->write_data_file( -file   => $sfile,
                                      -ticket => $ticket,
                                      -order  => [$refName], );
            chmod(0666, $sfile);
            $defSet->param('FileLocation', $sfile);
            &msg("SetCollection written to <span class='file'>".
                 "$sfile</span>\n", 'SetCollection') if ($vb);
        }
    }
    return $defSet;
}

sub _file_from_settings {
    &DETERMINE_SET;
    return () unless ($set);

    my $maxPer;
    if ($standSz =~ /\d+/) {
        $maxPer = int($standSz);
        $standSz  = "";
    } elsif ($standSz eq 'Insane') {
        $maxPer = 999;
    } elsif ($standSz eq 'Exhaustive') {
        $maxPer = 100;
    } elsif ($standSz eq 'Rapid') {
        $maxPer = 1;
    } else {
        $maxPer = 5;
    }
    my $dir  = "/stf/biohtml/GSEA";
    my $file = "";
    my $ns = $ad->namespace_token($reqNs);
    if ($maxPer > 0) {
        my $fset = $set;
        $fset =~ s/\s+/-/g;
        $file = sprintf("GSEA_%s_%s_%03d.set", $fset, $ns, $maxPer);
    }
    return ($maxPer, $dir, $file);
}

sub RANDOM_LISTS {
    my ($lists) = @_;
    return unless ($randc);
    unless ($set) {
        &err("You must define the reference set to do a random test", "SetNotDefined",'TICK');
        return;
    }
    $ahg->set($set);

    my $fullSet = $ahg->list_for_set( $set );
    my $size    = $args->val(qw(RANDSIZE)) || $#{$fullSet} + 1;
    my $headers = $ahg->option('headers') || [];

    my $msg = sprintf
        ("Generating %d random lists of size %s for set %s",
         $randc, $size, $set);
    if ($vb) {
        my $probes = $ahg->list_for_set( $set );
        $msg .= " [".($#{$probes} +1)."]";
        &msg($msg, "RandomLists");
    }
    $ticket->status('RandomLists', $msg) unless ($isTemp);

    if ($headNum < 1) {
        # We need to add a header to each existing list
        map { push @{$_->{headers}}, '' } @{$lists};
        $headNum  = 1;
    }
    for my $c (1..$randc) {
        # Add a header name:
        my $name = "Random List $c";
        my @head = ($name);
        # Pad any additional header columns
        map { push @head, '' } (2..$headNum);
        my $list = $ahg->random_set( $size );
        push @{$lists}, {
            name    => $name,
            headers => \@head,
            list    => $list,
        };
    }
}

sub APPLY_LIST_METADATA {
    my ($dat) = @_;
    my $mHash = $dat->{meta} || {};
    my @list  = @{$dat->{list}};
    my @metas = sort { uc($a) cmp uc($b) } keys %{$mHash};
    if (my $pMeta = $sc->param('metadata')) {
        # Make sure we will update any prior metadata keys
        my %prior = map { $_ => 1 } @{$pMeta};
        map { delete $prior{$_} } @metas;
        push @metas, keys %prior;
    }
    $sc->param('metadata', \@metas);
    foreach my $tag (@metas) {
        # Go ahead and grab metadata even for IDs that were filtered from
        # the list
        my %xtra = map { $_ => 1 } keys %{$mHash->{$tag}};
        map { delete $xtra{$_} } @list;
        foreach my $id (@list, keys %xtra) {
            my $val = $mHash->{$tag}{$id};
            $val = "" unless (defined $val);
            $sc->obj_param( $id, $tag, $val );
        }
    }
}

sub DO_LIST {
    my ($dat) = @_;

    my $reqlist  = $dat->{list};
    my $lnum     = $dat->{param}{LISTNUMBER};
    my $listName = $dat->{name};
    my @headers  = @{$dat->{headers}};

    
    $ahg->option('headers', \@headers);
    if ($#{$reqlist} < 0) {
        &err("$listName - Empty list", "EmptyQueryList", 'TICK')
            unless ($dat->{nullTransform});
        return 0;
    }
    $ticket->status('AnalyzeList', "<span class='query'>$listName</span> - ".
                    ($#{$reqlist}+1)." entries") if ($ticket && !$isTemp);
    my $ref     = $sc->set('ReferenceSet');
    my $thisSet = $ref->name;
    
    my $list = $reqlist;
    if (!$selfRef || $noExpand) {
        # Remove unknown IDs, unless the user is defining the query as the
        # world and is not requesting non-expansion
        my ($ok, $unknown) = $ref->trim_unknown($list);
        my $numUnk = $#{$unknown} + 1;
        if ($numUnk) {
            $list = $ok;
            my $msg = sprintf
                ("%d query list ID%s are not members of the reference set",
                 $numUnk,  $numUnk == 1 ? '' : 's' );
            $ticket->status('UnknownQueryTerm', $msg) if ($ticket && !$isTemp);
            $msg .= ": ".join(', ', @{$unknown}) unless ($numUnk > 50);
            &err($msg, "UnknownQueryTerm");
            if (my $ur = $dat->{meta}{UnfilteredRank} = $dat->{meta}{Rank}) {
                my $rf = $dat->{meta}{Rank} = {
                    map { $ok->[$_] => $_ + 1 } (0..$#{$ok})
                };
            }
        }
        if ($#{$ok} == -1) {
            &err("None of your query terms appear to be from the reference set! Either the reference set is inappropriate or corrupted, or there is a mismatch between the type of IDs in the reference set and your queries.", "EmptyQueryList");
            $ticket->status('EmptyQueryList', "None of the query terms were found in the reference set!") if ($ticket && !$isTemp);
        }
    }
    if ($selfRef) {
        # The user list *is* the world
        $ref->allowed_objects($list);
    }

    &APPLY_LIST_METADATA( $dat );


    my @ontos = $ref->connected_sets();
    my $setMP = $sc->param('MaximumRepresentation');
    if ($maxperc && (!$setMP || $maxperc < $setMP)) {
        # We need to exclude over-represented ontology terms
        my $maxcount = int($ref->allowed_count * $maxperc / 100);
        foreach my $oset (@ontos) {
            $oset->precache_connections( $ref );
            my $objs = $oset->allowed_objects || [1..$oset->obj_count];
            my @keep;
            for my $o (0..$#{$objs}) {
                my $tid   = $objs->[$o];
                my $conns = $oset->connections($ref, $tid);
                push @keep, $tid if ($#{$conns} < $maxcount);
            }
            if ($#keep < $#{$objs}) {
                $oset->allowed_objects(\@keep);
            }
        }
    }

    my $action = $mode;
    if ($action eq 'Auto') {
        unless ($ref->allowed_count) {
            $ticket->status('NullReference', "Empty reference set")
                if ($ticket && !$isTemp);
            return 0;
        }
        my $frac = ($#{$list} + 1) / $ref->allowed_count;
        $action = ($frac >= 0.50) ? 'GSEA' : 'Full';
    }

    &msg($listName) if ($listName && $nocgi && $vb);
    return &ONTO_MEMBERS($reqlist) if ($ontoMem);

    if ($format eq 'HTML') {
#        print $outfh $ahg->object_description_table('both')
#            if ($args->val(qw(QUERYSUM)));
    }
    
    my $results = $ahg->gsea
        ( -list     => $list,
          -ref      => $ref,
          -ascurve  => $asCurve,
          -terms    => $ontoReq,
          -pmode    => $pMode,
          -fast     => $isFast,
          -nonull   => $nonull,
          -nonot    => $noNot,
          -debug    => $doDebug,
          -sublist  => $action eq 'GSEA' ? 1 : 0,
          -precut   => $preCut,
          -pcut     => $postCut ? 0 : $pcut );

    $results->param('listnum', $lnum);
    $results->param('listname', $listName);
    $results->param('listhead', \@headers);
    $results->param('query list', $list);

    my $fmsg = $format;
    my $isLast = ($lnum == $#requests + 1) ? 1 : 0;
    $fmsg .= ": <span class='file'>$ofile</span>" if ($ofile);
    $ticket->status('FormatResults', $fmsg) 
        if ($ticket && (!$isTemp || $isLast));
    &format_results( -results => [$results],
                     -isfirst => ($lnum == 1) ? 1 : 0,
                     -islast  => $isLast, );

    if ($resFH) {
        $results->to_xml($resFH, ' ');
    }
    print $outfh $ahg->benchmark_table() if ($dumpsql > 0);
    return 1;
}

sub format_results {
    my $fargs = $ad->parseparams( @_ );
    my $rlist = $fargs->{RESULTS};
    my ($iF, $iL) = map { $fargs->{$_} } qw(ISFIRST ISLAST);
    for my $r (0..$#{$rlist}) {
        my $results = $rlist->[$r];
        my @fsArgs = 
            ( -format     => $format,
              -isfirst    => defined $iF ? $iF : ($r == 0) ? 1 : 0,
              -islast     => defined $iL ? $iL : ($r == $#{$rlist}) ? 1 : 0,
              -sumnum     => $args->val('sumnum'),
              -bonfterm   => $bonfgo,
              -nonot      => $noNot,
              -onlynot    => $args->val('onlynot'),
              -bonflist   => $bonflist,
              -ascurve    => $asCurve,
              -ticket     => $ticket,
              -comparelod => $args->val('comparelod'),
              -aslod      => $args->val('aslod'),
              -excludeterm => $exTerm,
              -pcut       => $postCut ? 0 : $pcut,
              -showtree   => $args->val('showtree'),
              -applybonf  => $args->val('applybonf'),
              -tagticket  => $args->val('tagticket'),
              -idsforterm => $idsForTerm,
              -stats      => $results,);
        my $lods = $results->param('lods');

        if ($format eq 'Excel') {
            push @fsArgs, ( -file => $ofile );
        } elsif ($asCurve) {
            # Let a file name be generated automatically
            
        } else {
            push @fsArgs, ( -fh => $outfh );
        }
        # open(FOO, ">/home/tilfordc/foo.txt"); print FOO $args->branch($results->{PARAMS}); close FOO; die "HERE";
        # warn "FORMATING: ".$args->branch(\@fsArgs);
        my $fmt = $ahg->format_statistics(@fsArgs);
        # &add_observed_lod_plot( $lods ) ;
        # Some iterative formats only return results at the end
        next unless ($fmt);
        if ($format eq 'Excel') {
            if ($nocgi) {
                $args->msg("Excel sheet at ".$fmt->file_path());
            } else {
                print $fmt->html_summary();
            }
        } elsif ($ofile && !$asCurve) {
            print $outfh $fmt;
        } else {
            print $fmt;
        }

        &add_observed_lod_plot( $lods ) 
            if ($probSz && $format ne 'HTML Summary' && $format ne 'Excel');
    }
}

sub add_observed_lod_plot {
    my ($lods) = @_;
    return unless ($probSz);
    my $pReq = '';
    if ($ofile) {
        $pReq = $ofile;
        $pReq =~ s/\.[^\.]{1,4}$//;
        $pReq .= "-LOD_Plot";
    }
    my ($html, $png) = $ahg->observed_lod_plot
        ( -size => $probSz,
          -file => $pReq,
          -lods => $lods );
    if ($format =~ /html/i) {
        print $outfh "<br />\n$html";
    } elsif ($nocgi) {
        $args->msg("LOD plot PNG at $png");
    } else {
        print "<br />\n$html";
    }
}

sub ticket_set_stats {
    my ($ticket) = @_;
    my $tick     = $ticket->ticket();
}

sub _make_sc_summary_file {
    my ($sc, $colFile) = @_;
    open (CFILE, ">$colFile") || &err
        ("Failed to write summary to $colFile:\n  $!",
         'FileWriteFailure','FATAL');
    print CFILE $sc->html_summary();
    close CFILE;
    chmod(0666, $colFile);
    $ticket->extend( { setSummary => $colFile } );
}

sub show_collection {
    my ($sc) = @_;
    if ($ticket) {
        my $colFile = $ticket->param('setSummary');
        unless ($colFile) {
            $colFile = "$tempDir/".$ticket->ticket.".setSummary.html";
            &_make_sc_summary_file( $sc, $colFile);
            $ticket->extend( { setSummary => $colFile } );
        }
        if ($format =~ /html/i) {
            my $url = &selfUrl({ticket => $ticket->ticket(),
                                show   => 'setSummary', clean => 1 });
            my $excl = &selfUrl({ticket => $ticket->ticket(), pcutoff => 0.05,
                                 results => 1, format => 'Excel' });
            print $outfh "<p><a class='butt' href='$url' onclick='return cuteGet(this)'>Show set summary</a> | <a class='butt' style='background-color:#9ff' href='$excl'>Export to Excel</a></p>\n";
        }
    }
    if (0 && $format =~ /html/i) {
        my $did = "DataSetLoad";
        print $outfh "<div id='$did' class='DynamicDiv' cantoggle='hide' menutitle='Dataset load criteria'>\n";
        map { print $outfh $_->html_summary( $_ ) } @_;
        print $outfh "</div>\n<script>jsmtk_manual_load('$did')</script>\n";
        if ($#_ == 0) {
            my %linkArgs = ( setfile    => $_[0]->param('FileLocation'),
                             query_path => $qfile,
                             selfref    => $selfRef,
                             pmode      => $pMode,
                             fast       => $isFast,
                             isMatrix   => $isMatrix ? 1 : 0,
                             headers    => $headNum || 0);
            $linkArgs{benchmark} = 1 if ($args->val(qw(BENCHMARK)));
            my @las;
            my @bits = split(/\//, $0);
            while (my ($key, $val) = each %linkArgs) {
                push @las, "$key: '$val'" if (defined $val);
            }
            my $ldat = sprintf("['%s', { %s } ]", $bits[-1],join(', ', @las));

            print $outfh "\n<script>\nsetLink = $ldat;\n</script>\n";
            $ahg->option('setlink', $ldat);
        }
    }

    if ($format !~ /html/i & $vb) {
        #my $dbTime = time - $start;
        if ($doDebug) {
            $args->msg("Finished dataset recovery");
            warn $ad->showbench;
        }
    }

}

sub show_notes {
    my $statType = $pMode; # $isFast ? "Odds" : "P-value";

    my $dt = `date`; chomp $dt;
    my @notes = ( ["Prepared on $dt"]);
    push @notes, [ "Using ticket ".&tick_link($ticket),'TicketSystem']
        if ($ticket);
    if ($ticket) {
        if (my $nt = $ticket->param('Notes')) {
            push @notes, ["User notes: $nt", 'JobNotes'];
        }
    }
    push @notes, [ "Full Reference Set: <span class='ref'>$set</span>",
                   "ReferenceSet", ];
    push @notes, [ "Sublist namespace: $refer",
                   "NameSpace", ] if ($refer);
    push @notes, [ "Analysis namespace: $transf",
                   "TransformNameSpace", ] if ($transf && $transf ne $refer);
    my $amNote;
    if ($mode eq 'GSEA') {
        if ($asCurve) {
            $amNote = "A full GSEA scan of your list is being performed for curve plotting";
        } else {
            $amNote = "A GSEA scan is being performed on your list";
        }
    } elsif ($mode eq 'Auto') {
        $amNote = "Analysis mode is Auto - small lists are treated as a single sublist, large lists have a full GSEA scan.";
    } else {
        $amNote = "Your list is being analyzed as a single sublist";
    }
    push @notes, [ $amNote, "AnalysisMode", ];
    
    push @notes, [ "The query list is also defining the entire ReferenceSet".
                   ($noExpand ? ". However, the reference set will not be expanded beyond the scope of the original reference" : ""),
                   "SelfReferential", ] if ($selfRef);
    push @notes, [ "Ontology terms that are not represented in your QueryList are ignored",
                   "IgnoreNullAccessions", ] if ($nonull);
    push @notes, [ "Statistical Metric: $pMode", 'StatsMode' ];

    push @notes, [ "P-values are not being calculated in order to expedite analysis",
                   "FastCalculation", ] if ($isFast);
    push @notes, ["Significant hits to under-represented terms will not be reported", 'IgnoreUnderRepresentation'] if ($noNot);
    if ($pcut) {
        my $what = ($bonfgo || $bonflist) ? 'corrected ' : '';
        push @notes, [ "$statType cutoffs will be corrected for number of terms tested",
                       "BonferroniCorrectTerms", ] if ($bonfgo);
        push @notes, [ "$statType cutoffs will be corrected for number of sublists tested",
                       "BonferroniCorrectLists", ] if ($bonflist);
        push @notes, [ "Terms with ${what}$statType less than <span class='p'>$pcut</span> will be rejected",
                       "pCutOff", ];
    }
    push @notes, [ "Only leaf terms on the ontology tree will be analyzed",
                   "LeafTermsOnly", ] if ($leafOnly);

    if ($exTerm) {
        my $msg = sprintf
            ("%d term%s (and all their children) are being excluded from the ".
             "results:<br /><table class='tab'><tbody>", 
             $#{$exTerm} +1, $#{$exTerm} == 0 ? '' : 's');
        foreach my $term (@{$exTerm}) {
            my $desc = "";
            my $s    = $sc ? $sc->set_for_node($term) : undef;
            if ($s) {
                $term    = $s->obj_name( $s->obj_id( $term) );
                my $desc = $s->desc($term);
            } else {
                my $gns = $ad->guess_namespace($term);
                ($term) = $ad->standardize_id($term, $gns);
                $desc   = $ad->description( -id => $term, -ns => $gns);
            }
            $desc ||= '';
            $msg .= "\n<tr><td class='term'>$term</td><td>$desc</td></tr>";
        }
        $msg .= "</tbody></table>";
        push @notes, [ $msg, "ExcludeTerm"];
    }
    push @notes, map { ["Input filtered: <b>$_->{txt}</b>", 'InputFilter'] } @inputFilter;
    
    if ($asCurve) {
        if (!$ontoReq || $#{$ontoReq} == -1) {
            &err("You must specify one or more distinct ontology terms (using -terms) if you wish to draw a GSEA curve", 'GseaProfile');
            return;
        }
        my $msg = sprintf("Rendering GSEA profile plot for %d ontology term%s",
                          $#{$ontoReq}+1, $#{$ontoReq} == 0 ? '' : 's');
        push @notes, [$msg, 'GseaProfile'];
        
    }
    if ($ofile) {
        my @urlparams = ( show => $ofile );
        push @urlparams, ( ticket => $ticket->ticket ) if ($ticket);
        my $link = &selfUrl({@urlparams}, $ofile, 'file butt');
        push @notes, ["Ouput is being directed to $link", "OutputTarget"];
    }
    if ($ontoMem) {
        @notes = ($notes[0]);
        push @notes, [ "A list is being generated of all Sublist members that are assigned to one or more ontology members", "AnalysisMode", ];
        
    }
    if ($#notes > -1) {
        if ($nocgi) {
            map { $_->[0] = "  $_->[0]" } @notes;
        }
        &inline_msg("<b class='hilite'>Notes:</b><br />");
        foreach my $ndat (@notes) {
            &inline_msg(@{$ndat});
        }
        &inline_msg("");
    }
}

sub ONTO_MEMBERS {
    my ($list) = @_;
    unless ($sc) {
        $args->err("Can not find set membership without a SetCollection");
        return 0;
    }
    my $ref     = $sc->set('ReferenceSet');
    my @useSets = $ref->connected_sets();
    my @unknown;
    foreach my $setObj (@useSets) {
        my ($terms) = $setObj->trim_unknown( $ontoReq );
        if ($#{$terms} == -1) {
            push @unknown, $setObj;
            next;
        }
        print $outfh $setObj->full_html_summary
            ( -what  => 'term',
              -terms => $terms, );
        print $outfh "<br />\n";
        print $outfh $setObj->full_html_summary
            ( -what       => 'accession',
              -queries    => $list,
              -terms      => $terms,
              -N          => $args->val(qw(N)),
              -rank       => 1, );
    }
    if ($#unknown == $#useSets) {
        $args->msg("Requested terms could not be found in any of the sets:",
                   "Sets : ".join(', ', map { $_->name() } @unknown),
                   "Terms: ".join(', ', @{$ontoReq}));
    }
    return 1;
}

sub DETERMINE_SET {
    my $req = $_[0];
    return $set if ($set);
    my $ns   = $refer;
    unless ($req) {
        $req = $requests[0] || {};
        $ns  = $req->{param}{NAMESPACE};
    }
    return $req->{param}{SET} if ($req && $req->{param}{SET});
    my $list = $req->{list};
    if ($req->{orig}) {
        $list = $req->{orig};
        # Even if we have transformed the list, we should always pick
        # the set based on the original ID
        $ns = $req->{param}{SOURCE_NS} || $ns;
    }
    my $N    = $#{$list || []} + 1;
    return undef unless ($N);

    my $size   = $N > 25 ? 25 : $N;
    my @sample = sort { rand(1) <=> rand(1) } @{$list};
    @sample    = splice(@sample, 0, $size);
    # my @sample = map { $list->[$_] } (0..($size-1));
    my $ns2 = 'set';
    $ns2 = 'BAAD' if ($sample[0] && $sample[0] =~ /^BrAr:/i);
    
    my $rows = $ad->convert
        ( -id => \@sample, -ns1 => $ns, -ns2 => $ns2, -min => $transMat,
          -nonull => 1,
          -cols => ['term_out','term_in']);
    my %sets;
    foreach my $row (@{$rows}) {
        my ($set, $id) =  @{$row};
        # Ignore UniGene IDs:
        next if ($set =~ /^[A-Z][a-z]\.\d+$/);
        # Ignore ListTracker as references:
        next if ($set =~ /^(ListTracker|WikiPathways):/);
        $sets{$set}{$id} = 1 if ($set && $id);
    }
    my @matches;
    while (my ($set, $hash) = each %sets) {
        my @hits = keys %{$hash};
        my $hnum = $#hits + 1;
        next unless ($set && $hnum);
        next if ($set =~ /^ListTracker/ && $hnum / $size < 0.9);
        push @matches, [ $set, $hnum ];
    }
    if ($#matches == -1) {
        my $msg = "Unable to find any <span class='ref'>Reference Sets</span> associated with the queries in your <span class='sublist'>Sublist</span>";
        if (my $tfile = $args->val(qw(FILE))) {
            my $url = "$gasUrl?paramfile=/home/tilfordc/public_html/gseaListHelper.param&idlist=$tfile";
            $msg .= ".<br />Maybe you provided a list of gene symbols? If so, you can convert them to loci for <a href='$url'>human</a>, <a href='${url}&int=mus+musculus'>mouse</a> or <a href='${url}&int=rattus+norvegicus'>rat</a>.";
        }
        if ($bgnd) {
            &task_failed($msg);
        } else {
            &err($msg, 'FailureToGuessSet');
        }
        return undef;
    }
    @matches = sort { $b->[1] <=> $a->[1] || $a->[0] cmp $b->[0] } @matches;
    my $topMat = $matches[0][1];
    if ($#matches == 0 || ($topMat > $matches[1][1] && 
                           ($size == $N || 
                            ($topMat == $size && $usePerfect)))) {
        # There is only one set
        # OR the top set is better than the second set 
        #    AND 
        #        all terms have been tested
        #          OR all terms TESTED match AND -useperfectguess is true
        #
        $set = $matches[0][0];
        &msg("The <span class='ref'>Reference Set</span> <b>$set</b> will be used for analysis", 'GuessedReferenceSet');
        return $set;
    }
    my $mnum = $#matches + 1;
    if ($bgnd && $ticket) {
        my $options = "<img src='/biohtml/images/AnimArrow.gif' /><span class='hilite'>Please choose a set to use:</span><form action='$shortProg'><input type='hidden' name='issetname' value='1' /><input type='hidden' name='rerun' value='1' /><input type='hidden' name='ticket' value='".$ticket->ticket()."' />";
        foreach my $sdat (@matches) {
            my ($sn, $num) = @{$sdat};
            $options .= "<br /><input numMatch='$num' class='alert' type='submit' name='set' value='$sn' />&nbsp;[$num/$size]";
        }
        $options .= "</form>";
        $ticket->status('ActionNeeded', $options);
        return undef;
    }

    my $msg = "I attempted to guess the <span class='ref'>Reference Set</span> for your <span class='sublist'>Sublist</span> by inspecting $size of its members. I found $mnum sets that might be appropriate - please <b><a href='#setchoice'>pick one of them</a></b> to use for the analysis. Think carefully about which one to use - picking the wrong reference can cause grossly misleading statistics!";
    &msg($msg, 'ChooseReferenceSet', 'alert');
    if ($nocgi) {
        $args->msg("  Identified sets:\n". join
                   ("\n", map { "   $_->[0] ($_->[1]/$size)" } @matches));
        return undef;
    }
    $setHTML = &help('ReferenceSet')."<b><a name='setchoice' />Please choose a Reference Set:</b><br />These sets match your <span class='sublist'>Sublist</span> - Choose one to launch the analysis.<br />(Sets with low numbers of matching members are unlikely to be appropriate)<br /><table<tbody>\n";
    $setHTML .= "<tr><th>Set</th><td># Matches</td></tr>\n";
    foreach my $sdat (@matches) {
        my ($sn, $num) = @{$sdat};
         $setHTML .= sprintf
             ("<tr><th><input type='submit' name='set' value='%s' /></th>".
              "<td>%d / %d</td></tr>\n", $sn, $num, $size);
    }
    $setHTML .= "</table></tbody>\n";
    return undef;
}

sub help { return $args->tiddly_link(@_); }

sub err {
    my $msg = shift;
    my $fh;
    if (ref($msg)) { $fh = $msg; $msg = shift }
    my ($key, $fatal) = @_;
    if ($nocgi) {
        $msg =~ s/\<.+?\>//g;
    } elsif ($key) {
        $msg = &help($key) . $msg;
    }
    $args->err($msg);
    if ($fatal) {
        my $notReallyFatal = ($fatal =~ /TICK/i) ? 1 : 0;
        if ($ticket && !$isTemp) {
            my $rep = ($fatal =~ /BR/i) ? '<br />' : ' ';
            $msg =~ s/[\n\r]+/$rep/g;
            my $tw = $notReallyFatal ? $key : 'TaskFailed';
            &task_failed($msg, $tw);
        }
        exit 99 unless ($notReallyFatal);
    }
}

sub msg {
    if ($nocgi) {
        my $msg = shift;
        $msg =~ s/\<.+?\>//g; # Strip HTML
        $args->msg($msg);
    } else {
        print &_html_msg_fmt( @_ );
    }
}

sub inline_msg {
    if ($format =~ /html/i) {
        print $outfh &_html_msg_fmt( @_ );
    } else {
        &msg(@_);
    }
}

sub inline_txt {
    my $msg = shift;
    if ($format =~ /tsv/i) {
        # Do not contaminate output file!
        return;
    } elsif ($format =~ /html/i) {
        print $outfh "$msg\n";
    } else {
        $msg =~ s/\<.+?\>//g; # Strip HTML
        print $outfh "$msg\n";
    }
}

sub _html_msg_fmt {
    my ($msg, $key, $class) = @_;
    $msg = &help($key) . $msg if ($key);
    $class = $class ? "$class twmsg" : 'twmsg';
    return "<span class='unsized $class'>$msg</span><br />\n";
}

# &tick_link($ticket)
sub tick_link {
    my ($ticket, $name) = @_;
    return "<i>No ticket found</i>" unless ($ticket);
    my $tick  = $ticket->ticket();
    my $path  = &short_ticket_path( $ticket );
    $name   ||= $tick;
    my $tlink = &selfUrl({ticket => $path}, $name, 'tick butt');
    return $tlink;
}

sub short_ticket_path {
    my $ticket = shift;
    return "" unless ($ticket);
    my $tick  = $ticket->ticket();
    my $path  = $ticket->path();
    if ($path =~ /^\Q$tDir\E\//) {
        # The ticket is in the default spot
        return $tick;
    }
    return $path;
}

sub task_failed {
    my ($bits, $tw, $isCommon) = @_;
    $tw   ||= 'TaskFailed';
    my $msg = ref($bits) ? join(" ", @{$bits}) : $bits;
    $ticket->status($tw, $msg) if ($ticket);
    &err($msg, $tw) unless ($bgnd);
    $msg =~ s/\<.+?\>//g;
    my @st = &stack_trace();
    map { s/^\s+// } @st;
    my @logs = ("", "- " x 35, `date`,
                "PID    = $$",
                "User   = $ldap",
                "Error  = $msg [$tw] ". join(' < ', @st ));
    push @logs, "Ticket = ".$ticket->path() if ($ticket);
    &log(@logs);
    unless ($#failedListGuess == -1) {
        foreach my $dat (@failedListGuess) {
            my ($lname, $ns, $samp) = @{$dat};
            $msg .= "\nFailed NS guess ($ns) for $lname. ID Sample:\n";
            $msg .= join("", map { "  $_\n" } @{$samp});
        }
    }
    $tw .= " Again" if ($isCommon);
    $args->send_mail($msg, "GSEA $tw", $args->val(qw(ERRORMAIL)), undef, undef, 1);
}

sub log {
    if (open(LOG, ">>$dumpfile")) {
        foreach my $line (@_) {
            print LOG "$line\n";
        }
        close LOG;
    }
}
