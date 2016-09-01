#!/stf/biobin/perl -w

BEGIN {
    # Needed to make my libraries available to Perl64:
    # use lib '/stf/biocgi/tilfordc/released';
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

=head1 Bulk Updating

 /stf/biocgi/tilfordc/working/maptracker/updateChemBioHopperData.sh

=cut

my $VERSION = 
    ' $Id$ ';

use strict;
use BMS::Branch;
use BMS::MapTracker::AccessDenorm;
use BMS::MapTracker::SciTegicSmiles;
use BMS::BmsArgumentParser;
use BMS::MapTracker::GenAccService;
use BMS::Utilities::FileUtilities;
use BMS::Utilities::Escape;
use BMS::ExcelHelper;

my (%allPubMeds);

my $urlEsc = {
    ' ' => '%20',
    '&' => '%26',
    '<' => '%3C',
    '>' => '%3E',
    '"' => '%22',
    '#' => '%23',
    '$' => '%24',
    "'" => '%27',
    '+' => '%2B',
    ',' => '%2C',
    '/' => '%2F',
    ':' => '%3A',
};
my $htmlEsc = {
    "'" => '&apos;',
    '"' => '&quot;',
    '<' => '&lt;',
    '>' => '&gt;',
};

my $fBlock = 30;

my $sMeta  = {
    Homologene            => [20],
    Locus                 => [10],
    Protein               => [10],
    Symbol                => [8],
    Species               => [15],
    Compound              => [15],
    Queries               => [15],
    Authorities           => [20],
    PubMed                => [10],
    MTID                  => [15],
    TargetDescription     => [20],
    CompoundDescription   => [20],
    SMILES                => [25],
    Date                  => [10],
    Count                 => [5],
    Title                 => [100],
    'Score'               => [5],
    'Ki'                  => [5],
    'IC50'                => [5],
    'EC50'                => [5],
    '%KD'                 => [5],
    '% Inhibition'        => [5],
    '%Ctrl 1uM'           => [5],
    'Generic'             => [5],
};

my $args = BMS::BmsArgumentParser->new
    ( -nocgi     => $ENV{HTTP_HOST} ? 0 : 1,
      -format    => 0,
      -keepnull  => 0,
      -minimize  => 1,
      -potency   => 5,
      -limit     => 50,
      -fork      => 20,
      -expand    => 10,
      -ageall    => '1 Oct 2013',
      -testmode  => 0,
      -errmail   => 'charles.tilford@bms.com',
      -tiddlywiki => 'ChemBioHopper', );

my $globalEh;

my $ldap    = $args->ldap() || "";
my $brkPt   = '<wbr>';
my $brkLen  = length($brkPt);
my $strImg  = "/biohtml/images/caffeineTiny.png";
my $seqImg  = "/biohtml/images/helixTiny.png";
my $moldy   = "http://cheminfo.pri.bms.com:8080/cgi/moldy_na.cgi";
my $chemFmt = 'http://research.pri.bms.com:8080/CSRS/services/lookup/image/SMILES/%s?param=w:150&param=h:150';
my $doBench = $args->val(qw(showbranch dobranch benchmark bench));
$doBench    = $ldap eq 'tilfordc' ? 0.001 : 0 unless (defined $doBench);
my $nocgi   = $args->{NOCGI};
my $vb      = !$nocgi || $args->{VERBOSE};
my $limit   = $args->{LIMIT} || 0;
my $forkNum = $args->val(qw(populate fork));
my $expLim  = $args->{EXPAND} || 0;
my $taxa    = $args->val(qw(taxa species)) || '';
my $checkInv = $args->val(qw(inventory));
my $isBeta  = ($0 =~ /working/) ? 1 : 0;
my $dodb    = $args->{DEBUG};
my $noNull  = $args->{KEEPUNDEF} ? 0 : 1;
my $debug   = BMS::Branch->new( -format => $nocgi ? 'text' : 'html');
my $truncLen = 80;
my $clearDiv = "<div style='clear:both'></div>\n";
my $longWait = "<img src='/biohtml/images/animated_clock.gif' />";
my $format   = lc($args->{FORMAT} || ($nocgi ? 'tsv' : 'html'));
my $escObj   = BMS::Utilities::Escape->new();

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

my $outFH = *STDOUT;

my $outFile = $args->val('htmlout');
if ($outFile) {
    open(OUTF, ">$outFile") || $args->death
        ("Failed to write output", $outFile, $!);
    $outFH = *OUTF;
}

my @preferredSpecies =
    ('Homo sapiens', 'Mus musculus', 'Rattus norvegicus', 
     'Canis lupus familiaris', 'Macaca mulatta');
my %prefSpec = map { $preferredSpecies[$_] => $_ + 1} (0..$#preferredSpecies);

my @gasCom = ( -age      => $args->val(qw(age ageall)),
               -cloudage => $args->val(qw(cloudage ageall)),
               -ageall   => $args->val(qw(ageall)),
               -format   => 'tsv',
               -fork     => $forkNum,
               -verbose  => 0,
               -nocgi     => 1,
               -keepcomma => 1,
               -warn     => 0,
               -quiet    => 1,
               -scramble => 1);
my $gasDir = "/stf/biohtml/tmp/ChemBioHopper/PID$$";
$args->assure_dir($gasDir);
system("rm -rf $gasDir/*");


if ($format =~ /xml/) {
    $format = 'xml';
} elsif ($format =~ /tsv/) {
    $format = 'tsv';
} else {
    $format = 'html';
}


my @chemTypes =
    ('was assayed with','is antagonized by','is agonized by','is inhibited by',
     'is functionally antagonized by','is functionally agonized by',
     'has substrate','is the source for');
my %targCount;

my $l10f = ($format eq 'html') ? \&log10HTMLfmt : \&log10fmt;

my $valFormats = {
    Ki    => [ $l10f,   5,  'log',  'KI'     ],
    EC50  => [ $l10f,   5,  'log',  'EC50'   ],
    IC50  => [ $l10f,   5,  'log',  'IC50'   ],
    ''    => [ \&defaultfmt, '', '',     ''       ],
    '%KD' => [ \&percfmt,    50, 'perc', 'PERCKD' ],
    '% Inhibition' => [ \&percfmt,    50, 'perc', 'PERCINHIB' ],
    '%Ctrl 1uM' => [ \&invpercfmt,    10, 'invperc', 'PERCCTRL' ],
};
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

%filters = () if ($args->{NOFILTERS});

my @convCom = ( -warn => $args->{WARN}, -cloudwarn => $args->{CLOUDWARN});

my (%taxToks, %tokCount, %notes, %go2kid, @badErr, %metaCache);
my $globalIter = 0;
my $xmlInd   = 0;
my @taxCols  = ('#f00', '#000','#33f','#3c0','#f60','#f39');
my %priTargs = ( SMI => ['AP'], SEQ => ['AR']);

my %goodGO =
    ( 
      'GO:0003707' => 'NHR',
      'GO:0004672' => 'Kinase',
      'GO:0004721' => 'Phosph',
      'GO:0004842' => 'UbLig',
      'GO:0004888' => 'TMR',
      'GO:0004930' => 'GPCR',
      'GO:0005216' => 'IonCh',
      'GO:0005576' => 'ExCell',
      'GO:0006986' => 'PrtFold',
      'GO:0008233' => 'Pept',
      'GO:0016651' => 'OxRed',
      'GO:0031012' => 'ECM',
      );

my @scoreClass = 
    ( [ 1.0 , 'sc10', "100%"],
      [  .9 , 'sc09', "90 - 99%"],
      [  .8 , 'sc08', "80 - 89%"],
      [  .5 , 'sc05', "50 - 79%"],
      [  .3 , 'sc03', "30 - 49%"],
      [  .01, 'sc01', "1 - 29%"],
      [  -99, 'scUn', "Unknown confidence"]);

my %typeName = ( Q => 'Query', K => 'Isomer', P => 'Non-isomer' );

my (%seenGO, %seenEC, %seenSC, %filterCount, @xmlAssays);

&DOC_START();

my $ageReq     = $args->val(qw(agereq agerequest)) || "";
$ageReq        = 0.001 if ($ageReq eq '0');

my $age        = $ageReq || $args->val(qw(age ageall));
my $cloudAge   = $ageReq || $args->val(qw(cloudage ageall));
my $goAge      = $args->val(qw(goage age ageall));

my $ad  = BMS::MapTracker::AccessDenorm->new( -noenv => 1,
                                              -age   => $age );

$ad->cloud_age($cloudAge);
my $chemAge    = $ad->standardize_age( $args->{CHEMAGE} || $age);

my $mt  = $ad->tracker();
my $lh  = BMS::MapTracker::LoadHelper->new
    ( -username  => 'SciTegic', 
      -testmode  => $args->{TESTMODE},
      -basedir   => 'rapidload');
my $sts = BMS::MapTracker::SciTegicSmiles->new
    ( -loader  => $lh,
      -verbose => $vb, );

my $unOff   = "<span class='note'>*</span>";
my $noLoc   = "<span class='note'>NoLoc</a>";
my $unDef   = "<span class='null'>?</span>";
my $multLoc = "<span class='note'>+N</span>";
my $multOrt = "<span class='note'>&dagger;</span>";
my $hopLink = "<a ns='%s' class='hop' href='chemBioHopper.pl?id=%s'>%s</a>";
my $hopXtra = "<a ns='%s' title='%s' class='hop %s' href='chemBioHopper.pl?id=%s'>%s</a>";
my $mkeyFrm = "<a name='mkey' class='hide mkey'>[%s]</a>";
my $elipses = "<a name='elips' onclick='return togNext(this, 1)' class='elips'>...</a>";
my $strcFrm = "<img class='popStr' onclick='togNext(this)' src='$strImg' />".
    "<img class='hide struct' name='struct' src='$chemFmt' />\n";
#    "<img class='hide struct' name='struct' src='$moldy?smiles=%s' />\n";
my $seqFrm = "<img class='popStr' onclick='togNext(this)' src='$seqImg' />".
    "<pre class='hide seq' name='seq'>%s</pre>\n";
my $bcrPri  = join(''," <th%s>",$hopLink,$strcFrm,"%s</th>\n");
my $bcrSeq  = join(''," <th>",$hopLink,$seqFrm,"</th>\n");
my $bcrSec  = join(''," <td>",$hopLink,$strcFrm,"</td>\n");

my @idKeys  = qw(queries id ids query term terms);
my %queries;
my %nsNameCache;
my $qCount = 0;
foreach my $key (@idKeys) {
    foreach my $val (split(/\s*[\n\r\t\,]+\s*/, $args->{uc($key)} || '')) {
        $queries{$val} ||= ++$qCount;
    }
}

if ($taxa) {
    my @taxae = $mt->get_taxa($taxa);
    if ($#taxae == 0) {
        $taxa = $taxae[0]->name;
    } elsif ($#taxae == -1) {
        &msg("UnknownTaxa", "I could not find a species '$taxa'",
             "Please correct the species name or try again...");
        $qCount = 0;
    } else {
        &msg("MultipleTaxa", "Your taxa request '$taxa' maps to multiple species",
             "Please choose the one you wish to use:", map { $_->name } @taxae);
        $qCount = 0;
    }
}
my $idText = join
    ("\n", sort {$queries{$a} <=> $queries{$b}} keys %queries) || "";

if ($qCount && !$args->val(qw(norun))) {
    &process();
    if ($format eq 'html') {
        print $outFH "<hr />";
        my $file = sprintf("/stf/biohtml/tmp/ChemBioHopper/CBH-%d-%d.param",
                           $$, time);
        $args->assure_dir($file, 'isFile');
        if (open(PARAM, ">$file")) {
            map { $args->blockquote($_, 'QUOTEDBLOCK') } @idKeys;
            my $com = sprintf("ChemBioHopper query by %s on %s", $ldap,`date`);
            print PARAM $args->to_text
                ( -comment => $com,
                  -skip => ['paramfile','valuefile','nocgi']);
            close PARAM;
        } else {
            $args->err("Failed to create parameter file", $file, $!);
            $file = "";
        }
        print"<a class='butt' href='chemBioHopper.pl?norun=1&valuefile=$file'>New Search</a>\n";
        print $outFH "<a class='butt' href='chemBioHopper.pl?valuefile=$file'>Link to Here</a>\n" if ($file);
        print $outFH "<br />";
    }
} else {
    &HTML_FORM();
}

&DOC_END;

sub process {
    my %ids;
    my @pfMsg;
    foreach my $tag (sort { uc($a) cmp uc($b) } keys %filters) {
        my $fv = $filters{$tag};
        my $vf = $valFormats->{$tag || ''};
        next unless ($fv && $vf);
        push @pfMsg, sprintf("%s : %s", &showtag($tag), &{$vf->[0]}( $fv ));
    }
    &msg("PotencyFilter", "Potency filters applied:", @pfMsg)
        unless ($#pfMsg == -1);
    &msg("LimitFilter", "Limiting number of hits per query to $limit")
        if ($limit);
    &msg("TaxaFilter", "Limiting results to $taxa") if ($taxa);
    foreach my $id (sort { $queries{$a} <=> $queries{$b} } keys %queries) {
        my @seqs = $mt->get_seq( -id => $id, -defined  => 1, -nocreate => 1 );

        my $class;
        foreach my $seq (@seqs) {
            # print $outFH "<pre>".$seq->to_text()."</pre>";
            if ($seq->is_class('CHEMICAL') || $seq->is_class('RNAI')) {
                $class = 'Chemical';
            } elsif ($seq->is_class('Gene Symbol')) {
                $class = 'Biological';
            } elsif ($seq->is_class('BIOMOLECULE') ||
                     $seq->is_class('LOCUS') ||
                     $seq->is_class('HOMOLOGENE') ||
                     $seq->is_class('GO')) {
                $class = 'Biological';
            } elsif ($seq->is_class('COLLECTION')) {

            }
            last if ($class);
        }
        $class ||= 'Unknown';
        push @{$ids{$class}}, $id;
    }

    &msg("BmsInventory", "Compound results will be checked for availability in BMS inventory. You should verify that the available isomers meet your needs, as they may not correspond to the isomers in the reported activities.")
        if ($checkInv && exists $ids{Biological});


    if ($format eq 'html') {
        print $outFH "<table class='tab'><caption>Toggle Verbose Information</caption><tbody>\n";
        print $outFH " <tr>\n";
        my @bits =
            ("showall(\"struct\",2)'>Structures",
             "showall(\"seq\",2)'>Sequences",
             "showall(\"mkey\",2)'>Variant Superscripts",
             "toggleTrunc()'>Descriptions");
        print $outFH join('', map { "  <td><span class='butt' onclick='$_</span></td>\n" }
                   @bits);
        print $outFH " </tr>\n";
        print $outFH "</tbody></table>\n";
    }
   
        
    foreach my $class (sort keys %ids) {
        my $meth; eval("\$meth = \\&process_$class");
        &{$meth}( $ids{$class} ) if ($meth);
    }
    &record_xml_assays();
    print $outFH &pubmed_html_summary();
    print $outFH &all_keys();
}

sub record_xml_assays() {
    return "" unless ($format eq 'xml');
    my $pad = ("  " x $xmlInd) || "";
    print $outFH "$pad<assays>\n";
    my $pre = "$pad  <assay ";
    my $pro = " />\n";
    foreach my $attr (@xmlAssays) {
        print $outFH $pre . &hash2attr($attr).$pro;
    }
    print $outFH "$pad</assays>\n";
}

sub process_Biological {
    my ($ids) = @_;

    my $pad = ("  " x $xmlInd) || '';
    if ($format eq 'html') {
        print $outFH "<h2>Targets</h2>\n";
    }
    my (@unk, @locs, %notLoci);
    my @tfilt = $taxa ? ( -int => $taxa, -intns => 'TAX') : ();
    foreach my $id (@{$ids}) {
        my $ns = $ad->guess_namespace_careful( $id );
        unless ($ns) {
            push @unk, $id;
            next;
        }
        $notes{"<span class='query'>Abc</span>"} ||= "Highlight indicating text you used as a query";
        ($id) = $ad->standardize_id($id, $ns);
        if ($ns eq 'LL') {
            # Query is already a locus link
            push @locs, [ $id, $ns, 1, $id, $ns];
        } else {
            push @{$notLoci{$ns}}, $id;
        }
    }
    my @nlns = keys %notLoci;
    my %overLim;
    unless ($#nlns == -1) {
        # There are some IDs we need to map to loci
        my $nlFile = "$gasDir/IDtoLL.tsv";
        $ad->bench_start("Map Query to LL");
        while (my ($ns, $ids) = each %notLoci) {
            unlink($nlFile) if (-e $nlFile);
            my $nlRows = &forked_data
                ( -id         => $ids, 
                  -ns1        => $ns, 
                  -ns2        => 'LL',
                  -distinct   => 1,
                  -nullscore  => -1,
                  @tfilt,
                  -age        => ($ns eq 'GO') ? $goAge : undef,
                  -ignorecase => ($ns eq 'SYM') ? 1 : 0,
                  -cols       => [ qw(termin termout matched) ],
                  -output     => $nlFile,
                  @convCom);
            if (-e $nlFile) {
                chmod(0666, $nlFile);
            } else {
                push @badErr, $args->err("Failed to find conversion file",
                                         $nlFile, join(',', @{$ids}));
                next;
            }
            my %qryResolv; map { $qryResolv{uc($_)}{$_} = 1;
                                 $qryResolv{$_}{$_}     = 1; } @{$ids};
            my %mapped;
            foreach my $row (@{$nlRows}) {
                my ($tin, $ll, $sc) = @{$row};
                my $idH = $qryResolv{$tin} || $qryResolv{uc($tin)};
                unless ($idH) {
                    push @badErr, $args->err("Programming error - failed to resolve '$tin' back to original user query", join(',', @{$ids}));
                    next;
                }
                my @idA = sort keys %{$idH};
                if ($#idA == -1) {
                    push @badErr, $args->err("Programming error (Mk2) - failed to resolve '$tin' back to original user query", join(',', @{$ids}));
                    next;
                } elsif ($#idA != 0) {
                    $args->msg("I have (arbitrarily) chosen '$idA[0]' as the case to use for your queries [".join(' + ', @idA)."]");
                    next;
                }
                my $targ = $mapped{$idA[0]} ||= {};
                if ($ll) {
                    $targ->{$ll} = $sc if (!defined $targ->{$ll} ||
                                           $targ->{$ll} < $sc);
                }
            }
            while (my ($id, $ml) = each %mapped) {
                my @sll = sort { $ml->{$b} <=> $ml->{$a}
                                 || $a cmp $b } keys %{$ml};
                my $num = $#sll + 1;
                unless ($num) {
                    # Failed to map
                    push @locs, [ $id, $ns, 1, $id, $ns];
                    next;
                }
                my @rows = map { [$_, 'LL', $ml->{$_}, $id, $ns ] } @sll;
                if ($expLim && $num > $expLim) {
                    # We are going to need to trim these down, but will
                    # do the requisit searches in bulk
                    if ($overLim{$id}) {
                        push @badErr, $args->err("Overlimit sees two namespaces for '$id' - $ns + $overLim{$id}[0]");
                    } else {
                        $overLim{$id} = [ $ns, \@rows ];
                    }
                } else {
                    push @locs, @rows;
                }
            }
        }
        $ad->bench_end("Map Query to LL");
    }
    # die $args->branch(\%overLim);
    my @olids = sort keys %overLim;
    my $olnum = $#olids + 1;
    if ($olnum) {
        $ad->bench_start("Prune LL");
        my $msg = "Some of your queries recover many LocusLink entries. Each of these queries will only report the 'top' $expLim entries (as measured by number of potent compounds), as per your expansion limit filter<ul>\n";
        my %llH;
        foreach my $id (@olids) {
            my ($ns, $rows) = @{$overLim{$id}};
            my $num = $#{$rows} + 1;
            $msg .= sprintf("<li><span class='query'>%s <span class='ns'>[%s]</span></span> = <b>%d Loci</b>", $id, $ns, $num);
            $msg .= " $longWait <span class='frantic'>may take a while!</span>" if ($num > $expLim * 5);
            $msg .= "</li>\n";
            map { $llH{$_->[0]} = 1 } @{$rows};
        }
        $msg .= "</ul>\n";
        &print_msg($msg, 'emphasis');

        # First convert loci to each primary target class
        my @sll = keys %llH;
        my $pcFile = "$gasDir/Prot-Cmpd.tsv";

        my %nc = map { $_ => {} } @sll;
        while (my ($cmpdns, $ptnss) = each %priTargs) {
            foreach my $ptns (@{$ptnss}) {
                $ad->bench_start("Prune LL - Fork Protein");
                my $lpFile = "$gasDir/LL-$ptns-1.tsv";
                unlink($lpFile) if (-e $lpFile);
                my $prots = &forked_data
                    ( -id         => \@sll, 
                      -ns1        => 'LL', 
                      -ns2        => $ptns,
                      -nonull     => 1,
                      @convCom,
                      -columns    => ['term_in','term_out'], 
                      -distinct   => 1,
                      -output     => $lpFile, );
                $ad->bench_end("Prune LL - Fork Protein");
                if (-e $lpFile) {
                    chmod(0666, $lpFile);
                } else {
                    next;
                }
                next unless ($prots);
                my %p2l;
                map { push @{$p2l{$_->[1]}}, $_->[0] } @{$prots};
                my @pq = keys %p2l;

                # Then convert those targets to compounds. Using
                # AccessDenorm for this generates A LOT of unhelpful
                # rows. This is because we are requesting DirectOnly
                # entries, but we will still generate the hits for all the
                # non-direct hits (via cloud expansion). For this reason,
                # we will check MapTracker directly, and only consider
                # entries that have at least one compound linked to them:

                my @useful;
                map { push @useful, $_ if (&has_direct_compound($_)) } @pq;
                next if ($#useful == -1);

                unlink($pcFile) if (-e $pcFile);
                $ad->bench_start("Prune LL - Fork Compound");
                my $filt = &forked_data
                    ( -id         => \@useful,
                      -directonly => 1,
                      -ns1        => $ptns,
                      -ns2        => $cmpdns,
                      -age        => $chemAge,
                      -nonull     => 1,
                      -cols       => [qw(termin termout auth matched)],
                      @convCom,
                      -output     => $pcFile,);
                $ad->bench_end("Prune LL - Fork Compound");
                if (-e $pcFile) {
                    chmod(0666, $pcFile);
                } else {
                    push @badErr, $args->err("Failed to find conversion file",
                                             $pcFile, join(',', @useful));
                    next;
                }
                
                foreach my $row (@{$filt || []}) {
                    my ($targ, $cmpd, $auth, $sc) = @{$row};
                    my $skipIt = 0;
                    my @abits = split(/ < /, $auth);
                    my ($cAuth, $tag, $pmids) = &parse_author( $abits[0] );
                    if (defined $sc) {
                        if (my $fc = $filters{$tag}) {
                            $skipIt = $tag if ($sc < $fc);
                        }
                    } else {
                        $skipIt = 'Ignore Null' if ($noNull);
                    }
                    if ($skipIt) {
                        $filterCount{$skipIt}++;
                        # $pFc{$targ}{$cmpd} = 1;
                        # $fnum++;
                        next;
                    }
                    map { $nc{$_}{$cmpd} = 1 } @{$p2l{$targ}}
                }
            }
        }
        # De-hashify NC
        while (my ($ll, $cH) = each %nc) {
            my @u = keys %{$cH};
            $nc{$ll} = $#u + 1;
        }
        # We now have a lookup has that lets us calculate how many compounds
        # would be associated with each locus
        $msg = "Filtering complete.<ul>\n";
        foreach my $id (@olids) {
            my ($ns, $rows) = @{$overLim{$id}};
            my @filt = sort { $nc{$b->[0]} <=> $nc{$a->[0]} ||
                                  $b->[0] cmp $a->[0] } @{$rows};
            @filt = splice(@filt, 0, $expLim);
            push @locs, @filt;
            my ($min, $max) = ($nc{$filt[-1][0]}, $nc{$filt[0][0]});
            my $range = ($min == $max) ? $min : "between $min and $max";
            $msg .= sprintf("<li><span class='query'>%s <span class='ns'>[%s]</span></span> using <b>%d Loci</b> with <b>%s</b> associated compound%s</li>\n",
                            $id, $ns, $#filt + 1, $range, $range eq '1' ? '' : 's');
        }
        $msg .= "</ul>\n";
        &print_msg($msg, 'ntxt');
        $ad->bench_end("Prune LL");
    }

    # Organize our queries by namespace
    $ad->bench_start("Organize loci");
    my %byns; map { push @{$byns{$_->[1]}}, $_ } @locs;
    my (%loci, %loc2targ, %nsnm);
    while (my ($lns, $rows) = each %byns) {
        $ad->bench_start("Organize loci - Basic Metadata");
        my %uH = map { $_->[0] => 1 } @{$rows};
        my @uLoc = keys %uH;
        map { $loci{$_} ||= { loc => $_, ns => $lns, via => {} } } @uLoc;
        
        my $lmetFile = "$gasDir/$lns-Meta.tsv";
        my $lmRows = &forked_data
            ( -ids        => \@uLoc, 
              -ns1        => $lns, 
              -mode       => 'simple',
              -cols       => [ qw(termin sym desc) ],
              -output     => $lmetFile,
              @convCom);
        foreach my $row (@{$lmRows}) {
            my ($ll, $sym, $desc) = @{$row};
            my $targ = $loci{$ll};
            $targ->{sym}  = $sym  || "";
            $targ->{desc} = $desc || "";
            # $targ->{taxa} = $tax  || "";
        }

        my @recNode = q(TAX);
        push @recNode, qw(RSR RSP)  if ($format eq 'html');
        push @recNode, qw(SYM ORTH HG)
            if ($format eq 'xml' || $format eq 'html');
        my %recover = map { $_ => 1 } @recNode;
        foreach my $row (@{$rows}) {
            my ($ll, $lns, $sc, $id, $ns) = @{$row};
            my $targ = $loci{$ll};
            # Associate the original query with the normalized locus:
            my $via  = $targ->{via}{$id} ||= [ $ns, -2 ];
            $via->[1] = $sc if ($via->[1] < $sc);
            $recover{$lns} = 1;
        }
        map { delete $recover{$_} } qw(GO);
        $ad->bench_end("Organize loci - Basic Metadata");

        # Get related IDs:
        $ad->bench_start("Organize loci - Related Objects");
        my @rns = keys %recover;
        # map { $recover{$_} = $ad->namespace_name($_) } @rns;
        my $lothFile = "$gasDir/$lns-Related.tsv";
        my $othRows = &forked_data
            ( -ids        => \@uLoc, 
              -ns1        => $lns, 
              -ns2        => \@rns,
              -cols       => [ qw(termin termout nsout matched) ],
              -nullsc     => -1,
              -nonull     => 1,
              -usetoken   => 1,
              -output     => $lothFile,
              @convCom);
        chmod(0666, $lothFile);
        foreach my $row (@{$othRows}) {
            my ($ll, $other, $rns, $sc) = @{$row};
            my $targ = $loci{$ll}{other}{$rns} ||= {};
            $targ->{$other} = $sc if (!$targ->{$other} ||
                                      $targ->{$other} < $sc);
        }
        map { $nsnm{$_} ||= $ad->namespace_name($_) } @rns;

        $ad->bench_end("Organize loci - Related Objects");

        my $gos = &go_terms(\@uLoc, $lns, undef, 'bulk');
        while (my ($ll, $goH) = each %{$gos}) {
            $loci{$ll}{gos} = $goH;
        }

        $ad->bench_start("Organize loci - Compounds");
        while (my ($cmpdns, $ptnss) = each %priTargs) {
            my %l2p;
            foreach my $ptns (@{$ptnss}) {
                my $l2pFile  = "$gasDir/$lns-$ptns.tsv";
                unlink($l2pFile) if (-e $l2pFile);
                my $ldRows = &forked_data
                    ( -ids        => \@uLoc, 
                      -ns1        => $lns,
                      -ns2        => $ptns,
                      -nonull     => 1,
                      -nullsc     => -1,
                      -cols       => [ qw(termin termout matched) ],
                      -output     => $l2pFile,
                      @convCom);
                if (-e $l2pFile) {
                    chmod(0666, $l2pFile);
                } else {
                    next;
                }
                foreach my $row (@{$ldRows}) {
                    my ($ll, $prot, $sc) = @{$row};
                    $l2p{$ll}{$prot} = $sc if (!defined $l2p{$ll}{$prot} ||
                                               $l2p{$ll}{$prot} < $sc);
                }
                # If the locus is a target itself, make sure it is included:
                map {$l2p{$_}{$_} = 1 } @uLoc if
                    ($ad->is_namespace($lns, $ptns));
                while (my ($in, $scHash) = each %l2p) {
                    while (my ($out, $sc) = each %{$scHash}) {
                        if (my $lt = $loc2targ{$in}{$out}) {
                            $lt->{cns}{$cmpdns} = 1;
                            $lt->{sc} = $sc if ($lt->{sc} < $sc);
                        } else {
                            my ($gns) = $ad->guess_namespace_careful
                                ($out, $ptns);
                            $loc2targ{$in}{$out} = {
                                tns => $ptns,
                                gns => $gns, 
                                sc  => $sc,
                                cns => { $cmpdns => 1 },
                            };
                        }
                    }
                }
            }
        }
        $ad->bench_end("Organize loci - Compounds");
    }
    $ad->bench_end("Organize loci");

    my %targ2chem;
    if ($format eq 'xml') {
        print $outFH "$pad<targets>\n";
        $xmlInd++; $pad .= "  ";
    }
    my @locObjects = map { $loci{$_} } &fancySort([ keys %loci]);
    for my $lo (0..$#locObjects) {
        my $obj    = $locObjects[$lo];
        my $loc    = $obj->{loc};
        my $ns     = $obj->{ns};
        my $othDat = $obj->{other} || {};
        my $nsn    = $ad->namespace_name($ns);
        my $fnum   = 0;
        my %via;
        while (my ($id, $dat) = each %{$obj->{via}}) { $via{uc($id)} = $dat }
        my @tax = sort keys %{$othDat->{TAX} || {}};

        my $gos = $obj->{gos};
        my $tc  = &target_class($gos);

        if ($format eq 'html') {
            print $outFH "<p class='biologic'>\n";
            printf($outFH "<span class='pri locus%s'>%s</span> ".
                   "<span class='ns'>[%s]</span>\n", 
                   $via{uc($loc)} ? ' query' : '', $loc, $ns);

            printf($outFH " <span class='sym'>%s</span>", $obj->{sym})
                if ($obj->{sym});
            print $outFH " <span class='taxa'>".join(' / ', @tax)."</span>"
                unless ($#tax == -1);
            print $outFH " - <b class='desc'>$obj->{desc}</b><br />\n"
                if ($obj->{desc});
            print $outFH "</p>\n";

            print $outFH "<table class='tab'><tbody>\n";
            print $outFH "<tr><th>Target Class</th><td>$tc</td></tr>\n" if ($tc);
        } elsif ($format eq 'xml') {
            print $outFH "$pad<target ".&hash2attr({
                id      => $loc, 
                ns      => $ns,
                isquery => $via{uc($loc)} ? 1 : undef,
            }).">\n";
            print $outFH join('', map {"$pad  <taxa>".&esc_xml($_)."</taxa>\n"} @tax);
            print $outFH "$pad  <desc>".&esc_xml($obj->{desc})."</desc>\n" if ($obj->{desc});
        }
        my @terms;
        while (my ($go, $dat) = each %{$gos}) {
            my ($sc, $auths, $terms, $isInternal) = @{$dat};
            my $ucgo = uc($go);
            # Only show leaf terms
            next if ($isInternal && !$via{$ucgo});
            my ($auth) = $ad->simplify_authors(@{$auths});
            my $lnk = sprintf($hopLink, 'GO', $go, $go);
            $lnk = "<span class='query'>$lnk</span>" if ($via{$ucgo});
            $seenEC{$auth}++;
            
            my $gd = $ad->description( -id => $go, -ns => 'GO');
            if ($format eq 'xml') {
                push @terms, { id => $go, auth => $auth, desc => $gd };
            } elsif ($format eq 'html') {
                push @terms, sprintf
                    (" <tr><td>%s</td><td class='gotxt %s'>%s</td><td class='ntxt'>%s</td></tr>\n", $lnk, &scClass($sc), $auth, $gd);
            }
        }
        if ($#terms == -1) {
            print $outFH "<tr><th>Unclassified</th><td class='ntxt'>No GO terms found for locus</td></tr>\n" if ($format eq 'html');
        } elsif ($format eq 'html') {
            print $outFH "<tr><th>GO Terms</th><td><table class='tab'><tbody>\n <tr>".
                join('',map {"<th>$_</th>"} qw(Term EC Description))."</tr>\n".
                join('', sort @terms)."</tbody></table></td></tr>\n";
#join("<br />\n", sort @terms). "</td></tr>\n";
        } elsif ($format eq 'xml') {
            print $outFH "$pad  <goterms>\n";
            foreach my $attr (@terms) {
                print $outFH "$pad    <goterm ".&hash2attr($attr)." />\n";
            }
            print $outFH "$pad  </goterms>\n";
            print $outFH "$pad  <relateds>\n";
        }
        
        foreach my $rns (sort { $nsnm{$a} cmp $nsnm{$b} } keys %{$othDat}) {
            next if ($rns eq 'TAX');
            my $isOrth = ($rns eq 'ORTH') ? 1 : 0;
            my $others = $othDat->{$rns};
            my @oDat = sort { $others->{$b} <=> $others->{$a} ||
                                  uc($a) cmp uc($b) } keys %{$others};
            my (@text, %orths);
            foreach my $other (@oDat) {
                my $sc = $others->{$other};
                if ($format eq 'xml') {
                    my ($taxa) = $isOrth ? &taxa( $other, $ns) : ();
                    push @text, { id => $other, ns => $rns, score => $sc,
                                  taxa => $taxa,
                                  isquery => $via{uc($other)} ? 1 : undef };
                    next;
                }
                unless ($isOrth) {
                    push @text, sprintf
                        ("<span class='%s %s%s' title='%s'>%s</span>", $rns,
                         &scClass($sc), $via{uc($other)} ? ' query' : '', 
                         &scTitle($sc), $other);
                    next;
                }
                my ($taxa) = &taxa( $other, $ns);
                next unless ($taxa);
                my $targ = $orths{$taxa} ||= [ -2, $taxa, {} ];
                $targ->[0] = $sc if ($targ->[0] < $sc);
                $targ->[2]{$other} = $sc if (!defined $targ->[2]{$other} ||
                                             $targ->[2]{$other} < $sc);
            }
            foreach my $tdat (sort { $b->[0] <=> $a->[0] ||
                                         $a->[1] cmp $b->[1] } values %orths) {
                my ($Tsc, $taxa, $others) = @{$tdat};
                my @sorted = sort { 
                    $others->{$b} <=> $others->{$a}
                } keys %{$others};
                my $sc = $others->{$sorted[0]};
                if ($sc > 0) {
                    # If we have non-null/non-zero hits, remove all
                    # null / zero entries:
                    until ($others->{$sorted[-1]} > 0) {
                        pop @sorted;
                    }
                }
                my $other = join(',', @sorted);
                #my $desc  = $ad->description(-id => $other, -ns => $ns);
                my $lnk   = sprintf($hopXtra, $ns, &esc_xml($other) . " (".
                                    &scTitle($sc).")",
                                    &scClass($sc),
                                    $other, $taxa);
                if (my $xtra = $#sorted) {
                    $lnk .= sprintf("<sup title='Total of %d loc%s recovered as potential orthologues'>%s</sup>", $xtra + 1, $xtra == 0 ? 'us' : 'i', $multOrt);
                     $notes{$multOrt} ||=
                            "Multiple possible orthologues were found for the locus; presumably only one is the true orthologue.";
                }
                &taxa_token($taxa);
                # $lnk .= "&nbsp;<b>[".&taxa_token($taxa)."]</b>";
                push @text, $lnk;
            }
            next if ($#text == -1);
            if ($format eq 'html') {
                my $rnsn   = $nsnm{$rns};
                print $outFH "<tr><th>$rnsn</th><td> ".&truncate_list(\@text).
                    "</td></tr>\n";
            } elsif ($format eq 'xml') {
                foreach my $attr (@text) {
                    print $outFH "$pad    <related ".&hash2attr($attr)." />\n";
                }
            }
        }
        print $outFH "$pad  </relateds>\n" if ($format eq 'xml');
        print $outFH "</tbody></table>\n" if ($format eq 'html');
        my $tDat  = $loc2targ{$loc};
        # die $loc unless ($tDat);
        $tDat ||= {};
        my @subTargs = sort { $tDat->{$b}{sc} <=> $tDat->{$a}{sc} ||
                           $a cmp $b } keys %{$tDat};
        if ($#subTargs == -1) {
            if ($format eq 'html') {
                print $outFH "<p class='ntxt'>No potential compound targets associated with this entity</p>\n";
            }
            next;
        }


        # Now find compounds linked to the targets
        my (%chemLU, %cmpds, %p2c, %pFc);
        # Find cmpds in bulk, organized by target namespace
        foreach my $prot (@subTargs) {
            my $pDat = $tDat->{$prot};
            map { 
                push @{$chemLU{ $pDat->{tns} }{ $_ }}, $prot;
            } keys %{$pDat->{cns}};
        }
        # $debug->branch(\%chemLU);
        while (my ($ptns, $cnss) = each %chemLU) {
            while (my ($cns, $targs) = each %{$cnss}) {
                my @useful;
                map { push @useful, $_ 
                          if (&has_direct_compound($_)) } @{$targs};
                next if ($#useful == -1);

                $ad->bench_start("Convert target to compounds");
                my $rows = $ad->convert
                    ( -ids => \@useful, -ns1 => $ptns, -ns2 => $cns, 
                      -directonly => 1, -age => $chemAge,
                      -nonull     => 1,
                      -columns    => ['term_in','term_out', 'matched','auth' ],
                      @convCom);
                $ad->bench_end("Convert target to compounds");
                
                foreach my $row (@{$rows}) {
                    my ($targ, $cmpd, $sc, $auth) = @{$row};
                    my $tag  = '';
                    my $pmids;
                    ($auth) = split(/ < /, $auth);
                    ($auth, $tag, $pmids) = &parse_author( $auth );
                    my $skipIt = 0;
                    if (defined $sc) {
                        if (my $fc = $filters{$tag}) {
                            $skipIt = $tag if ($sc < $fc);
                        }
                    } else {
                        $skipIt = 'Ignore Null' if ($noNull);
                        $sc = -1;
                    }
                    if ($skipIt) {
                        $filterCount{$skipIt}++;
                        $pFc{$targ}{$cmpd} = 1;
                        $fnum++;
                        next;
                    }
                    $cmpds{$cmpd} ||= {
                        mtid   => $cmpd,
                        ns     => $cns,
                        prot   => {},
                        scores => {},
                        pars   => [],
                    };
                    push @{$cmpds{$cmpd}{scores}{$tag}}, $sc;
                    push @{$cmpds{$cmpd}{prot}{$targ}},
                    [ $sc, $tag, $auth, $pmids ];
                    # print $outFH "<pre>$targ + $cmpd = $tag ($sc) $auth (".join(',',@{$pmids}).")</pre>\n";
                    $p2c{$targ}{$cmpd} = 1;
                }
            }
        }

        if ($format eq 'html') {
            # Find the 'niceset' target namespaces to show
            # This is to prevent display clutter with poorly annotated rna/protein
            my @prefs = ( ['RSP','SP','ENSP','UP','AP'],
                          ['RSR','ENST','AR'] );
            my (%priNS, @tmain, %haveNS);
            map { push @{$haveNS{$tDat->{$_}{gns}}}, $_ } @subTargs;
            my @hns = keys %haveNS;
            foreach my $pg (@prefs) {
                for my $pi (0..$#{$pg}) {
                    my $pns = $pg->[$pi];
                    my $found = 0;
                    foreach my $hn (@hns) {
                        if ($ad->is_namespace($hn, $pns)) {
                            $found++;
                            push @tmain, @{$haveNS{$hn}};
                            delete $haveNS{$hn};
                        }       
                    }
                    last if ($found);
                }
            }

            # Display any targets that have compounds, even if not a primary ns
            while (my ($hn, $targs) = each %haveNS) {
                my @nohit;
                foreach my $targ (@{$targs}) {
                    my @cs = keys %{$p2c{$targ}};
                    if ($#cs == -1) {
                        push @nohit, $targ;
                    } else {
                        push @tmain, $targ;
                    }
                }
                if ($#nohit == -1) {
                    delete $haveNS{$hn};
                } else {
                    $haveNS{$hn} = \@nohit;
                }
            }

            my @phead = ('Target', 'NS', 'Score', '#Cmpd', 'Description');
            my $pcn   = $#phead + 1;
            printf($outFH "<table class='tab'><caption>%d biological target%s associated with %s</caption><tbody>\n", $#subTargs +1 , $#subTargs == 0 ? '' : 's', $loc);
            print $outFH " <tr>".join('', map {"<th>$_</th>"} @phead)."</tr>";
            foreach my $targ (@tmain) {
                my ($ptns, $sc) = map { $tDat->{$targ}{$_} } qw(gns sc);
                my $nsn = $nsNameCache{$ptns} ||= $ad->namespace_name($ptns);
                my @cs = keys %{$p2c{$targ}};
                print $outFH "<tr>\n";
                my $scHtml;
                if ($sc == -1) {
                    $scHtml = $unDef;
                    $notes{$unDef} ||=
                        "Locus-to-target score is not defined";
                } else {
                    $scHtml = sprintf("%d%%", int(0.5 + 100 * $sc));
                }
                printf($outFH " <th%s>%s</th>\n", 
                       $via{uc($targ)} ? " class='query'" : '', $targ);
                printf($outFH " <td class='ns' title='%s'>%s</td>\n", $nsn,
                       $ad->namespace_token($ptns));
                printf($outFH " <td class='%s'>%s</td>\n", &scClass($sc),$scHtml);
                printf($outFH " <td align='center'>%s</td>\n", ($#cs + 1) || '');
                printf($outFH " <td class='desc'>%s</td>\n", 
                       &truncate_text($ad->description
                                      ( -id => $targ, -ns => $ptns)));
                print $outFH "</tr>\n";
            }
            @hns = sort {
                $ad->namespace_name($a) cmp $ad->namespace_name($b);
            } keys %haveNS;
            unless ($#hns == -1) {
                my $hcount = 0;
                map { $hcount += $#{$haveNS{$_}} + 1 } @hns;
                printf($outFH "<tr><th class='ntxt' colspan='$pcn'>%d target%s from less reliable namespaces</th></tr>\n", $hcount, $hcount == 1 ? ' is' : 's are');
                foreach my $hn (@hns) {
                    print $outFH "<tr>\n <th>".$ad->namespace_name($hn).
                        "</th>\n <td colspan='".($pcn-1)."'>";
                    my @txt;
                    foreach my $targ (sort { 
                        $tDat->{$b}{sc} <=> $tDat->{$a}{sc} ||
                            $a cmp $b } @{$haveNS{$hn}}) {
                        my $sc = $tDat->{$targ}{sc};
                        push @txt, sprintf
                            ("<span class='%s%s' title='%s'>%s</span>",
                             &scClass($sc), 
                             $via{uc($targ)} ? ' query' : '', 
                             &scTitle($sc), $targ);
                    }
                    print $outFH join(", ", @txt);
                    print $outFH " </td>\n</tr>\n";
                }
            }
            print $outFH "</tbody></table>\n\n";
        } elsif ($format eq 'xml') {
            my @attrs;
            foreach my $st (@subTargs) {
                my $pDat = $tDat->{$st};
                my @cs   = keys %{$p2c{$st}};
                my @ft   = keys %{$pFc{$st}};
                push @attrs, {
                    id    => $st,
                    ns    => $pDat->{gns},
                    score => $pDat->{sc},
                    cmpds => $#cs + 1,
                    filter => $#ft + 1,
                };
            }
            print $outFH "$pad  <subtargets>\n";
            foreach my $attr (sort { 
                $b->{cmpds} <=> $a->{cmpds} ||
                    $b->{filter} <=> $a->{filter} ||
                    $a->{id} cmp $b->{id} } @attrs) {
                print $outFH "$pad    <subtarget ".&hash2attr($attr)." />\n";
            }
            print $outFH "$pad  </subtargets>\n";
        }

        my @hitCmpds = values %cmpds;
        if ($#hitCmpds == -1) {
            if ($format eq 'html') {
                print $outFH "<p class='null'>";
                if ($fnum) {
                    printf($outFH "A total of %d assay%s were found with the above targets, but all of them <a href='#fkey'>failed your filters</a>\n", $fnum, $fnum == 1 ? '' : 's');
                } else {
                    print $outFH "No compounds associated with the above targets";
                }
                print $outFH "</p>\n";
            }
            next;
        }

        my (%top, %questionableParentage);
        # First find non-isomeric parents in bulk.
        foreach my $cDat (@hitCmpds) {
            my $mtid = $cDat->{mtid};
            my $ns   = $cDat->{ns};
            next unless ($ns eq 'SMI');
            # For SMILES, find non-isomeric parents
            my @pars = $ad->direct_parents( $mtid, $ns );
            unless ($#pars == -1) {
                # This entry is isomeric (we have at least one parent)
                $cDat->{pars} = \@pars;
                next;
            }
            # Hmmm... is this really a non-isomeric compound, or have
            # parents simply not been calculated?
            if (my $seq = $mt->get_seq($mtid)) {
                $questionableParentage{$mtid} = $seq->name()
                    unless ($seq->is_class('Non-Isomeric'));
            }
        }

        # Deal with entries that seem to lack parentage data:
        my @qps = values %questionableParentage;
        unless ($#qps == -1) {
            # There are parent-less compounds that have not been tested
            # Pass them through the canonicalizer / simplifier:
            $sts->simplify( \@qps );
            # ... then write data to MapTracker
            $lh->write();
            $lh->process_ready();
            # ... then update GenAcc and the internal parent array:
            foreach my $mtid (keys %questionableParentage ) {
                my @pars = $ad->direct_parents( $mtid, $ns, '1sec' );
                $cmpds{$mtid}{pars} = \@pars;
            }
        }

        # Now find the 'top level' non-isomeric compounds
        foreach my $cDat (@hitCmpds) {
            my $mtid = $cDat->{mtid};
            my $ns   = $cDat->{ns};
            my @pars = @{$cDat->{pars}};
            if ($#pars == -1) {
                # This is a top-level compound
                $top{$mtid} ||= $cDat;
            } else {
                # This is an isomer; note all of its parents
                foreach my $par (@pars) {
                    my $pDat = $cmpds{$par} ||= {
                        mtid   => $par,
                        ns     => $ns,
                        prot   => {},
                        pars   => [],
                        scores => {},
                    };
                    $top{$par} ||= $pDat;
                    push @{$pDat->{used}}, $mtid;
                    while (my ($tag, $scores) = each %{$cDat->{scores}}) {
                        push @{$pDat->{scores}{$tag}}, @{$scores};
                    }
                }
            }
        }

        # We want to make sure we show some assays from each assay
        # type eg we do not want a ton of ambit data to prevent Ki
        # values from being shown.

        my %byType;
        foreach my $cDat (values %top) {
            my $mtid = $cDat->{mtid};
            while (my ($tag, $scores) = each %{$cDat->{scores}}) {
                my @scs = sort { $b <=> $a } @{$scores};
                my $tot = 0; map { $tot += $_ } @scs;
                push @{$byType{$tag}}, {
                    mtid => $mtid,
                    best => $scs[0],
                    avg  => $tot / ($#scs + 1),
                };
            }
        }

        while (my ($tag, $hits) = each %byType) {
            $byType{$tag} = [ sort { 
                $b->{best} <=> $a->{best} ||
                    $b->{avg} <=> $a->{avg} ||
                    $a->{mtid} cmp $b->{mtid} } @{$hits} ];
        }
        
        my $numUsed = { tot => { used => 0, skip => 0 },
                        kid => { used => 0, skip => 0 },
                        par => { used => 0, skip => 0 } };

        my (@cycleTag, %structured, @keeping);
        my @btCols = qw(mtid best avg);
        my $count  = 0;
        my $recalc = 1;
        my $null   = { mtid => 0, best => -99, avg => -99 };
        my $sTag   = 'used';
        while (1) {
            if ($recalc) {
                @cycleTag = sort keys %byType;
                $recalc   = 0;
                last if ($#cycleTag == -1);
            }
            foreach my $tag (@cycleTag) {
                my $dat = shift @{$byType{$tag}};
                unless ($dat) {
                    # No more data for this assay tag
                    delete $byType{$tag};
                    $recalc++;
                    next;
                }
                my ($mtid, $best, $avg) = map { $dat->{$_} } @btCols;
                if (my $kd = $structured{$mtid}) {
                    # Already have noted this entry
                    if ($kd->{mtid}) {
                        $kd->{best} = $best if ( $kd->{best} < $best );
                        $kd->{avg}  = $avg  if ( $kd->{avg}  < $avg );
                    }
                    next;
                }
                my $cDat = $cmpds{$mtid};
                my $used = $cDat->{used} ||= [];
                my $uNum = $#{$cDat->{used}} + 1;
                $numUsed->{par}{$sTag}++;
                $numUsed->{kid}{$sTag} += $uNum;
                $numUsed->{tot}{$sTag} += $uNum || 1;
                if ($sTag eq 'skip') {
                    $structured{$mtid} = $null;
                    next;
                }
                
                my $ns   = $cDat->{ns};
                $sTag    = 'skip' if ($limit && 
                                      $numUsed->{tot}{used} >= $limit);

                my @kids = ($ns eq 'SMI') ?
                    $ad->direct_children( $mtid, $ns ) : ();
                $cDat->{kids} = \@kids;
                foreach my $id ($mtid, @{$used}) {
                    my $desc = &smiles_description($id, $ns);
                    my @dbits = sort $ad->convert
                        ( -id => $id, -ns1 => $ns, -ns2 => 'BMSC',
                          -nonull => 1, -age => $chemAge );
                    push @dbits, sort $ad->convert
                        ( -id => $id, -ns1 => $ns, -ns2 => 'SET',
                          -nonull => 1, -age => $chemAge );
                    push @dbits, $desc if ($desc);
                    $cmpds{$id}{desc} = join(', ', @dbits) || '';
                }

                push @keeping, $structured{$mtid} = {
                    mtid => $mtid,
                    best => $best,
                    avg  => $avg,
                    obj  => $cDat,
                };
            }
        }

        # Sort by best score first
        @keeping = map { $_->{obj} } sort { 
            $b->{best} <=> $a->{best} ||
                $b->{avg} <=> $a->{avg} ||
                $a->{mtid} cmp $b->{mtid} } @keeping;

        if ($format eq 'html') {
            print $outFH "<table class='bioreport tab'>\n  <caption>";
            printf($outFH "Showing %d assay%s",$numUsed->{tot}{used},
                   $numUsed->{tot}{used} == 1 ? '' : 's');
            if (my $ts = $numUsed->{tot}{skip}) {
                print $outFH "<sup class='alert'>($ts skipped)</sup>";
                $notes{"<sup class='alert'>(# skipped)</sup>"} = "More data exist than shown, increase (or remove) your limit filter to see more results";
            }
            printf($outFH " on %d structure%s ", $numUsed->{par}{used}, 
                   $numUsed->{par}{used} == 1 ? '' : 's');
            if (my $ps = $numUsed->{par}{skip}) {
                print $outFH "<sup class='alert'>($ps skipped)</sup> ";
                $notes{"<sup class='alert'>(# skipped)</sup>"} = "More data exist than shown, increase (or remove) your limit filter to see more results";
            }
            if (my $ku = $numUsed->{kid}{used}) {
                printf($outFH " with %d specific isomer%s", $ku, $ku == 1 ? '' : 's');
                if (my $kus = $numUsed->{kid}{skip}) {
                    print $outFH "<sup class='alert'>($kus skipped)</sup> ";
                    $notes{"<sup class='alert'>(# skipped)</sup>"} = "More data exist than shown, increase (or remove) your limit filter to see more results";
                }
            }
            print $outFH "</caption>\n<tbody>\n";

            my @chead = ('Structure', 'Isomer', 'Activity', 'Description');
            print $outFH " <tr>".join('', map {
                ref($_) ? "<th $_->[0]>$_->[1]</th>" : "<th>$_</th>" 
                } @chead)."</tr>\n";
            foreach my $cDat (@keeping) {
                print $outFH &bio_cmpd_row( $cDat, \%cmpds );
            }
            print $outFH "</tbody></table>\n\n";
        } elsif ($format eq 'xml') {
            my %distinct;
            my @stack = @keeping;
            while (my $cDat = shift @stack) {
                my $id   = $cDat->{mtid};
                next if ($distinct{$id});
                $distinct{$id} = $cDat;
                foreach my $kid (@{$cDat->{used} || []}) {
                    push @stack, $cmpds{$kid};
                }
            }
            foreach my $cDat (values %distinct) {
                my $targs = $cDat->{prot};
                foreach my $targ (sort keys %{$targs}) {
                    foreach my $scDat (sort { $b->[0] <=> $a->[0] } 
                                       @{$targs->{$targ}}) {
                        my ($sc, $tag, $auth, $pmids) = @{$scDat};
                        my $vf  = $valFormats->{$tag || ''};
                        $vf     = $vf ? $vf->[0] : \&defaultfmt;
                        my $act = ($sc == -1) ? '?' : &{$vf}($sc);
                        my $pars = join(',', @{$cDat->{pars} || []}) || undef;
                        push @xmlAssays, {
                            loc  => $loc,
                            lns  => $ns,
                            targ => $targ,
                            tns  => $tDat->{$targ}{gns},
                            cmpd => $cDat->{mtid},
                            cns  => $cDat->{ns},
                            zzcmpdpar => $pars,
                            score => $sc,
                            assay => $tag,
                            aval  => $act,
                            auth  => $auth,
                        };
                    }
                }
            }
            print $outFH "$pad</target> <!-- ".&esc_xml($loc)." -->\n";
        }        
    }
    if ($format eq 'xml') {
        $xmlInd--; $pad =~ s/  //;
        print $outFH "$pad</targets>\n";
    }
    unless ($#unk == -1) {
        print $outFH "<p class='unk'><b>Unknown IDs</b> ".join(', ', @unk)."</p>\n";
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
    # print $outFH "<pre>FOO $txt = $rv[0] + $rv[1] + ".join(",", @{$rv[2]})."</pre>\n" unless ($rv[0] eq 'Ambit');
    return @rv;
}

sub print_msg {
    my ($msg, $cls) = @_;
    if ($format eq 'html') {
        print $outFH "<div ";
        print $outFH "class='$cls'" if ($cls);
        print $outFH ">\n$msg\n</div>\n";
    } else {
        $msg =~ s/<[^>]+>//g;
        $args->msg(split(/\n/, $msg));
    }
}

sub has_direct_compound {
    my ($bio) = @_;
    unless (defined $targCount{$bio}) {
        $ad->bench_start("Filter targets");
        my $check = $mt->get_edge_dump
            ( -name      => $bio,
              -limit     => 1,
              -keepclass => ['Substance','Biomolecule'],
              -keeptype  => \@chemTypes );
        $ad->bench_end("Filter targets");
        $targCount{$bio} = $#{$check} + 1;
        # warn "$bio = $targCount{$bio}\n";
    }
    return $targCount{$bio};
}

sub bio_cmpd_row {
    my ($cDat, $cmpds, $rowStarted) = @_;
    my $id      = $cDat->{mtid};
    my $used    = $cDat->{used} || [];
    my $cns     = $cDat->{ns};
    my @targs   = sort keys %{$cDat->{prot}};
    my $rowUsed = ($#targs == -1) ? 0 : 1;
    my $row     = "";
    $row       .= "<tr>\n" unless ($rowStarted);
    my $smi     = $mt->get_seq( -id => $id, -defined => 1)->name;
    my $imgUrl  = &esc_url( $smi );
    if ($cns eq 'SMI') {
        if ($cmpds) {
            # This is the first call from the non-isomer form, need a special row
            my $totKids = $#{$cDat->{kids}} + 1;
            my $rowSpan = $#{$used} + 1 + $rowUsed;
            $rowSpan    = $rowSpan == 1 ? '' : " rowspan='$rowSpan'";
            my $kmsg    = ($totKids) ? sprintf
                ("\n  <br /><span class='ntxt'>%d isomer%s</span>",
                 $totKids, $totKids == 1 ? '' : 's') : "";
            if ($checkInv) {
                my @bmsIDs = $ad->convert
                    ( -id => [ $id, @{$cDat->{kids}} ], -ns1 => 'SMI', 
                      -ns2 => 'BMSC');
                my $sth = &crs_stock_sth();
                my %stock;
                foreach my $q ($sts->diversify_bms_ids(@bmsIDs)) {
                    $sth->execute($q);
                    my $brows = $sth->fetchall_arrayref();
                    foreach my $row (@{$brows}) {
                        my ($lot, $amnt, $refNum) = @{$row};
                        if ($amnt) {
                            if (!$stock{$q} || $stock{$q}[0] < $amnt) {
                                my $tip = "$amnt &micro;l";
                                $tip .= " Lot:$lot" if ($lot);
                                $tip .= " Ref:$refNum" if ($refNum);
                                $stock{$q} = [ $amnt, $tip ];
                            }
                        }
                    }
                }
                my @inStock = sort keys %stock;
                unless ($#inStock == -1) {
                    $notes{"<span class='inv'>BMY-123456</span>"} ||= 
                        "An isomeric form (possibly not the one you want - check!) is present in the BMS Inventory. Noted under the 'Structure' column in results";
                    my @bits = map { sprintf("<span class='inv' title='%s'>%s</span>", $stock{$_}[1], $_) } @inStock;
                    $kmsg .= "<br />\n" if ($kmsg);
                    $kmsg .= join(' ', @bits);
                }
            }
            # $kmsg .= $args->branch($cDat);#join(' + ', @{$cDat->{kids}});
            $row .= sprintf($bcrPri, $rowSpan, $cns, $id, $id, 
                            $imgUrl, $kmsg);
            $row .= " <td class='ntxt'>Non-isomeric</td>\n" if ($rowUsed);
        } else {
            $row .= sprintf($bcrSec, $cns, $id, $id, $imgUrl);
        }
    } elsif ($cns eq 'SEQ') {
        $row .= sprintf($bcrSeq, $cns, $id, $id, 
                        &fasta4seq($id));
        $row .= " <td class='ntxt'>Sequence</td>\n";
    }
    if ($rowUsed) {
        $row .= " <td>";
        $row .= "<table class='intab'><tbody>\n";
        foreach my $targ (@targs) {
            $row .= "  <tr><td>$targ</td>\n";
            my @actBits;
            foreach my $scDat (sort { $b->[0] <=> $a->[0] } 
                               @{$cDat->{prot}{$targ}}) {
                my ($sc, $tag, $auth, $pmids) = @{$scDat};
                my $act = "";
                $act .= &showtag($tag);
                my $vf = $valFormats->{$tag || ''};
                $vf    = $vf ? $vf->[0] : \&defaultfmt;
                $act .= ($sc == -1) ? '?' : &{$vf}($sc);
                $act .= " <span class='auth'>$auth</span>";
                if (my $lnk = &_pubmed_link( $pmids )) {
                    $act .= "&nbsp;$lnk";
                }
                push @actBits, $act;
            }
            $row .= "  <td>".join("<br />", @actBits)."</td></tr>\n";
        }
        $row .= " </tbody></table>";
        $row .= "</td>\n";
        $row .= sprintf(" <td class='desc'>%s</td>\n", 
                        &truncate_text($cDat->{desc}));
        $row .= "</tr>\n";
    }
    for my $k (0..$#{$used}) {
        my $kidDat = $cmpds->{ $used->[$k] };
        $row .= &bio_cmpd_row( $kidDat, undef,!$k && !$rowUsed);
    }
    return $row;
}

sub _pubmed_link {
    my $pmids = shift;
    my @nums;
    foreach my $id (@{$pmids || []}) {
        if ($id =~ /^PMID:(\d+)/) {
            push @nums, $1;
        }
    }
    my $n = $#nums + 1;
    return "" unless ($n);
    my $pl = $n == 1 ? '' : 's';
    map { $allPubMeds{$_}++ } @nums;
    return "<a class='pmid' href='http://www.ncbi.nlm.nih.gov/pubmed/".join(',', @nums)."' target='_blank' title='Retrieve $n article$pl'>$n</a>";
}

sub pubmed_html_summary {
    my @ids = sort { $a <=> $b } keys %allPubMeds;
    my $num = $#ids + 1;
    return "" unless ($num);
    my $assy = 0; map { $assy += $allPubMeds{$_} } @ids;
    my $html .= "<hr /><table class='tab'><caption><span class='pmid'> </span> Bibliography - <span class='pmid'>$num</span> articles in $assy assays<span class='pmid'> </span> </caption><tbody>\n";
    my $pmfile = "$gasDir/PubmedDesc.tsv";
    my $pbdRows = &forked_data
        ( -ids        => [ map { "PMID:$_" } @ids ], 
          -ns1        => 'PMID', 
          -mode       => 'desc',
          -nonull     => 1,
          -cols       => [ qw(termin desc) ],
          -output     => $pmfile,
          @convCom);
    my %lu = map { $_->[0] => $_->[1] } @{$pbdRows};
    
    $html .= " <tr>".join('', map { "<th>$_</th>" } qw(PMID Date Title))."</tr>\n";
    foreach my $id (@ids) {
        my $pmid = "PMID:$id";
        my ($dt, $desc) = ("", $lu{$pmid} || "");
        if ($desc =~ /^\[([0-9\-]+)\] (.+)/) {
            ($dt, $desc) = ($1, $2);
        }
        
        $html .= " <tr><th><a href='http://www.ncbi.nlm.nih.gov/pubmed/$id' target='_blank' title='Retrieve from PubMed'>$pmid</a></th><td>$dt</td><td>$desc</td></tr>\n";
        &add_eh_rows( $globalEh, 'PubMed', {
            PubMed => "PMID:$id",
            Date   => $dt,
            Count  => $allPubMeds{$id},
            Title  => $desc,
        }) if ($globalEh);
    }
    $html .= "</tbody></table>\n";
    return $html;
}

sub showtag {
    my $tag = shift;
    return "" unless ($tag);
    my $class = $tag;
    $class =~ s/[^A-Z0-9]/x/gi;
    return "<span class='$class'>$tag</span> ";
}

sub log10HTMLfmt {
    my $sc = shift;
    return '?' unless ($sc);
    return "10<sup>-".(10*$sc)."</sup>";
}

sub log10fmt {
    my $sc = shift;
    return '?' unless ($sc);
    return "1e-".(10*$sc);
}

sub defaultfmt {
    my $sc = shift;
    return defined $sc ? $sc : '?';
}


sub percfmt {
    my $sc = shift;
    return sprintf("%d%%", int(0.5 + 100 * $sc));
}

sub invpercfmt {
    my $sc = 1 - shift;
    return sprintf("%d%%", int(0.5 + 100 * $sc));
}

sub truncate_text {
    my $text = shift;
    $text = "" unless (defined $text);
    return $text unless (length($text) > $truncLen);
    $notes{$elipses} ||= 
        "Text truncated to $truncLen characters.<br />You can ".
       "<span onclick='toggleTrunc()' class='butt'>Toggle Descriptions</span>".
        "to see the full text.";
    my $pos = $truncLen;
    my $brk = index($text, $brkPt, $truncLen - $brkLen);
    $pos = $brk if ($brk != -1 && $brk < $truncLen);
    return substr($text, 0, $pos).&hide_text(substr($text, $pos));
}

sub truncate_list {
    my $list = shift;
    my $num  = shift || 20;
    my $text = join(', ', splice(@{$list},0,$num));
    $text .= &hide_text(join(', ', @{$list})) unless ($#{$list} == -1);
    return $text;
}

my $oscpDbh;
sub get_oscp1_dbh {
    return $oscpDbh ||= BMS::FriendlyDBI->connect
        ("dbi:Oracle:", 'maptracker/mtreadonly@oscp1',
         undef, { RaiseError  => 0,
                  PrintError  => 0,
                  LongReadLen => 100000,
                  AutoCommit  => 1, },
         -errorfile => '/scratch/OSCP1.err',
         -adminmail => 'tilfordc@bms.com', );
}

my $crsSTH;
sub crs_stock_sth {
    return $crsSTH if ($crsSTH);
    $ad->bench_start;
    my $dbh     = &get_oscp1_dbh();
    my $sql     = <<EOF;
SELECT psl.ext_sub_lot_id, ic.maxorderablesolutionamount
  FROM tapif.crs_inv_compound ic, rsims.pcris_substance_lots psl
 WHERE ic.compoundid = ?
   AND psl.LOT_ID = ic.lotid
EOF

    # 2015 Nov 17 - Rich Bischoff makes view to cover loss of table:
$sql = <<SQL;
SELECT 0, maxorderablesolutionamount 
  FROM locps.vw_max_orderable_amounts
 WHERE substanceid = ?
SQL


$crsSTH = $dbh->prepare
( -name => "Get wet inventory from CRS",
  -sql => $sql, );
    $ad->bench_end;
return $crsSTH;

}


sub hide_text {
    my $text = shift;
    return "" unless (defined $text);
    return "$elipses<span name='trunc' class='hide'>$text</span>";
}

sub scClass {
    my $sc = shift;
    $sc = -1 unless (defined $sc);
    for my $gs (0..$#scoreClass) {
        return $scoreClass[$gs][1] if ($sc >= $scoreClass[$gs][0]);
    }
    return 'scUn';
}

sub scTitle {
    my $sc = shift;
    return "Unknown confidence" if (!defined $sc || $sc < 0);
    return sprintf("%d%% confidence", int(0.5 + 100 * $sc));
}

sub process_Chemical {
    my ($ids) = @_;
    print $outFH "<h2>Compounds</h2>\n";
    # my @head = ('Target','Activity', 'Gs','&deg','Locus', 'T&rarr;L', 'Symbol', 'Target Class','Description');
    my @head = ('&deg','Locus','Symbol','Gs','Target Class',
                'Target','T&rarr;L','Activity','PMID', 'Target Description');
    my $numCols = $#head + 1;
    my $order   = 0;
    
    my $standardInput = &forked_data
        ( -ids    => $ids, -ns1 => 'ac', -ns2 => 'SMI,SEQ',
          -cols   => 'termin,termout,nsout', -directonly => 1,
          -auth   => '!MoreSpecific,!MoreGeneric',
          -output => "$gasDir/StandardizedIDs.tsv");
    my (%canonChem, %found);
    foreach my $row (@{$standardInput}) {
        my ($in, $out, $ns) = @{$row};
        if (!$out) {
            # No obvious standardized target
            $found{$in} ||= 0;
            next;
        } elsif ($out =~ /^MTID:(\d+)$/) {
            my $ccDat;
            unless ($ccDat = $canonChem{$out}) {
                my $mobj = $mt->get_seq( -id => $1, -defined => 1);
                $ccDat = $canonChem{$out} = {
                    order   => 1,  # Not informative via GAS
                    id      => $1,
                    mtid    => $out,
                    raw     => $mobj->name(),
                    queries => {},
                    qlook   => {},
                    kids    => [],
                    pars    => [],
                };
            }
            $found{$in}++;
            $ccDat->{ns}{$ns} = 1;
            $ccDat->{queries}{$in} = 1;
            $ccDat->{qlook}{uc($in)} = 1;
        } else {
            $args->msg("[!!]","Unrecognized 'standard' ID recovered from ".
                       "canonicalization", "$in = '$out'");
        }
    }
    # $args->death("$gasDir/StandardizedIDs.tsv", $args->branch(\%found));
    my @unkChem;
    while (my ($id, $status) = each %found) {
        push @unkChem, $id unless ($status);
    }
    my @canonIds = sort keys %canonChem;
    $notes{"<span class='query'>Abc</span>"} = 
        "Highlight indicating text you used as a query"
        unless ($#canonIds == -1);

    
    my (%byNS, %nsTok);
    while (my ($id, $ccDat) = each %canonChem) {
        my @nss = keys %{$ccDat->{ns}};
        my ($ns) = $#nss == 0 ?
            ($nsTok{$nss[0]} ||= $ad->namespace_token($nss[0])) :
            $ad->guess_namespace($id, 'AC');
        $ccDat->{ns} = $ns;
        $ccDat->{queries} = [ sort keys %{$ccDat->{queries}} ];
        push @{$byNS{$ns}}, $id;
    }
    # $args->death("$gasDir/StandardizedIDs.tsv", $args->branch(\%byNS));
    my %allCanon;
    while (my ($ns, $ids) = each %byNS) {
        map { $allCanon{$ns}{$_} = 1 } @{$ids};
        my $inToPar = &forked_data
            ( -ids    => $ids, -ns1 => $ns, -mode => 'parent',
              -cols   => 'child,parent', -depth => 1,
              -output => "$gasDir/$ns-CanonicalParents.tsv");
        my $parHash = &in_to_out_hash( $inToPar );
        while (my ($id, $arr) = each %{$parHash}) {
            $canonChem{$id}{pars} = $arr;
            map { $allCanon{$ns}{$_}++ } @{$arr};
        }
        my $inToKid = &forked_data
            ( -ids    => $ids, -ns1 => $ns, -mode => 'child',
              -cols   => 'parent,child', -depth => 1,
              -output => "$gasDir/$ns-CanonicalChildren.tsv");
        my $kidHash = &in_to_out_hash( $inToKid );
        while (my ($id, $arr) = each %{$kidHash}) {
            $canonChem{$id}{kids} = $arr;
            map { $allCanon{$ns}{$_}++ } @{$arr};
        }
    }

    # Get supporting information
    my @tfilt = $taxa ? ( -int => $taxa, -intns => 'TAX') : ();
    my (@fetchDesc, %descr, %cmpd2targ, %targData);
    # $args->death($args->branch(\%allCanon));
    while (my ($ns, $idHash) = each %allCanon) {
        my @ids = sort keys %{$idHash};
        map { $canonChem{$_} ||= {
            order => 0,
            mtid  => $_,
            ns    => $ns,
        } } @ids;
        push @fetchDesc, map { "#$ns#$_" } @ids;

        &bulk_chem_meta(\@ids, $ns, \%canonChem);

        my $targs = &forked_data
            ( -ids => \@ids, -ns1 => $ns, -ns2 => 'AP,AR,AL,SYM',
              -directonly => 1, -age => $chemAge, -nonull => 1,
              @tfilt, -nullscore => -1,
              -output => "$gasDir/$ns-toTarget.tsv",
              -cols => 'termin,termout,matched,nsout,auth' );
        foreach my $row (@{$targs}) {
            my ($cmpd, $targ, $sc, $tnsn, $auth) = @{$row};
            my $tns = $nsTok{$tnsn} ||= $ad->namespace_token($tnsn);
            my $tag  = '';
            my $pmids;
            ($auth, $tag, $pmids) = &parse_author( $auth, $cmpd, $targ );
            $auth =~ s/.+ \< //;
            if ($targ =~ /(.+)\.\d+$/ && ($tns eq 'AP' || $tns eq 'AR')) {
                # Versioned IDs occasionaly sneak in. They screw things up
                $targ = $1;
            }
            #warn "$cmpd + $targ = $sc, $auth, $tag\n" if ($targ eq 'P41143');
            push @{$cmpd2targ{$cmpd}{$targ}},
            [ $sc, $auth, $tag, $tns, $pmids ];
            #my $tkey = $targ;
            # Swiss-Prot variants will have the variant number removed
            # when they are mapped to loci; need to be able to track that
            # if ($targ =~ /^(.+)\-\d+$/) { $tkey = $1 };
            # push @{$tkeys{$tkey}}, $targ;
            $targData{$targ} ||= {
                loc   => $targ,
                lowNs => $tns,
                loci  => {},
                # map   => { $targ => 1 },
                # targ  => [ $targ ],
            };
        }
    }
    # Get information for targets
    my %targByNs;
    foreach my $tDat (values %targData) {
        my ($targ, $tns) = ($tDat->{loc}, $tDat->{lowNs});
        my ($gns) = $ad->guess_namespace_careful($targ, $tns);
        $tDat->{ns} = $gns || $tns;
        push @{$targByNs{$gns}}, $targ;
    }
    my %uniqueLL;
    while (my ($ns, $ids) = each %targByNs) {
        my $locs = &forked_data
            ( -ids => $ids, -ns1 => $ns, -ns2 => 'LL', -nonull => 1,
              -nullscore  => -1, 
              -output => "$gasDir/$ns-toLocusLink.tsv",
              -cols   => 'term_in,term_out,matched');
        foreach my $row (@{$locs}) {
            my ($targ, $loc, $sc) = @{$row};
            $targData{$targ}{loci}{$loc} = $sc if 
                (!defined $targData{$targ}{loci}{$loc} ||
                 $targData{$targ}{loci}{$loc} < $sc);
            $uniqueLL{$loc}++;
        }
        # Find targets without loci
        my @toHg;
        foreach my $targ (@{$ids}) {
            my @loc = keys %{$targData{$targ}{loci}};
            push @toHg, $targ if ($#loc == -1);
        }
        &objectsToHg(\@toHg, $ns, \%targData);
    }
    my @allLL = sort keys %uniqueLL;
    map { $targData{$_} ||= {
        loc   => $_,
        ns    => 'LL',
        loci  => {},
    } } @allLL;
    &objectsToHg(\@allLL, "LL", \%targData);

    my @fetchLL   = map { "#LL#$_" } @allLL;
    my @fetchTarg = map {
        $_->{ns} ? sprintf("#%s#%s", $_->{ns}, $_->{loc}) : $_->{loc}
    } values %targData;
    my @fetchHG   = map { 
        map { "#HG#$_" } keys %{$_->{hg} || {}}
    } values %targData;

    push @fetchDesc, (@fetchLL, @fetchTarg, @fetchHG);
    
    unless ($#fetchDesc == -1) {
        my $desc = &forked_data
            ( -ids => \@fetchDesc,
              -output => "$gasDir/Descriptions.tsv",
              -mode => 'desc', -cols => ['term,desc']);
        foreach my $row (@{$desc}) {
            my ($id, $d) = @{$row};
            $descr{$id} ||= $d || "";
        }
    }

    my @fetchTax = (@fetchLL, @fetchTarg);
#    my %taxa;
#    unless ($#fetchTax == -1) {
#        my $tax = &forked_data
#            ( -ids => \@fetchTax, -ns2 => 'TAX',
#              -output => "$gasDir/Taxae.tsv",
#              -cols => ['termin,termout']);
#        foreach my $row (@{$tax}) {
#            my ($id, $t) = @{$row};
#            $taxa{$id} ||= $t || "";
#        }
#    }

    my %sym;
    unless ($#fetchLL == -1) {
        
    }

    # &prebranch(\%canonChem);
    my $descFrm = " <span class='desc'>%s</span>\n";
    my $idFrm   = "<span class='pri mtid'>%s</span>\n";
    foreach my $ccDat (sort {$a->{order} <=> $b->{order}} values %canonChem) {
        # At this point canonChem also holds auxiliary data (kids and parents),
        # which we do not want to display:
        unless ($ccDat->{order}) {
            next;
        }
        my $mtid = $ccDat->{mtid};
        my $sns  = $ccDat->{ns};
        my $fnum = 0;
        my @search = ($mtid);
        push @search, @{$ccDat->{kids}};
        push @search, @{$ccDat->{pars}};

        my %toDisplay;
        map { $toDisplay{$_} = { type => 'K', id => $_ } } @{$ccDat->{kids}};
        map { $toDisplay{$_} = { type => 'P', id => $_ } } @{$ccDat->{pars}};
        foreach my $dat (values %toDisplay) {
            my $kmt      = $dat->{id};
            $dat->{num}  = 0;
            $dat->{raw}  = $mt->get_seq( -id => $kmt, -defined => 1)->name;
            $dat->{desc} = $descr{"MTID:$kmt"};
        }
        $toDisplay{$mtid} = {
            type => 'Q',
            num  => 0,
            id   => $mtid,
            raw  => $ccDat->{raw},
            desc => $descr{$mtid},
            hits => 0,
        };

        # Organize all targets, collect aliases
        my (%aH, %targs);
        my %neededAli = map { uc($_) => $_ } @{$ccDat->{queries}};
        foreach my $cmpd (@search) {
            while (my ($targ, $datArr) = each %{$cmpd2targ{$cmpd} || {}}) {
                my $tns = $datArr->[0][3];
                my $ok  = 0;
                foreach my $dat (@{$datArr}) {
                    my ($sc, $auth, $tag, $tns, $pmids) = @{$dat};
                    my $skipIt = 0;
                    if ($sc < 0) {
                        $skipIt = 'Ignore Null' if ($noNull);
                    } elsif (my $fc = $filters{$tag}) {
                        $skipIt = $tag if ($sc < $fc);
                    }
                    if ($skipIt) {
                        $filterCount{$skipIt}++;
                        $fnum++;
                        next;
                    }
                    $ok++;
                    my $actDat = $targs{$targ} ||= {
                        targ => $targ,
                        cmpd => {},
                        ns   => $tns,
                        pmid => {},
                    };
                    $actDat->{best} = $sc if
                        (!defined $actDat->{best} || $actDat->{best} < $sc);
                    $actDat->{aNum}++;
                    my $key = join("\t", $tag, $sc, $auth);
                    push @{$actDat->{cmpd}{$key}}, $cmpd;
                    map { $actDat->{pmid}{$key}{$_} = 1 } @{$pmids};
                }
                
                $toDisplay{$cmpd}{num}++ if ($ok);
            }
            while (my ($type, $alis) = each %{$canonChem{$cmpd}{meta}{alias}}) {
                map { $aH{$type}{$_}{$cmpd} = 1;
                      delete $neededAli{uc($_)} } @{$alis};
            }
        }
        map { $aH{Various}{$_}{$mtid} = 1 } values %neededAli;

        # Now map all targets to loci
        my (%loci, %targByNs);
        foreach my $targ (keys %targs) {
            my $tDat = $targData{$targ};
            my $ns   = $tDat->{ns};
            # Swiss-Prot variants will have the variant number removed
            # when they are mapped to loci; need to be able to track that
            my $tkey = $targ;
            if ($targ =~ /^(.+)\-\d+$/) { $tkey = $1 };
            $loci{$targ} = {
                loc  => $targ,
                key  => $tkey,
                map  => { $targ => 1 },
                ns   => $ns,
                targ => [ $targ ],
                best => $targs{$targ}{best},
                aNum => $targs{$targ}{aNum} || 0,
                tNum => 1,
                other => { },
            };
            my $lHash = $targData{$tkey}{loci} || {};
            my @locs  = sort { $lHash->{$b} <=> $lHash->{$a} } keys %{$lHash};
            next if ($#locs == -1);
            # If we have a locus, we do not need to hold on to the
            # target as a 'primary' locus for display:
            delete $loci{$targ};
            my $loc  = shift @locs;
            my $dat  = $loci{$loc} ||= {
                key  => $loc,
                loc  => $loc,
                map  => {},
                ns   => 'LL',
                targ => [],
                best => -2,
                other => {},
                aNum  => 0,
            };
            $dat->{map}{$targ}   = $lHash->{$loc};
            $dat->{other}{$targ} = \@locs;
            push @{$dat->{targ}}, $targ;
            #unless ($targs{$targ}{aNum}) { print $outFH "<pre>".$args->branch($targs{$targ})."</pre>"; die }
            $dat->{aNum} += ($targs{$targ}{aNum} || 0);
            $dat->{tNum}++;
            $dat->{best}  = $targs{$targ}{best}
            if ($dat->{best} < ($targs{$targ}{best} || -2));
        }
        # Gather symbol and taxa metadata
        while (my ($ns, $ids) = each %targByNs) {
            &bulk_meta($ns, $ids, \%targs);
        }

        # Now map all Loci to Homologene
        my (%homologene, @noHg, %locByNs);
        foreach my $lDat (values %loci) {
            my ($loc, $ns, $lkey) = map { $lDat->{$_} } qw(loc ns key);
            push @{$locByNs{$ns}}, $loc;
            my $hHash = $targData{$lkey}{hg} || {};
            my @hgs   = sort { $hHash->{$b} <=> $hHash->{$a} } keys %{$hHash};
            if ($#hgs == -1) {
                push @noHg, $lDat;
                next;
            }
            my $hg  = shift @hgs;
            my $dat  = $homologene{$hg} ||= {
                hg   => $hg,
                map  => {},
                ns   => 'HG',
                locs => [],
                best => -2,
                other => {},
            };
            my $ldat = $loci{$loc};
            $dat->{map}{$loc}   = $hHash->{$loc};
            $dat->{other}{$loc} = \@hgs;
            push @{$dat->{locs}}, $loc;
            $dat->{aNum} += $ldat->{aNum};
            $dat->{tNum} += $ldat->{tNum};
            $dat->{lNum}++;
            $dat->{best} = $ldat->{best} if ($dat->{best} < $ldat->{best});
        }
        
        # Gather symbol and taxa metadata
        while (my ($ns, $ids) = each %locByNs) {
            &bulk_meta($ns, $ids, \%loci);
        }
        
        # Deal with objects that have no homologene entry
        my $unkCnt = 0;
        foreach my $ldat (@noHg) {
            my $loc = $ldat->{loc};
            my $dat  = $homologene{$loc} = {
                hg   => 'UNK'.++$unkCnt,
                map  => { $loc => 1 },
                ns   => 'UNK',
                locs => [ $loc ],
                best => $loci{$loc}{best},
                other => {},
                lNum  => 1,
                aNum  => $ldat->{aNum},
                tNum  => $ldat->{tNum},
            };
        }

        my @sorted = sort { $b->{best} <=> $a->{best}
                            || $a->{hg} cmp $b->{hg} } values %homologene;

        my $numUsed = { tot  => { used => 0, skip => 0 },
                        targ => { used => 0, skip => 0 },
                        loc  => { used => 0, skip => 0 },
                        hg   => { used => 0, skip => 0 } };

        my @keeping;
        foreach my $dat (@sorted) {
            my $tag;
            if ($limit && $numUsed->{tot}{used} >= $limit) {
                
                $tag = 'skip';
            } else {
                $tag  = 'used';
            }
            $numUsed->{tot}{$tag}  += $dat->{aNum};
            $numUsed->{targ}{$tag} += $dat->{tNum};
            $numUsed->{loc}{$tag}  += $dat->{lNum};
            $numUsed->{hg}{$tag}++;
            push @keeping, $dat if ($tag eq 'used');
        }

        my @cmpSort = sort { $b->{num} <=> $a->{num} } values %toDisplay;

        my $keyCount = 0;
        my %mtKey;
        my $nullBlock = $clearDiv;
        my $numHit    = 0; map { $numHit++ if ($_->{num}) } @cmpSort;
        my $num = $#cmpSort + 1;
        foreach my $dat (@cmpSort) {
            my $type = $dat->{type};
            my $kmt  = $dat->{id};
            my $show = ($type eq 'Q') ? 
                $kmt : sprintf($hopLink, $sns, $kmt, $kmt);
            my $info = sprintf("<br />\n<span class='t%s'>%s</span>", 
                               $type, $typeName{$type});
            if ($dat->{num}) {
                # This compound has hits
                $info   .= sprintf(" <span class='hc'>%d activit%s</span>", 
                                   $dat->{num}, 
                                   $dat->{num} == 1 ? 'y' : 'ies');
            } elsif ($nullBlock) {
                if ($numHit) {
                    print $outFH $nullBlock;
                    printf($outFH "<a class='togLink' onclick='togNext(this,1)'>Click to see %d variant%s with no hits</a>",
                           $num, $num == 1 ? '' : 's');
                    print $outFH "<div class='hide'>\n";
                    $nullBlock = undef;
                } else {
                    # None of the compounds has hits
                    $nullBlock = "";
                }
            }

            printf($outFH "<div class='%s block'>\n", $type);
            printf($outFH $mkeyFrm, $mtKey{$kmt} = ++$keyCount);
            printf($outFH $idFrm, $show);
            if ($sns eq 'SMI') {
                printf($outFH $strcFrm, &esc_url( $dat->{raw} ));
            } else {
                printf($outFH $seqFrm, &fasta4seq($dat->{id}));
            }
            print $outFH $info;
            printf($outFH $descFrm, &truncate_text($dat->{desc})) if ($dat->{desc});
            print $outFH "</div>\n";
            $num--;
        }
        print $outFH $clearDiv;
        print $outFH "</div>\n" unless (defined $nullBlock);

        my @aT     = sort keys %aH;
        my $aliTab = "";
        my $qLook  = $ccDat->{qlook};
        foreach my $nsn (@aT) {
            my @alis = &fancySort( [keys %{$aH{$nsn}} ] );
            my @atxt;
            foreach my $ali (@alis) {
                my $txt = $qLook->{uc($ali)} ? 
                    "<span class='query'>$ali</span>" : $ali;
                my @refs = keys %{$aH{$nsn}{$ali}};
                $txt .= sprintf
                    ($mkeyFrm, join
                     (',', sort { $a <=> $b } map { $mtKey{$_}} @refs));
                push @atxt, $txt;
            }
            next if ($#atxt == -1);
            $aliTab .= " <tr><th>$nsn";
            if ($nsn eq 'Aureus ID') {
                my @ids = @alis; map { s/^AUR// } @ids;
                my $href = "http://otb.pri.bms.com/otb.lisp?func=btable&nfilter=3&server=molcdrp1&user=CLIENT_GPCR&qid=784&filtcol1=AUREUS_ID&filtmode1=IN&filtcol2=TYPE&filtmode2=M&filtval2=&filtcol3=VALUE&filtmode3=LE&filtval3=&filtval1="
                    .join(' ', @ids);
                $aliTab .= "<br /><a target='Aureus' class='external' href='$href'>View OTB</a>";
            }
            $aliTab .= "</th><td>".&truncate_list(\@atxt)."</td></tr>\n";
        }
        
        print $outFH "<table class='tab'><tbody>\n$aliTab</tbody></table>\n" 
            if ($aliTab);

        if ($#keeping == -1) {
            if ($format eq 'html') {
                print $outFH "<p class='null'>";
                if ($fnum) {
                    printf($outFH "A total of %d assay%s were found with the above compounds, but all of them <a href='#fkey'>failed your filters</a>\n", $fnum, $fnum == 1 ? '' : 's');
                } else {
                    print $outFH "No biological targets found";
                }
                print $outFH "</p><hr />\n";
            }
            next;
        }

        print $outFH "<table class='chemreport tab'>\n  <caption>";
        printf($outFH "Showing %d assay%s",$numUsed->{tot}{used},
               $numUsed->{tot}{used} == 1 ? '' : 's');
        if (my $as = $numUsed->{tot}{skip}) {
            print $outFH "<sup class='alert'>($as skipped)</sup>";
            $notes{"<sup class='alert'>(# skipped)</sup>"} = "More data exist than shown, increase (or remove) your limit filter to see more results";
        }
        printf($outFH " against %d target%s", $numUsed->{targ}{used}, 
               $numUsed->{targ}{used} == 1 ? '' : 's');
        if (my $ts = $numUsed->{targ}{skip}) {
            print $outFH "<sup class='alert'>($ts skipped)</sup> ";
            $notes{"<sup class='alert'>(# skipped)</sup>"} = "More data exist than shown, increase (or remove) your limit filter to see more results";
        }
        printf($outFH " within %d loc%s", $numUsed->{loc}{used}, 
               $numUsed->{loc}{used}== 1 ? 'us' : 'i');
        if (my $ls = $numUsed->{loc}{skip}) {
            print $outFH "<sup class='alert'>($ls skipped)</sup> ";
            $notes{"<sup class='alert'>(# skipped)</sup>"} = "More data exist than shown, increase (or remove) your limit filter to see more results";
        }
        printf($outFH " in %d orthologue group%s", $numUsed->{hg}{used}, 
               $numUsed->{hg}{used}== 1 ? '' : 's');
        if (my $hs = $numUsed->{hg}{skip}) {
            print $outFH "<sup class='alert'>($hs skipped)</sup> ";
            $notes{"<sup class='alert'>(# skipped)</sup>"} = "More data exist than shown, increase (or remove) your limit filter to see more results";
        }
        print $outFH "</caption>\n<tbody>\n";

        print $outFH "<tr>\n". join('', map { " <th>$_</th>\n" } @head) . "</tr>\n";

        my $hgBit = 0;
        for my $h (0..$#keeping) {
            my $hDat = $keeping[$h];
            my @locs = sort { $b->{best} <=> $a->{best}
                              || $a->{loc} cmp $b->{loc} } 
            map { $loci{$_} } @{$hDat->{locs}};
            my @hgPicker;
            for my $l (0..$#locs) {
                my $lDat = $locs[$l];
                my @targets = sort { $b->{best} <=> $a->{best}
                                   || $a->{targ} cmp $b->{targ} } 
                map { $targs{$_} } @{$lDat->{targ}};
                my $lRS = $lDat->{tNum} == 1 ? '' :
                    " rowspan='$lDat->{tNum}'";
                my $lID = $lDat->{loc};
                my $lNS = $lDat->{ns};
                my @taxa = &taxa( $lID, $lNS, $lDat );
                my $oneTax = $taxa[0];
                push @hgPicker, [$l, $prefSpec{$oneTax} || 999, $oneTax,
                                 &clean_taxa_token($oneTax) ];
                $lRS = "" if ($lNS ne 'LL');
                print $outFH "<!-- $lID $lNS |$lRS| -->\n";

                # my $locGOs = &go_terms( $lID, $lNS );
                my @goBaits = ([$lID, $lNS]);
                push @goBaits, map {[ $_->{targ}, $_->{ns} ] } @targets;
                my $fullGos;
                foreach my $bait (@goBaits) {
                    $fullGos = &go_terms( @{$bait}, $fullGos);
                }
                for my $t (0..$#targets) {
                    my $tDat = $targets[$t];
                    my $tID = $tDat->{targ};
                    my $tNS = $tDat->{ns};
                    my $isFirst = ($l || $t) ? 0 : 1;

                    print $outFH "<tr>\n";
                    if($isFirst) {
                        # This is the first row in a homologene group
                        # Print $OutFH Homologene spanning cell
                        my $title = "No HomoloGene Assigned";
                        my $HG    = $hDat->{hg};
                        $hgBit    = $hgBit ? 0 : 1;
                        my $hgcl  = $hgBit;
                        if ($HG =~ /^UNK/) {
                            $hgcl = 'u';
                        } else {
                            $title = $HG;
                            if (my $hdesc = $descr{$HG}) {
                                $title .= " - $hdesc";
                            }
                        }
                        my $rs = $hDat->{tNum};
                        printf($outFH " <td title='%s'%s class='hg%s'>&nbsp;</td>\n",
                               $title, ($rs == 1) ? '' : " rowspan='$rs'",
                               $hgcl);
                    }
                    #my $sym = &symbol($lID, $lNS, $lDat);
                    #$sym =~ s/[\*~]$//;
                    if ($lNS ne 'LL' || !$t) {
                        # Print $OutFH locus cell
                        if ($lNS eq 'LL') {
                            my $lnk = sprintf($hopLink,$lNS,$lID,$lID);
                            printf($outFH " <td%s>%s</td>\n", $lRS, $lnk);
                        } else {
                            printf($outFH " <td%s></td>\n", $lRS);
                        }
                        printf($outFH " <td%s>%s</td>\n", $lRS, &annotated_symbol
                               ($lID, $lNS, $lDat));
                        printf($outFH " <td%s>%s</td>\n", $lRS, join
                               (',', map { &taxa_token($_) } @taxa));
                        my $tc = &target_class( $fullGos );
                        $tc = "\n$tc " if ($tc);
                        printf($outFH " <td%s>%s</td>\n", $lRS, $tc );
                    }
                    #my $tc = &target_class(&go_terms($tID,$tNS,$locGOs));
                    #$tc = "\n$tc " if ($tc);
                    #print $outFH " <td>$tc</td>\n";

                    # Print $OutFH the specific protein target
                    my $tLnk = sprintf($hopLink,$tNS, $tID, $tID);
                    my $other = $lDat->{other}{$tID} || [];
                    unless ($#{$other} == -1) {
                        $tLnk .= sprintf("<span title='May also map to %s' class='note'>+%d</span>", join(',', sort @{$other}), $#{$other} + 1);
                        $notes{$multLoc} ||=
                            "Multiple loci mapped from target, only best shown";
                    }
                    print $outFH " <td>$tLnk</td>\n";

                    my $scCell = "";
                    my $scCl   = "";
                    if ($lNS eq 'LL') {
                        my $lsc = $lDat->{map}{$tID};
                        if ($lsc == -1) {
                            $scCell = $unDef;
                            $notes{$unDef} ||=
                                "Assignment is of unknown quality";
                        } else {
                            $scCell = sprintf("%d%%", int(0.5 + 100 * $lsc));
                        }
                    } else {
                        $scCell = "N/A";
                        $scCl   = " class='ntxt'";
                    }
                    print $outFH " <td$scCl>$scCell</td>\n";

                    my @actBits;
                    my %pmidH;
                    foreach my $key (sort keys %{$tDat->{cmpd}}) {
                        my ($tag, $sc, $auth) = split(/\t/, $key);
                        my (@srcs, @ehRows);
                        foreach my $srcId (@{$tDat->{cmpd}{$key}}) {
                            push @srcs, $mtKey{$srcId};
                            unless ($tDat->{eh}{$srcId}) {
                                push @ehRows, $tDat->{eh}{$srcId} ||= 
                                    &new_cmpd_eh_row($srcId, $sns);
                            }
                        }
                        foreach my $ehr (@ehRows) {
                            $ehr->{Protein}{$tID} = 1;
                            # $ehr->{Symbol} = $sym;
                            push @{$ehr->{$tag}}, $sc;
                            push @{$ehr->{Score}}, $sc;
                            map { $ehr->{Authorities}{$_}++
                                  } split(/ \+ /, $auth);
                            # $tDat->{eh}{Score} = [ $tDat->{best} ];
                            map {$ehr->{Queries}{$_}++} @{$ccDat->{queries}};
                            map {$ehr->{Species}{$_}++} @taxa;
                        }

                        my $act = "";
                        $act .= &showtag($tag);
                        my $vf = $valFormats->{$tag || ''};
                        $vf    = $vf ? $vf->[0] : \&defaultfmt;
                        $act .= ($sc == -1) ? '?' : &{$vf}($sc);
                        $act .= " <span class='auth'>$auth</span>";
                        $act .= sprintf ($mkeyFrm, join
                                         (',', sort { $a <=> $b } @srcs));
                        push @actBits, $act;
                        foreach my $pmid (keys %{$tDat->{pmid}{$key}}) {
                            $pmidH{$pmid} = 1;
                            $pmid =~ s/^PMID://;
                            map { $_->{PubMed}{$pmid} = 1 } @ehRows;
                        }
                    }
                    &bubble_up($tDat, $lDat);
                    print $outFH " <td style='white-space: nowrap;'>".
                        join("<br />", @actBits)."</td>\n";

                    # Pubmed
                    my $pmidTxt = &_pubmed_link( [keys %pmidH] );
                    print $outFH " <td style='text-align:center'>$pmidTxt</td>\n";


                    printf($outFH " <td>%s</td>\n", &truncate_text
                           ($ad->description(-id => $tID, -ns => $tNS)));
                    print $outFH "</tr>\n";
                    &eh_prot( $tDat, $ccDat );
                }
                # End of $lDat loop
                &eh_locus($lDat, $ccDat) if ($lDat->{ns} eq 'LL');;
                &bubble_up($lDat, $hDat);
            }
            # &prebranch(\@hgPicker);
            if ($#hgPicker > -1) {
                @hgPicker = sort { $a->[1] <=> $b->[1] ||
                                       $a->[2] cmp $b->[2] } @hgPicker;
                my $tax = [ map { $_->[3] } @hgPicker ];
                my $hgp = $hgPicker[0];
                my $loc = $locs[ $hgp->[0] ];
                my $lm  = $loc->{meta} ||= {};
                my $desc = $lm->{desc} || "";
                my $lt   = $hgp->[2];
                $desc = "[$lt] $desc" if ($desc && $lt);
                $hDat->{meta} = {
                    tax => $tax,
                    sym => $lm->{sym} || "",
                    desc => $desc,
                };
            } else {
                $hDat->{meta} = {
                    tax => '?',
                    sym => '?',
                    desc => '?',
                };
            }
            
            &eh_hg($hDat, $ccDat) if ($hDat->{ns} eq 'HG');
        }
        print $outFH "</tbody></table><hr />\n\n";
    }
    unless ($#unkChem == -1) {
        print $outFH "<p class='unk'><b>IDs without SMILES data:</b> ".
            join(', ', @unkChem)."</p>\n";
    }
}

sub objectsToHg {
    my ($ids, $ns, $targData) = @_;
    return if (!$ids || $#{$ids} == -1);
    my $hgs = &forked_data
        ( -id => $ids, -ns1 => $ns, -ns2 => 'HG', -nonull => 1,
          -nullscore  => -1,
          -output => "$gasDir/$ns-toHomoloGene.tsv",
          -cols   => 'term_in,term_out,matched');
    foreach my $row (@{$hgs}) {
        my ($id, $hg, $sc) = @{$row};
        next unless ($hg);
        $targData->{$id}{hg}{$hg} = $sc if 
            (!defined $targData->{$id}{hg}{$hg} ||
             $targData->{$id}{hg}{$hg} < $sc);
    }
}

sub fasta4seq {
    my ($id, $ns) = shift;
    $ns ||= 'SEQ';
    my @names;
    foreach my $name ( sort { uc($a) cmp uc($b) } $ad->convert
                       ( -id => $id, -ns1 => $ns, -ns2 => 'AC', 
                         -directonly => 1 )) {
        next if ($name =~ /^MTID/);
        push @names, $name;
    }
    my $fasta = ">".join(" ", @names);
    $fasta =~ s/  +/ /g;
    my $name  = $mt->get_seq( -id => $id, -defined => 1);
    return $fasta . "\nN" unless ($name);
    $name = $name->name;
    my $nl = length($name);
    for (my $i = 0; $i < $nl; $i += $fBlock) {
        $fasta .= "\n" . substr($name, $i, $fBlock);
    }
    return $fasta;
}

sub go_terms {
    my ($id, $ns, $prior, $isolateInput) = @_;
    $ad->bench_start("Get GO terms");
    my %gos;
    if ($prior) {
        # De-reference passed GO hash
        while ( my ($go, $x) = each %{$prior}) {
            $gos{$go} = [ $x->[0], [@{$x->[1]}],  [@{$x->[2]}], $x->[3]];
        }
    }

    my @params = (-id        => $id,
                  -ns1       => $ns,
                  -ns2       => 'GO',
                  -nonull    => 1,
                  -nullscore => -1,
                  -columns   => ['term_in', 'term_out', 'matched', 'auth' ],
                  @convCom);
    my $rows;
    if (ref($id)) {
        # Multiple IDs passed
        $ad->bench_start("Bulk GO Map");
        my $file  = "$gasDir/GO-termfile.tsv";
        unlink($file) if (-e $file);
        $rows = &forked_data( @params, -output => $file );
        chmod(0666, $file);
        $ad->bench_end("Bulk GO Map");
    } else {
        $rows = $ad->convert( @params );
    }

    foreach my $gdat (@{$rows || []}) {
        my ($term, $go, $sc, $auth) = @{$gdat};
        my $targ = $isolateInput ? $gos{$term}{$go} ||= [] : $gos{$go} ||= [];

        if ($auth =~ /^(\S+)/) { $auth = $1 }
        if (defined $targ->[0] && $targ->[0] >= $sc) {
            # This hit is no better than one already found
            if ($targ->[0] == $sc) {
                # ... it is the same as prior hits
                push @{$targ->[1]}, $auth;
                push @{$targ->[2]}, $term;
            }
        } else {
            # Either this is the first time we saw this term, or we have
            # just found a better score for it.
            $targ->[0] = $sc;
            $targ->[1] = [$auth];
            $targ->[2] = [$term];
        }
        # If the node is flagged as being inherited, then it is absolutely
        # internal (not a leaf):
        $targ->[3] ||= ($auth =~ /Inheritance/) ? 1 : 0;
    }
    my @hashes = $isolateInput ? values %gos : (\%gos);
    foreach my $hash (@hashes) {
        # die $args->branch($hash);
        my @checkLeaves;
        map { push @checkLeaves, $_ unless ($hash->{$_}[3]) } keys %{$hash};
        foreach my $go (@checkLeaves) {
            my $kids = $go2kid{$go};
            unless ($kids) {
                my @arr = $ad->direct_genealogy( $go, 1, 'GO' );
                $kids = $go2kid{$go} = \@arr;
            }
            for my $i (0..$#{$kids}) {
                if (exists $hash->{ $kids->[$i] }) {
                    # This node is not a leaf
                    $hash->{$go}[3] = 1;
                    last;
                }
            }
        }
    }
    $ad->bench_end("Get GO terms");
    return \%gos;
}

sub target_class {
    my ($gos) = @_;
    my @classes;
    while ( my ($go, $tok) = each %goodGO) {
        my $gdat = $gos->{$go};
        next unless ($gdat);
        my ($sc, $auths, $terms) = @{$gdat};
        next unless ($sc);
        my ($auth) = $ad->simplify_authors(@{$auths});
        $seenEC{$auth}++;
        $seenGO{$go}++;
        my %uniq = map { $_ => 1 } @{$terms};
        push @classes, sprintf
            ("  <b title='Via %s'>%s</b><sup class='gotxt %s'>%s</sup>\n",
             join(',', sort keys %uniq), $tok, &scClass($sc), $auth);
    }
    return join('', @classes) || '';
}

sub annotated_symbol {
    my $meta = &meta( @_ );
    my $sym  = $meta->{sym};
    if ($sym =~ s/\*$/$unOff/) {
        $notes{$unOff} ||= "Unoffical gene symbol";
    }
    return $sym || '';
}

sub smiles_description {
    my ($smi, $ns) = @_;
    my $desc  = $ad->description( -id => $smi, -ns => $ns || 'smi' );
    if ($desc =~ /Free Text: (.+)/) {
        my @bits = split(/ \/ /, $1);
        map { s/hydrochloride$/HCl/i;
              s/\[\d+[A-Z][a-z]?\]\-//g; } @bits;
        my %uniq = map { $_ => 1 } @bits;
        @bits = sort { length($a) <=> length($b)  || uc($a) cmp uc($b) }
        keys %uniq;
        while ($#bits > 1) {
            last if (length($bits[-1]) < 30);
            pop @bits;
        }
        $bits[-1] =~ s/([\-\]\)]+)/$1$brkPt/g
            if ($#bits != -1 && length($bits[-1]) > 30);
        return join(' / ', @bits) || '';
    } else {
        return $desc || '';
    }
}

sub lookup_conversion {
    my $rows = $ad->convert
        ( -nonull => 1, -columns => ['term_in','term_out', 'matched'],
          @_, @convCom);
    my %lu;
    foreach my $row (@{$rows || []}) {
        my ($in, $out, $sc) = @{$row};
        $sc = -1 unless (defined $sc);
        $lu{$in}{$out} = $sc unless
            (defined $lu{$in}{$out} && $lu{$in}{$out} > $sc);
    }
    return \%lu;
}

sub simple_lookup {
    my $lu = &lookup_conversion( @_ );
    my @primary = keys %{$lu};
    foreach my $p (@primary) {
        my $hash = $lu->{$p};
        my ($best) = sort { $hash->{$b} <=> $hash->{$a} } keys %{$hash};
        $lu->{$p} = $best;
    }
    return $lu;
}

sub process_Unknown {
    my ($ids) = @_;
    print $outFH "<h2>Unknown Requests</h2>\n";
    print $outFH "<p class='null'>The following queries were not recognized as either chemical or biological entities</p>\n";
    print $outFH join(', ', map { "<span class='query'>$_</span>" } &fancySort($ids))."<br />\n";
}

sub taxa {
    my $meta = &meta( @_ );
    my $rv = $meta->{tax} || [];
    return wantarray ? @{$rv} : $rv;
}

sub symbol {
    my $meta = &meta( @_ );
    return $meta->{sym} || "";
}

sub desc {
    my $meta = &meta( @_ );
    return $meta->{desc} || "";
}

sub meta {
    my ($id, $ns, $obj) = @_;
    return $obj->{meta} if ($obj && $obj->{meta});
    return {} unless ($id);
    $ns = $ad->guess_namespace($id) || "UNK" unless ($ns);
    my $rv = $metaCache{$ns}{$id};
    return $rv if ($rv);
    $ad->bench_start("One-off Meta Calculation");
    my @taxa = $ad->convert
        ( -id => $id, -ns1 => $ns, -ns2 => 'TAX');
    $rv = $metaCache{$ns}{$id} = {
        tax => \@taxa,
        sym => $ad->best_possible_symbol( $id, $ns, 'trunc warn poor short'),
        desc => $ad->description( -id => $id, -ns => $ns ),
    };
    $ad->bench_end("One-off Meta Calculation");
    return $rv;
}

sub chem_meta {
    my ($id, $ns, $obj) = @_;
    return $obj->{meta} if ($obj && $obj->{meta});
    return {} unless ($id);
    $ns = $ad->guess_namespace($id) || "UNK" unless ($ns);
    my $rv = $metaCache{$ns}{$id};
    unless ($rv) {
        # lazy
        &bulk_chem_meta( [$id], $ns );
        $rv = $metaCache{$ns}{$id};
    }
    return $rv;
}

sub bulk_chem_meta {
    my ($ids, $ns, $locCache) = @_;
    my $mcn   = $metaCache{$ns} ||= {};
    my (@needMeta);
    foreach my $id (@{$ids}) {
        push @needMeta, $id unless ($mcn->{$id});
    }
    unless ($#needMeta == -1) {
        $ad->bench_start("Bulk Meta Calculation");
        my $round = ++$globalIter;
        my $sfile = sprintf("%s/Sets-%s-%d.tsv", $gasDir, $ns, $round);
        my $sets = &forked_data
            ( -ids => \@needMeta, -ns1 => $ns, -ns2 => 'SET',
              -output => $sfile,
              -nonull => 1, -age => $chemAge,
              -cols => 'termin,termout');
        foreach my $row (@{$sets}) {
            my ($id, $set) = @{$row};
            my $key = "Sets";
            if ($set =~ /^(DrugBank|ICCB)\s+(.*)/) {
                $key .= " ($1)";
                $set  = $2;
            }
            $mcn->{$id}{alias}{$key}{$set} = 1;
        }
        my $afile = sprintf("%s/Alias-%s-%d.tsv", $gasDir, $ns, $round);
        my $alis = &forked_data
            ( -ids => \@needMeta, -ns1 => $ns, -ns2 => 'AC',
              -nonull => 1, -age => $chemAge,
              -auth => '!MoreGeneric,!MoreSpecific',
              -output => $afile,
              -cols   => 'termin,termout' );
        foreach my $row (@{$alis}) {
            my ($id, $ali) = @{$row};
            my $ns = $ad->guess_namespace($ali);
            my $nsn = $ns ? 
                $nsNameCache{$ns} ||= $ad->namespace_name($ns) : 'Various';
            # We have already expanded SMILES IDs to appropriate isomeric or
            # non-isomeric forms. We do not want to expand them again, as this
            # has the potential to branch into structures explicitly different
            # than the queries.
            next if ($nsn eq 'SMILES ID');
            if ($ns eq 'BMSC' && $ali =~ /^([A-Z]+)\-(\d+)$/) {
                # Normalize BMS Ids
                $ali = sprintf("%s-%06d", $1, $2);
            }
            $mcn->{$id}{alias}{$nsn}{$ali} = 1;
        }

        # Map alias hashes into arrays:
        foreach my $id (@needMeta) {
            my $mcni = $mcn->{$id} ||= {};
            foreach my $type (keys %{$mcni->{alias}}) {
                my @arr = sort keys %{$mcni->{alias}{$type}};
                $mcni->{alias}{$type} = \@arr;
            }
            $mcni->{mtid}     = $id if ($id =~ /^MTID/);
            $mcni->{best}     = $ad->best_compound_id( $mcni->{alias} );
            $mcni->{smiles} ||= $mt->get_seq(-id => $id,-defined => 1)->name();
        }
        &_desc_meta( $ns, $ids, $round );
        $ad->bench_end("Bulk Meta Calculation");
    }
    foreach my $id (@{$ids}) {
        my $m = $mcn->{$id} ||= {};
        $m->{desc}  ||= "";
        $m->{best}  ||= $id;
        $m->{alias} ||= {};
        $locCache->{$id}{meta} = $m if ($locCache);
    }
    # &prebranch($mcn);
}

sub _desc_meta {
    my ($ns, $ids, $round) = @_;
    my $dfile = sprintf("%s/Desc-%s-%d.tsv", $gasDir, $ns, $round);
    my $dDat = &forked_data
        ( -ids => $ids, -ns1 => $ns, -mode => 'desc',
          -output => $dfile,
          -min    => 1,
          -cols   => ['termin,desc']);
    foreach my $row (@{$dDat}) {
        if (my $id = $row->[0]) {
            $metaCache{$ns}{$id}{desc} = $row->[1] || "";
        }
    }
}

sub bulk_meta {
    my ($ns, $ids, $locCache) = @_;
    my (@needMeta);
    foreach my $id (@{$ids}) {
        push @needMeta, $id unless ($metaCache{$ns}{$id});
    }
    unless ($#needMeta == -1) {
        $ad->bench_start("Bulk Meta Calculation");
        my $round = ++$globalIter;
        my $tfile = sprintf("%s/Taxae-%s-%d.tsv", $gasDir, $ns, $round);
        my $taxDat = &forked_data
            ( -ids => \@needMeta, -ns1 => $ns, -ns2 => 'TAX',
              -output => $tfile,
              -min    => 1,
              -cols   => ['termin,termout']);
        foreach my $row (@{$taxDat}) {
            my $tax = $row->[1];
            next unless ($tax);
            if (my $id = $row->[0]) {
                push @{$metaCache{$ns}{$id}{tax}}, $tax;
            }
        }
        &_desc_meta( $ns, $ids, $round );
        my $sym = $ad->bulk_best_symbol( -ids => \@needMeta, -ns => $ns,
                                         -opts => 'trunc warn poor short');
        while (my ($id, $arr) = each %{$sym}) {
            $metaCache{$ns}{$id}{sym} = join(',', @{$arr}) || "";
        }
        $ad->bench_end("Bulk Meta Calculation");
    }
    foreach my $id (@{$ids}) {
        my $m = $metaCache{$ns}{$id} ||= {};
        $m->{tax} ||= [];
        $m->{sym} ||= "";
        $m->{desc} ||= "";
        $locCache->{$id}{meta} = $m if ($locCache);
    }
}

sub clean_taxa_token {
    my $taxa = shift || "";
    unless ($taxToks{$taxa.'!'}) {
        my $hack = $taxa;
        $hack =~ s/Human immunodeficiency virus type (\d+)/HIV-$1/;
        $hack =~ s/\([^\)]+\)//g;
        my $tok;
        if ($hack =~ /(H[CPI]V(-\d+)?)/) {
            $tok = $1;
        } elsif ($taxa eq 'Macaca mulatta') {
            # Prevent mouse collision with Mm
            $tok = "Rhe";
        } else {
            my @bits = split(/\s+/, $hack);
            $tok = join('', map { substr($_,0,1) } splice(@bits,0,2));
        }
        $taxToks{$taxa.'!'} = $tok
    }
    return $taxToks{$taxa.'!'};
}

sub taxa_token {
    my $taxa = shift || "";
    unless ($taxToks{$taxa}) {
        my $tok = &clean_taxa_token( $taxa );
        my $num = ++$tokCount{$tok};
        my $tit = $taxa;
        $tit    =~ s/\'/&apos;/g;
        if ($num > $#taxCols) {
            $num = 0;
        }
        $taxToks{$taxa} = sprintf("<span class='tax%d' title='%s'>%s</span>",
                                  $num, $tit, $tok);
    }
    return $taxToks{$taxa};
}

sub all_keys {
    return "" unless ($format eq 'html');

    my @keyCols;
    my $colNum = 2;
    my $kcount = 0;
    foreach my $ktxt (&filter_key(), &onto_key(), &score_key(),
                      &taxa_key(), &note_key()) {
        next unless $ktxt;
        my $cn = $kcount++ % $colNum;
        $keyCols[$cn] ||= "";
        $keyCols[$cn] .= $ktxt;
    }
    return "" if ($#keyCols == -1);
    my $html = "<table><tbody>\n<tr><td valign='top'>\n";
    $html .= join("</td><td valign='top'>\n", @keyCols);
    $html .= "</td></tr></tbody></table>\n";
    return "<hr />".$html;
}

sub taxa_key {
    my %byTok;
    map { push @{$byTok{ $taxToks{$_} }}, $_ } sort keys %taxToks;
    my @toks = sort { lc($byTok{$a}[0]) cmp lc($byTok{$b}[0]) } keys %byTok;
    return "" if ($#toks == -1);
    my $html = "<table class='tab'><caption>Genus-species (Gs) Key</caption><tbody>\n";
    $html .= " <tr>".join('', map {"<th>$_</th>"} ('', 'Scientific Name', "Common Name"))."</tr>\n";
    foreach my $tok (@toks) {
        my @taxa = @{$byTok{$tok}};
        next if ($taxa[0] =~ /\!$/);
        my @com  = map { map { $_->each_alias('GENBANK COMMON NAME') } $mt->get_taxa($_) } @taxa;
        $html .= sprintf(" <tr><th>%s</th><td>%s</td><td class='ntxt'>%s</td></tr>\n", 
                         $tok, join('<b>OR</b>', @taxa), join(', ', @com) || '');
    }
    $html .= "</tbody></table>\n";
    return $html;
}

sub note_key {
    my @notez = sort {$notes{$a} cmp $notes{$b} }keys %notes;
    return "" if ($#notez == -1);
    my $html = "<table class='tab'><caption>Alert Key</caption><tbody>\n";
    $html .= " <tr>".join('', map {"<th>$_</th>"} ('', "Alert Meaning"))."</tr>\n";
    foreach my $n (@notez) {
        $html .= "<tr><th>$n</th><td class='ntxt'>$notes{$n}</td></tr>\n";
    }
    $html .= "</tbody></table>\n";
    return $html;
}

sub filter_key {
    my @tags = sort keys %filterCount;
    return "" if ($#tags == -1);
    my $html = "";
    foreach my $tag (sort { uc($a) cmp uc($b) } keys %filters) {
        my $rm = $filterCount{$tag};
        next unless ($rm);
        my $fv = $filters{$tag};
        my $vf = $valFormats->{$tag || ''};
        next unless ($fv && $vf);
        $html .= sprintf(" <tr><th>%s</th><td>%s</td><td>%d</td></tr>\n",
                         &showtag($tag), &{$vf->[0]}( $fv ), $rm);
    }
    if ($html) {
        $html = "<table class='tab' id='fkey'>".
            "<caption>Assays hidden by user filters</caption><tbody>\n".
            " <tr><th>Filter</th><th>Value</th><th>Removed</th></tr>\n".
            $html .
            "</tbody></table>\n";
    }
    return $html;
   
}

sub onto_key {
    my @ontos = sort { $goodGO{$a} cmp $goodGO{$b} } keys %seenGO;
    my @ecs   = sort keys %seenEC;
    return "" if ($#ontos == -1 && $#ecs == -1);
    my $html = "<table class='tab'><caption>GeneOntology Classes</caption><tbody>\n";
    unless ($#ontos == -1) {
        $html .= " <tr>".join('', map {"<th>$_</th>"} ('', "Term","Description"))."</tr>\n";
        foreach my $go (@ontos) {
            $html .= sprintf
                (" <tr><th>%s</th><td>%s</td><td class='ntxt'>%s</td></tr>\n",
                 $goodGO{$go}, $go, $ad->description( -id => $go, -ns => 'GO'));
        }
    }
    unless ($#ecs == -1) {
        $html .= " <tr><th></th><th colspan='2'>Evidence Code (count)</th></tr>\n";
        foreach my $ec (@ecs) {
            $html .= sprintf(" <tr><th>%s</th><td colspan='2'><span class='ntxt'>%s</span> (%d)</td></tr>\n",
                             $ec, $ad->description( -id => $ec, -ns => 'EC') || '',
                             $seenEC{$ec});
            
        }
    }
    $html .= "</tbody></table>\n";
    return $html;
}

sub score_key {
    my @scs = (0..$#scoreClass);
    return "" if ($#scs == -1);
    my $html = "<table class='tab'><caption>Score Markup</caption><tbody>\n";
    $html .= " <tr>".join('', map {"<th>$_</th>"} ("Color","Confidence Range"))."</tr>\n";
    foreach my $i (@scs) {
        $html .= sprintf
            (" <tr><th class='%s'>ABCD</th><td>%s</td></tr>\n",
             $scoreClass[$i][1],$scoreClass[$i][2]);
    }
    $html .= "</tbody></table>\n";
    return $html;
}

sub help {
    return "" if ($nocgi || $format ne 'html');
    return $args->tiddly_link( @_ );
}

sub DOC_START {
    if ($format eq 'xml') {
        print $outFH "<?xml version='1.0' encoding='UTF-8'?>\n";
        my %data = ( contact => 'charles.tilford@bms.com',
                     date    => `date`,);
        print $outFH "<chembioset ".&hash2attr(\%data).">\n";
        $xmlInd++;
        return;
    }
    return if ($nocgi);
    my $mime = 'html';
    $args->set_mime( -mime => $mime,
                     -codeurl  => "http://bioinformatics.bms.com/biocgi/filePod.pl?module=_MODULE_&highlight=_LINE_&view=1#Line_LINE_",
                     -errordir => '/docs/hc/users/tilfordc/',
                     -redirect => $args->val('redirect'));
    $args->ignore_error("Expat.pm line 456");
    print $outFH "<html>\n<head>\n";
    print $outFH "<title>Chem-Bio Hopper</title>\n";
    print $outFH "<script src='/biohtml/javascript/chemBioHopper.js'></script>\n";
    print $outFH "<link type='text/css' rel='stylesheet'\n".
        "      href='/biohtml/css/chemBioHopper.css' />\n";
    print $outFH "<style>\n";
    for my $t (0..$#taxCols) {
        printf($outFH " .tax%d { color: %s; }\n", $t, $taxCols[$t]);
    }
    print $outFH "</style>\n";
    print $outFH " <link rel='shortcut icon' href='/biohtml/images/Bunny-16x16.png'>\n";
    print $outFH "</head>\n<body>\n";
    print $outFH "<center><span style='font-size:2em; font-weight:bold; color:orange;'>Chem-Bio Hopper</span><br />";
    print $outFH "<p style='color:red'>" . &help('BetaSoftware') . 
            "*** THIS VERSION IS BETA SOFTWARE ***</p>\n" if ($isBeta);
    print $outFH &help('SoftwareOverview','[Software Overview]');
    print $outFH "</center>\n";
}

sub HTML_FORM {
    return if ($nocgi);    

    print $outFH "<table class='tab'><tbody>\n";
    print $outFH "<tr>\n";
    print $outFH " <th>Enter one or more IDs<br /><span class='ntxt'>(chemical and/or biological)</span></th>\n";
    print $outFH "<th>Example Searches</th>\n";
    print $outFH "</tr><tr><td>\n";
    print $outFH "<form method='post'>\n";
    print $outFH "<textarea style='background-color:#cfc; background-position: bottom right; background-repeat: no-repeat; background-image:url(/biohtml/images/jumpingBunny.gif)' name='ids' cols='40' rows='10'>";
    print $outFH $idText || "";
    print $outFH "</textarea><br />\n";
    print $outFH &help('SpeciesFilter')."Filter By Species: ".
        "<input type='text' style='width: 15em' name='taxa' value='$taxa' />".
        "<br />\n";

    print $outFH &help('QueryLimit')."Limit hits per query to: ".
        "<input type='text' style='width: 8em' name='limit' value='$limit' />".
        " (0 = no limit)<br />\n";

    print $outFH &help('ExpandLimit')."Expand queries by at most: ".
        "<input type='text' style='width: 8em' name='expand' value='$expLim' />".
        " (0 = no limit)<br />\n";

    print $outFH &help('AgeLimit')."Refresh any data older than ".
        "<input type='text' style='width: 3em' name='agerequest' value='$ageReq' />".
        " days (0 = force update)<br />\n";

    print $outFH &help('BmsInventory').
        "<input type='checkbox' name='inventory'".
        ($checkInv ? '' : " CHECKED='CHECKED'")."/>".
        " Report compound availability in BMS inventory<br />\n";

    my @pts = sort { uc($a) cmp uc($b) } keys %{$valFormats};
    my (@head, @input);
    foreach my $pt (@pts) {
        next unless ($pt);
        my ($func, $def, $type, $key) = @{$valFormats->{$pt}};
        my $val = defined $args->{$key} ? $args->{$key} : $def;
        if ($type eq 'log') {
            push @input,
            "&le; 10<sup>-<input name='$key' value='$val' style='width: 2em' /></sup>";
        } elsif ($type eq 'perc') {
            push @input, "&ge; <input name='$key' value='$val' style='width: 3em' />\%";
        } elsif ($type eq 'invperc') {
            push @input, "&le; <input name='$key' value='$val' style='width: 3em' />\%";
        } else {
            next;
        }
        push @head, $pt;
    }
    unless ($#head == -1) {
        print $outFH &help('PotencyFilter')."Potency Filters:\n";
        print $outFH "<table class='tab'><tbody>\n";
        print $outFH " <tr>\n".join('', map { "  <th>$_</th>\n" } @head)." </tr>\n";
        print $outFH " <tr>\n".join('', map { "  <td>$_</td>\n" } @input)." </tr>\n";
        print $outFH "</tbody></table>\n";
    }
    print $outFH "Undefined potencies are ".
        "<input type='radio' name='keepundef' value='0'".
        ($args->{KEEPUNDEF} ? '' : " CHECKED='CHECKED'"). " /> Ignored ".
        "<input type='radio' name='keepundef' value='1'".
        ($args->{KEEPUNDEF} ? " CHECKED='CHECKED'" : ''). " /> Kept<br />\n";

    print $outFH "<center><input class='butt' name='go' type='submit' value='Hop!' /></center>\n";
    # print $outFH "<span style='background-color:yellow; color: red'><b>IN DEVELOPMENT</b><br />Currently information is only presented when querying compounds; Biological entities are being worked on...</span>\n"; 
    print $outFH "</form>\n";
    print $outFH "</td><td>\n";
    my $examples = {
        "LocusLink Gene"    => [ qw(LOC497756 LOC10280) ],
        "Protein ID"        => [ qw(NP_036236 IPI00307155 P03366) ],
        "Chemical Abstract" => [ "CAS:100937-52-8","CAS:129618-40-2" ],
        "Gene Symbol"       => [ qw(DPP4 P2RY1 Gpr30 SLC6A2) ],
        "Free Text"         => [ "Taxol", "Ifenprodi","dasatinib" ],
        "SMILES ID"         => [ "MTID:66322065","MTID:194949915" ],
        "Raw SMILES"        => [ "CC(=O)OCC[N](C)(C)C" ],
        "Affy Probe Set"    => ["94635_at","32983_at"],
        "Gene Ontology"     => ["GO:0045028"],
        "BMS ID"            => ["SQ-008370","DPH-068980","BMY-021915"],
        "Aureus ID"         => [qw(AUR9553 AUR100145)],
        "Wombat ID"         => ["SMDL-00005886","SMDL-00047760"],
        "PubChem ID"        => ["CID:23616954","SID:16694925"],
        "MDL ID"            => ["MFCD00066294","MFCD00242885"],
        "RNAi Reagent"      => ["TRCN0000010353"],
    };
    print $outFH "<table class='tab'><tbody>\n";
    foreach my $ns (sort keys %{$examples}) {
        my @arr = sort @{$examples->{$ns}};
        next if ($#arr == -1);
        print $outFH "<tr><th>$ns</th><td>\n";
        print $outFH join(' | ', map { sprintf($hopLink, $ns, $_, $_) } @arr);
        print $outFH "</td></tr>\n";
    }
    print $outFH "</tbody></table>\n";
    print $outFH "<span class='ntxt'>... and others!</span>\n";
    print $outFH "</td></tr>\n";
    print $outFH "</tbody></table>\n";

    print $outFH "<p class='ntxt'>Analysis can be <b>VERY</b> slow the first time a query is presented.<br />Please be patient; requesting a previously analyzed query should be rapid.</p>\n";
}
sub DOC_END {
    
    if ($globalEh) {
        &chem_excel();
        $globalEh->close;
        my $file = $globalEh->file_path;
        my $url = $args->path2url($file);
        $globalEh->url($url);
        if ($nocgi || $outFile) {
            $args->msg("Your Excel file was written to:", $file);
        } else {
            print $outFH $globalEh->html_summary;
            # print $outFH "<script>alert('$url')</script>\n";
            print $outFH "<script>document.location = '$url'</script>\n"
                if ($url && $url !~ /\'/);
        }
        
    }

    if ($doBench) {
        if ($nocgi) {
            &msg( $ad->showbench(-minfrac => $doBench ) );
        } elsif ($format eq 'html') {
            #print $outFH "<pre color='#686'>";
            print $outFH $ad->showbench( -minfrac => $doBench, -html => 1, -class => 'tab');
            #print $outFH "</pre>\n";
        }
    }
    if ($#badErr != -1) {
        die join("\n", @badErr);
    }
    if ($format eq 'html') {
        print $outFH "</body>\n</html>\n";
    } elsif ($format eq 'xml') {
        print $outFH "</chembioset>\n";
        $xmlInd--;
    }
    close OUTF if ($outFile);
}

sub msg {
    my $tw = shift;
    if ($format eq 'html') {
        print $outFH "<div class='msgh'>".&help($tw)."</div>\n";
        print $outFH "<div class='msgb'>".join("<br />\n", @_)."</div>";
        print $outFH "<br style='clear:both' />\n";
    } elsif ($format eq 'xml') {
        my $pad = ("  " x $xmlInd) || '';
        my $msg = join('', map { "$pad  $_\n" } @_);
        $msg =~ s/\<[^\>]+\>//g;
        print $outFH "$pad<msg ".&hash2attr({topic => $tw }).">\n$msg$pad</msg>\n";
    } elsif ($nocgi && $vb) {
        map { s/\<[^\>]+\>//g } @_;
        $args->msg(@_);
        return;
    }
}

sub prot_sort {
    my $list =shift;
    my @sorter;
    foreach my $item (@{$list}) {
        my $hack = lc($item);
        my $txt  = "";
        while ($hack =~ /^([a-z]+|\d+|[^a-z\d]+)/) {
            my $bit = $1;
            $hack =~ s/^\Q$bit\E//;
            if ($bit =~ /[a-z]/) {
                $txt .= sprintf("%8s", $bit);
            } elsif ($bit =~ /\d/) {
                $txt .= sprintf("%020d", $bit);
            } else {
                $txt .= $bit;
            }
        }
        push @sorter, [$item, $txt];
    }
    my @rv = map {$_->[0]} sort { $a->[1] cmp $b->[1] } @sorter;
    return wantarray ? @rv : \@rv;
}

sub fancySort {
    my $list = shift;
    my $ind = shift;
    my @sorter;
    foreach my $item (@{$list}) {
        my $mem = defined $ind ? $item->[$ind] : $item;
        if (!defined $mem) {
            push @sorter, [ '', -1, '', $item ];
        } elsif ($mem =~ /(.+?)(\d+)$/) {
            # Trailing numbers - sort by non-numeric, then numeric
            push @sorter, [$mem, $2, $1, $item];
        } elsif ($mem =~ /(.+?)(\d+)\-(\d+)$/) {
            # Swiss Prot variants
            push @sorter, [$mem, $2 + $1/1000, $1, $item];
        } elsif ($mem =~ /^(\d+)(.+?)/) {
            # Leading numbers - sort by number only
            push @sorter, [$mem, $1, '', $item];
        } else {
            push @sorter, [$mem, 0, $mem, $item];
        }
    }
    my @rv = map {$_->[3]} sort { uc($a->[2]) cmp uc($b->[2]) ||
                                  $a->[2] cmp $b->[2] ||
                                  $a->[1] <=> $b->[1] || 
                                  $a->[0] cmp $b->[0]} @sorter;
    return wantarray ? @rv : \@rv;
}

sub esc_url {
    return $escObj->esc_url( @_ );

    my $txt = shift;
    $txt =~ s/\%/\%25/g;
    while (my ($in, $out) = each %{$urlEsc}) {
        $txt =~ s/\Q$in\E/$out/g;
    }
    return $txt;
}

sub esc_xml {
    my $txt = shift;
    $txt =~ s/\&/\&amp\;/g;
    while (my ($in, $out) = each %{$htmlEsc}) {
        $txt =~ s/\Q$in\E/$out/g;
    }
    return $txt;
}

sub hash2attr {
    my ($hash) = @_;
    my @bbits;
    foreach my $param (sort keys %{$hash}) {
        my $val = $hash->{$param};
        next unless (defined $val);
        $val = join(',', @{$val}) if (ref($val));
        $val =~ s/[\t\r\n]+//g;
        $val =~ s/^\s+//;
        $val =~ s/\s+$//;
        $param =~ s/^zz\d*//; # leading zz just means sort to end
        push @bbits, sprintf("%s='%s'", $param, &esc_xml($val));
    }
    return join(' ', @bbits) || '';
}

sub forked_data {
    $args->msg_once("Working directory $gasDir");
    $ad->bench_start("Query GenAcc");
    my $gas = BMS::MapTracker::GenAccService->new( @gasCom, @_ );
    $gas->use_beta( $isBeta );
    my $rows = $gas->cached_array( 'clobber' );
    $ad->bench_end("Query GenAcc");
    return $rows;
}

sub in_to_out_hash {
    my $rows = shift;
    my %hash;
    foreach my $row (@{$rows}) {
        my ($in, $out) = @{$row};
        if ($out) {
            $hash{$in}{$out} = 1;
        } else {
            $hash{$in} ||= {};
        }
    }
    while (my ($in, $oH) = each %hash) {
        $hash{$in} = [ sort keys %{$oH} ];
    }
    return \%hash;
}

sub excel_helper {
    return $globalEh if ($globalEh);
    my $file = "$gasDir/ChemBioHopperReport.xlsx";
    my $eh = $globalEh = BMS::ExcelHelper->new( $file );
    my @acts = ('Score', 'Ki', 'IC50', 'EC50', '%KD', 
                '% Inhibition', '%Ctrl 1uM', 'Generic');
    my @sheets = ({
        name => "Homologene",
        lead => ['Homologene'],
        trail => ['Protein', 'Locus'],
    }, {
        name => "Locus",
        lead => ['Locus'],
        trail => ['Protein'],
    }, {
        name => "Protein",
        lead => ['Protein'],
    }, {
        name => "PubMed",
        cols => [qw(PubMed Date Count Title)],
    });
    foreach my $sdat (@sheets) {
        my @cols;
        my $name = $sdat->{name};
        if (my $col = $sdat->{cols}) {
            @cols = @{$col};
        } elsif (my $ld = $sdat->{lead}) {
            @cols = @{$ld};
            push @cols, qw(Symbol Compound Queries Species);
            push @cols, @acts;
            push @cols, qw(Authorities PubMed 
                           TargetDescription CompoundDescription MTID SMILES);
            if (my $tr = $sdat->{trail}) {
                push @cols, @{$tr};
            }
        }
        my @wids = map { $sMeta->{$_}[0] || 10 } @cols;
        $eh->sheet( -name    => $name,
                    -freeze  => 1,
                    -width   => \@wids,
                    -columns => \@cols, );
        $eh->{CBH_ORDER}{$name} = \@cols;
    }

    my $colScale  = 10;
    for (my $i = 0; $i <= 10; $i++) {
        my $fwd  = int(255 * $i / $colScale);
        my $bak  = 255 - $fwd;
        my $cc   = $eh->set_custom_color(40 + $i, $bak, $fwd, 0);
        my $name = "col$i";
        $eh->format( -name       => $name,
                     # -num_format => '0.00',
                     -align      => 'center',
                     -bg_color   => $cc );
    }
    $eh->format( -name     => 'colNull',
                 -align    => 'center',
                 -bg_color => 'silver' );
    
    $eh->format( -name     => 'YYYY-MM-DD',
                 -align    => 'center',
                 -num_format => 'yyyy-mm-dd' );
    $eh->format( -name     => 'YYYY-MM',
                 -align    => 'center',
                 -num_format => 'yyyy-mm' );
    $eh->format( -name     => 'YYYY',
                 -align    => 'center',
                 -num_format => 'yyyy' );
    

    return $globalEh;
}

sub prebranch {
    print "<pre>".$args->branch(@_)."</pre>";
}

sub eh_prot {
    my ($tDat, $mtid) = @_;
    my $id = $tDat->{targ};
    return unless ($id);
    my $rows = $tDat->{eh};
    return unless ($rows);
    my $eh = &excel_helper();
    # &prebranch($tDat);
    # my $sym = $ad->best_possible_symbol( $id, $, 'trunc warn poor short');
    my $meta = &meta( $id, $tDat->{ns}, $tDat);
    while (my ($cid, $ehr) = each %{$rows}) {
        next if ($eh->{CBH_DONE}{AP}{"$cid-$id"}++);
        $ehr->{Protein}{$id} = 1;
        $ehr->{Symbol}            ||= $meta->{sym} || "";
        $ehr->{Symbol}              =~ s/[\*~]$//;
        $ehr->{Species}           ||= join(',', @{$meta->{tax}});
        $ehr->{TargetDescription} ||= $meta->{desc};
        &add_eh_rows( $eh, 'Protein', $ehr);
    }
}

sub eh_locus {
    my ($lDat, $mtid) = @_;
    my $id = $lDat->{key};
    return unless ($id);
#     &prebranch($lDat);
    my $rows = $lDat->{eh};
    return unless ($rows);
    my $eh = &excel_helper();
    # &prebranch($tDat);
    # my $sym = $ad->best_possible_symbol( $id, $, 'trunc warn poor short');
    my $meta = &meta( $id, $lDat->{ns}, $lDat);
    while (my ($cid, $ehr) = each %{$rows}) {
        next if ($eh->{CBH_DONE}{LOC}{"$cid-$id"}++);
        $ehr->{Locus}{$id} = 1;
        $ehr->{Symbol}            = $meta->{sym};
        $ehr->{Symbol}            =~ s/[\*~]$//;
        $ehr->{Species}           = join(',', @{$meta->{tax}});
        $ehr->{TargetDescription} = $meta->{desc};
        &add_eh_rows( $eh, 'Locus', $ehr);
    }
}

sub eh_hg {
    my ($hDat, $ccDat) = @_;
    my $id = $hDat->{hg};
    return unless ($id);
    my $rows = $hDat->{eh};
    return unless ($rows);
    my $eh = &excel_helper();
    my $mtid = $ccDat->{mtid};
    my $meta = $hDat->{meta} || {};
    my $cns  = $ccDat->{ns};
    # &prebranch($hDat);
    while (my ($cid, $ehr) = each %{$rows}) {
        # my $cmet = &chem_meta( $cid, $cns );
        next if ($eh->{CBH_DONE}{HG}{"$cid-$id"}++);
        $ehr->{Homologene}{$id} = 1;
        $ehr->{Symbol}            = $meta->{sym};
        $ehr->{Symbol}            =~ s/[\*~]$//;
        $ehr->{Species}           = join(',', @{$meta->{tax}});
        $ehr->{TargetDescription} = $meta->{desc};
        &add_eh_rows( $eh, 'Homologene', $ehr);
    }
}

sub add_eh_rows {
    my ($eh, $sheet, $ehr, $noScore) = @_;
    my @cols = @{$eh->{CBH_ORDER}{$sheet}};
    my (@row, @fmt);
    # &prebranch($ehr);
    foreach my $col (@cols) {
        my ($val, $f);
        if ($val = $ehr->{$col}) {
            if (my $r = ref($val)) {
                if ($r eq 'HASH') {
                    my $joiner = $col eq 'SMILES' ? ' ' : ',';
                    $val = join($joiner, sort keys %{$val});
                } elsif ($noScore) {
                    $val = join(',', sort @{$val});
                } else {
                    ($val) = sort { $b <=> $a } @{$val};
                    if ($val < 0) {
                        $val = '?';
                        $f   = 'colNull';
                    } else {
                        $f = sprintf('col%d', int(0.5 + 10 * $val));
                        if (my $vf = $valFormats->{$col}) {
                            if ($vf->[2]  eq 'log') {
                                $val = &log10fmt($val);
                            } else {
                                $val = &{$vf->[0]}($val);
                            }
                        }
                    }
                    
                }
            }
        }
        if ($col eq 'Date' && $val &&
            ($val =~ /^(\d{4})\-(\d{2})\-(\d{2})$/ ||
             $val =~ /^(\d{4})\-(\d{2})$/ ||
             $val =~ /^(\d{4})$/)) {
            my ($y, $m, $d) = ($1, $2 || 1, $3 || 1);
            $f   = $d ? 'YYYY-MM-DD' : $m ? 'YYYY-MM' : 'YYYY';
            $val = ['date', sprintf("%04d-%02d-%02dT", $y, $m, $d)];
        }
            
        push @row, $val;
        push @fmt, $f;
    }
    $eh->add_row_explicit($sheet, \@row, \@fmt);
}

sub bubble_up {
    # Parents inherit from children
    my ($kid, $par) = @_;
    while (my ($mtid, $cH) = each %{$kid->{eh} || {}}) {
        my $ehr = $par->{eh}{$mtid} ||= {
            mtid => $mtid,
        };
        while (my ($col, $val) = each %{$cH}) {
            if (my $r = ref($val)) {
                if ($r eq 'ARRAY') {
                    push @{$ehr->{$col}}, @{$val};
                } else {
                    while (my ($k, $v) = each %{$val}) {
                        $ehr->{$col}{$k}{$v}++;
                    }
                }
            } else {
                $ehr->{$col} = $val;
            }
        }
    }
}

sub new_cmpd_eh_row {
    my ($mtid, $cns) = @_;
    my $cmet  = &chem_meta( $mtid, $cns );
    return {
        MTID     => { $mtid => 1 },
        Compound => { $cmet->{best} => 1 },
        SMILES   => { $cmet->{smiles} => 1 },
        CompoundDescription => $cmet->{desc},
    };
}

sub chem_excel {
    my $eh = $globalEh;
    return unless ($eh);
    my %aliNames;
    my @rows;
    foreach my $ns (sort keys %metaCache) {
        next unless ($ad->is_namespace($ns, 'AC'));
        foreach my $id (sort keys %{$metaCache{$ns}}) {
            my $m = $metaCache{$ns}{$id} || {};
            my $row = {
                Compound            => $m->{best},
                CompoundDescription => $m->{desc},
                SMILES              => $m->{smiles},
                MTID                => $m->{mtid},
            };
            while (my ($type, $alis) = each %{$m->{alias} || {}}) {
                $row->{$type} = $alis;
                $aliNames{$type}++;
            }
            push @rows, $row;
        }
    }
    my @alis  = sort keys %aliNames;
    map { $sMeta->{$_} ||= [20] } @alis;
    my $sname = "Compounds";
    my @cols  = ('Compound', 'SMILES');
    push @cols, @alis;
    push @cols, ('MTID', "CompoundDescription");
    my @wids = map { $sMeta->{$_}[0] || 10 } @cols;
    $eh->sheet( -name    => $sname,
                -freeze  => 1,
                -width   => \@wids,
                -columns => \@cols, );
    $eh->{CBH_ORDER}{$sname} = \@cols;
    foreach my $row (@rows) {
        &add_eh_rows( $eh, $sname, $row, 'noscore');
    }
}
