#!/stf/biobin/perl -w

BEGIN {
    # Needed to make my libraries available to Perl64:
    # use lib '/stf/biocgi/tilfordc/released';
    use lib '/stf/biocgi/tilfordc/patch_lib';
    # Allows usage of beta modules to be tested:
    my $prog = $0; my $dir = `pwd`;
    if ($prog =~ /(working|Beta)/ || $dir =~ /working/) {
        require lib;
        import lib '/stf/biocgi/tilfordc/perllib';
        $ENV{IS_BETA} = 1;
    } else {
        $ENV{IS_BETA} = 0;
    }
    $| = 1;
    print '';
    # print "Content-type: text/plain\n\n";
}

my $isbeta  = $ENV{IS_BETA};
my $VERSION = 
    ' $Id$ ';

use strict;
use BMS::MapTracker::StandardGeneLight;
use BMS::BmsArgumentParser;
use BMS::Utilities::Escape;
use BMS::TableReader;
use BMS::ExcelHelper;
use BMS::Utilities::FileUtilities;

my @specPriority = 
    ( 'Homo sapiens',
      'Mus musculus',
      'Rattus norvegicus',
      'Canis lupus familiaris',
      'Macaca mulatta',
      'Pan troglodytes',
      'Bos taurus',
      );

srand( time() ^ ($$ + ($$<<15)) );
my $args = BMS::BmsArgumentParser->new
    ( -nocgi          => $ENV{HTTP_HOST} ? 0 : 1,
      -errormail      => 'charles.tilford@bms.com',
      -showbest       => 10,
      -detail         => 3,
      -showfrac       => 100,
      -reliablesource => 'Accessions',
      -hasheader      => 1,
      -limit          => 30,
      -testlimit      => 0,
      -tiddlywiki     => 'GenAcc' );


my $nocgi   = $args->val(qw(nocgi));
my $limit   = $args->val(qw(limit));
my $source  = $args->val(qw(source genesource));
my $dumpSql = $args->val(qw(dumpsql));
my $taxReq  = $args->val(qw(taxa species)) || "";
my $format  = lc($args->val(qw(format)) || '');
my $doBench = $args->val(qw(benchmark showbench dobench));
my $dbname  = $args->val(qw(dbname db)) || ($isbeta ? 'sgbeta' : 'stndgene');
my $selkey  = $args->val(qw(selectkey)) || 'SgtSelect';
my $hasHead = $args->val(qw(hasheader)) ? 1 : 0;
my $detail  = $args->val(qw(detail));
$detail     = 3 unless (defined $detail);
my $ccToken = 'COLCHOICE';
my $cpToken = 'COLPICKED';

my $fu       = BMS::Utilities::FileUtilities->new(-urlcb => sub {
    my $path = shift;
    if ($path =~ /^\/stf\/(.+)/) {
        return "http://bioinformatics.bms.com/$1";
    } elsif ($path =~ /^\/home\/([^\/]+)\/public_html(.+)/) {
        return "http://bioinformatics.bms.com/~$1$2";        
    }
    return undef;
});


my $sgl = BMS::MapTracker::StandardGeneLight->new
    ( -db => $dbname );
my $esc = BMS::Utilities::Escape->new();
my $mode = "";

if ($format =~ /json/) {
    $format = 'json';
} elsif ($format =~ /gui|inter/) {
    $format = 'gui';
} elsif ($format =~ /html/) {
    $format = 'html';
} elsif ($format =~ /text|txt/) {
    $format = 'text';
} elsif ($format =~ /sel/) {
    $format = 'select';
} else {
    $format = 'xml';
}

my $gss = $sgl->all_gene_sources();
if ($nocgi) {
    $args->intercept_errors();
} elsif ($format eq 'gui') {
    $args->set_mime( );
    
} else {
    my $mime =
        $format eq 'json' ? 'json' : 
        ($format eq 'select' || $format eq 'text') ? 'text' :
        $format eq 'html' ? 'html' :
        'xml';
    $args->set_mime( -mime => $mime ) if ($mime);
    unless ($format eq 'html') {
        $args->msg_callback(sub {
        }, 'global');
        $args->err_callback(sub {
        }, 'global');
    }
}
if ($format eq 'gui') {
    &HTML_GUI;
    exit;
}

if ($format eq 'json') {
} elsif ($format eq 'select') {
} elsif ($format eq 'html') {
    print &HTML_START();
} elsif ($format eq 'xml') {
    print &XML_START();
}

my $spn        = $#specPriority;
my %specWeight = map { $specPriority[$_] => $spn + 1 - $_ } (0..$spn);

my ($mainResult, %collated, @keepSafe);
if (my $fileReq = $args->val(qw(inputfile))) {
    # Input file has been provided
    &parse_table($fileReq);
} else {
    # Look for explicitly provided queries
    my $reqs = $args->val(qw(id ids text)) || "";
    my @reqList = ref($reqs) ? @{$reqs} :  split(/\s*[\n\r]+\s*/, $reqs);


    for my $ri (0..$#reqList) {
        my $req = $reqList[$ri];
        $mainResult = &get_hits( $req );
        if ($format eq 'json') {
            print &to_json($mainResult);
        } elsif ($format eq 'select') {
            print &to_select($mainResult, $ri + 1);
        } elsif ($format eq 'text') {
            print &to_text($mainResult);
        } elsif ($format eq 'html') {
            &collate_results($mainResult);
            # $sgl->clear_cache();
        } else {
            unless ($ri) {
                print $mainResult->_xmlCom("Standard Gene XML Output\nCharles Tilford, BMS Bioinformatics\ncharles.tilford\@bms.com");
                print "\n".$mainResult->xml_help(2)."\n";
            }
            print &to_xml($mainResult);
        }
    }
}

if ($format eq 'json') {
} elsif ($format eq 'select') {
} elsif ($format eq 'html') {
    print &show_collated();
    print "<pre>".$keepSafe[0]->showbench()."</pre>"
        if ($doBench && $#keepSafe != -1);
    print &HTML_END();
} elsif ($format eq 'xml') {
    print &XML_END($mainResult);
}

sub parse_table {
    my ($fileReq) = @_;
    return unless ($fileReq);
    my $fref   = ref($fileReq);
    my $tr     = BMS::TableReader->new();
#    foreach my $val (1,13,26,27,28,51,52,53, 144,15125) { warn "$val = ".$tr->column_number_to_alphabet($val); } die;
    my $format = $args->val(qw(inputformat));
    $format  ||= $tr->format_from_file_name($fileReq . "");
    my $niceF  = $tr->format( $format, 'NonFatal' );
    $tr->has_header( $hasHead );
    $args->msg("[!]", "Failed to understand input format '$format'")
        unless ($niceF);
    if ($fref eq 'Fh') {
        # This is an upload from the user. We need to make a local copy
        my $path = "/scratch/SG2/cache-$$.3dtsv";
        $fu->assure_dir( $path, 'isFile');
        $tr->input($fileReq);
        open(OUT, ">$path") || $args->death
            ("Failed to make local table copy", $path, $!);
        print OUT $tr->export_as_3dtsv();
        close OUT;
        chmod(0775, $path);
        $tr = BMS::TableReader->new();
        $tr->has_header( $hasHead );
        $fileReq = $path;
        $args->msg("Input copied locally", $path);
    }
    $tr->input($fileReq);
    my (%selected, %picked);
    if ($args->val(qw(firstcol))) {
        $picked{1}{1} = ['Parse', ];
    }
    foreach my $arg ($args->all_keys()) {
        if ($arg =~ /^\Q$ccToken\E_(\d+)_(\d+)$/) {
            my $val = $args->val($arg);
            # warn "$arg = $val\n";
            $selected{$val}{$1}{$2} = 1;
        }
        if ($arg =~ /\Q$cpToken\E_(\d+)_(\d+)_(\d+)$/) {
            # Sheet, Row, Request
            my $val = $args->val($arg);
            push @{$picked{$1}{$2}}, [$val, $3];
        }
    }
    my @pickSheets = keys %picked;
    return &annotate_sheet($tr, \%picked) unless ($#pickSheets == -1);
    return &html_guided_search($tr, \%selected) if ($selected{Parse});
    &choose_table_columns( $tr );
}

sub annotate_sheet {
    my ($tr, $picked) = @_;
    my $sfx = $format =~ /xlsx/ ? 'xlsx' : 'xls';
    my $file = $args->val(qw(output)) || sprintf("/stf/biohtml/tmp/SG2-Assigned-Genes-%d-%d.%s", time, $$, $sfx);
    my $url = $fu->path2url($file);
    my $eh = BMS::ExcelHelper->new( $file );
    $eh->url($url);
    $eh->format( -name       => 'ignore',
                 -color      => 'gray');
    $eh->format( -name       => 'unknown',
                 -color      => 'orange');
    $eh->format( -name       => 'error',
                 -color      => 'red',
                 -background => 'yellow');
    $eh->format( -name       => 'error',
                 -color      => 'red',
                 -background => 'yellow');

    $eh->format( -name       => 'noconf',
                 -align      => 'center',
                 -color      => 'black',
                 -background => 'gray');
    $eh->format( -name       => 'lousyconf',
                 -align      => 'center',
                 -color      => 'yellow',
                 -background => 'red');
    $eh->format( -name       => 'poorconf',
                 -align      => 'center',
                 -background => 'orange');
    $eh->format( -name       => 'poorconf',
                 -align      => 'center',
                 -background => 'orange');
    $eh->format( -name       => 'mehconf',
                 -align      => 'center',
                 -background => 'yellow');
    $eh->format( -name       => 'okconf',
                 -align      => 'center',
                 -background => 'cyan');
    $eh->format( -name       => 'goodconf',
                 -align      => 'center',
                 -background => 'green');

    my $offset = $tr->has_header() ? 1 : 0;
    my $ra = $tr->random_access();
#     print $args->branch(-ref => $ra, -maxany => 5 );

    # print "<pre>".$args->branch($picked)."</pre>";
    my @sheets   = $tr->each_sheet;
    my %found;
    for my $s (0..$#sheets) {
        my $sn    = $s + 1;
        my $ws    = $ra->sheet($sn);
        my $sname = $ws->name();
        my $head  = $ws->head();
        my $maxC  = $ws->maxcol();
        my $annot = $picked->{$sn} || $picked->{$s};
        my $locC  = $maxC + 1;
        my $conC  = $locC + 1;
        my $symC  = $locC + 2;
        my $taxC  = $locC + 3;
        my $desC  = $locC + 4;
        $head ||= [];
        $head = [ @{$head} ];
        $head->[$locC] = "Locus";
        $head->[$conC] = "Conf";
        $head->[$symC] = "Symbol";
        $head->[$taxC] = "Taxa";
        $head->[$desC] = "Description";
        # print "<pre>".$args->branch($head)."</pre>";
        my @added = ($locC, $symC, $taxC, $conC, $desC);
        my $os    = $eh->sheet( -name    => $sname,
                                -freeze  => $head ? 1 : 0,
                                -columns => $head, );
        if ($annot) {
            my @wids = (12, 6, 12, 16, 40);
            for my $w (0..$#wids) {
                my $ind = $locC + $w;
                $os->set_column( $ind, $ind, $wids[$w] );
            }

        }
        my $data  = $ws->data();
        for my $r (0..$#{$data}) {
            my @row = @{$data->[$r]};
            my $rn  = $r + 1;
            my @frm;
            if ($annot && $annot->{$rn}) {
                my %newData;
                my $ord = 0;
                my $rowDat = $annot->{$rn};
                for my $l (0..$#{$rowDat}) {
                    my ($loc, $reqNum) = @{$rowDat->[$l]};
                    my $conf;
                    if ($loc =~ /(.+)\+(.+)$/) {
                        ($loc, $conf) = ($1, $2);
                    }
                    next if ($newData{$loc});
                    my $nd = $newData{$loc} = {
                        order => ++$ord,
                        cols  => {},
                    };
                    my $cd = $nd->{cols};
                    if ($loc eq 'ignore') {
                        $cd->{$locC} = "Ignored";
                        $cd->{$desC} = "User request to ignore this row";
                    } elsif ($loc eq 'not found') {
                        $cd->{$locC} = "Unknown";
                        $cd->{$desC} = "System unable to find any loci similar to query terms";
                    } elsif (my $gene = $sgl->gene_object($loc)) {
                        $cd->{$locC} = $loc;
                        $cd->{$conC} = $conf;
                        $cd->{$symC} = $gene->symbol();
                        $cd->{$taxC} = $gene->taxa();
                        $cd->{$desC} = $gene->desc();
                    } else {
                        $cd->{$locC} = "Error";
                        $cd->{$desC} = "Error recovering gene information for '$loc'";
                    }
                }
                my $allConf = $#added == -1 ? -1 : 999999;
                foreach my $ind (@added) {
                    my @bits;
                    foreach my $nd (sort {$a->{order} <=> 
                                              $b->{order}} values %newData) {
                        my $val = $nd->{cols}{$ind};
                        if ($ind == $conC) {
                            if (!defined $val) {
                                $allConf = -1;
                            } else {
                                $val = int(0.5 + 10 * $val) / 10;
                                if ($val < $allConf) {
                                    $allConf = $val;
                                }
                            }
                        }
                        push @bits, $val || "";
                    }
                    $row[$ind] = join(' || ', @bits) || "";
                }
                if ($row[$locC] eq 'Ignored') {
                    map { $frm[$_] = 'ignore' } @added;
                } elsif ($row[$locC] eq 'Error') {
                    map { $frm[$_] = 'error' } @added;
                } elsif ($row[$locC] eq 'Unknown') {
                    map { $frm[$_] = 'unknown' } @added;
                }
                $frm[$conC] = &_conf_class( $allConf );
            }
            $eh->add_row( $os, \@row, \@frm);
        }
    }
    $eh->close;
    print $eh->html_summary;
    # print $args->branch($picked);
}

sub _conf_key_html {
    return "<p><b>{Confidence Colors} :</b> ".
        "<span class='goodconf'>&gt; 50 (best)</span> | ".
        "<span class='okconf'>20 - 50</span> | ".
        "<span class='mehconf'>10 - 20</span> | ".
        "<span class='poorconf'>2 - 10</span> | ".
        "<span class='lousyconf'>&lt;= 2 (worst)</span> | ".
        "<span class='noconf'>Unknown</span> | ".
        "<img src='/biohtml/images/BlueAlert_16x16.gif' /> = 2+ genes with top score".
        "</p>";
}

sub _conf_class {
    my $c = shift;
    return 'noconf'    if (! defined $c || $c < 0);
    return 'lousyconf' if ($c <= 2);
    return 'poorconf'  if ($c <= 10);
    return 'mehconf'   if ($c <= 20);
    return 'okconf'    if ($c <= 50);
    return 'goodconf';
}

sub html_guided_search {
    my ($tr, $sel) = @_;
    # Only consider sheets with at least one parsed column:
    my $hh     = $tr->has_header();
    my @sheets = sort { $a <=> $b } keys %{$sel->{Parse} || {}};
    my %pivot;
    foreach my $act ('Parse', 'Display') {
        foreach  my $s (@sheets) {
            map { $pivot{$s}{$_} ||= $act } 
            sort { $a <=> $b } keys %{$sel->{$act}{$s} || {}};
        }
    }
    my $hfrm = "<input type='hidden' name='%s' value='%s' />\n";
    print "<form method='post'>\n";
    print &_conf_key_html();
    printf($hfrm, 'inputfile', $tr->file_path());
    printf($hfrm, 'hasheader', $hh);
    foreach my $s (@sheets) {
        $tr->select_sheet($s);
        my $sn   = $tr->sheet_name();
        my @cols = sort { $a <=> $b } keys %{$pivot{$s}};
        my %capture; map { $capture{$_} = 1 if ($pivot{$s}{$_} eq 'Parse') } @cols;
        my @head = $tr->header();
        print "<h3><i>Sheet:</i> $sn</h3>";
        
        print "<table class='tab'>\n";
        print "<tbody>\n";
        print "<tr>\n".join("", map { "  <th>$_</th>\n" } ("Gene Assignment", map { defined $head[$_] ? $head[$_] : '' } @cols) )."</tr>\n";
        my $num = 0;
        my $tlim = $args->val(qw(testlimit));
        my $urlFrm = "standardizeGeneTwo.pl?text=%s&format=select&selectkey=%s&limit=10&taxa=%s";
        my $ut = $fu->esc_url($taxReq || "");
        # $tr->next_clean_row() if ($hh);
        while (my $row = $tr->next_clean_row()) {
            my $qry = "";
            my @disp;
            foreach my $c (@cols) {
                my $val = $row->[$c];
                $val = "" unless (defined $val);
                $qry .= " $val" if ($capture{$c} && $val);
                push @disp, $val;
            }
            next unless ($qry);
            my $key = join('_', $cpToken, $s, $tr->rowcount());
            my $trg = "cg_$key";
            print "<tr>\n";
            my $url = sprintf($urlFrm, $fu->esc_url($qry),$key, $ut);
            printf("  <td id='%s'><script id='%s'>cuteGet('%s', '%s', '%s')</script></td>\n",
                   $trg, "$trg-SCRIPT", $url, $trg, "$trg-SCRIPT");
            map { print "  <td>$_</td>\n" } @disp;
            print "</tr>\n";
            $num++;
            last if ($tlim && $num >= $tlim);
        }
        print "</tbody>\n";
        print "</table>\n";
    }
    print <<HTML;
<b>Output Format:</b>
<input type='radio' name='format' value='htmlxls' checked='checked' />XLS
<input type='radio' name='format' value='htmlxlsx' />XLSX
 <i>Use for larger files</i><br />

<input type='submit' value='Generate Spreadsheet' style='background-color:lime' /><br />
</form>
HTML

    # print "<pre>".$args->branch( { select => $sel, pivot => \%pivot } )."</pre>";
}

sub choose_table_columns {
    my ($tr) = @_;
    my @sheets   = $tr->each_sheet;
    # print "<pre>".$args->branch(\@sheets)."</pre>";
    my $exNum    = 10;
    my $checkNum = 100;
    my $hh       = $tr->has_header();
    my $sorter   = 0;
    my (%columns);
    foreach my $s (@sheets) {
        $tr->select_sheet($s);
        # $tr->next_clean_row() if ($hh);
        for my $i (1..$checkNum) {
            my $row = $tr->next_clean_row();
            last unless ($row);
            for my $c (0..$#{$row}) {
                if (my $v = $row->[$c]) {
                    $columns{$s}{$c}{$v} ||= ++$sorter;
                }
            }
        }
    }
    my @ss = sort { $a <=> $b } keys %columns;
    if ($#ss == -1) {
        print "<i>No data found in your table</i><br />\n";
        return;
    }
    my @opts = ('Ignore', 'Parse', 'Display');
    my $html = "";
    my $hfrm = "<input type='hidden' name='%s' value='%s' />\n";
    $html .= "<form>\n";
    $html .= sprintf($hfrm, 'inputfile', $tr->file_path());
    $html .= sprintf($hfrm, 'hasheader', $hh);
    $html .= sprintf($hfrm, 'format', 'html');
    $html .= sprintf($hfrm, 'taxa', $taxReq);

    $html .= "<h3>Choose columns to annotate</h3>\n";
    $html .= "<b>Parse</b> = use text in column to search for genes<br />\n";
    $html .= "<b>Dispaly</b> = Do <i>not</i> use column for searching, but show values when validating search resutls.<br />\n";
    $html .= "<input type='submit' value='Choose Columns' style='background-color:lime' /><br />";
    $html .= "<table class='tab'>\n";
    $html .= "<tbody>\n";
    $html .= "<tr>\n".join("", map { "  <th>$_</th>\n" } ('Sheet', 'Column', @opts, "Sample Text (up to $exNum from $checkNum rows)") )."</tr>\n";
    foreach my $s (@ss) {
        $tr->select_sheet($s);
        my $sn   = $tr->sheet_name();
        my @head = $tr->header();
        my @cs   = sort { $a <=> $b } keys %{$columns{$s}};
        my $cn   = $#cs + 1;
        foreach my $c (@cs) {
            $html .= "<tr>\n";
            unless ($c) {
                # First column, also show the sheet name
                $html .= "  <th rowspan='$cn' style='align:top'>$sn</th>\n";
            }
            my $cname = $head[$c] || "";
            $cname = "<b>[".$tr->column_number_to_alphabet( $c + 1 ).
                "]</b> $cname";
            $html .= "  <td>$cname</td>\n";
            my $optName = join('_', $ccToken, $s, $c);
            my $val     = $args->val($optName) || $opts[0];
            foreach my $opt (@opts) {
                $html .= " <td style='text-align:center'><input type='radio' name='$optName' value='$opt'".($val eq $opt ? " checked='checked'" : "")." /></td>\n";
            }
            my $th   = $columns{$s}{$c};
            my @txts = sort { $th->{$a} <=> $th->{$b} } keys %{$th};
            @txts = splice(@txts, 0, $exNum);
            $html .= "  <td>".join(" <span style='color:red'>+</span> ", @txts)."</td>\n";
            $html .= "</tr>\n";
        }
    }
    $html .= "</tbody>\n";
    $html .= "</table>\n";
    $html .= "<input type='submit' value='Choose Columns' style='background-color:lime' /><br />";
    $html .= "</form>\n";
    print $html;
}

sub collate_results {
    my $results = shift;
    push @keepSafe, $results;
    map { push @{$collated{$_->name()}}, $results } $results->each_gene();


    return;


    foreach my $gene ($results->each_gene()) {
        # Look at the gene and the orthologues and find the "best" species
        # to cluster the gene under
        my %bySpec;
        push @{$bySpec{$gene->taxa}}, $gene;
        map { push @{$bySpec{$_->taxa}}, $_ } $gene->orthologues();
        my $useTax;
        foreach my $tax (@specPriority, sort keys %bySpec) {
            next unless ($tax);
            next unless ($bySpec{$tax});
            $useTax = $tax;
            warn sprintf("%s (%s %s) = %s %s\n", $gene->name, $gene->symbol || '?', $gene->taxa, $tax, join('+', map { $_->symbol || $_->name } @{$bySpec{$tax}}));
            last;
        }
        $useTax ||= "";
        my $genes = $bySpec{$useTax};
        unless ($genes) {
            $args->err("Failed to find a gene to cluster a hit under",
                       $gene->to_text());
            next;
        }
        my $key = join("\t", sort map { $_->name() } @{$genes});
        push @{$collated{$key}}, $gene;
    }
    # print &to_html($results);
    
}

sub cluster_results {
    my @allDirectHits;
    my %genes;
    my %bestParent;
    # Get all distinct direct loci, and assign each as its own preliminary
    # cluster parent
    while (my ($acc, $resultss) = each %collated) {
        my $gene = $resultss->[0]->gene($acc);
        $genes{$acc} = $gene;
        my $taxa = $gene->taxa();
        my $spw  = $specWeight{$taxa} || 0;
        my $targ = $bestParent{$acc} = [$acc, $spw];
        foreach my $orth ($gene->orthologues()) {
            # Look at each reported orthologue and see if any are better
            my $taxa = $orth->taxa();
            my $spw  = $specWeight{$taxa} || 0;
            if ($spw > $targ->[1]) {
                $targ->[0] = $orth->name();
                $targ->[1] = $spw;
            }
        }
    }
    # Look at indirectly linked orthologues and see if any of them are better
    my @allDirectGenes = values %genes;
    foreach my $gene (@allDirectGenes) {
        my $acc  = $gene->name();
        my $targ = $bestParent{$acc};
        foreach my $orth ($gene->orthologues()) {
            my $oacc = $orth->name();
            next unless (exists $bestParent{$oacc});
            if ($bestParent{$oacc}[1] > $targ->[1]) {
                $targ->[0] = $bestParent{$oacc}[0];
                $targ->[1] = $bestParent{$oacc}[1];
                $targ->[2] = $oacc;
            }
        }
    }
    %collated = ();
    while (my ($acc, $gene) = each %genes) {
        # die $args->branch($gene);
        my ($par, $spw, $oacc) = @{$bestParent{$acc}};
        push @{$collated{$par}}, $gene;
    }
}

sub show_collated {
    my %sorter;
    &cluster_results();
    # Due to limits and blacklisting, some hit genes may also hit poor
    # queries. Map those over.
    my %allResH = map { $_->spawn_id() => $_ } @keepSafe;
    my @allResults = values %allResH;
    my %allQueries;
    foreach my $result (@allResults) {
        map { $allQueries{$_} = 1 } $result->each_query();
    }
    foreach my $query (keys %allQueries) {
        my @wids = $sgl->all_word_ids_for_text( $query );
        foreach my $result (@allResults) {
            $result->current_query($query);
            $result->add_words_to_genes( \@wids );
        }
    }
    
    while (my ($key, $genes) = each %collated) {
        my ($best) = sort {
            $b->best_score_for_all_results(-1) <=> 
                $a->best_score_for_all_results(-1) ||
                uc($a->symbol) cmp uc($b->symbol) } @{$genes};
        
        $sorter{$key} = [ $best->best_score_for_all_results(-1),
                          uc($best->symbol) ];
    }
    my @keyz = sort { $sorter{$b}[0] <=> $sorter{$a}[0] ||
                          $sorter{$a}[1] cmp $sorter{$b}[1] } keys %collated;
    my @sets;
    my %qScore;
    my %allTaxa;
    foreach my $key (@keyz) {
        my $genes = $collated{$key};
        my %all;
        foreach my $gene (@{$genes}) {
            my $acc = $gene->name();
            my $sc  = $gene->best_score_for_all_results(-1);
            my $ah = $all{$acc} ||= {
                acc => $acc,
                obj => $gene,
                sc  => $sc,
                hit => 0,
            };
            $ah->{sc} = $sc if ($ah->{sc} < $sc);
        }
        # Summarize the top-level collated gene(s):
        my %shtmls;
        foreach my $acc (split("\t", $key)) {
            my $gene = $sgl->gene_object($acc);
            my @sumbits;
            if (my $sym = $gene->symbol()) {
                push @sumbits, "<span class='symbol'>".$gene->esc_xml($sym).
                    "</span>";
            }
            if (my $desc = $gene->desc()) {
                push @sumbits, "<span class='desc'>".$gene->esc_xml($desc).
                    "</span>";
            }
            my $shtml = " <div class='colsum'>".join(' ', @sumbits)."</div>\n";
            $shtmls{$shtml} = $shtml;            
        }
        my @scores;
        foreach my $gene (@{$genes}) {
            my $sc      = $gene->best_score_for_all_results(-1);
            my @queries = $gene->each_query();
            my $acc = $gene->name();
            my $ah = $all{$acc};
            map { $qScore{$_} = $sc 
                      if (!defined $qScore{$_} || $qScore{$_} < $sc);
              } @queries;
            $ah->{hit} += $#queries + 1;
            map { $ah->{qry}{$_} = $sc if (!defined $ah->{qry}{$_} ||
                                           $ah->{qry}{$_} < $sc) } @queries;
            foreach my $orth ($gene->orthologues()) {
                my $acc = $orth->acc();
                my $ah = $all{$acc} ||= {
                    acc => $acc,
                    obj => $orth,
                    via => $gene,
                    sc  => $sc,
                    hit => 0,
                };
                push @scores, defined $sc ? $sc : -1;
                map { $ah->{qry}{$_} = $sc if (!defined $ah->{qry}{$_} ||
                                               $ah->{qry}{$_} < $sc) } @queries;
            }
        }
        map { $_->{taxa} = $_->{obj}->taxa();
              $_->{sym}  = $_->{obj}->symbol();
              $_->{desc} = $_->{obj}->desc(); } values %all;
        my @sorted = sort { ($specWeight{$b->{taxa}} || -1) <=> 
                                ($specWeight{$a->{taxa}} || -1) ||
                                $b->{sc} <=> $a->{sc} ||
                                uc($a->{sym}) cmp uc($b->{sym}) } values %all;
        my ($best) = sort { $b <=> $a } @scores;
        # Extending related entities means some zero-score hits get included
        next unless ($best);
        push @sets, {
            sum  => join('', keys %shtmls),
            locs => \@sorted,
            best => $best,
        };
        map { $allTaxa{ $_->{taxa} }++ } @sorted;
                                
    }
    my $html = "";
    my @showTax = sort { $allTaxa{$b} <=> $allTaxa{$a} ||
                             ($specWeight{$b} || -1) <=> 
                             ($specWeight{$a} || -1)} keys %allTaxa;
    if ($#showTax > 0) {
        $html .= "<b>Show Taxa:</b>\n";
        my @jsTax;
        foreach my $taxa ('All', @showTax) {
            my $tcl = $taxa || ""; $tcl =~ s/[^a-z]+//gi;
            my $num = $allTaxa{$taxa};
            $html .= "<button class='taxbutt' onclick='showtax(\"$tcl\")'>$taxa";
            $html .= " ($num)" if ($num);
            $html .= "</button>\n";
            push @jsTax, $tcl unless ($tcl eq 'All');
        }
        $html .= "<br />\n";
        $html .= "<script>var allKnownTax = [".join(', ', map { "'$_'" } @jsTax)."];</script>\n";
    }
    my @th = ("Locus", "Symbol", "Species", "Description", "User Queries", "Search Notes");
    my $hr = "    <tr class='header'>\n";
    map { $hr .= "     <th>$_</th>\n" } @th;
    $hr   .= "    </tr>\n";
    my $butF  = "      <a onclick='toggleGene(this)' class='%s' qry='%s'>%s</a>%s";
    foreach my $set (sort { $b->{best} <=> $a->{best} } @sets) {
        $html .= "<div class='collate'>\n";
        $html .= $set->{sum};
        $html .= " <table class='loclist tab'>\n";
        $html .= "  <tbody>\n";
        $html .= $hr;
        foreach my $loc (@{$set->{locs}}) {
            my ($acc, $sc, $obj, $isHit, $sym, $taxa, $desc) =
                map { $loc->{$_} }
            qw(acc sc obj hit sym taxa desc);
            # print "<pre>".$args->branch($obj)."</pre>" if ($sym eq 'GAK');
            my @trClass = ('loc');
            my $tcl = $taxa || ""; $tcl =~ s/[^a-z]+//gi;
            push @trClass, $tcl if ($tcl);
            $html .= sprintf("    <tr class='%s' acc='%s'>\n",
                             join(' ', @trClass),
                             $obj->esc_xml_attr($acc) );
            $html .= sprintf("     <td class='acc'>%s</td>\n",
                             $obj->esc_xml($acc) );
            $html .= sprintf("     <td class='sym'>%s</td>\n",
                             $obj->esc_xml($sym) );
            $html .= sprintf("     <td class='taxa'>%s</td>\n",
                             $obj->esc_xml($taxa) );
            $html .= sprintf("     <td class='desc'>%s</td>\n",
                             $obj->esc_xml($desc) );
            $html .= "     <td class='qlist'>\n";
            my $qh = $loc->{qry};
            my @buttons;
            foreach my $q (sort { $qh->{$b} <=> $qh->{$a} } keys %{$qh}) {
                next unless ($q);
                my @qc     = ('query', 'qbutt');
                my $qsc    = $obj->score(undef, $q);
                my $schtml = "";
                push @qc, "nothit" unless ($isHit);
                if (defined $qsc) {
                    $schtml = "<sup class='score'>[$qsc]</sup>"
                        if ($isHit);
                    my $qbest = $qScore{$q};
                    my $qfrac = $qbest ? $qsc / $qbest : 0;
                    if ($qfrac == 1) {
                        push @qc, 'best';
                    } elsif ($qfrac > 0.9) {
                        push @qc, 'good';
                    } elsif ($qfrac > 0.5) {
                        push @qc, 'poor';
                    } else {
                        push @qc, 'bad';
                    }
                } else {
                    push @qc, "related";
                }
                push @buttons, sprintf
                    ($butF, join(' ', @qc), $obj->esc_xml_attr($q),
                     $obj->esc_xml($q), $schtml );
            }
            $html .= join("<br />\n", @buttons) || "";
            $html .= "\n     </td>\n";
            $html .= sprintf("     <td class='note'>%s</td>\n", $obj->note()."");
            $html .= "    </tr>\n";
        }
        $html .= "  </tbody>\n";
        $html .= " </table>\n";
        $html .= "</div>\n";
    }
    return $html;
}

sub HTML_START {
    my $css = "/biohtml/css/standardGene.css";

    return <<HTML;
<html><head>
  <link rel='stylesheet' type='text/css' href='$css' />
  <link rel="shortcut icon" href="/biohtml/images/BmsStandard_16x16.png">
  <script src="/biohtml/javascript/standardGene.js"></script>
  <script src="/biohtml/javascript/jsmtk/jsmtk.js"></script>
  <script src="/biohtml/javascript/miniAjax.js"></script>
  <title>BMS Gene Standardization</title>
</head><body>
HTML

    
}

sub HTML_END {
    return "</body></html>\n";
}

sub HTML_GUI {
    print &HTML_START();
    my @targClass = qw(GO:0003707 GO:0004672 GO:0004721 GO:0004842 GO:0004888 GO:0004930 GO:0005216 GO:0005576 GO:0006986 GO:0008233 GO:0016651 GO:0031012 GO:0003824 GO:0016298 GO:0005887 GO:0003774 GO:0005515 GO:0003700 GO:0005215 GO:0045298 GO:0016032);
    my $targClass = "";
    if (0) {
        $targClass .= "<b>Restrict to target class:<b> ".
            "<select id='targclass' name='targclass'>";
        $targClass .= "</select><br />\n";
    }
    
    print <<FORM;
<form method='post' enctype='multipart/form-data'>
<input type='submit' value='Start Search' style='background-color:lime' /><br />
<b>Restrict to species:</b>
<input type='text' size='20' name='taxa' value='$taxReq' /><br />
$targClass
<input type='hidden' name='format' value='html' />
<b>Upload table or spreadsheet:</b><br />
<input style='background-color:lightblue' type='file' size='16' name='inputfile' /><br /></br>
<b>Provide one or more queries (IDs, description, symbols, etc):</b><br />
<textarea style='background-color:yellow' cols='30' rows='30' name='ids'>
</textarea><br />
<input type='submit' value='Start Search' style='background-color:lime' /><br /></form>
FORM

    print &HTML_END();
}

sub get_hits {
    my @txts = @_;
    my $wss = $sgl->all_word_sources();
    my %spsts;
    # Organize all word sets by their Splitter + Stripper patterns
    # Word sets with identical split and strip patterns will be using identical
    # words for the query:
    foreach my $ws (@{$wss}) {
        push @{$spsts{join("\t", $ws->{split}, $ws->{strip})}}, $ws;
    }
    my $hitData = {};
    my $results = $sgl->empty_result_set("sg2:get_hits");
    
    my %allWords;
    my $relSource = $args->{RELIABLESOURCE};
    # $args->msg("Program: $0", "Working directory: ".`pwd`);
    $args->msg_once("Beta software") if ($isbeta);
    foreach my $txt (@txts) {
        # Cycle through each query text
        next unless ($txt);
        $results->current_query($txt);
        my $qdat = $hitData->{query}{$txt} = [];
        $args->msg("Searching for '$txt'");
        my @fullset;
        while (my ($spst, $wsa) = each %spsts) {
            my ($split, $strip) = split(/\t/, $spst);
            my $userWords = $sgl->split_words_slow($txt, $split, $strip);
            my @words = keys %{$userWords};
            next if ($#words == -1);
            # print $args->branch($userWords);
            my @wsids = map { $_->{id} } @{$wsa};
            $results->add_word_source( @wsids );
            my @using;
            foreach my $word (@words) {
                if (my $wid  = $sgl->single_word_id( $word )) {
                    my $dat  = $userWords->{$word};
                    my $targ = $hitData->{words}{$wid} ||= [ $word, $dat ];
                    map { $targ->[1]{$_} ||= 1 } keys %{$dat};
                    # Capture the word_id + the raw user strings it comes from
                    push @using, [ $wid, [ sort keys %{$userWords->{$word}} ]];
                } else {
                    push @{$hitData->{unrecognized}}, $word;
                }
                my ($wObj) = $results->add_word( $word );
                if (my $uwH = $userWords->{$word}) {
                    foreach my $uw (keys %{$uwH}) {
                        map { $wObj->user_word( $uw, $_ ) } @wsids;
                    }
                }
            }
            
            if ($#using == -1) {
                next;
            }
            map { $allWords{$_->[0]}{$txt} = 1 } @using;
            map { $hitData->{wordsource}{$_} ||=
                      [$sgl->get_wordSource($_)] } @wsids;
            push @fullset, [\@using, \@wsids];
        }
        my $found = $sgl->geneids_for_wordids
            ( -wordids   => \@fullset,
              -limit     => $limit,
              -dumpsql   => $dumpSql,
              -taxa      => $taxReq,
              -data      => $results,
              -level     => 3,
              # -wsid      => \@wsids,
              -blacklist => 1,
              -reliablesource => $relSource,
              -source    => $source );
        # $found->add_word_source( @wsids );
    }

    # Because of the limit, some genes may be hit by one query, but not
    # found with another valid one.
    $results->add_words_to_genes( \%allWords ) if ($limit);
    
    my %toBlackList;
    my %blList = map { $_ => $sgl->blacklist_for_wordid($_) }
    keys %{$hitData->{words} || {}};
    #foreach my $wid (keys %blList) {
    #    my @gsids = keys %{$blList{$wid}};
    #    if ($#gsids == -1) {
    #        delete $blList{$wid};
    #    }
    #}
    while ( my ($gid, $wdat) = each %{$hitData->{wordhits} || {}}) {
        while (my ($wid, $hdat) = each %{$wdat}) {
            next unless ($blList{$wid});
            $toBlackList{$hdat->[1]}{$wid} = 1;
        }
    }
    # warn "<pre>".$args->branch(\%toBlackList)."</pre>\n";
    while (my ($wsid, $wids) = each %toBlackList) {
        next; # Trying to fix this internal to geneids_for_wordids()
        $sgl->geneids_for_wordids
            ( -wordids   => [ keys %{$wids} ],
              -addblack  => 1,
              -dumpsql   => 1,
              -data      => $hitData,
              -wsid      => $wsid,
              -source    => $source,
              -level     => 2, );
    }
    $results->add_orthologues();
    if ($taxReq) {
        my $check = lc($taxReq);
        my @remove;
        foreach my $gene ($results->each_gene()) {
            push @remove, $gene unless (lc($gene->taxa()) eq $check);
        }
        $results->remove_gene( @remove );
    }
    return $results;
}

sub to_json {
    my $results = shift;
    return $results->to_json
        ( -detail => $detail,
          -pretty => $args->val(qw(pretty prettyprint)),
          -callback => $args->val(qw(jsonp callback)) );
}

sub to_select {
    my ($results, $reqNum) = @_;
    my $sel = "";
    my $descSize = 40;
    my $confClass;
    my @genes = sort { $b->score(-1) <=> $a->score(-1) } $results->each_gene();
    foreach my $gene (@genes) {
        my $sym = $gene->symbol();
        my $tax = join('', map { substr($_, 0, 1) }
                       split(/\s+/, $gene->taxa() || ""));
        my $desc = $gene->desc() || "";
        my $olen = length($desc);
        $desc    = substr($desc, 0, $descSize);
        my $dlen = length($desc);
        $desc    = $fu->esc_xml($desc);
        #$desc   .= '&elips;' if ($dlen < $olen);
        $desc   .= '...' if ($dlen < $olen);
        my $acc  = $gene->acc();
        my $sc   = $gene->score();
        my $cls  = &_conf_class( $sc );
        $confClass ||= $cls;
        $sel .= sprintf
            (" <option class='$cls' value='%s+%s' score='%s'>%s%s (%s) %s {%s}</option>\n",
             $acc, $sc, $sc, $acc, 
             $sym ? " [$sym]" : "", $tax || '?', $desc, $gene->score(0));
    }
    if ($sel) {
        $sel .=
            join("", map { sprintf(" <option class='otherconf' value='%s'>%s</option>\n",@{$_}) }
                 ['not found', 'None of the Above'], ['ignore', 'Ignore']);
    } else {
        $sel .=
            join("", map { sprintf(" <option class='otherconf' value='%s'>%s</option>\n",@{$_}) }
                 ['not found', 'Nothing found'], ['ignore', 'Ignore']);
    }
    $sel = sprintf("<select class='%s' name='%s_%d'>\n", $confClass,
                   $selkey, $reqNum || 0). $sel.  "</select>";
    if ($#genes > 0 && $genes[0]->score() == $genes[1]->score()) {
        # Two top genes of same score
        $sel .= "<img src='/biohtml/images/BlueAlert_16x16.gif' />";
        
    }
    return $sel;
}

sub to_text {
    my $results = shift;
    return $results->to_text
        ( -detail => $detail );
}

sub to_html {
    my $results = shift;
    return $results->to_html
        ( -detail => $detail );
}

sub XML_START {
    my $xml = <<EOF;
<?xml version="1.0" encoding="UTF-8" ?>
<standardGene>
EOF

return $xml;
}

sub XML_END {
    my $results = shift;
    my $xml = "";
    if ($doBench && $results) {
        $xml .= "  <benchmarks>\n";
        $xml .= $results->esc_xml( $results->showbench );
        $xml .= "  </benchmarks>\n";
    }
    $xml .= "</standardGene>\n";
    return $xml;
}

sub to_xml {
    my $results = shift;
    return $results->to_xml(2, $detail);
}

sub to_xmlOLD {
    my $rv = shift;
    my $xml = <<EOF;
<?xml version="1.0" encoding="UTF-8" ?>
<standardGene>
  <!-- Standard Gene XML Output
       Charles Tilford, BMS Bioinformatics -->
  <input>
    <!-- This section contains parameters provided to the program -->
EOF

    my $pad = "    ";
    $xml .= "$pad<!-- The individual search queries -->\n";
    foreach my $q (sort keys %{$rv->{query} || {}}) {
        $xml .= sprintf("%s<query>%s</query>\n", $pad, $fu->esc_xml($q));
    }
    my %hitWords;
    while (my ($gid, $wdat) = each %{$rv->{wordhits} || {}}) {
        while (my ($wid, $scdat) = each %{$wdat}) {
            $hitWords{$wid}{$gid} = $scdat;
        }
    }
    my %winfo;
    while (my ($wid, $wd) = each %{$rv->{words} || {}}) {
        my ($clean, $users) = @{$wd};
        $winfo{$wid} = [$clean, [keys %{$users}]];
    }
    foreach my $wid (sort { $winfo{$a}[0] cmp $winfo{$b}[0] } keys %hitWords) {
        my ($word, $users) = @{$winfo{$wid}};
        $xml .= sprintf
            ("%s<word%s>\n", $pad, &_xml_attr( {wid => $wid, word => $word}));
        map { $xml .=  sprintf
                  ("  %s<userWord>%s</userWord>\n",$pad,  $fu->esc_xml($_)) } @{$users};
        foreach my $gid (sort { $hitWords{$wid}{$b}[0] <=> $hitWords{$wid}{$a}[0] || $a <=> $b } keys %{$hitWords{$wid}}) {
            my ($sc, $wsid) = @{$hitWords{$wid}{$gid}};
            
            $xml .= sprintf
                ("  %s<gene%s>%s</gene>\n", $pad, &_xml_attr( { gid => $gid, score => $sc, wordsource => $rv->{wordsource}{$wsid}[4] }), $fu->esc_xml($rv->{genes}{$gid}{acc}));
            
        }
        $xml .= sprintf("%s</word>\n", $pad);
    }

    $xml .= "\n$pad<!-- Program path and parameters -->\n";
    $xml .= "$pad<program>".$fu->esc_xml($0)."</program>\n";
    my $rt = `date`; $rt =~ s/[\n\r]+$//;
    $xml .= "$pad<runtime>".$fu->esc_xml($rt)."</runtime>\n";
    $xml .= "\n";
    foreach my $param ($args->all_keys( -skip => [qw(nocgi tiddlywiki errormail)],)) {
        my $v = $args->val($param);
        my @vals = ($v);
        if (my $r = ref($v)) {
            if ($r eq 'ARRAY') {
                @vals = @{$v};
            } else {
                next;
            }
        }
        foreach my $val (@vals) {
            $xml .= sprintf
                ("%s<arg%s>%s</arg>\n", $pad,
                 &_xml_attr( { isDefault => $args->is_default($param) ? 1 : 0,
                               name      => $param } ), $fu->esc_xml($val));
        }
    }
    $xml .= "  </input>\n\n";
    my $scd   = $rv->{score}{gene};
    my @gids  = sort { $scd->{$b}{score} <=> $scd->{$a}{score} ||
                       $rv->{genes}{$a}{acc} cmp $rv->{genes}{$b}{acc} } keys %{$scd};
    my $bsc   = $#gids == -1 ? 0 : $scd->{$gids[0]}{score};
    my %resAttr = ( count     => $#gids + 1,
                    bestscore => $bsc );
    $xml .= sprintf("  <genes%s>\n", &_xml_attr(\%resAttr));
    $xml .= "$pad<!-- Standard gene objects recovered for the queries -->\n";
    my %orths;
    my $thresh   = $args->val(qw(showfrac)) || 1;
    my $maxDetail = $args->val(qw(showbest));
    my $scThresh = $bsc / $thresh;
    my %doneStuff;
    for my $g (0..$#gids) {
        my $gid    = $gids[$g];
        my $gene   = $rv->{genes}{$gid};
        my $acc    = $gene->{acc};
        my $sc     = $scd->{$gid}{score};
        if ( ($maxDetail && $g + 1 > $maxDetail) || $sc < $scThresh) {
            # We have shown enough "good" genes, just summarize the rest
            my $poorTag = 'relativelyPoorHit';
            if ($sc < $scThresh && ! $doneStuff{PoorGenes}++) {
                $xml .= "\n$pad<!-- The search criteria requested that genes scoring less than 1/$thresh the score of the best hit be summarized only.\n$pad     The '$poorTag' attribute shows the fold difference in score. -->\n"; 
                
            }
            if ($g + 1 > $maxDetail && !$doneStuff{PoorGenes} && !$doneStuff{ManyGenes}++) {
                $xml .= "\n$pad<!-- Search requested detailed information for at most $maxDetail genes. Remainder are summarized. -->\n"; 
            }
            $xml .= sprintf("%s<gene%s />\n", $pad, &_xml_attr( {
                dbPkey      => $gid,
                accession   => $acc,
                score       => $sc,
                $poorTag => $sc < $scThresh ? int(0.5 + $bsc / $sc) : undef,
            }));
            next;
        }

        $sgl->decorate_gene($gene);
        my ($sym, $offSym) = &_geneSym($gene);
        my $kvs    = $gene->{keyvals} || {};
        my %attr = ( species     => $kvs->{Taxa},
                     dbPkey      => $gid,
                     type        => $kvs->{Type},
                     accession   => $acc,
                     geneType    => $kvs->{GeneType},
                     score       => $sc,
                     symbol      => $sym,
                     symIsOfficial => $offSym );
        
        $xml .= sprintf("%s<gene%s>\n", $pad, &_xml_attr(\%attr));
        if (my $d = $kvs->{Description}) {
            $xml .= sprintf("%s  <desc>%s</desc>\n",
                            $pad, $fu->esc_xml($d->[0]));
        }
        my %src =( namespace => $kvs->{Namespace}, gsid => $gene->{gsid});
        $xml .= sprintf("%s  <source%s />\n", $pad, &_xml_attr(\%src));
        foreach my $kv (sort keys %{$kvs}) {
            if ($kv =~ /^(.+) Ortholog$/) {
                my $common = $1;
                foreach my $oloc (@{$kvs->{$kv}}) {
                    my $orth = $orths{$oloc} ||= $sgl->
                        decorate_gene( $sgl->object_for_geneacc($oloc));
                    my %hash = ( accession => $oloc,
                                 organism  => $common );
                    my $desc;
                    if ($orth) {
                        my $okv = $orth->{keyvals};
                        $desc = $okv->{Description} || [];
                        $desc = $desc->[0];
                        $hash{species} = $okv->{Taxa};
                        $hash{symbol}  = &_geneSym($orth);
                    }
                    $xml .= sprintf("%s  <orthologue%s>%s</orthologue>\n",
                                    $pad, &_xml_attr( \%hash ),
                                    $fu->esc_xml($desc));
                    
                }
            }
        }
        $xml .= sprintf("%s</gene>\n", $pad);
    }
    $xml .= "  </genes>\n";
    
    $xml   .= "</standardGene>\n";
       
}

sub _geneSym {
    my ($gene) = @_;
    my $kvs    = $gene->{keyvals} || {};
    my $sym    = $kvs->{"Official Symbol"};
    my $offSym = 1;
    unless ($sym) {
        ($sym)  = sort { length($a) <=> length($b) ||
                             lc($a) cmp lc($b) }
        @{$kvs->{"Official Symbol"} || []};
        $offSym = 0;
    }
    $offSym     = undef unless ($sym);
    return wantarray ? ($sym, $offSym) : $sym;
}

sub _xml_attr {
    my $hash = shift || {};
    my @kv;
    foreach my $key (sort keys %{$hash}) {
        my $v = $hash->{$key};
        $v = join(",", @{$v}) if (ref($v));
        next unless (defined $v && $v ne '');
        push @kv, sprintf("%s='%s'", $key, $fu->esc_xml_attr($v));
    }
    return ($#kv == -1) ? "" : " ".join(" ", @kv);
}
