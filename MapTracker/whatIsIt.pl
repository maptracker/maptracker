#!/stf/biobin/perl -w

BEGIN {
    # Needed to make my libraries available to Perl64:
    use lib '/apps/sys/perl/lib/site_perl/5.12.0/';
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
use BMS::BmsArgumentParser;
use BMS::MapTracker::WhatIsIt;
use BMS::TableReader;
use BMS::Utilities::Serialize;

my $args = BMS::BmsArgumentParser->new
    ( -nocgi          => $ENV{HTTP_HOST} ? 0 : 1,
      -errormail      => 'charles.tilford@bms.com',
      -verbose        => 1,
      -format         => 'json',
      -limit          => 0,
      -joiner         => ',',
      -splitter       => '\s*,\s*',
      -testlimit      => 0 );

$args->debug->skipkey([qw(entries)]);

my $nocgi     = $args->val(qw(nocgi)) ? 1 : 0;
my $detailSet = $args->val(qw(detailset setdetail));
my $detailMet = $args->val(qw(detailmeta metadetail));
my $doSet     = $args->val(qw(doset addset));
my $doMeta    = $args->val(qw(dometa meta addmeta));
my $bestOnly  = $args->val(qw(best bestonly onlybest));
my $format    = lc($args->val(qw(fmt format)) || "");
my $doPretty  = $args->val(qw(pretty prettyprint dopretty));
my $doHelp    = $args->val(qw(help addhelp showhelp h));
my $joiner    = $args->val(qw(join joiner));
my $splitter  = $args->val(qw(splitter split));
my $limit     = $args->val(qw(limit));
my $output    = $args->val(qw(output));
my $truncArr  = $args->val(qw(listtrunc arrtrunc truncarr trunclist));
my $redo      = $args->val(qw(clobber redo refresh)) || "";
$redo         = 'all' if ($redo eq '1');

my $mime;
if ($format =~ /(null|pop)/) {
    $format = 'null';
} elsif ($format =~ /json/) {
    $format = 'json';
    $mime = 'text';
} elsif ($format =~ /(text|txt)/) {
    $mime = $format = 'text';
} elsif ($nocgi) {
    $mime = $format = 'text';
} else {
    $mime = $format = 'html';
}

if ($nocgi) {
    $args->shell_coloring( );
} else {
    $args->set_mime( -mail     => 'charles.tilford@bms.com',
                     -codeurl  => "http://bioinformatics.bms.com/biocgi/filePod.pl?module=_MODULE_&highlight=_LINE_&view=1#Line_LINE_",
                     -mime     => $mime,
                     -errordir => '/docs/hc/users/tilfordc/' );
}
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

my ($wii, %dataSrcs, $t2i, %i2t, %cases);

my %rv = ( params => {
    output   => 'STDOUT',
    splitter => $splitter,
    joiner   => $joiner,
    'format'   => $format,
});

my $fh = *STDOUT;
if ($output) {
    open(OUT,">$output") || $args->death
        ("Failed to write output file", $output, $!);
    $fh = *OUT;
    $rv{params}{output} = $output;
}

&run();
&extra();
&format();
&finish();

sub run {
    &new_wii();
    return unless ($wii && $wii->dbh);
    if ($args->val(qw(build rebuild))) {
        &build( );
        $args->msg("Database schema updated");
        exit;
    }
    my $reqs = &get_reqs();
    &process( $reqs );
}

sub new_wii {
    $wii = BMS::MapTracker::WhatIsIt->new();
    $wii->param('age',      $args->val(qw(ageall age)));
    $wii->param('cloudage', $args->val(qw(ageall cloudage)));
    $wii->param('warn',     $args->val(qw(warn)));
    $wii->param('refresh',  $redo);
    $wii->param('splitter', $splitter);
    $wii->debug->maxany(25);
}

sub process {
    my $list = shift;
    if (!$list || $#{$list} == -1) {
        $doPretty ||= $doHelp;
        return;
    }
    $t2i = $wii->bulk_txt_ids( "User_Query_Text"  );
    # warn $args->branch( {list => $list, t2i => $t2i} );
    my @idU;
    while (my ($t, $i) = each %{$t2i}) {
        push @idU, $i;
        $i2t{$i} = $t;
    }
    $wii->temp_id_list( \@idU, "User_Query_IDs");
    &assign_type();
    &assign_sets() if ($doSet);
    &assign_meta() if ($doMeta);
    &collate();
}

sub extra {
    if ($args->val(qw(dbid addid))) {
        # Add database IDs
        if (exists $rv{ids}) {
            while (my ($name, $hash) = each %{$rv{ids}}) {
                $hash->{db_id} = $wii->id_for_text($name);
            }
        }
    }
    &add_errs();
    &add_help();
}

sub add_errs {
    my %u = map { $_ => 1 } $wii->all_stored_errs();
    my @errs = sort keys %u;
    push @{$rv{error}}, @errs unless ($#errs == -1);
}

sub add_help {
    return unless ($doHelp);
    my $help = $rv{help} ||= {};
    my $txt = <<EOF;
This object is a report from one or more IDs being passed by a tool or user.
The report is designed to help elucidate what exactly those IDs are.
Generally, this will be used by the requester to make decisions on what to do with the IDs.
This 'help' block is designed to co-exist with output.

ID    -> An identifier, eg CAV3, LOC859, 1001_AT etc
Type  -> A kind of identifier, eg Gene Symbol, LocusLink Gene, Affy Probe Set
Set   -> A collection of IDs, eg Homo sapiens LocusLink, HG_U95A
Input -> A discrete set of IDs passed to the tool

Types of Input:
'argument' : Provided as -id, -ids or -list. Will be split on newlines.
'file'     : Can be a simple list, TSV, or excel workbooks.
If a table is provided, all columns will be parsed, or you can specify -colnum (counting from 1)
If a workbook is provided, all sheets will be parsed, or you can specify one with -sheet

This section describes the structure and content of the object.
The other fields contain hash key descriptions.
A key of 'foo:bar' indicates foo is a hash/object, with bar being a sub key.

ids = This section contains details for each query. It is keyed to the uppercased query text.
ids:<ID>:db_id = Integer for nerds. If you define parameter -dbid as a true value, then the internal primary_key value of the text is shown here (column txt_id in table normtxt)
ids:<ID>:set = Array. List of Sets associated with the ID
ids:<ID>:type = String. The predicted Type of the ID
input = Array of hashes. Each hash describes a single Input.
input:[]:content = String. The most populous Type for the Input.
input:[]:contentDetail = Hash of Types. Contails actual counts for each Type
input:[]:contentDetail:<Type> = Array [ # of unique IDs, Fraction of unique IDs, # of rows, Fraction of rows ]. A gene symbol list of 'CAV3,PKY,PKY,1001_at' would then have Gene Symbol be [ 2, 0.667, 3, 0.75 ]
input:[]:entries = Hash of IDs. The specific IDs that came from this Input. Each ID points to an array of row numbers where it was found
input:[]:entryRows = Integer. The number of rows analyzed in this Input
input:[]:entryUniq = Integer. The number of distinct IDs found in the Input
input:[]:inpSrc = String. The type of input ('argument' or 'file')
input:[]:param = String. If inpSrc was 'argument', then the parameter name passed
input:[]:set = Array of strings. The best Set(s) inferred for this input.
input:[]:setDetail = Hash of Sets. Provides full count details for each Set identified within the Input
input:[]:setDetail:<Set> = Array [ # of unique IDs, Fraction of unique IDs]. Note that unlike Types (for which an ID will have only one), more than one set may be assigned to an ID
EOF


    my $cnt = 0;
    foreach my $line (split(/[\n\r]/, $txt)) {
        if ($line =~ /(\S+)\s+=\s+(.+)/) {
            my ($hier, $val) = ($1, $2);
            $help->{$hier} = $val;
        } else {
            my $hier = sprintf("%02d", $cnt++);
            $help->{$hier} = $line;
        }
    }
}

sub collate {
    # return;
    foreach my $sd (values %dataSrcs) {
        my $con = $sd->{contentDetail} ||= {};
        my $set = $sd->{setDetail}     ||= {};
        while (my ($txt, $arr) = each %{$sd->{entries}}) {
            my $type = $rv{ids}{$txt}{type} || "Unknown";
            $con->{$type}[0]++;
            $con->{$type}[2] += $#{$arr} + 1;
            my %uSets;
            foreach my $arr (@{$rv{ids}{$txt}{set}}) {
                if (my $sn = $detailSet ? $arr->[0] : $arr) {
                    $uSets{$sn}++;
                }
            }
            map { $set->{ $_ }[0]++ } keys %uSets;
        }

        my @types = keys %{$con};
        my @tot;
        for my $i (0,2) {
            map { $tot[$i] += $con->{$_}[$i] ||= $con->{$_}[0] ||= 0 } @types;
        }
        $sd->{entryUniq} = $tot[0];
        $sd->{entryRows} = $tot[2];
        foreach my $type (@types) {
            for my $i (0,2) {
                $con->{$type}[$i + 1] = $tot[$i] ? 
                    int(0.5 + 1000 * $con->{$type}[$i] / $tot[$i]) / 1000 : 0;
            }
        }
        my @sets = keys %{$set};
        foreach my $s (@sets) {
            $set->{$s}[1] = $tot[0] ? 
                    int(0.5 + 1000 * $set->{$s}[0] / $tot[0]) / 1000 : 0;
        }
        my @conSort = sort { $con->{$b}[1] <=> $con->{$a}[1] } @types;
        my $topCon  = shift @conSort;
        $topCon  = shift @conSort 
            while ($topCon && $topCon =~ /^Empty/ && $#conSort != -1);
        $sd->{content} = $topCon;
        my @setSort;
        foreach my $s (@sets) {
            my $f = $set->{$s}[1];
            my $sc = ($s =~ / RefSeq (RNA|Protein)$/ ||
                      $s =~ / LocusLink Gene$/) ? 100 : 
                      ($s =~ /^WikiPathways/) ? -5 : 5;
            push @setSort, [ $s, $f, $sc ];
        }
        @setSort = sort { $b->[1] <=> $a->[1] ||
                              $b->[2] <=> $a->[2] || 
                              $a->[0] cmp $b->[0] } @setSort;
        my @top;
        unless ($#setSort == -1) {
            foreach my $sd (@setSort) {
                last if ($sd->[1] < $setSort[0][1]);
                push @top, $sd->[0];
            }
        }
        $sd->{set} = \@top;
    }
}

sub assign_type {
    my $types = $wii->type_for_ids("User_Query_IDs");
    while (my ($id, $tid) = each %{$types}) {
        $rv{ids}{$i2t{$id}}{type} = $i2t{$tid} ||= $wii->text_for_id($tid);
    }
}
sub assign_meta {
    my $meta = $wii->meta_for_ids("User_Query_IDs");
    while (my ($id, $kv) = each %{$meta}) {
        my $txt = $i2t{$id};
        if ($detailMet) {
            $rv{ids}{$txt}{meta} = $kv;
        } else {
            while (my ($key, $vH) = each %{$kv}) {
                my @vals = sort { $vH->{$b} <=> $vH->{$a} ||
                                  $a cmp $b } keys %{$vH};
                if ($bestOnly && $#vals != -1) {
                    my @keep;
                    my $best = $vH->{$vals[0]};
                    while (my $val = shift @vals) {
                        last if ($vH->{$val} < $best);
                        push @keep, $val;
                    }
                    @vals = @keep;
                }
                $rv{ids}{$txt}{meta}{$key} = \@vals;
                $rv{meta}{metaKeys}{$key}++;
            }
        }
    }
}

sub assign_sets {
    my $sets  = $wii->sets_for_ids("User_Query_IDs");
    while (my ($id, $iArr) = each %{$sets}) {
        my %done;
        foreach my $arr (@{$iArr}) {
            $arr->[0] = $i2t{$arr->[0]} ||= $wii->text_for_id($arr->[0])
                if ($arr->[0]);
            next unless ($arr->[0]);
            if ($detailSet) {
                $arr->[2] = $i2t{$arr->[2]} ||= $wii->text_for_id($arr->[2])
                    if ($arr->[2]);
            } else {
                $arr = $arr->[0];
                next if ($done{$arr}++);
            }
            push @{$rv{ids}{$i2t{$id}}{set}}, $arr;
        }
    }
}

sub format {
    if ($format eq 'null') {
        # For populating
    } elsif ($format eq 'json') {
        &to_json();
    } elsif ($format eq 'text') {
        &to_text();
    } elsif ($format eq 'html') {
        &to_html();
    } else {
        print $fh $args->branch(\%rv);
    }
}

sub to_json {
    $wii->bench_start();
    my $ser = BMS::Utilities::Serialize->new();
    my $cb  = $args->val(qw(jsonp));
    print $fh "$cb(" if ($cb);
    print $fh $ser->obj_to_json(\%rv, $doPretty, { basicArray => 1 } );
    print $fh ")" if ($cb);
    print $fh "\n";
    $wii->bench_end();
}

sub styles {
    return <<EOF;
.unk { background-color: silver; margin-top: 4px; padding: 2px; white-space: pre; }
.tab, .tab table    { border-collapse: collapse; }
.tab th { background-color: #ffc; }
.tab th, .tab td { border: #fc9 solid 1px; padding: 2px;
                   empty-cells: show; vertical-align: top; }

.tab caption { background-color: #ffc; color: navy; text-size: 1.3em; font-weight: bold; text-align: left; }
EOF

}


sub to_html {
    $wii->bench_start();
    my $isFull = $args->val(qw(full dofull));
    print $fh "<html><head><title>What Is It? Output</title><style>".&styles()."</style> </head><body>\n" if ($isFull);
    $joiner = "\n";

    my ($id, $ih) = &input_table_array();
    print $fh &html_table($id, $ih, "Summary of ".($#{$id} + 1)." Input Sources");

    my ($data, $head) = &id_table_array();
    my (@known, @unknown);
    foreach my $row (@{$data}) {
        if ($row->[1] && $row->[1] ne 'Unknown') {
            push @known, $row;
        } else {
            push @unknown, $row->[0];
        }
    }
    print $fh &html_table(\@known, $head, "Summary of ".($#known + 1)." Encountered IDs")
        unless ($#known == -1);
    if ($#unknown != -1) {
        print $fh "<h4>".($#unknown + 1)." Unknown IDs</h4>\n";
        print $fh join("\n", map { "<span class='unk'>$_</span>" } @unknown);
        print $fh "<br />\n";
    }
    if (exists $rv{error} && $#{$rv{error}} != -1) {
        print "<h5>Errors</h5>\n";
        print "<ul>\n";
        print join('', map {
            "<li>".$args->esc_xml($_)."</li>\n" } @{$rv{error}});
        print "</ul>\n";
    }

    print $fh "</body></html>\n" if ($isFull);
    $wii->bench_end();
}

sub to_text {
    $wii->bench_start();
    my ($data, $head) = &id_table_array();
    print $fh &text_table($data, $head);
    $wii->bench_end();
}

sub id_table_array {
    $wii->bench_start();
    my @head = ("ID", "Type");
    push @head, "Set" if ($doSet);
    my $colOrder = {
        Description => 999,
    };
    my @mCols;
    foreach my $mcol (sort { ($colOrder->{$a} || 0) <=> ($colOrder->{$b} || 0)
                                 || $a cmp $b } keys %{$rv{meta}{metaKeys}}) {
        push @mCols, $mcol if ($mcol && $rv{meta}{metaKeys}{$mcol});
    }
    push @head, @mCols;
    my @data;
    foreach my $id (sort { uc($a) cmp uc($b) || $a cmp $b } keys %{$rv{ids}}) {
        my $idd = $rv{ids}{$id};
        my @row = ($id, $idd->{type});
        push @row, join($joiner, @{$idd->{set} || []}) || "" if ($doSet);
        foreach my $col (@mCols) {
            my $val = "";
            if (exists $idd->{meta}{$col}) {
                $val = join($joiner, @{$idd->{meta}{$col}});
            }
            push @row, defined $val ? $val : "";
        }
        push @data, \@row;
    }
    $wii->bench_end();
    return wantarray ? (\@data, \@head) : \@data;
}

sub input_table_array {
    $wii->bench_start();
    my @check = qw(inpSrc fmt path sheet col colname content contentDetail entryRows entryUniq set setDetail);
    my @inps = @{$rv{input}};
    my @colTok;
    foreach my $ccol (@check) {
        my $nonnull = 0;
        foreach my $inp (@inps) {
            if (my $iv = $inp->{$ccol}) {
                if (my $r = ref($iv)) {
                    if ($r eq 'ARRAY') {
                        next if ($#{$iv} == -1);
                    } elsif ($r eq 'HASH') {
                        my @u = keys %{$iv};
                        next if ($#u == -1);
                    } else {
                        next;
                    }
                }
                $nonnull = 1;
                last;
            }
        }
        push @colTok, $ccol if ($nonnull);
    }
    my $niceCol = {
        inpSrc => 'Type',
        fmt => 'Format',
        path => 'File Path',
        col => 'Col#',
        sheet   => 'Worksheet',
        colname => 'Column Name',
        content => 'Predicted Content',
        contentDetail => 'Content Detail',
        entryRows => '#Rows',
        entryUniq => 'UniqVals',
        set => 'Predicted Set',
        setDetail => 'Set Detail',
    };
    my @head = map { $niceCol->{$_} || $_ } @colTok;
    my @rows;
    foreach my $inp (@inps) {
        my @row;
        foreach my $ccol (@colTok) {
            my $iv = $inp->{$ccol};
            if (my $r = ref($iv)) {
                if ($r eq 'ARRAY') {
                    $iv = &_trunc_array($iv, $joiner);
                } elsif ($r eq 'HASH') {
                    if ($ccol =~ /Detail$/) {
                        my @det;
                        foreach my $k (sort { $iv->{$b}[0] <=> $iv->{$a}[0] } keys %{$iv}) {
                            push @det, sprintf("%.1f%% %s", 100 * $iv->{$k}[1], $k);
                        }
                        $iv = &_trunc_array(\@det, $joiner);
                    } else {
                        $iv = "";
                    }
                } else {
                    $iv = "";
                }
            }
            push @row, $iv;
        }
        push @rows, \@row;
    }
    $wii->bench_end();
    return wantarray ? (\@rows, \@head) : \@rows;
}

sub _trunc_array {
    my @arr = @{$_[0] || []};
    if ($truncArr && $#arr >= $truncArr) {
        my $onum = $#arr - ($truncArr - 1);
        @arr = @arr[0..4];
        push @arr, "+ $onum others";
    }
    return join($_[1], @arr);
}

sub html_table {
    $wii->bench_start();
    my ($rowRef, $head, $caption) = @_;
    my $html = "<table class='tab'>\n";
    $html .= " <caption>".$args->esc_xml($caption)."</caption>\n" if ($caption);
    $html .= "<tbody>\n";
    $html .= " <tr>".join('', map { "<th>$_</th>" } map { $args->esc_xml($_) } @{$head})."</tr>\n" if ($head);
    foreach my $row (@{$rowRef}) {
        my @ev;
        foreach my $v (@{$row}) {
            $v = "" unless (defined $v);
            $v = $args->esc_xml($v);
            $v =~ s/\n/<br \/>/g;
            push @ev, $v;
        }
        $html .= "<tr>".join('', map { "<td>$_</td>" } @ev);
    }
    $html .= "</tbody></table>\n";
    $wii->bench_end();
    return $html;
}


sub text_table {
    $wii->bench_start();
    my ($rowRef, $head) = @_;
    my @rows =  @{$rowRef};
    unshift @rows, $head if ($head);
    my @widths = map { 0 } @{$head ? $head : $rows[0]};
    if ($#widths == -1) {
        return "Empty Table\n";
    }
    foreach my $row (@rows) {
        map { my $l = defined $row->[$_] ? length($row->[$_]) : 0; 
              $widths[$_] = $l if ($widths[$_] < $l) } (0..$#{$row});
    }
    my $frm = "| ".join(' | ', map { '%-'.$_.'s' } @widths)." |\n";
    my $line = sprintf($frm, map { '-' x $_ } @widths);
    $line =~ s/\|/+/g; $line =~ s/ /-/g;
    my $txt  = $line;
    $txt    .= sprintf($frm, @{shift @rows}) . $line if ($head);
    foreach my $row (@rows) {
        $txt .= sprintf($frm, map { defined $row->[$_] ? $row->[$_] : '' }
                        (0..$#widths));
    }
    $txt .= $line;
    $wii->bench_end();
    return $txt;
}

sub get_reqs {
    $wii->bench_start();
    if (my $param = $args->val(qw(sourcefield))) {
        # Used to extract from a specific form field
        &load_mixed($param);
        $wii->bench_end();
        return &ready_reqs();
    }
    &load_mixed('mixedinput');
    
    foreach my $param (qw(idfile idpath file path idlist)) {
        foreach my $file ($args->each_split_val($param)) {
            next unless ($file);
            if (-e $file) {
                &load_file($file);
            } else {
                push @{$rv{error}}, "-$param '$file' : could not find file";
                next;
            }
        }
    }
    foreach my $param (qw(id ids list)) {
        &load_parameter($param);
    }
    $wii->bench_end();
    return &ready_reqs();
}

sub ready_reqs {
    foreach my $key (sort keys %dataSrcs) {
        my $sd = $dataSrcs{$key};
        push @{$rv{input}}, $sd;
        map { $rv{ids}{$_} ||= {} } keys %{$sd->{entries}};
    }
    my @rv = sort keys %{$rv{ids} || {}};
    $wii->temp_text_list( \@rv, "User_Query_Text") unless ($#rv == -1);
    return \@rv;
}

sub load_mixed {
    my $param = shift;
    my @vals = $args->each_split_val('/\s*[\n\r,]+\s*/', $param);
    return if ($#vals == -1);
    my $sd;
    for my $i (0..$#vals) {
        if ($vals[$i] && -e $vals[$i]) {
            &load_file($vals[$i]);
        } else {
            $sd ||= $dataSrcs{sprintf("_Param %30s", $param)} ||= {
                inpSrc    => 'argument',
                param     => $param,
                contentDetail => { },
                entries   => { },
            };
            &classify_text( $vals[$i], $sd, $i+1);
        }
    }
}

sub load_parameter {
    my $param = shift;
    my @vals = $args->each_split_val('/\s*[\n\r,]+\s*/', $param);
    return if ($#vals == -1);
    my $sd  = $dataSrcs{sprintf("_Param %30s", $param)} ||= {
        inpSrc    => 'argument',
        param     => $param,
        contentDetail => { },
        entries   => { },
    };
    for my $i (0..$#vals) {
        &classify_text( $vals[$i], $sd, $i+1);
    }
}

sub load_file {
    my $file = shift;
    $wii->bench_start();
    my $hasHeader = $args->val(qw(hasheader));
    my $tr        = BMS::TableReader->new();
    my $format;
    if (my $inFormat = $args->val(qw(informat))) {
        unless ($format = $tr->format( $inFormat, 'NonFatal' )) {
            push @{$rv{error}}, "-format '$inFormat' : not recognized";
        }
    }
    $format ||= $tr->format_from_file_name($file . "");
    $tr->has_header($hasHeader);
    $tr->format($format);
    $tr->input($file);
    my @sheets = $tr->each_sheet;
    if (my $sreq = $args->val(qw(sheet excelsheet))) {
        my $sheet = $tr->sheet($sreq);
        unless ($sheet) {
            push @{$rv{error}}, "-sheet '$sreq' : not found";
            return;
        }
        @sheets = ($sheet);
    }
    my $creq = $args->val(qw(col column colnum));
    for my $sn (0..$#sheets) {
        my $sheet = $sheets[$sn];
        $tr->select_sheet($sheet);
        my $sname = $tr->sheet_name();
        my $src   = sprintf("%60s\t%60s", $file, $sname);
        my $head  = $tr->has_header() ? $tr->header() : undef;
        my %formats;
        my @colSrc;
        my $rn = 0;
        while (my $row = $tr->next_clean_row()) {
            $rn++;
            my @is = $creq ? ($creq-1) : (0..$#{$row});
            foreach my $i (@is) {
                my $sd  = $colSrc[$i] ||= $dataSrcs
                {sprintf("%s\t%03d",$src, $i)} ||= {
                    inpSrc  => 'file',
                    path    => $file,
                    fmt     => $format,
                    sheet   => $sname,
                    col     => $i + 1,
                    colname => $head ? $head->[$i] || "" : undef,
                    contentDetail => { },
                    entries => { },
                };
                &classify_text( $row->[$i], $sd, $rn);
            }
            last if ($limit && $rn >= $limit);
        }
    }
    $wii->bench_end();
}

sub classify_text {
    my ($valReq, $sd, $rn) = @_;
    if (!defined $valReq || $valReq eq "") {
        $sd->{contentDetail}{"Empty"}[0]++;
        return;
    } elsif ($valReq =~ /^\s+$/) {
        $sd->{contentDetail}{"Empty Whitespace"}[0]++;
        return;
    }
    $valReq =~ s/^\s+//;
    $valReq =~ s/\s+$//;
    my @vals = $splitter ? split(/$splitter/, $valReq) : ($valReq);
    foreach my $val (@vals) {
        if ($val =~ /[\s\,]/) {
            # Space or comma = generic text
            $sd->{contentDetail}{"Text"}[0]++;
        } elsif ($val =~ /^[\-\+]?\d+$/) {
            $sd->{contentDetail}{"Integer"}[0]++;
        } elsif ($val =~ /^[\-\+]?(\d*\.)?\d+\s*%$/) {
            $sd->{contentDetail}{"Percentage"}[0]++;
        } elsif ($val =~ /^[\-\+]?\d*\.\d+$/ ||
                 $val =~ /^[\-\+]?(\d*\.\d+|\d+)[eE][\-\+]?(\d*\.\d+|\d+)$/) {
            $sd->{contentDetail}{"Float"}[0]++;
        } elsif ($val =~ /\// && -e $val) {
            $sd->{contentDetail}{"File Path"}[0]++;
        } elsif ($val =~ /^\S+\@\S+\.(com|edu)$/) {
            $sd->{contentDetail}{"eMail Address"}[0]++;
        } elsif ($val =~ /^[\d\.\-\+\s]+$/) {
            $sd->{contentDetail}{"Numeric Mix"}[0]++;
        } else {
            # Ok, we will consider this as a potential identifier
            # We can not uppercase string here - it destroys SMILES
            push @{$sd->{entries}{$val}}, $rn;
        }
    }
}

sub finish {
    if ($wii) {
        if (my $dbh = $wii->dbh()) {
            $dbh->disconnect();
        }
        if ($args->val(qw(benchmarks benchmark))) {
            if ($nocgi) {
                warn $wii->show_benchmarks( -shell => 1 );
            } else {
                print $fh $wii->show_benchmarks( -html => 1 );
            }
        }
    }
    close OUT if ($output);
}

sub build {
    my $dbh = $wii->dbh();
    $dbh->make_all();
}

sub connect {
    my $self = shift;
    my $dbName = 'whatisit';
    my ($dbType, $dbn, $dbh);
    $ENV{PGPORT} = 5433;
    $ENV{PGHOST} = 'salus.pri.bms.com';

    $dbType = "dbi:Pg:dbname=$dbName";
    $dbn    = 'tilfordc',
    eval {
        $dbh = BMS::FriendlyDBI->connect
            ($dbType, $dbn,
             undef, { RaiseError  => 0,
                      PrintError  => 0,
                      LongReadLen => 100000,
                      AutoCommit  => 1, },
             -errorfile => '/scratch/WhatIsItErrors.err',
             -noenv     => $args->{NOENV},
             -adminmail => 'tilfordc@bms.com', );
    };
    if ($dbh) {
        $dbh->schema( &schema() );
    } else {
        $args->err("Failed to connect to StandardGene database '$dbName'");
    }
    return $dbh;
}


1;
