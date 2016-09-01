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

my $VERSION = 
    ' $Id$ ';


=head1 Standardize Gene

A service designed to convert free text biological queries to Entrez Gene IDs

=head2 Usage

Command Line:

  standardizeGene.pl -param1 value1 -param2 value2 ...

URL:

 standardizeGene.pl?param1=value1&param2=value2 ...

 Stable URL: http://bioinformatics.bms.com/biocgi/standardizeGene.pl
   Beta URL: http://bioinformatics.bms.com/biocgi/tilfordc/working/maptracker/MapTracker/standardizeGene.pl

For both URL and command line, parameters are case-insensitive. On the
command line, parameters must be preceded with a dash, and a parameter
without a value will be assigned a value of 1.

=head2 Arguments

        id [Required] (also recognized: ids, input, text) The user
           query you wish to standardize. Multiple id parameters can
           be passed at once. Additionally, each passed value will be
           split on tabs, returns, newlines and commas.

   species (also recognized: taxa) Optional species filter. If
            provided, then only results matching the provided species
            will be returned.

    format [Default html] The format values should be returned
           as. Currently available formats are:

           html : Nested table structure, currently only used for
                  demonstration

           json : Serialized JavaScript Object. Includes internal
                  documentation under the 'software' object key.

           perl : DataDumper-like dump of underlying data structure

           If there are additional formats you would like, please let
           Charles Tilford know.

 textlimit [Default 30] When the program does fallback free text
           searching on a user query, this parameter limits the total
           number of hits returned. The advantage of doing so is that
           can accelerate the Oracle context search for commonly used
           words (like 'kinase'). The disadvantage is that the
           'correct' gene might be missed. To remove the limit, pass a
           value of 0.

      mime (also recognized: mimetype) Normally the mime-type emitted
           by the service will be automatically set depending on the
           format requested. If you wish to override this value, you
           can provide your own.

    pretty [Default 1] If a true value is provided, then JSON format
           will "pretty print" the serialized data, adding newlines
           and whitespace nesting for better human legibility.

=head2 Author

 Bristol-Myers Squibb
 Charles Tilford, 2010
 charles.tilford@bms.com

Please contact with bug reports or feature requests.

=cut

use strict;
use BMS::ArgumentParser;
use BMS::MapTracker::AccessDenorm;
use BMS::Utilities::Serialize;
use GD;

my $args = BMS::ArgumentParser->new
    ( -nocgi      => $ENV{HTTP_HOST} ? 0 : 1,
      -textlimit  => 30,
      -errormail  => 'charles.tilford@bms.com',
      -dumpsql    => 0,
      -pretty     => 1,
      -tiddlywiki => 'StndGene' );

my $nocgi     = $args->val(qw(nocgi));
my $format    = lc($args->val(qw(format)) || 'html');
my $input     = $args->val(qw(input id ids text)) || "";
my $specReq   = $args->val(qw(species taxa));
my $txtLimit  = $args->val(qw(textlimit)) || 0;
my $dumpsql   = $args->val(qw(dumpsql));

if ($format =~ /atom/) {
    $format = 'atom';
} elsif ($format =~ /perl|dump/) {
    $format = 'perl';
} elsif ($format =~ /json/) {
    $format = 'json';
} else {
    $format = 'html';
}

my $noHtml = $nocgi || $format eq 'json';

my @stndNS = qw(LL RSR RSP);

my %mimes = ( atom => 'xml',
              perl => 'html',
              json => 'application/json',
              html => 'html' );

my $taxRank = {
    'Homo sapiens' => 20,
    'Mus musculus' => 10,
    'Rattus norvegicus' => 5,
    'Canis lupus familiaris' => 4,
    'Pan troglodytes' => 3,
    'Macaca mulatta' => 2,
};

unless ($nocgi) {
    my $mime = $args->val(qw(mime mimetype)) || $mimes{$format} || 'plain';
    $args->set_mime( -mime => $mime ) if ($mime);
    if ($format eq 'atom') {
        $SIG{__WARN__} = sub { print STDERR join("\n", @_) };
    }
}

&HTML_HEADER();

my $ad = BMS::MapTracker::AccessDenorm->new( );
# die $args->branch(-maxany => 100, -ref => \%ENV);
if (&PROCESS($input)) {
    
} else {
    # print "<pre>".$args->to_text()."</pre>";
    &HTML_INTERFACE();
}

&HTML_FOOTER();

sub PROCESS {
    my ($req) = @_;
    return 0 unless ($req);
    my (%words, %phrases, %failed, %refused);
    my $minLenW = 2;
    my $minLenP = 3;
    my @reqs = ref($req) ? @{$req} : ( $req );
    foreach my $txt (@reqs) {
        foreach my $bit1 (split(/\s*[\t\n\r\,]+\s*/sm, $txt)) {
            $bit1 =~ s/\s+/ /g;
            my @morebits = ($bit1);
            if ($bit1 =~ / /) {
                $phrases{uc($bit1)} ||= $bit1;
                @morebits = split(/\s+/, $bit1);
            }
            foreach my $bit2 (@morebits) {
                if (length($bit2) < $minLenW) {
                    $failed{$bit2} = "IDs must be at lest $minLenW characters long"
                        if ($bit2 eq $bit1);
                    next;
                }
                if ($bit2 =~ /^\d+$/) {
                    if ($bit2 eq $bit1) {
                        $failed{$bit2} = "Pure integer IDs are not allowed";
                        $refused{$bit2} = $bit2;
                    }
                    next;
                }
                $words{uc($bit2)} ||= $bit2;
            }
        }
    }

    my %loci;
    my %overall;
    my @ucWords = keys %words;
    my %needed  = map { $_ => 1 } @ucWords;
    map { delete $phrases{$_} } @ucWords;

    foreach my $word (values %words) {
        
        my $ns = $ad->guess_namespace_careful( $word );
        unless ($ns) {
            $failed{uc($word)} = "Unrecognized identifier";
            next;
        }
        if ($ns eq 'LL') {
            my ($ll) = $ad->standardize_id($word, $ns);
            $loci{$ll}{$ll} = 1;
            delete $needed{uc($ll)};
            next;
        }
        my $rows = $ad->convert
            ( -id => $word, -ns1 => $ns, -ns2 => 'LL', -ignorecase => 1,
              -int => $specReq, -intns => 'TAX', -dumpsql => $dumpsql,
              -cols => ['term_in', 'term_out','matched'], -nullscore => -1);
        foreach my $row (@{$rows}) {
            my ($wd, $ll, $sc) = @{$row};
            next unless ($ll);
            unless (defined $loci{$ll}{$wd} && $loci{$ll}{$wd} > $sc) {
                $loci{$ll}{$wd} = $sc;
                delete $needed{uc($wd)};
            }
            unless (defined $overall{$ll} && $overall{$ll} > $sc) {
                $overall{$ll} = $sc;
            }
        }
    }
 
    my @phrases = (values %phrases, keys %needed);
    unless ($#phrases == -1) {
        my @rows;
        my @success;
        # Start with the longest phrases first:
        foreach my $phrase (sort { length($b) <=> length($a) } @phrases) {
            if (length($phrase) < $minLenP) {
                $failed{$phrase} ||= "Free text terms must be at lest $minLenP characters long";
                next;
            }
            my $doneAlready = 0;
            for my $s (0..$#success) {
                next unless ($success[$s] =~ /\Q$phrase\E/i);
                $doneAlready = 1;
                last;
            }
            if ($doneAlready) {
                delete $failed{$phrase};
                next;
            }
            my $textHits = $ad->description_lookup
                ( -desc => [$phrase, '!Deprecated'],
                  -ns => 'LL', -limit => $txtLimit ? $txtLimit + 1 : 0,
                  -int  => $specReq, -intns => 'TAX', -dumpsql => $dumpsql,
                  -split => $args->{SPLIT}, -join => 'or');
            
            my $num = $#{$textHits} + 1;
            if ($txtLimit && $num > $txtLimit) {
                $failed{$phrase} ||= "Not all free text matches shown (limit set to $txtLimit)";
                pop @{$textHits};
            } elsif ($num) {
                push @success, $phrase;
            }
            push @rows, @{$textHits};
        }
        my %needed = map { $_ => 1 } @phrases;
        my $txtSc = .1;
        foreach my $row (@rows) {
            my ($ll, $ns, $desc) = @{$row};
            $overall{$ll} = $txtSc;
            foreach my $phrase (@phrases) {
                if ($desc =~ /\Q$phrase\E/i) {
                    $loci{$ll}{$phrase} = $txtSc;
                    delete $needed{$phrase};
                }
            }
        }
        map { $failed{$_} ||= "No loci found through free text search" } keys %needed;
    }

    map { $failed{$_} ||= "No loci found using ID lookup" } keys %needed;
    # map { $failed{$_} ||= "Text search of phrases not yet implemented" } keys %phrases;

    my $data = {
        loci => \%loci,
        losc => \%overall,
        fail => \%failed,
        qry  => { %words, %phrases, %refused },
    };

    &extend_data( $data );

    my $rv = "";
    if ($format eq 'html') {
        $rv = &loci_to_html( $data );
    } elsif ($format eq 'perl') {
        $rv = $args->branch($data);
        $rv = "<pre>$rv</pre>" unless ($nocgi);
    } else {
        $rv = &loci_to_json( $data );
    }

    print $rv;
}

sub extend_data {
    my ($data) = @_;
    my @locs = keys %{$data->{loci}};
    return if ($#locs == -1);
    my $rnas = $ad->convert
        ( -id => \@locs, -ns1 => 'LL', -ns2 => 'RSR',
          -cols => ['term_in', 'term_out','matched'], -nullscore => -1);
    my %descs = ( LL => { map { $_ => 1 } @locs } );
    foreach my $row (@{$rnas}) {
        my ($ll, $rsr, $sc) = @{$row};
        next unless ($ll && $rsr);
        my $hash = $data->{l2r}{$ll} ||= {};
        next if (defined $hash->{$rsr} && $hash->{$rsr} > $sc);
        $hash->{$rsr} = $sc;
        $descs{RSR}{$rsr} = 1;
    }

    my $syms = $ad->convert
        ( -id => \@locs, -ns1 => 'LL', -ns2 => 'SYM',
          -cols => ['term_in', 'term_out','matched'], -nullscore => -1);
    foreach my $row (@{$syms}) {
        my ($ll, $sym, $sc) = @{$row};
        next unless ($ll && $sym);
        my $hash = $data->{l2s}{$ll} ||= {};
        next if (defined $hash->{$sym} && $hash->{$sym} > $sc);
        $hash->{$sym} = $sc;
    }

    my $taxa = $ad->convert
        ( -id => \@locs, -ns1 => 'LL', -ns2 => 'TAX',
          -cols => ['term_in', 'term_out','matched'], -nullscore => -1);
    foreach my $row (@{$taxa}) {
        my ($ll, $tax, $sc) = @{$row};
        next unless ($ll && $tax);
        my $hash = $data->{l2t}{$ll} ||= {};
        next if (defined $hash->{$tax} && $hash->{$tax} > $sc);
        $hash->{$tax} = $sc;
        $data->{t2l}{$tax}{$ll} = 1;
    }
    # Make sure we note loci that had no taxa:
    my %needTax = map { $_ => 1 } @locs;
    map { delete $needTax{$_} } keys %{$data->{l2t}};
    map { $data->{t2l}{'Unknown Taxonomy'}{$_} = 1 } keys %needTax;

    my @rsrs = keys %{$descs{RSR}};
    unless ($#rsrs == -1) {
        my $prots = $ad->convert
            ( -id => \@rsrs, -ns1 => 'rsr', -ns2 => 'rsp',
              -cols => ['term_in', 'term_out','matched'], -nullscore => -1);
        foreach my $row (@{$prots}) {
            my ($rsr, $rsp, $sc) = @{$row};
            next unless ($rsp && $rsr);
            my $hash = $data->{r2p}{$rsr} ||= {};
            next if (defined $hash->{$rsp} && $hash->{$rsp} > $sc);
            $hash->{$rsp} = $sc;
            $descs{RSP}{$rsp} = 1;
        }
    }
    while (my ($ns, $hash) = each %descs) {
        foreach my $id (keys %{$hash}) {
            $data->{desc}{$id} = $ad->description( -id => $id, -ns => $ns);
        }
    }
}

sub loci_to_json {
    my ($data) = @_;
    my $qrys  = $data->{qry};
    my @qtxt  = sort { length($b) <=> length($a) } keys %{$qrys || {}};
    my $rv = {
        userQuery => \@qtxt,
    };
    if ($VERSION =~ /\,v (\S+)/) { $rv->{software}{scriptVers} = $1; }
    my $scrp = $0;
    unless ($scrp =~ /^\//) {
        my $pwd = `pwd`;
        $pwd =~ s/[\n\r]+$//;
        $scrp = "$pwd/$scrp";
    }
    $rv->{software}{script} = $scrp;
    if (my $fail = $data->{fail}) {
        my @fk   = keys %{$fail};
        $rv->{failed} = $fail unless ($#fk == -1);
    }
    my $loci = $rv->{locus} = [];
    foreach my $ll (sort keys %{$data->{loci}}) {
        my %locDat = ( id    => $ll,
                       desc  => $data->{desc}{$ll},
                       score => $data->{losc}{$ll},
                       hitBy => $data->{loci}{$ll},
                       taxa  => $data->{l2t}{$ll}, );
        push @{$loci}, \%locDat;
        my $ind = $#{$loci};
        my (@goodTax, @badTax, @useTax);
        while (my ($tax, $sc) = each %{$data->{l2t}{$ll}}) {
            if ($sc == 1) {
                push @goodTax, $tax;
            } else {
                push @badTax, $tax;
            }
        }
        @useTax = @goodTax;
        if ($#goodTax == -1) {
            if ($#badTax == -1) {
                push @{$locDat{errors}}, "No taxa found";
            } elsif ($#badTax == 0) {
                push @{$locDat{errors}}, "Reported taxa not 100% confident";
                @useTax = @badTax;
            } else {
                push @{$locDat{errors}}, "Multiple low-confidence taxa recovered";
            }
        } elsif ($#goodTax > 0) {
            push @{$locDat{errors}}, "Multiple high-confidence taxa recovered";
        }
        unless ($#useTax == -1) {
            my @names;
            foreach my $tax (@useTax) {
                my $nm = $ad->description( -id => $tax, -ns1 => 'TAX' );
                if ($nm =~ /^(.+)\:/) { push @names, $1; }
            }
            $locDat{taxa}     = join(',', @useTax);
            $locDat{taxaName} = join(',', @names) unless ($#names == -1);
            push @{$rv->{byTaxa}{ $locDat{taxa} }}, $ind
        }
        foreach my $sym (sort { uc($a) cmp uc($b) }
                         keys  %{$data->{l2s}{$ll} || {}}) {
            my $sc = $data->{l2s}{$ll}{$sym};
            if ($sc == 1) {
                push @{$locDat{symOffical}}, $sym;
            } else {
                push @{$locDat{symOther}}, $sym;
            }
        }
        foreach my $rna (sort keys %{$data->{l2r}{$ll}}) {
            my %rDat = ( id   => $rna,
                         desc => $data->{desc}{$rna}  );
            push @{$locDat{rna}}, \%rDat;
            push @{$rDat{errors}}, "RNA to locus assignment confidence below 100%" if ($data->{l2r}{$ll}{$rna} < 1);
            my @rsps = sort keys %{$data->{r2p}{$rna}};
            foreach my $rsp (@rsps) {
                my %pDat = ( id   => $rsp,
                             desc =>  $data->{desc}{$rsp} );
                push @{$rDat{protein}}, \%pDat;
            }
            push @{$rDat{errors}}, "Multiple protein IDs for RNA"
                if ($#rsps > 0);
        }
    }
    foreach my $key ($args->all_keys) {
        $rv->{software}{arguments}{$key} = $args->val($key);
    }
    $rv->{software}{geekHelp} = {
        arguments => "Parameters used by service, including defaults and those passed by caller",
        byTaxa    => "Lookup hash organized by species. Values are array indices referencing the 'loci' array",
        desc      => "Human-readable description of the relevant item",
        errors    => "Array of human-readable problems encountered collecting results",
        failed    => "Queries that failed to recover information, with details",
        geekHelp  => "Help for programmers describing the hash keys used in this data structure",
        geekNotes => "General commentary provided for programmers",
        hitBy     => "Query term(s) that recovered the relevant item",
        id        => "The canonical ID for the relevant item",
        locus     => "List of LocusLink genes hit by the queries",
        protein   => "List of proteins associated with an RNA. Should be zero (non-protein coding) or one (protein coding)",
        rna       => "List of RNAs associated with a locus",
        score     => "Confidence score of the relevant item",
        script    => "The executable path of the service",
        scriptVers => "The software version of the service",
        software  => "Information pertaining to details of the software and how it was run",
        symOffical => "HGNC 'official' gene symbol. Should be at most one",
        symOther   => "Non-official gene symbols. Zero or more",
        taxa      => "NCBI species name of the relevant item",
        taxaName  => "Human-readable organism name, when available",
        userQuery => "The query string(s) provided to the service",
    };
    $rv->{software}{geekNotes} = ['Scores are between 0 (explicit does-not-match) to 1 (perfect match), plus -1 (undefined / unknown)',
                        ];
    my $ser = BMS::Utilities::Serialize->new();
    $ser->obj_to_json($rv, $args->val(qw(pretty)))."\n";
}

sub loci_to_html {
    my ($data) = @_;
    my $qrys  = $data->{qry};
    my @qtxt  = sort { length($b) <=> length($a) } keys %{$qrys || {}};
    my @cols  = ("Locus", "Transcript", "Protein", "Description");
    my $cn    = $#cols + 1;
    my $html  = "";
    my $deptag = "{Deprecated}";
    my $depHtm = "<span class='err'>$deptag</span>";
    my $locH  = $data->{losc} || {};
    my @taxae = sort { ($taxRank->{$b} || 0) <=> ($taxRank->{$a} || 0) ||
                           uc($a) cmp uc($b) } keys %{$data->{t2l}};
    if ($#taxae == -1) {
        $html .= "<div class='err'>No loci were found for your queries</div>\n";
    } else {
        $html .= &htmlScoreKey();
        $html .= "<table class='gatab'>\n";
        $html .= " <tbody>\n";
        $html .= " <tr>".join('', map { "  <th>$_</th>\n" } @cols)." </tr>\n";
        foreach my $tax (@taxae) {
            $html .= " <tr><td class='TAX' colspan='$cn'>";
            my $tdesc = $ad->description( -id => $tax, -ns1 => 'TAX') || "";
            if ($tdesc =~ /^([^\:]+)\:/) {
                $tdesc = $1;
                substr($tdesc, 0, 1) = uc(substr($tdesc, 0, 1));
                $tdesc = "<b>$tdesc</b> - $tax";
            } else {
                $tdesc = $tax;
            }
            $html .= $tdesc;
            $html .= "</td></tr>\n";
            my @locs = sort keys %{$data->{t2l}{$tax}};
            foreach my $ll (@locs) {
                my $rnaH = $data->{l2r}{$ll} || {};
                my @rnas = sort keys %{$rnaH};
                @rnas = ("") if ($#rnas == -1);
                my $rn = $#rnas + 1;
                $html .= "<tr class='locrow'>\n";
                $html .= " <td rowspan='$rn' class='LL'>";
                $html .= "  <div class='acc'>\n";
                $html .= "  <input type='checkbox' class='choose' name='choice' value='$ll' />\n";
                $html .= sprintf
                        ("  <span class='LL %s'>%s</span>\n",
                         &class4data($ll, $qrys, $locH->{$ll}), $ll);
                $html .= "  </div>\n";
                # Add symbols
                my $symH = $data->{l2s}{$ll} ||= {};
                my @syms;
                foreach my $sym (sort { $symH->{$b} <=> 
                                            $symH->{$a} } keys %{$symH}) {
                    push @syms, sprintf
                        ("<span class='SYM %s'>%s</span>",
                         &class4data($sym, $qrys, $symH->{$sym}), $sym);
                }
                push @syms, "<span class='note'>No symbols</span>"
                    if ($#syms == -1);
                $html .= "  <br />".join(" ", @syms)."\n";
                $html .= " </td>\n";
                my @dTargs = ([$ll]);
                for my $r (0..$#rnas) {
                    $html .= "<tr>\n" if ($r);
                    my $rsr = $rnas[$r];
                    if ($rsr) {
                        unshift @dTargs, [$rsr];
                        $html .= " <td class='acc RSR'>\n";
                        $html .= "  <div class='acc'>\n";
                        $html .= "  <input type='checkbox' class='choose' name='choice' value='$rsr' />\n";

                        $html .= sprintf
                            ("  <span class='%s'>%s</span>\n",
                             &class4data($rsr, $qrys, $rnaH->{$rsr}), $rsr);
                        $html .= "  </div>\n";
                        $html .= " </td>\n";
                        # Add protein
                        my $rspH = $data->{r2p}{$rsr} ||= {};
                        my @prots = sort keys %{$rspH};
                        if ($#prots == -1) {
                            $html .= "<td />";
                        } else {
                            $html .= " <td class='RSP'>\n";
                            foreach my $rsp (@prots) {
                                $html .= "  <div class='acc'>\n";
                                $html .= "  <input type='checkbox' class='choose' name='choice' value='$rsp' />\n";
                                $html .= sprintf
                                    ("  <span class='%s'>%s</span>\n",
                                     &class4data($rsp, $qrys, $rspH->{$rsp}), $rsp);
                                $html .= "  </div>\n";
                            }
                            $html .= " </td>\n";
                            unshift @dTargs, \@prots;
                        }
                    } else {
                        $html .= "<td /><td />";
                    }
                    # Add description : RSP > RSR > LL > No Description
                    $html .= " <td class='DESC'>\n";
                    while (my $dt = shift @dTargs) {
                        my @descs;
                        foreach my $id (@{$dt}) {
                            if (my $d = $data->{desc}{$id}) {
                                $d =~ s/\Q$deptag\E/$depHtm/g;
                                foreach my $qt (@qtxt) {
                                    if ($d =~ /(\Q$qt\E)/i) {
                                        my $cs  = $1;
                                        my $rep = "<span class='qry'>$cs</span>";
                                        $d =~ s/\Q$qt\E/$rep/gi;
                                    }
                                }
                                push @descs, $d;
                            }
                        }
                        unless ($#descs == -1) {
                            $html .= join(" <span class='join'>and</span> ",
                                          @descs);
                            last;
                        }
                    }
                    $html .= " </td>\n";
                    $html .= "</tr>\n";
                }
            }
        }
        my %byReason;
        my $f = $data->{fail} || {};
        map { push @{$byReason{$f->{$_}}}, $qrys->{uc($_)} || $_ } sort { uc($a) cmp uc($b) } keys %{$f};
        foreach my $reason (sort keys %byReason) {
            $html .= "<tr><td colspan='$cn' class='err'>$reason</td></tr>";
            $html .= "<tr class=''><td colspan='$cn'>\n";
            foreach my $qry (@{$byReason{$reason}}) {
                $html .= sprintf
                    ("  <span class='%s'>%s</span>\n",
                     &class4data($qry, $qrys), $qry);
            }
            $html .= "</td></tr>\n";
        }
        $html .= " </tbody>\n";
        $html .= "</table>";
        $html .= "<button onclick='alert(\"In a functional tool clicking this button\\nwould select the checked genes / RNAs / proteins\\nfor the scientist\")'>Choose checked entries</button>\n";
        $html .= "<button onclick='alert(\"At this point the researcher would be provided\\nwith additional options to help them find their gene,\\nor would be put in contact with an expert human for help\")'>None of these are appropriate</button><br />\n";
    }
    return $html;
}

sub htmlScoreKey {
    my $html = "<div class='key' style='font-weight: bold;'>\n";
    $html .= "Confidence Key:\n";
    foreach my $cls ('Un', (0..10)) {
        my $txt = $cls =~ /^\d+$/ ? sprintf("%d%%", $cls * 10) : 'Unk';
        $html .= " <span class='sc$cls'>$txt</span>\n";
    }
    $html .= "</div>\n";
    return $html;
}

sub class4data {
    my ($txt, $qry, $sc) = @_;
    my @cls;
    push @cls, &score2class( $sc ) if (defined $sc);
    push @cls, "qry" if ($txt && $qry && $qry->{uc($txt)});
    return join(" ", @cls) || "";
}

sub score2class {
    my $sc = shift;
    return 'scUn' if (!defined $sc || $sc < 0);
    return 'sc' . int( $sc * 10);
}

sub HTML_HEADER {
    return if ($noHtml);
    print <<EOF;
<html>
<head>
<link type='text/css' rel='stylesheet' href='/biohtml/css/stndGene.css' />
  <title>BMS Standard Gene</title>
  <link rel="shortcut icon" href="/biohtml/images/BmsStandard_16x16.png">
</head><body>
EOF

}

sub HTML_INTERFACE {
    return if ($noHtml);
    my $taxOpts = join("", map { "<option value='$_'>$_</option>\n" } sort {
        ($taxRank->{$b} || 0) <=> ($taxRank->{$a} || 0) ||
            uc($a) cmp uc($b) } keys %{$taxRank});
    print <<EOF;
<form action='standardizeGene.pl'>
<h3>Enter terms you wish to standardize</h3>
<textarea name='input' cols='40' rows='10'>$input</textarea><br />
Restrict to species: <select name='species'>
<option value=''></option>
$taxOpts
</select><br />
<input type='submit' value='Standardize' />
</form>
EOF

}

sub HTML_FOOTER {
    return if ($noHtml);
    print <<EOF;
</body></html>
EOF

}
