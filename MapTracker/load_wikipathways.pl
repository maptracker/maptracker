#!/stf/biobin/perl -w

my $isBeta;
BEGIN {
    # Needed to make my libraries available to Perl64:
    # use lib '/stf/biocgi/tilfordc/released';
    # use lib '/apps/sys/perl/lib/site_perl/5.12.0/';
    use lib '/stf/biocgi/tilfordc/patch_lib';
    # Allows usage of beta modules to be tested:
    my $prog = $0; my $dir = `pwd`;
    if ($prog =~ /working/ || $dir =~ /working/) {
        require lib;
        import lib '/stf/biocgi/tilfordc/perllib';
        $isBeta = 1;
    } else {
        $isBeta = 0;
    }
    $| = 1;
    print '';
}

use strict;
# use BMS::Utilities;
use BMS::MapTracker::AccessDenorm;
use BMS::MapTracker::GenAccService;
use BMS::FriendlyGraph;
use XML::Simple;
use BMS::BmsArgumentParser;
use BMS::Utilities::Escape;
use BMS::Utilities::FileUtilities;
use BMS::FriendlySAX;


my $args = BMS::BmsArgumentParser->new
    ( -nocgi      => $ENV{HTTP_HOST} ? 0 : 1,
      -ageall     => '31 Jul 1:00pm',
      -errormail  => 'charles.tilford@bms.com',
      -symlen     => 10,
      -loaddir    => 'WP',
      -bracket    => 1,
      -fork       => 20,
      -savetransz => 0,
      -output     => "Wikipathways_Node_Parse.txt",
      -wget       => '/work5/tilfordc/WGET/mirror_wikipathways.sh',
      -tmpdir     => "/stf/biohtml/tmp", );


my $noSafeAge = $args->val(qw(nosafeage));
my $age       = $args->val(qw(age ageall));
my $cage      = $args->val(qw(cloudage ageall));
my $tm        = $args->val(qw(testmode));
my $doWarn    = $args->val(qw(warn));
my $tmpdir    = $args->val(qw(tempdir tmpdir));
my $limit     = $args->val(qw(limit));
my $input     = $args->val(qw(input gpml xml));
my $output    = $args->val(qw(output));
my $maxSymLen = $args->val(qw(symlength symlen)) || 0;
my $clobber   = $args->val(qw(clobber));
my $minSymLen = 2;
my $symChar   = '[A-Z0-9\-]+';

$args->debug->skipkey( [qw( PARENT KIDS) ]);

my (%data, %notes, $fdat, $fc, %stuff, %nonLoc);

my @gasCom = ( -age       => $age,
               -cloudage  => $cage,
               -format    => 'tsv',
               -fork      => $args->val(qw(fork forknum)),
               -verbose   => 0,
               -keepcomma => 1,
               -warn      => $doWarn,
               -quiet     => 1,
               -scramble  => 1);

my $symBL = { 
    map { $_ => 1 } qw
        (ADP ATP GDP GTP CO2 LPS 
         Catalyst Leptin Ligands Protein ubiquitin vRNPs mRNA Adipose
         Agonist Antagonist Gene)
    };

my $regexp = {
    SP   => '([OPQ]\d[A-Z\d]{3}\d|[A-NR-Z]\d[A-Z][A-Z\d]{2}\d)(\-\d+)?',
    LL   => 'LOC\d+',
    UP   => '([OPQ]\d[A-Z\d]{3}\d|[A-NR-Z]\d[A-Z][A-Z\d]{2}\d)(\-\d+)?',
    ENSE => 'ENS[A-Z]{0,4}E\d+(\.\d+)?',
    ENSG => 'ENS[A-Z]{0,4}G\d+(\.\d+)?',
    ENSP => 'ENS[A-Z]{0,4}P\d+(\.\d+)?(\{[^\}]+\})?',
    ENST => 'ENS[A-Z]{0,4}T\d+(\.\d+)?(\{[^\}]+\})?',
};

if ($args->val(qw(store load))) {
    &store();
} elsif ($input) {
    if (-f $input) {
        &parse_file($input);
    } elsif (-d $input) {
        $input =~ s/\/$//;
        &parse_dir($input);
    }
}

if (exists $notes{"Inferred symbol"}) {
    my $h = $notes{"Inferred symbol"};
    while (my ($t, $n) = each %{$h}) {
        delete $h->{$t} if ($n == 1);
    }
}


foreach my $nk (sort keys %notes) {
    my @msg = ($nk);
    my $h = $notes{$nk};
    foreach my $snk (sort {$h->{$b} <=> $h->{$a} || $a cmp $b } keys %{$h}) {
        push @msg, "$snk : $h->{$snk}";
    }
    $args->msg(@msg);
}

if (-s $output) {
    $args->msg("Results serialized to file", $output);
}

sub read_file {
    return unless (-s $output);
    return if ($stuff{AlreadyRead}++);
    open(OUT, "<$output") || $args->death
        ("Failed to read output file", $output, $!);
    while (<OUT>) {
        s/[\n\r]+$//;
        my @row = split(/\t/);
        my ($wp, $vers, $taxa, $name) = @row;
        next unless ($taxa && $name);
        $taxa = "Canis lupus familiaris" if ($taxa eq 'Canis familiaris');
        # Only keep the most recent version
        next if ($data{$wp} && $data{$wp}{VERS} > $vers);
        my $dat = $data{$wp} = { VERS => $vers, name => $name, taxa => $taxa };
        for my $c (4..$#row) {
            my $v = $row[$c];
            if ($v =~ /^([A-Z]+)\:(.+)$/) {
                my ($ns, $ids) = ($1, $2);
                my @list = split(/\,/, $ids);
                map { s/\-\d+$// } @list if ($ns =~ /^(UP|TR|SP)$/);
                next if ($#list == -1);
                $dat->{IDS}{$ns} = \@list;
                if ($ns eq 'SYM') {
                    map { $notes{"Gene Symbol Length"}{length($_)}++ } @list;
                }
                if ($ns ne 'LL') {
                    map { $nonLoc{$taxa}{$ns}{$_}++ } @list;
                }
            } else {
                $args->msg("Malformed data entry column $c", $v);
            }
        }
    }
    my @found = keys %data;
    $args->msg("Found ".scalar(@found)." WP IDs in output file", $output);
    close OUT;
}

sub store {
    &read_file();
    my $lu = &find_aliases();
    my @wps = sort { $a <=> $b } keys %data;
    if (my $wp = $args->val(qw(wp id))) {
        @wps = ref($wp) ? @{$wp} : split(/[^0-9]+/, $wp);
    }
    my $wnm = "WikiPathways";
    my $lh      = BMS::MapTracker::LoadHelper->new
        ( -user     => $wnm,
          -basedir  => $args->val(qw(basedir loaddir)),
          -testfile => $args->val('testfile'),
          -testmode => $tm );
    my $num = 0;
    my %missed;
    foreach my $wp (@wps) {
        my $wH = $data{$wp};
        my ($vers, $name, $taxa) = map { $wH->{$_} } qw(VERS name taxa);
        my $par  = "$taxa WikiPathways";
        unless ($stuff{"Group for $taxa"}++) {
            my $gpar = "All WikiPathways";
            $lh->set_class($par, 'Group', 'tilfordc');
            $lh->set_class($gpar, 'Group', 'tilfordc')
                unless ($stuff{"Grandparent"}++);
            $lh->set_edge( -name1 => $par,
                           -name2 => $gpar,
                           -type  => 'is a member of',
                           );
            
        }
        next;
        my $id = "$wnm:$wp";
        if (0) {
            $lh->kill_edge( -name1 => $name,
                            -type  => 'has member');
            $lh->kill_class( $name, $wnm );
            $lh->kill_taxa( $name );
        }
        $lh->set_taxa( $id, $taxa );
        $lh->set_class( $id, $wnm );
        $lh->set_edge( -name1 => $id,
                       -name2 => "WP$wp",
                       -type  => "is the preferred lexical variant of",
                       -auth  => 'tilfordc' );
        $lh->set_edge( -name1 => $id,
                       -name2 => "#FreeText#$name",
                       -type  => "is a shorter term for" );
        $lh->kill_edge( -name1 => $id,
                        -type  => 'has member');
        my (%hit);
        foreach my $ns (sort keys %{$wH->{IDS}}) {
            my @ids = @{$wH->{IDS}{$ns}};
            foreach my $oid (@ids) {
                my @tags = (["Pathway Version",undef, $vers]);
                if ($ns eq 'LL') {
                    $hit{$oid} = [ 1, 'Explicit'];
                } elsif (my $dat = $lu->{$taxa}{$ns}{$oid}) {
                    my $mid = $dat->[0];
                    my $sc  = $dat->[1];
                    if ($hit{$mid} && $hit{$mid}[0] >= $sc) {
                        # warn "$oid -> $mid supplanted by other data\n";
                    } else {
                        $hit{$mid} = [ $sc, $sc == 1 ? 
                                       'Confident' : 'Inferred' ];
                    }
                } else {
                    $missed{$ns}{$oid}++;
                    next;
                }
            }
        }
        $lh->set_edge( -name1 => $id,
                       -name2 => $par,
                       -type  => 'is a member of',
                       -tags  => [["Pathway Version",undef, $vers]],
                       );
        
        while (my ($node2, $det) = each %hit) {
            my ($sc, $conf) = @{$det};
            $lh->set_edge( -name1 => $id,
                           -name2 => $node2,
                           -type  => 'has member',
                           -tags  => [['Assignment Confidence', $conf, $sc]] );
        }
        $lh->write_threshold_quick( 500 );
        $num++;
        last if ($limit && $num >= $limit);
    }
    $lh->write();
    my $missF = "WP_Missed_Mappings.tsv";
    open(MISS, ">$missF") || $args->death
        ("Failed to write missed alias file", $missF, $!);
    print MISS join("\t", qw(ID NS Pathways))."\n";
    foreach my $ns (sort keys %missed) {
        my $h = $missed{$ns};
        foreach my $id (sort { $h->{$b} <=> 
                                   $h->{$a} || $a cmp $b } keys %{$h}) {
            my $n = $h->{$id};
            print MISS join("\t", $id, $ns, $n)."\n";
        }
    }
    close MISS;
    $args->msg("[MISS]", $missF);
}

sub find_aliases {
    my $aliH = shift || \%nonLoc;
    my %rv;
    foreach my $tax (sort keys %{$aliH}) {
        my $taxH = $aliH->{$tax};
        foreach my $ns1 (sort keys %{$taxH}) {
            my @ids = sort keys %{$taxH->{$ns1}};
            my $dFile = "WP_$tax-$ns1-LL.tsv";
            $dFile =~ s/\s+/_/g;
            $args->msg("$tax [$ns1] ".scalar(@ids). " IDs");
            my $rData = &cached_data
                ( -ids => \@ids, -ns1 => $ns1, -ns2 => 'LL',
                  -format => 'tsv', -output => $dFile,
                  -nullscore => -1, -keepbest => 1,
                  -int  => $tax, -intns => 'TAX', -ignorecase => 1,
                  -warn => $doWarn, -cols => 'termin,termout,score' );
            my %lu;
            foreach my $row (@{$rData}) {
                my ($in, $out, $sc) = @{$row};
                next unless ($in && $out);
                $lu{$in}{$sc}{$out} = 1;
            }
            while (my ($in, $scH) = each %lu) {
                my @scores = sort { $b <=> $a } keys %{$scH};
                my $sc = $scores[0];
                my @outs = keys %{$scH->{$sc}};
                if ($#outs == 0) {
                    $sc /= 2 if ($sc > 0 && $#scores != 0);
                    $rv{$tax}{$ns1}{$in} = [ $outs[0], $sc ];
                }
            }
        }
    }
    return \%rv;
}

sub parse_dir {
    my $dir = shift;
    &read_file();
    opendir(DIR, $dir) || $args->death
        ("Failed to read directory", $dir, $!);
    my @files;
    foreach my $file (readdir DIR) {
        next if ($file =~ /^\./);
        if ($file =~ /WP(\d+)_(\d+)\.gpml$/) {
            my ($wp, $vers) = ($1, $2);
            next unless(!$data{$wp} || $data{$wp}{VERS} < $vers);
            push @files, "$dir/$file";
            last if ($limit && $#files + 1 >= $limit);
        }
    }
    closedir DIR;
    foreach my $file (@files) {
        &parse_file($file);
    }
}

sub parse_file {
    my $file = shift;
    return unless ($file);
    
    $fdat = undef;
    if ($file =~ /WP(\d+)_(\d+)\.gpml$/) {
        my ($wp, $vers) = ($1, $2);
        $fdat = { WP => $wp, VERS => $vers, file => $file };
    } else {
        $args->msg("Could not determine ID from file name", $file);
        return;
    }
    my $fs = BMS::FriendlySAX->new
        ( -file    => $file,
          -tag     => ['DataNode'],
          -skip    => ['Graphics'],
          -method  => \&_parse_node,  );
    &write_line( $fdat );
    # warn $args->branch($fdat);
}

sub write_line {
    my ($fdat) = @_;
    my @row;
    my ($wp, $vers, $taxa, $name) = @row =
        ($fdat->{WP}, $fdat->{VERS}, $fdat->{taxa}, $fdat->{name});
    my $file = $fdat->{file};
    unless ($name) {
        ($name, $taxa) = ("","");
        if ($fdat->{IDS}) {
            $args->msg("Failed to recover graph name", $file);
        } else {
            $args->msg("No recognizable nodes", $file);
        }
    }
    my @mbits;
    foreach my $ns (sort keys %{$fdat->{IDS}}) {
        my @i = @{$fdat->{IDS}{$ns}};
        my %u = map { $_ => 1 } @i;
        my @ids;
        foreach my $id (sort keys %u) {
            if ($id =~ /^\d+$/) {
                $notes{"Rejected $ns"}{"Pure numeric ID"}++;
            } elsif ($ns =~ /^(SYM)$/ && $id !~ /^$symChar$/) {
                $notes{"Rejected $ns"}{"Non-alphanumeric"}++;
            } elsif ($ns eq 'SYM' && length($id) > $maxSymLen) {
                $notes{"Rejected $ns"}{"> $maxSymLen characters"}++;
            } elsif ($ns eq 'SYM' && length($id) < $minSymLen) {
                $notes{"Rejected $ns"}{"< $minSymLen characters"}++;
            } elsif ($ns eq 'SYM' && $symBL->{$id}) {
                $notes{"Rejected $ns"}{"Blacklisted symbols"}++;
            } else {
                push @ids, $id;
            }
        }
        next if ($#ids == -1);
        push @mbits, scalar(@ids)." $ns";
        push @row, "$ns:".join(',', @ids);
    }
    my $line = join("\t", map { defined $_ ? $_ : '' } @row)."\n";
    if ($fc) {
        die "HERE";
    } elsif ($output) {
        open(OUT, ">>$output") || $args->death
            ("Failed to write output", $output, $!);
        print OUT $line;
        close OUT;
    } else {
        print $line;
    }
    $args->msg("Parsed $file");
    if (my $err = $fdat->{Error}) {
        $args->msg("[ERRORS]", @{$err});
    }
}

sub _parse_node {
    my $node = shift;
    unless ($fdat->{name}) {
        my $par = $node->{PARENT};
        if ($fdat->{name} = &_clean_text($par->{ATTR}{Name})) {
            $fdat->{taxa} = &_clean_text($par->{ATTR}{Organism});
        } else {
            warn BMS::FriendlySAX::node_to_text($node);
            $args->death("Failed to find name");
        }
    }
    my $type = $node->{ATTR}{Type} || '-UNDEF-';
    unless ($type eq 'GeneProduct' || 
            $type eq 'Protein' ||
            $type eq 'RNA' ||
            $type eq 'Rna') {
        $notes{"Ignored type"}{$type}++;
        return;
    }
    my ($good);
    foreach my $xr (@{$node->{BYTAG}{Xref}}) {
        my $ids = &_clean_list($xr->{ATTR}{ID});
        next unless ($ids);
        my $db = &_clean_text($xr->{ATTR}{Database}) || "-UNDEF-";
        my $ns;
        if ($db eq 'Entrez Gene') {
            $ns = 'LL';
            for my $i (0..$#{$ids}) {
                $ids->[$i] = "LOC".$ids->[$i] if ($ids->[$i] =~ /^\d+$/);
            }
        } elsif ($db eq 'UniProt' || $db eq 'Uniprot/TrEMBL') {
            $ns = 'UP';
        } elsif ($db eq 'SwissProt') {
            $ns = 'SP';
            map { s/ .+// } @{$ids};
        } elsif ($db eq 'Affy') {
            $ns = 'APS';
        } elsif ($db =~ /Ensembl/i) {
            foreach my $id (@{$ids}) {
                if ($id =~ /^ENS[A-Z]{0,4}([GTPE])/) {
                    $ns = "ENS$1";
                    last;
                }
            }
        } else {
            $notes{"Ignored XREF"}{$db}++;
        }
        if ($ns) {
            $good++;
            &_store_ids( $fdat, $ns, $ids);
        }
    }
    return if ($good);
    if (my $name = &_clean_text($node->{ATTR}{TextLabel})) {
        my $l = length($name);
        if ($l <= $maxSymLen &&
            $l >= $minSymLen &&
            $name =~ /^$symChar$/i &&
            ! $symBL->{$name}) {
            push @{$fdat->{IDS}{SYM}}, $name;
            # $notes{"Inferred symbol"}{$name}++;
        }
    }
    # warn BMS::FriendlySAX::node_to_text($node);
}

sub _store_ids {
    my ($fdat, $ns, $ids) = @_;
    if (my $re = $regexp->{$ns}) {
        foreach my $id (@{$ids}) {
            if ($id =~ /^$re$/i) {
                push @{$fdat->{IDS}{$ns}}, $id;
            } elsif ($ns eq 'LL') {
                # Lots of "Entrez" IDs are actually symbols
                push @{$fdat->{IDS}{SYM}}, $id;
            } else {
                push @{$fdat->{Error}}, "Malformed $ns $id";
            }
        }
    } else {
        push @{$fdat->{IDS}{$ns}}, @{$ids}; 
    }
}

sub _clean_text {
    my $txt = shift;
    $txt = "" unless (defined $txt);
    $txt =~ s/[\t\n\r\s]+/ /g;
    $txt =~ s/^\s+//;
    $txt =~ s/\s+$//;
    $txt =~ s/\P{IsASCII}//g;
    return $txt;
}

sub _clean_list {
    my $txt = &_clean_text( @_ );
    my @rv;
    foreach my $bit (split(/\s*[\/\,]\s*/, $txt)) {
        push @rv, $bit if ($bit);
    }
    return $#rv == -1 ? undef : \@rv;
}

sub cached_data {
    my $gas = BMS::MapTracker::GenAccService->new( @gasCom, @_ );
    $gas->use_beta( $isBeta );
    my $rows = $gas->cached_array( $clobber );
    # my %foo = (@gasCom, @_); die $args->branch(\%foo)."less ".$gas->val('output').".param\n\n";;
    return $rows;
}
