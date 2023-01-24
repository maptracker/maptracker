#!/stf/biobin/perl -w

BEGIN {
    # use lib '/perl/lib/site_perl/5.005';
    # Needed to make my libraries available to Perl64:
    use lib '/stf/biocgi/tilfordc/released';
    # Allows usage of beta modules to be tested:
    my $prog = $0; my $dir = `pwd`;
    if ($prog =~ /working/ || $dir =~ /working/) {
	require lib;
	import lib '/stf/biocgi/tilfordc/perllib';
    }
    $| = 1;
    print '';
}

=head1


 -nowget : Do NOT try to update Affy data files. An update should be
           allowed at least once when loading after a hiatus

=head2 Load all probe sequences

  alias lac maptracker/MapTracker/loadAffyCSV.pl

  lac -nowget -all -type probe_fasta -testmode 0

=head2 Load a single array design

  lac -nowget

=head2 Load a single file

  lac -nowget -file \
      /home/tilfordc/affymetrix/wget/arrays/Human_PrimeView/Human_PrimeView.probe_fasta.2011-08-25.gz

=head2 Load one or more arrays from its directory

  lac -nowget -all -array Human_PrimeView,HG-U133_Plus_2 -testmode 0

=head3 Annotation files






 Main page: http://www.affymetrix.com/support/technical/annotationfilesmain.affx?hightlight=true&rootCategoryId=

 URL Format: http://www.affymetrix.com/Auth/analysis/downloads/na31/ivt/HG-U133A.na31.annot.csv.zip


=head3 Probe CSV

 Current chips : http://www.affymetrix.com/estore/browse/level_three_category_and_children.jsp?parent=35871&expand=true&category=35585&fromAccordionMenu=true&subCategory=35585

 Need to go to a product, click on Technical Documentation tab, then scroll down.

 URL Format: http://www.affymetrix.com/Auth/analysis/downloads/data/HG-U133A.probe_tab.zip

=head3 Target fasta

 As above, but:

 URL Format: http://www.affymetrix.com/Auth/analysis/downloads/data/HG-U133A.target.zip


=head2

 Developers Network : http://www.affymetrix.com/estore/partners_programs/programs/developer/index.affx?category=34018&categoryIdClicked=34018&rootCategoryId=34002&navMode=34018&parent=34018&aId=partnersNav#1_2

=cut

my $VERSION = 
    ' $Id$ ';

use strict;
use BMS::ArgumentParser;
use BMS::TableReader;
use BMS::MapTracker::AccessDenorm;
use BMS::MapTracker::LoadHelper;
use BMS::Utilities::ReportingUtilities;
use BMS::Utilities::SequenceUtilities;
use BMS::ExcelHelper;
use Bio::SeqIO;
use Date::Manip;

my $args = BMS::ArgumentParser->new
    ( -nocgi    => $ENV{HTTP_HOST} ? 0 : 1,
      -cache    => 10000,
      -testmode => 1,
      -loaddir  => 'AffyCSV',
      # -dir      => "/home/tilfordc/affymetrix/annotations",
      -verbose  => 1,
      -file     => "", );

$args->shell_coloring();

my $base    = "/home/tilfordc/affymetrix/wget";
my $hBase   = "/home/tilfordc/public_html/people/neuhausi/AffyParse";
my $limit   = $args->val('limit');
my $fileReq = $args->val(qw(files csv file input));
my $dirReq  = $args->val(qw(dir directory dirs directories));
my $tm      = $args->val(qw(tm testmode));
my $vb      = $args->val(qw(vb verbose));
my $db      = $args->val('debug') || 0;
my $cache   = $args->val('cache');
my $skipRE  = $args->val('skip');
my $keepRE  = $args->val('keep');
my $backFill = $args->val(qw(backfill));
my $mode    = lc($args->val(qw(mode)) || "");
my $clobber = $args->val(qw(clobber));
my $killOld = $args->val(qw(killold));
my $showRow = $args->val(qw(showrow showrows));
my $prbClsOnly = $args->val(qw(probeclassonly)) || 0;

my $ftns    = '#FreeText#';
my ($observations, %attr, %seenSet, %globals, %fileRecovery, %multProbes);

my $lhCounter = 0;

my $gridFile = "$base/ArrayGrid.tsv";
my $gridExc  = "$base/ArrayGrid.xls";
my $thickBar = ("=" x 70) . "\n";
my $typeTok  = {
    'Annot CSV'          => 'annot',
    'Probeset Annot CSV' => 'annot',
    'Probe FASTA'        => 'probe_fasta',
    'Probe Tabular'      => 'probe_tab',
};

my $ad = BMS::MapTracker::AccessDenorm->new
    ( -age => $args->val('age'),
      -ageall => $args->val('ageall') );
my $mt  = $ad->tracker();
$mt->user('tilfordc');
my $ru  = BMS::Utilities::ReportingUtilities->new();
my $su  = BMS::Utilities::SequenceUtilities->new();
my $lh  = BMS::MapTracker::LoadHelper->new
        ( -user     => 'Affymetrix',
          -tracker  => $mt,
          -basedir  => $args->val(qw(basedir loaddir)),
          -testfile => $args->val('testfile'),
          -testmode => $tm );
my $oldSDB = $mt->get_searchdb
    ( -name => "Affymetrix SNP Mappings" );

my @rClass = qw(RSR ENST AR);
my %mtClass = map { $_ => $ad->primary_maptracker_class( $_ ) } @rClass;

my @descTags = qw(definition def prod ug_title);

my $standardColumns = {
    "Allele A"                        => "AlleleA",
    "Allele B"                        => "AlleleB",
    "Allele Frequencies"              => "Frequency",
    "Chromosome"                      => "Chr",
    "Chromosome Start"                => "ChrStart",
    "Chromosome Stop"                 => "ChrEnd",
    "Copy Number Variation"           => "CNV",
    "Flank"                           => "Flank",
    "GeneChip Array"                  => "Array",
    "Heterozygous Allele Frequencies" => "Het",
    "Physical Position"               => "Pos",
    "Probe Set ID"                    => "ProbeSet",
    "Probe ID"                        => "ProbeSet",
    "probeset_id"                     => "ProbeSet",
    "RefSeq Transcript ID"            => "RefSeq",
    "Representative Public ID"        => "MainRNA",
    "Sequence Type"                   => "Type",
    "Species Scientific Name"         => "Taxa",
    "Strand"                          => "Strand",
    "target strandedness"             => "Sense",
    "Target Description"              => "Description",
    "Transcript Assignments"          => "AllRNA",
    "mrna_assignment"                 => "AllRNA",
    "dbSNP RS ID"                     => "dbSNP",
    'Associated Gene'                 => "Impact",
    "PROBE_INTERROGATION_POSITION"    => "ProbePos",
    "probe interrogation position"    => "ProbePos",
    "probe_count"                     => "ProbeCount",
    "PROBE_X_POS"                     => "ChipX",
    "PROBE_Y_POS"                     => "ChipY",
    "probe x"                         => "ChipX",
    "probe y"                         => "ChipY",
    "PROBE_SEQUENCE"                  => "ProbeSeq",
    "probe sequence"                  => "ProbeSeq",
    "PROBESET_ID"                     => "ProbeSet",
    "ALLELE"                          => "Allele",
    "TARGET_STRANDEDNESS"             => "Sense",
#    "Annotation Description"          => "AnnotDesc",
};

my $bldMap = {
   crigri1    => 'Cricetulus griseus',
   E_COLI_2   => 'Escherichia coli',
   ECOLI_ASV2 => 'Escherichia coli',
   ECOLI      => 'Escherichia coli',

};
my %expectedColumns = map { $_ => 1 } values %{$standardColumns};

my $typeCode = {
    '3UTR'       => 'UTR Variant',
    '5UTR'       => 'UTR Variant',
    'UTR-3'      => 'UTR Variant',
    'UTR-5'      => 'UTR Variant',
    'exon'       => 'Exonic Variant',
    'CDS'        => 'Coding Variant',
    'cds'        => 'Coding Variant',
    'downstream' => 'Genomic Variant',
    'intron'     => 'Intronic Variant',
    'upstream'   => 'Genomic Variant',
    'splice-site' => 'Splice Site Variant',
};

my $noHeader = {
    'Mapping250K_Nsp.probe_tab.2008-08-20.gz' =>  [qw(ProbeSet ChipX ChipY SnpPos ProbeSeq Strand MatchType Allele)],
    'GenomeWideSNP_5.probe_tab.2007-03-16.gz' => [qw(ProbeSet ChipX ChipY SnpPos ProbeSeq Strand MatchType Allele)],
    'Mapping250K_Sty.probe_tab.2008-08-20.gz' => [qw(ProbeSet ChipX ChipY SnpPos ProbeSeq Strand MatchType Allele)],
};

my $designs = {
    "Human Genome U133 Plus 2.0 Perfect Match Peg Array" => "HT_HG_U133_PLUS_PM",
};

my $noUpdate = $args->val(qw(nowget noupdate nomirror));
&remote_update() unless ($noUpdate);

my $allFiles = &available_files();
my $chipData = &grid();

if ($args->val(qw(listfile listfiles list))) {
    &list_all_files();
}

my @reqs;
my $lMod    = $limit ? "-LIMIT" : "";
my $obsFile = "AffyLoadObservations$lMod.txt";
my $mpFile  = "AffyDuplicateProbes$lMod.xlsx";
my $xyFile;

open(OBSV, ">$obsFile") || $args->death
    ("Failed to write observations file", $obsFile, $!);

my $dumpFile = $args->val(qw(dump));
if ($dumpFile && $#reqs != -1) {
    open(DUMP, ">$dumpFile") || $args->death("failed to dump data", $dumpFile, $!);
} else {
    $dumpFile = undef;
}

my $chipOnly = $args->val(qw(arrayonly chiponly));

if ($fileReq) {
    @reqs = ref($fileReq) ? @{$fileReq} : split(/[\s\,]+/, $fileReq);
} elsif ($dirReq) {
    $dirReq =~ s/\/+$//;
    my @dr = ref($dirReq) ? @{$dirReq} : split(/[\s\,]+/, $dirReq);
    @reqs = &get_all_files(@dr);
} elsif ($args->val(qw(all))) {
    &load_all_files();
} else {
    $args->death
        ("You need to provide at least one Affy CSV annotation file eg:",
         "  -file HT_HG-U133_Plus_PM.na27.1.annot.csv",
         "Or a path to a directory of files:",
         "  -dir ",
         "Or pass -all to analyze all files in the default location ($base)");
}

foreach my $file (@reqs) {
    if ($args->val(qw(fasta dofasta makefasta))) {
        &snp_fasta_file($file);
    } elsif ($chipOnly) {
        &basic_array($file);
    } else {
        &parse_csv($file);
    }
}
$lh->write();


if ($backFill) {
    $backFill = "UnrecognizedRNA.tsv" if ($backFill eq '1');
    open(BKFL, ">$backFill") || $args->death
        ("Failed to write backfill file", $backFill, $!);
    my @list = sort keys %{$globals{unknown}};
    foreach my $rnaU (@list) {
        print BKFL join("\t", $rnaU, join
                        (',', sort keys %{$globals{unknown}{$rnaU}}))."\n";
    }
    close BKFL;
    my $bl = $backFill;
    $bl =~ s/\.tsv$//;
    $bl .= ".list";
    open(BKFL, ">$bl") || $args->death
        ("Failed to write backfill list", $bl, $!);
    map { print BKFL "$_\n" } @list;
    close BKFL;
                
    $args->msg(scalar(@list)." unrecognized RNAs extracted", $backFill, $bl);

}
close OBSV;
close DUMP if ($dumpFile);

&record_xy();

my %files = ( "Parse observations" => $obsFile,
              "Multiple Probe Entries" => $mpFile,
              "Probe coordinates"  => $xyFile,
              "Dump file" => $dumpFile );
my @fMsg;
foreach my $ft (sort keys %files) {
    my $file = $files{$ft};
    next unless ($file && -s $file);
    push @fMsg, ("$ft: $file");
}
$args->msg("[>]",@fMsg) unless ($#fMsg == -1);
$args->msg("[FINISHED]", $ru->getdate("0HR:0MIN:0SEC MON DAY"));

sub grid {
    &make_grid($gridFile) if ($args->val(qw(clobber)) || !-s $gridFile);
    open(GF, "<$gridFile") || $args->death
        ("Failed to read grid", $gridFile, $!);
    my $head = <GF>;
    $head =~ s/[\n\r]+$//;
    $head = [ split(/\t/, $head) ];
    my %rv;
    while (<GF>) {
        s/[\n\r]+$//;
        my @row = split(/\t/);
        my %data;
        for my $h (0..$#{$head}) {
            $data{$head->[$h]} = $row[$h];
        }
        $rv{$data{chip_type}} = \%data;
    }
    return \%rv;
}

sub make_grid {
    my $file = shift;
    my %rows;
    my @aads = sort keys %{$allFiles};
    my %cols = (chip_type               => 1,
                genome_species          => 2, 
                isSnp                   => 3,
                netaffx_annotation_date => 4,
                );
    foreach my $aad (@aads) {
        my $list = $allFiles->{$aad}{annot};
        unless ($list) {
            $args->msg("[!!]","No annot file for $aad");
            next;
        }
        my ($best) = sort { $b->{date} cmp $a->{date} } @{$list};
        my $path = $best->{path};
        &tr_and_attr( $path );
        $rows{$aad} = { %attr };
        map { $cols{$_} ||= 0 } keys %attr;
    }
    map { delete $cols{$_} } qw(colMap colHeader SDB);
    my $cn = 100;
    map { $cols{$_} ||= ++$cn } keys %cols;
    my @head = sort { $cols{$a} <=> $cols{$b} } keys %cols;
    my $eh = BMS::ExcelHelper->new( $gridExc );
    my $sname = "Array Report";
    $eh->sheet( -name    => $sname,
                -cols    => \@head,
                -width   => [ map { 10 } @head ],
                -freeze  => [1,1] );

    open(GF, ">$file") || $args->death
        ("Failed to make grid", $file, $!);
    print GF join("\t", @head)."\n";
    foreach my $aad (@aads) {
        my @row = map {defined $_ ? $_ : "" } map { $rows{$aad}{$_} } @head;
        print GF join("\t", @row)."\n";
        $eh->add_row($sname, \@row);
    }
    close GF;
    $eh->close;
    $args->msg("[>]","Array grid created", $file, $gridExc);
}


sub list_all_files {
    $args->msg("Available files from $base");
    foreach my $aad (sort keys %{$allFiles}) {
        my @types = sort keys %{$allFiles->{$aad}};
        for my $t (0..$#types) {
            my $type = $types[$t];
            my $dl   = $allFiles->{$aad}{$type};
            for my $d (0..$#{$dl}) {
                my $dat = $dl->[$d];
                my $path = $dat->{path};
                $path =~ s/\Q$base\E\/?//;
                printf("%20s %12s %10s %s\n", ($t || $d) ? "" : $aad, $d ? "" : $type,
                       $dat->{date}, $path);
            }
        }
    }
}

sub parse_csv_snp {
    my ($tr) = @_;
    $args->msg("[+]","Parsing $attr{filePath} as SNP data");
    if (my $head = $attr{colHeader}) {
        $tr->set_header($head);
        while (my ($in, $out) = each %{$standardColumns}) {
            $tr->remap_header_name( $in, $out );
        }

        # die $args->branch([$tr->header()]);
    }
    map { $tr->ignore_column_name( $_ ) } ('ChrX pseudo-autosomal region 1','ChrX pseudo-autosomal region 2','Fragment Enzyme Type Length Start Stop','Microsatellite','OMIM','In Final List','Cytoband','% GC');
    my $fmt   = $attr{chrFormat};
    my $sdb   = $attr{SDB};
    my $aad   = $attr{chip_type};
    my @errs;
    push @errs, "Failed to determine chromosome accession format"
        unless ($fmt);
    push @errs, "No SearchDB defined" unless ($sdb);
    push @errs, "Could not determine chip type" unless ($aad);
    unless ($#errs == -1) {
        $args->msg("[!!]", "Could not parse SNP chip", @errs);
        return;
    }
    # die $args->branch(\%attr);
    my @cols = qw(ProbeSet Chr ChrStart ChrEnd Strand);
    while (my $row = $tr->next_clean_hash()) {
        my ($pset, $chr, $s, $e, $str) = map { $row->{$_} } @cols;
        if ($chr =~ /^(\d+|X|Y|MT)$/i) {
            my $cname = sprintf($fmt, uc($chr));
            my $len = $e - $s + 1;
            $lh->add_mapping
                ( -name1 => $pset, 
                  -name2 => $cname,
                  -strand => $str eq '-' ? -1 : 1,
                  -score  => 100, 
                  -data => [ [ 1, $len, $s, $e  ] ], 
                  -onfail => 'return',
                  -searchdb => $sdb, );
        } elsif ($chr eq '---') {
            $lh->set_class($pset, "Untrustworthy");
        } else {
            $args->msg_once("Non-chromosomal location '$chr' for $pset");
        }
        $lh->set_edge( -name1 => $pset,
                       -name2 => $aad,
                       -type  => 'is a member of');
        &note_impact( $row, $pset );
        # warn $args->branch($row);
        $lh->write_threshold_quick(500);
    }
    $lh->write();
}

sub file_path {
    my ($file) = @_;
    return undef unless ($file);
    unless (-e $file) {
        my @found;
        unless ($file =~ /\//) {
            @found = split(/[\n\r]+/, `find $base -type f -name "$file*"`);
        }
        if ($#found == 0) {
            $file = $found[0];
        } else {
            unshift @found, "Maybe it was one of these?" if ($#found != -1);
            $args->err("Requested file not found", $file, @found);
            return undef;
        }
    }
    return $file;
}

sub basic_array {
    my $file = &file_path(@_);
    return unless ($file);

    my $tr = &tr_and_attr( $file );
    return unless ($tr);
    &set_chip_info();
    $args->msg("[BASIC]", $attr{chip_type});
}

sub parse_csv {
    my $file = &file_path(@_);
    return unless ($file);

    if ($file =~ /probe_fasta/) {
        return &parse_probe_file($file);
    }

    my $tr = &tr_and_attr( $file );
    return unless ($tr);

    print OBSV "\n". ("=" x 50)."\n";
    print OBSV "$attr{chip_type}\n";
    print OBSV "   $attr{filePath}\n";

    &set_chip_info();
    if ($attr{isSnp}) {
        &parse_csv_snp($tr);
    } else {
        &parse_csv_annot($tr);
    }


    &note_observations();
}

sub set_chip_info {
    my $aad = $attr{chip_type};
    return unless ($aad);
    $lh->set_class($aad, 'CHIPSET');
    $lh->set_edge( -name1 => $aad,
                   -name2 => 'Affymetrix Designs',
                   -type  => 'is a member of' );
    if (my $tax = $attr{'genome_species'}) {
        $lh->kill_species($aad);
        $lh->set_species($aad, $tax);
    } else {
        &obsv_error("Failed to find species for array", $aad);
    }
}

sub obsv_error {
    my $txt = $args->msg("[!!]", @_);
    print OBSV $txt;
}

sub stnd_id {
    my $id = shift;
    if ($id =~ /^\d+$/) {
        if (my $psPrfx = $attr{ps_prefix}) {
            $id = $psPrfx . $id;
        } else {
            $args->death("Integer Probeset ID, no Prefix found!", $id);
        }
    }
    return $id;
}

sub parse_csv_annot {
    my $tr       = shift;
    my $num      = 0;
    my $colNum   = $attr{colMap};
    my $psCol    = $colNum->{ProbeSet};
    my $procFunc = $backFill ? \&backfill_rna : \&process_data;
    while (my $row = $tr->next_clean_row()) {
        my $id = $row->[ $psCol ] = &stnd_id( $row->[ $psCol ] );
        warn &show_row($row) if ($showRow);
        $args->msg("[".sprintf("%6.3f M", $num/1000000)."]",  $ru->
                   getdate("0HR:0MIN:0SEC MON DAY") ." - $id")
            unless (++$num % 10000);
        if ($dumpFile) {
            print DUMP "$id\n" if ($id);
            next;
        }
        my %data;
        my @bits;
        while (my ($sn, $c) = each %{$colNum}) {
            my $val = $row->[$c];
            $val = "" unless (defined $val);
            $data{$sn} = $val;
            push @bits, sprintf("%15s : %s", $sn, substr($val, 0, 80));
        }
        &{$procFunc}(\%data, \@bits);
        if ($db > 1) {
            $args->msg("---", @bits);
            $lh->write();
        } else {
            $lh->write_threshold_quick( $cache );
        }
    }
    $lh->write();
}

sub note_observations {
    my @omsg;
    foreach my $obs (sort keys %{$observations}) {
        push @omsg, "$obs : ".$observations->{$obs};
    }
    return if ($#omsg == -1);

    $args->msg("Observed deviations:", @omsg);
    print OBSV join("\n", @omsg)."\n\n";
}

sub backfill_rna {
    my ($data, $bits) = @_;
    my $pset = $data->{ProbeSet};
    unless ($pset) {
        $observations->{"ERR - NO PROBESET FOUND"}++;
        return;
    }
    foreach my $key (qw(MainRNA AllRNA)) {
        foreach my $rdat (@{&array_2d( $data->{$key} )} ) {
            my ($rnaU, $rdesc, $src, $prbNum, $unk1) = @{$rdat};
            my $rnaV;
            if ($rnaU =~ /(.+)\.\d+$/) {
                $rnaV = $rnaU;
                $rnaU = $1;
            } elsif ($rnaU =~ /\./) {
                # $args->err("Potential versioned RNA for $pset : '$rnaU'");
                $observations->{"Potenitally versioned RNAs"}++;
            }
            if ($rnaU =~ /^\d+$/) {
                $observations->{"Integer RNA ID ignored"}++;
                next;
            }

            my $ns;
            if ($rnaU =~ /^g(\d+)$/) {
                #$observations->{"GI RNA ignored"}++;
                #next;
                $rnaU = "gi$1";
            }
            my $obj = $mt->get_seq("#None#$rnaU");
            unless ($obj && $obj->is_class('RNA')) {
                $globals{unknown}{$rnaU}{$pset} = 1;
            }
        }
    }
}

sub process_data {
    my ($data, $bits) = @_;
    my $pset = $data->{ProbeSet};
    unless ($pset) {
        $observations->{"ERR - NO PROBESET FOUND"}++;
        return;
    }
    if (my $aad = $attr{chip_type}) {
        $lh->set_edge( -name1 => $pset,
                       -name2 => $aad,
                       -type  => 'is a member of');
    } else {
        $observations->{"Unable to assign array design"}++;
    }

    $lh->set_class($pset, 'AFFYSET');
    my $tax = $data->{Taxa} || $attr{'genome_species'};
    if ($tax && $tax ne '---') {
        $lh->kill_species($pset);
        $lh->set_species($pset, $tax);
    } else {
        $observations->{"No taxa found for probeset"}++;
    }
    
    my %tags;
    my $desc = $data->{Description} || "";
    $desc = "" if ($desc eq '---');
    while ($desc =~ /(\/(\S+?)=([^\/]*?)\s*)$/) {
        my ($rep, $tag, $val) = ($1, lc($2), $3);
        $desc =~ s/\Q$rep\E//;
        next if ($val eq '');
        push @{$tags{$tag}}, $val;
        push @{$bits}, sprintf("%15s : %s", $tag, substr($val, 0, 80));
    }
    $desc =~ s/\s+/ /;
    $desc =~ s/ $//; $desc =~ s/^ //;
    # Not interested in descriptions that are just GI numbers
    $desc = "" if ($desc =~ /^g\d+$/);
    # $tags{residual} = $desc; warn $args->branch(\%tags);

    if (my $flank = $data->{Flank}) {
        if (my $fmt = $attr{chrFormat}) {
            my ($pos, $chr) = ($data->{Pos}, $data->{Chr});
            if ($pos && $chr && $chr ne '---' && $pos ne '---') {
                my $cname = sprintf($fmt, $chr);
                my $sdb   = $attr{SDB};
                $lh->kill_mapping_by_sdb( $pset, $sdb );
                $lh->kill_mapping_by_sdb( $pset, $oldSDB ) if ($killOld);
                $lh->add_mapping
                    ( -name1 => $pset, 
                      -name2 => $cname,
                      -strand => $data->{Strand} eq '-' ? -1 : 1,
                      -score  => 100, 
                      -data => [ [ 1, 1, $pos, $pos  ] ], 
                      -onfail => 'return',
                      -searchdb => $sdb, );
            }
        }
        if ($pset =~ /^(AFFX-)?SNP/) {
            $lh->set_class($pset, 'SNP');
            if (my $rs = $data->{dbSNP}) {
                if ($rs =~ /^rs\d+$/) {
                    $lh->set_edge( -name1 => $pset,
                                   -name2 => $rs,
                                   -type  => 'is a reliable alias for' );
                } else {
                    $observations->{"Malformed dbSNP"}++;
                    $args->msg('[!]',"Malformed dbSNP for $pset - $rs");
                }
            }
            my %tagAssay;
            my @aOrder = ($data->{AlleleA} || "", $data->{AlleleB} || "");
            foreach my $fdat (@{&array_2d( $data->{Frequency} )} ) {
                my ($af, $bf, $popShort) = @{$fdat};
                next unless (defined $af && $af ne '' &&
                             defined $bf && $bf ne '' );
                unless ($popShort) {
                    $args->msg('[!]',"Missing incidence $pset - [$af/$bf]");
                    next;
                }
                my $pop = "$popShort AffyHapMap";
                push @{$tagAssay{$pop}{Incidence}{$aOrder[0]}}, $af;
                push @{$tagAssay{$pop}{Incidence}{$aOrder[1]}}, $bf;
            }
            foreach my $hdat (@{&array_2d( $data->{Het} )} ) {
                my ($het, $popShort) = @{$hdat};
                next unless (defined $het && $het ne '');
                unless ($popShort) {
                    $args->msg('[!]',"Missing heterozygosity $pset - [$het]");
                    next;
                }

                my $pop = "$popShort AffyHapMap";
                push @{$tagAssay{$pop}{Incidence}{Heterozygosity}}, $het;
            }
            while (my ($pop, $tagH) = each %tagAssay) {
                my @tags;
                while (my ($tn, $tvh) = each %{$tagH}) {
                    while (my ($tv, $nums) = each %{$tvh}) {
                        if ($#{$nums} == 0) {
                            push @tags, ["#PGx#$tn", "#PGx#$tv", $nums->[0]];
                        } else {
                            $args->msg('[!]',"Multiple Tag Values for $pset",
                                       "$tn | $tv | ".join(',', @{$nums})); 
                        }
                    }
                }
                $lh->set_edge( -name1 => $pset,
                               -name2 => $pop,
                               -tags  => \@tags,
                               -type  => 'was assayed against' );
            }
            &note_impact( $data, $pset );

        } else {
            $args->msg('[!]',"Flank set for unrecognized ID format $pset"); 
        }
        if (0) {
            if ($flank =~ /^([a-z]*)\[([actg\/]+)\]([a-z]*)/i) {
                my ($lft, $atxt, $rgt) = (lc($1 || ""), uc($2), lc($3 || ""));
                my @als   = split(/\//, $atxt);
                my $ambig = $su->ambiguous_code(\@als);
                foreach my $allele (@als) {
                    my $probe = "#Sequence#".$lft.$allele.$rgt;
                    $lh->set_class($probe, 'Affy Probe');
                    $lh->set_taxa( $probe, $tax ) if ($tax);
                    $lh->set_edge( -name1 => $pset, 
                                   -name2 => $probe,
                                   -type  => 'has member');
                }
            } else {
                $args->msg('[!]',"Failed to parse flank for $pset - $flank");
            }
        }
    }
    my (%rnas, %rnaNS, $probeCounts);
    # die $args->branch($data);
    foreach my $key (qw(RefSeq MainRNA AllRNA)) {
        my $capture = $key eq 'RefSeq' || $key eq 'MainRNA' ? 1 : 0;
        foreach my $rdat (@{&array_2d( $data->{$key} )} ) {
            my ($rnaU, $rdesc, $src, $prbNum, $unk1) = @{$rdat};
            my $prbTot;
            next unless ($rnaU);
            my $rnaV;
            if ($rnaU =~ /(.+)\.\d+$/) {
                $rnaV = $rnaU;
                $rnaU = $1;
            } elsif ($rnaU =~ /\./) {
                # $args->err("Potential versioned RNA for $pset : '$rnaU'");
                $observations->{"Potenitally versioned RNAs"}++;
            }
            if ($rnaU =~ /^\d+$/) {
                $observations->{"Integer RNA ID ignored"}++;
                next;
            }

            my $ns;
            if ($rnaU =~ /^g(\d+)$/) {
                #$observations->{"GI RNA ignored"}++;
                #next;
                $rnaU = "gi$1";
                $ns  = 'GI';
                $capture = 0;
            } else {
                ($ns) = $ad->guess_namespace($rnaU, 'AR');
            }
            my $arr = $rnas{$rnaU} ||= [ $rnaU, $ns, 0, undef, "", $rnaV ];
            $arr->[2] ||= $capture;
            if ($rdesc && $rdesc =~ /^chr.{1,3}$/) {
                # HuGene
                # RnaU chr PercMatch #Match #Tot ???
                $prbNum = $rdat->[3];
                if ($prbTot = $rdat->[4]) {
                    $probeCounts ||= {};
                    $probeCounts->{$prbTot}++;
                    if (!$arr->[6]) {
                        $arr->[6] = $prbTot;
                    } elsif ($arr->[6] != $prbTot) {
                        # Inconcistent counts within a single RNA!!
                        $arr->[6] = $prbTot if ($arr->[6] < $prbTot);
                        # args->msg_once("[?]","$pset : Inconsistent total probe count: $arr->[6] != $prbTot");
                        $observations->{"Inconsistent probe count for a distinct target"}++;
                    }
                }
                $arr->[2] ||= 1 if ($ns eq 'RSR');
            }
            if ($prbNum) {
                if ($prbNum =~ /^\d+$/) {
                    if (!defined $arr->[3] || $prbNum > $arr->[3]) {
                        $arr->[3] = $prbNum;
                        $arr->[5] = $rnaV;
                    }
                } else {
                    $observations->{"ERR - Non-numeric probe count"}++;
                }
            }
            $arr->[4] ||= $unk1;
            $rnaNS{$ns} = 1;
        }
    }
    # die $args->branch(\%rnas);
    my $fallback = "";
    unless ($rnaNS{RSR}) {
        # No RefSeq mentioned
        if (my $ens = $rnaNS{ENST}) {
            # Ensembl is available, use that
            $fallback = 'ENST';
        } elsif (my $gi = $rnaNS{GI}) {
            # Ick, we have a GI number
            $fallback = 'GI';
        } else {
            # Oops, we only have generic IDs
            $fallback = 'AR';
        }
    }

    # die $args->branch(\%rnas);
    my @allRNAs = values %rnas;
    my ($guessTot, @sharedTags);
    if ($guessTot = $data->{ProbeCount}) {
        $probeCounts = { $guessTot => 1 };
        map { $_->[6] = $guessTot } @allRNAs;
    } elsif ($probeCounts) {
        # Some of the newer files include total probe counts
        my @tot = sort { $b <=> $a } keys %{$probeCounts};
        $guessTot = $tot[0];
        if ($#tot > 0) {
            $observations->{"Inconsistent probe count for a probeset"}++;
            push @sharedTags, ["Suspicious", "${ftns}Affymetrix reports $tot[-1]-$guessTot probes for the probe set", undef];
            $lh->set_class( $pset, 'Suspicious', 'tilfordc');
        }
    }
    unless ($guessTot) {
        # The file does not explicitly list the number of probes in the set
        # We will take the maximum matches observed as the total probe count
        # This will probably be an undercount in a few cases, but it should
        # be generally correct and will be validated by NOSHA anyway.
        ($guessTot) = sort { $b <=> $a } map { $_->[3] || 0 } @allRNAs;
    }
    my $targs = 0;
    foreach my $dat (@allRNAs) {
        my ($rnaU, $ns, $cap, $num, $unk1, $rnaV, $prbTot) = @{$dat};
        next unless ($cap || ($ns eq $fallback));
        $prbTot ||= $guessTot;
        my $edgeTags = @sharedTags ? [@sharedTags] : undef;
        if (defined $num) {
            my $frac = int(0.5 + 100 * ($prbTot ? $num / $prbTot : 0))/100;
            $edgeTags ||= [];
            if ($frac > 1) {
                $frac = 1;
                $observations->{"Fraction probes matching greater than 1"}++;
                push @{$edgeTags}, 
                ["Suspicious", "${ftns}Affymetrix reports match over 100%", undef];
                $lh->set_class( $pset, 'Suspicious', 'tilfordc');
            }
            push @{$edgeTags}, ["Fraction of probes matched",$rnaV,$frac];
        }
        $lh->set_edge( -name1 => $pset,
                       -name2 => $rnaU,
                       -tags  => $edgeTags,
                       -type  => "is a probe for" );
        $targs++;

        #$lh->kill_edge( -name1 => $pset,
        #                -name2 => "$rnaU.%",
        #                -type  => "is a probe for" );
    }
    $observations->{"No targets for probe set"}++ unless ($targs);
    my $pdesc;
    for my $d (0..$#descTags) {
        if (my $def = $tags{$descTags[$d]}) {
            ($def) = sort { length($a) <=> length($b) } @{$def};
            unless ($def =~ /^g\d+$/) {
                $pdesc = $def;
                last;
            }
        }
    }
    $pdesc = $desc if (!$pdesc && !$rnas{$desc});
    if ($pdesc) {
        $lh->set_edge( -name1 => $pset,
                       -name2 => "$ftns$pdesc",
                       -type  => "is a shorter term for" );
    } elsif ($pset !~ /^AFFX/) {
        $observations->{"No description for probe set"}++;
    }
}

sub note_impact {
    my ($data, $pset) = @_;
    my %types;
    my $impacts = &array_2d( $data->{Impact} );
    foreach my $gdat (@{$impacts}) {
        my ($rna, $rtype, $dist) = @{$gdat};
        next unless ($rna);
        my $type = $typeCode->{$rtype || ''} || "";
        unless ($type) { 
            $args->msg('[!]',"Unknown impact '$rtype' on $pset",
                       join(" + ", @{$gdat}));
            next;
        }
        $dist *= -1 if ($rtype eq 'upstream');
        $lh->set_edge
            ( -name1 => $pset,
              -name2 => $rna,
              -tags  => [ ['Impact', "#Classes#$type", $dist ]],
              -type  => 'has an impact on',);
        $types{$type}++;
    }
    map { $lh->set_class($pset, $_) } keys %types;
}

sub parse_probe_file {
    my $file = shift;
    my @errs = &init_probe_attr($file);
    unless ($#errs == -1) {
        &obsv_error("Can not parse probe file", @errs);
        return;
    }

    &set_chip_info();
    print OBSV "\n". ("=" x 50)."\n";
    print OBSV "$attr{chip_type}\n";
    print OBSV "   $attr{filePath}\n";

    if ($attr{isSnp}) {
        return &parse_snp_probe_file( $file );
    } else {
        return &parse_profiling_probe_file( $file );
    }
}

sub parse_profiling_probe_file {
    my ($file) = shift;
    my $prCache = $cache * 10;
    return unless ($file);
    my @preErrs;
    push @preErrs, "File not found : $file" unless (-e $file);

    unless ($#preErrs == -1) {
        $args->err("Can not parse probe file", @preErrs);
        return;
    }
    # die $args->branch(\%attr);
    %seenSet = ();
    my ($tr, $nextSeq);
    my @colN  = qw(ProbeSeq ChipX ChipY);
    if ($file =~ /probe_tab/) {
        # TSV file
        ($tr) = &init_probe_TableReader
            ($file, [@colN, 'ProbeSet','Sense']);
        $nextSeq = sub {
            return $tr->next_clean_hash();  
        };
    } else {
        # Fasta file
        my $fh;
        open($fh, "gunzip -c \"$file\" |") || $args->death
            ("Failed to open fasta file", $file, $!);
        
        my $stream = Bio::SeqIO->new( -format => 'fasta', -fh => $fh, );
        my $baseSmatch = $attr{chip_type} eq 'ECOLI' ? 'Sense' : 'Antisense';
        $nextSeq = sub {
            
            my $bs = $stream->next_seq();
            return $bs unless ($bs);
            # die $args->branch($bs);
            my $id = $bs->display_id();
            my $desc   = $bs->desc() || "";
            map { s/\;$// } ($id, $desc);

            my $sMatch = $baseSmatch;
            my %data   = ( ProbeSeq => $bs->seq(), DisplayId => $id );

            if ($id =~ /^probe:([^:]+):([^:]+):(\d+):(\d+)$/) {
                $data{ProbeSet} = $2;
                $data{ChipX}    = $3;
                $data{ChipY}    = $4;
            } elsif ($id =~ /^probe:([^:]+):\d+\-(\d+);(\d+):(\d+)$/) {
                # Newer gene format
                # probe:CanGene-1_0-st-v1:1353150-14255000;119:1137
                $data{ProbeSet} = $2;
                $data{ChipX}    = $3;
                $data{ChipY}    = $4;
                $data{PurgePrb} = $data{ProbeSeq};
                $data{ProbeSeq} = $su->revcom($data{ProbeSeq});
                $sMatch = 'Sense';
            } elsif ($id =~ /^probe:([^:]+):\d+;(\d+):(\d+)$/) {
                # This can be mixed with the above format
                # probe:CanGene-1_1-st-v1:714513;512:600
                $data{ChipX}    = $2;
                $data{ChipY}    = $3;
                $data{PurgePrb} = $data{ProbeSeq};
                $data{ProbeSeq} = $su->revcom($data{ProbeSeq});
                $sMatch = 'Sense';
            } elsif ($id =~ /^([^:]+):(\S+)$/) {
                my ($arr, $pset) = ($1, $2);
                if ($desc =~ /^(\d+)\; (\d+)\; (\d+); (.*)$/) {
                    $data{ProbeSet} = $pset;
                    $data{ChipX}    = $1;
                    $data{ChipY}    = $2;
                    $desc = $4;
                    $desc =~ s/^\s+//;
                } else {
                    $observations->{"Unrecognized display format"}++;
                    $args->death("[?]","Unrecognized ID", $id, $desc);
                    return \%data;
                }
            } else {
                $observations->{"Unrecognized display format"}++;
                $args->death("[?]","Unrecognized ID", $id);
                return \%data;
            }
            
            foreach my $bit (split(/;\s*/, $desc)) {
                next unless ($bit);
                if ($bit =~ /^(\S+)=(.+)$/) {
                    $data{$1} = $2;
                } elsif ($bit =~ /sense$/i) {
                    $data{Sense} = $bit;
                }
            }
            if (my $pset = $data{ProbeSetID}) {
                if (!$data{ProbeSet}) {
                    $data{ProbeSet} = $pset;
                } elsif ($data{ProbeSet} ne $pset) {
                    $observations->{"Uncertain probeset ID"}++;
                    $args->msg("[x]","Probeset ID mismatch: DisplayId=$data{ProbeSet} vs Desc:$pset");
                }
            }
            $data{SenseOk} = !$data{Sense} || ($data{Sense} eq $sMatch) ? 1 :0;
            return \%data;
        }
    }

    my $num  = 0;
    my $set  = { id => "", };
    while (my $data = &{$nextSeq}()) {
        last if ($limit && $num >= $limit);
        my $id = &stnd_id( $data->{ProbeSet}  );
        next unless ($id);
        my $sense = $data->{Sense};
        unless ($data->{SenseOk}) {
            my $what = $data->{Sense} || "SenseNotDefined";
            $observations->{"Skipping '$what'"}++;
            next;
        }
        #if (my $purge = $data->{PurgePrb}) {
        #    &purge_probe($purge, $id);
        #}
        my ($seq, $x, $y) = map { $data->{$_} } @colN;
        # warn "$set->{id} eq $id";
        if ($set->{id} eq $id) {
            $tr->extend_limit() if ($tr);
        } else {
            &process_oligo( $set );
            if ($set->{id}) {
                $args->msg("[".sprintf("%6.3f M", $num/1000000)."]",  $ru->
                           getdate("0HR:0MIN:0SEC MON DAY") ." - $id")
                    unless (++$num % 10000);
            }
            $set = {};
        }
        unless ($seq) {
            $args->msg("[!]","Null oligo entry for $id");
            $observations->{"Null oligo in probe file"}++;
            next;
        }
        $seq = uc($seq);
        $set->{id} ||= $id;
        push @{$set->{ol}{$seq}}, "$x.$y";
        $args->branch($set);
    }
    &process_oligo( $set );
    $lh->write();
    &note_observations();
    $args->msg("Parse complete");
}

sub purge_probe {
    my ($seq, $id) = @_;
    $seq = "#Sequence#$seq";
    # This might clear some oligos that were used on both strands:
    map { $lh->kill_class($seq, $_) } qw(AFFYPROBE AFFYSEQ);

    # $lh->set_taxa($seq, $tid);
    # $lh->set_length($seq, $slen);
    $lh->kill_edge( -name1 => $seq,
                    -name2 => $id,
                    -type  => 'is a member of' );
    
    if ($lh->operations() >= $cache) {
        $lh->write();
        $args->msg("[#]", $id) if ($vb);
    }
}

sub set_probe_class {
    # I needed to backfill these at some point...
    my $ols = shift;
    foreach my $seq (@{$ols}) {
        my $mtSeq = "#Sequence#$seq";
        map { $lh->set_class($mtSeq, $_) } qw(AFFYPROBE AFFYSEQ);
    }
    $lh->write() unless (++$lhCounter % 100);
}

sub process_oligo {
    # $args->msg_once("NOT DOING OLIGOS"); return;
    my $set = shift;
    my $id  = $set->{id};
    return unless ($id);
    my @ols = sort keys %{$set->{ol}};
    my $aad = $attr{chip_type};
    my $tid = $attr{Taxid};
    
    return &set_probe_class( \@ols ) if ($prbClsOnly);

    if (my $prior = $seenSet{$id}) {
        $args->msg("[!!]", "ID duplicated in file: $id [$prior]");
        $observations->{"Duplicated ID"}++;
    }
    $seenSet{$id}++;
    
    # MEsses up XY coordinate stuff...
    #$lh->kill_edge( -name1 => $id,
    #                -type  => 'has member');
    unless ($xyFile) {
        $xyFile  = "AffyProbeCoordinates.txt";
        open(XYF, ">$xyFile") || $args->death
            ("Failed to write XY coordinates file", $xyFile, $!);
    }
    foreach my $seq (@ols) {
        my $slen  = length($seq);
        my $mtSeq = "#Sequence#$seq";
        my $xyn   = $#{$set->{ol}{$seq}} + 1;
        foreach my $xy (@{$set->{ol}{$seq}}) {
            print XYF join("\t", $seq, $aad, $xy, $id)."\n";
        }

        my @errs;
        unless ($slen == 25) {
            push @errs, "Oligo is not 25bp long";
            $observations->{"Oligo != 25bp"}++;
        }

        my $hack = $seq; $hack =~ s/[ACTG]//g;
        if ($hack) {
            push @errs, "Unusual characters in oligo: '$hack'";
            my %uniq = map { $_ => 1 } split('', $hack);
            my $char = join('',sort keys %uniq);
            $observations->{"Weird oligo characters: $char"}++;
        }
        unless ($#errs == -1) {
            $args->msg("[!!]", "Errors parsing '$id' [$seq]", @errs);
            $lh->set_class($mtSeq, 'SUSPICIOUS');
        }
        if ($xyn == 0) {
            $observations->{"Probe coordinates lacking"}++;
        } elsif ($xyn != 1) {
            $observations->{"Duplicate probes in probeset"}++;
        }

        map { $lh->set_class($mtSeq, $_) } qw(AFFYPROBE AFFYSEQ);
        $lh->set_taxa($mtSeq, $tid);
        $lh->set_length($mtSeq, $slen);
        $lh->set_edge( -name1 => $id,
                       -name2 => $aad,
                       -type  => 'is a member of' );
    }
    $lh->set_taxa($id, $tid);
    $lh->set_class($id, 'AFFYSET');

    if ($lh->operations() >= $cache) {
        $lh->write();
        $args->msg("[#]", $id) if ($vb);
    }
}

sub record_xy {
    if ($xyFile) {
        close XYF;
        return unless (-s $xyFile);
    } else {
        return;
    }
    my $tmp = $xyFile.".sort";
    system("sort -S 1G \"$xyFile\" > \"$tmp\"");
    system("mv \"$tmp\" \"$xyFile\"");

    open(XYF, "<$xyFile") || $args->death
        ("Failed to read XY coordinates file", $xyFile, $!);
    my %dups;
    my $set = [ "", [] ];
    while (<XYF>) {
        s/[\n\r]+//;
        my @row = split(/\t/);
        my $seq = shift @row;
        if ($seq ne $set->[0]) {
            &write_xy($set);
            $set = [ $seq, [\@row] ];
        } else {
            push @{$set->[1]}, \@row;
        }
    }
    &write_xy($set);
    close XYF;
    $lh->write();
    my @mprb = sort keys %multProbes;
    return if ($#mprb == -1);

    my %aads  = map { $_ => 1 } map { keys %{$_} } values %multProbes;
    my @head  = sort keys %aads;
    unshift @head, ('Probe', 'MaxDup', '#Arrays');
    push @head, 'Probe sets';
    my @wid  = map { 10 } @head;
    $wid[0]  = 40;
    $wid[-1] = 50;
    my %cu = map { $head[$_] => $_ } (0..$#head);

    my $sname = "Array Report";
    my $eh = BMS::ExcelHelper->new( $mpFile );
    $eh->sheet( -name    => $sname,
                -cols    => \@head,
                -width   => \@wid,
                -freeze  => [1,1] );
    $eh->format( -name  => "mult",
                 -bg_color => 'yellow',
                 -bold  => 1,
                 -align => 'center' );
    my $psInd = $cu{'Probe sets'};
    my $naInd = $cu{'#Arrays'};
    my $mpInd = $cu{'MaxDup'};
    foreach my $seq (@mprb) {
        my @row = ($seq);
        my @frm;
        my %ps;
        my ($nArr, $maxP) = (0,0);
        while (my ($aad, $dat) = each %{$multProbes{$seq}}) {
            $nArr++;
            my $ind = $cu{$aad};
            my $num = $dat->[0];
            $maxP = $num if ($maxP < $num);
            $row[ $ind ] = $num;
            $frm[ $ind ] = 'mult';
            map { $ps{$_} = 1 } @{$dat->[1]};
        }
        $row[ $psInd ] = join(' ', sort keys %ps);
        $row[ $mpInd ] = $maxP;
        $row[ $naInd ] = $nArr;
        $eh->add_row($sname, \@row, \@frm);
    }
    $eh->close;
}

sub write_xy {
    my $set = shift;
    my $seq = shift @{$set};
    return unless ($seq);
    my $mtseq = "#Sequence#$seq";
    my %counts;
    $lh->kill_edge( -name2 => "#Sequence#".$seq,
                    -tags  => [['X', undef, undef],
                               ['Y', undef, undef]],
                    -type  => 'has member');
    foreach my $row (@{$set->[0]}) {
        my ($aad, $xy, $id) = @{$row};
        $lh->set_edge( -name1 => $id,
                       -name2 => "#Sequence#".$seq,
                       -tags  => [['X.Y', $aad, $xy]],
                       -type  => 'has member');
        $counts{$aad}{$xy} = $id;
    }
    while (my ($aad, $xyh) = each %counts) {
        my @u = keys %{$xyh};
        unless ($#u == 0) {
            my %ps = map { $_ => 1 } values %{$xyh};
            $multProbes{$seq}{$aad} = [ $#u + 1, [keys %ps] ];
        }
    }
    $lh->write_threshold_quick( $cache );
}

sub parse_snp_probe_file {
    my ($file) = shift;
    return unless ($file);
    if ($args->val(qw(copynumber iscopynumber)) || $file =~ /CN_probe_tab/) {
        return &parse_CN_probe_file( $file );
    }
    my @preErrs;
    push @preErrs, "File not found : $file" unless (-e $file);

    unless ($#preErrs == -1) {
        $args->err("Can not parse probe file", @preErrs);
        return;
    }

    $args->msg("[!!]", "Need new logic for SNP probe files");
    return;


    my @colN = qw(ProbeSet ProbePos ProbeSeq Allele Strand);
    my ($tr, $head) = &init_probe_TableReader($file, \@colN);
    my $ff   = &init_probe_fasta( $file, "AffySnpDump.fa");
    #my $colNum = &header_hash( \@head);
    #return unless ($colNum);
    # die $args->branch($colNum);
    my $set = { id => "", };

    my $num = 0;
    while (my $data = $tr->next_clean_hash()) {
        my ($id, $pos, $seq, $allele, $str) = map { $data->{$_} } @colN;
        next unless ($id);
        if ($set->{id} eq $id) {
            $tr->extend_limit();
        } else {
            &process_snp_oligo( $set );
            $set = {};
            if ($set->{id}) {
                $args->msg("[".sprintf("%6.3f M", $num/1000000)."]",  $ru->
                           getdate("0HR:0MIN:0SEC MON DAY") ." - $id")
                    unless (++$num % 10000);
            }
        }
        unless ($seq) {
            $observations->{"Null oligo entry"}++;
            $args->msg("[!]","Null oligo entry for $id");
            next;
        }
        $set->{id} ||= $id;
        $set->{str}{$str}++;
        if ($str eq 'f') {
            # We need to reverse-complement the sequence
            $seq = $su->revcom($seq);
        }
        # Weird. Not sure why the sign on the offset is reversed.
        $pos *= -1;
        $set->{ol}{lc($seq)}{$allele}{$pos} = 1;
    }
    &process_snp_oligo( $set );
    close FASTA;
    $lh->write();

    $args->msg("Parse complete, fasta file created", $ff);
}

sub process_snp_oligo {
    my $set = shift;
    my $id  = $set->{id};
    return unless ($id);
    my @ols = sort keys %{$set->{ol}};
    my (%posH, @alleles, @errs);
    push @errs, "Oligo count != 2" unless ($#ols == 1);
    for my $o (0..$#ols) {
        my $ol = $ols[$o];
        push @errs, "Oligo != 25bp : $ol" unless (length($ol) == 25);
        my @als = keys %{$set->{ol}{$ol}};
        if ($#als != 0) {
            push @errs,"Multiple alleles for $ol : ". join(',', @als);
            next;
        }
        push @alleles, $als[0];
        my @pos = keys %{$set->{ol}{$ol}{$als[0]}};
        if ($#pos != 0) {
            push @errs,
            "Multiple positions for allele $als[0] in $ol : ".
                join(',', @pos);
            next;
        }
        my $index = 12 + $pos[0];
        my $seen = uc(substr($ol, $index, 1));
        if ($seen eq $als[0]) {
            substr($ols[$o], $index, 1) = $seen;
        } else {
            push @errs,"Reported allele does not match : $seen != $als[0]";
        }
        $posH{$ol}{$pos[0]} = 1;
    }
    my %needed = map { $_ => 1 } @alleles;
    my %shared;
    while (my ($ol, $hash) = each %posH) {
        my @poses = keys %{$hash};
        if ($#poses == 0) {
            my $index = $poses[0] + 12;
            my $data  = $shared{$index+1} ||= {};
            $data->{F}{Left}{substr($ol, 0, $index)}++;
            $data->{F}{Right}{substr($ol, $index + 1)}++;
            $data->{Alleles}{uc(substr($ol, $index, 1))}++;
        } else {
            push @errs, "Inconsistent position for $ol : ".join(",", @poses);
        }
    }
    while (my ($pos, $data) = each %shared) {
        my @als = sort keys %{$data->{Alleles}};
        $data->{Alleles} = \@als;
        map { delete $needed{$_} } @als;
        while (my ($side, $hash) = each %{$data->{F}}) {
            my @uniq = keys %{$hash};
            if ($#uniq == 0) {
                $data->{$side} = $uniq[0];
            } else {
                push @errs, "Inconsistent $side flank at position $pos : ".
                    join(",", @uniq);
            }
        }
    }
    my @missingAl = sort keys %needed;
    push @errs, "Some alleles do not appear to be represented : ".
        join(",", @missingAl) unless ($#missingAl == -1);

    unless ($#errs == -1) {
        my @lines;
        foreach my $ol (sort @ols) {
            foreach my $al (sort keys %{$set->{ol}{lc($ol)}}) {
                foreach my $pos ( sort { $a <=> $b
                                         } keys %{$set->{ol}{lc($ol)}{$al}}) {
                    push @lines, sprintf("%s [%s] %d", $ol, $al, $pos);
                }
            }
        }
        push @lines, "....|....|..x.|....|....|";
        my $stxt = "[".join("/", sort keys %{$set->{str}})."]";
        $args->msg("[!]", "$id $stxt fails safety checking",@lines, @errs);
        return;
    }
    my @poses = sort { $a <=> $b } keys %shared;
    for my $p (0..$#poses) {
        my $pos   = $poses[$p];
        my $data  = $shared{$pos};
        my @als   = @{$data->{Alleles}};
        my $ambig = sprintf("%s%s%s", $data->{Left}, $#als == 0 ? $als[0] :
                            $su->ambiguous_code(\@als), $data->{Right});
        my $name  = $#poses == 0 ? $id : sprintf("%s|%02d", $id, $p+1);
        printf(FASTA ">%s  /alleles='%s' /authority='Affymetrix' /array='%s' /taxid='%d' /alleleSize='1' /allelePos='%d'\n%s\n", 
               $name, join(',',@als), $attr{chip_type}, $attr{Taxid},$pos,$ambig);
    }
    foreach my $ol (@ols) {
        my $mtn = "#Sequence#$ol";
        $lh->set_length($mtn, 25);
        $lh->set_class($mtn, 'Affy Probe Sequence');
        $lh->set_taxa($mtn, $attr{genome_species});

        $lh->set_edge( -name1 => $id,
                       -name2 => $mtn,
                       -type  => "has member" );
    }
    $lh->write_threshold_quick( $cache );
    # $args->msg($id, $ambig, @ols);
}

sub parse_CN_probe_file {
    my ($file) = shift;
    return unless ($file);
    $args->death("Need to update for new work flow");
    my @preErrs;
    push @preErrs, "File not found : $file" unless (-e $file);
    unless ($#preErrs == -1) {
        $args->err("Can not parse CN probe file", @preErrs);
        return;
    }
    my @colN = qw(ProbeSet ProbeSeq ChipX ChipY);
    my ($tr, $head) = &init_probe_TableReader($file, \@colN);
    my $ff   = &init_probe_fasta( $file, "AffyCopyNumberDump.fa");
    my (%seen, %same);
    my @seqClass = qw(AFFYPROBE AFFYSEQ CNV);
    my @setClass = qw(AFFYSET CNV);
    my $aad      = $attr{chip_type};
    my $tid      = $attr{Taxid};
    while (my $data = $tr->next_clean_hash()) {
        my ($id, $seq, $x, $y) = map { $data->{$_} } @colN;
        next unless ($id);
        my @errs;
        push @errs, "Unexpected ID format" unless ($id =~ /^CN_\d+$/);
        $seq = uc($seq);
        my $slen = length($seq);
        push @errs, "Oligo is not 25bp long" unless ($slen == 25);
        my $hack = $seq; $hack =~ s/[ACTG]//g;
        push @errs, "Unusual characters in oligo: '$hack'" if ($hack);
        if (my $prior = $seen{$id}) {
            $args->msg("[!!]", "ID duplicated in file: $id [$prior/$seq]");
        }
        $seen{$id} ||= $seq;
        unless ($#errs == -1) {
            $args->msg("[!!]", "Errors parsing '$id' [$seq]", @errs);
            next;
        }
        printf(FASTA ">%s  Copy Number Variant /authority='Affymetrix' /array='%s' /taxid='%d' /type='CNV'\n%s\n", 
               $id, $aad, $tid, $seq);

        push @{$same{$seq}}, $id;
        $seq = "#Sequence#$seq";
        map { $lh->set_class($seq, $_) } @seqClass;
        map { $lh->set_class($id, $_) } @setClass;
        map { $lh->set_taxa($_, $tid) } ($seq, $id);
        $lh->set_length($seq, $slen);
        $lh->set_edge( -name1 => $id,
                       -name2 => $aad,
                       -type  => 'is a member of' );
        $lh->set_edge( -name1 => $id,
                       -name2 => $seq,
                       -die   => $args->death("NO TAGS HERE"),
                       -tags  => [ ['X', $aad, $x],
                                   ['Y', $aad, $y ] ],
                       -type  => 'has member');
        if ($lh->operations() >= $cache) {
            $lh->write();
            $args->msg("[#]", $id) if ($vb);
        }
    }
    close FASTA;
    my @theSame;
    while (my ($seq, $ids) = each %same) {
        next if ($#{$ids} == 0);
        my @ids = @{$ids};
        push @theSame, "$seq : ".join(' ', @ids);
        for my $i (0..$#ids) {
            for my $j (0..$#ids) {
                next if ($i == $j);
                $lh->set_edge( -name1 => $ids[$i],
                               -name2 => $ids[$j],
                               -type  => 'SAMEAS',
                               -auth  => 'tilfordc' );
            }
        }
    }
    unless ($#theSame == -1) {
        $args->msg("[?]","Some probes have multiple accessions", @theSame);
    }
    $lh->write();
    $args->msg("Parse complete, fasta file created", $ff);
}

sub init_probe_fasta {
    my ($file, $defOut) = @_;
    my $ff = $args->val(qw(output fasta)) || $defOut;
    open(FASTA, ">$ff") || $args->death("Failed to write fasta file", $ff, $!);
    $args->msg("Processing file:", $file, "Generating Fasta:", $ff);
    return $ff;
}

sub init_probe_attr {
    my $file = shift;
    return ("No file path provided") unless ($file);
    return ("File does not exist") unless (-s $file);

    my ($aad, $sf)  = &aad_from_file( $file );
    $observations = {};
    my @errs;
    if (my $gAttr = $chipData->{$aad}) {
        %attr = %{$gAttr};
        &set_tax_id();
        $args->msg("[<]", "Beginning probe sequence parse for $attr{chip_type}", $sf);
        return ();
    } else {
        return ("Failed to find precomputed attributes for $aad");
    }
}

sub set_tax_id {
    my $spec = $attr{genome_species};
    return unless ($spec);
    my ($tax, $taxMT) = $ad->standardize_taxa( $spec, 'TAX' );
    if ($taxMT) {
        $attr{genome_species} = $tax;
        $attr{Taxid} = $taxMT->id;
    } else {
        &obsv_error("Failed to normalize species name", $spec);
    }
}

sub init_probe_TableReader {
    my ($file, $coln) = @_;
    my $short = $file; $short =~ s/.+\///;
    my $lacksHead = $noHeader->{$short};
    my $tr = BMS::TableReader->new
        ( -limit     => $limit,
          -colmap    => $standardColumns,
          -hasheader => $lacksHead ? 0 : 1,
          -format    => 'tsv' );
    unless ($lacksHead) {
        while (my ($in, $out) = each %{$standardColumns}) {
            $tr->remap_header_name( $in, $out );
        }
    }
    $tr->input($file);
    $tr->select_sheet(1);
    if ($lacksHead) {
        $tr->set_header($lacksHead);
    } 
    # $tr->has_header(1);
    # warn $args->branch($tr);
    my @head = $tr->header();
    my (@exp, @ign);
    foreach my $cn (@head) {
        if ($expectedColumns{$cn}) {
            push @exp, $cn;
        } else {
            push @ign, $cn;
            $tr->ignore_column_name($cn);
        }
    }
    if ($db) {
        $args->msg("=== Recognized Headers: ", @exp,
                   "=== Ignored Headers:", @ign) ;
    }

    my %neededCol = map { $_ => 1 } @{$coln};
    map { delete $neededCol{$_} } @head;
    my @missing = sort keys %neededCol;
    $args->death("Probe file does not have all the required columns",
                 $file, @missing, "Present:", map { "'$_'" } @head) unless ($#missing == -1);
    return ($tr, \@head);
}

sub header_hash {
    my $header = shift;
    if (!$header || $#{$header} == -1) {
        $args->err("Empty input file header");
        return undef;
    }
    my %colNum;
    for my $c (0..$#{$header}) {
        my $cn = $header->[$c];
        unless ($cn) {
            $args->err("Blank column header in column $c");
            next;
        }
        if (my $sn = $standardColumns->{$cn}) {
            $colNum{$sn} = $c;
        }
    }
    return \%colNum;
}

sub array_1d {
    my $txt = shift;
    return [] if (!defined $txt || $txt =~ /^\s*$/);
    return [ split(/\s*\/{2,}\s*/, $txt) ];
}


sub array_2d {
    my $txt = shift;
    return [] if (!defined $txt || $txt =~ /^\s*$/);
    my @rv;
    foreach my $part (split(/\s*\/{3,}\s*/, $txt)) {
        my @row;
        foreach my $val (split(/\s*\/\/\s*/, $part)) {
            $val = "" if (!defined $val || $val eq '---');
            push @row, $val;
        }
        push @rv, \@row;
    }
    return \@rv;
}

sub extract_2d {
    my ($txt, $index) = @_;
    $index ||= 0;
    my $arr = &array_2d($txt);
    my @vals;
    foreach my $row (@{$arr}) {
        my $val = $row->[$index];
        push @vals, $val if ($val ne '');
    }
    return @vals;
}

sub get_all_files {
    my @stack = @_;
    my @files;
    while (my $dir = shift @stack) {
        $args->msg("Finding CSV files : $dir");
        opendir(DIR, $dir) 
            || $args->death("Failed to read directory contents", $dir, $!, 2);
        foreach my $file (readdir DIR) {
            next if ($file =~ /^\.+$/);
            my $path = "$dir/$file";
            if (-d $path) {
                push @stack, $path;
            } elsif ($file =~ /\.annot\..*\.gz$/) {
                next if ($keepRE && $file =~ /$skipRE/);
                next if ($keepRE && $file !~ /$keepRE/);
                push @files, $path;
            } else {
                # $args->msg("Ignoring : $path");
            }
        }
        closedir DIR;
    }
    return wantarray ? @files : \@files;
}

sub load_all_files {
    my @types = qw(annot probe_fasta);
    if (my $tr = $args->val(qw(filetype type))) {
        @types = ($tr);
    }
    my $allAges = $args->val(qw(allage allages));
    my $skip    = lc($args->val(qw(skipuntil skipto)) || "");
    $skip =~ s/-/_/g;
    my $onlyChip;
    foreach my $cname ($args->each_split_val('/[\n\r\,\s]/',
                                             qw(chip array design))) {
        if (my $arr = &standardize_array($cname)) {
            $onlyChip ||= {};
            $onlyChip->{$arr} = 1;
        } elsif ($cname) {
            $args->msg("[?]","Failed to recognize chip request '$cname'");
        }
    }
    $args->msg("[LIMIT]","Only processing requested arrays",
               join(' ', sort keys %{$onlyChip})) if ($onlyChip);
    foreach my $dArr (sort keys %{$allFiles}) {
        if ($skip) {
            next unless ($skip eq lc($dArr));
            $skip = "";
        }
        if ($onlyChip && !$onlyChip->{$dArr}) {
            next;
        }
        my $struct = $allFiles->{$dArr};
        foreach my $type (@types) {
            my $list = $struct->{$type};
            unless ($list) {
                $args->msg("[-]", "No $type files for $dArr");
                next;
            }
            my @files = sort { $b->{date} cmp $a->{date} } @{$list};
            @files = ($files[0]) unless ($allAges);
            foreach my $fdat (@files) {
                my $file = $fdat->{path};
                if ($chipOnly) {
                    &basic_array($file);
                } else {
                    &parse_csv($file);
                    $lh->write();
                }
            }
        }
    }
}

sub available_files {
    my $adir = "$base/arrays";
    my %data;
    foreach my $path ($args->read_dir( -dir => $adir, -recurse => 1 )) {
        my @bits   = split(/\//, $path);
        my $file   = pop @bits;
        my $dArr   = &standardize_array(pop @bits);
        if ($file =~ /^(.+)\.([^\.]+)\.((\d{4})-(\d{2})-(\d{2}))\.([a-z]{2,6})$/) {
            my ($type, $date, $sfx) = ($2, $3, $7);
            my $fArr = &standardize_array($1);
            $args->msg_once("[?]", "Directory '$dArr' != File '$fArr'", $path)
                unless ($dArr eq $fArr);
            push @{$data{$dArr}{$type}}, {
                type => $type,
                array => $dArr,
                date  => $date,
                path  => $path,
                sfx   => $sfx,
            };
        }
    }
    while (my ($arr, $tHash) = each %data) {
        while (my ($type, $list) = each %{$tHash}) {
            $tHash->{$type} = [ sort { $b->{date} cmp $a->{date} } @{$list} ];
        }
    }
    return \%data;
}

sub aad_from_file {
    my $file = shift;
    my @fb = split(/\//, $file);
    my $sf = $fb[-1];
    my $aadFromFile = "";
    if ($sf =~ /^([^\.]+)\./) {
        $aadFromFile = &standardize_array($1);
    }
    return wantarray ? ($aadFromFile, $sf) : $aadFromFile;
}

sub tr_and_attr {
    my $file = shift;
    my ($aad, $sf)  = &aad_from_file( $file );
    $observations = {};
    my $tr = BMS::TableReader->new( -limit => $limit,
                                    -quotes => '"',
                                    -format => 'csv',
                                    -input  => $file, );
    $tr->input($file);
    $args->msg("[<]", "Beginning Parse", $sf);
    my ($sheet) = $tr->each_sheet(); # Will be just one for CSV
    $tr->select_sheet($sheet);
    # Find the header, set attributes
    my $header;
    %attr = ( filePath => $file ); 
    while (my $row = $tr->next_clean_row()) {
        $tr->extend_limit();
        if ($#{$row} == -1) {
            next;
        } elsif ($row->[0] =~ /^\#/) {
            if ($row->[0] =~ /^\#\%([^=]+)=(.+)\s*$/) {
                my ($k, $v) = (lc($1), $2);
                $k =~ s/\-/_/g;
                $attr{$k} ||= $v;
            }
            next;
        }
        $header = $attr{colHeader} = $row;
        last;
    }

    if ($attr{chip_type} && 
        $attr{chip_type} =~ /^(([A-Z][a-z]{1,2}|CHO)Gene)-/) {
        # These designs are using integer probeset IDs
        # We need to prefix them to load them into MapTracker
        $attr{ps_prefix} = $1 . '_';
        
    }
    $attr{chip_type} = $aad;

    unless ($attr{'genome_species'}) {
        foreach my $key ('genome_version', 'chip_type') {
            if (my $v = $attr{$key}) {
                last if ($attr{'genome_species'} = $bldMap->{$v});
            }
        }
    }
    &set_tax_id();
    if ($attr{'genome_species'} && $attr{'genome_version_ncbi'}) {
        my ($spec, $build) = 
            ($attr{'genome_species'}, $attr{'genome_version_ncbi'});
        $spec = lc($spec);
        $spec =~ s/ /_/g;
        $build =~ s/\.\d+$//;
        my $bname;
        if ($spec eq 'homo_sapiens') {
            if ($build =~ /^GRCh(\d+)$/i) {
                $bname = "GRCh$1";
            } elsif ($build =~ /^\d+$/) {
                $bname = $build < 37 ? "NCBI$build" : "GRCh$build";
            } else {
                &obsv_error("Unrecognized genome build",$build);
                $build = "";
            }
        } elsif ($spec eq 'mus_musculus') {
            if ($build == 37) {
                $bname = "NCBIM$build"; 
            } elsif ($build == 38) {
                $bname = "GRCm$build"; 
            }
        } elsif ($spec eq 'rattus_norvegicus') {
            if ($build eq 'Rnor3') {
                $bname = "RGSC3_4";
            }
        }
        if ($bname) {
            $attr{chrFormat} = "$spec.chromosome.\%s.$bname";
        } else {
            $args->err("No logic to build chromosome name for species",
                       "$spec build $build");
        }
        $attr{SDB} =  $mt->make_searchdb
            ( -name => "Affymetrix $bname SNP Mappings",
              -type => "external",
              -path => "Affymetrix SNP genotyping positional data against build $bname" ) if ($bname);
    }
    unless ($header) {
        &obsv_error("Failed to identify header");
        return undef;
    }

    unless ($attr{colMap} = &header_hash( $header) ) {
        &obsv_error("Failed to find recognizable column headers");
        return undef;
    }

    $attr{isSnp} = 1 if (exists $attr{colMap}{Frequency} &&
                         defined $attr{colMap}{Frequency});
    $attr{isSnp} = 1 if ($attr{dbsnp_version});

    if ($db) {
        my (@get, @noGet);
        foreach my $h (@{$header}) {
            if (exists $standardColumns->{$h} && $standardColumns->{$h}) {
                push @get, $h;
            } else {
                push @noGet, $h;
            }
        }
        $args->msg("Header Report:", "=== Recognized and Captured:",
                   @get, "=== Not captured:", @noGet);
        $args->msg("Global Attributes", map { 
            sprintf("%35s : %s", $_, $attr{$_}) } sort keys %attr);
    }
    # die $args->branch(\%attr);
    return $tr;
}

sub standardize_array {
    my $req = shift;
    return "" unless ($req);
    my $rv = uc($req);
    $rv =~ s/[\s\-]+/_/g;
    $rv =~ s/^GENOMEWIDESNP_/GW/;
    if ($rv =~ /^MAPPING(.+)/) {
        $rv = $1 . "_SNP";
    }
    return $rv;
}

sub remote_update {

    my $wl = "$base/ArrayWhitelist.txt";
    open(WL,"<$wl") || $args->death("Failed to read array whitelist", $wl, $!);
    while (<WL>) {
        s/[\n\r]+$//;
        s/^\s+//;
        s/\s+$//;
        $fileRecovery{WhiteList}{uc($_)} = $_ if ($_);
    }
    close WL;

    require LWP::UserAgent;
    require XML::Twig;

    my $site    = "https://www.affymetrix.com";
    my $url     = "$site/analysis/downloads/netaffxapi/GetFileList.jsp";
    my $tdir    = "$base/working";
    my $listFile = "$base/fileList.xml";
    $args->assure_dir($listFile, 'isfile');
    if ($args->val(qw(reuselist))) {
        if (-s $listFile) {
            $args->msg("[>]","Reusing previously recovered list file",
                       $listFile);
        } else {
            $args->death("Request to reuse list file that does not exist",
                         $listFile);
        }
    } else {
        my $ua      = LWP::UserAgent->new;
        $ua->proxy('http', $ENV{http_proxy} ||
                   'http://proxy-server.bms.com:8080');
        my $listUrl = "$url?license=$ENV{AFFYLICENSE}&user=$ENV{AFFYUSER}&password=$ENV{AFFYPASSWORD}";
        # my $request = HTTP::Request->new('GET', $listUrl);
        $args->msg("[<]","Requesting file list");
        my $rsp = $ua->get( $listUrl, ':content_file' => $listFile );
        my $rMsg = $rsp->{_msg} || "-No message returned-";
        if ($rMsg eq 'OK') {
            $args->msg("[FILE]", $listFile);
        } else {
            $args->err("Possible error while recovering file list:", $rMsg);
        }
    }
    my $twig  = XML::Twig->new( twig_handlers => 
                                { Array => \&parse_file_xml, } );
    $twig->parsefile( $listFile );
    my $repFile  = "$hBase/NetAffxFileReport.txt";
    my $missFile = "$hBase/MissingFiles.txt";
    open(REP, ">$repFile") || $args->death
        ("Failed to write file report", $repFile, $!);
    open(MISS, ">$missFile") || $args->death
        ("Failed to write missing data file", $missFile, $!);
    print REP `date`;
    print MISS "Array\tMissing File\n";
    foreach my $ucarr (sort keys %{$fileRecovery{Capture}}) {
        my $adat  = $fileRecovery{Capture}{$ucarr};
        my $array = $adat->{array};
        print REP "\n$array\n";
        foreach my $fdat (sort { $a->{tok} cmp $b->{tok} } 
                          map { $_->[0] } values %{$adat->{files}}) {
            printf REP ("  %15s %s %s\n", $fdat->{tok}, $fdat->{date}, $fdat->{url});
        }
        if (my $errs = $adat->{error}) {
            printf REP "  ERRORS:\n";
            foreach my $k (sort keys %{$errs}) {
                my $v = $errs->{$k};
                printf REP "    $k : $v\n";
                print MISS join("\t", $array, $k)."\n"
                    if ($v eq 'No file found');
            }
        }
    }

    foreach my $key ('Skipped Arrays', 'Skipped File Types',) {
        my @skipped = sort keys %{$fileRecovery{$key} || {}};
        next if ($#skipped == -1);
        print REP "\n$thickBar$key\n$thickBar\n";
        
        foreach my $k2 (@skipped) {
            print REP "  $k2 : $fileRecovery{$key}{$k2}\n";
        }
    }
    close REP;
    close MISS;
    $args->msg("[FILE]", "Available file parse report", 
               $repFile, $listFile, $missFile);
    &mirror();
}

sub mirror {
    my $log = "$base/wget.log";
    my $dir = "$base/zip";
    my $tmp = "$dir/tmp";
    my %needed = %{$fileRecovery{WhiteList}};
    foreach my $ucarr (sort keys %{$fileRecovery{Capture}}) {
        my $adat   = $fileRecovery{Capture}{$ucarr};
        my $array  = $adat->{array};
        my $arrTok = $array; $arrTok =~ s/\s+/_/g;
        delete $adat->{error};
        foreach my $fdat (sort { $a->{tok} cmp $b->{tok} } 
                          map { $_->[0] } values %{$adat->{files}}) {
            my $url = $fdat->{url};
            my $tok = $fdat->{tok};
            my $dt  = $fdat->{date};
            my $nm  = $fdat->{name};
            my $gz  = sprintf("%s.%s.%s.gz", $arrTok, $tok, $dt);
            my $gzP = sprintf("%s/arrays/%s/%s", $base, $arrTok, $gz);
            if (-s $gzP && !$clobber) {
                $fdat->{gz} = $gzP;
                &probe_tab_to_probe_fasta( $gzP ) if ($tok eq 'probe_tab');
                next;
            }
            my $cmd = "wget --mirror --directory-prefix='$dir' -nv -o $log $url";
            unlink($log) if (-e $log);
            my $rv  = `$cmd`;
            $args->msg("[?]","Possible wget error: $rv", $cmd) if ($rv);
            my $short = $url;
            $short    =~ s/^.+\/\///;
            my $path  = "$dir/$short";
            unless (-e $path) {
                $adat->{error}{"Failed to find $tok file"} = $path;
                next;
            }
            system("rm -rf \"$tmp\"") if (-d $tmp);
            $args->assure_dir($tmp);
            my $zcmd = "unzip -d \"$tmp\" $path";
            my $zrv  = `$zcmd`;
            # $args->msg("[?]","Possible unzip error: $zrv", $zcmd) if ($zrv);
            my @found = split(/[\n\r]+/, `find $tmp -iname '$nm'`);
            if ($#found == -1) {
                $adat->{error}{"Failed to unzip $tok file"} = $nm;
                next;
            }
            $args->assure_dir( $gzP, 'isFile');
            my $gzcmd = "gzip -c \"$found[0]\" > \"$gzP\"";
            my $gzrv  = `$gzcmd`;
            $args->msg("[?]","Possible gzip error: $gzrv", $gzcmd) if ($gzrv);
            unless (-e $gzP) {
                $adat->{error}{"Failed to gzip $tok file"} = $gzP;
                next;
            }
            delete $needed{$ucarr};
            # $args->death($url, $log, $tmp, $gzP);
            $args->msg("[WGET]", $short, $url);
        }

    }
}

sub probe_tab_to_probe_fasta {
    my $tabFile = shift;
    return unless ($tabFile && -s $tabFile);
    # return if ($tabFile =~ /SNP/);
    my $faFile = $tabFile;
    $faFile =~ s/probe_tab/probe_fasta/;
    return if (-s $faFile);
    my ($aad, $sf)  = &aad_from_file( $tabFile );
    unless ($aad) {
        $args->msg("[!]","Failed to get array design from file", $tabFile);
        return;
    }
    $args->msg("[-]","Backfilling $aad Fasta from probe_tab", $tabFile);
    my $faRaw = $faFile;
    $faRaw =~ s/\.gz$//;
    open(RAWF, ">$faRaw") || $args->death
        ("Failed to write fasta file", $faRaw, $!);
    my @colN = qw(ProbeSet ChipX ChipY ProbeSeq);
    my ($tr, $head) = &init_probe_TableReader($tabFile, \@colN);
    push @colN, qw(ProbePos Sense);
    while (my $data = $tr->next_clean_hash()) {
        my ($id, $x, $y, $seq, $pos, $sen) = 
            map { $_ eq '---' ? '0' : $_ }
        map { defined $_ ? $_ : 0 }
        map { $data->{$_} } @colN;
        printf(RAWF ">probe:%s:%s:%d:%d; Interrogation_Position=%d; %s;\n%s\n",
               $aad, $id, $x, $y, $pos, $sen, $seq);
    }
    close RAWF;
    my $gzcmd = "gzip \"$faRaw\"";
    my $gzrv  = `$gzcmd`;
    $args->msg("[?]","Possible gzip error: $gzrv", $gzcmd) if ($gzrv);
    $args->msg("[>]", "Fasta file generated from tab file", $faFile);
}

sub parse_file_xml {
    my ($twig, $node) = @_;
    my $array = $node->{att}{name};
    return unless ($array);
    my $ucarr = uc($array);
    unless (exists $fileRecovery{WhiteList}{$ucarr} && 
            $fileRecovery{WhiteList}{$ucarr}) {
        $fileRecovery{'Skipped Arrays'}{$array} = "Not whitelisted";
        return;
    }
    
    my %found;
    foreach my $annot ($node->children('Annotation')) {
        my $type = $annot->{att}{type};
        my $ttok = $typeTok->{$type};
        unless ($ttok) {
            $fileRecovery{'Skipped File Types'}{$type} = "Not in whitelist";
            next;
        }

        my $targ = $fileRecovery{Capture}{$ucarr} ||= {
            array => $array,
            files => {},
        };

        foreach my $fobj ($annot->children('File')) {
            my $rawDt = $fobj->{att}{date};
            unless ($rawDt ) {
                $targ->{error}{$ttok} = "No date defined for file";
                next;
            }
            my $fname  = $fobj->{att}{name};
            unless ($fname) {
                $targ->{error}{$ttok} = "filename not defined";
                next;
            }
            my @urls = $fobj->children('URL');
            my $unum = $#urls + 1;
            if ($unum != 1) {
                $targ->{error}{$ttok} = "$unum URLs defined";
                next;
            }
            $targ->{ok}++;
            my $url  = $urls[0]->text();
            my $dt   = ParseDate( $rawDt );
            my $nice = UnixDate($dt, '%Y-%m-%d');
            push @{$targ->{files}{$ttok}}, {
                name => $fname,
                type => $type,
                tok  => $ttok,
                url  => $url,
                date => $nice,
            };
        }
        if (exists $targ->{files}{$ttok} && $#{$targ->{files}{$ttok}} > 0) {
            # Keep only the most recent one
            my @srt = sort { $b->{date} cmp $a->{date} 
                         } @{$targ->{files}{$ttok}};
            $targ->{files}{$ttok} = [ $srt[0] ];
        }
    }
    unless (exists $fileRecovery{Capture}{$ucarr} &&
            $fileRecovery{Capture}{$ucarr}{ok}  ) {
        $fileRecovery{'Skipped Arrays'}{$array} = 
            "No relevant files discovered";
    }
    my $targ = $fileRecovery{Capture}{$ucarr};
    foreach my $ttok ('annot', 'probe_fasta', 'probe_tab') {
        unless ($targ->{files}{$ttok}) {
            $targ->{error}{$ttok} = "No file found";
        }
    }
  
}

sub show_row {
    my $row = shift;
    my $head = $attr{colHeader} || [];
    my $rv = "------------------\n";
    for my $i (0..$#{$row}) {
        $rv .= sprintf(" %20s : %s\n", $head->[$i] || '?', $row->[$i]);
    }
    return $rv;
}

sub snp_fasta_file {
    my $file = shift;
    my $tr = &tr_and_attr( $file );
    return unless ($tr);
    my $colNum   = $attr{colMap};
    my $psCol    = $colNum->{ProbeSet};
    my $rsCol    = $colNum->{dbSNP};
    my $flCol    = $colNum->{Flank};
    while (my $row = $tr->next_clean_row()) {
        my $id    = &stnd_id( $row->[ $psCol ] );
        if (!$id || $id eq '---') {
            $args->msg("[?]","Row without ID");
            next;
        }
        my $flank = lc($row->[$flCol] || "");
        if (!$flank || $flank eq '---') {
            $args->msg("[?]","$id : No Flank data");
            next;
        }
        if ($flank =~ /^([a-z]*)\[([actg\/]+)\]([a-z]*)/) {
            my ($lft, $atxt, $rgt) = ($1 || "", uc($2), $3 || "");
            my @als   = split(/\//, $atxt);
            my $ambig = $su->ambiguous_code(\@als);
            $flank    = $lft.$ambig.$rgt;
        } else {
            $args->msg("[?]","$id : Odd Flank : $flank");
            next;
        }
        print ">$id";
        if (my $rs = $row->[$rsCol]) {
            print " $rs" unless ($rs eq '---');
        }
        print "\n$flank\n";
    }    
}
