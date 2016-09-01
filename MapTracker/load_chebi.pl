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
use LWP::UserAgent;

my $args = BMS::BmsArgumentParser->new
    ( -cache    => 20000,
      -nocgi    => $ENV{'HTTP_HOST'} ? 0 : 1,
      -dir      => '',
      -nocgi    => 1,
      -limit    => 0,
      -testmode => 1,
      -basedir  => "/work5/tilfordc/maptracker/ChEBI",
      -testfile => "ChEBI_Loader.txt",
      -errfile  => "ChEBI_Loader_Errors.err",
      -age      => 7,
      -progress => 300,
      -errormail  => 'charles.tilford@bms.com',
      -preload  => 0,
      -verbose  => 1, );

$args->shell_coloring();

my $limit     = $args->val(qw(lim limit));
my $tm        = $args->val(qw(tm testmode));
my $vb        = $args->val(qw(vb verbose)) || 0;
my $user      = $args->val(qw(auth authority user));
my $errFile   = $args->val(qw(errfile));
my $idTag     = "ChEBI ID";
my $idFmt     = '^ChEBI:(\d+)$';

my $lh = BMS::MapTracker::LoadHelper->new
    ( -username  => 'ChEBI',
      -basedir   => 'ChEBI',
      -loadtoken => 'ChEBI',
      -testmode => $tm, );

my $canCmd    = "/usr/bin/nice -n19 /stf/biocgi/tilfordc/working/maptracker/MapTracker/canonicalize_smiles_file.pl";

my $errFH;
if ($errFile) {
    open(ERRFILE, ">$errFile") || $args->death
        ("Failed to write error file", $errFile, $!);
    $errFH = *ERRFILE;
}
&parse_sdf($args->val(qw(sdf file input)));

$lh->write() if ($lh->row_count());

if ($errFile) {
    close ERRFILE;
    if (-s $errFile) {
        $args->msg("Errors observerd:", $errFile);
    } else {
        unlink $errFile;
    }
}
$args->msg("Finished");

sub parse_sdf {
    my $file = shift;
    unless ($file) {
        $args->msg("Please provide a path to the ChEBI SDF file",
                   "It will likely be called ChEBI_complete.sdf");
        exit;
    }
    my $canDat = &canonical_smiles( $file );
    open(FILE, "<$file") || $args->death
        ("Failed to read SDF FILE", $file, $!);
    my $num = 0;
    while (my $data = &next_sdf_record(*FILE)) {
        &process_sdf( $data, $canDat );
        $num++;
        last if ($limit && $num >= $limit);
    }
    close FILE;
}

sub canonical_smiles {
    my $file = shift;
    my $canFile = $file;
    $canFile    =~ s/\.[^\.]+$//;
    $canFile   .= ".canonical.smi";
    unless (-s $canFile) {
        my $rawFile  = &raw_smiles( $file );
        my $cmd1     ="$canCmd -user ChEBI -mode canon -preload -fork 1 -progress 60 -testmode 0 -file \"$rawFile\" -output \"$canFile\"";
        system($cmd1);

        my $simpFile = $file;
        $simpFile    =~ s/\.[^\.]+$//;
        $simpFile .= ".Simple.smi";
        my $cmd2     ="$canCmd -user ChEBI -mode simplify -preload -fork 1 -progress 60 -testmode 0 -file \"$canFile\" -output \"$simpFile\"";
        system($cmd1);
    }
    open(CANF, "<$canFile") || $args->death
        ("Failed to read canonical SMILES file", $canFile, $!);
    my $lookup = {};
    while (<CANF>) {
        s/[\n\r]+$//;
        my ($smi, $id) = split(/\s+/);
        next unless ($smi);
        if ($id) {
            $lookup->{$id} = "$smi";
        } else {
            &err("SMILES without ID : $smi");
        }
    }
    return $lookup;
}

sub chebi_id {
    my $data = shift;
    if (my $arr = $data->{$idTag}) {
        if ($#{$arr} == 0) {
            my $id = $arr->[0];
            $id =~ s/^chebi/ChEBI/i;
            return $id;
        } elsif ($#{$arr} > 0) {
            &err("Multiple $idTag fields : ". join(',', @{$arr}) || '??');
            return "";
        }
    }
    &err("No $idTag field : ". join(',', @{$data->{Name} || []}) || '??');
    return "";
}

sub raw_smiles {
    my $file = shift;
    my $rawFile = $file;
    $rawFile    =~ s/\.[^\.]+$//;
    $rawFile   .= ".raw.smi";
    unless (-s $rawFile) {
        open(RAWFILE, ">$rawFile") || $args->death
            ("Failed to write raw SMILES file", $rawFile, $!);
        open(SRCFILE, "<$file") || $args->death
            ("Failed to read SDF file", $file, $!);
        my ($num, $mnum) = (0,0);
        while (my $data = &next_sdf_record(*SRCFILE)) {
            my $smi = $data->{SMILES} || [];
            my $id  = &chebi_id( $data );
            next unless ($id);
            if ($#{$smi} == 0) {
                print RAWFILE $smi->[0] . " $id\n";
                $num++;
                next;
            } elsif ($#{$smi} > 0) {
                &err("Multiple SMILES - $id : ". join(" ", @{$smi}));
                next;
            }
            my $mesSmi = &mes_smiles( $id );
            unless ($mesSmi) {
                &err("No SMILES recovered from MES : $id");
                next;
            }
            print RAWFILE $mesSmi . " $id\n";
            $mnum++;
        }
        close SRCFILE;
        close RAWFILE;
        $args->msg("[FILE]", "Raw SMILES extracted", $rawFile,
                   "$num in SDF, $mnum from MES");
    }
    return $rawFile;
}

sub next_sdf_record {
    my $fh = shift;
    my $inHead = 1;
    my $section;
    my $data = { };
    my $cntr = 0;
    while (<$fh>) {
        s/[\n\r]+$//;
        s/\s+$//;
        s/\P{IsASCII}//g;
        if (/^\${4}$/) {
            $cntr++;
            last;
        }
        # Need to make sure I understand the header...
        #if (/^\s*Marvin/) {
        #    $inHead = 0;
        #} elsif ($inHead) {
        #    s/^\s+//;
        #    $data->{Name}{$_} ||= ++$cntr if ($_);
        #    next;
        #}
        if (/\>\s+\<(.+)\>\s*$/) {
            # New section
            $section = $1;
            next;
        }
        if ($section) {
            s/^\s+//;
            next if ($_ eq '');
            $data->{$section}{$_} ||= ++$cntr;
        }
    }
    return undef unless ($cntr);
    foreach my $key (sort keys %{$data}) {
        my $hash = $data->{$key};
        my @arr = sort { $hash->{$a} <=> $hash->{$b} } keys %{$hash};
        $data->{$key} = \@arr;
    }
    return $data;
}

sub process_sdf {
    my ($data, $lookup) = @_;
    return unless ($data);
    my $id = &chebi_id( $data );
    return unless ($id);
    my $smi = $lookup->{$id};
    unless ($smi) {
        my $msg = "No canonical SMILES for $id";
        my $smid = $data->{SMILES} || [];
        if ($#{$smid} == 0 && $smi->[0]) {
            $msg .= " = ".$smi->[0];
        }
        &err($msg);
        return;
    }
    $data->{SMILES} = $smi;
    my $mtsmi = "#SMILES#$smi";
    $lh->set_edge( -name1 => $id,
                   -name2 => $mtsmi,
                   -type  => 'is a reliable alias for' );
    $lh->set_edge( -name1 => $mtsmi,
                   -name2 => $id,
                   -type  => 'is a reliable alias for' );
    foreach my $sid (@{$data->{'Secondary ChEBI ID'} || []}) {
        if ($sid =~ /$idFmt/i) {
            $sid = "ChEBI:$1";
            $lh->set_edge( -name1 => $sid,
                           -name2 => $mtsmi,
                           -type  => 'is a reliable alias for' );
            $lh->set_edge( -name1 => $id,
                           -name2 => $sid,
                           -type  => 'is the preferred lexical variant of');
        } else {
            &err("Malformed Secondary ID for $id : $sid");
        }
    }
    foreach my $desc (@{$data->{'Definition'} || []}) {
        $desc =~ s/<[^>]+>//g;
        $desc =~ s/\s{2,}/ /g;
        next unless ($desc);
        $lh->set_edge( -name1 => $mtsmi,
                       -name2 => "#FreeText#$desc",
                       -type  => 'is a shorter term for' );
    }
    foreach my $key ('ChEBI Name','Synonyms', 'BRAND Names', 'INN') {
        my $tk = $key;
        $tk =~ s/s$//;
        my @tags = (["Alias Type", $tk, undef]);
        foreach my $ali (@{$data->{$key} || []}) {
            $ali =~ s/<[^>]+>//g;
            $ali =~ s/\s{2,}/ /g;
            next unless ($ali);
            $lh->set_edge( -name1 => $mtsmi,
                           -name2 => "#FreeText#$ali",
                           -tags  => \@tags,
                           -type  => 'is a reliable alias for' );
        }
    }
    $lh->write_threshold_quick();
    return;

    &_to_one_line( $data );
}

sub _to_one_line {
    my $data = shift;
    my $smi = $data->{SMILES};
    my $row = $smi;
    
    foreach my $tag ($idTag, 'Definition', 'Synonyms') {
        if (my $d = $data->{$tag}) {
            $row .= join('', map { " $_" } @{$d}) || "";
        }
    }
    print "$row\n";
}

sub _to_one_line_detailed {
    my $data = shift;
    my $smi = ${$data->{SMILES} || []}[0];
    delete $data->{SMILES};
    my $row = $smi;
    foreach my $key (sort keys %{$data}) {
        foreach my $val (@{$data->{$key}}) {
            $row .= " \"$key=\"$val";
        }
    }
    print "$row\n";
}

sub err {
    my $msg = shift;
    return unless ($msg);
    $msg .= "\n";
    if ($errFH) {
        print $errFH $msg;
    } else {
        warn $msg;
    }
}

sub mes_smiles {
    my $id = shift;
    return "" unless ($id);
    unless ($id =~ /$idFmt/i) {
        &err("Malformed ChEBI ID : $id");
        return "";
    }
    my $url = "http://research.pri.bms.com:8080/CSRS/".
        "services/lookup/smiles/MES_NAME/$id";
    my $ua      = LWP::UserAgent->new;
    my $request = HTTP::Request->new('GET', $url);
    my $tmp    = "/scratch/MES_Output.smi";
    unlink($tmp) if (-e $tmp);
    $ua->request($request, $tmp );
    unless (-e $tmp) {
        &err("No MES response for $id");
        return "";
    }
    open(TMP, "<$tmp") || $args->death
        ("Failed to read LWP scratch file", $tmp, $!);
    my $text = "";
    while (<TMP>) {
        $text .= $_;
    }
    close TMP;
    $text =~ s/[\n\r]+$//;
    sleep(1);
    return $text;
}
