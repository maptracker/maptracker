#!/stf/sys64/bin/perl -w

BEGIN {
    # Allows usage of beta modules to be tested:
    my $prog = $0; my $dir = `pwd`;
    if ($prog =~ /working/ || $dir =~ /working/) {
	warn "\n\n *** This is Beta Software ***\n\n";
	require lib;
	import lib '/stf/biocgi/tilfordc/perllib';
    }
    require lib;
    import lib '/stf/biocgi/tilfordc/released/Bio/SeqIO';
}

my $VERSION = ' $Id$ ';

use strict;
use BMS::MapTracker::LoadHelper;
use BMS::Fetch3;
use BMS::Branch;
use BMS::CommonCAT;

my $args = &PARSEARGS
    ( -dir      => '/work5/tilfordc/rgd',
      -nocgi    => 1,
      -testmode => 1,
      -h        => undef,
      -help     => undef,
      -cache    => 100000,
      -clobber  => 0,
      -taxa     => '',
      -task     => 'catalog8',
      -errors   => 'RGD_Parse_Errors.txt',
      -log      => 'RGD_Parse_Log.txt',
      );

my $dir   = $args->{DIR};
my $cache = $args->{CACHE};
my $limit = $args->{LIMIT};
my $tm    = $args->{TESTMODE};
my $based = $args->{BASEDIR};

my $lh = BMS::MapTracker::LoadHelper->new
    ( -username => 'RGD',
      -basedir  => $based,
      -carpfile => '>>' . $args->{ERRORS},
      -testmode => $tm,
      -dumpsql  => $args->{DUMPSQL});
my $mt = $lh->tracker;
my $debug = BMS::Branch->new
    ( -skipkey => [ 'RESULTS','_typemap','DBUTIL','PARENT',
                    'BENCHMARKS','OPTS','OPT_THINGS'],
      -format => 'text' );

my $uncertain = {};

my $edgetags = {
    splice => [ 'Similarity type', '#META_VALUES#RGD splice variation' ],
};

my $classes = {
    gene => 'Gene',
    splice => 'Gene',
    allele => 'Gene',
    pseudogene => 'Pseudogene',
    'predicted-low' => 'Gene Model',
    'predicted-moderate' => 'Gene Model',
    'predicted-high' => 'Gene Model',
    'predicted-no evidence' => 'Gene Model',
};

my $species = {
    rat => 'Rattus norvegicus',
    human => 'Homo sapiens',
    mouse => 'Mus musculus',
};

my ($filedat, $doing, $logfh);


&read_genes;
&read_qtls;
&read_ests;
&read_history;
&read_sslps;
&read_reference;
&read_strains;

# IGNORE:
# MAP_DATA
# GENES_MGD
# GENE_REF_ID_2_PUBMED_ID

$lh->write();

sub read_strains {
    $doing = "Strains";
    &_open_file('STRAINS');
    while (my $record = &_next_record) {
        &process_strain($record);
    }
}

sub process_strain {
    my ($rec) = @_;
    my $rgdid = &vet_integer($rec, 'rgd_id', 'RGD:');
    return unless ($rgdid);

    $lh->set_class($rgdid, 'RGD');
    $lh->set_class($rgdid, 'Strain');
    $lh->set_taxa($rgdid, 'Rattus norvegicus' );

    if (my $sf = $rec->{full_name}) {
        $sf = "#FreeText#$sf";
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $sf,
                       -type  => 'is a shorter term for' );
        $lh->kill_edge( -name1 => $rgdid,
                        -type  => 'is a shorter term for' );
    }

    if (my $sym = $rec->{strain_symbol}) {
        $sym = "#Token#$sym";
        $lh->set_class($sym, 'Strain');
        $lh->set_taxa($sym, 'Rattus norvegicus' );
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $sym,
                       -type  => 'is a reliable alias for' );
        $lh->kill_edge( -name1 => $rgdid,
                        -type  => 'is a reliable alias for' );
    }

    #$debug->branch($rec);
}

sub read_reference {
    $doing = "Reference";
    &_open_file('REF_ID_2_PUBMED_ID');
    while (my $record = &_next_record) {
        &process_reference($record);
    }
}

sub process_reference {
    my ($rec) = @_;
    my $rgdid = &vet_integer($rec, 'ref_rgd_id', 'RGD:');
    my $pmid  = &vet_integer($rec, 'pubmed_id', 'PMID:');
    unless ($rgdid && $pmid) {
        return;
    }
    $lh->set_edge( -name1 => $rgdid,
                   -name2 => $pmid,
                   -type  => 'is a reliable alias for' );
    $lh->set_edge( -name2 => $rgdid,
                   -name1 => $pmid,
                   -type  => 'is a reliable alias for' );
    
    $lh->set_class($rgdid, 'publication');
    $lh->set_class($rgdid, 'RGD');
    $lh->set_class($pmid, 'pubmed', 'pubmed');
}

sub read_history {
    $doing = "History";
    &_open_file('HISTORY');
    while (my $record = &_next_record) {
        &process_history($record);
    }
}

sub process_history {
    my ($rec) = @_;
    my $oldid = &vet_integer($rec, 'old_rgd_id', 'RGD:');
    my $newid = &vet_integer($rec, 'new_rgd_id', 'RGD:');
    return unless ($oldid && $newid && $oldid ne $newid);
    $lh->set_class($oldid, 'RGD');
    $lh->set_class($oldid, 'DEPRECATED');
    $lh->set_edge( -name1 => $oldid,
                   -name2 => $newid,
                   -type  => 'is a deprecated entry for' );
}

sub read_ests {
    $doing = "ESTs";
    &_open_file('ESTS');
    while (my $record = &_next_record) {
        &process_est($record);
    }
}

sub process_est {
    my ($rec) = @_;
    my $rgdid = &vet_integer($rec, 'rgd_id', 'RGD:');
    $lh->set_class($rgdid, 'EST');
    $lh->set_class($rgdid, 'RGD');
    $lh->set_taxa($rgdid, 'Rattus norvegicus' );

    if (my $clone = &vet_integer($rec, 'clone_seq_rgd_id', 'RGD:')) {
        $lh->set_class($clone, 'Clone');
        $lh->set_class($clone, 'RGD');
        $lh->set_taxa($clone, 'Rattus norvegicus' );
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $clone,
                       -type  => 'was derived from' );
    }

    &assign_pubmed( $rec, $rgdid);

    if (my $name = $rec->{rgd_est_name}) {
        $lh->set_class($name, 'EST');
        $lh->set_taxa($name, 'Rattus norvegicus' );
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $name,
                       -type  => 'is a reliable alias for' );
        $lh->set_edge( -name2 => $rgdid,
                       -name1 => $name,
                       -type  => 'is a reliable alias for' );
    }

    if (my $gene = &vet_integer($rec, 'associated_gene_rgd_id', 'RGD:')) {
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $gene,
                       -type  => 'is a member of' );
        $lh->kill_edge( -name1 => $rgdid,
                        -type  => 'is a member of' );
    }
}

sub read_sslps {
    $doing = "SSLPs";
    &_open_file('SSLPS');
    while (my $record = &_next_record) {
        &process_sslp($record);
    }
}

sub process_sslp {
    my ($rec) = @_;
    my $rgdid = &vet_integer($rec, 'sslp_rgd_id', 'RGD:');
    return unless ($rgdid);

    my $spec = $species->{ $rec->{species} };
    unless ($spec) {
        &msg($rec, "Unknown species", $rec->{species});
        return;
    }
    $lh->set_class($rgdid, 'SSLP');
    $lh->set_class($rgdid, 'RGD');
    $lh->set_taxa($rgdid, $spec );

    &assign_pubmed( $rec, $rgdid);

    if (my $sym = $rec->{sslp_symbol}) {
        $sym = "#Token#$sym";
        $lh->set_class($sym, 'SSLP');
        $lh->set_taxa($sym, $spec );
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $sym,
                       -type  => 'is a reliable alias for' );
        $lh->kill_edge( -name1 => $rgdid,
                        -type  => 'is a reliable alias for' );
    }
    
    my @genes = &vet_integer($rec, 'associated_gene_rgd_id', 'RGD:');
    $lh->kill_edge( -name1 => $rgdid,
                    -type  => 'is a probe for' );
    foreach my $gene (@genes) {
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $gene,
                       -type  => 'is a probe for' );
    }
    
    if (my $clone = &vet_integer($rec, 'clone_seq_rgd_id', 'RGD:')) {
        $lh->set_class($clone, 'Clone');
        $lh->set_class($clone, 'RGD');
        $lh->set_taxa($clone, $spec);
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $clone,
                       -type  => 'was derived from' );
    }

    my @stss =  &vet_integer($rec, 'unists', 'UniSTS:');
    foreach my $sts (@stss) {
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $sts,
                       -type  => 'RELIABLE' );
        $lh->set_edge( -name2 => $rgdid,
                       -name1 => $sts,
                       -type  => 'RELIABLE' );        
        $lh->set_taxa($sts, $spec);
    }
    # die $debug->branch($rec) if ($rec->{unists});
}

sub read_qtls {
    $doing = "QTLs";
    &_open_file('QTLS');
    while (my $record = &_next_record) {
        &process_qtl($record);
    }
}

sub process_qtl {
    my ($rec) = @_;
    my $rgdid = &vet_integer($rec, 'qtl_rgd_id', 'RGD:');
    return unless ($rgdid);

    my $spec = $species->{ $rec->{species} };
    unless ($spec) {
        &msg($rec, "Unknown species", $rec->{species});
        return;
    }
    $lh->set_class($rgdid, 'QTL');
    $lh->set_class($rgdid, 'RGD');
    $lh->set_taxa($rgdid, $spec );

    if (my $sf = $rec->{qtl_name}) {
        $sf = "#FreeText#$sf";
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $sf,
                       -type  => 'is a shorter term for' );
        $lh->kill_edge( -name1 => $rgdid,
                        -type  => 'is a shorter term for' );
    }

    if (my $desc = $rec->{gene_desc}) {
        $desc = "#FreeText#$desc";
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $desc,
                       -type  => 'has comment' );
        $lh->kill_edge( -name1 => $rgdid,
                        -type  => 'has comment' );
    }
    &assign_pubmed( $rec, $rgdid);
    
    my @strains = &vet_integer($rec, 'strain_rgd_id', 'RGD:');
    foreach my $strain (@strains) {
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $strain,
                       -type  => 'was derived from' );
    }

    my @omims = &vet_integer($rec, 'omim_id', 'MIM:');
    foreach my $omim (@omims) {
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $omim,
                       -type  => 'RELIABLE' );
        $lh->set_edge( -name2 => $rgdid,
                       -name1 => $omim,
                       -type  => 'RELIABLE' );        
    }
    
    if (my $sym = $rec->{qtl_symbol}) {
        $sym = "#Token#$sym";
        $lh->set_class($sym, 'QTL');
        $lh->set_taxa($sym, $spec );
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $sym,
                       -type  => 'is a reliable alias for' );
        $lh->kill_edge( -name1 => $rgdid,
                        -type  => 'is a reliable alias for' );
    }
}

sub read_genes {
    $doing = "Genes";
    &_open_file('GENES');
    while (my $record = &_next_record) {
        &process_gene($record);
    }
}

sub process_gene {
    my ($rec) = @_;
    my $rgdid = &vet_integer($rec, 'gene_rgd_id', 'RGD:');
    my $class = $classes->{ $rec->{gene_type} || ''};
    unless ($class) {
        &msg($rec, "Unknown class", $rec->{gene_type});
        return;        
    }

    return unless ($rgdid);

    $lh->set_class($rgdid, $class);
    $lh->set_class($rgdid, 'RGD');
    $lh->set_taxa($rgdid, 'Rattus norvegicus');

    $lh->kill_edge( -name1 => $rgdid,
                    -type  => 'alias' );

    if (my $sf = $rec->{name}) {
        $sf = "#FreeText#$sf";
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $sf,
                       -type  => 'is a shorter term for' );
        $lh->kill_edge( -name1 => $rgdid,
                        -type  => 'is a shorter term for' );
    }

    if (my $desc = $rec->{gene_desc}) {
        $desc = "#FreeText#$desc";
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $desc,
                       -type  => 'has comment' );
        $lh->kill_edge( -name1 => $rgdid,
                        -type  => 'has comment' );
    }
    
    if (my $sym = $rec->{symbol}) {
        $sym = "#GeneSymbols#$sym";
        $lh->set_class($sym, 'genesymbol');
        $lh->set_taxa($sym, 'Rattus norvegicus');
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $sym,
                       -type  => 'is a reliable alias for' );
        $lh->kill_edge( -name1 => $rgdid,
                        -type  => 'is a reliable alias for' );
    }
    
    my @locids = &vet_integer($rec, 'entrez gene', 'LOC');
    foreach my $locid (@locids) {
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $locid,
                       -type  => 'RELIABLE' );
        $lh->set_edge( -name2 => $rgdid,
                       -name1 => $locid,
                       -type  => 'RELIABLE' );        
    }
    
    my @ensid = &vet_ensemble($rec, 'ensembl_id');
    foreach my $ensg (@ensid) {
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $ensg,
                       -type  => 'RELIABLE' );
        $lh->set_edge( -name2 => $rgdid,
                       -name1 => $ensg,
                       -type  => 'RELIABLE' );        
    }
    
    my @ugs = &vet_unigene($rec, 'unigene_id');
    foreach my $ugid (@ugs) {
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $ugid,
                       -type  => 'is a member of');
    }

    my @humids = &vet_integer($rec, 'human_homolog_rgd_id', 'RGD:');
    foreach my $humid (@humids) {
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $humid,
                       -type  => 'HOMOLOGUE' );
        $lh->set_class($humid, $class );
        $lh->set_class($humid, 'RGD' );
        $lh->set_taxa($humid, 'Homo sapiens');
        if (my $hsym = $rec->{human_homolog_symbol}) {
            $hsym = "#GeneSymbols#$hsym";
            $lh->set_edge( -name1 => $humid,
                           -name2 => $hsym,
                           -type  => 'reliable' );
            $lh->set_class($hsym, 'genesymbol');
            $lh->set_taxa($hsym, 'Homo sapiens');
            
        }
        if (my $hdesc = $rec->{human_homolog_name}) {
            $hdesc = "#FreeText#$hdesc";
            $lh->set_edge( -name1 => $humid,
                           -name2 => $hdesc,
                           -type  => 'is a shorter term for' );
            $lh->set_class($hdesc, 'text');
            
        }
    }

    my @musids = &vet_integer($rec, 'mouse_homolog_rgd_id', 'RGD:');
    foreach my $musid (@musids) {
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $musid,
                       -type  => 'HOMOLOGUE' );
        $lh->set_class($musid, $class );
        $lh->set_class($musid, 'RGD' );
        $lh->set_taxa($musid, 'Mus musculus');
        if (my $hsym = $rec->{mouse_homolog_symbol}) {
            $hsym = "#GeneSymbols#$hsym";
            $lh->set_edge( -name1 => $musid,
                           -name2 => $hsym,
                           -type  => 'reliable' );
            $lh->set_class($hsym, 'genesymbol');
            $lh->set_taxa($hsym, 'Mus musculus');
            
        }
        if (my $hdesc = $rec->{mouse_homolog_name}) {
            $hdesc = "#FreeText#$hdesc";
            $lh->set_edge( -name1 => $musid,
                           -name2 => $hdesc,
                           -type  => 'is a shorter term for' );
            $lh->set_class($hdesc, 'text');
            
        }
    }

    &assign_pubmed( $rec, $rgdid);
    
    my @variants = &vet_integer($rec, 'splice_rgd_id', 'RGD:');
    foreach my $varid (@variants) {
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $varid,
                       -type  => 'is similar to' );
    }

    my @qtls = &vet_integer($rec, 'qtl_rgd_id', 'RGD:');
    foreach my $qtl (@qtls) {
        $lh->set_edge( -name1 => $rgdid,
                       -name2 => $qtl,
                       -type  => 'is fully contained by' );
    }

    # alias_value
    # gdb id
    # mgd_id
    # qtl_rgd_id

    $lh->write_threshold_quick( $cache );

}

sub assign_pubmed {
    my ($rec, $rgdid) = @_;
    foreach my $tag ('curated_ref','uncurated') {
        my @pmids = &vet_integer($rec, $tag . '_pubmed_id', 'PMID:');
        foreach my $pmid (@pmids) {
            $lh->set_class($pmid, 'pubmed', 'pubmed');
            $lh->set_edge( -name1 => $rgdid,
                           -name2 => $pmid,
                           -type  => 'is referenced in' );
        }
    }
    foreach my $pubid (&vet_integer($rec, 'curated_ref_rgd_id', 'RGD:')) {
            $lh->set_class($pubid, 'publication');
            $lh->set_class($pubid, 'RGD');
            $lh->set_edge( -name1 => $rgdid,
                           -name2 => $pubid,
                           -type  => 'is referenced in' );        
    }
}

sub vet_integer {
    my ($rec, $key, $prefix) = @_;
    my $txt = $rec->{$key};
    return wantarray ? () : "" unless ($txt);
    $prefix ||= "";
    my @reqs = wantarray ? split(",", $txt) : ($txt);
    my @retval;
    foreach my $req (@reqs) {
        if ($req =~ /^\d+$/) {
            push @retval, "$prefix$req";
        } else {
            &msg($rec, "Malformed $key $prefix ", "'$req'");
        }
    }
    return wantarray ? @retval : $retval[0];    
}

sub vet_unigene {
    my ($rec, $key) = @_;
    my $txt = $rec->{$key};
    return wantarray ? () : "" unless ($txt);
    my @reqs = wantarray ? split(",", $txt) : ($txt);
    my @retval;
    foreach my $req (@reqs) {
        if ($req =~ /^[A-Z][a-z]\.\d+$/) {
            push @retval, $req;
        } else {
            &msg($rec, 'Malformed UniGene ID', $req);
        }
    }
    return wantarray ? @retval : $retval[0];    
}

sub vet_ensemble {
    my ($rec, $key) = @_;
    my $txt = $rec->{$key};
    return wantarray ? () : "" unless ($txt);
    my @reqs = wantarray ? split(",", $txt) : ($txt);
    my @retval;
    foreach my $req (@reqs) {
        if ($req =~ /^ENSRNOG\d+$/) {
            push @retval, $req;
        } else {
            &msg($rec, 'Malformed Ensembl ID', $req);
        }
    }
    return wantarray ? @retval : $retval[0];    
}

sub _open_file {
    my ($file, $cols, $isclust) = @_;
    unless ($file =~ /^\//) {
        $file = "$dir/$file";
    }
    unless (-e $file) {
        die "I could not locate '$file'\n  ";
    }

    &msg({  }, 'Reading File', $file);
    my ($fh, $isgz, $count);
    if ($file =~ /\.gz$/) {
	open(GZIP, "gunzip -c $file|") || 
            die "Failed to establish gunzip pipe for '$file' ($file)";
        $fh = *GZIP;
        $isgz = 1;
        $count = `gunzip -c $file | wc -l`;
    } else {
	open(FILE, "<$file") || die "Failed to open '$file' ($file):\n  $!\n  ";
        $fh = *FILE;
        $count = `wc -l $file`;
    }
    if ($count =~ /(\d+)/) {
        $count = $1;
    } else {
        $count = 0;
    }
    unless ($cols) {
        my $line = <$fh>;
        chomp($line);
        $cols = [ split("\t", lc($line)) ];
    }

    $filedat = {
        fh         => $fh,
        file       => $file,
        cols       => $cols,
        gz         => $isgz,
        num        => 0,
        reject     => 0,
        clust      => $isclust,
        count      => $count,
        start      => time,
        clustcount => 0,
    };
    
}

sub _next_record {
    # $is_single is true if each row is processed seperately
    my $is_single = !$filedat->{clust};
    if ($limit && $is_single && 
        $filedat->{num} - $filedat->{reject} >= $limit) {
        # 1. The user has requested a limit
        # 2. This file is not having rows clustered
        # 3. We have analyzed a limits worth of data
        # Close file and return undef to indicate termination
        return undef;
    }
    my $fh   = $filedat->{fh};
    my @cols = @{$filedat->{cols}};
    my %hash;
    while (1) {
        my $line = <$fh>;
        return undef unless ($line);
        # GRR. Some files have comments
        next if ($line =~ /^\#/);
        chomp $line;
        my @row = split("\t", $line);
        for my $i (0..$#cols) {
            $row[$i] ||= '';
            my $cell = $row[$i] eq '-' ? '' : $row[$i];
            $hash{ $cols[$i] } = $cell;
        }
        
        $filedat->{num}++;
        last;
    }

    my @ids;

    my $ops = $lh->operations;
    if ($ops >= $cache) {
        $filedat->{rows} += $ops;
        my $lines = $filedat->{num};
        my $txt = sprintf("%4.3fk Lines, %5.3fk DB Rows",
                          $lines/1000, $filedat->{rows}/1000);
        if (my $count = $filedat->{count}) {
            my $frac = $lines / $count;
            my $elapsed = (time - $filedat->{start});
            my $remain = $elapsed * (1 - $frac) / $frac;
            $txt .= sprintf(", %2.2f%% [%.2f hr remain]", 
                            $frac * 100, $remain / 3600);
        }
        # Show the message if there is only a single subfork, or if this is
        # the first subfork
        &msg( {}, "Progress", $txt);
        $lh->write;
        # processes steadily grow over time, particularly 'info'
        # I suspect it may be cached taxa entries
        $mt->clear_cache('seqnames', 'taxa');
    }
    return \%hash;
}

sub _close_file {
    return unless ($filedat);
    my $fh = $filedat->{fh};
    close $fh;
    undef $filedat;
}

sub msg {
    my ($obj, $msg, $detail) = @_;
    my $locid = $obj->{gene_rgd_id} || '++';
    $detail ||= "";
    my $txt = sprintf(" -- %10s : %10s : %s : %s\n", $locid, $doing || 'MAIN', 
                      $msg, substr($detail,0,75));
    warn $txt;
    print $logfh $txt if ($logfh && $txt);
    return if ($locid eq '++');
    $uncertain->{$msg} ||= {};
    $uncertain->{$msg}{$detail}++;
}

