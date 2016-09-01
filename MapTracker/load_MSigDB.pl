#!/stf/biobin/perl -w

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
use BMS::BmsArgumentParser;
use BMS::MapTracker::LoadHelper;
use BMS::MapTracker::AccessDenorm;
use BMS::FriendlySAX;
use BMS::ForkCritter;
use BMS::TableReader;

# http://www.broad.mit.edu/gsea/downloads.jsp#msigdb
# ftp://gseaftp.broad.mit.edu/pub/gsea/msigdb_v2.5.xml


my $MDIR = "/work5/tilfordc/WGET/MSigDB";
my $MSV  = "5.1";

my $args = BMS::BmsArgumentParser->new
    ( -nocgi     => 1,
      # -file      => '/work5/tilfordc/WGET/msigdb_v2.5.xml',
      -xmlfile   => "$MDIR/msigdb_v${MSV}.xml",
      -connect   => "$MDIR/msigdb.v${MSV}.entrez.gmt",
      -progress  => 180,
      -testmode  => 1,
      -verbose   => 1,
      -cache     => 5000,
      -msg       => "MSigDB_Messages.txt",
      -basedir   => 'MSigDB',
      -fork      => 1,
      -load      => 0,
      -ageall    => '28 Apr 2016',
      -paramalias => {
          testmode   => [qw(tm)],
          basedir    => [qw(loaddir)],
          xxxx => [qw()],
          xxxx => [qw()],
          xxxx => [qw()],
          xxxx => [qw()],
          xxxx => [qw()],
      });

$args->ignore_error("User limit ");
$args->shell_coloring();

my $categories = {
    h  => "Hallmark",
    c1 => "Chromosome Location",
    c2 => "Curated Gene Sets",
    c3 => "Motif Gene Sets",
    c4 => "Computational Gene Sets",
    c5 => "Gene Ontology Gene Sets",
    c6 => "Oncogenic Signatures",
    c7 => "Immunologic Signatures",
};

my %sccs = ( CC => "GO Cellular Component",
             MF => "GO Molecular Function",
             BP => "GO Biological Process", );

# This was assigned to 2251 Immunologic sets:
my %weirdSystematicName = map { $_ => 1 } qw(MNA);
# I guess 'MNA' = 'M# NotApplicable'. The others are M997, M7029, etc

my @fooStack;

my %xmlEsc = ( apos => "'", );

my $slashes = '///';
my $limit   = $args->{LIMIT};
my $vb      = $args->{VERBOSE};
my $tm      = $args->{TESTMODE};
my $cache   = $args->{CACHE};
my $prog    = $args->{PROGRESS};
my $basedir = $args->val('basedir');
my $file    = $args->val(qw(xmlfile));
my $taxOnly = $args->val(qw(taxonly));
my $doLoad  = $args->{LOAD};
my $noTree  = $args->{NOTREE};
my $mfile   = $args->{MSG};
my $testfile = $args->val(qw(testfile));
my $symTax  = "Homo sapiens";
my $mtns    = '#MSigDB#';
my $prfx    = 'MSigDB:';
my $ft      = '#FreeText#';

unlink($testfile) if ($testfile);

my %okTaxa = map { $_ => 1 } ('Escherichia coli K12');

my $debug = BMS::Branch->new
    ( -noredundancy => 1,
      -format       => 'text',
      -skipkey => [ 'PARENT' ],
      -maxhash  => 10,
      -maxarray => 10,);

my ($fc, $lh, $mt, $ad, %announced, %symbols, @globalMsg, %s2l, %l2t, %badIds);

my %files = ( Messages => $mfile,
              TestOutput => $testfile );

foreach my $of (values %files) { unlink($of) if ($of && -e $of); }

# die &purge_old_links();

if ($taxOnly) {
    &assign_members();
} elsif ($args->{CHECK}) {
    &check_all_symbols();
} elsif ($args->{SYSTEMATIC}) {
    &check_systematic();
} elsif (my $treef = $args->{TREE}) {
    &parse_treefile($treef);
} else {
    &parse();
}

if ($vb) {
    while (my ($name, $path) = each %files) {
        next unless ($path && -s $path);
        $args->msg("[>]", sprintf("  %20s :  %s", $name, $path));
    }
}

if ($basedir && !($doLoad || $tm)) {
    $args->msg("Data are ready for loading in:", $basedir);
}

$args->msg("Finished",`date`);

sub purge_old_links {
    $args->msg("Purging all connections to old IDs");
    my $lh     = &init_lh(undef, $testfile);
    foreach my $gc ('Biological process','Cellular component','Molecular Function') {
        my $mtid =  $mtns . $prfx . $gc;
        $lh->kill_taxa($mtid);
        $lh->kill_class($mtid);
        $lh->kill_edge( -name1 => $mtid,
                        -type  => 'is a child of' );
    }
    $lh->write(); return;
    for my $c (1..5) {
        my $mtid = sprintf("%s%sc%d%%", $mtns, $prfx, $c);
        $lh->kill_taxa($mtid);
        $lh->kill_class($mtid);
        $lh->kill_edge( -name1 => $mtid,
                        -type  => 'is a child of' );
        $lh->kill_edge( -name1 => $mtid,
                        -type  => 'is a parent of' );
        $lh->kill_edge( -name1 => $mtid,
                        -type  => 'is a member of' );
        $lh->kill_edge( -name1 => $mtid,
                        -type  => 'is a shorter term for' );
        $lh->kill_edge( -name1 => $mtid,
                        -type  => 'is attributed to' );
        $lh->kill_edge( -name1 => $mtid,
                        -type  => 'is reliably aliased by');
    }
    $lh->write();
}

sub check_all_symbols {
    open(FILE, "<$file") || die "Failed to read $file:\n  $!\n  ";
    while (<FILE>) {
        my $num = 0;
        if (/MEMBERS_SYMBOLIZED=\"([^\"]+)\"/) {
            my $mems = $1;
            while (my ($xml, $esc) = each %xmlEsc) {
                $mems =~ s/\&$xml\;/$esc/g;
            }
            foreach my $s1 (split(/\s*\,\s*/, $mems)) {
                next unless ($s1);
                foreach my $sym (split(/[\s_]+\/+[\s_]+/, $s1)) {
                    $sym =~ s/[_\/\s]+$//;
                    $sym =~ s/^[_\/\s]+//;
                    next unless ($sym);
                    $num++ unless ($symbols{$sym}++);
                }
            }
        }
        last if ($limit && $num >= $limit);
    }
    close FILE;

    my @syms = sort keys %symbols;

    push @globalMsg,(["Total symbols found", $#syms + 1],
                     );
    $fc ||= BMS::ForkCritter->new
        ( -inputtype   => 'array',
          -limit       => $limit,
          -progress    => $prog,
          -verbose     => $vb );
    $fc->reset();

    $mfile = "Symbol_Messages.txt";
    $fc->output_file('MSG',">>$mfile") if ($mfile);
    $fc->output_file('TESTFILE', ">>$testfile") if ($testfile);
    
    $fc->method( \&sym2loc );
    $fc->init_method( \&initialize );
    $fc->finish_method( \&finalize );
    $fc->input(\@syms);

    my $failed = $fc->execute( $args->{FORK} );
    $args->death("$failed processes did not execute properly") if ($failed);
    $args->msg("Finished forking symbols check", "");
    
}

sub assign_members {
    my $cfile  = $args->val(qw(connect));
    my $lh     = &init_lh(undef, $testfile);
    my $ad     = &init_ad();
    $args->msg("Reading connections from $cfile");
    my $tr     = BMS::TableReader->new();
    $tr->has_header( 0 );
    $tr->limit( $limit );
    $tr->format('tsv');
    $tr->input($cfile);
    my @sheets = $tr->each_sheet;
    my %seen;
    foreach my $sheet (@sheets) {
        $tr->select_sheet($sheet);
        while (my $row = $tr->next_clean_row()) {
            my $stnd = &stnd_chr( shift @{$row} );
            if (length($stnd) > 94) { 
                $args->msg("ID over 94 characters", $stnd);
                next;
            }

            my $mtid = $mtns . $prfx . $stnd;
            my $url  = shift @{$row};
            $lh->kill_edge( -name1 => $mtid,
                            -type  => 'is attributed to' )
                unless ($taxOnly);
            $lh->kill_taxa( $mtid );
            my @errs;
            my %locs;
            foreach my $gid (@{$row}) {
                if ($gid =~ /^\d+$/) {
                    my $llid = "LOC$gid";
                    $locs{$llid}++;
                    $lh->set_edge( -name1 => $mtid,
                                   -name2 => $llid,
                                   -type  => 'is attributed to' )
                        unless ($taxOnly);
                } else {
                    push @errs, $gid;
                }
            }
            my @allLL = sort keys %locs;
            my @taxa  = $ad->convert
                ( -id => \@allLL, -ns1 => 'LL', -ns2 => 'TAX' );
            my %u = map { $_ => 1 } @taxa;
            @taxa = keys %u;
            map { $lh->set_taxa( $mtid, $_ ) } @taxa;
            $args->msg("[!]", "Multiple taxae for list $stnd", @taxa)
                if ($#taxa > 0);
            my $num   = $#allLL + 1;
            $seen{$stnd} = $num;
            $args->msg("[!]", "Malformed locus IDs", $stnd, @errs)
                unless ($#errs == -1);
        }
    }
    $lh->write();
    $args->msg("Finished setting attributions");
    return \%seen;
}

sub check_systematic {
    $args->msg("[?]","Not checking systematic names - using Entrez ID file");
    return;


    die "This method has not been checked for using the new IDs!!";

    $args->msg("Identifying IDs that are not uniquely assigned");
    $fc ||= BMS::ForkCritter->new
        ( -inputtype   => 'sax',
          -inputargs   => [ -tag => 'GENESET' ],
          -limit       => $limit,
          -progress    => $prog,
          -verbose     => $vb );

    $fc->reset();

    my $checkFile = "MSigDB-CheckID.tsv";
    $fc->output_file('CHECK',$checkFile);
    $fc->output_file('TESTFILE', ">>$testfile") if ($testfile);

    $fc->method( \&quick_record );
    $fc->input($file);
    my $failed = $fc->execute( $args->{FORK} );
    $args->death("$failed processes did not execute properly") if ($failed);
    $args->msg("Finished forking systematic IDs", "");

    open(FILE, "<$checkFile") || $args->death
        ("Failed to read file", $checkFile, $!);
    my %ids;
    while (<FILE>) {
        s/[\n\r]+$//;
        my ($id, $stnd) = split(/\t/);
        push @{$ids{$id}}, $stnd;
    }
    
    my $badText = "";

    foreach my $id (sort keys %ids) {
        my @stnd = @{$ids{$id}};
        my $num  = $#stnd + 1;
        next if ($num <= 1);
        $badIds{$id} = $num;
        $badText .= join("\t", $id, $num, join(' + ',@stnd))."\n";
    }

    my @uniq = keys %badIds;
    return if ($#uniq == -1);

    my $lh = &init_lh(undef, $testfile);
    foreach my $id (@uniq) {
        my $mtid = $mtns . $prfx . $id;
        $lh->kill_edge( -name1 => $mtid,
                        -type  => 'is attributed to' );
        $lh->kill_edge( -name1 => $mtid,
                        -type  => 'is a child of' );
        $lh->set_class($mtid, "Suspicious", "tilfordc");
    }
    $lh->write();
    $lh = undef;
    
    $args->msg("[!!]","Will ignore ".scalar(@uniq)." bad IDs");
    
    my $bf = "BadMSigDBids.tsv";
    if (open(FILE, ">$bf")) {
        print FILE join("\t", "SystematicName", "NameCount", "StandardNames")."\n";
        print FILE $badText;
        close FILE;
        $files{"Bad IDs"} = $bf;
    } else {
        $args->err("Failed to write bad ids", $bf, $!);
    }
    $args->msg("Finished deprecating bad IDs", "");
}

sub quick_record {
    my ($record) = @_;
    # All information is stored in XML attributes
    my $data = $record->{ATTR};
    my $id   = $data->{SYSTEMATIC_NAME};
    my $stnd = $data->{STANDARD_NAME};
    $fc->write_output('CHECK', "$id\t$stnd\n");
}

sub parse {
    &check_systematic();
    $fc ||= BMS::ForkCritter->new
        ( -inputtype   => 'sax',
          -inputargs   => [ -tag => 'GENESET' ],
          -limit       => $limit,
          -progress    => $prog,
          -verbose     => $vb );
    $fc->reset();

    my $treeFile = "MSigDB-Tree.tsv";
    my $idFile   = "MSigDB-IDs.tsv";
    unlink($treeFile);
    $args->death("Failed to remove old treefile", $treeFile) if (-e $treeFile);
    $fc->output_file('MSG', ">>$mfile") if ($mfile);
    $fc->output_file('TESTFILE', ">>$testfile") if ($testfile);
    $fc->output_file('TREE', $treeFile );
    $fc->output_file('IDS',  $idFile );

    $fc->method( \&parse_record );
    $fc->init_method( \&initialize );
    $fc->finish_method( \&finalize );
    $fc->input($file);

    $args->msg("Parsing $file");


    my $failed = $fc->execute( $args->{FORK} );
    $args->death("$failed processes did not execute properly") if ($failed);
    $args->msg("Finished forking primary data loop", "");
    &parse_treefile($treeFile);
    my $entrezIDs = &assign_members();
    &parse_ids($idFile, $entrezIDs);
}

sub parse_ids {
    my ($idFile, $entrezIDs) = @_;
    $files{"IDFile"} = $idFile;
    open(IDFILE, "<$idFile") || $args->death
        ("Failed to read ID file", $idFile, $!);
    my %ids;
    while (<IDFILE>) {
        s/[\n\r]+$//;
        my ($id, $par, $desc) = split(/\t/);
        $ids{$id}{id}     ||= $id;
        $ids{$id}{desc}   ||= $desc;
        $ids{$id}{parent} ||= $par;
        $ids{$id}{XML}      = 1;
    }
    close IDFILE;
    while (my ($id, $num) = each %{$entrezIDs}) {
        $ids{$id}{id}     ||= $id;
        $ids{$id}{num}    ||= $num;
        $ids{$id}{GMT}      = 1;
        
    }
    open(IDFILE, ">$idFile") || $args->death
        ("Failed to write ID file", $idFile, $!);
    print IDFILE join("\t", qw(ID Loci Parent Problem Description))."\n";
    my @cols = qw(id num parent foo desc);
    my @src  = qw(XML GMT);
    foreach my $dat (sort { $a->{id} cmp $b->{id} } values %ids) {
        my @row = map { $dat->{$_} } @cols;
        my @probs;
        map { push @probs, "NotIn$_" unless ($dat->{$_}) } @src;
        $row[3] = join(" ", @probs);
        print IDFILE join("\t", map { defined $_ ? $_ : "" } @row)."\n";
    }
    close IDFILE;
    $args->msg("ID Sanity Check performed", $idFile);
}

sub stnd_chr {
    my $stnd = shift;
    my ($chr, $band, $sBnd);
    if (!$stnd) {
    } elsif ($stnd =~ /^chr([xy0-9]+)(p|q)?(.+?)$/i) {
        $chr   = "Chr".uc($1);
        $band  = lc($2 || "");
        $sBnd  = $3 || "";
        $stnd = join('', $chr, $band, $sBnd);
    }
    return wantarray ? ($stnd, $chr, $band, $sBnd) : $stnd;
}

sub parse_treefile {
    my $file = shift;
    return unless ($file && -s $file);
    return if ($noTree);
    $args->msg("Parsing Tree file");
    open(FILE, "<$file") || $args->death("Failed to read treefile", $file, $!);
    $files{"TreeFile"} = $file;
    my %nodes;
    my %msigNames;
    while (<FILE>) {
        s/[\n\r]+$//;
        my @tree = split(/\t/);
        # map { $_ = "${mtns}$prfx$_" } @tree;
        my $mtid = shift @tree;
        my $name = shift @tree;
        unless ($name) {
            $args->msg("[!!]", "No name for $mtid");
            next;
        }
        # $msigNames{$name} = $mtid;
        unshift @tree, $mtid;
        for my $t (1..$#tree) {
            my ($kid, $par) = ($tree[$t-1], $tree[$t]);
            my $kn = $nodes{$kid} ||= { id => $kid };
            $kn->{par} = $par;
            my $pn = $nodes{$par} ||= { id => $par };
            $pn->{kids}{$kid} ||= $kn;
        }
        $nodes{$mtid}{name}   = $name;
        $nodes{$mtid}{isleaf} = 1;
    }
    close FILE;
   #  print $args->branch($nodes{ChrYp11}); exit;
    while (my ($name, $mtid) = each %msigNames) {
        # An MSig node is internal in the tree
        next unless (exists $nodes{$name});
        my $kid = $nodes{$mtid};
        while (my ($gk, $hash) = each %{$nodes{$name}{kids}}) {
            $kid->{kids}{$gk} ||= $hash;
            $nodes{ $gk }{par}  = $mtid;
        }
        delete $nodes{ $kid->{par} }{kids}{$name};
        delete $nodes{$name};
    }
    my @roots;
    foreach my $node (values %nodes) {
        push @roots, $node unless (exists $node->{par} && $node->{par});
    }
    if ($#roots == -1) {
        $args->warn("No root nodes found in $file");
    } else {
        # warn $args->branch( \@roots );
    }
    my $lh = &init_lh(undef, $testfile);
    my %arms = ( p => 'short', q => 'long' );
    foreach my $node (values %nodes) {
        my $nid  = $node->{id};
        next unless ($nid);
        my $mtid = join('', $mtns, $prfx, $nid);
        map { $lh->set_class($mtid, $_) } qw(accession msigdb);
        my $par  = $node->{par};
        unless ($par) {
            $lh->kill_edge( -name2 => $mtid,
                            -type  => 'is a child of' );
            $lh->kill_edge( -name2 => $mtid,
                            -type  => 'is a parent of' );
            next;
        }
        my $pid = "${mtns}$prfx$par";
        $lh->set_edge( -name1 => $mtid,
                       -name2 => $pid,
                       -type  => 'is a child of' );
        $lh->kill_edge( -name1 => $mtid,
                        -type  => 'is a parent of' );
        $lh->kill_edge( -name1 => $mtid,
                        -type  => 'is a child of' );
        if ($nid =~ /^Chr(.+)/) {
            my $bit = $1;
            my ($desc, $inon, $fail);
            if ($bit =~ /^(\d{1,2}|x|y|z|w)(.*?)$/i) {
                my ($chr, $bit2) = ($1, $2 || "");
                $desc = "Chromosome ". uc($chr);
                $inon = "on";
                if ($bit2 =~ /^(p|q)(.*)$/) {
                    my ($arm, $bit3) = ($arms{$1}, $2 || "");
                    if ($arm) {
                        $desc = "$arm arm of $desc";
                        if ($bit3 =~ /^\d+$/) {
                            $desc = "region $bit3 of the $desc";
                            $inon = "in";
                        } else {
                            $fail = "Band '$bit3'" if ($bit3);
                        }
                    } else {
                        $fail = "Arm token '$1'" if ($1);
                    }
                } else {
                    $fail = "Arm '$bit2'" if ($bit2);
                }
            } else {
                $fail = "Chromosome '$bit'";
            }
            if ($fail) {
                &msg("Failed to parse chromosome band name", $nid, $fail);
            } elsif ($desc) {
                $desc = "Genes $inon $desc";
                # warn "$nid = $desc\n";
                $lh->set_edge( -name1 => $mtid,
                               -name2 => $ft.$desc,
                               -type  => 'is a shorter term for' );
                $lh->kill_edge( -name1 => $mtid,
                                -type  => 'is a shorter term for' );
                $lh->kill_taxa( $mtid );
                $lh->set_taxa( $mtid, 'Homo sapiens' );
            }
        }
    }

    # Set some general connections for high level categories:
    my $container = "MSigDB Gene Sets";
    $lh->kill_edge( -name2 => $container,
                    -type  => 'is a member of' );
    while (my ($cnum, $catName) = each %{$categories}) {
        my $mtid = $mtns . $prfx . "Category " . $catName;
        $lh->set_edge( -name1 => $mtid,
                       -name2 => $container,
                       -type  => 'is a member of' );
        my $desc = "MSigDB ontology category $cnum : $catName";
        $lh->kill_edge( -name1 => $mtid,
                        -type  => 'is a shorter term for' );
        $lh->set_edge( -name1 => $mtid,
                       -name2 => $ft.$desc,
                       -type  => 'is a shorter term for' );
    }

    $lh->write();
    $lh = undef;
    $args->msg("Finished processing tree data","");
}

sub parse_record {
    my ($record) = @_;
    # die BMS::FriendlySAX::node_to_text( $record, undef, 'expand' );
    # All information is stored in XML attributes
    my $data  = $record->{ATTR};
    my $sysn  = $data->{SYSTEMATIC_NAME};
    my $stnd  = $data->{STANDARD_NAME};
    my $chip  = $data->{CHIP};
    my $cat   = lc($data->{CATEGORY_CODE} || "");
    my $id    = $stnd;

    # We are completely ignoring MEMBERS  / MEMBERS_SYMBOLIZED 
    # They are malformed beyond recognition in many cases

    my ($db, $df) = map { 
        $data->{$_} || '';
    } qw(DESCRIPTION_BRIEF DESCRIPTION_FULL);
    my @reliable;
    my @parents;
    my $catName = $categories->{$cat};
    unless($catName) {
        &msg("Unknown category", $cat);
        return;
    }
    if (my $bad = $badIds{$sysn}) {
        $args->msg("[!]","Ignoring $sysn with $bad multiple assignments");
        return;
    }
    my $forceTaxa;
    if ($cat eq 'c1') {
        # Chromosomal assignment
        $forceTaxa = "Homo sapiens";
        my ($schr, $chr, $band, $sBnd) = &stnd_chr($stnd);
        if ($chr) {
            $id  = $stnd = $schr;
            push @parents, $chr.$band if ($sBnd);
            push @parents, $chr       if ($band);
        } else {
            &msg("Failed to find chromosome information", $id, $db);
            return;
        }
    } elsif ($cat eq 'c5') {
        # GO Terms
        if ($db =~ /^Genes annotated by the GO term (GO\:\d{7})\./) {
            push @reliable, $ft.$stnd;
            $stnd = $1;
            $db   = $1;
            my ($par2, $pdesc);
            if (my $tag = $data->{TAGS}) {
                $par2 = $tag;
            } elsif (my $scc = $data->{SUB_CATEGORY_CODE}) {
                if ($par2 = $sccs{$scc}) {
                    $pdesc = "$par2 Ontology Tree";
                } else {
                    &msg("Unrecognized GO sub category", $id, $scc);
                }
            } else {
                &msg("Failed to find GO category", $id, $db);
                $par2 = "Unclassified GO";
                $pdesc = "";
                # return;
            }
            if ($par2) {
                push @parents, $par2;
                if ($pdesc) {
                    my $pmtid = $mtns . $prfx . $par2;
                    $lh->set_edge( -name1 => $pmtid,
                                   -name2 => $ft.$pdesc,
                                   -type  => 'is a shorter term for' );
                }
            } else {
                &msg("Failed to find GO category", $id, $db);
            }
            push @parents, 'Gene Ontology';
        } else {
            &msg("Failed to find GO ID", $id, $db);
            return;
        }
    }

    push @parents, "Category $catName";

    $fc->write_output("IDS", join("\t", $id, $parents[0], $db || $df)."\n");
    if ($cat eq 'c1') {
        # We will handle the tree all at once after forking
        $fc->write_output("TREE", join("\t", $id, $stnd, @parents)."\n");
        @parents = ();
    }
    unless ($id) {
        &msg("No ID for geneset");
        return;
    }

    &msg("Short ID", $id) if
        (length($id) <= 10 && $id !~ /^(MORF|GCM|GNF2|module)_/i &&
         $id !~ /\$/ && $id !~ /^Chr/);

    push @reliable, $mtns . $sysn unless ($weirdSystematicName{$sysn});
    my @descText = ($db, $df);
    @parents     = () if ($noTree);

    unshift @parents, $id;

    for my $p (0..$#parents) {
        my $node = $parents[$p] = $mtns . $prfx . $parents[$p];
        map { $lh->set_class($node, $_) } qw(accession msigdb);
        $lh->kill_edge( -name1 => $node,
                        -type  => 'is a child of' );
    }


    my $mtid = $parents[0];
    for my $i (1..$#parents) {
        $lh->set_edge( -name1 => $parents[$i-1],
                       -name2 => $parents[$i],
                       -type  => 'is a child of' );
    }
    $lh->kill_class($mtid, "Suspicious", "tilfordc");


    foreach my $desc (@descText) {
        next if (!$desc || $desc =~ /^n\/?a$/i);
        if ($desc =~ /^XX/) {
            # someone is just pasting entire SwissProt headers
            my @bits;
            foreach my $line (split(/\s*<br>\s*/, $desc)) {
                if ($line =~ /^(SC|SF|FF)\s+(.+)/) {
                    push @bits, $2;
                }
            }
            if ($#bits == -1) {
                &msg("Failed to parse UniProt description",
                     $id, substr($desc,0,100));
                next;
            } else {
                $desc = join(' ', @bits);
                $desc =~ s/\[\d+\]/ /g;
                $desc =~ s/\s{2,}/ /g;
            }
        }
        $desc =~ s/\.$//; # Remove terminal period
        $desc =~ s/\<[^\>]+\>//g; # Remove HTML tags
        $desc =~ s/\P{IsASCII}//g; # Remove non-ASCII
        if (length($desc) > 500) {
            #&msg("Skipping long description", $id, substr($desc,0,100));
            #next;
            $desc = substr($desc, 0, 500).' ...';
        }
        $lh->set_edge( -name1 => $mtid,
                       -name2 => $ft.$desc,
                       -type  => 'is a shorter term for' );
    }
    map { $lh->set_edge( -name1 => $mtid,
                         -name2 => $_,
                         -type  => 'is reliably aliased by' ) } @reliable;
    $lh->write_threshold_quick($cache);
}

sub check_symbols {
    my ($syms, $ns) = @_;
    return unless( $syms );
    my @splits = ($syms =~ /\Q$slashes\E/) ?
        split(/\Q$slashes\E/, $syms) : ($syms);
    my @singles;
    foreach my $sp (@splits) {
        push @singles, split(/\,/, $sp);
    }
    # Remove leading junk
    map { s/^[\s\\\/\-_]+//;
          s/[\s\\\/\-_]+$// } @singles;
    my $taxHash = &sym2loc( \@singles, $ns );
    return wantarray ? ($taxHash, $#singles +1) : $taxHash;
}

sub sym2loc {
    my %rv;
    my $list;
    my $ns = 'SYM';
    if (ref($_[0])) {
        ($list, $ns) = @_;
    } else {
        $list = [ @_ ];
    }
    foreach my $req (@{$list}) {
        next unless ($req);
        my $sym = uc($req);
        unless ($s2l{$sym}) {
            my ($rows, $gns, $cSeq);
            if ($sym =~ /^\d+$/) {
                $rows = [];
                &msg("Pure integer symbol", $sym);
            } elsif ($sym =~ /^(NULL)$/) {
                $rows = [];
                &msg("Bogus symbol name", $sym);
            } elsif ($sym =~ /^LOC\d+$/) {
                $rows = [[ $sym, 1]];
            } else {
                ($gns, $cSeq) = $ad->guess_namespace($req, $ns, 'class');
                $gns = $ad->guess_namespace_careful($req) if (!$gns || !$cSeq);
                &msg("Treating input as non-symbol", $req, $gns)
                    unless ($gns eq 'SYM');
                my $adNs = $gns eq 'UNK' ? undef : $gns;
                $rows = $ad->convert
                    ( -id => $sym, -ns1 => $adNs, -ns2 => 'll', -nonull => 1,
                      -nullscore => -1, -ignorecase => 1,
                      -cols => ['term_out','matched'] );
                if ($#{$rows} == -1) {
                    my $list = $mt->get_edge_dump
                        ( -name => "#GeneSymbols#$req",
                          -type      => 'is reliably aliased by',
                          -orient    => 1,
                          -return    => 'name array' );
                    my @sec;
                    foreach my $dat (@{$list}) {
                        my $id = $dat->[0];
                        my $nns = $ad->guess_namespace_careful($id);
                        if ($nns) {
                            push @sec, $ad->convert
                                ( -id => $id, -ns1 => $nns, -ns2 => 'll');
                        }
                    }
                    my %uniq = map { $_ => 1 } @sec;
                    @sec = sort keys %uniq;
                    unless ($#sec == -1) {
                        push @{$rows}, map {[ $_, -1 ]} @sec;
                        &msg("Rescued loci", $sym, $gns, join(' + ', @sec));
                    }
                }
            }
            if ($#{$rows} == -1) {
                &msg("No loci found for ID", $sym, $gns);
                $s2l{$sym} = {};
            } else {
                my %uniq = map { $_->[0] => 1 } @{$rows};
                my $taxa = &loc2tax( keys %uniq );
                my %byTax;
                foreach my $row (@{$rows}) {
                    my ($ll, $sc) = @{$row};
                    if (my $tax = $taxa->{$ll}) {
                        push @{$byTax{$tax}{$sc}}, $ll;
                    }
                }
                while (my ($tax, $scHash) = each %byTax) {
                    my ($topScore) = sort { $b <=> $a } keys %{$scHash};
                    $s2l{$sym}{$tax} = $scHash->{$topScore};
                }
            }
        }
        while (my ($tax, $locs) = each %{$s2l{$sym}}) {
            map { $rv{$tax}{$_} = 1 } @{$locs};
        }
    }
    foreach my $tax (keys %rv) {
        $rv{$tax} = [ sort keys %{$rv{$tax}} ];
    }
    return \%rv;
}

sub loc2tax {
    my %rv;
    foreach my $loc (@_) {
        next unless ($loc);
        unless (defined $l2t{$loc}) {
            my @taxa = sort {length($a) <=> length($b) } $ad->convert
                ( -id => $loc, -ns1 => 'll', -ns2 => 'tax' );
            if ($#taxa == 1 && $taxa[1] =~ /^\Q$taxa[0]\E/) {
                # second taxa is just more specific than first
                @taxa = ($taxa[0]);
            }
            if ($#taxa == -1) {
                &msg("No taxa for locus", $loc);
                $l2t{$loc} = "";
            } elsif ($#taxa == 0) {
                $l2t{$loc} = $taxa[0];
            } elsif ($okTaxa{$taxa[0]}) {
                # Taxa with known annoying variant subspecies
                $l2t{$loc} = $taxa[0];
            } else {
                my %genSpec;
                foreach my $tax (@taxa) {
                    if ($tax =~ /^(\S+ \S+)/) {
                        $genSpec{$1}++;
                    } else {
                        $genSpec{""}++;
                    }
                }
                my @gs = keys %genSpec;
                if ($#gs == 0 && $gs[0]) {
                    my $tax = $l2t{$loc} = $gs[0];
                    # &msg("Taxa Rescue on Genus-Species", $loc, $tax, @taxa);
                } else {
                    &msg("Multiple taxae for locus", $loc, @taxa);
                    $l2t{$loc} = "";
                }
            }
        }
        $rv{$loc} = $l2t{$loc};
    }
    return \%rv;
}

sub msg {
    return if ($#_ == -1);
    my $txt = join("\t", map { defined $_ ? $_ : '-undef-' } @_);
    return if ($announced{$txt});
    if ($mfile) {
        if ($fc) {
            $fc->write_output('MSG', $txt."\n");
        } else {
            open(MFILE,">>$mfile") || $args->death
                ("Failed to append message", $mfile, $!);
            print MFILE "$txt\n";
            close MFILE;
        }
    } else {
        $args->msg($txt) if ($vb);
    }
}

sub initialize {
    my $ad = &init_ad();
    $mt = $ad->tracker();
    $lh = &init_lh( $mt );
    return unless ($fc);
    if ($fc->child == 1 && $mfile) {
          map { &msg( @{$_} ) } @globalMsg;
          @globalMsg = ();
    }
}

sub init_ad {
    $ad = BMS::MapTracker::AccessDenorm->new
        ( -age     => $args->{AGE},
          -ageall  => $args->{AGEALL} );
}

sub init_lh {
    # warn `ls -lh $testfile` if ($testfile && -s $testfile);
    my ($mtpass, $tf) = @_;
    my $lh = BMS::MapTracker::LoadHelper->new
        ( -username => 'MSigDB', 
          -basedir  => $basedir,
          -testmode => $tm,
          -testfile => $tf ? ">>$tf" : undef,
          -tracker  => $mtpass,);
    srand( time() ^ ($$ + ($$<<15)) );
    $mt = $lh->tracker;
    if ($mtpass && $fc) {
        if (my $fh = $fc->output_fh('TESTFILE')) {
            $lh->redirect( -stream => 'TEST', -fh => $fh );
        }
    }
    return $lh;
}

sub finalize {
    $lh->write();
    # warn "$lh FINAL CONTENT ".$lh->rows_written()."\n";
    if ($basedir && $doLoad && !$tm) {
        $args->msg("Loading MapTracker $basedir") if ($vb && $fc->child == 1);
        $lh->process_ready();
    }
    $lh = undef;
}
