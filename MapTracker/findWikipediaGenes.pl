#!/stf/biobin/perl -w

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
    }
    $| = 1;
    print '';
    # print "Content-type: text/plain\n\n";
}

=head1 FILES

http://dumps.wikimedia.org/enwiki/

  1.4  GB       externallinks.sql.gz
  0.9  GB       page.sql.gz
  0.08 GB       redirect.sql.gz
  1.3  GB       templatelinks.sql.gz


=cut

my $VERSION = 
    ' $Id$ ';

use strict;
use BMS::BmsArgumentParser;
use BMS::MapTracker::AccessDenorm;
use BMS::MapTracker::GenAccService;
use BMS::Utilities::FileUtilities;
use BMS::MapTracker::LoadHelper;

my $args = BMS::BmsArgumentParser->new
    ( -nocgi      => $ENV{HTTP_HOST} ? 0 : 1,
      -dir        => '/work5/tilfordc/WGET/wikipedia',
      -fork       => 15,
      -basedir    => 'Wikipedia',
      -loadtoken  => 'WP',
      -testmode   => 1,
      -progress   => 30,
      # -prefix     => 'enwiki-latest',
      -errormail  => 'charles.tilford@bms.com', );

$args->shell_coloring();

my $limit     = $args->val(qw(limit));
my $dir       = $args->val(qw(directory dir));
my $clobber   = $args->val(qw(clobber));
my $safe      = '2 Mar 2011 4:40pm';
my $age       = $args->val(qw(age ageall allage)) || $safe;
my $cAge      = $args->val(qw(cloudage ageall allage)) || $safe;
my $forkNum   = $args->val(qw(fork));
my $tm        = $args->val(qw(testmode));
my $testfile  = $args->val(qw(testfile));
my $progress  = $args->val(qw(progress));
my $filePrfx  = $args->val(qw(prfx prefix)) || "";
$filePrfx     =~ s/\-$//;
my $fu        = BMS::Utilities::FileUtilities->new();
my $quotePH   ='--QuOtE--';

unless ($filePrfx) {
    $args->msg("[!]","You need to provide the prefix for the .gz files, eg:",
               "-prefix enwiki-20130403");
    exit;
}

my $trail    = ');';
my $spiderT  = 0;
my $notFound = "--NOT FOUND--";
my $fetchRaw = "http://en.wikipedia.org/w/index.php?action=raw&title=";

my $ad        = BMS::MapTracker::AccessDenorm->new
    ( -age      => $age,
      -cloudage => $cAge );
my $ageDays  = int(0.5 + 10 * $ad->age()) / 10;
my $mt        = $ad->tracker;
my $lh;

my @gasCom = ( -age      => $age,
               -cloudage => $cAge,
               -format   => 'tsv',
               -fork     => $forkNum,
               -verbose  => 0,
               -keepcomma => 1,
               -warn     => 0,
               -quiet    => 1,
               -scramble => 1);

my @blacklist = qw
    (Uniprot Uniprot/doc Infobox_protein Infobox_protein/doc Taxonomy/
     UCSC_Genome_Browser Ensembl EntrezGene Infobox_protein_-_short
     gene protein);

my %isBL = map { $_ => 1 } @blacklist;

# http://en.wikipedia.org/wiki/Help:Namespace
my %nss = ( 0 => 'Main',
            2 => 'User',
            4 => 'Wikipedia',
            6 => 'File',
            8 => 'MediaWiki',
            10 => 'Template',
            12 => 'Help',
            14 => 'Category',
            90 => 'Thread',
            92 => 'Summary',
            100 => 'Portal',
            108 => 'Book', );
foreach my $id (sort { $a <=> $b } keys %nss) {
    $nss{$id + 1} ||= $nss{$id}." Talk";
}

unlink($testfile) if ($testfile);

# &raw_text('Paclitaxel'); die;

&associate();

$args->msg("Finished", `date`);

sub associate {
    my $outfile = &build_links();
    open(ALINK, "<$outfile") || $args->death
        ("Failed to read Loci-Article file", $outfile, $!);
    # <ALINK>;
    my $num = 0;
    my $wpNS = "#Wikipedia#";
    my %titles;
    &_init_mt();
    while (<ALINK>) {
        s/[\n\r]+$//;
        my @arts = split(/\t/);
        my $id   = shift @arts;
        foreach my $artL (@arts) {
            if ($artL =~ /^(\d+)\:(.+)/) {
                my ($level, $title) = ($1, $2);
                $titles{$title}++;
                $lh->set_edge( -name1 => $id,
                               -name2 => "$wpNS$title",
                               -type  => 'is referenced in',
                               -tags  => [['Level',undef,$level]] );
            }
        }
        $num++;
        $lh->write_threshold_quick(1000);
        last if ($limit && $num >= $limit);
    }
    close ALINK;
    $lh->write();
    foreach my $title (sort keys %titles) {
        my $mtt = "$wpNS$title";
        my $cls = 'WIKIPEDIA';
        if ($title =~ /^(.+)\#/) {
            my $par = $1;
            $cls ='WPSECTION';
            $lh->set_edge( -name1 => $mtt,
                           -name2 => "$wpNS$par",
                           -type  => 'is a child of', );
        }
        $lh->set_class($mtt, $cls);
    }
    $lh->write();
}

sub build_links {
    my $outfile  = "$dir/AllArticles.tsv";
    return $outfile unless  (&_generate_file($outfile));
    my $genes = &read_gene_associations();
    my $redir = &redirects();
    open(OUT, ">$outfile")|| $args->death
        ("Failed to write full article file", $outfile, $!);
    my %full;
    $args->msg("[PARSE]","Assembling Gene-to-Articles in depth");
    my %okPrfx = map { $_ => 1 } qw(Category);
    my %ignoredPrfx;
    foreach my $gene (sort { $a->{id} cmp $b->{id} } values %{$genes}) {
        my @arts  = @{$gene->{titles}};
        my $id    = $gene->{id};
        my %links = map { $_ => 1 } @arts;
        foreach my $title (@arts) {
            my $text = &raw_text($title);
            foreach my $line (split(/[\n\r]+/, $text)) {
                while ($line =~ /(\[\[([^\]]+)\]\])/) {
                    my ($rep, $link) = ($1, $2);
                    # Remove the match to prevent iLoops:
                    $line =~ s/\Q$rep\E//;
                    # Discard 'other stuff':
                    my ($main) = split(/\|/, $link);
                    my $sec = "";
                    if ($main =~ /([^\#]+)\#(.+)/) {
                        # Subsection eg Drug_metabolism#Acetylation
                        ($main, $sec) = ($1, $2);
                    }
                    if ($main =~ /^(\:?[^\:]+)\:/) {
                        # Wow. Lots of irrelevant stuff here.
                        # Languages: es:, arz:, be-x-old:, etc
                        # Image: and File:
                        # simple: - may be nice one day, but sparse now
                        # wikt: and Wiktionary: - sparse
                        # doi:
                        # species:
                        my $prfx = $1;
                        unless ($okPrfx{$prfx}) {
                            $ignoredPrfx{$prfx}++;
                            next;
                        }
                    }
                    next if ($main =~ /^([a-z]{2}|File|Image)\:/i);
                    $main =~ s/\s/_/g;
                    if (my $red = $redir->{$main}) { 
                        $main = $red;
                    }
                    next if ($isBL{$main} || $main =~ /^\d+$/);
                    $links{$main}         ||= 2;
                    $links{"$main#$sec"}  ||= 3 if ($sec);
                }
            }
        }
        my @bits;
        foreach my $title (sort { $links{$a} <=> $links{$b} ||
                                      $a cmp $b } keys %links) {
            my $dist = $links{$title};
            $full{$title}++ unless ($dist > 2);
            push @bits, sprintf("%d:%s", $dist, $title);
        }
        print OUT join("\t", $id, @bits)."\n";
    }
    close OUT;
    my @ips = sort { $ignoredPrfx{$b} <=> $ignoredPrfx{$a} || $a cmp $b } keys %ignoredPrfx;
    my $ipFile = "$dir/AllArticles-IgnoredPrefices.tsv";
    if ($#ips == -1) {
        unlink($ipFile) if (-e $ipFile);
    } else {
        open(IPF, ">$ipFile") || $args->death
            ("Failed to write ignored prefix file", $ipFile, $!);
        foreach my $ip (@ips) {
            print IPF "$ip\t$ignoredPrfx{$ip}\n";
        }
        close IPF;
        $args->msg("[FILE]","Ignored prefices", $ipFile);
    }
    my @all = sort keys %full;
    my %hist; map { $hist{$_ <= 100 ? $_ : 101 }++ } values %full;
    my @hists = sort { $a <=> $b } keys %hist;
    $args->msg(scalar(@all)." total articles referenced",
               map { sprintf("%s genes : %d articles", $_ == 101 ? "100+" : $_, $hist{$_}) } @hists);
    return $outfile;
}

sub read_gene_associations {
    my $locArtF  = "$dir/LociToArticles.tsv";
    &make_gene_association($locArtF) if (&_generate_file($locArtF));
    open(LOCART, "<$locArtF") || $args->death
        ("Failed to read Loci-Article file", $locArtF, $!);
    <LOCART>;
    my %genes;
    while (<LOCART>) {
        s/[\n\r]+$//;
        my @arts = split(/\t/);
        my $id   = shift @arts;
        my @aa   = ( id => $id, sym => shift @arts, taxa => shift @arts,
                     desc => pop @arts, titles => \@arts );
        $genes{$id} = { @aa };
    }
    close LOCART;
    return \%genes;
}

sub file_for_title {
    my $title  = shift;
    my $short  = $title;
    $short     =~ s/\//-/g;
    $short     = substr($short, 0, 250) if (length($short) > 250);
    my $path   = sprintf("%s/RawText/%s/%s.wiki", $dir,
                         uc(substr($short, 0, 1)), $short);
    return $path;
}

sub raw_text {
    my $title  = shift;
    my $path   = &file_for_title( $title );
    my $cmd;
    unless (-s $path) {
        my $wait = 3 - (time - $spiderT);
        my $url  = $fetchRaw . $fu->esc_url($title);
        $url  =~ s/\'/\%27/g;
        $url  =~ s/\+/\%2B/g;
        $fu->assure_dir( $path, 1 );
        sleep($wait) if ($wait > 0);
        my $cpath = $path;
        map { $cpath =~ s/\Q$_\E/\\$_/g; } ("'", '"', ' ');
        $cpath =~ s/\'/\\\'/g;
        $cmd = "wget --quiet '$url' -O '$cpath'";
        $args->msg("[>]", "wget -> $path");
        system($cmd);
        $spiderT = time;
    }
    my $rv = "";
    if (open(RAW, "<$path")) {
        while (<RAW>) { $rv .= $_; }
        close RAW;
    } else {
        $args->msg("[ERROR]", "Failed to read raw text for ".$title,$path, $!,$cmd);
    }
    return $rv;
}

sub make_gene_association {
    my $locArtF  = shift;
    my $art      = &map_page_ids();
    my @nihParam = qw(cmd db list_uids);
    my (%byLocus, %probs, %titleCount);
    foreach my $data (@{$art}) {
        my $title = $data->{Title};
        my %u;
        foreach my $url (@{$data->{urls}}) {
            my ($domain, $path, $params) = &parse_url_args($url);
            if ($domain eq 'www.ncbi.nlm.nih.gov') {
                if ($path !~ /query\.fcgi$/ && $path ne 'sites/entrez') {
                    $probs{"Path not captured: $path"}++;
                    next;
                }
                my ($cmd, $db, $ids) = map {
                    lc($params->{$_} || '') } @nihParam;
                $ids ||= lc($params->{termtosearch} || '');
                if ($cmd ne 'retrieve' && $cmd ne 'showdetailview') {
                    $probs{"Unknown command: '$cmd'"}++;
                    next;
                }
                if ($db ne 'gene') {
                    $probs{"Unknown DB: '$db'"}++;
                    next;
                }
                if (!$ids || $ids !~ /^[\d\,]+$/) {
                    $probs{"Unknown IDs: '$ids'"}++;
                    # warn "$url\n";
                    next;
                }
                # Ok, this looks like a locus ID(s)
                my @ids = split(/\,/, $ids);
                foreach my $id (@ids) {
                    my $lid = "LOC$id";
                    $byLocus{$lid}{$title}++;
                    $u{$lid}++;
                }
            } elsif ($domain =~ /nih/) {
                $probs{"Domain not captured: $domain"}++;
            }
        }
        my @ul = keys %u;
        $titleCount{$#ul + 1}++;
    }
    my @prbs = sort keys %probs;
    $args->msg("Could not identify LocusLink target for some articles",
               map { "$_ : $probs{$_}" } @prbs) unless ($#prbs == -1);
    my @loci = &fancySort( [ keys %byLocus] );

    my %locMeta;
    my $metaF = "$dir/LociMetaFile.tsv";
    my $rows  = &forked_convert
        ( -output => $metaF, -ids => \@loci, -mode => 'simple',
          -cols   => 'termout,sym,taxa,desc' );
    foreach my $row (@{$rows}) {
        my ($id, $sym, $taxa, $desc) = @{$row};
        $locMeta{$id} = {
            sym  => $sym,
            taxa => $taxa,
            desc => $desc,
        };
    }
    
    open(LOCART, ">$locArtF") || $args->death
        ("Failed to write Locus-Article output file", $locArtF, $!);

    my @metaCols = qw(sym taxa desc);
    my (%lociCount, %usedTitles, %taxCount);
    print LOCART join("\t", qw(Locus Symbol Taxa WP1 WP2 WPetc Description))."\n";
    foreach my $id (@loci) {
        my @arts = sort keys %{$byLocus{$id}};
        my ($sym, $taxa, $desc) = map { $locMeta{$id}{$_} || "" } @metaCols;
        $taxCount{ $taxa || "Unknown" }++;
        map {$usedTitles{$_}++}@arts;
        $lociCount{$#arts + 1}++;
        print LOCART join("\t", $id, $sym, $taxa, @arts, $desc)."\n";
        if ($#arts + 1 > 3) {
            $args->msg("[HVY]", "$id [$sym] $taxa $desc", @arts);
        }
    }
    close LOCART;

    my @ut   = keys %usedTitles;

    my $otTax = "Other Taxae";
    my @cMsg = (($#loci+1)." distinct loci", ($#ut+1)." distinct articles");
    foreach my $taxa (sort { $taxCount{$b} <=> $taxCount{$a} } keys %taxCount){
        my $c = $taxCount{$taxa};
        if ($c > 10) {
            push @cMsg, "$taxa loci : $c";
        } else {
            $taxCount{$otTax} += $c;
        }
    }
    if (my $c = $taxCount{$otTax}) { push @cMsg, "$otTax : $c"; }
    foreach my $c (sort { $a <=> $b } keys %lociCount) {
        push @cMsg, sprintf("Loci with %d articles : %d", $c, $lociCount{$c});
    }
    foreach my $c (sort { $a <=> $b } keys %titleCount) {
        push @cMsg, sprintf("Articles with %d loci : %d", $c, $titleCount{$c});
    }
    $args->msg(@cMsg);
    
}

sub parse_url_args {
    my $url = shift;
    my ($domain, $path, %params) = ("","");
    if ($url =~ /^https?\:\/\/([^\/]+)(\/(.+))?$/) {
        ($domain, $path) = ($1, $3 || "");
        if ($path =~ /(.+)\?(.+)/) {
            $path = $1;
            foreach my $pbit (split(/\&/, $2)) {
                if ($pbit =~ /([^\=]+)\=(.+)/) {
                    my ($p, $v) = (lc($1), $2);
                    if (exists $params{$p}) {
                        # Multiple values
                        # For the moment destroy these values
                        $params{$p} = undef;
                    } else {
                        $params{$p} = $v;
                    }
                }
            }
        }
    }
    return ($domain, $path, \%params);
}


sub map_page_ids {
    my $outfile = &make_map_file();

    open(MAPOUT, "<$outfile")|| $args->death
        ("Failed to read Link output file", $outfile, $!);
    my $headTxt = <MAPOUT>;
    $headTxt =~ s/[\n\r]+$//;
    my @head = split(/\t/, $headTxt);
    pop @head if ($head[-1] =~ /url/i);
    my @articles;
    while (<MAPOUT>) {
        s/[\n\r]+$//;
        my @urls = split(/\t/, $_);
        my %data;
        foreach my $key (@head) {
            $data{$key} = shift @urls;
        }
        $data{urls} = \@urls;
        push @articles, \%data;
    }
    close MAPOUT;
    return \@articles;
}

sub make_map_file {
    my $outfile  = "$dir/mappedIDs.tsv";
    return $outfile unless  (&_generate_file($outfile));
    my $idH      = &read_chembio_links();

    my @temps;
    my $pass = 0;

    my $cb = sub {
        my ($dat, $id, $nsn, $title, $dt) = @_;
        return 0 if ($dat->{primary});
        if ($isBL{$title}) {
            $dat->{primary} = -1;
            return 0;
        }
        $dat->{title} = $title;
        $dat->{date}  = $dt;
        $dat->{nsn}   = $nsn;
        if ($nsn eq 'Main') {
            # Main page
            $dat->{primary} = $id;
        } elsif ($nsn eq 'Template') {
            # Template
            push @temps, $dat;
        } else {
            # Weird other namespace
            $dat->{primary} = -1;
        }
        return 1;
    };

    do {
        $args->msg("[PASS]", ++$pass);
        @temps = ();
        &scan_page_file( $idH, $cb );
        my $tnum = $#temps + 1;
        if ($tnum) {
            # We need to map some templates over to their parent document
            my %need = map { $_->{title} => $_ } @temps;
            $args->msg("Finding parents for $tnum templates", $tnum <= 15 ?
                       (sort map { $_->{title}} @temps) : ());
            my $tempFile = &normalize_templatelink_file();
            open(LINKFILE, "<$tempFile") || $args->death
                ("Failed to read template file", $tempFile, $!);
            my $lt = time;
            my $count = 0;
            my $htxt = <LINKFILE>;
            while (<LINKFILE>) {
                $count++;
                s/[\n\r]+$//;
                my @pars  = split(/\t/);
                my $title = shift @pars;
                my $dat   = $need{$title};
                next unless ($dat);
                my $ns    = shift @pars;
                my $parC  = shift @pars;
                $dat->{primary} ||= \@pars;
                foreach my $parId (@pars) {
                    my $parDat = $idH->{$parId} ||= {
                        id   => $parId,
                        urls => {},
                    };
                    # Map over URLs from the template
                    map { $parDat->{urls}{$_} = 1 } keys %{$dat->{urls}};
                    # Map over template inheritance
                    while (my ($kt, $kn) = each %{$dat->{templates} || {}}) {
                        $parDat->{templates}{$kt} = $kn + 1 if
                            (!defined $parDat->{templates}{$kt} ||
                             $parDat->{templates}{$kt} > $kn + 1);
                    }
                    # Note that this template is a direct kid
                    $parDat->{templates}{$title} = 1;
                }
                if (time - $lt >= $progress) {
                    $args->msg("  [Links]", sprintf
                               ("%8.3fk records", $count/1000));
                    $lt = time;
                }
            }
            close LINKFILE;
            # In case we missed any, mark up primary key to prevent i-loop
            map { $_->{primary} ||= -1 } @temps;
            $args->msg("  [LINKS]", sprintf
                       ("%8.3fk records", $count/1000));
        }
    } while ($#temps > -1);

    my $weirdFile = $outfile;
    $weirdFile =~ s/\.tsv$/-Weird.tsv/;
    $weirdFile .= "-Weird.tsv" if ($weirdFile eq $outfile);

    open(WEIRD, ">$weirdFile")|| $args->death
        ("Failed to write 'weird' output file", $weirdFile, $!);

    open(OUT, ">$outfile")|| $args->death
        ("Failed to write Map output file", $outfile, $!);
    my $ucscUrl = '';

    foreach my $fh (*WEIRD, *OUT) {
        print $fh join("\t", "Title", "Namespace", "ID", "Date", "Via Templates", "URLs")."\n";
    }

    foreach my $dat ( sort { uc($a->{title} || "") cmp uc($b->{title} || "") }
                      values %{$idH}) {
        # next if ($dat->{primary} || 0 < 1);
        my $kh    = $dat->{templates} || {};
        my $kids  = join('||', map { "$_:$kh->{$_}" } sort keys %{$kh});
        my $title = $dat->{title};
        my $ns    = $dat->{nsn} || "";
        my $id    = $dat->{id};
        my $fh    = *OUT;
        if (!$title) {
            $title = $notFound;
            $fh = *WEIRD;
        }
        $fh = *WEIRD unless ($ns && $ns eq 'Main' && $id);
        
        print $fh join("\t", $title, $ns, 
                       $dat->{id} || '-1', $dat->{date} || '', $kids || "",
                       sort keys %{$dat->{urls}})."\n";
    }
    close OUT;
    close WEIRD;
    return $outfile;
}

sub _normalize_args {
    my $txt = shift;
    return join('&', sort split(/\&/, $txt));
}

sub _generate_file {
    my $file = shift;
    return "Clobber requested" if ($clobber);
    return "No file argument provided" unless ($file);
    return "File does not exist" unless (-e $file);
    return "File is empty" unless (-s $file);
    return 0;
}

sub read_chembio_links {
    my $outfile  = "$dir/scannedLinks.tsv";
    &get_chembio_links($outfile) if (&_generate_file($outfile));
    open(LINKOUT, "<$outfile")|| $args->death
        ("Failed to read Link output file", $outfile, $!);
    my $idH = {};
    while (<LINKOUT>) {
        s/[\n\r]+$//;
        my @urls = split(/\t/, $_);
        my $id   = shift @urls;
        $idH->{$id} = {
            id => $id,
            urls => { map { $_ => 1 } @urls },
        };
    }
    close LINKOUT;
    return $idH;
}

sub get_chembio_links {
    my ($outfile) = @_;
    my $linkFile  = &normalize_externallinks_file();
    my $rejects   = "$dir/RejectedUrls.tsv";

    my @grab = 
        (
         'www\.ncbi\.nlm\.nih\.gov[^\']+db=gene',
         'www\.ensembl\.org[^\']+gene=',
         'www\.uniprot\.org\/uniprot\/',
         'genome\.ucsc\.edu[^\']+hgTracks'
        );
    my $grabRE = '('.join('|', @grab).')';
    $args->msg("Getting biological links via:", split(/\|/, $grabRE), `date`);
    my ($lines, $count, $found) = (0,0, 0);
    my $lt = time;
    my (%ids, %hits, %reject);
    open(LINKS, "<$linkFile") || $args->death
        ("Failed to read Link file", $linkFile, $!);
    my $htxt = <LINKS>;
    while (<LINKS>) {
        s/[\n\r]+$//;
        my @ids = split(/\t/);
        my $url = shift @ids;
        $lines++;
        if ($url =~ /\{\{\{/) {
            $reject{$url} = "Generic variable URL";
            next;
        }
        if ($url =~ /ncbi/i) {
            
            if ($url =~ /pubmed/i) {
                $reject{$url} = "Pubmed link";
                next;
            } elsif ($url !~ /(retrieve|TermToSearch)/i) {
                $reject{$url} = "No retrieve argument";
                next;
            }
        } elsif ($url =~ /uniprot/) {
            if ($url =~ /taxonomy/) {
                $reject{$url} = "Taxonomy node";
                next;
            }
        } elsif ($url =~ /ensembl/ && $url =~ /ensemble/) {
            $reject{$url} = "Wrong Ensemble";
            next;
        }
        my $dom = shift @ids;
        map { $hits{Total}{$_} = 1;
              $hits{$dom}{$_}  = 1;
              $ids{$_}{$url}   = 1;
          } @ids;
    }
    close LINKS;
    my @tmsg;
    foreach my $type (sort keys %hits) {
        my @u = keys %{$hits{$type}};
        push @tmsg, sprintf("%-20s %d pages", $type, $#u + 1);
    }
    open(OUT, ">$outfile")|| $args->death
        ("Failed to write Link ouput file", $outfile, $!);

    foreach my $id (sort {$a <=> $b } keys %ids) {
        print OUT join("\t", $id, sort keys %{$ids{$id}})."\n";
    }
    close OUT;

    open(REJ, ">$rejects")|| $args->death
        ("Failed to write Rejects file", $rejects, $!);

    foreach my $url (sort keys %reject) {
        print REJ join("\t", $url, $reject{$url})."\n";
    }
    close REJ;
    return $outfile;
}

sub scan_page_file {
    my ($hash, $cb) = @_;
    my $pageFile = &normalize_page_file();
    # die $args->branch( -maxany => 10, -ref => $hash);

    open(PAGEFILE, "<$pageFile") || $args->death
        ("Failed to read Page file", $pageFile, $!);
    my $count = 0;
    my %found;
    my $lt = time;
    while (<PAGEFILE>) {
        s/[\n\r]+$//;
        my ($id, $title, $ns, $dt) = split(/\t/);
        if (my $dat = $hash->{$id}) {
            my $nsn = $nss{$ns} || "Unknown NS $ns";
            if (&{$cb}($dat, $id, $nsn, $title, $dt)) {
                $found{$nsn}++;
            }
        }
        $count++;
        if (time - $lt >= $progress) {
            $args->msg("  [Page]", sprintf
                       ("%8.3fk records", $count/1000), map {
                           "$_: $found{$_}" } sort keys %found);
            $lt = time;
        }
    }
    close PAGEFILE;
    $args->msg("  [PAGE]", sprintf
               ("%8.3fk records", $count/1000), map {
                   "$_: $found{$_}" } sort keys %found);
}

sub normalize_externallinks_file {
    my $extFile = "$dir/$filePrfx-externallinks.sql.gz";
    my $tsvFile  = $extFile;
    $tsvFile     =~ s/\..+//;
    $tsvFile    .= "-Filtered.tsv";
    $args->death("Can not normalize external links file", $extFile)
        if ($tsvFile eq $extFile);
    return $tsvFile unless &_generate_file($tsvFile);
    $args->msg("[PARSE]","Normalizing main external links",$extFile,$tsvFile);
    my $lead = 'INSERT INTO `externallinks` VALUES (';

    my @domains = qw(ncbi ensembl uniprot genome genenames emolecules
                     drugbank chemspider fdasis);
    push @domains, ('jax\.org', 'ebi\.ac\.uk');
    my $domRE   = '('.join('|', sort @domains).')';
    open(EXTFILE, "gunzip -c $extFile |") || $args->death
        ("Failed to read Extlate Links file", $extFile, $!);
    my (%domains, %urls);
    while (<EXTFILE>) {
        s/^\Q$lead\E//;
        s/\Q$trail\E//;
        foreach my $bit (split(/\)\,\(/)) {
            $bit =~ s/\\\'/$quotePH/g;
            if ($bit =~ /^(\d+)\,\'([^\']+)\'\,\'/) {
                my ($id, $url) = ($1, $2);
                $url =~ s/$quotePH/\'/g;
                if ($url =~ /^https?\:\/\/([^\/]+)/) {
                    my $domain = $1;
                    if ($domain =~ /$domRE/) {
                        my $targ = $urls{$url} ||= {
                            url => $url,
                            dom => $domain,
                            ids => [],
                        };
                        push @{$targ->{ids}}, $id;
                        $domains{$domain}++;
                    }
                }
            }
        }
    }
    close EXTFILE;
    $args->msg("Captured domain summary", map { "$_ : $domains{$_}" }
               sort {$domains{$b} <=> $domains{$a} } keys %domains);
    
    open(TSVFILE, ">$tsvFile") || $args->death
        ("Failed to write External Links TSV file", $tsvFile, $!);
    print TSVFILE join
        ("\t", qw(URL Domain ID1 ID2 IDetc))."\n";
    foreach my $dat (sort { $#{$b->{ids}} <=> $#{$a->{ids}} ||
                            $a->{url} cmp $b->{url} } values %urls) {
        my @ids = sort {$a <=> $b } @{$dat->{ids}};
        print TSVFILE join("\t", $dat->{url}, $dat->{dom}, @ids)."\n";
    }
    close TSVFILE;
    return $tsvFile;
}

sub normalize_templatelink_file {
    my $tempFile = "$dir/$filePrfx-templatelinks.sql.gz";
    my $tsvFile  = $tempFile;
    $tsvFile     =~ s/\..+//;
    $tsvFile    .= "-Normalized.tsv";
    $args->death("Can not normalize temp file", $tempFile)
        if ($tsvFile eq $tempFile);
    return $tsvFile unless &_generate_file($tsvFile);
    $args->msg("[PARSE]","Normalizing main template links",$tempFile,$tsvFile);
    my $lead = 'INSERT INTO `templatelinks` VALUES (';
    open(TEMPFILE, "gunzip -c $tempFile |") || $args->death
        ("Failed to read Template Links file", $tempFile, $!);
    my %temps;
    while (<TEMPFILE>) {
        s/^\Q$lead\E//;
        s/\Q$trail\E//;
        foreach my $bit (split(/\)\,\(/)) {
            $bit =~ s/\\\'/$quotePH/g;
            if ($bit =~ /^(\d+)\,(\d+)\,\'([^\']+)\'/) {
                my ($parId, $ns, $title) = ($1, $2, $3);
                $title =~ s/$quotePH/\'/g;
                my $targ = $temps{$title} ||= {
                    title => $title,
                    ns    => $ns,
                    par   => [],
                };
                push @{$targ->{par}}, $parId;
            }
        }
    }
    close TEMPFILE;

    open(TSVFILE, ">$tsvFile") || $args->death
        ("Failed to write Template Links TSV file", $tsvFile, $!);
    print TSVFILE join
        ("\t", qw(TemplateTitle TemplateNS ParCount Par1 Par2 ParEtc))."\n";
    foreach my $dat (sort { $#{$a->{par}} <=> $#{$b->{par}}  ||
                                uc($a->{title}) cmp uc($b->{title}) }
                     values %temps) {
        my @pars = @{$dat->{par}};
        print TSVFILE join("\t", $dat->{title}, $dat->{ns},
                           $#pars + 1, @pars)."\n";
    }
    close TSVFILE;
    return $tsvFile;
}

sub normalize_page_file {
    my $pageFile = "$dir/$filePrfx-page.sql.gz";
    my $tsvFile  = $pageFile;
    $tsvFile     =~ s/\..+//;
    $tsvFile    .= "-MainAndTemplate.tsv";
    $args->death("Can not normalize page file", $pageFile)
        if ($tsvFile eq $pageFile);
    return $tsvFile unless &_generate_file($tsvFile);
    $args->msg("[PARSE]","Normalizing main page file", $pageFile, $tsvFile);

    my $lead     = 'INSERT INTO `page` VALUES (';
    open(PAGEFILE, "gunzip -c $pageFile |") || $args->death
        ("Failed to read Page file", $pageFile, $!);
    open(TSVFILE, ">$tsvFile") || $args->death
        ("Failed to write Page TSV file", $tsvFile, $!);
    while (<PAGEFILE>) {
        s/^\Q$lead\E//;
        foreach my $bit (split(/\)\,\(/)) {
            my ($id, $ns, $bit2);
            if ($bit =~ /^(\d+)\,(\d+),(.+)/) {
                ($id, $ns, $bit2) = ($1, $2, $3);
                # ONLY get templates and main pages:
                next unless ($ns == 0 || $ns == 10);
            } else {
                next;
            }
            $bit2 =~ s/\\\'/$quotePH/g;
            if ($bit2 =~ /^\'([^\']+)\'\,(\'.+)/) {
                my ($title, $xtra) = ($1, $2, $3, $4);
                $title =~ s/$quotePH/\'/g;
                # On first load, there were 23,178,057 pages total,
                # but only 397,867 "redirect" pages. 373M vs 996M,
                # vs 2GB for full SQL file
                my @xtras = split(/\,/, $xtra);
                my $dt = $xtras[5];
                $dt =~ s/\'//g;
                print TSVFILE join("\t", $id, $title, $ns, $dt)."\n";
            }
        }
    }
    close PAGEFILE;
    close TSVFILE;
    return $tsvFile;
}

sub redirects {
    my $pivotFile = &make_pivot_redirect();
    open(PIV, "<$pivotFile") || $args->death
        ("Failed to read pivot file", $pivotFile, $!);
    <PIV>;
    my %redir;
    while (<PIV>) {
        s/[\n\r]+$//;
        my @srcs = split(/\t/);
        my $main = shift @srcs;
        map { $redir{$_} = $main } @srcs;
    }
    close PIV;
    return \%redir;
}

sub make_pivot_redirect {
    my $redirFile = "$dir/$filePrfx-redirect.sql.gz";
    my $redirBase = $redirFile;
    $redirBase    =~ s/\..+//;
    my $pivotFile = "$redirBase-Pivot.tsv";
    return $pivotFile unless  (&_generate_file($pivotFile));

    my $errFile   = "$redirBase-Errors.tsv";
    my $lead = 'INSERT INTO `redirect` VALUES (';
    $args->msg("[PARSE]","Pivoting redirects into title space",
               $redirFile, $pivotFile);
    open(REDIRFILE, "gunzip -c $redirFile |") || $args->death
        ("Failed to read Redir file", $redirFile, $!);
    my ($lines, $count) = (0,0, 0);
    my (%perrs, %ids);
    while (<REDIRFILE>) {
        s/^\Q$lead\E//;
        s/\Q$trail\E//;
        $lines++;
        foreach my $bit (split(/\)\,\(/)) {
            $bit =~ s/\\\'/$quotePH/g;
            if ($bit =~ /^(\d+)\,(\d+)\,\'([^\']+)\'\,(.+)/) {
                my ($srcid, $ns, $targ) = ($1, $2, $3);
                $targ =~ s/$quotePH/\'/g;
                if ($ns) {
                    if (my $nsn = $nss{$ns}) {
                        $targ = "$nsn:$targ";
                    } else {
                        $perrs{UnknownNamespace}{$ns} = 1;
                        $targ = "UnknownNS$ns:$targ";
                    }
                }
                push @{$ids{$srcid}}, $targ;
            }
        }
    }
    close REDIRFILE;
    my %byTitle;
    my $cb = sub {
        my ($dat, $id, $nsn, $src, $dt) = @_;
        $src = "$nsn:$src" unless ($nsn eq 'Main');
        map { $byTitle{$src}{$_} = 1 } @{$dat};
        return 1;
    };
    &scan_page_file( \%ids, $cb );
    open(ERR, ">$errFile")|| $args->death
        ("Failed to write pivot error file", $errFile, $!);
    foreach my $pe (sort keys %perrs) {
        print ERR join("\t", $pe, sort keys %{$perrs{$pe}})."\n";
    }
    my %repivot;
    foreach my $src (sort keys %byTitle) {
        my $using = $src;
        my $lvl   = 0;
        my (@path, %seen);
        while (1) {
            push @path, $using;
            $lvl++;
            if ($seen{$using}++) {
                print ERR join("\t", "CircularRedirect", @path)."\n";
                $using = "";
                last;
            }
            my @trgs = sort keys %{$byTitle{$using} || {}};
            unless ($#trgs == 0) {
                # Multiple target documents
                print ERR join("\t", "MultipleTargets", @path,
                               join('+', @trgs))."\n";
                $using = "";
                last;
            }
            # A single redirect, we will take it
            $using = $trgs[0];
            if ($byTitle{$using}) {
                # Repeat cycle if target is itself redirected;
                next;
            }
            # Otherwise break out
            last;
        }
        if ($using) {
            push @{$repivot{$using}}, $src;
            #shift @path;
            #print PIV join("\t", $src, $using, $lvl, 
            #               join(' > ', @path) || "")."\n";
        }
    }
    close ERR;
    open(PIV, ">$pivotFile")|| $args->death
        ("Failed to write pivot file", $pivotFile, $!);
    print PIV join("\t", qw(MainArticle Redirect1 Redirect2 RedirectEtc))."\n";
    foreach my $using (sort keys %repivot) {
        print PIV join("\t", $using, sort @{$repivot{$using}})."\n";
    }
    close PIV;
    return $pivotFile;
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
        } elsif ($mem =~ /^(\d+)(.+?)/) {
            # Leading numbers - sort by number only
            push @sorter, [$mem, $1, '', $item];
        } else {
            push @sorter, [$mem, 0, $mem, $item];
        }
    }
    # $debug->branch(\@sorter) if ($sorter[0][0] =~ /^PMID/);
    my @rv = map {$_->[3]} sort { $a->[2] cmp $b->[2] ||
                                  $a->[1] <=> $b->[1] || 
                                  $a->[0] cmp $b->[0]} @sorter;
    return wantarray ? @rv : \@rv;
}

sub forked_convert {
    my $gas = BMS::MapTracker::GenAccService->new
        ( -fork    => $forkNum,
          @_,
          -format  => 'tsv',
          -age      => $age,
          -cloudage => $cAge,
          -verbose  => 0,
          -orastart => $ad->oracle_start(),
          -scramble => 1 );
    my $ids = $gas->val(qw(id ids idlist idfile));
    return [] unless ($ids);
    # warn $ids;
    $gas->use_beta( 1 );
    my $rows = $gas->cached_array(  );
    return $rows;
}

sub _init_mt {
    return $lh if ($lh);
    $lh = BMS::MapTracker::LoadHelper->new
        ( -username => 'Wikipedia',
          -userdesc => 'Data extracted from Wikipedia articles',
          -tracker  => $mt,
          -basedir  => $args->val(qw(basedir)),
          -loadtoken => $args->{LOADTOKEN},
          # -carpfile => '>>' . $args->{ERRORS},
          -testmode => $tm,
          -testfile => $testfile ? ">>$testfile" : undef,
          -dumpsql  => $args->{DUMPSQL});
    return $lh;
}
