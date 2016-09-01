#!/stf/biobin/perl -w

BEGIN {
    # Needed to make my libraries available to Perl64:
    # use lib '/stf/biocgi/tilfordc/released';
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

my $VERSION = 
    ' $Id$ ';

use strict;
use BMS::ArgumentParser;
use BMS::MapTracker;
use BMS::MapTracker::Network;
use BMS::MapTracker::PathWalker;
use BMS::FriendlyPanel;
use BMS::DBLinkManager;
#  use BMS::MapTracker::Mapping;
use BMS::ExcelHelper;
use BMS::Fetch3;
use BMS::MapTracker::LoadHelper;
use BMS::MapTracker::PopulateByGenbank;
use Bio::DB::SeqHash::BtreeNew;
use Bio::DB::SeqHash::Sim4;

my $autoadded = {};
my $pathdir = "/stf/biocgi/tilfordc/working/maptracker/MapTracker/Paths";


my $admin = { tilfordc => 1, siemersn => 1, limje => 1, bruccolr => 1,
              hinsdalj => 1, };

my $defaults = { 
    DUMPSQL     => 0,
    SHOWMAP     => 'brief integrated',
    IMAGEWIDTH  => 700,
    DEBUG       => 0,
    NOSCAFFOLD  => 1,
    SHOWBENCH   => 0,
    SAFETY      => 0,
    DOCUT       => 1,
    CUTAFFY     => 8,
    CUTSIM4     => 80,
    CUTBRIDGE   => 0,
    NOCGI       => $ENV{'HTTP_HOST'} ? 0 : 1,
    SHOW_CVS_VERS => 'html cvs',
    SWITCHTO    => "",
    ERRORMAIL   => 'charles.tilford@bms.com',
};

my $queryDefaults = {
    HELPTEXT    => '',
    HELPTYPE    => '',
    HELPID      => 0,
    SEQNAME     => "",
    MAPONLY     => "",
    SEQID       => 0,
    SEQNAMES    => "",
    FETCH       => "",
    CLUSTERSEQ  => "",
    OVERLAP     => 0,
    EDIT        => 0,
    LIMITRELATE => 50,
    USELABEL    => 1,
    MAXGROUP    => 5,
};


my $kp_list = 
    [ Automatic            => "Automatic - Program decides best path rules",
      LDAP_map             => "Reporting Structure",
      LDAP_brief           => "Compact Reporting Structure",
      RefSeq_net           => "Gene-centric data",
      Denormalized_View    => "Simplified Gene Structure",
      Chromosome           => "Chromosome-centric data",
      Probes               => "Probe-centric information",
      Coregulated_Probes   => "Co-regulation network",
      Coregulated_Probes_Strict   => "Co-regulation network (strict)",
      Coregulated_Probes_Loose   => "Co-regulation network (loose)",
      Antiregulated_Probes => "Anti-regulation network",
      Unconnected_Net      => "Unconnected node set",
      Chemical_Information => "Data surrounding chemical entities",
      FDA_Drug_Information => "FDA information regarding drug filings",
     ];


my @foci = 
    ( map => "Coordinate Mappings",
      net => "Networks",
      inf => "Information",
      loa => "Upload Data",
      adv => "Advanced Interface");

my %fhash = @foci;
my %bhash;
foreach my $key (keys %fhash) {
    $bhash{ lc($fhash{$key}) } = $key;
}


my $known_paths = { @{$kp_list} };

my $args = BMS::ArgumentParser->new
    ( %{$defaults}, %{$queryDefaults} );
$args->xss_protect("i|b|em|u");

$args->set_val('TESTMODE', $args->val('SAFETY') ? 0 : 1);
if ($args->val('SWITCHTO') eq 'Advanced Interface') {
    print "Location:mapTracker.pl\n\n";
    exit;
}
$args->set_if_false('FOCUS',  $args->val('MODE'));
if (!$args->val('FOCUS')) {
    if ($args->val('MAPONLY')) {
        $args->setval('FOCUS', 'map');
    } elsif ($args->val('PATH')) {
        $args->setval('FOCUS', 'net');
    }
}

my $focus = substr(lc($args->val('FOCUS') || 'inf'), 0, 3);
$focus = 'map' unless ($fhash{$focus});
my $depth = defined $args->val('MAXDEPTH') ?  $args->val('MAXDEPTH') : 1;
$depth--;

my $uselab = $args->val('USELABEL');
my %stuff;

if (defined $args->val('GETMAPS')) {
    if ($args->val('GETMAPS') =~ /direct/i || $args->val('GETMAPS') eq '1') {
        $args->val('NOSCAFFOLD') = 1;    
    } else {
        $args->val('NOSCAFFOLD') = 0;
    }
}

my $ldap = $ENV{'REMOTE_USER'} || $ENV{'LDAP_USER'} || 0;
$args->set_mime( -mail     => 'charles.tilford@bms.com',
                 -codeurl  => "http://bioinformatics.bms.com/biocgi/filePod.pl?module=_MODULE_&highlight=_LINE_&view=1#Line_LINE_",
                 -errordir => '/docs/hc/users/tilfordc/' )
    unless ($args->val('NOCGI'));
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

# $args->msg("Prog = $0", "INC = ",@INC, "Module = ",$args->module_path('BMS::FriendlyPanel'));
my $tabwid = 600;


# my $testeh = BMS::ExcelHelper->new("/stf/biohtml/tmp/foo.xls"); $testeh->sheet( -name    => "hello", -columns => [ 'This', 'That', 'Other Things' ],); print "<a href='/biohtml/tmp/foo.xls'>Test Excel sheet</a><br />"; die;

#&WRITE_LOG( -args     => $args,
#            -capture  => [ 'SEQNAME', 'SEQNAMES', 'MAPONLY', 'FOCUS',
#                           'PATH', 'NWFILE', 'EXCLUDENODES', 'EXPANDNODES',
#                           'USEEDGE', 'USEPATH', 'FORMATNODE', 'NODECOLOR',
#                           'NODESHAPE', 'AUTOINFO'], );

$args->set_if_false('PATH', 'Automatic');
$args->set_if_false('AUTOINFO', 'Network');

my $autoinfo = 'Always';
if ($args->val('AUTOINFO') =~ /net/i) {
    $autoinfo = 'Network';
} elsif ($args->val('AUTOINFO') =~ /excel/i) {
    $autoinfo = 'Excel';
}


# GLOBAL VARIABLES:
my (%minscores, @cutargs, $linkargs, $gvprogs, $gvers,
    $bestvers, $blisvers, @ncbi_vers, $filtermsg, $mapsfound, $frm);

my $mt;

eval {
    $mt = BMS::MapTracker->new( -dumpsql  => $args->val('DUMPSQL'),
                                -dumpfh   => *STDOUT, 
                                -dumplft  => "<pre>",
                                -dumprgt  => "</pre>\n",
                                -username => $ldap,
                                -userdesc => "BMS Username",
                                -ishtml   => 1,
                                );
};


my $eh;

&SETGLOBALS;


# my $selfurl  = sprintf("http://%s%s", $ENV{SERVER_NAME}, $ENV{SCRIPT_NAME});
my ($reqcount, $foundcount) = (0,0);


print "<pre><b>Library Paths:</b>\n".join("\n", @INC)."</pre>" 
    if ($args->val('DEBUG'));

my $isbeta = 0;
my $dofind = 1;
&HTMLSTART;
unless ($mt) {
    print "<center><h1><font color='red'>MapTracker Database Unavailable</font></h1></center>\n";
    warn "Error details:\n\n" . $@;
    exit;
}
$mt->{DEBUG} = 1 if ($args->val('DEBUG'));
my $aid  = $mt->user->id;
my $help = $mt->help;

my $betabit = $isbeta ? "tilfordc/working/snptracker/" : "";

$args->val('NWFILE') = $args->val('NWFILE_PATH') if
    (!$args->val('NWFILE') && $args->val('USENWFPATH'));

my $starttime = time;
my $lasttime  = $starttime;

my ($fp, $doneOnce, $tableStarted);
my $isBriefTable = ($args->val('SHOWMAP') =~ /brief/i && 
                    $args->val('SHOWMAP') =~ /integ/i) ? 1 : 0;

&ACT_ON_ARGS;
if ($eh) {
    $eh->close;
    print $eh->html_summary;
}
print $mt->taxa_html_key;


&FORM_FINDALL if ($dofind);

$mt->show_benchmarks if ($args->val('SHOWBENCH'));

&HTMLEND;

sub ACT_ON_ARGS {
    if (my $st = $args->val('SWITCHTO')) {
        &SWITCH_FOCUS($st);
    }

    if ($args->val('SHOWCV')) {
        &controlled_vocabularies();
        return;
    }

    if ($focus eq 'loa') {
        &LOADONLY();
        return;
    }

    my $sns = $args->val('SEQNAMES');
    $sns =~ s/\s+/\n/g if ($args->val('SPLITSPACE'));
    my @params = ($sns, $args->val('SEQNAME'));
    push @params, $args->val('MAPONLY') if ($focus eq 'map');

    my $requests = &_split_up(@params);
    return if ($#{$requests} < 0);

    $mt->get_all_classes();

    $args->setval('SEQNAMES', join("\n", @{$requests}));
    # print $mt->javascript();
    if ($focus eq 'net') {
        &NETONLY($requests);        
    } elsif ($focus eq 'inf') {
        &INFO($requests);        
    } else {
        &MAPONLY($requests);
    }
    
    # &INIT_EXCEL if ($autoinfo eq 'Excel');

}

sub SWITCH_FOCUS {
    my ($st) = @_;
    $st    = lc($st);
    my $sf = $args->val('FOCUS') || "";
    if ($fhash{$st}) {
        $focus = $fhash{$st};
    } elsif ($bhash{$st}) {
        $focus = $bhash{$st};
    } else {
        return;
    }
    if ($sf eq 'loa') {
        if (!$args->val('SEQNAMES')) {
            $args->setval('SEQNAMES', $args->val('FETCH') || "");
        }
    } elsif ($focus eq 'loa') {
        if (!$args->val('FETCH')) {
            $args->setval('FETCH', $args->val('SEQNAMES') || "");
        }
    }
}

sub _split_up {
    my @retval;
    foreach my $input (@_) {
        next unless ($input);
        # Always only split on returns
        foreach my $req (split(/[\n\r\t]+/, $input)) {
            $req =~ s/^\s+//; $req =~ s/\s+$//;
            push @retval, $req if ($req);
        }        
    }
    return \@retval;
}

sub SHOW_MISSING {
    my ($missing) = @_;
    return if (!$missing || $#{$missing} < 0);
    print $help->make_link(93);
    printf("<font color='orange' size='+1'><b>%d request failed to find ".
           "entries in the database:</font></b><ul><li>\n",
           $#{$missing} + 1, $#{$missing} == 0 ? '' : 's');
    print join("</li>\n<li>", @{$missing});
    print "</li></ul>\n";
    print $frm->{post};
    printf($frm->{submit}, 'Search SeqStore for missing entries', 'doload');
    printf($frm->{hidden}, 'focus', 'load');
    printf($frm->{hidden}, 'fetch', join("\t", @{$missing}));
    print "</form>\n";
}

sub WARN_EXCESSIVE {
    my ($seqs) = @_;
    my $count = $#{$seqs} + 1;
    if ($count <= 10) {
        # Seems reasonable to me...
        return;
    }
    my $color = 'orange';
    $color = 'red' if ($count > $args->val('LIMITRELATE'));
    printf("<font color='%s'><b>Preparing to analyze %d entries...".
           "</b></font></br >", $color, $count);
}

sub LOADONLY {
    &FETCH_SEQS();
}

sub FETCH_SEQS {
    my $requests = &_split_up($args->val('FETCH'));
    return if ($#{$requests} < 0);

    my $fetch = BMS::Fetch3->new( -format => 'genbank', );
    my $opts  = $fetch->options;
    $opts->set( -noalias      => 1,
                -get_versions => 'smartall',
                -version      => 1, );

    my @missing;
    my @bioseqs;
    printf("<font color='blue' size='+1'>".
           "Checking SeqStore database for %d request%s...".
           "</font><br />",
           $#{$requests} + 1, $#{$requests} == 0 ? '' : 's');
    foreach my $name (@{$requests}) {
        my @fnd = $fetch->fetch( -seqname => $name);
        if ($#fnd < 0) {
            push @missing, $name;
            next;
        }
        push @bioseqs, @fnd;
    }
    &SHOW_MISSING( \@missing );
    return if ($#bioseqs < 0);

    printf("<font color='green'>".
           "A total of %d sequence%s were recovered".
           "</font><br />",
           $#bioseqs + 1, $#bioseqs == 0 ? '' : 's');
    
    
    my $cf = "/stf/biohtml/tmp/mt_web_carp_$$.txt";
    my $lh = BMS::MapTracker::LoadHelper->new
        ( -username => $ENV{'REMOTE_USER'} || $ENV{'LDAP_USER'},
          -testmode => $args->val('TESTMODE'),
          -basedir  => '/stf/biohtml/tmp/MapTracker',
          -logfile  => '/stf/biohtml/tmp/mt_web_load.log',
          -carpfile => $cf,
          -cacheseq => 1,
          -tracker  => $mt, );
    $lh->redirect( -stream => 'test', -fh => *STDOUT );
    my $pbg = BMS::MapTracker::PopulateByGenbank->new
        ( -loadhelper => $lh );
    printf("<font color='blue'>".
           "Transfering metadata from sequences to Database...".
           "</font><br />");
    if ($args->val('TESTMODE')) {
        print "<i>No database load will occur, as you have requested just to see the data being loaded</i><br />";
    }
    my @rnas;
    foreach my $bs (@bioseqs) {
        $pbg->add_bioseq( $bs );
        $pbg->analyze;
        push @rnas, $bs if ($bs->{_PBG_CRUFT_}{IS_RNA});
    }
    my $rc = $lh->row_count;
    printf("<font color='green'>".
           "%d row%s of metadata extracted".
           "</font></pre><font color='#006600'>", $rc, $rc == 1 ? '' : 's');
    
    $lh->write;
    print "</font></pre>\n";
    &MAP_BIOSEQS($lh, \@rnas) if ($#rnas > -1);


    $lh->process_ready;
    undef $pbg;
    undef $lh;
    print "<br />" if ($args->val('TESTMODE'));
    if (-s $cf) {
        print "<font color='red'>Errors were encounted while loading data:<pre>";
        open (CF, "<$cf") || die "FATAL PROGRAM ERROR - Could not read parsing errors from '$cf'\n  $!\n  ";
        while(<CF>) {
            print $_;
        }
        close CF;
        print "</pre></font>\n";
        
    } else {
        # warn $cf;
    }
}

sub MAP_BIOSEQS {
    my ($lh, $rnas) = @_;

    my $ndir = "/gcgblast/NOSHA/";
    my $fasta = $ndir . "NCBI_35_Unmasked_Chromosomes";
    return unless (-e $fasta);
    
    my $db  = Bio::DB::SeqHash::BtreeNew->new();
    my $alg = Bio::DB::SeqHash::Sim4->new();
    
}

sub request_to_seqs {
    my ($requests) = @_;
    my (@missing, @seqs);
    foreach my $request (@{$requests}) {
        my @found = $mt->get_seq( -name => $request, -nocreate => 1);
        if ($#found < 0) {
            # Ok, we failed in all attempts
            push @missing, $request;
            next;
        }
        push @seqs, @found;        
    }
    &SHOW_MISSING( \@missing );
    &WARN_EXCESSIVE( \@seqs );
    return @seqs;
}

sub INFO {
    my ($requests) = @_;
    my @seqs = &request_to_seqs($requests);
    return if ($#seqs < 0);
    foreach my $seq (@seqs) {
        $seq->read_lengths;
        $seq->read_classes;
        $seq->read_taxa;
        $seq->read_edges( -limit => $args->val('LIMITRELATE'),
                          -nodistinct => 1, );
        print $seq->to_html;
    }
}

sub NETONLY {
    my ($requests) = @_;
    my @seqs = &request_to_seqs($requests);
    return if ($#seqs < 0);
    
    my $path = $args->val('PATH');
    my $extra  = "";
    my $isauto = 0;
    if ($path =~ /^auto$/i || $path =~ /^automatic$/i) {
        ($path, $extra) = &AUTO_NET(\@seqs);
        $isauto = 1;
    }
    if ($path =~ /^full$/i) {
        my $nw = BMS::MapTracker::Network->new( -tracker => $mt );
        $nw->add_root( @seqs );
        foreach my $seq (@seqs) {
            $nw->expand( -node    => $seq,
                         -space   => $args->val('SPACE'),
                         -limit   => $args->val('LIMIT', 'LIMITRELATE'),
                         -groupat => 5,
                         -recurse => $args->val('RECURSE'), );
        }
        &SHOW_NETWORK( $nw );
    } else {
        my $pw = &PATH_WALK( $path, \@seqs, undef, $isauto);
        &SHOW_PATH( $pw );
        &NET_TO_EXCEL($pw->network);
    }
    print $extra;
}

sub PATH_WALK {
    my ($path, $seqs, $nw, $isauto) = @_;
    my $fullpath = "$pathdir/$path";
    unless (-e $fullpath) {
        $fullpath .= '.path';
        unless (-e $fullpath) {
            printf("%s<font color='red'>I could not find the PathWalker file '$fullpath'</font><br />\n", $help->make_link(74));
            return undef;
        }
    }
    my $vb = $args->val('DEBUG') ? 1 : 0;
    my $pw = BMS::MapTracker::PathWalker->new( -tracker => $mt,
                                               -verbose => $vb, );
    $pw->error( -output => 'html');
    my $pdat = $pw->load_path( $fullpath );
    my %pargs = ( pid      => $$,
                  limit    => $args->val('LIMITRELATE'),
                  cautious => $isauto,
                  recurse  => $depth );
    while (my ($tag, $val) = each %pargs) {
        $pw->param($tag, $val);
    }
    if ($nw) {
        $pw->network($nw);
    } else {
        $pw->clear_network;
    }
    foreach my $seq (@{$seqs}) {
        $pw->add_root( $seq );
    }
    $pw->walk;
    return $pw;
}

sub SHOW_PATH {
    my ($pw) = @_;
    printf("<font color='blue' size='+1'><b>%s</b> [%s]</font> ".
           "<font color='green' size='-1'>%s</font><br />\n",
           $pw->param('path_short') || "Unknown Network",
           $pw->param('path_name') || "File?",
           $pw->param('path_desc')  || "");
    my $nw = $pw->network;
    &SHOW_NETWORK( $nw, $pw );
}

sub SHOW_NETWORK {
    my ($nw, $pw) = @_;
    my $traverse = $nw->traverse_network
        ( -maxcluster => $args->val('MAXGROUP'),
          -labelwith  => $uselab ? 'is a shorter term for' : 0, );
    return unless ($traverse);


    my $basepath = $mt->file_path('TMP');
    my $priroot  = $nw->{ROOTS}[0];
    my $basefile = $priroot ? $priroot->name : "Network_" . $mt->new_unique_id;

    my $gp = lc($args->val('GVPROG') || "");

    my $fileinfo = &HANDLE_NETWORK_FILE( $nw, '' );
    if ($gp eq 'gml') {
        &WRITE_FLAT( $nw->to_gml( -traverse => $traverse ),
                     $basefile, 'gml');
    } elsif ($gp eq 'hypv') {
        &WRITE_FLAT( $nw->to_hyperview( -traverse => $traverse ),
                     $basefile, 'hypv');
    } elsif ($gp eq 'sif') {
        &WRITE_FLAT( $nw->to_sif( -traverse => $traverse ),
                     $basefile, 'sif');
    } else {
        my $string = $nw->to_graphviz_html( -program    => $gp,
                                            -traverse   => $traverse );
        print $string . "<br />\n";
        my ($kstr, $dummy) = $nw->graphviz_key
            ( -filename  => $$."_key" );
        print "<b>Key:</b><br />\n$kstr" if ($kstr);
        my $obj = $pw ? $pw : $nw;
        print $obj->show_html_options();
    }

    # Kludge
#    my @edges = sort { $a->node1->name cmp $b->node1->name ||
#                       $a->node2->name cmp $b->node2->name ||
#                       $a->space->id <=> $b->space->id } $nw->each_edge;
#    foreach my $edge (@edges) {
#        print $edge->to_html;
#    }
}

sub HANDLE_NETWORK_FILE {
    my ($nw, $sf) = @_;
    my $string = "";
    return $string unless ($nw);

    my ($file, $path);
    my $fso = $nw->{FILE_SOURCE};
    unless ($nw->{CURRENT_ITER}) {
        # Do NOT generate a new file if we are displaying an older
        # iteration of a loaded file
        if (my $nf = $args->val('NWFILE')) {
            unless ($sf) {
                my @bits = split(/\//, $nf);
                $sf = $bits[-1];
            }
            $file = $sf;
            $file =~ s/\.Mk(\d+)$//;
        } else {
            my $priroot = $nw->{ROOTS}[0];
            $file = $priroot ? $priroot->name :
                "Network_" . $mt->new_unique_id;
            $file =~ s/\W+/_/g;
            $file = "$file.net";
        }
        $file .= sprintf(".Mk%03d", $nw->{ITERATION} + 1);
        $path  = $mt->file_path('TMP') . $file;
        $fso   = $path;
        $args->setval('NWFILE', $path);
        $nw->to_flat_file( -file => $path );
        $string = sprintf
            ("%s<font color='green'>You may save the above network with ".
             "<a href='%s%s'>this link</a></font><br />\n", 
             $help->make_link(80), $mt->file_url('TMP'), $file);
    }
    if ($fso) {
        # Make links to view past iterations of the network:
        $path = $fso;
        my @bits = split(/\//, $path);
        $file = $bits[-1];
        my $maxiter = $nw->{MAX_ITER};
        if ($maxiter > 1) {
            my $curiter = $args->val('ITERATION') || $maxiter;
            $string .= $help->make_link(82) .
                "<b>View Network edit iteration:</b>";
            for my $i (1..$maxiter) {
                my $link = sprintf
                    ("<a href='mapTracker.pl?iteration=%d&nwfile=%s'>%d</a>",
                     $i, $fso, $i);
                $link = "<b>$i</b>" if ($i == $curiter);
                $string .= " $link";
            }
            $string .= "</br>\n";
        }
    }
#    $string .= "<script language='javascript'>\n  <!--\n";
#    $string .= "      netdata = { shortname:'$file', path:'$path' };\n";
#    $string .= "  // -->\n</script>\n";
    return $string;
}


sub AUTO_NET {
    my ($seqs) = @_;
    my %paths = ( all => {}, );
    $foundcount = $#{$seqs} + 1;
    foreach my $seq (@{$seqs}) {
        my $sn  = $seq->name;
        my $esn = uc($seq->namespace->name());
        #print "<p>$sn</p>";
        my @appropriate;
        if ($seq->is_class('Probe', 'Iconix Probe')) {
            push @appropriate, [ 'Probes', 2 ];
            push @appropriate, [ 'Coregulated_Probes', 2 * $foundcount /1.5 ];
            push @appropriate, [ 'Antiregulated_Probes',  2 * $foundcount/1.8];
            push @appropriate, [ 'Coregulated_Probes_Strict', 2 * $foundcount /1.6 ];
            push @appropriate, [ 'Coregulated_Probes_Loose', 2 * $foundcount /1.7 ];
        }
        
        my $check = $mt->get_edge_dump( -name => $seq,
                                        -space => 'Denorm_Locus',
                                        -keeptype => 'DENORM',
                                        -limit => 1);
        if ($esn eq 'CLASSES') {
            push @appropriate, ['Database_Structure', 20];
        }
        if ($seq->is_class('gDNA')) {
            push @appropriate, ['Chromosome', 1];
        }
        if ($seq->is_class('Chemical')) {
            push @appropriate, ['Chemical_Information', 1];
        }

        if ($seq->is_class('Drug')) {
            push @appropriate, ['FDA_Drug_Information', 2];
        }
        if ($seq->is_class('TradeName')) {
            push @appropriate, ['FDA_Drug_Information', 1];
        }

        if ($seq->is_class('BMS') && $seq->is_class('PERSON')) {
            push @appropriate, ['LDAP_map', 1];
            push @appropriate, ['LDAP_brief', $foundcount / 5 ];
        }
        if ($seq->is_class('bio', 'Locus', 'gi', 'genesymbol')) {
            if ($#{$check} < 0) {
                push @appropriate, ['RefSeq_net', 1];
            } else {
                push @appropriate, ['Denormalized_View', 1];
                push @appropriate, ['RefSeq_net', 0.2];
            }
            push @appropriate, [ 'Coregulated_Probes', $foundcount / 2.7  ];
            push @appropriate, [ 'Antiregulated_Probes', $foundcount / 2.8  ];
        }
        if ($seq->is_class('homologene')) {
            push @appropriate, ['Homologene', 10];
        }
        push @appropriate, [ 'Full', 0.1 ];
        $paths{$sn} = {};
        foreach my $apdat (@appropriate) {
            my ($ap, $inc) = @{$apdat};
            $paths{$sn}{$ap} += $inc;
            $paths{all}{$ap} += $inc;
        }
    }

    my @rank = sort {$paths{all}{$b} <=> $paths{all}{$a}} keys %{$paths{all}};
    my %allpaths = map { $_ => 1 } @rank;
    my $best = $rank[0];
    delete $paths{all};
    my $walked = 0;
    my $nw;
    my %remain_track = %{$known_paths};
    my $pw;

    my $path_name = 'Full';
    if ($best) {
        foreach my $seq (@{$seqs}) {
            delete $paths{$seq->name}{$best};
        }
        delete $allpaths{$best};
        $path_name = $best;
    }

    my %suggest = ();
    my @sns = keys %paths;
    foreach my $sn (@sns) {
        while (my ($ap, $count) = each %{$paths{$sn}}) {
            $suggest{$ap} += $count;
        }
    }
    delete $suggest{'Full'};
    my $allsn = join(",", @sns);
    my @allsug = sort {$suggest{$b} <=> $suggest{$a}} keys %suggest;
    my $text = "";
    if ($#allsug > -1) {
        $text .= sprintf($frm->{boldbr}, 'green', '+1', "The following ".
                         "custom views also may be appropriate:");
        foreach my $ap (@allsug) {
            $text .= &SUGGEST_PATH($allsn, $known_paths, $ap, $suggest{$ap});
            delete $remain_track{$ap};
        }
    }

=pod

    my @remain = keys %remain_track;
    if ($#remain > -1) {
        $text .= sprintf($frm->{boldbr}, 'green', '+1', "These custom ".
                         "views are available but may not be appropriate:");
        
        for (my $i = 4; $i <= $#{$kp_list}; $i += 2) {
            my $ap = $kp_list->[$i];
            next unless ($remain_track{$ap});
            $text .= &SUGGEST_PATH($allsn, $known_paths, $ap);
        }
    }

=cut

    
    $text .= "<p />";
    return ($path_name, $text);
}

sub SUGGEST_PATH {
    my ($allsn, $known_paths, $ap, $score) = @_;
    my $txt = "<li>";
    my $lab = $known_paths->{$ap} || "unknown path";
    $lab = sprintf("[%.1f] ", $score) . $lab if (defined $score);
    $txt .= sprintf($frm->{abr}, 'path', "$ap&seqnames=$allsn", $lab );
    $txt .= "</li>";
    return $txt;
}

sub SETGLOBALS {
    @cutargs  = ( -cutbridge => $args->val('CUTBRIDGE'));
    %minscores = ();
    $linkargs = "";
    foreach my $key ($args->each_key()) {
        $key = uc($key);
        next unless (exists $defaults->{$key});
        my $val = $args->val($key);
        next if ($defaults->{$key} eq $val);
        $val = 1 if ($val =~ /^ARRAY/);
        $linkargs .= "$key=$val".'&';
    }
    $linkargs =~ s/\&+/\&/g;
    $linkargs =~ s/\&$//;
    $linkargs =~ s/^\&//;
    $linkargs ||= 'Foo=1';

    $frm = {
        check  => "<input type='checkbox'  name='%s' value='1' %s/> %s<br />\n",
        check2 => "<input type='checkbox'  name='%s' value='1' %s/> %s ",
        chkgrp => "<input type='checkbox'  name='%s' value='%s' %s/> %s ",
        text   => "<input type='text' name='%s' value='%s' size='%d' />\n",
        agen   => "<a href='%s'>%s</a>\n",
        anew   => "<a href='%s' target='_blank'>%s</a>\n",
        abr    => "<a href='mapTracker.pl?$linkargs&%s=%s'>%s</a><br />\n",
        abrnew => "<a href='mapTracker.pl?$linkargs&%s=%s' target='othermap'>%s</a><br />\n",
        abrstr => "<a href='mapTracker.pl?$linkargs&%s=%s'>%s</a> %s<br />\n",
        submit => "<input type='submit' value='%s' name='%s'/>\n",
        tarea  => "<textarea name='%s' cols='%d' rows='%d'>%s</textarea>\n",
        post   => "<form action='mapBrowser.pl' method='post' enctype='multipart/form-data' name='mainfrm' id='mainfrm'>\n",
        col    => "<font color='%s'>%s</font>",
        colbr  => "<font color='%s'>%s</font><br />\n",
        bold   => "<font color='%s' size='%s'><b>%s</b></font>",
        boldbr => "<font color='%s' size='%s'><b>%s</b></font><br />\n",
        hidden => "<input type='hidden' name='%s' value='%s' />\n",
        radio  => "<input type='radio' name='%s' value='%s' %s/>\n",
        text   => "<input type='text' name='%s' value='%s' size='%d' />\n",
    };

    if ($args->val('DOCUT')) {
        # Set up map retrieval arguments
        my $docut = 0;
        if ($args->val('CUTAFFY')) {
            $minscores{'MicroBlast Cluster'} = $args->val('CUTAFFY');
            $docut = 1;
        }
        if ($args->val('CUTSIM4')) {
            $minscores{'Sim4'} = $args->val('CUTSIM4');
            $docut = 1;
        }
        if ($docut) {
            push @cutargs, ( -minscore => \%minscores);
            $filtermsg = "";
            my $what = 'direct';
            $what .= ' and scaffolded' if ($args->val('CUTBRIDGE'));
            $filtermsg .= sprintf
                ($frm->{boldbr}, 'blue', '+0', 
                 "Score filters applied to $what maps:");
            $filtermsg .= "<b><font color='brick'>";
            foreach my $key (sort keys %minscores) {
                $filtermsg .= sprintf("&nbsp;&nbsp;&nbsp;%s: %s<br />\n",
                                      $key, $minscores{$key});
            }
            $filtermsg .= "</font></b>\n";
        }
    }

}

sub MAPONLY {
    my ($requests) = @_;
    my @seqs;
    my @missing;
    foreach my $request (@{$requests}) {
        my ($name, $start, $end, $seq);
        my @found;
        if ($request =~ /^(.+)\:(\d+)-(\d+)$/) {
            # This looks like a specific range request
            ($name, $start, $end) = ($1, $2, $3);
            @found = $mt->get_seq(-name => $name, -nocreate => 1);
        }
        if ($#found < 0) {
            # Either no range request, or it failed
            # Use the full string as a name
            ($start, $end) = (undef, undef);
            $name = $request;
            @found = $mt->get_seq( -name => $name, -nocreate => 1);
        }
        if ($#found < 0) {
            # Ok, we failed in all attempts
            push @missing, $request;
            next;
        }
        if ($start) {
            # Set the user-defined range boundaries
            foreach my $seq (@found) {
                $seq->range( [$start, $end] );
            }
        }
        push @seqs, @found;
    }

    &SHOW_MISSING( \@missing );
    return if ($#seqs < 0);

    my %user_request = map { $_->id => 1 } @seqs;

    my @stack = @seqs;
    my @all_extra;
    while (my $seq = shift @stack) {
        $seq->read_classes;
        $seq->read_lengths;
        $seq->read_taxa;
        my @extra = &LOAD_MAP( $seq, \%user_request );
        &SCAFFOLD( $seq ) unless ($args->val('NOSCAFFOLD'));
        print "<hr />" . $seq->javascript_link() . "<font size='-1'>";

        my ($start, $end) = $seq->range;
        if ($start) {
            printf("<font color='brick'>%d - %d</font> ", $start, $end);
        }
        if ($#extra > -1) {
            # If request did not have map, but others possible, use them:
            printf("<i>Maps found for %d related sequence%s</i>",
                   $#extra + 1, $#extra == 0 ? '' : 's');
            unshift @stack, map { $_->[1] } @extra;
            push @all_extra, @extra;
        } elsif (my $count = $seq->map_count) {
            printf("%d mapping%s recovered", $count, $count == 1 ? '' : 's');
        } else {
            print "<font color='red'>No mappings found in database</font>";
        }
        print "</font><br />\n";
        &LIST_SEQ( $seq );
    }
    print "</table>" if ($isBriefTable);

    if ($#all_extra > -1) {
        @all_extra = sort { $a->[0] cmp $b->[0] } @all_extra;
        print $help->make_link(71);
        print "<font color='blue'>Some of your requests did not have maps for themselves, but did have mappings for related sequences:</font>";
        print "<pre>".join("\n", map { $_->[2] } @all_extra)."</pre>\n";
        
    }

    if ($mapsfound) {
        print "<table><tr><td bgcolor='#dddddd' nowrap='1'>\n";
        printf("<b><i>Settings applied to %d recovered Mapping%s:".
               "</i></b><br />\n", $mapsfound, $mapsfound == 1 ? '' : 's');
        print $help->make_link(57) . $filtermsg if ($filtermsg);
        if ($args->val('NOSCAFFOLD')) {
            print $help->make_link(21);
            printf($frm->{boldbr}, 'blue', '+0', 
                   "Only directly mapped features considered.");
        } else {
            print $help->make_link(20);
            printf($frm->{boldbr}, 'blue', '+0', 
                   "Both direct and scaffolded features considered");
        }
        print "</td></tr></table><br />\n";
    }
}

sub LOAD_MAP {
    my ($seq, $requested) = @_;
    my @foundmap = &READ_MAPS($seq);;
    return () if ($#foundmap > -1);

    # Keep track of additional sequences we recovered
    $requested ||= {};
    my $check = BMS::MapTracker::Network->
        new( -root => $seq, -tracker => $mt );

    # We did not find any maps - are there other sequnces we could use?
    if ($seq->is_class('locus')) {
        my @nodes = ( $seq );
        if ($seq->is_class('Gene Symbol')) {
            my @found;
            # Directly connected loci:
            push @found, $check->explicitly_expand
                ( -edgelist => ['reliable'],
                  -limit    => $args->val('LIMITRELATE'), );
            # Loci two steps distant:
            push @found, $check->explicitly_expand
                ( -edgelist => ['is a lexical variant of',
                                'reliable'],
                  -limit    => $args->val('LIMITRELATE'), );
            @nodes = ();
            foreach my $node (@found) {
                push @nodes, $node if ($node->is_class('locus'));
            }
        }
        foreach my $node (@nodes) {
            my @kept = $check->explicitly_expand
                ( -node      => $node,
                  -edgelist  => ['is a locus containing',
                                'unversioned'],
                  -groupat   => 100,
                  -complete  => 0,
                  -keepclass => 'rna' );
        }
        # print "<pre>"; $check->to_flat_file; print "</pre>";
        return &NET_TO_LIST( $check, $requested);
    }

    my @consider = ('is an unversioned accession of',
                    'is a reliable alias for',
                    'PRIORVERSION',
                    'is the same as' );
    $check->expand( -recurse => 3,
                    -keeptype => \@consider );

    return &NET_TO_LIST( $check, $requested);

}

sub READ_MAPS {
    my ($seq) = @_;
    return (1) if ($seq->{_MAPS_READ_});
    my @foundmap;
    my $overlap  = $args->val('OVERLAP');
    $overlap   ||= $seq->range;
    my @readargs = ( -overlap => $overlap,
                     -limit   => 500, );
    if ($seq->is_class('gdna')) {
        # Get variant maps seperately for gDNA
        push @foundmap, $seq->read_mappings
            (@readargs,  @cutargs, 
             -tossclass => 'variant',
             -force     => 1);
        push @foundmap, $seq->read_mappings
            (@readargs,  @cutargs, 
             -keepclass => 'variant',
             -tossclass => 'repetitive',
             -force     => 1);
    } else {
        @foundmap = $seq->read_mappings(@readargs, @cutargs);
    }
    $mapsfound += $#foundmap + 1;
    $seq->{_MAPS_READ_} = 1;
    return @foundmap;
}

sub NET_TO_LIST {
    my ($check, $requested) = @_;
    my %try = ();
    my ($root) = $check->each_root;
    foreach my $sid ($check->all_seq_ids) {
        # If this is one of the sequences already requested, skip for now:
        next if ( $requested->{ $sid } );
        my $oseq = $check->node( $sid );
        $oseq->range( $root->range );
        my @newmaps = &READ_MAPS($oseq);
        next if ($#newmaps < 0);
        # The related sequence has some maps, so we will include it:
        $try{ $sid } = $oseq;
    }
    my @goodmap = values %try;
    my @extraseqs;
    if ($#goodmap > -1) {
        foreach my $oseq (@goodmap) {
            my $path = $check->find_path( -end => $oseq);
            $path->save_best;
            my ($best) = $path->paths_as_html;
            next unless ($best);
            my $key = $oseq->name;
            if ($key =~ /(.+)\.(\d+)$/) {
                $key = sprintf("%s.%06d", $1, $2);
            }
            push @extraseqs, [ $key, $oseq, $best];
        }
    }

    @extraseqs = sort { $a->[0] cmp $b->[0] } @extraseqs;
    return @extraseqs;
}

sub HTMLSTART {
    return if ($args->val('NOCGI'));
    my $prog = $0;
     $isbeta = 1 if ($prog =~ /working/);
    print "<html><head>\n";
    print "<title>MapTracker Browser</title>\n";
    print "<link rel='shortcut icon' href='/biohtml/images/MapTracker_Small.gif'>\n";
    if ($mt) {
        print $mt->html_css();
        print $mt->javascript_head();
    }
    print "</head><body bgcolor='white'><center>\n";
    printf($frm->{boldbr}, 'orange', '+3', 'MapTracker Browser');
    if ($isbeta) {
        print $mt->help->make_link(1) if ($mt);
	print "<font color='red'>*** THIS VERSION IS BETA SOFTWARE ***</font><br />\n";
        print "<script> jsmtk_root_object.add_event( window, 'load', function() { document.body.appendChild(jsmtk_error_list) })</script>\n";
    }
#    print $mt->help->make_link(76) if ($mt);
#    print &SHOW_CVS_VERSION($VERSION, $args->val('SHOW_CVS_VERS') ) ."<br />\n";
    print "<font color='brick' size='-1'>";
    print $mt->help->make_link($ldap ? 2 : 3) if ($mt);
    printf("<i>In use by %s</i></font><br />\n", $ldap);
    print "</center>";
}
sub HTMLEND {
    return if ($args->val('NOCGI'));
    $mt->register_javascript_cache();
    print $mt->javascript_data( 'html' );    
    print "</body></html>\n";
}

sub FORM_FINDALL {
    return if ($args->val('NOCGI'));
    if ($focus eq 'net') {
        &FORM_FIND_NET;        
    } elsif ($focus eq 'loa') {
        &FORM_LOAD;        
    } elsif ($focus eq 'inf') {
        &FORM_INFO;        
    } else {
        &FORM_FIND_MAP;
    }
    if ( $admin->{$ldap} ) {
        &FORM_ADMIN  ;
        &FORM_DUMPSQL;
    }
    print "</form>\n";
    &FORM_CVS;
}

sub FORM_HEADER {
    return if ($args->val('NOCGI'));
    my ($hid, $label, $subtxt) = @_;
    print $frm->{post};
    print "<table width='$tabwid' ><tr><td width='$tabwid' bgcolor='#ffff33'>";
    print "<center>" . $help->make_link($hid);
    print "Current focus: <b><font size='+1' color='blue'>$label</font></b>";
    print "<br />Switch Focus: ";
    for (my $i = 0; $i <= $#foci; $i += 2) {
        my ($key, $desc) = ($foci[$i], $foci[$i+1]);
        next if ($key eq $focus);
        printf($frm->{submit}, $desc, 'switchto');
    }
    print "</center>";
    print "</td></tr>\n";
    #print "<tr><td width='$tabwid' bgcolor='#ff9966'>&nbsp;</td></tr>";
    print "</table>\n";

    print "<table width='$tabwid'>\n";
    print "<tr><td valign='top'>\n";
    printf($frm->{submit}, $subtxt, 'do'.$focus);
    printf($frm->{hidden}, 'focus', $focus);
}

sub TEXT_AREA {
    my ($hid, $label, $key) = @_;
    print "<br />\n";
    print $help->make_link($hid);
    print "<b>$label:</b> ";
    print "<i><a onclick='javascript:document.mainfrm.".
        lc($key).".value=\"\";' href='#'>clear</a></i>";
    print "<br />\n";
    print "<textarea name='$key' cols='60' rows='10' id='".lc($key)."'>";
    print $args->formval($key);
    print "</textarea><br />\n";
    return;
}

sub FORM_NAME_INPUT {
    my ($label) = @_;
    print "<br />\n";
    print $help->make_link(94);
    print "<b>$label:</b> ";
    print "<i><a onclick='javascript:document.mainfrm.snms.value=\"\";' ".
        "href='#'>clear</a></i>";
    print "<br />\n";
    print "<textarea name='seqnames' cols='60' rows='10' id='snms'>";
    print $args->val('SEQNAMES');
    print "</textarea>\n";
    

    print"</td></tr></table>\n";
    return;
}

sub FORM_LOAD {
    &FORM_HEADER(95, "Load Data into Database", "Upload your request");
    &TEXT_AREA(96, "Fetch Sequence data from SeqStore", 'fetch' );
    print $help->make_link(97);
    printf($frm->{check},'safety', $args->val('SAFETY') ? 'CHECKED ':"",
	   "<b>Safety - you must check this box to load data</b>", );
}

sub FORM_INFO {
    &FORM_HEADER(999, "Retrieve Basic Information", "Retrieve Info");
    &TEXT_AREA(96, "One or more names or identifiers", 'seqnames' );
}

sub FORM_CVS {
    return if ($args->val('NOCGI'));
    print "<a href='mapBrowser.pl?SHOWCV=1'>Show Controlled Vocabularies</a><br />\n";
}

sub FORM_FIND_NET {
    &FORM_HEADER(60, "Network Construction", "Build Network");
    &TEXT_AREA(98, "One or more names", 'seqnames');

    print $help->make_link(61);
    print "<b>Network Path Rules:</b> ";
    print "<select name='path'>\n";
    my $path = $args->val('PATH') || "";
    my $matched;
    for (my $i = 0; $i <= $#{$kp_list}; $i += 2) {
        my $seltag = '';
        if ($path eq $kp_list->[$i]) {
            $seltag = 'SELECTED ';
            $matched = 1;
        }
        printf("  <option value='%s' %s>%s</option>\n", 
	       $kp_list->[$i], $seltag, $kp_list->[$i+1]);
    }
    if (!$matched && $args->val('PATH')) {
        # None of the stock paths matched, use the one provided
        printf("  <option value='%s' %s>%s</option>\n", 
	       $args->val('PATH'), 'SELECTED ', $args->val('PATH'));
    }
    print "</select><br />\n";
}

sub FORM_FIND_MAP {
    return if ($args->val('NOCGI'));
    &FORM_HEADER(4, "Coordinate Mappings", "Download Mapping Data");
    &TEXT_AREA(99, "One or more names or locations", 'seqnames');

    print "<br />\n";
    print $help->make_link(100);
    printf($frm->{check}, 'splitspace', $args->val('SPLITSPACE') ? 'CHECKED ':"",
	   "Spaces are also used to separate names" );

    print $help->make_link(101);
    printf($frm->{hidden}, 'noscaffold', 0);
    printf($frm->{check}, 'noscaffold', $args->val('NOSCAFFOLD') ? 'CHECKED ':"",
	   "Only get direct mappings" );

#    print "</td></tr><tr><td valign='top'>\n";

    print $help->make_link(26);
    print "<b>Show map data as:</b><br />\n";
    print "&nbsp;&nbsp;&nbsp;";

    printf($frm->{radio}, 'showmap', 'full table',
	   $args->val('SHOWMAP') =~ /full/i ? 'CHECKED ':"", );
    print "Full table ";

    printf($frm->{radio}, 'showmap', 'brief integrated',
	   $args->val('SHOWMAP') =~ /brief int/i ? 'CHECKED ':"", );
    print "Integrated table ";

    printf($frm->{radio}, 'showmap', 'brief', $args->val('SHOWMAP') =~ /brief/i 
           && $args->val('SHOWMAP') !~ /integr/i? 'CHECKED ':"", );
    print "Brief table ";

    printf($frm->{radio}, 'showmap', 'image',
	   $args->val('SHOWMAP') =~ /image/i ? 'CHECKED ':"", );
    print "Image, size ";
    printf($frm->{text}, 'imagewidth', $args->formval('IMAGEWIDTH'), 4);
    print "px<br />\n";

    print $help->make_link(56);
    printf($frm->{hidden}, 'docut', 0);
    printf($frm->{check},'docut', $args->val('DOCUT') ? 'CHECKED ':"",
	   "<b>Filter mappings by score:</b>", );
    # print "<b>Do not display maps with scores below:</b><br />\n";
    print "&nbsp;&nbsp;&nbsp;";
    print "MicroBlast: ";
    printf($frm->{text}, 'cutaffy', $args->formval('CUTAFFY'), 4);
    print "&nbsp;&nbsp;&nbsp;";
    print "Sim4: ";
    printf($frm->{text}, 'cutsim4', $args->formval('CUTSIM4'), 4);
    print "&nbsp;&nbsp;&nbsp;";
    printf($frm->{check},'cutbridge', $args->val('CUTBRIDGE') ? 'CHECKED ':"",
	   "Apply to scaffolds", );

    print"</td></tr></table>\n";
    return;
}

sub NET_TO_EXCEL {
    my ($nw) = @_;
    my @roots = $nw->each_root;
    my @nodes = $nw->each_node;
    foreach my $root (@roots) {
        $nw->find_all_distances( $root );
    }
    my @refs;
    foreach my $node (@nodes) {
        my $nid = $node->id;
        my $min = 9999999;
        my $ref;
        foreach my $root (@roots) {
            my $rid = $root->id;
            my $dist = $nw->distance($rid, $nid);
            # Skip if this node is not connected to this root
            next unless (defined $dist);
            if ($dist < $min) {
                $min = $dist;
                $ref = $root;
            }
        }
        push @refs, [ $node, $ref, $min ] if ($ref);
    }
    @refs = sort { $a->[1]->name cmp $b->[1]->name ||
                       $a->[2] <=> $b->[2] ||
                       $a->[0]->name cmp $b->[0]->name } @refs;
    &INIT_EXCEL;
    my @opts = (-freeze  => 1,
                -columns => [ 'Name','Classes', 'Provenance', 'Distance' ], );

    $eh->sheet( -name    => "Network Nodes", @opts);
    $eh->sheet( -name    => "Network Edges",
                -freeze  => 1,
                -columns => [ 'Name','Connected With', 'Name', 'Authority',
                              'Preferred Connection', 'Distance'] );

    $eh->sheet( -name    => "Text Nodes", @opts);

    $eh->sheet( -name    => "Classes",
                -freeze  => 1,
                -columns => [ 'Name','Class', 'Authority' ], );

    $eh->sheet( -name    => "Taxa",
                -freeze  => 1,
                -columns => [ 'Name','Taxa ID', 'Species Name' ], );

    foreach my $dat (@refs) {
        my ($node, $root, $dist) = @{$dat};
        my $name = $node->name;
        my $prov = "User Query";
        if ($dist > 0) {
            $prov = "Near " . $root->name;
        }
        my $classes = $node->each_class( 'hash name');
        my @names   = sort {uc($a) cmp uc($b) } keys %{$classes};
        my $cnames  = join(", ", @names);
        my $sheet = (length($name) > 20 || $node->is_class('text')) ?
            'Text Nodes' : 'Network Nodes';
        $eh->add_row($sheet, [ $name, $cnames, $prov, $dist ]);
        foreach my $cname (@names) {
            my $auths = join(", ", sort { uc($a) cmp uc($b) } map 
                             { $_->name } @{$classes->{$cname}});
            $eh->add_row('Classes', [ $name, $cname, $auths ]);
        }
        my $nid = $node->id;
        foreach my $edge ($nw->edges_from_node($node)) {
            my $auths = join(", ", $edge->each_authority_name);
            if (my ($pet) = $edge->each_tag('Preferred Edge')) {
                # This is a denormalized edge
                my $pe = $pet->valname;
                my ($reads, $node1, $node2) = $edge->reads();
                my ($dt) = $edge->each_tag('Denormalization');
                $dist = $dt ? $dt->num || 1 : 1;
                $eh->add_row('Network Edges',
                             [ $node1->name, $reads, $node2->name, $auths, 
                               $pe, $dist ]);
            } else {
                my ($reads, $refAgain, $child) = $edge->reads($node);
                $eh->add_row('Network Edges',
                             [ $name, $reads, $child->name, $auths ]);
            }
        }
        foreach my $taxa ($node->each_taxa) {
            $eh->add_row('Taxa',
                         [ $name, $taxa->id, $taxa->name ]);
        }
    }
}

sub INIT_EXCEL {
    my $file = "MapTracker_Summary_$$.xls";
    $eh = BMS::ExcelHelper->new($mt->file_path('TMP') . $file);

    $eh->url($mt->file_url('TMP') . $file);

    $eh->sheet( -name    => "Help",
                -freeze  => 1,
                -columns => [ 'Column / Worksheet','Contains', 'Description' ],
                -width   => [ 15, 30, 120 ], );

    $eh->add_row('Help', [ 'Network Nodes', 'Sheet: All nodes in network', 'A worksheet, one row per node; Indicates the distance to the closest root' ]);

    $eh->add_row('Help', [ 'Text Nodes', 'Sheet: All long nodes in network', 'A worksheet; Like above, but contains those nodes that are long (tend to be less useful text descriptions)' ]);

    $eh->add_row('Help', [ 'Network Edges', 'Sheet: All connections in network', 'A worksheet, one row per edge; Lists both nodes, how they are connected, and the authorities making the claim' ]);

    $eh->add_row('Help', [ 'Classes', 'Sheet: All class assignments', 'A worksheet, one row per class assignment; Lists a node, a class, and the authorities. A node may have multiple rows in this sheet' ]);

    $eh->add_row('Help', [ '' ]);

    $eh->add_row('Help', [ 'Name', 'The name of an entity', 'Names are the central component of MapTracker - they are identifiers that can be classified or connected to each other' ]);
    $eh->default_width('name', 24);

    $eh->add_row('Help', [ 'Provenance', 'How a name got here', 'Indicates why a name was included in the spreadsheet - did the user request it, or was it discovered during analysis?' ]);
    $eh->default_width('Provenance', 20);
    
    $eh->add_row('Help', [ 'Distance', 'Number of edges between nodes', 'How far away two nodes (names) are in a network' ]);
    $eh->default_width('Distance', 10);
    
    $eh->add_row('Help', [ 'Class', 'A class assignment', "Classes describe the sort of thing that a name is - examples include 'mRNA', 'Chemical Entity', 'Cell Type'" ]);
    $eh->default_width('Class', 15);
    $eh->default_width('Subject Class', 15);

    $eh->add_row('Help', [ 'Classes', 'Multiple classes', "As above, but could contain multiple class listings" ]);
    $eh->default_width('Classes', 45);
    
    $eh->add_row('Help', [ 'Authority', 'Who or what assigned the data', "Authorities are usually people or organizations, but can be software programs. They are the source of the displayed data." ]);
    $eh->default_width('Authority', 20);
    $eh->default_width('Authorities', 40);
    
    $eh->add_row('Help', [ 'Taxa ID', 'NCBI Taxa ID', "An integer code used by NCBI to identify taxa. In wide use as a species classification system" ]);
    $eh->default_width('Taxa ID', 6);
    
    $eh->add_row('Help', [ 'Species Name', 'Genus and species', "The scientific name of the species. In some cases may include subspecies." ]);
    $eh->default_width('Species Name', 30);
    
    $eh->add_row('Help', [ 'Length', 'Maximum length of a sequence', "Some names refer to biological sequences, and may have a length assigned - units are not provided" ]);
    $eh->default_width('Length', 8);
    
    $eh->add_row('Help', [ 'Connected With', 'An edge between two names', "This is a directional edge between the name on the left and the name on the right" ]);
    $eh->default_width('Connected With', 35);
    
    $eh->add_row('Help', [ 'Preferred Connection', 'Best denormalized edge', "Similar to 'Connected With', but used when one or more edges have been collapsed to a single denormalized edge" ]);
    $eh->default_width('Preferred Connection', 35);
    
    $eh->add_row('Help', [ 'Other Name', 'The other name in an edge', "When edges are reported, 'Name' will be on the left of the edge, and 'Other Name' on the right'" ]);
    $eh->default_width('Other Name', 20);
    
    $eh->add_row('Help', [ 'Edge Token', 'Non-directional edge type', "'Connected With' is usually a more readable column for the edge. 'Edge Token' allows you to sort edges irrespective of their directionality, though." ]);
    $eh->default_width('Edge Token', 20);
    
    $eh->add_row('Help', [ 'Query', 'Requested name in a mapping', "'Query' is used to indicate the name you requested in a mapped alignment" ]);
    $eh->default_width('Query', 24);
    
    $eh->add_row('Help', [ 'Subject', 'Discovered name in a mapping', "'Subject' is used to indicate the name found in a mapped alignment (using your query)" ]);
    $eh->default_width('Subject', 24);
    
    $eh->add_row('Help', [ 'Score', 'An alignment score', "A score value assigned to a mapping between your query and the indicated subject. Note that units are not shown. Most scores are percent match, but Affy probe sets will be number of probes matched to genome." ]);
    $eh->default_width('Score', 8);
    
    $eh->add_row('Help', [ 'Strand', 'Relative strand of the alignment', "If +1 (1), then the query and subject are oriented in the same direction. If -1, then they are oriented in opposite directions." ]);
    $eh->default_width('Strand', 6);
    
    $eh->add_row('Help', [ 'HSP Count', 'Number of HSPs in alignment', "The number of High Scoring Pairs in an alignment. Each HSP *might* be conceptually the same as an exon, but there is no gurantee of this." ]);
    $eh->default_width('HSP Count', 6);
    
    $eh->add_row('Help', [ 'Query Start', 'Alignment start position on query', "This is the first (smallest) query coordinate in the alignment" ]);
    $eh->default_width('Query Start', 9);
    
    $eh->add_row('Help', [ 'Query End', 'Alignment end position on query', "This is the last (largest) query coordinate in the alignment" ]);
    $eh->default_width('Query End', 9);
    
    $eh->add_row('Help', [ 'Query Span', 'Total width of query in alignment', "The number of bases spanned across query (end - start + 1)" ]);
    $eh->default_width('Query Span', 9);
    
    $eh->add_row('Help', [ 'Query Coords', 'Query Coordinates', "Single column displaying both start and end of query. If start and end are the same, one coordinate will be shown. If the location is a gap, that will be indicated." ]);
    $eh->default_width('Query Coords', 15);
    
    $eh->add_row('Help', [ 'Subject Start', 'Alignment start position on subject', "This is the first (smallest) subject coordinate in the alignment" ]);
    $eh->default_width('Subject Start', 12);
    
    $eh->add_row('Help', [ 'Subject End', 'Alignment end position on subject', "This is the last (largest) subject coordinate in the alignment" ]);
    $eh->default_width('Subject End', 12);
    
    $eh->add_row('Help', [ 'Subject Span', 'Total width of subject in alignment', "The number of bases spanned across subject (end - start + 1)" ]);
    $eh->default_width('Subject Span', 15);
    
    $eh->add_row('Help', [ 'Subject Coords', 'Subject Coordinates', "Single column displaying both start and end of subject. If start and end are the same, one coordinate will be shown. If the location is a gap, that will be indicated." ]);
    $eh->default_width('Subject Coords', 20);
    
    $eh->add_row('Help', [ 'Transform', 'Coordinate transform in alignment', "A tag indicating how the query and subject are mapped to each other. Direct is 1:1, Translate is 1:3, and 'Bridge' indicates that the mapping is indirect via a common sequence (usually genomic DNA such as a full chromosome)" ]);
    $eh->default_width('Transform', 30);
    
    $eh->add_row('Help', [ 'Map ID', 'Internal database ID for mapping', "Probably not of use for most users. It is a primary key for the MapTracker database." ]);
    $eh->default_width('Map ID', 10);
    
    $eh->add_row('Help', [ 'Search Database', 'The database used in alignment', "The database used to perform the alignment. It could be direct assignment from a third party, or a search algorithm between one or more queries and a sequence database." ]);
    $eh->default_width('Search Database', 40);
    
    $eh->add_row('Help', [ '', '', "" ]);
    $eh->default_width('', 20);
    
    print $help->make_link(90);
    print "<font color='green'>Output is being redirected to an excel file...</font><br />\n";
}

sub FORM_DUMPSQL {
    print $help->make_link(27) . "<b>SQL Dump:</b> <select name='dumpsql'>\n";
    my $sqllvl = { 0 => 'None',
		   1 => 'Major Only',
		   2 => 'Expanded',
		   3 => 'Full SQL', };

    for my $i (0..3) {
	printf("  <option value='%d'%s>%s</option>\n",$i, 
	       $args->val('DUMPSQL') == $i ? ' SELECTED' : '', $sqllvl->{$i});
    }
    print "</select>\n";
    print "<br />&nbsp;&nbsp;&nbsp;";
    printf($frm->{check},'showbench', $args->val('SHOWBENCH') ? 'CHECKED ':"",
	   "Show benchmarks", );
}

sub FORM_ADMIN {
    print "<table><tr><th width='$tabwid' bgcolor='#ff3300'>\n";
    print $help->make_link(88) . "Administration";
    print "</th></tr>\n";
    print "<tr><td valign='top'>\n";

    printf($frm->{check},'debug', $args->val('DEBUG') ? 'CHECKED ':"",
	   "<b>Turn on debugging</b>", );
    print"</td></tr></table>\n";
    return; 
}

sub SCAFFOLD {
    my ($seq) = @_;
    if ($seq->is_class( 'protein' )) {
        # For proteins, use mRNA alignments as first scaffold.
        $seq->scaffold( @cutargs,
                        -usebridge => [ 'mrna' ], 
                        -limit     => $args->val('LIMITRELATE'),  );
    }

    my @scafargs = (@cutargs,
                    -usebridge  => [ 'gdna' ], 
                    -limit      => 1000,
                    -genomevers => $gvers);
    if ($seq->is_class('gdna')){
        my @repeats = ('', 'repetitive', 'very repetitive', 'extremely repetitive');
        push @scafargs, ( -tossclass => [ 'gdna'],);
        
    }  elsif ($seq->is_class( 'genomicregion' )) {
        # No variants for genomic regions
        push @scafargs, ( -tossclass => [ 'variant', ],);
        print $help->make_link(54);
        printf($frm->{boldbr}, 'orange', '+1', 
               "'Variants' not recovered for ". $seq->name);

    } elsif (!$args->val('GETXENO')) {
        push @scafargs, ( -tossclass => [ 'gdna'],);
    }
    $seq->scaffold( @scafargs  );
}

sub LIST_SEQ {
    my ($seq) = @_;

    unless ($doneOnce) {
        if ($isBriefTable) {
            print "<table border='1'><tr><th colspan='12' bgcolor='tan'>".
                sprintf("Mapping summary for %d Quer%s</th></tr>\n",
                        $#_ + 1, $#_ == 0 ? 'y' : 'ies');
            print &BMS::MapTracker::Mapping::html_row_header(1,1);
        }
        $doneOnce = 1;
    }

    if ($eh) {
        $seq->to_excel( $eh, "User Query");
        return;
    }
    my $sm = $args->val('SHOWMAP');
    if ($sm =~ /image/i && !$fp) {
	my $opts = $mt->friendlyPanelOptions;
        while (my ($pkey, $hash) = each %{$opts}) {
            $hash->{'-fgcolor'} = \&BMS::FriendlyPanel::color_by_score;
        }
	my $dblm = BMS::DBLinkManager->new
	( -map => {  
	    'snptracker' => "http://bioinformatics.bms.com/go/snptracker?snpacc=%s", 
	    'maptracker' => "mapTracker.pl?$linkargs&%s",
	    'mapbrowser' => "mapBrowser.pl?seqname=%s",
	    'blis' => "http://blis.hpw.pri.bms.com/Homo_sapiens/contigview?%s",
	    'LSFSNP' => "http://nunu.hpw.pri.bms.com/lifeseqV2bin/cgi-bin/wgetz?-id+5pd_h1L3QE3+-e+[LSF_OCT02_SNP:%s]",
	},);
	$fp = BMS::FriendlyPanel->new
            ( -opts  => $opts,
              -path => $mt->file_path('TMP'),
              -url  => $mt->file_url('TMP'),
              -dblm  => $dblm,
              -width => $args->val('IMAGEWIDTH'),
              -autocollapse => {
                  default   => [15, 50, 200],
                  affyset   => [100, 100, 200],
                  variant   => [35, 200, 2000 ],
              } );
    }

    my %seqDrawn;
    $sm = 0 if ($args->val('SHOWMAP') =~ /integ/i);
    print $seq->show_maps( -showmap => $sm, 
                           -fp      => $fp,
                           %{$args} ); 
    $fp->addJava( -linkcols => 1 ) if ($fp);
}


sub controlled_vocabularies {
    print "<div id='clas' class='TabbedPane' pane='cvs' tab='Classes'>\n";
    print $mt->classTreeHTML();
    print "</div>";

    print "<div id='rel' class='TabbedPane' pane='cvs' tab='Edge Types'>\n";
    print $mt->typeTableHTML();
    print "</div>";
    
    print "<div id='ns' class='TabbedPane' pane='cvs' tab='NameSpaces'>\n";
    print $mt->spaceTableHTML();
    print "</div>";
    
    print "<div id='auth' class='TabbedPane' pane='cvs' tab='Authorities'>\n";
    print $mt->authorityTableHTML();
    print "</div>";
    
    print "<div id='sdb' class='TabbedPane' pane='cvs' tab='Search DBs'>\n";
    print $mt->searchdbTableHTML();
    print "</div>";
    
}
