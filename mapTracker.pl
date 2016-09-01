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
use BMS::MapTracker;
use BMS::MapTracker::Network;
use BMS::MapTracker::PathWalker;
use BMS::FriendlyPanel;
use BMS::DBLinkManager;
use BMS::MapTracker::Mapping;
use BMS::ExcelHelper;
use BMS::BmsArgumentParser;

my $autoadded = {};
my $pathdir = "/stf/biocgi/tilfordc/working/maptracker/MapTracker/Paths";


my $admin = { tilfordc => 1, siemersn => 1, limje => 1, bruccolr => 1,
              hinsdalj => 1, };

my $kp_list = 
    [ Automatic            => "Automatic - Program decides best path rules",
      None                 => "None - Do not draw a network",
      Full                 => "Full Network - Expand with options set below",
      LDAP_map             => "Reporting Structure",
      LDAP_brief           => "Compact Reporting Structure",
      RefSeq_net           => "Gene-centric data",
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

my $known_paths = { @{$kp_list} };

my $args = BMS::BmsArgumentParser->new
    ( -nocgi       => $ENV{'HTTP_HOST'} ? 0 : 1,
      -errormail   => 'charles.tilford@bms.com',
      -dumpsql     => 0,
      -genomevers  => "all",
      -whichvers   => "blis",
      -clustermax  => 100000,
      -clusterpad  => 1000,
      -seqvers     => 'all',
      -seqmap      => 1,
      -showmap     => 'image',
      -imagewidth  => 700,
      -genomelimit => 200,
      -debug       => 0,
      -getclass    => 1,
      -getlength   => 1,
      -gettaxa     => 1,
      -gettype     => 1,
      -getmaps     => 'scaffold',
      -showbench   => 0,
      -docut       => 1,
      -cutaffy     => 8,
      -cutsim4     => 95,
      -cutbridge   => 0,
      -congeal     => 0,
      -congdist    => 25,
      -autoinfo    => 'Network',
      -path        => 'Automatic',

      -netsize     => 10,
      -nwfile      => "",
      -limitrelate => 50,
      -maxdepth    => 1,
      -maxrelation => 50,
      -maxgroup    => 5,
      -sametaxa    => 1,
      -settaxa     => "",
      -hierdepth   => 3,
      -gvdata      => 'all',
      -gvcustom    => '',
      -uselabel    => 1,
      -helptext    => '',
      -helptype    => '',
      -helpid      => 0,
      -seqname     => "",
      -maponly     => "",
      -seqid       => 0,
      -seqnames    => "",
      -clusterseq  => "",
      -overlap     => 0,
      -edit        => 0,
      -limit       => 20,
      );

$args->xss_protect("i|b|em|u");

my $nocgi     = $args->val(qw(nocgi));
my $depth     = $args->val(qw(maxdepth)); $depth = 1 unless (defined $depth);
$depth--;

my $uselab    = $args->val(qw(USELABEL));
my $maxRel    = $args->val(qw(maxrelation));
my $maxGroup  = $args->val(qw(maxgroup));
my $showMap   = $args->val(qw(showmap)) || "";
my $getMaps   = $args->val(qw(getmaps)) || "";
my $doDebug   = $args->val(qw(debug))   || "";
my $showBench = $args->val(qw(SHOWBENCH)) || 0;

my %stuff;

if ($getMaps =~ /none/i || $getMaps eq '0') {
    $getMaps = $args->set_param('getmaps', 0);
} elsif ($getMaps =~ /direct/i || $getMaps eq '1') {
    $getMaps = $args->set_param('getmaps', 1);
} else {
    $getMaps = $args->set_param('getmaps', 2);
}

my $ldap = $args->ldap();
$args->set_mime( -mail     => 'charles.tilford@bms.com',
                 -codeurl  => "http://bioinformatics.bms.com/biocgi/filePod.pl?module=_MODULE_&highlight=_LINE_&view=1#Line_LINE_",
                 -errordir => '/docs/hc/users/tilfordc/' )
    unless ($nocgi);

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

my $tabwid = 600;

my $autoinfo = $args->val(qw(autoinfo));
if ($autoinfo =~ /net/i) {
    $autoinfo = 'Network';
} elsif ($autoinfo =~ /excel/i) {
    $autoinfo = 'Excel';
} else {
    $autoinfo = 'Always';
}

# GLOBAL VARIABLES:
my (%minscores, @cutargs, $linkargs, $gvprogs, $gvers,
    $bestvers, $blisvers, @ncbi_vers, $filtermsg, $mapsfound, $frm);

my $argFile   = "/stf/biohtml/tmp/MT-Arguments-$$-";
my $argIter   = 1;
while (1) {
    my $try = $argFile .  $argIter . '.param';
    unlink($try);
    if (-e $try) {
        $argIter++;
        last if ($argIter > 100);
        next;
    }
    if (open(ARGF, ">$try")) {
        print ARGF $args->to_text
            ( -nodefaults => 1,
              -comment => "User parameter file for $ldap in mapTracker.pl");
        close ARGF;
        chmod(0666, $try);
        $linkargs = "valuefile=$try";
    } else {
        $args->err("Failed to write parameter file", $try, $!);
    }
    last;
}
$linkargs ||= "novaluefile=1";

my $mt;
eval {
    $mt = BMS::MapTracker->new( -dumpsql  => $args->val(qw(DUMPSQL)),
                                -dumpfh   => *STDOUT, 
                                -dumplft  => "<pre>",
                                -dumprgt  => "</pre>\n",
                                -username => $ldap,
                                -userdesc => "BMS Username",
                                -ishtml   => 1,
                                );
};

$mt->{DEBUG} = 1 if ($doDebug && $mt);
my $eh;

&SETGLOBALS;

my $selfurl  = sprintf("http://%s%s", $args->host(), $ENV{SCRIPT_NAME});
my ($reqcount, $foundcount) = (0,0);

print "<pre><b>Library Paths:</b>\n".join("\n", @INC)."</pre>" 
    if ($doDebug);

my $isbeta = 0;
my $dofind = 1;
&HTMLSTART;
unless ($mt) {
    print "<center><h1><font color='red'>MapTracker Database Unavailable</font></h1></center>\n";
    warn "Error details:\n\n" . $@ ;
    exit;
}
&SET_GENOME_VERSIONS;

my $help   = $mt->help;
my $aid = $mt->user->id;

my $betabit = $isbeta ? "tilfordc/working/snptracker/" : "";

my $url = {
    blisvc => "http://blis.hpw.pri.bms.com/Homo_sapiens/contigview",
    blisst => "http://blis.hpw.pri.bms.com/Homo_sapiens/bms_dump_regions.pl?out=snptracker&redirect=http://horta.pri.bms.com/biocgi/". $betabit . "snpTracker.pl%3Ffilepath=__FILE__&getgenome=",
};


$args->set_param('NWFILE', $args->val(qw(NWFILE_PATH))) if
    (!$args->val(qw(NWFILE)) && $args->val(qw(USENWFPATH)));

my $starttime = time;
my $lasttime  = $starttime;

&ACT_ON_ARGS;
if ($eh) {
    $eh->close;
    print $eh->html_summary;
} else {
    print $mt->taxa_html_key;
}

&FORM_FINDALL if ($dofind);

if ($showBench) {
    print $help->make_link(14);
    printf($frm->{boldbr}, 'green', '+1', "Program Benchmarks:");
    #print "<pre><font color='brick' size='+1'>";
    warn $mt->showbench;
    # print "</font></pre>";
}

&HTMLEND;

sub ACT_ON_ARGS {
    if (my $sn = $args->val(qw(shownote))) {
        printf("<p>%s</p>", $sn);
    }
    
    if ($args->val(qw(QUICKRELATE))) {
        &QUICK_RELATE;
        &QUICK_BOX;
        return;
    }
    if ($args->val(qw(QUICKUNRELATE))) {
        &QUICK_UNRELATE;
        &QUICK_BOX;
        return;
    }

    if (my $ge = $args->val(qw(getexample))) {
        my @seqs = &GET_EXAMPLES( $ge,
                                  $args->val(qw(EXCLUDEKEY)),
                                  $args->val(qw(EXAMPLIMIT)));
        return;
    }

    if (my $hname = $args->val(qw(SHOWHIERARCHY))) {
        &DRAW_HIERARCHY($hname);
        &FINISH_OUT;
        return;
    }

    if ($args->val(qw(CLASSTREE))) {
        print "<b><font color='navy' size='+1'><u>Allowed classes in MapTracker:</u></font></b><br />\n";
        print $mt->classTreeHTML;
    }
    if ($args->val(qw(TYPELIST))) {
        print "<b><font color='green'>Allowed relationships in MapTracker:</font></b><br />\n";
        print $mt->typeTableHTML;
    }
    if ($args->val(qw(SPACELIST))) {
        print "<b><font color='green'>Known MapTracker namespaces:</font></b><br />\n";
        my @spaces = sort { $a->id <=> $b->id } $mt->get_all_namespaces;
        print "<table border='1'>\n";
        print "<tr><th>". join("</th><th>", "Name", "ID", "Description", "Sensitive?") . "</th></tr>\n";
        foreach my $space (@spaces) {
            printf("<tr><td>%s</td><td>%d</td><td>%s</td><td>%s</td></tr>\n", $space->name, $space->id, $space->desc, $space->sensitive ? 'Yes' : '');
        }
        print "</table>\n";
    }

    &INIT_EXCEL if ($autoinfo eq 'Excel');

    if (my $mo = $args->val(qw(MAPONLY))) {
        &MAPONLY($mo);
        return;
    }

    my ($donames, $docluster, $doedit) = 
        ( $args->val(qw(seqname seqnames)) ? 1 : 0, 
          $args->val(qw(CLUSTERSEQ)) ? 1:0,
          $args->val(qw(EDIT)) ? 1:0 );
    $args->set_param('EDIT', $ldap) if ($args->val(qw(EDIT)) eq '1');

    if ($donames + $docluster + $doedit > 1) {
        # This happens if one submit pressed with data in both boxes,
        # or if data for multiple types of analysis are passed
        
        ($donames, $docluster) = ($args->val(qw(DONAMES)), 
                                  $args->val(qw(DOCLUSTER)));
    }
    my @namelist;
    if ($args->val(qw(SHOWSTRUCTURE))) {
        my $oldPath = $args->val(qw(path));
        $args->set_param('path', 'Database_Structure');
        &DO_NETWORK ( ['Unversioned Accession'] );
        &FINISH_OUT;
        $args->set_param('path', $oldPath);
        return;
    } elsif ($docluster) {
        @namelist = &PARSE_LIST( $args->val(qw(CLUSTERSEQ)) );
    } else {
        @namelist = &PARSE_LIST($args->val(qw(SEQNAMES)));
        push @namelist, $args->val(qw(SEQNAME)) if ($args->val(qw(SEQNAME)));
    }
    my %redun = map { $_ => 1 } @namelist;
    delete $redun{undef};
    @namelist = sort keys %redun;
    &SET_EDITS if ($doedit );

    if ($args->val(qw(ALLUSERS))) {
        print "<table border='1'><tr><th>Name</th><th>ID</th><th>Description</th></tr>\n";
        my @auths =  sort {$a->name cmp $b->name } $mt->get_all_authorities;
        foreach my $u ( @auths ) {
            print "<tr>";
            
            printf("<td>$frm->{bold}</td><td>%d</td><td>%s</td>", 'blue','+1', $u->name, $u->id, $u->desc || "");
            # print $u->full_html . "<br />\n";
            print "</tr>\n";
        }
        print "</table>";
        
    }
    if (my $mi = $args->val(qw(map_id))) {
        print "<table border='1'>\n";
        foreach my $map ($mt->get_mappings( -mapid => $mi )) {
            print $map->to_html( -table => 0,  );
        }
        print "</table>\n";
    }


    &ADD_USER if ($args->val(qw(NEWUSERNAME)));

    my @seqs = &GET_NAMES( @namelist );
    my $did_something = $#seqs + 1;
    my $nwf  = $args->val(qw(NWFILE));
    if ($nwf) {
        # A network file was provided.
        &DO_NETWORK (\@seqs);
        $did_something ||= 1;
    } elsif ($doedit) {
        # The user wants to edit the sequences
        $args->set_param('GETMAPS', 0);
        &DECORATE_SEQ( \@seqs );
        print "<form name='update' method='post'>\n";
        printf($frm->{submit}, 'Record all edits', 'doedit'); print "<br />\n";
        printf($frm->{hidden}, 'edit', $ldap);
        printf($frm->{hidden}, 'seqname', $args->formval(qw(SEQNAME)));
        printf($frm->{hidden}, 'seqnames', $args->formval(qw(SEQNAMES)));
        &LIST_SEQ( @seqs );
        printf($frm->{submit}, 'Record all edits', 'doedit'); print "<br />\n";
        print "</form>";
    } elsif ($foundcount) {
        my $nw = &DO_NETWORK( \@seqs );
        my $edges = $nw ? $nw->edge_count : 0;
        my $showmeta;
        if ($autoinfo eq 'Always') {
            $showmeta = 1;
        } elsif ($autoinfo eq 'Excel') {
            $showmeta = 1;
        } elsif ($autoinfo eq 'Network') {
            if ($args->val(qw(PATH)) eq 'Unconnected_Net' && $nw) {
                $showmeta = 0;
            } elsif ($reqcount > 1) {
                $showmeta = 0;
            } else {
                $showmeta = 1;
            }
        }

        if ($showmeta) {
            &DECORATE_SEQ( \@seqs );
            &LIST_SEQ( @seqs );
        } elsif ($autoinfo eq 'Network') {
            print $help->make_link(54);
            printf($frm->{colbr}, 'orange', "Object metadata reporting has been suppresed due to the creation of a network from multiple user requests. To see metadata, turn off 'Auto-suppress', or turn off network generation.");
            
        }
    }
    &FINISH_OUT if ($did_something);
}

sub FINISH_OUT {
    printf("%s<font color='green'>Finished. All supporting files can be found".
           " <a href='%s'>here</a></font><br />\n",$help->make_link(83),
           $mt->file_url('TMP'));

}

sub GET_EXAMPLES {
    my ($query, $excludekey, $limit) = @_;
    $limit = 10 unless (defined $limit);
    $excludekey ||= "";
    my ($what, $tag) = split('_', $query);
    my $sql = "SELECT s.seqname FROM ";
    my @tabs = ("seqname s");
    my @clause;
    print "<b>Request to get example database entries where:</b><br />";
    if ($what =~ /class/i) {
        my $class = $mt->get_class($tag);
        unless ($class) {
            print "<font color='red'>I do not know of a class called '$tag'</font><br />";
        }
        my $cid = $class->id;
        push @tabs, "seq_class c";
        push @clause, "c.class_id = $cid";
        if ($excludekey) {
            my @bits = split('_', $excludekey);
            for (my $i = 0; $i < $#bits; $i += 2) {
                my ($min, $max) = ($bits[$i], $bits[$i+1]);
                push @clause, "(c.name_id < $min OR c.name_id > $max)";
            }
        }
        push @clause, "s.name_id = c.name_id";
        
        printf("<li>Class = %s</li>\n", $class->name);
    } elsif ($what =~ /rel/i || $what =~ /type/i) {
        my $type = $mt->get_type($tag);
        unless ($type) {
            print "<font color='red'>I do not know of a relationship called '$tag'</font><br />";
        }
        my $tid = $type->id;
        push @tabs, "edge e";
        push @clause, "e.type_id = $tid";
        if ($excludekey) {
            my @bits = split('_', $excludekey);
            for (my $i = 0; $i < $#bits; $i += 2) {
                my ($min, $max) = ($bits[$i], $bits[$i+1]);
                push @clause, "((e.name1 < $min OR e.name1 > $max) AND (e.name2 < $min OR e.name2 > $max))";
            }
        }
        push @clause, "(s.name_id = e.name1 OR s.name_id = e.name2)";
        
        printf("<li>Relation = %s</li>\n", $type->name);
    }
    $sql .= join(', ', @tabs) . " WHERE " . join(' AND ', @clause);
    $sql .= " LIMIT $limit" if ($limit);
    $mt->_showSQL( $sql, "Search database for example entries");
    my $rows = $mt->dbi->get_all_rows( $sql );
    my @seqs = map { $mt->get_seq($_->[0]) } @{$rows};
    printf("A total of %d entr%s found", $#seqs + 1, $#seqs == 0 ?
           'y was' : 'ies were');
    if ($#seqs < 0) {
        print "<br />\n";
        return;
    }
    my @ids = sort { $a <=> $b } map { $_->id } @seqs;
    $excludekey .= "_" if ($excludekey);
    $limit *= 2;
    printf("; <a href='mapTracker.pl?getexample=%s&examplimit=%d&".
           "excludekey=%s%d_%d'>Find more examples</a><br />",
           $query, $limit, $excludekey, $ids[0], $ids[-1]);
    print "<hr />\n";
    &DECORATE_SEQ( \@seqs );
    &LIST_SEQ( @seqs );
}

sub SETGLOBALS {
    @cutargs  = ( -cutbridge => $args->val(qw(CUTBRIDGE)));
    %minscores = ();

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
        post   => "<form action='mapTracker.pl' method='post' enctype='multipart/form-data'>\n",
        col    => "<font color='%s'>%s</font>",
        colbr  => "<font color='%s'>%s</font><br />\n",
        bold   => "<font color='%s' size='%s'><b>%s</b></font>",
        boldbr => "<font color='%s' size='%s'><b>%s</b></font><br />\n",
        hidden => "<input type='hidden' name='%s' value='%s' />\n",
        radio  => "<input type='radio' name='%s' value='%s' %s/>\n",
        text   => "<input type='text' name='%s' value='%s' size='%d' />\n",
    };

    if ($args->val(qw(DOCUT))) {
        # Set up map retrieval arguments
        my $docut = 0;
        if (my $ca = $args->val(qw(CUTAFFY))) {
            $minscores{'MicroBlast Cluster'} = $ca;
            $docut = 1;
        }
        if (my $cs = $args->val(qw(CUTSIM4))) {
            $minscores{'Sim4'} = $cs;
            $docut = 1;
        }
        if ($docut) {
            push @cutargs, ( -minscore => \%minscores);
            $filtermsg = "";
            my $what = 'direct';
            $what .= ' and scaffolded' if ($args->val(qw(CUTBRIDGE)));
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

    $gvprogs = 
        [ [ ''   => "Rigid tree (neat levels, messy connections)" ],
          [ 'neato' => "Dispersed graph (chaotic, but better connections)" ],
          [ 'twopi' => "Radial layout (inefficient but legible)" ],
          [ 'gml'   => "Flat file export: GML" ],
          [ 'sif'   => "Flat file export: SIF" ],
          [ 'hypv'  => "Flat file export: Hyperviewer" ],
          ];

}

sub SET_GENOME_VERSIONS {
    ($bestvers, $blisvers) = $mt->genomeVersions;
    @ncbi_vers = (
                  [ 'blis', "BLIS (NCBI $blisvers)" ],
                  );
    for my $i (29..$bestvers) {
        push @ncbi_vers, [ $i, "NCBI $i"];
    }    

    $gvers = $args->val(qw(genomevers)) || 0;
    if ($gvers =~ /specific/i) {
        $gvers = $args->val(qw(WHICHVERS));
        $gvers = $blisvers if ($gvers =~ /blis/i);
    } elsif ($gvers =~ /best/i) {
        $gvers = $bestvers;
    }
    $gvers ||= 0;
}

sub MAPONLY {
    my ($text) = @_;
    my @missing;
    my @seqs;
    my %requested;
    foreach my $request (split(/[\n\r]+/, $text)) {
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
            push @missing, $name;
            next;
        }
        push @seqs, @found;
        map { $requested{$_}++ } @found;
        foreach my $seq (@found) {
            # Set the user-defined range boundaries
            $seq->range( [$start, $end] ) if ($start);
        }
    }

    my @stack = @seqs;
    while (my $seq = shift @stack) {
        my @extra = &LOAD_MAP( $seq, \%requested );
        &SCAFFOLD( $seq ) if ($args->val(qw(GETMAPS)) > 1);
        # If request did not have map, but others possible, use them:
        foreach my $ext (@extra) {
            push @stack, $ext;
            push @seqs,  $ext;
            $ext->range( $seq->range );
        }
    }
    &LIST_SEQ( @seqs );
}

sub DO_NETWORK {
    my ($seqs) = @_;
    my $path = $args->val(qw(PATH));
    my $nf   = $args->val(qw(NWFILE));
    my $sf;
    my ($nw, $lab);
    if ($nf) {
        my $iter = $args->val(qw(ITERATION));
        $nw = BMS::MapTracker::Network->new( -tracker => $mt );
        if (my $fh = $args->val(qw(NWFILE_FH))) {
            # Passed as a text entry box
            $nw->from_flat_file( -fh => $fh );
            $sf = $nf;
        } elsif ($nf =~ /\.\./) {
            $args->err("Relative file paths disallowed", "'$nf'");
        } else {
            # Passed as direct file
            $nw->from_flat_file( -file      => $nf,
                                 -iteration => $iter);
            my @bits = split(/\//, $nf);
            $sf = $bits[-1];
        }
        if ($seqs && $#{$seqs} > -1) {
            $nw->add_root( @{$seqs} );
            printf("<font color='green'>Added %d sequence%s as roots to ".
                   "network: %s</font><br />\n", $#{$seqs}+1, $#{$seqs} == 0 ?
                   '' : 's', join(", ", map {ref($_) ? $_->name : $_ } @{$seqs}));
        }
        &ALTER_NET($nw);
        $lab = "Network loaded from $nf";
        if ($iter) {
            $lab .= 
                " <font color='brick'><i>Edit iteration $iter shown</i></font>";
        }
    } elsif ($path =~ /^none$/i) {
        return undef;
    } elsif ($path =~ /full/i) {
        $nw = &BASIC_NET;
        $lab = "Full Network Expansion";
    } elsif ($path =~ /^auto/i) {
        ($nw, $lab) = &AUTO_NET( $seqs );
        unless ($nw) {
            printf("%s<font color='red'>No appropriate network construction set was identified for your queries.</font><br />\n", $help->make_link(78));
        }
    } else {
        $lab = &PATH_WALK($path, $seqs);
        $nw  = $lab->network;
    }
    my $fileinfo = &HANDLE_NETWORK_FILE( $nw, $sf );
    &SHOW_NET( $nw, $lab) if ($nw);
    print $fileinfo;
    return $nw;
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
        if (my $nf = $args->val(qw(NWFILE))) {
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
        $args->set_param('NWFILE', $path);
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
            my $curiter = $args->val(qw(ITERATION)) || $maxiter;
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
    $string .= "<script language='javascript'>\n  <!--\n";
    $string .= "      netdata = { shortname:'$file', path:'$path' };\n";
    $string .= "  // -->\n</script>\n";
    return $string;
}

sub ALTER_NET {
    my ($nw) = @_;
    if (my $exc = $args->val(qw(EXCLUDENODES))) {
        my @names;
        foreach my $sid ( split(/[\n\r\t]+/, $exc) ) {
            $nw->exclude_node( $sid );
            push @names, $mt->get_seq($sid)->name;
        }
        printf("<li><font color='green'>Excluded %d node%s from network: ".
               "</font><font color='blue'>%s</font></li>\n",
               $#names + 1, $#names == 0 ? '' : 's', join(", ", @names));
    }
    my $snp = $args->val(qw(SETNETPARAM));
    my @params = ref($snp) ? @{$snp} : ($snp);
    foreach my $param (@params) {
        next unless ($param);
        my ($tag, $val) = split(/[\t\n\r]+/, $param);
        if ($tag && defined $val) {
            $nw->param($tag, $val);
            print "<li><font color='green'>Set <b>$tag</b> to ".
                "<b>$val</b></font></li>\n";
        }
    }
    my %vis_key = ( 0 => ['green','Fully Displayed'],
                    1 => ['orange', 'Hidden, but still followed'],
                    2 => ['red', 'Suppressed'], );
    
    for my $i (1..100) {
        my $val = $args->val('NWVIS'.$i);
        last unless ($val);
        my ($reads, $level) = split(/\=/, $val);
        $level = $nw->visualize_edge($reads, $level);
        printf("<li><font color='%s'><b>%s:</b> %s</font></li>\n",
               @{$vis_key{$level}}, $reads);
        
    }
    if (my $sr = $args->val(qw(SETROOT))) {
        my (@sids, @names);
        foreach my $sid ( split(/[\n\r\t]+/, $sr) ) {
            my $seq = $mt->get_seq($sid);
            next unless ($seq);
            push @names, $seq->name;
            push @sids, $sid;
        }
        my $act = "";
        if ($args->val(qw(ROOTACTION)) =~ /del/i) {
            $act = "deleted";
            $nw->remove_root(@sids);
        } else {
            $act = "added";
            $nw->add_root(@sids);
        }
        printf("<li><font color='green'>%d node%s %s as roots:</font> ".
               "<font color='blue'>%s</font></li>\n",$#sids + 1,
               $#sids == 0 ? '' : 's', $act, join(", ", @names));
        
    }
    if (my $exp = $args->val(qw(EXPANDNODES))) {
        my (@sids, @names);
        if ($exp =~ /^all$/i) {
            foreach my $seq ($nw->each_node) {
                push @names, $seq->name;
                push @sids, $seq->id;
            }
        } else {
            foreach my $sid ( split(/[\n\r\t]+/, $exp) ) {
                my $seq = $mt->get_seq($sid);
                next unless ($seq);
                push @names, $seq->name;
                push @sids, $sid;
            }
        }
        my $msg = "Network expanded";
        if (my $keeptype = $args->val(qw(USEEDGE))) {
            my $types = ref($keeptype) ? [@{$keeptype}] : [$keeptype];
            my %hash  = map { lc($_) => 1 } @{$types};
            if ($hash{'all possible edges'}) {
                $msg .= " along all possible edges";
                $types = undef;
            } else {
                $msg .= " along '" . join('+', @{$types}) . "'";
            }
            if ($depth >= 0) {
                foreach my $sid ( @sids ) {
                    $nw->expand( %{$args},
                                 -recurse  => $depth,
                                 -node     => $sid,
                                 -limit    => $maxRel,
                                 -groupat  => $maxGroup
                                 -keeptype => $types, );
                }
            } else {
                my @connected = $nw->connect_internal
                    ( %{$args},
                      -node     => \@sids,
                      -limit    => $maxRel,
                      -groupat  => $maxGroup,
                      -keeptype => $types );
                my $num = $#connected + 1;
                $msg .= " ($num internal nodes have new edges)";
            }
        }

        if (my $path = $args->val(qw(USEPATH))) {
            $msg .= " using path '$path'";
            $nw->remember_roots;
            $nw->clear_roots;
            &PATH_WALK( $path, \@sids, $nw );
            $nw->recall_roots;
        }

        printf("<li><font color='green'>$msg from:</font> ".
               "<font color='blue'>%s</font></li>\n",
               join(", ", @names));
        
    }
    if (my $sc = $args->val(qw(FORMATNODE))) {
        my $color = $args->val(qw(NODECOLOR));
        my $shape = $args->val(qw(NODESHAPE));
        foreach my $sid ( split(/[\n\r\t]+/, $sc) ) {
            $nw->node_format($sid, 'color', $color) if ($color);
            $nw->node_format($sid, 'shape', $shape) if ($shape);
        }
    }
}


sub BASIC_NET {
    my ($seqs) = @_;
    my $nw = BMS::MapTracker::Network->new( -tracker => $mt );
    my @exargs;
    unless ($uselab) {
        @exargs = ( -skipreads => [] );
    }
    foreach my $seq (@{$seqs}) {
        $nw->add_root( $seq );
        $nw->expand( -recurse  => $depth,
                     -groupat  => $args->val(qw(MAXGROUP)),
                     -node     => $seq,
                     -sametaxa => $args->val(qw(SAMETAXA)),
                     -settaxa  => $args->val(qw(SETTAXA)),
                     -limit    => $args->val(qw(MAXRELATION)),
                     -nogroup  => ['refseq', 'ipi'], 
                     @exargs);
    }
    
    my $lab = "";
    if ($uselab) {
        $lab = 'is a shorter term for';
        $nw->expand_all( -recurse => 1,
                         -keeptype => 'is a shorter term for');
    }
    # Now connect all existing nodes to each other
    my @allnodes = $nw->each_node;
    $nw->connect_internal( -node => \@allnodes );
    return $nw;
}

sub SHOW_NET {
    my ($nw, $lab) = @_;
    my $pw;
    if (ref($lab)) {
        $pw = $lab;
        # The label is actually a PathWalker Object
        printf("<font color='blue' size='+1'><b>%s</b> [%s]</font> ".
               "<font color='green' size='-1'>%s</font><br />\n",
               $pw->param('path_short') || "Unknown Network",
               $pw->param('path_name') || "File?",
               $pw->param('path_desc')  || "");
    } else {
        printf($frm->{boldbr}, 'blue', '+1', $lab || "");
    }
    my $traverse = $nw->traverse_network
        ( -maxcluster => $maxGroup,
          -labelwith  => $uselab ? 'is a shorter term for' : 0, );
    return unless ($traverse);


    my $priroot  = $nw->{ROOTS}[0];
    my $basefile = $priroot ? $priroot->name : "Network_" . $mt->new_unique_id;


    my $gp = lc($args->val(qw(GVPROG)) || "");

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
        my $quietRoot = $pw ? $pw->param('quietroot') : $args->{QUIETROOT};
        my $string = $nw->to_graphviz_html ( -program    => $gp,
                                             -quietroot  => $quietRoot,
                                             -traverse   => $traverse );
        print $string . "<br />\n";
        my ($kstr, $dummy) = $nw->graphviz_key
            ( -filename  => $$."_key" );
        print "<b>Key:</b><br />\n$kstr" if ($kstr);
        my $obj = $pw ? $pw : $nw;
        print $obj->show_html_options();
    }
    print $stuff{POST_NET} if ($stuff{POST_NET});
}

sub WRITE_FLAT {
    my ($string, $basefile, $suffix) = @_;
    my $us = uc($suffix);
    unless ($string) {
        print $help->make_link(92);
        print "<font color='red'>Unable to export Net to $us</font><br />\n";
        return;
    }
    my $basepath = $mt->file_path('TMP');
    my $file = $basepath. "$basefile.$suffix";
    open(FILE, ">$file") || die "Could not write to '$file':\n$!\n ";
    print FILE $string;
    close FILE;
    printf
        ("<p>%s<font color='green'>Network exported as a ".
         "<a href='%s%s'>%s file</a></font></p>\n", 
         $help->make_link(91), $mt->file_url('TMP'),"$basefile.$suffix", $us);
}

sub AUTO_NET {
    my ($seqs) = @_;
    my %paths = ( all => {}, );
    foreach my $seq (@{$seqs}) {
        my $sn = $seq->name;
        #print "<p>$sn</p>";
        my @appropriate;
        if ($seq->is_class('Probe', 'Iconix Probe')) {
            push @appropriate, [ 'Probes', 2 ];
            push @appropriate, [ 'Coregulated_Probes', 2 * $foundcount /1.5 ];
            push @appropriate, [ 'Antiregulated_Probes',  2 * $foundcount/1.8];
            push @appropriate, [ 'Coregulated_Probes_Strict', 2 * $foundcount /1.6 ];
            push @appropriate, [ 'Coregulated_Probes_Loose', 2 * $foundcount /1.7 ];
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
        if ($seq->is_class('bio', 'Locus', 'gi')) {
            push @appropriate, ['RefSeq_net', 1];
            push @appropriate, [ 'Coregulated_Probes', $foundcount / 2.7  ];
            push @appropriate, [ 'Antiregulated_Probes', $foundcount / 2.8  ];
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

    if ($best) {
        foreach my $seq (@{$seqs}) {
            delete $paths{$seq->name}{$best};
        }
        delete $allpaths{$best};
        $pw = &PATH_WALK( $best, $seqs, undef, 'isauto');
        if ($pw) {
            ($nw, $pw) = $pw->isa('BMS::MapTracker::PathWalker') ?
                ($pw->network, $pw) : ($pw, undef);
            delete $remain_track{$best};
            $walked++;
        }
    }

    my %suggest = ();
    my @sns = keys %paths;
    foreach my $sn (@sns) {
        while (my ($ap, $count) = each %{$paths{$sn}}) {
            $suggest{$ap} += $count;
        }
    }
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
    
    $text .= "<p />";
    $stuff{POST_NET} = $text;
    return ($nw, $pw);
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

sub PATH_WALK {
    my ($path, $seqs, $nw, $isauto) = @_;
    return &BASIC_NET($seqs) if ($path =~ /^none$/i || $path =~ /^full/i);
    my $fullpath = "$pathdir/$path";
    unless (-e $fullpath) {
        $fullpath .= '.path';
        unless (-e $fullpath) {
            printf("%s<font color='red'>I could not find the PathWalker file '$fullpath'</font><br />\n", $help->make_link(74));
            return undef;
        }
    }
    my $pw = BMS::MapTracker::PathWalker->new( -tracker => $mt,
                                               -verbose => 0, );
    $pw->error( -output => 'html');
    my $pdat = $pw->load_path( $fullpath );
    my %pargs = ( pid      => $$,
                  limit    => $args->val(qw(LIMITRELATE)),
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


sub DRAW_HIERARCHY {
    my ($req) = @_;
    my @seqs;
    my $hier = BMS::MapTracker::Network->new( -tracker => $mt );
    foreach my $name (split(/[\t\,\r\n]+/, $req)) {
        my $seq = $mt->get_seq( $name );
        push @seqs, $seq if ($seq);
        $hier->add_root($seq);
    }
    my $is_report = $seqs[0]->is_class( 'person' );
    my ($cedge, $pedge) = $is_report ? 
        ('reports to', 'has report') : ('is a child of', 'is a parent of');
    my $childdepth = $args->val(qw(HIERDEPTH));
    foreach my $seq (@seqs) {
        $hier->expand( -node    => $seq,
                       -recurse => 10,
                       -groupat => 5,
                       -limit   => 50,
                       -keeptype => [ $cedge, ], );
        $hier->expand( -node    => $seq,
                       -recurse => $childdepth,
                       -groupat => 50,
                       -limit   => 50,
                       -keeptype => [ $pedge, ], );
    }
    $hier->expand_all( -recurse  => 0,
                       -keepauth => $aid, );
    $hier->expand_all( -recurse  => 0,
                       -keeptype => [ 'is a shorter term for' ], );
    $hier->proxy_by_edge() if ($is_report);
    foreach my $seq (@seqs) {
        print $hier->hierarchy_html( -node      => $seq,
                                     -maxdepth  => $childdepth,
                                     -childedge => $cedge, );
    }
    print &HANDLE_NETWORK_FILE($hier);
    # &SHOW_NET($hier);
}

sub PARSE_LIST {
    my ($ref) = @_;
    my %redun = map {$_ => 1} split(/[\n\s\r\,]+/, $ref);
    my @names = sort keys %redun;
    delete $redun{undef};
    $ref = join("\n", @names);
    return @names;
}

sub HTMLSTART {
    print "<html><head>\n";
    print "<title>MapTracker - BMS Sequence Information Database</title>\n";
    print "<link rel='shortcut icon' href='/biohtml/images/MapTracker_Small.gif'>\n";
    print $mt->javascript_head() if ($mt);
    print "</head><body bgcolor='white'><center>\n";
    printf($frm->{boldbr}, 'orange', '+3', 'MapTracker');
    my $prog = $0;
    if ($prog =~ /working/) {
        print $mt->help->make_link(1) if ($mt);
        print "<font color='red'>*** THIS VERSION IS BETA SOFTWARE ***</font><br />\n";
        $isbeta = 1;
    }
    print "<font color='brick' size='-1'>";
    print $mt->help->make_link($ldap ? 2 : 3) if ($mt);
    printf("<i>In use by %s</i></font><br />\n", $ldap);
    print "</center>\n";
}

sub HTMLEND {
    print $mt->javascript_data( 'html' );
    print "</body></html>\n";
}

sub FORM_FINDALL {
    print "<form action='mapBrowser.pl'>This is the advanced <font size='-1'>(ie needlessly complex)</font> interface. You can also use the <input type='submit' value='Simple Interface'></form>";
    print $frm->{post};
    &FORM_FIND_NAME;
    &FORM_NETWORK;
    # &FORM_FIND_CLUSTERS;
    &FORM_GLOBAL_SETTINGS;
    &FORM_FIND_INFO;
    if ( $admin->{$ldap} ) {
        &FORM_ADMIN  ;
        &FORM_DUMPSQL;
    }

    print "</form>\n";
}

sub FORM_GLOBAL_SETTINGS {
    print "<table width='$tabwid'><tr><th width='$tabwid' bgcolor='#ffcc00'>\n";
    print $help->make_link(19) . "Global Settings";
    print "</th></tr>\n";
    print "<tr><td valign='top'>\n";

    print $help->make_link(26);
    print "<b>Show map data as:</b><br />\n";
    print "&nbsp;&nbsp;&nbsp;";

    printf($frm->{radio}, 'showmap', 'full table',
           $showMap =~ /full/i ? 'CHECKED ':"", );
    print "Full table ";

    printf($frm->{radio}, 'showmap', 'brief integrated',
           $showMap =~ /brief int/i ? 'CHECKED ':"", );
    print "Integrated table ";

    printf($frm->{radio}, 'showmap', 'brief', $showMap =~ /brief/i 
           && $showMap !~ /integr/i? 'CHECKED ':"", );
    print "Brief table ";

    printf($frm->{radio}, 'showmap', 'image',
           $showMap =~ /image/i ? 'CHECKED ':"", );
    print "Image, size ";
    printf($frm->{text}, 'imagewidth', $args->formval(qw(IMAGEWIDTH)), 4);
    print "px<br />\n";

    print $help->make_link(56);
    printf($frm->{hidden}, 'docut', 0);
    printf($frm->{check},'docut', $args->formval(qw(DOCUT)) ? 'CHECKED ':"",
           "<b>Filter mappings by score:</b>", );
    # print "<b>Do not display maps with scores below:</b><br />\n";
    print "&nbsp;&nbsp;&nbsp;";
    print "MicroBlast: ";
    printf($frm->{text}, 'cutaffy', $args->formval(qw(CUTAFFY)), 4);
    print "&nbsp;&nbsp;&nbsp;";
    print "Sim4: ";
    printf($frm->{text}, 'cutsim4', $args->formval(qw(CUTSIM4)), 4);
    print "&nbsp;&nbsp;&nbsp;";
    printf($frm->{check},'cutbridge', $args->formval(qw(CUTBRIDGE)) ? 'CHECKED ':"",
           "Apply to scaffolds", );

#    print $help->make_link(72);
#    printf($frm->{check},'congeal', $args->formval(qw(CONGEAL)) ? 'CHECKED ':"",
#    "<b>Ignore small gaps in direct genomic maps</b>", );



    my $sgv = $args->val(qw(genomevers));
    print $help->make_link(24);
    print "<b>Scaffolds should use genome version:</b><br />\n";
    print "&nbsp;&nbsp;&nbsp;";
    printf($frm->{radio}, 'genomevers', 'best',
           $sgv =~ /best/i ? 'CHECKED ':"", );
    print "Most Recent ";
    printf($frm->{radio}, 'genomevers', 'all',
           $sgv =~ /all/i ? 'CHECKED ':"", );
    print "All<br />&nbsp;&nbsp;&nbsp;";
    printf($frm->{radio}, 'genomevers', 'specific',
           $sgv =~ /specific/i ? 'CHECKED ':"", );
    print "Specific: ";
    print "<select name='whichvers'>\n";
    foreach my $dat (@ncbi_vers) {
        printf("  <option value='%s' %s>%s</option>\n", $dat->[0],
               $args->val(qw(WHICHVERS)) =~ /$dat->[0]/i ? 'CHECKED ':"", $dat->[1]);
    }
    print "</select>\n";
    print "<br />\n";
    
    #print $help->make_link(52);
    #print "<b>Limit Genomic Mapping:</b><br />\n";
    #print "&nbsp;&nbsp;&nbsp;";
    #print "Maximum of ";
    #printf($frm->{text}, 'genomelimit', $args->val(qw(GENOMELIMIT)), 4);
    #print " entries.<br />\n";

    print"</td></tr></table>\n";
}

sub FORM_FIND_NAME {
    my $selectBox = $mt->classTreeSelect;
    print "<table width='$tabwid' ><tr><th colspan='2' width='$tabwid' bgcolor='#ff9966'>\n";
    print $help->make_link(4) . "Name (Term) Search";
    print "</th></tr>\n";
    print "<tr><td valign='top'>\n";

    print $help->make_link(59);
    print "<b>One or more names:</b><br />\n";
    printf($frm->{tarea}, 'seqnames', 30, 6, $args->formval(qw(SEQNAMES)));

    # print "<br />";
    print $help->make_link(59);
    print "<b>Single name or phrase:</b><br />\n";
    printf($frm->{text}, 'seqname', $args->formval(qw(SEQNAME)), 30);
    print "<b>Wildcard Limit:</b> \n";
    printf($frm->{text}, 'limit', $args->formval(qw(LIMIT)), 3);
    print " <span style='font-size:smaller'>(0 for no limit)</span><br />\n";
    
    print "</td><td valign='top' width='$tabwid'>\n";
    printf($frm->{submit}, 'Retrieve Information', 'donames');
    print "<br />\n";

    print $help->make_link(85);
    print "<b>Metadata associated with names...</b><br />\n";
    printf($frm->{radio}, 'autoinfo', 'Always',
           $autoinfo eq 'Always' ? 'CHECKED ':"", );
    print "should always be displayed<br />";
    printf($frm->{radio}, 'autoinfo', 'Network',
           $autoinfo eq 'Network' ? 'CHECKED ':"", );
    print "is suppressed for multi-node nets<br />";
    printf($frm->{radio}, 'autoinfo', 'Excel',
           $autoinfo eq 'Excel' ? 'CHECKED ':"", );
    print "is recorded in an excel spreadsheet<br />";

    # print $help->make_link(87);

    print $help->make_link(28);
    print "<b>Retrieve Metadata for:</b><br />\n";
    printf($frm->{hidden}, 'getclass', 0);
    printf($frm->{hidden}, 'gettype', 0);
    printf($frm->{hidden}, 'gettaxa', 0);
    printf($frm->{hidden}, 'getlength', 0);
    printf($frm->{hidden}, 'scaffold', 0);
    print "<table>";


    print "<tr><td>";
    printf($frm->{check},'getclass', $args->val(qw(GETCLASS)) ? 'CHECKED ':"",
           "Classes", );
    print "</td><td>";
    printf($frm->{check}, 'gettaxa', $args->val(qw(GETTAXA)) ? 'CHECKED ':"",
           "Species (Taxa)", );
    print "</td></tr>";

    print "<tr><td>";
    printf($frm->{check}, 'gettype', $args->val(qw(GETTYPE)) ? 'CHECKED ':"",
           "Relationships", );
    print "</td><td>";
    printf($frm->{check},'getlength', $args->val(qw(GETLENGTH)) ? 'CHECKED ':"",
           "Lengths", );
    print "</td></tr>";

    print "</table>";


    print $help->make_link(31);
    print "<b>Get mapping information:</b><br />";
    printf($frm->{radio}, 'getmaps', '0',
           $args->val(qw(GETMAPS)) == 0 ? 'CHECKED ':"", );
    print "None";
    printf($frm->{radio}, 'getmaps', '1',
           $args->val(qw(GETMAPS)) == 1 ? 'CHECKED ':"", );
    print "Direct";
    printf($frm->{radio}, 'getmaps', '2',
           $args->val(qw(GETMAPS)) == 2 ? 'CHECKED ':"", );
    print "Scaffolds";
    print "<br />\n";


    #print "<b>Try to walk to names of class:</b>\n";
    #print "$selectBox<br />";
    print"</td></tr></table>\n";

    print $help->make_link(59);
    print "<b>Specific Map Location:</b> \n";
    printf($frm->{text}, 'maponly', $args->formval(qw(MAPONLY)), 50);
    return;
}

sub INIT_EXCEL {
    my $file = "MapTracker_Summary_$$.xls";
    $eh = BMS::ExcelHelper->new($mt->file_path('TMP') . $file);
    $eh->url($mt->file_url('TMP') . $file);

    $eh->sheet( -name    => "Help",
                -freeze  => 1,
                -columns => [ 'Column Name','Use', 'Description' ],
                -width   => [ 15, 30, 120 ], );

    $eh->add_row('Help', [ 'Name', 'The name of an entity', 'Names are the central component of MapTracker - they are identifiers that can be classified or connected to each other' ]);
    $eh->default_width('name', 24);

    $eh->add_row('Help', [ 'Provenance', 'How a name got here', 'Indicates why a name was included in the spreadsheet - did the user request it, or was it uncovered by another analysis?' ]);
    $eh->default_width('Provenance', 15);
    
    $eh->add_row('Help', [ 'Class', 'A class assignment', "Classes describe the sort of thing that a name is - examples include 'mRNA', 'Chemical Entity', 'Cell Type'" ]);
    $eh->default_width('Class', 15);
    $eh->default_width('Subject Class', 15);
    
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

sub FORM_NETWORK {
    print "<table width='$tabwid' ><tr><th width='$tabwid' bgcolor='#ff9999'>\n";
    print $help->make_link(60) . "Network Construction";
    print "</th></tr>\n";
    print "<tr><td valign='top'>\n";

    print $help->make_link(81);
    print "<b>Load Network File:</b> ";
    print "<input type='file' name='nwfile' /><br />\n";
    my $nwf = $args->val(qw(NWFILE));
    if ($nwf =~ /^\//) {
        my @bits = split(/\//, $nwf);
        my $sf = $bits[-1];
        printf($frm->{hidden}, 'NWFILE_PATH', $nwf);
        print $help->make_link(84);
        printf($frm->{check},'usenwfpath', $args->formval(qw(USENWFPATH)) ? 'CHECKED ':"",
               "<b>Apply NW options to $sf</b>", );
    }

    print $help->make_link(61);
    print "<b>Network Path Rules:</b> ";
    print "<select name='path'>\n";
    my $path = $args->val(qw(PATH)) || "";
    for (my $i = 0; $i <= $#{$kp_list}; $i += 2) {
        printf("  <option value='%s' %s>%s</option>\n", $kp_list->[$i],
               $path eq $kp_list->[$i] ? 'SELECTED ':"", $kp_list->[$i+1]);
    }
    print "</select><br />\n";
    
    print $help->make_link(62);
    print "<b>Draw:</b> \n";
    print "<select name='gvprog'>\n";
    my $gvp = $args->val(qw(GVPROG)) || "";
    foreach my $dat (@{$gvprogs}) {
        printf("  <option value='%s' %s>%s</option>\n", $dat->[0],
               $gvp =~ /$dat->[0]/i ? 'SELECTED ':"", $dat->[1]);
    }
    print "</select><br />\n";
    
    printf($frm->{hidden}, 'sametaxa', 0);
    print $help->make_link(63);
    printf($frm->{check}, 'sametaxa', $args->val(qw(SAMETAXA)) ? 'CHECKED ':"",
           "Only follow links to entries of the same species", );
    print "Only get a specific taxa:  ";
    printf($frm->{text}, 'settaxa', $args->formval(qw(SETTAXA)), 30);
    print "<br />\n";

    print $help->make_link(64);
    print "Maximum network depth of ";
    printf($frm->{text}, 'maxdepth', $depth + 1, 3);
    print "<br />\n";

    print $help->make_link(65);
    print "Do not expand network for groups larger than  ";
    printf($frm->{text}, 'maxgroup', $args->formval(qw(MAXGROUP)), 3);
    print " entries<br />\n";

    print $help->make_link(66);
    print "Limit any given type of relationship to ";
    printf($frm->{text}, 'maxrelation', $args->formval(qw(MAXRELATION)), 3);
    print " entries<br />\n";

    printf($frm->{hidden}, 'uselabel', 0);
    print $help->make_link(67);
    printf($frm->{check}, 'uselabel', $args->val(qw(USELABEL)) ? 'CHECKED ':"",
           "Label nodes with descriptions when available", );

    printf($frm->{hidden}, 'alsogroup', 0);
    print $help->make_link(68);
    printf($frm->{check}, 'alsogroup', $args->val(qw(ALSOGROUP)) ? 'CHECKED ':"",
           "Connect groups to any singly-listed members", );

    print "</td></tr></table>\n";
    return;
}

sub FORM_FIND_CLUSTERS {
    print "<table width='$tabwid'><tr><th colspan='2' width='$tabwid' bgcolor='#33ff99'>\n";
    print $help->make_link(6) . "Genomic Name Clustering";
    print "</th></tr>\n";
    print "<tr><td valign='top' width='10'>\n";
    printf($frm->{tarea}, 'clusterseq', 30, 8, $args->formval(qw(CLUSTERSEQ)));
    print "</td><td valign='top'>\n";
    print $help->make_link(33);
    print "<b>Cluster distance:</b> ";
    printf($frm->{text}, 'clustermax', $args->formval(qw(CLUSTERMAX)), 10);
    print "bp<br />\n";
    print $help->make_link(34);
    print "<b>Cluster pad:</b> ";
    printf($frm->{text}, 'clusterpad', $args->formval(qw(CLUSTERPAD)), 10);
    print "bp<br />\n";
    printf($frm->{submit}, 'Cluster Names','docluster'); print "<br />\n";
    print"</td></tr></table>\n";
    return;
}

sub FORM_DUMPSQL {
    print $help->make_link(27) . "<b>SQL Dump:</b> <select name='dumpsql'>\n";
    my $sqllvl = { 0 => 'None',
                   1 => 'Major Only',
                   2 => 'Expanded',
                   3 => 'Full SQL', };

    for my $i (0..3) {
        printf("  <option value='%d'%s>%s</option>\n",$i, 
               $args->val(qw(DUMPSQL)) == $i ? ' SELECTED' : '', $sqllvl->{$i});
    }
    print "</select>\n";
    print "<br />&nbsp;&nbsp;&nbsp;";
    printf($frm->{check},'showbench', $showBench ? 'CHECKED ':"",
           "Show benchmarks", );
}

sub FORM_FIND_INFO {
    print "<table><tr><th width='$tabwid' bgcolor='#66ccff'>\n";
    print $help->make_link(5) . "Basic Information";
    print "</th></tr>\n";
    print "<tr><td valign='top'>\n";
    printf($frm->{abrstr}, 'classtree', 1, '<b>Class Tree</b>',
           "Show the tree of all allowed name classes");
    printf($frm->{abrstr}, 'typelist', 1, '<b>List Relationships</b>',
           "Show all allowed relationships");
    printf($frm->{abrstr}, 'allusers', 1, '<b>List Authorities</b>',
           "Show all known authorities");
    printf($frm->{abrstr}, 'spacelist', 1, '<b>List Namespaces</b>',
           "Show all allowed namespaces in database");
    printf($frm->{abrstr}, 'showstructure', 1, '<b>Summarize Network</b>',
           "Show a graph representing common organization within network");
    print"</td></tr></table>\n";
    return; 
}

sub FORM_ADMIN {
    print "<table><tr><th width='$tabwid' bgcolor='#ff3300'>\n";
    print $help->make_link(88) . "Administration";
    print "</th></tr>\n";
    print "<tr><td valign='top'>\n";

    print "Add User - Name: ";
    printf($frm->{text}, 'newusername', '', 20);
    print " Description: ";
    printf($frm->{text}, 'newuserdesc', 'BMS e-mail address', 20);
    printf($frm->{submit}, 'Execute', 'doadmin'); print "<br />\n";

    printf($frm->{check},'debug', $doDebug ? 'CHECKED ':"",
           "<b>Turn on debugging</b>", );
    print"</td></tr></table>\n";
    return; 
}

sub ADD_USER {
    my ($name, $desc) = ($args->val(qw(NEWUSERNAME)), $args->val(qw(NEWUSERDESC)));
    return unless ($name);
    $desc ||= "Unknown";
    my $auth = $mt->make_authority($name, $desc);
    print $auth->full_html;
}

sub GET_NAMES {
    my @seqs = ();
    my @missing = ();
    my $asked = $#_ + 1;
    my %found_taxa;
    my $limit = $args->val(qw(LIMIT));
    for my $i (0..$#_) {
        my $name = $_[$i];
        my @s     = $mt->get_seq( -name     => $name, 
                                  -nocreate => 1,
                                  -limit    => $limit );
        if ($limit && $#s + 1 >= $limit) {
            my $count = $#s + 1;
            warn "Found $count database entries for '$name', but a search limit of $limit was imposed - there could more entries in the DB\n";
        }
        my @taxas = $mt->get_taxa( $name );
        foreach my $taxa (@taxas) {
            next unless ($taxa);
            $found_taxa{ $taxa->id } = $taxa;
        }

        if ($#taxas < 0 && $#s < 0) {
            push @missing, $name;
            next;
        }
        foreach my $ss (@s) {
            next unless ($ss);
            push @seqs, $ss;
        }
        if (time - $lasttime > 15) {
            my $dt = `date`; chomp $dt;
            printf("<pre><font color='blue'>Searching DB for your queries ".
                   "- %d of %d complete [%s]</font></pre>", $i+1 , $#_+1,$dt);
            $lasttime = time;
        }
    }

    my @taxa = sort { uc($a->name) cmp uc($b->name) } values %found_taxa;
    if ( $#taxa > -1 ) {
        print $help->make_link(75);
        printf($frm->{boldbr}, 'blue', '+1', 
               "Some of your requests matched taxa identifiers:");
        print join("<br />\n", map { $_->javascript_link } @taxa);
        print "<p />\n";
    }

    if ($#missing > -1) {
        my $num = $#missing + 1;
        print $help->make_link(14);
        printf($frm->{boldbr}, 'red', '+1', 
               "$num / $asked names are not in MapTracker:");
        print "<b>" . join(", ", @missing) . "</b></br>\n";
        print "<hr />\n";
    }
    ($reqcount, $foundcount) = ($#_ + 1, $#seqs + 1);
    return @seqs;
}

sub DECORATE_SEQ {
    my ($seqs) = @_;
    my @stack = @{$seqs};
    my %requested = map { $_->id => 1 } @stack;
    my $done = 0;
    my $dectime = time;
    my $idle    = 15;
    my $setidle = 0;
    while (my $seq = shift @stack) {
        $done++;
        my $sn = $seq->name;
        $seq->read_classes   if ($args->val(qw(GETCLASS)));
        $seq->read_lengths   if ($args->val(qw(GETLENGTH)));
        $seq->read_taxa      if ($args->val(qw(GETTAXA)));
        $seq->read_relations( -limit => $args->val(qw(LIMITRELATE)) )
            if ($args->val(qw(GETTYPE)));
        if ($args->val(qw(GETMAPS))) {
            my @extraseqs = &LOAD_MAP( $seq, \%requested );
            push @{$seqs}, @extraseqs;
            push @stack, @extraseqs;
            map { $autoadded->{$_->id} = 1 } @extraseqs;
        }
        if ($args->val(qw(GETMAPS)) > 1) {
            &SCAFFOLD( $seq );
        }
        if (time - $lasttime > $idle) {
            # If this is taking a while, at intervals let the user
            # know something is happening.
            my $dt = `date`; chomp $dt;
            my $elapsed = time - $dectime;
            my $rate    = $elapsed ? $done / $elapsed : 9999;
            my $remain  = $rate    ? ($#stack + 1) / $rate : 9999;
            unless ($setidle) {
                # Recalculate the idle interval to prevent the screen
                # from filling up with long messages
                $setidle = int($remain / 10);
                $setidle = $idle if ($setidle < $idle);
                $idle    = $setidle;
            }
            my $u = 'sec';
            if ($remain > 60) {
                $remain /= 60; $u = 'min';
            }
            
            printf("<pre><font color='blue'>Retrieving data for your queries ".
                   "- %d done, %d remain (est. %.1f %s to go) [%s]</font></pre>",
                   $done, $#stack + 1, $remain, $u, $dt);
            $lasttime = time;
        }
    }
}

sub LOAD_MAP {
    my ($seq, $requested) = @_;
    $requested ||= {};
    my @foundmap;
    my $limit = 100;
    my $overlap  = $args->val(qw(OVERLAP));
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

    # Keep track of additional sequences we recovered
    my @extraseqs;
    if ($#foundmap < 0) {
        # We did not find any maps - are there others we could use?
        my @consider = ('is an unversioned accession of',
                        'is a reliable alias for',
                        'PRIORVERSION',
                        'is the same as' );
        my %try = ();
        my $check = BMS::MapTracker::Network->new( -root    => $seq,
                                                   -tracker => $mt );
        $check->expand( -recurse => 3,
                        -keeptype => \@consider );
        
        foreach my $sid ($check->all_seq_ids) {
            # If this is one of the sequences already requested, skip for now:
            next if ( $requested->{ $sid } );
            my $oseq = $check->node( $sid );
            my @newmaps = $oseq->read_mappings(@readargs, @cutargs);
            if ($#newmaps > -1 ) {
                # The related sequence has some maps, so we will include it:
                $try{ $sid } = $oseq;
                $mapsfound += $#newmaps + 1;
            }
        }

        my @goodmap = values %try;
        if ($#goodmap > -1) {
            my @pstrings;
            foreach my $oseq (@goodmap) {
                my $path = $check->find_path( -end => $oseq);
                $path->save_best;
                if (my ($best) = $path->paths_as_html) {
                    push @pstrings, "  $best";
                }
            }
            if ($#pstrings > -1) {
                print $help->make_link(71);
                printf("<font color='blue'>No maps found for %s, so I am ".
                       "also including:</font><pre>%s</pre>\n", 
                       $seq->name, join("\n", @pstrings));
                
                print "</pre>\n";
                push @extraseqs, @goodmap;
            }
        }
    }
    return @extraseqs;
}

sub SCAFFOLD {
    my ($seq) = @_;
    if ($seq->is_class( 'protein' )) {
        # For proteins, use mRNA alignments as first scaffold.
        $seq->scaffold( @cutargs,
                        -usebridge => [ 'mrna' ], 
                        -limit     => 20,  );
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
               "'Variants' not recovered for ". $args->esc_xml($seq->name));

    }
    $seq->scaffold( @scafargs  );
}

sub LIST_SEQ {

    if ($eh) {
        foreach my $seq (@_) {
            $seq->to_excel( $eh, "User Query");
        }
        return;
    }
    my $fp;
    if ($showMap =~ /image/i) {
        my $opts = $mt->friendlyPanelOptions;
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
              -width => $args->val(qw(IMAGEWIDTH)),
              -autocollapse => {
                  default   => [15, 50, 200],
                  affyset   => [100, 100, 200],
                  variant   => [35, 200, 2000 ],
              } );
    }
    $showMap = 0 unless ($args->val(qw(GETMAPS)));

    if ($mapsfound) {
        print "<table><tr><td bgcolor='#dddddd' nowrap='1'>\n";
        printf("<b><i>Settings applied to %d recovered Mapping%s:".
               "</i></b><br />\n", $mapsfound, $mapsfound == 1 ? '' : 's');
        print $help->make_link(57) . $filtermsg if ($filtermsg);
        if ($args->val(qw(GETMAPS)) > 1) {
            print $help->make_link(20);
            printf($frm->{boldbr}, 'blue', '+0', 
                   "Both direct and scaffolded features considered");
        } elsif ($args->val(qw(GETMAPS))) {
            print $help->make_link(21);
            printf($frm->{boldbr}, 'blue', '+0', 
                   "Only directly mapped features considered.");
        }
        print "</td></tr></table><br />\n";
    }
    my %seqDrawn;
    $showMap = 0 if ($args->val(qw(SHOWMAP)) =~ /integ/i);
    foreach my $seq (@_) {
        print $seq->to_html( -fp      => $fp,
                             -showmap => $showMap,
                             -linkargs => $linkargs,
                             -edit     => $args->val(qw(EDIT)) );
        print "<hr />\n";
    }
    if ($args->val(qw(SHOWMAP)) =~ /brief/i && $args->val(qw(SHOWMAP)) =~ /integ/i) {
        print "<table border='1'><tr><th colspan='12' bgcolor='tan'>".
            sprintf("Mapping summary for %d Quer%s</th></tr>\n",
                    $#_ + 1, $#_ == 0 ? 'y' : 'ies');
        print &BMS::MapTracker::Mapping::html_row_header(1,1);
        foreach my $seq (@_) {
            print $seq->show_maps( -showmap => $showMap,
                                   %{$args} );
        }
        print "</table>";
    }
    #warn $args->val(qw(SHOWMAP));
    $fp->addJava( -linkcols => 1 ) if ($args->val(qw(SHOWMAP)) =~ /image/i);
}

sub CLUSTER_SEQ {
    my %subjects = ();
    my %copies;
    my @queue = @_;
    my (%notMapped, %done, %tried, %isalias, %hasalias, %isMapped );
    while (my $seq = shift @queue) {
        my $sn = $seq->name;
        next if ($done{$sn});
        $done{$sn} = 1;
        $seq->read_mappings( -keepclass => 'gdna'
                             -clear     => 1,);
        $copies{$sn} = {};
        $copies{$sn}{allvers} = 0;
        foreach my $map ($seq->each_mapping) {
            my $in = $map->seq_index($seq);
            my $ni = !$in || 0;
            my @seqs = $map->seqs;
            my $subj = $seqs[$ni];
            my ($chr, $ver);
            my $subn = $subj->name;
            if ($subn =~ /Human_Chr_(\S+)\.NCBI_(\d+)/ ||
                $subn =~ /Human_Frag_(\S+)\.NCBI_(\d+)/) {
                ($chr, $ver) = ($1, $2);
            } else {
                # This subject is not a genomic contig
                next;
            }
            $copies{$sn}{$ver} ||= 0;
            $copies{$sn}{$ver}++;
            $copies{$sn}{allvers}++;
            my $loc = $map->loc_for_seq( -seq => $ni );
            $subjects{$ver} ||= {};
            $subjects{$ver}{$chr} ||= [];
            push @{$subjects{$ver}{$chr}},[ $loc->start,$loc->end,$subj,$seq ];
        }
        if ($copies{$sn}{allvers}) {
            if (my $origname = $isalias{$sn}) {
                $isMapped{$origname} ||= [];
                push @{$isMapped{$origname}}, $sn;
            }
        } else {
            if (my $origname = $isalias{$sn}) {
                $notMapped{$origname} = $hasalias{$origname};
            } else {
                my $alias = BMS::MapTracker::Network->new( -root    => $seq,
                                                           -tracker => $mt );
                $alias->expand
                    ( -recurse => 4, -groupat => 50, -limit   => 50,
                      -keeptype => [ 'ALIAS', 'PRIMARYACC', 'UNVERSIONED', 
                                     'PRIORVERSION',], );
                my $alifound = 0;
                $hasalias{$sn} = [];
                foreach my $sid ($alias->all_seq_ids) {
                    next if ($sid == $seq->id);
                    $alifound++;
                    my $ali = $mt->get_seq($sid);
                    push @queue, $ali;
                    push @{$hasalias{$sn}}, $ali->name;
                    # do not get into an infinite loop with aliases:
                    $isalias{ $ali->name } = $sn;
                    
                }
                unless ($alifound) {
                    $notMapped{$sn} = 1;
                }
            }
        }
    }
    my @isfound = sort keys %isMapped;
    if ($#isfound > -1) {
        printf($frm->{boldbr}, 'green', '+1', 
               "Some names did not have genomic mappings, but one or more aliases of their's did:");
        foreach my $name ( @isfound ) {
            # needed to clear messages from unfound aliases:
            delete $notMapped{$name};
            my $dat = $isMapped{$name};
            print "<b>".$args->esc_xml($name).": </b> ".join(", ", @{$dat})."<br />";
        }
    }
    
    my @unmapped = sort keys %notMapped;
    if ($#unmapped > -1) {
        print $help->make_link(18);
        printf($frm->{boldbr}, 'red', '+1', 
               "Some names did not have genomic mappings:");
        foreach my $name (@unmapped) {
            my $dat = $notMapped{$name};
            print "<b>$name</b>";
            if (ref($dat) eq 'ARRAY') {
                print "(also tried ".join(", ", @{$dat}).")";
            }
            print "<br />";
        }
        print "<hr />\n";
    }

    # Find the most recent genome version:
    my @allversions = sort { $b <=> $a } keys %subjects;
    my @versions;
    if ($gvers) {
        @versions = ($gvers);
        unless ($subjects{$gvers}) {
            print $help->make_link(17);
            printf($frm->{boldbr}, 'red', '+1', "No genomic clusters were ".
                   "found with specified genome version $gvers.");
            return;
        }
    } else {
        @versions = @allversions;
    }
    my %allChr = ();
    foreach my $ver (@versions) {
        foreach my $chr (keys %{$subjects{$ver}}) {
            $allChr{$chr} = 1;
        }
    }
    my @chrs = sort keys %allChr;
    if ($#chrs < 0) {
        print $help->make_link(17);
        printf($frm->{boldbr}, 'red', '+1', "No genomic clusters were found.");
        return;
    }
    
    my $max = $args->val(qw(CLUSTERMAX));
    print $help->make_link(13);
    printf($frm->{boldbr}, 'green', '+2', "Clustering within $max bp:");
    my $out = "";
    $out .= "<table>";
    my (@allgg, @allrg, @allbest);
    my ($totlen, $totrna, $totbest) = (0,0,0);
    my $cpad = $args->val(qw(CLUSTERPAD));
    foreach my $chr (@chrs) {
        my $name = ($chr =~ /\_/i) ? 
            sprintf("Human_Frag_%s", $chr) : sprintf("Human_Chr_%s", $chr);
        if ($#versions > 0) {
            $out .= "<tr><td colspan='4' bgcolor='yellow'>\n";
            $out .= sprintf($frm->{boldbr}, 'blue', '+1', $name);
            $out .= "</td></tr>";
        }
        foreach my $ver (@versions) {
            next unless ($subjects{$ver}{$chr});
            my @locs = sort { $a->[0] <=> $b->[0] } @{$subjects{$ver}{$chr}};
            my $seed = shift @locs;
            my @clusters = ( [$seed] );
            foreach my $loc (@locs) {
                if ($loc->[0] - $clusters[-1][-1][0] > $max) {
                    push @clusters, [ $loc ];
                } else {
                    push @{$clusters[-1]}, $loc;
                }
            }

            $name .= sprintf(".NCBI_%d", $ver);
            if ($#versions > 0) {
                $out .= sprintf("<tr><td rowspan='%d' bgcolor='yellow'>",$#clusters+1);
                $out .= sprintf($frm->{boldbr}, 'brick', '+1', "NCBI_$ver");
                $out .= "</td>\n";
            } else {
                $out .= "<tr><td colspan='3' bgcolor='yellow'>\n";
                $out .= sprintf($frm->{boldbr}, 'blue', '+1', $name);
                $out .= "</td></tr>";
            }
            
            for my $cn (0..$#clusters) {
                my $cluster = $clusters[$cn];
                my ($start, $stop) = ($cluster->[0][0], $cluster->[-1][1]);
                my ($chrSeq) = $cluster->[0][2];
                my @seqs = map { $_->[3]->name } @{$cluster};
                for my $i (0..$#seqs) {
                    next if ($copies{$seqs[$i]}{$ver} <= 1);
                    $seqs[$i] = sprintf("<font color='red'>%s&nbsp;%dx</font>",
                                        $seqs[$i], $copies{$seqs[$i]}{$ver});
                }
                $out .= "<tr>" unless ($#versions > 0 && $cn == 0);
                $out .= "<td valign='top' align='center' bgcolor='#ff9999'>";
                my @rnaMaps = $mt->get_mappings( -name1     => $chrSeq,
                                                 -keepclass => 'mrna',
                                                 -overlap   => [$start, $stop] );
                $start -= $cpad;
                $stop  += $cpad;
                my $coord = sprintf("chr=%s&vc_start=%d&vc_end=%d", 
                                    $chr, $start, $stop);
                my $len = $stop - $start + 1;
                my $gg = sprintf("%s:%d-%d", $chr, $start, $stop);
                if ($ver == $blisvers) {
                    $out .= sprintf($frm->{anew}, $url->{blisvc} ."?".$coord, 
                                    'View in BLIS');
                    $out .= "<br />\n";
                    $out .= sprintf($frm->{anew}, $url->{blisst} .$gg, 
                                    'Export to SnpTracker');
                    $out .= "<br />\n";
                    push @allgg, $gg; $totlen += $len;
                    push @allbest, $gg; $totbest += $len;
                }
                $out .= sprintf("<b>%d bp<br />%d&nbsp;-&nbsp;%d</b>\n",$len,$start,$stop);
                $out .= "</td><td valign='top' align='center' bgcolor='#ccffff'>";
                if ($#rnaMaps > -1) {
                    my @range = (); my @rnaNames = ();
                    foreach my $map (@rnaMaps) {
                        my $in = $map->seq_index($chrSeq);
                        my $ni = !$in || 0;
                        my @seqs = $map->seqs;
                        my $rnaSeq = $seqs[$ni];
                        my $rnaN   = $rnaSeq->name;
                        push @rnaNames, $rnaN;
                        my $loc = $map->loc_for_seq( -seq => $in );
                        push @range, ($loc->start, $loc->end);
                    }
                    @range = sort {$a <=> $b} @range;
                    my ($rnaStart, $rnaEnd) = ($range[0], $range[-1]);
                    $rnaStart -= $cpad;
                    $rnaEnd   += $cpad;
                    # Make sure we still have all the SNPs:
                    $rnaStart = $start if ($rnaStart > $start);
                    $rnaEnd   = $stop  if ($rnaEnd < $stop);
                    my $coord = sprintf("chr=%s&vc_start=%d&vc_end=%d", 
                                        $chr, $rnaStart, $rnaEnd);
                    my $rg = sprintf("%s:%d-%d", $chr, $rnaStart, $rnaEnd);
                    my $rlen = $rnaEnd - $rnaStart +1;
                    if ($ver == $blisvers) {
                        $out .= sprintf($frm->{anew},$url->{blisvc}."?".$coord,
                                        'mRNAs in BLIS');
                        $out .= "<br />\n";
                        $out .= sprintf($frm->{anew}, $url->{blisst} . $rg, 
                                        'mRNAs to SnpTracker');
                        $out .= "<br />\n";
                        push @allrg, $rg; $totrna += $rlen;
                        pop @allbest; $totbest -= $len;
                        push @allbest, $rg; $totbest += $rlen;
                    }
                    $out .= sprintf("<b>%d bp<br /></b>\n",$rlen);
                    $out .= "<font color='orange'><b>Regional&nbsp;mRNAs</b></font><br />\n";
                    foreach my $rnaN (@rnaNames) {
                        $out .= sprintf($frm->{abrnew}, 'seqname', $rnaN, $rnaN);        
                    }
                }
                $out .= sprintf("</td><td><b>%s</b></td></tr>", join(", ", sort @seqs));
            }
        }
    }
    my $what;
    if ($#allgg > -1) {
        $out .= "<tr><td colspan='4' bgcolor='#99ff33'>\n";
        $out .= sprintf($frm->{boldbr}, 'blue', '+1', "Execute SnpTracker with ".
                        "<i>all</i> Clusters <font size='-1'>(Caution - these links may take a while!)</font>");
        $out .= "</td></tr>";
        $out .= "<tr>";
        if ($#versions > 0) {
            $out .= "<td bgcolor='yellow'>";
            $out .= sprintf($frm->{boldbr}, 'brick', '+1',"NCBI_$blisvers<br />(BLIS)");
            $out .= "</td>\n";
        }
        $out .= "<td valign='top' align='center' bgcolor='#ff9999'>";
        $out .= sprintf("<b>%d Clusters<br />%d bp</b><br />\n",$#allgg+1, $totlen);
        $out .= sprintf($frm->{anew}, $url->{blisst} .join(",",@allgg), 
                        'Export to SnpTracker');

        $out .= "</td><td valign='top' align='center' bgcolor='#ccffff'>";
        if ($#allrg > -1) {
            $out .= sprintf("<b>%d Clusters<br />%d bp</b><br />\n",$#allrg+1, $totrna);
            $out .= sprintf($frm->{anew}, $url->{blisst} .join(",",@allrg), 
                            'Export RNA to SnpTracker');
        }

        $out .= "<td valign='top' align='center' bgcolor='#ccff99'>";
        $out .= sprintf("<b>%d Clusters<br />%d bp</b><br />\n",$#allbest+1, $totbest);
        $what = $url->{blisst} .join(",",@allbest);
        $out .= sprintf($frm->{anew}, $what, 
                        'Export \'best\' to SnpTracker');

        $out .= "</td></tr>";
    }

    $out .= "</table>\n";
    print $out;

    if (my $where = $args->val(qw(AUTOLAUNCH))) {
        if ($what) {
            print "<script language='javascript'>\n<!--\n";
            print <<EOF;
            function redirect() {
                // var what = escape("$what");
                window.location.href = '$what';
            }
            setTimeout("redirect();", 10)
            // -->\n</script>
EOF

            exit;        
        } else {
            print "<p>";
            print $help->make_link(89);
            printf($frm->{boldbr}, 'red', '+1', 
                   "Sorry, I could not find any clusters to send to $where");
            print "</p>";
        }
    }
    
}

sub SET_EDITS {
    my %edits;
    my @match = ('setclass','deleteclass','settype','deletetype');
    my %check = map { uc($_) => 1 } @match;
    my %found;
    while ( my ($tag, $val) = each %{$args}) {
        my @stuff = split('_', $tag);
        my $type = shift @stuff;
        next unless ($check{$type} && $val);
        $found{$type} ||= [];
        push @{$found{$type}}, [$val, @stuff];
    }
    while (my ($type, $data) = each %found) {
        if ($type eq 'SETCLASS') {
            &SETCLASS($data);
        } elsif ($type eq 'DELETECLASS') {
            &DELETECLASS($data);
        } elsif ($type eq 'SETTYPE') {
            &SETTYPE($data);
        } elsif ($type eq 'DELETETYPE') {
            &DELETETYPE($data);
        } else {
            printf($frm->{boldbr}, 'red', '+1', "I do not know how to update with '$type':");
            $args->debug->branch($data);
        }
    }
}

sub SETCLASS {
    my ($data) = @_;
    my $num = $#{$data} + 1;
    my $what = sprintf("%d Sequence%s", $num, $num == 1 ? "" : "s");
    printf($frm->{boldbr}, 'darkgreen', '+1', "Assigning classes to $what");
    print "<pre><font color='blue'>";
    foreach my $row (@{$data}) {
        my ($cids, $sid) = @{$row};
        $cids = ref($cids) eq 'ARRAY' ? $cids : [$cids];
        foreach my $cid (@{$cids}) {
            my $seq   = $mt->get_seq( $sid );
            my $class = $mt->get_class($cid);
            if ($seq && $class) {
                printf("  Assigning '%s' to name '%s'\n",
                       $class->name, $seq->name);
            } else {
                next;
            }
            $mt->seqclass($sid, $cid);
        }
    }
    print "</font></pre>\n";
}

sub DELETECLASS {
    my ($data) = @_;
    my $num = $#{$data} + 1;
    my $what = sprintf("%d Sequence%s", $num, $num == 1 ? "" : "s");
    my $dbh = $mt->dbi;
    printf($frm->{boldbr}, 'darkgreen', '+1', "Removing classes from $what");
    print "<pre><font color='blue'>";
    foreach my $row (@{$data}) {
        my ($cids, $sid) = @{$row};
        $cids = ref($cids) eq 'ARRAY' ? $cids : [$cids];
        foreach my $cid (@{$cids}) {
            my $seq   = $mt->get_seq( $sid );
            my $class = $mt->get_class($cid);
            if ($seq && $class) {
                printf("  Removing associations of '%s' to name '%s'\n",
                       $class->name, $seq->name);
            } else {
                next;
            }
            my $sql = "DELETE FROM seq_class WHERE name_id = $sid ".
                "AND authority_id = $aid AND class_id = $cid";
            $mt->_showSQL($sql, "Delete Class") if ($mt->{DUMPSQL} > 2);
            $dbh->command($sql);
        }
    }
    print "</font></pre>\n";
}

sub SETTYPE {
    my ($data) = @_;
    my $num = $#{$data} + 1;
    my $what = sprintf("%d Sequence%s", $num, $num == 1 ? "" : "s");
    printf($frm->{boldbr}, 'darkgreen', '+1', 
           "Assigning relationships to $what");
    print "<pre><font color='blue'>";
    foreach my $row (@{$data}) {
        my ($tids, $sid) = @{$row};
        $tids = ref($tids) eq 'ARRAY' ? $tids : [$tids];
        foreach my $tid (@{$tids}) {
            my $seq   = $mt->get_seq( $sid );
            my ($tag, $dir) = ("FORNAME_$sid", 0);
            if ($tid < 0) {
                $tid *= -1; $dir = 1;
                $tag = "REVNAME_$sid";
            }
            next unless ( $args->val(qw($tag)));
            my $type  = $mt->get_type($tid);
            my $oseq  = $mt->get_seq( $args->val(qw($tag)) );
            my @seqs  = ($seq, $oseq);
            if ($seq && $type && $oseq) {
                my @reads = $type->reads;
                printf("  Building relationship '%s %s %s'\n",
                       $seqs[$dir]->name, $reads[$dir], $seqs[!$dir]->name);
            } else {
                next;
            }
            $mt->relate($seqs[$dir]->id, $seqs[!$dir]->id, $type->id);
        }
    }
    print "</font></pre>\n";
}

sub DELETETYPE {
    my ($data) = @_;
    my $num = $#{$data} + 1;
    my $what = sprintf("%d Sequence%s", $num, $num == 1 ? "" : "s");
    my $dbh = $mt->dbi;
    printf($frm->{boldbr}, 'darkgreen', '+1', "Removing relationships from $what");
    print "<pre><font color='blue'>";
    foreach my $row (@{$data}) {
        my ($reads, $sid, $oid) = @{$row};
        $reads = ref($reads) eq 'ARRAY' ? $reads : [$reads];
        foreach my $read (@{$reads}) {
            my $seq  = $mt->get_seq( $sid );
            my $oseq = $mt->get_seq( $oid );
            my $type = $mt->get_type($read);
            my @seqs  = ($seq, $oseq);
            if ($seq && $oseq && $type) {
                my @rs = $type->reads;
                if ($read eq $rs[0]) {
                    # Forward direction
                } elsif ($read eq $rs[1]) {
                    @seqs = reverse @seqs;
                } else {
                    $mt->error("Type ".$type->name." does not have a relationship '$read'");
                    next;
                }
                printf("  Removing relationship '%s %s %s'\n",
                       $seqs[0]->name, $read, $seqs[1]->name);
            } else {
                next;
            }
            my $sql = sprintf
                ("DELETE FROM relation WHERE name1 = %d AND name2 = %d ".
                 "AND authority_id = %d AND type_id = %d", 
                 $seqs[0]->id, $seqs[1]->id, $aid, $type->id );
            $mt->_showSQL($sql) if ($mt->{DUMPSQL} > 2);
            $dbh->command($sql);
        }
    }
    print "</font></pre>\n";
}

sub QUICK_RELATE {
    my ($n1, $n2, $type) = &_QUICK_REL_PARSE(@_);
    return unless ($n1 && $n2 && $type);
    my ($reads) = $type->reads;
    my $tid = $type->id;
    print "<font color='green'>You have succesfully <u>set</u> the relationship(s):</font><pre>";
    foreach my $seq1 (@{$n1}) {
        foreach my $seq2 (@{$n2}) {
            $mt->relate($seq1->id, $seq2->id, $tid);
            printf("<font color='green'>SET:</font> <font color='orange'><b>%s</b> %s <b>%s</b></font>   <a href='mapTracker.pl?quickunrelate=1&name1=%d&name2=%d&type=%d'>undo</a>\n", $seq1->name, $reads, $seq2->name, $seq1->id, $seq2->id, $tid );
        }
    }
    if ($#{$n1} > 0) {
        my $allid = join('%0D', map {$_->id} @{$n1});
        printf("\n   <a href='mapTracker.pl?quickunrelate=1&name1=%s&name2=%d&type=%d'>Undo all</a>", $allid, $n2->[0]->id, $tid );
    } elsif ($#{$n2} > 0) {
        my $allid = join('%0D', map {$_->id} @{$n2});
        printf("\n   <a href='mapTracker.pl?quickunrelate=1&name1=%d&name2=%s&type=%d'>Undo all</a>", $n1->[0]->id, $allid, $tid );
    }
    print "</pre>";
    return;
}

sub QUICK_UNRELATE {
    my ($n1, $n2, $type) = &_QUICK_REL_PARSE(@_);
    return unless ($n1 && $n2 && $type);
    my ($reads) = $type->reads;
    my $tid = $type->id;
    print "<font color='green'>You have succesfully <u>deleted</u> the relationship(s):</font><pre>";
    my @killed;
    foreach my $nm1 (@{$n1}) {
        my $seq1 = $mt->get_seq($nm1);
        foreach my $nm2 (@{$n2}) {
            my $seq2 = $mt->get_seq($nm2);
            my $where =  sprintf
                ("FROM relation WHERE name1 = %d AND name2 = %d ".
                 "AND authority_id = %d AND type_id = %d", 
                 $seq1->id, $seq2->id, $aid, $tid );
            my $dbh = $mt->dbi;
            my $exists = $dbh->get_single_value("SELECT count(*) $where");
            if ($exists) {
                my $sql = "DELETE $where";
                $mt->_showSQL($sql) if ($mt->{DUMPSQL} > 2);
                $dbh->command($sql);
                printf("<font color='red'>DELETED:</font> <font color='orange'><b>%s</b> %s <b>%s</b></font>   <a href='mapTracker.pl?quickrelate=1&name1=%d&name2=%d&type=%d'>undo</a>\n", $seq1->name, $reads, $seq2->name, $seq1->id, $seq2->id, $tid );
                push @killed, [ $seq1->id, $seq2->id ];
            } else {
                printf("<font color='#996600'>IGNORED:</font> <font color='orange'><b>%s</b> %s <b>%s</b></font>  (did not exist in DB)\n", $seq1->name, $reads, $seq2->name);
            }
        }
    }
    if ($#{$n1} > 0 && $#killed > 0) {
        my $allid = join('%0D', map { $_->[0] } @killed);
        printf("\n   <a href='mapTracker.pl?quickrelate=1&name1=%s&name2=%d&type=%d'>Undo all</a>", $allid, $n2->[0]->id, $tid );
    } elsif ($#{$n2} > 0 && $#killed > 0) {
        my $allid = join('%0D', map { $_->[1] } @killed);
        printf("\n   <a href='mapTracker.pl?quickrelate=1&name1=%d&name2=%s&type=%d'>Undo all</a>", $n1->[0]->id, $allid, $tid );
    }
    print "</pre>";
    return;    
}

sub _QUICK_REL_PARSE {
    my ($name1,$name2,$tname) = ($args->val(qw(NAME1)),$args->val(qw(NAME2)),$args->val(qw(TYPE)));
    my ( @n1, @n2);
    foreach my $n (split(/[\n\r\t]/, $name1)) {
        push @n1, $mt->get_seq($n) if ($n);
    }
    foreach my $n (split(/[\n\r\t]/, $name2)) {
        push @n2, $mt->get_seq($n) if ($n);
    }
    unless ($#n1 > -1 && $#n2 > -1) {
        print "<font color='red'>To alter relationships you need to provide both names (Provided with '$name1' and '$name2')</font><br />\n";
        return;
    }
    if ($#n1 > 0 && $#n2 >0 ) {
        print "<font color='red'>You have included multiple names for both the first AND second term in a relationship. One of the names must be unique (this is to avoid hard-to-revert mutliplicative mistakes)</font><br />\n";
        return;    
    }
    my $type = $mt->get_type($tname);
    unless ($type) {
        print "<font color='red'>You need to provide a valid relationship type in order to relate $name1 with $name2 (I could not find one using '$tname')/font><br />\n";
        return;
    }
    return (\@n1, \@n2, $type);
}

sub QUICK_BOX {
    print "<p><font size='-1' color='#cc9966'><i>This is a static status window that reports success or failure for 'quick' edits. It may be useful to shrink it and leave it in a corner of your screen while you perform your work. Note that the page you are working from will not reflect any edits you are making until you reload it.</i></font></p>";
    $dofind = 0;
}
