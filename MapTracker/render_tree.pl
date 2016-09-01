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


my %counters;

my $templateDir = "/stf/biohtml/TumbleWeed/Templates";

my $allowed = {
    name => [ 'symbol', 'gene' ],
    val  => [ 'value', 'count', 'pc', 'percent control' ],
    expt => [ 'experiment', 'bms_no', 'cmpd', 'compound' ],
};

my $OLDstndBundle = 
    join("\\n", 
         ("agc.ph  /angle=25 /title=AGC",  "camk.ph /angle=90 /title=CAMK", 
          "ck1.ph  /angle=355 /title=CK1", "cmgc.ph /angle=155 /title=CMGC",
          "ste.ph  /angle=330 /title=STE", "tk.ph   /angle=215 /title=TK",
          "tkl.ph  /angle=280 /title=TKL") );
my $stndBundle = 
    join("\\n", 
         ("agc.ph  /angle=35 /title=AGC",  "camk.ph /angle=90 /title=CAMK", 
          "ck1.ph  /angle=0 /title=CK1", "cmgc.ph /angle=155 /title=CMGC",
          "ste.ph  /angle=320 /title=STE", "tk.ph   /angle=215 /title=TK",
          "tkl.ph  /angle=270 /title=TKL") );

my @overlayCols =
    ( nodecolor   => "Light Grey",
      labelcolor  => "Light Grey",
      edgecolor   => "Light Grey",
      branchcolor => "Dark Grey",
      bundlecolor => "Light Blue",
      );

my $hermColor ="val <= 1 red\\nval <= 2 gold\\nval <= 3 blue\\nviolet";

my $examp = {
    'Huge TumbleWeeds'  => [ {
        name     => "All Manning Kinases",
        bundle   => "ePK.ph Tree 1 Root",
        kidns    => "ManningKinase",
        desc       => "Manning tree as a single alignment. Such data is not that accurate",
    } ],
    'XML Templates'  => [ {
        name     => "Ambit panel - Medium",
        precan   => "Manning_Medium_Mk2.xml",
        desc       => "Medium sized TumbleBundle for Ambit data, example shows only three specific experiments",
        resultpath  => "/home/tilfordc/public_html/Current_Ambit_Data.txt",
        experiment  => 'BMS-326107\\nBMS-594647\\nBMS-723387',
        custscheme  => $hermColor,
        allownull    => 1,        
    },{
        name     => "Ambit panel - Poster",
        precan   => "Manning_Poster_Mk2.xml",
        desc       => "Huge TumbleBundle for Ambit data, example with a single experiment",
        resultpath  => "/home/tilfordc/public_html/Current_Ambit_Data.txt",
        experiment  => 'BMS-326107',
        custscheme  => $hermColor,
        allownull    => 1,        
    },{
        name     => "Ambit panel - Large",
        precan   => "Manning_Large_Mk2.xml",
        desc       => "Large TumbleBundle for Ambit data, example with a single experiment",
        resultpath  => "/home/tilfordc/public_html/Current_Ambit_Data.txt",
        experiment  => 'BMS-326107',
        custscheme  => $hermColor,
        allownull    => 1,        
    },{
        name     => "Ambit panel - Mini",
        precan   => "Manning_Mini_Mk2.xml",
        desc       => "Tiny ambit template - example will show ALL experiments",
        resultpath  => "/home/tilfordc/public_html/Current_Ambit_Data.txt",
        custscheme  => $hermColor,
        allownull    => 1,        
    }, ],
    'Kinase Subfamilies' => [ {
        name     => 'AGC Kinases',
        bundle   => "agc.ph Tree 1 Root",
        kidns    => "ManningKinase",
    }, {
        name     => 'CAMK Kinases',
        bundle   => "camk.ph Tree 1 Root",
        kidns    => "ManningKinase",
    }, {
        name     => 'CK1 Kinases',
        bundle   => "ck1.ph Tree 1 Root",
        kidns    => "ManningKinase",
    }, {
        name     => 'CMGC Kinases',
        bundle   => "cmgc.ph Tree 1 Root",
        kidns    => "ManningKinase",
    }, {
        name     => 'STE Kinases',
        bundle   => "ste.ph Tree 1 Root",
        kidns    => "ManningKinase",
    }, {
        name     => 'TK Kinases',
        bundle   => "tk.ph Tree 1 Root",
        kidns    => "ManningKinase",
    }, {
        name     => 'TKL Kinases',
        bundle   => "tkl.ph Tree 1 Root",
        kidns    => "ManningKinase",
    } ],
    'TumbleBundles'  => [ {
        name     => 'Bundle of all kinases',
        desc     => "Single TumbleBundle of all Manning kinases, including atypicals",
        bundle   => $stndBundle,
        kidns    => "ManningKinase",
        residual => 'ePK.ph',
    }, {
        name     => 'Poster of all kinases',
        desc     => "Hi-resolution render of all Manning Kinases, suitable for poster printing",
        bundle   => $stndBundle,
        kidns    => "ManningKinase",
        residual => 'ePK.ph',
        edgesize => 5,
        scale    => 50,
        bundlesize => 5,
        branchsize => 5,
        nodesize   => 15,
        labelsize  => 24,
    }, {
        name     => 'Mini Bundle',
        desc     => "Itty-bitty TumbleBundle, as might be used for a panel showing many compounds",
        bundle   => $stndBundle,
        kidns    => "ManningKinase",
        rootsize   => 0,
        branchsize => 0,
        labelsize  => 0,
        scale      => 2,
        nodesize   => 1,
        residual => 'ePK.ph',
    } ],
    'Experimental Overlay'  => [ {
        name     => 'Ambit Data',
        bundle   => $stndBundle,
        desc       => "Colorized assay results for a handful of compounds",
        kidns      => "ManningKinase",
        nodesize   => 0,
        rootsize   => 0,
        branchsize => 0,
        labelsize  => 0,
        scale      => 2,
        @overlayCols,
        resultpath  => "/home/tilfordc/public_html/random/Text_Kinase_Data.tsv",
        residual => 'ePK.ph',
    }, {
        name       => 'Scientist Requests',
        desc       => "Example of custom color scheme showing relative interest amongst the TK family",
        bundle     => "tk.ph",
        kidns      => "ManningKinase",
        @overlayCols,
        edgesize   => 5,
        scale      => 50,
        bundlesize => 5,
        branchsize => 5,
        nodesize   => 15,
        datasize   => 13,
        labelsize  => 24,
        ascending  => 1,
        resultpath  => "/home/tilfordc/public_html/random/scientist_votes.txt",
        custscheme  => "val >= 5 ff0000\\nval >= 2 ffdd22\\nval >= 1 0000ff\\n#cccccc",
    }, {
        name       => 'Scientist Requests (atypical kinases)',
        desc       => "Example of custom color scheme showing relative interest amongst the atypical kinases",
        bundle     => $stndBundle,
        kidns      => "ManningKinase",
        edgesize   => 5,
        scale      => 50,
        bundlesize => 5,
        branchsize => 5,
        nodesize   => 15,
        datasize   => 13,
        labelsize  => 24,
        @overlayCols,
        resultpath  => "/home/tilfordc/public_html/random/scientist_votes.txt",
        residual    => 'ePK.ph',
        residualonly => 1,
        custscheme  => "val >= 5 ff0000\\nval >= 2 ffdd22\\nval >= 1 0000ff\\n#cccccc",
    }, {
        name       => 'Hi-Res overlay',
        desc       => "Medium-scale poster-sized data overlay, selecting a single compound from a larger data set",
        bundle     => $stndBundle,
        kidns      => "ManningKinase",
        nodesize   => 9,
        @overlayCols,
        rootsize   => 0,
        branchsize => 0,
        labelsize  => 14,
        edgesize   => 5,
        datasize   => 7,
        experiment => 'BMS-185779',
        scale      => 30,
        bundlesize => 0,
        resultpath  => "/home/tilfordc/public_html/Current_Ambit_Data.txt",
        residual    => 'ePK.ph',
        custscheme  => $hermColor,
        allownull    => 1,        
    }, {
        name       => 'All Ambit Data',
        bundle     => $stndBundle,
        desc       => '(initial settings are for just 3 specific compounds)',
        kidns      => "ManningKinase",
        nodesize   => 3,
        @overlayCols,
        rootsize   => 0,
        branchsize => 0,
        labelsize  => 0,
        edgesize   => 1,
        datasize   => 3,
        scale      => 5,
        bundlesize => 3,
        resultpath  => "/home/tilfordc/public_html/Current_Ambit_Data.txt",
        experiment  => 'BMS-326107\\nBMS-594647\\nBMS-723387',
        residual    => 'ePK.ph',
        custscheme  => $hermColor,
        allownull    => 1,        
    }, {
        name       => 'Empty Values Mk1',
        bundle     => $stndBundle,
        desc       => 'Demonstrates color mark-up of empty/null experimental values',
        kidns      => "ManningKinase",
        nodesize   => 7,
        @overlayCols,
        rootsize   => 0,
        branchsize => 0,
        labelsize  => 9,
        edgesize   => 4,
        datasize   => 5,
        scale      => 10,
        bundlesize => 3,
        resultpath  => "/home/tilfordc/public_html/Current_Ambit_Data.txt",
        experiment  => 'BMS-723387',
        residual    => 'ePK.ph',
        custscheme  => $hermColor,
        allownull    => 1,
    } ],
};


use strict;
use BMS::ArgumentParser;
use BMS::MapTracker;
use BMS::MapTracker::Network;
use BMS::TumbleWeed;


my $defaults = {
    branchcolor => 'Dim Gray',
    branchsize  => 3,
    bundle      => "",
    bundlecolor => 'Yellow',
    bundlesize  => 1,
    children    => "",
    coloredge   => 1,
    datacolor   => 'Fuchsia',
    datasize    => 3,
    edgecolor   => 'Gray',
    edgesize    => 1,
    titlecolor  => 'Light Pink',
    titlesize   => 0,
    kidns       => "",
    label       => 1,
    labelcolor  => 'Green',
    labelsize   => 9,
    nodecolor   => 'Blue',
    nodesize    => 9,
    residual    => "",
    root        => '',
    rootcolor   => 'Yellow',
    rootsize    => 7,
    scale       => 30,
    experiment  => "",
    custscheme  => "",
    nocarp      => 0,
    residualonly => 0,
    allownull    => 0,
    precan       => '',
    labelfont    => 'arial',
    legendname   => 3,
    legendkey    => 2,
    onlylabeldata => '',
};

my (@custScheme, @allCustColors, $tempDir);


my $args = BMS::ArgumentParser->new
    ( -nocgi         => $ENV{'HTTP_HOST'} ? 0 : 1,
      -resultpath  => '',
      -tilecol     => 5,
      -edgelen     => 0.5,
      -graphviz    => 0,
      -htmltree    => 0,
      -htmlwidth   => 0,
      -tumbleweed  => 1,
      -width       => 0,
      -tiddlywiki  => 'RenderTree',
      %{$defaults},
      -show_cvs_vers => 'html cvs', );

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


my $nocgi   = $args->val(qw(NOCGI));
my $legName = $args->val(qw(LEGENDNAME));
my $legKey  = $args->val(qw(LEGENDKEY));
my $fastaMeta = $args->val(qw(metafasta fastameta));

my $ldap = $ENV{'REMOTE_USER'} || $ENV{'LDAP_USER'} || 0;
$args->set_mime( -mail => 'charles.tilford@bms.com' ) unless ($nocgi);

#&WRITE_LOG( -args     => $args,
#            -capture  => [ 'BUNDLE', 'PRECAN', 'RESIDUAL', 'KIDNS', 'EXPERIMENT', 'RESULTPATH', 'RESULTS' ], );

my $mt;
my $isbeta = 0;
my $lentag = 'Branch Length';

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


&HTMLSTART;
unless ($mt) {
    $args->err("<center><h1>".&help('NoMapTracker'). "<font color='red'>MapTracker Database Unavailable</font></h1></center>",
               "Error details:\n" . $@,
               "Any feature requiring database access will fail.",
               "Operations using static files (eg XML-defined trees) will still work");
}
&ACT_ON_ARGS;
&HTMLFORM;
&HTMLEND;


sub ACT_ON_ARGS {
    return if ($args->val(qw(NOACTION)));
    BMS::CommonCAT->error_handler( \&err );
    my $targ;
    if ($targ = $args->val(qw(PRECAN))) {
        $targ .= '.xml' if ($targ !~ /^\// && $targ !~ /\.xml$/);
    } else {
        my $path;
        if (my $utfh = $args->val(qw(USER_TREE_FH))) {
            $args->{BUNDLE} ||= '';
            $path = &MAKE_TEMP_FILE( 'USER_TREE', "User Tree");
        } elsif (my $fa = $args->val('fasta')) {
            $path = &run_alignment($fa);
        }
        if ($path) {
            if ($args->val(qw(BUNDLE))) {
                $args->{BUNDLE} .= "\n$path";
            } else {
                $args->{BUNDLE} = "$path";
            }
            $targ = $args->{BUNDLE};
        }

    }
    return unless ($targ);

    if (my $rfh = $args->val(qw(RESULTS_FH))) {
        $args->{RESULTPATH} = &MAKE_TEMP_FILE
            ( 'RESULTS', "Experimental Data");
    }

    &PARSE_SCHEME( $args->val(qw(CUSTSCHEME)) );
    &PARSE_RESULTS();
    &PARSE_MULTI( $targ );
}

sub run_alignment {
    my $fa = shift;
    unless ($fa) {
        &msg("Can not run alignment without fasta data", "RawAlignment","");
        return "";
    }
    my $file = &MAKE_TEMP_FILE_FROM_TEXT( "User Fasta Sequence", "fa", $fa );
    my ($shtxt, $base, $stderr) = &t_coffee_script( $file );
    my $sh     = &MAKE_TEMP_FILE_FROM_TEXT( "runAlign", "sh", $shtxt);
    chmod(0775, $sh);
    system($sh);
    if (-s $stderr) {
        &msg("Run log found: ".&_link($stderr));
    } else {
        &err("Run log not found", "NoStdErr");
    }
    my @chk = (["Raw Alignment", "$base.aln"],
               ["HTML Alignment", "$base.html"]);
    foreach my $c (@chk) {
        my ($wht, $f) = @{$c};
        &msg("$wht: ".&_link($f)) if (-s $f);
    }
    my $dnd = "$base.dnd";
    unless (-s $dnd) {
        &err("DND tree file was not created", "NoDND");
        return "";
    }
    $fastaMeta = $file;
    return $dnd;
}

sub t_coffee_script {
    my $file = shift;
    my $txt = "#!/bin/bash\n";
    my $dir = "";
    if ($file =~ /(.+)\/([^\/]+)$/) {
        ($dir, $file) = ($1, $2);
        $txt .= "cd \"$dir\"\n";
    }
    my $base = $file;
    $base =~ s/\.fa$//;
    my $stderr = "$base.stderr";
    my $stdout = "$base.stdout";
    $txt .= <<EOF;

/stf/biobin/t_coffee \\
    -seq "$file" \\
    2> "$stderr" \\
    1> "$stdout"
EOF

    if ($dir) {
        $stderr = "$dir/$stderr";
        $base   = "$dir/$base";
    }
    return ($txt, $base, $stderr);
}

sub _link {
    my ($path) = @_;
    return $args->path2link($path, { 
        style => 'font-family: monospace',
        target => 'renderTreeLinks' });

}

sub err {
    my ($msg, $key, $class) = @_;
    $class ||= 'err';
    &msg($msg, $key, $class);
}

sub msg {
    my ($msg, $key, $class) = @_;
    if ($nocgi) {
        $msg =~ s/<[^>]+>//g;
        warn "$msg\n";
    } else {
        $class = $class ? "$class twmsg" : 'twmsg';
        $msg = &help($key) . $msg if ($key);
        $msg = "<span class='$class'>$msg</span><br />\n";
        print $msg;
    }
}

sub TEMP_DIR {
    unless ($tempDir) {
        $tempDir = sprintf("/stf/biohtml/tmp/TumbleWeeds/%d_%d", $$, time);
        $args->assure_dir($tempDir);
        &msg("Working directory created: ".&_link($tempDir),"WorkingDirectory");
    }
    return $tempDir;
}

sub MAKE_TEMP_FILE {
    my ($key, $base) = @_;
    my $fh     = $args->{$key.'_FH'};
    my $source = $args->val($key) || "User";
    my $suffix = 'txt';
    if ($source =~ /\.([^\.]+)$/) {
        $suffix = $1;
    }
    my $text = "";
    while(<$fh>) {
        $text .= $_;
    }
    return &MAKE_TEMP_FILE_FROM_TEXT( $base, $suffix, $text);
}

sub MAKE_TEMP_FILE_FROM_TEXT {
    my ($base, $suffix, $text) = @_;
    $base    ||= "User Data";
    my $fbase = $base; 
    $fbase    =~ s/\s+/_/g;
    my $tfile = sprintf("%s/%s.%s", &TEMP_DIR(), $fbase, $suffix);
    open(TFILE, ">$tfile")
        || $args->death("Failed to write temporary file", $tfile, $!);
    print TFILE $text;
    close TFILE;
    
    chmod(0777, $tfile);
    my $link = &_link($tfile);
    $args->msg("[>]", "Your $base has been written to a temporary file",
               $link);
    return $tfile;
}

sub PARSE_RESULTS {
    my $fh;
    if (my $path = $args->val(qw(RESULTPATH))) {
        unless (-e $path) {
            &err("I could not find a server-side experiment file at '$path'",
                   'NoServerFile');
            return;
        }
        my $success = open( RFH, "<$path" );
        unless ($success) {
            &err("Failed to open '$path'\n  $!",'FileOpenFailure');
            return;
        }
        $fh = *RFH;
    } else {
        $fh = $args->val(qw(RESULTS_FH));
    }
    unless ($fh) {
        &err("No experimental data provided, drawing template tree.",
               'NoExperimentalData', 'wrn');
        return;
    }
    my $head = <$fh>;
    $head =~ s/[\n\r]+$//;
    my $exp;
    my $expReq = $args->val(qw(EXPERIMENT EXPERIMENTS)) || '';
    $expReq =~ s/^\s+//;
    if ($expReq) {
        $exp = {};
        foreach my $cmpd (split(/[\n\r\t]+/, $expReq)) {
            next unless ($cmpd);
            $exp->{&stnd_cmpd_id($cmpd)} = 1;
        }
        $args->{EXPERIMENT} = join("\n", sort keys %{$exp});
    }

    my ($results, $errs);
    
    if ($head =~ /^BMS_Number\t/) {
        ($results, $errs) = &HERMS_FILE($fh, $head, $exp);
    } else {
        ($results, $errs) = &NORMAL_FILE($fh, $head, $exp);
    }
    close $fh;

    my $doAscend = $args->val(qw(ASCENDING)) ? 1 : 0;
    my $count = 0;
    while (my ($cmpd, $hash) = each %{$results}) {
        my (@arr, @nulls);
        while (my ($gene, $val) = each %{$hash}) {
            if (defined $val) {
                push @arr, [$gene, $val];
            } else {
                push @nulls, [$gene, $val];
            }
        }
        @arr = $doAscend ? 
            sort { $a->[1] <=> $b->[1] } @arr :
            sort { $b->[1] <=> $a->[1] } @arr;
        $results->{$cmpd} = [@arr, @nulls ];
        $count++;
    }
    unless ($count) {
        if ($exp) {
            my $key = "Filters for specific experiments fail to find data";
            $key .= &help('ExperimentalDataMissing') unless($nocgi);
            map { $errs->{$key}{$_}++ } keys %{$exp};
        } else {
            my $key = "No data were found";
            $key .= &help('NoExperimentalDataInFile') unless($nocgi);
            $errs->{$key} = {};
        }
    }
    my @etxt = sort keys %{$errs};
    if ($#etxt > -1 && !$args->val(qw(NOCARP))) {
        my @bits;
        foreach my $bit (@etxt) {
            push @bits, ("$bit:", map { "  $_" } sort keys %{$errs->{$bit}});
        }
        $args->err("There were errors parsing your experimental data",
                   @bits);
    }
    $args->{AMBIT} = $results;
}

sub HERMS_FILE {
    my ($fh, $head, $exp) = @_;
    my @genes;
    foreach my $col (split(/\t/,$head)) {
        my @bits = split('-', $col);
        push @genes, $bits[-1];
    }
    my (%results, %errs);
    my $empt = $args->val(qw(ALLOWNULL));
    while (<$fh>) {
        s/[\n\r]+$//;
        my @row  = split(/\t/);
        my $cmpd = &stnd_cmpd_id($row[0]);
        next if ($exp && !$exp->{$cmpd});
        for my $c (1..$#genes) {
            my $sym = $genes[$c];
            my $val = $row[$c];
            $val = undef if (defined $val && $val eq '');
            if (!defined $val) {
                $results{$cmpd}{$sym} = undef if ($empt);
                next;
            }
            if (&IS_NUMBER($val)) {
                $results{$cmpd}{$sym} = $val;
            }
        }
    }
    return (\%results, \%errs);
}

sub NORMAL_FILE {
    my ($fh, $head, $exp) = @_;
    $head =~ s/[\n\r]+$//;
    my @cols = split(/\t/, lc($head));
    # LookUp hash:
    my %lu = map { $cols[$_] => $_ } (0..$#cols);

    my @colErrs;
    my (%results, %errs);
    foreach my $col (sort keys %{$allowed}) {
        my @arr = ($col, @{$allowed->{$col}});
        foreach my $try (@arr) {
            $lu{$col} = $lu{$try} if (defined $lu{$try}) ;
        }
        push @colErrs, sprintf
            (" %s - also allowed: %s\n", $col, join(", ", @{$allowed->{$col}}))
            unless (defined $lu{$col});
    }
    if ($#colErrs > -1) {
        foreach my $ce (@colErrs) {
            my $key = "Missing column";
            $key .= &help('DataColumnMissing') unless($nocgi);
            $errs{$key}{$ce}++;
        }
        return ( undef, \%errs );
    }
    my @colnums = map { $lu{$_} } qw(expt name val);
    my $empt = $args->val(qw(ALLOWNULL));

    my $ns = $args->val(qw(KIDNS));
    while (<$fh>) {
        s/[\n\r]+$//;
        next unless ($_);
        my @row = split(/\t/, $_);
        my @cells = map { defined $row[ $_ ] ? $row[$_] : "" } @colnums;
        # Strip leading and trailing whitespace:
        map { s/^\s+//; s/\s+$//; } @cells;
        my ($cmpd, $sym, $val) = map { $_ ne '' ? $_ : undef } @cells;

        my @details;
        push @details, "no value" unless (defined $val || $empt );
        push @details, "no experiment name" unless (defined $cmpd);
        push @details, "no gene name" unless (defined $sym);
        if ($#details > -1) {
            my $msg = sprintf("%s (%s)", $_, join(', ', @details));
            my $key = "Malformed data line";
            $key .= &help('MalformedDataLine') unless($nocgi);
            $errs{$key}{$msg}++;
            next;
        }
        next if ($exp && !$exp->{$cmpd});
        if (defined $results{$cmpd}{$sym}) {
            my $key = "Multiple assay results defined";
            $key .= &help('DuplicatedAssays') unless($nocgi);
            $errs{$key}{"$cmpd vs $sym"}++;
            next;
        }
        if ($ns) {
            unless ($mt) {
                my $msg = "Can not parse node name with namespace (#$ns#$sym) unless MapTracker database is online";
                $msg .= &help('NoMapTracker') unless($nocgi);
                die "$msg\n  ";
            }
            my $seq = $mt->get_seq(-name => "#$ns#??$sym", -nocreate => 1);
            unless ($seq) {
                my $key = "Could not find gene";
                $key .= &help('UnknownGene') unless($nocgi);
                $errs{$key}{$sym}++;
            }
        }
        $sym =~ s/^\#.+\#//;
        $results{$cmpd}{$sym} = $val;
    }
    return (\%results, \%errs);
}

sub IS_NUMBER {
    my ($val) = @_;
    return ($val =~ /^[\+-]?\d+$/ || $val =~ /^[\+-]?\d+\.\d+$/) ? 1 : 0;
}

sub PARSE_MULTI {
    my ($req) = @_;
    my @reqs;
    my $uns = $args->val(qw(KIDNS)) || '';
    $uns = "#$uns#" if ($uns);
    my (@nws, @layouts, @canvi);
    foreach my $req (split(/[\r\n\t]+/, $req)) {
        my ($lo, $nw) = &LAYOUT_FOR_QUERY( $req );
        next unless ($lo);
        my @objs = ref($lo) eq 'ARRAY' ? @{$lo} : $lo;
        foreach my $obj (@objs) {
            if ($obj->isa('TumbleWeed::Layout')) {
                push @layouts, $obj;
            } elsif ($obj->isa('TumbleWeed::Canvas')) {
                push @canvi, $obj;
            } else {
                &err("I don't know what to do with '$lo' from $req",
                       'InappropriateXmlObject');
            }
        }
        push @nws, $nw;
    }
    if ($#canvi > -1) {
        my $count = $#canvi + 1;
        if ($count > 1) {
            &err("You have defined multiple ($count) canvases - ".
                   "you should define only 1.", 'TooManyCanvases');
            return;
        } elsif ($#layouts > -1) {
            &err("You have defined both layouts and a canvas - ".
                   "please do one or the other.", 'MixedXmlInput');
            return;
        }
        &CANVAS( $canvi[0] );
    } else {
        if (my $res = &GET_RESIDUAL( \@nws ) ) {
            foreach my $param (qw(angle radius)) {
                $res->param( $param, 0 ) unless ($res->param($param));
            }
            if ($args->val(qw(RESIDUALONLY))) {
                @layouts = $res;
            } else {
                push @layouts, $res;
            }
        }
        &TUMBLEWEED( \@layouts );
    }
}

sub PARAMS_FOR_QUERY {
    my $req = $_[0] || '';
    my %params;
    while ($req =~ /(\/[a-z]+\=\S+)/i) {
        my $tag = $1;
        if ($tag =~ /([a-z]+)\=(\S+)/) {
            $params{ lc($1) } = $2;
        }
        $req =~ s/\Q$tag\E//;
    }
    $req =~ s/^\s+//;
    $req =~ s/\s+$//;
    return ($req, \%params);
}

sub LAYOUT_FOR_QUERY {
    my ($req) = @_;
    my ($name, $params) = &PARAMS_FOR_QUERY( $req );
    my ($lo, $nw);
    if ($name) {
        $name = "$templateDir/$name"
            if ($name =~ /.xml$/ && ! -e $name && -e "$templateDir/$name");
        if ($name =~ /^\//) {
            unless ( -e $name) {
                &err("I could not find a server-side tree file at '$name'",
                       'NoServerFile');
            }
            $lo = [];
            if ($name =~ /.xml$/) {
                my $parser = TumbleWeed::Parser->new();
                my @objs = $parser->parse_xml
                    ($name, %{$args}, -colors => \@allCustColors);
                push @{$lo}, @objs;
                &msg("Loaded XML file ".&_link($name),
                     'XmlDataFormat');
            } else {
                # This is a path to a flat file
                my $format = 'newick';
                my $tw     = &NEW_TW(undef, $params);
                my @trees  = $tw->tree( $name, $format);
                foreach my $tree (@trees) {
                    $tree = $tree->reroot() if ($args->val(qw(REROOT)));
                    push @{$lo}, TumbleWeed::Layout->new( $tree );
                }
                &msg("Loaded tree file ".&_link($name),
                     'LocalTreeFile');
            }
        }
        unless ($lo) {
            ($nw) = &NW_FOR_ROOT($name);
            if ($nw) {
                my $tw = &NEW_TW(undef, $params);
                $tw->tree( $nw );
                $lo = TumbleWeed::Layout->new( $tw->tree );
            }
        }
    }
    if ($lo) {
        my @list = ref($lo) eq 'ARRAY' ? @{$lo} : ($lo);
        foreach my $layout (@list) {
            while (my ($tag, $val) = each %{$params}) {
                $layout->param( $tag, $val);
            }
        }
    }
    return wantarray ? ($lo, $nw) : $lo;
}

sub NEW_TW {
    my ($tw, $params) = @_;
    $tw ||= BMS::TumbleWeed->new( );
    if ($params) {
        foreach my $capture (qw(minlength minleaflength)) {
           $tw->param($capture, $params->{$capture});
        }
    }
    $tw->param('preserve name', $args->val(qw(KEEPNAME)));
    $tw->param('meta name', $args->val(qw(metaname)));
    if ($fastaMeta) {
        $tw->set_metadata_from_fasta( $fastaMeta );
    }
    if (my $der = $args->val(qw(derived))) {
        my @reqs = ref($der) ? @{$der} : ($der);
        foreach my $dm (@reqs) {
            next unless ($dm);
            my @bits = split(/\s*,\s*/, $dm);
            my $trg  = shift @bits;
            my $src  = shift @bits;
            my %hash;
            foreach my $bit (@bits) {
                if ($bit =~ /^\s*([^=]+)\s*=\s*(.+)/) {
                    $hash{$1} = $2;
                }
            }
            $tw->derived_metadata( $trg, $src, \%hash );
        }
    }
    return $tw;
}

sub GET_RESIDUAL {
    my ($nws) = @_;
    return undef if (!$nws || $#{$nws} < 0);
    my ($residual, $params) = &PARAMS_FOR_QUERY($args->val(qw(RESIDUAL)));
    return undef unless ($residual);
    unless ($mt) {
        my $msg = "Can not compute residual nodes unless MapTracker database is online";
        $msg .= &help('NoMapTracker') unless($nocgi);
        die "$msg\n  ";
    }

    my ($full, $froot) = &NW_FOR_ROOT($residual);
    &msg("Extracting residuals from ". $froot->javascript_link(),
         'ExtractingResidualTree');
    my %keeping = map { $_->id => $_ } &LEAVES_FOR_NW( $full );
    foreach my $nw (@{$nws}) {
        map { delete $keeping{ $_->id } } &LEAVES_FOR_NW( $nw );
    }
    my $res    = BMS::MapTracker::Network->new( -tracker => $mt );
    my $tname  = $residual;
    if ($tname =~ /^(.+\.ph)/ || $tname =~  /^(.+\.dnd)/) {
        $tname = $1;
    }
    foreach my $leaf (values %keeping) {
        my $lname = $leaf->name;
        $res->add_root($leaf);
        $res->expand( -node     => $leaf,
                      -keeptype => "is a child of",
                      -recurse  => 30,
                      -filter   => "TAG = 'Branch Length' AND VAL = '$tname'",
                      );
    }
    my $tw = &NEW_TW(undef, $params);
    $tw->tree( $res );
    my $lo = TumbleWeed::Layout->new( $tw->tree );
    while (my ($tag, $val) = each %{$params}) {
        $lo->param( $tag, $val);
    }
    $lo->param('title', 'Residual') unless ($lo->param('title'));
    return $lo;
}

sub NW_FOR_ROOT {
    my ($req) = @_;
    unless ($mt) {
        my $msg = "Can not recover networks by root IDs unless the MapTracker database is online";
        $msg .= &help('NoMapTracker') unless($nocgi);
        die "$msg\n  ";
    }
    my @try = ($req);
    unshift @try, "$req Tree 1 Root" if ($req !~ /\s/);
    my $root;
    my @msgs;
    foreach my $r (@try) {
        my @roots = $mt->get_seq( -name => $r, -nocreate => 1);

        if ($#roots < 0) {
            push @msgs, ["Could not find any data for '$r'",
                         'UnknownMapTrackerRoot'];
        } elsif ($#roots > 0) {
            my @ns = map { $_->namespace->name } @roots;
            push @msgs, ["Multiple roots found for for '$r': ".
                         join(' / ', @ns),
                         'AmbiguousMapTrackerRoot'];
        } else {
            $root = $roots[0];
            last;
        }
    }
    unless ($root) {
        $args->err("Unable to find matches to '$req'");
        map { &err( @{$_} ) } @msgs;
        return ();
    }
    $args->{ROOT} = $root->namespace_name;
    my $nw = BMS::MapTracker::Network->new( -tracker => $mt );
    $nw->add_root( $root );
    &msg("Building network for ". $root->javascript_link(),
         'BuildingTreeNetwork');
    $nw->expand( -keeptype => 'is a parent of',
                 -recurse  => 30,);
    return ($nw, $root);
}

sub LEAVES_FOR_NW {
    my ($nw) = @_;
    my @leaves;
    foreach my $node ($nw->each_node) {
        push @leaves, $node unless ($node->is_class('ANONYMOUS'));
    }
    return @leaves;
}

sub TUMBLEWEED {
    my ($layouts) = @_;
    my $num = $#{$layouts} + 1;
    return unless ($num);
    my $twc;
    my @comArgs = ( -colors => \@allCustColors );
    if ($num == 1) {
        # Single Tumbleweed
        my $lo = $layouts->[0];
        $lo->calculate_tumbleweed();
        $lo->param('font', $args->val(qw(LABELFONT))) unless ($lo->param('font'));
        $twc = $lo->render( %{$args}, @comArgs );
    } else {
        my $tw = &NEW_TW();
        $twc = $tw->tumble_bundle( %{$args}, @comArgs,
                                   -font     => $args->val(qw(LABELFONT)),
                                   -layouts  => $layouts );
    }
    my $file = "TumbleWeedCanvas_$$.xml";
    my $path = "/stf/biohtml/tmp/$file";
    my $url  = "/biohtml/tmp/$file";
    if (open(TWC, ">$path")) {
        print TWC $twc->to_xml();
        close TWC;
        chmod(0777, $path);
        if (-s $path) {
            &msg("The canvas for the TumbleWeed has been written to <a href='$url' ".
                 "target='_blank'>$path</a>",'XmlDataFormatWritten');
        } else {
            $args->err("An apparently successful attempt was made to write the TumbleWeed canvas file, but it appears to be blank",
                       $path
                       );
        }
    } else {
        $args->death
            ("Failed to write canvas file", $path, $!);
    }
    &CANVAS( $twc );
}

sub CANVAS {
    my ($twc) = @_;
    $twc->param('font', $args->val(qw(LABELFONT)));
    my %passed = %{$args};
    my $dir = $passed{DIR} = &TEMP_DIR();
    $passed{URL} = $args->path2url($dir);
    if (my $hw = $args->val(qw(HTMLWIDTH))) {
        my $class = 'scalable' . ++$counters{SCALE};
        $passed{IMGCLASS} = $class;
        $passed{NOMAP} = 1;
    }
    if ($#custScheme > -1 ) {
        $passed{COLMETH} = \&CUSTOM_COLOR;
    }
    $passed{CALLBACK} = \&add_key;
    my ($file, $html, $gd);
    if (my $adat = $args->val(qw(AMBIT))) {
        $passed{DATA} = $adat;
        $twc->param('nocarp', $args->val(qw(NOCARP)));
        ($html, $gd) = $twc->image_tiles( %passed );
    } elsif ($args->val(qw(canvasxpress cx))) {
        my $fg = $twc->to_friendly_graph();
        my $file = "/stf/biohtml/tmp/TumbleWeedCanvasExport.json";
        open(OUT, ">$file") || $args->death
            ("Failed to generate CX file", $file, $!);
        print OUT $fg->to_canvasXpress
            ( -pretty => 1,
              -domid   => 'canvas',
              -nolayout => 1,
              -bracket => 1, #$args->val(qw(bracket mingyi)) ? 1 : 0,
              );
        close OUT;
        $args->msg("Completed CX export", &_link($file));
        return;

    } else {
        ($file, $html, $gd) = $twc->image( %passed );
    }
    if (my $class = $passed{IMGCLASS}) {
        my $hw = $passed{HTMLWIDTH};
        my ($width,$height) = $gd->getBounds();
        printf("<style> .$class { width: %.1fem; height: %.1fem; } </style>\n",
               $hw, $height * $hw / $width);
    }
    print "$html<br />\n";
    if (!$nocgi && !$args->val(qw(AMBIT))) {
        my $tfile = "/stf/biohtml/tmp/TumbleWeed_ExpTemplate_$$.txt";
        if (open(TEMP, ">$tfile")) {
            print TEMP $twc->template_experimental_data_file;
            close TEMP;
            my $url = $tfile; $url =~ s/^\/stf//;
            &msg("A <a href='$url' target='_blank'>template experimental data file</a> has been created", "TemplateDataFile");
        } else {
            &err("Failed to write temporary file '$file'","FileWriteFailure");
        }
    }
}

sub add_key {
    my ($gd, $name, $twc) = @_;
    my $nFont   = $twc->font($legName);
    my $font    = $twc->font($legKey);
    return unless ($nFont || $font);
    my $coords = $twc->largest_corners( $gd );
    my ($x1, $y1, $x2, $y2) = @{$coords};
    my $color  = 'black';
    my $nSize  = $nFont ? $nFont->height : $font->height;
    my $dy     = $font  ? $font->height + 2 : 0;
    my $y      = $y1;
    my $dir    = 1;
    my @order  = @custScheme;
    if ($y1) {
        $dir    = -1;
        $y      = $y2 + ($nSize * $dir);
        @order  = reverse @custScheme;
    }
    my $x = $x1 + 2;
    if ($nFont) {
        $y += $dir * 4;
        my $col = $twc->col('black',$gd);
        my $dx  = $nFont->width * length($name);
        $gd->rectangle($x-2, $y-2, $x + $dx + 3, $y + $nSize + 3, $col);
        $gd->string($nFont, $x, $y, $name, $col);
        $y += ($nSize + 6) * $dir;
    }
    if ($font) {
        foreach my $cs (@order) {
            my ($color, $test) = @{$cs};
            $test =~ s/\$val/Value/g;
            $gd->string($font, $x, $y, $test, $twc->col($color,$gd));
            $y += $dy * $dir;
        }
    }
}

sub PARSE_SCHEME {
    my ($text) = @_;
    foreach my $line (split(/[\n\r]+/, $text)) {
        next unless ($line);
        $line =~ s/^\s+//; $line =~ s/\s+$//;
        next unless ($line);
        my @testBits;
        # Find the longest set of terminal words that makes a valid color:
        my @colorWords = split(/\s+/, $line);
        while ($#colorWords != -1) {
            last if (BMS::TumbleWeed::standardize_color
                     ( join(' ', @colorWords)));
            push @testBits, shift @colorWords;
        }
        if ($#colorWords == -1) {
            &err("Could not identify a valid color at the end of '$line'",
                 "BadHexColor");
            next;
        }
        my $test = join(' ', @testBits) || '';
        $test =~ s/\$//g;
        $test =~ s/\s+\=\s+/ == /g;
        $test =~ s/val(ue|ues)?/\$val/g;
        my $color = join(' ', @colorWords);
        push @custScheme, [$color, $test];
        push @allCustColors, $color;
    }
    if ($#custScheme > -1) {
        my $msg = "<table class='tab'><caption>Custom color scheme</caption><tbody>\n";
        $msg.= "<tr><th>Num</th><th>Test</th><th>Color</th></tr>\n";
        for my $i (0..$#custScheme) {
            my ($col, $test) = @{$custScheme[$i]};
            $test ||= "Default";
            my $hc  = BMS::TumbleWeed::standardize_color($col);
            my $avg = 0; map { $avg += $_ / 3 } @{$hc};
            $col = uc("#".$col) if ($col =~ /^[0-9A-F]+$/i);
            my $hcol = join('', map {sprintf("%02x", $_)} @{$hc});
            $msg   .= sprintf
                ("<tr><th>%d</th><td>%s</td><th style='background-color:#%s; color:%s'>%s</th></tr>\n",
                 $i + 1, $test, $hcol, $avg < 128 ? 'white' : 'black', $col);
#            $msg   .= sprintf
#                ("<tr><th>%d</th><td>%s</td><th style='background-color:rgb(%s); color:%s'>%s</th></tr>\n",
#                 $i + 1, $test, join(',',@{$hc}), $avg < 128 ? 'white' : 'black', $col);
        }
        $msg .= "</tbody></table>\n";
        print $msg;
    }
}

sub CUSTOM_COLOR {
    my ($val) = @_;
    unless (defined $val) {
        if ($args->val(qw(ALLOWNULL))) {
            my ($color, $test) = @{ $custScheme[-1] || []};
            return $color if (!$test && $color);
        }
        return undef;
    }
    for my $i (0..$#custScheme) {
        my $dat = $custScheme[$i];
        my $bool;
        my ($color, $test) = @{$dat};
        if ($test) {
            eval('$bool = ' . $test);
            return $color if ($bool);
        } else {
            return $color;
        }
    }
    return undef;
}

sub SHOW_PARENTS {
    my ($robj) = @_;
    my @focus = ($robj);
    my @parents;
    $robj->{DISTANCE_STUFF} = 0;
    while (my $foc = shift @focus) {
        next if ($foc->{DONE_PARENT_STUFF}++);
        my @pedge = $foc->read_edges( -keeptype => 'is a child of');
        my @pars  = map { $_->other_node($foc) } @pedge;
        map { $_->{DISTANCE_STUFF} = $foc->{DISTANCE_STUFF} + 1 } @pars;
        push @parents, @pars;
        push @focus, @pars;
    }
    my $string = "";
    if ($#parents > -1) {
        $string = "<ol>\n";
        foreach my $par (@parents) {
           $string .= sprintf
               ("<li><a href='render_tree.pl?root=%s'>%s</a> [%d]</li>",
                $par->id, $par->name, $par->{DISTANCE_STUFF});
        }
        $string .= "</ol>\n";
    }    
    return $string;
}

sub DRAW_TREE {
    return unless ($args->val(qw(GRAPHVIZ)));
    my ($nw) = @_;
    $nw->format_node('graph', 'nodesep', 0);
    $nw->format_node('graph', 'ranksep', '0');
    $nw->format_node('graph', 'overlap', 'false');
    $nw->format_node('graph', 'splines', 'false');
    $nw->format_node('graph', 'packmode', 'graph');
    $nw->format_node('graph', 'maxiter', '100000');
    $nw->format_node('graph', 'epsilon', '0.000000001');
    $nw->format_node('graph', 'epsilon', '0.0000001');
    $nw->format_node('graph', 'mclimit', '100');
    $nw->format_node('graph', 'start', 'regular');

    $nw->format_node('node', 'fontsize', '1');
    $nw->format_node('node', 'shape', 'point');
    $nw->format_node('node', 'height', '0.1');
    $nw->format_node('node', 'width', '0.1');
    $nw->format_node('node', 'color', 'blue');

    $nw->format_node('edge', 'weight', '1');

    my $mod = 1;
    my ($lensum, $lencount) = (0,0);
    foreach my $edge ($nw->each_edge) {
        foreach my $tag ($edge->each_tag($lentag)) {
            $lensum += $tag->num || 0;
            $lencount++;
        }
    }
    if ($lensum) {
        $mod = $args->val(qw(EDGELEN)) * $lencount / $lensum;
    } else {
        $mod = 1;
    }
    my $stylemap = {
        'is a parent of' => {
            arrowhead => 'none',
            arrowtail => "none",
        },
        'is a child of' => {
            mirror => 'is a parent of',
        },
    };
    my ($string, $data) = $nw->to_graphviz_html
        ( -program    => 'neato',
          -simpleedge => 1,
          -lengthmod  => $mod,
          -stylemap   => $stylemap,
          -lengthtag  => $lentag,
          -showname   => 1,
          -anonopts   => {
              width  => 0.03,
              height => 0.03,
              color  => 'brown',
          },
          -edgeopts   => {
             # weight => 99999,
          });
    print $string;
    print "<br />\n";
    printf( "<a href='%s' target='_gvrender'>Rendering data</a><br />\n", $data->{dir});
}


sub HTML_TREE {
    return unless ($args->val(qw(HTMLTREE)));
    my ($nw) = @_;
    my ($rid) = $nw->find_tree_root();
    if ($rid) {
        unless ($mt) {
            my $msg = "Can not display HTML tree unless MapTracker database is online";
            $msg .= &help('NoMapTracker') unless($nocgi);
            die "$msg\n  ";
        }
        my $root   = $mt->get_seq($rid);
        print $mt->javascript_include( 'bio/PhylogeneticTree.js' );
        print "<ul class='PhylogeneticTree'>\n";
        print &RECURSE_TREE( $nw, $root, 0, 1 );
        print "</ul>\n";
    }
}

sub RECURSE_TREE {
    my ($nw, $node, $dist, $depth) = @_;
    $dist  ||=0;
    $depth ||=0;
    my $pad = '  ' x $depth;
    my $string = sprintf("%s<li branchlength='%.3f'>", $pad, $dist);
    my @edges = $nw->edges_from_node( $node, 'is a parent of' );
    if ($#edges < 0) {
        # Terminal node
        $string .= $node->name;
    } else {
        $string .= "<ul>\n";
        foreach my $edge (@edges) {
            my ($lensum, $lencount) = (0,0);
            foreach my $tag ($edge->each_tag($lentag)) {
                $lensum += $tag->num || 0;
                $lencount++;
            }
            my $kid = $edge->other_node($node);
            my $kdist = int(0.5 + 1000 * $lensum / $lencount) /1000;
            $string .= &RECURSE_TREE( $nw, $kid, $kdist, $depth+1);
        }
        $string .= "$pad</ul>";
    }
    $string .= "</li>\n";
    return $string;
}
    
sub HTMLFORM {
    return if ($args->val(qw(NOCGI)));

    my $button = "<input style='background-color:#dfd;color:#00f;font-size:larger;font-weight:bold' type='submit' value='Draw Tree'>";
    print "<div id='jsmtkAjaxProgress'></div>\n";
    print "<form method='POST' enctype='multipart/form-data'>\n";
    print "$button<br />";
    print "<hr />\n";

    print "<table><tbody>";
    print "<tr><td valign='top'>\n";

    print "<h4>".&help('PreCannedTrees'). "Pre-Canned Tree Layouts</h4>\n";
    print &help('PreCannedFile');
    print "<input type='text' size='20' name='precan' id='precan' value='$args->{PRECAN}' /><br />\n";
    my $success = opendir(TMPDIR, $templateDir);
    if ($success) {
        print "<ul>\n";
        foreach my $file (readdir TMPDIR) {
            my $path = "$templateDir/$file";
            my $desc = "";
            next unless (-f $path && $file =~ /\.xml$/);
            my $name = $file; $name =~ s/\.xml$//;
            printf("<li><a class='mtclass' onclick='set_example({%s})'>".
                   "%s</a> %s</li>\n", "precan:\"$file\"", $name, $desc);
        }
        closedir TMPDIR;
        print "</ul>\n";
    } else {
        &err("Failed to read template directory '$templateDir': $!",
               'TemplateDirectoryUnavailable');
    }
    


    print "<div style='background-color:#ccf' class='DynamicDiv' cantoggle='hide' menutitle='Advanced Tree Settings'>\n";
    print "<h4>".&help('DefiningTheTree')."Define Your Phylogenetic Tree</h4>\n";

    print "<b>Enter one or more tree roots</b> ".
        &help('TreeRootSelection').":<br />\n";
    print "<textarea id='bundle' cols='50' rows='10' name='bundle'>";
    print $args->val(qw(BUNDLE));
    print "</textarea><br />\n";
    
    print "<b>Paste fasta sequence for <i>de novo</i> alignment:</b> ".
        &help('RawAlignment').":<br />\n";
    print "<textarea id='fasta' name='fasta' cols='50' rows='10' name='bundle'>";
    print $args->val('fasta') || "";
    print "</textarea><br />\n";

    print "<table><tbody>\n";

    print "<tr><th class='right'>Tree Namespace</th><td>";
    print &help('TreeNamespace');
    print "<input type='text' size='20' name='kidns' id='kidns' value='$args->{KIDNS}' /><br />\n";
    print "</td></tr>";

    print "<tr><th class='right'>Residual Tree</th><td>";
    print &help('ResidualTree');
    print "<input type='text' size='20' id='residual' name='residual' value='$args->{RESIDUAL}' /><br />\n";
    print &help('KeepOnlyResiduals');
    printf("<input type='checkbox' name='residualonly' id='residualonly' value='1'%s /> ",
           $args->val(qw(RESIDUALONLY)) ? " checked='1'" : "");
    print "<em>ONLY</em> keep residuals (discard primary networks)<br />\n";
    print "</td></tr>";

    print "<tr><th class='right'>Tree file on <em>your computer</em></th><td>";
    print &help('LocalTreeFile');
    print "<input type='file' name='user_tree'>";
    print "</td></tr>";

    print "<tr><td /><td>";
    print &help('RerootTree');
    printf("<input type='checkbox' name='%s' id='%s' value='1'%s /> %s",
           'reroot','reroot', $args->val(qw(REROOT)) ? " checked='1'" : "",
           "Re-root tree");
    print "</td></tr>";

    print "<tr><td /><td>";
    print &help('KeepNames');
    printf("<input type='checkbox' name='%s' id='%s' value='1'%s /> %s",
           'keepname','keepname', $args->val(qw(KEEPNAME)) ? " checked='1'" : "",
           "Do not clean names");
    print "</td></tr>";

    print "</tbody></table>\n";

    print "<h4>".&help('ImageSize'). "TumbleWeed Image Size</h4>\n";

    print "<table><tbody>";
    print "<tr><th class='right'>Fixed Radius</th><td>\n";
    print &help('ImageWidth')."<input type='text' size='4' name='width' value='$args->{WIDTH}' />pixels";
    print "</td></tr>\n";

    print "<tr><th class='right'>Auto-radius</th><td>\n";
    print &help('AutoScale')."<input type='text' size='4' name='scale' value='$args->{SCALE}' />pixels";
    print "</td></tr>\n";

    print "<tr><th class='right'>Scalable HTML</th><td>\n";
    print &help('ScalableHtml')."<input type='text' size='4' name='htmlwidth' value='$args->{HTMLWIDTH}' />em\n";
    print "</td></tr>\n";

    print "<tr><th class='right'>Tiling Columns</th><td>\n";
    print &help('TilingColumns')."<input type='text' size='2' name='tilecol' value='$args->{TILECOL}' />\n";
    print "</td></tr>\n";

    print "</tbody></table>\n";


    print "<h4>".&help('FormattingFeatures'). "Feature Color and Size</h4>\n";
    print "<p style='font-style:italic'>Hexadecimal or 'typical' color names, <a href='http://htmlhelp.com/cgi-bin/color.cgi' target='colors'>examples are here</a>.</p>\n";
    print "<table><tbody>\n";
    print "<tr>".join('', map {"<th>$_</th>"} ('Feature', 'Color', 'Size') )."</tr>\n";
    my $tFrm = "<input type='text' size='%d' id='%s' name='%s' value='%s' />";
    foreach my $feat (qw(Node Branch Root Edge Bundle Data Title Label)) {
        my $key = uc($feat."COLOR");
        print "<tr><th class='right'>$feat</th>\n  <td>";
        printf($tFrm, 15, lc($key), $key, $args->{$key}, );
        print "</td>\n  <td>";
        my $skey = uc($feat."SIZE");
        printf($tFrm, 2, lc($skey), $skey, $args->{$skey}, );
        print "</td>\n";
        print "</tr>\n";
    }
    my @fonts = qw(arial courier times);
    my %mods  = ( '' => '', bd => 'Bold', bi => 'Bold Italic', i => 'Italic');

    print "<tr><th class='right'>Font</th>\n";
    print "  <td colspan='2'><select name='labelfont'>\n";
    foreach my $font (@fonts) {
        foreach my $mkey (sort keys %mods) {
            my $fname = "$font$mkey";
            my $flab  = "$font $mods{$mkey}";
            substr($flab,0,1) = uc(substr($flab,0,1));
            printf("  <option value='%s'%s>%s</option>\n", $fname,
                   $args->val(qw(LABELFONT)) eq $fname ? " selected='selected'" : '',
                   $flab);
        }
    }
    print "</select></td></tr>\n";
    print "</tbody></table>\n";


    print "</div>\n";

    print "<div style='background-color:#cfc' class='DynamicDiv' cantoggle='hide' menutitle='Example Searches'>\n";

    print "<h4>".&help('ExampleSearches'). "Example Searches</h4>\n";

    print "<input id='exampstatus' type='text' size='60' /><br />";
    foreach my $exType (sort keys %{$examp}) {
        print "<b>$exType</b>\n";
        print "<ul>";
        foreach my $hash (@{$examp->{$exType}}) {
            my %settings = (%{$defaults}, %{$hash});
            my @tags;
            while (my ($tag, $val) = each %settings) {
                push @tags, sprintf("%s:\"%s\"", $tag, $val);
            }
            printf("<li><a class='mtclass' onclick='set_example({%s})'>".
                   "%s</a> %s</li>\n", join(",", @tags), $settings{name}, $settings{desc} || '');
        }
        print "</ul>\n";
    }

    print "</div>\n";

    print &help('ParameterFile'). "<b>Parameter File:</b><br />\n";
    printf($tFrm, 15, 'valuefile', 'valuefile', '');
    if (my $prior = $args->val(qw(VALUEFILE))) {
        print "<span style='color:brown; font-size:0.8em; font-style:italic;'>This page was loaded using Parameter File <u>$prior</u>. You can re-use that file, but it might over-ride other entries listed on this page</span><br />\n";
    }

    print "</td><td valign='top'>\n";

    print "<h4>".&help('DefiningExperimentalData'). 
        "Overlay Experimental Results:</h4>\n";
    
    print "<table><tbody>\n";
    print "<tr><th class='right'>File on <em>your computer</em></th><td>";
    print &help('LocalDataFile');
    print "<input type='file' name='results'>";
    print "</td></tr>";

    print "<tr><th class='right'>Path on <em>server</em></th><td>";
    print &help('NetworkDataFile');
    print "<input id='resultpath' type='text' size='30' name='resultpath' value='$args->{RESULTPATH}' />";
    print "</td></tr>";
    print "</tbody></table><br />\n";

    printf("<input type='checkbox' name='allownull' ".
           "id='allownull' value='1'%s />", 
           $args->val(qw(ALLOWNULL)) ? " checked='1'" : "");
    print &help('NullColoration');
    print " Empty cells get default color<br />\n";

    printf("<input type='checkbox' name='ascending' ".
           "id='ascending' value='1'%s />", 
           $args->val(qw(ASCENDING)) ? " checked='1'" : "");
    print &help('DataDirection');
    print " Larger values are more interesting<br />\n";

    printf("<input type='checkbox' name='nolabel' ".
           "id='nolabel' value='1'%s />", 
           $args->val(qw(NOLABEL)) ? " checked='1'" : "");
    print &help('NoLabels');
    print " Do not show node labels<br />\n";


    printf("<input type='checkbox' name='nocarp' ".
           "id='nocarp' value='1'%s />", 
           $args->val(qw(NOCARP)) ? " checked='1'" : "");
    print &help('ErrorSuppression');
    print " Do not pester me about errors in my data<br />\n";

    printf("<input type='checkbox' name='cx' ".
           "id='cx' value='1'%s />", 
           $args->val(qw(CX)) ? " checked='1'" : "");
    print &help('CanvasXpress');
    print " Create CanvasXpress data structure<br />\n";

    printf("<input type='hidden' name='coloredge' value='0' /><input type='checkbox' name='coloredge' value='1'%s />",
           $args->val(qw(COLOREDGE)) ? " checked='1'" : "");
    print &help('ColorEdges');
    print " Use results to color edges as well<br />\n";

    print "<b>Custom Color Scheme:</b> ".
        &help('ColorScheme')."<br />\n";
    print "<textarea id='custscheme' cols='30' rows='5' name='custscheme'>";
    print $args->val(qw(CUSTSCHEME));
    print "</textarea><br />\n";

    print "<b>Specific Experiments / Compounds:</b>".
        &help('ExperimentFiltering')."<br />\n";
    print "<textarea id='experiment' cols='30' rows='5' name='experiment'>";
    print $args->val(qw(EXPERIMENT));
    print "</textarea><br />\n";
    print &help('OnlyLabelData')."<b>Only show labels for colors:</b><br />\n".
        "<input id='onlylabeldata' type='text' size='30' name='onlylabeldata' value='$args->{ONLYLABELDATA}' />\n";

    print "</td></tr>\n";
    print "</tbody></table>\n";
    



    print "<hr />\n";
    print "$button<br />";
    print "</form>\n";    
}

sub allowed_ambit_columns {
    my $html = "<ol>\n";
    foreach my $col (sort keys %{$allowed}) {
        $html .= "<li><b>$col</b><br /><i>allowed aliases:</i> ".join(" / ", @{$allowed->{$col}})."\n";
    }
    $html .= "</ol>\n";
    return $html;
}

sub HTMLSTART {
    return if ($args->val(qw(NOCGI)));
    my $prog = $0;
    $isbeta = 1 if ($prog =~ /working/);
    print "<!DOCTYPE html PUBLIC '-//W3C//DTD XHTML 1.0 Transitional//EN'".
        " 'http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd'>\n";
    print "<html><head>\n";
    print "<title>Render Tree</title>\n";
    print "<link rel='shortcut icon' href='/biohtml/images/cactus2.png'>\n";
    print "<link type='text/css' rel='stylesheet'\n".
        "      href='/biohtml/css/friendlyBlast.css' />\n";

    print $mt->javascript_head() if ($mt);

    my @defJs;
    while (my ($tag, $val) = each %{$defaults}) {
    };


    print <<EOF;

<style>
    .twmsg {
      color:   #008;
    }
    .twhelp {
        background-color: #ff3;
        text-decoration: none;
        padding-left:  2px;
        padding-right: 2px;
        margin-left:   2px;
        margin-right:  2px;
        font-size:     smaller;
        font-weight:   bold;
      color:   #f00;
      display: inline;
      /* border:  #f33 solid 1px; */
    }
    .twov { 
      font-size: larger;
    color: #090; 
    }
    .TumbleTile {
      border: solid #ffcc99 2px;
    }
    h2 {
        background-color: #ccffcc;
      color: #3333ff;
    }
    h4 {
        background-color: #ccffff;
      color: #009900;
    }
    .wrn { color:red; background-color: #ddd; }
    em {
      color: red;
        font-weight: bold;
    }
    p.help, td.help {
        font-style: italic;
      color: #0099cc;
    }
    th.right, td.right {
      text-align: right;
      vertical-align: middle;
    }
    td {
        vertical-align: top;
    }
    caption {
        background-color: #ffff99;
    }
</style>

    <script>
    function set_example( struct ) {
        var key;
        for (key in struct) {
            if (key == 'name' || key == 'desc') continue;
            var el = jsmtk_get_element(key);
            if (jsmtk_undef(el)) continue;
            var val = struct[key];
            if (el.type == 'checkbox') {
                el.checked = (val && val != '0') ? true : null;
            } else {
                el.value   = val;
            }
        }
        var stat = jsmtk_get_element('exampstatus');
        stat.value = "Click [Draw Tree] to try '" + struct.name+ "'";
    }
    </script>

EOF

    print "</head><body bgcolor='white'><center>\n";
    print "<font color='orange' size='+3'><b>Render Tree</b></font><br />\n";
    if ($isbeta) {
        print &help('BetaSoftware');
	print "<font color='red'>*** THIS VERSION IS BETA SOFTWARE ***</font><br />\n";
    }
      # print &help('ReleaseVersion');
    # print &SHOW_CVS_VERSION($VERSION, $args->val(qw(SHOW_CVS_VERS)) ) ."<br />\n";
    print "<font color='brick' size='-1'>";
    print &help($ldap ? 'LdapUser' : 'UnknownUser');
    printf("<i>In use by %s</i></font><br />\n", $ldap);
    print &help('SoftwareOverview', "Program Overview", 'twov');
    print "</center>";
}
sub HTMLEND {
    return if ($args->val(qw(NOCGI)));
    my $url = $ENV{REQUEST_URI};
    $url =~ s/\?.*$//;
    my @ubits;
    my %esc = (
               '?' => '%3F',
               '#' => '%23',
               '&' => '%26',
               '/' => '%2F',
               '=' => '%3D',
               '\\' => '%5C',

               "\n" => '%0A',
               "\r" => '%0D',
               "\t" => '%09',
               ' ' => '%20',

               '$' => '%24',
               ':' => '%3A',
               ';' => '%3B',
               '<' => '%3C',
               '>' => '%3E',
               '@' => '%40',
               '[' => '%5B',
               ']' => '%5D',
               '^' => '%5E',
               '`' => '%60',
               '{' => '%7B',
               '|' => '%7C',
               '}' => '%7D',
               '~' => '%7E',
 );
    while (my ($key, $val) = each %{$args}) {
        next unless (defined $val);
        next if ($args->{isDefault}{$key} || $args->{notPassed}{$key});
        my @vals;
        if (my $rf = ref($val)) {
            if ($rf eq 'ARRAY') {
                @vals = @{$val};
            } else {
                next;
            }
        } else {
            @vals = $val;
        }
        foreach my $v (@vals) {
            next if (!defined $v || $v eq '');
            $v =~ s/\%/\%25/g;
            while (my ($i,$o) = each %esc) {
                $v =~ s/\Q$i\E/$o/g;
            }
            push @ubits, join("=",$key, $v);
        }
    }
    printf("<a class='butt' href='%s?%s'>Link to this page</a> ".
           "<i>(right click and copy)</i><br />\n", $url, join('&', @ubits))
        unless ($#ubits == -1);

    # $mt->register_javascript_cache();
    print $mt->javascript_data( 'html' ) if ($mt);
    if ($isbeta) {
        print "<h4>".&help('JsmtkDebugging'). 
            "JSMTK Messages:</h4>\n";
        print "<div id='jsmtk_errors'></div>\n";
    }
    print "</body></html>\n";
}

sub stnd_cmpd_id {
    my $cmpd = shift;
    $cmpd =~ s/^\s+//;
    $cmpd =~ s/\s+$//;
    if ($cmpd =~ /^([A-Z\s]{1,3})\-(\d{1,6})$/i) {
        # Standardize BMS IDs
        my ($prfx, $num) = (uc($1), $2 + 0);
        $prfx =~ s/\s+//g;
        $cmpd = sprintf("%-3s-%06d", $prfx, $num);
    }
    return $cmpd;
}

sub help { return $args->tiddly_link( @_ ); }
