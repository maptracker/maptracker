#!/stf/biobin/perl -w

BEGIN {
    # Needed to make my libraries available to Perl64:
    use lib '/stf/biocgi/tilfordc/released';
    # Allows usage of beta modules to be tested:
    my $prog = $0;
    my $pwd = `pwd`;
    if ($prog =~ /working/ || $pwd =~ /working/) {
	warn "\n*** THIS VERSION IS BETA SOFTWARE ***\n\n";
	require lib;
	import lib '/stf/biocgi/tilfordc/perllib';
    }
    require lib;
    import lib '/stf/biocgi/tilfordc/patch_lib';
    $| = 1;
    print '';
}

my $VERSION = ' $Id$ ';

# use lib '/perl/lib/site_perl/5.005/BMS/XPRESS';
use strict;
use BMS::MapTracker::LoadHelper;
use BMS::FriendlyDBI;
use BMS::FriendlySAX;
use BMS::MapTracker::SciTegicSmiles;
use BMS::BmsArgumentParser;
use BMS::ForkCritter;

my $args = BMS::BmsArgumentParser->new
    ( -nocgi    => $ENV{'HTTP_HOST'} ? 0 : 1,
      -cache    => 5000,
      -testmode => 1,
      -limit    => 0,
      -verbose  => 1,
      -loaddir  => 'PubMed',
      -dir      => "/tmine/medline",
      -err      => "PubMed_Parse_Errors.txt",
      );



my $testfile   = $args->{TESTFILE};
my $tm         = $testfile ? 1 : $args->{TESTMODE};
my $cache      = $args->{CACHE};
my $vb         = $args->{VERBOSE};
my $prog       = $args->{PROGRESS} || 60;
my $baseDir    = $args->val(qw(basedir loaddir)),
my $clobber    = $args->val(qw(clobber));
my $limit      = $args->{LIMIT};
my $eFile      = $args->val(qw(errors err)),
my $fork       = $args->{FORK} || 1;
my $doDump     = $args->val(qw(dodump dump));
my $idReq      = $args->val(qw(id));
my $isTrial    = $args->val(qw(trial istrial));
$args->shell_coloring( );

$idReq = { map { $_ => 1 } split(/[\,\s]+/, $idReq) } if ($idReq);

my @mnThree    = qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);
my @mnFull     = qw(January February March April May June 
                    July August September October November December);
# These are not quite right, but close enough:
my %mnHash     =
    ( winter    => 1,
      spring    => 4,
      spr       => 4,
      summer    => 7,
      sum       => 7,
      autumn    => 10,
      fall      => 10,
      easter    => 4,  # http://www.ncbi.nlm.nih.gov/pubmed/2019856
      christmas => 12, # http://www.ncbi.nlm.nih.gov/pubmed/13954194
      '1st Quart' => 1, # PMID:10237212
      '2d Quart'  => 4, # PMID:10248150
      '3d Quart'  => 7, # PMID:10236507
      '4th Quart' => 10, # PMID:10249456
      '-00 Winter' => 1, # PMID:10711319
      '-94 Winter' => 1, # PMID:11362190
      'N0v' => 11, # PMID:5275180
      '' => 1, #
      '' => 1, #
      '' => 1, #
      '' => 1, #
      '' => 1, #
      '' => 1, #
      );
foreach my $arr (\@mnThree, \@mnFull) {
    map { $mnHash{lc($arr->[$_])} = $_ + 1 } (0..$#{$arr});
}


my (%mesh, %issnName, %doneMsg, $lh, $mt, $sts);

unlink($eFile) if ($clobber);

my $ft         = "#FreeText#";
my $fc = BMS::ForkCritter->new
    ( -init_meth   => \&initialize,
      -finish_meth => \&finish,
      -method      => \&parse_file,
      -progress    => $prog,
      -exitcode    => 42,
      -verbose     => $vb );
$fc->output_file( 'errors',   $eFile );
$fc->output_file( 'TestFile', $testfile) if ($testfile);


my @skipTag =
    qw(SpaceFlightMission Language CommentsCorrectionsList CommentsCorrections PersonalNameSubjectList NumberOfReferences GeneralNote CitationSubset OtherAbstract DateCreated DateCompleted DateRevised SupplMeshList MedlineJournalInfo Pagination PublicationTypeList xxJournalIssue);


# Should these be further explored?
push @skipTag, qw(InvestigatorList GeneSymbolList OtherAbstract OtherID NameOfSubstance);

&get_from_xml();

if (-s $eFile && open(ERR, "<$eFile")) {
    my %uniq;
    while (<ERR>) {
        $uniq{$_} = 1;
    }
    close ERR;
    if (open(ERR, ">$eFile")) {
        foreach my $u (sort keys %uniq) {
            print ERR $u;
        }
        close ERR;
    }
}
$args->msg("[!!]", "Errors recorded", $eFile) if (-s $eFile);

sub get_from_xml {
    my @gz;
    if (my $freq = $args->val(qw(input file))) {
        push @gz, $freq;
    } else {
        my $dir = $args->val(qw(srcdir dir));
        $dir = "/net/minerva$dir" if (! -d $dir &&  -d "/net/minerva$dir");
        my $year = $args->val(qw(year));
        unless ($year) {
            my @years;
            opendir(DIR, $dir) || die "Failed to read $dir:\n  $!\n  ";
            foreach my $file (readdir DIR) {
                push @years, $file if ($file =~ /^\d+$/);
            }
            closedir DIR;
            ($year) = sort { $b <=> $a } @years;
            $args->death("Failed to find year from $dir", $!)
                unless ($year);
        }
        $dir .= "/$year";
        $args->death("Failed to find directory for $year", $dir)
            unless (-d $dir);
        my @srcs;
        unless ($args->{UPDATE}) {
            push @srcs, 'base';
            # push @srcs, 'baseline';
            push @srcs, 'unzipped';
            push @srcs, '';
        }
        push @srcs, 'updates';
        foreach my $src (@srcs) {
            # Only consider the unzipped directory if nothing has been found
            # in baseline:
            next if (($src eq 'unzipped' || $src eq '') && $#gz != -1);
            my $sdir = $dir;
            $sdir   .= "/$src" if ($src);
            unless (-d $sdir) {
                $args->msg("[!!]", "Failed to find $src directory",$dir);
                next;
            }
            opendir(DIR, $sdir) || die "Failed to read $sdir:\n  $!\n  ";
            my @found;
            foreach my $file (readdir DIR) {
                push @found, "$sdir/$file" if ($file =~ /\.xml(\.gz)?$/);
            }
            push @gz, @found;
            $args->msg( sprintf("%d files in %s", $#found + 1, $sdir)) if ($vb);
        }
    }
#    if ($limit) {
#        warn "Only parsing a single file\n" if ($vb);
#        @gz = (shift @gz);
#    }
    $fc->input_type( 'array' );
    # Use reverse() to start with the most recent files first:
    $fc->input([ reverse @gz ]);
    if ($isTrial) {
        $args->msg("[+]",scalar(@gz)." files found", @gz,"Not running - trial mode");
        return;
    }
    my $failed = $fc->execute( $fork );
    $args->err("$failed tasks failed to execute!") if ($failed);
}

sub parse_file {
    my ($file) = @_;
    eval {
        BMS::FriendlySAX->new
            ( -file    => $file,
              -verbose => 0,
              -method  => \&parse,
              -textmeth => \&cautious_text,
              -limit   => $limit,
              -tag     => "MedlineCitation",
              -quietlimit => 1,
              -skip    => \@skipTag,
              );
      };
    return 42;
}

sub cautious_text {
    my ($arr) = @_;
    my $rv = join('', @{$arr});
    $rv =~ s/\s+/ /g;
    $rv =~ s/\s+$//; $rv =~ s/^\s+//;
    $rv =~ s/\P{IsASCII}//g;
    return $rv;
}

# &get_from_ngbpm();
sub parse {
    my $record = shift;
    my @ids = &deep_text($record, 'PMID');
    if ($#ids == -1) {
        &err("No ID for record");
        return;
    } elsif ($#ids != 0) {
        &err("Multiple IDs for record", @ids);
        return;
    }
    my @stripable;

    my $id = $ids[0];
    return if ($idReq && !$idReq->{$id});
    $id = "PMID:$id";
    if ($doDump) {
        print BMS::FriendlySAX::node_to_text( $record )."\n\n";
        return;
    }
    $lh->set_class( $id, 'PUBMED');

    my @titles = &deep_text($record, 'Article','ArticleTitle');
    # Remove all old title links
    $lh->kill_edge( -name1    => $id,
                    -override => 1,
                    -auth     => 0,
                    -type     => 'is a shorter term for' );
    if ($#titles == 0) {
        my @tags;
        if (my $dt = &get_date($record, $id)) {
            push @tags, ["Date", undef, $dt];
        }
        my $title = $titles[0];
        $lh->set_edge( -name1 => $id,
                       -name2 => $ft.$title,
                       -tags  => \@tags,
                       -type  => 'is a shorter term for' );
    } elsif ($#titles != 0) {
        &err("Multiple titles for record", @titles);
        return;
    }

    my @journals = &dig_deep($record, qw(Article Journal));
    if ($#journals == 0) {
        my $journal = $journals[0];
        my ($title) = map {$ft.$_} &deep_text($journal, 'Title');
        my @issns = &deep_text($journal, 'ISSN');
        my $pkey  = $title;
        if ($#issns == 0) {
            my $issn = $issns[0];
            if ($issn =~ /^[A-Z0-9]{4}\-[A-Z0-9]{4}$/) {
                $pkey = "ISSN:$issn";
                $issnName{$pkey} = $title;
            } else {
                &err("Malformed ISSN", $id, $issn);
            }
        } elsif ($#issns == -1) {
            &err("No ISSN", $title);
        } else {
            &err("Multiple ISSN", $title, @issns);
        }
        $lh->set_edge( -name1 => $id,
                       -name2 => $pkey,
                       -type  => "is a member of" );
    }

    foreach my $auth (&dig_deep
                     ($record, qw(Article
                                  AuthorList
                                  Author) )) {
        my ($name) =  &deep_text($auth, 'LastName');
        next unless ($name);
        my ($init) = &deep_text($auth, 'Initials');
        if ($init) {
            $init =~ s/([A-Z])/$1\./g;
            $name .= ",$init";
        }
        $lh->set_class($name, 'Author');
        $lh->set_edge( -name1 => $id,
                       -name2 => $name,
                       -type  => 'has contribution from');
    }

    foreach my $tn (&dig_deep
                     ($record, qw(MeshHeadingList
                                  MeshHeading
                                  DescriptorName))) {
        next unless ($tn->{ATTR}{MajorTopicYN} eq 'Y');
        if (my $mesh = &lu_mesh($tn->{TEXT})) {
            $lh->set_edge( -name1 => $id,
                           -name2 => $mesh,
                           -type  => 'has attribute');
        }
        
    }

    my @chemBaits;
    foreach my $chem (&dig_deep
                     ($record, qw(ChemicalList Chemical))) {
        foreach my $cas (&deep_text($chem, 'RegistryNumber')) {
            push @chemBaits, ($cas, "CAS:$cas") if ($cas);
        }
        push @stripable, [$chem, 'RegistryNumber'];
    }

    
    if ($#chemBaits != -1) {
        $args->msg_once("[!!]","Working on canonicalization", "Ignoring compound information!");
        @chemBaits = ();
                       
    }
    unless ($#chemBaits == -1) {
        foreach my $smi ($sts->term_to_canonical( @chemBaits ) ) {
            # Ignore elemental forms:
            if ($smi =~ /^\#Smiles\#(\[[A-Z][a-z]?([\+\-]\d*)?\])$/) {
                $sts->{PP_IGNORED}{$1}++;
                next;
            }
            $lh->set_edge( -name1 => $id,
                           -name2 => $smi,
                           -type  => "contains a reference to" );
        }
    }
    
    $lh->write_threshold_quick($cache);
    push @stripable, [ $record, 'MeshHeadingList','KeywordList','Article' ];

    # STILL NEED TO EXPLORE: KeywordList

    if ($tm) {
        map { &strip_record( @{$_} ) } @stripable;
        print BMS::FriendlySAX::node_to_text( $record )."\n\n"
            if ($#{$record->{KIDS}} > 0);
    }
}

sub get_date {
    my ($record, $id) = @_;
    my @dates;
    # Try first to get the article date:
    foreach my $dnode (&dig_deep($record, 'Article', 'ArticleDate')) {
        if (my $dt = &_standardize_date($dnode, $id)) {
            push @dates, $dt;
        }
        # print BMS::FriendlySAX::node_to_text( $dnode )."\n\n";
    }
    if ($#dates == -1) {
        # See if there are Journal entries:
        foreach my $dnode (&dig_deep($record, 'Article',
                                     'Journal', 'JournalIssue', 'PubDate')) {
            if (my $dt = &_standardize_date($dnode, $id)) {
                push @dates, $dt;
            }
        }
    }
    return $dates[0] if ($#dates == 0);
    return undef;
}

sub _standardize_date {
    my ($dnode, $id) = @_;
    my $dt;
    return $dt unless ($dnode);
    # Get the year
    my @yrs = &deep_text($dnode, 'Year');
    if ($#yrs == -1) {
        # No year entry
        return &_try_medline_date($dnode, $id);
    }
    if ($#yrs != 0) {
        &err("Date Error - Multiple year entries", $id, @yrs);
        return $dt;
    }
    $dt = $yrs[0];
    unless ($dt =~ /^\d{4}$/) {
        &err("Date Error - Unrecognized year entry", $id, $dt);
        return $dt;
    }

    # Get the month
    my @mns = &deep_text($dnode, 'Month');
    return $dt if ($#mns == -1);
    if ($#mns != 0) {
        &err("Date Error - Multiple month entries", $id, @mns);
        return $dt;
    }
    # Allow standard abbreviations:
    my $mn = &_standardize_month($mns[0]);
    return $dt unless ($mn);
    $dt .= sprintf(".%02d", $mn);

    # Get the day:
    my @dys = &deep_text($dnode, 'Day');
    return $dt if ($#dys == -1);
    if ($#dys != 0) {
        &err("Date Error - Multiple day entries", $id, @dys);
        return $dt;
    }
    my $dy = $dys[0];
    if ($dy =~ /^\d{1,2}$/) {
        $dt .= sprintf("%02d", $dy);
    } else {
        &err("Date Error - Unrecognized day entry", $id, $dy);
    }
    return $dt;
}

sub _standardize_month {
    my ($req, $id) = @_;
    return $req unless ($req);
    $req =~ s/^\s+//;
    # Use as-is if it is already a 1-2 digit integer:
    if ($req =~ /^\d{1,2}$/) {
        return $req;
    } elsif ($req =~ /^\-\d{4}$/) {
        # The month is actually only a date range
        # Quietly ignore
        return undef;
    }
    my $mn = $req;
    # Remove leading year range eg '-1994 ':
    $mn =~ s/^\-\d{4}\s*//;
    # Remove all but the leading letter stuff:
    $mn =~ s/[^a-z].+//i;
    # Can we map what is left to a month number?
    $mn = $mnHash{lc($mn)};
    unless ($mn) {
        &err("Date Error - Unrecognized month entry", $id, $req);
        return undef;
    }
    return $mn;
}

sub _try_medline_date {
    my ($dnode, $id) = @_;
    my $dt;
    my @mldt = &deep_text($dnode, 'MedlineDate');
    return $dt if ($#mldt == -1);
    if ($#mldt != 0) {
        &err("Date Error - Multiple MedlineDate entries", $id, @mldt);
        return $dt;
    }
    if ($mldt[0] =~ /^(\d{4})(.*)$/) {
        my $remainder;
        ($dt, $remainder) = ($1, $2 || '');
        $remainder =~ s/^\s+//;
        $remainder =~ s/\s+$//;
        if (my $mn = &_standardize_month($remainder, $id)) {
            $dt .= sprintf(".%02d", $mn);
        }
    } else {
        &err("Date Error - Unrecognized MedlineDate entry", $id, $mldt[0]);
    }
    return $dt;
}


sub dig_deep {
    my @stack = (shift); # The seed node
    my @digs  = @_;      # The xml tag names we will dig through
    while ($#stack != -1 && $#digs != -1) {
        my @dug;
        my $dig = shift @digs;
        foreach my $node (@stack) {
            push @dug, @{$node->{BYTAG}{$dig} || []};
        }
        @stack = @dug;
    }
    return @stack;
}

sub deep_text {
    my @nodes = &dig_deep(@_);
    my @text;
    foreach my $node (@nodes) {
        if (my $txt = $node->{TEXT}) {
            push @text, $txt;
        }
    }
    return @text;
}

sub err {
    my $msg = join("\t", map { defined $_ ? $_ : '-undef-' } @_);
    $fc->write_output('errors',"$msg\n") unless ($doneMsg{$msg}++);
}

sub lu_mesh {
    my ($text) = @_;
    unless (defined $mesh{$text}) {
        my $m = '';
        if (my $seq = $mt->get_seq
            ( -id => $ft.$text, -nocreate => 1, -defined => 1)) {
            my $edges = $mt->get_edge_dump
                ( -name      => $seq->id,
                  -keeptype  => ['is reliably aliased by',
                                 'is a longer term for'],
                  -orient    => 1,
                  -keepclass => 'meshdesc' );
            my %idh = map { $_->[0] => 1 } @{$edges};
            my @ids = keys %idh;
            if ($#ids == 0) {
                my $md = $mt->get_seq( $ids[0] );
                $m = $md->name;
            } elsif ($#ids == -1) {
                &err("MeSH text does not point to ID", $text);
            } else {
                &err("MeSH text points to multiple IDs", $text, @ids);
            }
        } else {
            &err("MeSH text is unrecognized", $text);
        }
        $mesh{$text} = $m;
    }
    return $mesh{$text};
}

sub strip_record {
    my $record = shift;
    my %kill   = map { $_ => 1 } @_;
    my @keep;
    map { push @keep, $_ unless ($kill{$_->{NAME}}) } @{$record->{KIDS} || []};
    $record->{KIDS} = \@keep;
    &collapse_empty($record);
}

sub collapse_empty {
    my $record = shift;
    return unless ($record->{KIDS} && $#{$record->{KIDS}} == -1);
    my @keep;
    my $par = $record->{PARENT};
    my $chk = $record . "";
    foreach my $kid (@{$par->{KIDS}}) {
        push @keep, $kid unless ("$kid" eq $chk);
    }
    $par->{KIDS} = \@keep;
    &collapse_empty($par);
}


sub get_from_ngbpm {
    my $dbh = BMS::FriendlyDBI->connect
        ("dbi:mysql:", 'DM500_072006AL/sysdm500@ICOP1',
         undef, { RaiseError  => 0,
                  PrintError  => 0,
                  LongReadLen => 100000,
                  AutoCommit  => 1, },
         -errorfile => '/scratch/ngbpm.err',
         -adminmail => 'tilfordc@bms.com', );
}

sub initialize {
    $lh = BMS::MapTracker::LoadHelper->new
        ( -username => 'PubMed',
          -loaddir  => $baseDir,
          -testmode => $tm,
          );
    $mt = $lh->tracker();
    $sts = BMS::MapTracker::SciTegicSmiles->new
        ( -loader  => $lh,
          # -msgfile => $msgfile,
          -verbose => $vb );
    $sts->{PP_IGNORED} = {};
    if (my $fh = $fc->output_fh('TestFile')) {
        $lh->redirect( -stream => 'TEST', -fh => $fh );
    }
}

sub finish {
    foreach my $smi (keys %{$sts->{PP_IGNORED}}) {
        &err("Ignored elemental form", $smi);
    }
    while (my ($issn, $title) = each %issnName) {
        $lh->set_class($issn, 'ISSN');
        $lh->set_edge( -name1 => $issn,
                       -name2 => $title,
                       -type  => 'is a shorter term for' );
    }
    $lh->write();
    if ($baseDir && !$tm) {
        if ($lh->process_ready()) {
            sleep(300);
            $lh->process_ready();
        }
    }
}


=pod

Prior to load:

  seqname  |    edge    |  mapping  |           snapshot            
-----------+------------+-----------+-------------------------------
 619506325 | 1160015589 | 530176306 | 2011-05-11 11:50:51.468693-04
-----------+------------+-----------+-------------------------------
 620107759 | 1160160582 | 530176306 | 2011-05-11 16:56:04.017013-04
-----------+------------+-----------+-------------------------------
 627206222 | 1175255331 | 530176306 | 2011-05-13 09:48:55.264129-04


=cut
