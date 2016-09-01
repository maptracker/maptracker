#!/stf/sys64/bin/perl -w

BEGIN {
    # Needed to make my libraries available to Perl64:
    # use lib '/stf/biocgi/tilfordc/released';
    # Allows usage of beta modules to be tested:
    my $prog = $0; my $dir = `pwd`;
    if ($prog =~ /working/ || $dir =~ /working/) {
	warn "\n\n *** This is Beta Software ***\n\n";
	require lib;
	import lib '/stf/biocgi/tilfordc/perllib';
    }
    $| = 1;
    print '';
}

=head1 Arguments

     -file Optional file path specifying the ontology you want to
           parse. If not provided, then the local OBO directory
           (specified by -dir, below) will be scanned and all parsable
           ontologies will be analyzed.

      -dir Default /work5/tilfordc/obo/ontology

    -limit Optional integer. If provided, onlt that number of nodes
           will be parsed for each ontology.

 -testmode Default 1. If non-zero, then no database load will
           occur. If set to quiet, then parsing will occur and output
           will be discarded (though you will still get information
           about unkown elements found while parsing). Any other value
           will result in pseudo-tabular output being generated and
           sent to the testfile (option described below).

 -testfile Default 'OBO_Loader.txt'. A text file containing detailed
           records for what would have been loaded if -testmode was
           zero. Pass zero for this option if you want output on the
           screen.

  -verbose Default 1. Displays progress information on screen if
           true. Pass zero to supress information display.

    -accok Default 0. Normally accessions will be rejected if they
           fail to meet certain regular expression criteria. Pass a
           true value if you want to skip this step - VERY
           DANGEROUS. Some ontologies have messed up accessions in
           them, you could load junk data if you are not
           careful. Useful for MGED, which has text IDs (as opposed to
           accessions).

   -prefix A namespace prefix to append onto ontology
           accessions. Again, used for MGED; if you specify '-prefix
           MGED' then 'BioAssayPackage' will become
           'MGED:BioAssayPackage'.

   -commit Default null. If true, then at the end of analysis the
           database will be immediately loaded (rather than waiting
           for the every 10 minute cron to run), assuming -testmode is
           false.

=head1 Updating OBO

 OBO is at sourceforge:

 http://obo.sourceforge.net/main.html

 ... and unfortunately relies on CVS rather than FTP. You can update
 the ontologies by going to the OBO directory (see -dir above) and:

 cvs update -d

 Very sluggish.

=cut

use strict;
use BMS::Branch;
use BMS::CommonCAT;
use GO::Parser;
use BMS::FriendlySAX;
use BMS::MapTracker::LoadHelper;

my $debug = BMS::Branch->new( );

my $args = &PARSEARGS
    ( -cache    => 20000,
      -dir      => '/work5/tilfordc/obo/ontology',
      -nocgi    => 1,
      -limit    => 0,
      -testmode => 1,
      -testfile => "OBO_Loader.txt",
      -verbose  => 1,
      -prefix   => '',
      -accok    => 0,
      );

my $limit = $args->{LIMIT};
my $tm    = $args->{TESTMODE};
my $tf    = $args->{TESTFILE} ? $args->{TESTFILE} : '';
my $cache = $args->{CACHE};
my $vb    = $args->{VERBOSE} ? 1 : 0;
my $prfx  = $args->{PREFIX};
$prfx =~ s/\:$//;

my $allobo = 'Open Biomedical Ontologies';

my $standardizeDirection = {
    is_a         => 'is_a',
    has_subclass => 'is_a',
    part_of      => 'part_of',
    has_part     => 'part_of',
};

my %synrefs = map { uc($_) => 1 } qw(ISBN HTTP PMID CAS FTP);

my %ignoreTerms = map { uc($_) => 1 }
qw(APWeb CFG CL DELTA FAO GR IRRI MFO MPD MedPixRadiologyTeachingFiles NMF Nmice POC Oryzabase RGD TAIR ZEA ZFIN ACV JTE PG SMB);

my %collated;
my %doneFile = map { $_ => 1 } qw(MGEDOntology gene_ontology);

my $lh  = BMS::MapTracker::LoadHelper->new
    ( -username => 'OBO',
      -testfile => $tf,
      -testmode => $tm,
      );

$lh->set_class($allobo, 'group');

warn `date` if ($vb);

if ($tm && $tf && $vb) {
    warn "Test output to:\n  less -S $tf\n\n";
}

my $xsldir  = "/stf/sys/perl/lib/site_perl/5.8/GO/xsl";
my $dir     = $args->{DIR};
my $linkdir = "$dir/all_ontologies";
unless (-d $linkdir) {
    mkdir($linkdir, 0777);
    chmod(0777, $linkdir);
}

if (my $file = $args->{FILE}) {
    &PARSE_FILE($file);
} else {
    warn "Parsing all OBO ontologies\n" if ($vb);
    foreach my $file (&scan_ontologies( $dir )) {
        &PARSE_FILE($file);
    }
}
# $lh->write();
&finalize;
warn `date` if ($vb);

sub scan_ontologies {
    my ($dir, $done) = @_;
    return () unless (-d $dir);
    $done ||= {};
    opendir(DIR, $dir) || die "Failed to open directory '$dir':\n  $!\n  ";
    my @files;
    foreach my $file (readdir DIR) {
        next if ($file eq 'CVS');
        my $fullpath = "$dir/$file";
        # Symlinks may create circular structures...
        next if ($done->{$fullpath}++);
        if (-d $fullpath) {
            push @files, &scan_ontologies( $fullpath, $done )
                unless ( $file eq '.' || $file eq '..');
        } elsif ($file =~ /\.obo$/ || $file =~ /\.owl$/) {
            push @files, $fullpath;
        }
    }
    return @files;
}


sub CONVERT_FILE {
    my ($fn, $outf) = @_;
    
    my @bits = split(/\//, $fn);
    my $short = $bits[-1];
    my $slink = "$linkdir/$short";
    unless (-e $slink) {
        my $rel = $fn;
        if ($rel =~ /^\Q$dir\E/) {
            # We can make the symlink relative to the OBO directory
            $rel =~ s/^\Q$dir\E//;
            $rel =~ s/^\///;
            $rel = "../$rel";
        }
        my $cmd = "ln -s $rel $linkdir/$short";
        system($cmd);
    }

    unless ($outf) {
        $outf = $fn;
        $outf =~ s/\.[^\.]+$/\.obo_xml/;
    }

    if (!-e $outf || ((-M $outf) > (-M $fn) && -s $outf)) {
        # If the XML file does not exist, or is younger than the obo, make it:
        if ($fn =~ /\.owl$/) {
            # The GO::Parser handling of this was failing...
            my $cmd = "xsltproc $xsldir/owl_to_oboxml.xsl $fn > $outf";
            system($cmd);
        } else {
            my $parser = new GO::Parser( { handler => 'xml' });
            $parser->handler->file($outf);
            $parser->parse( $fn );
        }
    }
    $short =~ s/\.[^\.]+$//;
    return ($outf, $short);
}

sub PARSE_FILE {
    my ($file) = @_;
    # warn $file; system("grep 'is_obsolete' $file | head -n 10"); return;
    # system("grep '<default-namespace>' $file"); return;

    my ($xml, $short) = &CONVERT_FILE($file);
    return if (!$xml || $doneFile{$short}++);

    # allow sodium to be linked to 'Na'
    $lh->allow_bogus(1) if ($short eq 'chebi');

    warn "  Parsing $short\n" if ($vb);
    eval {
        my $fs = BMS::FriendlySAX->new
            ( -file     => $xml,
              -tag      => 'term',
              -skip     => 'xxxx',
              -limit    => $limit,
              # -final    => \&finalize,
              -textmeth => \&process_text,
              -method   => \&parse_record,  );
    };
    $lh->allow_bogus(0);
    $lh->write();
    my $err = $@ || '';
    warn "$err\n" if ($err && $err !~ /Ungraceful LIMIT exit/);    
}

sub process_text {
    my ($txt) = @_;
    # Kill leading space
    $txt =~ s/^\s+//;
    # Kill trailing space
    $txt =~ s/\s+$//;
    # Escaped tabs and returns to real tabs
    $txt =~ s/\\[ntr]/\t/g;
    # tabs and returns to spaces
    $txt =~ s/[\n\r\t]+/ /g;
    # Space runs to single space
    $txt =~ s/\s\s+/ /g;
    # Some characters are observered to be randomly escaped, sometimes twice
    $txt =~ s/\\+(\:|\"|\!|\,)/$1/g;
    warn $txt if ($txt =~ /\\/);
    return $txt;
}

sub is_acc {
    my ($txt) = @_;
    return ($args->{ACCOK} ||
            $txt =~ /^[A-Za-z]{2,6}\:\d+$/ ||
            $txt =~ /^[A-Za-z]{2,6}_root\:\d+$/ ||
            $txt =~ /^MESH\:[A-Z][\d\.]*$/ ||
            $txt eq 'MESH:root') ? 1 : 0;
}

sub parse_record {
    my ($hash) = @_;
    my $inodes = $hash->{BYTAG}{id};
    return if (!$inodes || $#{$inodes} != 0);
    my $id = $inodes->[0]{TEXT} || '';
    return unless ($id);
    $id = "$prfx:$id" if ($prfx);

    unless (&is_acc($id)) {
        warn "    $id || Bad accession\n";
        return;
    }
    $lh->set_class($id, 'obo');

    my $rchk = $hash->{BYTAG}{is_root};
    if ($rchk && $rchk->[0]{TEXT} eq '1') {
        # This is a root node
        $lh->set_edge( -name1 => $allobo,
                       -name2 => $id,
                       -type  => 'has member');
    }

    # Identify synonyms
    my $synodes   = $hash->{BYTAG}{synonym} || [];
    my %shash;
    foreach my $synode (@{$synodes}) {
        my $scope = '#FreeText#' . ($synode->{ATTR}{scope} || 'unknown');
        my $txtnodes  = $synode->{BYTAG}{synonym_text};
        foreach my $tnode (@{$txtnodes}) {
            my $st  = $tnode->{TEXT};
            if (&is_acc($st)) {
                # This is actually another ontology term
                $lh->set_edge( -name1 => $id,
                               -name2 => $st,
                               -type  => 'is mapped to' );
            } else {
                $shash{"#FreeText#$st"} = $scope;
            }
        }
    }

    # Set the name
    my $nnodes = $hash->{BYTAG}{name} || [];
    $lh->kill_edge( -name1 => $id,
                    -type  => 'is a shorter term for' );
    foreach my $nnode (@{$nnodes}) {
        my $name = '#FreeText#' . $nnode->{TEXT};
        $lh->set_edge( -name1 => $id,
                       -name2 => $name,
                       -type  => 'is a shorter term for' );
        $lh->kill_edge( -name2 => $name,
                        -type  => 'is a lexical variant of', );
        while (my ($syn, $scope) = each %shash) {
            next if (uc($syn) eq uc($name));
            $lh->set_edge( -name1 => $syn,
                           -name2 => $name,
                           -type  => 'is a lexical variant of',
                           -tags  => [[ 'Scope', $scope, undef ]]);
        }
    }

    # Set more detailed descriptions:
    my $defnodes = $hash->{BYTAG}{def} || [];
    foreach my $defnode (@{$defnodes}) {
        my $descnodes = $defnode->{BYTAG}{defstr} || [];
        my $xnodes    = $defnode->{BYTAG}{dbxref} || [];
        my @tags;
        foreach my $xnode (@{$xnodes}) {
            my ($db, $acc) = ( $xnode->{BYTAG}{dbname}, 
                               $xnode->{BYTAG}{acc});
            next unless ($db && $acc);
            ($db, $acc) = ($db->[0]{TEXT}, $acc->[0]{TEXT});
            # If the DB contains whitespace, it's likely bogus
            next if ($db =~ /\s/ );
            my $ucdb = uc($db);
            if ($acc =~ /^http[\: ](\/\/.+)/i) {
                # Standardize hyperlinks (which sometimes lack the ':')
                $db   = 'http';
                $ucdb = 'HTTP';
                $acc  = $1;
            }
            if ($acc =~ /^isbn[\: ]([\d\-]+)$/i) {
                # Standardize hyperlinks (which sometimes lack the ':')
                $db  = $ucdb = 'ISBN';
                $acc = $1;
            }
            if ($synrefs{$ucdb}) {
                my $syn = "$db:$acc";
                if ($ucdb eq 'HTTP' || $ucdb eq 'FTP') {
                    $syn = "$db://$acc" unless ($acc =~ /^\/\//);
                    $lh->set_class($syn, 'hyperlink');
                } elsif ($ucdb eq 'CAS') {
                    if ($acc =~ /([\d\-]+)/) {
                        # Get the meat out of extraneous chars (such as [])
                        $syn = "$db:$1";
                    } else {
                        next;
                    }
                    # $lh->set_class($syn, 'cas');
                }
                push @tags,  [ 'Referenced In', $syn, undef ];
             } elsif (($ucdb eq 'FB'   && $acc =~ /FBrf\d+/)) {
                # Use the reference as is:
                push @tags, [ 'Referenced In', $acc ];
            } else {
                push @{$collated{$db}{'Term description xref'}}, $acc
                    unless ($ignoreTerms{$ucdb} ||
                            length($acc) < 4 ||
                            $acc =~ /Mouse Genome Informatics Curator/i ||
                            $acc =~ /TJL staff/i);
            }
        }
        foreach my $descnode (@{$descnodes}) {
            my $desc = '#FreeText#' . $descnode->{TEXT};
            $lh->set_edge( -name1 => $id,
                           -name2 => $desc,
                           -type  => 'has comment',
                           -tags  => \@tags );
        }
        
    }

    # Comments

    my $comnodes = $hash->{BYTAG}{comment} || [];
    foreach my $cn (@{$comnodes}) {
        my $txt = $cn->{TEXT};
        $lh->set_edge( -name1 => $id,
                       -name2 => '#FreeText#' . $txt,
                       -type  => 'has comment', );
    }

    # Deprecation
    my $obso = $hash->{BYTAG}{is_obsolete};
    if ($obso && $obso->[0]{TEXT} eq '1') {
        # This term is obsolete
        $lh->set_class($id, 'deprecated');
    } else {
        $lh->kill_class($id, 'deprecated');
    }

    # Build hierarchy

    # CAREFUL - we are allowing bi-directional edges to go in. So far
    # it looks like ontologies are always defined by the child
    # referencing its parents. If that changes (if parents ever
    # reference children) then this code would start deleting properly
    # set edges.

    $lh->kill_edge( -name1 => $id,
                    -type  => 'is a child of');

    my %parentage;
    my $isas = $hash->{BYTAG}{is_a} || [];
    foreach my $isa (@{$isas}) {
        $parentage{ $isa->{TEXT} } = 'is_a';
    }
    my $rels = $hash->{BYTAG}{relationship} || [];
    foreach my $rel (@{$rels}) {
        my ($type, $to) = ( $rel->{BYTAG}{type}, $rel->{BYTAG}{to});
        unless ($type && $to) {
            next;
        }
        $parentage{ $to->[0]{TEXT} } = $type->[0]{TEXT};
    }

    my @oids = sort keys %parentage;
    if ($#oids > -1) {

        foreach my $oid (@oids) {
            my $useid = $prfx ? "$prfx:$oid" : $oid;
            unless (&is_acc($oid)) {
                warn "    $oid || Bad parent accession\n";
                next;
            }
            my $type = $parentage{$oid};
            my $dir  = $standardizeDirection->{ $type };
            unless ($dir) {
                push @{$collated{$type}{'Unknown relationship'}}, $id;
                next;
            }
            my ($first, $second) = ($dir eq $type) 
                ? ($id, $useid) : ($useid, $id);
            $lh->set_edge( -name1 => $first,
                           -name2 => $second,
                           -type  => 'is a child of',
                           -tags  => [ ['GO Type', "#OBO_REL#$dir" ] ] );
        }
    }

    $lh->write_threshold_quick( $cache );
}

sub finalize {
    $lh->write();
    my @probs = sort keys %collated;
    warn "Some identifiers were not recognized:\n" if ($#probs > -1);
    foreach my $id (@probs) {
        warn "  $id:\n";
        foreach my $desc (sort keys %{$collated{$id}}) {
            my $arr    = $collated{$id}{$desc};
            my $tail   = $#{$arr};
            my @sample = $arr->[0];
            push @sample, $arr->[ int($tail / 2) ] if ($tail > 1);
            push @sample, $arr->[$tail] if ($tail > 0);
            my $examp = join(", ", @sample);
            warn sprintf("    %s [%d] %s\n", $desc, $tail+1, $examp);
        }
    }

    if (!$tm && $args->{COMMIT}) {
        warn "Loading data into database\n" if ($vb);
        $lh->process_ready();
    }
}
