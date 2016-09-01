#!/stf/biobin/perl -w
# $Id$ 

# Failed to generate canonical key        CID:8706390


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

use strict;
use BMS::BmsArgumentParser;
use BMS::MapTracker::LoadHelper;
use BMS::FriendlySAX;
use BMS::MapTracker::AccessDenorm;
use BMS::ForkCritter;
use BMS::MapTracker::SciTegicSmiles;

my $args = BMS::BmsArgumentParser->new
    ( -cache    => 5000,
      -nocgi    => $ENV{'HTTP_HOST'} ? 0 : 1,
      -dir      => '/work5/tilfordc/MeSH',
      -limit    => 0,
      -testmode => 1,
      -loaddir  => "MeSH",
      -age      => 7,
      -forkfile => 1,
      -progress => 60,
      -errormail  => 'charles.tilford@bms.com',
      -strip    => 0,
      -verbose  => 1, );

if ($args->val(qw(nocgi))) {
    $args->shell_coloring();
} else {
    $args->set_mime(  );
}




# ftp://nlmpubs.nlm.nih.gov/online/mesh/.xmlmesh/



my $limit      = $args->{LIMIT};
my $dir        = $args->{DIR};
my $testfile   = $args->val(qw(testfile));
my $tm         = $args->{TESTMODE} || ($testfile ? 1 : 0);
my $cache      = $args->{CACHE};
my $vb         = $args->{VERBOSE};
my $prog       = $args->{PROGRESS};
my $smiF       = $args->{FASTSMILES};
my $year       = $args->{YEAR};
my $baseDir    = $args->val(qw(basedir loaddir));
my $clobber    = $args->{CLOBBER};
my $doStrip    = $args->{STRIP} ? 1 : 0;
my $treeOnly   = $args->val(qw(tree treeonly));
my $doDump     = $args->val(qw(dump dodump));

$args->msg("[I]","Only processing tree data") if ($treeOnly);

my $concRelMap = {
    BRD => 'is a child of',
    NRW => 'is a parent of',
    REL => 'is similar to',
};

my $ft         = "#FreeText#";
my $dPrfx      = "MeSH:";
my $tPrfx      = "Tree:";
my $sPrfx      = "SCR:";
my $uPrfx      = "UMLS:";
my $qPrfx      = "Qual:";
my $cPrfx      = "Conc:";

my (%trees);

my $lh = BMS::MapTracker::LoadHelper->new
    ( -username => 'MeSH',
      -basedir  => $baseDir,
      -testfile => $testfile,
      -testmode => $tm,
      );

my $sts = BMS::MapTracker::SciTegicSmiles->new
    ( -loader  => $lh,
     # -msgfile => $msgfile,
      -verbose => $vb );

&parse();

$lh->write();


sub parse {
    unless ($year) {
        my @dir = split(/[\n\r]+/, `ls -lh $dir/*.gz`);
        $args->msg("[!!]", "You need to provide the year you want parsed",
                   "Available:", @dir);
        exit;
    }
    $args->msg("Begin parsing - $year", `date`);

    
    &parse_desc() unless ($args->{NODESC});
    &parse_supp() unless ($args->{NOSUPP});
}

=pod Descriptors

 EntryCombinationList - used to describe more specific synthetic terms
 that can be used in certain situations. eg: "Abdomen" in the context
 of qualifier "injuries" can use instead "Abdominal Injuries"

=cut

sub parse_desc {
    my $dfile = "$dir/desc$year.gz";
    $args->msg("[#]","Processing Descriptor XML", $dfile);
    eval {
        BMS::FriendlySAX->new
            ( -file    => $dfile,
              -verbose => 0, #1,
              -method  => \&parseDesc,
              -limit   => $limit,
              -tag     => "DescriptorRecord",
              -quietlimit => 1,
              -skip    => [qw(ConsiderAlso RunningHead RelatedRegistryNumberList Annotation ConceptUMLSUI ThesaurusIDlist ActiveMeSHYearList CASN1Name DateCreated DateRevised DateEstablished AllowableQualifiersList RecordOriginatorsList OnlineNote xTermList PublicMeSHNote PreviousIndexingList HistoryNote xxEntryCombinationList)],
              );
      };
    $lh->write();

    $args->msg("[#]","Processing Descriptor Tree");
    # Make sure trees have unique Descriptor:
    foreach my $tree (keys %trees) {
        my $descs = $trees{$tree};
        unless ($#{$descs} == 0) {
            &err("Multiple Descriptors for Tree", $tree, @{$descs});
            $trees{$tree} = "";
            next;
        }
        $trees{$tree} = $descs->[0];
    }
    # Prevent Kid => Parent edges that result in cycles
    my %forbid = 
        ( "MeSH:D004989\tMeSH:D009014" => 1, # Morals -> Ethics
          "MeSH:D007140\tMeSH:D007135" => 1, # IG Fab Frags -> IG Var Reg
          "MeSH:D020155\tMeSH:D006885" => 1,
          # 3-Hydroxybutyric Acid -> Hydroxybutyrates
          );

    my (%notLeaf, %DAG, %edges);
    while (my ($tree, $id) = each %trees) {
        $lh->set_class($tree, 'MESHTREE');
        $lh->set_edge( -name1 => $tree,
                       -name2 => $id,
                       -type  => 'is a reliable alias for');
        $lh->set_edge( -name2 => $tree,
                       -name1 => $id,
                       -type  => 'is a reliable alias for');
        my $tpar = "MeSH Tree Root";
        my $dpar = "MeSH Descriptor Root";
        if ($tree =~ /^(.+)\.[^\.]+$/) {
            $tpar = $1;
            $dpar = $trees{$tpar};
        }
        $lh->kill_edge( -name1 => $tree,
                        -type  => 'is a child of');
        $lh->kill_edge( -name2 => $tree,
                        -type  => 'is a child of');

        $lh->set_edge( -name1 => $tree,
                       -name2 => $tpar,
                       -type  => 'is a child of');
        if ($dpar) {
            my $key = "$id\t$dpar";
            if ($forbid{$key}) {
                &err("Forbidding parentage", $id, $dpar);
            } else {
                $DAG{$id}{$dpar} = 1;
                $notLeaf{$dpar}  = 1;
                $edges{$key}++;
                $lh->set_edge( -name1 => $id,
                               -name2 => $dpar,
                               -type  => 'is a child of');
                $lh->kill_edge( -name1 => $id,
                                -type  => 'is a child of');
            }
        } else {
            $DAG{$id} ||= {};
            &err("Failed to find Descriptor ancestor", $tree, $id)
                unless ($limit);
        }
    }

    # Now verify we do not have cycles
    my @nodes = keys %DAG;
    my @edges = keys %edges;
    my @leaves;
    foreach my $id (sort @nodes) {
        push @leaves, $id unless ($notLeaf{$id});
    }
    my ($lnum, $nnum, $enum) = map { $#{$_} + 1 } (\@leaves,\@nodes,\@edges);
    
    my $pnum = 0;
    my $cnum = 0;
    my $ti = time;
    $args->msg("[v]","Checking for cycles");
    for my $l (0..$#leaves) {
        my $leaf = $leaves[$l];
        my @stack = ([$leaf]);
        while (my $path = shift @stack) {
            my $len   = $#{$path};
            my @pars  = keys %{$DAG{$path->[$len]} || {}};
            if ($#pars == -1) {
                # This path is complete and non-cyclic
                #warn "   ".join(' > ', @{$path})."\n";
                $pnum++;
            } else {
              PLOOP: foreach my $par (@pars) {
                  for my $p (0..$#{$path}) {
                      if ($par eq $path->[$p]) {
                          &err("Cycle detected", $p+1, @{$path}, $par);
                          $cnum++;
                          next PLOOP;
                      }
                  }
                  push @stack, [@{$path}, $par];
              }
            }
        }
        if ($vb && (time - $ti > 15)) {
            $ti = time;
            $args->msg(sprintf("  Leaf %6d - %s - %s", $l+1, $leaf,`date`));
        }
    }
    if ($vb) {
        my $sum = sprintf("%d leaves in %d nodes via %d edges and %d paths\n".
                          "  %.2f edges per node\n".
                          "  %.2f edges per leaf\n",
                          $lnum, $nnum, $enum, $pnum,
                          $nnum ? $enum / $nnum : 0, 
                          $lnum ? $enum / $lnum : 0 );
        $sum .= sprintf("  %.2f edges per branch\n", $enum / ($nnum -$lnum))
            if ($lnum < $nnum);
        $sum .= "  ".`date`;
        $args->msg( $sum );
    }

    if ($cnum) {
        $args->death("$cnum cycles were detected in the mapped DAG.",
                     "Please update the %forbid hash to break the cycles");
    }
    
    $lh->write();
    if ($@ && $@ !~ /user limit/i) {
        $args->death("FriendlySAX error:", $@);
    }
}


sub parse_supp {
    $args->msg("[-]", "No capture logic is in place for supplemental data!","Need to understand where these data are best integrated into MapTracker");
    my $dfile = "$dir/supp$year.gz";
    $args->msg("[#]","Processing Supplemental XML", $dfile);
    eval {
        BMS::FriendlySAX->new
            ( -file    => $dfile,
              -verbose => 0, #1,
              -method  => \&parseSupp,
              -quietlimit => 1,
              -limit   => $limit,
              -tag     => "SupplementalRecord",
              -skip    => [qw(ActiveMeSHYearList DateCreated DateRevised DateEstablished RecordOriginatorsList EntryCombinationList)],
              );
      };
    if ($@ && $@ !~ /user limit/i) {
        $args->death("FriendlySAX error:", $@);
    }
}

sub parseDesc {
    my $record = shift;
    print BMS::FriendlySAX::node_to_text( $record )."\n\n" if ($doDump);
    my @ids    = &deep_text($record, 'DescriptorUI');
    if ($#ids == -1) {
        return &err($record, "No ID found");
    } elsif ($#ids != 0) {
        return &err("Multiple IDs found", @ids);
    }
    my $id  = $dPrfx . $ids[0];
    my @trs = map { $tPrfx.$_ } &deep_text($record, qw(TreeNumberList
                                                        TreeNumber));
    map { push @{$trees{$_}}, $id; } @trs;
    return if ($treeOnly);

    map { $lh->set_edge( -name1 => $_,
                         -name2 => $id,
                         -type  => "is a reliable alias for" ) } @trs;

    $lh->set_class($id, 'MESHDESC');
    my @names = map { $ft.$_ } &deep_text($record, 'DescriptorName', 'String');
    my $relName = $id;
    if ($#names == 0) {
        $relName = $names[0];
        $lh->set_class($relName, 'MESHDESC');
        $lh->set_edge( -name1 => $id,
                       -name2 => $relName,
                       -type  => "is a reliable alias for" );
    }
    foreach my $name (@names) {
        $lh->set_edge( -name1 => $id,
                       -name2 => $name,
                       -type  => "is a shorter term for" );
    }



    $lh->kill_edge( -name1 => $id,
                    -type  => "is a child of" );
    $lh->kill_edge( -name1 => $id,
                    -type  => "is a parent of" );
    # THIS MAY BE A VERY BAD IDEA - no circularity checks!!!
    foreach my $par (map { $dPrfx.$_ } &deep_text
                     ($record, qw(PharmacologicalActionList
                                  PharmacologicalAction
                                  DescriptorReferredTo
                                  DescriptorUI))) {
        # eg D000001 -> D000900
        #    Calcimycin -> Anti-Bacterial Agents
        $lh->set_edge( -name1 => $id,
                       -name2 => $par,
                       -type  => "is a child of" ) unless ($id eq $par);
    }

    # THIS MAY BE A VERY BAD IDEA - no circularity checks!!!
    foreach my $kid (map { $dPrfx.$_ } &deep_text
                     ($record, qw(EntryCombinationList
                                  EntryCombination
                                  ECOUT 
                                  DescriptorReferredTo
                                  DescriptorUI))) {
        $lh->set_edge( -name1 => $id,
                       -name2 => $kid,
                       -type  => "is a parent of" ) unless ($id eq $kid);
    }
    
    my (@onames, @classes, $smi, %concepts, @crel);
    my $pcid = "";
    foreach my $concept (&dig_deep($record, 'ConceptList', 'Concept')) {
        my ($cid) = map {$cPrfx.$_} &deep_text($concept, 'ConceptUI');
        $lh->set_class($cid, 'MESHCONC');
        my $lid   = $cid;
        if ($concept->{ATTR}{PreferredConceptYN} eq 'Y') {
            if ($pcid) {
                &err("Multiple Preferred Concept IDs", $id, $pcid, $cid);
            } else {
                $pcid = $cid;
                $lid  = $id;
            }
        }
        my @alias = &deep_text($concept, 'ConceptName', 'String');
        push @alias, &deep_text($concept, 'TermList', 'Term', 'String');

        my @types;
        foreach my $st (&dig_deep($concept,
                                  qw(SemanticTypeList
                                     SemanticType))) {
            my @stids = map { $uPrfx. $_ } &deep_text($st, 'SemanticTypeUI');
            next unless ($#stids == 0);
            push @types, $stids[0];
        }
        foreach my $rel (&dig_deep($concept,
                                  qw(ConceptRelationList
                                     ConceptRelation))) {
            my @nodes = map {$cPrfx.$_} (&deep_text($rel, 'Concept1UI'),
                                         &deep_text($rel, 'Concept2UI'));
            unless ($#nodes == 1) {
                &err("Unusual node count for concept relation", $id, @nodes);
                next;
            }
            push @crel, [@nodes, $rel->{ATTR}{RelationName}];
        }
        my @cas = &deep_text($concept, 'RelatedRegistryNumber');
        foreach my $id (&deep_text($concept, 'RegistryNumber')) {
            if ($id =~ /^[A-Z0-9]{10}$/) {
                push @cas, "UNII-$id";
            }
        }
        $concepts{$cid} = {
            id    => $cid,
            link  => $lid,
            alias => \@alias,
            type  => \@types,
            cas   => \@cas,
            com   => [ &deep_text($concept, 'ScopeNote') ],
        };
        &strip_record($concept, qw(RegistryNumber ConceptUI ConceptRelationList SemanticTypeList TermList ConceptName ScopeNote PharmacologicalActionList)) if  ($doStrip);
    }

    foreach my $rdat (@crel) {
        my ($n1, $n2, $rn) = @{$rdat};
        $n1 = $id if ($n1 eq $pcid);
        $n2 = $id if ($n2 eq $pcid);
        if (my $type = $concRelMap->{$rn}) {
            $lh->set_edge( -name1 => $n1,
                           -name2 => $n2,
                           -type  => $type );
        } else {
            &err("Unknown concept relation", $n1, $rn, $n2);
        }
    }
    
    foreach my $cdat (values %concepts) {
        my $lid = $cdat->{link};
        if ($lid eq $id) {
            $lh->set_edge( -name1 => $id,
                           -name2 => $cdat->{id},
                           -type  => "is the same as" );
        }
        map { $lh->set_edge( -name1 => $lid,
                             -name2 => $ft.$_,
                             -type  => "has comment" ) } @{$cdat->{com}};

        map { $lh->set_edge( -name1 => $lid,
                             -name2 => $ft.$_,
                             -type  => "is a shorter term for",
                             ) } @{$cdat->{alias}};
        
        map { $lh->set_edge( -name1 => $lid,
                             -name2 => $_,
                             -type  => "is a child of" ) } @{$cdat->{type}};
    }
    
    my @maybeCAS = @{$concepts{$pcid}{cas} || []};
    my @CAS;
    foreach my $ctxt (@{$concepts{$pcid}{cas} || []}) {
        my $id;
        if ($ctxt =~ /^(CAS:)?(\d+\-\d+\-\d+)$/ ||
            $ctxt =~ /^(CAS:)?(\d+\-\d+\-\d+) \(.+\)$/ ) {
            if (my $name = $3) {
                push @onames, $ft.$name;
            }
            $id = "CAS:$2";
            push @CAS, $id;
        } elsif ($ctxt =~ /^EC (\d.+)/) {
            $id = "EC:$1";
        } elsif ($ctxt =~ /^UNII-/) {
            $id = $ctxt;
            push @CAS, $id;
        }
        push @onames, $id if ($id);
    }
    if ($#CAS != -1) {
        push @classes, 'Chemical';
        my @smis;
        if (1) {
            $args->msg_once("[!!]","Working on canonicalization", "Ignoring compound information!");
        } else {
            @smis = $sts->term_to_canonical( @CAS );
        }
        if ($#smis == 0) {
            # We have a unique SMILES assignment
            $smi = $smis[0];
        }
        #foreach my $cas (@CAS) {
        #    $lh->set_class($cas, 'CAS');
        #}
    }

    foreach my $name ( @onames) {
        $lh->set_edge( -name1 => $relName,
                       -name2 => $name,
                       -type  => "is the preferred lexical variant of" )
            unless ($relName eq $name);
    }

    foreach my $relid (map { $dPrfx.$_ } &deep_text($record, 'SeeRelatedList','SeeRelatedDescriptor','DescriptorReferredTo','DescriptorUI')) {
        $lh->set_edge( -name1 => $id,
                       -name2 => $relid,
                       -type  => "is similar to" )
                unless ($id eq $relid);
    }
    
    foreach my $class (@classes) {
        map { $lh->set_class($_, $class) } ($id, @onames);
    }
    if ($smi) {
        foreach my $name ($id, @onames) {
            $lh->set_edge( -name1 => $smi,
                           -name2 => $name,
                           -type  => 'is a reliable alias for', );
        }
    }

    &strip_record($record, qw(PharmacologicalActionList TreeNumberList DescriptorName SeeRelatedList EntryCombinationList))
        if ($doStrip);
}

sub parseSupp {
    my $record = shift;

    $lh->write_threshold_quick($cache);
    print BMS::FriendlySAX::node_to_text( $record )."\n\n" if ($doDump);
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
            $txt =~ s/\P{IsASCII}//g;
            $txt =~ s/^\s+//;
            $txt =~ s/\s+$//;
            $txt =~ s/\s+/ /g;
            push @text, $txt unless ($txt =~ /^(\d+|\s*)$/);
        }
    }
    return @text;
}

sub err {
    my $record = ref($_[0]) ? shift : undef;
    $args->msg("[ERR]", @_);
}
