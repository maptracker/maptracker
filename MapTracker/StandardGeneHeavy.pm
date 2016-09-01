# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::StandardGeneHeavy;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use strict;
use BMS::MapTracker::StandardGeneLight;
use BMS::MapTracker::AccessDenorm;
use BMS::Utilities::BmsDatabaseEnvironment;
use BMS::Utilities::Benchmark;
use BMS::MapTracker::GenAccService;
use BMS::FriendlyDBI;

use vars qw(@ISA);
@ISA      = qw(BMS::MapTracker::StandardGeneLight);

our $smallScore = 0.0001;

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
        VB     => 0,
    };
    bless ($self, $class);
    my $args = $self->parseparams( @_ );
    $self->verbose( $args->{VERBOSE} || $args->{VB} || 0);

    my $debug = $self->debug();
    $debug->max_array(300);
    $debug->max_hash(30);
    $debug->extend_param('skip_key', ['TRACKER','SCHEMA','URLS','CHILDREN','SPACE', 'CLASSES']);
    $debug->format( $ENV{HTTP_HOST} ? 'html' : 'text' );
    $self->_connect( @_ );
    $self->set_schema();
    if ($args->{REBUILD}) {
        $self->{DBH}->make_all();
        $self->_set_basic();
    }
    $self->standard_sths();
    $self->{AGE}      = $args->{AGE}      || $args->{AGEALL};
    $self->{CLOUDAGE} = $args->{CLOUDAGE} || $args->{AGEALL};
    $self->clobber( $args->{CLOBBER} );
    $BMS::MapTracker::StandardGeneLight::doNotAlterDB = 0;
    return $self;
}

sub nextval { return shift->dbh->nextval(@_); }

sub benchTime {
    my $self = shift;
    my $pad  = shift || "";
    my $ti   = $self->lastbench();
    my @units = ('sec','min','hour','day');
    my @mod   = (1,60,60,24);
    my $sfx   = "";
    my $u;
    if (my $num = shift) {
        $sfx = sprintf(" = %s/sec", int(0.5 + 10 * $num / $ti)/10) if ($ti);
    }
    while (1) {
        last if ($#units == -1);
        $u = shift @units;
        $ti /= shift @mod;
        last if ($ti < 100);
    }
    $self->msg("   $pad", sprintf("%.1f %s%s", $ti, $u, $sfx));
}

sub geneset_for_taxa {
    my $self = shift;
    my ($treq) = @_;
    unless ($treq) {
        $self->err("Can not find gene set without taxa");
        return ();
    }
    my $mt    = $self->tracker();
    my @taxs  = $mt->get_taxa( $treq );
    if ($#taxs == -1) {
        $self->err("Failed to find species for '$treq'");
        return ();
    } elsif ($#taxs != 0) {
        $self->err("Multiple taxae for '$treq'","Please select one of the designations below", map { $_->name() } @taxs);
        return ();
    }
    my $mttax = $taxs[0];
    my $alias = $mttax->each_alias_class();
    # warn $self->branch($alias);
    my $taxa  = $mttax->name();
    my $gsNm  = "$taxa LocusLink";
    my $gsid  = $self->taxa_gene_set( $taxa );
    $self->set_keyval( $gsid, {
        Taxa      => $taxa,
        Type      => 'Gene Set',
        TaxaName => $alias->{'GENBANK COMMON NAME'} || $alias->{'COMMON NAME'},
        Alias     => $alias->{ALL},
        Namespace => ["LocusLink","RefSeq"],
    }, 1);
    return ($gsid, $gsNm, $taxa);
}

sub load_taxa {
    my $self = shift;
    my $args  = $self->parseparams( -fork => 20,
                                    @_ );
    $self->{FORKNUM} = $args->{FORK} || 20;
    my $limit = $args->{LIMIT};
    my $prog  = $args->{PROG} || $args->{PROGRESS} || 0;
    my $ad    = $self->denorm();
    my $mt    = $self->tracker();
    my $basicOnly = $args->{BASIC};

    my ($gsid, $gsNm, $taxa) = $self->geneset_for_taxa
        ($args->{TAXA} || $args->{SPECIES});

    return unless ($gsid);

    $self->bench_start('Get Locus');
    $self->msg("Loading $gsNm [$gsid] into ". $self->dbname);
    my @priNS = map { $ad->namespace_name( $_ ) } qw(LL RSR RSP);
    my ($lns, $rns, $pns) = @priNS;
    my %priNSh = map { $_ => 1 } @priNS;

    my $baseName = $taxa;
    $baseName =~ s/[^A-Z]//gi;
    $baseName = "SGH-$baseName";

    $self->msg_once("[-]", "Files stored as: $baseName".'*');

    my $lrp = $self->LRP_data($taxa, $baseName);

    my (%objects, @genes, %parents);
    my $geneLimit = 0;
    # my $geneLimit = 5;
    my $geneCount = 0;
    foreach my $row (@{$lrp}) {
        my ($loc, $rna, $prot) = @{$row};
        # What is the "primary gene identifier" for this row?
        # Some organisms do not have loci, only protein.
        my $gene = $loc || $rna || $prot;
        # Have we seen the gene already?
        my $alreadyThere = $objects{$gene};
        # last if ($limit && !$alreadyThere && $#genes + 1 >= $limit);
        $objects{$loc} ||= {
            Accession => $loc,
            Taxa      => $taxa,
            Type      => "Locus",
            Namespace => $lns,
        } if ($loc);
        $objects{$rna} ||= {
            Accession => $rna,
            Taxa      => $taxa,
            Type      => "RNA",
            Namespace => $rns,
        } if ($rna);
        $objects{$prot} ||= {
            Accession => $prot,
            Taxa      => $taxa,
            Type      => "Protein",
            Namespace => $pns,
        } if ($prot);
        unless ($alreadyThere) {
            last if ($geneLimit && $geneCount >= $geneLimit);
            # New locus
            $objects{$gene}{GeneType} = $objects{$gene}{Type};
            push @genes, $objects{$gene};
            # Record itself as related to simplify description recovery
            $objects{$gene}{related}{$gene}{1} = 1;
            $geneCount++;
        }
        
        # Set parentage:
        my @hier = ($prot, $rna, $loc);
        for my $i (0..1) {
            my $kid = $hier[$i];
            next unless ($kid);
            # Find the parent of the child. It is possible for a loci to
            # have a protein but no RNA
            my $par;
            for (my $j = $i + 1; $j <= $#hier; $j++) {
                last if ($par = $hier[$j]);
            }
            next unless ($par);
            if (my $prior = $parents{$kid}) {
                $self->errMsg("Multiple parents for $kid",
                              "Using $prior ignoring $par")
                    unless ($prior eq $par);
            } else {
                $parents{$kid} = $par;
                $objects{$par}{kids}{$kid} = $objects{$kid};
                # Record the child as a related member of the Gene
                # with score 1
                $objects{$gene}{related}{$kid}{1} = 1;
                
            }
        }
    }
    # die $self->branch(\%objects) if ($geneLimit);

    #my %kids;
    #while (my ($kid, $par) = each %parents) {
    #    $objects{$kid}{Parent} = $par;
    #    push @{$kids{$par}}, $kid;
    #}
    
    my $gFile = "$baseName-GeneIDs.list";
    my $gData = {
        list => $gFile,
        gnum => $#genes + 1,
        objs => \%objects,
        base => $baseName,
        gene => \@genes,
        gsid => $gsid,
        ntok => {},
        nnam => {},
        prog => $prog,
    };

    open(GFILE, ">$gFile") || $self->death("Failed to write Gene ID file", $gFile, $!);
    foreach my $gdat (@genes) {
        my ($ns, $acc) = ($gdat->{Namespace}, $gdat->{Accession});
        my $gid = sprintf
            ("#%s#%s", $gData->{ntok}{$ns} ||= $ad->namespace_token($ns),$acc);
        print GFILE "$gid\n";
    }
    close GFILE;

    $self->_add_symbols( $gData );
    $self->_add_orthologs( $gData );
    # $self->_add_probes( $gData );
    if ($basicOnly) {
        $self->msg("  ", "-basic mode will skip accessions");
    } else {
        $self->_add_related( $gData );
    }
    # die $self->branch( -maxany => 9999, -ref => $self->_extract_descriptions($objects{LOC2580}, \%objects));
    # $self->_add_descriptions( $gData );
    $self->_set_children( $gData );
    $self->_set_parameters( $gData );
    $self->_load_words( $gData ) unless ($basicOnly);
    $self->msg("Finished", `date`);
}

sub errMsg {
    my $self = shift;
    $self->msg("[!!]", @_);
}

sub LRP_data {
    # Locus - RNA - Protein
    my $self = shift;
    my ($taxa, $baseName) = @_;
    my $file = $self->_temp_file_name($baseName, 'LocusRnaProtein');
    unless ($self->use_existing_file($file)) {
        $self->bench_start('Get Locus');
        my $llList = $self->forked_convert
            ( -id => $taxa, -ns1 => 'TAX', -ns2 => 'LL',
              -cols => 'termout', -nonull => 1, -fork => 0,
              -output => $self->_temp_file_name($baseName, 'AllLoci'));
        my @ll   = map { $_->[0] } @{$llList};
        my $lNum = $#ll + 1;
        $self->bench_end('Get Locus');
        $self->benchTime("", $lNum);
        my %lrp;
        if ($lNum) {
            # At least one loci, initialize tree with them
            map { $lrp{$_} = {} } @ll;

            # Get RNA assocaited with the loci:
            $self->bench_start('Locus to RNA');
            $self->msg("   ","Getting RNA for $lNum loci");
            my $l2rArr = $self->forked_convert
                ( -id => \@ll, -ns1 => 'LL', -ns2 => 'RSR', -nonull => 1,
                  -min  => 1, -cols => 'term_in,term_out', -directonly => 1,
                  -output => $self->_temp_file_name($baseName, 'LL', 'RSR'));
            my %gotR;
            foreach my $row (@{$l2rArr}) {
                my ($loc, $rna) = @{$row};
                next unless ($loc && $rna);
                if (my $prior = $gotR{$rna}) {
                    $self->errMsg("Multiple loci recovered for RNA",
                                  "$rna will use $prior, not $loc")
                        unless ($prior eq $loc);
                } else {
                    $lrp{$loc}{$rna} ||= {};
                    $gotR{$rna} = $loc;
                }
            }
            my @rnas = sort keys %gotR;
            my $rNum = $#rnas + 1;
            $self->bench_end('Locus to RNA');
            $self->benchTime("   ", $lNum);

            # Get protein associated with the RNA:
            $self->bench_start('RNA to Protein');
            $self->msg("   ","Getting protein for $rNum RNA");
            my $r2pArr = $#rnas == -1 ? [] : $self->forked_convert
                ( -id => \@rnas, -ns1 => 'RSR', -ns2 => 'RSP', -nonull => 1,
                  -min  => 1, -cols => 'term_in,term_out', -directonly => 1,
                  -output => $self->_temp_file_name($baseName, 'RSR', 'RSP'));
            my %gotP;
            foreach my $row (@{$r2pArr}) {
                my ($rna, $prot) = @{$row};
                next unless ($rna && $prot);
                if (my $loc = $gotR{$rna}) {
                    $lrp{$loc}{$rna}{$prot} ||= {};
                    if (my $prior = $gotP{$prot}) {
                        $self->errMsg
                            ("Multiple loci recovered for protein",
                             "$prot will use $prior, not $loc via $rna")
                            unless ($prior eq $loc);
                    } else {
                        $gotP{$prot} = $loc;
                    }
                } else {
                    $self->errMsg("Failed to find locus for RNA",
                                  "$rna -> $prot");
                }
            }
            $self->bench_end('RNA to Protein');
            $self->benchTime("   ", $rNum);
            
            # Are there any protein that do NOT have an RNA intermediate?
            $self->bench_start('Locus to Protein');
            $self->msg("   ","Getting Protein for $lNum loci");
            my $l2pArr = $self->forked_convert
                ( -id => \@ll, -ns1 => 'LL', -ns2 => 'RSP', -nonull => 1,
                  -min  => 1, -cols => 'term_in,term_out', -directonly => 1,
                  -output => $self->_temp_file_name($baseName, 'LL', 'RSP'));
            foreach my $row (@{$l2pArr}) {
                my ($loc, $prot) = @{$row};
                next unless ($loc && $prot);
                if (my $prior = $gotP{$prot}) {
                    $self->errMsg
                            ("Inconsistent recovery of protein from locus",
                             "$prot will use $prior, not $loc found directly")
                            unless ($prior eq $loc);
                } else {
                    $lrp{$loc}{""}{$prot} ||= {};
                }
            }
            $self->bench_end('Locus to Protein');
            $self->benchTime("   ", $#{$l2pArr} + 1);


        } else {
            # No loci, we should try to get protein directly from taxa
            $self->death("Need to code taxa -> protein");
        }
        open(FILE, ">$file") || $self->death
            ("Failed to write LSP file", $file, $!);
        foreach my $loc (sort keys %lrp) {
            my @rnas = sort keys %{$lrp{$loc}};
            @rnas    = ("") if ($#rnas == -1);
            foreach my $rna (@rnas) {
                my @prots = sort keys %{$lrp{$loc}{$rna} || {}};
                @prots = ("") if ($#prots == -1);
                foreach my $prot (@prots) {
                    print FILE join("\t", $loc, $rna, $prot)."\n";
                }
            }
        }
        close FILE;
    }
    my @rv;
    open(FILE, "<$file") || $self->death
        ("Failed to read LSP file", $file, $!);
    while (<FILE>) {
        s/[\n\r]+$//;
        push @rv, [ split(/\t/) ];
    }
    close FILE;
    return \@rv;
}

sub _add_symbols {
    my $self = shift;
    my ($gData) = @_;
    $self->bench_start();
    my $objs   = $gData->{objs};
    my $objNum = $gData->{gnum};
    $self->msg("   ","Getting Symbols for $objNum genes");

    my $splitter = '[^A-Z0-9_\-]+';
    my $symId = $self->get_wordSource
        ("Official Symbols",
         { desc => "Official gene symbols, as defined by the HGNC"},
         20, $splitter, '[^A-Z0-9]');

    my $aliId = $self->get_wordSource
        ("Unofficial Symbols", 
         { desc => "Unofficial gene symbols and aliases"},
         6, $splitter, '[^A-Z0-9]');

    my $syms = $self->forked_convert
        ( -idlist    => $gData->{list}, -standardize => 1, -ns2 => 'SYM',
          -nullscore => -1, -nonull => 1,
          -cols => "termin,termout,matched",
          -output => $self->_temp_file_name($gData->{base}, "Gene", 'SYM'));
    foreach my $row (@{$syms}) {
        my ($t1, $t2, $sc) = @{$row};
        # Ignore zero scores
        next unless ($sc);
        my ($wsid, $sTyp) = $sc == 1 ?
            ($symId,"Official Symbol") : ($aliId,"Unofficial Symbol");
        my $targ = $objs->{$t1};
        unless ($targ) {
            $self->errMsg("Unknown gene '$t1' found while capturing symbols");
            next;
        }
        push @{$targ->{$sTyp}}, $t2;
        # Do not load symbols that split into two or more words:
        my @words = $self->split_words($t2, $splitter);
        next unless ($#words == 0);
        push @{$targ->{hits}{$wsid}{$t2}}, 1;
    }
    $self->bench_end();
    $self->benchTime("   ", $objNum);
}

sub _add_probes {
    my $self = shift;
    my ($gData) = @_;
    $self->bench_start();
    my $objs   = $gData->{objs};
    my $objNum = $gData->{gnum};
    $self->msg("   ","Getting related objects for $objNum genes");

    my $apsId = $self->get_wordSource
        ("Affy Probe Sets",
         { desc => "Affymetrix probeset identifiers"},
         800, '[^A-Z0-9_\-\.]+', '[^A-Z0-9_\-]', );
    my $aps = $self->forked_convert
        ( -idlist    => $gData->{list}, -standardize => 1, -ns2 => 'APS',
          -nullscore => -1, -nonull => 1,
          -cols => "termin,termout,matched",
          -output => $self->_temp_file_name($gData->{base}, "Gene", 'APS'));

    foreach my $row (@{$aps}) {
        my ($t1, $t2, $sc) = @{$row};
        # Ignore zero scores
        next unless ($sc);
        my ($wsid, $sTyp) = ($apsId, "Affy Probe Sets");
        my $targ = $objs->{$t1};
        unless ($targ) {
            $self->errMsg("Unknown gene '$t1' found while capturing symbols");
            next;
        }
        push @{$targ->{$sTyp}}, $t2;
        push @{$targ->{hits}{$wsid}{$t2}}, 1;
    }
    $self->bench_end();
    $self->benchTime("   ", $objNum);
}

sub _add_orthologs {
    my $self = shift;
    my ($gData) = @_;
    $self->bench_start();
    my $objs   = $gData->{objs};
    my $objNum = $gData->{gnum};

    $self->msg("   ","Getting Orthologs for $objNum genes");
    my $capturedOrth = {
        'Bos taurus'             => 'Cow',
        'Caenorhabditis elegans' => 'Worm',
        'Canis lupus familiaris' => 'Dog',
        'Homo sapiens'           => 'Human',
        'Macaca mulatta'         => 'Rhesus',
        'Mus musculus'           => 'Mouse',
        'Pan troglodytes'        => 'Chimp',
        'Rattus norvegicus'      => 'Rat',
    };
    my $l2oArr = $self->forked_convert
        ( -idlist => $gData->{list}, -standardize => 1, -ns2 => 'ORTH', 
          -min => 0.3, -nonull => 1, -cols => 'termin,termout,taxa,score',
          -output => $self->_temp_file_name($gData->{base}, 'Gene', 'ORTH'));
    my %orths;
    foreach my $row (@{$l2oArr}) {
        my ($t1, $t2, $tax, $sc) = @{$row};
        if (my $name = $capturedOrth->{$tax || ""}) {
            push @{$orths{$t1}{$name}{$sc}}, $t2;
        }
    }
    while (my ($gene, $th) = each %orths) {
        while (my ($name, $sh) = each %{$th}) {
            my ($bestSc) = sort {$b <=> $a} keys %{$sh};
            $objs->{$gene}{"$name Ortholog"} = $sh->{$bestSc};
        }
    }
    $self->bench_end();
    $self->benchTime("   ", $objNum);
}

sub _add_related {
    my $self = shift;
    my ($gData, $limit) = @_;
    $self->bench_start();
    my $objs   = $gData->{objs};
    my $objNum = $gData->{gnum};
    my $ad     = $self->denorm();
    my $nnam   = $gData->{nnam};
    my $accId = $self->get_wordSource
        ("Accessions",
         { desc => "Canonical accessions as specified by source database"},
         1000, '[^A-Z0-9_\-\.]+', '[^A-Z0-9]', );

    # Capture pure integer for some namespaces:
    my $numId = $self->get_wordSource
        ("Integer Accessions",
         { desc => "Accessions with only the integer component"},
         50, '\s+', '', );
    # WONT ACTUALLY WORK: LL is the /primary/ namespace, and will not be
    # in the 'related' output
    my %getInts = map { $_ => 1 } qw(LL);

    my $rfile = $self->_get_related_file( $gData );
    $self->msg("Finding related accessions for $objNum genes");
    open(RFILE, "<$rfile") || $self->death
        ("Failed to read related file", $rfile, $!);
    while (<RFILE>) {
        s/[\n\r]+$//;
        my @dat  = split(/\t/);
        my $acc  = shift @dat;
        my $ns   = shift @dat;
        my $desc = shift @dat;
        next if ($ns eq 'SYM');
        foreach my $gdat (@dat) {
            if ($gdat =~ /(.+)\:([^\:]+)$/) {
                my ($gene, $sc) = ($1, $2);
                my $targ = $objs->{$gene};
                unless ($targ) {
                    $self->errMsg("Unknown gene '$gene' found via $acc");
                    next;
                }
                push @{$targ->{hits}{$accId}{$acc}}, $sc;
                $targ->{related}{$acc}{$sc} = 1;
                if ($getInts{$ns} && $acc =~ /^[^\d]*(\d+)[^\d]*$/) {
                    my $num = $1 + 0;
                    push @{$targ->{hits}{$numId}{$num}}, $sc;
                }
            }
        }
        my $aTarg = $objs->{$acc} ||= {
            Accession => $acc,
        };
        $aTarg->{Namespace}   ||= $nnam->{$ns} ||= $ad->namespace_name($ns);
        $aTarg->{Description} ||= $desc || "";
    }
    close RFILE;
    # We need to note the accessions of the genes themselves!
    map { push @{$_->{hits}{$accId}{$_->{Accession}}}, 1 } @{$gData->{gene}};
    $self->bench_end();
    $self->benchTime("   ", $objNum);    
}

sub _get_related_file {
    my $self = shift;
    my ($gData) = @_;
    my $file = $self->_temp_file_name($gData->{base}, 'AllRelated');
    unless ($self->use_existing_file($file)) {
        my $ad     = $self->denorm();
        my $objNum = $gData->{gnum};
        my %priNs = map { $_ => 1 } qw(LL RSR RSP);
        open(RFILE, ">$file") || $self->death
            ("Failed to write related file", $file, $!);
        foreach my $ns2 (qw(AL AR AP APS PH NRDB TRC ENSE ILMN)) {
            $self->bench_start();
            $self->msg("   ","Recovering ".$ad->namespace_name($ns2));
            my %recovered;
            my $l2oArr = $self->forked_convert
                ( -idlist => $gData->{list}, -standardize => 1, -ns2 => $ns2,
                  -nullscore => -1, -noself => 1,
                  -cols => "termin,termout,matched",
                  -output => $self->_temp_file_name
                  ($gData->{base}, 'Related','Gene', $ns2));
            foreach my $row (@{$l2oArr}) {
                my ($t1, $t2, $sc) = @{$row};
                # Note the input
                $recovered{$t1} ||= {};
                next unless ($sc && $t2);
                # Ignore gi entries
                next if ($t2 =~ /^gi/);
                $recovered{$t2}{$t1} = $sc
                    if (!$recovered{$t2}{$t1} ||
                        $recovered{$t2}{$t1} < $sc);
            }
            my @accs = sort keys %recovered;
            $self->bench_end();
            $self->benchTime("     ", $objNum);

            $self->bench_start("Description");
            my $anum = $#accs + 1;
            $self->msg("     ", "Getting descriptions for $anum accessions");
            my $dArr = $self->forked_convert
                ( -id => \@accs, -ns1 => $ns2, -mode => 'desc',
                  -cols => "termin,desc", -nonull => 1,
                  -output => $self->_temp_file_name
                  ($gData->{base}, 'Desc', $ns2));
            my %desc;
            foreach my $row (@{$dArr}) {
                my ($acc, $d) = @{$row};
                next unless ($acc && $d);
                $d =~ s/[\s\t]+/ /g;
                $desc{$acc} = $d;
            }
            $self->bench_end("Description");
            $self->benchTime("     ", $anum);
            
            $self->bench_start("Write");
            $self->msg("     ", "Adding to file");
            foreach my $acc (@accs) {
                my ($gns) = $ad->guess_namespace($acc, $ns2);
                # next if ($priNs{$gns});
                my @row = ($acc, $gns, $desc{$acc} || "");
                foreach my $gene (sort keys %{$recovered{$acc}}) {
                    push @row, join(':', $gene, $recovered{$acc}{$gene});
                }
                print RFILE join("\t", @row)."\n";
            }
            $self->bench_end("Write");
            $self->benchTime("     ", $anum);    
        }
        close RFILE;
    }
    return $file;
}

sub _add_descriptions {
    die "This should be captured automatically now";
    my $self = shift;
    my ($gData) = @_;
    $self->bench_start();
    my $objs = $gData->{objs};

    my @allObjs = sort { $a->{Accession} cmp $b->{Accession} } values %{$objs};
    my $objNum  = $#allObjs + 1;
    $self->msg("Getting descriptions for $objNum accessions");

    my @objIds;
    my $ad    = $self->denorm();
    my $ntok  = $gData->{ntok};
    foreach my $gDat (@allObjs) {
        my ($acc, $ns) = ($gDat->{Accession}, $gDat->{Namespace});
        if ($ns) {
            push @objIds, sprintf("#%s#%s", $ntok->{$ns} ||=
                                  $ad->namespace_token($ns), $acc );
        } else {
            push @objIds, $acc;
        }
    }
    my $descs = $self->forked_convert
        (  -id => \@objIds, -standardize => 1, -mode => 'desc',
           -cols => 'termin,desc', -keepnull => 1,
           -output => $self->_temp_file_name($gData->{base}, 'ObjectDesc'));
    foreach my $row (@{$descs}) {
        my ($acc, $desc) = @{$row};
        next unless ($desc);
        my $targ = $objs->{$acc};
        unless ($targ) {
            $self->errMsg("Unknown object '$acc' found with descriptions",
                          $desc);
            next;
        }
        $targ->{Description} = $desc;
    }
    $self->bench_end();
    $self->benchTime("   ", $objNum);
}

sub _load_words {
    my $self = shift;
    my ($gData) = @_;
    $self->bench_start();
    my $objs    = $gData->{objs};
    my $genes   = $gData->{gene};
    my $gsid    = $gData->{gsid};
    my $prog    = $gData->{prog};
    my $objNum  = $#{$genes} + 1;

    my ($accId,$accW)   = $self->get_wordSource("Accessions");
    my ($numId,$numW)   = $self->get_wordSource("Integer Accessions");
    my ($symId,$symW)   = $self->get_wordSource("Official Symbols");
    my ($aliId,$aliW)   = $self->get_wordSource("Unofficial Symbols");
    my ($txtId,$txtW)   = $self->get_wordSource
        ("Text",
         { desc => "Free text, such as found in descriptions"},
         1, '[^A-Z0-9_\-]+', '[^A-Z0-9]');
    my ($txSId,$txSW)   = $self->get_wordSource
        ("Text Fragments", 
         { desc => "Same sources as 'Text', but more aggressively split"},
         0.8, '[^A-Z0-9]+', '[^A-Z0-9]');

    my %id2wgt = ( $accId => $accW,
                   $symId => $symW,
                   $numId => $numW,
                   $aliId => $aliW,
                   $txtId => $txtW,
                   $txSId => $txSW );
    my $clearWords = $self->dbh->named_sth
        ("Clear word hits for Gene and WordSource");
    my @clearIDs = ($accId, $symId, $aliId, $txtId, $txSId);
    $self->msg("Loading words $objNum words");
    my $ti    = time;
    my $num   = 0;
    my (%worderrs, %errs);
    foreach my $gDat (@{$genes}) {
        my $acc  = $gDat->{Accession};
        # Gene IDs should have been set via _set_parameters() first
        my $gid  = $self->get_gene( $acc, $gsid );
        map { $clearWords->execute( $gid, $_ ) } @clearIDs;
        my $hits = $gDat->{hits};
        while (my ($wsid, $accH) = each %{$hits}) {
            while (my ($acc, $scores) = each %{$accH}) {
                my ($sc) = sort { $b <=> $a } @{$scores};
                # Very small value for undefined scores:
                $sc = $smallScore if ($sc < $smallScore);
                $sc *= $id2wgt{$wsid};
                my $num = $self->set_word_hits
                    ( -geneid   => $gid, 
                      -sourceid => $wsid,
                      -text     => $acc,
                      -score    => $sc );
                my $hn = $num ? $#{$num} + 1 : '-undef-';
                $worderrs{"$acc : $hn hits recorded"}++ unless ($hn eq '1');
            }
        }
        # Make a big bag of words from all the associated IDs for this gene
        my $descs = $self->_extract_descriptions( $gDat, $objs, \%errs );

        # TO DO
        # We need to scale text scores according to the confidence of
        # the source object to the query.

        # Add accessions to free text as well
       # push @{$descs{1}}, $acc;
       # while (my ($acc, $scs) = each %{$hits->{$accId}}) {
       #     my ($sc) = sort { $b <=> $a } @{$scs};
       #     $sc = $smallScore if ($sc < $smallScore);
       #     push @{$descs{$sc}}, $acc;
       # }
        my @exclude;
        foreach my $sc (sort { $b <=> $a } keys %{$descs}) {
            my $descText = join(' ', @{$descs->{$sc}});
            my $textIds = $self->set_word_hits
                ( -geneid   => $gid, 
                  -sourceid => $txtId,
                  -exclude  => \@exclude,
                  -text     => $descText,
                  -score    => $sc * $txtW,
                  -clear    => 0 );
            push @exclude, @{$textIds || []};
            my $smallIds = $self->set_word_hits
                ( -geneid   => $gid, 
                  -sourceid => $txSId,
                  -text     => $descText,
                  -exclude  => \@exclude,
                  -score    => $sc * $txSW,
                  -clear    => 0 );
            push @exclude, @{$smallIds || []};
        }
        $num++;
        if ($prog && (time - $ti >= $prog)) {
            $ti = time;
            my $par = $gDat->{Parent};
            my $what = $par ? "$acc via $par" : $acc;
            $self->msg("[W]", sprintf("%s %.1f%%", $what,
                                      100 * $num/$objNum));
        }
    }
    foreach my $we (sort keys %worderrs) {
        my $num = $worderrs{$we};
        $we .= " [$num]" unless ($num eq "1");
        push @{$errs{"Failed to set unique accession word"}}, $we;
    }
    foreach my $err (sort keys %errs) {
        $self->errMsg($err, @{$errs{$err}});
    }
    $self->bench_end();
    $self->benchTime("   ", $objNum);
}

sub _extract_descriptions {
    my $self = shift;
    my ($gDat, $objs, $errs) = @_;
    my %descs;
    if (my $desc = $gDat->{Description}) {
        # The description directly associated with the gene
        push @{$descs{1}}, $desc;
    }
    while (my ($rel, $scs) = each %{$gDat->{related}}) {
        if (my $rDat = $objs->{$rel}) {
            if (my $desc = $rDat->{Description}) {
                my ($sc) = sort { $b <=> $a } keys %{$scs};
                $sc = $smallScore if ($sc < $smallScore);
                push @{$descs{$sc}}, $desc;
            }
        } elsif ($errs) {
            push @{$errs->{"Failed to recover related accession"}},
            "$rel for ". $gDat->{Accession};
        }
    }
    return \%descs;
}

sub _set_parameters {
    my $self = shift;
    my ($gData) = @_;
    $self->bench_start();

    my $objs    = $gData->{objs};
    my $genes   = $gData->{gene};
    my $gsid    = $gData->{gsid};
    my $prog    = $gData->{prog};
    my @allObjs = sort { $a->{Accession} cmp $b->{Accession} } values %{$objs};
    my $objNum  = $#allObjs + 1;
    my $geneNum = $#{$genes} + 1;

    # Find prior genes in the database:
    my $priGene = $self->all_genes( -genesource => $gsid );
    my @clearP  = qw(related hits kids);
    my $ti      = time;
    my $num     = 0;
    $self->msg("Creating $geneNum Gene entries");
    # Do formal genes first
    foreach my $gDat (@{$genes}) {
        my $acc    = $gDat->{Accession};
        my %params = %{$gDat};
        map { delete $params{$_} } @clearP;
        my $gid    = $self->get_gene( $acc, $gsid, $gDat->{Parent}, \%params );
        $num++;
        if ($prog && (time - $ti >= $prog)) {
            $ti = time;
            my $par = $gDat->{Parent};
            my $what = $par ? "$acc via $par" : $acc;
            $self->msg("[G]", sprintf("%s %.1f%%", $what,
                                      100 * $num/$geneNum));
        }
    }

    $self->msg("Setting metadata for $objNum accessions");
    # Now do all accessions generically
    $num = 0;
    foreach my $gDat (@allObjs) {
        my $acc    = $gDat->{Accession};
        my $tid    = $self->txt_id( $acc );
        my %params = %{$gDat};
        map { delete $params{$_} } @clearP;
        $self->set_keyval($tid, \%params, 1);
        $num++;
        if ($prog && (time - $ti >= $prog)) {
            $ti = time;
            $self->msg("[A]", sprintf("%s %.1f%%", $acc,
                                      100 * $num/$objNum));
        }
    }

    $self->bench_end();
    $self->benchTime("   ", $objNum);
}

sub _set_children {
    my $self = shift;
    my ($gData) = @_;
    $self->bench_start();
    my $genes   = $gData->{gene};
    my $prog    = $gData->{prog};
    my $objs    = $gData->{objs};
    my $objNum  = $#{$genes} + 1;
    my $gsid    = $gData->{gsid};
    my $clear   = $self->dbh->prepare
        ( -name => "Clear all children for parent",
          -sql  => "DELETE FROM grp WHERE par_id = ?",
          -level => 1);
    # Clearing kids is needed in case an entry now has a different parent:
    my $clKid   = $self->dbh->prepare
        ( -name => "Clear all entries for child",
          -sql  => "DELETE FROM grp WHERE obj_id = ?",
          -level => 1);
    my $set = $self->dbh->prepare
        ( -name => "Add child for a parent",
          -sql  => "INSERT INTO grp (obj_id, par_id, obj_type, par_type) VALUES (?, ?, ?, ?)",
          -level => 1);
    $self->msg("Setting parentage structure for $objNum genes");
    my $ti      = time;
    my $num     = 0;
    foreach my $gDat (@{$genes}) {
        my @stack = ($gDat);
        my %seen;
        while ($#stack != -1) {
            my $obj = shift @stack;
            my $onm = $obj->{Accession};
            my $oid = $self->txt_id( $onm );
            my $oty = $obj->{Type};
            # Clear any old parentage
            $clear->execute($oid);
            my @kids = sort keys %{$obj->{kids} || {}};
            if ($#kids == -1) {
                # No children for this object. Put a blanked row in
                $set->execute( undef, $oid, undef, $oty );
            } else {
                foreach my $knm (@kids) {
                    my $kobj = $objs->{$knm};
                    push @stack, $kobj;
                    my $kid = $self->txt_id( $knm );
                    my $kty = $kobj->{Type};
                    $clKid->execute( $kid );
                    $set->execute( $kid, $oid, $kty, $oty );
                    $kobj->{Parent} = $onm;
                }
            }
        }
        $num++;
        if ($prog && (time - $ti >= $prog)) {
            $ti = time;
            my @kids = sort keys %{$gDat->{kids} || {}};
            $self->msg("[K]", sprintf("%s -> %d kids %.1f%%",
                                      $gDat->{Accession}, $#kids+1,
                                      100 * $num/$objNum));
        }
    }
    $self->bench_end();
    $self->benchTime("   ", $objNum);    
}

sub clobber {
    my $self = shift;
    my $nv   = shift;
    if (defined $nv) { $self->{CLOBBER} = $nv ? 1 : 0; }
    return $self->{CLOBBER};
}

sub use_existing_file {
    my $self = shift;
    my ($file, $clobber) = @_;
    # Can not use the file if it is not defined or does not exist:
    return 0 unless ($file && -e $file);
    # Never use the file if clobber is specified:
    $clobber = $self->clobber() unless (defined $clobber);
    # die "Hey!" if ($clobber);
    return 0 if ($clobber);
    # The file exists, see how old it is
    my $age = $self->{AGE} ? $self->denorm->standardize_age($self->{AGE}) : 0;
    # Return the path with no further action if no age is defined
    # or if the modified time on the file is less than the age
    return 1 if (!$age || -M $file <= $age);
    # There are a variety of reasons why this is imperfect, but
    # it should be good for most cases
    return 0;
}

sub _temp_file_name {
    my $self     = shift;
    my $baseName = shift;
    my @txt      = @_;
    my $ad       = $self->denorm();
    my @bits;
    foreach my $bit (@txt) {
        if (my $tok = $ad->namespace_token($bit)) {
            push @bits, $tok;
        } else {
            $bit =~ s/[^A-Z0-9]+/_/gi;
            push @bits, $bit;
        }
    }
    return join("-", $baseName, @bits).".tsv";
}

sub blacklist {
    my $self = shift;
    my $args  = $self->parseparams( -number => 100,
                                    @_ );
    my (@gss, @wss, @human);
    if (my $treq = $args->{TAXA} || $args->{SPECIES}) {
        my ($gsid, $nm) = $self->geneset_for_taxa( $treq );
        if ($gsid) {
            @gss = ($gsid);
            push @human, "$nm [$gsid]";
        } else {
            $self->err("Failed to find GeneSource for taxa '$treq'");
            return;
        }
    } elsif (my $gReq = $args->{GENESOURCE}) {
        if (my $gsid = $self->get_geneSource($gReq)) {
            @gss = ($gsid);
            my $obj = $self->genesource_object($gsid);
            push @human, $obj->name()." [$gsid]";
        } else {
            $self->err("Failed to find GeneSource '$gReq'");
            return;
        }
    } else {
        my $allGss = $self->all_gene_sources();
        @gss = map { $_->{id} } @{$allGss};
    }
    if (my $wReq = $args->{WSID} || $args->{WORDSOURCE}) {
        if (my $wsid = $self->get_wordSource($wReq)) {
            @wss = ($wsid);
            my $obj = $self->wordsource_object($wsid);
            push @human, $obj->name()." [$wsid]";
        } else {
            $self->err("Failed to find WordSource '$wReq'");
            return;
        }
    }
    my $force    = $args->{FORCE};
    my $maskFrac = $args->{FRACTION} || $args->{FRAC};
    my $maskNum  = $args->{NUMBER};
    my $sql = "SELECT wh.word_id, wh.ws_id, count(wh.gene_id) ".
        "FROM word_hit wh, gene g ".
        "WHERE g.gene_id = wh.gene_id AND g.gs_id = ?".
        ($#wss == -1 ? '' : " AND wh.ws_id IN (".join(', ', @wss).") ");
    my $having = "";
    if ($force) {
        # We are masking user-specified words
        my %wids;
        my @reqs = ref($force) ? @{$force} : ($force);
        # Should maybe make the splitting wordsource dependent...
        foreach my $word (map { $self->split_words( $_ ) } @reqs) {
            my $wid = $self->single_word_id( $word );
            $wids{$wid}++;
        }
        delete $wids{0};
        my @u = sort {$a <=> $b} keys %wids;
        if ($#u == -1) {
            $self->death("Failed to force blacklist, no word IDs recovered",
                         "-force => ".(join(',', @reqs) || $force));
        } else {
            $sql .= " AND wh.word_id IN (".join(',', @u).")";
        }
    } elsif ($maskFrac || $maskNum) {
        # Masking based on frequency of occurence
        # We can not simultaneously do both
        $having = " HAVING count(wh.gene_id) > ?";
    } else {
        $self->death("Unable to calculate blacklist","No criteria provided");
    }
    $sql .= " GROUP BY wh.word_id, wh.ws_id$having";


    my $sth = $self->dbh->prepare
        ( -name => "Find highly represented words",
          -sql  => $sql,
          -limit => $args->{LIMIT},
          -level => 1);
    my $clear = $self->dbh->prepare
        ( -name => "Clear old blacklist",
          -sql  => "DELETE FROM blacklist ".
          "WHERE word_id = ? AND ws_id = ? AND gs_id = ?",
          -level => 3);
    my $add = $self->dbh->prepare
        ( -name => "Add blacklist row",
          -sql  => "INSERT INTO blacklist ".
          "(word_id, ws_id, gs_id, freq) VALUES (?,?,?,?)",
          -ignore => $self->_ignore_dup(),
          -level => 3);
    my $isTrial = $args->{TRIAL} || $args->{ISTRIAL};
    my $dumpSql = $isTrial || $args->{DUMPSQL};
    foreach my $gsid (@gss) {
        my @allGenes = $self->all_genes( -genesource => $gsid );
        my $numGenes = $#allGenes + 1;
        next unless ($numGenes);
        my @binds = ($gsid);
        unless ($force) {
            my $lim = int($numGenes * $maskFrac) if ($maskFrac);
            $lim    = $maskNum if ($maskNum && (!$lim || $lim > $maskNum));
            push @binds, $lim;
        }
        warn $sth->pretty_print(@binds) if ($dumpSql);
        next if ($isTrial);
        $sth->execute(@binds);
        my $rows = $sth->fetchall_arrayref();
        foreach my $row (@{$rows}) {
            my ($wid, $wsid, $cnt) = @{$row};
            $clear->execute( $wid, $wsid, $gsid );
            $add->execute( $wid, $wsid, $gsid, $cnt / $numGenes);
        }
        my $gsobj = $self->object_for_gsid( $gsid);
        $self->msg("Blacklist for ".join(', ', @human)." : ".
                   scalar(@{$rows}). " ids");
    }
}

sub forked_convert {
    my $self = shift;
    my $gas = BMS::MapTracker::GenAccService->new
        ( -fork    => $self->{FORKNUM} || 0,
          @_,
          -format  => 'tsv',
          -age      => $self->{AGE},
          -cloudage => $self->{CLOUDAGE},
          -verbose  => 0,
          -orastart => $self->denorm->oracle_start(),
          -scramble => 1 );
    my $ids = $gas->val(qw(id ids idlist));
    return [] unless ($ids);
    # warn $ids;
    $gas->use_beta( 1 );
    my $rows = $gas->cached_array(  );
    return $rows;
}

sub gene {
    my $self = shift;
    my ($name, $params) = @_;
    return 0 unless ($name);
    $self->death("Not implemented. What am I doing here??");
}

sub get_wordSource {
    my $self   = shift;
    my $name   = shift;
    my $params = shift;
    my @rows   = $self->SUPER::get_wordSource( $name );
    if ($name && !$rows[0]) {
        my $dbh   = $self->dbh();
        if ($name =~ /^\d+$/) {
            $self->death
                ("Can not define a new word source as integer '$name'");
        } else {
            my ($weight, $spChar, $stChar) = @_;
            unless ($weight) {
                $self->err("Can not add WordSource '$name' without a weight");
                return wantarray ? () : undef;
            }
            $spChar ||= '[^A-Z0-9_\-\.:]';
            $stChar ||= '[^A-Z0-9]';
            my $seq = $self->nextval('main_seq');
            my $add = $self->{STHS}{ADD_WS} ||= $dbh->prepare
                ( -name => "Create a new wordSource",
                  -sql  => "INSERT INTO word_source (ws_id, name, weight, splitchar, stripchar) VALUES (?,?,?,?,?)",
                  -ignore => $self->_ignore_dup(),
                  -level => 3);
            $add->execute( $seq, $name, $weight, $spChar, $stChar );
            @rows = $self->SUPER::get_wordSource( $name );
        }
    }
    $self->set_keyval($rows[0], $params, 1) if ($params);
    return wantarray ? @rows : $rows[0];
}

sub set_word_hits {
    my $self = shift;
    my $args  = $self->parseparams( -score => 1,
                                    @_ );
    my $gid   = $args->{GENEID};
    my $wsid  = $args->{SOURCEID};
    my $text  = $args->{TEXT};
    return undef unless ($gid && $wsid && $text);
    my ($wsid2, $wgt, $splitchar, $stripchar) = $self->get_wordSource($wsid);
    my $sc    = $args->{SCORE};
    my $clear = $args->{CLEAR};
    my $excl  = $args->{EXCLUDE};
    my $patt  = $args->{PATTERN};
    my $strp  = $args->{STRIP};
    $patt   ||= $splitchar;
    $strp   ||= $stripchar;
    my $dbh   = $self->dbh();
    $dbh->named_sth("Clear word hits for Gene and WordSource")->
        execute($gid, $wsid) if ($clear);
    my @wids = $self->wordids( $text, $patt, $strp );
    if ($excl && $#{$excl} != -1) {
        # We want to exclude some word IDs. This is generally done to leave
        # out IDs previously assigned by a 'better' word source or with
        # a better score
        my %hash = map { $_ => undef } @wids;
        map { delete $hash{$_} } @{$excl};
        @wids = sort { $a <=> $b } keys %hash;
    }
    unless ($#wids == -1) {
        my $add = $self->{STHS}{ADD_WORDHIT} ||= $dbh->prepare
            ( -name => "Add a WordHit",
              -sql  => "INSERT INTO word_hit (word_id, ws_id, gene_id, score)".
              " VALUES (?,?,?,?)",
              -ignore => $self->_ignore_dup(),
              -level => 3);
        map { $add->execute($_, $wsid, $gid, $sc) } @wids;
    }
    return \@wids;
}




sub activate_trace {
    my $self = shift;
    my $dbh  = $self->dbh();
    foreach my $sql
        ("alter session set timed_statistics=true",
         "alter session set max_dump_file_size=unlimited",
         "alter session set tracefile_identifier='CHARLES'",
         "alter session set events '10046 trace name context forever, level 8'") {
            $dbh->execute($sql);
        }
}

sub deactivate_trace {
    my $self = shift;
    $self->dbh->execute
        ("alter session set events '10046 trace name context off'");
}

sub mainseq { return shift->nextval('main_seq'); }

sub denorm {
    my $self = shift;
    return $self->{DENORM} ||=
        BMS::MapTracker::AccessDenorm->new
            ( -age      => $self->{AGE},
              -cloudage => $self->{CLOUDAGE} );
}

sub tracker {
    return shift->denorm->tracker();
}

*dbi = \&dbh;
sub dbh {
    return shift->{DBH};
}

sub set_schema {
    my $self = shift;
    return $self->{SCHEMA} if ($self->{SCHEMA});

    my %tables;

    $tables{ "word" } =
    { name  => 'word',
      com   => 'Normalized list of words. All words should be upper case, and will reference a word_id',
      ignore => { insert => 'ORA-00001' },
      checklength => [ 'word' ],
      sequence => { 
          'main_seq' => 1,
      },
      pkey  => 'word_id',
      index => {
          word_word   => {
              cols => [ 'word' ],
              unique => 1,
          },
      },
      cols  => 
          [['word_id', 'integer',
            'Integer primary key for the word.' ],

           ['word', 'varchar(100)',
            'A single word, all uppercase' ],
           ] };

    $tables{ "keyval" } =
    { name  => 'keyval',
      com   => 'Key-Value pairs for any object referenced by a primary ID',
      checklength => [ 'key','value' ],
      sequence => { 
          'main_seq' => 1,
      },
      index => {
          kv_primary   => {
              cols => [ 'kv_id' ],
          },
          kv_key   => {
              cols => [ 'key_id' ],
          },
          kv_key_and_val   => {
              cols => [ 'key_id', 'val_id' ],
          },
      },
      fkey  => {
          key_id => 'normtxt.txt_id',
          val_id => 'normtxt.txt_id',
      },
      cols  => 
          [['kv_id', 'integer',
            'Integer foreign key for the object being annotated.' ],

           ['key_id', 'integer',
            'Keyname associated with the annotation, points to NORMTXT' ],

           ['val_id', 'integer',
            'Value associated with the annotation, points to NORMTXT' ],
           ] };

    $tables{ "normtxt" } =
    { name  => 'normtxt',
      com   => 'Table holding normalized text. Case sensitive and allows spaces',
      checklength => $self->usePg ? undef : [ 'txt' ],
      sequence => { 
          'main_seq' => 1,
      },
      index => {
          nt_txt => {
              cols => [ 'txt' ],
              unique => 1,
          },
          txt_up   => {
              cols => [ 'upper(txt)' ],
          },
      },
      pkey  => 'txt_id',
      cols  => 
          [['txt_id', 'integer',
            'Integer primary key for the text object.' ],

           ['txt', $self->usePg ? 'text' : 'varchar(4000)',
            'Any string' ],
           ] };

    $tables{ "word_source" } =
    { name  => 'word_source',
      com   => 'Description of sources of words related to the genes',
      checklength => [ 'name' ],
      sequence => { 
          'main_seq' => 1,
      },
      index => {
          ws_name   => {
              cols => [ 'upper(name)' ],
          },
      },
      pkey  => 'ws_id',
      cols  => 
          [['ws_id', 'integer',
            'Primary key for the source.' ],

           ['name', 'varchar(100)',
            'Brief name describing the source' ],

           ['weight', 'number',
            'A relative weight (>= 0) to assign to words from this source' ],

           ['splitchar', 'text',
            'The regular expression that will be used to split text from this source into individual words' ],
           ['stripchar', 'text',
            'Optional regular expression that defines characters to be removed from words. Applied after splitting.' ],
           ] };

    $tables{ "gene_source" } =
    { name  => 'gene_source',
      com   => 'Description of sources for the genes',
      checklength => [ 'name' ],
      sequence => { 
          'main_seq' => 1,
      },
      index => {
          gs_name   => {
              cols => [ 'upper(name)' ],
              unique => 1,
          },
      },
      pkey => 'gs_id',
      cols  => 
          [['gs_id', 'integer',
            'Primary key for the source.' ],

           ['name', 'varchar(200)',
            'Brief name describing the source' ],

           ] };

    $tables{ "blacklist" } =
    { name  => 'blacklist',
      com   => 'A list of frequently seen words to ignore',
      index => {
          bl_primary   => {
              cols => [ 'word_id', 'ws_id', 'gs_id' ],
              unique => 1,
          },
      },
      fkey  => {
          word_id => 'word.word_id',
          ws_id   => 'word_source.ws_id',
          gs_id   => 'gene_source.gs_id',
      },
      cols  => 
          [['word_id', 'integer',
            'Foreign key for the word being blacklisted.' ],

           ['ws_id', 'integer',
            'Foreign key for the word source.' ],

           ['gs_id', 'integer',
            'Foreign key for the gene source.' ],

           ['freq', 'number',
            'The fraction of genes having this word assigned to them' ],
           ] };


    $tables{ "gene" } =
    { name  => 'gene',
      com   => 'List of genes, and gene objects',
      ignore => { insert => 'ORA-00001' },
      checklength => [ 'acc','sym','desc' ],
      sequence => { 
          'main_seq' => 1,
      },
      index => {
          gene_acc   => {
              cols => [ 'acc' ],
              unique => 1,
          },
          gene_acc_upper   => {
              cols => [ 'upper(acc)' ],
          },
          gene_par_key   => {
              cols => [ 'par_id' ],
          },
          gene_by_source => {
              cols => ['gs_id'],
          },
      },
      pkey  => 'gene_id',
      fkey  => {
          par_id  => 'gene.gene_id',
          gs_id   => 'gene_source.gs_id',
      },
      cols  => 
          [['gene_id', 'integer',
            'Integer primary key for the gene.' ],

           ['acc', 'varchar(100)',
            'The accession for this gene object' ],

           ['par_id', 'integer',
            'An optional parent gene object, represented by the gene_id' ],

           ['gs_id', 'integer',
            'Foreign key to the gene source set' ],

           ] };

    $tables{ "grp" } =
    { name  => 'grp',
      com   => 'Parentage table associating genes, RNAs and proteins to each other',
      index => {
          grp_primary   => {
              cols   => [ 'obj_id' ],
              unique => 1,
          },
          grp_secondary   => {
              cols   => [ 'par_id' ],
          },
      },
      fkey  => {
          obj_id  => 'normtxt.txt_id',
          par_id  => 'normtxt.txt_id',
      },
      cols  => 
          [['obj_id', 'integer',
            'Foreign key for the primary object' ],

           ['par_id', 'integer',
            'Foreign key for the parent object, if any' ],

           ['obj_type', 'varchar(10)',
            'Type of primary object (gene, rna or protein)' ],

           ['par_type', 'varchar(10)',
            'Type of parent object (gene, rna or protein)' ],

           ] };
    
    $tables{ "word_hit" } =
    { name  => 'word_hit',
      com   => 'Association of a word from a particular source to a gene',
      index => {
          wh_primary   => {
              cols   => [ 'word_id', 'ws_id', 'gene_id' ],
              unique => 1,
          },
          wh_gene   => {
              cols   => [ 'gene_id', 'ws_id' ],
          },
          wh_gene_word   => {
              cols   => [ 'gene_id', 'word_id' ],
          },
      },
      fkey  => {
          word_id => 'word.word_id',
          ws_id   => 'word_source.ws_id',
          gene_id => 'gene.gene_id',
      },
      cols  => 
          [['word_id', 'integer',
            'Foreign key for the word' ],

           ['ws_id', 'integer',
            'Foreign key for the word source' ],

           ['gene_id', 'integer',
            'Foreign key for the gene' ],

           ['score', 'number',
            'Arbitrary score for the match' ],
           ] };
    

    $tables{ "manual_task" } =
    { name  => 'manual_task',
      com   => 'Set of IDs that need to be hand-curated',
      checklength => [ 'name' ],
      sequence => { 
          'main_seq' => 1,
      },
      index => {
          mt_name   => {
              cols => [ 'upper(name)' ],
              unique => 1,
          },
      },
      pkey => 'task_id',
      cols  => 
          [['task_id', 'integer',
            'Primary key for the task.' ],

           ['name', 'varchar(200)',
            'Brief name describing the task' ],

           ] };

    $tables{ "task_member" } =
    { name  => 'task_member',
      com   => 'List of objects that are assigned to a task',
      checklength => [ 'external_id' ],
      sequence => { 
          'main_seq' => 1,
      },
      index => {
          tm_xid   => {
              cols => [ 'external_id', 'task_id' ],
              unique => 1,
          },
          tm_taskid  => {
              cols => [ 'task_id' ],
          },
      },
      pkey => 'member_id',
      cols  => 
          [['task_id', 'integer',
            'Foregin key from manual_task' ],

           ['member_id', 'integer',
            'Primary key for the task member' ],

           ['external_id', 'varchar(200)',
            'The ID of the task as it is known in the external system' ],

           ] };

    $tables{ "task_assign" } =
    { name  => 'task_assign',
      com   => 'Gene assignments to a task member',
      sequence => { 
          'main_seq' => 1,
      },
      index => {
          ta_acc   => {
              cols => [ "upper(acc)", "auth", 'member_id' ],
              unique => 1,
          },
          ta_auth => {
              cols => [ 'auth' ],
          },
      },
      pkey => 'assign_id',
      cols  => 
          [['assign_id', 'integer',
            'Primary key for the task member' ],

           ['member_id', 'integer',
            'Foregin key from task_member' ],

           ['acc', 'varchar(100)',
            'The assigned gene accession' ],

           ['auth', 'varchar(100)',
            'The authority making the assignment' ],

            ['conf', 'integer',
             'A confidence score, 0-100. Negative values are auto-assigned' ],

          ] };

   $tables{ "v_hit" } =
    { name  => 'v_hit',
      com   => 'Denormalized view of Word:Gene hits',
      view  =>
"
 SELECT w.word, g.acc AS gene, gs.name AS GeneSource, ws.name AS WordSource, 
        wh.score, wh.score / ws.weight AS rawscore
   FROM word w, gene g, word_source ws, gene_source gs, word_hit wh
  WHERE w.word_id = wh.word_id
    AND g.gene_id = wh.gene_id
    AND ws.ws_id = wh.ws_id
    AND gs.gs_id = g.gs_id;
"
};

    $tables{ "v_grp" } =
    { name  => 'v_grp',
      com   => 'Denormalized view of parentage that adds accessions',
      view  =>
"
 SELECT n1.txt AS Object, g.obj_type, n2.txt AS Parent, g.par_type, g.obj_id, g.par_id
   FROM grp g, normtxt n1, normtxt n2
  WHERE n1.txt_id = g.obj_id
    AND n2.txt_id = g.par_id
"
};

    $tables{ "v_kv" } =
    { name  => 'v_kv',
      com   => 'Denormalized view of Key-Value data',
      view  =>
"
SELECT 'Gene' as Type, o.acc AS Object, kv.kv_id AS OID,
       tk.txt AS Key, tv.txt AS Value
 FROM keyval kv, normtxt tk, normtxt tv, gene o
WHERE o.gene_id = kv.kv_id
  AND tk.txt_id = kv.key_id
  AND tv.txt_id = kv.val_id
UNION
SELECT 'Word' as Type, o.word AS Object, kv.kv_id AS OID,
       tk.txt AS Key, tv.txt AS Value
 FROM keyval kv, normtxt tk, normtxt tv, word o
WHERE o.word_id = kv.kv_id
  AND tk.txt_id = kv.key_id
  AND tv.txt_id = kv.val_id
UNION
SELECT 'WordSource' as Type, o.name AS Object, kv.kv_id AS OID,
       tk.txt AS Key, tv.txt AS Value
 FROM keyval kv, normtxt tk, normtxt tv, word_source o
WHERE o.ws_id = kv.kv_id
  AND tk.txt_id = kv.key_id
  AND tv.txt_id = kv.val_id
UNION
SELECT 'GeneSource' as Type, o.name AS Object, kv.kv_id AS OID,
       tk.txt AS Key, tv.txt AS Value
 FROM keyval kv, normtxt tk, normtxt tv, gene_source o
WHERE o.gs_id = kv.kv_id
  AND tk.txt_id = kv.key_id
  AND tv.txt_id = kv.val_id
UNION
SELECT 'Text' as Type, o.txt AS Object, kv.kv_id AS OID,
       tk.txt AS Key, tv.txt AS Value
 FROM keyval kv, normtxt o, normtxt tk, normtxt tv
WHERE o.txt_id = kv.kv_id
  AND tk.txt_id = kv.key_id
  AND tv.txt_id = kv.val_id
"
};

    $tables{ "queries" } =
    { name  => 'queries',
      com   => 'Shows Postgres SQL statements currently running for ALL databases',
      db    => 'postgres',
      view  =>
"
 SELECT pg_stat_activity.datname, pg_stat_activity.usename, date_trunc('second'::text, now() - pg_stat_activity.query_start) AS query_age, date_trunc('second'::text, now() - pg_stat_activity.backend_start) AS backend_age, btrim(pg_stat_activity.current_query) AS current_query
   FROM pg_stat_activity
  WHERE pg_stat_activity.current_query <> '<IDLE>'::text
  ORDER BY date_trunc('second'::text, now() - pg_stat_activity.query_start), date_trunc('second'::text, now() - pg_stat_activity.backend_start)
"
};

    $tables{ "sgq" } =
    { name  => 'sgq',
      com   => 'Shows Postgres SQL statements currently running for this DB.',
      db    => 'postgres',
      view  =>
"
 SELECT date_trunc('second'::text, now() - pg_stat_activity.query_start) AS query_age, date_trunc('second'::text, now() - pg_stat_activity.backend_start) AS backend_age, pg_stat_activity.procpid AS pid, substring(btrim(pg_stat_activity.current_query), 1, 150) AS current_query
   FROM pg_stat_activity
  WHERE pg_stat_activity.current_query <> '<IDLE>'::text AND pg_stat_activity.datname ~~ 'stndgene%'::text AND upper(pg_stat_activity.current_query) !~~ '% FROM STNDGENE%'::text
  ORDER BY date_trunc('second'::text, now() - pg_stat_activity.query_start), date_trunc('second'::text, now() - pg_stat_activity.backend_start)
"
};
    
    $tables{ "v_xid" } =
    { name  => 'v_xid',
      com   => 'Shows state of transaction IDs in Postgres',
      db    => 'postgres',
      view  =>
"
 SELECT c.relname, s.usename, c.relpages::double precision / 1000::double precision AS kilopages, floor(c.reltuples / 1000::double precision) AS kilotuples, age(c.relfrozenxid)::double precision / 1000000::double precision AS mega_xid
   FROM pg_class c, pg_namespace ns, pg_shadow s
  WHERE ns.oid = c.relnamespace AND ns.nspname = 'public'
    AND c.relkind = 'r' AND s.usesysid = c.relowner
  ORDER BY c.reltuples DESC
"
};

    $tables{ "v_black" } =
    { name  => 'v_black',
      com   => 'Human readable blacklist',
      view  =>
"SELECT w.word as word, ws.name as wordsource, gs.name as genesource, bl.freq
   FROM word w, word_source ws, gene_source gs, blacklist bl
  WHERE w.word_id = bl.word_id
    AND ws.ws_id = bl.ws_id
    AND gs.gs_id = bl.gs_id"
};


    $self->{SCHEMA} = \%tables;
    $self->dbh->schema(\%tables);
    return $self->{SCHEMA};
}

sub _set_basic {
    my $self = shift;
    my $dbh  = $self->dbh();
    $dbh->prepare( -name => "Create null wordsource",
                   -sql  => "INSERT INTO word_source (ws_id, name, weight)".
                   " VALUES (?,?,?)",
                   -ignore => $self->_ignore_dup(),
                   -level => 3)->execute
                   ( 0, "Undefined word source", 0 );
    $dbh->prepare( -name => "Create null genesource",
                   -sql  => "INSERT INTO gene_source (gs_id, name)".
                   " VALUES (?,?)",
                   -ignore => $self->_ignore_dup(),
                   -level => 3)->execute
                   ( 0, "Undefined gene source" );
}

sub standard_sths {
    my $self = shift;
    my $dbh  = $self->dbh;
    #$dbh->update_lock_method( \&weak_lock );
    #$dbh->update_unlock_method( \&clear_lock );

    my $dfm = $dbh->date_format;
    my $toCache =
        [

         ["Get Text ID", 4,
          "SELECT txt_id FROM normtxt WHERE txt = ?"],

         ["Get Gene by Name", 2,
          "SELECT gene_id, acc, par_id, gs_id FROM gene WHERE upper(acc) = upper(?)"],

         ["Get Gene by Name and Source", 2,
          "SELECT gene_id FROM gene WHERE upper(acc) = upper(?) AND gs_id = ?"],
         ["Deprecate Gene", 2,
          "UPDATE gene SET gs_id = 0, par_id = 0 WHERE upper(acc) = upper(?) AND gs_id = ?"],

         ["Clear word hits for Gene and WordSource", 3,
          "DELETE FROM word_hit WHERE gene_id = ? and ws_id =?"],

         ["Clear word hits for Gene", 3,
          "DELETE FROM word_hit WHERE gene_id = ?"],
         ];

    foreach my $row (@{$toCache}) {
        my ($name, $level, $sql, $limit) = @{$row};
        next unless ($name && $sql);
        $dbh->note_named_sth( $name, $sql, $level);
    }
}

sub get_gene {
    my $self = shift;
    my ($name, $src, $par, $params) = @_;
    # warn join(" + ", map { defined $_ ? $_ : '-NULL-' } ($name, $src, $par))."\n";
    return wantarray ? () : 0 unless ($name);
    return $name if ($name =~ /^\d+$/);
    $name   = substr($name, 0, 100);
    my $dbh = $self->dbh();
    my $get = $dbh->named_sth("Get Gene by Name and Source");
    my $sid = $self->get_geneSource( $src );
    my $gid = $get->get_single_value( $name, $sid );
    if ($gid) {
        if (defined $par) {
            my $pid = $self->get_gene( $par, $src );
            my $upd = $self->{STHS}{UPDATE_GENE} ||= $dbh->prepare
                ( -name => "Update an existing gene",
                  -sql  => "UPDATE gene SET par_id = ?, gs_id = ? WHERE gene_id = ?",
                  -level => 3);
            $upd->execute( $pid || undef, $sid, $gid );
        }
    } else {
        my $pid = $self->get_gene( $par, $src );
        my $seq = $self->nextval('main_seq');
        my $add = $self->{STHS}{ADD_GENE} ||= $dbh->prepare
            ( -name => "Create a new gene",
              -sql  => "INSERT INTO gene (gene_id, acc, par_id, gs_id) VALUES (?,?,?,?)",
              -ignore => $self->_ignore_dup(),
              -level => 3);
        $add->execute( $seq, $name, $pid || undef, $sid );
        $gid  = $get->get_single_value( $name, $sid );
    }
    $self->set_keyval($gid, $params, 1) if ($params);
    return $gid || 0;
}


