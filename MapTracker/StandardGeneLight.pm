# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::StandardGeneLight;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use strict;
use BMS::Utilities::BmsDatabaseEnvironment;
use BMS::Utilities::Benchmark;
use BMS::FriendlyDBI;

use vars qw(@ISA);
@ISA      = qw(BMS::Utilities::BmsDatabaseEnvironment
               BMS::Utilities::Benchmark );

our $doNotAlterDB = 1;
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
    return $self;
}

our $res_set_counter = 0;
sub empty_result_set {
    my ($self, $note) = @_;
    my $rv = BMS::MapTracker::StandardGeneLight::Results->new( $self );
    $rv->{NEW_NOTE} = $note;
    $rv->{SPAWN_NUM} = ++$res_set_counter;
    return $rv;
}

our $complaints = {};
sub complain_once {
    my $self = shift;
    my $key  = join("\t", @_) || "";
    return unless ($key);
    unless ($complaints->{$key}++) {
        $self->err(@_);
    }
}

sub usePg { return 1; }

sub verbose {
    my $self = shift;
    my $nv   = shift;
    if (defined $nv) {
        if ($nv) {
            my $cb = ref($nv) ? $nv : $self->{OLD_VB};
            $self->msg_callback($cb);
        } else {
            $self->{OLD_VB} = $self->msg_callback();
            $self->msg_callback( 0 );
        }
        $self->{VB} = $nv ? 1 : 0;
    }
    return $self->{VB};
}

*dbi = \&dbh;
sub dbh {
    return shift->{DBH};
}

sub dbname { return shift->{DBNAME}; }

sub _ignore_dup { return ['duplicate key value','ORA-00001']; }

sub _connect {
    my $self = shift;
    my $args = $self->parseparams( @_ );
    my $dbName = $self->{DBNAME} = 
        $args->{DB} || $args->{DBNAME} || 'stndgene';
    $self->set_oracle_environment('minerva');
    $self->bench_start('DB Connect');
    my ($dbType, $dbn);
    if ($self->usePg) {
        $ENV{PGPORT} = 5433;
        $ENV{PGHOST} = 'elephant.pri.bms.com';
        $ENV{PGLIB}  = '/apps/sys/postgres/postgres/lib';

        $dbType = "dbi:Pg:dbname=$dbName";# ; host=$ENV{PGHOST}; port=$ENV{PGPORT}";
        $dbn    = 'tilfordc',
    } else {
        $dbType = "dbi:Oracle:";
        if (0) {
            $dbn    = 'limje/limje@SPANDX';
            $dbn    = 'stndgene/stndgene@LSFDEV';
        } else {
            $dbn    = 'stndgene/stndgene@'.($dbName || "LSPANP1");
        }
    }
    eval {
        $self->{DBH} = BMS::FriendlyDBI->connect
            ($dbType, $dbn,
             undef, { RaiseError  => 0,
                      PrintError  => 0,
                      LongReadLen => 100000,
                      AutoCommit  => 1, },
             -errorfile => '/scratch/StndGeneErrors.err',
             -noenv     => $args->{NOENV},
             -adminmail => 'tilfordc@bms.com', );
    };
    unless ($self->{DBH}) {
        $self->death("Failed to connect to StandardGene database '$dbName'");
    }
    $self->bench_end('DB Connect');
    # $self->set_schema();
}

sub split_words {
    my $self = shift;
    my $text  = shift;
    return () unless ($text);
    $text     =~ s/^\s+//;
    $text     =~ s/\s+$//;
    $text     =~ s/\P{IsASCII}//g;
    my $spRE  = shift || '[^A-Z0-9_\-\.:]+';
    my $stRE  = shift || '[^A-Z0-9]';
    my @words = split(/$spRE/, uc($text));
    map { s/$stRE//g; } @words;
    my %u = map { substr($_, 0, 100) => 1 } @words;
    delete $u{""};
    delete $u{0};
    return keys %u;
}

sub split_words_slow {
    # Preserves the case of original user text. Should not be too much slower
    my $self = shift;
    my $text  = shift;
    return {} unless ($text);
    $text     =~ s/^\s+//;
    $text     =~ s/\s+$//;
    $text     =~ s/\P{IsASCII}//g;
    # $self->err( "$text");
    my $spRE  = shift || '[^A-Z0-9_\-\.:]+';
    my $stRE  = shift || '[^A-Z0-9]';
    my @words = split(/$spRE/i, $text);
    my %u;
    foreach my $user (@words) {
        my $clean = uc($user);
        $clean =~ s/$stRE//g;
        $u{substr($clean,0,100)}{$user} = 1 if ($clean);
    }
    return \%u;
}

sub geneids_for_wordids {
    my $self = shift;
    $self->bench_start();
    my $args = $self->parseparams( -score => 1,
                                   -data  => undef,
                                   -limit => 50,
                                   @_ );
    my $black   = $args->{BLACKLIST} || 1;
    # my $lvls    = $args->{LEVELS} || $args->{LEVEL} || 0;
    my $limit   = $args->{LIMIT} || $args->{LIM};
    my $widReq    = $args->{WORDIDS} || $args->{WORDID};
    my $priRes  = $args->{DATA} || $args->{RESULTS};
    my $dumpSql = $args->{DUMPSQL};
    my $taxa    = $args->{TAXA} || $args->{SPECIES};
    my $addBlack = $args->{ADDBLACK};
    # $wids       = [ $wids ] unless (ref($wids));
    # my @words   = @{$wids};
    # my $wnum    = $#words;
    # $lvls       = $wnum if ($lvls > $wnum);
    # $lvls      = 0 if ($black);
    my $dbh     = $self->dbh;
    my ($gsids);
    if (my $gs = $args->{GENESOURCE} || $args->{SOURCE}) {
        my @reqs = ref($gs) ? @{$gs} : ($gs);
        my %u;
        foreach my $req (@reqs) {
            if (my $gsid = $self->get_geneSource( $req )) {
                $u{$gsid} = 1;
            } else {
                $self->err("Failed to find GeneSource '$req'");
            }
        }
        my @arr = sort { $a <=> $b } keys %u;
        $gsids = \@arr unless ($#arr == -1);
    }
    # Standardize wordids into clusters of wordids with wordset ids
    my $baseWordSets;
    if (my $wsreq = $args->{WSID}) {
        # Explicit word sets have been passed
        my @wsa = ref($wsreq) ? @{$wsreq} : ($wsreq);
        $baseWordSets = \@wsa unless ($#wsa == -1);
    }
    my $results = $self->empty_result_set( "SGL:geneids_for_wordids" );
    my @wordsAndSets;
    my $wRef = ref($widReq);
    if (!$wRef) {
        # Single wordID
        @wordsAndSets = ( [[$widReq]] );
    } elsif ($wRef ne 'ARRAY') {
        $self->err("No mechanism to handle word IDs passed as -wordids => '$widReq'");
        $self->bench_end();
        return $results;
    } else {
        # The primary request is an array reference
        if ($#{$widReq} == -1) {
            # oops - it is empty
            $self->bench_end();
            return $results;
        }
        # What is the data type for the first entry?
        my $wRef2 = ref($widReq->[0]);
        if ($wRef2 eq 'ARRAY') {
            # First entry is also an array reference
            if ($#{$widReq->[0]} != 1) {
                $self->err("When passing word IDs as 2D arrays, it should be of format:",
                           "-wordids => [ [wid1, wid2,...], [wsid1, wsid2... ]]");
                $self->bench_end();
                return $results;
            }
            # Ok, looks properly formated as a joint 2D array:
            # [wordidArray, wordsourceidArray]
            @wordsAndSets = @{$widReq};
        } else {
            # Looks to be just a 1D array of word IDs
            @wordsAndSets = ( [$widReq, $baseWordSets ] );
        }
    }
    my $relWS;
    my $relPri = 0;
    if (my $rws = $args->{RELIABLESOURCE}) {
        # User is providing one or more 'reliable' word sources
        my @reqs = ref($rws) ? @{$rws} : split(/\s*\,\s*/, $rws);
        foreach my $rw (@reqs) {
            next unless ($rw);
            my ($wsid) = $self->get_wordSource( $rw );
            if ($wsid) {
                $relWS ||= {};
                $relWS->{$wsid} ||= ++$relPri;
            } else {
                $self->complain_once("Can not find word source '$rw'",
                                     "= can not set as a reliable source");
            }
        }
        if ($relWS) {
            # We need to sort the sources with most reliable first
            my @sorter;
            for my $w (0..$#wordsAndSets) {
                my $was = $wordsAndSets[$w];
                my ($wids, $wsids) = @{$was};
                my $pri = 9999 + $w;
                foreach my $wsid (@{$wsids}) {
                    if (my $rp = $relWS->{$wsid}) {
                        $pri = $rp if ($rp < $pri);
                    }
                }
                push @sorter, [$pri, $was ];
            }
            @wordsAndSets = map { $_->[1] }
            sort { $a->[0] <=> $b->[0] } @sorter;
        }
    }
    my (%foundGenes, %usedWords, $priorityComplete);
    for my $w (0..$#wordsAndSets) {
        # We have a bag of 1+ words from 1+ word sets
        my ($wids, $wsids) = @{$wordsAndSets[$w]};
        if ( $priorityComplete ) {
            push @{$priorityComplete}, @{$wsids};
            next;
        }
       #  $self->err("[DEBUG]","Searching ".($#{$wids}+1)." word IDs in:", map { $self->wordsource_object($_)->to_text() } @{$wsids});

        my (@binds, @gw);
        my %tabs = ( "word_hit wh" => 1 );

        # Select for our words:
        push @gw, "wh.word_id = ?";
        # push @gw, "wh.word_id IN " .join(',', map { '?' } @{$wids}).")";
        # push @binds, @{$wids};

        if ($wsids && $#{$wsids} != -1) {
            # Select for specific word sources:
            push @gw, "wh.ws_id IN (".join(',', map { '?' } @{$wsids}).")";
            push @binds, @{$wsids};
        }

        if ($gsids) {
            # Select for specifc gene sources:
            $tabs{"gene g"}++;
            push @gw, "wh.gene_id = g.gene_id AND g.gs_id IN ".
                join(',', map { '?' } @{$gsids}).")";
            push @binds, @{$gsids};
        }

        if ($taxa) {
            # Only capture specific taxa
            map {$tabs{$_}++} ("keyval taxKV", "normtxt taxK", "normtxt taxV");
            push @gw, join(' AND ',
                           "taxKV.kv_id = g.gene_id", 
                           "upper(taxK.txt) = upper('Taxa')",
                           "taxKV.key_id = taxK.txt_id",
                           "upper(taxV.txt) = upper(?)",
                           "taxKV.val_id = taxV.txt_id");
            push @binds, $taxa;
        }

        if ($black) {
            # Apply a black list filter
            $tabs{"gene g"}++;
            my $what  = $black == -1 ? "EXISTS" : "NOT EXISTS";
            my $blsql = "$what (SELECT freq FROM blacklist bl WHERE ".
                join(" AND ",
                     "bl.word_id = wh.word_id", 
                     "bl.ws_id = wh.ws_id",
                     "bl.gs_id = g.gs_id");
            if ($black < 1 && $black > 0) {
                # Fractional value indicates we want a specific frequency used
                $blsql .= " AND bl.freq < ?";
                push @binds, $black;
            }
            $blsql .= ")";
            push @gw, $blsql;
        }
        if (exists $tabs{"gene g"}) {
            push @gw, "g.gene_id = wh.gene_id";
        }
        my $sql .= "SELECT wh.gene_id, wh.score, wh.ws_id FROM ".
            join(", ", sort keys %tabs);
        $sql .= " WHERE ".join(' AND ', @gw) unless ($#gw == -1);
        my $get = $self->dbh->prepare
            ( -sql   => $sql,
              -name  => "Find gene hits for single word",
              -level => 2 );
        warn $get->pretty_print( $wids->[0], @binds ) if ($dumpSql && !$w);
        foreach my $wid (@{$wids}) {
            my $rawUser;
            if (ref($wid)) {
                # The word ID includes data for the user strings it comes from
                ($wid, $rawUser) = @{$wid};
            }
            $usedWords{$wid}++;
            $get->execute( $wid, @binds );
            my $rows = $get->fetchall_arrayref();
            foreach my $row (@{$rows}) {
                my ($gid, $wsc, $wsid) = @{$row};
                $foundGenes{$gid}{w}{$wid}{$wsc} = $wsid;
                $priorityComplete ||= [] if ($relWS && $relWS->{$wsid});
            }
        }
    }
    my @allWids = sort { $a <=> $b } keys %usedWords;
    my $wnum    = $#allWids + 1;
    my @gids    = keys %foundGenes;
    if ($black) {
        $self->bench_start("Add-back Blacklist");
        # If we excluded the primary search by blacklist, now we want to
        # go back and capture any of our original wordids that may have
        # been missed in the found genes

        my $widFilt = " AND wh.word_id IN (".
            join(',', map { '?' } @allWids).")";
        my $recap = $self->dbh->prepare
            ( -sql   => "SELECT wh.word_id, wh.score, wh.ws_id FROM word_hit wh WHERE wh.gene_id = ?$widFilt",
              -name  => "Recover blacklisted words for genes",
              -level => 2 );
        warn $recap->pretty_print( $gids[0], @allWids ) if ($dumpSql);
        foreach my $gid (@gids) {
            $recap->execute( $gid, @allWids );
            my $rows = $recap->fetchall_arrayref();
            foreach my $row (@{$rows}) {
                my ($wid, $wsc, $wsid) = @{$row};
                $foundGenes{$gid}{w}{$wid}{$wsc} = $wsid;
                # if ($usedWords{$wid});
                # warn "Blacklisted $wid in $gid found\n" if ($addBlack);
            }
        }
        $self->bench_end("Add-back Blacklist");
    }


=pod cut out

    #unless ($gsid) { # ?? Why is this here? To check for gene existence?
    #    $sql .= ", gene g";
    #    push @gw, "wh.gene_id = g.gene_id";
    #}

    # $results->current_query("--SGL INTERNAL--");
    if ($addBlack) {
        # We want to add blacklisted scores to only those genes already found
        my @gids;
        if ($priRes) {
            @gids = map { $_->id } $priRes->each_gene();
        } else {
            $self->err("Can not add blacklist terms back to data if -data is not provided");
            $self->bench_end($bname);
            return $results;
        }
        if ($#gids == -1) {
            $self->bench_end($bname);
            return $results;
        }
        push @gw, "wh.gene_id IN (".join(',', map { '?' } @gids).")";
        push @binds, @gids;
        $black = -1;
        $limit = 0;
    }

=cut


    # For each gene, pick best score from each word and tally total
    foreach my $gid (@gids) {
        my $gH = $foundGenes{$gid};
        my $sc = 0;
        my @wids = keys %{$gH->{w}};
        foreach my $wid (@wids) {
            my $sH    = $gH->{w}{$wid};
            my ($wsc) = sort { $b <=> $a } keys %{$sH};
            my $wsid  = $sH->{$wsc};
            $gH->{w}{$wid} = [$wsc, $wsid];
            $sc += $wsc;
        }
        # modify by fraction of query words matched
        my $mod = $wnum ? ($#wids + 1) / $wnum : 1;
        # warn $self->branch($gH)."$gid = $sc * $mod (".join(',', @allWids).")\n" if ($gid eq '229299');
        $gH->{sc} = $sc * $mod;
    }
    # print $self->branch({ WordsAndSets => \@wordsAndSets, FoundGenes => \%foundGenes, UsedWords => \%usedWords });
    if ($limit) {
        # Keep only the best scoring genes
        @gids = sort { $foundGenes{$b}{sc} <=>
                           $foundGenes{$a}{sc} } @gids;
        @gids = splice(@gids, 0, $limit);
    }

    foreach my $gid (@gids) {
        # die $self->branch(\%foundGenes);
        my ($gene) = $results->add_gene($gid);
        $gene->note('Direct hit');
        # $gene->msg("[Gene]", $gene->to_text());
        while (my ($wid, $dat) = each %{$foundGenes{$gid}{w}}) {
            $results->gene_word_hit($gid, $wid, @{$dat});
        }
    }
    if ($priRes) {
        # Previous results were provided.
        # We need to merge the new genes into them
        $priRes->merge($results);
    }
    $self->bench_end();
    return $results;
}

sub all_gene_sources {
    my $self = shift;
    $self->bench_start();
    my $sql  = "SELECT gs.gs_id, gs.name FROM gene_source gs WHERE gs.gs_id != 0";
    my $get  = $self->{STHS}{GET_ALL_GENESOURCES} ||= $self->dbh->prepare
        ( -name => "Get all gene sources",
          -sql  => $sql,
          -level => 2);
    $get->execute();
    my $rows = $get->fetchall_arrayref;
    if (wantarray) {
        $self->bench_end();
        return map { $_->[1] } @{$rows};
    }
    my @com = ( type => 'gene_source' );
    my @rv;
    foreach my $row (@{$rows}) {
        my $kvs = $self->keyvals($row->[0]);
        push @rv, { @com,
                    id => $row->[0],
                    name => $row->[1],
                    keys => $kvs };
    }
    $self->bench_end();
    return \@rv;
}

sub all_word_sources {
    my $self = shift;
    unless ($self->{ALL_WORD_SOURCES}) {
        $self->bench_start();
        my $get  = $self->{STHS}{GET_ALL_WORDSOURCES} ||= $self->dbh->prepare
            ( -name => "Get all word sources",
              -sql  => "SELECT ws_id, name, weight, splitchar, stripchar ".
              "FROM word_source ws WHERE ws.ws_id != 0",
              -level => 2);
        $get->execute();
        my $rows = $get->fetchall_arrayref;
        if (wantarray) {
            $self->bench_end();
            return map { $_->[1] } @{$rows};
        }
        my @com = ( type => 'word_source' );
        my @rv;
        foreach my $row (@{$rows}) {
            my $kvs = $self->keyvals($row->[0]);
            push @rv, { @com,
                        id     => $row->[0],
                        name   => $row->[1],
                        weight => $row->[2],
                        split  => $row->[3],
                        strip  => $row->[4],
                        keys   => $kvs };
        }
        $self->{ALL_WORD_SOURCES} = \@rv;
        $self->bench_end();
    }
    return $self->{ALL_WORD_SOURCES};
}

sub all_words_for_text {
    my $self = shift;
    $self->bench_start();
    my $txt  = shift;
    $txt     = "" unless (defined $txt);
    my $wss = $self->all_word_sources();
    my %spsts;
    foreach my $ws (@{$wss}) {
        push @{$spsts{join("\t", $ws->{split}, $ws->{strip})}}, $ws;
    }
    my %rv;
    while (my ($spst, $wsa) = each %spsts) {
        my ($split, $strip) = split(/\t/, $spst);
        my $userWords = $self->split_words_slow($txt, $split, $strip);
        while (my ($clean, $userH) = each %{$userWords}) {
            map { $rv{$clean}{$_} = 1 } keys %{$userH};
        }
    }
    $self->bench_end();
    return wantarray ? sort keys %rv : \%rv;
}

sub all_word_ids_for_text {
    my $self = shift;
    my @words = $self->all_words_for_text( @_ );
    return $self->bulk_word_ids( @words );
}


sub all_genes {
    my $self = shift;
    $self->bench_start();
    my $args = $self->parseparams( @_ );
    my $sql  = "SELECT g.gene_id FROM gene g";
    my (@where, @binds);
    if (my $gs = $args->{GENESOURCE}) {
        if (my $gsid = $self->get_geneSource( $gs )) {
            push @where, "g.gs_id = ?";
            push @binds, $gsid;
        } else {
            $self->err("Failed to find GeneSource '$gs'");
        }
    }
    $sql .= " WHERE ".join(' AND ', @where) unless ($#where == -1);
    my $sth = $self->dbh->prepare
            ( -name => "Get all genes",
              -sql  => $sql,
              -limit => $args->{LIMIT},
              -level => 1, );
    my @gids = $sth->get_array_for_field( @binds );
    if (wantarray) {
    $self->bench_end();
        return @gids;
    }
    my @rv   = map { $self->object( $_, 'gene' ) } @gids;
    $self->bench_end();
    return \@rv;
}


our $simpleObjectCache     = {};
our $simpleObjectCacheSize = 0;
our $simpleObjectMaxSize   = 100000;

our $heavyObjectCache = {};
our $heavyObjectCacheSize = 0;

sub clear_cache { 
    $heavyObjectCache      = {};
    $heavyObjectCacheSize  = 0;
    $simpleObjectCache     = {};
    $simpleObjectCacheSize = 0; }

sub object {
    my $self = shift;
    my ($id, $type, $obj) = @_;
    unless ($id =~ /^\d+$/) {
        # Can we do a search with non-numeric IDs?
        my $targ = $simpleObjectCache->{$type ||= ''} ||= {};
        unless ($obj = $targ->{$id}) {
            if ($type eq 'gene') {
                $obj = $self->object_for_geneacc( $id );
            } elsif ($type eq 'grp') {
                $obj = $self->object_for_text( $id );
            } else {
                $self->err("Can not get '$type' without numeric ID");
            }
        }
        return &_add_simple_cache($id, $obj, $targ);
    }
    $obj = $simpleObjectCache->{$id};
    return $obj if (defined $obj);
    $type ||= $self->get_type_for_id( $id );
    return undef unless ($type);
    if ($type eq 'gene') {
        $obj = $self->object_for_geneid($id);
    } elsif ($type eq 'grp') {
        $obj = $self->object_for_txtid($id);
    } elsif ($type eq 'word') {
        $obj = $self->object_for_wid($id);
    } elsif ($type eq 'gene_source') {
        $obj = $self->object_for_gsid($id);
    } elsif ($type eq 'word_source') {
        $obj = $self->object_for_wsid($id);
    }
    return &_add_simple_cache($id, $obj, $simpleObjectCache);
}

sub _add_simple_cache {
    my ($key, $obj, $targ) = @_;
    return undef unless ($obj);
    if ($simpleObjectCacheSize > $simpleObjectMaxSize) {
        $simpleObjectCache = {};
        $simpleObjectCacheSize = 0;
    }
    $simpleObjectCacheSize++;
    return $targ->{$key} = $obj;
}

sub cached_object {
    my $self = shift;
    my ($id, $type) = @_;
    my $simple = $self->object($id, $type);
    return undef unless ($simple);
    $id = $simple->{id};
    my $obj = $heavyObjectCache->{$id};
    return $obj if (defined $obj);
    $type ||= lc($simple->{type} || "");
    $type =~ s/[^a-z]//g;
    if ($type eq 'gene') {
        $obj = $self->gene_object($id);
    } elsif ($type eq 'grp') {
        $obj = $self->grp_object($id);
    } elsif ($type eq 'word') {
        $obj = $self->word_object($id);
    } elsif ($type eq 'genesource') {
        $obj = $self->genesource_object($id);
    } elsif ($type eq 'wordsource') {
        $obj = $self->wordsource_object($id);
    }
    return undef unless ($obj);
    if ($heavyObjectCacheSize > 1000) {
        $heavyObjectCache = {};
        $heavyObjectCacheSize = 0;
    }
    $heavyObjectCacheSize++;
    return $heavyObjectCache->{$id} = $obj;
}

sub get_type_for_id {
    my $self = shift;
    my $id   = shift;
    die "NOT IMPLEMENTED";
}

sub gene_object { return BMS::MapTracker::StandardGeneLight::Gene->new( @_ ); }
sub grp_object { return BMS::MapTracker::StandardGeneLight::GRP->new( @_ ); }
sub word_object { return BMS::MapTracker::StandardGeneLight::Word->new( @_ ); }
sub wordsource_object { return BMS::MapTracker::StandardGeneLight::WordSource
                            ->new( @_ ); }
sub genesource_object { return BMS::MapTracker::StandardGeneLight::GeneSource
                            ->new( @_ ); }
sub task_object { return BMS::MapTracker::StandardGeneLight::Task->new( @_ ); }


sub object_for_geneid {
    my $self = shift;
    $self->bench_start();
    my $id   = shift;
    my $get  = $self->{STHS}{GET_GENE_DATA} ||= $self->dbh->prepare
        ( -name => "Get gene data",
          -sql  => "SELECT g.acc, g.gs_id FROM gene g WHERE g.gene_id = ?",
          -level => 3);
    $get->execute( $id );
    my $rows = $get->fetchall_arrayref();
    if ($#{$rows} == -1) {
        $self->bench_end();
        return 0;
    }
    my ($acc, $gsid) = @{$rows->[0]};
    $self->bench_end();
    return {
        type  => 'gene',
        id    => $id,
        acc   => $acc,
        gsid  => $gsid,
    };
}

sub object_for_geneacc {
    my $self = shift;
    $self->bench_start();
    my $acc  = shift;
    my $get  = $self->{STHS}{GET_GENE_BY_ACC} ||= $self->dbh->prepare
        ( -name => "Get gene data",
          -sql  => "SELECT g.gene_id, g.gs_id FROM gene g WHERE upper(g.acc) = upper(?)",
          -level => 3);
    # warn $get->pretty_print($acc);
    $get->execute( $acc );
    my $rows = $get->fetchall_arrayref();
    if ($#{$rows} == -1) {
        $self->bench_end();
        return 0;
    }
    my ($id, $gsid) = @{$rows->[0]};
    $self->bench_end();
    return {
        type  => 'gene',
        id    => $id,
        acc   => $acc,
        gsid  => $gsid,
    };
}

sub object_for_gsid {
    my $self = shift;
    $self->bench_start();
    my $id   = shift;
    my $get  = $self->{STHS}{GET_GENESOURCE_DATA} ||= $self->dbh->prepare
        ( -name => "Get genesource data",
          -sql  => "SELECT gs.name FROM gene_source gs WHERE gs.gs_id = ?",
          -level => 3);
    $get->execute( $id );
    my $rows = $get->fetchall_arrayref();
    if ($#{$rows} == -1) {
        $self->bench_end();
        return 0;
    }
    my ($name) = @{$rows->[0]};
    $self->bench_end();
    return {
        type  => 'gene_source',
        id    => $id,
        name  => $name,
    };
}

sub object_for_wsid {
    my $self = shift;
    $self->bench_start();
    my $id   = shift;
    my $get  = $self->{STHS}{GET_WORDSOURCE_DATA} ||= $self->dbh->prepare
        ( -name => "Get wordsource data",
          -sql  => "SELECT ws.name, ws.weight FROM word_source ws WHERE ws.ws_id = ?",
          -level => 3);
    $get->execute( $id );
    my $rows = $get->fetchall_arrayref();
    if ($#{$rows} == -1) {
        $self->bench_end();
        return 0;
    }
    my ($name, $weight) = @{$rows->[0]};
    $self->bench_end();
    return {
        type   => 'gene_source',
        id     => $id,
        name   => $name,
        weight => $weight,
    };
}

sub object_for_wid {
    my $self = shift;
    $self->bench_start();
    my $id   = shift;
    my $get  = $self->{STHS}{GET_WORD_BY_ID} ||= $self->dbh->prepare
        ( -name => "Get word data",
          -sql  => "SELECT w.word FROM word w WHERE w.word_id = ?",
          -level => 3);
    $get->execute( $id );
    my $rows = $get->fetchall_arrayref();
    if ($#{$rows} == -1) {
        $self->bench_end();
        return 0;
    }
    my ($word) = @{$rows->[0]};
    $self->bench_end();
    return {
        type   => 'word',
        id     => $id,
        word   => $word,
        name   => $word,
    };
}

sub object_for_word {
    my $self = shift;
    $self->bench_start();
    my $word = shift;
    my $get  = $self->{STHS}{GET_WORD_BY_WORD} ||= $self->dbh->prepare
        ( -name => "Get word data",
          -sql  => "SELECT w.word_id FROM word w WHERE word = upper(?)",
          -level => 3);
    $get->execute( $word );
    my $rows = $get->fetchall_arrayref();
    if ($#{$rows} == -1) {
        $self->bench_end();
        return 0;
    }
    my ($id) = @{$rows->[0]};
    $self->bench_end();
    return {
        type   => 'word',
        id     => $id,
        word   => $word,
        name   => $word,
    };
}

sub object_for_txtid {
    my $self = shift;
    my $id   = shift;
    my $text = $self->text_for_id( $id );
    return {
        type   => 'text',
        id     => $id,
        text   => $text,
    };
}

sub object_for_text {
    my $self = shift;
    my $text = shift;
    my $id   = $self->txt_id( $text );
    return {
        type   => 'text',
        id     => $id,
        text   => $text,
    };
}

sub text_for_id {
    my $self = shift;
    my $id   = shift;
    return "" unless ($id);
    my $get  = $self->{STHS}{GET_TEXT_FOR_ID} ||= $self->dbh->prepare
        ( -name => "Get text for id",
          -sql  => "SELECT txt FROM normtxt WHERE txt_id = ?",
          -level => 3);
    $get->execute( $id );
    my $rows = $get->fetchall_arrayref();
    return $#{$rows} == 0 ? $rows->[0][0] || "" : "";
}

sub txt_id {
    my $self = shift;
    my $txt  = shift;
    return 0 unless ($txt);
    $self->bench_start();
    $txt      = substr($txt, 0, 4000);
    my $get   = $self->{STHS}{GET_ID_FOR_TEXT} ||= $self->dbh->prepare
        ( -name => "Get normtxt ID for string",
          -sql  => "SELECT txt_id FROM normtxt WHERE txt = ?",
          -level => 3);
    my $tid   = $get->get_single_value( $txt );
    unless ($tid || $doNotAlterDB) {
        my $seq = $self->mainseq();
        my $add = $self->{STHS}{ADD_TXT} ||= $self->dbh->prepare
            ( -name => "Create a new NormText ID",
              -sql  => "INSERT INTO normtxt (txt_id, txt) VALUES (?,?)",
              -ignore => $self->_ignore_dup(),
              -level => 3);
        $add->execute( $seq, $txt );
        $tid  = $get->get_single_value( $txt );
    }
    $self->bench_end();
    return $tid || 0;
}

sub decorate_object {
    my $self = shift;
    my ($obj, $deep) = @_;
    return undef unless ($obj && $obj->{id});
    $self->bench_start();
    if (my $type = $obj->{type}) {
        if ($type eq 'gene') {
            $self->decorate_gene( $obj, $deep );
        }
    } else {
        $self->add_keyvals_to_object( $obj, $deep );
    }
    $self->bench_end();
}

sub decorate_gene {
    my $self = shift;
    $self->bench_start();
    my ($obj, $deep) = @_;
    $self->add_keyvals_to_object( $obj, $deep );
    $self->add_kids_to_gene( $obj );
    $self->bench_end();
    return $obj;
}

sub add_kids_to_gene {
    my $self = shift;
    my $obj  = shift;
    return undef unless ($obj && $obj->{id} && $obj->{type} eq 'gene');
    return $#{$obj->{kidids}} + 1 if ($obj->{kidids});
    my $get  = $self->{STHS}{GET_GENE_KIDS} ||= $self->dbh->prepare
        ( -name => "Get gene data",
          -sql  => "SELECT g.gene_id FROM gene g WHERE g.par_id = ?",
          -level => 3);
    my @kids = $get->get_array_for_field( $obj->{id} );
    $obj->{kidids} = \@kids;
    return $#kids + 1;
}

sub add_keyvals_to_object {
    my $self = shift;
    my $obj  = shift;
    return undef unless ($obj && $obj->{id});
    $obj->{keyvals} ||= $self->keyvals( $obj->{id} );
}

sub keyvals {
    my $self = shift;
    my $id   = shift;
    return undef unless ($id);
    $self->bench_start();
    my $get  = $self->{STHS}{GET_KEYVALS_FOR_ID} ||= $self->dbh->prepare
        ( -name => "Get all gene sources",
          -sql  => "SELECT tk.txt, tv.txt FROM keyval kv, normtxt tk, normtxt tv WHERE kv.kv_id = ? AND tk.txt_id = kv.key_id AND tv.txt_id = kv.val_id",
          -level => 3);
    $get->execute($id);
    my $rows = $get->fetchall_arrayref;
    my %rv;
    map { $rv{$_->[0]}{$_->[1]} = 1 } @{$rows};
    while (my ($k, $vs) = each %rv) {
        $rv{$k} = [ sort keys %{$vs} ];
    }
    $self->bench_end();
    return \%rv;
}

sub blacklist_for_wordid {
    my $self = shift;
    my ($wid, $gsid) = @_;
    return undef unless ($wid);
    $self->bench_start();
    my $get  = $gsid ?
        $self->{STHS}{GET_BLACKLIST_FOR_ID_AND_GSID} ||= $self->dbh->prepare
        ( -name => "Get word blacklist information for gene source",
          -sql  => "SELECT ws_id, gs_id, freq FROM blacklist WHERE word_id = ? AND gs_id = ?",
          -level => 3)
        : $self->{STHS}{GET_BLACKLIST_FOR_ID} ||= $self->dbh->prepare
        ( -name => "Get all word blacklist information",
          -sql  => "SELECT ws_id, gs_id, freq FROM blacklist WHERE word_id = ?",
          -level => 3);
    $get->execute($gsid ? ($wid, $gsid) : ($wid));
    my $rows = $get->fetchall_arrayref();
    if ($#{$rows} == -1) {
        $self->bench_end();
        return undef;
    }
    my %rv;
    foreach my $row (@{$rows}) {
        my ($wsid, $gsid, $f) = @{$row};
        my $targ = $rv{$gsid} ||= {};
        $targ->{$wsid} = $f if
            (!defined $targ->{$wsid} || $targ->{$wsid} < $f);
    }
    $self->bench_end();
    return \%rv;
}

# = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = #
# GET / SET Methods
# = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = #

sub wordids {
    my $self = shift;
    my $keepCase = wantarray ? 0 : {};
    my @words;
    if ($keepCase) {
        $keepCase = $self->split_words_slow( @_ );
        @words    = keys %{$keepCase};
    } else {
        @words = $self->split_words(@_);
    }
    return wantarray ? () : {} if ($#words == -1);
    my %ids;
    foreach my $clean (@words) {
        if (my $wid = $self->single_word_id( $clean )) {
            my $targ = $ids{$wid} ||= [ $clean, {} ];
            map { $targ->[1]{$_} = 1 } keys %{$keepCase->{$clean}}
            if ($keepCase);
        } else {
            $self->err("Failed to create word_id for '$clean'");
        }
    }
    return \%ids if ($keepCase);
    return sort { $a <=> $b } keys %ids;
}

sub single_word_id {
    my $self = shift;
    my $word = shift;
    return 0 unless ($word);
    $word = uc($word);
    my $get   = $self->{STHS}{GET_WORD_ID} ||= $self->dbh->prepare
        ( -name => "Get Word ID",
          -sql  => "SELECT word_id FROM word WHERE word = ?",
          -level => 4);
    my $wid = $get->get_single_value( $word );
    unless ($wid || $doNotAlterDB) {
        my $seq = $self->nextval('main_seq');
        my $add = $self->{STHS}{ADD_WORD} ||= $self->dbh->prepare
            ( -name => "Create a new word",
              -sql  => "INSERT INTO word (word_id, word) VALUES (?,?)",
              -ignore => $self->_ignore_dup(),
              -level => 3);
        $add->execute( $seq, $word );
        $wid  = $get->get_single_value( $word );
    }
    return $wid || 0;
}

sub bulk_word_ids {
    my $self = shift;
    $self->bench_start();
    my %ids;
    my $get   = $self->{STHS}{GET_WORD_ID} ||= $self->dbh->prepare
        ( -name => "Get Word ID",
          -sql  => "SELECT word_id FROM word WHERE word = ?",
          -level => 4);
    foreach my $word (@_) {
        if (my $wid = $get->get_single_value( uc($word) )) {
            $ids{$wid} ||= $word;
        }
    }
    $self->bench_end();
    return wantarray ? sort { $a <=> $b } keys %ids : \%ids;
}

sub grp_parents {
    my $self = shift;
    my $id = shift;
    my $rv = [];
    if ($id) {
        my $get   = $self->{STHS}{GRP_PARENT_FROM_ID} ||= $self->dbh->prepare
            ( -name => "Get GRP Parent by ID",
              -sql  => "SELECT par_id, par_type FROM GRP WHERE obj_id = ?",
              -level => 4);
        $get->execute( $id );
        $rv = $get->fetchall_arrayref();
    }
    return wantarray ? @{$rv} : $rv;
}

*grp_kids = \&grp_children;
sub grp_children {
    my $self = shift;
    my $id = shift;
    my $rv = [];
    if ($id) {
        my $get   = $self->{STHS}{GRP_KIDS_FROM_ID} ||= $self->dbh->prepare
            ( -name => "Get GRP Kids by ID",
              -sql  => "SELECT obj_id, obj_type FROM GRP WHERE par_id = ?",
              -level => 4);
        $get->execute( $id );
        $rv = $get->fetchall_arrayref();
    }
    return wantarray ? @{$rv} : $rv;
}

sub word_for_wordid {
    my $self = shift;
    my $wid = shift;
    return "" unless ($wid);
    my $get   = $self->{STHS}{GET_WORD_FROM_ID} ||= $self->dbh->prepare
        ( -name => "Get Word by ID",
          -sql  => "SELECT word FROM word WHERE word_id = ?",
          -level => 4);
    return $get->get_single_value( $wid ) || "";
}

sub get_wordSource {
    my $self = shift;
    my @rv = (0,0,"","");
    if (my $name = shift) {
        my $dbh  = $self->dbh();
        my $rows;
        if ($name =~ /^\d+$/) {
            # An ID is being passed directly
            my $get   = $self->{STHS}{GET_WS_ID} ||= $self->dbh->prepare
                ( -name => "Get WordSource by ID",
                  -sql  => "SELECT ws_id, weight, splitchar, stripchar, name ".
                  "FROM word_source WHERE ws_id = ?",
                  -level => 3);
            $rows     = $get->selectall_arrayref( $name );
        } else {
            my $get   = $self->{STHS}{GET_WS_NAME} ||= $self->dbh->prepare
                ( -name => "Get WordSource by Name",
                  -sql  => "SELECT ws_id, weight, splitchar, stripchar, name ".
                  "FROM word_source WHERE upper(name) = upper(?)",
                  -level => 3);
            $rows     = $get->selectall_arrayref( $name );
        }
        if ($#{$rows} > 0) {
            $self->err("Multiple word_source entries for '$name'",
                       map { join(" | ", @{$_} ) }
                       sort { $a->[0] <=> $b->[0] } @{$rows});
        } elsif ($#{$rows} == 0) {
            @rv = @{$rows->[0]} if ($rows->[0][0]);
        }
    }
    return wantarray ? @rv : $rv[0];    
}

sub get_geneSource {
    my $self = shift;
    my ($req, $params) = @_;
    my @rv = (0,"");
    if ($req) {
        my $dbh  = $self->dbh();
        my $get;
        if ($req =~ /^\d+$/) {
            $get = $self->{STHS}{GET_GS_BY_ID} ||= $self->dbh->prepare
                ( -name => "Get a geneSource ID",
                  -sql  => "SELECT gs_id, name FROM gene_source".
                  " WHERE gs_id = ?",
                  -level => 3);
        } else {
            $get = $self->{STHS}{GET_GS_BY_NAME} ||= $self->dbh->prepare
                ( -name => "Get a geneSource ID",
                  -sql  => "SELECT gs_id, name FROM gene_source".
                  " WHERE upper(name) = upper(?)",
                  -level => 3);
        }
        my $rows = $get->selectall_arrayref( $req );
        if ($#{$rows} > 0) {
            $self->err("Multiple gene_source entries for '$req'",
                       map { join(" | ", @{$_} ) }
                       sort { $a->[0] <=> $b->[0] } @{$rows});
        } else {
            @rv = @{$rows->[0]} if ($#{$rows} == 0 && $rows->[0][0]);
            if (!$rv[0] && !$doNotAlterDB && $req !~ /^\d+$/) {
                # No ID found, we can modify the DB, and we are given a name
                my $seq = $self->nextval('main_seq');
                my $add = $self->{STHS}{ADD_GS} ||= $self->dbh->prepare
                    ( -name => "Create a new geneSource",
                      -sql  => "INSERT INTO gene_source (gs_id, name) VALUES (?,?)",
                      -ignore => $self->_ignore_dup(),
                      -level => 3);
                $add->execute( $seq, $req );
                $rows = $get->selectall_arrayref( $req );
                @rv = @{$rows->[0]} if ($rows->[0][0]);
            }
            $self->set_keyval($rv[0], $params, 1) if ($params && $rv[0]);
        }
    }
    return wantarray ? @rv : $rv[0];    
}

sub get_task {
    my $self = shift;
    my @rv = (0,"");
    if (my $name = shift) {
        my $dbh  = $self->dbh();
        my $rows;
        if ($name =~ /^\d+$/) {
            # An ID is being passed directly
            my $get   = $self->{STHS}{GET_TASK_ID} ||= $self->dbh->prepare
                ( -name => "Get ManualTask by ID",
                  -sql  => "SELECT task_id, name ".
                  "FROM manual_task WHERE task_id = ?",
                  -level => 3);
            $rows     = $get->selectall_arrayref( $name );
        } else {
            my $get   = $self->{STHS}{GET_TASK_NAME} ||= $self->dbh->prepare
                ( -name => "Get ManuakTask by Name",
                  -sql  => "SELECT task_id, name ".
                  "FROM manual_task WHERE upper(name) = upper(?)",
                  -level => 3);
            $rows     = $get->selectall_arrayref( $name );
            if ($#{$rows} == -1) {
                # Auto populate the task
                 my $add = $self->{STHS}{ADD_TASK} ||= $dbh->prepare
                     ( -name => "Create a new ManualTask",
                       -sql  => "INSERT INTO manual_task (task_id, name) VALUES (?,?,?)",
                       -ignore => $self->_ignore_dup(),
                       -level => 3);
                 my $seq = $self->nextval('main_seq');
                 $add->execute( $seq, $name );
                 $rows = $get->selectall_arrayref( $name );
                 if ($#{$rows} == -1) {
                     $self->err("Failed to create new task entry for '$name'");
                 }
            }
        }
        if ($#{$rows} > 0) {
            $self->err("Multiple manual_task entries for '$name'",
                       map { join(" | ", @{$_} ) }
                       sort { $a->[0] <=> $b->[0] } @{$rows});
        } elsif ($#{$rows} == 0) {
            @rv = @{$rows->[0]} if ($rows->[0][0]);
        }
    }
    return wantarray ? @rv : $rv[0];    
}

sub set_keyval {
    return 0 if ($doNotAlterDB);
    my $self = shift;
    my ($id, $params, $clear) = @_;
    return unless ($id && $params);
    $self->bench_start();
    $self->death("Non-integer ID '$id' provided") unless ($id =~ /^\d+$/);
    my $dbh = $self->dbh();
    if ($clear) {
        my $nuke = $self->{STHS}{CLEAR_KV_ENTRIES} ||= $self->dbh->prepare
            ( -name => "Clear all KeyVals",
              -sql  => "DELETE FROM keyval WHERE kv_id = ?",
              -level => 3);
        $nuke->execute($id);
    }
    my $add = $self->{STHS}{SET_KV_ENTRY} ||= $self->dbh->prepare
            ( -name => "Set KeyVal",
              -sql  => "INSERT INTO keyval (kv_id, key_id, val_id) VALUES (?,?,?)",
              -level => 3);
    my $num = 0;
    while (my ($key, $val) = each %{$params}) {
        my $kid = $self->txt_id($key);
        next unless ($kid);
        my @vals = ref($val) ? @{$val} : ($val);
        foreach my $v (@vals) {
            if (my $vid = $self->txt_id($v)) {
                $add->execute($id, $kid, $vid);
                $num++;
            }
        }
    }
    $self->bench_end();
    return $num;
}

sub taxa_gene_set {
    my $self = shift;
    my ($taxa) = @_;
    my $gsNm  = "$taxa LocusLink";
    return $self->get_geneSource( $gsNm );
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::StandardGeneLight::Common;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use BMS::Utilities::Escape;
use BMS::Utilities::Benchmark;
use Scalar::Util qw(weaken);

use vars qw(@ISA);
@ISA      = qw(BMS::Utilities::Escape BMS::Utilities::Benchmark);

sub sgl     { return shift->{SGL}; }
sub dbh     { return shift->sgl->dbh(); }

sub results {
    my $self = shift;
    my $query = shift;
    $query = $self->last_query() unless (defined $query);
    return exists $self->{RESULTS}{$query} ? $self->{RESULTS}{$query} : undef;
}

*each_result = \&all_results;
sub all_results {
    my $self = shift;
    my %rv;
    map { $rv{$_} = $self->{RESULTS}{$_} 
          if ($self->{RESULTS}{$_}) } $self->queries();
    return wantarray ? values %rv : \%rv;
}

*each_query = \&queries;
sub queries {
    return sort keys %{shift->{RESULTS} || {}};
}

sub set_results {
    my $self = shift;
    my $results = shift;
    if ($results) {
        my %qHash; my $num = 0;
        map { $qHash{$_} ||= ++$num if (defined $_) } @_;
        my @queries = sort { $qHash{$a} <=> $qHash{$b} } keys %qHash;
        @queries = ($results->current_query() || "") if ($#queries == -1);
        foreach my $query (@queries) {
            weaken($self->{RESULTS}{$query} = $results);
        }
        $self->{LAST_QUERY} = $queries[-1];
    }
    return $results;
}

sub last_query {
    return shift->{LAST_QUERY} || "";
}

sub obj_id {
    my $self = shift;
    my $req  = shift;
    return undef unless $req;
    if (ref($req)) {
        return $req->id();
    } elsif ($req =~ /^\d+$/) {
        return $req;
    }
    if (my $type = shift) {
        if (my $obj = $self->sgl->object($req, $type)) {
            return $obj->{id};
        }
    }
    $self->err("Not sure how to get ID for '$req'");
    return undef;
}

sub context_data {
    my $self = shift;
    my $nv   = shift;
    $self->{CONTEXTDATA} = $nv if (defined $nv);
    return $self->{CONTEXTDATA};
}

sub _xmlCom {
    my $self = shift;
    my ($text, $indent) = @_;
    $indent ||= 0;
    # Indent to allow for "<!-- ";
    $indent   += 5;
    my $pad    = " " x $indent;
    my $block  = 80 - $indent;
    my $search = int($block / 2);
    my @lines;
    foreach my $com ( split(/[\n\r]/, $text)) {
        $com ||= "";
        $com =~ s/\s+/ /g;
        $com =~ s/^\s+//;
        $com =~ s/\s+$//;
        $com = $self->esc_xml( $com );
        while ($com ne "") {
            my $pos = length($com);
            if ($pos <= $block) {
                # Remaining comment fits in block
                push @lines, $com;
                last;
            }
            # Find a space to split the string on
            $pos = $block;
            while ($pos > $search &&
                   substr($com, $pos, 1) ne " ") { $pos--; }
            if ($pos <= $search) {
                # We need to *increase* the block
                while ($pos < length($com) &&
                       substr($com, $pos, 1) ne " ") { $pos++; }
                if ($pos >= length($com)) {
                    push @lines, $com;
                    last;
                }
            }
            push @lines, substr($com, 0, $pos);
            $com = substr($com, $pos + 1);
        }
    }
    return "" if ($#lines == -1);
    map { $_ = "$pad$_" } @lines;
    substr($lines[0], $indent - 5, 5) = "<!-- ";
    $lines[-1] .= " -->";
    # die $self->branch(\@lines);
    return join('', map { "$_\n" } @lines);
}

sub _xml_attr {
    my $self = shift;
    my $hash = shift || {};
    my @kv;
    foreach my $key (sort keys %{$hash}) {
        my $v = $hash->{$key};
        $v = join(",", @{$v}) if (ref($v));
        next unless (defined $v && $v ne '');
        push @kv, sprintf("%s='%s'", $key, $self->esc_xml_attr($v));
    }
    return ($#kv == -1) ? "" : " ".join(" ", @kv);
}

sub _xml_bar { 
    my $self = shift;
    my $tok  = $self->esc_xml(shift || "-");
    my $len  = shift || 60;
    my $num  = int(0.99 + $len / length($tok));
    my $bar  = substr($tok x $num, 0, $len);
    return "<!-- $bar -->";
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::StandardGeneLight::Object;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use vars qw(@ISA);
@ISA      = qw(BMS::MapTracker::StandardGeneLight::Common);

sub id      { return shift->{DBID}; }
sub type    { return shift->{BASE}{type}; }

sub keyvals {
    my $self = shift;
    return $self->{KEYVALS} ||= $self->sgl->keyvals( $self->id() );
}

sub each_key {
    my $self = shift;
    my $kv   = $self->keyvals();
    return sort keys %{$kv};
}

sub keyval {
    my $self = shift;
    my $key  = shift;
    my @rv;
    if ($key) {
        my $kv = $self->keyvals();
        @rv = @{$kv->{$key}} if (exists $kv->{$key});
    }
    return @rv if (wantarray);
    my $opts = shift;
    my $val = $rv[0] || "";
    return $val if ($#rv <= 0 || !$opts);
    if ($opts =~ /short/i) {
        ($val) = sort { length($a) cmp length($b) } @rv;
    }
    return $val || "";
}

sub param {
    # Allows local parameters (keyvalues not in DB) to be set
    my $self = shift;
    my ($key, $val) = @_;
    return undef unless ($key);
    my $store = $self->{PARAMS} ||= {};
    if ($val) {
        $store->{$key} = $val;
    }
    return $store->{$key};
}

*description = \&desc;
sub desc { return shift->keyval('Description', 'short'); }

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::StandardGeneLight::Word;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use Scalar::Util qw(weaken);

use vars qw(@ISA);
@ISA      = qw(BMS::MapTracker::StandardGeneLight::Object);

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my ($sgl, $id, $text) = @_;
    return undef unless ($id);
    my $self = {
        USERWORD => {},
    };
    bless ($self, $class);
    weaken($self->{SGL}  = $sgl);
    $self->{DBID} = $id;
    if ($text && $id < 1) {
        # Creating a word that is not stored in database
        # Negative numbers can be used to uniquely store
        $self->{WORD} = uc($text);
    }
    return $self;
}

sub word {
    my $self = shift;
    unless (defined $self->{WORD}) {
        $self->{WORD} = $self->sgl->word_for_wordid( $self->id() );
    }
    return $self->{WORD};
}

sub type { return "word"; }

sub to_text {
    my $self = shift;
    my $id   = $self->id();
    return $self->word()." [UNKNOWN WORD]" if ($id < 1);
    return sprintf("%s [word_id = %d]",$self->word, $id);
}

sub to_html {
    my $self = shift;
    my $opts = shift;
    my $idHTML = "";
    if ($opts->{ID}) {
        my $id = $self->id();
        $idHTML = sprintf(" <span class='id'>[%s]</span>",
                          $id < 1 ? 'UNK' : $id);
    }
    return sprintf("<div class='word'>%s%s</div>", $self->word(), $idHTML); 
}

sub user_word {
    my $self = shift;
    my ($uword, $ws) = @_;
    return unless ($uword);
    my $wsid = $self->obj_id($ws) || 0;
    $self->{USERWORD}{$uword}{$wsid} = 1;
}

sub user_words {
    my $self = shift;
    my @rv;
    my $store = $self->{USERWORD};
    foreach my $uword (sort { uc($a) cmp uc($b) || $a cmp $b } keys %{$store}){
        push @rv, [$uword, [sort { $a <=> $b } keys %{$store->{$uword}} ]];
    }
    return @rv;
}

sub to_xml {
    my $self = shift;
    my ($indent, $detail) = @_;
    $indent ||= 0;
    $detail ||= 0;
    my $pad = " " x $indent;
    my $id = $self->id();
    my $attr = {
        dbid    => $id > 0 ? $id : undef,
        unknown => $id > 0 ? undef : '1',
        word    => $self->word(),
    };
    my $xml = sprintf("%s<word%s", $pad, $self->_xml_attr($attr));
    return "$xml />\n" unless ($detail);
    $xml .= ">\n";
    foreach my $uwd ($self->user_words() ) {
        my ($uw, $wsA) = @{$uwd};
        my $uat = { wsids => join(',', @{$wsA}) };
        $xml .=sprintf("  %s<userWord%s>%s</userWord>\n", $pad,
                       $self->_xml_attr($uat), $self->esc_xml($uw));
    }
    if ($detail > 1) {
        my @bl = sort { $b->[0] <=> $a->[0] } $self->blacklist();
        my $res = $self->results();
        my %oksrcs = map { $_->id() => 1 } ($res->each_gene_source,
                                            $res->each_word_source);
        foreach my $bld (@bl) {
            my ($f, $wsid, $gsid) = @{$bld};
            next unless ($oksrcs{$gsid} && $oksrcs{$wsid});
            my $gs = $res->gene_source($gsid);
            my $ws = $res->word_source($wsid);
            $xml .= sprintf("  %s<blacklist%s>%s in %s</blacklist>\n", $pad, $self->_xml_attr({
                frequency => sprintf("%.5f", $f),
                wsid => $wsid,
                gsid => $gsid,
            }), $ws ? $ws->name() : "-?WordSOURCE?-",
                            $gs ? $gs->name() : "-?GENESOURCE?-" );
        }
    }
    $xml .= "$pad</word>\n";
    return $xml;
}

sub to_hash {
    my $self = shift;
    my ($detail) = @_;
    $detail ||= 0;
    my $id = $self->id();
    my $rv = {
        dbid    => $id > 0 ? $id : undef,
        unknown => $id > 0 ? undef : '1',
        word    => $self->word(),
        userwords => [ $self->user_words() ],
    };
    if ($detail > 1) {
        my @bl = sort { $b->[0] <=> $a->[0] } $self->blacklist();
        my $res = $self->results();
        my %oksrcs = map { $_->id() => 1 } ($res->each_gene_source,
                                            $res->each_word_source);
        my $bla = $rv->{blacklist} = [];
        foreach my $bld (@bl) {
            my ($f, $wsid, $gsid) = @{$bld};
            next unless ($oksrcs{$gsid} && $oksrcs{$wsid});
            my $gs = $res->gene_source($gsid);
            my $ws = $res->word_source($wsid);
            push @{$bla}, { frequency => sprintf("%.5f", $f),
                            wsid => $wsid,
                            gsid => $gsid,
                            wordsource => $ws ? $ws->name() : "-?WordSOURCE?-",
                            genesource => $gs ? $gs->name() : "-?GENESOURCE?-",
                        };
        }
    }
    return $rv;
}

sub blacklist {
    my $self = shift;
    unless ($self->{BLACKLIST}) {
        my $bl = $self->{BLACKLIST} = [];
        my $blH = $self->sgl->blacklist_for_wordid( $self->id );
        while (my ($gsid, $wsH) = each %{$blH}) {
            while (my ($wsid, $f) = each %{$wsH}) {
                push @{$bl}, [$f, $wsid, $gsid];
            }
        }
    }
    return @{$self->{BLACKLIST}};
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::StandardGeneLight::WordSource;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use Scalar::Util qw(weaken);

use vars qw(@ISA);
@ISA      = qw(BMS::MapTracker::StandardGeneLight::Object);

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my ($sgl, $id) = @_;
    return undef unless ($id);
    my $self = { };
    bless ($self, $class);
    weaken($self->{SGL}  = $sgl);
    $self->{DBID} = $id;
    my @info = $sgl->get_wordSource( $id );
    $self->{BASE} = {
        type   => 'word_source',
        id     => $info[0],
        weight => $info[1],
        splchr => $info[2],
        strchr => $info[3],
        name   => $info[4],
    };
    return $self;
}

sub split  { return shift->{BASE}{splchr}; }
sub strip  { return shift->{BASE}{strchr}; }
sub name   { return shift->{BASE}{name}; }
sub weight { return shift->{BASE}{weight}; }

sub to_text {
    my $self = shift;
    return sprintf("%s [ws_id = %d] weight:%s", $self->name, $self->id,
                   $self->weight());
}

sub to_html {
    my $self = shift;
    my $opts = shift;
    my $idHTML = "";
    if ($opts->{ID}) {
        my $id = $self->id();
        $idHTML = sprintf(" <span class='id'>[%s]</span>",
                          $id < 1 ? 'UNK' : $id);
    }
    return sprintf("<div class='wordsource'>%s%s</div>", 
                   $self->name(), $idHTML); 
}


sub to_hash {
    my $self = shift;
    return {
        name => $self->name,
        dbid => $self->id,
        weight => $self->weight,
        split => $self->split,
        strip => $self->strip,
    };
}

sub to_xml {
    my $self = shift;
    my ($indent, $detail) = @_;
    my $pad = $indent ? " " x $indent : "";
    my $xml = sprintf
        ("%s<wordSource%s>%s</wordSource>\n", $pad, $self->
         _xml_attr( { dbid => $self->id, weight => $self->weight,
                      split => $self->split, strip => $self->strip, }),
         $self->name);
    return $xml;
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::StandardGeneLight::GeneSource;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use Scalar::Util qw(weaken);

use vars qw(@ISA);
@ISA      = qw(BMS::MapTracker::StandardGeneLight::Object);

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my ($sgl, $id) = @_;
    return undef unless ($id);
    my $self = { };
    bless ($self, $class);
    weaken($self->{SGL}  = $sgl);
    $self->{DBID} = $id;
    my @info = $sgl->get_geneSource( $id );
    $self->{BASE} = {
        type   => 'gene_source',
        id     => $info[0],
        name   => $info[1],
    };
    return $self;
}

sub name   { return shift->{BASE}{name}; }

sub to_text {
    my $self = shift;
    return sprintf("%s [gs_id = %d]", $self->name, $self->id);
}

sub to_html {
    my $self = shift;
    my $opts = shift;
    my $idHTML = "";
    if ($opts->{ID}) {
        my $id = $self->id();
        $idHTML = sprintf(" <span class='id'>[%s]</span>",
                          $id < 1 ? 'UNK' : $id);
    }
    return sprintf("<div class='genesource'>%s%s</div>", 
                   $self->name(), $idHTML); 
}
sub to_hash {
    my $self = shift;
    return {
        name => $self->name,
        dbid => $self->id,
    };
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::StandardGeneLight::GRP;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use Scalar::Util qw(weaken);

use vars qw(@ISA);
@ISA      = qw(BMS::MapTracker::StandardGeneLight::Object);

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my ($sgl, $id) = @_;
    return undef unless ($id);
    my $base = $sgl->object( $id, 'grp' );
    my $self = { 
        BASE => $base,
        DBID => $base->{id},
    };
    bless ($self, $class);
    weaken($self->{SGL}  = $sgl);
    return $self;
}

# sub type { return shift->grp_type(); }

sub grp_type {
    my $self = shift;
    return $self->keyval('Type', 'short');
}
# sub type    { return shift->{BASE}{type}; }

# This will be over-ridden for Gene objects
sub txtid { return shift->{DBID}; }

*name = \&acc;
sub acc {
    my $self = shift;
    unless ($self->{ACC}) {
        $self->{ACC} = $self->sgl->text_for_id( $self->txtid );
    }
    return $self->{ACC};
}

sub to_text {
    my $self = shift;
    return sprintf("%s [ id = %d] %s", $self->acc, $self->id, $self->desc);
}

sub to_xml {
    my $self = shift;
    my ($indent, $detail) = @_;
    $indent ||= 0;
    my $pad = $indent ? " " x $indent : "";
    my $type = $self->grp_type();
    my $tag = lc($type);
    my ($sym, $isOff) = $self->symbol();
    my $xml    = sprintf("%s<%s%s", $pad, $tag, $self->_xml_attr( {
        accession  => $self->name(), id => $self->id, 
        taxa => $self->taxa(), symbol => $sym,
        perl => 'GRP',
        desc => $self->desc(), type => $self->grp_type()}));
    my $inner = $detail ? $self->_child_xml( $indent + 2 ) : "";
    if ($inner) {
        $xml .= join('', ">\n", $inner, "$pad</$tag>\n");
    } else {
        $xml .= " />\n";
    }
    return $xml;
}

sub to_html {
    my $self = shift;
    my $opts = shift;
    my $html = "<div class='grp'>";
    $html .= sprintf("<span class='acc'>%s</span>", $self->name);
    if ($self->can('score')) {
        my $sc = $self->score();
        $html .= " <sup class='score'>{$sc}</sup>" if (defined $sc);
    }
    if (my $sym = $self->symbol()) {
        $html .= sprintf(" <span class='symbol'>%s</span>", $sym);
    }
    if (my $taxa = $self->taxa()) {
        $html .= sprintf(" <span class='taxa'>[%s]</span>", $taxa);
    }
    if (my $desc = $self->desc()) {
        $html .= sprintf(" <span class='desc'>%s</span>", $desc);
    }
    if ($opts->{ID}) {
        my $id = $self->id();
        $html = sprintf(" <span class='id'>[%s]</span>",
                        $id < 1 ? 'UNK' : $id);
    }
    $html .= "</div>\n";
    return $html;
}

sub _child_xml {
    my $self = shift;
    my $indent = shift || 0;
    my $pad = $indent ? " " x $indent : "";
    my @kids = $self->children();
    my %byType;
    map { $byType{ lc($_->grp_type()) }++ } $self->all_children();
    my $kNum = $#kids + 1;
    return "" unless ($kNum);
    my $xml .= sprintf("%s<children%s", $pad, $self->_xml_attr( {
        count => $kNum, parent => $self->acc, %byType,
    }));
    if ($kNum) {
        $xml .= ">\n";
        foreach my $kid (@kids) {
            $xml .= $kid->to_xml($indent + 2, 1);
        }
        $xml .= sprintf("%s</children>\n", $pad);
    } else {
        $xml .= " />\n";
    }
    return $xml;
}

sub to_hash {
    my $self = shift;
    my ($detail) = @_;
    my ($sym, $isOff) = $self->symbol();
    my $rv = {
        accession  => $self->name(),
        id => $self->id, 
        taxa => $self->taxa(),
        symbol => $sym,
        desc => $self->desc(), 
        type => $self->grp_type(),
    };
    $rv->{kids} = $self->_child_array($detail) if ($detail);
    return $rv;
}

sub _child_array {
    my $self = shift;
    my ($detail) = @_;
    my @kids = $self->children();
    my @arr  = map { $_->to_hash($detail) } @kids;
    return \@arr;
}

sub parents {
    my $self = shift;
    unless ($self->{PARENTS}) {
        my @arr;
        foreach my $dat ($self->sgl->grp_parents( $self->txtid )) {
            my ($oid, $type) = @{$dat};
            next unless ($oid);
            if (my $rel = $self->_get_relative( $oid )) {
                push @arr, $rel;
            }
        }
        $self->{PARENTS} = \@arr;
    }
    return @{$self->{PARENTS}};
}

sub all_children {
    my $self = shift;
    my $lvl  = shift || 1;
    my $hash = shift || {};
    foreach my $kid ($self->children()) {
        my $k = $kid->id();
        next if ($hash->{ $k });
        $hash->{$k} = [ $kid, $lvl ];
        $kid->all_children($lvl + 1, $hash);
    }
    return wantarray ? map { $_->[0] } values %{$hash} : $hash;
}

sub children {
    my $self = shift;
    unless ($self->{CHILDREN}) {
        my @arr;
        foreach my $dat ($self->sgl->grp_children( $self->txtid ) ) {
            my ($oid, $type) = @{$dat};
            next unless ($oid);
            if (my $rel = $self->_get_relative( $oid )) {
                push @arr, $rel;
            }
        }
        $self->{CHILDREN} = \@arr;
    }
    return @{$self->{CHILDREN}};
}

sub _get_relative {
    my $self = shift;
    my $oid  = shift;
    my $rel = $self->sgl->cached_object($oid, 'gene' ) ||
        $self->sgl->cached_object($oid, 'grp' );
    return $rel;
}

*species =\&taxa;
sub taxa {
    my $self = shift;
    my $tax  = $self->keyval('Taxa');
    return $tax || "";
}

*sym = \&symbol;
sub symbol {
    my $self = shift;
    my $sym  = $self->keyval('Official Symbol');
    my $isOff = 1;
    unless ($sym) {
        $isOff = 0;
        ($sym)  = sort { length($a) <=> length($b) ||
                             lc($a) cmp lc($b) }
        $self->keyval('Unofficial Symbol');
    }
    $sym ||= "";
    return wantarray ? ($sym, $isOff) : $sym;
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::StandardGeneLight::Gene;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use Scalar::Util qw(weaken);

use vars qw(@ISA);
@ISA      = qw(BMS::MapTracker::StandardGeneLight::Object
               BMS::MapTracker::StandardGeneLight::GRP);

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my ($sgl, $id) = @_;
    return undef unless ($id);
    my $self = { };
    bless ($self, $class);
    weaken($self->{SGL}  = $sgl);
    $self->{BASE} = $sgl->object($id, 'gene');
    return undef unless ($self->{BASE});
    $self->{DBID} = $self->{BASE}{id};
    return $self;
}

sub acc  { return shift->{BASE}{acc}; }
sub gsid { return shift->{BASE}{gsid}; }

sub to_text {
    my $self = shift;
    return sprintf("%s%s (%s) %s", $self->acc, $self->symbol ? " [".$self->symbol."]" : "", $self->taxa(), $self->desc || "");
#    return sprintf("%s [gene_id = %d] %s", $self->acc, $self->id,
#                   $self->desc || "");
}

sub score {
    my $self = shift;
    my ($null, $query) = @_;
    # Scores are meaningful only in the context of a Result object
    my $rv;
    if (my $results = $self->results($query)) {
        $rv = $results->gene_score($self);
    }
    return defined $rv ? $rv : $null;
}

sub note {
    my $self = shift;
    if (my $results = $self->results()) {
        return $results->gene_note($self, @_);
    }
    return wantarray ? () : "";
}

sub best_score_for_all_results {
    my $self = shift;
    $self->bench_start();
    my $null = shift;
    my @all;
    foreach my $results ($self->all_results) {
        my $sc = $results->gene_score($self);
        if (defined $sc) {
            push @all, $sc;
        } elsif (defined $null) {
            push @all, $null;
        }
    }
    my ($best) = sort { $b <=> $a } @all;
    $best = $null unless (defined $best);
    $self->bench_end();
    return $best;
}

sub txtid {
    my $self = shift;
    unless (defined $self->{TXTID}) {
        $self->{TXTID} = $self->sgl->txt_id($self->acc);
    }
    return $self->{TXTID};
}

sub gene_source {
    my $self  = shift;
    my $query = shift;
    if (my $results = $self->results($query)) {
        return $results->add_gene_source( $self->gsid );
    } else {
        return $self->sgl->cached_object($self->gsid,'genesource' );
    }
}

sub to_hash {
    my $self = shift;
    my ($detail) = @_;
    $detail ||= 0;
    my $gs  = $self->gene_source();
    my $gt  = $self->keyval('GeneType');
    my ($sym, $isOff) = $self->symbol();
    my @allSym = map { $self->keyval($_) }
    ('Official Symbol','Unofficial Symbol');
    my $rv = {
        accession     => $self->name(),
        gid           => $self->id, 
        taxa          => $self->taxa(),
        symbol        => $sym,
        symIsOfficial => $isOff,
        allSymbols    => \@allSym,
        desc          => $self->desc(),
        type          => $self->type(),
        score         => $self->score,
        gsid          => $self->gsid,
        geneType      => $gt,
    };
    if ($detail > 1) {
        my @orthObjs = $self->orthologues();
        my @orths = map { $_->to_hash(0) } @orthObjs;
        $rv->{orthologues} = \@orths;
    }
    if ($detail > 2) {
        my $sgl     = $self->sgl;
        my @wDats = sort { $b->[2] <=> $a->[2] ||
                               $a->[1] <=> $b->[1] } $self->word_hits();
        my $wArr = $rv->{wordhits} = [];
        foreach my $wDat (@wDats) {
            my ($gid, $wid, $wsc, $wsid) = @{$wDat};
            my $word = $sgl->cached_object($wid, 'word');
            my $ws   = $sgl->cached_object($wsid, 'word_source');
            push @{$wArr}, {
                wid   => $wid,
                score => $wsc,
                word  => $word->word,
                wsid  => $wsid,
                wordsource => $ws ? $ws->name() : '-?WORDSOURCE?-',
            };
        }
    }
    return $rv;
}

sub to_xml {
    my $self = shift;
    my ($indent, $detail) = @_;
    $indent ||= 0;
    $detail ||= 0;
    my $id2 = $indent + 2;
    my $pad = $indent ? " " x $indent : "";
    my $gs  = $self->gene_source();
    my $gt  = $self->keyval('GeneType') || 'Unknown';
    my $desc = $self->desc();
    my ($sym, $isOff) = $self->symbol();
    my $xml    = sprintf("%s<gene%s", $pad, $self->_xml_attr( {
        accession     => $self->name(),
        gid           => $self->id, 
        taxa          => $self->taxa(),
        symbol        => $sym,
        symIsOfficial => $isOff,
        desc          => $desc,
        type          => $self->type(),
        perl          => 'Gene',
        score         => $self->score,
        gsid          => $self->gsid, 
        geneType      => $gt,
    }));
    my $inner = "";
    $inner     .= $self->_child_xml( $indent + 2 ) if ($detail);
    if ($detail > 1) {
        # warn $self->branch($self) if ($self->results);
        foreach my $note ($self->note()) {
            $inner .= sprintf("  %s<note%s>%s</note>\n", $pad, '',
                              $self->esc_xml($note));
        }
        my @orths = $self->orthologues();
        my $oNum  = $#orths + 1;
        $inner .= sprintf("  %s<orthologues%s", $pad, $self->_xml_attr( {
            count => $oNum,
        }));
        if ($oNum) {
            $inner .= ">\n";
            foreach my $orth (@orths) {
                $inner .= $orth->to_xml($indent + 4, 0);
                #my ($sym, $isOff) = $orth->symbol();
                #$inner .= sprintf("  %s<orthologue%s />\n", $pad, $self->_xml_attr( {
                #    accession  => $orth->name(), gid => $orth->id, 
                #    taxa => $orth->taxa(), symbol =>  $sym,
                #    symIsOfficial => $isOff,
                #    desc => $orth->desc(), type => $orth->type()
                #    }) );
            }
            $inner .= sprintf("  %s</orthologues>\n", $pad);
        } else {
            $inner .= " />\n";
        }
    }
    if ($detail > 2) {
        my @wDats = sort { $b->[2] <=> $a->[2] || $a->[1] <=> $b->[1] } $self->word_hits();
        my $wNum = $#wDats + 1;
        $inner .= sprintf("  %s<words%s", $pad, $self->_xml_attr( {
            count => $wNum,
        }));
        if ($wNum) {
            $inner .= ">\n";
            my $sgl     = $self->sgl;
            foreach my $wDat (@wDats) {
                my ($gid, $wid, $wsc, $wsid) = @{$wDat};
                my $word = $sgl->cached_object($wid, 'word');
                my $ws   = $sgl->cached_object($wsid, 'word_source');
                $inner .= sprintf("    %s<word%s>%s</word>\n", $pad, $self->_xml_attr( {
                    wid   => $wid,
                    score => $wsc,
                    word  => $word->word,
                    wsid  => $wsid,
                }), $self->esc_xml($ws ? $ws->name() : '-?WORDSOURCE?-'));
            }
            $inner .= sprintf("  %s</words>\n", $pad);
        } else {
            $inner .= " />\n";
        }
    }
    if ($inner) {
        $xml .= join('', ">\n", $inner, "$pad</gene>\n");
    } else {
        $xml .= " />\n";
    }
    return $xml;
}

*each_orthologue = \&orthologues;
sub orthologues {
    my $self = shift;
    my $sgl  = $self->sgl;
    my %byId;
    
    foreach my $k ($self->each_key()) {
        if ($k =~ /^(.+) Ortholog$/) {
            foreach my $kv ($self->keyval($k)) {
                my $orth = $sgl->cached_object($kv, 'gene');
                # warn "$k = $kv [$orth]\n";
                $byId{$orth->id} ||= $orth if ($orth);
            }
        }
    }
    return values %byId;
}

sub word_hits {
    my $self = shift;
    my $query = shift;
    my @rv;
    if (my $results = $self->results($query)) {
        @rv = $results->hits_for_gene( $self );
    }
    return @rv;
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::StandardGeneLight::Results;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use BMS::Utilities::Serialize;

use vars qw(@ISA);
@ISA = qw(BMS::MapTracker::StandardGeneLight::Common
          BMS::Utilities::Serialize);
use Scalar::Util qw(weaken);

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my ($sgl) = @_;
    my $self = {
        GENES    => {},
        WORDS    => {},
        WORDSRCS => {},
        GENESRCS => {},
        SCORES   => {},
        WORDHITS => {},
        GENEHITS => {},
        ITER     => 0,
        
    };
    bless ($self, $class);
    weaken($self->{SGL}  = $sgl);
    return $self;
}

sub results { return shift; }
sub spawn_id { return shift->{SPAWN_NUM}; }

our $unkWordId = 0;
our %unkWords;
sub add_word {
    my $self = shift;
    my $store = $self->{WORDS};
    my @rv;
    # DOES NOT ACCEPT RAW IDS
    # Words can be integers, so we can not count on an integer being a DB ID
    foreach my $word (@_) {
        next unless ($word);
        my ($id, $obj);
        if (ref($word)) {
            $id = $word->id;
            $obj = $word;
        } else {
            # Again: asuming we have a true word (not an ID) at this point
            $id = $self->sgl->single_word_id( $word );
            unless ($id) {
                # This is not a recognized word
                $id = $unkWords{ uc($word) } ||= --$unkWordId;
            }
        }
        unless ($store->{$id}) {
            my $novel = $store->{$id} =
                $obj || $self->sgl->word_object( $id, $word );
            $novel->set_results($self);
        }
        push @rv, $store->{$id};
    }
    return @rv;
}

sub add_word_id {
    my $self = shift;
    my $store = $self->{WORDS};
    my @rv;
    foreach my $id (@_) {
        unless ($store->{$id}) {
            my $novel = $store->{$id} = $self->sgl->word_object( $id );
            $novel->set_results($self);
        }
        push @rv, $store->{$id};
    }
    return @rv;
}

sub word {
    # Returns the specific instance of a word in the results
    my $self = shift;
    my ($req, $create) = @_;
    my $wid;
    if (ref($req)) {
        $wid = $req->id;
    } elsif (exists $self->{WORDS}{$req}) {
        # If we see the request as a word_id key, assume an ID was passed
        # This has a small chance of causing a problem
        $wid = $self->{WORDS}{$req};
    } elsif ($create) {
        # Looks like we have a string, or an ID not in the results
        my ($word) = $self->add_word($req);
        $wid = $word->id;
    } else {
        $wid = $self->sgl->single_word_id( $req );
    }
    if ( $wid && exists $self->{WORDS}{$wid}) {
        return $self->{WORDS}{$wid};
    }
    return undef;
}

sub remove_word {
    my $self = shift;
    foreach my $word (@_) {
        if (my $wid = $self->obj_id($word)) {
            delete $self->{WORDS}{$wid};
            delete $self->{WORDHITS}{$wid};
            map { delete $_->{$wid} } values %{$self->{GENEHITS}};
        }    
    }
}

sub each_word {
    # Will only return genes that have been formally set with add_word()
    return values %{shift->{WORDS}};
}

sub each_hit_word {
    # Will return all words that have been recorded as having a hit
    # with gene_word_hit()
    my $self = shift;
    return map { $self->word($_) || 
                     $self->sgl->word_object( $_ ) } keys %{$self->{WORDHITS}};
}

sub ignored_words {
    # Will return all words that are recorded as hits by gene_word_hit()
    # BUT have not been recorded via add_word()
    my $self = shift;
    my %ids = map { $_ => 1 } keys %{$self->{WORDHITS}};
    map { delete $ids{$_} } keys %{$self->{WORDS}};
    return map { $self->sgl->word_object( $_ ) } keys %ids;
}

sub add_gene {
    my $self = shift;
    my $store = $self->{GENES};
    my @rv;
    foreach my $gene (@_) {
        next unless ($gene);
        my ($id, $obj) = ($gene);
        if (ref($gene)) {
            $id = $gene->id;
            $obj = $gene;
        }
        unless ($store->{$id}) {
            my $novel = $store->{$id} = $obj || 
                $self->sgl->cached_object($id,'gene');
            $novel->set_results($self);
        }
        push @rv, $store->{$id};
    }
    return @rv;
}

sub add_orthologues {
    my $self  = shift;
    my $args  = $self->parseparams( -mod => 0.8,
                                    @_ );
    my $mod   = $args->{MOD} || 0;
    my @genes = $self->each_gene();
    my %orths;
    foreach my $gene (@genes) {
        my $sc  = $gene->score();
        my $acc = $gene->acc();
        foreach my $orth ($gene->orthologues()) {
            my $targ = $orths{ $orth->id } ||= [ $orth, -1 ];
            my $osc  = int(0.5 + 1000 * $sc * $mod) / 1000;
            if ($targ->[1] < $osc) {
                $targ->[1] = $osc;
                $targ->[2] = $acc;
            }
        }
    }
    # Remove any genes we already had:
    map { delete $orths{$_->id()} } @genes;
    my @rv;
    foreach my $odat (values %orths) {
        my ($orth, $sc, $src) = @{$odat};
        # warn "($orth, $sc, $src)";
        $self->add_gene($orth);
        $self->set_gene_score( $orth, $sc );
        $orth->note( "Orthologue of $src" );
        push @rv, $orth;
    }
    return @rv;
}

my $sortingCounter = 0;
sub gene_note {
    my $self = shift;
    my ($gene, $note, $reset) = @_;
    return wantarray ? () : "" unless ($gene);
    my ($id, $obj) = ($gene);
    if (ref($gene)) {
        $id = $gene->id;
        $obj = $gene;
    }
    if (defined $note) {
        if ($note) {
            $self->{GENE_NOTE}{$id} = {} if ($reset);
            $self->{GENE_NOTE}{$id}{$note} ||= ++$sortingCounter;
        } else {
            delete $self->{GENE_NOTE}{$id};
        }
    }
    my $hash = $self->{GENE_NOTE}{$id} || {};
    my @notes = sort { $hash->{$a} <=> $hash->{$b} } keys %{$hash}; 
   return wantarray ? @notes : join(". ", @notes);
}

sub gene {
    # Returns the specific instance of a gene in the results
    my $self = shift;
    my ($gene, $create) = @_;
    if (my $gid = $self->obj_id($gene, 'gene')) {
        unless (exists $self->{GENES}{$gid}) {
            if ($create) {
                $self->add_gene($gid);
            } else {
                return undef;
            }
        }
        return $self->{GENES}{$gid};
    }
    return undef;
}

sub remove_gene {
    my $self = shift;
    foreach my $gene (@_) {
        if (my $gid = $self->obj_id($gene)) {
            delete $self->{GENES}{$gid};
            delete $self->{GENEHITS}{$gid};
            map { delete $_->{$gid} } values %{$self->{WORDHITS}};
        }    
    }
}

sub each_gene {
    # Will only return genes that have been formally set with add_gene()
    return values %{shift->{GENES}};
}

sub each_hit_gene {
    # Will return all genes that have been recorded as having a hit
    # with gene_word_hit()
    my $self = shift;
    return map { $self->gene($_) || 
                     $self->sgl->cached_object($_, 'gene' ) } keys %{$self->{GENEHITS}};
}

sub ignored_genes {
    # Will return all genes that are recorded as hits by gene_word_hit()
    # BUT have not been recorded via add_gene()
    my $self = shift;
    my %ids = map { $_ => 1 } keys %{$self->{GENEHITS}};
    map { delete $ids{$_} } keys %{$self->{GENES}};
    return map { $self->sgl->cached_object($_,'gene' ) } keys %ids;
}

sub add_gene_source {
    my $self = shift;
    my $store = $self->{GENESRCS};
    my @rv;
    foreach my $gene_source (@_) {
        next unless ($gene_source);
        my ($id, $obj) = ($gene_source);
        if (ref($gene_source)) {
            $id = $gene_source->id;
            $obj = $gene_source;
        }
        unless ($store->{$id}) {
            my $novel = $store->{$id} = $obj ||
                $self->sgl->genesource_object( $id );
            $novel->set_results($self);
        }
        push @rv, $store->{$id};
    }
    return @rv;
}

sub gene_source {
    # Returns the specific instance of a gene_source in the results
    my $self = shift;
    my ($gene_source, $create) = @_;
    if (my $gid = $self->obj_id($gene_source)) {
        unless (exists $self->{GENESRCS}{$gid}) {
            if ($create) {
                $self->add_gene_source($gid);
            } else {
                return undef;
            }
        }
        return $self->{GENESRCS}{$gid};
    }
    return undef;
}

sub each_gene_source {
    # Will only return genes that have been formally set with add_gene()
    return values %{shift->{GENESRCS}};
}

# Returns computed score based on word hits
sub gene_score {
    my $self = shift;
    if (my $gid = $self->obj_id(shift)) {
        unless (defined $self->{SCORES}{G}{$gid}) {
            $self->{SCORES}{G}{$gid} = $self->_basic_score( $gid );
        }
        return $self->{SCORES}{G}{$gid};
    }
    return undef;
}

# Manually set the score:
sub set_gene_score {
    my $self = shift;
    if (my $gid = $self->obj_id(shift)) {
        my $nv = shift;
        if (defined $nv) {
            $self->{SCORES}{G}{$gid} = $nv;
        } else {
            delete $self->{SCORES}{G}{$gid};
            return undef;
        }
        return $self->{SCORES}{G}{$gid};
    }
    return undef;
}

sub _basic_score {
    my $self = shift;
    $self->bench_start();
    my $gid  = shift;
    #my @debugBits;
    my $sgl = $self->sgl;
    my $sc  = 0;
    my @wfg = $self->words_for_gene( $gid );
    foreach my $word (@wfg) {
        # words_for_gene() will dynamically set the context_data
        # needed to compute the score
        my $ctx = $word->context_data();
        $sc += $ctx->[0];
        #push @debugBits, $word->word . "=".$ctx->[0];
    }
    my %qHash;
    my @queries = $self->each_query();
    foreach my $query ( @queries ) {
        my $h = $sgl->all_words_for_text($query);
        while (my ($word, $user) = each %{$h}) {
            $qHash{$word} ||= $user;
        }
    }
    my @qWords = sort keys %qHash;
    my $qNum   = $#qWords + 1;
   
    my $mod = 1;
    # This is being dealt with elsewhere - yes?
    # my $mod    = $qNum ? ($#wfg + 1) / $qNum : 1;
    
    
    #$self->msg("Gene ".$sgl->gene_object($gid)->acc(). " = $sc",
    #           (map { "Query: '$_'" } @queries), "$qNum Query Words: ".
    #           (join(",", @qWords) || ""), @debugBits, "Modifier: $mod");
    my $rv = int(0.5 + 1000 * $sc * $mod) / 1000;
    $self->benchend();
    return $rv;
 }

sub best_score {
    my $self = shift;
    my ($best) = sort { ($b || -1) <=> ($a || -1) }
    map { $self->gene_score($_) } $self->each_gene();
    return $best;
}

sub words_for_gene {
    my $self = shift;
    my @rv;
    if (my $gid = $self->obj_id(shift)) {
        if (exists $self->{GENEHITS}{$gid}) {
            while (my ($wid, $dat) = each %{$self->{GENEHITS}{$gid}}) {
                my ($word) = $self->add_word_id($wid);
                $word->context_data( $dat );
                push @rv, $word;
            }
        }
    }
    return @rv;
}

sub add_word_source {
    my $self = shift;
    my $store = $self->{WORDSRCS};
    my @rv;
    foreach my $ws (@_) {
        next unless ($ws);
        my ($id, $obj) = ($ws);
        if (ref($ws)) {
            $id = $ws->id;
            $obj  = $ws;
        }
        unless ($store->{$id}) {
            my $novel = $store->{$id} = $obj ||
                $self->sgl->wordsource_object( $id );
            $novel->set_results($self);
        }
        push @rv, $store->{$id};
    }
    return @rv;
}

*ws = \&word_source;
sub word_source {
    # Returns the specific instance of a word_source in the results
    my $self = shift;
    my ($ws, $create) = @_;
    if (my $wsid = $self->obj_id($ws)) {
        unless (exists $self->{WORDSRCS}{$wsid}) {
            if ($create) {
                $self->add_word_source($wsid);
            } else {
                return undef;
            }
        }
        return $self->{WORDSRCS}{$wsid};
    }
    return undef;
}

sub each_word_source {
    return values %{shift->{WORDSRCS}};
}

our $targTypes = [ 'G', 'W' ];
sub gene_word_hit {
    my $self = shift;
    my ($gene, $word, $wsc, $ws) = @_;
    my $gid   = $self->obj_id($gene) || 0;
    my $wid   = $self->obj_id($word) || 0;
    my $wsid  = $self->obj_id($ws)   || 0;
    my $wtarg = $self->{WORDHITS}{$wid}{$gid} ||= [$wsc, {} ];
    my $gtarg = $self->{GENEHITS}{$gid}{$wid} ||= [$wsc, {} ];
    my @targs = ($wtarg, $gtarg);
    for my $t (0..$#targs) {
        my $targ = $targs[$t];
        if ($wsc > $targ->[0]) {
            # Better score than currently present
            $targ->[0] = $wsc;
            $targ->[1] = { $wsid => 1 };
            # Need to clear past summed score calculations
            my $id = $t ? $gid : $wid;
            delete $self->{SCORES}{$targTypes->[$t]}{$id};
        } elsif ($wsc == $targ->[0]) {
            # Same score as present
            $targ->[1]{ $wsid } = 1;
        }
    }
}

sub add_words_to_genes {
    my $self = shift;
    my ($wordReq, $geneReq) = @_;
    return unless ($wordReq);
    my @genes = $geneReq ?
        ($self->sgl->gene_object($geneReq)) : $self->each_gene();
    return if ($#genes == -1);
    # $self->msg("[GENES]", map { $_->to_text() } @genes);
    my @gids = map { $_->id } @genes;
    my @wids;
    if (my $wr = ref($wordReq)) {
        if ($wr eq 'ARRAY') {
            @wids = @{$wordReq};
        } elsif ($wr eq 'HASH') {
            @wids = keys %{$wordReq};
        }
    }
    return if ($#wids == -1);
    my $sql = "SELECT wh.gene_id, wh.word_id, wh.score, wh.ws_id ".
        "FROM word_hit wh WHERE ";
    $sql .= "wh.gene_id IN (".join(', ', @gids).") AND wh.word_id IN (".
        join(', ', @wids).")";
    my $get = $self->dbh->prepare
        ( -sql   => $sql,
          -name  => "Find word hits for a specific set of genes and words",
          -level => 2 );

    $get->execute();
    # warn $get->pretty_print();
    my $rows = $get->fetchall_arrayref();
    foreach my $row (@{$rows}) {
        $self->gene_word_hit( @{$row} );  
        my ($gid, $wid, $wsc, $wsid) = @{$row};
        # $self->msg("[AW2G]", sprintf("%s [%s] %s %s", $self->sgl->gene_object($gid)->to_text(), $self->sgl->word_object($wid)->to_text(), $wsc, $wsid));
    }

}

sub each_hit {
    my $self = shift;
    my @rv;
    foreach my $gid (keys %{$self->{GENEHITS}}) {
        push @rv, $self->hits_for_gene($gid);
    }
    return @rv;
}

sub keep_all_hits {
    # Simply officially record all genes and words noted in the hits
    my $self = shift;
    
}

sub hits_for_gene {
    my $self = shift;
    my $gene = shift;
    my $gid  = $self->obj_id($gene);
    my @rv;
    my $wH = $gid && exists $self->{GENEHITS}{$gid} ?
        $self->{GENEHITS}{$gid} : {};
    while ( my ($wid, $scdat) = each %{$wH}) {
        my ($wsc, $wsids) = @{$scdat};
        foreach my $wsid (keys %{$wsids}) {
            push @rv, [$gid, $wid, $wsc, $wsid];
        }
    }
    return @rv;
}

sub merge {
    my $self = shift; # The recipient
    my $donor = shift; # Another Results object we will take data from
    # Make sure we bring over the local objects from the donor
    # This does not try to merge those objects, it will simply use them
    # if no native object exists in the recipient
    foreach my $gene ($donor->each_gene()) {
        my @notes = $gene->note();
        $self->add_gene( $gene );
        # We may not have kept every gene for every hit
        # Only bring over hits for which genes have been set
        foreach my $hit ($donor->hits_for_gene( $gene )) {
            $self->gene_word_hit( @{$hit} );
        }
        map { $gene->note($_) } @notes;
    }
    $self->add_word( $donor->each_word() );
    $self->add_word_source( $donor->each_word_source() );
    $self->add_gene_source( $donor->each_gene_source() );
}

sub current_query {
    my $self = shift;
    if (my $nv = shift) {
        $self->{CURRENT_QUERY} = $nv;
        $self->{ALL_QUERIES}{$nv} ||= ++ $self->{ITER};
    }
    return $self->{CURRENT_QUERY} || "";
}

*all_queries = \&each_query;
sub each_query {
    my $self = shift;
    my $hash = $self->{ALL_QUERIES} || {};
    return sort { $hash->{$a} <=> $hash->{$b} } keys %{$hash};
}

sub to_text {
    my $self = shift;
    $self->bench_start();
    my $text = "Results Report:\n";
    my @qs   = $self->all_queries();
    my @wss  = sort { $b->weight <=> $a->weight } $self->each_word_source();
    my @ws   = $self->each_word();
    my @genes  = sort { $b->score(-1) <=> $a->score(-1) } $self->each_gene();
    unless ($#qs == -1) {
        $text .= sprintf("  %d Quer%s:\n", $#qs + 1, $#qs == 0 ? 'y' : 'ies');
        map { $text .= sprintf("    %s\n", $_) } @qs;
    }
    unless ($#wss == -1) {
        $text .= sprintf("  %d Word Source%s:\n", $#wss + 1, $#wss == 0 ? '' : 's');
        map { $text .= sprintf("    %s\n", $_->to_text) } @wss;
    }
    if ($#genes != -1) {
        my $bsc = $self->gene_score($genes[0]);
        $text .= sprintf("  %d Gene%s, best score %s:\n", 
                         $#genes + 1, $#genes == 0 ? '' : 's', $bsc);
        my %gsidH;
        foreach my $gene (@genes) {
            $text .= sprintf("    %s {%s}\n", $gene->to_text(),
                             $self->gene_score($gene));
            $gsidH{$gene->gsid}++;
        }
        my @gsids = sort { $gsidH{$b} <=> $gsidH{$a} } keys %gsidH;
        if ($#gsids != -1) {
            $text .= sprintf("  %d Gene Source%s:\n", 
                             $#gsids + 1, $#gsids == 0 ? '' : 's');
            foreach my $gsid (@gsids) {
                my $gs = $self->gene_source($gsid, 1);
                $text .= sprintf("    %s\n", $gs ? $gs->to_text() : "-?GENESOURCE?-");
            }
        }
    }
    my @gss   = $self->each_gene_source();
    my %observed = map { $_->id => $_ } (@gss, @wss);
    unless ($#ws == -1) {
        $text .= sprintf("  %d Word%s:\n", $#ws + 1, $#ws == 0 ? '' : 's');
        foreach my $word (@ws) {
            $text .= "    ".$word->to_text;
            my @uw = $word->user_words;
            my %un = map { $_->[0] => 1 } @uw;
            delete $un{$word->word};
            my @novel = keys %un;
            unless ($#novel == -1) {
                $text .= ". User provided:";
                foreach my $u (@uw) {
                    $text .= sprintf(" %s (%s)", $u->[0], join(',', @{$u->[1]}));
                }
            }
            my @bl = $word->blacklist();
            #die $self->branch(\%observed);
            if ($#bl != -1) {
                my %wss;
                foreach my $bld (@bl) {
                    my ($f, $wsid, $gsid) = @{$bld};
                    if (my $ws = $observed{$wsid}) {
                        if (my $gs = $observed{$gsid}) {
                            push @{$wss{$ws->name}}, sprintf
                                ("%s = %.2f%%", $gs->name, $f * 100);
                        }
                    }
                }
                my @bltxt;
                foreach my $wsn (sort keys %wss) {
                    push @bltxt, sprintf("%s : %s",$wsn, join
                                         (" / ", @{$wss{$wsn}}));
                }

                unless ($#bltxt == -1) {
                    $text .= " Blacklisted:";
                    map { $text .= "\n      $_" } @bltxt;
                }
            }
            $text .= "\n";
        }
    }
    my @igword = $self->ignored_words;
    unless ($#igword == -1) {
        $text .= sprintf("  %d Ignored Word%s:\n",
                         $#igword + 1, $#igword == 0 ? '' : 's');
        map { $text .= sprintf("    %s\n", $_->to_text()) } @igword;
    }
    if ($#igword == -1 && $#ws == -1) {
        $text .= "  /no words used/\n";
    }

    my @hits = sort { $b->[2] <=> $a->[2] ||
                          $a->[0] <=> $b->[0] } $self->each_hit();
    if ($#hits == -1) {
        $text .= "  /no hits/\n";
    } else {
        $text .= sprintf("  %d Word Hit%s:\n", 
                         $#hits + 1, $#hits == 0 ? '' : 's');
        foreach my $hit (@hits) {
            my ($gid, $wid, $wsc, $wsid) = @{$hit};
            my $gene = $self->gene($gid, 1);
            my ($word) = $self->add_word_id($wid);
            my $ws   = $self->ws($wsid, 1);
            $text .= sprintf("    %s\n      by %s {%s} in %s\n",
                             $gene ? $gene->to_text() : '-?GENE?-',
                             $word ? $word->to_text() : '-?WORD?-', $wsc,
                             $ws   ? $ws->name()   : '-?WORDSOURCE?-');
        }
    }
    
    my @iggene = $self->ignored_genes;
    unless ($#iggene == -1) {
        $text .= "  Ignored Genes:\n";
        map { $text .= sprintf("    %s\n", $_->to_text()) } @iggene;
    }
    $self->bench_end();
    return $text;
}

sub to_xml {
    my $self = shift;
    $self->bench_start();
    my ($indent, $detail) = @_;
    $indent ||= 0;
    my $id2 = $indent + 2;
    my $pad = $indent ? " " x $indent : "";
    my $xml = "$pad<results>\n";
    $xml .= $self->_xmlCom("The results represent the findings of a single search, which involves one or more queries.", $id2);
    $xml .= "\n";
    $xml .= $self->_xml_genes( $id2, $detail )."\n";
    $xml .= $self->_xml_word_sources( $id2, $detail )."\n";
    $xml .= $self->_xml_input( $id2, $detail )."\n";
    $xml .= "$pad</results>\n";
    $self->bench_end();
    return $xml;
}

sub _xml_genes {
    my $self = shift;
    $self->bench_start();
    my ($indent, $detail) = @_;
    my $pad    = $indent ? " " x $indent : "";
    my @genes  = sort { $b->score(-1) <=> $a->score(-1) } $self->each_gene();
    my $num    = $#genes + 1;
    my $bsc    = $num ? $self->gene_score($genes[0]) : 0;
    my $xml    = sprintf("%s<genes%s> %s\n", $pad, $self->_xml_attr( {
        count => $num, bestscore => $bsc }), $self->_xml_bar("==GENE=="));
    $xml .= $self->_xmlCom("Each gene entry provides details for an individual hit gene, or for a non-hit gene that was related to one that was hit.", $indent + 2);
    my %full;
    foreach my $gene (@genes) {
        $xml .= $gene->to_xml( $indent + 2, $detail )."\n";
        $full{ $gene->id }   ||= $gene;
        map { $full{ $_->id} ||= $_ } $gene->orthologues();
    }
    my %isDirect = map { $_->id => 1 } @genes;
    my @orths;
    map { push @orths, $_ unless ($isDirect{$_->id}) } values %full;
    unless ($#orths == -1) {
        $xml .= $self->_xmlCom("RELATED GENES: Orthologues\nThe genes listed below were not directly hit by the query, but are orthologues of one or more of the genes listed above. They may be relevant.", $indent + 2)."\n";
        foreach my $gene (@orths) {
            $xml .= $gene->to_xml( $indent + 2, $detail )."\n";
        }
    }

    $xml .=sprintf("%s</genes> %s\n", $pad, 
                   $self->_xml_bar("==GENE=="));
    my %lookup;
    foreach my $gene (values %full) {
        if (my $taxa = $gene->taxa) {
            push @{$lookup{Taxa}{$taxa}}, $gene;
        }
        if (my $sym = $gene->symbol) {
            push @{$lookup{Symbol}{uc($sym)}}, $gene;
        }
    }
    $xml .= sprintf("\n%s<lookups> %s\n", $pad, $self->_xml_bar("==LOOKUP=="));
    $xml .= $self->_xmlCom("The lookup section simply organizes the genes shown above into potentially useful groupings. The entries below will be sparsely annotated, for detailed information reference the elements above.\n\nTaxa: Groups of genes from a common taxa (species)\nSymbol: Groups of genes with a shared (case-insensitive) gene symbol. Bear in mind that symbols are not guaranteed to be the same across orthologues.", $indent + 2);
    foreach my $lk (sort keys %lookup) {
        my @vals = sort keys %{$lookup{$lk}};
        $xml .= sprintf("  %s<lookup%s>\n", $pad, $self->_xml_attr( {
            count => $#vals + 1, type => $lk }));
        foreach my $val (@vals) {
            my @genes = sort { $a->id <=> $b->id } @{$lookup{$lk}{$val}};
            my ($bsc) = sort { $b <=> $a } map { $_->score || 0 } @genes;
            $xml .= sprintf("    %s<lookupSet%s>\n", $pad, $self->_xml_attr( {
                count => $#genes + 1, set => $val, type => $lk, bestscore => $bsc }));
            map { $xml .= $_->to_xml( $indent + 6, 0 ) } @genes;
            $xml .= sprintf("    %s</lookupSet>\n", $pad);
        }
        $xml .= sprintf("  %s</lookup>\n", $pad);
    }
    $xml .= sprintf("\n%s</lookups> %s\n", $pad, $self->_xml_bar("==LOOKUP=="));
    $self->bench_end();
    return $xml
}

sub _xml_word_sources {
    my $self = shift;
    $self->bench_start();
    my ($indent, $detail) = @_;
    my $pad = $indent ? " " x $indent : "";
    my @wss = sort { $b->weight <=> $a->weight } $self->each_word_source();
    my $xml = sprintf("%s<wordSources%s> %s\n", $pad, $self->_xml_attr( {
        count => $#wss + 1 }), $self->_xml_bar("==WS=="));
    $xml .= $self->_xmlCom("WordSources represent the different types of annotations that provide searchable words to a gene. Each source has a base score weight (larger = more informative), a splitting pattern for breaking text into words, and a stripping pattern for removing non-informative characters after splitting.", $indent + 2);
    map { $xml .= $_->to_xml( $indent + 2, $detail ) } @wss;
    $xml   .= sprintf("%s</wordSources> %s\n", $pad, 
                      $self->_xml_bar("==WS=="));
    $self->bench_end();
    return $xml;
}

sub _xml_input {
    my $self = shift;
    $self->bench_start();
    my ($indent, $detail) = @_;
    my $pad = $indent ? " " x $indent : "";
    my $xml ="$pad<input> ".$self->_xml_bar("==INP==")."\n";
    $xml .= $self->_xmlCom("This section describes the search criteria used to execute the search.", $indent + 2);

    $xml .= "\n";
    my @qs   = $self->all_queries();
    $xml .= sprintf("  %s<queries%s", $pad, $self->_xml_attr( {
        count => $#qs + 1 }));
    if ($#qs == -1) {
        $xml .= " />\n";
        $xml .= $self->_xmlCom("No queries were noted for some reason", $indent + 4);
    } else {
        $xml .= ">\n";
        $xml .= $self->_xmlCom("Queries are the full strings passed by the user to the search before either splitting, stripping or uppercasing have occured.", $indent + 4);
        for my $i (0..$#qs) {
            my $q = $qs[$i];
            $xml .= sprintf("    %s<query%s>%s</query>\n", $pad, 
                            $self->_xml_attr( { charlen => length($q),
                                                id => 'Query'.($i+1),}),
                            $self->esc_xml($q));
        }
        $xml .= "  $pad</queries>\n";
    }
    $xml .= "\n";

    my @ws = $self->each_word();
    $xml .= sprintf("  %s<words%s>\n", $pad, $self->_xml_attr( {
        count => $#ws + 1 }));
    $xml .= $self->_xmlCom("Words are the actual search terms used to query the database. They are generated from user queries by splitting and stripping according to the wordSource being searched, and then converted to full uppercase.\nUserWords represent the user input after splitting, but before character stripping and uppercasing occur.\nWords that occur frequently in a particular gene source + word source will be 'blacklisted'. A blacklisted word will not return hits by itself, but can contribute to a gene's score if a non-blacklisted word recovers that gene", $indent + 4);
    foreach my $word (@ws) {
        $xml .= $word->to_xml($indent + 4, $detail);
    }
    $xml .= "  $pad</words>\n";
    $xml .= "$pad</input> ".$self->_xml_bar("==INP==")."\n";
    $self->bench_end();
    return $xml;
}

sub xml_help {
    my $self = shift;
    $self->bench_start();
    my ($indent, $detail) = @_;
    my $coms = <<COM;
Common XML Attributes:
dbid : The internal primary key for the backend search database
wid : A word ID
gid : A gene ID
wsid : A word_source ID
gsid : A gene_source ID
word : A normalized word used for searching
COM

    $self->bench_end();
    return $self->_xmlCom($coms);

}

sub to_html {
    my $self = shift;
    $self->bench_start();
    my $args = $self->parseparams( -detail => 0,
                                   @_ );
    my $html = "<div class='results'>\n";
    $html .= "<div class='queries'>".join(" ", map { $_->to_html() } $self->each_word())."</div>";
    foreach my $gene (sort { $b->score(-1) <=>
                                 $a->score(-1) }  $self->each_gene()) {
        $html .= $gene->to_html();
    }
    $html .= "</div>\n";
    $self->bench_end();
    return $html;
}

sub to_json {
    my $self = shift;
    my $args = $self->parseparams( -detail => 0,
                                   @_ );
    my $detail = $args->{DETAIL};
    my $hash = $self->to_hash($detail);
    my $json = $self->obj_to_json($hash, $args->{PRETTY} ? 0 : undef);
    if (my $cb = $args->{JSONP} || $args->{CALLBACK}) {
        $json = "$cb($json)";
    }
    return $json;
}

sub to_hash {
    my $self = shift;
    my ($detail) = @_;
    my @genes  = map { $_->to_hash($detail) } 
    sort { $b->score(-1) <=> $a->score(-1) } $self->each_gene();
    my @wss    = map { $_->to_hash($detail) }
    sort { $b->weight <=> $a->weight } $self->each_word_source();
    my $rv = {
        genes => \@genes,
        wordsources => \@wss,
        input => $self->_input_hash(),
        help => $self->_help_hash(),
    };
    return $rv;
}

sub _input_hash {
    my $self = shift;
    my ($detail) = @_;
    my @qs   = $self->all_queries();
    my @ws   =  map { $_->to_hash($detail) } $self->each_word();
    my $rv   = {
        words => \@ws,
        database => $self->sgl->dbname,
    };
    my $qArr = $rv->{queries} = [];
    for my $i (0..$#qs) {
        my $q = $qs[$i];
        push @{$qArr}, {
            charlen => length($q),
            id => 'Query'.($i+1),
            query => $q,
        };
    }
    return $rv;
}

sub _help_hash {
    return {
        psych => "Some day this hash will have help describing the data structure",
    };
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::StandardGeneLight::Task;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use vars qw(@ISA);
@ISA = qw(BMS::MapTracker::StandardGeneLight::Object);
use Scalar::Util qw(weaken);

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my ($sgl, $req) = @_;
    my $self = { };
    bless ($self, $class);
    weaken($self->{SGL}  = $sgl);
    my @info = $sgl->get_task( $req );
    $self->{BASE} = {
        type   => 'manual_task',
        id     => $info[0],
        name   => $info[1],
    };
    return $self;
}

sub name   { return shift->{BASE}{name}; }

sub each_member {
    my $self = shift;
    # Do this live, rather than caching
    my $sgl = $self->sgl;
    my %tab = ( tm => 'task_member' );
    my @wc  = ("tm.task_id = ?");
    my @binds = ($self->id);
    my $sql = "SELECT tm.member_id, tm.external_id FROM ".
        join(", ", map { "$tab{$_} $_" } sort keys %tab). 
        " WHERE ".join(" AND ", @wc);

    my $get = $sgl->dbh->prepare
        ( -name   => "Get task members",
          -sql    => $sql,
          -level  => 3);
    # $get->execute($self->id);
    my $rows = $get->selectall_arrayref( $self->id );
    my @tms;
    foreach my $row (@{$rows}) {
        push @tms, BMS::MapTracker::StandardGeneLight::TaskMember->new
            ($self, @{$row});
    }
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::StandardGeneLight::TaskMember;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use vars qw(@ISA);
@ISA = qw(BMS::MapTracker::StandardGeneLight::Object);
use Scalar::Util qw(weaken);

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my ($task, $id, $exid) = @_;
    my $self = { };
    bless ($self, $class);
    weaken($self->{TASK}  = $task);
    $self->{BASE} = {
        type   => 'task_member',
        id     => $id,
        name   => $exid,
    };
    return $self;
}

sub task { return shift->{TASK}; }
sub sgl  { return shift->task->sgl(); }

*acc = \&name;
*external_id = \&name;
sub name   { return shift->{BASE}{name}; }

sub each_assignment {
    my $self = shift;
}
