# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::WhatIsIt;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use strict;
use BMS::Utilities::Benchmark;
use BMS::FriendlyDBI;
use BMS::MapTracker::GenAccService;

use vars qw(@ISA);
@ISA = qw( BMS::Utilities::Benchmark );

our $doNotAlterDB = 0;
sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
        VB     => 0,
        STORED_ERRS => [],
    };
    bless ($self, $class);
    $self->_connect( @_ );
    return $self;
}

sub dbh { return shift->{DBH}; }
sub mainseq { return shift->{DBH}->nextval('main_seq'); }
sub param {
    my $self = shift;
    my $key  = shift;
    return undef unless ($key);
    $key     = uc($key);
    my $val  = shift;
    $self->{PARAMS}{$key} = $val if (defined $val);
    return $self->{PARAMS}{$key};
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

sub id_for_text {
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
              -ignore => 'duplicate key value',
              -level => 3);
        $add->execute( $seq, $txt );
        $tid  = $get->get_single_value( $txt );
    }
    $self->bench_end();
    return $tid || 0;
}

sub cached_id_for_text {
    my $self = shift;
    my $txt = shift || "";
    unless (defined $self->{TXT_ID_CACHE}{$txt}) {
        $self->{TXT_ID_CACHE}{$txt} = $self->id_for_text( $txt );
    }
    return $self->{TXT_ID_CACHE}{$txt};
}

sub unknown_id {
    return shift->cached_id_for_text("Unknown");
}

sub temp_id_list {
    my $self = shift;
    $self->bench_start();
    my ($list, $tt) = @_;
    $self->dbh->list_to_temp_table
        ( $list, [ "integer" ], [ "member" ], $tt  );
    $self->bench_end();
}

*temp_txt_list = \&temp_txt_list;
sub temp_text_list {
    my $self = shift;
    $self->bench_start();
    my ($list, $tt) = @_;
    $self->dbh->list_to_temp_table
        ( $list, [ "text" ], [ "member" ], $tt  );
    $self->bench_end();
}

sub temp_list_values {
    my $self = shift;
    $self->bench_start();
    my $tt   = shift;
    my $getList = $self->dbh->prepare
        ( -name => "Just read contents of a temp list",
          -sql  => "SELECT tl.member FROM $tt tl",
          -level => 3);
    my @all = $getList->get_array_for_field();
    $self->bench_end();
    return \@all;
}

*bulk_txt_ids = \&bulk_ids_for_text;
sub bulk_ids_for_text {
    my $self = shift;
    $self->bench_start();
    my $dbh  = $self->dbh();
    my $list = shift;
    my $tt   = "list_of_texts";
    if (ref($list)) {
        # Array passed
        $self->temp_txt_list( $list, $tt );
    } else {
        # name of temporary table passed
        $tt = $list;
    }
    my $sth = $dbh->prepare
        ( -name => "Get normtxt ID for string list",
          -sql  => "SELECT tl.member, t.txt_id FROM $tt tl ".
          "LEFT OUTER JOIN normtxt t ON tl.member = t.txt",
          -level => 3);
    $sth->execute(  );
    my $rows = $sth->fetchall_arrayref();
    my %rv   = map { $_->[0] => $_->[1] || $self->id_for_text($_->[0]) } @{$rows};
    $self->bench_end();
    return \%rv;
}

sub bulk_text_for_ids {
    my $self = shift;
    $self->bench_start();
    my $dbh  = $self->dbh();
    my $list = shift;
    my $tt   = "list_of_ids";
    my $rv;
    if (ref($list)) {
        # Array passed
        $self->temp_id_list( $list, $tt );
    } else {
        # name of temporary table passed
        $tt = $list;        
    }
    my $sth = $dbh->prepare
        ( -name => "Get strings for normtxt IDs",
          -sql  => "SELECT t.txt_id, t.txt FROM normtxt t, $tt tl ".
          "WHERE t.txt_id = tl.member",
          -level => 3);
    $sth = $dbh->prepare
        ( -name => "Get strings for normtxt IDs",
          -sql  => "SELECT tl.member, t.txt FROM $tt tl LEFT OUTER JOIN normtxt t ON tl.member = t.txt_id",
          -level => 3);
    $sth->execute(  );
    my $rows = $sth->fetchall_arrayref();
    my %rv   =  map { $_->[0] => $_->[1] } @{$rows};
    $self->bench_end();
    return \%rv;
}

sub type_for_ids {
    my $self = shift;
    $self->bench_start();
    my $dbh  = $self->dbh();
    my $list = shift;
    my $tt   = "ids_to_type";
    my $redo = lc($self->param('refresh') || "");
    my $rv;
    if (ref($list)) {
        # Array passed
        $self->temp_id_list( $list, $tt );
    } else {
        # name of temporary table passed
        $tt = $list;
        $list = $self->temp_list_values( $tt );
    }
    my (%rv, @need);
    if ($redo =~ /(typ|all)/) {
        # Force recalculate
        @need = @{$list};
    } else {
        my $sth = $redo =~ /null/ ? 
            $self->{STHS}{READ_type_for_objid} ||= $dbh->prepare
            ( -name => "Get object types for normtxt ID",
              -sql  => "SELECT ot.type_id FROM objtype ot WHERE ot.obj_id = ? AND ot.type_id != ".$self->unknown_id(),
              -level => 3)
            :
            $self->{STHS}{READ_type_for_objid} ||= $dbh->prepare
            ( -name => "Get object types for normtxt ID",
              -sql  => "SELECT ot.type_id FROM objtype ot WHERE ot.obj_id = ?",
              -level => 3);
        foreach my $id (@{$list}) {
            if (my $typ = $sth->get_single_value( $id )) {
                $rv{$id} = $typ;
            } else {
                push @need, $id;
            }
        }
    }
    unless ($#need == -1) {
        my $i2t  = $self->bulk_text_for_ids( \@need );
        my $t2i  = {};
        my @txts = &_process_i2t( $i2t, $t2i );
        my $rows = $self->forked_convert
            ( -ids => \@txts,
              -mode => 'simple',
              -cols => 'termin,nsin' );
        my $set = $self->{STHS}{INSERT_ObjType} ||= $dbh->prepare
            ( -name => "Set object types for normtxt id",
              -sql  => "INSERT INTO objtype (obj_id, type_id) VALUES (?, ?)",
              -level => 3);


        my (%nsid, %types);
        my %needH = map { $_ => 1 } @txts;
        my $uid = $self->unknown_id();
        foreach my $row (@{$rows}) {
            my ($txt, $ns) = @{$row};
            if (my $tid = $self->_unmap_t2i($txt, $t2i, $ns)) {
                if ($ns) {
                    my $typ = $nsid{$ns} ||= $self->id_for_text($ns);
                    $set->execute($tid, $typ);
                    $rv{$tid} = $typ;
                } else {
                    # Do not bother storing
                    $rv{$tid} = $uid;
                }
                delete $needH{$txt};
            } else {
                $self->store_err("Failed to convert '$txt' [".($ns || '?').
                                 "] back to ID");
            }
        }
        my @unk = keys %needH;
        if ($#unk != -1) {
            map {  $set->execute( $self->_unmap_t2i($_, $t2i), $uid); } @unk;
        }
    }
    $self->bench_end();
    return \%rv;
}

sub _process_i2t {
    my ($i2t, $t2i) = @_;
    $t2i ||= {};
    my @txts;
    while (my ($id, $txt) = each %{$i2t}) {
        push @txts, $txt;
        $t2i->{$txt}       = $id;
        $t2i->{uc($txt)} ||= $id;
    }
    return @txts;
}

sub _unmap_t2i {
    my $self = shift;
    my ($txt, $t2i, $ns) = @_;
    my $utxt = uc($txt);
    my $rv = $t2i->{$txt} || $t2i->{$utxt};
    return $rv if ($rv || !$ns);
    if ($ns eq 'Gene Symbol') {
        return $t2i->{$txt.'*'}  || $t2i->{$txt.'~'} ||
            $t2i->{$utxt.'*'} || $t2i->{$utxt.'~'} ||
            $t2i->{$utxt.'*~'} || $t2i->{$txt.'*~'} || undef;
    }
    if ($ns eq 'SMILES ID' && $txt =~ /^MTID:(\d+)$/) {
        if (my $seq = $self->tracker->get_seq($1)) {
            return $self->_unmap_t2i( $seq->name, $t2i, $ns );
        }
    }
    return undef;
}

sub sets_for_ids {
    my $self = shift;
    $self->bench_start();
    my $dbh  = $self->dbh();
    my $list = shift;
    my $tt   = "ids_to_sets";
    my $redo = lc($self->param('refresh') || "");
    my $rv;
    if (ref($list)) {
        # Array passed
        $self->temp_id_list( $list, $tt );
    } else {
        # name of temporary table passed
        $tt = $list;        
        $list = $self->temp_list_values( $tt );
    }
    my (%rv, @need);
    if ($redo =~ /(typ|all)/) {
        @need = @{$list};
    } else {
        my $sth = $self->{STHS}{READ_setdata_for_objid} ||= $dbh->prepare
            ( -name => "Get set information for normtxt ID",
              -sql  => "SELECT os.set_id, os.conf, os.note_id FROM objset os WHERE os.obj_id = ?",
              -level => 3);
        foreach my $id (@{$list}) {
            $sth->execute( $id );
            my $rows = $sth->fetchall_arrayref();
            if ($#{$rows} == -1 || !defined $rows->[0][1]) {
                # die $self->text_for_id($id).$self->branch($rows);
                push @need, $id;
            } else {
                push @{$rv{$id}}, @{$rows};
            }
        }
    }
    unless ($#need == -1) {
        my $i2t   = $self->bulk_text_for_ids( \@need );
        my $types = $self->type_for_ids(\@need);
        my $t2i  = {};
        my @txts = &_process_i2t( $i2t, $t2i );
        my %needH = map { $_ => 1 } @txts;
        my %byType;
        while (my ($tid, $typ) = each %{$types}) {
            push @{$byType{$typ}}, $i2t->{$tid};
        }
        my (%sets, %tmap);
        while (my ($typ, $txts) = each %byType) {
            my $ns    = $self->text_for_id($typ);
            my ($targNS) = ('SET');
            my $rows = $self->forked_convert
                ( -ids    => $txts,
                  -mode   => 'convert',
                  -ns1    => $ns,
                  -ns2    => $targNS,
                  -nonull => 1,
                  -nullscore => -1,
                  -cols   => 'termin,termout,score,auth' );
           
            foreach my $r (@{$rows}) {
                my $txt = shift @{$r};
                if (my $tid = $self->_unmap_t2i($txt, $t2i, $ns)) {
                    $r->[0] = $tmap{ $r->[0] } ||= $self->id_for_text($r->[0]);
                    $r->[2] = $tmap{ $r->[2] } ||= $self->id_for_text($r->[2]);
                    push @{$sets{$tid}}, $r;
                    push @{$rv{$tid}}, $r;
                    delete $needH{$txt};
                } else {
                    $self->store_err("Failed to convert '$txt' [$ns] back to ID");
                }
            }
        }
        # Use a score of -2 to record "no sets found"
        map { $sets{$self->_unmap_t2i($_, $t2i)} ||= 
                  [ [undef, -2, undef] ] } keys %needH;

        my $set = $self->{STHS}{INSERT_ObjSet} ||= $dbh->prepare
            ( -name => "Set object sets for normtxt id",
              -sql  => "INSERT INTO objset (obj_id, set_id, conf, note_id) VALUES (?, ?, ?, ?)",
              -level => 3);

        my $clear = $self->{STHS}{DELETE_ObjSet} ||= $dbh->prepare
            ( -name => "Clear object sets for normtxt id",
              -sql  => "DELETE FROM objset WHERE obj_id = ?",
              -level => 3);

        while (my ($tid, $rows) = each %sets) {
            $clear->execute($tid);
            map { $set->execute( $tid, @{$_} ) } @{$rows};
        }
    }
    $self->bench_end();
    return \%rv;
}

*meta_for_ids = \&metadata_for_ids;
sub metadata_for_ids {
    my $self = shift;
    $self->bench_start();
    my $dbh  = $self->dbh();
    my $list = shift;
    my $tt   = "ids_to_meta";
    my $redo = lc($self->param('refresh') || "");
    if (ref($list)) {
        # Array passed
        $self->temp_id_list( $list, $tt );
    } else {
        # name of temporary table passed
        $tt = $list;
        $list = $self->temp_list_values( $tt );
    }
    my ($sth, %i2t);
    my %rv = map { $_ => {} } @{$list};
    unless ($redo =~ /(meta|all)/) {
        my $sth = $dbh->prepare
            ( -name => "Get metadata for normtxt IDs",
              -sql  => "SELECT kv.key_id, kv.val_id, kv.conf ".
              "FROM keyval kv WHERE kv.obj_id = ?",
              -level => 3);
        foreach my $tid (@{$list}) {
            $sth->execute( $tid );
            my $rows = $sth->fetchall_arrayref();
            foreach my $row (@{$rows}) {
                my ($ki, $vi, $conf) = @{$row};
                if ($ki) {
                    my $key = $i2t{$ki} ||= $self->text_for_id($ki);
                    if ($vi) {
                        my $val = $i2t{$vi} ||= $self->text_for_id($vi);
                        $rv{$tid}{$key}{$val} = $conf;
                    } else {
                        $rv{$tid}{$key} ||= {};
                    }
                } else {
                    $rv{$tid} ||= {};
                }
            }
        }
    }
    my $types = $self->type_for_ids($tt);
    my %byType;
    while (my ($tid, $typ) = each %{$types}) {
        push @{$byType{$typ}}, $tid;
    }

    my %update;
    while (my ($typ, $tids) = each %byType) {
        my $ns = $self->text_for_id($typ);
        # What are the criteria for already having data?
        my ($check, $doDesc, $doTax, $doSym, @derived);
        if ($ns eq 'Affy Probe Set') {
            $check = "Description";
            ($doDesc, $doTax, $doSym) = (1, 1, 1);
            @derived = ("RefSeq RNA", "Ensembl Transcript",
                        "LocusLink Gene", "Ensembl Gene");
        } elsif ($ns eq 'Gene Symbol') {
            $check = "LocusLink Gene";
            $doTax = 1;
            @derived = ("LocusLink Gene");

        } elsif ($ns eq 'RefSeq Protein' || $ns eq 'RefSeq RNA') {
            $check = "LocusLink Gene";
            ($doDesc, $doTax, $doSym) = (1, 1, 1);
            @derived = ($check, "Swiss-Prot", "UniProt");
            push @derived, $ns =~ /Prot/ ? "RefSeq RNA" : "RefSeq Protein";

        } elsif ($ns eq 'Ensembl Protein' || $ns eq 'Ensembl Transcript') {
            $check = "Ensembl Gene";
            ($doDesc, $doTax, $doSym) = (1, 1, 1);
            @derived = ($check, "Swiss-Prot", "UniProt");
            push @derived, $ns =~ /Prot/ ? "RefSeq RNA" : "RefSeq Protein";

        } elsif ($ns eq 'Ensembl Gene') {
            $check = "Symbol";
            ($doDesc, $doTax, $doSym) = (1, 1, 1);
            @derived = ('Ensembl Protein', 'Ensembl Transcript', 
                        "Swiss-Prot", "UniProt");

        } elsif ($ns eq 'Wikipedia Article') {
            $check = "WiiChecked";
            ($doDesc, $doTax, $doSym) = (1, 1, 1);
            @derived = ("LocusLink Gene");

        } elsif ($ns eq 'UniProt Name') {
            $check = "UniProt";
            ($doDesc, $doTax, $doSym) = (1, 1, 1);
            @derived = ("UniProt", "LocusLink Gene");

        } elsif ($ns eq 'LocusLink Gene') {
            $check = "Symbol";
            ($doDesc, $doTax, $doSym) = (1, 1, 1);
            @derived = ("RefSeq RNA", "RefSeq Protein", "Swiss-Prot", "UniProt");

        } elsif ($ns eq 'Namespace Name') {
            $check = "Description";
            $doDesc = 1;

        } elsif ($ns eq 'BMS Compound ID') {
            $check = "Description";
            $doDesc = 1;

        } elsif ($ns eq 'SMILES ID' || $ns eq 'SMDL Index' 
                 || $ns eq 'Any Chemical') {
            $check = "Description";
            $doDesc = 1;
            @derived = ("BMS Compound ID");

        } elsif ($ns eq 'Unknown') {
            $check = "Description";
            $doDesc = 1;

        } elsif ($ns eq 'Any Protein' || $ns eq 'Any RNA' || $ns eq 'UniProt'
                 || $ns eq 'Swiss-Prot') {
            $check = "Description";
            ($doDesc, $doTax, $doSym) = (1, 1, 1);
            @derived = ("LocusLink Gene");

        } elsif ($ns eq 'Sequence Feature') {
            # Nothing to really do here
        } else {
            $self->msg_once("[!]", "No logic for recovering metadata for '$ns'");
        }
        next unless ($check);
        my @need;
        foreach my $tid (@{$tids}) {
            push @need, $tid unless (exists $rv{$tid}{$check});
        }
        next if ($#need == -1);

        $self->bench_start("GenAcc");
        map { $update{$_} ||= {} } @need;
        my $i2t    = $self->bulk_text_for_ids( \@need );
        my $t2i    = {};
        my @txts   = &_process_i2t( $i2t, $t2i );
        if ($doDesc) {
            my $rows = $self->forked_convert
                ( -ids    => \@txts,
                  -mode   => 'description',
                  -ns1    => $ns,
                  -nonull => 1,
                  -nullscore => -1,
                  -cols   => 'termin,desc' );
            foreach my $row (@{$rows}) {
                my ($txt, $desc) = @{$row};
                if (my $id = $self->_unmap_t2i($txt, $t2i, $ns)) {
                    $update{$id}{Description}{$desc} = 1 if ($desc);
                } else {
                    $self->store_err("Failed to map '$txt' [$ns] to ID via Description");
                }
            }
        }
        if ($doTax) {
            my $rows = $self->forked_convert
                ( -ids    => \@txts,
                  -ns1    => $ns,
                  -ns2    => 'TAX',
                  -nonull => 1,
                  -nullscore => -1,
                  -cols   => 'termin,termout,score' );
            foreach my $row (@{$rows}) {
                my ($txt, $tax, $sc) = @{$row};
                if (my $id = $self->_unmap_t2i($txt, $t2i, $ns)) {
                    $update{$id}{Taxa}{$tax} = $sc;
                } else {
                    $self->store_err("Failed to map '$txt' [$ns] to ID via TAX");
                }
            }
        }
        if ($doSym) {
            my $rows = $self->forked_convert
                ( -ids    => \@txts,
                  -ns1    => $ns,
                  -ns2    => 'SYM',
                  -nonull => 1,
                  -nullscore => -1,
                  -cols   => 'termin,termout,score' );
            foreach my $row (@{$rows}) {
                my ($txt, $sym, $sc) = @{$row};
                # die "$txt, $sym, $sc" unless (defined $sc);
                if (my $id = $self->_unmap_t2i($txt, $t2i, $ns)) {
                    my $key = $sc >= 0.7 ? "Symbol" : "Unofficial Symbol";
                    $update{$id}{$key}{$sym} = $sc;
                } else {
                    $self->store_err("Failed to map '$txt' [$ns] to ID via SYM");
                }
            }
        }
        foreach my $ns2 (@derived) {
            my $rows = $self->forked_convert
                ( -ids    => \@txts,
                  -ns1    => $ns,
                  -ns2    => $ns2,
                  -nonull => 1,
                  -nullscore => -1,
                  -cols   => 'termin,termout,score' );
            foreach my $row (@{$rows}) {
                my ($txt, $out, $sc) = @{$row};
                if (my $id = $self->_unmap_t2i($txt, $t2i, $ns)) {
                    $update{$id}{$ns2}{$out} = $sc;
                } else {
                    $self->store_err("Failed to map '$txt' [$ns] to ID via $ns2");
                }
            }
        }
        foreach my $tid (@need) {
            $update{$tid}{$check} = {} unless ($update{$tid}{$check});
        }
        $self->bench_end("GenAcc");
    }
    # warn $self->branch(\%update);
    my @tids = keys %update;
    unless ($#tids == -1) {
        $self->bench_start("Update");
        my $set = $self->{STHS}{INSERT_KeyVal} ||= $dbh->prepare
            ( -name => "Set object keyval for normtxt id",
              -sql  => "INSERT INTO keyval (obj_id, key_id, val_id, conf) VALUES (?, ?, ?, ?)",
              -level => 3);
        my $clear = $self->{STHS}{CLEAR_KeyVal} ||= $dbh->prepare
            ( -name => "Set object keyval for normtxt id",
              -sql  => "DELETE FROM keyval WHERE obj_id = ? AND key_id = ?",
              -level => 3);
        my %t2i;
        while (my ($tid, $kH) = each %update) {
            $rv{$tid} = $kH;
            while (my ($key, $vH) = each %{$kH}) {
                next unless ($key);
                my @toAdd;
                my $kid = $t2i{$key} ||= $self->id_for_text($key);
                my @vals = keys %{$vH};
                if ($#vals == -1) {
                    push @toAdd, [$tid, $kid, undef, undef ];
                } else {
                    foreach my $val (@vals) {
                        my $vid = $t2i{$val} ||= $self->id_for_text($val);
                        push @toAdd, [$tid, $kid, $vid, $vH->{$val}];
                    }
                }
                $clear->execute($tid, $kid);
                map { $set->execute( @{$_} ) } @toAdd;
            }
        }
        $self->bench_end("Update");
    }
    $self->bench_end();
    return \%rv;
}

sub forked_convert {
    my $self = shift;
    my $file = "WhatIsIt-$$.tsv";
    unlink($file);
    my $gas = BMS::MapTracker::GenAccService->new
        ( -fork    => 15,
          -ignorecase => 1,
          @_,
          -format   => 'tsv',
          -age      => $self->param('age'),
          -cloudage => $self->param('cloudage'),
          -warn     => $self->param('warn'),
          -splitter => $self->param('splitter'),
          -verbose  => 0,
          -output   => $file,
          -scramble => 1 );
    $gas->use_beta( 1 );
    my $rows = $gas->cached_array(  );
    unlink($file);
    return $rows;
}

sub denorm {
    my $self = shift;
    unless ($self->{AD}) {
        my $foo = BMS::MapTracker::GenAccService->new();
        $self->{AD} = $foo->denorm();
    }
    return $self->{AD};
}

sub tracker {
    my $self = shift;
    unless ($self->{MT}) {
        $self->{MT} = $self->denorm()->tracker();
    }
    return $self->{MT};
}

sub _connect {
    my $self = shift;
    $self->bench_start();
    my $dbName = 'whatisit';
    my ($dbType, $dbn, $dbh);
    $ENV{PGPORT} = 5433;
    $ENV{PGHOST} = 'elephant.pri.bms.com';

    $dbType = "dbi:Pg:dbname=$dbName";
    $dbn    = 'tilfordc',
    eval {
        $dbh = BMS::FriendlyDBI->connect
            ($dbType, $dbn,
             undef, { RaiseError  => 0,
                      PrintError  => 0,
                      LongReadLen => 100000,
                      AutoCommit  => 1, },
             -errorfile => '/scratch/WhatIsItErrors.err',
             -adminmail => 'tilfordc@bms.com', );
    };
    if ($dbh) {
        $dbh->schema( $self->schema() );
    } else {
        $self->err("Failed to connect to StandardGene database '$dbName'");
    }
    $self->bench_end();
    return $self->{DBH} = $dbh;
}

sub schema {
    my %tables;

    $tables{ "normtxt" } =
    { name  => 'normtxt',
      com   => 'Table holding normalized text. Case sensitive and allows spaces',
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

           ['txt','text','Any string' ],
           ] };

    $tables{ "keyval" } =
    { name  => 'keyval',
      com   => 'Key-Value pairs for any object referenced by a primary ID',
      sequence => { 
          'main_seq' => 1,
      },
      index => {
          kv_primary   => {
              cols => [ 'obj_id', 'key_id' ],
          },
          kv_key_and_val   => {
              cols => [ 'key_id', 'val_id' ],
          },
      },
      fkey  => {
          obj_id => 'normtxt.txt_id',
          key_id => 'normtxt.txt_id',
          val_id => 'normtxt.txt_id',
      },
      cols  => 
          [['obj_id', 'integer',
            'Integer foreign key for the object being annotated.' ],

           ['key_id', 'integer',
            'Keyname associated with the annotation, points to NORMTXT' ],

           ['val_id', 'integer',
            'Value associated with the annotation, points to NORMTXT' ],

           ['conf', 'real',
            'A number reflecting the confidence of the key/val assignment.' ],

           ] };

    $tables{ "objtype" } =
    { name  => 'objtype',
      com   => 'Association of an object with a controlled type',
      index => {
          ot_primary   => {
              cols => [ 'obj_id', 'type_id' ],
          },
          ot_type   => {
              cols => [ 'type_id' ],
          },
      },
      fkey  => {
          obj_id => 'normtxt.txt_id',
          type_id => 'normtxt.txt_id',
      },
      cols  => 
          [['obj_id', 'integer',
            'Integer foreign key for the object being annotated.' ],

           ['type_id', 'integer',
            'The type assigned to the object' ]],
       };

    $tables{ "objset" } =
    { name  => 'objset',
      com   => 'Association of an object with a set',
      index => {
          os_primary   => {
              cols => [ 'obj_id', 'set_id' ],
          },
          os_set   => {
              cols => [ 'set_id' ],
          },
      },
      fkey  => {
          obj_id => 'normtxt.txt_id',
          set_id => 'normtxt.txt_id',
          note_id => 'normtxt.txt_id',
      },
      cols  => 
          [['obj_id', 'integer',
            'Integer foreign key for the object being annotated.' ],

           ['set_id', 'integer',
            'The set containing the object' ],

           ['conf', 'real',
            'A number reflecting the confidence of the set assignment.' ],

           ['note_id', 'integer',
            'Notes associated with the assignment' ]],
       };
    
   $tables{ "v_type" } =
    { name  => 'v_type',
      com   => 'Denormalized view of object type assignments',
      view  =>
"
 SELECT ot.obj_id, ot.type_id, t1.txt AS Object, t2.txt AS Type
   FROM objtype ot, normtxt t1, normtxt t2
  WHERE t1.txt_id = ot.obj_id
    AND t2.txt_id = ot.type_id
"
};

   $tables{ "v_set" } =
    { name  => 'v_set',
      com   => 'Denormalized view of object set assignments',
      view  =>
"
 SELECT os.obj_id, t1.txt AS Object, t2.txt AS Set, os.conf, t3.txt AS Note
   FROM objset os
   LEFT OUTER JOIN normtxt t2 ON t2.txt_id = os.set_id
   LEFT OUTER JOIN normtxt t3 ON t3.txt_id = os.note_id
   JOIN normtxt t1 ON t1.txt_id = os.obj_id
"
};

    $tables{ "v_clients" } =
    { name  => 'v_clients',
      com   => 'Show clients currently connected to postgres',
      db    => 'postgres',
      view  =>
"
SELECT datname, usename,
       date_trunc('second'::text, now() - backend_start) AS backend_age,
       client_addr 
  FROM pg_stat_activity 
 ORDER BY datname, client_addr, backend_age
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

    $tables{ "v_xid" } =
    { name  => 'v_xid',
      com   => 'Shows state of transaction IDs in Postgres',
      db    => 'postgres',
      view  =>
"
 SELECT c.relname, s.usename, c.relpages::double precision / 1000::double precision AS kilopages, floor(c.reltuples / 1000::double precision) AS kilotuples, age(c.relfrozenxid)::double precision / 1000000::double precision AS mega_xid, pg_size_pretty(pg_total_relation_size(c.relname)) AS disk
   FROM pg_class c, pg_namespace ns, pg_shadow s
  WHERE ns.oid = c.relnamespace AND ns.nspname = 'public'
    AND c.relkind = 'r' AND s.usesysid = c.relowner
  ORDER BY c.reltuples DESC
"
};

    return \%tables;
}

sub store_err {
    my $self = shift;
    foreach my $msg (@_) {
        push @{$self->{STORED_ERRS}}, $msg if ($msg);
    }
}

sub all_stored_errs {
    return wantarray ? @{shift->{STORED_ERRS}} : shift->{STORED_ERRS};
}

1;
