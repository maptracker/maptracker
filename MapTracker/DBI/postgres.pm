# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::DBI::postgres;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use strict;
use vars qw(@ISA);

use BMS::MapTracker::DBI;

@ISA = qw(BMS::MapTracker::DBI);


sub connect {
    my $self = shift;
    my $name = $self->dbname;
    $self->death("MapTracker can not connect to Postgres without a DB name")
        unless ($name);
    my $connectString = "dbi:Pg:dbname=$name";
    my $port          = $self->dbport;
    my $host          = $self->dbhost;
    my $options       = { pg_server_prepare => 0 };
    $connectString   .= "; host=$host" if ($host);
    $connectString   .= "; port=$port" if ($port);

    eval {
        $self->{DBH} = DBI->connect
            ( $connectString, $self->dbuser, $self->dbpass, $options );
    };
    if ($self->{DBH}) {
        $self->pg_standard_handles();
    } else {
        $self->{ERRSTR} = $@;
    }
    return $self->{DBH};
}

sub clean_like_query {
    my $self = shift;
    my $qry  = $_[0];
    # DO NOT use '_' as a single char wildcard - use '?' instead
    $qry =~ s/_/\\_/g;
    $qry =~ s/\?/_/g;
    # Allow '*' as a flexible-length wildcard:
    # $qry =~ s/\*/\%/g; # Nope, changed mind (used in SMILES notation)
    return $qry
}

sub insert_array {
    my $self = shift;
    my ($table, $array, $keepDup) = @_;
    my $dbh = $self->dbh();
    my $sql = "COPY $table FROM STDIN";
    my $sth = $self->prepare($sql);
    $sth->execute();
    
    my %seen;
    foreach my $row (@{$array}) {
        # undef and '' are treated as NULL
        my $line = join("\t",map {defined $_ && $_ ne '' ? $_ : '\N'} @{$row});
        # Unless the user passes a flag to keep duplicates, skip them:
        next if (!$keepDup && $seen{ $line }++);
        my $rv   = $dbh->func("$line\n", 'putline');
        #warn "$table: $line";
    }
    $dbh->func("\\.\n", 'putline');
    # If there are constraint violations, they will occur here:
    $dbh->func('endcopy');
    # ... but I can not figure out how to catch them...
    die $self->dbh_error( $sql, undef, 'insert_array') if ($dbh->err);
}

##########################################################################
# Interface calls that need to be defined by sub-modules
##########################################################################

sub nextval {
    my $self = shift;
    my ($seq) = @_;
    my $sth   = $self->named_sth
        ("Next sequence value for $seq", "SELECT nextval('$seq')", 3);
    return $sth->get_single_value();
}

sub lastval {
    my $self = shift;
    my ($seq) = @_;
    my $sth   = $self->named_sth
        ("Last sequence value for $seq", "SELECT last_value FROM $seq", 3);
    return $sth->get_single_value();
}

sub limit_syntax {
    return "LIMIT";
}

sub schema_owner {
    return 'public';
}

sub inefficient_subselect {

    # I think this was only a problem with earlier versions of
    # Postgres, it appears to be much more efficient in 8.3 - I am
    # leaving it as-is for now.

    return 1;
}

=pod

    PostGres is not competent to reliably choose indices on even
    slightly complex queries. The following query should utilize edge
    index "dg_nm1spcd" btree (name1, space_id, type_id) after it finds
    the name_id for 'NM_001234', but chooses to SEQ SCAN edge instead.

      SELECT DISTINCT e.name1, e.name2, e.type_id, e.edge_id,
             e.space_id, e.live, e.created
        FROM seqname s1, edge e
       WHERE upper(s1.seqname) = 'NM_001234'
         AND e.name1 = s1.name_id
         AND e.space_id = 1
         AND e.type_id IN (6,7)
         AND e.live = 't';

                                             QUERY PLAN
-----------------------------------------------------------------
 Unique  (cost=111746282.53..111746427.59 rows=7253 width=29)
   ->  Sort  (cost=111746282.53..111746300.66 rows=7253 width=29)
         ->  Nested Loop  (cost=100000000.00..111745817.45 rows=7253 width=29)
               ->  Seq Scan on edge e  (cost=100000000.00)
                     Filter: ((space_id = 1) AND ((type_id = 6) OR 
                              (type_id = 7)) AND (live = true))
               ->  Index Scan using test_tab_pkey on seqname s1  (cost=5.73)
                     Index Cond: ("outer".name1 = s1.name_id)
                     Filter: (upper(seqname) = 'NM_001234'::text)


=cut

sub pg_standard_handles {
    my $self = shift;
    my $toCache =
        [
         ];
    foreach my $row (@{$toCache}) {
        my ($name, $level, $sql, $limit) = @{$row};
        next unless ($name && $sql);
        $sql = $self->standardize_sql( -sql => $sql, -limit => $limit)
            if ($limit);
        $self->named_sth( $name, $sql, $level);
    }
}
