# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::DBI::oracle;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use strict;
use vars qw(@ISA);

use BMS::MapTracker::DBI;

@ISA = qw(BMS::MapTracker::DBI);


sub connect {
    my $self = shift;
    my $name = $self->dbname;
    $self->death("MapTracker can not connect to Oracle without a DB name")
        unless ($name);

    $ENV{ORACLE_HOME} = '/u01/home/oracle/product/10.1.0.3';
    $ENV{TNS_ADMIN}   = $ENV{ORACLE_HOME} . '/network/admin';

    my $dbstr = $self->dbuser;
    $dbstr   .= '/' . $self->dbpass if ( $self->dbpass );
    $dbstr   .= '@' . $name;
    my $options = { LongReadLen => 2000000,
                    AutoCommit  => 1, };
    $self->{DBH} = DBI->connect
        ( "dbi:Oracle:",  $dbstr, undef, $options );
    $self->ora_standard_handles();
    return $self->{DBH};
}

sub clean_like_query {
    my $self = shift;
    my $qry  = $_[0];
    # DO NOT use '_' as a single char wildcard - use '?' instead
    $qry =~ s/_/\\_/g;
    $qry =~ s/\?/_/g;
    # Allow '*' as a flexible-length wildcard:
    $qry =~ s/\*/\%/g;
    return $qry
}

sub insert_array {
    my $self = shift;
    my ($table, $array, $keepDup) = @_;
    my $dbh = $self->dbh();
    die "Have not figured out best way for array insert in oracle";
}

##########################################################################
# Interface calls that need to be defined by sub-modules
##########################################################################

sub nextval {
    my $self = shift;
    my ($seq) = @_;
    my $sth   = $self->named_sth
        ("Next sequence value for $seq", "SELECT $seq.nextval FROM DUAL", 3);
    return $sth->get_single_value();
}

sub lastval {
    my $self = shift;
    my ($seq) = @_;
    my $sth   = $self->named_sth
        ("Last sequence value for $seq", "SELECT $seq.currval FROM DUAL", 3);
    return $sth->get_single_value();
}

sub limit_syntax {
    return "AND ROWNUM <=";
}

sub schema_owner {
    return uc(shift->dbuser);
}

sub inefficient_subselect {
    return 0;
}

sub ora_standard_handles {
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
