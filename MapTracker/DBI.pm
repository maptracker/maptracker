# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::DBI;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use strict;
use vars qw(@ISA @EXPORT );
use Scalar::Util qw(weaken);

use DBI;
use BMS::MapTracker::Shared;
use BMS::MapTracker::DBI::Schema;
use BMS::Utilities::BmsDatabaseEnvironment;

@ISA = qw(BMS::MapTracker::Shared BMS::Utilities::BmsDatabaseEnvironment);

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
        DBNAME => '',
        DBTYPE => '',
        DBPASS => '',
        DBUSER => '',
        DBHOST => '',
        DUMPFH  => undef,
        DUMPLFT => "\n",
        DUMPRGT => "\n",
        DUMPSQL => 0,
        STHS    => {},
        BEGIN_NESTING => 0,
    };
    bless ($self, $class);
    my $args = $self->parseparams( @_ );
    $self->dbtype( $args->{DBTYPE} || $args->{TYPE} );
    $self->dbname( $args->{DBNAME} || $args->{NAME} );
    $self->dbuser( $args->{DBUSER} || $args->{USER} );
    $self->dbpass( $args->{DBPASS} || $args->{PASSWORD} || $args->{SSAPBD});
    $self->dbhost( $args->{DBHOST} || $args->{HOST} );
    $self->dbport( $args->{DBPORT} || $args->{PORT} );
    $self->dbadmin( $args->{DBADMIN} );

    $self->sql_dump( $args->{DUMPSQL} || $args->{SQLDUMP} || 0);
    $self->standard_handles();

    return $self;
}

sub DESTROY {
    my $self = shift;
    if (my $leftover = $self->{BEGIN_NESTING}) {
        $self->err("WARNING: It appears that commit() was not called - ".
                   "needed to call at least $leftover more times");
        $self->{BEGIN_NESTING} = 1;
        $self->commit();
    }
    # warn "MapTracker DBI object destroyed\n  ";
}

#*connect = \&dbh;
sub dbh {
    my $self = shift;
    unless ($self->{DBLIVE}) {
        my $type = $self->dbtype;
        eval("require BMS::MapTracker::DBI::$type");
        if ($@) {
            $type ||= '-NOT DEFINED-';
            $self->death("Failure to instantiate software for '$type' ".
                         "MapTracker database:\n   $@");
        }
        bless($self, "BMS::MapTracker::DBI::$type");
        $self->release();
        my $dbh = $self->connect();
        unless ($dbh) {
            my $err = $DBI::errstr || $self->{ERRSTR} || '-Unknown Error-';
            my $msg = sprintf
                ("Failed to connect to MapTracker %s instance:\n  %s\n",
                 $type, $err);
            my $stuff = {
                Host => $self->dbhost,
                Port => $self->dbport,
                User => $self->dbuser,
                Name => $self->dbname,
                Code => $0,
                Admin => $self->dbadmin,
            };
            foreach my $tag (sort keys %{$stuff}) {
                my $val = $stuff->{$tag};
                $val = '-NOT DEFINED-' unless (defined $val);
                $msg .= "  $tag: $val\n";
            }
            my @meths;
            for my $i (1..10) {
                my ($p, $f, $l, $s) = caller($i);
                last unless ($s);
                ($p, $f, $l) = caller($i + 1);
                push @meths, sprintf("%s %s", $s, $l ? "[$l]" : '');
            }
            if ($#meths > -1) {
                $msg .= "Call stack:\n";
                $msg .= join('', map { "  $_\n" } @meths);
            }
            if (my $admin = $self->dbadmin) {
                my $cmd  = qq(| Mail -s "MapTracker: $err" $admin);
                open (MAIL, $cmd) || die "$msg\nCould not send mail to Admins!\n  $!\n";
                my $userStuff = {
                    "End User" => $ENV{'HTTP_MAIL'} || $ENV{'REMOTE_USER'} ||
                    $ENV{'LDAP_USER'} || $ENV{'HTTP_CN'} || $ENV{'USER'} || 
                    $ENV{'LOGNAME'} || $ENV{'REMOTE_ADDR'},
                    "URL" => $ENV{REQUEST_URI},
                    "Web Host" => $ENV{HTTP_HOST},
                };
                foreach my $tag (sort keys %{$userStuff}) {
                    my $val = $userStuff->{$tag};
                    print MAIL "  $tag: $val\n" if (defined $val);
                }
                print MAIL "\n$msg";
                close MAIL;
                $msg .= "\nThe administrator ($admin) has been notified by email of this problem\n";
            }
            die $msg;
        }
        $self->{DBLIVE} = 1;
        weaken($dbh->{private_mt_dbi} = $self);
        $self->standard_handles();
        if (0) {
            my @meths;
            for my $i (1..10) {
                my ($p, $f, $l, $s) = caller($i);
                last unless ($s);
                push @meths, $s;
            }
            warn "DB CONNECTED by ",join(" / ", @meths);
        }
        if (1) {
            # Allow '\\' as escapes
            $self->prepare
                ("SET standard_conforming_strings TO 'off'")->execute();
            # Turn off annoying escape warnings in Postgres
            
            $self->prepare
                ("SET escape_string_warning TO 'off'")->execute();
        }
    }
    return $self->{DBH};
}

sub schema {
    my $self = shift;
    unless ($self->{SCHEMA}) {
        $self->{SCHEMA} = BMS::MapTracker::DBI::Schema->new
            ( -dbi => $self );
    }
    return $self->{SCHEMA};
}

*sqldump  = \&sql_dump;
*dumpsql  = \&sql_dump;
*dump_sql = \&sql_dump;
sub sql_dump {
    my $self = shift;
    if (defined $_[0] && $_[0] ne $self->{DUMPSQL}) {
        # New SQL debug level
        $self->{DUMPSQL} = $_[0];
    }
    return $self->{DUMPSQL};
}

*dismiss = \&release;
sub release {
    my $self = shift;
    return unless ($self->{DBLIVE});
    if (0) {
        my @meths;
        for my $i (1..5) {
            my ($p, $f, $l, $s) = caller($i);
            last unless ($s);
            push @meths, $s;
        }
        warn "DB RELEASED by ",join(" / ", @meths);
    }
    my $dbh = $self->{DBH};
    $dbh->disconnect() if ($dbh);
    # Try to keep these: TAB_INFO COLMAPS SCHEMA
    foreach my $key (qw( DBH STHS DBLIVE)) {
        delete $self->{$key};
    }
}

sub dbadmin {
    my $self = shift;
    if (defined $_[0]) {
        $self->{DBADMIN} = $_[0];
    }
    return $self->{DBADMIN};
}

sub dbtype {
    my $self = shift;
    if ($_[0] && $_[0]) {
        my $type = lc($_[0]);
        if ($type ne $self->{DBTYPE}) {
            # New database type
            $self->{DBTYPE} = $type;
            $self->release();
        }
    }
    return $self->{DBTYPE};
}

sub dbname {
    my $self = shift;
    if (defined $_[0] && $_[0] ne $self->{DBNAME}) {
        # New database name
        $self->{DBNAME} = $_[0];
        $self->release();
    }
    return $self->{DBNAME};
}

*dbpassword = \&dbpass;
*password = \&dbpass;
sub dbpass {
    my $self = shift;
    if (defined $_[0] && $_[0] ne $self->{DBPASS}) {
        # New database password
        $self->{DBPASS} = $_[0];
        $self->release();
    }
    return $self->{DBPASS};
}

*username = \&user;
sub user {
    my $self = shift;
    if (my $username = $_[0]) {
        if (ref( $username ) && $username->isa('BMS::MapTracker::Authority')) {
            $username = $username->name;
        }
        # The MapTracker (NOT database) username
        if ($username =~ /readonly/i) {
            # Read-only access, no writing allowed
            $self->{READONLY} = 1;
        } else {
            # Actual user, able to write to database
            $self->{READONLY} = 0;
        }
        $self->{MTUSER} = $username;
    }
    return $self->{MTUSER};
}

sub readonly {
    return shift->{READONLY};
}

sub dbuser {
    my $self = shift;
    if (defined $_[0] && $_[0] ne $self->{DBUSER}) {
        # New database username
        $self->{DBUSER} = $_[0];
        $self->release();
    }
    return $self->{DBUSER};
}

*port = \&dbport;
sub dbport {
    my $self = shift;
    if (defined $_[0] && (!$self->{DBPORT} || $_[0] ne $self->{DBPORT})) {
        # New database port
        $self->{DBPORT} = $_[0];
        $self->release();
    }
    return $self->{DBPORT};
}

*host = \&dbhost;
sub dbhost {
    my $self = shift;
    if (defined $_[0] && $_[0] ne $self->{DBHOST}) {
        # New database host
        $self->{DBHOST} = $_[0];
        $self->release();
    }
    return $self->{DBHOST};
}

sub inefficient_subselect {
    return 0;
}

sub manual_escape {
    my $self = shift;
    my ($text) = @_;
    $text =~ s/\\.//g;    # Remove escapes
    $text =~ s/\\$//;     # Remove terminal backslash
    $text =~ s/\'/\\\'/g; # Escape quotes
    return $text;
}

sub pretty_print {
    my $self = shift;
    my ($sql, $args) = @_;
    $args ||= {};

    $self->benchstart;
    my $sqlcom = "/* %s : %s */\n";
    $sql =~ s/[\s\n\t]+/ /g;
    $sql = " $sql ";
    my $maxtag = 12;
    my @tags = ("CREATE TABLE", "AS SELECT", "INSERT INTO", "SELECT", 
                "UPDATE", "LOCK",
		["UNION", "\nUNION\t\n"], "LIMIT", "VALUES", "COPY",
		"DELETE FROM", "FROM", "WHERE", "OR", "AND", "ORDER BY",
                "GROUP BY", "HAVING");
    foreach my $set (@tags) {
	# Case sensitive - the SQL should have keywords in caps
	my ($tag, $out);
	if (ref($set)) {
	    ($tag, $out) = @{$set};
	} else {
	    $tag = $set;
	    $out = "\n$tag\t";
	}
	$sql =~ s/[\n ]+$tag[\n ]+/$out/g;
    }
    $sql =~ s/\([\n ]*/\(/g;

    
    $sql =~ s/^ //; $sql =~ s/ $//;
    $sql .= ';' unless ($sql =~ /\;$/);
    $sql =~ s/\;/\;\n/g;
    my @lines = split("\n", $sql);
    my @newlines;
    my $maxline = 60;
    my $indent = 0; my @pad = ("");
    while ($#lines > -1) {
	my $line = shift @lines;
	next if ($line =~ /^\s*$/);
	# Wrap long lines:
	if (length($line) > $maxline) {
            # Temporarily mask spaces inside quotes:
            my $iloop = 0;
            while ($line =~ /(\'[^\']*?) ([^\']*?\')/) {
                $line =~ s/(\'[^\']*?) ([^\']*?\')/$1\n$2/;
                last if (++$iloop > 500);
            }
	    my $pos = rindex($line, " ", $maxline - 2);
            # Reset spaces that were inside quotes:
            $line =~ s/\n/ /g;
	    if ($pos > 0) {
		my $tail = " \t" . substr($line, $pos+1);
		unshift @lines, $tail;
		$line = substr($line, 0, $pos);
	    }
	}
        my @bits = split("\t", $line);
        my $pre = shift @bits;
        my $pro = join(" ", @bits);
	# Manage parentheses indenting:
	if ($indent > 0) {
	    ($pre, $pro) = ("", $pad[-1] . "$pre $pro");
	}
        # print "<pre>($pre, $pro)</pre>";
	my $newindent = $indent;
	$newindent += ( $line =~ tr/\(/\(/ );
	$newindent -= ( $line =~ tr/\)/\)/ );
	if ($newindent > $indent) {
	    my $ppos = index($pro, "(");
	    push @pad, " " x ($ppos+1);
	} elsif ($newindent < $indent) {
	    pop @pad;
	}
	$indent = $newindent;
	my $pline = sprintf("%".$maxtag."s %s", $pre, $pro);
	push @newlines, $pline;
    }
    $sql = join("\n", @newlines);
    my $header = "";
    if (my $bvs = $args->{BIND}) {
        # $header .= sprintf($sqlcom, 'Bind variables' ,join(", ", @{$bvs}));
        $sql =~ s/\?/BIND_VARIABLE/g;
        my @binds = @{$bvs};
        while ($#binds > -1 && $sql =~ /BIND_VARIABLE/) {
            my $bv = shift @binds;
            if (!defined $bv) {
                $bv = "NULL";
            } elsif (my $r = ref($bv)) {
                if ($r eq 'ARRAY') {
                    $bv = "'{".join(',', map { $_ =~ /^\d+$/ ? $_ : "\"$_\"" } @{$bv})."}'";
                } else{
                    $header .= sprintf($sqlcom, 'SQL ERROR', "Bind variable is type '$r'");
                }
            } elsif ($bv !~ /^\d+$/) {
                $bv =~ s/\\/\\\\/g;
                $bv =~ s/\'/\\\'/g;
                $bv = "'$bv'";
            }
            $sql =~ s/BIND_VARIABLE/$bv/;
        }
        if ($sql =~ /BIND_VARIABLE/) {
            $sql =~ s/BIND_VARIABLE/\?/g;
            $header .= sprintf($sqlcom, 'SQL ERROR', "Too few bind variables");
        } elsif ($#binds > -1 ) {
            $header .= sprintf($sqlcom, 'SQL ERROR', "Extra bind variables: ".
                               join(', ', @binds));
        }
    }
    my @history;
    for my $hist (2..4) {
	my @f = split "::", (caller($hist))[3] || ""; # Calling funciton
	push @history, $f[$#f] if ($f[$#f]);
    }
    my $callHist = (join " < ", @history) || "";
    $header.= sprintf($sqlcom, $args->{NAME} || "Un-named SQL", $callHist);
    my $text = $self->{DUMPLFT} .$header . $sql . $self->{DUMPRGT};
    
    $self->benchstop;
    if (my $fh = $self->{DUMPFH}) {
        print $fh $text ;
    } else {
        warn $text;
    }
    return $sql;
}

sub standardize_sql {
    my $self = shift;
    unshift @_, '-sql' if ($#_ == 0);
    my $args  = $self->parseparams( @_ );
    my $sql   = $args->{SQL};
    my $dumpsql = $args->{DUMPSQL} || $self->{DUMPSQL};
    if (my $limit = $args->{LIMIT} || $args->{ROWNUM}) {
        $sql .= sprintf(" %s %d", $self->limit_syntax, $limit);
    }
    $self->pretty_print( $sql, $args) 
        if ($dumpsql && (!$args->{LEVEL} || $dumpsql >= $args->{LEVEL}));
    return wantarray ? ($sql, $args) : $sql;   
}


# Allow user to define the order in which they are passing columns
sub user_column_order {
    my $self = shift;
    my $tab  = lc(shift);
    if ($#_ > -1) {
        my $dborder = $self->column_order( $tab );
        my @cols    = @{$dborder->{-1}};
        unless ($#cols == $#_) {
            my $msg = sprintf
                ("Failure defining user_column_order( '%s', [ %s ]) - ".
                 "That table has %d column%s, not %d.", $tab,
                 join(",", map { "'$_'" } @_), $#cols+1, 
                 $#cols == 0 ? '' : 's', $#_ + 1);
            $self->death($msg);
        }
        my @map;
        my $swaps = 0;
        for my $i (0..$#_) {
            my $col = $_[$i];
            my $index = $dborder->{lc($col)};
            unless (defined $index) {
                $self->death("Failed to defined user_column_order() for ".
                             "'$tab' : column $col not present");
            }
            push @map, $index;
            $swaps++ if ($i != $index);
        }
        if ($swaps) {
            # We will need to do a mapping
            $self->{COLMAPS}{$tab} = \@map;
        } else {
            delete $self->{COLMAPS}{$tab};
        }
    }
    return $self->{COLMAPS}{$tab};
}

sub build_array {
    my $self = shift;
    my ($table, $data) = @_;
    $table = lc($table);
    my $rows = [];
    if (!ref($data)) {
        # Null request, do nothing
    } elsif (ref($data) eq 'HASH') {
        my $dborder = $self->column_order( $table );
        my @row;
        while (my ($colname, $val) = each %{$data}) {
            my $index = $dborder->{lc($colname)};
            $self->death("Failure to build_array in '$table' with ".
                         "nonexistent column '$colname'")
                unless (defined $index);
            $row[ $index ] = $val;
        }
        $rows = [ \@row ];
    } elsif ($#{$data} < 0) {
        # Do nothing - empty array
    } else {
        # Wrap up 1D arrays as 2D
        $data = [ $data ] if (ref($data->[0]) ne 'ARRAY');
        if (my $map = $self->user_column_order( $table )) {
            # We need to re-map the columns
            my @mapped;
            foreach my $row (@{$data}) {
                my @correct;
                for my $i (0..$#{$map}) {
                    $correct[ $map->[$i] ] = $row->[$i];
                }
                push @mapped, \@correct;
            }
            $rows = \@mapped;
        } else {
            # We can use the rows as-is
            $rows = $data;
        }
    }
    push @{$self->{ARRAYS}{$table}}, @{$rows};
}

sub write_array {
    my $self = shift;
    # Record any information the user may have passed
    $self->build_array( @_ ) if ($#_ > -1);
    
    foreach my $table (sort keys %{$self->{ARRAYS}}) {
        my $array = $self->{ARRAYS}{$table};
        if ($array && $#{$array} > -1) {
            my $sthname = "Count existing rows in $table";
            my $check = $self->exists_named_sth($sthname) ?
                $self->named_sth($sthname) : undef;
            if ($self->can('insert_array')) {
                # Prefilter before locking - minimize time DB is locked
                my $list = $check ? 
                    $self->filter_array( $array, $check) : $array;

                $self->begin_work;
                my $sth = $self->prepare
                    ("LOCK TABLE $table IN EXCLUSIVE MODE");
                $sth->execute();
                # Re-filter after locking (to be safe)
                $list = $self->filter_array( $list, $check) if ($check);
                $self->insert_array( $table, $list);
                $self->commit;
                # I am not checking to remove more generic class assignments

            } else {
                $self->death("This adaptor has no mechanism to write_array()");
            }
        }
        delete $self->{ARRAYS}{$table};
    }
}

sub filter_array {
    my $self = shift;
    my ($array, $sth) = @_;
    my @filtered;
    foreach my $row (@{$array}) {
        my $rv = $sth->get_single_value( @{$row} );
        if (!$rv) {
            push @filtered, $row;
        }
    }
    return \@filtered;
}

sub column_order {
    my $self = shift;
    my ($tab, $col) = @_;
    if ($tab) {
        my $tabhash = $self->schema->table_info( $tab );
        $self->death("No information found for table '$tab'")
            unless ($tabhash);
        if (defined $col) {
            my $rv = $tabhash->{lc($col)};
            $self->death("Table '$tab' does not have information for '$col'")
                unless (defined $rv);
            return $rv;
        }
        return $tabhash;
    }
    return $self->schema->table_info();
}

##########################################################################
# Standard Statement handles
##########################################################################

sub standard_handles {
    my $self = shift;
    # DO NOT CHANGE THE NAME KEYS UNLESS YOU TRACK DOWN ALL REFERENCES TO THEM
    my $toCache =
        [
         # Various select statements
         ["Identify edges via full specification", 3,
          "SELECT edge_id FROM edge ".
          " WHERE name1 = ? AND name2 = ? AND type_id = ? AND space_id = ?"],

         ["Get children of a class", 3,
          "SELECT class_id FROM class_list".
          " WHERE parent_id = ?"],

         # Fast name retrieval
         ["Fast name standardization", 3,
          "SELECT seqname, name_id FROM seqname".
          " WHERE upper(seqname) = upper(?)"],
         ["Fast name from ID", 3,
          "SELECT seqname FROM seqname WHERE name_id = ?"],
         ["Fast name standardization with namespace", 3,
          "SELECT seqname, name_id FROM seqname".
          " WHERE upper(seqname) = upper(?) AND space_id = ?"],
         ["Fast name standardization with case-sensitive namespace", 3,
          "SELECT seqname, name_id FROM seqname".
          " WHERE upper(seqname) = upper(?) AND space_id = ? AND seqname = ?"],
         ["Fast class check", 3,
          "SELECT DISTINCT class_id FROM seq_class WHERE name_id = ?"],
         

         # Finding and Detailing Edges
         ["Retrieve edge by ID", 3,
          "SELECT space_id, name1, name2, type_id, live, created ".
          "  FROM edge WHERE edge_id = ?"],

         ["Get edge history", 3,
          "SELECT authority_id, dates, live FROM edge_auth_hist".
          " WHERE edge_id = ?"],
         ["Live authorities for edge", 3,
          "SELECT authority_id FROM edge_auth_hist".
          " WHERE edge_id = ? AND live = 't'"],

         ["Get edge tags", 3,
          "SELECT authority_id, tag_id, value_id, numeric_value".
          "  FROM edge_meta WHERE edge_id = ?" ],
         ["Fast tag names for edge + tag value", 3,
          "SELECT sn.seqname FROM edge_meta em, seqname sn".
          " WHERE em.edge_id = ? AND em.tag_id = ?".
          "   AND sn.name_id = em.value_id" ],
         ["Fast tag numbers for edge + tag value", 3,
          "SELECT em.numeric_value FROM edge_meta em".
          " WHERE em.edge_id = ? AND em.tag_id = ?" ],
         

         # Finding and Detailing Mappings
         ["Select Locations for a Mapping", 3,
          "SELECT start1, end1, start2 FROM location ".
          " WHERE map_id = ? ORDER BY start1"],
         ["Determine if a sequence has a mapping in a searchdb", 3,
          "SELECT map_id FROM mapping where name1 = ? AND db_id = ?", 1],

         # Finding authorities
         ["Retrieve authority by name", 3,
          "SELECT authority_id, authname, descr FROM authority ".
          " WHERE upper(authname) = upper(?)"],

         # Finding Types (Relationships)
         ["Load all Types", 3,
          "SELECT type_id, label, reads_forward, reads_backward, ".
          "descr, class1, class2 FROM relationship ORDER BY type_id"],

         # Finding Searchdbs
         ["Retrieve searchdb by ID", 3,
          "SELECT db_id, dbname, type, dbpath FROM searchdb".
          " WHERE db_id = ?"],
         ["Retrieve searchdb by name", 3,
          "SELECT db_id, dbname, type, dbpath FROM searchdb".
          " WHERE upper(dbname) = ?"],

         # Finding and detailing namespaces
         ["Get all namespaces", 3,
          "SELECT space_id FROM namespace WHERE space_id > 0"],
         ["Retrieve namespace by ID", 3,
          "SELECT space_id, space_name, descr, case_sensitive FROM namespace".
          " WHERE space_id = ?"],
         ["Retrieve namespace by name", 3,
          "SELECT space_id, space_name, descr, case_sensitive FROM namespace".
          " WHERE upper(space_name) = upper(?)" ],
         ["Get child namespaces", 3,
          "SELECT child_id FROM space_hierarchy WHERE parent_id = ?"],

         # Finding and detailing for Taxa
         ["Find taxa by ID", 3,
          "SELECT tax_id FROM species".
          " WHERE tax_id = ?"],
         ["Find taxa by unambiguous name", 3,
          "SELECT tax_id FROM species".
          " WHERE upper(taxa_name) = upper(?)"],
         ["Find taxa by ambiguous name", 3,
          "SELECT tax_id FROM species".
          " WHERE upper(taxa_name) LIKE upper(?)"],
         ["Find taxa by unambiguous alias", 3,
          "SELECT tax_id FROM species_alias".
          " WHERE upper(alias) = upper(?)"],
         ["Find taxa by ambiguous alias", 3,
          "SELECT tax_id FROM species_alias".
          " WHERE upper(alias) LIKE upper(?)"],
         ["Retrieve Taxa details", 3,
          "SELECT taxa_name, parent_id, taxa_rank, hide_flag, merged_id".
          "  FROM species WHERE tax_id = ?"],
         ["Find child taxa", 3,
          "SELECT tax_id FROM species WHERE parent_id = ?"],
         ["Find Taxa aliases", 3,
          "SELECT alias, name_class FROM species_alias WHERE tax_id = ?"],

         # Detailing sequences
         ["Retrieve length assignments for a seqname", 2,
          "SELECT len, authority_id FROM seq_length WHERE name_id = ?"],
         ["Retrieve class assignments for a seqname", 2,
          "SELECT class_id, authority_id FROM seq_class WHERE name_id = ?"],
         ["Retrieve taxa assignments for a seqname", 2,
          "SELECT tax_id, authority_id FROM seq_species WHERE name_id = ?"],
         ["Check if sequence has mappings", 3,
          "SELECT map_id FROM mapping WHERE name1 = ? OR name2 = ?", 1],
        

         # Insertion commands
         ["Create a new seqname entry", 3,
          "INSERT INTO seqname (name_id, seqname, space_id)".
          "     VALUES (?,?,?)"],
         ["Create a new searchdb entry", 2,
          "INSERT INTO searchdb ( db_id,dbname,type,dbpath )".
          "     VALUES (?,?,?,?)" ],
         ["Create a new authority entry", 2,
          "INSERT INTO authority (authority_id, authname, descr) ".
          "     VALUES (?,?,?)" ],
         ["Create a new taxa entry", 2,
          "INSERT INTO species".
          " (tax_id, taxa_name, parent_id, taxa_rank, hide_flag) ".
          " VALUES ( ?, ?, ?, ?, ? )"],
         ["Create a taxa alias", 3,
          "INSERT INTO species_alias".
          " (tax_id, alias, name_class) VALUES( ?, ?, ? )"],
         ["Create a new class entry", 2,
          "INSERT INTO class_list".
          " ( class_id, parent_id, seqclass, descr ) VALUES ( ?, ?, ?, ? )"],
         ["Create a new transform entry", 2,
          "INSERT INTO transform".
          " ( trans_id, transname, step1, step2 ) VALUES ( ?, ?, ?, ? )"],
         ["Create a new relationship entry", 2,
          "INSERT INTO relationship".
          " ( type_id, label, reads_forward, reads_backward,".
          "   descr, class1, class2 ) VALUES ( ?, ?, ?, ?, ?, ?, ? )" ],

         ["Update taxa data", 3,
          "UPDATE species ".
          "   SET taxa_name = ?, parent_id = ?, taxa_rank = ?, ".
          "       hide_flag = ? WHERE tax_id = ?"],
         ["Clear taxa aliases", 3,
          "DELETE FROM species_alias WHERE tax_id = ?"],
         
        
         # Find current load tasks
         ["Find all locked tasks", 3,
          "SELECT task, host, pid FROM load_status WHERE directory = ?"],
         ["Find currently locked tasks", 3,
          "SELECT host, pid FROM load_status".
          " WHERE task = ? AND directory = ?"],
         ["Lock a task", 3,
          "INSERT INTO load_status (directory, task, host, pid, start) ".
             "VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)"],
         ["Update task lock time", 3,
          "UPDATE load_status SET start = CURRENT_TIMESTAMP".
          " WHERE directory = ? AND task = ?"],
         ["Unlock a task", 3,
          "DELETE FROM load_status".
          " WHERE directory = ? AND host = ? AND PID = ? AND task = ?"],
         ["Unlock all tasks", 3,
          "DELETE FROM load_status".
          " WHERE directory = ? AND host = ? AND PID = ?"],


         # Purging data
         ["Get all edges associated with seqname", 3,
          "SELECT edge_id FROM edge WHERE name1 = ? OR name2 = ?"],
         ["Clear meta tags", 3,
          "DELETE FROM edge_meta WHERE edge_id = ?"],
         ["", ,
          ""],
         ["", ,
          ""],
         ["", ,
          ""],

         ["", ,
          ""],
         ["", ,
          ""],
        
          ];

    # Duplication prevention
    foreach my $tab (qw(seq_class seq_species seq_length)) {
        my $dborder = $self->column_order( $tab );
        my $name    = "Count existing rows in $tab";
        my @cols    = @{$dborder->{-1}};
        my $sql     = "SELECT COUNT(name_id) FROM $tab WHERE ".
            join(" AND ", map { "$_ = ?" } @cols);
        push @{$toCache}, [ $name, 3, $sql, 2 ];
    }

    foreach my $table (qw(authority edge edge_meta edge_auth_hist
                          searchdb seqname namespace)) {
        my $name ="Lock $table exclusively";
        my $sql  = "LOCK TABLE $table IN EXCLUSIVE MODE";
        push @{$toCache}, [ $name, 4, $sql ];
    }

    $self->{WAITING_STHS} = {};

    foreach my $row (@{$toCache}) {
        my ($name, $level, $sql, $limit) = @{$row};
        next unless ($name && $sql);
        $self->{WAITING_STHS}{uc($name)} = $row;
    }
}

*get_all_rows = \&selectall_arrayref;
sub selectall_arrayref {
    my $self = shift;
    my ($sql, $args) = $self->standardize_sql( @_ );
    my $bind = $args->{BIND} || [];
    my $dbh  = $self->dbh;
    my $rv   = $dbh->selectall_arrayref($sql, $args->{ATTR}, @{$bind});
    # warn $self->branch({dbh => $dbh, sql => $sql, binds => [ map { "'$_'" } @{$bind || []}], attr => $args->{ATTR}, rv => $rv});
    die $self->dbh_error( $sql, $args, "selectall_arrayref") if ($dbh->err);
    return $rv;
}

*get_single_row = \&selectrow_array;
sub selectrow_array {
    my $self = shift;
    my ($sql, $args) = $self->standardize_sql( @_ );
    my $bind = $args->{BIND} || [];
    my $dbh  = $self->dbh;
    my @rv   = $dbh->selectrow_array
        ($sql, $args->{ATTR}, @{$bind});
    die $self->dbh_error( $sql, $args, "selectrow_array") if ($dbh->err);
    return @rv;
}

*command = \&do;
sub do {
    my $self = shift;
    my ($sql, $args) = $self->standardize_sql( @_ );
    my $bind = $args->{BIND} || [];
    my $dbh  = $self->dbh;
    my $rv   = $dbh->do
        ($sql, $args->{ATTR}, @{$bind});
    die $self->dbh_error( $sql, $args, "do") if ($dbh->err);
    return $rv;    
}

##########################################################################
# Convienence calls
##########################################################################

sub get_single_value {
    my $self = shift;
    my ($rv) = $self->selectrow_array( @_ );
    return $rv;
}

sub get_array_for_field {
    my $self = shift;
    my $arr  = $self->selectall_arrayref( @_ );
    my @rv   = map { $_->[0] } @{$arr};
    return @rv;
}

##########################################################################
# Use as-is from DBI
##########################################################################

sub commit {
    my $self = shift;
    if ($self->{BEGIN_NESTING} < 1) {
        $self->{BEGIN_NESTING} = 0;
        $self->err("commit() called without prior begin_work()");
    } else {
        if ($self->{BEGIN_NESTING} == 1) {
            if (my $dbh  = $self->dbh) {
                $dbh->commit;
                $self->death("Failed to commit: ".$dbh->errstr) if ($dbh->err);
            } else {
                $self->death("Unable to commit: Database handle is expired");
            }
        }
        $self->{BEGIN_NESTING}--;
    }
}

sub begin_work {
    my $self = shift;
    my $rv;
    if (!$self->{BEGIN_NESTING}) {
        my $dbh  = $self->dbh;
        $rv   = $dbh->begin_work;
        die $self->dbh_error( 'BEGIN', { }, "begin_work") if ($dbh->err);
    }
    $self->{BEGIN_NESTING}++;
    return $rv;
}

sub prepare {
    my $self = shift;
    my $dbh  = $self->dbh;
    my ($sql, $args) = $self->standardize_sql( @_ );
    my $sth = $dbh->prepare( $sql );
    
    die $self->dbh_error( $_[0], { NAME => $args->{NAME} || '-UNKNOWN-' },
                          "prepare")
        if ($dbh->err);
    bless($sth, 'BMS::MapTracker::DBI::st');
    $sth->mt_name($args->{NAME});
    $sth->mt_level($args->{LEVEL});
    return $sth;
}

##########################################################################
# Interface calls that need to be defined by sub-modules
##########################################################################

sub nextval {
    my $self = shift;
    $self->death("Your DBI adaptor needs to define the SQL syntax ".
                 "for sequence next values: nextval()");
}

sub lastval {
    my $self = shift;
    $self->death("Your DBI adaptor needs to define the SQL syntax ".
                 "for sequence last values: lastval()");
}



##########################################################################
# Statement handle simulation
##########################################################################

# Just checks to see if a STH is available under a name
sub exists_named_sth {
    my $self = shift;
    my $name = uc($_[0]);
    return ($self->{STHS}{$name}) ? 1 : 0;
}

# Will die if the name does not exist
sub named_sth {
    my $self = shift;
    my $name = uc($_[0]);
    if (!$self->{STHS}{$name}) {
        my ($lcName, $sql, $level, $limit) = @_;
        if (!$sql && $self->{WAITING_STHS}{$name}) {
            # Pre-canned statement handle that has not been initiated yet
            ($lcName, $level, $sql, $limit) = @{$self->{WAITING_STHS}{$name}}
        }
        # warn "Making $name = $sql";
        if ($sql) {
            # The user is defining the statment handle
            my $args  = { NAME => "CACHED: $lcName", LIMIT => $limit };
            my $sth  = $self->prepare( -sql    => $sql,
                                       -name   => $lcName,
                                       -limit  => $limit,
                                       -level  => $level, );
            die $self->dbh_error( $_[1], $args, "named_sth")
                if ($self->dbh->err);

            $self->{STHS}{$name} = $sth;
            if (defined $level && $self->sql_dump >= $level) {
                $sth->pretty_print( );
            }
        }
    }
    unless ($self->{STHS}{$name}) {
        $self->death("Attempt to recover non-existant statement handle ".
                     "for name '$name'");
    }
    return $self->{STHS}{$name};
}

sub dbh_error {
    my $self = shift;
    my ($sql, $args, $meth) = @_;
    $self->pretty_print($sql, $args);
    $meth = $meth ? $meth. "() " : '';
    my @history;
    for my $hist (1..3) {
        my ($pack, $file, $j4, $subname) = caller($hist);
        last unless ($subname);
        my ($j1, $j2, $line) = caller($hist-1);
        $subname =~ s/^BMS\:\:MapTracker\:\://;
        push @history, "$subname:$line";
    }
    my $dbh = $self->dbh();
    return sprintf("%sFailure: ERR %d : %s\n  %s\n  ", $meth, $dbh->err,
                   $dbh->errstr, join(" / ", @history));
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::DBI::st;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use strict;
use vars qw(@ISA);
@ISA = qw(DBI::st);

#sub DESTROY {
#    my $self = shift;
#    warn "DESTROYED: $self->{Statement}\n ";
#}

sub mt_name {
    my $self = shift;
    if ($_[0]) {
        $self->{private_MT_NAME} = $_[0];
    }
    return $self->{private_MT_NAME};
}

sub mt_level {
    my $self = shift;
    if ($_[0]) {
        $self->{private_MT_LEVEL} = $_[0];
    }
    return $self->{private_MT_LEVEL};
}

sub mt_dbi {
    my $self = shift;
    my $dbh  = $self->{Database};
    return $dbh->{private_mt_dbi};
}

sub execute {
    my $self = shift;
    my $rv;
    # $@ = 0;
    eval {
        $rv = $self->SUPER::execute( @_ );
    };
    my $dbi = $self->mt_dbi();
    if (my $errnum = $dbi->dbh->err) {
        $self->pretty_print( @_ );
        die sprintf
            ("STH execute fails: ERR %d : %s\n  ", $errnum, $dbi->dbh->errstr);
    } elsif ($@) {
        $self->pretty_print( @_ );
        $dbi->err( "Failed to execute STH");
        die;
    }
    my $sqldump = $dbi->sql_dump();
    if ($sqldump && $self->mt_level && $sqldump >= $self->mt_level) {
        $self->pretty_print( @_ );
    }
    return $rv;
}

sub pretty_print {
    my $self = shift;
    my $sql  = $self->{Statement};
    my $name = $self->mt_name || "-Unknown STH-";
    my $dbi  = $self->mt_dbi();
    return $dbi->pretty_print( $sql, { NAME => "STH: $name",
                                       BIND => $#_ > -1 ? \@_ : undef });
}

sub selectall_arrayref {
    my $self = shift;
    $self->execute( @_ );
    my $rows = $self->fetchall_arrayref();
    return $rows;
}

sub selectrow_array {
    my $self = shift;
    $self->execute( @_ );
    my $rows = $self->fetchall_arrayref();
    return $rows->[0] ? @{$rows->[0]} : ();
}

sub get_single_value {
    my $self = shift;
    my ($rv) = $self->selectrow_array( @_ );
    return $rv;
}

sub get_array_for_field {
    my $self = shift;
    my $arr  = $self->selectall_arrayref( @_ );
    my @rv   = map { $_->[0] } @{$arr};
    return @rv;
}

