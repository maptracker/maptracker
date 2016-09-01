# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::DBI::Schema;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use strict;
use vars qw(@ISA @EXPORT );
use Scalar::Util qw(weaken);

use BMS::MapTracker::Shared;
@ISA    = qw(BMS::MapTracker::Shared);

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
    };
    bless ($self, $class);
    my $args = $self->parseparams( @_ );
    my $dbi  = $args->{DBI};
    $self->death("A new Schema object can not be made without being provided ".
                 "a MapTracker::DBI object") unless ($dbi);
    weaken( $self->{DBI} = $dbi );
    return $self;
}

sub dbi {
    return shift->{DBI};
}

sub table_info {
    my $self = shift;
    my $treq = lc(shift || '');
    if ($treq) {
        # Information for a specific table requested
        unless ($self->{TAB_INFO}{$treq}) {
            if (0) {
                # Gets called in standard_handles - duplication prevention STH
                my @meths;
                for my $i (1..10) {
                    my ($p, $f, $l, $s) = caller($i);
                    last unless ($s);
                    push @meths, $s;
                }
                warn "Col dat for $treq\n  ",join("\n   ", @meths);
            }
            my $dbh    = $self->dbi->dbh;
            return undef unless ($dbh);
            $self->benchstart;
            my $colsth = $dbh->prepare("SELECT * FROM $treq where 1=0");
            $colsth->execute();
            my @cols  = map { lc($_) } @{$colsth->{NAME}};
            for my $index (0..$#cols) {
                my $cname = $cols[$index];
                $self->{TAB_INFO}{$treq}{$cname} = $index;
                $self->{TAB_INFO}{$treq}{$index} = $cname;
            }
            # Index -1 stores a list of the columns in proper order
            $self->{TAB_INFO}{$treq}{-1} = \@cols;            
            $self->benchend;
        }
        return { %{$self->{TAB_INFO}{$treq}} };
    } else {
        # Information for all tables is requested
        unless ($self->{FULL_TAB_INFO_DONE}) {
            warn "Full col dat request";
            # We need to popuate information for all tables
            my @tabs = $self->all_tables();
            map { $self->table_info( $_ ) } @tabs;
            $self->{FULL_TAB_INFO_DONE} = 1;
        }
        return $self->{TAB_INFO};
    }
}

sub all_tables {
    my $self   = shift;
    unless ($self->{ALL_TABLES}) {
        $self->benchstart;
        my @tabs;
        my $schema = $self->schema;
        my $dbi    = $self->dbi;
        my $dbh    = $dbi->dbh;
        my $sth    = $dbh->table_info
            (undef, $dbi->schema_owner,  undef, 'TABLE');
        my $rows   = $sth->fetchall_arrayref;
        foreach my $row (@{$rows}) {
            my ($qual, $owner, $table, $type) = @{$row};
            $table = lc($table);
            push @tabs, $table if ($schema->{$table});
        }
        $self->{ALL_TABLES} = \@tabs;
        $self->benchend;
    }
    return wantarray ? @{$self->{ALL_TABLES}} : [ @{$self->{ALL_TABLES}} ];
}


sub schema {
    my $self = shift;
    return $self->{SCHEMA_TABLES} if ($self->{SCHEMA_TABLES});

    my %tables = ();

    $tables{'seqname'} = {
	desc => 'List of all known names (the primary node objects) in the DB',
	cols => [ { name => 'name_id',
                    desc => 'Primary key for this table',
		    pkey => 'sequence', },
		  { name => 'seqname',
		    desc => 'The human-readable name, like NM_001234.3',
		    type => 'text', },
		  { name => 'space_id',
                    desc => 'FKEY for the namespace for this name',
		    fkey => 'namespace.space_id', },
		  ],
	index => [ 'upper(seqname)' ],
    };

    $tables{'seq_species'} = {
	desc => 'An assignment of a species to a particular seqname',
	cols => [ { name => 'tax_id',
		    type => 'integer', },
		  { name => 'name_id',
		    fkey => 'seqname.name_id', },
		  { name => 'authority_id',
		    fkey => 'authority.authority_id', },
		  ],
	index => [ 'name_id' ],
    };

    $tables{'seq_class'} = {
	desc => 'An assignment of a cless to a particular seq name',
	cols => [ { name => 'class_id',
		    fkey => 'class_list.class_id', },
		  { name => 'name_id',
		    fkey => 'seqname.name_id', },
		  { name => 'authority_id',
		    fkey => 'authority.authority_id', },
		  ],
	index => [ [ 'name_id', 'class_id'], [ 'class_id' ] ],
    };

    $tables{'seq_length'} = {
	desc => 'Stores the length (end position) of a sequence',
	cols => [ { name => 'name_id',
		    fkey => 'seqname.name_id', },
                  { name => 'len',
                    type => 'integer', },
		  { name => 'authority_id',
		    fkey => 'authority.authority_id', },
		  ],
	index => [ 'name_id' ],
    };


    $tables{'species'} = {
	desc => 'A species designation, based on taxa ID',
	cols => [ { name => 'tax_id',
		    type => 'integer',
		    pkey => 1, },
		  { name => 'taxa_name',
		    type => 'varchar(255)', },
		  { name => 'taxa_rank',
		    type => 'varchar(20)', },
		  { name => 'parent_id',
		    type => 'integer', },
		  { name => 'hide_flag',
		    type => 'boolean', },
		  { name => 'merged_id',
		    type => 'integer', },
		  ],
	index => [ 'upper(taxa_name)' ],
    };

    $tables{'species_alias'} = {
	desc => 'Collection of common names for a species',
	cols => [ { name => 'tax_id',
		    fkey => 'species.tax_id', },
		  { name => 'alias',
		    type => 'varchar(255)', },
		  { name => 'name_class',
		    type => 'varchar(50)', },
		  ],
	index => [ 'upper(alias)' ],
    };

    $tables{'class_list'} = {
	desc => 'A list of sequence types (mRNA, gDNA, etc)',
	cols => [ { name => 'class_id',
		    pkey => 'sequence', },
		  { name => 'parent_id',
		    desc => 'Can designate a parent class',
		    type => 'integer', },
		  { name => 'seqclass',
		    type => 'varchar(20)',
		    uniq => 1, },
		  { name => 'class_name',
		    desc => 'Tidier name than seqclass',
		    type => 'varchar(50)', },
		  { name => 'descr',
		    desc => 'Description of the name class',
		    type => 'varchar(255)', },
		  ],
    };

    $tables{'relationship'} = {
	desc => 'The kinds of relationships that two names can have',
	cols => [ { name => 'type_id',
		    pkey => 'sequence', },
		  { name => 'label',
		    desc => 'Single word describing the relationship',
		    type => 'varchar(20)',
		    uniq => 1, },
		  { name => 'reads_forward',
		    desc => 'A few words, as in "is translated to" ',
		    type => 'varchar(50)', },
		  { name => 'reads_backward',
		    desc => 'The label of the inverse eg "is translated from"',
		    type => 'varchar(50)', },
		  { name => 'class1',
		    desc => 'Allowed Seq Class for sequence 1"',
		    fkey => 'class_list.class_id', },
		  { name => 'class2',
		    desc => 'Allowed Seq Class for sequence 2"',
		    fkey => 'class_list.class_id', },
		  { name => 'descr',
		    desc => 'A longer description of the relationship',
		    type => 'varchar(255)', }, ],
    };

    $tables{'edge'} = {
	desc => 'A relationship between two names',
	cols => [ { name => 'edge_id',
		    pkey => 'sequence default', },
		  { name => 'space_id',
		    fkey => 'namespace.space_id', },
                  { name => 'name1',
		    fkey => 'seqname.name_id', },
		  { name => 'name2',
		    fkey => 'seqname.name_id', },
		  { name => 'type_id',
		    fkey => 'relationship.type_id', },
		  { name => 'live',
		    type => 'boolean', },
		  { name => 'created',
		    type => 'timestamp', },
		  ],
	index => [ [ 'name1', 'name2', 'space_id', 'type_id' ], 
                   [ 'name1', 'space_id', 'type_id'],
                   [ 'name2', 'space_id', 'type_id'],
                   [ 'space_id'],
                   ],
    };
    
    $tables{'edge_auth_hist'} = {
	desc => 'Update and delete history for the edge',
	cols => [ { name => 'edge_id',
		    fkey => 'edge.edge_id', },
		  { name => 'authority_id',
		    fkey => 'authority.authority_id', },
		  { name => 'dates',
                    desc => 'Array of dates when live status changed',
		    type => 'timestamp(0) without time zone[]', },
		  { name => 'size',
                    desc => 'The number of changes that have occured (the size of dates[])',
		    type => 'integer', },
		  { name => 'live',
                    desc => 'Flag indicating if the edge is alive for the authority',
		    type => 'boolean', },
		  ],
	index => [ [ 'edge_id', 'authority_id' ], 
                   ],
    };
    
    $tables{'edge_meta'} = {
	desc => 'Metadata associated with an edge',
	cols => [ { name => 'edge_id',
		    fkey => 'edge.edge_id', },
		  { name => 'authority_id',
		    fkey => 'authority.authority_id', },
		  { name => 'tag_id',
		    fkey => 'seqname.name_id', },
		  { name => 'value_id',
		    fkey => 'seqname.name_id',
                    null => 1, },
                  { name => 'numeric_value',
		    type => 'numeric',
                    null => 1, },
		  ],
	index => [ [ 'edge_id', 'authority_id', 'tag_id', 
                     'value_id', 'numeric_value' ], 
                   [ 'tag_id', 'value_id'],
                   [ 'tag_id', 'numeric_value'],
                   [ 'value_id' ],
                   ],
    };
    
    $tables{'authority'} = {
	desc => 'An entity that has entered data into the system',
	cols => [ { name => 'authority_id',
		    pkey => 'sequence', },
		  { name => 'authname',
		    desc => 'A name or other textual identifier',
		    type => 'varchar(100)', },
		  { name => 'descr',
		    desc => 'A longer description of the individual',
		    type => 'varchar(255)', }, ],
	index => [ 'upper(authname)' ],
    };
    
    $tables{'transform'} = {
	desc => 'A kind of coordinate mapping',
	cols => [ { name => 'trans_id',
		    pkey => 'sequence', },
		  { name => 'transname',
		    desc => 'Short description of the relationship',
		    type => 'varchar(50)', },
		  { name => 'step1',
		    desc => 'The number of units advanced for name1',
		    type => 'float', },
		  { name => 'step2',
		    desc => 'The number of units advanced for name2',
		    type => 'float', },
		  ],
    };
    
    $tables{'mapping'} = {
	desc => 'Two names can be mapped in coordinate space',
	cols => [ { name => 'map_id',
		    pkey => 'sequence', },
		  { name => 'name1',
		    fkey => 'seqname.name_id', },
		  { name => 'start1',
		    desc => 'The very first coordinate of name1',
		    type => 'float', },
		  { name => 'end1',
		    desc => 'The very last coordinate of name1',
		    type => 'float', },
		  { name => 'name2',
		    fkey => 'seqname.name_id', },
		  { name => 'start2',
		    desc => 'The very first coordinate of name2',
		    type => 'float', },
		  { name => 'end2',
		    desc => 'The very last coordinate of name2',
		    type => 'float', },
		  { name => 'trans_id',
		    fkey => 'transform.trans_id', },
		  { name => 'authority_id',
		    fkey => 'authority.authority_id', },
		  { name => 'map_score',
		    type => 'float', },
		  { name => 'strand',
		    desc => 'The strand of name2',
		    type => 'integer', },
		  { name => 'db_id',
		    fkey => 'searchdb.db_id', },
		  ],
	index => [ [ 'name1','start1','end1' ], 
		   [ 'name2','start2','end2'], 
		   [ 'name1', 'name2', ],
		   [ 'db_id', 'name1', ],
		   [ 'db_id', 'name2', ],
		   ],
    };
    
    $tables{'secure_group'} = {
	desc => 'A grouping with access limitations',
	cols => [ { name => 'group_id',
		    pkey => 'sequence', },
		  { name => 'groupname',
		    desc => 'A short name of the group',
		    type => 'varchar(100)', },
		  { name => 'descr',
		    desc => 'A longer description of the group',
		    type => 'varchar(255)', },
		  { name => 'moderator',
		    fkey => 'authority.authority_id', },
		  ],
	index => [ 'upper(groupname)' ],
    };
    # NOT USING!!!
    delete $tables{'secure_group'};

    $tables{'group_membership'} = {
	desc => 'Who belongs to any given group',
	cols => [ { name => 'group_id',
		    fkey => 'secure_group.group_id', },
		  { name => 'authority_id',
		    fkey => 'authority.authority_id', },
		  { name => 'access_level',
		    desc => 'Simple access control, smaller = more access',
		    type => 'integer', },
		  ],
    };
    # NOT USING!!!
    delete $tables{'group_membership'};
    
    $tables{'location'} = {
	desc => 'An unbroken run of map coordinates',
	cols => [ { name => 'map_id',
		    fkey => 'mapping.map_id', },
		  { name => 'start1',
		    desc => 'The first coordinate of name1',
		    type => 'float', },
		  { name => 'end1',
		    desc => 'The last coordinate of name1',
		    type => 'float', },
		  { name => 'start2',
		    desc => 'The first coordinate of name2.',
		    type => 'float', },
		  ],
    };
    
    $tables{'searchdb'} = {
	desc => 'A search database used to generate mappings',
	cols => [ { name => 'db_id',
		    type => 'integer',
		    pkey => 'sequence', },
		  { name => 'dbname',
		    desc => 'A short name of the database',
		    type => 'varchar(100)', },
		  { name => 'type',
		    desc => 'Short text describing the type of the DB',
		    type => 'varchar(50)', },
		  { name => 'dbpath',
		    desc => 'The unix path to the database',
		    type => 'varchar(500)', },
		  ],
	index => [ 'upper(dbname)' ],
    };

    $tables{'namespace'} = {
	desc => 'Namespaces to sub-classify seqnames',
	cols => [ { name => 'space_id',
		    type => 'integer',
		    pkey => 'sequence', },
		  { name => 'space_name',
		    desc => 'A short name for the namespace',
		    type => 'varchar(20)', },
		  { name => 'descr',
		    desc => 'Longer description for the namespace',
		    type => 'varchar(100)', },
		  { name => 'case_sensitive',
		    desc => 'Flag if these names are case-sensitive',
		    type => 'boolean', },
		  ],
	index => [ 'upper(space_name)' ],
    };

    $tables{'space_hierarchy'} = {
	desc => 'Defines namespaces that are fully contained within other ones',
	cols => [ { name => 'parent_id',
		    desc => 'The larger namespace',
		    type => 'integer', },
		  { name => 'child_id',
		    desc => 'The smaller namespace',
		    type => 'integer', },
		  ],
	index => [ 'parent_id' ],
    };

    $tables{'load_status'} = {
	desc => 'Tracks loader jobs',
	cols => [ { name => 'directory',
		    desc => 'The path to flat files being loaded',
		    type => 'varchar(1000)', },
		  { name => 'task',
		    desc => 'The operation being undertaken',
		    type => 'varchar(20)', },
		  { name => 'pid',
		    desc => 'The process ID of the loader',
		    type => 'integer', },
		  { name => 'host',
		    desc => 'The host machine running the process',
		    type => 'varchar(20)', },
		  { name => 'start',
		    desc => 'The time the process started',
		    type => 'timestamp', },
		  ],
	index => [ ['directory', 'task'] ],
    };

    $self->{SCHEMA_TABLES} = \%tables;
    return \%tables;
}

1;
