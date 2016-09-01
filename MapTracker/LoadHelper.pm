# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::LoadHelper;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

$BMS::MapTracker::LoadHelper::VERSION = 
    ' $Id$ ';

use strict;
use vars qw(@ISA);
use BMS::MapTracker;
use BMS::MapTracker::Shared;
use BMS::MapTracker::Mapping;
use Scalar::Util qw(weaken);

@ISA = qw(BMS::MapTracker::Shared);

my $defDir = "/work5/tilfordc/maptracker";
our $globalFileCounter = 0;

=head1 NAME

BMS::MapTracker::LoadHelper - API for loading data into the MapTracker database

=head1 SYNOPSIS

 my $lh = BMS::MapTracker::LoadHelper->new
     ( -authority => 'NCBI',
       -testmode  => 1 );
 
 $lh->set_class('NM_001234', 'RNA');
 
 $lh->set_taxa('NM_001234', 'Homo sapiens');
 
 $lh->set_edge( -name1 => 'NM_001234', 
                -name2 => 'NM_001234.3',
                -type  => 'is an unversioned accession of' );
 
 $lh->set_length('NM_001234.3', 1329);
 
 $lh->write( );

=head1 DESCRIPTION



=head1 Global Methods

The following subroutines affect all load operations, but in
themselves do not perform any actions that could lead to alteration of
the database.

=head2 new

 Title   : new
 Usage   : my $lh = BMS::MapTracker::LoadHelper->new(@arguments)
 Function: Creates a new object and returns a blessed reference to it.
 Returns : A blessed BMS::MapTracker object
 Args    : Associative array of arguments. Recognized keys [Default]:

 -username Required. The name of the default user, Optionally -user
           may be used as an argument name instead. You may change
           this value at a later time using default_user().

  -tracker Optional BMS::MapTracker object. If not provided, one will be
           generated.

 -testmode Default 0.

           If a false value, then the database will be altered.

           If set to 'quiet', then all operations are quietly forgotten.

           If any other true value, then no database writes will
           occur. Instead, a text summary will be printed on screen
           (or to a file or filehandle if spoecified by
           redirect()). If the value matches one or more table names
           (seqname class taxa edge map length), then only data from
           those tables will be shown.

  -basedir Using LoadHelper to set data in MapTracker does not
           directly load the database. Instead, it generates flat
           files that can later be parsed and loaded (functionality
           contained in this module, but usually this step is
           performed by a cron job). -basedir sets the path of the
           folder where these flat files are written (default
           '/work5/tilfordc/maptracker'). You could set an alternative
           directory if you wished. Such files would not be seen by
           the standard cron job, of course. You could then either
           inspect them for debugging purposes, or load them yourself
           using the process_ready() method.

  -logfile When the flatfiles are parsed into the DB, log entries are
           written to the path provided here. The default is
           '/work5/tilfordc/mt_load.log'

 -carpfile If the program encounters "bad data", it normally reports
           to STDERR. You can specify a file path using this parameter
           as an alternative.

 -testfile When the program is in -testmode it normally outputs the
           summary to STDOUT. If you would rather direct output to a
           file, use this parameter.

       -fh Default STDERR. Can be redirected to a file if you wish

 -maxwidth Default 40. If you are running in testmode and dumping out
           your load requests as ascii text (either to STDOUT or a
           file, as described above), this parameter defines the
           maximum character width of a column. Past that size, the
           cell contents will be substringed and concatenated with
           '...'.

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = { 
	IDS => {
	    CLASS     => {},
	    TYPE      => {},
            SPACE     => {},
            TYPEFULL  => {},
	    TAXA      => {},
	    AUTHORITY => {},
            SDB       => {},
	},
        OBJS => {
            TRANSFORM => {},
            SDB       => {},
        },
        GRIPES => {},
        TEXT => {
            class_id     => {},
            authority_id => {},
            type_id      => {},
            space_id     => { 1 => 'None' },
            db_id        => {},
            tax_id       => {},
        },
        ALLOW_BOGUS    => 0,
        FILE_LIST      => {},
        TO_LOAD        => {},
        COUNTER        => 0,
        BLOCKSIZE      => 10,
        TOTAL_COUNT    => 0,
        ALLOW_SELF_REF => {},
        TEST_COUNTER   => 0,
        DISMISS_OK     => 1,
        REDIRECT       => {
            TEST => {
                file => undef,
                fh   => *STDERR,
            },
            CARP => {
                file => undef,
                fh   => *STDERR,
            },
            LOG => {
                file => undef,
                fh   => *STDERR,
            },
        },
        OPERATIONS => 0,
        LAST_WRITE => 0,
    };
    bless ($self, $class);
    $self->intercept_errors();
    my $args = $self->parseparams
        ( -username => undef,
          -testmode => 0,
          -basedir  => $defDir,
          -logfile  => '>>/work5/tilfordc/mt_load.log',
          -sorttemp => '/tmp',
          -maxwidth => 40,
          @_ );
    
    my $uname = $args->{USERNAME} || $args->{USER};
    $self->death("You must define -username in new()") unless ($uname);

    if ($args->{TRACKER}) {
        weaken( $self->{TRACKER} = $args->{TRACKER} );
        # Do NOT dismiss tracker objects provided by the user
        $self->can_dismiss(0);
    } else {
        # We need to instantiate the tracker object with the username:
        $self->tracker($uname, $args->{USERDESC});
        # And then populate the authority ID:
        $self->get_authority_id( $uname );
        # ... otherwise the tracker object will be instantiated as read-only
    }

    $self->default_user( $uname, $args->{USERDESC} );
    $self->death("You have not provided a proper default user name")
        if (uc($uname) eq 'READONLY');


    my $mt = $self->tracker();

    $mt->make_namespace
        ( -name      => 'META_TAGS',
          -desc      => 'Tag names used to qualify edges',
          -sensitive => 0 );

    foreach my $key ('SORTTEMP', 'LOGFILE','TESTFILE', 'MAXWIDTH') {
        $self->{$key} = $args->{$key};
    }

    # Establish file paths and directories:
    $self->directory( $args->{LOADDIR} || $args->{BASEDIR} || $defDir );
    $self->load_token( $args->{LOADTOKEN} );

    $self->{TESTMODE}    = lc($args->{TESTMODE} || "");

    if (my $cf = $args->{CARPFILE}) {
        $self->redirect( -stream => 'CARP',
                         -file   => $cf );
    }
    if (my $tf = $args->{TESTFILE}) {
        $self->redirect( -stream => 'TEST',
                         -file   => $tf);
    }

    # Record information for all types:
    foreach my $type ($mt->get_all_types) {
        my ($rf, $rb) = $type->reads;
        my ($tok, $tid) = ($type->name, $type->id );

        my $dir = ($rf eq $rb) ? 0 : 1;
        
        
        $self->{IDS}{TYPE}{ $tid } = [ $tid, $dir ];
        $self->{IDS}{TYPE}{ uc($rf) } = [ $tid, $dir ];
        $self->{IDS}{TYPE}{ uc($tok) } = [ $tid, $dir ];

        $self->{IDS}{TYPE}{ $tid * -1} = [ $tid, $dir * -1];
        $self->{IDS}{TYPE}{ uc($rb) } = [ $tid, $dir * -1];
        $self->{TEXT}{type_id}{ $tid } = $tok;
    }
    # $debug->branch($self->{IDS}{TYPE});die;


    # Make note of allowed self-referential edges:
    foreach my $reads ('physically interacts with',
                       'genetically interacts with',
                       'inhibits',
                       'is an antagonist for',
                       'is an agonist for',
                       'is a functional antagonist for',
                       'is a functional agonist for',
                       'is a simpler form of',
                       'is a substrate for',
                       'has part') {
        my $tid = $self->get_type_id( $reads );
        if ($tid) {
            $self->{ALLOW_SELF_REF}{$tid} = $reads;
        } else {
            $self->death( "I failed to find an ID for '$reads'" );
        }
    }

    # NOTE: Certain species, such as C. elegans, have the
    # mind-numbingly warped convention of naming the gene, transcript
    # and protein the *exact* same thing. In such cases, it could be
    # reasonable to have "FOO is a locus with FOO can be translated to
    # FOO." However, from a graph perspective this would be pretty
    # chaotic. For the time being, I am going to leave out the LOC ->
    # RNA -> PROT edges from the above list.

    $self->dismiss();

    return $self;
}

sub DESTROY {
    my $self = shift;
    # If we are dying, we should delete the IN_PROGRESS file
    $self->unlock_task();
    $self->{TRACKER} = undef;
    while (my ($key, $sdat) = each %{$self->{REDIRECT}}) {
        # Close any filehandles created via a filename
        close $sdat->{fh} if ($sdat->{file});
    }
    if ($self->{TESTMODE} && $self->{TESTMODE} !~ /hush/) {
        my $pre = ($self->{TESTMODE} =~ /quiet/) ? "At most" : "A total of";
        $self->msg("[LH]", sprintf("%s %d rows would have been written",
                                   $pre, $self->rows_written));
    }
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 tracker

 Title   : tracker
 Usage   : my $mt = $lh->tracker( );
 Function: Get the MapTracker object used by LoadHelper
 Returns : The BMS::MapTracker DBI object
 Args    : None

Returns the active BMS::MapTracker object. If a tracker object does not exist
an object will be automatically instantiated for use.

=cut

sub tracker {
    my $self = shift;
    unless ($self->{TRACKER}) {
        my $auth = $_[0] || $self->default_user();
        my $desc = $_[1];
        $self->{TRACKER} = BMS::MapTracker->new
            ( -username => $auth,
              -userdesc => $desc );
    }
    return $self->{TRACKER};
}

sub can_dismiss {
    my $self = shift;
    if (defined $_[0]) {
        $self->{DISMISS_OK} = $_[0] ? 1 : 0;
    }
    return $self->{DISMISS_OK};
}

=head2 dismiss

 Title   : dismiss
 Usage   : $lh->dismiss( )
 Function: Closes the database connection for the tracker object
 Returns : 
 Args    : None

Will close the database connection for the current tracker
object. This is useful in some highly parallel environments to prevent
maxing out the number of DB connections.

Note that if you explicitly provided a tracker object, it can not be
dismissed. This behavior is present because LoadHelper calls dismiss
automatically in several places, and it was assumed that closing the
connection on an object you created yourself would be undesirable. If
you wish to dismiss your own MapTracker object (say C<$mt>), you can
call C<< $mt->dbi->release( ) >>.

=cut

sub dismiss {
    my $self = shift;
    if ($self->can_dismiss() && $self->{TRACKER}) {
        if (my $dbi = $self->{TRACKER}{DBI}) {
            $dbi->release();
        }
    }
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 directory

 Title   : directory
 Usage   : my $path = $lh->directory($newvalue);
 Function: Get / set a directory path for the object
 Returns : A string
 Args    : None

LoadHelper operates by writing flat files to disk, which are then parsed in bulk for loading into the database. This indirection is designed to allow multiple processes to execute a load, while letting a single process manage alteration of the database (which should hopefully reduce locking and avoid duplication of identical DB operations).

The default directory is F</work5/tilfordc/maptracker>. Any other directory may be specified, although if read/write permissions are inadequate you will of course get an error. If a specified directory does not exist, LoadHelper will attempt to create it.

LoadHelper will also generate the following directory substructure:

=over 2

=item F<./ready>

=item F<./ready/name2id>

=item F<./ready/novel>

=item F<./ready/problem>

=back

Raw files are written to the base directory. When LoadHelper is confident that they have been written intact, they are moved (shell C<mv>) to F<ready>. 'Ready' files then await analysis by C<process_ready()> (typically called by a cron job). At which point seqname name strings are turned into database primary IDs (moved to F<name2id>) and duplicate rows filtered out (moved to F<novel>) before being loaded. If a load file has fatal errors, it is moved to F<problem>.

=cut

sub directory {
    my $self = shift;
    my ($path) = @_;
    if ($path) {
        # Remove terminal slash
        $path =~ s/(\/ready)?\/$//;
        $path = "$defDir/$path" unless ($path =~ /\//);
        $self->_set_dir('BASE', $path);
        $self->{STND_DIR} = ($path eq $defDir) ? 1 : 0;
        my $rpath = "$path/ready";
        $self->_set_dir('READY', $rpath);
        $self->_set_dir('ID',   "$rpath/name2id");
        $self->_set_dir('NOV',  "$rpath/novel");
        $self->_set_dir('PROB', "$rpath/problem");
    }
    return $self->{BASEDIR};
}

sub load_token {
    my $self = shift;
    my $tok  = shift;
    $self->{LOADTOKEN} = $tok if (defined $tok);
    return $self->{LOADTOKEN};
}

sub _set_dir {
    my $self = shift;
    my ($key, $path) = @_;
    $self->{uc($key) . 'DIR'} = $path;
    unless (-d $path) {
        mkdir($path, 0777);
        chmod(0777, $path);
    }
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 allow_bogus

 Title   : allow_bogus
 Usage   : $lh->allow_bogus( $value );
 Function: Set whether suspicious seqname names are allowed
 Returns : The current value (either 1 or 0)
 Args    : A true or false value

LoadHelper keeps a list of names that are not really names at all, but
string representations of null. As of writing, the list includes:

 -
 na
 n/a
 null

Ideally, your parser should scan for such entries and reject them
before passing information to LoadHelper - in reality, it can be hard
to anticipate where such entries will occur. So the default behavior
of LoadHelper is to look for these entries, and prevent them from
being used.

want to load it into the database, you can turn this surveilance off -
to do so, pass a true value, eg C<< $lh->allow_bogus( 1 ) >>. After you
have loaded the potentially bogus value, be sure to stop allowing such
values to pass surveilance C<< $lh->allow_bogus( 0 ) >>.

=cut

sub allow_bogus {
    my $self = shift;
    my ($value) = @_;
    if (defined $value) {
        $self->{ALLOW_BOGUS} = ($value) ? 1 : 0;
    }
    return $self->{ALLOW_BOGUS};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 redirect

 Title   : redirect
 Usage   : $lh->redirect( );
 Function: Redirect reporting output of the module
 Returns : A file handle
 Args    : Associative array of arguments. Recognized keys [Default]:

   -stream Required. A token identifying the type of information being
           redirected. Three tokens are recognized:

      CARP Warning messages - default *STDERR
      TEST Testmode output  - default *STDOUT
       LOG Processing log file notes (not implemented?)

     -file Path to a file which should capture the output

       -fh File handle that should capture the ouput.

It is required that either -file or -fh be provided.

=cut

sub redirect {
    my $self = shift;
    my $args = $self->parseparams( -stream => undef,
                                   -file   => undef,
                                   @_ );
    my $key  = uc($args->{STREAM});
    unless (exists $self->{REDIRECT}{$key}) {
        $self->err("Can not redirect to unknown -stream $key");
        return undef;
    }
    my $sdat = $self->{REDIRECT}{$key};
    if (my $file = $args->{FILE}) {
        $file = ">$file" unless ($file =~ /^\>/);
        my $oldfh = $sdat->{fh};
        # print $oldfh "Stream $key redirected to $file\n";
        $sdat->{fh} = $self->_get_file_handle( $file );
        $file =~ s/^\>//g;
        $sdat->{file} = $file;
        chmod(0666, $file);
    } elsif (my $fh = $args->{FH}) {
        $sdat->{fh}   = $fh;
        $sdat->{file} = undef;
    } else {
        $self->err("Specify either -file or -fh to redirect output");
    }
    # Bob's call to autoflush
    # select((select( $sdat->{fh} ), $| = 1)[$[]);
    return $sdat->{fh};
}

sub _get_file_handle {
    my $self = shift;
    my ($file) = @_;
    my $fh;
    open($fh, $file) || $self->death
        ("Failed to open file", $file, $!);
    return $fh;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 default_user

 Title   : default_user
 Usage   : my $auth = $lh->default_user( $newValue)
 Function: Gets / Sets the default user (authority).
 Returns : A BMS::MapTracker::Authority object ID
 Aliases : user(), authority()
 Args    : [0] Optional new value. In most cases you probably do not
               want to change this value

All set / kill operations have a user parameter. If you leave this
parameter empty, then the value specifed by C<default_user( )> will be
used. You can change the default authority by passing a new value (as
either the string form of the authority name, the database authority
ID, or a L<BMS::MapTracker::Authority> object). In most cases you will
want to do this only once, at the instantiation of the module.

=cut

*user = \&default_user;
*authority = \&default_user;
sub default_user {
    my $self = shift;
    if (my $req = $_[0]) {
        my $aid  = $self->get_authority_id( $req, $_[1] );
        unless ($aid) {
            $self->death("Failed to set the default user", "'$req'");
        }
        $self->{DEFAULT_UID} = $aid;
    }
    return $self->{DEFAULT_UID};
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# Define the default order of columns for each table

sub _columns {
    my $self = shift;
    my ($req) = @_;
    unless ($self->{COLUMNS}) {
        my $mt = $self->tracker;
        my $dbi = $mt->dbi;
        # Initialize
        $self->{COLUMNS} = {
            seq_class   => [ 'name_id', 'class_id', 'authority_id' ],
            seq_species => [ 'name_id', 'tax_id',   'authority_id', ],
            seq_length  => [ 'name_id', 'len',      'authority_id', ],
            location    => [ 'map_id',  'start1',   'end1', 'start2',],
            mapping     => [ 'name1',   'name2',    'authority_id', 'db_id',
                             'map_id',  'trans_id','map_score','strand',
                             'start1','end1','start2','end2',],
            pseudo_edge => [ 'name1',   'name2',   'type_id', 'space_id',
                             'authority_id', 'tag', 'value', 'number'],
            edge        => [ 'edge_id', 'name1', 'name2', 
                             'type_id', 'space_id', 'live', 'created' ],
            edge_meta   => [ 'edge_id', 'authority_id', 
                             'tag_id', 'value_id', 'numeric_value'],
            edge_auth_hist => ['edge_id','authority_id','dates','size','live'],
        };
        my %toLoad = map { $_ => 1 } keys %{$self->{COLUMNS}};
        map { delete $toLoad{ $_ } } qw(edge_meta edge_auth_hist location);
        $self->{TASK_COLS} = [ sort keys %toLoad ];

        $self->{NAME_COLS} = {};
        $self->{COL_INDEX} = {};
        $self->{NOT_NAMES} = {};
        foreach my $tab (keys %{$self->{COLUMNS}}) {
            $self->_user_column_order($tab) unless ($tab eq 'pseudo_edge');

            $self->{NAME_COLS}{$tab} = [];
            $self->{NOT_NAMES}{$tab} = [];
            $self->{COL_INDEX}{$tab} = {};
            for my $i (0..$#{$self->{COLUMNS}{$tab}}) {
                my $colname = $self->{COLUMNS}{$tab}[$i];
                if ($colname =~ /^name/ || 
                    $colname eq 'tag' || $colname eq 'value') {
                    push @{$self->{NAME_COLS}{$tab}}, $i;
                } else {
                    push @{$self->{NOT_NAMES}{$tab}}, $i;
                }
                $self->{COL_INDEX}{$tab}{$colname} = $i;
            }
        }
    }
    if ($req) {
        $req = lc($req);
        $self->death("No columns defined for '$req'") unless
            (exists $self->{COLUMNS}{$req});
        return [ @{$self->{COLUMNS}{$req}} ];
    }
    # Probably should make local copy for safety...
    return $self->{COLUMNS};
}

sub _user_column_order {
    my $self  = shift;
    $self->benchstart;
    foreach my $tab (@_) {
        my $cols  = $self->_columns( $tab );
        $self->tracker->dbi->user_column_order( $tab, @{$cols} );
    }
    $self->benchend;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head1 Set / Kill Methods

LoadHelper is designed to accept information in multiple different
formats. It is hoped that this will allow for the generation of
simple, easily-understood loaders. For all set / kill methods, the
following variables have certain safety checks and features associated
with them:

   $name - The core components of MapTracker are just text 'nodes' to
           which other data are associated. Names in LoadHelper must
           be simple strings. If the name does not already exist in
           the database, it will be automatically created.

           The database is case-insensitive, but it records the case
           of the first instance of a name it sees. So if the database
           will recognize 'bill gaTES' and 'Bill Gates' as the same
           entity, but if you would like that name displayed as the
           later, you should pass it as that case (in the event that
           your load is the first time the DB encounters it).

           The following names are not allowed:

           * Pure integers (eg 1234), or undef

           * Things that look like Perl objects - that is, they
             contain 'ARRAY(', 'HASH(' or 'CODE('

           * Certain text representations of 'null' - see allow_bogus()

  $class - Classes may be passed as BMS::MapTracker::Class objects,
           their database ID, their 'token', or their English
           name. For example, using either '2', 'NA' or 'nucleic acid'
           will all specify the same class. Names and tokens are
           case-insensitive.

   $taxa - Taxa (species) may be set with either a database ID, the
           NCBI taxa number (eg 9606 for humans), the species name
           ('Homo sapiens') an NCBI taxa alias ('man', 'human') or a
           BMS::MapTracker::Taxa object.

 $length - Must be a pure integer. Used to specify the end position of
           a biological sequence.

    $sdb - A BMS::MapTracker::SearchDB object. Can also be specified
           as a database ID, or as the name of the SearchDB.

   $auth - Authorities may be specified either by a database ID,
           BMS::MapTracker::Authority object, or case-insensitive
           name.

           If the authority is zero or undefined, the default
           authority will be used (see above). An exception is some
           kill functions, where passing a defined 0 as the authority
           will kill entries by *all* authorities.


 For all non-Name objects (Class, Relationship, Authority, SearchDB),
 if a request is made to use a non-existant entity, a warning will be
 shown and no action taken.

 Deleting information with kill methods usually needs all columns of
 the target table to be defined. However, it is sometimes useful to
 delete data by specifying only some columns (to clean up the larger
 boo-boos). In order to do this, you must pass a null_override term
 (just a boolean flag) to indicate that you are intending to pass only
 some of the columns.

=head2 set_class

 Title   : set_class
 Usage   : $lh->set_class( $name, $class, $auth)
 Function: Assign a class to a name
 Returns : 1 on success, zero on fail.
 Args    : [0] Name
           [1] Class
           [2] Authority

=cut

sub set_class {
    my $self = shift;
    $self->benchstart;
    my ($name, $cname, $auth) = @_;
    my $cid = $self->get_class_id( $cname );
    my $aid = $self->get_authority_id( $auth || $self->{DEFAULT_UID} );
    my @errs;
    push @errs, 'Unknown class' unless (defined $cid);
    push @errs, 'Unknown authority' unless ($aid);
    if (my $err = $self->_check_name( $name )) {
        push @errs, "name $err";
    }
    if ($#errs > -1) {
        $self->carp( 'set_class', \@errs,
                      [ 'Name', $name, 'Class', $cname, 'Authority', $auth ]);
        $self->benchend;
	return 0; 
    }
    push @{$self->{TO_LOAD}{'set.seq_class'}}, [ $name, $cid, $aid ];
    $self->benchend;
    $self->{OPERATIONS}++;
    return 1;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 kill_class

 Title   : kill_class
 Usage   : $lh->kill_class($name, $class, $auth, $null_override)
 Function: Delete a class assignment from the database
 Returns : 1 on success, zero on fail.
 Args    : [0] Name
           [1] Class - optional
           [2] Authority
           [3] Null override

The method requires that both the name and authority be provided,
unless C<$null_override> is true, in which case one may be
undefined. To use an undefined authority, pass 0 (zero) - an honest
C<undef> will result in the default_user() being used.

=cut

sub kill_class {
    my $self = shift;
    $self->benchstart;
    my ($name, $cname, $auth, $auth_override) = @_;
    my @errs;
    if (defined $name) {
        if (my $err = $self->_check_name( $name )) {
            push @errs, "name $err";
        }
    } else {
        $name = "";
    }
    my $cid = "";
    if (defined $cname) {
        $cid = $self->get_class_id( $cname );
        push @errs, "Unknown class" unless ($cid);
    }
    my $aid = $self->{DEFAULT_UID};
    if (defined $auth) {
        if ($auth) {
            $aid = $self->get_authority_id( $auth );
            push @errs, "Unknown authority" unless ($aid);
        } else {
            $aid = "";
        }
    }
    unless ($name && $aid) {
        if ($auth_override) {
            push @errs, "Must define either name or authority in override mode"
                unless ($name || $aid);
        } else {
            push @errs, "Must define both name and authority";
        }
    }
    if ($#errs > -1) {
        $self->carp( 'kill_class', \@errs,
                      [ 'Name', $name, 'Class', $cname, 'Authority', $auth ]);
        $self->benchend;
	return 0; 
    }
    
    push @{$self->{TO_LOAD}{'kill.seq_class'}}, [ $name, $cid, $aid ];
    $self->benchend;
    $self->{OPERATIONS}++;
    return 1;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 kill_class_deep

 Title   : kill_class_deep
 Usage   : $lh->kill_class_deep($name, $parentclass, $auth, $null_override)
 Function: Given a parent class, delete all entries assigned to that
           class, or any of its more specific children.
 Returns : 1 on success, zero on fail.
 Args    : [0] Name
           [1] Parent Class
           [2] Authority
           [3] Null override

This method is identical to kill_class(), except that kill directives
will be issued for all children of the class, too. So if you issue the
command for class 'bread', you will also kill assignments to 'Rye',
'Wheat', 'Whole Wheat' etc.

=cut

sub kill_class_deep {
    my $self = shift;
    $self->benchstart;
    my ($name, $cname, $auth, $override) = @_;
    my $mt  = $self->tracker;
    my @errs;
    if (defined $name) {
        if (my $err = $self->_check_name( $name )) {
            push @errs, "name $err";
        }
    } else {
        $name = "";
    }
    my @cids;
    my $cob = $mt->get_class( $cname );
    if ($cob) {
        my @allkids = sort { $a <=> $b } $cob->me_and_the_kids;
        @cids = map { $self->get_class_id($_) } @allkids;
    } else {
        push @errs, 'Unknown class';
    }
    my $aid = $self->{DEFAULT_UID};
    if (defined $auth) {
        if ($auth) {
            $aid = $self->get_authority_id( $auth || $self->{DEFAULT_UID} );
            push @errs, "Unknown authority" unless ($aid);
        } else {
            $aid = "";
        }
    }
    unless ($name && $aid) {
        if ($override) {
            push @errs, "Must define either name or authority in override mode"
                unless ($name || $aid);
        } else {
            push @errs, "Must define both name and authority";
        }
    }
    if ($#errs > -1) {
        $self->carp( 'kill_class_deep', \@errs,
                      [ 'Name', $name, 'Class', $cname, 'Authority', $auth ]);
        $self->benchend;
	return 0; 
    }
    push @{$self->{TO_LOAD}{'kill.seq_class'}}, map {[$name, $_, $aid]} @cids;
    $self->{OPERATIONS}++;
    $self->benchend;
    return 1;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 set_edge

 Title   : set_edge
 Usage   : $lh->set_edge( @arguments )
 Function: Assign an edge between two names
 Returns : 1 on success, zero on fail.
 Args    : Associative array of arguments. Recognized keys:

    -name1 The first node of the edge. Aliases -node1 or -left

    -name2 The second node of the edge. Aliases -node2 or -right

     -type The edge type. Alias -relationship

     -auth Default is the default authority for the LoadHelper
           object. Alias -authority

 -allow_self Default 0. LoadHelper will disallow most self-referential
           edges, generally for a good reason. If you find that
           LoadHelper is preventing you from setting a
           self-referential edge, and you do in fact wish the edge to
           be defined, pass a true value for -allow_self.

    -space Default None. The namespace of the edge (NOT the nodes) to
           be put under. You could also specify the namespace by
           prefixing the edge type with "#TheName#", eg
           "#ALIASES#shortfor". -namespace can be used as an
           alternative parameter name.

     -tags Set of meta-data tag-value pairs you wish to assign to the
           edge. Can be a hash or array reference. Multiple values can
           be provided for the same tag (requires use of an array
           reference, of course). If passing a 1-D array, the array
           will be processed as alternating tag, value pairs.

           Purely numeric values are stored in a seperate column than
           those containing non-numeric characters. If a hash or 1-D
           array is passed, the value will be inspected to determine
           if the character or numeric column should be populated. If
           you wish to populate BOTH columns (e.g. "Affinity (mM)
           0.000432"), then you must pass the tags as a 2-D array.

           So the three lines below represent identical requests:

           -tags => [ color, red, size, large, cost, 17.43 ]

           -tags => { color => red, size => large, cost => 17.43}

           -tags => [ [color,red] , [size,large], [cost,undef,17.43] ]

=cut

*add_edge = \&set_edge;
sub set_edge {
    my $self = shift;
    $self->benchstart;
    my $args  = $self->parseparams( -allow_self => 0,
                                    @_ );
    my $tname = $args->{TYPE}  || $args->{RELATIONSHIP};
    my $ereq  = $args->{SPACE} || $args->{NAMESPACE};
    my $auth  = $args->{AUTH}  || $args->{AUTHORITY};
    my ($name1, $name2) = ($args->{NAME1} || $args->{NODE1} || $args->{LEFT},
                           $args->{NAME2} || $args->{NODE2} || $args->{RIGHT});


    my $aid  = $self->get_authority_id( $auth || $self->{DEFAULT_UID} );
    my ($tid, $tdir, $nsid)   = $self->get_type_id($tname);
    my ($tag_list, $tag_errs) = $self->_tag_request_to_hash( $args->{TAGS} );

    foreach my $tuple (@{$tag_list}) {
        my ($tag, $tup) = @{$tuple};
        unless ($tag) {
            $tag_errs->{"Null tag name for value '$tup'"}++;
        }
        if (my $err = $self->_check_name( $tag )) {
            # Check to see that tag name is well-formed
            $tag_errs->{"Tag name '$tag': $err"}++;
        }
        if ($tup eq "\t") {
            # We need AT LEAST ONE of tag_val OR numeric_value
            $tag_errs->{"Null tag value for tag '$tag'"}++;
        }
        my ($val, $num) = split("\t", $tup);
        if ($val) {
            if (my $err = $self->_check_name( $val )) {
                # Check to see that tag value is well-formed
                $tag_errs->{"Tag value '$val': $err"}++;
            }
        }
    }


    my @errs = sort keys %{$tag_errs};
    push @errs, 'Unknown authority' unless ($aid);
    if (my $err = $self->_check_name( $name1 )) {
        push @errs, "name1 $err";
    }
    if (my $err = $self->_check_name( $name2 )) {
        push @errs, "name2 $err";
    }
    if (!$tid) {
        push @errs, "Unknown relationship type";
    } elsif (uc($name1||'') eq uc($name2||'') && ! $args->{ALLOW_SELF} &&
             !$self->{ALLOW_SELF_REF}{$tid} ) {
        # An entity is related to itself with a edge type that
        # is either tautological or makes no sense
        my ($n1, $space, $hc) = $self->strip_tokens($name1);
        if ($hc) {
            # This is a case-sensitive name-space, check to see if the
            # names are identical WITHOUT upper-casing them:
            my ($n2) = $self->strip_tokens($name2);
            push @errs, "Disallowed self-reference" if ($n1 eq $n2);
        } else {
            push @errs, "Disallowed self-reference";
        }
    }

    if ($ereq) {
        $nsid = $self->get_space_id( $ereq );
        push @errs, "Unknown namespace" unless ($nsid);
    }
    if ($#errs > -1) {
        $self->carp( 'set_edge', \@errs,
                      [ 'Name1', $name1, 'Name2', $name2, 
                        'Type', $tname, 'Authority', $auth,
                        'Tags', $args->{TAGS}, ]);
        $self->benchend;
	return 0; 
    }

    $tdir ||= 0;
    $nsid ||= 1;
    # Swap the names if the user is providing a reverse edge
    my @names = ($tdir < 0) ? ($name2, $name1) : ($name1, $name2);


    my $key = join("\t", @names, $tid, $nsid, $aid);
    $self->{EDGE_LOAD}{'set'}{ $key } ||= {};
    foreach my $tuple (@{$tag_list}) {
        my ($tag, $tup) = @{$tuple};
        if ($tag =~ /^(\#[^\#]{1,20}\#)/) {
            # Standardize namespaces to upper case;
            substr($tag, 0, length($1)) = uc($1);
        } else {
            # Put in the meta_tags namespace
            $tag = '#META_TAGS#' . $tag;
        }
        $self->{EDGE_LOAD}{'set'}{ $key }{$tag}{$tup}++;
    }
    $self->{OPERATIONS} += ($#{$tag_list} + 1) || 1;
    $self->benchend;
    return 1;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 kill_edge

 Title   : kill_edge
 Usage   : $lh->kill_edge( @arguments )
 Function: Delete an edge between two names, or delete some of the
           metadata assigned to that edge.
 Returns : 1 on success, zero on fail.
 Args    : Associative array of arguments. Recognized keys:

    -name1 The first node of the edge. Aliases -node1 or -left

    -name2 The second node of the edge. Aliases -node2 or -right

     -type The edge type. Alias -relationship

     -auth Default is the default authority for the LoadHelper
           object. Alias -authority

     -tags As for set_edge.

    -space Default None. The namespace of the edge (NOT the nodes) to
           be put under. You could also specify the namespace by
           prefixing the edge type with "#TheName#", eg
           "#ALIASES#shortfor". -namespace can be used as an
           alternative parameter name.

 -override Default 0. Normally, an authority must be specified. If
           none is provided, the default authority will be used. If
           you wish to delete edges regardless of authority, pass a
           value of '0' for -auth AND set -override to be true.

Please be careful - poorly specified requests can destroy huge swaths
of the UberGraph.

=cut

*delete_edge = \&kill_edge;
sub kill_edge {
    my $self = shift;
    $self->benchstart;
    my $args  = $self->parseparams( -override => 0,
                                    @_ );
    my @errs;
    my ($name1, $name2) = ($args->{NAME1} || $args->{NODE1} || $args->{LEFT},
                           $args->{NAME2} || $args->{NODE2} || $args->{RIGHT});
    if (defined $name1) {
        if (my $err = $self->_check_name( $name1 )) {
            push @errs, "name1 $err";
        }
    } else {
        $name1 = "";
    }
    if (defined $name2) {
        if (my $err = $self->_check_name( $name2 )) {
            push @errs, "name2 $err";
        }
    } else {
        $name2 = "";
    }

    my $tname = $args->{TYPE} || $args->{READS} || $args->{RELATIONSHIP};
    my ($tid, $tdir, $nsid) = ("", 0);
    if (defined $tname) {
        ($tid, $tdir, $nsid) = $self->get_type_id($tname);
        push @errs, 'Unknown type' unless ($tid);
    }
    my $ereq = $args->{SPACE} || $args->{NAMESPACE};
    if ($ereq) {
        $nsid = $self->get_space_id($ereq);
        push @errs, 'Unknown namespace' unless( $nsid );
    }

    my $aid  = $self->{DEFAULT_UID};
    my $auth = defined($args->{AUTH}) ? $args->{AUTH} : $args->{AUTHORITY};
    if (defined $auth) {
        if ($auth) {
            $aid = $self->get_authority_id( $auth || $self->{DEFAULT_UID} );
            push @errs, 'Unknown authority' unless ($aid);
        } else {
            $aid = "";
        }
    }

    my ($tag_list, $tag_errs) = $self->_tag_request_to_hash( $args->{TAGS} );
    foreach my $tuple (@{$tag_list}) {
        my ($tag, $tup) = @{$tuple};
        if ($tag) {
            if (my $err = $self->_check_name( $tag )) {
                # Check to see that tag name is well-formed
                $tag_errs->{"Tag name '$tag': $err"}++;
            }
        }
        my ($val, $num) = split("\t", $tup);
        if ($val) {
            if (my $err = $self->_check_name( $val )) {
                # Check to see that tag value is well-formed
                $tag_errs->{"Tag value '$val': $err"}++;
            }
        }
    }

    push @errs, sort keys %{$tag_errs};
    push @errs, "Must define authority" unless ( $aid || $args->{OVERRIDE});
    unless ( $name1 || $name2 || ($tname && $auth)) {
        push @errs, "Must provide at least one name, or a type + authority";
    }
    if ($#errs > -1) {
        $self->carp( 'kill_edge', \@errs,
                      [ 'Name1', $name1, 'Name2', $name2, 
                        'Type', $tname, 'Authority', $auth,
                        'Tags', $args->{TAGS}, ]);
        $self->benchend;
	return 0; 
    }

    # Swap the names if the user is providing a reverse edge
    my @names = ($tdir < 0) ? ($name2, $name1) : ($name1, $name2);
    $nsid ||= 1;

    my $key = join("\t", map {$_ || ""} (@names, $tid, $nsid, $aid));
    $self->{EDGE_LOAD}{'kill'}{ $key } ||= {};
    foreach my $tuple (@{$tag_list}) {
        my ($tag, $tup) = @{$tuple};
        if ($tag) {
            if ($tag eq '%') {
                # Special case - request to kill all tags but leave edge alive
                # Leave the token as is
            } elsif ($tag =~ /^(\#[^\#]{1,20}\#)/) {
                # Standardize namespaces to upper case;
                substr($tag, 0, length($1)) = uc($1);
            } else {
                # Put in the meta_tags namespace
                $tag = '#META_TAGS#' . $tag;
            }
        } else {
            $tag = "";
        }
        $self->{EDGE_LOAD}{'kill'}{ $key }{$tag}{$tup}++;
    }
    $self->{OPERATIONS}++;
    $self->benchend;
    return 1;
}

sub _tag_request_to_hash {
    my $self = shift;
    my $tags = shift || {};
    my @tag_list;
    my %tag_errs;
    if (ref($tags) eq 'HASH') {
        while (my ($tag, $value) = each %{$tags}) {
            push @tag_list, [$tag || "", $self->_value_to_tuple( $value )];
        }
    } elsif (ref($tags) eq 'ARRAY') {
        if (ref($tags->[0]) eq 'ARRAY') {
            # 2-D array passed
            foreach my $tuple (@{$tags}) {
                if ($#{$tuple} < 1) {
                    $tag_errs{"Tag-val-num tuple has less than 2 elements"}++
                        unless ($tuple->[0] && $tuple->[0] eq '%');
                } elsif ($#{$tuple} > 2) {
                    $tag_errs{"Tag-val-num tuple has more than 3 elements"}++;
                } else {
                    # Map undefined values to ""
                    my ($tag, $val, $num) = map { defined $_ ? $_ : "" } 
                    ( $tuple->[0], $tuple->[1], $tuple->[2]);
                    push @tag_list, [$tag, "$val\t$num"];
                }
            }
        } else {
            # 1-D array
            if ($#{$tags} < 0) {
                # Empty array - do nothing
            } elsif (($#{$tags} + 1) % 2) {
                $tag_errs{"Odd-element tag list"}++;
            } else {
                for (my $i=0; $i < $#{$tags}; $i += 2) {
                    my ($tag,$val) = ($tags->[$i], $tags->[$i+1]);
                    push @tag_list, [$tag, $self->_value_to_tuple( $val )];
                }
            }
        }
    } else {
        $tag_errs{"Could not understand -tags '$tags'"}++;
    }
    return (\@tag_list, \%tag_errs );
}

sub _value_to_tuple {
    my $self = shift;
    my ($val) = @_;
    return "\t" if (!defined $val || $val eq '');
    if ($val =~ /^[\+\-]?\d+$/ || 
        $val =~ /^[\+\-]?\d*\.\d+$/) {
        # Numeric value
        return "\t$val";
    } else {
        # Text value
        return "$val\t";
    }
}

sub _normalize_edge {
    my $self = shift;
    return unless ($self->{EDGE_LOAD});
    $self->benchstart;
    my $now = "\t" . time;
    foreach my $act (sort keys %{$self->{EDGE_LOAD}}) {
        my $tl = $self->{TO_LOAD}{"$act.pseudo_edge"} ||= [];
        foreach my $key (sort keys %{$self->{EDGE_LOAD}{$act}}) {
            my @bits = split(/\t/, $key);
            my $tags = $self->{EDGE_LOAD}{$act}{ $key };
            my @tagsets;
            foreach my $tag (sort keys %{$tags}) {
                foreach my $tuple (sort keys %{$tags->{$tag}}) {
                    push @tagsets, [ $tag, split("\t", $tuple) ];
                }
            }
            @tagsets = ([]) if ($#tagsets < 0);
            foreach my $tup (@tagsets) {
                push @{$tl}, [ @bits, @{$tup} ];
            }
        }
    }
    delete $self->{EDGE_LOAD};
    $self->benchend;
    return 1;
}


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 set_taxa

 Title   : set_taxa
 Usage   : $lh->set_taxa( $name, $taxa, $auth)
 Function: Assign a taxa to a name
 Returns : 1 on success, zero on fail.
 Aliases : set_species( )
 Args    : [0] Name
           [1] Taxa
           [2] Authority

=cut

*set_species = \&set_taxa;
sub set_taxa {
    my $self = shift;
    $self->benchstart;
    my ($name, $tname, $auth) = @_;
    my $tid = $self->get_taxa_id( $tname );
    my $aid = $self->get_authority_id( $auth || $self->{DEFAULT_UID} );
    my @errs;
    if (!defined $tid) {
        push @errs, 'Unknown taxa';
    } elsif ($tid == 12908) {
        push @errs, "Not bothering to set taxa to 'undefined'";
    } elsif ($tid == 0) {
        push @errs, "NCBI taxa ID = zero";
    }
    push @errs, 'Unknown authority' unless ($aid);
    if (my $err = $self->_check_name( $name )) {
        push @errs, "name $err";
    }
    if ($#errs > -1) {
        $self->carp( 'set_taxa', \@errs,
                      [ 'Name', $name, 'Taxa', $tname, 'Authority', $auth ]);
        $self->benchend;
	return 0; 
    }
    push @{$self->{TO_LOAD}{'set.seq_species'}}, [ $name, $tid, $aid ];
    $self->benchend;
    return 1;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 kill_taxa

 Title   : kill_taxa
 Usage   : $lh->kill_class($name, $taxa, $auth)
 Function: Delete a taxa assignment from the database
 Returns : 1 on success, zero on fail.
 Aliases : kill_species( )
 Args    : [0] Name
           [1] Taxa - optional
           [2] Authority

=cut

*kill_species = \&kill_taxa;
sub kill_taxa {
    my $self = shift;
    $self->benchstart;
    my ($name, $tname, $auth) = @_;
    my $tid = "";
    my @errs;
    if (defined $tname) {
        $tid = $self->get_taxa_id( $tname );
        push @errs, "Unknown taxa" unless ($tid);
    }
    my $aid = $self->{DEFAULT_UID};
    if (defined $auth) {
        if ($auth) {
            $aid = $self->get_authority_id( $auth || $self->{DEFAULT_UID} );
            push @errs, 'Unknown authority' unless ($aid);
        } else {
            $aid = "";
        }
    }

    if (defined $name) {
        if (my $err = $self->_check_name( $name )) {
            push @errs, "name $err";
        }
    } else {
        $name = "";
    }
    push @errs, "Must define both name and authority" unless ($name && $aid);
    if ($#errs > -1) {
        $self->carp( 'kill_taxa', \@errs,
                      [ 'Name', $name, 'Taxa', $tname, 'Authority', $auth ]);
        $self->benchend;
	return 0; 
    }

    push @{$self->{TO_LOAD}{'kill.seq_species'}}, [ $name, $tid, $aid ];
    $self->{OPERATIONS}++;
    $self->benchend;
    return 1;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 set_length

 Title   : set_length
 Usage   : $lh->set_length( $name, $length, $auth)
 Function: Assign a length to a name
 Returns : 1 on success, zero on fail.
 Args    : [0] Name
           [1] Length
           [2] Authority

=cut

sub set_length {
    my $self = shift;
    $self->benchstart;
    my ($name, $length, $auth) = @_;
    my @errs;
    unless (defined $length && $length =~ /^\d+$/) {
        push @errs, "Non-integer length";
    }
    my $aid = $self->get_authority_id( $auth || $self->{DEFAULT_UID} );
    push @errs, 'Unknown authority' unless ($aid);
    if (my $err = $self->_check_name( $name )) {
        push @errs, "name $err";
    }
    if ($#errs > -1) {
        $self->carp( 'set_length', \@errs,
                      [ 'Name', $name, 'Length', $length,'Authority', $auth ]);
        $self->benchend;
	return 0; 
    }
    push @{$self->{TO_LOAD}{'set.seq_length'}}, [ $name, $length, $aid];
    $self->{OPERATIONS}++;
    $self->benchend;
    return 1;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 kill_length

 Title   : kill_length
 Usage   : $lh->kill_length($name, $length, $auth, $override)
 Function: Delete a length assignment from the database
 Returns : 1 on success, zero on fail.
 Args    : [0] Name
           [1] Length - optional
           [2] Authority

=cut

sub kill_length {
    my $self = shift;
    $self->benchstart;
    my ($name, $length, $auth, $override) = @_;
    my @errs;
    if (defined $length) {
        push @errs, "Non-integer length" if ($length !~ /^\d+$/);
    } else {
        $length = "";
    }
    my $aid = $self->{DEFAULT_UID};
    if (defined $auth) {
        if ($auth) {
            $aid = $self->get_authority_id( $auth || $self->{DEFAULT_UID} );
            push @errs, 'Unknown authority' unless ($aid);
        } else {
            $aid = "";
        }
    }

    if (defined $name) {
        if (my $err = $self->_check_name( $name )) {
            push @errs, "name $err";
        }
    } else {
        $name = "";
    }
    push @errs, "Must define both name and authority"
        unless ($name && ($aid || $override));
    if ($#errs > -1) {
        $self->carp( 'kill_length', \@errs,
                      [ 'Name', $name, 'Length', $length,'Authority', $auth ]);
        $self->benchend;
	return 0; 
    }

    push @{$self->{TO_LOAD}{'kill.seq_length'}}, [ $name, $length, $aid ];
    $self->{OPERATIONS}++;
    $self->benchend;
    return 1;
}


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub _params_from_nosha_hit {
    my $self = shift;
    my ($hit) = @_;
    unless ($hit->isa('Bio::DB::SeqHash::Hit')) {
        $self->carp( 'params_from_nosha_hit', ["Request is not a NOSHA Hit"],
                      [ 'Request', $hit ]);
        return (undef, undef);
    }
    my $database  = $hit->subject->db;
    my $hitfile   = $database->hitfile;
    my @dbstuff   = split(/\//, $hitfile);
    my $shortdb   = $dbstuff[-1];
    my $sdb = $self->tracker->make_searchdb( -name => $shortdb,
                                             -path => $hitfile,
                                             -type => 'NOSHA' );
    my $type = '';
    if ($database =~ /unmasked/i || $database =~ /\.genome\./) {
        $type = 'Unmasked ';
    } elsif ($database =~ /masked/i) {
        $type = 'Masked ';
    }

    my $alg = $hit->algorithm;
    unless ($alg) {
        $self->carp( 'params_from_nosha_hit',["No Algorithm assigned to Hit"],
                      [  ]);
        return ($sdb, undef);
    }
    my ($auth, $desc);
    if ($alg->{MT_AUTH}) {
        # Explicitly set
        $auth = $alg->{MT_AUTH};
    } elsif ($alg->isa('Bio::DB::SeqHash::Sim4')) {
        my $min_id = $alg->param('min_percid');
        $auth = sprintf('Nosha-Sim4 %s%d', $type, $min_id );
        $desc = sprintf("Nosha-targeted Sim4 alignments against %s genomic ".
                        "data, with total identity of at least %d percent",
                        $type, $min_id);
    } elsif ($alg->isa('Bio::DB::SeqHash::Snp')) {
        $auth = "Nosha-SNP";
    } elsif ($alg->isa('Bio::DB::SeqHash::Oligo')) {
        $auth = "Nosha-Oligo";
    } else {
        $self->carp( 'params_from_nosha_hit', 
                      ["Do not know how to get authority from algorithm"],
                      [ 'Algorithm', $alg ]);
        return ($sdb, undef);
    }
    $self->tracker->get_authority( $auth, $desc);
    return ($sdb, $auth);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 set_mapping

 Title   : set_mapping
 Usage   : $lh->set_mapping( @params )
 Function: Add one or more mappings to the database

 Returns : Zero on failure. For success, if requested in array context
           all BMS::MapTracker::Mapping objects. If a scalar context
           was requested, then just the first Mapping object is
           returned.

 Aliases : add_mapping( ), add_map( ), set_map( )
 Args    : Associative array of arguments. Recognized keys [Default]:

    -name1 The name of the first member of the pair

    -name2 Second member

   -strand Relative orientation of name1 to name2; 1 or -1

 -authority The authority specifying the mapping

     -data A 2-D array reference. Each entry is the boundary
           coordinates for a single ungapped location, of the form C<[
           name1_start, name1_end, name2_start, name2_end ]>. An alias
           parameter is -locations.

    -score Optional score for the mapping, unconstrained number

 -searchdb BMS::MapTracker::SearchDB object to associate with the mapping.


You can also add the mapping by passing a single NOSHA object, either
a Bio::DB::SeqHash::Hit or Bio::DB::SeqHash::Result object.

C<< $lh->set_mapping( $noshaHitObject ) >>

C<< $lh->set_mapping( $noshaResultObject ) >>

If a result is passed, then all Hits within the result will be loaded.

=cut

*set_map     = \&set_mapping;
*add_map     = \&set_mapping;
*add_mapping = \&set_mapping;
sub set_mapping {
    my $self = shift;
    $self->benchstart;
    # Check the name syntax:
    my @maps;
    if ($#_ == 0) {
        # Hopefully a NOSHA Hit or Result
        my $nosha = $_[0];
        my @hits;
        if ($nosha->isa('Bio::DB::SeqHash::Result')) {
            @hits = $nosha->each_hit;
        } elsif ($nosha->isa('Bio::DB::SeqHash::Hit')) {
            @hits = ( $nosha );
        } else {
            push @maps, [ undef, ["Single argument '$nosha' not appropriate"]];
        }
        foreach my $hit (@hits) {
            my $result = $hit->result;
            my ($name1, $name2) = 
                ( $hit->{PROXYQRY} || $result->query->display_id,
                  $hit->{PROXYSUB} || $hit->subject->display_id );
            my ($sdb, $auth) = $self->_params_from_nosha_hit( $hit );
            my @errs;
            push @errs, "Unable to determine Search DB" unless ($sdb);
            push @errs, "Unable to determine Authority" unless ($auth);

            push @maps, [ BMS::MapTracker::Mapping->new
                          ( -transform => $self->get_transform('direct'),
                            -strand    => $hit->strand,
                            -data      => $hit->ungapped_bounds,
                            -score     => $hit->score,
                            -searchdb  => $sdb ),
                          \@errs, $hit, $name1, $name2, $auth ];
        }
    } else {
        # Working from explicitly passed parameters:
        my $args = $self->parseparams( @_ );
        my ($name1, $name2) = ( $args->{NAME1} || $args->{ID1},
                                $args->{NAME2} || $args->{ID2}, );
        my $auth = $args->{AUTHORITY} || $args->{AUTHORITY_ID} || 
            $args->{AUTH};

        my $sdb = $self->get_sdb
            ( $args->{SEARCHDB} || $args->{SEARCH_DB} || $args->{SDB});
       
        # Generate a map object to make sure the locations are ok
        # To avoid DB write calls, we will supress name1/name2
        # (aka id1/id2) as well as authority (authority_id)
        
        my $trans = $self->get_transform($args->{TRANSFORM} || 'direct');

        my $mapobj = BMS::MapTracker::Mapping->new
            ( -transform => $trans,
              -onfail    => 'return',
              -score     => $args->{SCORE} || $args->{MAP_SCORE},
              -strand    => $args->{STRAND},
              -data      => $args->{LOCATIONS} || $args->{DATA},
              -searchdb  => $sdb,
              );
        if ($mapobj) {
            $mapobj->searchdb($sdb);
            push @maps, [ $mapobj, [], $args, $name1, $name2, $auth ];
        } else {
            $self->carp( 'set_mapping', ["Coordinate failure for mapping"],
                          ['Name1',$name1, 'Name2',$name2,'Authority',$auth]);
            $self->benchend;
            return ();
        }
    }

    my $errcount = 0;
    my (@just_maps, @mappings);
    foreach my $mdat (@maps) {
        my ($map, $errs, $obj, $name1, $name2, $auth) = @{$mdat};
        push @{$errs}, 'Location errors' unless ($map);
        if (my $err = $self->_check_name( $name1 )) {
            push @{$errs}, "name1 $err";
        }
        if (my $err = $self->_check_name( $name2 )) {
            push @{$errs}, "name2 $err";
        }
        my $aid  = $self->{DEFAULT_UID};
        if (defined $auth) {
            $aid = $self->get_authority_id( $auth );
            push @{$errs}, 'Unknown authority' unless ($aid);
        }

        if ($#{$errs} > -1) {
            $errcount++;
            my @tags = ('Name1', $name1, 'Name2', $name2,'Authority', $auth);
            if (ref($obj) eq 'Bio::DB::SeqHash::Hit') {
                # Show the output of the hit
                my $fh = $self->{REDIRECT}{CARP}{fh};
                print $fh $obj->to_text;
                push @tags, ("Details", "See above");
            } else {
                # Provide some information on the location specification
                my $locs = $obj->{LOCATIONS} || $obj->{DATA};
                my $cloc = "Not an array!";
                if (!$locs || $#{$locs} < 0) {
                    $locs = [ [ $obj->{START1}, $obj->{END1},
                                $obj->{START2}, $obj->{END2}, ] ];
                    if (defined $locs->[0][0] && defined $locs->[0][1] &&
                        defined $locs->[0][3]) {
                        $cloc = 1;
                    } else {
                        $cloc = "Not passed!";
                    }
                } elsif (ref($locs) eq 'ARRAY') {
                    $cloc = $#{$locs} + 1;
                }
                push @tags, ("LocCount", $cloc);
            }
            $self->carp( 'add_mapping', $errs, \@tags);
            next;
        }
        push @just_maps, $map;

        my $sdbid = "";
        if ($map->searchdb) {
            $sdbid = $map->searchdb->id;
        }

        my ($start1, $end1, $start2, $end2) = $map->ranges;
        my $tid   = $map->transform->id;
        my $score = $map->score; $score = "" unless (defined $score);
        my $str   = $map->strand; $str = "" unless (defined $str);
        my $mid   = "";
        # Do not generate a new map_id if we are just in testmode:
        #my $mid   = 'TEST_' . sprintf("%05d", ++$self->{TEST_COUNTER});

        # Until we know that ALL maps load ok, just store info in
        # temporary structures:
        my @maprow = ( $name1, $name2, $aid, $sdbid, $mid, $tid, 
                       $score, $str, $start1, $end1, $start2, $end2 );

        foreach my $loc ($map->locations) {
            push @maprow, join(',', @{$loc});
        }
        push @mappings, \@maprow;
    }
    if ($errcount) {
        # At least one error! Bump out...
        my $success = $#just_maps + 1;
        $self->carp( 'add_mapping',["At least one mapping was NOT erroneous"],
                      [ 'GoodMaps', $success]) if ($success);
        $self->benchend;
	return 0; 
    }

    # Everything seems to be ok - load the maps into the cache:
    push @{$self->{TO_LOAD}{'set.mapping'}}, @mappings;

    $self->{OPERATIONS}++;
    $self->benchend;
    return wantarray ? @just_maps : $just_maps[0];
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 kill_mapping_by_sdb

 Title   : kill_mapping_by_sdb
 Usage   : $lh->kill_mapping_by_sdb($name, $sdb, $auth, $options)
 Function: Delete all mappings associated for a particular name from a
           specified SearchDB
 Returns : 1 on success, zero on fail.
 Aliases : kill_map_by_sdb( )
 Args    : [0] Name
           [1] SearchDB
           [2] Authority
           [3] Options

Alternatively, the method may be called with a NOSHA Hit object:

C<< $lh->kill_mapping_by_sdb( $noshaHitObject ) >>

The query and databases will be automatically determined from the
NOSHA object.

Behavior can be altered with the fourth options parameter, a
string. If it matches 'override', then the requirment for providing an
authority can be ignored. If it matches 'both' then the name will be
matched to both name1 and name2. The later is important, as the
default behavior is to match only name1. 'override both' will apply
both options.

=cut

*kill_map_by_sdb = \&kill_mapping_by_sdb;
sub kill_mapping_by_sdb {
    my $self = shift;
    $self->benchstart;
    my ($name, $dbname, $auth, $opts) = @_;
    if ($name && ref($name) eq 'Bio::DB::SeqHash::Hit') {
        # The passed argument is actually a NOSHA hit
        my $hit = $name;
        ($dbname, $auth) = $self->_params_from_nosha_hit( $hit );
        $name = $hit->result->query->display_id;
    }
    $opts = lc($opts || "");

    my @errs;
    if (defined $name) {
        if (my $err = $self->_check_name( $name )) {
            push @errs, "name $err";
        }
    } else {
        $name = "";
    }

    my $did = $self->get_sdb_id( $dbname );
    push @errs, "Unknown SearchDB" unless ($did);
    my $aid = $self->{DEFAULT_UID};
    if (defined $auth) {
        if ($auth) {
            $aid = $self->get_authority_id( $auth || $self->{DEFAULT_UID} );
            push @errs, 'Unknown authority' unless ($aid);
        } else {
            $aid = "";
        }
    }
    push @errs, "Must define name, search_db and authority"
        unless ( $name && ($aid || $opts =~ /over/) && $did);
    if ($#errs > -1) {
        $self->carp( 'kill_map_by_sdb', \@errs,
                      [ 'Name', $name, 'SearchDB',$dbname,'Authority',$auth ]);
        $self->benchend;
	return 0; 
    }
    push @{$self->{TO_LOAD}{'kill.mapping'}}, [ $name, "", $aid, $did,
                                                "","","","","","","","",];
    push @{$self->{TO_LOAD}{'kill.mapping'}}, [ "", $name, $aid, $did,
                                                "","","","","","","","",]
                                                    if ($opts =~ /both/);
    $self->{OPERATIONS}++;
    $self->benchend;
    return 1;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 kill_mapping

 Title   : kill_mapping
 Usage   : $lh->kill_mapping($name1, $name2, $auth, $override)
 Function: Delete all mappings between two sequences
 Returns : 1 on success, zero on fail.
 Aliases : kill_map( )
 Args    : [0] Name1
           [1] Name2
           [2] Authority
           [3] Null Override - allows 1 of above to be left out

Alternatively, the method may be called with a NOSHA Hit object:

C<< $lh->kill_mapping( $noshaHitObject ) >>

The query, subject and authority will be automatically determined from
the NOSHA object.

=cut

*kill_map = \&kill_mapping;
sub kill_mapping {
    my $self = shift;
    $self->benchstart;
    my ($name1, $name2, $auth, $auth_override) = @_;
    if ($name1 && ref($name1) eq 'Bio::DB::SeqHash::Hit') {
        # The passed argument is actually a NOSHA hit
        my $hit = $name1;
        my $dbname;
        ($dbname, $auth) = $self->_params_from_nosha_hit( $hit );
        ($name1,$name2) = ($hit->query->display_id, $hit->subject->display_id);
    }
    my @errs;
    my $aid = $self->{DEFAULT_UID};
    if (defined $auth) {
        if ($auth) {
            $aid = $self->get_authority_id( $auth || $self->{DEFAULT_UID} );
            push @errs, 'Unknown authority' unless ($aid);
        } else {
            $aid = "";
        }
    }
    if (defined $name1) {
        if (my $err = $self->_check_name( $name1 )) {
            push @errs, "name1 $err";
        }
    } else {
        $name1 = "";
    }
    if (defined $name2) {
        if (my $err = $self->_check_name( $name2 )) {
            push @errs, "name2 $err";
        }
    } else {
        $name2 = "";
    }
    unless ($name1 && $name2 && $aid) {
        if ($auth_override) {
            my $count = ($name1 ? 1:0) + ($name2 ? 1:0) + ($aid ? 1:0);
            if ($count < 2 && !($auth_override eq 'SINGLE' && $count == 1)) {
                push @errs, "You must define at least two terms in override ".
                    "mode (name1, name2, authority)";
            }
        } else {
            push @errs, "Must define both names and authority";
        }
    }
    if ($#errs > -1) {
        $self->carp( 'kill_mapping', \@errs,
                      [ 'Name1', $name1, 'Name2', $name2, 'Authority',$auth ]);
        $self->benchend;
	return 0; 
    }

    push @{$self->{TO_LOAD}{'kill.mapping'}}, [ $name1, $name2, $aid,
                                                 "","","","","","","","",""];
    $self->{OPERATIONS}++;
    $self->benchend;
    return 1;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head1 Convienence Methods

The following methods are designed to package common Bioinformatics
operations into convienent calls. In addition, they help maintain
loose business rules for how typical data is organized within the
database. For this reason are encouraged to use these methods whenever
possible.

=head2 process_gi

 Title   : process_gi
 Usage   : $lh->process_gi($num, $taxa, $class, $cleanold)
 Function: Automatically generate database entries for an NCBI gi identifeier
 Returns : The 'prefered' GI format ('gi1234'), a string.
 Args    : [0] The GI number. This can be a pure integer, or of any format:
               gi1234, gi:1234, g1234, g:1234
           [1] Optional taxa
           [2] Optional class

Two name entries will be made in the database, of the format "gi1234"
and "GI:1234". If taxa and class are provided, both will be set
(authority NCBI). They will be related as "gi1234 is the prefered
lexical variant of GI:1234" (authority tilfordc).

If the provided C<$num> does not look valid, an error will be displayed,
no action taken, and an empty string will be returned.

Actions performed:

  Entries linked in both direction by 'is a reliable alias for' edges
  Both entries set to class 'GI', plus any user classes provided
  Both entries set to user taxa, if provided
  Authority is 'NCBI' for all operations

=cut

sub process_gi {
    my $self = shift;
    my ($num, $taxa, $classes, $cleanold) = @_;
    $classes ||= []; 
    $classes = [ $classes ] unless (ref($classes));
    return "" unless ($num);
    if ($num =~ /^(g|gi)\:?(\d+)$/i) {
        $num = $2;
    }
    unless ($num =~ /^\d+$/) {
        $self->carp('process_gi', ["Request is inappropriate for GI"],
                     [ 'Request', $num ]);
        return "";
    }
    my @gis = ("gi$num", "GI:$num");
    foreach my $gi (@gis) {
        $self->set_class($gi, 'gi', 'ncbi');
        foreach my $class (@{$classes}) {
            $self->set_class($gi, $class, 'ncbi');
        }
        $self->set_taxa($gi, $taxa, 'ncbi') if ($taxa);
    }
    $self->set_edge( -name1 => $gis[0], 
                     -name2 => $gis[1], 
                     -type  => 'lexical', 
                     -auth  => 'tilfordc');
    if ($cleanold) {
        # Remove old associations between gi1234 and GI:1234
        $self->kill_edge( -name1 => $gis[0],
                          -name2 => $gis[1], 
                          -type  => 'alias', 
                          -auth  => 0, -override => 1 );
    }
    return $gis[0];
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 process_versioning

 Title   : process_versioning
 Usage   : $lh->process_versioning($idU, $maxv, $classes, $taxa, $auth, $sep)
 Function: Automatically populate the database with data related to versioning
 Returns : The versioned identifier (as a string)
 Args    : [0] The unversioned identifier, eg AC1234
           [1] The highest known version number, eg 3
           [2] Optional array ref of classes
           [2] Optional taxa
           [2] Authority, default authority will be used otherwise
           [2] Version separator, default is a period ('.')

The example parameters above ('AC1234', 3) would have the following
operations performed:

  Four entries added to database: AC1234, AC1234.1, AC1234.2, AC1234.3
  AC1234 set to class 'Unversioned', others set to 'Versioned'
  User classes, if any, applied to all four
  Taxa, if provided, applied to all four
  Edge 'is an unversioned accession of':
      AC1234 -> AC1234.1
      AC1234 -> AC1234.2
      AC1234 -> AC1234.3
  Edge 'is an earlier version of':
      AC1234.1 -> AC1234.2
      AC1234.2 -> AC1234.3

If C<$idU> appears to be versioned, an error will be displayed, no
action will be taken, and an empty string will be returned.

If C<$maxv> is null or zero, then an empty string is returned, and
C<$idU> has classes and taxa set if possible.

=cut

sub process_versioning {
    my $self = shift;
    my ($idU, $maxvers, $classes, $taxa, $auth, $sep, $minVers) = @_;
    return unless ($idU);
    $sep ||= '.';
    if ($idU =~ /\Q$sep\E\d+$/) {
        $self->carp('process_versioning', ["Base ID appears to be versioned"],
                     [ 'ID', $idU, 'MaxVers', $maxvers, 'Separator', $sep]);
        return "";
    }
    $classes ||= []; 
    $classes = [ $classes ] unless (ref($classes));
    $self->set_class($idU, 'unversioned', $auth);
    $self->set_taxa($idU, $taxa, $auth) if ($taxa);
    foreach my $class( @{$classes}) {
        $self->set_class($idU, $class, $auth);
    }
    return "" unless ($maxvers);
    $minVers ||= 1;
    for my $v ($minVers..$maxvers) {
        my $idV = $idU . $sep . $v;
        foreach my $class( @{$classes}) {
            $self->set_class($idV, $class, $auth);
        }
        $self->set_class($idV, 'versioned', $auth);
        $self->set_edge( -name1 => $idU, 
                         -name2 => $idV, 
                         -type  => 'unversioned', 
                         -auth  => $auth);
        $self->set_taxa($idV, $taxa, $auth) if ($taxa);
        next if ($v == 1);
        my $idP = $idU . $sep . ($v-1);
        $self->set_edge( -name1 => $idP,
                         -name2 => $idV, 
                         -type  => 'priorversion',
                         -auth  => $auth);
    }
    return $idU . $sep . $maxvers;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 process_refseq

 Title   : process_refseq
 Usage   : my ($idU, $class, $idV) = $lh->process_refseq($acc, $taxa)
 Function: Automatically set information for RefSeq IDs
 Returns : An array of three values:
           [0] The unversioned accession
           [1] The MapTracker class (string) of the sequence
           [2] The versioned accession (if a versioned acc was provided)
 Args    : [0] The accession
           [1] Optional taxa

The program will verify that the accession is of the format XX_000000
or XX_000000.0. The version number can have any number of digits,
while the accession can have between 6 and 9 digits. The program will
also determine the appropriate sequence class ('protein',
'proteinmodel', 'mrna', 'chr', etc) from the prefix. If it fails in
any of these tasks, it will complain and return an empty set.

The class will be set to the sequence class and 'RefSeq'. Taxa will be
set if provided. In addition, if the accession was a versioned
accession, then process_verisioning() will also be called.

=cut

my $refseq_formats = {
    NM => ['mrna',         [ '\d{6}', '\d{9}' ] ],
    NP => ['protein',      [ '\d{6}', '\d{9}' ] ],
    NZ => ['htgs',         [ '[A-Z]{4}\d{8}' ] ],
    AP => ['protein',      [ '\d{6}', '\d{9}' ] ],
    NC => ['chr',          [ '\d{6}', '\d{9}' ] ],
    NG => ['gdna',         [ '\d{6}', '\d{9}' ] ], 
    NR => ['ncrna',        [ '\d{6}', '\d{9}' ] ],
    NT => ['contig',       [ '\d{6}', '\d{9}' ] ],
    NW => ['assembly',     [ '\d{6}', '\d{9}' ] ],
    XM => ['mrnamodel',    [ '\d{6}', '\d{9}' ] ],
    XP => ['proteinmodel', [ '\d{6}', '\d{9}' ] ],
    XR => ['ncrnamodel',   [ '\d{6}', '\d{9}' ] ],
    YP => ['proteinmodel', [ '\d{6}', '\d{9}' ] ],
    ZP => ['proteinmodel', [ '\d{8}' ] ],
};

sub process_refseq {
    my $self = shift;
    my ($acc, $taxa, $bequiet, $oneVersOnly) = @_;
    return () if (!$acc);
    my ($prefix, $suffix, $maxvers, $idU);
    if ($acc =~ /^(\S+)\.(\d+)$/) {
        ($acc, $maxvers) = ($1, $2);
    }
    $acc = uc($acc);
    if ($acc =~ /^([A-Z]{2})_(\S+)$/) {
        ($prefix, $suffix) = ($1, $2);
        $idU  = $acc;
    } else {
        $self->carp('process_refseq', ["Does not look like RefSeq"],
                     [ 'Accession', $acc ]) unless ($bequiet);
        return ();
    }

    my $cdat = $refseq_formats->{$prefix};
    unless ($cdat) {
        $self->carp('process_refseq', ["Unknown RefSeq prefix '$prefix'"],
                     [ 'Accession', $acc ]) unless ($bequiet);
        return ();
    }

    my ($class, $regexps) = @{$cdat};
    my $isOk = 0;
    for my $i (0..$#{$regexps}) {
        my $re = $regexps->[$i];
        if ($suffix =~ /^$re$/) {
            $isOk = $re;
            last;
        }
    }

    unless ($isOk) {
        $self->carp('process_refseq', 
                     ["Suffix '$suffix' is invalid for $prefix"],
                     [ 'Accession', $acc ]);
        return ();
    }
    my $idV = $self->process_versioning
        ($idU, $maxvers, [ 'refseq', $class ], $taxa, 'refseq',undef, 
         $oneVersOnly ? $maxvers : undef);
    return ($idU, $class, $idV);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 process_reference

 Title   : process_reference
 Usage   : $lh->process_reference($ref, $object)
 Function: Automatically populate the database with data related to versioning
 Returns : The versioned identifier (as a string)
 Args    : [0] BioPerl Bio::Annotation::Reference object
           [1] Optional object to link the reference to

Useful when parsing Bio::Seq::RichSeq objects. The following
information is extracted:

  If no authority is provided, it will default to 'NCBI'
  PubMed ID is extracted as format 'PMID:###', classed as 'PubMed'
  Title extracted, placed in 'FreeText' node namespace, classed as 'TEXT'
  Set edge PMID -is a shorter term for-> Title
  Set edge $object -is referenced in-> PMID, if $object provided
  Author list extracted, for each author:
     Name is rejected if it does not look like a person
     Class set to 'Author'
     Set edge Author -contributes to-> PMID

If the PubMed ID is not provided, or is not an integer, I<and> the
reference location begins with 'Patent' and has something vaguely
looking like a patent number following it, then this path is followed
instead:

  Patent number is extracted (very poorly validated - hard to do)
  PatNum set to class 'patent'
  Set edge $object -is a member of-> PatNum, if $object provided
  If a title is found, it is treated as above, being attached to PatNum
  If author text is present, and contains text 'Assigned to ...':
     Assignee name extracted, put in node namespace 'Assignees'
     Classed as Entity
     Set edge Assignee -is the owner of-> PatNum
     Attempt made to strip out stock ticker from assignee

=cut

sub process_reference {
    my $self = shift;
    my ($refs, $obj, $auth) = @_;
    $self->benchstart;
    
    my @list;
    $refs = [ $refs ] unless (ref($refs) eq 'ARRAY');
    my @errs;
    foreach my $req (@{$refs}) {
        if ($req->isa('Bio::Seq::RichSeq')) {
            push @list, $req->annotation->get_Annotations('reference');
        } elsif ($req->isa('Bio::Annotation::Collection')) {
            push @list, $req->get_Annotations('reference');
        } elsif ($req->isa('Bio::Annotation::Reference')) {
            push @list, $req;
        } elsif ($req->isa('Bio::Seq')) {
            next;
        } else {
            push @errs, 'Unknown object $req';
        }
    }

    if ($#errs > -1) {
        $self->carp( 'process_reference', \@errs, [] );
        $self->benchend;
	return {}; 
    }

    my %pmids;
    foreach my $ref (@list) {
        my $id = $ref->pubmed || $ref->medline;
        if ($id && $id =~ /^\d+$/) {
            $self->_do_pubmed( $id, $ref, $obj, $auth, \%pmids );
        } elsif ($ref->location =~ /^Patent /) {
            $self->_do_patent( $ref, $obj, $auth );
        }
    }
    $self->benchend;
    return \%pmids;
}

sub _do_patent {
    my $self = shift;
    my ($ref, $obj, $auth) = @_;
    my $pnum;
    if ($ref->location =~ /^Patent ([^\;]+)\;/) {
        $pnum = $1;
    } else {
        return;
    }
    $self->set_class($pnum, 'patent', $auth);
    $self->set_edge( -name1 => $obj,
                     -name2 => $pnum,
                     -type  => 'memberof',
                     -auth  => $auth ) if ($obj);
    if (my $title = $ref->title) {
        $title =~ s/^[\"\']//;
        $title =~ s/[\"\']$//;
        $title =~ s/\.$//;
        $title =~ s/\s+/ /g;
        $self->set_edge( -name1 => $pnum,
                       -name2 => '#FREETEXT#'.$title,
                       -type  => 'shortfor',
                       -auth  => $auth );
    }
    if (my $assgn = $ref->authors) {
        if ($assgn =~ /Assigned to (.+)\./) {
            my $name = $1;
            if ($name =~ /^\((.+)\-\) (.+)$/) {
                my $sym = $1;
                $name = $2;
                if (length($sym) < length($name)) {
                    $sym = '#ASSIGNEES#'.$sym;
                    $self->set_class($sym, 'entity', $auth);
                    $self->set_edge( -name1 => $sym,
                                     -name2 => '#ASSIGNEES#'.$name,
                                     -type  => 'shortfor',
                                     -auth  => $auth );
                }
            }
            $name = '#ASSIGNEES#'.$name;
            $self->set_class($name, 'entity', $auth);
            $self->set_edge( -name1 => $name,
                             -name2 => $pnum,
                             -type  => 'owns',
                             -auth  => $auth );
        }
    }
}

sub _do_pubmed {
    my $self = shift;
    my ($id, $ref, $obj, $auth, $pmids) = @_;
    $auth ||= 'NCBI';
    my $pmid = "PMID:$id";
    $self->set_class( $pmid, 'PUBMED', $auth);
    my $title = $ref->title;
    if ($title) {
        $title =~ s/^[\"\']//;
        $title =~ s/[\"\']$//;
        $title =~ s/\.$//;
        $title =~ s/\s+/ /g;
        if (0) {
            # Clean up titles that are in the NULL namespace
            foreach my $term($title, substr($title, 0, 100) ) {
                $self->kill_class( $term, 'TEXT', $auth);
                $self->kill_edge( -name1 => $pmid,
                                  -name2 => $term,
                                  -type  => 'SHORTFOR',
                                  -auth  => $auth);
            }
        }
        # All titles will be put in the FREETEXT namespace:
        $title = '#FREETEXT#' . $title;
        $self->set_class( $title, 'TEXT', $auth);
        $self->set_edge( -name1 => $pmid, 
                         -name2 => $title, 
                         -type  => 'SHORTFOR', 
                         -auth  => $auth);
    }
    $pmids->{$pmid} = {
        title   => $title,
        authors => [],
    };
    my @auths = split(/[\,\;] /, $ref->authors);
    foreach my $aname (@auths) {
        my $standard = $self->standardize_author( $aname, $auth );
        if ($standard) {
            $self->set_edge( -name1 => $standard, 
                             -name2 => $pmid, 
                             -type  => 'CONTRIBUTES', 
                             -auth  => $auth);
            push @{$pmids->{$pmid}{authors}}, $standard;
        } else {
            $self->carp( 'process_reference', ['Author parsing fails'],
                         [ 'PMID', $pmid, 'Author', $aname ]);
        }
    }
    $self->set_edge( -name1 => $obj, 
                     -name2 => $pmid, 
                     -type  => 'reference', 
                     -auth  => $auth) if ($obj);
    
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 standardize_author

 Title   : standardize_author
 Usage   : $clean_name = $lh->standardize_author( $author_name);
 Function: Formats author names in a standardized fashion
 Returns : The 'prefered' author format
 Args    : The author name, as string

The method is saavy to Jr. and Sr. plus roman numerals as potential
'suffixes'. Shuffles surname and initials around to get to a standard
format C<Surname,Initials> or C<Surname,Initials Suffix>. If it fails
to find this, will return a blank string.

Common failures are seen with groups such as "European Fruit Bat
Sequencing Consortium".

=cut

sub standardize_author {
    my $self = shift;
    my ($name, $auth) = @_;
    return "" unless ($name);
    $name =~ s/ et al.\s*$//;
    my $suffix = "";
    if ($name =~ /^(.+) (Jr\.|Sr\.|I|V)$/ ||
        $name =~ /^(.+) (II|III|IV|VI|VII)\.?$/) {
        ($name, $suffix) = ($1, " " . $2);
    }
    my @bits  = split(/[\s\,]+/, $name);
    my $inits = pop @bits;
    # Remove dashes in initials (seen in Swiss-Prot)
    $inits       =~ s/\-//g;
    my $surname  = join(' ', @bits);
    return "" unless ($inits   =~  /^([A-Z]\.){1,4}$/);
    my $standard = "$surname,$inits$suffix";
    $self->set_class($standard, 'author', $auth);
    return $standard;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head1 Data Management

The following methods are used to count, inspect or commit pending operations.

=head2 data_content

 Title   : data_content
 Usage   : $lh->data_content( )
 Function: Print a text representation of pending data
 Returns : 
 Args    : 

Will draw ASCII 'database table' representations for all set / kill
requests that have not yet been cleared (by a write()
command). Normally this will be to *STDOUT, but if redirect() has been
set for the 'TEST' stream, the output will go there.

=cut

sub data_content {
    my $self = shift;
    $self->benchstart;
    $self->_normalize_edge;
    my @tasks = sort keys %{$self->{TO_LOAD}};
    my $fh = $self->{REDIRECT}{TEST}{fh};
    print $fh "<pre>\n" if ($ENV{'HTTP_HOST'} && !$self->{TFH});
    my $maxw = $self->{MAXWIDTH} + 3;
    my $showonly;
    my $tm = $self->{TESTMODE};
    if ($tm) {
        my @show;
        push @show, 'pseudo_edge' if ($tm =~ /edge/);
        push @show, 'seq_class'   if ($tm =~ /class/);
        push @show, 'mapping'     if ($tm =~ /map/);
        push @show, 'seqname'     if ($tm =~ /name/);
        push @show, 'seq_length'  if ($tm =~ /len/);
        push @show, 'seq_species' if ($tm =~ /(tax|spec)/);
        if ($#show > -1) {
            $showonly = { map { $_ => 1 } @show };
        }
    }
    my $shown = 0;
    foreach my $task (@tasks) {
        my ($action, $table) = split(/\./, $task);
        next if ($showonly && !$showonly->{$table});
        my $cs = $self->_columns( $table );
        my $data = $self->{TO_LOAD}{$task};
        my @cols = @{$cs};
        my @keep = ();
        my @width;
        for my $i (0..$#cols) {
            my $col = $cols[$i];
            my $luh = $self->{TEXT}{$col};
            my @lens;
            if ($luh) {
                @lens = sort { $b <=> $a } map 
                { length(defined $_->[$i] ? $luh->{$_->[$i]} || $_->[$i]: '') }
                @{$data};
            } else {
                @lens = sort { $b <=> $a } map 
                { length( defined $_->[$i] ? $_->[$i] : '') } @{$data};
            }
            my $max = $lens[0];
            next unless ($max);
            my $colw = length($col);
            $colw = 7 if ($colw < 7); # Nicely accomodate '-UNDEF-'
            if ($max > $maxw) {
                $max = $maxw;
            } elsif ($colw > $max) {
                $max = $colw;
            }
            push @width, $max;
            push @keep, $i;
        }
        if ($table eq 'mapping' && $action eq 'set') {
            push @width, 4;
        }
        my $frm  = '|'. join('|', map { ' %-'.$_.'s ' } @width)."|\n";
        my $hdr  = '+'.join('+', map {'-' x ($_+2) } @width) . "+\n";

        print $fh "\n==> ".uc($action). " on $table:\n\n";
        my @rows;
        for my $i (0..$#{$data}) {
            my @vals;
            for my $j (0..$#keep) {
                my $index = $keep[$j];
                my $luh = $self->{TEXT}{$cols[$index]};
                my $val = $data->[$i][ $index];
                if (!defined $val) {
                    $val = '-UNDEF-';
                } elsif ($luh) {
                    $val = $luh->{ $val } || $val || '-UNDEF-';
                }
                $val = substr($val,0,$maxw-3) . '...'
                    if ($maxw && length($val) > $maxw);
                $vals[$j] = $val;
            }
            if ($table eq 'mapping' && $action eq 'set') {
                # Add location count:
                push @vals, $#{$data->[$i]} - $#cols;
            }
            push @rows, join("\t", @vals);
        }
        if ($table eq 'mapping' && $action eq 'set') {
            push @cols, 'Locs';
            push @keep, $#cols;
        }
        my $prior = ""; my $rc = 0;
        $shown += $#rows + 1;
        foreach my $row (sort { uc($a) cmp uc($b) } @rows) {
            # Do not skip "duplicate" rows if any values are long
            # enough to have been elipses'ed (this was very
            # distressing with MSigDB, where many gene sets have long
            # names that are only different in the last few
            # characters):
            next if ($prior eq uc($row) && $prior !~ /\.\.\./);
            $self->{TOTAL_COUNT}++;
            $prior = uc($row);
            unless ($rc++ % 100) {
                print $fh $hdr;
                print $fh sprintf($frm, map { $cols[$_] } @keep);
                print $fh $hdr;
            }
            print $fh sprintf($frm, split(/\t/, $row));
        }
        print $fh $hdr;
    }
    if ($ENV{'HTTP_HOST'} && !$self->{TFH}) {
        print $fh "</pre>\n";
    } else {
        print $fh "\n" if ($shown);
    }
    $self->benchend;
    return $shown;
}

sub count_careful {
    my $self = shift;
    $self->benchstart;
    my @tasks = sort keys %{$self->{TO_LOAD}};
    my $count = 0;
    foreach my $task (@tasks) {
        my $data = $self->{TO_LOAD}{$task};
        my %hash;
        foreach my $row (@{$data}) {
            my $key = join("\t", map { defined $_ ? $_ : '-UNDEF-' } @{$row});
            $hash{$key}++;
        }
        my @unique = keys %hash;
        $count += $#unique + 1;
    }
    return $count;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 clear_data

 Title   : clear_data
 Usage   : $lh->clear_data( )
 Function: Discard all data that is stored awaiting a call to write().
 Returns : The number of operations that were discarded
 Args    :

=cut

sub clear_data {
    my $self = shift;
    foreach my $task ( keys %{$self->{TO_LOAD}} ) {
        delete $self->{TO_LOAD}{$task};
    }
    delete $self->{EDGE_LOAD};
    my $rv = $self->{OPERATIONS};
    $self->{OPERATIONS} = 0;
    return $rv;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 write

 Title   : write
 Usage   : $lh->write( )
 Function: Commit pending operations
 Returns : 
 Args    : 

The behavior of write( ) is influenced by the value of -testmode (set when the
object is created with new() ):

  Zero: Writing will result in data being written to flat files
  (inside the directory specified by directory()), pending ultimate
  alteration of the MapTracker database (via process_ready()).

  Quiet: Your assignments are silently discarded.

  Other Value: data_content() will be called to display all pending
  operations.

At the end of write( ) the internal buffers are emptied with
clear_buffer(). Also, you should always call write( ) before you exit
your script or destroy the LoadHelper object, to be sure that all
cached data is processed.

Within loops, you should use either write_threshold() or
write_threshold_quick() for optimal file generation and load speed.

=cut

sub write {
    my $self = shift;
    $self->benchstart;
    $self->_normalize_edge;
    $self->{LAST_WRITE} = time;
    if (my $tm = $self->{TESTMODE}) {
        if ($tm =~ /count/) {
            # Just report the count of rows
            my $rc = $self->count_careful;
            warn sprintf(" %d rows would have been written to DB\n", $rc)
                unless ($tm =~ /total/);
            $self->{TOTAL_COUNT} += $rc;
        } elsif ($tm !~ /quiet/) {
            # Rows will be acurately counted in data_content()
            $self->data_content;
            my $done = $self->rows_written();
        } else {
            # Just count how many rows there are
            $self->{TOTAL_COUNT} += $self->row_count;
        }
        $self->clear_data;
        $self->benchend;
        return;
    }

    my @tasks = sort keys %{$self->{TO_LOAD}};
    if ($#tasks < 0) {
        $self->benchend;
        return;
    }

    my $cnum = ++$self->{COUNTER};


    # Write out individual task files - these should never collide,
    # since the writing process will move them to READYDIR as soon as
    # they are done writting.

    my %move;
    foreach my $task (@tasks) {
        # I am now sorting the output data and removing duplicates to take load
        # off of the primary loader script
        my @rows = sort map { join("\t", @{$_}) } @{$self->{TO_LOAD}{$task}};
        next if ($#rows < 0);
        
        my $path = sprintf("%s/%d_%05d.%s",
                           $self->{BASEDIR}, $$, $cnum, $task);
        open(OUT, ">$path") || $self->death
            ("Could not write data to file", $path, $!);
        my $prior = "";
        foreach my $row (@rows) {
            next if ($prior eq $row);
            $prior = $row;
            print OUT "$row\n";
            $self->{TOTAL_COUNT}++;
        }
        close OUT;
        chmod(0666, $path);
        $move{$task} = $path;
    }

    # Collisions can occur here - if the same process is creating and
    # discarding LoadHelper objects (as the parent process might do
    # while forking), COUNTER will be reset to zero. We need a
    # mechanism for identifying existing files and avoiding collision
    # with them.
    foreach my $task (sort keys %move) {
        my $path    = $move{$task};
        my $newPath = sprintf("%s/%d_%05d.%s",
                              $self->{READYDIR}, $$, $cnum, $task);
        while (-e $newPath) {
            $cnum = ++$self->{COUNTER};
            $newPath = sprintf("%s/%d_%05d.%s",
                               $self->{READYDIR}, $$, $cnum, $task);
        }
        my $cmd = "mv $path $newPath";
        system($cmd);
    }
    $self->clear_data;
    $self->benchend;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 write_threshold

 Title   : write_threshold
 Usage   : $lh->write_threshold( $limit )
 Function: Writes pending operations when operation count exceeds threshold
 Returns : 
 Args    : [0] Number of rows needed to actually write. Default 1000.

This method will execute a write() if the total number of pending
operations exceeds the requested C<$limit>. Current operations are
counted using row_count(), which is accurate but can be slow.

Unless you really care how exact the limit is, you should use write_threshold_quick()

=head2 write_threshold_quick

 Title   : write_threshold_quick
 Usage   : $lh->write_threshold_quick( $limit, $time )
 Function: Writes pending operations when operation count exceeds threshold
 Returns : 
 Args    : [0] Number of rows needed to actually write. Default 1000.
           [1] Optional time parameter (seconds).

Essentially the same as write_threshold(), but uses operations() to
count the total number of pending rows, which is much faster than
row_count().

The time parameter is optional. It allows a write() to occur even if
the operation limit has not been reached, so long as the time elapsed
(in seconds) since the last write exceeds the value passed.

=cut

sub write_threshold {
    my $self = shift;
    my ($limit) = @_;
    $limit ||= 1000;
    $self->write if ($self->row_count > $limit);
}

sub write_threshold_quick {
    my $self  = shift;
    my ($limit, $ti) = @_;
    $limit ||= 1000;
    $self->write if ($self->operations > $limit || 
                     ($ti && $self->{LAST_WRITE} + $ti < time));
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

my @bogus_list =
    ('na', '-', 'n/a', 'null');
my $bogus_name = { map { $_ => 1 } @bogus_list };

sub _check_name {
    my $self = shift;
    my ($name) = @_;
    if (!$name) {
        return "not defined";
    }
    ($name) = $self->tracker->strip_tokens($name);
    if ($name =~ /^\d+$/i) {
        return "is a pure integer";
    } elsif ($bogus_name->{lc($name)} && !$self->{ALLOW_BOGUS}) {
        return "appears to be a bogus name";
    } elsif ($name =~ /[\t\r\n]/) {
        return "contains newlines or tabs";
    } elsif ($name =~ /(ARRAY|HASH|CODE)\(/) {
        return "is a perl object reference";
    }
    return "";
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub carp {
    my $self = shift;
    my ($func, $errs, $args) = @_;
    my $argstr = "";
    for (my $i = 0; $i < $#{$args}; $i += 2) {
        my $val = &STRUCT2STRING($args->[$i+1]);
        $val =~ s/\t/[TAB]/g;
        $val =~ s/\n/[NL]/g;
        $val =~ s/\r/[CR]/g;
        $argstr .= sprintf(" %s:'%s'", $args->[$i], $val);
    }
    my $fh = $self->{REDIRECT}{CARP}{fh};
    printf($fh " xx %s failed: %s -\n    %s\n", 
           $func, join(',', @{$errs}), $argstr);
    return "";
}

sub STRUCT2STRING {
    my ($val) = @_;
    if (!defined $val || $val eq '') {
        $val = '-UNDEF-';
    } elsif (my $r = ref($val)) {
        my $rv = $val;
        # Convert "HASH(0x20000000010915d0)" -> "HASH" :
        if ($rv =~ /^([^\(]+)\(/ ) { $rv = $1 };
        if ($r eq 'HASH') {
            my @pairs;
            my @keys = sort {uc($a) cmp uc($b)} keys %{$val};
            foreach my $key (@keys) {
                push @pairs, "$key => " . &STRUCT2STRING($val->{$key});
            }
            $val = $rv."{ " . join(",", @pairs). " }";
        } elsif ($r eq 'ARRAY') {
            $val = $rv."[ " . join(",", map{ &STRUCT2STRING($_) }@{$val})." ]";
        }
    }
    return $val;
}

sub _log {
    my $self = shift;
    my ($text, $count, $time, $u) = @_;
    my $msg = `date`;
    $u = $u ? " $u" : "";
    chomp $msg;
    $msg  = "[$msg] ";
    if (my $tok = $self->load_token() ) {
        $msg .= "($tok) ";
    } elsif (!$self->{STND_DIR}) {
        if ($self->directory() =~ /([^\/]+)$/) {
            # Use the directory name as a token
            $msg .= "(".substr($1,0,8).") ";
        } else {
            $msg .= "(*) ";
        }
    }
    $msg .= $text if ($text);
    my $timetext;
    if (defined $time) {
        my $units = 'sec';
        my $ptime = $time;
        if ($ptime > 100) {
            $ptime /= 60;
            $units = 'min';
            if ($ptime > 100) {
                $ptime /= 60;
                $units = 'hr';
            }
        }
        $timetext= sprintf("%.1f %s",$ptime, $units);
    }

    if (defined $count) {
        $msg .= " [$count$u";
        if (defined $time) {
            $msg .= " / $timetext";
            if ($time) {
                my $rate = $count / $time;
                my $ru   = 'sec';
                if ($rate < 1) {
                    $rate *= 60;
                    $ru = 'min';
                    if ($rate < 1) {
                        $rate *= 60;
                        $ru = 'hr';
                    }
                }
                $msg .= sprintf(" = %.2f%s/%s", $rate, $u, $ru);
            }
        }
        $msg .= "]";
    } elsif ($timetext) {
        $msg .= "[$timetext]";
    }


    if (my $lf = $self->{LOGFILE}) {
        open(LOGF, $lf) || $self->death("Could not append to log", $lf, $!);
        print LOGF "$msg\n";
        close LOGF;
    }

    # my $fh = $self->{REDIRECT}{LOG}{fh};
    # print $fh "$msg\n";
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub get_class_id {
    my $self = shift;
    my ($name) = @_;
    return undef unless (defined $name);
    my $ucname = uc($name);
    return $self->{IDS}{CLASS}{ $ucname } if ($self->{IDS}{CLASS}{ $ucname });
    my $class = ref($name) ? $name : $self->tracker->get_class($name);
    return undef unless ($class);
    my $cid = $class->id;
    $self->{IDS}{CLASS}{ $ucname }  = $cid;
    $self->{TEXT}{class_id}{ $cid } = $class->name;
    return $cid;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# In array context will return (type_id, direction, space_id)
sub get_type_id {
    my $self = shift;
    my ($text) = @_;
    return wantarray ? () : undef unless ($text);
    my ($name, $nsid) = $self->strip_tokens($text);
    my $ucname = uc($name);
    my $tinfo = $self->{IDS}{TYPE}{ uc($name) };
    return wantarray ? () : undef unless ($tinfo);
    return wantarray ? (@{$tinfo}, $nsid) : $tinfo->[0];
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub get_type_info {
    my $self = shift;
    my ($name) = @_;
    return [] unless (defined $name);
    my $ucname = uc($name);
    return $self->{IDS}{TYPEFULL}{ $ucname } if 
        ($self->{IDS}{TYPEFULL}{ $ucname });
    my ($type, $dir, $space) = $self->tracker->get_type($name);
    return [] unless ($type);
    my $tid = $type->id;
    $self->{TEXT}{type_id}{ $tid } = $type->name;
    return $self->{IDS}{TYPEFULL}{ $ucname } = [$tid, $dir, $space];
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub get_taxa_id {
    my $self = shift;
    my ($name) = @_;
    return undef unless ($name);
    my $obj;
    if (ref($name)) {
        if ($name->isa('BMS::MapTracker::Taxa')) {
            $obj  = $name;
            $name = $obj->name;
        } else {
            return undef;
        }
    }
    my $ucname = uc($name);
    unless ( defined $self->{IDS}{TAXA}{ $ucname } ) {
        unless ($obj) {
            my @taxas = $self->tracker->get_taxa($name);
            $obj = $taxas[0] if ($#taxas == 0);
            $self->dismiss();
        }
        if ($obj) {
            my $id = $obj->id;
            $self->{TEXT}{tax_id}{ $id } = $obj->name;
            $self->{IDS}{TAXA}{ $ucname } = $id;
        } else {
            $self->{IDS}{TAXA}{ $ucname } = 0;
        }
    }
    return $self->{IDS}{TAXA}{ $ucname };
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub get_authority_id {
    my $self = shift;
    my ($name) = @_;
    return undef unless ($name);
    my $obj;
    if (ref($name)) {
        if ($name->isa('BMS::MapTracker::Authority')) {
            $obj  = $name;
            $name = $obj->name;
        } else {
            return undef;
        }
    }
    my $ucname = uc($name);
    unless ( defined $self->{IDS}{AUTHORITY}{ $ucname } ) {
        unless ($obj) {
            $obj = $self->tracker->get_authority($name);
            $self->dismiss();
        }
        if ($obj) {
            my $id = $obj->id;
            $self->{TEXT}{authority_id}{ $id } = $obj->name;
            $self->{IDS}{AUTHORITY}{ $ucname } = $id;
        } else {
            $self->{IDS}{AUTHORITY}{ $ucname } = 0;
        }
    }
    return $self->{IDS}{AUTHORITY}{ $ucname };
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub get_sdb {
    my $self = shift;
    my ($name, $path, $type) = @_;
    return undef unless ($name);
    my $obj;
    if (ref($name)) {
        if ($name->isa('BMS::MapTracker::Searchdb')) {
            $obj  = $name;
            $name = $obj->name;
        } else {
            return undef;
        }
    }
    my $ucname = uc($name);
    unless ( defined $self->{OBJS}{SDB}{ $ucname } ) {
        unless ($obj) {
            if ($path && $type) {
                $obj = $self->tracker->make_searchdb
                    ( -name => $name,
                      -path => $path,
                      -type => $type,);
            } else {
                $obj = $self->tracker->make_searchdb
                    ( -name     => $name,
                      -nocreate => 1 );
            }
            $self->dismiss();
        }
        if ($obj) {
            my $id = $obj->id;
            $self->{TEXT}{db_id}{ $id } = $obj->name;
            $self->{OBJS}{SDB}{ $ucname } = $obj;
            $self->{OBJS}{SDB}{ $id } = $obj;
        } else {
            $self->{OBJS}{SDB}{ $ucname } = 0;
        }
    }
    return $self->{OBJS}{SDB}{ $ucname };
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub get_sdb_id {
    my $self = shift;
    my ($name, $path, $type) = @_;
    my $obj = $self->get_sdb($name, $path, $type);
    return undef unless ($obj);
    return $obj->id;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

# returns ($id, $case_sensitive) in array context
sub get_space_id {
    my $self = shift;
    my ($name) = @_;
    return wantarray ? (undef,undef) : undef unless ($name);
    my $obj;
    if (ref($name)) {
        if ($name->isa('BMS::MapTracker::Namespace')) {
            $obj  = $name;
            $name = $obj->name;
        } else {
            return wantarray ? (undef,undef) : undef;
        }
    }
    my $ucname = uc($name);
    unless ( defined $self->{IDS}{SPACE}{ $ucname } ) {
        unless ($obj) {
            $obj = $self->tracker->get_namespace($name);
            $self->dismiss();
        }
        if ($obj) {
            my $id = $obj->id;
            my $hc = $obj->case_sensitive;
            $self->{TEXT}{space_id}{ $id } = $obj->name;
            $self->{IDS}{SPACE}{ $ucname } = [$id, $hc];
        } else {
            $self->{IDS}{SPACE}{ $ucname } = [0,0];
        }
    }
    return wantarray ? @{$self->{IDS}{SPACE}{ $ucname }} : 
        $self->{IDS}{SPACE}{ $ucname }[0];
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub get_transform {
    my $self = shift;
    my ($name) = @_;
    return undef unless ($name);
    my $ucname = uc($name);
    unless ( defined $self->{OBJS}{TRANSFORM}{ $ucname } ) {
        my $obj = $self->tracker->get_transform( $name );
        $self->dismiss();
        if ($obj) {
            $self->{OBJS}{TRANSFORM}{ $ucname } = $obj
        } else {
            $self->{OBJS}{TRANSFORM}{ $ucname } = 0;
        }
    }
    return $self->{OBJS}{TRANSFORM}{ $ucname };
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 row_count

 Title   : row_count
 Usage   : $lh->row_count( )
 Function: Counts the number of data rows stored in internal buffers
 Returns : An integer
 Args    :

This method accurately counts the number of rows (or pseudo rows for
maps and edges) that will be written out if write() were called. See also operations().

=head2 operations

 Title   : operations
 Usage   : $lh->operations( )
 Function: Estimates the number of data rows stored in internal buffers
 Returns : An integer
 Args    :

Estimates the number of pending rows. Should provide a value identical
or very close to row_count() in most situations.

=cut

sub row_count {
    my $self = shift;
    my $count = 0;
    $self->_normalize_edge;
    foreach my $key (keys %{$self->{TO_LOAD}}) {
	$count += $#{$self->{TO_LOAD}{$key}} + 1;
    }
    return $count;
}

sub operations {
    my $self = shift;
    return $self->{OPERATIONS};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 rows_written

 Title   : rows_written
 Usage   : my $count = $lh->rows_written
 Function: Reports the total number of rows recorded.
 Returns : An integer
 Args    :

If using testmode in 'quiet' or 'count' mode, this will be an estimate
that may be higher than the number of rows that would actually be
written.

=cut

sub rows_written {
    my $self = shift;
    return $self->{TOTAL_COUNT};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 


=head1 Database Loading

The following methods are used to actually alter the relational
database. Under normal conditions, these methods are utilized by
periodic cron jobs that scan the load folders.

You may find that you want to manually execute these methods
(particularly process_ready()). One case would be that you have
defined a non-standard load directory (using directory()). Another is
that you want to make sure all data is loaded into the database before
proceeding, because you will need to query the new results in later
parts of your code.

=head2 delete_duplicates

 Title   : delete_duplicates
 Usage   : $lh->delete_duplicates( )
 Function: Delete ALL duplicate rows from the database.
 Returns : 
 Args    : 

This function is EXTREMELY slow. You should not use it unless you are
a database administrator.

=cut

sub delete_duplicates {
    my $self   = shift;
    my $mt     = $self->tracker;
    my $dbi    = $mt->dbi;
    $self->benchstart;
    my $dup_cols = {
        seq_class   => [ 'name_id', 'class_id', 'authority_id'],
        #seq_species => [ 'name_id', 'tax_id', 'authority_id'],
        #seq_length  => [ 'name_id', 'len', 'authority_id'],
    };

    my $lim = 1000;
    foreach my $tab (sort keys %{$dup_cols}) {
        my $cola = $dup_cols->{$tab};
        next unless ($cola);
        my @cols = @{$cola};
        my @pairs = ("a.oid IN ( SELECT oid FROM $tab )");

        my $colGroup = join(', ', @cols);
        my $sql = "CREATE TABLE temp_$tab AS SELECT count(*), $colGroup FROM $tab GROUP BY $colGroup HAVING count(*) > 1";
        $dbi->do( -sql   => $sql,
                  -name  => "Extract duplicated rows in $tab",
                  -level => 0);
        
        $self->death("WORKING");

        my $sth = $dbi->prepare
            ( -sql   => "SELECT count, $colGroup FROM temp_$tab",
              -name  => "Retrieve information on duplicate rows in $tab",
              -level => 2,
              -limit => $lim );

        while (1) {
            my $rows = $sth->selectall_arrayref();
            last if ($#{$rows} < 0);
            foreach my $row (@{$rows}) {
                my $count = shift @{$row};
                if ($count > 1) {
                    my @pairs;
                    for my $i (0..$#cols) {
                        push @pairs, "$cols[$i] = $row->[$i]";
                    }
                }
            }            
        }


        # Using the above SQL, we can NOT guarantee that a.oid will always be
        # the same oid. For this reason, we can have a situation like:

        # 1708117408  -  1699167122
        # 1699167122  -  1708117408

        # (empirically observed). The logic below is designed to make
        # sure that we always keep one entry from the duplicates - if
        # we just deleted all rows based on oids from one column
        # (a.oid or b.oid), we could end up deleting everything.

        my $deleted = 0;
        my $start = time;
        while (1) {
            my $rows = $sth->selectall_arrayref();
            last if ($#{$rows} < 0);
            my (%group, %ingroup);
            my $counter = 0;
            foreach my $row (@{$rows}) {
                my ($aid, $bid) = @{$row};
                if ($ingroup{$aid} && $ingroup{$bid}) {
                    # We already have the two in the same group:
                    next if ($ingroup{$aid} == $ingroup{$bid});

                    # Both entries were already assigned to a group.
                    # We will keep A-group, add in the entries from
                    # B-group, and then delete B-group altogether.

                    my $gid = $ingroup{$aid};
                    my $og  = $ingroup{$bid};
                    foreach my $id ( keys %{$group{$og}}) {
                        $group{$gid}{$id} = 1;                    
                        $ingroup{$id} = $gid;
                    }
                    delete $group{$og};
                } elsif ($ingroup{$aid}) {
                    # Add B to the group A is in
                    my $gid = $ingroup{$aid};
                    $ingroup{$bid} = $gid;
                    $group{$gid}{$bid} = 1;                    
                } elsif ($ingroup{$bid}) {
                    # Add A to the group B is in
                    my $gid = $ingroup{$bid};
                    $ingroup{$aid} = $gid;
                    $group{$gid}{$aid} = 1;
                } else {
                    # Neither in a group, make a new group
                    $counter++;
                    $group{$counter} = {
                        $aid => 1,
                        $bid => 1,
                    };
                    $ingroup{$aid} = $counter;
                    $ingroup{$bid} = $counter;
                }
            }

            while (my ($gid, $hash) = each %group) {
                my @ids = sort {$a <=> $b } keys %{$hash};
                my $keep = shift @ids;
                next if ($#ids < 0);
                my $dql = "DELETE FROM $tab WHERE oid IN (".
                    join(",", @ids) . ")";
                #$mt->_showSQL($dql, "Remove duplicate rows in $tab [$keep]");
                $dbi->do($dql);
                $deleted += $#ids + 1;
            }
            if ($deleted > 5000) {
                $self->_log("Deleted duplicates from $tab", $deleted, 
                            time - $start);
                $start = time;
                $deleted = 0;
            }
        }
        $self->_log("Deleted duplicates from $tab", $deleted, 
                    time - $start) if ($deleted);
    }
    $self->benchend;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 process_ready

 Title   : process_ready
 Usage   : $lh->process_ready( @args )

 Function: Read the base directory for any files awaiting loading, and
           upload that data into the MapTracker database.

 Returns : Zero if another process had locked the directory, 1 if
           processing was succesfully performed.

 Args    : Associative array of arguments. Recognized keys [Default]:

   -prefix If provided, only process files that start with the
           indicated prefix (which should just be a series of
           digits). This option is primarily useful for debugging, if
           you want to test load a small number of files.

 -cautious Optional integer value. If provided, then concatenation of
           files awaiting load will be broken down by prefix - so if
           you use -cautious 2, then the files will be subcategorized
           by their first two digits. That would result in at most 100
           separate concatenations. Cautious is useful to avoid
           concatenating a huge number of input files.

=cut

sub process_ready {
    my $self  = shift;
    my $args  = $self->parseparams
        ( -recursion => 0,
          -haltfile  => "/work5/tilfordc/maptracker/HaltFile.txt",
          @_ );
    my $recursion = $args->{RECURSION};
    $self->can_dismiss(0);
    $self->benchstart;
    my $cols  = $self->_columns;
    my $done  = 0;
    my $start = $self->hitime;
    unless ($recursion) {
        $self->{PR_START} = $start;
        $recursion = 0;
    }
    
    if (my $haltFile = $args->{HALTFILE}) {
        if (-e $haltFile) {
            my $msg = `cat $haltFile` || "No details found in file";
            $msg = "\nMapTracker data loading is halted due to the presence of a Haltfile:\n  $haltFile\nMessage from file:\n\n$msg\n\nTo begin data loading, the file must be removed.\nIf you are not the creator of the file, you should find that person BEFORE considering removing it!\n\n  ";
            warn $msg;
            return 0;
        }
    }

    $self->{LOAD_BENCH} = $args->{BENCHMARK} ? 1 : 0;

    $self->tracker->dbi->prepare
        ("SET escape_string_warning TO 'off'")->execute();

    # One task will turn name strings into name_ids:
    $done += $self->get_ids();
    # Another task removes uneccesary rows, and generates edge_ids:
    $done += $self->gather();
    # The final task loads / deletes data:
    $done += $self->load_novel();

    if ($done) {
        my $elapsed = time - $start;
        if ($elapsed > 60) {
            # If we took longer than a minute, check again to see what has
            # arrived for us in the interim.
            $done += $self->process_ready( -recursion => $recursion + 1);
        }
        unless ($recursion) {
            my $elapsed = $self->hitime - $self->{PR_START};
            $self->_log("All operations finished", $done, $elapsed);
        }
    }
    $self->benchend;
    return $done;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub load_novel {
    my $self   = shift;
    $self->benchstart;
    my $novdir = $self->{NOVDIR};
    my $prbdir = $self->{PROBDIR};
    my $done   = 0;
    foreach my $tab (@{$self->{TASK_COLS}}) {
        # Do not do anything if the table is being vacuumed
        if ($tab =~ /edge/) {
            next if ($self->check_task("Vacuum edge") || 
                     $self->check_task("Vacuum edge_meta") ||
                     $self->check_task("Vacuum edge_auth_hist") );
        } else {
            next if ($self->check_task("Vacuum $tab"));
        }
        my $task = "Load $tab";
        next unless ($self->lock_task($task));
 
        opendir(NOVDIR, $novdir) || $self->death
            ( "Failed to read directory contents", $novdir, $!);
        my @files;
        foreach my $file (readdir NOVDIR) {
            my $full  = "$novdir/$file";
            # We will now set or kill entries for the current table
            if ($file =~ /Batch\.([^\.]+).+\.novel$/) {
                next unless ($1 eq $tab);
            } else {
                if ($file ne '.' && $file ne '..') {
                    $self->_log("$file - Unknown file type in novel");
                    system("mv $full $prbdir/$file");
                }
                next;
            }
            push @files, [ $file, $full, -M $full ];
        }

        # Sort oldest files to front of list:
        @files = sort { $b->[2] <=> $a->[2] } @files;
        foreach my $fdat (@files) {
            my ($file, $full, $fsize) = @{$fdat};
            my $total = 0;
            my $data  = {};

            open( NF, "<$full") || $self->death
                ( "Failed to read novel file", $full, $!);
            $self->{PARSING_FILE} = $full;
            while (<NF>) {
                chomp;
                my @cols = split(/\t/, $_);
                my $act  = shift @cols;
                push @{$data->{$act}}, \@cols;
                $total++;
            }
            close NF;

            if ($tab eq 'pseudo_edge') {
                $done += $self->_update_edges($data);
            } else {
                if ($tab eq 'mapping') {
                    $done += $self->_set_mapping( $data->{'set'});
                } elsif (exists $self->{COLUMNS}{$tab}) {
                    # For other tables we should only have set operations
                    # $done += $self->_set_basic( $tab, $data->{'set'});
                } else {
                    $self->_log("$file - no functionality for loading '$tab'");
                    system("mv $full $prbdir/$file");
                    next;
                }
                # For these tables we should only be setting - verify:
                delete $data->{'set'};
                my @rm = keys %{$data};
                if ($#rm > -1) {
                    $self->_log("$file - Unknown actions : ".join(',',@rm));
                    system("mv $full $prbdir/$file");
                    next;
                }
            }
            unlink($full);
            warn $self->showbench() if ($self->{LOAD_BENCH} && $total);
        }
        closedir NOVDIR;
        $self->unlock_task($task);
    }
    $self->benchend;
    return $done;
}

sub _update_edges {
    my $self = shift;
    $self->benchstart;
    my ($data)    = @_;
    # $debug->branch($data);
    my $mt        = $self->tracker;
    my $dbi       = $mt->dbi;

    my $killMeta = $dbi->prepare
        ( -sql   =>
          "DELETE FROM edge_meta WHERE edge_id = ? AND authority_id = ?",
          -name  => "Delete all meta tags for edge",
          -level => 3,);
    my $killMetaFull = $dbi->prepare
        ( -sql   =>
          "DELETE FROM edge_meta WHERE edge_id = ?",
          -name  => "Delete all meta tags for edge for ALL authorities",
          -level => 3,);
    my $findFullAuth = $dbi->prepare
        ( -sql   =>
          "SELECT authority_id FROM edge_auth_hist WHERE edge_id = ?",
          -name  => "Get all authorities for an edge",
          -level => 3,);
    my $checkTag = $dbi->prepare
        ( -sql   => "SELECT set_tag_func(?::integer,?::integer,?::integer,?::integer,?::numeric)",
          -name  => "Check if a tag is already set",
          -level => 3,);

    my $lockSTH  = $dbi->named_sth("Lock edge_meta exclusively");

    my %by_eid;
    # Organize data by edge_id
    while (my ($act, $rows) = each %{$data}) {
        my $isSet = ($act eq 'set') ? 1 : 0;
        foreach my $row (@{$rows}) {
            my ($eid, $aid, $tag, $val, $num) = @{$row};
            unless ($eid && defined $aid) {
                $self->death("Update Edge: Failed to recover both edge_id and auth_id", join(' + ', map { defined $_ ? $_ : '-NULL-' } ($eid, $aid, $tag, $val, $num)), $self->{PARSING_FILE}, "Line $.");
            }
            if ($tag || $val || defined $num) {
                # Some tag information has been specified
                push @{$by_eid{$eid}{$aid}{$act}}, [$tag, $val, $num];
            } else {
                # Make sure the hash keys are at least in place
                $by_eid{$eid}{$aid}{$act} ||= [];
            }
        }
    }
    # Order edge_ids to take advantage of BETWEEN, if possible

    my @stack     = sort { $a <=> $b } keys %by_eid;
    my $eidCount  = $#stack + 1;
    my $blockSize = 100;
    my %summary;
    my $tot = 0;
    # $blockSize = 5; warn "DEBUGGING IN BLOCKS OF $blockSize";
    while ($#stack != -1) {
        # Work in small groups of edge IDs
        my @eids = splice(@stack, 0, $blockSize);
        my (%setLive, %killTokens, %setTokens);

        # Figure out what is being set and what is being killed
        # self->benchstart('organize');
        # About 2 msec per 100
        # Having this outside of locking should help a wee bit.

        # The locking is a real problem. In retrospect I would not have
        # done it this way but do not have time to alter the system
        # at the moment.

        my (@km, @kmf);
        foreach my $eid (@eids) {
            while (my ($aid, $ahash) = each %{$by_eid{$eid}}) {
                foreach my $act ('kill', 'set') {
                    my $tagList = $ahash->{$act};
                    next unless ($tagList);

                    if ($act eq 'kill') {
                        # We are deleting data for this edge
                        my %kt;
                        if ($#{$tagList} == -1) {
                            # We are killing the entire edge
                            # Remove all tags
                            if ($aid) {
                                # Explicitly defined authority
                                # $killMeta->execute($eid, $aid);
                                push @km, [$eid, $aid];
                                $setLive{$eid}{$aid} ||= 'f';
                            } else {
                                # ALL authorities - we need to find all
                                # existing authority IDs to set history flags:
                                my @aa = $findFullAuth->
                                    get_array_for_field($eid);
                                map { $setLive{$eid}{$_} ||= 'f' } @aa;
                                # $killMetaFull->execute($eid);
                                push @kmf, $eid;
                            }
                        } else {
                            # We are killing specific tags, but not edge
                            foreach my $tags (@{$tagList}) {
                                my ($tag, $val, $num) = @{$tags};
                                $tag ||= 0;
                                if ($tag eq '%') {
                                    # Special request to clear all tags
                                    if ($aid) {
                                        # $killMeta->execute($eid, $aid);
                                        push @km, [$eid, $aid];
                                        $summary{'meta cleared'}++;
                                    } else {
                                        # $killMetaFull->execute($eid);
                                        push @kmf, $eid;
                                        $summary{'meta purged'}++;
                                    }
                                    %kt = ();
                                    last;
                                } else {
                                    my $tok = join("\t", $tag, $val || 0,
                                                   defined $num ? $num : 'x');
                                    $kt{$tok} = 1;
                                }
                            }
                        }
                        # We will kill meta tags in bulk
                        map { $killTokens{$_}{$aid}{$eid} = 1 } keys %kt;
                    } else {
                        # We are setting this edge
                        $setLive{$eid}{$aid} = 't';
                        foreach my $tags (@{$tagList}) {
                            my ($tag, $val, $num) = @{$tags};
                            my $tok = join("\t", $tag, $val || 0,
                                           defined $num ? $num : 'x');
                            push @{$setTokens{$tok}{$aid}}, $eid;
                        }
                    }
                }
            }
        }
        # $self->benchend('organize');

        $dbi->begin_work;
        $lockSTH->execute();

        $self->benchstart("Delete meta tags bulk");
        map { $killMeta->execute( @{$_} ) } @km;
        map { $killMetaFull->execute( $_ ) } @kmf;
        $self->benchend("Delete meta tags bulk");
        

        # Do bulk deletes of specific meta tags
        $self->benchstart("Delete meta tags");
        while (my ($tok, $auths) = each %killTokens) {
            my $setDat = exists $setTokens{$tok} ? $setTokens{$tok} : {};
            my ($tag, $val, $num) = split("\t", $tok);
            my $bsql = "DELETE FROM edge_meta WHERE authority_id = ?";
            $bsql   .= " AND tag_id = $tag"   if ($tag);
            $bsql   .= " AND value_id = $val" if ($val);
            $bsql   .= " AND numeric_value = $num" if ($num ne 'x');
            $bsql   .= ' AND edge_id %s';
            while (my ($aid, $eidHash) = each %{$auths}) {
                # Do not bother deleting entries that we will just replace:
                my $setAidDat = exists $setDat->{$aid} ? $setDat->{$aid} : [];
                map { delete $eidHash->{$_} } @{$setAidDat};
                my @eids = keys %{$eidHash};
                my @sths = $self->_list_to_range_sths
                    ( $bsql, \@eids, "Delete specific meta tags" );
                my $delnum = $#eids + 1;
                next unless ($delnum);
                $summary{'meta deleted'} += $delnum;
                foreach my $sthdat (@sths) {
                    my ($sth, $bindlist) = @{$sthdat};
                    foreach my $binds (@{$bindlist}) {
                        $sth->execute($aid, @{$binds});
                    }
                }
            }
        }
        $self->benchend("Delete meta tags");

        $self->benchstart("Set meta tags");
        my @stoks = keys %setTokens;
        my @set_meta;
        foreach my $stok (@stoks) {
            my ($tag, $val, $num) = split("\t", $stok);
            my @brow = ($tag, $val, $num eq 'x' ? undef : $num);
            while (my ($aid, $eids) = each %{$setTokens{$stok}}) {
                foreach my $eid (@{$eids}) {
                    my @binds = ($eid, $aid, @brow);
                    push @set_meta, \@binds if
                        ($checkTag->get_single_value(@binds));
                }
            }
        }
        my $toDo = $#set_meta + 1;
        $summary{'meta added'} += $toDo;
        $dbi->write_array( 'edge_meta', \@set_meta) if ($toDo);
        $self->benchend("Set meta tags");
        
        # Set the timestamps and change edge 'live' flag as needed
        my %stamps;
        while (my ($eid, $auths) = each %setLive) {
            # Re-organize by the live status (t/f)
            while (my ($aid, $status) = each %{$auths}) {
                push @{$stamps{$status}}, [$eid, $aid];
            }
        }
        while (my ($status, $edgeList) = each %stamps) {
            if ($status eq 'f') {
                $self->_timestamp_edges_false( $edgeList, \%summary );
            } else {
                $self->_timestamp_edges_true( $edgeList, \%summary );
            }
        }
        $dbi->commit;
        $tot += $#eids + 1;
        #warn "DONE: $tot\n".$self->showbench();
    }
    #warn $self->showbench();
    #die;
    
    my $total = 0;
    my @bits;
    foreach my $key (sort keys %summary) {
        if (my $num = $summary{$key}) {
            push @bits, "$num $key";
            $total += $num;
        }
    }
    @bits = "No changes" if ($#bits == -1);
    $self->benchend;
    $self->_log("Table edge - ".join(', ', @bits),
                $eidCount, $self->lastbench('full')) if ($eidCount);
    return $total;
}



sub _timestamp_edges_true {
    my $self = shift;
    my ($data, $summary) = @_;
    return 0 unless ($data);
    my $tscount = $#{$data} + 1;
    return 0 unless ($tscount);
    $self->benchstart('survey');
    my $mt  = $self->tracker;
    my $dbi = $mt->dbi;

    # For each edge/auth pair, find the current state
    my $getCurrentStatus = $dbi->prepare
        ( -sql   =>
          "SELECT live FROM edge_auth_hist".
          " WHERE edge_id = ? AND authority_id = ?",
          -name  => "Get the live status for an edge / authority pair",
          -level => 3,);
    
    my (%seen, @toChange, @toCreate);
    foreach my $dat (@{$data}) {
        my ($eid, $aid) = @{$dat};
        next if ($seen{$aid}{$eid}++);
        my $rows = $getCurrentStatus->selectall_arrayref( $eid, $aid );
        if ($#{$rows} == -1) {
            # No entry created yet
            push @toCreate, $dat;
        } elsif ($#{$rows} != 0) {
            # Whoops - error!
            $self->death("Multiple edge_auth_hist entries",
                         "edge_id = $eid AND authority_id = $aid");
        } elsif ($rows->[0][0] == 0) {
            # The pair is currently false
            push @toChange, $dat;
        }
        # Otherwise the entry exists and is already true - nothing more
        # needs to be done.
    }
    my $numChanges = $#toCreate + $#toChange + 2;
    $self->benchend('survey');
    return $numChanges unless ($numChanges);

    my $bulksize = 5000;
    my $now      = $self->db_timestamp;
    my $lockSTH  = $dbi->named_sth("Lock edge_auth_hist exclusively");

    # The update is expensive - perform it only when needed
    # (ie, the edge is not already true)
    my $makeEdgeTrue = $dbi->prepare
        ( -sql   => 
          "UPDATE edge SET live = 't'".
          " WHERE edge_id = ? AND live != 't'",
          -name  => "Update edge to be live",
          -level => 3,);

    unless ($#toCreate == -1) {
        # We need to create some new pair entries
        $self->_user_column_order( 'edge_auth_hist' );
        $summary->{created} += $#toCreate + 1;
        #$dbi->begin_work;
        #$self->benchstart('acquire lock');
        #$lockSTH->execute();
        #$self->benchend('acquire lock');

        $self->benchstart('validate');
        my @toInsert;
        foreach my $dat (@toCreate) {
            my ($eid, $aid) = @{$dat};
            my $rows = $getCurrentStatus->selectall_arrayref( $eid, $aid );
            # If another process has created the pair, ignore it
            next unless ($#{$rows} == -1);
            # Otherwise, add it to the insertion block
            push @toInsert, [$eid, $aid,"{$now}", 1, 't'];
        }
        $self->benchend('validate');
        next if ($#toInsert == -1);

        $self->benchstart('create EAH');
        $dbi->write_array( 'edge_auth_hist', \@toInsert);
        $self->benchend('create EAH');

        $self->benchstart('set edge live');
        map { $makeEdgeTrue->execute( $_->[0] ) } @toInsert;
        $self->benchend('set edge live');

        # $dbi->commit;
    }

    unless ($#toChange == -1) {
        # There are some false entries we need to switch to true
        $summary->{'on'} += $#toChange + 1;
        my $updatePairToTrue = $dbi->prepare
            ( -sql   =>
              "UPDATE edge_auth_hist ".
              "   SET live = 't', dates[ size + 1 ] = '$now', size = size + 1".
              " WHERE edge_id = ? AND authority_id = ? AND live = 'f'",
              -name  => "Update live edge history to be true",
              -level => 3,);

        # Do the updates in a non-locking mode
        $self->benchstart('set EAH to true');
        map { $updatePairToTrue->execute( $_->[0], $_->[1] ) } @toChange;
        $self->benchend('set EAH to true');

        my $verifyPairIsTrue = $dbi->prepare
            ( -sql   =>
              "SELECT edge_id FROM edge_auth_hist ".
              " WHERE edge_id = ? AND authority_id = ? AND live = 't'",
              -name  => "Confirm that edge/auth pair is still live",
              -level => 3,);

        # Validate and change edge while locked; it is possible that another
        # process has since set the pair to be false while we were getting
        # organized.

        #$dbi->begin_work;
        #$self->benchstart('acquire lock');
        #$lockSTH->execute();
        #$self->benchend('acquire lock');

        my @confirmForUpdate;
        $self->benchstart('confirm for update');
        foreach my $dat (@toChange) {
            my ($eid, $aid) = @{$dat};
            # Make sure another process has not set to be false:
            my ($check) = $verifyPairIsTrue->
                get_array_for_field( $eid, $aid );
            push @confirmForUpdate, $eid if ($check);
        }
        $self->benchend('confirm for update');

        $self->benchstart('set edge live');
        map { $makeEdgeTrue->execute( $_ ) } @confirmForUpdate;
        $self->benchend('set edge live');
        
        #$dbi->commit;
    }

    return $numChanges;
}

sub _timestamp_edges_false {
    my $self = shift;
    my ($data, $summary) = @_;
    return 0 unless ($data);
    my $tscount = $#{$data} + 1;
    return 0 unless ($tscount);
    $self->benchstart('survey');
    my $mt  = $self->tracker;
    my $dbi = $mt->dbi;

    # For each edge/auth pair, we want to act only if the pair is now live
    my $findLive = $dbi->prepare
        ( -sql   =>
          "SELECT size FROM edge_auth_hist".
          " WHERE edge_id = ? AND authority_id = ? AND live = 't'",
          -name  => "Find live notations that need to be changed",
          -level => 3,);

    my (%seen, @toChange);
    foreach my $dat (@{$data}) {
        my ($eid, $aid) = @{$dat};
        next if ($seen{$aid}{$eid}++);
        my ($sz) = $findLive->get_array_for_field( $eid, $aid );
        push @toChange, $dat if ($sz);
    }
    $self->benchend('survey');
    return 0 if ($#toChange == -1);

    # At least one pair needs to be updated
    my $now = $self->db_timestamp;

    # This statement is designed to only update when appropriate
    my $updatePairToFalse = $dbi->prepare
        ( -sql   =>
          "UPDATE edge_auth_hist ".
          "   SET live = 'f', dates[ size + 1 ] = '$now', size = size + 1".
          " WHERE edge_id = ? AND authority_id = ? AND live = 't'",
          -name  => "Update live edge history to be false",
          -level => 3,);
    
    my $isEdgeStillAlive = $dbi->prepare
        ( -sql   =>
          "SELECT authority_id FROM edge_auth_hist".
          " WHERE edge_id = ? AND live = 't'",
          -name  => "Determine if any authorities remain alive for an edge",
          -limit => 1,
          -level => 3,);

    # If we get to the point of setting a whole edge false, it means
    # that the authority was previously true, so the whole edge should 
    # have been true, too - we will not bother checking to see if the
    # edge is true in the WHERE clause.
    my $makeEdgeFalse = $dbi->prepare
        ( -sql   => 
          "UPDATE edge SET live = 'f' WHERE edge_id = ?",
          -name  => "Update edge to be live = false",
          -level => 3,);

    my $lockSTH  = $dbi->named_sth("Lock edge_auth_hist exclusively");

    $self->_user_column_order( 'edge_auth_hist' );

    # Lock the database
    #$dbi->begin_work;
    #$self->benchstart('acquire lock');
    #$lockSTH->execute();
    #$self->benchend('acquire lock');

    # Set the pair to be false, or do nothing if another process has
    # already done so:
    $self->benchstart('set EAH to false');
    map { $updatePairToFalse->execute( $_->[0], $_->[1] ) } @toChange;
    $self->benchend('set EAH to false');
    $summary->{'off'} += $#toChange + 1;

    # Now see if there are any remaining Authorities set to live
    $self->benchstart('survey all authorities');
    my @fullyFalse;
    foreach my $dat (@toChange) {
        my $eid = $dat->[0];
        my ($liveOne) = $isEdgeStillAlive->get_array_for_field( $eid );
        # If no authorities claim this edge as live, capture the edge_id
        push @fullyFalse, $eid unless ($liveOne);
    }
    $self->benchend('survey all authorities');

    my $full = $#fullyFalse + 1;
    if ($full) {
        # There are some edges where all authorities are false
        # We need to set the whole edge as false
        $self->benchstart('set edge false');
        map { $makeEdgeFalse->execute( $_ ) } @fullyFalse;
        $self->benchend('set edge false');
        $summary->{'full off'} += $full;
        $summary->{'off'} -= $full;
    }
    # $dbi->commit;
    return $#toChange + 1;
}

sub _list_to_range_sths {
    my $self = shift;
    my ($base, $array, $name) = @_;

    # Take an array (passed as reference):
    my @arr    = sort {$a <=> $b } @{$array};
    my $seed   = shift @arr;
    return () unless ($seed);
    my @ranges = ( [ $seed, $seed ] );
    # Find continuous ranges of values:
    foreach my $id (@arr) {
        my $last = $ranges[-1];
        if ($id == $last->[1]) {
            # Duplicate value
        } elsif ($id == $last->[1] + 1) {
            # The range is continuous, extend it
            $last->[1] = $id;
        } else {
            # Need to start a new range
            push @ranges, [ $id, $id ];
        }
    }

    # Now turn into SQL, and find singletons
    my (@singles, %sql);
    foreach my $range (@ranges) {
        my ($x,$y) = @{$range};
        if ($y - $x < 5) {
            # If range is less than 5 elements, break it back out to singles
            for my $i ($x..$y) {
                push @singles, $i;
            }
        } else {
            # Record as a range
            push @{$sql{"BETWEEN ? AND ?"}}, [$x,$y];
            #push @sql, "BETWEEN $x AND $y";
        }
    }

    # Set the singles as groups of up to 50:
    my $bulksize = 20;
    my $bulkList = join(",", map { '?' } (1..$bulksize));
    while ($#singles + 1 >= $bulksize) {
        my @chunk = splice( @singles, 0, $bulksize);
        push @{$sql{"IN ( $bulkList )"}}, \@chunk;
    }

    # Finally, record individual leftovers
    foreach my $id (@singles) {
        push @{$sql{"= ?"}}, [$id];
    }

    my @sths;
    my $dbi = $self->tracker->dbi;
    $name ||= "";
    while (my ($where, $list) = each %sql) {
        my $sth = $dbi->prepare
            ( -sql   => sprintf($base, $where),
              -name  => $name || "Anonymous ranged select",
              -level => 3,);
        push @sths, [$sth, $list];
    }
    return @sths;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub gather {
    my $self  = shift;
    my $iddir  = $self->{IDDIR};
    my $novdir = $self->{NOVDIR};
    my $prbdir = $self->{PROBDIR};
    my $mt     = $self->tracker;
    my $dbi    = $mt->dbi;
    my $done   = 0;
    foreach my $tab (@{$self->{TASK_COLS}}) {
        # Do not do anything if the table is being vacuumed
        if ($tab =~ /edge/) {
            next if ($self->check_task("Vacuum edge") || 
                     $self->check_task("Vacuum edge_meta") ||
                     $self->check_task("Vacuum edge_auth_hist") );
        } else {
            next if ($self->check_task("Vacuum $tab"));
        }
        my $task = "Gather $tab";
        next unless ($self->lock_task($task));

        opendir(IDDIR, $iddir) || $self->death
            ("Failed to read contents of gather directory", $iddir, $!);
        my @files;
        foreach my $file (readdir IDDIR) {
            my $full = "$iddir/$file";
            if ($file =~ /Batch\.([^\.]+).+\.ids$/) {
                next unless ($1 eq $tab);
            } else {
                unless ($file =~ /\.novel/ || $file eq '.' || $file eq '..') {
                    $self->_log("$file - Unknown file type in name2id");
                    system("mv $full $prbdir/$file");
                }
                next;
            }
            push @files, [ $file, $full, -M $full ];
        }
        # Sort oldest files to front of list:
        @files = sort { $b->[2] <=> $a->[2] } @files;
        foreach my $fdat (@files) {
            my ($file, $full, $fsize) = @{$fdat};
            # We are looking for text files generated by LoadHelper set/kill
            my ($lines, $total, $tokill) = (0,0,0);
            $self->{PARSING_FILE} = $full;
            $self->benchstart;
            my $data = {};
            open( NF, "<$full") || $self->death
                ("Failed to read gather file", $full, $!);
            while (<NF>) {
                chomp;
                my @cols = split(/\t/, $_);
                my $act  = shift @cols;
                push @{$data->{$act}}, \@cols;
                $total++;
            }
            close NF;

            if ($tab eq 'pseudo_edge') {
                $lines = $self->_gather_edges( $data );
            } elsif ($tab eq 'mapping') {
                $lines = $self->_gather_maps( $data);
            } elsif (exists $self->{COLUMNS}{$tab}) {
                # Basic entries are now immediately loaded
                # ($lines, $tokill) = $self->_gather_basic( $tab, $data);
                $self->_update_basic( $tab, $data );
            } else {
                $self->_log("$file - No mechanism to scan '$tab'");
                $self->benchend;
                $self->{PARSING_FILE} = "";
                system("mv $full $prbdir/$file");
                next;
            }

            my $count = $#{$lines || []} + 1;
            if ($count) {
                # Now write the appropriate lines to a new file
                my $file = sprintf("Batch.%s.%08d.%06d.novel", $tab, $$, 
                                   ++$self->{COUNTER});
                open(OUT, ">$iddir/$file") || $self->death
                    ("Failed to write to gather file", "$iddir/$file", $!);
                foreach my $line (@{$lines}) {
                    print OUT "$line\n";
                }
                close OUT;
                # The next stage is complete - move to its load directory:
                system("mv $iddir/$file $novdir/$file");
            }
            if ($tokill && $#{$tokill} != -1) {
                foreach my $where (@{$tokill}) {
                    # If duplicate entries were found, get rid of them now
                    # The original entry should be safely in $file
                    my $sql = "DELETE FROM $tab WHERE $where";
                    $mt->_showSQL($sql, "Delete duplicate rows from $tab", 2);
                    $dbi->do($sql);
                }
                $self->_log("Table $tab - Duplicate entries removed");
            }
            unlink($full);
            $self->{PARSING_FILE} = "";
            $self->benchend;
            $done += $total;
            warn $self->showbench() if ($self->{LOAD_BENCH} && $total);
        }
        closedir IDDIR;
        $self->unlock_task($task);
    }
    return $done;
}

sub _gather_edges {
    my $self = shift;
    $self->benchstart;
    my ($data) = @_;
    my $mt     = $self->tracker;
    my $tin    = $mt->type_information();
    my (%keep, %summary);

    my $rnum = 0;
    foreach my $act ('set','kill') {
        my $rows = $data->{$act};
        next unless ($rows);
        $rnum += $#{$rows} + 1;
        # Turn the edge specifications into tokens:
        my @tokens;
        my $is_partial = ($act eq 'set') ? 0 : 1;
        foreach my $row (@{$rows}) {
            my ($n1, $n2, $tid, $nsid) = 
                (shift @{$row}, shift @{$row}, shift @{$row}, shift @{$row});
            my $token;
            if ($is_partial) {
                $token = $mt->partial_edge_columns_to_key
                    ([$n1, $n2, $tid, $nsid], $tin);
            } else {
                unless ($n1 && $n2 && $tid) {
                    unshift @{$row}, "NOT FULLY SPECIFIED";
                    next;
                }
                $token = $mt->edge_columns_to_key
                    ([$n1, $n2, $tid, $nsid], $tin);
            }
            unshift @{$row}, $token;
            push @tokens, $token;
        }
        # Turn the tokens into edge_ids (in bulk)
        my ($edges, $counts) = $mt->bulk_edge2id( \@tokens, $is_partial);
        while (my ($tag, $val) = each %{$counts}) { $summary{$tag} += $val; }
        # $debug->branch({ rows => $rows, edges => $edges} );
        # Now replace the tokens as appropriate

        foreach my $row (@{$rows}) {
            my $token = shift @{$row};
            my $eids  = $edges->{$token};
            unless ($eids) {
                # Either this was a kill on non-existing edges, or it was
                # a malformed set
                next;
            }
            my $aid = shift @{$row} || 0;
            if ($is_partial) {
                # Killing edges - we expect an array reference
                unless (ref($eids) eq 'ARRAY') {
                    $self->_log("Edge ID list '$eids' for '$token' not array");
                    next;
                }
                my $tagDat   = join("\t", @{$row}) || '';
                my $fullKill = ($tagDat eq "\t\t" || $tagDat eq '') ? 1 : 0;
                foreach my $eid (@{$eids}) {
                    my $line = join("\t", $act, $eid, $aid, $tagDat);
                    $keep{$line}++;
                }
            } else {
                # Setting edges - there will be but one edge_id
                unless ($eids =~ /^\d+$/) {
                    $self->_log("Edge ID '$eids' for '$token' not integer");
                    next;
                }
                my $line = join("\t", $act, $eids, $aid, @{$row});
                $keep{$line}++;
            }
        }
    }
    my @lines = sort keys %keep;

    my @bits;
    foreach my $key (sort keys %summary) {
        my $num = $summary{$key};
        push @bits, "$num $key" if ($num);
    }
    
    my $what = ($#bits == -1) ? 
        "No edges identified??" : join(' + ', @bits)." IDs";
    $self->benchend;
    $self->_log("Table edge - $what",
                $rnum, $self->lastbench('full')) if ($rnum);

    return \@lines;
}

sub _gather_maps {
    my $self = shift;
    my ($data) = @_;
    $self->_kill_maps( $data->{'kill'});
    my $toset = $data->{'set'};
    return undef unless ($toset);

    # For now, mapping data is loaded in-toto
    # Later, we may want to identify existing rows and remove them.
    $self->benchstart();
    my (%keep);
    my $total = 0;
    foreach my $row (@{$toset}) {
        my $line = join("\t", 'set', @{$row});
        $total++ unless ($keep{$line}++);
    }
    $self->benchend();
    $self->_log("Table mapping - $total distinct lines", $#{$toset} + 1,
                $self->lastbench('full'));
    return [sort keys %keep];
}

sub _kill_maps {
    my $self = shift;
    my ($tokill) = @_;
    return unless ($tokill);
    my $count = $#{$tokill} + 1;
    return unless ($count);
    my $tab  = 'mapping';
    my $cols = $self->{COLUMNS}{$tab};
    $self->death("Can not kill data for unknown table '$tab'") unless ($cols);

    $self->benchstart;
    my $mt   = $self->tracker;
    my $dbi  = $mt->dbi;
    my $mids = 0;
    foreach my $row (@{$tokill}) {
        my @wherebits;
        for my $i (0..$#{$row}) {
            my $val = $row->[$i];
            next unless (defined $val && $val ne '');
            if ($val =~ /^\d+$/) {
                $val = "= $val";
            } else {
                $val = $self->_name_to_where($val);
            }
            push @wherebits, $cols->[$i] . " $val";
        }
        my $where = join(' AND ', @wherebits);
        my $sql   = "SELECT map_id from $tab WHERE $where";
	my @ids = sort { $a <=> $b } $dbi->get_array_for_field
            ( -sql   => $sql,
              -name  => "Find maps to delete",
              -level => 2 );

        next if ($#ids < 0);
        $mids += $#ids + 1;
        while (my @set = splice(@ids, 0, 50) ) {
            my $midstr = join(",", @set);
            # Delete location entries, then mapping entries:
            foreach my $kt ('location', 'mapping') {
                my $kill = "DELETE FROM $kt WHERE map_id IN ($midstr)";
                $dbi->do( -sql   => $kill,
                          -name  => "Delete mapping data from $kt",
                          -level => 2,);
            }
        }
    }
    $self->benchend;
    my $act = $count ? sprintf
        ("kill %d entr%s", $mids, $mids == 1 ? 'y' : 'ies') : "No changes";
    $self->_log("Table $tab - $act", $count, $self->lastbench('full'));
    return $mids;
}

# SELECT name_id, count(name_id) FROM seq_class WHERE name_id in (5294141,9359192,9371895,9372369) AND class_id = 3 AND authority_id = 84 group by name_id;

sub _gather_basic {
    my $self = shift;
    my ($tab, $data) = @_;
    # We need to kill off data first, before we check to see if anything
    # needs to be (re)set
    $self->_kill_basic( $tab, $data->{'kill'});
    my $toset = $data->{'set'};
    return () unless ($toset);
    $self->benchstart;
    my $mt     = $self->tracker;
    my $dbi    = $mt->dbi;
    my @cols   = @{$self->{COLUMNS}{$tab}};
    my (%keep, %kill);

    # Only bother passing on those entries not already in DB
    foreach my $row (@{$toset}) {
        my $where = $self->_generic_sql( $row, \@cols );
        # Skip if we could not construct a proper where clause:
        next unless ($where);
        my $sth   = $dbi->prepare
            ( -sql   => "SELECT count(*) FROM $tab WHERE $where",
              -name  => "Find existing entries in $tab",
              -level => 3 );
        my $count = $sth->get_single_value();
        # If there is a unique value in the DB, skip
        next if ($count == 1);
        # No entry in database, or more than one
        my $line = join("\t", 'set', @{$row});
        $keep{$line}++;
        if ($count > 1) {
            # Oops. More than one entry. This should not happen, but we
            # check to be sure
            $kill{$where} = 1;
        }
    }
    $self->benchend;
    return ( [sort keys %keep], [ keys %kill ]);
}

sub _set_basic {
    my $self = shift;
    my ($tab, $toset) = @_;
    return 0 unless ($toset);
    my $count = $#{$toset} + 1;
    return 0 unless ($count);
    $self->benchstart;
    $self->tracker->dbi->write_array( $tab, $toset );
    $self->benchend;
    $self->_log("Table $tab - set", $count, $self->lastbench('full'));
    return $count;
}

sub _kill_basic {
    my $self = shift;
    my ($tab, $tokill) = @_;
    return unless ($tokill);
    my $count = $#{$tokill} + 1;
    return unless ($count);
    my $cols = $self->{COLUMNS}{$tab};
    $self->death("Can not kill data for unknown table '$tab'") unless ($cols);
    $self->benchstart;
    my $mt   = $self->tracker;
    my $dbi  = $mt->dbi;
    foreach my $row (@{$tokill}) {
        my @wherebits;
        for my $i (0..$#{$row}) {
            my $val = $row->[$i];
            next unless (defined $val && $val ne '');
            if ($val =~ /^\d+$/) {
                $val = "= $val";
            } else {
                $val = $self->_name_to_where($val);
            }
            push @wherebits, $cols->[$i] . " $val";
        }
        my $where = join(' AND ', @wherebits);
        my $sth = $dbi->prepare
            ( -sql   => "DELETE from $tab WHERE $where",
              -name  => "Delete entries from $tab",
              -level => 2 );
        $sth->execute();
    }
    $self->benchend;
    $self->_log("Table $tab - kill", $count, $self->lastbench('full'));
    return $count;
}

sub _update_basic {
    my $self = shift;
    $self->benchstart;
    my ($tab, $data) = @_;
    my $mt           = $self->tracker;
    my $dbi          = $mt->dbi;
    my $cols         = $self->{COLUMNS}{$tab};
    my $toset        = $data->{set};
    my $tokill       = $data->{kill};
    my $opcount      = $#{$tokill} + $#{$toset} + 2;
    my @cols         = @{$self->{COLUMNS}{$tab}};
    
    my (%checkSet, @willAdd, @willRemove, @willReAdd, %summary);
    # Prebuild the where clauses for setting entries
    foreach my $row (@{$toset}) {
        my $where = $self->_generic_sql( $row, \@cols );
        # Skip if we could not construct a proper where clause:
        $checkSet{$where} = $row if ($where);
    }

    # Delete rows from database
    foreach my $row (@{$tokill}) {
        my @wherebits;
        my $type = 'deletions';
        for my $i (0..$#{$row}) {
            my $val = $row->[$i];
            unless (defined $val && $val ne '') {
                # Deleting all entries for this column
                $type = 'extended deletions';
                next;
            }
            if ($val =~ /^\d+$/) {
                $val = "= $val";
            } else {
                # Generally a wild card request, or case-sensitive
                $val  = $self->_name_to_where($val);
                $type = 'complex deletions';
            }
            push @wherebits, $cols->[$i] . " $val";
        }
        my $where = join(' AND ', @wherebits);
        unless ($where) {
            $self->death("Failed to generate WHERE clause for delete",
                         "table $tab", $self->{PARSING_FILE} || "-FILE ?-");
        }
        # Ignore the kill request if the clause matches a set request:
        next if ($checkSet{$where});
        # Kill the rows
        my $sth = $dbi->prepare
            ( -sql   => "DELETE from $tab WHERE $where",
              -name  => "Delete entries from $tab",
              -level => 2 );
        $sth->execute();
        $summary{$type}++;
    }

    my $checkBase = "SELECT count(*) FROM $tab WHERE ";
    while (my ($where, $row) = each %checkSet) {
        my $sth   = $dbi->prepare
            ( -sql   => $checkBase . $where,
              -name  => "Find existing entries in $tab",
              -level => 3 );
        my $count = $sth->get_single_value();
        if ($count == 1) {
            # No need to do anything - unique entry already in DB
        } elsif ($count) {
            # Uh-oh - multiple counts, we need to remove them and re-add
            push @willRemove, $where;
            push @willReAdd,  $row;
        } else {
            # No entries, need to add a single row
            push @willAdd, $row;
        }
    }

    unless ($#willRemove == -1) {
        foreach my $where (@willRemove) {
            my $sth = $dbi->prepare
                ( -sql   => "DELETE from $tab WHERE $where",
                  -name  => "Delete entries from $tab",
                  -level => 2 );
            $sth->execute();
        }
        $dbi->write_array( $tab, \@willReAdd );
        $summary{'duplicates'} = $#willRemove + 1;
    }

    unless ($#willAdd == -1) {
        $dbi->write_array( $tab, \@willAdd );
        $summary{'added'} = $#willAdd + 1;
    }
    
    my $total = 0;
    my @bits;
    foreach my $key (sort keys %summary) {
        my $num = $summary{$key};
        if ($num) {
            $key =~ s/s$// if ($num == 1);
            push @bits, "$num $key";
            $total += $num;
        }
    }
    @bits = "No changes" if ($#bits == -1);
    $self->benchend;
    $self->_log("Table $tab - ".join(', ', @bits),
                $opcount, $self->lastbench('full')) if ($opcount);
    return 0;
}

sub _set_mapping {
    my $self = shift;
    my ($toset) = @_;
    return 0 unless ($toset);
    my $count = $#{$toset} + 1;
    return 0 unless ($count);
    $self->benchstart;
    $self->_user_column_order('mapping', 'location');

    my $mt   = $self->tracker;
    my $dbi  = $mt->dbi;

    my (@mapping, @location);
    foreach my $row (@{$toset}) {
        my $mid   = $dbi->nextval('mapping_seq');
        
        my @locs  = splice( @{$row}, 12 );
        $row->[4] = $mid;
        
        push @mapping, $row;
        next if ($#locs < 1);
        foreach my $loc (@locs) {
            my ($s1, $e1, $s2) = split(/\,/, $loc);
            push @location, [ $mid, $s1, $e1, $s2 ];
        }
    }
    $dbi->write_array( 'mapping',  \@mapping );
    $dbi->write_array( 'location', \@location ) if ($#location > -1);

    my $tot = $#location + $#mapping + 2;
    $self->benchend;
    $self->_log("Table mapping + location - set $tot", 
                $count, $self->lastbench('full'));
    return $count;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub get_ids {
    my $self = shift;
    # Do not do anything if the table is being vacuumed
    return 0 if ($self->check_task("Vacuum seqname"));
    my $task = "Names to IDs";
    return 0 unless ($self->lock_task($task));
    $self->benchstart;
    my $ready    = $self->{READYDIR};

    # Organize the files by batch ID
    opendir(READY, $ready) || $self->death
        ("Failed to read contents of ID file", $ready, $!);
    my %struct;
    foreach my $file (readdir READY) {
        # We are looking for text files generated by LoadHelper set/kill
        if ($file =~ /(\S+)\.(kill|set)\.(.+)/) {
            my ($batch, $act, $remain) = ($1, $2, $3);
            unless ( $struct{$batch} ) {
                $struct{$batch} = {
                    batch => $batch,
                    age   => 0,
                    files => [],
                };
            }
            my $age = -M "$ready/$file";
            $struct{$batch}{age} = $age if ($struct{$batch}{age} < $age);
            push @{$struct{$batch}{files}}, "$act.$remain";
        }
    }
    closedir READY;

    # Order batches by oldest first
    my @batches = sort { $b->{age} <=> $a->{age} } values %struct;
    my $output  = {};
    my $done    = 0;
    foreach my $obj (@batches) {
        my $batch = $obj->{batch};
        my @bits = @{$obj->{files}};
        foreach my $bit (@bits) {
            my ($act, $tab);
            if ($bit =~ /^([^\.]+)\.([^\.]+)/) {
                ($act, $tab) = ($1, $2);
            } else {
                next;
            }
            $output->{$tab} ||= {
                rows  => [],
                files => [],
            };
            my $file = "$ready/$batch.$bit";
            open(BIT, "<$file") || $self->death
                ( "Failed to read file", $file, $!);
            while (<BIT>) {
                chomp;
                push @{$output->{$tab}{rows}}, [ $act, split(/\t/, $_) ];
            }
            close BIT;
            push @{$output->{$tab}{files}}, $file;
        }
        $done += $self->_act_on_stack($output);
    }
    $done += $self->_act_on_stack($output, 'force');
    $self->unlock_task($task);
    $self->benchend;
    return $done;
}

sub _act_on_stack {
    my $self = shift;
    my ($output, $force) = @_;
    my @tabs    = sort keys %{$output};
    my $maxrows = 100000;
    my $mt      = $self->tracker;
    my $ready   = $self->{READYDIR};
    my $iddir   = $self->{IDDIR};
    my $done    = 0;
    foreach my $tab (@tabs) {
        my $rows = $output->{$tab}{rows};
        # Only process this table if we have reached a row limit, or if
        # the call included a force parameter:
        next unless ($force || $#{$rows} >= $maxrows);
        $self->benchstart;
        # What columns are names?
        my @ncols = @{$self->{NAME_COLS}{$tab}};
        # First pass identifies the names we need to lookup:
        my %name2id;
        foreach my $row (@{$rows}) {
            my $act = $row->[0];
            foreach my $index (@ncols) {
                my $val = $row->[$index + 1];
                # Skip empty cells
                next unless ($val);
                # Assume that a percent sign is a wildcard kill:
                next if ($val =~ /\%/ && $act eq 'kill');
                $name2id{ $val } = 0;
            }
        }
        #$debug->branch([$rows, \%name2id]);
        # Get the name_ids in bulk:
        my @tokens = keys %name2id;
        # my @sids = $mt->bulk_seq2id( @tokens );
        my $sdat = $mt->bulk_seq2id( @tokens );
        my ($sids, $novel) = @{$sdat};
        for my $i (0..$#tokens) {
            $name2id{ $tokens[$i] } = $sids->[$i];
        }

        # Now make a pass through the data again, substituting IDs
        my $file = sprintf("Batch.%s.%08d.%06d.ids", $tab, $$, 
                           ++$self->{COUNTER});
        my %redun;
        open(OUT, ">$ready/$file") || $self->death
            ( "Failed to write to file", "$ready/$file", $!);
        foreach my $row (@{$rows}) {
            my $act = $row->[0];
            foreach my $index (@ncols) {
                my $val = $row->[$index + 1];
                next if (!$val || $val =~ /\%/ && $act eq 'kill');
                $row->[$index + 1] = $name2id{ $val } || '';
            }
            my $line = join("\t", @{$row}) . "\n";
            next if ($redun{$line});
            $redun{$line} = 1;
            print OUT $line;
        }
        close OUT;

        system("mv $ready/$file $iddir/$file");
        foreach my $tokill ( @{$output->{$tab}{files}} ) {
            unlink($tokill);
        }
        # Keep footprint small - clear the cache after each pass:
        $mt->clear_cache('seqnames', 'edges','taxa');
        delete $output->{$tab};
        my $known = $#tokens - $novel + 1;
        my $rnum  = $#{$rows} + 1;
        my @bits;
        push @bits, "$novel novel" if ($novel);
        push @bits, "$known known" if ($known);
        my $what = ($#bits == -1) ? 
            "No name operations??" : join(' + ', @bits)." names";
        $self->benchend;
        $self->_log("Table $tab - $what",
                    $rnum, $self->lastbench('full'));
        $done += $rnum;
    }
    return $done;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub each_locked_task {
    my $self   = shift;
    my $dbi    = $self->tracker->dbi;
    my $dir    = $self->{READYDIR};
    my $sth    = $dbi->named_sth("Find all locked tasks");
    my $rows   = $sth->selectall_arrayref( $dir );
    my %hash;
    map { $hash{$_->[0]}{$_->[1]} = $_->[2] } @{$rows};
    return wantarray ? (sort keys %hash ) : \%hash;
}

sub get_host {
    my $hname  = $ENV{'HOST'} || $ENV{'PGHOST'} || '';
    if ($hname =~ /^(\S+?)\..+\.com/) {
        $hname = lc($1);
    }
    return $hname;
}

sub check_task {
    my $self   = shift;
    $self->benchstart;
    my ($task, $dir) = @_;
    $dir     ||= $self->{READYDIR};
    my $mt     = $self->tracker;
    my $dbi    = $mt->dbi;
    my $sth    = $dbi->named_sth("Find currently locked tasks");
    my $hname  = &get_host();
    my $rows   = $sth->selectall_arrayref( $task, $dir );
    $rows->[0] ||= [];
    my ($host, $pid) = @{$rows->[0]};
    $host ||= '';
    $pid  ||= 0;
    my $retval = 0;
    if ($pid) {
        # There is already a lock on this task
        if ($host eq $hname) {
            # The lock is on the machine we are using
            if ($pid == $$) {
                # This is our own lock! We already have the lock for this task
                # Update the time to help a human know it is still active
                my $updSTH = $dbi->named_sth("Update task lock time");
                $updSTH->execute( $dir, $task );
                $retval = 1;
            } else {
                # Verify that the process is still active
                my $pdir = "/proc/$pid";
                unless (-d $pdir) {
                    # The process died without deleting the entry!
                    # Make sure it is really dead
                    system("kill -9 $pid");
                    # Delete the entry from load_status
                    my $lockSTH = $dbi->named_sth("Unlock a task");
                    $lockSTH->execute( $dir, $hname, $pid, $task );
                    $pid = 0;
                }
            }
        }
    }
    $self->benchend;
    return $pid;
}

sub lock_task {
    my $self   = shift;
    my ($task) = @_;
    if ($self->{LOCKED}{$task}) {
        warn "Re-locking task $task ?!\n  ";
        return 1 ;
    }
    my $mt     = $self->tracker;
    return -1  unless ($mt);
    my $dbi    = $mt->dbi;
    return -1  unless ($dbi);
    $self->benchstart;
    my $dir    = $self->{READYDIR};
    my $hname  = &get_host();
    $dbi->begin_work;
    $dbi->do("LOCK TABLE load_status IN EXCLUSIVE MODE");
    my $pid    = $self->check_task( $task, $dir );
    unless ($pid) {
        # We can lock this task for ourselves
        my $lockSTH = $dbi->named_sth("Lock a task");
        $lockSTH->execute( $dir, $task, $hname, $$);
        $self->{LOCKED}{$task} = 1;
    }
    $dbi->commit;
    $self->benchend;
    return $pid ? 0 : 1;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub unlock_task {
    my $self   = shift;
    my ($task) = @_;
    if ($task && !$self->{LOCKED}{$task}) {
        warn "No need to unlock task $task ?!\n  ";
        return;
    } elsif (!$task) {
        my @pending = keys %{$self->{LOCKED}};
        return if ($#pending == -1);
    }
    return unless ($self->tracker);
    my $dbi = $self->tracker->dbi;
    return unless ($dbi);

    $self->benchstart;
    my $hname  = &get_host();
    if ($task) {
        my $lockSTH = $dbi->named_sth("Unlock a task");
        $lockSTH->execute( $self->{READYDIR}, $hname, $$, $task );
        delete $self->{LOCKED}{$task};
    } else {
        my $lockSTH = $dbi->named_sth("Unlock all tasks");
        $lockSTH->execute( $self->{READYDIR}, $hname, $$ );
        $self->{LOCKED} = {};
    }
    $self->benchend;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub _process_map_kill {
    my $self = shift;
    $self->benchstart;
    my $ti = Time::HiRes::time;
    my ($data, $coldat) = @_;
    my $mt = $self->tracker;
    my $dbi = $mt->dbi;
    my (%grouped, @twoname, @onename);
    foreach my $row (@{$data}) {
        my ($name1, $name2, $aid, $sdbid) = @{$row};
        if ($sdbid) {
            my $key = "db_id\t$sdbid";
            $key .= "\t$aid" if ($aid);
            $grouped{$key} ||= [];
            push @{$grouped{$key}}, $name1;
        } elsif ($name1 && $name2) {
            push @twoname, [$name1, $name2, $aid];
        } else {
            my $name = $name1 || $name2;
            unless ($name) {
                $self->error
                    ("Attempt to kill mapping without a name specified");
                next;
            }
            if ($aid) {
                push @{$grouped{"authority_id\t$aid"}}, $name;
            } elsif ($name =~ /^\d+/) {
                push @onename, $name;
            } else {

                # DOES SOMETHING NEED TO BE DONE HERE???

            }
        }
    }
    my %mids;
    my $blocksize = $self->{BLOCKSIZE};
    my $requested = 0;
    my $base = "SELECT map_id FROM mapping WHERE";
    foreach my $dat (@twoname) {
        my ($n1, $n2, $aid) = @{$dat};
        my $w1 = $self->_terms_to_where_clause([[$n1, $n2, $aid]], $coldat);
        my $c1 = $self->_where_clause_to_sql( $w1 );
        my $w2 = $self->_terms_to_where_clause([[$n2, $n1,$aid]], $coldat);
        my $c2 = $self->_where_clause_to_sql( $w2 );
        if ($#{$c1} != 0 || $#{$c2} != 0) {
            $self->death
                ("Attempt to delete maps using $n1 + $n2 fails due to ".
                 "multiple where clauses",
                 "[ ".join(' &&& ', @{$c1})." ]",
                 "[ ".join(' &&& ', @{$c2})." ]" );
        }
        my ($t1, $t2) = ($c1->[0], $c2->[0]);
        my $sql  = "$base $t1 UNION $base $t2";

        my $rows = $dbi->selectall_arrayref
            (-sql   => $sql,
             -name  => "Find Old Mappings by names",
             -level => 1 );

        map { $mids{$_->[0]} = 1 } @{$rows};
        $requested += $#{$rows} + 1;
    }

    @onename = sort { $a <=> $b } @onename;
    $requested += $#onename + 1;
    while ($#onename > -1) {
        my @set  = splice(@onename, 0, $blocksize);
        my $sids = join(',', @set);
        my $sql = "$base name1 IN ($sids) UNION $base name2 IN ($sids)";

        my $rows = $dbi->selectall_arrayref
            (-sql   => $sql,
             -name  => "Find Old Mappings by name_id",
             -level => 1 );
        map { $mids{$_->[0]} = 1 } @{$rows};
    }

    while (my ($key, $sidlist) = each %grouped) {
        my ($groupcol, $colid, $aid) = split(/\t/, $key);
        my $mod = $aid ? " AND authority_id = $aid" : "";
        my $gbase = "$base $groupcol = $colid AND ";
        my @sids = @{$sidlist};
        $requested += $#sids + 1;
        while ($#sids > -1) {
            # Work in groups of 100 entries at a time
            my @set  = splice(@sids, 0, $blocksize);
            my $text = join(",", @set);
            my @sqlbits;
            foreach my $sid (@set) {
                foreach my $col ('name1', 'name2') {
                    push @sqlbits, sprintf
                        ("%s %s = %d%s", $gbase, $col, $sid, $mod);
                }
            }
            my $sql  = join(' UNION ', @sqlbits);
            my $rows = $dbi->selectall_arrayref
                (-sql   => $sql,
                 -name  => "Find Old Mappings by $groupcol",
                 -level => 1 );
            map { $mids{$_->[0]} = 1 } @{$rows};
        }
    }
    my @found_mids = keys %mids;
    if ($#found_mids < 0) {
        $self->benchend;
        $self->_log("Table mapping - deleted none",
                    $requested, $self->lastbench('full'));
        return;
    }
    $mt->_delete_map( @found_mids );
    $self->benchend;
    $self->_log("Table mapping - delete map_ids",
                $#found_mids + 1, $self->lastbench('full'));
    return;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

my $case_codes = {
    '?'  =>  1, # This single, specific case
    '!?' => -1, # Any case but this one
    '?!' => -1, # Any case but this one
    '??' =>  0, # Any case at all
};

sub strip_tokens {
    my $self   = shift;
    my ($text) = @_;
    return () unless ($text);
    my ($honor, $nsid) = (undef, undef);
    while (1) {
        if ($text =~ /^([\!\?]{1,2})(.+)/) {
            # Case-sensitive flags
            # ?foo  => Ignore case
            # !?foo => get all cases except foo
            $text  = $2;
            $honor = $case_codes->{ $1 };
            next;
        }
        if ($text =~ /^\#([^\#]{1,20})\#(.+)/) {
            # Namespace token
            # #GENESYMBOLS#
            # #NONE#
            $text  = $2;
            my $hon;
            ($nsid, $hon) = $self->get_space_id($1);
            $honor = $hon unless (defined $honor);
            next;
        }
        last;
    }
    return ($text, $nsid, $honor);
}

sub _generic_sql {
    my $self = shift;
    my ($vals, $cols) = @_;
    my @wherebits;
    # $debug->branch([$vals, $cols]);
    for my $i (0..$#{$cols}) {
        my $val = $vals->[$i];
        return "" unless (defined $val && $val =~ /^\d+/);
        push @wherebits, $cols->[$i] . ' = ' . $val;
    }
    return join(" AND ", @wherebits);
}

sub _name_to_where {
    my $self = shift;
    my ($val) = @_;
    if ($val  =~ /^MTID:(\d+)$/i) {
        return "= $1";
    }
    my ($clean, $nsid, $hc) = $self->strip_tokens($val);
    $clean =~ s/\'/\'\'/g;
    my $sql = "IN (SELECT name_id FROM seqname WHERE upper(seqname) LIKE '". 
        uc($clean) ."'";
    $sql .= " AND space_id = $nsid" if ($nsid);
    $sql .= sprintf(" AND seqname %s '%s'", $hc < 0 ? 'NOT LIKE' : 'LIKE',
                    $clean) if ($hc);
    $sql .= ")";
    return $sql;
}

sub dump_rebuild {
    my $self = shift;
    my $args = $self->parseparams( -verbose => 1,
                                   @_ );
    my $tab  = $args->{TABLE};
    $self->death( "no -table specified for dump_rebuild()") unless ($tab);
    my $niceTab = $tab = lc($tab);
    substr($niceTab, 0, 1) = uc(substr($niceTab, 0, 1));

    my $bar     = ('#' x 70)."\n";
    my $dmpTab  = "dump_$tab";
    my $oldTab  = "old_$tab";
    my $verbose = $args->{VERBOSE};
    my $mt      = $self->tracker;
    my $dbi     = $mt->dbi;
    my $prfx    = "MapTracker$niceTab";
    my $sfx     = "$$.txt";
    my $joiner  = '_';
    my $schema  = join($joiner, $prfx, 'Schema', $sfx);
    my $dumped  = join($joiner, $prfx, 'Dump', $sfx);
    my $idx     = sprintf("Index_%d_%d_", $$, time);
    my $idxCnt  = 0;
    my $errBase = "dump_rebuild( -table => '$tab' )";
    my $lockSTH = $dbi->prepare("LOCK TABLE $tab IN EXCLUSIVE MODE");
    my $safety  = $dbi->prepare("SELECT relid FROM pg_stat_user_tables".
                                " WHERE upper(relname) = upper(?)");

    my $foundSrc = $safety->get_single_value($tab);
    my $foundDmp = $safety->get_single_value("$dmpTab");
    my $foundOld = $safety->get_single_value("$oldTab");

    if ($args->{ROLLBACK}) {
        if ($foundSrc && $foundOld) {
            $self->benchstart('Rollback');
            $dbi->begin_work;
            $lockSTH->execute();
            $dbi->do("ALTER TABLE $tab RENAME TO $dmpTab");
            $dbi->do("ALTER TABLE $oldTab RENAME TO $tab");
            $dbi->commit;
            $self->benchend('Rollback');
            warn "Rollback of $oldTab to $tab succesfull\n" if ($verbose);
        } else {
            my $what = "Neither $tab nor $oldTab exists.";
            if ($foundSrc) {
                $what = "Current table $tab exists, but $oldTab does not.";
            } elsif ($foundOld) {
                $what = "Current table $tab is missing! Safety table $oldTab is present, however.\n";
            }
            warn "$errBase Failed. $what\n";
        }
        return;
    }


    unless ($foundSrc) {
        warn "$errBase Failed. Source table $tab not found.\n";
        return;
    }
    if ($args->{CLEARDUMP} && $foundDmp) {
        $self->benchstart("Drop dump");
        $dbi->do("DROP TABLE $dmpTab");
        $self->benchend("Drop dump");
        warn "Removed old dump $dmpTab\n" if ($verbose);
        $foundDmp = 0;
    }
    if ($foundDmp) {
        warn "$errBase Failed. Dumped data $dmpTab already exists, use -cleardump to remove\n";
        return;
    }
    if ($foundOld) {
        warn "$errBase Failed. Safety file exists. If you trust the safety:\n".
            "  DROP TABLE $oldTab;\n".
            "... otherwise:\n".
            "  DROP TABLE $tab;\n  ALTER TABLE $oldTab RENAME TO $tab;\n";
        return;
    }

    $dbi->begin_work;
    $lockSTH->execute();

    # Dump the table to file:
    $self->benchstart("Dump table");
    my $scmd = sprintf("pg_dump%s -t %s maptracker > %s",
                       $verbose ? ' -v' : '', $tab, $schema);
    warn "\n$bar$scmd\n$bar\n" if ($verbose);
    system($scmd);
    $self->benchend("Dump table");

    # Make sure commands operate on a new name for the table
    $self->benchstart("Prepare temp table");
    open (SCHEMA, "<$schema") || $self->death
        ("Failed to read schema file", $schema, $!);
    open (DUMP, ">$dumped") || $self->death
        ("Failed to write table loader", $dumped, $!);
    
    my $inDataBlock = 0;
    while (<SCHEMA>) {
        if ($inDataBlock) {
            if (/^\\\./) {
                $inDataBlock = 0;
            }
        } else {
            my ($orig, $rep);
            if (/^(CREATE TABLE) (\Q$tab\E)/i) {
                my ($pre, $t) = ($1, $2);
                $orig = "$pre $t";
                $rep  = "$pre $dmpTab";
                $inDataBlock = 1 if (/^COPY/);
            } elsif (/^(COPY) (\Q$tab\E)/i) {
                my ($pre, $t) = ($1, $2);
                $orig = "$pre $t";
                $rep  = "$pre $dmpTab";
                $inDataBlock = 1 if (/^COPY/);
            } elsif (/(public\.)(\Q$tab\E) (OWNER TO)/) {
                my ($pre, $t, $pro) = ($1, $2, $3);
                $orig = "$pre$t $pro";
                $rep  = "$pre$dmpTab $pro";
            } elsif (/^\s*(ALTER TABLE ONLY|ADD CONSTRAINT) (\S+)/) {
                my ($pre, $t) = ($1, $2);
                $orig = "$pre $t";
                my $mod = ($pre =~ /CONSTRAINT/i) ?
                    $idx.(++$idxCnt) : "dump_$t";
                $rep  = "$pre $mod";
            } elsif (/(CREATE INDEX|CREATE UNIQUE INDEX) (\S+) (ON) (\S+)/) {
                # To assure a unique name we will let the system generate one
                my ($pre, $t, $pro, $c) = ($1, $2, $3, $4);
                $orig = "$pre $t $pro $c";
                $rep  = "$pre $idx".(++$idxCnt)." $pro dump_$c";
                print DUMP "SELECT now();\n\n";
            } elsif (/^\s*(CREATE|ALTER|COPY)/) {
                $self->death("Failed to recognize SQL command", $_);
            }
            s/\Q$orig\E/$rep/ if ($orig);
        }
        print DUMP $_;
    }
    close SCHEMA;
    unlink( $schema );
    close DUMP;

    $self->benchend("Prepare temp table");

    $self->benchstart("Load temp table");
    my $rcmd = sprintf("psql%s -t maptracker < %s",
                       $verbose ? ' -e' : '', $dumped);
    warn "\n$bar$rcmd\n$bar\n" if ($verbose);
    system($rcmd);
    $self->benchend("Load temp table");
    if ($args->{SAVEDUMP}) {
        warn "Dump file: $dumped\n" if ($verbose);
    } else {
        unlink( $dumped );
    }

    $self->benchstart("Rename and analyze");
    $dbi->do("ANALYZE $dmpTab");
    unless ($args->{NOSWAP}) {
        $dbi->do("ALTER TABLE $tab RENAME TO $oldTab");
        $dbi->do("ALTER TABLE $dmpTab RENAME TO $tab");
    }
    $self->benchend("Rename and analyze");
    $dbi->commit;



    return;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub set_relation {
    my $self = shift;
    unless ($self->{GRIPES}{set_relation}) {
        $self->carp( 'set_relation', ["Deprecated method call"],
                     [ 'Use Instead', 'set_edge()' ]);
        my $crusty = ("-" x 70) . "\n".
            "HEY! You should not be using set_relation!\n".
            "Please look at documentation for set_edge()\n".
            "Your calls to set_relation are being redirected to set_edge.\n".
            ("-" x 70) . "\n";
        warn $crusty;
        $self->{GRIPES}{set_relation} = $crusty;
    }
    return $self->set_edge( -name1 => $_[0],
                            -name2 => $_[1],
                            -type  => $_[2],
                            -auth  => $_[3] );
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub kill_relation {
    my $self = shift;
    unless ($self->{GRIPES}{kill_relation}) {
        $self->carp( 'kill_relation', ["Deprecated method call"],
                     [ 'Use Instead', 'kill_edge()' ]);
        my $crusty = ("-" x 70) . "\n".
            "HEY! You should not be using kill_relation!\n".
            "Please look at documentation for kill_edge()\n".
            "Your calls to kill_relation are being redirected to kill_edge.\n".
            ("-" x 70) . "\n";
        warn $crusty;
        $self->{GRIPES}{kill_relation} = $crusty;
    }
    return $self->kill_edge( -name1    => $_[0],
                             -name2    => $_[1],
                             -type     => $_[2],
                             -auth     => $_[3],
                             -override => $_[4] );
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

1;
