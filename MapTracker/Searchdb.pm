# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::Searchdb;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

BEGIN {
}

$BMS::MapTracker::Searchdb::VERSION = 
    ' $Id$ ';

my $types = { 'NOSHA'      => 1,
	      'MICROBLAST' => 1,
              'EXTERNAL'   => 1,
	      'FLAT FILE'  => 1,
              'ALGORITHM'  => 1,
	      'UNKNOWN'    => 1, };

use strict;
use BMS::Branch;
use BMS::CommonCAT;

=head1 PRIMARY METHODS
#-#-#-#-#--#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-

=head2 new

 Title   : new
 Usage   : my $obj = BMS::MapTracker::Searchdb->new(@arguments)
 Function: Creates a new object and returns a blessed reference to it.
 Returns : A blessed BMS::MapTracker::Searchdb object
 Args    : Associative array of arguments. Recognized keys [Default]:

     -name A human readable name for the Searchdb

       -id The database ID for this Searchdb

     -path Required. The unix path to the DB file

     -type Required. Short type of database

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {  };
    bless ($self, $class);
    my $args = $self->parseparams( -name  => undef,
				   -type  => 'Not provided',
				   -id    => 0,
				   -path  => 'Not provided',
				   @_ );
    my ($name) = ($args->{NAME});
    unless ($name) {
	$self->death("You must define the name of the Searchdb");
    }
    $self->name( $name );
    $self->id( $args->{ID} );
    $self->type( $args->{TYPE} );
    $self->path( $args->{PATH} );
    return $self;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 id

 Title   : id
 Usage   : $obj->id($id)
 Function: Gets / Sets the database ID
 Returns : The database ID
 Args    : Optional integer

=cut


sub id {
    my $self = shift;
    my ($id) = @_;
    if ($id) {
	$self->death("Searchdb ID '$id' is not an integer")
	    unless ($id =~ /^\d+$/);
	$self->{ID} = $id;
    }
    return $self->{ID};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 type

 Title   : type
 Usage   : $obj->type($type)
 Function: Gets / Sets the type of the Searchdb
 Returns : The type
 Args    : Optional type

=cut


sub type {
    my $self = shift;
    my ($type) = @_;
    if ($type) {
	$type = uc($type);
	unless ($types->{$type}) {
	    $self->death("Searchdb type '$type' is not recognized")
	}
	$self->{TYPE} = $type;
    }
    return $self->{TYPE};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 name

 Title   : name
 Usage   : $obj->name($name)
 Function: Gets / Sets the name of the Searchdb
 Returns : The name
 Args    : Optional name

=cut


sub name {
    my $self = shift;
    my ($name) = @_;
    if ($name) {
	if ($name =~ /^\d+$/) {
	    $self->death("You can not use an integer as a DB name");
	}
	$self->{NAME} = substr($name, 0,100);
    }
    return $self->{NAME};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 path

 Title   : path
 Usage   : $obj->path($path)
 Function: Gets / Sets the path of the Searchdb
 Returns : The path
 Args    : Optional path

=cut


sub path {
    my $self = shift;
    my ($path) = @_;
    if ($path) {
	if ($path !~ /^\// && 
            $self->type ne 'EXTERNAL' && $self->type ne 'ALGORITHM') {
	    $self->death("Searchdb path must start with a '/' (you provided '$path')")
	}
	$self->{PATH} = substr($path, 0,500);
    }
    return $self->{PATH} || "";
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 full_text

 Title   : full_text
 Usage   : print $obj->full_text($pad)
 Function: Generates an ASCII string describing the map in full
 Returns : The string
 Args    : Optional pad string (for left offset)

=cut


sub full_text {
    my $self = shift;
    my ($pad) = @_;
    $pad ||= "";
    my $string = sprintf("%sSearchdb '%s' [ID %s] (%s)\n%s\n", $pad, 
			 $self->name, $self->id || 'not assigned',
			 $self->type, $self->path );
    return $string;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_html

 Title   : to_html
 Usage   : $obj->to_html($pad)
 Function: Generates an ASCII string describing the map in full
 Returns : The string
 Args    : Optional pad string (for left offset)

=cut


sub to_html {
    my $self = shift;
    my $string = sprintf("<font color='blue'><b>Searchdb '%s'</b></font> ".
			 "[ID %s] Step %s vs %s\n", 
			 $self->name,$self->id || 'not assigned',$self->step,);
    return $string;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 

 Title   : 
 Usage   : $obj->($id)
 Function: 
 Returns : 
 Args    : 

=cut


sub f {
    my $self = shift;
    my ($id) = @_;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

1;
