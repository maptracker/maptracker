# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::Transform;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

BEGIN {
}

$BMS::MapTracker::Transform::VERSION = 
    ' $Id$ ';

use strict;
use BMS::Branch;
use BMS::CommonCAT;

=head1 PRIMARY METHODS
#-#-#-#-#--#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-

=head2 new

 Title   : new
 Usage   : my $obj = BMS::MapTracker::Transform->new(@arguments)
 Function: Creates a new object and returns a blessed reference to it.
 Returns : A blessed BMS::MapTracker::Transform object
 Args    : Associative array of arguments. Recognized keys [Default]:

     -name A human readable name for the transform

       -id The database ID for this transform

    -step1 Required. The step interval for the "first" sequence

    -step2 Required. The step interval for the "second" sequence.

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = { 
	B_LOCS => [],
    };
    bless ($self, $class);
    my $args = $self->parseparams( -step1 => undef,
				   -step2 => undef,
				   -id    => 0,
				   -name  => "Unknown transform name",
				   @_ );
    my ($s1, $s2) = ($args->{STEP1}, $args->{STEP2});
    unless ($s1 && $s2) {
	$self->death("You must define both -step1 and -step2");
    }
    $self->step($s1, $s2);
    $self->name( $args->{NAME} );
    $self->id( $args->{ID} );
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
	$self->death("Transform ID '$id' is not an integer")
	    unless ($id =~ /^\d+$/);
	$self->{ID} = $id;
    }
    return $self->{ID};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 name

 Title   : name
 Usage   : $obj->name($name)
 Function: Gets / Sets the name of the transform
 Returns : The name
 Args    : Optional name

=cut


sub name {
    my $self = shift;
    my ($name) = @_;
    if ($name) {
	$self->{NAME} = substr($name, 0,50);
    }
    return $self->{NAME};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 step

 Title   : step
 Usage   : $obj->step($step1, $step2)
 Function: Gets / Sets the step intervals for the sequence pair
 Returns : An array of (step1, step2)
 Args    : Optionally two step values

=cut


sub step {
    my $self = shift;
    my ($step1, $step2) = @_;
    if ($step1) {
	$self->death("You are required to set both step values at once")
	    unless ($step2);
	$self->{STEP1} = $step1;
	$self->{STEP2} = $step2;
    }
    return ($self->{STEP1}, $self->{STEP2});
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
    my $string = sprintf("%sTransform '%s' Step %s vs %s [ID %s]\n", $pad, 
			 $self->name, $self->step,$self->id || 'not assigned');
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
    my $string = sprintf("<font color='blue'><b>Transform '%s'</b></font> ".
			 "[ID %s] Step %s vs %s\n", 
			 $self->name,$self->id || 'not assigned',$self->step,);
    return $string;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 bridge

 Title   : bridge
 Usage   : $obj->bridge($sequence, [$locs])

 Function: If the transform is being used as a bridge, this method
           sets / gets the sequence object used for bridging. You can
           also set an array reference of the locations used to

 Returns : A Seqname object, or undef if none defined
           If requested in array context, also returns a 2D array ref

 Args    : Optional Mapping object

=cut


sub bridge {
    my $self = shift;
    if ($_[0]) {
	$self->death("'$_[0]' is not a Seqname object")
	    unless (ref($_[0]) && $_[0]->isa('BMS::MapTracker::Seqname'));
	$self->{BRIDGE} = $_[0];
	if ($_[1]) {
	    $self->death("The second argument of bridge() must be an array, ".
			 "not '$_[1]'") unless (ref($_[1]) eq 'ARRAY');
	    $self->{B_LOCS} = $_[1];
	}
    }
    return wantarray ? ($self->{BRIDGE}, $self->{B_LOCS}) : $self->{B_LOCS};
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
