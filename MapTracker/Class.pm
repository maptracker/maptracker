# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::Class;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

BEGIN {
}

$BMS::MapTracker::Class::VERSION = 
    ' $Id$ ';

use strict;
use vars qw(@ISA);
use Scalar::Util qw(weaken);

use BMS::MapTracker::Shared;
use BMS::Branch;

@ISA = qw(BMS::MapTracker::Shared);

my $debug = BMS::Branch->new( -skipkey => { TRACKER => 1,
					    CHILDREN => 1,
					    EVERY_CHILD => 1,},
			      -format  => 'html', );

=head1 PRIMARY METHODS
#-#-#-#-#--#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-

=head2 new

 Title   : new
 Usage   : my $obj = BMS::MapTracker::Class->new(@arguments)
 Function: Creates a new object and returns a blessed reference to it.
 Returns : A blessed BMS::MapTracker::Class object
 Args    : Associative array of arguments. Recognized keys [Default]:

    -token A short, unique token for the class

     -name A human readable name for the class

       -id The database ID for this class

     -desc A description of the class

   -parent A database *ID* for the parent class

 -children An array reference of children class *objects*.

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
	CHILDREN => [],
    };
    bless ($self, $class);
    my $args = $self->parseparams( -name      => undef,
				   -token     => undef,
				   -id        => undef,
				   -parent    => 0,
				   -children  => [],
				   -desc      => "",
				   -tracker   => undef,
				   @_ );
    $self->desc( $args->{DESC} );
    $self->name( $args->{NAME} );
    $self->token( $args->{TOKEN} );
    $self->id( $args->{ID} );
    weaken($self->{TRACKER} = $args->{TRACKER});
    $self->parent( $args->{PARENT} );
    foreach my $child ( @{$args->{CHILDREN}} ) {
	$self->add_child( $child );
    }
    return $self;
}

sub DESTROY {
    my $self = shift;
    $self->{EVERY_CHILD} = undef;
    $self->{TRACKER} = undef;
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
    if (defined $id) {
	$self->death("Class ID '$id' is not an integer")
	    unless ($id =~ /^\d+$/);
	$self->{ID} = $id;
    }
    return $self->{ID};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 name

 Title   : name
 Usage   : $obj->name($name)
 Function: Gets / Sets the name of the class
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

=head2 token

 Title   : token
 Usage   : $obj->token($token)
 Function: Gets / Sets the token of the class
 Returns : The token
 Args    : Optional token

=cut


sub token {
    my $self = shift;
    my ($token) = @_;
    if ($token) {
	$self->{TOKEN} = substr($token, 0,20);
    }
    return $self->{TOKEN};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 desc

 Title   : desc
 Usage   : $obj->desc($id)
 Function: Gets / Sets the description
 Returns : A string for the description
 Args    : Optional description

=cut


sub desc {
    my $self = shift;
    my ($desc) = @_;
    if ($desc) {
	$self->{DESC} = substr($desc, 0,255);
    }
    return $self->{DESC};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 tracker

 Title   : tracker
 Usage   : $obj->tracker($name)
 Function: Gets / Sets the MapTracker object
 Returns : The MapTracker object assigned to this object
 Args    : Optional MapTracker object

=cut


sub tracker {
    my $self = shift;
    return $self->{TRACKER};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 parent

 Title   : parent
 Usage   : $obj->parent($id)
 Function: Gets / Sets the ID of the parent class
 Returns : The database ID
 Args    : Optional integer

=cut


sub parent {
    my $self = shift;
    my ($id) = @_;
    if ($id) {
	$self->death("Parent class ID '$id' is not an integer")
	    unless ($id =~ /^\d+$/);
	$self->{PARENT} = $id;
    }
    return $self->{PARENT};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 add_child

 Title   : add_child 
 Usage   : $obj->add_child($classObject)
 Function: Designate another class as a child of this one
 Returns : 
 Args    : 

=cut


sub add_child {
    my $self = shift;
    my ($class) = @_;
    if ($class) {
	$self->death("'$class' is not a Class object")
	    unless ($class->isa('BMS::MapTracker::Class'));
        # Make sure the child has not already been added:
        for my $i (0..$#{$self->{CHILDREN}}) {
            my $existing = $self->{CHILDREN}[$i];
            return $self->{CHILDREN} if ($existing->id == $class->id);
        }
	push @{$self->{CHILDREN}}, $class;
    }
    return $self->{CHILDREN};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 has_child

 Title   : has_child 
 Usage   : $obj->has_child($classObject)

 Function: Returns 1 if the class is the same (based on db ID) as the
           one provided, or if any of the children (and great
           children) are the same. Useful for determining if a class
           'isa' a parent class.

           'rna'  ->has_child( 'mrna' )  ... true
           'mrna' ->has_child( 'rna' )   ... false

 Returns : 0 or 1
 Args    : One or more BMS::MapTracker::Class objects

=cut


sub has_child {
    my $self = shift;
    my $mt   = $self->tracker;
    foreach my $cReq (@_) {
        my $class = $mt ? $mt->get_class($cReq) : $cReq;
        return 0 unless ($class);
        $self->death("'$class' is not a Class object")
            unless (ref($class) && $class->isa('BMS::MapTracker::Class'));
        my $id = $class->id;
        return 1 if ($self->id == $id);
        foreach my $kid ($self->every_child) {
            return 1 if ($kid->id == $id);
        }
    }
    return 0;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 has_parent

 Title   : has_parent 
 Usage   : $obj->has_parent($classObject)

 Function: As for has_child( ), but tests for parents
 Returns : 0 or 1
 Args    : A BMS::MapTracker::Class object

=cut

*is_class = \&has_parent;
sub has_parent {
    my $self = shift;
    my $mt   = $self->tracker;
    foreach my $cReq (@_) {
        my $class = $mt ? $mt->get_class($cReq) : $cReq;
        return 0 unless ($class);
        $self->death("'$class' is not a Class object")
            unless (ref($class) && $class->isa('BMS::MapTracker::Class'));
        return 1 if ($class->has_child($self));
    }
    return 0;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 each_child

 Title   : each_child
 Usage   : $obj->each_child
 Function: Retrieve all immediate child classes
 Returns : An array of class objects
 Args    : 

=cut
   

sub each_child {
    my $self = shift;
    return @{$self->{CHILDREN}};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 every_child

 Title   : every_child
 Usage   : $obj->every_child

 Function: Retrieve all immediate child *and* grandchild classes

 Returns : An array of class objects
 Args    : 

=cut
   

sub every_child {
    my $self = shift;
    unless ($self->{EVERY_CHILD}) {
	# Only do the recursion once:
        my @all_kids = ();
        foreach my $kid (@{$self->{CHILDREN}}) {
            push @all_kids, $kid;
            push @all_kids, $kid->every_child;
        }
        $self->{EVERY_CHILD} = \@all_kids;
    }
    return @{$self->{EVERY_CHILD}};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 every_parent

 Title   : every_parent
 Usage   : $obj->every_parent
 Function: Retrieve all parents and grandparents
 Returns : An array of class objects
 Args    : 

=cut

sub every_parent {
    my $self = shift;
    unless ($self->{EVERY_PARENT}) {
	# Only do the recursion once:
        my $loop = $self;
        my @parents;
        while ($loop = $loop->parent) {
            $loop = $self->tracker->get_class($loop);
            push @parents, $loop if ($loop);
        }
        $self->{EVERY_PARENT} = \@parents;
    }
    return @{$self->{EVERY_PARENT}};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 specificity

 Title   : specificity
 Usage   : $obj->specificity

 Function: Retrieve a count of how far away the class is from 'Unknown'

 Returns : An integer
 Args    : 

=cut

sub specificity {
    my $self = shift;
    unless (defined $self->{SPECIFICITY}) {
        my @list = $self->every_parent;
        $self->{SPECIFICITY} = $#list + 1;
    }
    return $self->{SPECIFICITY};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 me_and_the_kids

 Title   : me_and_the_kids
 Usage   : my @children = $obj->me_and_the_kids

 Function: Retrieve all immediate child *and* grandchild classes *and*
           this class itself. Trust me, it has uses.

 Returns : An array of class objects, starting with this class
 Args    : 

=cut
   

sub me_and_the_kids {
    my $self = shift;
    return ($self, $self->every_child);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 me_and_the_folks

 Title   : me_and_the_folks
 Usage   : $obj->me_and_the_folks

 Function: Retrieve all parent *and* grandparent classes *and*
           this class itself. Trust me, it has uses.

 Returns : An array of class objects
 Args    : 

=cut
   

sub me_and_the_folks {
    my $self = shift;
    return ($self, $self->every_parent);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_text

 Title   : to_text
 Usage   : $obj->to_text($pad)
 Function: Generates an ASCII string describing the class
 Returns : The string
 Args    : Optional pad string (for left offset)

=cut

sub to_text {
    my $self = shift;
    my ($pad) = @_;
    $pad ||= "";
    #$debug->branch($self);
    my $string = sprintf("%s[ID %d %s] %s : %s\n", $pad, $self->id, 
			 $self->token, $self->name, $self->desc || "");
    return $string;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 javascript_link

 Title   : javascript_link
 Usage   : my $html_anchor = $obj->javascript_link( $classes )
 Function: Generates an <A> string for use with maptracker javascript
 Returns : A string
    Args : Optional HTML classes

=cut

sub javascript_link {
    my $self = shift;
    my ($userClass) = @_;
    return $self->SUPER::javascript_link
        ($userClass, 'class', $self->id, $self->name);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_javascript

 Title   : to_javascript
 Usage   : $obj->to_javascript()
 Function: Generates an ASCII string encoding a JavaScript object
 Returns : The string

 Args : If a true value is passed to the method, then taxa, classes
        and lengths will be read in. Otherwise only data already
        recovered for the sequence will be included.

=cut

sub to_javascript {
    my $self = shift;
    my ($force) = @_;
    if (!$self->{JSTEXT} || $force) {
        my $name = $self->name;
        $name =~ s/\//\/\//g;
        $name =~ s/\'/\\\'/g;
        my $desc = $self->desc;
        $desc =~ s/\//\/\//g;
        $desc =~ s/\'/\\\'/g;
        my $txt  = sprintf
            ("type:'class', id:%d, name:'%s', token:'%s', desc:'%s'",
             $self->id, $name, $self->token, $desc);
        $txt .= ", parent:" . ($self->parent || 0);
        $txt .= ", children:[ ".join(',', map {$_->id} $self->each_child)." ]";
        $self->{JSTEXT} = "{ $txt }";   
    }
    return $self->{JSTEXT};    
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 full_text

 Title   : full_text
 Usage   : $obj->full_text($pad)
 Function: Generates an ASCII string describing the class and all kids
 Returns : The string
 Args    : Optional pad string (for left offset)

=cut


sub full_text {
    my $self = shift;
    my ($pad) = @_;
    $pad ||= "";
    my $string = $self->to_text($pad);
    foreach my $kid ($self->each_child) {
	$string .= $kid->full_text("$pad  ");
    }
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
