# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::Namespace;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

BEGIN {
}

$BMS::MapTracker::Namespace::VERSION = 
    ' $Id$ ';

use strict;
use BMS::Branch;
use BMS::CommonCAT;
use Scalar::Util qw(weaken);

my $debug = BMS::Branch->new
    ( -skipkey => ['TRACKER', 'CHILDREN', 'CLASSES', 'FP', 'BENCHMARKS',
		   'SEQNAMES', 'AUTHORITIES','TYPES',],
      -maxarray => 40,
      -maxhash  => 40,
      -format => 'text', -noredundancy => 1, );

=head1 PRIMARY METHODS
#-#-#-#-#--#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-

=head2 new

 Title   : new
 Usage   : my $obj = BMS::MapTracker::Namespace->new(@arguments)
 Function: Creates a new object and returns a blessed reference to it.
 Returns : A blessed BMS::MapTracker::Namespace object
 Args    : Associative array of arguments. Recognized keys [Default]:

     -name A human readable name for the transform

       -id The database ID for this transform

 -sensitive Case sensitivity flag

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = { 
    };
    bless ($self, $class);
    my $args = $self->parseparams( -id    => 0,
				   -name  => undef,
				   @_ );
    $self->{ID}    = $args->{ID};
    $self->{NAME}  = $args->{NAME};
    $self->{DESC}  = $args->{DESC};
    $self->{IS_CS} = (!$args->{SENSITIVE} || $args->{SENSITIVE} eq 'f') ? 0:1;
    $self->tracker( $args->{TRACKER} );
    return $self;
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
    if ($_[0]) {
        # Prevent circular references by weakening link to tracker object
	weaken($self->{TRACKER} = $_[0]);
    }
    return $self->{TRACKER} || BMS::MapTracker->new();
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 id

 Title   : id
 Usage   : my $id = $obj->id
 Function: Gets the database ID
 Returns : The database ID
 Args    : 

=cut

sub id {
    my $self = shift;
    return $self->{ID};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 desc

 Title   : desc
 Usage   : my $description = $obj->desc
 Function: Gets the description for the namespace
 Returns : The description
 Args    : 

=cut

*description = \&desc;
sub desc {
    my $self = shift;
    return $self->{DESC};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 name

 Title   : name
 Usage   : my $name = $obj->name
 Function: Gets the name of the sequence
 Returns : The name
 Args    : 

=cut

sub name {
    my $self = shift;
    return $self->{NAME};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 case_sensitive

 Title   : case_sensitive
 Usage   : my $is_case_sensitive = $obj->case_sensitive
 Function: Gets the case sensitivity flag
 Returns : 0 if the name space is not case sensitive, 1 if it is
 Args    : 

=cut

*sensitive = \&case_sensitive;
*is_sensitive = \&case_sensitive;
sub case_sensitive {
    my $self = shift;
    return $self->{IS_CS};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 children

 Title   : children
 Usage   : my @child_spaces = $obj->children
 Function: Gets all child Namespace objects, if any
 Returns : An array of Namespace objects
 Args    : 

=cut

sub children {
    my $self = shift;
    # warn $self->{TRACKER}$debug->branch($self);
    unless ($self->{CHILDREN}) {
        # We have not read in the children yet
        $self->{CHILDREN} = {};
        my $id   = $self->id;
        my $mt   = $self->tracker;
        my @kids = $mt->dbi->named_sth("Get child namespaces")->
            get_array_for_field( $id );
        foreach my $kid (@kids) {
            # Skip if we have already encountered this child
            next if ($self->{CHILDREN}{$kid});
            my $child = $mt->get_space( $kid );
            $self->{CHILDREN}{$kid} = $child;
            # Recursively get all children, to arbitrary depth:
            foreach my $gkid ($child->children) {
                $self->{CHILDREN}{$gkid->id} = $gkid;
            }
        }
    }
    return sort { $a->id <=> $b->id } values %{$self->{CHILDREN}};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 all_ids

 Title   : all_ids
 Usage   : my @db_ids = $obj->all_ids
 Function: Gets a list of all database IDs appropriate to this
           space. The list is composed of the ID for this namespace,
           and the ID for all children.
 Returns : An array of integers
 Args    : 

=cut

sub all_ids {
    my $self = shift;
    unless ($self->{ALL_IDS}) {
        # First time called, need to calculate
        my @ids = map { $_->id } $self->children;
        push @ids, $self->id;
        $self->{ALL_IDS} = [ sort { $a <=> $b } @ids ];
    }
    return @{$self->{ALL_IDS}};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 all_ids_sql

 Title   : all_ids_sql
 Usage   : my @db_ids = $obj->all_ids_sql
 Function: Convienence call - designed to be plugged into a SQL string
 Returns : A string such as "(4,8)"
 Args    : 

=cut

sub all_ids_sql {
    my $self = shift;
    unless ($self->{ALL_IDS_SQL}) {
        # First time called, need to calculate
        $self->{ALL_IDS_SQL} = '(' .join(',', $self->all_ids) . ')';
    }
    return $self->{ALL_IDS_SQL};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_text

 Title   : to_text
 Usage   : print $obj->to_text
 Function: Show the contents of the object as a text string
 Returns : A string
 Args    : 

=cut

sub to_text {
    my $self = shift;
    unless ($self->{TO_TEXT}) {
        # First time called, need to calculate
        $self->{TO_TEXT} = sprintf
            ("%s [%d] %s%s\n", $self->name, $self->id, $self->case_sensitive ?
             " (case sensitive)" : "", $self->desc);
    }
    return $self->{TO_TEXT};
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
        my $txt  = sprintf("type:'ns', id:%d, name:'%s', desc:'%s', cs:%d",
                           $self->id, $name, $desc, $self->case_sensitive);
        $self->{JSTEXT} = "{ $txt }";   
    }
    return $self->{JSTEXT};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
1;
