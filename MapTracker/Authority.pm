# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::Authority;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

$BMS::MapTracker::Authority::VERSION = 
    ' $Id$ ';

use strict;
use vars qw(@ISA);
use BMS::Branch;
use BMS::MapTracker::Shared;

@ISA = qw(BMS::MapTracker::Shared);

=head1 PRIMARY METHODS
#-#-#-#-#--#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-

=head2 new

 Title   : new
 Usage   : my $obj = BMS::MapTracker::Authority->new(@arguments)
 Function: Creates a new object and returns a blessed reference to it.
 Returns : A blessed BMS::MapTracker::Authority object
 Args    : Associative array of arguments. Recognized keys [Default]:

     -name The name of the person / authority

       -id The database ID for this authority

     -desc A description of the authority

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {  };
    bless ($self, $class);
    my $args = $self->parseparams( -name => undef,
				   -id   => undef,
				   -desc => 0,
				   @_ );
    $self->desc( $args->{DESC} );
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
	$self->death("Authority ID '$id' is not an integer")
	    unless ($id =~ /^\d+$/);
	$self->{ID} = $id;
    }
    return $self->{ID};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 name

 Title   : name
 Usage   : $obj->name($name)
 Function: Gets / Sets the name of the authority
 Returns : The name
 Args    : Optional name

=cut


sub name {
    my $self = shift;
    my ($name) = @_;
    if ($name) {
	$self->{NAME} = substr($name, 0,100);
    }
    return $self->{NAME};
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

=head2 copy

 Title   : copy
 Usage   : my $newobj = $obj->copy
 Function: Generates a new object that has all the same values as the first
 Returns : An Authority object
 Args    : Optional description

=cut


sub copy {
    my $self = shift;
    my $authority = BMS::MapTracker::Authority->new
	( -desc => $self->desc, -id => $self->id, -name => $self->name);
    return $authority;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 write

 Title   : write
 Usage   : $obj->write($trackerObject)
 Function: Writes the user data to the database.
 Returns : 
 Args    : 

=cut


sub write {
    my $self = shift;
    my ($mt) = @_;

    my $dbi = $mt->dbi;
    $dbi->begin_work;
    $dbi->named_sth("Lock authority exclusively")->execute();
    # Make sure we are not duplicating data in the DB
    my ($id, $name, $desc) = $dbi->named_sth("Retrieve authority by name")->
        selectrow_array( $self->name );
    if ($id) {
	# This entry already exists - update object
	$self->id($id);
	$self->name($name);
	$self->desc($desc);
    } else {
	# New entry, make a copy and send it on
	$id = $dbi->nextval('authority_seq');
	($name, $desc) = ($self->name, $self->desc);
        $dbi->named_sth("Create a new authority entry")->
            execute( $id, $self->name, $self->desc );
	$self->id($id);
    }
    $dbi->commit;
    return $self;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_text

 Title   : to_text
 Usage   : $obj->to_text($pad)
 Function: Generates an ASCII string naming the authority
 Returns : The string
 Args    : Optional pad string (for left offset)

=cut

sub to_text {
    my $self = shift;
    my ($pad) = @_;
    $pad ||= "";
    my $string = sprintf("%s[ID %s] %s\n", $pad, $self->id || 'not assigned',
			 $self->name, );
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
    my ($userClass, $args) = @_;
    return $self->SUPER::javascript_link
        ($userClass, 'auth', $self->id, $self->name, $args);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_javascript

 Title   : to_javascript
 Usage   : $obj->to_javascript()
 Function: Generates an ASCII string encoding a JavaScript object
 Returns : The string

    Args :

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
        my $txt  = sprintf("type:'auth', id:%d, name:'%s', desc:'%s'",
                           $self->id, $name, $desc);
        $self->{JSTEXT} = "{ $txt }";   
    }
    return $self->{JSTEXT};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_html

 Title   : to_html
 Usage   : print $obj->to_html($pad)
 Function: Generates an ASCII string naming the authority
 Returns : The string
 Args    : 

=cut

sub to_html {
    my $self = shift;
    my $string = sprintf("<font color='brown'>[%s]</font>", $self->name, );
    return $string;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 full_text

 Title   : full_text
 Usage   : $obj->full_text($pad)
 Function: Generates an ASCII string describing the authority in full
 Returns : The string
 Args    : Optional pad string (for left offset)

=cut

sub full_text {
    my $self = shift;
    my ($pad) = @_;
    $pad ||= "";
    my $string = $self->to_text($pad);
    $string .= "$pad  ". $self->desc . "\n";
    return $string;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 full_html

 Title   : full_html
 Usage   : print $obj->full_html($pad)
 Function: Generates an ASCII string naming the authority
 Returns : The string
 Args    : 

=cut

sub full_html {
    my $self = shift;
    my $string = sprintf("<font color='blue' size='+1'><b>%s</b></font> ".
			 "<font color='brown' size='-1'>[%d]</font><br />\n", 
			 $self->name, $self->id);
    $string .= sprintf("<i>%s</i><br />\n", $self->desc || "no description", );
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
