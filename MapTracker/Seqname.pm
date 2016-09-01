 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::Seqname;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use strict;
BEGIN {
    # Squelch irritating BioPerl error:
    # Replacement list is longer than search list 
    # at /apps/sys/perl/lib/site_perl/5.12.0/Bio/Range.pm line 251.
    use BMS::MapTracker::Shared;
    my $foo = BMS::ErrorInterceptor->new();
    $foo->ignore_error('Replacement list is longer than search list');
}

$BMS::MapTracker::Seqname::VERSION = 
    ' $Id$ ';

use vars qw(@ISA);
use BMS::Branch;
use Bio::Seq::RichSeq;
use Bio::SeqFeature::Generic;
use Scalar::Util qw(weaken);

@ISA = qw(BMS::MapTracker::Shared);

my $debug = BMS::Branch->new
    ( -skipkey => ['TR  KER', 'CHILDREN', 'CLASSES', 'FP',
		   'SEQNAMES', 'AUTHORITIES', 'MT_META', '_MAP_OBJECTS_'],
      -format => 'html', -noredundancy => 1, );

my $mtpl = 'mapTracker.pl';
my $mbpl = 'http://bioinformatics.bms.com/biocgi/mapBrowser.pl';

=head1 PRIMARY METHODS
#-#-#-#-#--#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-

=head2 new

 Title   : new
 Usage   : my $obj = BMS::MapTracker::Seqname->new(@arguments)
 Function: Creates a new object and returns a blessed reference to it.
 Returns : A blessed BMS::MapTracker::Seqname object
 Args    : Associative array of arguments. Recognized keys [Default]:

       -id Required. The database ID for the entry.

     -name Required. The human name for the sequence

       -mt MapTracker object

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
	CLASSES     => {},
	LENGTHS     => {},
	TAXA        => {},
	SEQNAMES    => {},
	AUTHORITIES => {},
	RELATED     => {},
        EDGES       => {},
	RELATENOTE  => {},
	MAPPINGS    => [],
	TASKS       => {},
    };
    bless ($self, $class);
    my $args = $self->parseparams( -id        => undef,
				   -name      => undef,
				   -mt        => undef,
				   -tracker   => undef,
				   @_ );
    $self->{NAME}  = $args->{NAME};
    $self->{ID}    = $args->{ID};
    $self->{SPACE} = $args->{SPACE} || $args->{NAMESPACE};
    unless ($args->{ID} =~ /^\d+$/) {
        $self->death("Seqname ID '".$args->{ID}."' is not an integer");
    }
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
    unless ($self->{TRACKER}) {
        $self->death("Request for tracker object failed");
    }
    return $self->{TRACKER};
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

sub mtid {
    my $self = shift;
    return "MTID:" . $self->{ID};
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

=head2 namespace_name

 Title   : namespace_name
 Usage   : my $name = $obj->namespace_name
 Function: Gets the name of the sequence with a tag for the namespace
 Returns : The name, with namespace pre-pended
 Args    : 

=cut

sub namespace_name {
    my $self = shift;
    unless ($self->{NS_NAME}) {
        $self->{NS_NAME} = '#' . $self->namespace->name . '#' . $self->name;
    }
    return $self->{NS_NAME};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 namespace

 Title   : namespace
 Usage   : $obj->name($name)
 Function: Gets / Sets the name of the sequence
 Returns : The name
 Args    : Optional name

=cut


sub namespace {
    my $self = shift;
    return $self->{SPACE};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 task

 Title   : task
 Usage   : my $value = $obj->task( $name, $value )
 Function: Sets / gets the value for a task
 Returns : The old value
    Args : Optional new value

=cut

sub task {
    my $self = shift;
    my ($name, $val) = @_;
    my $retval = $self->{TASKS}{$name};
    $self->{TASKS}{$name} = $val if (defined $val);
    return $retval;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 read_lengths

 Title   : read_lengths
 Usage   : $obj->read_lengths
 Function: Reads length associations from a MapTracker DB. A tracker
           object should be provided, either through new() or
           tracker().
 Returns : A hash reference of lengths associated with this name
 Args    : Optional arguments array:

    -force Normally the function will not execute if it has done so
           already - this means you do not need to worry about wasting
           CPU cycles by calling a method that might have been called
           already by another function. If you need to force a re-read
           of the class data, set -force to true.

=cut

sub read_lengths {
    my $self = shift;
    my $args = $self->parseparams( -force  => 0,
				   @_ );
    if (!$self->task('read_lengths') || $args->{FORCE}) {
        # Read from database if not already done, or if user is forcing
        $self->benchstart;
        my $rows = $self->tracker->dbi->
            named_sth("Retrieve length assignments for a seqname")->
            selectall_arrayref( $self->id );
        foreach my $row (@{$rows}) {
            my ($len, $aid) = @{$row};
            $self->add_length($len, $aid);
        }
        $self->task('read_lengths', 1);
        $self->benchend;
    }
    return keys %{$self->{LENGTHS}};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 add_length

 Title   : add_length
 Usage   : $obj->add_length($length, $authority)
 Function: Adds a sequence length by a given authority
 Returns : 
    Args : A length (integer) and an Authority.

=cut

sub add_length {
    my $self = shift;
    my ($len, $authReq) = @_;
    my $mt   = $self->tracker;
    my $auth = $mt->get_authority( $authReq );
    $self->death("add_length($len) is not called with an integer")
        unless ($len =~ /^\d+$/);
    $self->death("I could not find the Authority '$authReq'") unless ($auth);
    my $aid = $auth->id;
    $self->{LENGTHS}{$len}       ||= {};
    $self->{LENGTHS}{$len}{$aid} ||= $auth;
    $self->{AUTHORITIES}{$aid}   ||= $auth;
    return 1;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 max_length

 Title   : max_length
 Usage   : my $len = $obj->max_length()
 Function: Returns the maximum length specified for the sequence
 Returns : An integer, or undef if not defined. Note that zero is a
           valid length, particularly in the context of SNPs which can
           have deletion alleles.
    Args : 

=cut

sub max_length {
    my $self = shift;
    $self->read_lengths;
    my @lens = sort { $b <=> $a } keys %{$self->{LENGTHS}};
    return $lens[0];
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 each_length

 Title   : each_length
 Usage   : my $lens = $obj->each_length()

 Function: Get a list of lengths assigned by each authority. 

 Returns : A hash reference, keyed to the length, with each
           key pointing to an array of authorities.

=cut

sub each_length {
    my $self = shift;
    my @lens = $self->read_lengths;
    return @lens if (wantarray);
    my $hash = {};
    foreach my $len (@lens) {
        while (my ($aid, $auth) = each %{$self->{LENGTHS}{$len}}) {
            $hash->{$len} ||= [];
            push @{$hash->{$len}}, $auth;
        }
    }
    return $hash;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 read_classes

 Title   : read_classes
 Usage   : $obj->read_classes
 Function: Reads class associations from a MapTracker DB. A tracker
           object should be provided, either through new() or
           tracker().
 Returns : A hash reference of Class objects associated with this name
 Args    : Optional arguments array:

    -force Normally the function will not execute if it has done so
           already - this means you do not need to worry about wasting
           CPU cycles by calling a method that might have been called
           already by another function. If you need to force a re-read
           of the class data, set -force to true.

=cut

sub read_classes {
    my $self = shift;
    my $args = $self->parseparams( -force  => 0,
				   @_ );
    if (!$self->task('read_classes') || $args->{FORCE}) {
        # Read from database if not already done, or if user is forcing
        $self->benchstart;
        my $rows = $self->tracker->dbi->
            named_sth("Retrieve class assignments for a seqname")->
            selectall_arrayref( $self->id );
        foreach my $row (@{$rows}) {
            my ($cid, $aid) = @{$row};
            $self->add_class($cid, $aid);
        }
        $self->task('read_classes', 1);
        $self->benchend;
    }
    return $self->{CLASSES};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 add_class

 Title   : add_class
 Usage   : $obj->add_class($class, $authority)
 Function: Adds a class asignment by a given authority
 Returns : 
    Args : A Class and an Authority. Objects, names or DB ids may be
           used as the arguments.

=cut

sub add_class {
    my $self = shift;
    my ($classReq, $authReq) = @_;
    my $mt    = $self->tracker;
    my $class = $mt->get_class($classReq);
    my $auth  = $mt->get_authority($authReq);
    $self->death("Could not get class for '$classReq'") unless ($class);
    $self->death("Could not get authority for '$authReq'") unless ($auth);
    my ($cid, $aid) = ($class->id, $auth->id);
    $self->{CLASSES}{$cid} ||= [ $class, {} ];
    $self->{CLASSES}{$cid}[1]{$aid} ||= $auth;
    $self->{AUTHORITIES}{$aid}      ||= $auth;
    return $class;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 each_class

 Title   : each_class
 Usage   : $obj->each_class($mode)
 Function: Returns a list of each class assocaited with the name
 Returns : An array of Class objects

    Args : [0] Optional mode string.

           If not set, then an array of class objects will be returned

           If set to 'name', then the class name will be substituted
           for the object.

           If 'id', then class_id will be substituted

           If 'hash', then a hash reference with class_id as keys will
           be returned. The keys will point to a hash reference of
           structure:

           { class => $class, auths => $arrayRefofAuthorityObjects }

           'hash' can be combined with 'id' or 'name', in which case
           the hash structure will be simplified to use the id/name as
           a key which will point to an array of authorities.

           If the key contains the word 'auth' then 

=cut

sub each_class {
    my $self = shift;
    my ($mode) = @_;
    $mode = lc($mode || 'array');
    $self->read_classes;

    my (%hash, @myArray);
    my $asHash = $mode =~ /hash/ ? 1 : 0;
    my $aName  = $mode =~ /auth/ ? 1 : 0;
    my $format = 'obj';
    if ($mode =~ /name/) {
        $format = 'name';
    } elsif ($mode =~ /id/) {
        $format = 'id';
    }
    while (my ($cid, $data) = each %{$self->{CLASSES}}) {
        my $class = $data->[0];
        if ($asHash) {
            my $auths = [ values %{$data->[1]} ];
            $auths = [ map { $_->name } @{$auths} ] if ($aName);
            if ($format eq 'name') {
                $hash{$class->name} = $auths;
            } elsif ($format eq 'id') {
                $hash{ $class->id } = $auths;
            } else {
                $hash{$class->id} = {
                    class => $class,
                    auths => $auths,
                };
            }
        } else {
            if ($format eq 'name') {
                $class = $class->name;
            } elsif ($format eq 'id') {
                $class = $class->id;
            }
            push @myArray, $class;
        }
    }
    if ($asHash) {
        return \%hash;
    } elsif ($format eq 'obj') {
        my @foo = sort { $a->id() <=> $b->id() } @myArray;
        @foo = sort { $a->id() <=> $b->id() } @foo;
        @foo = sort { $a->id() <=> $b->id() } @foo;
        @foo = sort { $a->id() <=> $b->id() } @foo;
        return @foo;
        #warn "Getting ready";
        #warn $#myArray;
        #map { warn "[$_] : ". $myArray[$_] ." has id = ". $myArray[$_]->id."\n    " } (0..$#myArray);
        #warn "Trouble ahead";
        #return sort { $a->id() <=> $b->id() } @myArray;
#        return @foo;
    } else {
        return sort { uc($a) cmp uc($b) } @myArray;
    }
    return @myArray;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 representative_class

 Title   : representative_class
 Usage   : $obj->representative_class

 Function: Returns a single class object, even if the name has more
           than one class associated with it. This call will trigger a
           database read if one has not already been done.

 Returns : A Class object
    Args : 

=cut

sub representative_class {
    my $self = shift;
    $self->read_classes;
    my %classCounter;
    my $bio = $self->tracker->get_class( 'bio' );
    foreach my $c ($self->each_class) {
	my $cn = $c->token;
	$classCounter{$cn} ||= 0;
	$classCounter{$cn}++;
	# Weight Biomolecules more:
	$classCounter{$cn} += 5 if ($bio->has_child($c));
    }
    my @classes = sort { $classCounter{$b} <=> $classCounter{$a} } keys
	%classCounter;
    # There is probably a more elegant way to do this than with names...
    $classes[0] ||= 'unknown';
    my $mt = $self->tracker;
    return $mt->get_class( $classes[0] );
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 is_class

 Title   : is_class
 Usage   : $obj->is_class($class)

 Function: Provided with a class object, will return 1 if *any* of the
           classes associated with this sequence match. This method
           will trigger a call to read_classes, if it has not been
           done so already.



 Returns : 0 or 1
    Args : One or more class objects

=cut

sub is_class {
    my $self = shift;
    $self->read_classes;
    my @classes = $self->each_class;
    for my $i (0..$#_) {
        my @to_test = $self->tracker->param_to_list(  $_[$i], 'class' );
        for my $ti (0..$#to_test) {
            for my $ci (0..$#classes) {
                return 1 if ($to_test[$ti]->has_child( $classes[$ci] ) );
            }
        }
    }
    return 0;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 each_authority

 Title   : each_authority
 Usage   : my @auths = $obj->each_authority
 Function: Returns a list of each authority associated with the name
 Returns : An array of Authority objects

    Args : [0] Optional return mode. If 'id', then the returned array
           will be the authority IDs. If 'name', then the array will
           be names (strings). Any other value and the array will be
           BMS::MapTracker::Authority objects.

=cut

sub each_authority {
    my $self = shift;
    my ($mode) = @_;
    $mode = lc($mode || 'obj');
    my @auths = values %{$self->{AUTHORITIES}};
    
    if ($mode eq 'id') {
        return sort { $a <=> $b } map {$_->id} @auths;
    } elsif ($mode eq 'name') {
        return sort { uc($a) cmp uc($b) } map {$_->name} @auths;
    }
    return sort { uc($a->name) cmp uc($b->name) } @auths;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 read_callbacks

 Title   : read_callbacks
 Usage   : my @cbs = $obj->read_callbacks
 Function: Loads all callback methods associated with this node.
 Returns : An array of the callbacks
 Args    : Optional arguments array:

    -force Normally the function will not execute if it has done so
           already - this means you do not need to worry about wasting
           CPU cycles by calling a method that might have been called
           already by another function. If you need to force a re-read
           of the class data, set -force to true.

=cut

*each_callback = \&read_callbacks;
sub read_callbacks {
    my $self = shift;
    my $args = $self->parseparams( -force  => 0,
                                   -return => 'node',
                                   -match  => [],
				   @_ );
    if (!$self->task('read_callbacks') || $args->{FORCE}) {
        $self->benchstart;
        my $mt = $self->tracker;
        my $edges = $mt->get_edge_dump( -name   => $self,
                                        -return => 'object array',
                                        -space  => 'callbacks' );
        foreach my $edge (@{$edges}) {
            $self->{CALLBACKS}{ $edge->id } = $edge;
        }
        $self->task('read_callbacks', 1);
        $self->benchend;
    }
    my @edges = values %{$self->{CALLBACKS}};
    return () if ($#edges < 0);

    my @matches = @{$args->{MATCH}};
    my @retval;
    my $rt = lc($args->{RETURN});
    if ($rt =~ /eval/) {
        push @matches, [ 'Language', 'Perl' ];
    }
  CBEDGE: foreach my $edge (@edges) {
      for my $i (0..$#matches) {
          next CBEDGE unless ($edge->has_tag( @{$matches[$i]} ) );
      }
      my @pris = $edge->has_tag('Priority');
      my $pri  = 100;
      if ($#pris > -1) {
          # The edge has priority defined - take the highest value
          @pris = sort { $b->[0] <=> $a->[0] } map { $_->num } @pris;
          $pri = $pris[0];
      }
      push @retval, [ $pri, $edge ];
  }
    @retval = map { $_->[1] } sort { $b->[0] <=> $a->[0] } @retval;
    if ($rt =~ /edge/) {
        # leave array as edge objects
    } elsif ($rt =~ /eval/) {
        # Turn to nodes:
        @retval = map { $_->other_node( $self ) } @retval;
        # Then evaluate the node name:
        @retval = map { $_->{EVALED} ||= eval($_->name) } @retval;
   } else {
       @retval = map { $_->other_node( $self ) } @retval;
    }
    return @retval;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 copy_number

 Title   : copy_number
 Usage   : my $copynumber = $obj->copy_number( $genomeVersion );

 Function: 



 Returns : 0 or 1
    Args : Optional genome version number

=cut

sub copy_number {
    my $self = shift;
    my $mt = $self->tracker;
    $self->read_mappings;
    my $gids = $mt->getHumanGenomeIDs;
    
    return 0;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 read_taxa

 Title   :read_taxa 
 Usage   : $obj->read_taxa
 Function: Reads taxa associations from a MapTracker DB. A tracker
           object should be provided, either through new() or
           tracker().
 Returns : A hash reference of Taxa objects associated with this name
 Args    : Optional arguments array:

    -force Normally the function will not execute if it has done so
           already - this means you do not need to worry about wasting
           CPU cycles by calling a method that might have been called
           already by another function. If you need to force a re-read
           of the class data, set -force to true.

=cut

sub read_taxa {
    my $self = shift;
    my $args = $self->parseparams( -force  => 0,
				   @_ );
    return 1 if ($self->task('read_taxa') && !$args->{FORCE});
    if (!$self->task('read_taxa') || $args->{FORCE}) {
        # Read from database if not already done, or if user is forcing
        $self->benchstart;
        my $rows = $self->tracker->dbi->
            named_sth("Retrieve taxa assignments for a seqname")->
            selectall_arrayref( $self->id );
        foreach my $row (@{$rows}) {
            my ($tid, $aid) = @{$row};
            $self->add_taxa($tid, $aid);
        }
        $self->task('read_taxa', 1);
        $self->benchend;
    }
    return 1;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 add_taxa

 Title   : add_taxa
 Usage   : $obj->add_taxa($taxa, $authority)
 Function: Adds a taxa asignment by a given authority
 Returns : 
    Args : A Taxa and an Authority. Objects, names or DB ids may be
           used as the arguments.

=cut

sub add_taxa {
    my $self = shift;
    $self->benchstart;
    my ($taxaReq, $authReq) = @_;
    my $mt     = $self->tracker;
    my ($taxa) = $mt->get_taxa($taxaReq);
    my $auth   = $mt->get_authority($authReq);
    unless ($taxa && $auth) {
        $authReq = '-UNDEF-' unless (defined $authReq);
        $taxaReq = '-UNDEF-' unless (defined $taxaReq);
        my $sn   = $self->name || '-NAME-NOT-DEFINED-';
        $self->death("Failed to add taxa for entry '$sn' using ".
                     "taxa '$taxaReq', authority '$authReq'");
    }
    my ($tid, $aid) = ($taxa->id, $auth->id);
    $self->{TAXA}{$tid} ||= [ $taxa, {} ];
    $self->{TAXA}{$tid}[1]{$aid} ||= $auth;
    $self->{AUTHORITIES}{$aid}   ||= $auth;
    $self->benchstop;
    return $taxa;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 each_taxa

 Title   : each_taxa
 Usage   : $obj->each_taxa($mode)
 Function: Returns a list of each taxa assocaited with the name
 Returns : An array of Taxa objects

    Args : [0] Optional mode string.

           If not set, then an array of taxa objects will be returned

           If set to 'name', then the taxa name will be substituted
           for the object.

           If 'id', then taxa_id will be substituted

           If 'hash', then a hash reference with taxa_id as keys will
           be returned. The keys will point to a hash reference of
           structure:

           { taxa => $taxa, auths => $arrayRefofAuthorityObjects }

           'hash' can be combined with 'id' or 'name', in which case
           the hash structure will be simplified to use the id/name as
           a key which will point to an array of authorities.

=cut

sub each_taxa {
    my $self = shift;
    my ($mode) = @_;
    $mode = lc($mode || 'array');
    $self->read_taxa;

    my (%hash, @array);
    my $asHash = $mode =~ /hash/ ? 1 : 0;
    my $format = 'obj';
    if ($mode =~ /name/) {
        $format = 'name';
    } elsif ($mode =~ /id/) {
        $format = 'id';
    }
    while (my ($cid, $data) = each %{$self->{TAXA}}) {
        my $taxa = $data->[0];
        if ($asHash) {
            my $auths = [ values %{$data->[1]} ];
            if ($format eq 'name') {
                $hash{$taxa->name} = $auths;
            } elsif ($format eq 'id') {
                $hash{ $taxa->id } = $auths;
            } else {
                $hash{$taxa->id} = {
                    taxa => $taxa,
                    auths => $auths,
                };
            }
        } else {
            if ($format eq 'name') {
                $taxa = $taxa->name;
            } elsif ($format eq 'id') {
                $taxa = $taxa->id;
            }
            push @array, $taxa;
        }
    }
    if ($asHash) {
        return \%hash;
    } elsif ($format eq 'obj') {
        return sort { uc($a->name) cmp uc($b->name) } @array;
    } else {
        return sort { uc($a) cmp uc($b) } @array;
    }
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 each_map_feat

 Title   : each_map_feat
 Usage   : $obj->each_map_feat

 Function: For each mapping object, returns a BioPerl Generic
           SeqFeature object describing the location of the other
           sequence on this sequence.

 Returns : An array of feature objects
    Args : Associative array of arguments. Recognized keys [Default]:

 -keepclass
 -tossclass Optional Class object or array reference of such. Useful
            only if a single name is being used - the classes of the
            other name in the mapping will be compared to those
            provided, and the mapping will be kept (or tossed) if at
            least one of the classes matches.

    -offset Default 0. Used if your template sequence does not start
            at position 1. If it rather starts at position 100, you
            should provide an offset of 99. This value will be
            subtracted from the stop/start coordinates. This is handy
            if you are using only part of a large sequence (eg a whole
            chromosome).

     -scale Optional scaling value. Be sure to adjust the parent
            sequence accordingly!

    -fphtml Default 0. If true, add HTML tags for use in FriendlyPanel

  -collapse Default 1. If true, then group features by their name, and
            collapse any that have the same name and identical
            location strings.

  -copiesin ?? Should I add a mechanism to count copy number? How?

  -featbase Default 'query'. Defines which coordinates to use for the
            feature. 'query' will give the coordinates from *this*
            sequence - for example, if the mapping is a transcript on
            a genome, start-stop might be something like 1-2236. If
            instead you request 'subject', then the same request could
            be 99482354-99531005.

 -addobject Default 0. If true, then each feature will have the
            Mapping object(s) added under hash key
            _MAP_OBJECTS_. This key will point to an array, which
            will contain all map objects for that feature (not that
            -collapse could cause some features to have more than one
            object).

   -congeal Default 0. If true, then collapse small gaps when
            calculating the feature coordinates.

  -debridge Default undef. Optional sequence class used to find a
            'better' authority for maps that are the result of a
            bridge between two other maps. Provide one or more
            classes, and the parent map with matching sequence will be
            used for the authority. For example, if you provide
            'variant' (or ['variant']), then the parent that
            contributed the variant sequence will be used for the
            authority. If both sequences match, then the original map
            authority (Bridge Builder) will be used.

=cut

sub each_map_feat {
    my $self = shift;
    my $sname = $self->name;
    $self->benchstart;
    my $mt   = $self->tracker;
    my $args = $self->parseparams( -keepclass => [],
				   -tossclass => [],
				   -offset    => 0,
				   -fphtml    => 0,
				   -scale     => 0,
				   -collapse  => 1,
				   -featbase  => 'query',
                                   -congeal   => 0,
                                   -debridge  => undef,
				   @_ );
    my $cong   = $args->{CONGEAL};
    my $pad    = '&nbsp;&nbsp;';
    my $offset = $args->{OFFSET} || 0;
    my $scale  = $args->{SCALE}  || 0;
    my @kcs    = $mt->param_to_list( $args->{KEEPCLASS}, 'class');
    my @tcs    = $mt->param_to_list( $args->{TOSSCLASS}, 'class');
    my $var    = $mt->get_class('variant');
    my $probe  = $mt->get_class('probe');
    my ( $bestvers, $blisvers ) = $mt->genomeVersions;
    my %collapseDat;
    my $useSelf;
    if ($args->{FEATBASE} =~ /query/i) {
	$useSelf = 1;
    } elsif ($args->{FEATBASE} =~ /subject/i) {
	$useSelf = 0;
    } else {
	$self->death("each_map_feat() must specify -featbase as either 'query' or 'subject', not '$args->{FEATBASE}'");
    }

    my (@feats, %glocs, %gvers, %seen);
    my %sids;
    my $debridge = $args->{DEBRIDGE};
    $debridge = [ $debridge ] if ($debridge && !ref($debridge));

    my @taxids = map { $_->id } $self->each_taxa;


  MAPLOOP: foreach my $map ($self->each_mapping) {
      # warn $map->other_seq( $self )->namespace_name;
      my $lkey = $map->key_for_seq($self);
      if (my $prevf = $seen{$lkey}) {
          # We already have this location recorded.
          # It might be a bridged location from diverse sources, though
          next unless (ref($prevf));
          $self->_set_genomic_loc($prevf, $map);
          $self->_record_authority($prevf, $map, $debridge);
          push @{$prevf->{_MAP_OBJECTS_}}, $map if ($args->{ADDOBJECT});
          next;
      }
      $seen{$lkey} = 1;
      my $seq  = $map->other_seq( $self );
      my $self_index = $map->seq_index( $self );
      for my $tc (0..$#tcs) {
	  next MAPLOOP if ($seq->is_class( $tcs[$tc] ));
      }
      my $tossit = ($#kcs > -1) ? 1 : 0;
      for my $kc (0..$#kcs) {
	  if ($seq->is_class($kcs[$kc])) { $tossit = 0; last; }
      }
      next MAPLOOP if ($tossit);
      $sids{ $seq->id }++;

      my ($feat, $colapseKey, $pri);
      if ($seq->is_class($var)) {
          $pri = 'variant';
      } else {
          my $repClass = $seq->representative_class;
          $pri = lc($repClass->token);
      }
      my $name     = $seq->name;
      my $locobj   = $map->loc_for_seq( -seq     => $self, 
                                        -offset  => $offset, 
                                        -scale   => $scale,
                                        -congeal => $cong, );

      if ($args->{COLLAPSE}) {
          # We want similar sequences to use one glyph
          # Get location, we will use it as a key
          $colapseKey = "$pri\t" . $locobj->to_FTstring;
          my ($taxa) = $seq->each_taxa;
          $colapseKey .= $taxa->id if ($taxa);
          $feat = $collapseDat{$colapseKey};
      }
      unless ($feat) {
          # We need to make a new feature
          $feat = Bio::SeqFeature::Generic->new( -primary => $pri);
	  $feat->location( $locobj );
          weaken($locobj->{PARENTOBJ} = $feat);
          $collapseDat{$colapseKey} = $feat;
          push @feats, $feat;
      }
      push @{$feat->{MT_META}{COLLAPSE}}, $map;
  }

    foreach my $feat (@feats) {
        my @maps = @{$feat->{MT_META}{COLLAPSE}};
        $feat->{_MAP_OBJECTS_} = \@maps if ($args->{ADDOBJECT});
        my $pri  = $feat->primary_tag;
        my $tvs  = $feat->{MT_META}{TAGVAL} ||= {};
        my %struct;
        foreach my $map (@maps) {
            my $oseq  = $map->other_seq( $self );
            my $oname = $oseq->name;
            my $onsn  = $oseq->namespace_name;
            my $oid   = $oseq->id;
            my ($di, $sv) = ($oname, 0);
            if ($oseq->is_class('gdna')) {
                my ($s,$t,$sn,$bd) = $mt->parse_genomic_sequence_name($oname);
                if ($s) {
                    $sv = $bd;
                    if ($sn =~ /^$t/i) {
                        # The entitiy name also includes the type
                        $di = "$s $sn";
                    } else {
                        $di = "$s $t $sn";
                    }
                }
            } elsif ($oname =~ /^(.+)\.(\d+)$/) {
                ($di, $sv) = ($1, sprintf("%04d",$2));
            }
            
            $tvs->{'name'}{$di}++;
            my ($start, $stop) = $map->ranges($oseq);
            if ($oseq->is_class($var)) {
                $tvs->{'Accession'}{$di}++;
                $tvs->{'db_xref'}{"SnpTracker:$oname"}++;
            } elsif ($oseq->is_class($probe)) {
                $tvs->{'db_xref'}{"Bart:$oname"}++;
                $tvs->{'db_xref'}{"KNN:$oname"}++;
            }
            my $score  = $map->score;
            my $aut    = $self->_record_authority($feat, $map, $debridge);
            my $prov   = "Directly placed by <span class='auth'>$aut</auth>";
            my $pos    = $start;

            $tvs->{'positional_authority'}{$aut}++;
            if (defined $score) {
                $tvs->{'score'}{$score}++;
            }
            $self->_set_genomic_loc( $feat, $map);

            my ($bseq, $bloc) = $map->transform->bridge;
            if ($bseq) {
                # This is a bridged mapping
                $prov = $bseq->name;
                my ($s,$t,$sn,$bd) = $mt->parse_genomic_sequence_name($prov);
                if ($s) {
                    $prov = "$s $t $sn <span class='build'>$bd</span>";
                }
                $prov = "Bridge from $prov";
                $pos = $bloc->[0][0];
            }
            $struct{$di}{$sv}{id}{$oid}++;
            push @{$struct{$di}{$sv}{info}}, {
                prov => $prov,
                loc  => [$start, $stop, $score ],
            }
        }
        if (my $scores = $tvs->{'score'}) {
            my ($sc) = sort { $b <=> $a } keys %{$scores};
            $feat->score( $sc );
        }
        while (my ($tag, $vals) = each %{$tvs}) {
            map { $feat->add_tag_value($tag, $_) } sort keys %{$vals};
        }
        delete $feat->{MT_META};
        next unless ($args->{FPHTML});
        my @dis = sort keys %struct;

        my %htmls;
        foreach my $di (@dis) {
            my @svs = sort keys %{$struct{$di}};
            foreach my $sv (@svs) {
                my $shtml = ""; #"<ul>";
                my @infos = @{$struct{$di}{$sv}{info}};
                my @ids   = keys %{$struct{$di}{$sv}{id}};
                $sv =~ s/^0+//;
                my $acc = $di;
                if ($#dis == 0) {
                    # Only a single accession
                    $acc = $sv ? $sv : "";
                } elsif ($sv) {
                    $acc .= ($sv =~ /^\d+$/) ? ".$sv" : " $sv";
                }
                foreach my $info (@infos) {
                    $shtml .= "<div class='hang'>&raquo; ".$info->{prov}."<br />";
                    my ($s, $e, $sc) = @{$info->{loc}};
                    my $sct = defined $sc ? sprintf 
                        (" <span class='score'>[%s]</span>", $sc):"";
                    $shtml .= "CoordLink$s-$e$sct";
                    $shtml .= "</div>";
                }
                # $shtml .= "</ul>";
                $htmls{$shtml}{acc}{$acc}++;
                map { $htmls{$shtml}{id}{$_}++ } @ids;
            }
        }
        my $html = "";
        while (my ($shtml, $dat) = each %htmls) {
            my @accs = sort keys %{$dat->{acc}};
            my @ids  = sort { $a <=> $b } keys %{$dat->{id}};
            my $tok = join(", ", sort { lc($a) cmp lc($b) } @accs);
            if ($tok) {
                $tok = sprintf("Version%s <span class='build'>%s</span>",
                               $#accs == 0 ? '':'s', $tok) if ($#dis == 0);
                $html .= "<b>$tok</b>";
            }
            while ($shtml =~ /CoordLink([\d\.]+)-([\d\.]+)/) {
                my ($s,$e) = ($1, $2);
                my $rep = sprintf("<a href='%s?seqnames=%s' class='coord'>",
                                  $mbpl, join('%0A', map { "$_:$s-$e" } @ids));
                if ($s == 1 && $e ==0) {
                    $rep .= "Deletion";
                } else {
                    $rep .= &COMMAIZE($s) .' - '. &COMMAIZE($e);
                }
                $rep .= "</a>";
             #   my $rep = sprintf("<a href='%s' class='coord'>%s - %s</a>",
             #                     $url, &COMMAIZE($s), &COMMAIZE($e));
                $shtml =~ s/CoordLink$s\-$e/$rep/g;
            }
            $html .= $shtml;
        }
        $feat->{__HTML__} = [$html];
    }

=pod

          if ($seq->is_class('contig', 'fullgdna')) {
              # This is a genomic mapping.
              #warn "<b>$name</b><br />\n";
              my ($smallname, $vers) = ($name,0);
              my ($s, $t, $sn, $b) = $mt->parse_genomic_sequence_name($name);
              if ($s) {
                  ($smallname, $vers) = ("$s $t $sn", $b);
              } elsif ($seq->is_class('VERSIONED') && 
                       $name =~ /^([^\.]+)\.(\d+)$/ ) {
                  ($smallname, $vers) = ($1, 'vers ' . $2);
              }

              $gvers{$smallname} ||= {};
              $gvers{$smallname}{$vers}++;
              unless ( $glocs{ $key } ) {
                  # To get perfect resolution we did not scale above.
                  # Scale now if we need to:
                  $locobj = $map->loc_for_seq( -seq     => $self, 
                                               -offset  => $offset, 
                                               -scale   => $scale,
                                               -congeal => $cong,)
                      if ($scale && $scale != 1);
                  $glocs{ $key } = {
                      loc  => $locobj,
                      hits => {},
                  };
              }
              $glocs{ $key }{hits}{ $smallname } ||= {};
              $glocs{ $key }{hits}{ $smallname }{ $vers } ||= [];
              push @{$glocs{ $key }{hits}{ $smallname }{ $vers }}, $map;
              next MAPLOOP;
          } elsif ( $collapseDat{$name} ) {
              # This name was seen before - is the map the same?
              foreach my $dat (@{$collapseDat{$name}}) {
                  my ($ofeat, $omap) = @{$dat};
                  next unless( $map->shares_coordinates( $omap, $name ) );
                  # This feature has the same name and coordinates
                  push @{$ofeat->{MT_META}{COLLAPSE}}, $map;
                  $feat  = $ofeat; 
                  last;
              }
          }


      }
    
    foreach my $feat (@feats) {

       my @otaxa      = map { $_->each_taxa } @seqs;
        my $other_taxa;
        if ($#taxids > -1 && $#otaxa > -1) {
            # The query (this sequence) has at least 1 defined taxa
            my %istaxa = map { $_->id => 1 } @otaxa;
            my $hit_taxa = 0;
            map { $hit_taxa += $istaxa{$_} || 0 } @taxids;
            if ($hit_taxa == 0) {
                # None of the query taxa match
                $other_taxa = ($#otaxa > 0) ? '#000000' : $otaxa[0]->as_color;
            }
        }
        if ($other_taxa) {
            $feat->primary_tag( $feat->primary_tag . '_XENO');
             $feat->{_BGCOLOR_} = $other_taxa;
            my $locobj = $feat->location;
            if ($locobj->isa('Bio::Location::Split')) {
                foreach my $sl ($locobj->sub_Location) {
                    $sl->{_BGCOLOR_} = $other_taxa;
                }
            }
        }


	foreach my $auth (@auths) {
	    $feat->add_tag_value('authority', $auth);
	}
	$feat->add_tag_value('db_xref', "MapTracker:map_id=$midString")
	  if ($midString);
	if ($args->{FPHTML}) {
	    $feat->{__HTML__} ||= [];
            if ($other_taxa) {
                push @{$feat->{__HTML__}}, map {$_->javascript_link()} @otaxa;
            }
	    my @keys = sort keys %{$feat->{MT_META}{FROM}};
	    foreach my $key (@keys) {
		my $text = "";
		$text .= "<u>$key</u><br />";
		my @subkeys = sort keys %{$feat->{MT_META}{FROM}{$key}};
		my @txts;
		foreach my $sk (@subkeys) {
		    my $line = "&nbsp;$sk";
		    my @ps  = sort { $a <=> $b } keys 
			%{$feat->{MT_META}{FROM}{$key}{$sk}};
		    if ($#ps > 0) {
			$line .= " <span class='blue'>Multiple Locations:</span>";
		    }
		    foreach my $pos (@ps) {
                        my @scores;
                        foreach my $dat 
                            (sort { $b->[0] <=> $a->[0] } 
                             @{$feat->{MT_META}{FROM}{$key}{$sk}{$pos}}) {
                                my ($sc, $frac) = @{$dat};
                                if (!defined $frac) {
                                    push @scores, $sc;
                                } else {
                                    my $msg = "Fully&nbsp;Bridged";
                                    if ($frac < 1) {
                                        $msg = sprintf
                                            ("%.2f%%&nbsp;covered", 100*$frac);
                                    }
                                    push @scores, "~$sc&nbsp;[$msg]";
                                }
                        }

			$line .= sprintf
			    ("<br />&nbsp;&nbsp;<span class='smaller'>%s</span>",
			     &comma_number($pos)) if ($#ps > 0);

			my $sc = join(",", @scores);
			$line .= " <span class='score'>Score: $sc</span>" 
			    if ($sc);
		    }
		    push @txts, $line;
		}
		$text .= join("<br />", @txts);
		push @{$feat->{__HTML__}}, $text;
	    }
	    push @{$feat->{__HTML__}}, "<b>Classed By:</b> $authString";
            my $tag = sprintf("%s:%d-%d", $self->name, 
                              $feat->start + $offset, $feat->end + $offset);
            push @{$feat->{__HTML__}}, "<a href='$mbpl?seqname=$tag'>".
                "Zoom to Fit</a>";
	}
	delete $feat->{MT_META};
    }


    # Now treat the genomic sequences specially:


    foreach my $smallname (keys %gvers) {
        my @versions = sort keys %{$gvers{$smallname}};
        $gvers{$smallname} = join(", ", @versions);
    }

    foreach my $key ( keys %glocs ) {
        my $locobj = $glocs{ $key }{loc};
        my @hits = sort keys %{$glocs{ $key }{hits}};
        next unless ($#hits > -1);
        my (@labbits, @html);
        my $text = "<table class='buildtable'>\n";
        $text .= "<caption>Genomic Locations:</caption>\n";
        $text .= "<tbody>";
        my @allmaps;
        my $other_taxa = 0;
        my @ots;
        # my $other_taxa = $#taxids < 0 ? 0 : ;1
        foreach my $smallname (@hits) {
            my @versions = sort keys %{$glocs{ $key }{hits}{ $smallname }};
            my @labmod;
            my $vtag = join(", ", @versions);
            push @labmod, $vtag if ($vtag ne $gvers{$smallname});
            my %copies;
            my $jump = "";
            foreach my $vers (@versions) {
                my @maps = @{$glocs{ $key }{hits}{ $smallname }{ $vers }};
                my $mnum = $#maps + 1;
                $copies{$mnum}++;
                my $vtag = "";
                if ($vers) {
                    $vtag = $vers;
                    $vtag = "<span class='multicopy'>$vtag</span>" if ($mnum > 1);
                }
                $jump .= "<tr><td style='text-align:right;padding-left:2em'>$vtag</td><td>";
                my @links;
                push @allmaps, @maps;
                foreach my $map (@maps) {
                    my $seq  = $map->other_seq( $self );
                    if (!$other_taxa && $seq->is_taxa( \@taxids ) == 0) {
                        # This sequence is a different taxa
                        @ots = $seq->each_taxa;
                        $other_taxa = ($#ots > 0) ? 
                            '#000000' : $ots[0]->as_color;
                    }
                    my ($start, $stop) = $map->ranges($seq);
                    my $nsname = $seq->namespace_name;
                    my $tag    = sprintf
                        ("%s:%d-%d", CGI::escape($nsname), $start, $stop);
                    
                    push @links, 
                    [ $start, $stop, $nsname, sprintf
                      ("<a href='$mbpl?seqname=%s'>%s - %s</a> <b>[<span class='score'>%s</class>]</b>", $tag, &COMMAIZE($start), 
                       &COMMAIZE($stop), $map->score || "") ];
                }
                @links = sort { $a->[0] <=> $b->[0] } @links;
                if ($#links > 0) {
                    my ($start, $stop, $nsname) = 
                        ($links[0][0], $links[-1][1], $links[0][2],);
                    my ($dist, $uni) = (($stop - $start + 1)/1000, 'kb');
                    if ($dist > 1000) {
                        $dist /=1000; $uni = 'Mb';
                    }
                    push @links, 
                    [ $start, $stop, $nsname, sprintf
                      ("<a href='$mbpl?seqname=%s:%d-%d'>View %.3f %s</a> ".
                       " <i>covers all %d copies</i>", 
                       $nsname, $start, $stop, $dist, $uni, $#links + 1) ]; 
                }
                @links = map { $_->[-1] } @links;
                $jump .= "<span class='coord'>" . join("<br />", @links).
                    "</span></td></tr>";
            }
            my @allcop = sort {$a <=> $b} keys %copies;
            my ($minc, $maxc) = ($allcop[0], $allcop[-1]);
            my $nametag = $smallname;
            my $labtag  = $smallname;
            if ($maxc > 1) {
                my $coptext = $minc == $maxc ? $minc : "$minc-$maxc";
                push @labmod, $coptext ."x";
                $nametag .= " <span class='multicopy'>$coptext copies</span>";
            }
            $labtag .= " (".join(", ", @labmod).")" if ($#labmod > -1);
            $text .= "<tr><th colspan='2'>$nametag</th></tr>$jump";
            push @labbits, $labtag;
        }
        $text .= "</tbody></table>\n";
        push @html, $text;
        
        
        my $ptag = $other_taxa ? 'GDNA_XENO' : 'GDNA';
	my $feat = Bio::SeqFeature::Generic->new( -primary => $ptag);
        if ($other_taxa) {
            # Factory::get_option does not appear to be called 
            # Factory::option does not appear to be called with bgcolor:
            # Glyph::bgcolor is never being called
            $feat->{_BGCOLOR_} = $other_taxa;
            if ($locobj->isa('Bio::Location::Split')) {
                foreach my $sl ($locobj->sub_Location) {
                    $sl->{_BGCOLOR_} = $other_taxa;
                }

            }
            foreach my $ot (@ots) {
                unshift @html, $ot->javascript_link();
            }
            
        }
    }
=cut

    $self->benchend;
    return @feats;
}

sub COMMAIZE {
    my ($num) = @_;
    my $newnum = "";
    my $remain = $num - int($num);
    $num = int($num) . '';
    while ((my $len = length($num)) > 3) {
        $newnum =  ',' . substr($num, $len - 3, 3) . $newnum;
        $num = substr($num, 0, $len - 3);
    }
    $newnum   = $num . $newnum;
    if ($remain) {
        $newnum .= int(0.5 + 1000 * $remain)/1000;
        $newnum =~ s/^0+/0/; $newnum =~ s/\.0\./\./;
    }
    return $newnum;
}
sub _set_genomic_loc {
    my $self = shift;
    my ($feat, $map) = @_;
    my @parents = $map->parents;
    return unless ($parents[0]);
    # This is a scaffolded map, what is the scaffold location of the
    # other sequence?
    my $seq  = $map->other_seq( $self );
    my $sin  = $map->seq_index( $seq );
    # Get the map used to scaffold in the other sequence:
    my $par  = $parents[$sin];
    my $scaf = $par->other_seq( $seq );
    return unless ($scaf->is_class('gdna'));
    my $sname = $scaf->name;
    my ($start, $stop) = $par->ranges( $scaf );
    my $str   = $par->strand;
    my $gloc  = sprintf("%s:%d-%d,%d", $sname,$start,$stop,$str);
    $feat->{MT_META}{TAGVAL}{'genomic_location'}{$gloc}++;
    return $gloc;
}

sub _record_authority {
    my $self = shift;
    my ($feat, $map, $debridge) = @_;
    my $aut;
    if ($debridge) {
        # The user wants to deconvolute Bridge Builder maps to get a more
        # informative authority
        my @match;
        foreach my $test ($map->seqs) {
            push @match, $test if ($test->is_class(@{$debridge}));
        }
        if ($#match == 0) {
            if (my $parmap = $map->parent_for_seq( $match[0] )) {
                $aut = $parmap->authority->name;
            }
        }
    }
    $aut ||= $map->authority->name;
    $feat->{MT_META}{AUTH}{$aut} = 1;
    return $aut;
}

sub comma_number {
    my ($num) = @_;
    $num = int(0.999999 + $num);
    my @bits = split('', $num);
    my $text = "";
    my $counter = 0;
    for (my $i = $#bits; $i >= 0; $i--) {
	$text = ',' . $text if ($counter && !($counter % 3));
	$text = $bits[$i] . $text;
	$counter++;
    }
    return $text;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 each_mapping

 Title   : each_mapping
 Usage   : $obj->each_mapping
 Function: Returns a list of each mapping assocaited with the name
 Returns : An array of Mapping objects
    Args : 

=cut

sub each_mapping {
    my $self = shift;
    my @maps = @{$self->{MAPPINGS}};
    return @maps;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 map_count

 Title   : map_count
 Usage   : my $integer = $obj->map_count
 Function: Gets the number of maps stored in this object
 Returns : The count of the number of maps
    Args : 

=cut

*mapping_count = \&map_count;
sub map_count {
    my $self = shift;
    return $#{$self->{MAPPINGS}} + 1;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 each_mapping_by_subject

 Title   : each_mapping_by_subject
 Usage   : $obj->each_mapping_by_subject
 Function: Returns a list of each mapping assocaited with the name
 Returns : An array, with entries [ $subject, \@map_list ]
    Args : 

=cut

sub each_mapping_by_subject {
    my $self = shift;

    my %submaps; # Mappings grouped by subject
    my %seen;
    foreach my $map ( $self->each_mapping ) {
        my $key = $map->key_for_seq($self);
        next if ($seen{$key});
        $seen{$key} = 1;
        my $oseq  = $map->other_seq( $self );
	my $oname = $oseq->name;
	$submaps{ $oname } ||= { maxscore => 0,
                                 obj      => $oseq,
                                 maps     => [] };
	push @{$submaps{ $oname }{maps}}, $map;
	my $sc = $map->score;
	$submaps{$oname}{maxscore} = $sc 
            if (defined $sc && $sc > $submaps{$oname}{maxscore});
    }
    my @names = sort { $submaps{ $b }{maxscore} <=> $submaps{ $b }{maxscore} ||
                           $a cmp $b } keys %submaps;
    my @retval;
    foreach my $name (@names) {
        my @maps = sort { ($b->score || 0) <=> 
                              ($a->score || 0) } @{$submaps{ $name }{maps}};
                push @retval, [ $submaps{ $name }{obj}, \@maps ];
    }
    return @retval;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 add_edge

 Title   : add_edge
 Usage   : $obj->add_edge($edge [, $edge2, $edge3, ... ])
 Function: Adds one or more edge objects
 Returns : 
    Args : 

=cut

sub add_edge {
    my $self = shift;
    $self->benchstart;
    foreach my $edge (@_) {
        my $eid = $edge->id;
        $self->{EDGES}{$eid} = $edge;
        # Also add it to the other node
        my $other = $edge->other_seq( $self );
        $other->{EDGES}{$eid} = $edge;
    }
    $self->benchend;
    return 1;
}

sub each_edge {
    my $self = shift;
    return values %{$self->{EDGES}};
}

sub edge_hash {
    my $self = shift;
    my %edges;
    foreach my $edge ($self->each_edge) {
        my ($reads, $refNode, $other) = $edge->reads($self);
        my $oid = $other->id;
        $edges{ $reads }{ $oid } ||= [];
        push @{$edges{ $reads }{ $oid }}, $edge;
    }
    return \%edges;
}

*description = \&desc;
sub desc {
    my $self = shift;
    unless ($self->{DESC}) {
        my @nodes;
        # Make sure the proper edge type has been read
        $self->read_edges( -limit => 10, -keeptype => 'shortfor' )
            unless ($self->task('read_edges'));
        # Find edges that are 'shortfor'
        my %unique;
        foreach my $edge ($self->each_edge) {
            $unique{ $edge->node2->id } = $edge->node2 if
                ($edge->reads($self) eq 'is a shorter term for');
        }
        # Sort so shorter descriptions are near front
        #@nodes = sort {length($a->name) <=> length($b->name)} values %unique;
        # CHANGE. Sort so -MOST RECENT- descriptions are near front
        @nodes = sort { $b->id <=> $a->id } values %unique;

        $self->{DESC} = \@nodes;
        $self->task('desc', 1);
    }
    return wantarray ? @{$self->{DESC}} : $self->{DESC}[0];
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 read_edges

 Title   : read_edges
 Usage   : $obj->read_edges

 Function: Identifies other sequences related to this one. A tracker
           object should be provided, either through new() or
           tracker().

 Returns : A hash reference of Seqname objects associated with this name
 Args    : Optional arguments array:

    -force Normally the function will not execute if it has done so
           already - this means you do not need to worry about wasting
           CPU cycles by calling a method that might have been called
           already by another function. If you need to force a re-read
           of the class data, set -force to true.

    -limit Default 0. If non-zero, then limit the returned hits to
           this number *for each edge type*. This is designed to
           prevent unwieldy number of hits returning

 -keeptype Default []. Optional array ref (or single string) or edge
           types that you wish to read

=cut

*read_relations = \&read_edges;
sub read_edges {
    my $self = shift;
    my $args = $self->parseparams( -force  => 0,
				   -limit  => 0,
                                   -space    => 'all',
				   -keeptype => undef,
                                   -keepclass => undef,
				   @_ );
    return $self->each_edge if 
	($self->task('read_edges') && !$args->{FORCE});

    $self->benchstart;
    my $mt = $self->tracker;
    $self->death("You need to provided a MapTracker object to read edges")
	unless ($mt);

    my ($list, $comps) = $mt->get_edge_dump
        ( -name    => $self,
          -limit   => $args->{LIMIT},
          -space   => $args->{SPACE},
          -nodistinct => $args->{NODISTINCT}, 
          -keeptype => $args->{KEEPTYPE} || $args->{TYPE},
          -keepclass => $args->{KEEPCLASS} || $args->{CLASS},
          -return  => 'obj array' );

    $self->{FULLEDGE} ||= {};
    map { $self->{FULLEDGE}{$_} ||= 1 } @{$comps};
    $self->add_edge( @{$list} );
    
    # Make a note that edges have been read, unless this was for
    # specific types only:

    $self->task('read_edges', 1) unless 
        ($args->{KEEPTYPE} || $args->{KEEPCLASS});
    $self->benchend;
    return @{$list};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 scaffold

 Title   :scaffold
 Usage   : $obj->scaffold()

 Function: Uses currently stored mappings in an attempt to scaffold
           (bridge) twice-removed sequences that are mapped to a
           common sequence..

 Returns : A hash reference of Seqname objects associated with this name
 Args    : 

  -usebridge An optional array reference of classes that you want to
             use for the bridging - for example, you can request that
             only genomic sequences be used.

 -tossbridge Alternatively, you can provide an array ref of classes
             that you do NOT want to use.

             The above tests include class children - so if you are
             testing for 'variation', 'snp' (a child of variation)
             will count as well.

  -keepclass

  -tossclass Like above, but these parameters reflect the types of
             mappings that are returned.

 -genomevers Optional integer. If provided and a potential bridge
             matches /\.NCBI(\d\d)$/, then the bridge will be kept
             only if the version matches.

=cut

sub scaffold {
    my $self = shift;
    $self->benchstart;
    my $mt = $self->tracker;
    $self->death("You must provide a MapTracker object in order to scaffold")
	unless ($mt);
    my $args = $self->parseparams( -usebridge  => [],
				   -tossbridge => [],
				   -keepclass  => [],
				   -tossclass  => [], 
				   -genomevers => 0,
				   @_
				   );
    my @useclass  = $mt->param_to_list( $args->{USEBRIDGE}, 'class');
    my @tossclass = $mt->param_to_list( $args->{TOSSBRIDGE}, 'class');
    my @maps = $self->each_mapping;
    my @newMaps = ();
    my $gv = $args->{GENOMEVERS};
    my %seen;
  MAPLOOP: foreach my $map (@maps) {
      # We will only scaffold once per location - ignoring multiple maps set
      # by different authorities
      my $key = $map->key_for_seq($self, 'IGNORE AUTHORITY');
      next if ($seen{$key});
      $seen{$key} = 1;
      # Check the class of the other sequence to see if it should be used
      my $in = $map->seq_index($self);
      my @seqs = $map->seqs;
      my $other = $seqs[!$in];
      foreach my $tc (@tossclass) {
	  next MAPLOOP if ($other->is_class($tc));
      }
      my $tossit = ($#useclass > -1) ? 1 : 0;
      foreach my $kc (@useclass) {
	  if ($other->is_class($kc)) { $tossit = 0; last; }
      }
      next MAPLOOP if ($tossit);
      if ($gv && $other->name =~ /\.NCBI_?(\d\d)$/i) {
	  next MAPLOOP unless ($gv == $1);
      }
      foreach my $nmap ($map->bridge( -name => $self, %{$args})) {
          my $oseq = $nmap->other_seq($self);
          # Do not add bridges to ourselves
          next if ($oseq->id == $self->id);
          push @newMaps, $nmap;
      }
  }
    
    # Is this the right place to store these? Store these elsewhere?
    push @{$self->{MAPPINGS}} , @newMaps;
    # CIRCULAR REFERENCE :
    # $seqobj->{MAPPINGS}[0] = $mapobj; $mapobj->{SEQ1} = $seqobj;

    $self->benchend;
    return @newMaps;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 has_parent

 Title   : has_parent
 Usage   : $obj->has_parent( $parent )

 Function: Uses the 'is a child of' edge type to determine if this
           sequence is a child (or grand^N child) of $parent

 Returns : 0 if the sequence and none of its children are the parent,
           an integer if otherwise. The integer is the distance to the
           parent - 1 indicates the query itself matched, 2 is a
           parent that is 1 step away, etc. Not guaranteed to return
           the shortest distance.

 Args    : The parent Seqname (name, ID or object)

=cut

sub has_parent {
    my $self = shift;
    $self->benchstart;
    my ($req, $level) = @_;
    $level ||= 1;
    if ($level > 100) {
	$self->death("Infinite loop in has_parent? More than 100 iterations");
    }
    my $mt     = $self->tracker;
    my $par    = $mt->get_seq($req);
    my $retval = 0;
    if ($par) {
	if ($par->id == $self->id) {
	    # The request is the parent!
	    $retval = $level;
	} else {
	    my $sid   = $self->id;
	    my $data  = $mt->get_edge_dump( -name     => $self,
                                            -keeptype => 'is a child of',
                                            -limit    => 0,
                                            -return   => 'array');
	    # Are any of the children the parent?
	    foreach my $row (@{$data}) {
		my ($sid1, $sid2, $tid, $aid) = @{$row};
		# What is the ID of the other member?
		my $oid = ($sid1 == $sid) ? $sid2 : $sid1;
		my $oseq = $mt->get_seq($oid);
		# Recursively check:
		if ( my $kid = $oseq->has_parent( $par, $level + 1 ) ) {
		    $retval = $kid;
		    last;
		}
	    }
	}
    } else {
	$self->error("has_parent() could not find the parent seq '$req'");
    }
    $self->benchend;
    # warn sprintf("%s%s = %d ", "   " x $level, $self->name, $retval) if ($retval);
    return $retval;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 has_mappings

 Title   : has_mappings
 Usage   : $obj->has_mappings
 Function: Returns 1 if the seqname has mappings associated with it.
 Returns : 1 or 0
 Args    : 

=cut

sub has_mappings {
    my $self = shift;
    unless (defined $self->{HAS_MAPPINGS}) {
        if ( $self->task('read_mappings') ) {
            $self->{HAS_MAPPINGS} = $#{$self->{MAPPINGS}} < 0 ? 0 : 1;
        } else {
            my $id = $self->id;
            my $exist = self->tracker->dbi->
                named_sth("Check if sequence has mappings")->
                get_single_value( $id, $id);
            $self->{HAS_MAPPINGS} = $exist ? 1 : 0;
        }
    }
    return $self->{HAS_MAPPINGS};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 read_mappings

 Title   : read_mappings
 Usage   : $obj->read_mappings
 Function: Retrieves mappings for this sequence.
 Returns : A hash reference of Seqname objects associated with this name
 Args    : Optional arguments array:

    -force Normally the function will not execute if it has done so
           already - this means you do not need to worry about wasting
           CPU cycles by calling a method that might have been called
           already by another function. If you need to force a re-read
           of the class data, set -force to true.

 -keepbest Default undef. If true, then only the top scoring map(s)
           will be kept. The value that you pass is a percentage that
           will determine how closely matched a 'not-best' map should
           be to be kept. That is, if the best map has a score of 93,
           and you specify -keepbest 90, then any map with a score of
           at least (93 x 0.90) = 83.7 will be kept. Passing 0 will
           keep all maps, passing 100 will keep only the top score,
           and passing 101 will keep *nothing*.

    -clear Default 0. If true, then any old mappings will be
           deleted. This also implies a -force.

=cut

sub read_mappings {
    my $self = shift;
    my $args = $self->parseparams( -force  => 0,
                                   -keepbest => undef,
				   -clear  => 0,
				   @_ );
    if ($args->{CLEAR}) {
	$self->{MAPPINGS} = [];
	$self->task('read_mappings', 0);
    }
    return @{$self->{MAPPINGS}} if
	($self->task('read_mappings') && !$args->{FORCE});
    $self->benchstart;
    my $mt = $self->tracker;
    $self->death("You need to provide a MapTracker object to read mappings")
	unless ($mt);
    my $rng = $self->range;
    if ($args->{OVERLAP}) {
        $self->range( $args->{OVERLAP} ) unless ($rng);
    } elsif ($rng) {
        $args->{OVERLAP} = $rng;
    }
    my @maps = $mt->get_mappings( -name1 => $self, %{$args});
    if ($#maps > -1 && $args->{KEEPBEST}) {
        my @sorted = sort { $b->score <=> $a->score } @maps;
        @maps = ();
        my $limit = $sorted[0]->score * $args->{KEEPBEST} / 100;
        foreach my $map (@sorted) {
            last if ($map->score < $limit);
            push @maps, $map;
        }
    }
    
    $self->{MAPPINGS} ||= [];
    push @{$self->{MAPPINGS}} , @maps;
    # CIRCULAR REFERENCE :
    # $seqobj->{MAPPINGS}[0] = $mapobj; $mapobj->{SEQ1} = $seqobj;

    $self->task('read_mappings', 1);
    $self->benchend;
    return @maps;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 javascript_link

 Title   : javascript_link
 Usage   : my $html_anchor = $obj->javascript_link( $classes, $args, $taxicon )
 Function: Generates an <A> string for use with maptracker javascript
 Returns : A string
    Args : [0] Single string of HTML classes (optional)

=cut

sub javascript_link {
    my $self = shift;
    my ($userClass, $args, $taxicon) = @_;
    my @classes;
    push @classes, 'mtdep' if ($self->is_class('deprecated'));
    push @classes, $userClass if ($userClass);
    my $lnk = $self->SUPER::javascript_link
        (join(' ', @classes), 'node', $self->id, $self->name, $args);
    if ($taxicon) {
        my %imgs = map { $_->img_tag() => 1 } $self->each_taxa();
        $lnk = join('', sort keys %imgs) . $lnk;
    }
    return $lnk;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_javascript

 Title   : to_javascript
 Usage   : $obj->to_javascript()
 Function: Generates an ASCII string encoding a JavaScript object
 Returns : The string

 Args : If a defined value is passed to the method, then taxa, classes
        and lengths will be read in. If the value passed is an
        integer, then edges will be read, with the value of the
        integer specifying the limit (per edge type). Otherwise only
        data already recovered for the sequence will be included.

=cut

sub to_javascript {
    my $self = shift;
    my ($force) = @_;
    if (!$self->{JSTEXT} || defined $force) {
        my $name = $self->name;
        $name =~ s/\\/\\\\/g;
        $name =~ s/\'/\\\'/g;
        my $txt  = sprintf("type:'node', id:%d, name:'%s', ns:[%d]",
                           $self->id, $name, $self->namespace->id);

        if (defined $force) {
            $self->read_classes();
            $self->read_taxa();
            $self->read_lengths();
            if ($force =~ /^\d+/) {
                $self->read_edges( -limit => $force );
            } else {
                # At least get descriptions
                $self->desc();
            }
        }
        if ($self->task('read_classes')) {
            my @vals;
            foreach my $dat (values %{$self->{CLASSES}}) {
                my ($obj, $hash) = @{$dat};
                push @vals, sprintf("[ %d, [%s] ]", $obj->id , 
                                    join(",",sort {$a<=>$b} keys %{$hash}));
            }
            $txt .= sprintf(", 'class':[%s]", join(", ", @vals));
        }

        if ($self->task('read_taxa')) {
            my @vals;
            foreach my $dat (values %{$self->{TAXA}}) {
                my ($obj, $hash) = @{$dat};
                push @vals, sprintf("[ %d, [%s] ]", $obj->id , 
                                    join(",",sort {$a<=>$b} keys %{$hash}));
            }
            $txt .= sprintf(", taxa:[%s]", join(", ", @vals));
        }

        if ($self->task('read_lengths')) {
            my @vals;
            foreach my $len ( sort {$a <=> $b} keys %{$self->{LENGTHS}}) {
                push @vals, sprintf
                    ("[ %d, [%s] ]", $len, 
                     join(",",sort {$a<=>$b} keys %{$self->{LENGTHS}{$len}}));
            }
            $txt .= sprintf(", len:[%s]", join(", ", @vals));
        }

        if ($self->task('desc')) {
            $txt .= sprintf(", sf:[%s]", join(", ", map {$_->id} $self->desc));
        }
        $txt .= ", edgeDone:" . ($self->task('read_edges') ? 'true':'false');
        my $full = join(", ",map { "'$_':1" } sort keys %{$self->{FULLEDGE}});
        $txt .= ", fullEdge:{ $full }";
        my @byReads;
        my $hash = $self->edge_hash();
        foreach my $reads (sort keys %{$hash}) {
            my @edges;
            while (my ($oid, $arr) = each %{$hash->{$reads}}) {
                # We need to quote the edge_id to cover transient string IDs
                push @edges, map {sprintf("'%s':1", $_->id )} @{$arr};
            }
            push @byReads, sprintf("'%s':{ %s }", $reads, join(",", @edges));
        }
        if ($#byReads > -1 || $self->task('read_edges')) {
            $txt .= sprintf(", edges:{ %s }", join(", ", @byReads));
        }
        $self->{JSTEXT} = "{ $txt }";   
    }
    return $self->{JSTEXT};
}


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_text

 Title   : to_text
 Usage   : $obj->to_text($pad)
 Function: Generates an ASCII string describing the sequence
 Returns : The string
 Args    : Optional pad string (for left offset)

=cut

sub to_text {
    my $self = shift;
    $self->benchstart;
    my $mt = $self->tracker;
    my ($pad) = @_;
    $pad ||= "";
    my $string = "";
    $string .= sprintf("%s%s [ID %d]\n", $pad,$self->name, $self->id);

    my $chash = $self->each_class('hash name auth');
    my @cnames = sort { uc($a) cmp uc($b) } keys %{$chash};
    if ($#cnames > -1) {
	$string .= "$pad  Classified as:\n";
	foreach my $name (@cnames) {
            my @anames = sort { uc($a) cmp uc($b) } @{$chash->{$name}};
	    $string .= sprintf("$pad    %s [%s]\n", $name, join(", ",@anames));
	}
    }
    
    my $thash  = $self->each_taxa('hash');
    my @tids = sort { $a <=> $b } keys %{$thash};
    if ($#tids > -1) {
	$string .= "$pad  Assigned to Taxa:\n";
	foreach my $tid (@tids) {
            my $name = $thash->{$tid}{taxa}->name;
            my @anames = sort { uc($a) cmp uc($b) } map 
            { $_->name } @{$thash->{$tid}{auths}};
	    $string .= sprintf("$pad    %d %s [%s]\n", $tid, 
			       $name, join(", ", @anames));
	}
    }
    
    my $rels  = $self->edge_hash;
    my @reads = sort { uc($a) cmp uc($b) } keys %{$rels};
    if ($#reads > -1) {
	foreach my $read_as (@reads) {
	    $string .= sprintf("$pad  %s\n", $read_as);
            my @nodes = sort { uc($a->name) cmp uc($b->name) } map
            { $mt->get_seq($_) } keys %{$rels->{ $read_as }};
            foreach my $node (@nodes) {
                my @edges  = @{$rels->{ $read_as }{ $node->id }};
                my %ahash = map { $_ => 1 } map 
                { $_->each_authority_name } @edges;
                my @anames = sort { uc($a) cmp uc($b) } keys %ahash;
		$string .= sprintf("$pad    %s [%s]\n", 
				   $node->name,  join(", ", @anames));
            }
        }
    }
    
    my @maps = $self->each_mapping;
    if ($#maps > -1 ) {
	$string .= "$pad  Mapped locations:\n";
	my $nid = $self->id;
	foreach my $map (@maps) {
	    $string .= $map->to_text("$pad  ")."\n";
	}
    }
    
    $self->benchend;
    return $string;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_html

 Title   : to_html
 Usage   : $obj->to_html
 Function: Generates an ASCII string describing the sequence
 Returns : The string
 Args    : Optional arguments array:

  -showmap Default 'table', which will generate an HTML table with
           each map location indicated in full detail. If '0', then no
           map data will be shown.

       -fp BMS::FriendlyPanel object, required for showing maps as
           images.

 -linkargs Default "". Optional arguments to add to mapTracker.pl
           links.

 -usealias Default 1. If true, then include data from sequences that
           were identified as aliases.

=cut

sub to_html {
    my $self = shift;
    $self->benchstart;
    my $args = $self->parseparams
	( -showmap  => 'table',
	  -fp       => undef,
	  -linkargs => "",
	  -drawrel  => 1,
	  -usealias => 1,
          -display  => 'taxa edge class length map',
	  -decorate => 'is a shorter term for',
	  @_ );
    unless ($self->{DONE_HTML_STYLES}++) {
        print <<EOF;
<style>
.edgeInfo { float:left; width: 20em; clear:none; margin: 3px; padding: 1px; background-color: #ffd; }
</style>
EOF

    }
    my $mt      = $self->tracker;
    my $larg    = $args->{LINKARGS};
    my $help    = $mt->help;
    my $seqname = $self->name;
    my $sid     = $self->id;
    my $nsrow   = "";
    my $string  = "";
    my $ns = $self->namespace;
    if ($ns->id > 1) {
        my $nsn  = $ns->name;
        my $text = sprintf("%s <span class='nsdesc'>%s</span>", 
                           $ns->name, $ns->description);
        $nsrow = $self->_simple_row("Namespace:", $text);
        if ($nsn eq 'Smiles') {
            # Escape: % +
            my $urlName = $seqname;
            $urlName    =~ s/\%/\%25/g;
            $urlName    =~ s/\+/\%2B/g;
            $urlName    =~ s/\#/\%23/g;
            # $seqname   .= "<br /><img src='http://cheminfo.pri.bms.com:8080/cgi/moldy_na.cgi?smiles=$urlName' />";
            $seqname   .= "<br /><img src='http://research.pri.bms.com:8080/CSRS/services/lookup/image/SMILES/$urlName?param=w:350&param=h:150' />";
        }
    }

    $string .= $mt->html_css;
    $string .= sprintf
        ("<span class='sn'>%s</span> <span class='idc'>[%d]</span><br />\n",
         $seqname, $sid);

    my $tabrows = "";
    my $disp  = uc($args->{DISPLAY} || "");
    $tabrows .= $self->_taxa_html if ($disp =~ /TAX/ || $disp =~ /SPEC/);
    $tabrows .= $self->_length_html if ($disp =~ /LEN/);
    $tabrows .= $nsrow;
    $tabrows .= $self->_class_html(  ) if ($disp =~ /CLASS/);
    $tabrows .= $self->_edge_html( $args ) 
        if ($disp =~ /EDGE/ || $disp =~ /REL/);
    
    $string .= "<table>$tabrows</table>" if ($tabrows);
    $string .= $self->show_maps( @_ ) if ($disp =~ /MAP/);
    $self->benchend;
    return $string;
}



sub show_maps {
    my $self  = shift;
    my $args    = $self->parseparams( @_ );
    my $string  = "";
    my $nummaps = $self->map_count;
    if ($nummaps < 1) {
        if ($self->task('read_mappings') && $self->range) {
            $string .= sprintf
                ("No mappings were found in the range %d-%d. You may be able ".
                 "to find more data by expanding your range<br />\n",
                 @{$self->range});
        }
        return $string;
    }
    return $string . $self->draw_maps( @_ ) if ($args->{SHOWMAP} =~ /image/i);

    my $mt    = $self->tracker;
    my $help  = $mt->help;
    if (!$args->{SHOWMAP}) {
        # User does not want maps displayed - just make a note that there
        # are some in the database
	return sprintf
	    ("%s<span class='hidemap'>%d maps found (but not shown)".
             "</span><br />\n", $help->make_link(35),
	     $nummaps, $nummaps == 1 ? "" : "s");
    }

    my @subjects = $self->each_mapping_by_subject;
    if ($args->{SHOWMAP} =~ /full/i) {
        # Show full information about each map
	$string .= "<table border='1'><tr><th colspan='5' bgcolor='tan'>";
	$string .= "Mapped Locations</th></tr>\n";
	foreach my $odat (@subjects) {
            my ($oseq, $maps) = @{$odat};
	    foreach my $map (@{$maps}) {
		$string .= $map->to_html( -table => 0);
	    }
	}
	$string .= "</table>\n";
        return $string;
    }

    # Display a table summarizing the maps:

    my $larg       = $args->{LINKARGS};
    my $numsubs    = $#subjects + 1;
    my $isintegral = ($args->{SHOWMAP} =~ /integ/i) ? 1 : 0;

    unless ($isintegral) {
        $string .= "<table border='1'><tr><th colspan='11' bgcolor='tan'>";
        $string .= sprintf("Summary of %d Mapped Subject%s</th></tr>\n",
                           $numsubs, $numsubs == 1 ? "" : "s");
        $string .= $subjects[0][1][0]->html_row_header(  );
    }
    foreach my $odat (@subjects) {
        my ($oseq, $maps) = @{$odat};
        my $oname = $oseq->name;
        my $highlight = 0;
        if ($#{$maps} > 0) {
            # Are there disagreements?
            # Check to see if all the mappings for a given seqname
            # have the same coordinates - if not, specify a warning
            # Highlight
            my (%qlocs, %slocs);
            foreach my $map (@{$maps}) {
                my (@stag, @qtag);
                foreach my $loc ($map->locations_for_seq( $self )) {
                    push @qtag, ($loc->[0], $loc->[1]);
                    push @stag, ($loc->[2], $loc->[3]);
                }
                $qlocs{ join("\t", @qtag) }++;
                $slocs{ join("\t", @stag) }++;
            }
            my @quniq = keys %qlocs;
            my @suniq = keys %slocs;
            if ($#quniq > 0) {
                $highlight ||= {};
                $highlight->{'Query Range'} = 'yellow';
            }
            if ($#suniq > 0) {
                $highlight ||= {};
                $highlight->{'Subject Range'} = 'yellow';
            }
        }
        for my $i (0..$#{$maps}) {
            my $map = $maps->[$i];
            $string .= $map->to_html_row( -linkargs  => $larg,
                                          -ingroup   => $#{$maps}+1,
                                          -isfirst   => !$i,
                                          -query     => $self,
                                          -addquery  => $isintegral,
                                          -highlight => $highlight, );
        }
    }
    $string .= "</table>\n" unless ($isintegral);
    return $string;
}

sub draw_maps {
    my $self = shift;
    $self->benchstart;
    my $args = $self->parseparams(  -fp       => undef,
				    @_ );
    my $string = "";
    my $bs     = $self->as_bioseq( @_ );

    # Add zoom and pan information:
    my $offset = $bs->{SEQ_OFFSET} || 0;
    my $numblocks = 20;
    my $scale  = $bs->{SEQ_SCALE}  || 1;
    my $len    = $bs->length * $scale;
    my $offend = $offset + $len;
    my $width  = int($len / $numblocks) || 1;
    my $half   = int($width / 2);
    my $name   = $self->name;
    my $start  = $offset + 1;
    for (my $start = $offset + 1; $start < $offend; $start += $width) {
        my $end = $start  + $width - 1;
        $end = $offend if ($end > $offend);
        my $feat = Bio::SeqFeature::Generic->new
            ( -primary => "Zoom",
              -start   => ($start / $scale) - $offset, 
              -end     => ($end   / $scale) - $offset, );

        my $center = $start + $half;
        my $text ="<ul>";
        foreach my $zoom (10, 5, 2, 1, 0.5, 0.2, 0.1 ) {
            my $newlen = $len / $zoom;
            next if ( $newlen < 10);
            $text .= " <li>";
            $newlen /= 2;
            my $newstart = $center - $newlen;
            $newstart    = 1 if ($newstart < 1);
            my $nn = sprintf("<a href='mapBrowser.pl?seqname=%s:%d-%d'>",
                             $name,$newstart,$center+$newlen);
            if ($zoom eq "1") {
                $text .= $nn . "Recenter</a>";
            } else {
                $text .= "Zoom " . (($zoom < 1) ? 'out' : 'in') . " $nn";
                $text .= "x $zoom</a>";
            }
            $text .= "</li>\n";
        }
        $text .= "</ul>\n";
        $feat->{__HTML__} = [ $text ];
        $feat->{__HTML_TITLE__} = "Zoom or Recenter:";
        $bs->add_SeqFeature($feat);
    }
    
    $string   .= $args->{FP}->insertImage( -bioseq => $bs,
                                           -fh     => 0, );
    $self->benchend;
    return $string;
}

sub range {
    my $self = shift;
    if ($_[0]) {
        # Allow [ $start, $stop], ($start, $stop) or "$start,$stop"
        $self->{RANGE} = ref($_[0]) ? 
            $_[0] : $_[1] ? [$_[0],$_[1]] : [ split(/[\r\n\s\,]+/, $_[0])];
    }
    # Allow returning of ($start, $stop) or [ $start, $stop ];
    return wantarray ? ($self->{RANGE} ? @{$self->{RANGE}} : undef)
        : $self->{RANGE};
}

sub as_bioseq {
    my $self = shift;
    $self->benchstart;
    my $args = $self->parseparams( -offset   => 0,
				   -center   => 0,
				   -maxlen   => 500000,
				   -scale    => 0,
				   @_ );
    my $maxlen = $args->{MAXLEN};
    my $off    = $args->{OFFSET};
    my $offend;
    my $maxseqlen = $self->max_length;
    if (my $rng = $self->range) {
        ($off, $offend) = ($rng->[0]-1, $rng->[1]) if ($rng->[0] && $rng->[1]);
        $offend = $maxseqlen if (defined $maxseqlen && $offend > $maxseqlen);
    } elsif (defined $maxseqlen) {
        $offend = $maxseqlen;
    }
    my @feats = $self->each_map_feat( -fphtml => 1,
				      -offset => $off, 
				      -scale  => $args->{SCALE});

    if ($offend) {
	# do not need to do anything, range defined
    } else {
	# We need to define the coordinate range by looking at the feats
	my ($min, $max) = (9999999999999, 0);
	my $cg = 0; # center of gravity
	foreach my $feat (@feats) {
	    my ($start, $end) = ($feat->start, $feat->end);
	    my $flen = $end - $start + 1;
#	next if ($flen > 1000000);
	    $max = $end if ($max < $end);
	    $min = $start if ($min > $start);
	    $cg += ($start + ($end-$start)/2);
	}
	# warn "$min-$max ($off - $offend)\n";
	if ($max > $maxlen) {
	    # Hmm, this is a big sequence
	    if ($max - $min < $maxlen) {
		# But the region with data is reasonably sized
		$min = int($min / 1000) * 1000;
		return $self->as_bioseq( @_, -offset => $min );
	    }
	    my $scale  = $max / 10000;
	    my $lscale =int(log($scale)/log(10));
	    $lscale = 10 ** $lscale;
	    $scale = (int($scale/$lscale) + 1) * $lscale;
	    # warn "scaling by $scale";
	    $self->benchend;
	    return $self->as_bioseq( @_, -scale => $scale );
	}
	$offend = $max;
    }

    my $bs = Bio::Seq::RichSeq->new
	( -desc       => "Maximal length defined by maps", );
    $bs->alphabet('protein') if ($self->is_class('protein'));
    if ($off) {
	$bs->{SEQ_OFFSET} = $off;
    }
    
    if ($args->{SCALE} && $args->{SCALE} != 1) {
	$bs->{SEQ_SCALE} = $args->{SCALE};
    }
    my $name = $self->name;
    $bs->display_id($name);
    
    # Calculate length of *displayed* sequence
    my $len = $offend - $off; # Offset adjusted in each_map_feat
    $len = 2 if ($len < 2);
    $bs->seq('N' x $len);

    my $seqlen = $self->max_length || $offend;
    my $basefeat = Bio::SeqFeature::Generic->new
	( -primary => "Query",
	  -start => 1 - $off, -end => $seqlen - $off);
    $basefeat->{__HTML__} = "This feature indicates the context in which all other features are presented. It is the 'base' on which the other features are displayed. The coordinates displayed along the bottom axis are for <em>this</em> sequence.";
    $basefeat->{__HTML_TITLE__} = $name;
    $basefeat->add_tag_value ('name', $name);
    $bs->add_SeqFeature($basefeat);
    #@feats = sort { $b->score || 0 <=> $a->score || 0 } @feats;

    foreach my $feat (@feats) {
	$bs->add_SeqFeature($feat);
    }
    $self->benchend;
    return $bs;
}


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub authority_html {
    my $self = shift;
    my $mt   = $self->tracker;
    my ($list, $sep) = @_;
    $sep ||= ", ";
    my @auths    = map { $mt->get_authority($_) } @{$list};
    my @autnames = sort { uc($a) cmp uc($b) } map {$_->name} @auths;
    my $txt = sprintf("<span class='auth'>[%s]</span>",
		      join($sep, @autnames));
    return $txt;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# Turns a hash ref keyed to authority IDs into a list of authority names

sub _authority_list {
    my $self = shift;
    my ($hash) = @_;
    my $adat  = $self->{AUTHORITIES};
    my @autnames = sort map { $adat->{$_}->name } keys %{$hash};
    return @autnames;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# 

sub _alists2html {
    my $self = shift;
    my ($list, $sep) = @_;
    $sep ||= ", ";
    my %seen;
    my @parts;
    for my $i (0..$#{$list}) {
	my @subparts;
	foreach my $sp (@{$list->[$i]}) {
	    push @subparts, $sp unless ($seen{$sp});
	    $seen{$sp} = 1;
	}
	my $part = join(", ", @subparts);
	next unless $part;
	push @parts, $part;
    }
    return "<span class='alias'>[".join($sep, @parts)."]</span>";
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub is_taxa {
    my $self = shift;
    my ($taxa) = @_;
    if (my $rt = ref($taxa)) {
        if ($rt eq 'ARRAY') {
            return -1 if ($#{$taxa} < 0);
            $taxa = { map { $_ => 1 } @{$taxa} };
        } elsif ($rt eq 'BMS::MapTracker::Taxa') {
            $taxa = { $taxa->id => 1 };
        }
    } else {
        $taxa = {};
        foreach my $req (@_) {
            foreach my $tobj ($self->tracker->get_taxa( $req )) {
                $taxa->{ $tobj->id } = 1;
            }
        }
    }
    my @taxids = map { $_->id } $self->each_taxa;
    # There are no taxa entries at all - return -1
    return -1 if ($#taxids < 0);
    my $keep = 0;
    foreach my $id (@taxids) {
	# The taxa matches, return 1
	return 1 if ($taxa->{$id});
    }
    # There were entries, but none matched the request, return 0
    return 0;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub mt_url {
    my $self = shift;
    my ($largs) = @_;
    $largs .= "&" if ($largs);
    return sprintf("<a href='mapTracker.pl?%sseqname=%s'>\%s</a>",
		   $largs, $self->name, $self->name);
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub _length_html {
    my $self = shift;
    my $string = "";
    # If we did not look for lengths, do not note anything:
    return $string unless ( $self->task('read_lengths') );
    my $lens = $self->each_length;
    my @nums = sort {$a <=> $b } keys %{$lens};
    return "" if ($#nums < 0);
    foreach my $len (@nums) {
        my @alinks = map { $_->javascript_link() } @{$lens->{$len}};
        $string .= sprintf("%s [%s] ", $len, join(", ", @alinks));
    }
    return $self->_simple_row("Possible lengths:", $string);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub _taxa_html {
    my $self = shift;
    # If we did not look for taxa, do not note anything:
    return "" unless ( $self->task('read_taxa') );

    my $mt     = $self->tracker;
    my @cells  = ();
    my $thash  = $self->each_taxa('hash');
    my @tids   = sort { $a <=> $b } keys %{$thash};
    foreach my $tid (@tids) {
        my $tax    = $thash->{$tid}{taxa};
        my $text   = $tax->javascript_link();
        my @alinks = map { $_->javascript_link() } 
        @{$thash->{$tid}{auths}};
        $text .= " [".join(", ", @alinks)."]";
	push @cells, $text;
    }
    my $string = join(", ", @cells);
    return $string ? $self->_simple_row("Assigned Taxa:", $string) : "";
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub _class_html {
    my $self = shift;
    my $sid  = $self->id;
    # If we did not look for classes, do not note anything:
    return "" unless ( $self->task('read_classes') );
    my @classes = sort { uc($a->name) cmp uc($b->name) } $self->each_class();
    my $classDat = $self->each_class( 'hash' );
    my @cells = ();
    foreach my $class (@classes) {
        my $text = $class->javascript_link();
        my @alinks = map { $_->javascript_link() } 
        @{$classDat->{$class->id}{auths}};
        $text .= " [".join(", ", @alinks)."]";
	push @cells, $text;
    }
    my $string = join(", ", @cells);
    return $string ? $self->_simple_row("Classified as:", $string) : "";
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub _edge_html {
    my $self = shift;
    my ($args) = @_;
    # If we did not look for realtions, do not note anything:
    return "" unless ( $self->task('read_edges') );
    $self->benchstart;

    my $string = "";
    my $sid    = $self->id;
    my $mt     = $self->tracker;
    my $rels   = $self->edge_hash;
    my @reads  = sort { uc($a) cmp uc($b) } keys %{$rels};
    my $dec    = $args->{DECORATE};
    my @dreads;
    if ($dec) {
        # The user wants to decorate
        my ($type, $dir) = $mt->get_type($dec);
        @dreads = $dir ? $type->reads($dir) : ($type->reads);
    }

    my $cols = 6;
    my $long_text = {
        'has comment' => 1,
        
    };
    my $unitsize = 20;

    foreach my $read_as (@reads) {
        $string .= "\n<!-- Start $read_as Edges -->\n";
        my @nodes = sort { uc($a->name) cmp uc($b->name) } map
        { $mt->get_seq($_) } keys %{$rels->{ $read_as }};
        my $num = 0;
        my @cells;
	foreach my $other ( @nodes ) {
            my $oname = $other->name;
            my $oid   = $other->id;
            my @edges = @{$rels->{ $read_as }{ $oid }};
            my $desc  = $other->desc;
            map { $_->read_tags } @edges;
            my @stuff = map { $_->javascript_link('', $other,1) } @edges;
            if ($desc) {
                $desc = $desc->name;
                $desc = substr($desc, 0, 50) . "..." if (length($desc) > 40);
                push @stuff, $desc;
            }
            
	    push @cells, join("<br />", @stuff);
            $num += $#edges + 1;
	}
        my @ids = map {$_->id} @nodes;
        my $desc = "$read_as:<br /></b><span class='smaller'>";
        $desc .= sprintf("%d entr%s found", $num,
                         $num == 1 ? "y" : "ies");
        unless ($self->{FULLEDGE}{$read_as} || $self->{FULLEDGE}{all}) {
            $desc .=
                "<br /><span class='warn'>More edges exist than shown</span>";
        }
        $desc .= "</span><b>";
        my $tabstr = "";
        foreach my $cell (@cells) {
            $tabstr .= "<div class='edgeInfo'>$cell</div>\n";
        }
 
        #my $tabstr = "<table><tbody>\n";
        #my $mod = 8;
        #for (my $i = 0; $i <= $#cells; $i++) {
        #    $tabstr .= "  <tr>" unless ($i % $mod);
        #    $tabstr .= "    <td valign='top' NOWRAP>$cells[$i]</td>\n";
        #    $tabstr .= "</tr>\n" unless (($i+1) % $mod);
        #}
        #$tabstr .= "</tbody>\n</table>";
	
        $string .= $self->_simple_row($desc, $tabstr);
        $string .= "<!-- End $read_as Edges -->\n\n";
    }
    $self->benchstop;
    return $string;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub _simple_row {
    my $self = shift;
    my ($head, $text) = @_;
    return sprintf("<tr><td valign='top' align='right' nowrap='1'>".
                   "<b>%s</b></td><td>%s</td></tr>\n", $head, $text);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub to_excel {
    my $self = shift;
    my ($eh, $type) = @_;
    $self->_class_excel($eh, $type);
    $self->_taxa_excel($eh, $type);
    $self->_length_excel($eh, $type);
    $self->_edge_excel($eh, $type);
    $self->_map_excel($eh, $type);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub _class_excel {
    my $self = shift;
    my ($eh, $type) = @_;
    my $chash = $self->each_class('hash name auth');
    my @names = sort {uc($a) cmp uc($b) } keys %{$chash};
    return if ($#names < 0);

    # Initialize the sheet if needed
    unless ($eh->has_sheet('Class Assignment')) {
        $eh->sheet
            ( -name   => 'Class Assignment',
              -freeze => 1,
              -cols   => [ 'Name', 'Provenance', 'Class', 'Authorities' ],
              );
    }
    my $name = $self->name;
    foreach my $cname (@names) {
        my @anames = sort { uc($a) cmp uc($b) } @{$chash->{$cname}};
        $eh->add_row('Class Assignment', 
                     [ $name, $type, $cname, join(', ', @anames)]);
    }
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub _taxa_excel {
    my $self = shift;
    my ($eh, $type) = @_;
    my $thash = $self->each_taxa('hash');
    my @tids  = sort { $a <=> $b } keys %{$thash};
    return if ($#tids < 0);

    # Initialize the sheet if needed
    unless ($eh->has_sheet('Species Assignment')) {
        $eh->sheet
            ( -name   => 'Species Assignment',
              -freeze => 1,
              -cols   => [ 'Name', 'Provenance', 'Taxa ID', 'Species Name', 
                           'Authorities' ],
              );
    }
    my $name = $self->name;
    foreach my $tid (@tids) {
        my @auths = map { $_->name } @{$thash->{$tid}{auths}};
        my $tname = $thash->{$tid}{taxa}->name;
        $eh->add_row('Species Assignment', 
                     [ $name, $type, $tid, $tname, join(', ', @auths)]);
    }
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub _length_excel {
    my $self = shift;
    my ($eh, $type) = @_;
    my @lens = keys %{$self->{LENGTH}};
    return if ($#lens < 0);

    # Initialize the sheet if needed
    unless ($eh->has_sheet('Lengths')) {
        $eh->sheet
            ( -name   => 'Lengths',
              -freeze => 1,
              -cols   => [ 'Name', 'Provenance', 'Length', 'Authorities' ],
              );
    }
    my $name = $self->name;
    foreach my $len (@lens) {
        my @auths = sort map {$_->name} values %{$self->{LENGTH}{$len}};
        $eh->add_row('Lengths', 
                     [ $name, $type, $len, join(', ', @auths)]);
    }
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub _edge_excel {
    my $self = shift;
    my ($eh, $type) = @_;
    my @reads = keys %{$self->{RELATED}};
    return if ($#reads < 0);

    my $mt = $self->tracker;
    # Initialize the sheet if needed
    unless ($eh->has_sheet('Relationships')) {
        $eh->sheet
            ( -name   => 'Relationships',
              -freeze => 1,
              -cols   => [ 'Name', 'Connected With', 'Other Name','Provenance',
                           'Authorities', 'Edge Token',  ],
              );
    }
    my $name = $self->name;
    foreach my $edge ($self->each_edge) {
        my ($reads, $refName, $other) = $edge->reads($self);
        my @auths = $edge->each_authority_name;
        my $token = $edge->type->name;
        $eh->add_row('Relationships', 
                     [ $name, $reads, $other->name, $type, 
                       join(', ', @auths), $token ]);
        
    }
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub _map_excel {
    my $self = shift;
    my ($eh, $type) = @_;
    return  if ($self->map_count < 1);

    # Initialize the sheets if needed
    unless ($eh->has_sheet('Map Overview')) {
        my $ws = $eh->sheet
            ( -name   => 'Map Overview',
              -freeze => 1,
              -cols   => [ 'Query', 'Provenance', 'Subject', 'Subject Class', 'Score', 'Strand', 'Query Coords', 'Subject Coords' ],
              );
        my $cen = $eh->format('center');
        $ws->set_column(4,7,undef, $cen);
        $ws->freeze_panes(1,1);
    }
    
    unless ($eh->has_sheet('Map Detail')) {
        my $ws = $eh->sheet
            ( -name   => 'Map Detail',
              -freeze => 1,
              -cols   => [ 'Query', 'Provenance', 'Subject', 'Subject Class', 'Score', 'Strand', 'HSP Count', 'Query Start', 'Query End', 'Query Span', 'Subject Start', 'Subject End', 'Subject Span', 'Authority', 'Transform', 'Map ID', 'Search Database' ],
              );
        my $cen = $eh->format('center');
        $ws->set_column(4,12,undef, $cen);
        $ws->freeze_panes(1,1);
    }
    
    foreach my $map ($self->each_mapping) {
        $map->to_excel( $self, $eh, $type);
    }
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
1;
