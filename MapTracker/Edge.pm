# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::Edge;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

BEGIN {
}

$BMS::MapTracker::Edge::VERSION = 
    ' $Id$ ';

use strict;
use vars qw(@ISA);
use BMS::Branch;
use BMS::MapTracker::Shared;

use Scalar::Util qw(weaken);
@ISA = qw(BMS::MapTracker::Shared);

my $debug = BMS::Branch->new
    ( -skipkey => ['TRACKER', 'TYPES','-tracker','CHILDREN', 'BENCHMARKS',
                   '__HTML__', 'factory', 'CLASSES', 'AUTHORITIES'],
      -noredundancy => 1,
      -format => 'html', );



=head1 PRIMARY METHODS

=head2 new

 Title   : new
 Usage   : my $obj = BMS::MapTracker::Edge->new(@arguments)
 Function: Creates a new object and returns a blessed reference to it.
 Returns : A blessed BMS::MapTracker::Edge object
 Args    : Associative array of arguments. Recognized keys [Default]:

       -id The database ID for this edge

    -name1 The first node in the edge (alias -left)

    -name2 The seconde node (alias -right)

    -names Alternative to -name1 and -name2, should be an array
           reference with two entries.

     -type The Relationship connecting name1 and name2. Please note
           that if you specify a relationship by the reads_backwards
           string, then internally the object will swap name1 and name2.

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = { };
    bless ($self, $class);
    my $args = $self->parseparams( -id    => 0,
				   -name1 => undef,
				   -name2 => undef,
				   @_ );

    my $mt   = $args->{TRACKER};
    my $treq = $args->{TYPE}  || $args->{READS}     || $args->{REALTION};
    my $sreq = $args->{SPACE} || $args->{EDGESPACE} || $args->{NAMESPACE};
    my $created = $args->{CREATED};
    my ($type, $dir) = $mt->get_type( $treq );
    my ($n1r, $n2r);
    if (my $names = $args->{NAMES}) {
        ($n1r, $n2r) = @{$names};
    } else {
        ($n1r, $n2r) = ( $args->{NAME1} || $args->{NODE1} || $args->{LEFT},
                         $args->{NAME2} || $args->{NODE2} || $args->{RIGHT});
    }
    my $space = $mt->get_namespace( $sreq );
    my ($n1, $n2, @errs);
    my @n1s = $mt->get_seq( $n1r );
    my @n2s = $mt->get_seq( $n2r );
    if ($#n1s == 0) {
        $n1 = $n1s[0];
    } elsif ($#n1s > 0) {
        push @errs, sprintf("%d possible node1 for '%s'", $#n1s+1, $n1r);
    } else {
        $n1r ||= '-undef-';
        push @errs, "No such node1 '$n1r'";
    }
    if ($#n2s == 0) {
        $n2 = $n2s[0];
    } elsif ($#n2s > 0) {
        push @errs, sprintf("%d possible node2 for '%s'", $#n2s+1, $n2r);
    } else {
        $n2r ||= '-undef-';
        push @errs, "No such node2 '$n2r'";
    }
    push @errs, "Unknown type '$treq'" unless ($type);
    push @errs, "Unknown namespace '$sreq'" unless ($space);
    if ($#errs > -1) {
        my $msg = "Failed to generate edge:\n" .join('',map {"  $_\n"} @errs);
        foreach my $tag (sort keys %{$args}) {
            my $val = $args->{$tag};
            if (ref($val) && $val =~ /^BMS::MapTracker/) {
                my $text = "$val";
                $text .= " '". $val->name ."'" if ($val->can('name'));
                $text .= " [". $val->id ."]" if ($val->can('id'));
                $val = $text;
            }
            $msg .= sprintf("  -%s => '%s'\n", lc($tag), defined $val ?
                            $val : '-undef-');
        }
        $self->error($msg);
        return undef;
    }
    if ($dir < 0) {
        ($n1, $n2) = ($n2, $n1);
    }
    $self->{ID}    = $args->{ID};
    # The third entry (index 2) is also accessible by index -1.
    # -1 is used to indicate a node not in the edge, so accessing either
    # the NODES or NIDS array with -1 returns undef or 0, respectively

    # STORING NODES IS CAUSING PERSISTANT MEMORY LOOP STRUCTURES
    # $edge->{NODES}[0] = $node + $node->{EDGES}{$eid} = $edge

    # $self->{NODES}  = [ $n1, $n2, undef ];
    $self->{NIDS}   = [ $n1->id, $n2->id, 0 ];
    $self->{TYPE}   = $type;
    $self->{SPACE}  = $space;
    $self->{LIVE}   = (defined $args->{LIVE} && $args->{LIVE} eq 't') ? 1 : 0;
    $self->{CREATE} = $created;
    $self->tracker( $mt );
    # Make sure the node objects are aware of this edge (need only add to one):
    $n1->add_edge($self);
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

=head2 space

 Title   : space
 Usage   : my $edgespace = $obj->space
 Function: Gets the edgespace for this node
 Returns : The BMS::MapTracker::Namespace object
 Args    : 

=cut

*namespace = \&space;
sub space {
    my $self = shift;
    return $self->{SPACE};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 created

 Title   : created
 Usage   : my $date = $obj->created
 Function: Gets the date the edge was created
 Returns : A string
 Args    : 

=cut

sub created {
    my $self = shift;
    return $self->{CREATE} || "";
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 is_live

 Title   : is_live
 Usage   : my $one_or_zero = $obj->is_live
 Function: Indicates if the edge is live or not
 Returns : Zero or one
 Args    : 

=cut

sub is_live {
    my $self = shift;
    return $self->{LIVE};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 type

 Title   : type
 Usage   : my $type = $obj->type
 Function: Gets the Relationship object
 Returns : The Relationship object
 Args    : 

=cut

*relationship = \&type;
sub type {
    my $self = shift;
    return $self->{TYPE};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 nodes

 Title   : nodes
 Usage   : my ($node1, $node2) = $obj->nodes( $optional_reference_node )

 Function: Gets the two nodes associated with this edge, or the other
           node of the pair if you provide a reference

 Returns : An array of two BMS::MapTracker::Seqname objects if no
           reference is provided, otherwise an array of one node. The
           method uses wantarray - if called in a scalar context only
           one node will be returned.

    Args : Optional reference node - if a node from the edge is
           provided, the other node is returned. If the reference node
           is NOT part of this edge, then undef will be returned.

=cut

*node = \&nodes;
*seqs = \&nodes;
sub nodes {
    my $self = shift;
    my ($ref) = @_;
    my @nids = ($self->{NIDS}[0], $self->{NIDS}[1] );
    if ($ref) {
        my $index = $self->node_index( $ref );
        return undef if ($index < 0);
        @nids = ( $self->{NIDS}[ !$index ] );
    }
    my @retval = map { $self->tracker->get_seq_by_id( $_ ) } @nids;
    return wantarray ? @retval : $retval[0];
}

*seq1 = \&node1;
sub node1 {
    my $self = shift;
    return $self->tracker->get_seq_by_id( $self->{NIDS}[0] );
}

*seq2 = \&node2;
sub node2 {
    my $self = shift;
    return $self->tracker->get_seq_by_id( $self->{NIDS}[1] );
}

*other_seq = \&other_node;
sub other_node {
    my $self = shift;
    my ($request) = @_;
    my $ri = $self->node_index( $request );
    return undef if ($ri < 0);
    return $self->tracker->get_seq_by_id( $self->{NIDS}[ !$ri ] );
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 node_index

 Title   : node_index
 Usage   : my $int = $obj->node_index( $node )
 Function: Indicates which 'side' of the relationship a node is on
 Returns : 0 for the left, 1 for the right and -1 if node not in edge
 Args    : A node request (name, object or database id)

=cut

sub node_index {
    my $self = shift;
    my ($node_request) = @_;
    if (my @nodes = $self->tracker->get_seq( $node_request)) {
        my %nhash = map { $_->id => 1 } @nodes;
        # $debug->branch( [ $self->{NIDS}, \%nhash ]);
        for my $i (0..1) {
            return $i if ($nhash{ $self->{NIDS}[$i] });
        }
    }
    # Either node is not in database, or not in edge
    return -1;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 reads

 Title   : reads
 Usage   : my $reads = $obj->reads( $optional_reference_node )
 Function: Gets the 'reads as' string for the edge

 Returns : In array context, ($readsString, $ref_node, $other_node) -
           In scalar context, just the reads string

    Args : Optional reference node. The default is the 'left' node. If
           you provide the other node as reference, then the
           "reads_backwards" string will be provided.

=cut

sub reads {
    my $self = shift;
    my ($node_request) = @_;
    my $index = 0;
    if ($node_request) {
        if ($node_request eq '-1') {
            $index = 1;
        } else {
            $index = $self->node_index( $node_request);
            return wantarray ? ("", undef, undef) : "" if ($index < 0);
        }
    }
    my $type   = $self->type;
    my $reads  = $index ? $type->reads('rev') : $type->reads('for');
    return $reads unless (wantarray);
    my $ref   = $self->tracker->get_seq_by_id( $self->{NIDS}[ $index] );
    my $other = $self->tracker->get_seq_by_id( $self->{NIDS}[!$index] );
    return ($reads, $ref, $other);
}

# Designed to return both the forward and reverse reads string for
# edges where node1 and node2 are identical

sub reads_careful {
    my $self = shift;
    if ($self->{NIDS}[0] == $self->{NIDS}[1]) {
        # The two nodes are the same, so you could read this edge in
        # either direction
        my @reads = $self->type->reads;
        my %nonredun = map { $_ => 1 } @reads;
        return sort keys %nonredun;
    } else {
        my $read = $self->reads( @_ );
        return ( $read );
    }
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 history

 Title   : history
 Usage   : my $history = $obj->history( $optional_author )
 Function: Gets the update history for the edge

 Returns : A hash keyed to authority ID, pointing to an array of
           history events. Each event is an array of a time plus a
           'live' flag. If the flag is 'f', the event was a deletion
           of the edge for that authority, otherwise it is an update
           of edge data.

    Args : Optional authority. If an authority request is provided,
           then only the history for that authority will be
           returned.

=cut

sub history {
    my $self = shift;
    my ($authreq) = @_;
    unless ($self->{HISTORY}) {
        my $mt   = $self->tracker;
        my $eid  = $self->id;
        my $rows = $mt->dbi->named_sth("Get edge history")->
            selectall_arrayref( $eid );
        $self->{HISTORY}  = {};
        $self->{LIVEAUTH} = {};
        $self->{DEADAUTH} = {};
        foreach my $row (@{$rows}) {
            my ($aid, $dates, $live)  = @{$row};
            my $auth = $mt->get_authority($aid);
            my $key  = $live ? 'LIVE' : 'DEAD';
            $self->{$key.'AUTH'}{ $aid } = $auth;
            if (ref($dates)) {
                # Postgres adaptor is returning a native Perl Array
                $self->{HISTORY}{$aid} = $dates;
            } elsif ($dates =~ /^\{\"(.+)\"\}$/) {
                # Adaptor is returning a stringified version of array
                my @dts  = split(/\"\,\"/, $1);
                $self->{HISTORY}{$aid} = \@dts;
            } else {
                die "Error parsing history dates '$dates' for edge_id = $eid GOT:\n  '$dates'\n  ";
            }
        }
    }
    if ($authreq) {
        my $auth = $self->tracker->get_authority($authreq);
        if ($auth) {
            return (exists $self->{HISTORY}{$auth->id}) ?
                @{$self->{HISTORY}{$auth->id}} : ();
        } else {
            $self->error
                ("Could not provide history for unkown authority '$authreq'");
            return ();
        }
    }
    return %{$self->{HISTORY}};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 each_authority

 Title   : each_authority
 Usage   : my @auth_objects = $obj->each_authority
 Function: Gets a list of all authority objects CURRENTLY live on the edge
 Returns : An array
 Args    : 

 Title   : each_authority_name
 Function: As above, but returns an array of strings

=cut

sub each_authority {
    my $self = shift;
    unless ($self->{LIVEAUTHS}) {
        if ($self->{LIVEAUTH}) {
            # Live authorities are available from a prior history call
            $self->{LIVEAUTHS} = [ values %{$self->{'LIVEAUTH'}} ];
        } else {
            # Save the time of calling a full history, and just query
            # authorities directly.
            my $mt   = $self->tracker;
            my $get  = $mt->dbi->named_sth("Live authorities for edge");
            my @aids = $get->get_array_for_field( $self->id );
            $self->{LIVEAUTHS} = [ map { $mt->get_authority($_) } @aids ];
        }
    }
    return @{$self->{LIVEAUTHS}};
}

sub each_authority_name {
    my $self = shift;
    return map {$_->name } $self->each_authority;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 dead_authorities

 Title   : dead_authorities
 Usage   : my @authobjs = $obj->dead_authorities

 Function: Like each_authority(), but returns only those authorities
           that once touched the edge but are now listing the edge as
           deleted.

 Returns : An array of authority objects
 Args    : 

 Title   : dead_authority_names
 Function: As above, but returns an array of strings

=cut


sub dead_authorities {
    my $self = shift;
    $self->history unless ($self->{DEADAUTH});
    return values %{$self->{DEADAUTH}};
}

sub dead_authority_names {
    my $self = shift;
    return map {$_->name } $self->dead_authorities;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 all_authorities

 Title   : all_authorities
 Usage   : my @authobjs = $obj->all_authorities
 Function: Returns the combined output of each_authority and dead_authorities
 Returns : An array of authority objects
 Args    : 

 Title   : all_authority_names
 Function: As above, but returns an array of strings

=cut


sub all_authorities {
    my $self = shift;
    return ($self->each_authority, $self->dead_authorities);
}

sub all_authority_names {
    my $self = shift;
    return map {$_->name } $self->all_authorities;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 hash_key

 Title   : hash_key
 Usage   : my $key = $obj->hash_key
 Function: Gets a hash key describing the edge

 Returns : A string, which will be 3 integers (node1 name_id, node2
           name_id, type_id) joined by tabs. Note that the namespace
           is not included in this key - this is to allow edges from
           different namespaces to be combined together.

    Args : 

=cut

sub hash_key {
    my $self = shift;
    unless ($self->{HASH_KEY}) {
        my @nids = map {$_->id} $self->nodes;
        $self->{HASH_KEY} = join("\t", @nids, $self->type->id);
    }
    return $self->{HASH_KEY};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 full_hash_key

 Title   : full_hash_key
 Usage   : my $key = $obj->full_hash_key
 Function: Gets a full_hash key describing the edge

 Returns : As above, but will also include the namespace id at the end
           of the key.
    Args : 

=cut

sub full_hash_key {
    my $self = shift;
    return $self->{FULL_KEY} ||= $self->hash_key . "\t" . $self->space->id;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 Convienence calls utilizing TagCollection

    The following methods are actually executed within the
    TagCollection object contained by this Edge. They are provided
    here for convienence. If you wish to access the TagCollection
    object directly, you may recover it with the read_tags() method
    (or the alias tag_collection()).

   add_tag()

   each_tag()

   each_tag_name ()

=cut

sub add_tag {
    my $self = shift;
    $self->{TO_TEXT} = $self->{TO_HTML} = "";
    return $self->read_tags->add_tag( @_ );
}

sub each_tag {
    return shift->read_tags->each_tag( @_ );
}

sub each_tag_name {
    return shift->read_tags->each_tag_name( @_ );
}

sub tags_as_hash {
    return shift->read_tags->tags_as_hash( @_ );
}

sub has_tag {
    my $self = shift;
    my ($tagname, $tagval, $auth) = @_;
    $auth = $self->tracker->get_authority($auth) if ($auth);
    my @matches;
    foreach my $tag ($self->each_tag($tagname)) {
        if ($tagval && $tag->valname ne $tagval) {
            # Found a tag, but the value does not match
            next;
        } elsif ($auth && $auth->id != $tag->auth->id) {
            # Found a tag, but the authority does not match
            next;
        }
        push @matches, $tag;
    }
    # Nothing matched
    return wantarray ? @matches : $#matches + 1;
}


sub first_tag_value {
    my $self = shift;
    my ($tag) = $self->read_tags->each_tag( @_ );
    return $tag->valname || $tag->num;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

*tag_collection = \&read_tags;
sub read_tags {
    my $self = shift;
    my ($force) = @_;
    if ($self->{COLLECTION}) {
        if (!$force) {
            # Tags have already been set, user is happy with that
            return $self->{COLLECTION};
        } elsif (lc($force) eq 'clear') {
            # Tags already set, user wishes to clear them out
            $self->{COLLECTION} = undef;
        }
    }
    my $collection = $self->{COLLECTION} ||= 
        BMS::MapTracker::TagCollection->new( $self );
    my $id = $self->id;
    # A non-integer ID represents an edge that was manually constructed -
    # that is, it is not in the database:
    return $collection unless ($id =~ /^\d+$/);
    my $mt   = $self->tracker;
    my $meta = $mt->dbi->named_sth("Get edge tags")->selectall_arrayref( $id );
    #$mt->dbi->named_sth("Get edge tags")->pretty_print($id) if ($id == 109503289);
    # $debug->branch($meta) if ($#{$meta} > -1);
    # Preload tags
    $mt->bulk_seq_for_ids( map { $_->[1], $_->[2] } @{$meta});
    foreach my $row (@{$meta}) {
        $collection->add_tag(@{$row});
    }
    return $collection;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_text

 Title   : to_text
 Usage   : print $obj->to_text
 Function: Show the contents of the object as a text string
 Returns : A string
 Args    : 

=cut

*to_text_full = \&to_text;
sub to_text {
    my $self = shift;
    unless ($self->{TO_TEXT}) {
        # First time called, need to calculate (once)
        my ($reads, $n1, $n2) = $self->reads();
        my $space = $self->space;
        my $sptxt = $space->id == 1 ? '' : ' [' . $space->name. ']';
        my $txt   = sprintf
            ("[%s] %s %s %s%s\nHistory:\n", 
             $self->id, $n1->name, $reads, $n2->name, $sptxt);
        my %hist = $self->history;
        my %auths = map { $_->id => $_ } $self->all_authorities;
        my @aids  = sort { $auths{$a}->name cmp $auths{$b}->name } keys %auths;
        foreach my $aid (@aids) {
            $txt .= sprintf("  %s:\n", $auths{$aid}->name);
            my $ha = $hist{$aid};
            for my $d (0..$#{$ha}) {
                $txt .= sprintf("     %s [%s]\n", 
                                $ha->[$d], ($d % 2) ? '-' : '+');
            }
        }
        my $tagcol = $self->tag_collection->to_text(2);
        $txt .= "Meta Tags\n$tagcol" if ($tagcol);
        $self->{TO_TEXT} = $txt;
    }
    return $self->{TO_TEXT};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_text_short

 Title   : to_text_short
 Usage   : print $obj->to_text_short
 Function: Show the contents of the object as a text string
 Returns : A string
 Args    : 

=cut

sub to_text_short {
    my $self = shift;
    unless ($self->{TO_TEXT_SHORT}) {
        # First time called, need to calculate (once)
        my ($reads, $n1, $n2) = $self->reads();
        my $space = $self->space;
        my $sptxt = $space->id == 1 ? '' : ' [' . $space->name. ']';
        $self->{TO_TEXT_SHORT} = sprintf
            ("%-20s %-40s %-20s%s\n", $n1->name, $reads, $n2->name, $sptxt);
    }
    return $self->{TO_TEXT_SHORT};
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
    my ($userClass, $node, $addtax) = @_;
    my ($args, $name, @cls);
    my $lnk = "";
    push @cls, $userClass if ($userClass);
    push @cls, 'mtdis' if ($self->has_tag('Dissent'));
    if ($node) {
        $name = $node->name;
        $args = "{'node':" . $node->id . "}";
        if ($addtax) {
            my %taxa = map { $_->icon('url') => 1 } $node->each_taxa;
            $lnk .= join("", map { "<img src='$_' />" } sort keys %taxa);
        }
        push @cls, 'mtdep' if ($node->is_class('deprecated'));
    }
    $lnk .= $self->SUPER::javascript_link
        (join(' ', @cls), 'edge', $self->id, $name, $args);
    return $lnk;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_javascript

 Title   : to_javascript
 Usage   : $obj->to_javascript()
 Function: Generates an ASCII string encoding a JavaScript object
 Returns : The string

=cut

sub to_javascript {
    my $self = shift;
    my ($force) = @_;
    if (!$self->{JSTEXT} || defined $force) {
        # Transient edges will have ids such as Transient:1 or Foo:1;
        # we need to quote them.

        my $txt  = sprintf
            ("type:'edge', id:'%s', nodes:[%d,%d], rel:%d, ns:[%d], ".
             "live:%d, created:'%s'",
             $self->id, $self->node1->id, $self->node2->id, $self->type->id,
             $self->namespace->id, $self->is_live, $self->created );
        $txt .= sprintf(", auth:[ %s ]", 
                        join(",", map { $_->id } $self->each_authority));
        $txt .= sprintf(", deadauth:[ %s ]", 
                        join(",", map { $_->id } $self->dead_authorities));
        $txt .= ", tags: " . $self->read_tags->to_javascript();
        my %history = $self->history;
        my @hist;
        while (my ($aid, $arr) = each %history) {
            my @points;
            for my $d (0..$#{$arr}) {
                push @points, sprintf("['%s',%d]", $arr->[$d],
                                      ($d % 2) ? 0 : 1);
            }
            push @hist, sprintf("%d:[ %s ]", $aid, join(", ", @points));
        }
        $txt .= sprintf(", history: { %s }", join(", ", @hist));
        $self->{JSTEXT} = "{ $txt }";   
    }
    return $self->{JSTEXT};
}

sub register_full_javascript {
    my $self = shift;
    my $mt   = $self->tracker;
    return unless ($mt);
    my @objs = ($self);
    push @objs, $self->nodes;
    push @objs, ($self->namespace, $self->type);
    foreach my $tag ($self->each_tag) {
        push @objs, $tag->all_objects;
    }
    
    $mt->register_javascript( @objs );
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_html

 Title   : to_html
 Usage   : print $obj->to_html
 Function: Show the contents of the object as a html string
 Returns : A string
 Args    : 

=cut

sub to_html {
    my $self = shift;
    unless ($self->{TO_HTML}) {
        # First time called, need to calculate (once)
        my ($reads, $n1, $n2) = $self->reads();
        my $space = $self->space;
        my $sptxt = $space->id == 1 ? '' : 
            " <font color='green'><i>" . $space->name. "</i></font>";
        my $txt = sprintf
            ("<b><font color='blue'>%s <font color='brick'>%s</font> %s".
             "</font></b>%s <font size='-1' color='brown'>[%s]</font><br />\n",
             $n1->name, $reads, $n2->name, $sptxt, $self->id);
        $txt .= $self->tag_collection->to_html(2);
        $self->{TO_HTML} = $txt;
    }
    return $self->{TO_HTML};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub kill {
    my $self = shift;
    my ($lh, $authreq) = @_;
    if (!$lh || !ref($lh) || !$lh->isa('BMS::MapTracker::LoadHelper')) {
        $lh ||= '-UNDEF-';
        $self->error("$lh is not a LoadHelper object");
        return;
    }
    my @auths = $authreq ? ($authreq) : $self->each_authority();
    my ($n1, $n2) = map { $_->namespace_name } $self->nodes;
    my $reads = $self->reads;
    my $space = $self->space;
    foreach my $auth (@auths) {
        $lh->kill_edge( -name1 => $n1,
                        -name2 => $n2,
                        -auth  => $auth,
                        -type  => $reads,
                        -space => $space );
    }
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::TagCollection;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use strict;
use Scalar::Util qw(weaken);

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
        TAGS  => [],
        NAMES => {},
        AUTH  => {},
        KEYS  => {},
        ALIST => [],
        NLIST => [],
    };
    bless ($self, $class);
    my ($parent) = @_;
    weaken($self->{PARENT} = $parent);
    return $self;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub parent {
    return shift->{PARENT};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub add_tag {
    my $self = shift;
    my ($areq, $tagid, $valid, $num) = @_;
    # Turn all requests into objects:
    my $par  = $self->parent;
    my $mt   = $par->tracker;

    my $auth = $mt->get_authority($areq);
    my ($tag, $val) = ( $mt->get_seq_by_id( $tagid ), 
                        $mt->get_seq_by_id( $valid ) );
    # Make a tag object:
    my $tobj = BMS::MapTracker::Tag->new( $par, $auth, $tag, $val, $num);
    # What is the unique key representing this object?
    my $key  = $tobj->hash_key;
    # If the tag is already in the collection, return it:
    return $self->{KEYS}{$key} if ($self->{KEYS}{$key});
    # New tag, make indices for it, and add to internal stack:
    $self->{KEYS}{$key} = $tobj;
    push @{$self->{TAGS}}, $tobj;
    my $tname = uc($tag->name);
    my $aid   = $auth->id;
    unless ($self->{NAMES}{$tname}) {
        # First time this (case-insensitve) name is seen
        $self->{NAMES}{$tname} = [];
        # For pretty-printing, make note of the case of the tag name
        # Obviously, if multiple cases exist for a tag spelling, only
        # the first instance will be captured with this method.
        # The assumption is that tags are not going to have case-sensitive
        # information in the tag name. If they do, the user can extract that
        # by just getting all tags and directly inspecting the objects.
        push @{$self->{NLIST}}, $tag->name;
    }
    unless ($self->{AUTHS}{$aid}) {
        # First time this authority is seen
        $self->{AUTHS}{$aid} = [];
        push @{$self->{ALIST}}, $auth;
    }
    push @{$self->{NAMES}{$tname}}, $tobj;
    push @{$self->{AUTHS}{$aid}},   $tobj;
    # We updated the collection, clear the text string
    $self->{TO_TEXT} = $self->{TO_HTML} = "";
    return $tobj;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 each_authority

 Title   : each_authority
 Usage   : my @authorities = $obj->each_authority
 Function: Gets a list of all authorities represented in this collection
 Returns : An array of BMS::MapTracker::Authority objects
    Args : 

=cut

sub each_authority {
    return @{shift->{ALIST}};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 each_authority_name

 Title   : each_authority_name
 Usage   : my @auth_names = $obj->each_authority_name
 Function: As above, but just returns names instead of objects
 Returns : An array of strings
    Args : 

=cut

sub each_authority_name {
    return map {$_->name} shift->each_authority;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub each_tag {
    my ($self, $request) = @_;
    return @{$self->{TAGS}} unless ($request);
    my $mt = $self->parent->tracker;
    if (ref($request)) {
        if ($mt->_safe_isa($request,'BMS::MapTracker::Authority')) {
            # Request for all tags for a given authority
            my $aid = $request->id;
            return () unless (exists $self->{AUTHS}{$aid});
            return @{$self->{AUTHS}{$aid}};
        } elsif ($mt->_safe_isa($request,'BMS::MapTracker::Seqname')) {
            # Request for a tag name
            $request = $request->name;
        } else {
            die "I do not know how to get tags with '$request'\n ";
        }
    }
    $request = uc($request);
    return () unless (exists $self->{NAMES}{$request});
    return @{$self->{NAMES}{$request}};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 each_tag_name

 Title   : each_tag_name
 Usage   : my @tag_names = $obj->each_tag_name
 Function: Gets a list of all assigned tag names
 Returns : An array of strings
    Args : 

  NOTE: Requests for information by tag name is
        case-insensitive. However, this function will return the names
        in the case that they exist in within the database (so that
        printed output looks a bit nicer)

=cut

sub each_tag_name {
    return @{shift->{NLIST}};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 tags_as_hash

 Title   : tags_as_hash
 Usage   : my $hash_ref = $obj->tags_as_hash
 Function: Gets the tag / value structure as a hash reference
 Returns : A hash reference. Keys will be n array of strings
    Args : 

  NOTE: Requests for information by tag name is
        case-insensitive. However, this function will return the names
        in the case that they exist in within the database (so that
        printed output looks a bit nicer)

=cut

sub tags_as_hash {
    my $self = shift;
    my %hash;
    foreach my $tname ($self->each_tag_name) {
        foreach my $tag ($self->each_tag($tname)) {
            my $key = $tag->formatted_value();
            push @{$hash{$tname}{$key}}, $tag->auth->name;
        }
    }
    return \%hash;
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
    my ($pad) = @_;
    unless ($self->{TO_TEXT}) {
        my $text     = "";
        foreach my $auth (sort {$a->name cmp $b->name} $self->each_authority){
            my @tags = sort {uc($a->tag->name) cmp 
                                 uc($b->tag->name) } $self->each_tag($auth);
            $text .= $auth->name . ":\n";
            my %tns;
            foreach my $tag ( @tags ) {
                push @{$tns{$tag->tagname}}, "    " . $tag->formatted_value;
            }
            foreach my $tn (sort keys %tns) {
                $text .= "  $tn\n";
                $text .= join("\n", @{$tns{$tn}}) ."\n";
            }
        }
        $self->{TO_TEXT} = $text;
    }
    my $retval = $self->{TO_TEXT};
    if ($pad && $retval) {
        my $ptext = " " x $pad;
        $retval =~ s/\n/\n$ptext/g;
        $retval = $ptext . $retval;
    }
    return $retval;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_javascript

 Title   : to_javascript
 Usage   : $obj->to_javascript()
 Function: Generates an ASCII string encoding a JavaScript object
 Returns : The string

=cut

sub to_javascript {
    my $self = shift;
    my ($force) = @_;
    if (!$self->{JSTEXT} || defined $force) {
        my $txt = join(", ", map { $_->to_javascript } $self->each_tag) || "";
        $self->{JSTEXT} = "[ $txt ]";   
    }
    return $self->{JSTEXT};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_html

 Title   : to_html
 Usage   : print $obj->to_html
 Function: Show the contents of the object as a html string
 Returns : A string
 Args    : 

=cut

sub to_html {
    my $self = shift;
    my ($pad) = @_;
    unless ($self->{TO_HTML}) {
        my @tags = sort $self->each_tag_name;
        my $afrm = "<font color='grey'>%s</font>";
        return $self->{TO_HTML} = " " if ($#tags < 0);
        my $html = "";
        foreach my $tname (@tags) {
            $html .= "<li><b><font color='orange'>$tname</font></b> ";
            my %hash;
            foreach my $tag ($self->each_tag($tname)) {
                my $key = $tag->formatted_value;
                push @{$hash{$key}}, $tag->auth->name;
            }
            my %seenAuth;
            while (my ($val, $auths) = each %hash) {
                my $astr = sprintf($afrm, join(", ",@{$auths}));
                $hash{$val} = $astr;
                $seenAuth{$astr}++;
            }

            my @distincts = keys %seenAuth;
            my @vals = sort keys %hash;
            if ($#distincts == 0) {
                # All entries are from the same combination of authors
                $html .= join(", ", @vals ) . " ". $distincts[0];
            } else {
                # different authorites
                my @parts;
                foreach my $val (@vals) {
                    push @parts, "$val $hash{$val}";
                }
                $html .= join(", ", @parts);
            }
            $html .= "</li>\n";
        }
        $html .= "";
        $self->{TO_HTML} = $html;
    }
    my $retval = $self->{TO_HTML};
    if ($pad) {
        my $phtml = " " x $pad;
        $retval =~ s/\n/\n$phtml/g;
        $retval = $phtml . $retval;
    }
    return $retval;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::Tag;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use strict;
use Scalar::Util qw(weaken);

my @cbmatch = ( [ 'Language', 'Perl' ],
                [ 'Utility',  'Tag Formatting' ] );

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = { };
    bless ($self, $class);
    my ($parent, $auth, $tag, $value, $num) = @_;
    weaken($self->{PARENT} = $parent);
    $self->{AUTH} = $auth;
    $self->{TAG}  = $tag;
    $self->{TAGNAME} = $tag->name;
    $self->{VAL}  = $value;
    $self->{VALNAME} = $value ? $value->name : "";
    $self->{NUM}  = $num;
    return $self;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub parent {
    return shift->{PARENT};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
*authority = \&auth;
sub auth {
    return shift->{AUTH};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
*authority_name = \&authname;
sub authname {
    return shift->{AUTH}->name;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub tag {
    return shift->{TAG};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub tagname {
    return shift->{TAGNAME};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub tag_nsname {
    return shift->{TAG}->namespace_name;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
*value = \&val;
sub val {
    return shift->{VAL};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
*valuename = \&valname;
sub valname {
    return shift->{VALNAME};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
*value_nsname = \&val_nsname;
sub val_nsname {
    my $self = shift;
    return $self->{VAL} ? $self->{VAL}->namespace_name : "";
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# Ultimately will put callback logic here
*formatted_val = \&formatted_value;
sub formatted_value {
    my $self = shift;
    my @cbs;
    my ($tn, $tv) = ($self->tag, $self->val);
    push @cbs, $tn->each_callback
        ( -match => [ @cbmatch, ['Trigger', 'Tag Name'] ],
          -return => 'edge', ) if ($tn);
    push @cbs, $tv->each_callback
        ( -match => [ @cbmatch, ['Trigger', 'Tag Value'] ],
          -return => 'edge',) if ($tv);

    my $cb = \&default_callback;
    if ($#cbs > -1) {
        # NEED TO SORT THIS
        $cb = eval($cbs[0]->node2->name);
    }
    return &{$cb}( $self );
}

sub default_callback {
    my $tag = shift;
    my @bits;
    push @bits, $tag->valname if ($tag->valname);
    push @bits, $tag->num if (defined $tag->num);
    return join(" : ", @bits);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
*numeric_value = \&num;
*numeric_val = \&num;
*number = \&num;
sub num {
    return shift->{NUM};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub to_text_full {
    my $self = shift;
    unless ($self->{TO_TEXT_FULL}) {
        $self->{TO_TEXT_FULL} = sprintf
            ("[%d] %s: %s", $self->parent_id,$self->auth->name,$self->to_text);
    }
    return $self->{TO_TEXT_FULL};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub to_text {
    my $self = shift;
    unless ($self->{TO_TEXT}) {
        my $num = $self->num;
        my $txt = substr($self->tagname,0,20) . " =";
        $txt .= $self->formatted_value;
        $self->{TO_TEXT} = $txt;
    }
    return $self->{TO_TEXT};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_javascript

 Title   : to_javascript
 Usage   : $obj->to_javascript()
 Function: Generates an ASCII string encoding a JavaScript object
 Returns : The string

=cut

sub to_javascript {
    my $self = shift;
    my ($force) = @_;
    if (!$self->{JSTEXT} || defined $force) {
        my $txt  = sprintf("auth:%d, tag:%d", $self->auth->id, $self->tag->id);
        $txt .= ", val:" . $self->val->id if ($self->val);
        $txt .= ", num:" . $self->num if (defined $self->num);
        $self->{JSTEXT} = "{ $txt }";   
    }
    return $self->{JSTEXT};
}

sub all_objects {
    my $self = shift;
    my @objs = ($self->tag, $self->authority);
    push @objs, $self->value if ($self->value);
    return @objs;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub hash_key {
    my $self = shift;
    unless ($self->{KEY}) {
        my ($val, $num) = ($self->val, $self->num);
        $self->{KEY} = 
            join("\t", $self->parent->id, $self->tag->id, $self->auth->id, 
                 $val ? $val->id : '', defined $num ? $num : '');
    }
    return $self->{KEY};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
1;
