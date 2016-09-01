# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::OntologyWidget;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

$BMS::MapTracker::OntologyWidget::VERSION = 
    ' $Id$ ';

use strict;
use BMS::MapTracker;
use BMS::Branch;
use BMS::CommonCAT;
use Scalar::Util qw(weaken);

my $debug = BMS::Branch->new
    ( -skipkey => ['CLASSES', 'BENCHMARKS', 'CACHED_STH', 'OPT_THINGS', 
		   'TRACKER', 'CHILDREN', '_sfc', 'ALGORITHM', '__HTML__' ],
      -noredundancy => 1,
      -hideempty    => 1,
      -maxarray     => 50);

=head1 PRIMARY METHODS
#-#-#-#-#--#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-

=head2 new

 Title   : new
 Usage   : my $iw = BMS::MapTracker::OverlapWidget->new(@arguments)
 Function: Creates a new object and returns a blessed reference to it.
 Returns : A blessed BMS::MapTracker::OverlapWidget object
 Args    : Associative array of arguments. Recognized keys [Default]:

  -tracker A BMS::MapTracker database object. If you are already using
           such an object, you should generally provide it to
           OntologyWidget (usually makes things a bit more
           efficient). If not, then do not provide a value - a new
           database handle will be automatically generated.

 -use_objs Default 0. Defines the entities that should be returned to
           you. If a false value, then you will always get strings
           back for nodes (eg "GO:0019912"). If true, then nodes will
           be returned as BMS::MapTracker::Seqname objects. Objects
           are more powerful than strings (and can always be converted
           to strings with $obj->name), but are not needed in many
           cases.

           You can access or change this parameter at any time with
           the use_objects() method.

 -toparent Default 'is a child of'. The MapTracker edge(s) that should
           be followed to get from a node TO THE PARENT node(s). You
           may set multiple allowed edge types by passing an array
           reference. These edges may be set / retrieved with
           parent_edge_types().

  -tochild Default 'is a parent of'. The MapTracker edge(s) that
           should be followed to get from a node TO THE PARENT
           node(s). You may set multiple allowed edge types by passing
           an array reference. These edges may be set / retrieved with
           child_edge_types().

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
        CACHE    => {},
        TYPES    => { parent => [], child => [] },
        DESCS    => {},
        PARENTS  => {},
        CHILDREN => {},
        KID_NUM  => {},
        USE_OBJS => 0,
    };
    bless ($self, $class);
    # $self->clear;

    my $args = $self->parseparams
        ( -tracker  => undef,
          -verbose  => 0,
          -tochild  => 'is a parent of',
          -toparent => 'is a child of',
          @_ );
    if (my $mt = $args->{TRACKER}) {
	$self->death("'$mt' is not a MapTracker object!")
	    unless (ref($mt) && $mt->isa("BMS::MapTracker"));
        weaken( $self->{TRACKER} = $mt );
    } else {
        $self->{TRACKER} = BMS::MapTracker->new ( -username => 'readonly',@_ );
    }
    $self->parent_edge_types( $args->{TOPARENT});
    $self->child_edge_types( $args->{TOCHILD});
    return $self;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 tracker

 Title   : tracker
 Usage   : my $mt = $iw->tracker
 Function: Gets the MapTracker database object
 Returns : A BMS::MapTracker object
 Args    : 

=cut

sub tracker {
    my $self = shift;
    return $self->{TRACKER};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 use_objects

 Title   : use_objects
 Usage   : my $using_objects = $ow->use_objects( $optional_new_value );

 Function: Sets / Gets the flag to use objects or not. If the program
           is set to use objects, then returned values will be
           MapTracker::SeqName objects. Otherwise, returned values
           will be strings.

 Returns : 0 or 1
 Args    : Optional new value, will be mapped to 1 or 0

  Examples:

    $ow->use_objects(1); # Now all returned nodes will be objects

    $ow->use_objects(0); # Now all returned nodes will be strings

=cut

sub use_objects {
    my $self = shift;
    if (defined $_[0]) {
        $self->{USE_OBJS} = $_[0] ? 1 : 0;
    }
    return $self->{USE_OBJS};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 parent_edge_types

 Title   : parent_edge_types
 Usage   : my @types = $ow->parent_edge_types( $optional_new_value );

 Function: Gets / Sets the edge types you want to use to find parents
           from a node. Normally you will have only one edge type, but
           it is possible to set more than one. For this reason, the
           returned value is a list.

 Returns : A list of one or more edge types

    Args : Optional new value(s). Multiple values may be passed either
           as an array reference, or simply as a list.

  Examples:

    # Set a single allowed edge type
    $ow->parent_edge_type( 'is a child of' );

    # Set a multiple edge types
    $ow->parent_edge_type( 'is a child of', 'reports to' );

=cut

*parent_edge_type = \&parent_edge_types;
sub parent_edge_types {
    my $self = shift;
    $self->_types_to_array('parent', @_) if ($_[0]);
    return @{$self->{TYPES}{parent}};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 child_edge_types

 Title   : child_edge_types
 Usage   : my @types = $ow->child_edge_types( $optional_new_value );

 Function: Gets / Sets the edge types you want to use to find children
           from a node. Normally you will have only one edge type, but
           it is possible to set more than one. For this reason, the
           returned value is a list.

 Returns : A list of one or more edge types

    Args : Optional new value(s). Multiple values may be passed either
           as an array reference, or simply as a list.

  Examples:

    Same as for parent_edge_types

=cut

*child_edge_type = \&child_edge_types;
sub child_edge_types {
    my $self = shift;
    $self->_types_to_array('child', @_) if ($_[0]);
    return @{$self->{TYPES}{child}};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 parent

 Title   : parent
 Usage   : my $parent  = $ow->parent( $node );
           my @parents = $ow->parent( $node );

 Function: Finds the first-level parent of the requested node

 Returns : The parent, or undef if the node has no parent. Note that
           some ontologies allow nodes to have multiple parents. These
           will be recovered by this call, but if you call in a scalar
           context you will only get one. Call in array context if you
           wish to recover all first-level parents

 Args    : [0] The node you are interested in

  Examples:

    # Get all possible parents:
    my @immediate_parents = $ow->parent( $mynode );

    # Be risky:
    my $i_only_really_want_one_parent = $ow->parent( $mynode );

=cut

*direct_parent = \&parent;
*direct_parents = \&parent;
*parents = \&parent;
sub parent {
    my $self = shift;
    my $node = $self->tracker->get_seq( $_[0] );
    return undef unless ($node);
    my $hash = $self->_parent_hash( $self->_cache_node( $node ) );
    return wantarray ? () : undef unless (exists $hash->{by_depth}{1});
    return $self->_return_for_list( $hash->{by_depth}{1} );
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 all_parents

 Title   : all_parents
 Usage   : my @parents = $ow->all_parents( $node );
           my @parents_plus_depth = $ow->all_parents( $node, 1 );
           my %depth_hash = $ow->all_parents( $node, 'hash');

 Function: Gets all parents, grand-parents, great-grand... etc for the
           requested node.

 Returns : An array. If the depth flag is not set, then the array will
           just be a list of parent nodes, sorted by the distance the
           parent is from your request, with 'closer' parents being at
           the start of the list. If two or more parents are the same
           distance away, their order in the list will be arbitrary.

           If you do set the depth flag, then each element in the
           returned list will be a 2-element hash reference. The first
           element will be a parent node, the second will be an
           integer indicating the distance of the parent from your
           request.

           If the depth flag is set as 'hash', then the returned value
           will be a hash, keyed to depth (integer), with each value
           being an array reference of parents.

 Args    : [0] The node you are querying
           [1] A flag that will include distance valuess if set

  Examples:

    my @parents = $ow->all_parents( $mynode );
    print "All parents of $mynode: " . join(", ", @parents) . "\n";

    my @par_depth = $ow->all_parents( $mynode, 1 );
    foreach my $data (@par_depth) {
        my ($par, $depth) = @{$data};
        print "  $par = depth $depth\n";
    }

    my %par_hash = $ow->all_parents( $mynode, 'hash' );
    my @depths = sort { $a <=> $b } %par_hash;
    foreach my $depth (@depth) {
        print "  All parents at distance $depth:\n";
        print "     ".join(", ", @{$par_hash{$depth}})."\n";
    }


=cut

sub all_parents {
    my $self = shift;
    my ($req, $depth_flag) = @_;
    my $mt   = $self->tracker;
    my $node = $mt->get_seq( $req );
    return () unless ($node);
    my $hash = $self->_parent_hash( $self->_cache_node( $node ) );
    my ($nids, @depth);
    foreach my $depth ( sort { $a <=> $b } keys %{$hash->{by_depth}}) {
        push @{$nids}, @{$hash->{by_depth}{$depth}};
        push @depth, map { $depth } @{$hash->{by_depth}{$depth}};
    }

    my @ret_array;
    if ($self->use_objects) {
        @ret_array = $self->_ids_to_objects( $nids );
    } else {
        @ret_array = $self->_ids_to_names( $nids );
    }

    if ($depth_flag) {
        # The user is looking for depth context
        if (lc($depth_flag) eq 'hash') {
            # The user wants the parents organized as a hash
            my %ret_hash;
            for my $i (0..$#depth) {
                $ret_hash{$depth[$i]} ||= [];
                push @{$ret_hash{$depth[$i]}}, $ret_array[$i];
            }
            return %ret_hash;
        } else {
            # The user wants data as an array of [parent, depth]
            for my $i (0..$#depth) {
                $ret_array[$i] = [ $ret_array[$i], $depth[$i] ];
            }
        }
    }
    return @ret_array;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 has_parent

 Title   : has_parent
 Usage   : my $test_result = $ow->has_parent($query, $parent_to_check  );

 Function: Checks to see if your query is a child (or grand ^ n-child)
           of a provided parent node.

 Returns : 0 if the child is not related to the parent, otherwise a
           positive integer indicating how distantly related the child
           is. 1 indicates an immediate child, 2 a grandchild, etc.

 Args    : [0] The child node
           [1] The presumptive parent node

  Examples:

    # Use for logical testing
    if ($ow->has_parent($query_node, $compare_node) ) {
        # Entry to this block means that $compare_node is, at some
        # distance, a parent of $query_node
    }

    # Use for distance measurement
    if (my $dist = $ow->has_parent($query_node, $compare_node) ) {
        print "$query node is $dist edges away from parent $compare_node";
    } else {
        die "$query_node is not a child of $compare_node!!";
    }

=cut

sub has_parent {
    my $self = shift;
    my ($creq, $preq) = @_;
    my $mt   = $self->tracker;
    my $par  = $mt->get_seq( $preq );
    my $node = $mt->get_seq( $creq );
    return undef unless ($node && $par);
    my $pid  = $self->_cache_node( $par );
    my $hash = $self->_parent_hash( $self->_cache_node( $node ) );
    return (exists $hash->{by_id}{$pid}) ? $hash->{by_id}{$pid} : 0;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 parent_count

 Title   : parent_count
 Usage   : my $foo = $ow->parent_count( $node );

 Function: Returns the number of distinct parents the node has

 Returns : An integer greater or equal to zero
 Args    : Your query node

  Examples:

    # How many distinct parents does a node have?
    printf("%s has %d parents\n", $node, $ow->parent_count($node));

=cut

sub parent_count {
    my $self = shift;
    my $node = $self->tracker->get_seq( $_[0] );
    return undef unless ($node);
    my $hash = $self->_parent_hash( $self->_cache_node( $node ) );
    return $hash->{count};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 children

 Title   : children
 Usage   : my @children = $ow->children( $node );

 Function: Gets all first-level children for a node

 Returns : An array of all direct children. The array will be empty if
           none exist. Because this method only gets first-level
           children, it should be reliably fast.

           Direct children are cached once they are recovered from the
           database, so subsequent calls with the same query should be
           faster.

 Args    : [0] The query node

  Examples:

    # Get only immediate children:
    my @kids = $ow->children( $myNode );

=cut

sub children {
    my $self = shift;
    my $node = $self->tracker->get_seq( $_[0] );
    return wantarray ? () : undef unless ($node);
    my $kids = $self->_child_list( $self->_cache_node( $node ) );
    return $self->use_objects ? 
        $self->_ids_to_objects( $kids) : $self->_ids_to_names( $kids);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 all_children

 Title   : all_children
 Usage   : my @all_children   = $ow->all_children( $node );
           my @kids_and_depth = $ow->all_children( $node, 1 );
           my %kids_by_depth  = $ow->all_children( $node, 'hash' );

 Function: Gets all children for a node, including every indirect
           child. This method will recursively call children(), and
           could take a very long time to complete if called with
           nodes near the top of the tree.

           Called from the root node on a 13,000 node GeneOntology
           tree, the method takes betwenn 100-120 seconds on a single
           itanium processor.

           Because children() caches recovered results, subsequent
           calls with the same query will be very rapid - in the above
           example, recalling children for the GO root took a
           second. Note also that by calling all_children on the root
           node, we have effectively cached all child information for
           the entire tree (not really needed, though, as caching will
           occur when required).

           In general, you should probably avoid using all_children if
           possible. In many cases you can perform the logic you
           require by using has_parent() or has_child(), which are
           both fast, efficient methods. Only call all_children if you
           really require a list of all child nodes.

 Returns : An array of all children for the node. The array will be empty if
           none exist.

           all_children() utilizes a 'depth flag' similar to
           all_parents(). If the flag is false (or not passed), the
           return value will just be a list of all children.

           If the flag is true, the returned array will be composed of
           [child, distance] pairs.

           If the flag is 'hash', then a hash will be returned, with
           keys being the integer distances, and values array
           references of children at that distance.

 Args    : [0] The query node
           [1] Optional depth flag.

  Examples:

    # Get only the child nodes:
    my @kids = $ow->all_children( $myNode );

    # Get child and distance information
    foreach my $info ($ow->all_children( $myNode, 1)) {
        my ($child, $distance) = @{$info};
        print "$child is $distance away from parent $myNode\n";
    }

    # Get hash structure 
    my %kid_dist = $ow->all_children( $myNode, 'hash');
    if (exists $kid_dist{3}) {
        my @at3 = @{$kid_dist{3}};
        printf("These nodes are 3 edges from parent %s: %s\n",
               $myNode, join(", ", @at3));
    } else {
        warn "Node $myNode does not have any children 3 edges away\n";
    }

=cut

*each_child  = \&all_children;
*every_child = \&all_children;
sub all_children {
    my $self = shift;
    my ($req, $depth_flag) = @_;
    my $node = $self->tracker->get_seq( $req );
    return wantarray ? () : undef unless ($node);
    my $nid  = $self->_cache_node( $node );
    my @stack = ( [ $nid, 0 ] );
    my %by_id;
    while (my $info = shift @stack) {
        my ($kid, $depth) = @{$info};
        if (defined $by_id{$kid}) {
            # We have already encountered this child
            $by_id{$kid} = $depth if ($by_id{$kid} > $depth);
            next;
        }
        $by_id{$kid} = $depth;
        my $gkids = $self->_child_list( $kid );
        push @stack, map  { [ $_, $depth + 1 ] } @{$gkids};
    }
    delete $by_id{$nid};
    
    my $uo   = $self->use_objects;
    my @kids = sort { $by_id{$a} <=> $by_id{$b} } keys %by_id;
    $self->{KID_NUM}{$nid} ||= $#kids + 1;
    
    if ($depth_flag) {
        my $cache = $self->{CACHE};
        if ($depth_flag eq 'hash') {
            my %ret_hash;
            while ( my ($id, $depth) = each %by_id) {
                $ret_hash{$depth} ||= [];
                push @{$ret_hash{$depth}}, $uo ?
                    $cache->{$id} : $cache->{$id}->name;
            }
            return %ret_hash;
        } else {
            # The user wants [node, distance] pairs
            my @retval;
            if ($uo) {
                @retval = map {[ $cache->{$_}, $by_id{$_} ]} @kids;
            } else {
                @retval = map {[ $cache->{$_}->name, $by_id{$_} ]} @kids;
            }
            return @retval;
        }
    }
    return $uo ? 
        $self->_ids_to_objects( \@kids) : $self->_ids_to_names( \@kids);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 has_child

 Title   : has_child
 Usage   : my $test_result = $ow->has_child($query, $child_to_check  );

 Function: Checks to see if your query is a parent (or grand^n-parent)
           of a provided child node.

 Returns : 0 if the parent is not related to the child, otherwise a
           positive integer indicating how distantly related the child
           is. 1 indicates an immediate child, 2 a grandchild, etc.

           Note that this is a conveinence call. has_child($a,$b) is
           the same logic as has_parent($b,$a) - and that is how the
           API handles this call, since it is MUCH easier to walk up
           the tree than down.

 Args    : [0] The parent node
           [1] The presumptive child node

  Examples:

    Same as has_parent()

=cut

sub has_child {
    my $self = shift;
    my ($creq, $preq) = @_;
    return $self->has_parent($preq, $creq);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 is_connected

 Title   : is_connected
 Usage   : my $distance = $ow->is_connected($node1, $node2 );

 Function: Checks to see if two nodes are connected, and if so,
           reports how far apart they are.

 Returns : 0 if the nodes are not connected, and an integer
           representing how many edges separate the nodes if they
           are.

           If node1 is a parent of node2, then the integer will be
           positive - otherwise, if node2 is a parent of node1, then
           the integer will be negative.

 Args    : [0] The 'first' node
           [1] The 'second' node

  Examples:

    # Report if two nodes are connected
    if (my $dist = $ow->is_connected($node1, $node2)) {
        print "$node1 and $node2 are connected, distance $dist\n";
    } else {
        print "$node1 and $node2 are not directly connected\n";
    }

=cut

sub is_connected {
    my $self = shift;
    my ($node1, $node2) = @_;
    my $dist;
    $dist = $self->has_parent($node1, $node2);
    return $dist if ($dist);
    $dist = $self->has_parent($node2, $node1 );
    return $dist * -1 if ($dist);
    return 0;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 common_parent

 Title   : common_parent
 Usage   : my $parent_node = $ow->common_parent($node1, $node2, ... );

 Function: Finds the closest common parent (ancestor) for two or more
           nodes.

 Returns : undef if the nodes do not share a parent (which should only
           happen if they are not in the same ontology), or the closest
           node that they share.

           If there are multiple parents at equal distance from both
           nodes, then all such parents will be returned if called in
           list context.

 Args    : A list of two or more nodes

  Examples:

    # Report if two nodes are connected
    if (my $dist = $ow->common_parent($node1, $node2)) {
        print "$node1 and $node2 are connected, distance $dist\n";
    } else {
        print "$node1 and $node2 are not directly connected\n";
    }
    # Bear in mind that you can pass arbitrarily large lists of nodes

=cut

*common_ancestor = \&common_parent;
sub common_parent {
    my $self = shift;
    my $mt   = $self->tracker;
    if ($#_ < 0) {
        # Hmm. No parameters passed
        return wantarray ? () : undef;
    } elsif ($#_ == 0) {
        # Only one node passed - return itself
        my $node = $mt->get_seq( $_[0] );
        return $self->_return_for_list( [$self->_cache_node( $node )] );
    }

    my @hashes;
    foreach my $request (@_) {
        my $node = $mt->get_seq( $request );
        return wantarray ? () : undef unless ($node);
        push @hashes, $self->_parent_hash( $self->_cache_node( $node ) );
    }
    # Sort the entries with fewest parents to front:
    @hashes = sort { $a->{count} <=> $b->{count} } @hashes;
    # Arbitrarily use the first one as a reference:
    my $ref_hash = shift @hashes;
    # We need to also check the requested nodes themselves:
    my @reference = ([ $ref_hash->{self}, 0 ]);
    while (my ($id, $dist) = each %{$ref_hash->{by_id}}) {
        push @reference, [$id, $dist];
    }
    my @common;
    
    # Cycle through each parent ($id) in the reference structure:
  REFLOOP: foreach my $info (@reference) {
      my ($id, $dist) = @{$info};
      # Now look in each of the other parent structures
      for my $i (0..$#hashes) {
          # If this node is the *same* as $id, we can carry on without
          # incrementing the distance (contributed distance = 0)
          next if ($hashes[$i]{self} == $id );
          # If the reference parent ($id) is not shared in this structure,
          # then $id is not a viable common parent:
          my $comp = $hashes[$i]{by_id};
          next REFLOOP unless (exists $comp->{$id});
          # This parent is shared with reference, increment the distance
          $dist += $comp->{$id};
      }
      # All nodes share this parent, store the summed distance:
      push @common, [$id, $dist ];
  }
    # If no common parents found, return an empty array or undef
    return wantarray ? () : undef if ($#common < 0);
    # Sort the results by summed distance:
    @common = sort { $a->[1] <=> $b->[1] } @common;
    # The best (shortest) distance:
    my $best = $common[0][1];
    # Find all name_ids that are $best distance in total:
    my @all_best;
    foreach my $info (@common) {
        last if ($info->[1] > $best);
        push @all_best, $info->[0];
    }
    return $self->_return_for_list( \@all_best );
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 child_count

 Title   : child_count
 Usage   : my $count = $ow->child_count( $node );

 Function: Returns the number of distinct children the node has

           CAUTION: In order to calculate this value, the system must
           actually recover all children for the node. Once that has
           been done, the result is cached and will be returned on
           subsequent calls (for a particular query) almost instantly,
           but the first call may take a while.

           Calculation is performed by all_children() - so if you have
           already called all_children() for a query, this call should
           return the count immediately.

 Returns : An integer greater or equal to zero
 Args    : Your query node

  Examples:

    printf("%s has %d children at all levels", $node, $ow->child_count($node));

=cut

sub child_count {
    my $self = shift;
    my $node = $self->tracker->get_seq( $_[0] );
    return undef unless ($node);
    my $nid = $self->_cache_node( $node );
    unless (defined $self->{KID_NUM}{$nid}) {
        $self->all_children($node);
    }
    return $self->{KID_NUM}{$nid};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 root

 Title   : root 
 Usage   : my $root_node = $ow->root( $mynode );
           my @roots = $ow->root( $mynode );

 Function: Gets the root(s) of a node. It is assumed that in most
           cases a node can only have one root. However, just to be
           careful, the system will check for multiple roots.

           A root is defined simply as a node that does not have any
           parents itself. For this reason, ALL nodes will have a root
           - if not a 'real' root, they will think that they are
           themselves a root, and will report themselves as their own
           root.

 Returns : In a scalar context, only a single root will be
           returned. This should be sufficient for most well-formed
           ontologies. If you expect multiple parents, call in array
           context.

 Args    : [0] The query node

  Examples:

    printf("%s is the root for %s\n", $ow->root($node), $node, );

=cut

sub root {
    my $self = shift;
    my $node = $self->tracker->get_seq( $_[0] );
    return wantarray ? () : undef unless ($node);
    my $hash = $self->_parent_hash( $self->_cache_node( $node ) );
    return $self->_return_for_list( $hash->{roots} );
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 distance_to_root

 Title   : distance_to_root 
 Usage   : my $distance = $ow->distance_to_root( $mynode );

 Function: Gets the distance from the query node to the root, in
           number of edges. A node 1 edge away from the root will
           return 1. Calling with a node that is itself a root will
           return 0.

           In the event that the node has multiple roots, the shortest
           distance will be reported.

 Returns : An integer 0 or greater

 Args    : [0] The query node

  Examples:

    printf("%s is %d edges from root\n", $node, $ow->distance_to_root($node));

=cut

sub distance_to_root {
    my $self = shift;
    my $node = $self->tracker->get_seq( $_[0] );
    return wantarray ? () : undef unless ($node);
    my $hash = $self->_parent_hash( $self->_cache_node( $node ) );
    return 0 if ($hash->{is_root});
    my $dist;
    foreach my $rid (@{$hash->{roots}}) {
        my $rd = $hash->{by_id}{$rid};
        $dist = $rd if (!$dist || $dist > $rd);
    }
    return $dist;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 description

 Title   : description
 Usage   : my $shortest_description = $ow->description( $node );
           my @all_descriptions = $ow->description( $node );

 Function: Gets the description for a node, if any.

 Returns : A string, if called in scalar context. Note that there may
           be more than one description - if called in a list context,
           you can get all descriptions - otherwise, you will only get
           the shortest one.

 Args    : [0] The node you wish a description for

  Examples:

    # Print a node and its description
    my $desc = $ow->description($request);
    # To avoid printing empty parens if there is no description,
    # we check desc, and pad with () only if $desc is not empty
    printf("Request: %s%s\n", $request, $desc ? " ($desc)" : "");

=cut

*desc = \&description;
sub description {
    my $self = shift;
    my $node = $self->tracker->get_seq( $_[0] );
    return wantarray ? () : "" unless ($node);
    my $nid  = $self->_cache_node( $node );
    unless ($self->{DESCS}{$nid}) {
        my $nids = $self->_get_nodes_for_type($nid, 'is a shorter term for');
        
        $self->{DESCS}{$nid} = [ sort { length($a) cmp length($b) }
                                 $self->_ids_to_names( $nids ) ];
    }
    return wantarray ? @{$self->{DESCS}{$nid}} : $self->{DESCS}{$nid}[0] || "";
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 description_to_node

 Title   : description_to_node
 Usage   : my @nodes = $ow->description_to_node($desc, $regex1, $regex2, ...);

 Function: Gets an node from descriptive text. This allows you to use
           the human description to get an ontology accession - for
           example, calling with 'protein kinase activity' should
           return 'GO:0004672'.

           Note however that descriptive text is very poorly
           controlled, and it is possible to recover multiple
           (potentially inappropriate) hits from the MapTracker
           database. For this reason, you may also include one or more
           regular expression tests to check against the recovered
           names. If you choose to do so, then all tests must match a
           node name for that node to be returned.

 Returns : An array of the nodes that matched

 Args    : [0] The descriptive text you wish to search on
               You may use SQL wildcards - leading wildcards will be slow
           [1..] Optional regular expressions

  Examples:

    # Get only one node that is described as 'molecular_function'
    my ($mf) = $ow->description_to_node('molecular_function');

    # Get all nodes that have descriptions starting with 'kinase', and
    # have accessions starting with 'GO:'. It is very important to use
    # the regexp here, because many entries in MapTracker begin with
    # 'kinase'
    my @kinase_nodes = $ow->description_to_node('kinase%', '^GO:');

=cut

sub description_to_node {
    my $self = shift;
    my $text = shift;
    my $mt = $self->tracker;
    my %seen;
    foreach my $tnode ($mt->get_seq($text)) {
        my $nids = $self->_get_nodes_for_type
            ($tnode->id, 'is a longer term for');
        map { $seen{$_} ||= $mt->get_seq($_) } @{$nids};
    }
    my @matches = values %seen;
    return () if ($#matches < 0);
    if ($#_ > -1) {
        my @good;
        foreach my $node (@matches) {
            my $name   = $node->name;
            my $isgood = 1;
            foreach my $re (@_) {
                if ($name !~ /$re/) {
                    $isgood = 0;
                    last;
                }
            }
            push @good, $node if ($isgood);
        }
        @matches = @good;
    }
    return $self->use_objects ? @matches : map {$_->name} @matches;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# Turn arguments into an array reference of relationship types
sub _types_to_array {
    my $self = shift;
    my $meth = shift;
    my $mt   = $self->tracker;
    my @requests;
    foreach my $request (@_) {
        if (ref($request) eq 'ARRAY') {
            push @requests, @{$request};
        } else {
            push @requests, $request;
        }
    }
    my @types;
    foreach my $req (@requests) {
        my ($type, $dir) = $mt->get_type($req);
        unless ($type) {
            $self->error("Setting $meth edge types: ".
                         "I am not aware of a relationship '$req'");
            next;
        }
        push @types, $type->reads($dir);
    }
    return $self->{TYPES}{$meth} = \@types;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# Build (and cache) a hash structure containing info on parent data for node
# This function also caches the child count value for a node
sub _parent_hash {
    my $self   = shift;
    my ($nid) = @_;
    unless ($self->{PARENTS}{$nid}) {
        my (%by_id, %by_depth, %has_root);
        # Get all immediate parents:
        my $pids    = $self->_get_nodes_for_type($nid, $self->{TYPES}{parent});
        # Note if this node is a root (no parents):
        my $is_root = $#{$pids} < 0 ? 1 : 0;
        foreach my $pid (@{$pids}) {
            # This parent is 1 removed from the request
            $by_id{$pid} = 1;
            # For each parent, recursively get its parent hash:
            my $phash  = $self->_parent_hash( $pid );
            map { $has_root{ $_ }++ } @{$phash->{roots}};
            my @pars   = keys %{$phash->{by_id}};
            foreach my $gpid (@pars) {
                # The depth relative to the request is +1 from the parent
                my $depth = $phash->{by_id}{$gpid} + 1;
                $by_id{$gpid} = $depth if 
                    (!$by_id{$gpid} || $by_id{$gpid} > $depth);
            }
        }
        # The stack was seeded with the query node - get rid of it
        delete $by_id{$nid};
        my $count = 0;
        while (my ($id, $depth) = each %by_id) {
            $by_depth{$depth} ||= [];
            push @{$by_depth{$depth}}, $id;
            $count++;
        }
        my @roots;
        if ($is_root) {
            # If the node is a root, list itself as the only root
            @roots = ( $nid );
        } else {
            @roots = sort { $has_root{$b} <=> $has_root{$a} } keys %has_root;
        }
        $self->{PARENTS}{$nid} = {
            count    => $count,
            is_root  => $is_root,
            roots    => \@roots,
            by_id    => \%by_id,
            by_depth => \%by_depth,
            self     => $nid,
        };
    }
    return $self->{PARENTS}{$nid};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# Build (and cache) a list of all kids directly attached to a node
sub _child_list {
    my $self = shift;
    my ($nid) = @_;
    unless ($self->{CHILDREN}{$nid}) {
        $self->{CHILDREN}{$nid} = $self->_get_nodes_for_type
            ($nid, $self->{TYPES}{child});
    }
    return $self->{CHILDREN}{$nid};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# Caches a BMS::MapTracker::Seqname object internally, returns the name_id
sub _cache_node {
    my $self = shift;
    my ($node) = @_;
    my $nid = $node->id;
    $self->{CACHE}{$nid} ||= $node;
    return $nid;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# Turns an array reference of name_ids into objects
sub _ids_to_objects {
    my $self = shift;
    return map { $self->{CACHE}{$_} } @{$_[0]};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# Turns an array reference of name_ids into strings (via object->name())
sub _ids_to_names {
    my $self = shift;
    return map { $self->{CACHE}{$_}->name } @{$_[0]};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# Given an array reference of name_ids, returns an array of objs or strings
sub _return_for_list {
    my $self = shift;
    my ($list) = @_;
    my @retval = $self->use_objects ? 
        $self->_ids_to_objects( $list) : $self->_ids_to_names( $list);
    return wantarray ? @retval : $retval[0];
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# Given a node request and an edge type, returns an array reference of all
# name_ids connected to the requested node by that type
sub _get_nodes_for_type {
    my $self = shift;
    my ($node, $types) = @_;
    my $mt = $self->tracker;
    my $edge_data = $mt->get_edge_dump( -name     => $node,
                                        -keeptype => $types,
                                        -orient   => 1, );
    my %new_nodes;
    
    map { $new_nodes{$_->[0]} ||= $mt->get_seq($_->[0]) } @{$edge_data};
    map { $self->_cache_node( $_ ) } values %new_nodes;
    return [ keys %new_nodes ];
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
1;
