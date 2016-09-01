# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::Services;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

$BMS::MapTracker::Services::VERSION = 
    ' $Id$ ';

use strict;
use Scalar::Util qw(weaken);
use vars qw(@ISA);
use BMS::MapTracker;
use BMS::MapTracker::Network;
use BMS::MapTracker::Shared;
use BMS::MapTracker::AccessDenorm;
use BMS::Branch;

@ISA = qw(BMS::MapTracker::Shared);

my $nocgi  = $ENV{'HTTP_HOST'} ? 0 : 1,
my $idf    = 'is denormalized from';
my $idt    = 'is denormalized to';
my $sf     = 'is a shorter term for';
my $peBit  = "TAG = 'Preferred Edge' AND VAL =";
my $dnBit  = "TAG = '#META_TAGS#Denormalization' AND ";
my $bbBit  = "$dnBit ( VAL = '#META_VALUES#Backbone' OR VAL = '#META_VALUES#Self Referential' )";
my $basedir = "/work5/tilfordc/maptracker/services";

my $vers   = "";
if ($BMS::MapTracker::Services::VERSION =~ /v (\d+\.\d+)/) {
    $vers = "Service.pm v$1";
}

my $debug = BMS::Branch->new
    ( -skipkey => ['TRACKER', 'TYPES','-tracker','CHILDREN',
                   '__HTML__', 'NETWORK', 'AUTHORITIES', 'CLASSES', 'TASKS',
                   'TAXA', ],
      -noredundancy => 1,
      -format => $nocgi ? 'text' : 'html', );



sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
        QUERIES  => {},
        BACKBONE => {},
        KIDBONE  => {},
        PARBONE  => {},
        NULLMSG  => 'No data found',
        NOSERV   => 'MapTracker Database is down',
    };
    bless ($self, $class);
    my $args = $self->parseparams
        ( -tracker   => undef,
          -username  => '',
          -verbose   => 0,
          -nocgi     => $ENV{'HTTP_HOST'} ? 0 : 1,
          -limit     => 0,
          -namespace => 'DENORM_LOCUS',
          -jump      => 0,
          -format    => 'tsv',
          @_ );
    my $mt   = $args->{TRACKER};
    if (defined $mt) {
        # Weaken link to external tracker objects
        weaken($self->{TRACKER} = $mt) if ($mt);
    } else {
        eval {
            $mt = BMS::MapTracker->new
                ( -dumpsql  => $args->{DUMPSQL},
                  -dumpfh   => *STDOUT, 
                  -dumplft  => $nocgi ? '' : "<pre>",
                  -dumprgt  => $nocgi ? "\n\n" : "</pre>\n",
                  -username => $args->{USERNAME},
                  -dbadmin  => $args->{DBADMIN},
                  -ishtml   => 1,
                  );
        };
        if ($mt) {
            $self->{TRACKER} = $mt;
        } else {
            return undef if ($args->{QUIETFAIL});
            $self->death("Failed to initialize MapTracker database handle");
        }
    }
    my $ad = BMS::MapTracker::AccessDenorm->new
        ( -tracker => $mt );
    # $ad->age(0);
    $self->{DENORM} = $ad;
    $self->verbose( $args->{VERBOSE} );
    $self->limit( $args->{LIMIT} );
    $self->namespace( $args->{NS} || $args->{NAMESPACE});
    $self->allow_jump( $args->{JUMP} );
    $self->format( $args->{FORMAT} || 'tsv');
    return $self;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 tracker

 Title   : tracker
 Usage   : $obj->tracker($name)
 Function: Gets the MapTracker object
 Returns : The MapTracker object assigned to this object
 Args    : 

=cut


sub tracker {
    my $self = shift;
    return $self->{TRACKER};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 denorm

 Title   : denorm
 Usage   : $obj->denorm($name)
 Function: Gets the denormalization object
 Returns : The denormalization object assigned to this object
 Args    : 

=cut


sub denorm {
    my $self = shift;
    return $self->{DENORM};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 format

 Title   : format
 Usage   : my $val = $obj->format( $newVal )
 Function: 
 Returns : 
 Args    : 

=cut


sub format {
    my $self = shift;
    if (my $nv = $_[0]) {
        if ($nv =~ /html/i || $nv =~ /clean/i) {
            $self->{FORMAT}   = $nv =~ /clean/i ? 'clean' : 'html';
            $self->{ROWFORM}  = "  <tr>\n%s\n  </tr>\n";
            $self->{CELLFORM} = "    <td>%s</td>";
            $self->{HEADFORM} = "    <th>%s</th>";
            $self->{CELLSEP}  = "\n";
            $self->{JOINER}   = '<br />';
        } elsif ($nv =~ /tsv/i) {
            $self->{FORMAT}   = 'tsv';
            $self->{ROWFORM}  = "%s\n";
            $self->{CELLFORM} = '%s';
            $self->{HEADFORM} = '%s';
            $self->{CELLSEP}  = "\t";
            $self->{JOINER}   = ',';
        } elsif ($nv =~ /js/i || $nv =~ /java/i) {
            $self->{FORMAT}   = 'js';
            $self->{ROWFORM}  = "[%s], ";
            $self->{CELLFORM} = '"%s"';
            $self->{HEADFORM} = '"%s"';
            $self->{CELLSEP}  = ",";
            $self->{JOINER}   = ',';
        } else {
            $self->error("I do not know how to set format() to '$nv'");
            $self->format('clean');
        }
    }
    return $self->{FORMAT};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 structure

 Title   : structure
 Usage   : $obj->structure
 Function: 
 Returns : 
 Args    : 

=cut


sub structure {
    my $self = shift;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 limit

 Title   : limit
 Usage   : $obj->limit($name)
 Function: Gets the MapTracker object
 Returns : The MapTracker object assigned to this object
 Args    : 

=cut


sub limit {
    my $self = shift;
    if (defined $_[0]) {
        if ($_[0] =~ /^\d+$/) {
            $self->{LIMIT} = $_[0];
        } else {
            $self->error("Failed to set limit to '".$_[0]."' - you must pass ".
                         "an integer value (zero to ignore limit)");
        }
    }
    return $self->{LIMIT};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 queries

 Title   : queries
 Usage   : my @seqnames = $obj->queries( @optionalNewQueries )
 Function: Adds / Gets the queries currently held
 Returns : An array of queries
 Args    : Optional array of new queries

=cut


*query = \&queries;
sub queries {
    my $self = shift;
    my $mt   = $self->tracker();
    my @rv;
    if ($mt) {
        foreach my $req (@_) {
            next unless ($req);
            my @seqs = $mt->get_seq($req);
            foreach my $seq (@seqs) {
                $self->{QUERIES}{$seq->id} ||= $seq;
                $self->{QUERYNAMES}{$seq->name}++;
            }
            if ($#seqs < 0) {
                $self->error("Could not find database entry for '$req'")
                    if ($self->verbose);
            }
        }
    } else {
        # MapTracker offline, can only store names
        foreach my $req (@_) {
            if (ref($req)) {
                $self->{QUERYNAMES}{$req->name}++;
            } else {
                $self->{QUERYNAMES}{$req}++;
            }
        }
    }
    return values %{$self->{QUERIES}};
}

sub query_names {
    return keys %{shift->{QUERYNAMES}};
}

*user_query = \&user_queries;
sub user_queries {
    my $self = shift;
    my $mt   = $self->tracker();
    return unless ($mt);
    foreach my $req (@_) {
        next unless ($req);
        my @seqs = $mt->get_seq($req);
        foreach my $seq (@seqs) {
            $self->{USER_QUERIES}{ $seq->id } = $seq;
        }
    }
}

sub js_for_user_queries {
    my $self = shift;
    my $mt   = $self->tracker;
    return unless ($mt);
    foreach my $id (keys %{$self->{USER_QUERIES}}) {
        $mt->register_javascript_literal
            ( "{ type:'node', id:'$id', isQuery:true}" );
    }
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 clear_queries

 Title   : clear_queries
 Usage   : $obj->clear_queries()
 Function: Clears the stored list of queries
 Returns : 
 Args    : 

=cut

*clear_query = \&clear_queries;
sub clear_queries {
    my $self = shift;
    my ($user) = @_;
    $self->{QUERIES} = {};
    $self->{QUERYNAMES} = {};
    if ($user) {
        # Also clear user queries
        $self->{USER_QUERIES} = {};
    }
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 verbose

 Title   : verbose
 Usage   : my $vb = $obj->verbose( $newValue )
 Function: 
 Returns : 
 Args    : 

=cut


sub verbose {
    my $self = shift;
    if (defined $_[0]) {
        $self->{VERBOSE} = $_[0] ? 1 : 0;
    }
    return $self->{VERBOSE};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 namespace

 Title   : namespace
 Usage   : my $ns = $obj->namespace( $newValue )
 Function: Sets / Gets the denormailzed namespace to search
 Returns : The namespace being used
 Args    : 

=cut


sub namespace {
    my $self = shift;
    if (defined $_[0]) {
        my $mt = $self->tracker;
        return undef unless ($mt);
        my $ns = $mt->get_namespace( $_[0] );
        if ($ns) {
            $self->{NS} = $ns;
            $self->{NST} = '#' . $ns->name . '#';
        } else {
            $self->error("Failed to find a namespace called '$_[0]'");
        }
    }
    return $self->{NS};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 null_message

 Title   : null_message
 Usage   : $obj->null_message
 Function: 
 Returns : 
 Args    : 

=cut


sub null_message {
    my $self = shift;
    if (defined $_[0]) {
        $self->{NULLMSG} = $_[0];
    }
    return $self->{NULLMSG};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 no_service

 Title   : no_service
 Usage   : $obj->no_service
 Function: 
 Returns : 
 Args    : 

=cut


sub no_service {
    my $self = shift;
    if (defined $_[0]) {
        $self->{NOSERV} = $_[0];
    }
    return $self->{NOSERV};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 allow_jump

 Title   : allow_jump
 Usage   : my $isJumpAllowed = $obj->allow_jump( $newValue )

 Function: Sets / Gets the flag that allows jumping. If jumping is
           allowed, then once a backbone is entered it can be
           connected to another one via connections between backbone
           nodes.

 Returns : The current value (0 or 1)
 Args    : Optional new value

=cut


sub allow_jump {
    my $self = shift;
    if (defined $_[0]) {
        $self->{DOJUMP} = $_[0] ? 1 : 0;
    }
    return $self->{DOJUMP};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 direct_backbone

 Title   : direct_backbone
 Usage   : my @nodes = $obj->direct_backbone( @optionalList )
 Function: Finds the denormalized branch nodes for the queries
 Returns : A list of the branch nodes

    Args : Optional list of nodes used as starting point for
           search. If not provided, then all queries will be
           used. Note that the backbones that you get for any given
           query will be conditional on the denormaliztion used, and
           whether you are allowing jumping.

=cut

sub direct_backbone {
    my $self = shift;
    $self->benchstart;
    my $mt    = $self->tracker;
    return () unless ($mt);
    my $nst   = $self->{NST};
    my $aj    = $self->allow_jump();
    my $key   = "$nst-$aj";
    my $limit = $self->limit;

    my @requests = $self->_args_to_requests( @_ );
    if ($#requests < 0) {
        $self->error("Attempt to direct_backbone() without any queries")
            if ($self->verbose);
        $self->benchend;
        return ();
    }

    # See if any of these requests were already processed:
    my (%hits, @need2do, %query);
    foreach my $req (@requests) {
        $query{ $req->id } = 1;
        my $found = $self->{ BACKBONE }{ $key }{ $req->id };
        if ($found) {
            # We have already identified the backbone nodes
            map { $hits{ $_->id } = $_ } @{$found};
        } else {
            push @need2do, $req;
        }
    }

    my $nw = $self->network();
    foreach my $node (@need2do) {
        my $edgeList = $mt->get_edge_dump
            ( -name     => $node,
              -return   => 'object array',
              -keeptype => "$nst$idt",
              -limit    => $limit * 5 );
        my @edges = sort { $a->node2->id <=> $b->node2->id } @{$edgeList};
        # Note all the backbone nodes observed
        my %bbNodes = map { $_->node2->id => 1 } @edges;
        my (%keeping);
        my $keepcount = 0;
        foreach my $edge (@edges) {
            # warn $edge->to_text_short;
            $nw->add_edge($edge);
            my $to  = $edge->node2;
            my $tid = $to->id;
            next if (exists $keeping{$tid});
            if ($query{ $tid }) {
                # Always keep query nodes
                $keeping{ $tid } = $to;
                $keepcount++;
            } elsif (!$limit || $keepcount < $limit) {
                # Either there is no limit, or we have not reached it yet
                if ($aj || ! $bbNodes{ $edge->node1->id }) {
                    # When jumping is allowed, keep all nodes

                    # Otherwise, keep only if the 'from' node is not
                    # itself a backbone node.

                    $keeping{ $tid } = $to;
                    $keepcount++;
                }
            }
        }
        my @nodes = values %keeping;
        $self->{ BACKBONE }{ $key }{ $node->id } = \@nodes;
        map { $hits{ $_->id } = $_ } @nodes;
    }
    $self->benchend;
    return sort {$a->id <=> $b->id } values %hits;
}

sub _args_to_names {
    my $self = shift;
    # No specific request, use all queries
    return $self->query_names if ($#_ < 0 );
    my %names;
    if (my $mt = $self->tracker) {
        # use standard MapTracker interface - helps normalize case
        my @objs = $self->_args_to_requests( @_ );
        %names = map { $_->name => 1 } @objs;
    } else {
        %names = map { $_ => 1 } @_;
        $self->queries( @_ );
    }
    return keys %names;
}

sub _args_to_requests {
    my $self = shift;
    # No specific request, use all queries
    return $self->queries if ($#_ < 0 );
    # The user is requesting information for specific nodes
    my $mt = $self->tracker;
    # Without MapTracker online we can not do anything:
    return () unless ($mt);

    my %objects;
    foreach my $req (@_) {
        my @objs = $mt->get_seq($req);
        map { $objects{$_->id} = $_ } @objs;
    }
    my @requests = values %objects;
    # Add the new requests to the query list
    $self->queries(@requests);
    return @requests;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 child_backbone

 Title   : child_backbone
 Usage   : my @nodes = $obj->child_backbone( @queries )

 Function: Gets all backbone nodes that are directly associated with
           the queries (by calling direct_backbone) as well as all
           nodes that are children (denormalized from) those nodes.

 Returns : A list of backbone nodes
    Args : Optional list of nodes used as starting point for
           search. If not provided, then all queries will be
           used. Note that the backbones that you get for any given
           query will be conditional on the denormaliztion used, and
           whether you are allowing jumping.

=cut

sub child_backbone {
    my $self  = shift;
    $self->benchstart;
    my @nodes = $self->_expand_backbone( 'KID', @_ );
    $self->benchend;
    return @nodes;
}

sub parent_backbone {
    my $self  = shift;
    $self->benchstart;
    my @nodes = $self->_expand_backbone( 'PAR', @_ );
    $self->benchend;
    return @nodes;
}

sub full_backbone {
    my $self  = shift;
    $self->benchstart;
    my %hits;
    map { $hits{ $_->id } = $_ } $self->child_backbone( @_ );
    map { $hits{ $_->id } = $_ } $self->parent_backbone( @_ );
    $self->benchend;
    return values %hits;
}

sub _expand_backbone {
    my $self  = shift;
    my $dir   = shift;
    my $mt    = $self->tracker;
    return () unless ($mt);
    my @nodes = $self->direct_backbone( @_ );
    return @nodes if ($#nodes < 0);
    
    $self->benchstart;
    my $nw    = $self->network;
    my $nst   = $self->{NST};

    my (%hits, @need2do);
    # $mt->{DEBUG} = 1;
    foreach my $node (@nodes) {
        my $found = $self->{ $dir.'BONE' }{ $nst }{ $node->id };
        if ($found) {
            # We have already identified the backbone nodes
            map { $hits{ $_->id } = $_ } @{$found};
        } else {
            push @need2do, $node;
        }        
    }

    if ( $#need2do > -1 ) {
        # Expand in bulk:
        my ($rf, $rb) = $dir eq 'KID' ? ($idf, $idt) : ($idt, $idf);
        $nw->add_root( @need2do );
        my @full = $nw->expand
            ( -node     => \@need2do,
              -keeptype => "$nst$rf",
              -filter   => $bbBit,
              -groupat  => 1000,
              -recurse  => 5 );
        my $nsid = $self->namespace->id;
        my (%found, %isParent);
        # Then dissect out the edges associated with each query:
        foreach my $newNode (@full) {
            foreach my $edge ( $nw->edges_from_node( $newNode, $rb )) {
                # warn $edge->to_text_short;
                if ($edge->namespace->id == $nsid) {
                    my $node = $edge->other_node( $newNode );
                    my ($qID, $newID) = ($node->id, $newNode->id);
                    $found{ $qID }{ $newID } = $newNode;
                    unless ($qID == $newID) {
                        # No circularity
                        $isParent{1}{ $qID }{ $newID } = 1;
                    }
                }
            }
        }

        # Determine indirect parentage
        my $depth    = 1;
        my $continue = 1;
        # Consider all nodes that have parentage at this depth:
        while ($continue) {
            $continue = 0;
            foreach my $pid (keys %{$isParent{ $depth }} ) {
                my %nextDepth;
                foreach my $kid (keys %{$isParent{ $depth }{ $pid }} ) {
                    next unless (exists $isParent{ 1 }{ $kid });
                    map { $nextDepth{$_} = 1 } keys %{$isParent{ 1 }{ $kid }};
                }
                # No circularity:
                delete $nextDepth{$pid};
                my @gkids = keys %nextDepth;
                next if ($#gkids < 0);
                # We found a new level - make note
                $continue = 1;
                map { $isParent{ $depth + 1 }{ $pid }{$_} = 1 } @gkids;
                map { $found{ $pid }{ $_ } ||= $mt->get_seq($_) } @gkids;
                #printf("%d has children %s at depth %d\n", $pid, join(',', @gkids), $depth);
            }
            $depth++;
        }
        #$debug->branch(\%isParent);
        
        
        foreach my $node (@need2do) {
            my $hash = $found{ $node->id } || {};
            my @others = values %{ $hash };
            $self->{ $dir.'BONE' }{ $nst }{ $node->id } = \@others;
            map { $hits{ $_->id } = $_ } @others;
        }
    }
    $self->benchend;
    return sort {$a->id <=> $b->id } values %hits;
}

sub backbone_root {
    my $self  = shift;
    my $mt    = $self->tracker;
    return wantarray ? () : undef unless ($mt);
    $self->benchstart;
    my @reqs  = $self->_args_to_requests( @_ );
    $self->parent_backbone(@reqs);
    my $nw    = $self->network;
    
    my %seen;
    my @roots;
    while (my $node = shift @reqs) {
        my $nid = $node->id;
        next if ($seen{ $nid }++);
        my @parents;
        foreach my $up ($nw->nodes_from_node( $node, $idt)) {
            push @parents, $up unless ($up->id == $nid);
        }
        if ($#parents < 0) {
            # No parents - this is a root node
            push @roots, $node;
        } else {
            # Not a root, recurse through its parents
            push @reqs, @parents;
        }
    }

    $self->benchend;
    return wantarray ? @roots : $roots[0];
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 clear_backbone

 Title   : clear_backbone
 Usage   : $obj->clear_backbone
 Function: Clears backbone node information
 Returns : 
 Args    : 

=cut


sub clear_backbone {
    my $self = shift;
    foreach my $key ('BACK', 'KID', 'PAR') {
        $self->{ $key.'BONE' } = {};
    }
    return 1;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 nodes_from

 Title   : nodes_from
 Usage   : $obj->nodes_from( @args )
 Function: Find nodes that are denormalized from the query
 Returns : A list of node objects
 Args    : Associative array of arguments. Recognized keys [Default]:

=cut


sub get_nodes_from {
    my $self = shift;
    $self->benchstart;
    my @rv = $self->_nodes_from_to(1, @_);
    $self->benchend;
    return @rv;
}

sub get_nodes_to {
    my $self = shift;
    $self->benchstart;
    my @rv = $self->_nodes_from_to(0, @_);
    $self->benchend;
    return @rv;
}
sub _nodes_from_to {
    my $self  = shift;
    my $index = shift;
    my $mt    = $self->tracker;
    return () unless ($mt);
    my $nst   = $self->{NST};
    my $et    = $index ? $idt : $idf;

    my $args  = $self->parseparams( -query     => undef,
                                    -keepclass => undef,
                                    -keeptype  => undef,
                                    -tosstype  => undef,
                                    -return    => 'nodes',
                                    @_);

    my $req = $args->{QUERY} || $args->{NODES} || $args->{NODE};
    my @queries;
    if ($req) {
        my @reqs = (ref($req) && ref($req) eq 'ARRAY') ? @{$req} : ($req);
        foreach my $req (@reqs) {
            push @queries, $mt->get_seq($req);
        }
    } else {
        @queries = $self->queries;
    }
    
    my @type_filter;
    foreach my $filt ('KEEP', 'TOSS') {
        my $tf = $args->{$filt.'TYPE'};
        next unless ($tf);
        my @arr = ref($tf) ? @{$tf} : ($tf);
        my %hash;
        foreach my $treq (@arr) {
            my ($type, $dir) = $mt->get_type($treq);
            if ($type) {
                my @reads = $dir ? $type->reads($dir) : $type->reads();
                map { $hash{'#READS_AS#'.$_} = 1 } @reads;
            }
        }
        my $val = join("' , '", sort keys %hash);
        next unless ($val);
        my $op  = $filt eq 'KEEP' ? 'IN' : 'NOT IN';
        push @type_filter, "( TAG = 'Preferred Edge' AND VAL $op ( '$val' ) )";
    }
    my $filter = join(" AND ", @type_filter);
    

    my $edges = $mt->get_edge_dump
        ( -name      => \@queries,
          -return    => 'object array',
          -keeptype  => "$nst$et",
          -keepclass => $args->{KEEPCLASS},
          -filter    => $filter, );

    my %rv;
    my $asEdge = lc($args->{RETURN}) =~ /edge/ ? 1 : 0;
    foreach my $edge (@{$edges}) {
        my $obj = $edge;
        unless ($asEdge) {
            my @nodes = $edge->nodes;
            $obj = $nodes[ $index ];
        }
        $rv{$obj->id} = $obj;
    }
    return sort { $a->id <=> $b->id } values %rv;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 network

 Title   : network
 Usage   : $obj->network
 Function: 
 Returns : 
 Args    : 

=cut


sub network {
    my $self = shift;
    unless ($self->{NETWORK}) {
        my $mt = $self->tracker();
        return undef unless ($mt);
        $self->{NETWORK} = BMS::MapTracker::Network->new
            ( -tracker => $mt );
    }
    return $self->{NETWORK};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 clear_network

 Title   : clear_network
 Usage   : $obj->clear_network
 Function: 
 Returns : 
 Args    : 

=cut


sub clear_network {
    my $self = shift;
    $self->{NETWORK} = undef;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 loci

 Title   : loci
 Usage   : my @nodes = $obj->loci( @optionalQueries )
 Function: Select out nodes that are LocusLink loci
 Returns : A list of nodes

    Args : Optional list of queries - if not provided, then all the
           nodes in the current network will be used.

=cut


sub loci {
    my $self = shift;
    $self->benchstart;
    my @requests;
    if ($#_ > -1 ) {
        my $mt = $self->tracker;
        return () unless ($mt);
        foreach my $req (@_) {
            my $obj = $mt->get_seq($req);
            push @requests, $obj if ($obj);
        }
    } else {
        @requests = $self->network->each_node();
    }
    my @loci;
    foreach my $node (@requests) {
        push @loci, $node if ($node->is_class('locus') && 
                              $node->name =~ /^LOC\d+$/);
    }
    $self->benchend;
    return @loci;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 locus_table

 Title   : locus_table
 Usage   : my $text = $obj->locus_table( @optionalQueries )
 Function: 
 Returns : 
 Args    : 

=cut


sub locus_table {
    my $self = shift;
    $self->benchstart;
    my @full = $self->full_backbone( @_ );
    my %data  = map { $_->id => $_ } @full;
    my @loci = $self->loci( @full );
    my $nw   = $self->network;
    my $nst  = $self->{NST};
    my $frm  = $self->format;

    my @cols = ('Locus', 'Symbol', 'Taxa', 'Description', 'RNA', 'Protein');

    # Get reliable aliases for the loci
    $self->benchstart('get_locus_reliable');
    $nw->add_root( @loci );
    $nw->expand( -node => \@loci,
                 -keeptype => "$nst$idf",
                 -filter   => "$peBit 'is reliably aliased by'",
                 -groupat  => 1000,
                 -recurse  => 0, );
    $self->benchend('get_locus_reliable');

    my @rows;
    foreach my $loc (@loci) {
        my @syms;
        foreach my $edge ($nw->edges_from_node($loc)) {
            my $onode = $edge->other_node($loc);
            if ($onode->is_class('genesymbol')) {
                push @syms, $onode->name;
            }
        }
        my $desc = $loc->desc;
        map { $data{$_->id} = $_ } $loc->desc;
        if ($desc) {
            $desc = $desc->name;
            $desc = $self->escape($desc)
                if ($frm eq 'html' || $frm eq 'clean');
        }
        my @row  = ($self->format_node($loc), join(",", @syms), 
                    $self->species_for_node($loc), $desc  );

        my (@rnas, @prots);
        foreach my $le ($nw->edges_from_node($loc, $idf )) {
            my ($ldn) = $le->has_tag('Denormalization');
            next unless ($ldn->num == 0);
            my $rna = $le->node1;
            $data{ $rna->id } = $rna;
            push @rnas, $self->format_node( $rna );
            foreach my $re ($nw->edges_from_node($rna, $idf )) {
                my ($rdn) = $re->has_tag('Denormalization');
                next unless ($rdn->num == 0);
                my $prot = $re->node1;
                $data{ $prot->id } = $prot;
                push @prots, $self->format_node( $prot );
            }
        }
        push @row, ($self->join_set(@rnas), $self->join_set(@prots));
        push @rows, \@row;
    }
    $self->benchend;
    return $self->format_table( \@rows, \@cols, \%data);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 go_table

 Title   : go_table
 Usage   : my $text = $obj->go_table( @queries )
 Function: 
 Returns : 
 Args    : 

=cut


sub go_table {
    my $self  = shift;
    my $ad    = $self->denorm;
    return $self->format_table([ [ $self->no_service ] ]) unless ($ad);
    $self->benchstart;
    my @cols  = 
        ('Accession', 'Description', 'Subset', 'Parentage', 'Evidence');
    my @requests = $self->_args_to_names( @_ );
    my %gos;
    foreach my $req (@requests) {
        my $ans   = $ad->guess_namespace( $req );
        my $godat = $ad->assignments
            ( -acc => $req, -ans => $ans, -ons => 'GO');
        foreach my $row (@{$godat}) {
            my ($acc, $onto, $ec, $matched, $acns, $onns, $subset, $adesc, $odesc, $parent) = @{$row};
            next unless ($onto);
            $gos{$onto}{ec}{$ec}++;
            $gos{$onto}{desc}{$odesc}++;
            $gos{$onto}{subset}{$subset}++;
            $gos{$onto}{par}{$parent}++;
        }
    }
    my @rows;
    foreach my $go (sort keys %gos) {
        my $dat = $gos{$go};
        my @row = ($self->format_node( $go ), 
                   $self->join_set(sort keys %{$dat->{desc}}),
                   $self->join_set(sort keys %{$dat->{subset}}),
                   );
        # Get the smallest parentage value:
        my ($par) = sort { $a <=> $b } keys %{$dat->{par}};
        my @ecs = map { $self->format_node( $_, undef, 'Evidence_Codes' ) 
                        } sort keys %{$dat->{ec}};
        push @row, ($par, $self->join_set( @ecs ) );
        push @rows, \@row;
    }
    $self->benchend;
    return $self->format_table( \@rows, \@cols, {});
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 backbone_for_query

 Title   : backbone_for_query
 Usage   : my $text = $obj->backbone_for_query( @queries )
 Function: 
 Returns : 
 Args    : 

=cut

sub backbone_for_query {
    my $self  = shift;
    my $mt    = $self->tracker;
    return () unless ($mt);
    $self->benchstart;
    my @reqs  = $self->_args_to_requests( @_ );
    my @bbs   = $self->direct_backbone( @reqs );
    my $nw    = $self->network;

    my @rows;
    foreach my $query (@reqs) {
        foreach my $edge ($nw->edges_from_node($query, $idt)) {
            my $onode = $edge->other_node($query);
            next if ($onode->id == $query->id);
            push @rows, [ $query, $onode, $edge ];
        }
    }
    $self->benchend;
    return @rows
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 loci_for_go

 Title   : loci_for_go
 Usage   : my $text = $obj->loci_for_go( @queries )
 Function: 
 Returns : 
 Args    : 

=cut

sub loci_for_go {
    my $self  = shift;
    my $mt    = $self->tracker;
    return $self->format_table([ [ $self->no_service ] ]) unless ($mt);
    $self->benchstart;

    my @bdat  = $self->backbone_for_query( @_ );
    my %struct;
    
    foreach my $bd (@bdat) {
        my ($query, $other, $edge) = @{$bd};
        # warn $edge->to_text_short;
        next unless ($query->is_class('go'));
        my $qid = $query->id;
        my $oid = $other->id;
        foreach my $root ($self->backbone_root( $other )) {
            next unless ($root->is_class('locus'));
            my $rid = $root->id;
            push @{$struct{$qid}{$rid}{edges}}, $edge;
            $struct{$qid}{$rid}{via}{ $oid } = $other;
        }
    }

    my @cols = ('GO ID', 'GO Term', 'Locus', 'Locus Description', 'Species', 'Via', 'Evidence', 'Reference');
    my (@rows, %data);
    my @gos = sort { $a->name cmp $b->name } 
    map { $mt->get_seq($_) } keys %struct;

    foreach my $go (@gos) {
        my $gid = $go->id;
        my $gd  = $go->desc ? $go->desc->name : '';
        my @locs = sort { $a->name cmp $b->name } 
        map { $mt->get_seq($_) } keys %{$struct{$gid}};
        foreach my $loc (@locs) {
            my $desc = $loc->desc;
            my @row = ( $self->format_node( $go ), $gd,
                        $self->format_node($loc),
                        $desc ? $desc->name : '', 
                        $self->species_for_node( $loc ), );
            my $lid = $loc->id;
            my @vias = values %{$struct{$gid}{$lid}{via}};
            @vias = () if ($#vias == 0 && $vias[0]->id == $lid);
            map { $data{ $_->id } = $_ } ( $loc, $go, @vias);
            @vias = map { $self->format_node( $_ ) } @vias;
            push @row, $self->join_set(@vias);

            my @edges = @{$struct{$gid}{$lid}{edges}};
            my (%tags);
            foreach my $edge (@edges) {
                foreach my $ec ($edge->has_tag('GO Evidence')) {
                    $tags{ec}{ $ec->val->id } = $ec->val;
                }
                foreach my $ref ($edge->has_tag('Referenced In')) {
                     $tags{ref}{ $ref->val->id } = $ref->val;
                }
            }
            foreach my $tn ('ec', 'ref') {
                my $hash = $tags{$tn} || {};
                my @nodes = sort { $a->name cmp $b->name } values %{$hash};
                my @links = map { $self->format_node($_) } @nodes;
                push @row, $self->join_set(@links);
            }

            push @rows, \@row;
        }
    }
    $self->benchend;
    return $self->format_table( \@rows, \@cols, \%data);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 primary_sequences

 Title   : primary_sequences
 Usage   : $obj->primary_sequences
 Function: 
 Returns : 
 Args    : 

=cut


sub primary_sequences {
    my $self  = shift;
    my $ad    = $self->denorm;
    return $self->format_table([ [ $self->no_service ] ]) unless ($ad);
    $self->benchstart;
    
    my @stack;
    foreach my $req ($self->_args_to_names( @_ )) {
        my $ns = $ad->guess_namespace( $req );
        push @stack, [ $req, $ns ] if ($ns);
    }

    my (%found, %done);
    while (my $dat = shift @stack) {
        my ($name, $ns) = @{$dat};
        next if ($done{$name}++);
        my $rows = [];
        if ($ns eq 'LL') {
            # Get RNAs
            $rows = $ad->convert( -id => $name, -ns1 => $ns, -ns2 => 'AR');
        } elsif ($ns eq 'AR' || $ns eq 'RSR') {
            # Get Proteins
            $rows = $ad->convert( -id => $name, -ns1 => $ns, -ns2 => 'AP');
        }
        my %nonredun;
        map { $nonredun{$_->[0]}{$_->[1]}++;
              $found{$_->[0]}{auth}{$_->[2]}++ } @{$rows};
        while (my ($kid, $nshash) = each %nonredun) {
            my ($ns2) = $ad->simplify_namespaces
                ( $ad->namespace_parents
                  ( keys %{$nshash}, $ad->guess_namespace($kid) ) );
            $found{$kid}{ns}{$ns2}++;
            $found{$kid}{par}{$name}++;
            push @stack, [$kid, $ad->namespace_token($ns2) ];
        }
    }

    my (@rows, %data);
    my @cols = ('Accession','Class', 'Description', 'Authority', 'Source');
    foreach my $acc (sort keys %found) {
        my @ns   = $ad->simplify_namespaces( keys %{$found{$acc}{ns}} );
        my @auth = map {$self->format_authority($_)} sort
            $ad->simplify_authors( keys %{$found{$acc}{auth}} );
        my @source =  map {$self->format_node($_)} sort
            keys %{$found{$acc}{par}};
        my @desc = $ad->description( -id => $acc, -ns => $ns[0] );
        my @row  = ( $self->format_node( $acc ), join(", ", @ns),
                     $self->join_set(@desc), join(", ", @auth),
                     $self->join_set(@source) );
        push @rows, \@row;
    }
    $self->benchend;
    return $self->format_table( \@rows, \@cols, \%data);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 secondary_sequences

 Title   : secondary_sequences
 Usage   : $obj->secondary_sequences
 Function: 
 Returns : 
 Args    : 

=cut


sub secondary_sequences {
    my $self  = shift;
    my $mt    = $self->tracker;
    return $self->format_table([ [ $self->no_service ] ]) unless ($mt);
    $self->benchstart;
    my @bbs   = $self->child_backbone( @_ );
    my $nst   = $self->{NST};

    my @cols = ('Secondary', 'Relation','Primary');
    my @bios;
    foreach my $node ( sort {$a->name cmp $b->name }@bbs) {
        push @bios, $node if ($node->is_class('bio'));
    }
    my %data  = map { $_->id => $_ } @bios;

    $self->benchstart('get_secondary_seq');
    my $edges = $mt->get_edge_dump
        ( -name      => \@bios,
          -return    => 'object array',
          -keeptype  => "$nst$idf",
          -tossclass => 'gi',
          -keepclass => ['rna','protein', 'ipi'], );
    $self->benchend('get_secondary_seq');

    my %primary = map { $_->id => $_ } @bbs;
    my %secondary;
    foreach my $edge (@{$edges}) {
        my ($sec, $pri) = $edge->nodes;
        next if ($primary{$sec->id});
        my $sid = $sec->id;
        my $pid = $pri->id;
        $secondary{$sid}{node} ||= $sec;
        $secondary{$sid}{pri}{$pid}{node} ||= $pri;
        my ($rt) = $edge->has_tag('Preferred Edge');
        $secondary{$sid}{pri}{$pid}{reads}{$rt->valname}++ if ($rt);
    }
    my %done;
    my @rows;
    foreach my $data (sort { $a->{node}->name 
                                 cmp $b->{node}->name} values %secondary) {
        my $sec = $data->{node};
        my $sn  = $sec->name;
        $sn =~ s/\.\d+$//;
        next if ($done{$sn}++);
        $data{ $sec->id } = $sec;
        my @pds = sort { $a->{node}->name 
                             cmp $b->{node}->name} values %{$data->{pri}};
        foreach my $pdat (@pds) {
            my @reads = $pdat->{reads} ? sort keys %{$pdat->{reads}} : ();
            push @rows, [ $self->format_node($sec), $self->join_set(@reads), 
                          $self->format_node($pdat->{node})];
        }
    }
    $self->benchend;
    return $self->format_table( \@rows, \@cols, \%data);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 homologs

 Title   : homologs
 Usage   : my $text = $obj->homologs
 Function: 
 Returns : 
 Args    : 

=cut

*homologues = \&homologs;
sub homologs {
    my $self  = shift;
    my $mt    = $self->tracker;
    return $self->format_table([ [ $self->no_service ] ]) unless ($mt);
    $self->benchstart;
    my @bbs   = $self->child_backbone( @_ );
    my $nst   = $self->{NST};

    # TODO Add option to get parents, too
    $self->benchstart('get_homologues');
    my $edges = $mt->get_edge_dump
        ( -name      => \@bbs,
          -return    => 'object array',
          -keeptype  => "$nst$idf",
          -filter   => "$peBit 'is homologous to'" );
    $self->benchend('get_homologues');

    my %stats;
    my @info;
    foreach my $edge (@{$edges}) {
        my ($homo, $ref) = $edge->nodes;
        my %hash;
        foreach my $tag ($edge->each_tag) {
            my $tn = $tag->tagname;
            next unless ($tn =~ /^Align/ || $tn =~ /^Ka/);
            my $num = $tag->num;
            if ($tn =~ /^Ka/) {
                $num = int (0.5 + 1000 * $num)/1000;
            } else {
                $num = int(10000 * $tag->num)/100 . '%';
            }
            #if ($tag->valname eq 'Reciprocal Best Match') {
            #    $num = "<td class='rbm'>$num</td>";
            #}
            $stats{$tn}++;
            $hash{$tn} = $num;
        }
        push @info, [ $homo, $ref, \%hash ];
    }
    my @statcols = sort keys %stats;    
    my @cols = ('Homolog', 'Species', 'Primary', @statcols);
    my (@rows, %done, %data);
    foreach my $dat ( sort { $a->[1]->name cmp $b->[1]->name ||
                             $a->[0]->name cmp $b->[0]->name } @info) {
        my ($homo, $ref, $stats) = @{$dat};
        my $hname = $homo->name;
        $hname =~ s/\.\d+$//;
        next if ($done{$hname}++);
        $data{ $homo->id } = $homo;
        $data{ $ref->id } = $ref;
        my @row = ( $self->format_node($homo), $self->species_for_node($homo),
                    $self->format_node($ref));
        foreach my $col (@statcols) {
            my $val = $stats->{$col};
            push @row, defined $val ? $val : '';
        }
        push @rows, \@row;
    }
    $self->benchend;
    return $self->format_table( \@rows, \@cols, \%data);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 comment_table

 Title   : comment_table
 Usage   : $obj->comment_table
 Function: 
 Returns : 
 Args    : 

=cut


sub comment_table {
    my $self = shift;
    $self->benchstart;
    my $nw    = $self->network;
    my @cols  = ('Feature', 'PubMed', 'Comment' );
    my @nodes = sort { $a->name cmp $b->name } $self->child_backbone( @_ );
    $nw->expand( -node => \@nodes,
                 -keeptype => 'has comment' );
    my (@rows, %data);
    foreach my $node (@nodes) {
        my @coms = $nw->edges_from_node($node, 'has comment');
        next if ($#coms < 0);
        $data{ $node->id } = $node;
        foreach my $edge (@coms) {
            my @pmids = map { $_->val } $edge->has_tag('Referenced In');
            my $com   = $edge->node2;
            map { $data{ $_->id } = $_ } @pmids;
            my $ptxt = $self->join_set( map { $self->format_node($_) } @pmids);
            push @rows, [ $self->format_node($node), $ptxt, $com->name ];
        }
    }
    $self->benchend;
    return $self->format_table( \@rows, \@cols, \%data);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 probe_table

 Title   : probe_table
 Usage   : $obj->probe_table
 Function: 
 Returns : 
 Args    : 

=cut


sub probe_table {
    my $self  = shift;
    my $ad    = $self->denorm();
    return $self->format_table([ [ $self->no_service ] ]) unless ($ad);
    $self->benchstart;

    my @requests = $self->_args_to_names( @_ );

    my %queryNamespace;
    foreach my $req (@requests) {
        my $ns1   = $ad->guess_namespace( $req );
        if ($ns1 eq 'AR' || $ns1 eq 'RSR') {
            $queryNamespace{$req}{$ns1}++;
        } else {
            my $rnas = $ad->convert
                ( -id => $req, -ns1 => $ns1, -ns2 => 'AR', -min => 0.8 );
            foreach my $row (@{$rnas}) {
                my ($racc, $rns) = @{$row};
                $queryNamespace{$racc}{$ad->namespace_token($rns)}++;
            }
        }
    }
    my @rdat;
    while (my ($racc, $hash) = each %queryNamespace) {
        my $ns = $ad->guess_namespace( $racc );
        ($ns)  = keys %{$hash} unless ($ns);
        push @rdat, [$racc, $ns];
    }

    my %results;
    foreach my $dat (@rdat) {
        my ($id, $ns1) = @{$dat};
        if ($ns1 eq 'RSR' || $ns1 eq 'AR') {
            # Get affy probes
            my $affy = $ad->convert
                ( -id => $id, -ns1 => $ns1, -ns2 => 'APS');
            foreach my $row (@{$affy}) {
                my ($term, $ns2, $auth, $match) = @{$row};
                $results{$id}{$ns2}{$term}{$match || -1}{$auth}++;
            }
        }
    }

    my @cols  = ('Feature', 'Probe', 'Type', 'Member of', 'Matched','Authority' );
    my (@rows, %data);
    foreach my $id (sort keys %results) {
        foreach my $ns2 (sort keys %{$results{$id}}) {
            foreach my $term (sort keys %{$results{$id}{$ns2}}) {
                my @sets;
                if ($ns2 eq 'Affy Probe Set') {
                    @sets = sort $ad->convert
                        ( -id => $term, -ns1 => $ns2, -ns2 => 'AAD');
                }
                my $stxt = join(", ", map { $self->format_node($_) } @sets);
                my @m    = sort {$b <=> $a} keys %{$results{$id}{$ns2}{$term}};
                my @row  = ( $self->format_node($id), 
                             $self->format_node($term), $ns2, $stxt );
                my %ahash = map { $_ => 1 } map 
                { keys %{$results{$id}{$ns2}{$term}{$_}} } @m;
                my @auths = map {$self->format_authority($_)} sort keys %ahash;
                my $match = $m[0];
                $match = ($match < 0) ? 
                "" : sprintf("%d%%", 0.5 + 100 * $match);
                push @row, ($match, $self->join_set(@auths));
                push @rows, \@row;
            }
        }
    }
    $self->benchend;
    return $self->format_table( \@rows, \@cols, \%data);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 escape

 Title   : escape
 Usage   : my $newText = $obj->escape( $oldText );
 Function: 
 Returns : 
 Args    : 

=cut


sub escape {
    my $self = shift;
    my $txt  = $_[0];
    $txt = '' unless(defined $txt);
    my $frm  = $self->format;
    if ($frm eq 'html' || $frm eq 'clean') {
        # Escape < unless the string starts with <
        $txt =~ s/\</\&lt\;/g unless ($txt =~ /^\</);
    } elsif ($frm eq 'tsv') {
        $txt =~ s/\t/TAB/g;
    } elsif ($frm eq 'csv') {
        $txt =~ s/\"/\\\"/g;
    }
    return $txt;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 format_node

 Title   : format_node
 Usage   : my $text = $obj->format_node( $node )
 Function: Turns a node object into a text string
 Returns : A string
 Args    : The node object

=cut

sub format_node {
    my $self = shift;
    my ($node, $edge, $ns) = @_;
    if (!ref($node)) {
        # Node was passed as a string
        if (my $mt = $self->tracker) {
            my $req = $ns ? "#$ns#$node" : $node;
            my $obj = $mt->get_seq( -name => $req, -nocreate => 1);
            return $node unless ($obj);
            $node = $obj;
        } else {
            return $node;
        }
    }
    if ($self->format eq 'html') {
        my $class = exists $self->{USER_QUERIES}{$node->id} ? 'mtquery' : '';
        if ($edge) {
            $edge->read_tags();
            $class .= ' mtdis' if ($edge->has_tag('Dissent'));
        }
        return $node->javascript_link($class);
    }
    return $node->name;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 format_class

 Title   : format_class
 Usage   : my $text = $obj->format_class( $class )
 Function: Turns a class object into a text string
 Returns : A string
 Args    : The class object

=cut

sub format_class {
    my $self = shift;
    my ($class) = @_;
    if ($self->format eq 'html') {
        return $class->javascript_link();
    }
    return $class->name;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 format_authority

 Title   : format_authority
 Usage   : my $text = $obj->format_authority( $authority )
 Function: Turns a authority object into a text string
 Returns : A string
 Args    : The authority object

=cut

sub format_authority {
    my $self = shift;
    my ($authority, $args) = @_;
    if (!ref($authority)) {
        # String passed
        return $authority if ($self->format ne 'html'); # Ok, keep as string
        my $mt = $self->tracker();
        return $authority unless ($mt);
        # Try to get formal MapTracker object
        my $aobj = $mt->get_authority($authority);
        if ($aobj) {
            $authority = $aobj
        } else {
            return $authority;
        }
    }
    if ($self->format eq 'html') {
        return $authority->javascript_link('', $args);
    }
    return $authority->name;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 format_edge

 Title   : format_edge
 Usage   : my $text = $obj->format_edge( $edge )
 Function: Textualize an edge
 Returns : A string
 Args    : An edge object

=cut


sub format_edge {
    my $self = shift;
    my ($edge) = @_;
    my ($dn) = $edge->has_tag('Denormalization');
    my $frm  = $self->format;
    my $txt = '';
    if ($dn && $dn->num > 1) {
        $txt = "[".$dn->num."]";
        $txt = "<font class='mtdenorm'>$txt</font>" if ($frm eq 'html');
    } else {
        
    }
    return $txt;
}

sub format_denormalization {
    my $self = shift;
    my ($edge) = @_;
    my ($dn) = $edge->has_tag('Denormalization');
    return "" unless ($dn && $dn->num > 1);
    my $txt = "[".$dn->num."]";
    if ($self->format eq 'html') {
        return $self->javascript_link('mtdenorm', 'edge', $edge->id, $txt);
    }
    return $txt;
}


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 format_provenance

 Title   : format_provenance
 Usage   : my $text = $obj->format_provenance($node, $edges)

 Function: Textualize the provenaance of a node in the context of one
           or more edges.

 Returns : A string
 Args    : [0] A node object
           [1] An array referrence of one or more edges

=cut


sub format_provenance {
    my $self = shift;
    my ($node, $edges) = @_;
    my @provs;
    foreach my $edge (@{$edges}) {
        my $other = $edge->other_node($node);
        my $txt = $self->format_node( $other ) . 
            $self->format_denormalization($edge);
        push @provs, $txt;
    }
    return $self->join_set(@provs);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 format_connection

 Title   : format_connection
 Usage   : my $text = $obj->format_connection($node, $edge)
 Function: Generates text representing a node in the context of an edge
 Returns : A string
 Args    : [0] A node object
           [1] An edge object

=cut


sub format_connection {
    my $self = shift;
    my ($node, $edge) = @_;
    my $onode = $edge->other_node($node);
    my ($rt)  = $edge->has_tag('Preferred Edge');
    my $txt   = $self->format_node( $onode );
    if ($rt) {
        $rt = $rt->valname;
        if ($onode->id == $edge->node2) {
            return "$txt $rt";
        } else {
            return "$rt $txt";
        }
    } else {
        my $reads = $edge->reads($node);
        return "$reads $txt";
    }
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 species_for_node

 Title   : species_for_node
 Usage   : my $txt = $obj->species_for_node( $node )
 Function: Returns a string representation of the species for a node
 Returns : A string
 Args    : The node object

=cut


sub species_for_node {
    my $self = shift;
    my ($node) = @_;
    my @bits;
    my $mt     = $self->tracker;
    return '' unless ($mt);
    my $frm    = $self->format;
    foreach my $taxa ($node->each_taxa) {
        if ($frm eq 'html') {
            push @bits, $taxa->javascript_link();
        } else {
            push @bits, $taxa->name;
        }
    }
    return $self->join_set(@bits);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 join_set

 Title   : join_set
 Usage   : my $text = $obj->join_set( @arrayOfStrings )
 Function: Concatenates an array of strings
 Returns : A string
 Args    : One or more strings

=cut


sub join_set {
    my $self = shift;
    my $val = join($self->{JOINER}, @_);
    $val = "" unless (defined $val);
    return $val;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 format_table

 Title   : format_table
 Usage   : my $text = $obj->format_table( $rows, $headers )
 Function: Convert tabular data to a string
 Returns : A string
 Args    : [0] 2D array reference of data
           [1] 1D array reference of column headers

=cut


sub format_table {
    my $self = shift;
    my ($data, $head, $nodes) = @_;
    if ($#{$data} < 0) {
        return $self->null_message;
    }
    my $frm  = $self->format;
    my $ishtml = ($frm eq 'html' || $frm eq 'clean') ? 1 : 0;
    my @rows;
    # Assemble the header
    if ($head && $#{$head} > -1) {
        my @header = @{$head};
        @header = map { $self->escape( $_ ) } @header unless ($ishtml);
        @header = map { sprintf($self->{HEADFORM}, defined $_ ? $_ : '') } @header;
        push @rows, \@header;
    }
    # Assemble cell contents:
    foreach my $rowRef (@{$data}) {
        my @row = @{$rowRef};
        @row = map { $self->escape( $_ ) } @row unless ($ishtml);
        @row = map { sprintf($self->{CELLFORM}, defined $_ ? $_ : '') } @row;
        push @rows, \@row;
    }
    # Assemble rows into strings:
    @rows = map { sprintf( $self->{ROWFORM}, $_) }
    map { join($self->{CELLSEP}, @{$_}) } @rows;
    # Concatenate table:
    my $table = join('', @rows);

    if ($ishtml) {
            
        $table = "<table class='" . $self->table_class . "'>\n<tbody>\n" .
            $table . "</tbody></table>\n";
        if ($nodes) {
            my $mt = $self->tracker;
            if ($mt) {
                $mt->_store_js();
                $self->js_for_node( -node => [values %{$nodes} ]);
                $table .= $mt->javascript_data( $frm );
                $mt->_restore_js();
            }
        }
        $table = "<!-- $vers -->\n$table" if ($vers);
    }
    return $table;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 table_class

 Title   : table_class
 Usage   : $obj->table_class
 Function: 
 Returns : 
 Args    : 

=cut


sub table_class {
    my $self = shift;
    if ($_[0]) {
        $self->{TABCLASS} = $_[0];
    }
    return $self->{TABCLASS} || "";
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 js_for_node

 Title   : js_for_node
 Usage   : $obj->js_for_node
 Function: 
 Returns : 
 Args    : 

=cut


sub js_for_node {
    my $self = shift;
    my $mt = $self->tracker;
    return unless ($mt);
    unshift @_, '-node' if ($#_ == 0);
    my $args = $self->parseparams
        ( -node     => undef,
          -decorate => '',
          -edge     => undef,
          @_ );
    my $dec  = lc($args->{DECORATE});
    my $req = $args->{NODE};
    my @reqs = (ref($req) && ref($req) eq 'ARRAY') ? @{$req} : ($req);
    my (@seqs);
    foreach my $request (@reqs) {
        my $obj = $mt->get_seq($request);
        push @seqs, $obj if ($obj);
    }
    my @objs = @seqs;

    if ($dec =~ /tax/ || $dec =~ /spec/ || $dec =~ /full/) {
        my %taxa = map { $_->id => $_ } map { $_, $_->all_parents(1) }
        map { $_->each_taxa } @seqs;
        push @objs, values %taxa;
    }
    if ($dec =~ /class/ || $dec =~ /full/) {
        map { $_->read_classes } @seqs;
    }
    if ($dec =~ /len/ || $dec =~ /full/) {
        map { $_->read_lengths } @seqs;
    }
    if (defined $args->{EDGE}) {
        my $kt = $args->{KEEPTYPE};
        $kt = [ split("\n", $kt) ] if ($kt);
        my @edgenodes;
        foreach my $seq (@seqs) {
            my @edges = $seq->read_edges( -limit    => $args->{EDGE},
                                          -force    => 1,
                                          -keeptype => $kt );
            push @edgenodes, map { $_->other_seq( $seq ) } @edges;
        }
        push @objs, @edgenodes;
        if (my $ed = $args->{EDGEDEC}) {
            $self->js_for_node( -node     => \@edgenodes,
                                -decorate => $ed );
        }
    }
    if ($dec =~ /desc/ || $dec =~ /sf/ || $dec =~ /full/) {
        map { push @objs, $_->desc() } @seqs;
    }
    push @objs, map { ( $_, $_->nodes ) } map { $_->each_edge } @seqs;
    $mt->register_javascript( @objs );
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 js_for_namespace

 Title   : js_for_namespace
 Usage   : $obj->js_for_namespace
 Function: 
 Returns : 
 Args    : 

=cut

*js_for_ns = \&js_for_namespace;
sub js_for_namespace {
    my $self = shift;
    my $mt   = $self->tracker;
    return unless ($mt);
    my @objs = map { $mt->get_namespace( $_ ) } @_;
    $mt->register_javascript( @objs );
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 js_for_authority

 Title   : js_for_authority
 Usage   : $obj->js_for_authority
 Function: 
 Returns : 
 Args    : 

=cut


*js_for_auth = \&js_for_authority;
sub js_for_authority {
    my $self = shift;
    my $mt   = $self->tracker;
    return unless ($mt);
    my @objs = map { $mt->get_authority( $_ ) } @_;
    $mt->register_javascript( @objs );
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 js_for_taxa

 Title   : js_for_taxa
 Usage   : $obj->js_for_taxa
 Function: 
 Returns : 
 Args    : 

=cut


sub js_for_taxa {
    my $self = shift;
    my $mt   = $self->tracker;
    return unless ($mt);
    my @objs = map {$_, $_->all_parents(1) } map { $mt->get_taxa( $_ ) } @_;
    $mt->register_javascript( @objs );
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 js_for_class

 Title   : js_for_class
 Usage   : $obj->js_for_class
 Function: 
 Returns : 
 Args    : 

=cut

sub js_for_class {
    my $self = shift;
    my $mt   = $self->tracker;
    return unless ($mt);
    my @objs = map { $mt->get_class( $_ ) } @_;
    $mt->register_javascript( @objs );
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 js_for_type

 Title   : js_for_type
 Usage   : $obj->js_for_type
 Function: 
 Returns : 
 Args    : 

=cut

*js_for_rel = \&js_for_type;
sub js_for_type {
    my $self = shift;
    my $mt   = $self->tracker;
    return unless ($mt);
    my @objs = map { $mt->get_type( $_ ) } @_;
    $mt->register_javascript( @objs );
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 js_for_edge

 Title   : js_for_edge
 Usage   : $obj->js_for_edge
 Function: 
 Returns : 
 Args    : 

=cut

sub js_for_edge {
    my $self  = shift;
    my $mt    = $self->tracker;
    return unless ($mt);
    my @objs  = map { $mt->get_edge( $_ ) } @_;
    my @tags  = map { ( $_->tag, $_->value ) } map { $_->each_tag } @objs;
    my @nodes = map { $_->nodes } @objs;
    $mt->register_javascript( @objs, @tags, @nodes );
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 key_table

 Title   : key_table
 Usage   : $obj->key_table
 Function: 
 Returns : 
 Args    : 

=cut


sub key_table {
    my $self = shift;
    my $string = "<p>Features displayed by MapTracker may contain formatting based on the qualities of the feature. Examples of special formatting used are shown below:</p>\n<table class='mtmild'>\n<tr><th>Class</th><th>Example</th><th>Description</th></tr>\n";

    my @keys =
        ( [ 'Query', 'mtquery', 'Hello World',
            'The entry was matched to your original query, and is why the current locus was selected (although more than one entry might match)' ],
          
          [ 'Deprecated', 'mtdep', 'Crufty ID',
            'The entry represents a deprecated term - in most cases, this is an accession that has been replaced by a newer one.' ],
          
          [ 'Dissent', 'mtdis', "32067_at",
            "Indicates that there is dissent / disagreement over displayed data. At least one authority thinks that the information is partially or entirely incorrect. Clicking the link may provide additional details." ],

          [ 'Indirect', 'mtdenorm', '[6]',
            "This small superscript indicates that the displayed node is not directly connected to a 'backbone' entry - instead, it is that many edges distant." ],
          
          [ 'RBM', 'rbm', '95.33%',
            "Indicates that an alignment of homologues is a 'Reciprocal Best Match'" ],
          
          [ 'Node', 'mtnode', "Some object",
            "Nodes are the primary data entities in MapTracker. They are pieces of text representing genes, RNA, descriptions, etc." ],
          
          [ 'Class', 'mtclass', "Some classification",
            "Classes are used to classify nodes, and are organized as a hierarchy" ],
          
          [ 'Taxa', 'mttaxa', "Homo sapiens",
            "Nodes may have 1 or more taxa (species) assigned to them. Taxa are also arranged as a hierarchy" ],
          
          [ 'Namespace', 'mtns', "None",
            "Namespaces are used to separate nodes (and edges) that have the same name but represent different entities." ],
          
          [ 'Authority', 'mtauth', "Abraham Lincoln",
            "An authority is a person, group or algorithm that is making a claim about the displayed data" ],

          [ 'Edge', 'mtedge', "A is a child of B",
            "Edges connect two nodes together by a specific kind of relationship. They may have meta-data associated with them as well." ],

          [ '', '', "",
            "" ],
          );
    foreach my $row (@keys, @_) {
        next unless (ref($row));
        my ($name, $class, $ex, $desc) = @{$row};
        next unless ($name);
        $string .= "  <tr><th>$name</th><td align='center' nowrap>";
        $string .= "<font class='$class'>$ex</font>";
        $string .= "</td><td>$desc</td></tr>";
    }

    $string .="</table>\n";
    return $string;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 make_filter

 Title   : make_filter
 Usage   : $obj->make_filter( @args )
 Function: 
 Returns : 
 Args    : 

=cut


sub make_filter {
    my $self = shift;
    my $mt   = $self->tracker;
    return unless ($mt);
    my $args = $self->parseparams
        ( -node     => undef,
          -decorate => '',
          -edge     => undef,
          @_ );
    my $name = $args->{NAME} || $args->{FILTERNAME};
    my $user = $args->{USER};
    return unless ($name && $user);
    my $olduser = $mt->user;
    $mt->user($user);
    my $uns = $mt->make_namespace( -name => "User $user",
                                   -desc => "User data for $user",
                                   -sensitive => 0);
    return unless ($uns);
    $uns = '#' . $uns->name . '#';
                                   
    my $ns = '#Filters#';
    my $tm = $args->{TESTMODE} || 0;

    my @tags;
    my @fbits = split(/[\n\r]+/, $args->{FILTERS} || $args->{FILTER});
    foreach my $bit (@fbits) {
        my @row = split("\t", $bit);
        my $tag = shift @row;
        foreach my $val (@row) {
            my ($node, $type, $id);
            if ($val =~ /^([a-z]+)_(\d+)$/) {
                ($type, $id) = ($1, $2);
                if ($type eq 'node') {
                    $node = $mt->get_seq($id);
                    $id   = undef;
                } elsif ($type eq 'class') {
                    my $class = $mt->get_class($id);
                    next unless ($class);
                    $node = $mt->get_seq("#Classes#" . $class->name);
                } elsif ($type eq 'taxa') {
                    my @taxa = $mt->get_taxa($id);
                    next unless ($#taxa == 0);
                    $node = $mt->get_seq("#Taxa#" . $taxa[0]->name);
                } else {
                    next;
                }
            } else {
                my $type = $mt->get_type($val);
                if ($type) {
                    $node = $mt->get_seq("#Reads_As#" . $val);
                    $id = undef;
                } else {
                    next;
                }
            }
            unless ($node) {
                warn "no node for '$val'";
                next;
            }
            push @tags, [ $ns . $tag, $node->namespace_name, $id];
        }
    }

    my $loader = BMS::MapTracker::LoadHelper->new
        ( -username => $user,
          -basedir  => $basedir,
          -testmode => $tm, );
    $name    = $uns . $name;
    my $root = $ns . "All Filters";
    my $set  = $ns . "$user Filters";
    $loader->set_edge( -name1 => $root,
                       -name2 => $set,
                       -type  => $ns.'has member' );

    # Clear the old tags:
    $loader->kill_edge( -name1 => $set,
                        -name2 => $name,
                        -type  => $ns.'has member',
                        -tags  => [ ['%', '%'] ]);

    $loader->set_edge( -name1 => $set,
                       -name2 => $name,
                       -type  => $ns.'has member',
                       -tags  => \@tags);
    if (my $desc = $args->{FILTERDESC}) {
        $loader->set_edge( -name1 => $name,
                           -name2 => '#FreeText#' . $desc,
                           -type  => 'is a shorter term for' );
        $loader->kill_edge( -name1 => $name,
                            -type  => 'is a shorter term for' );
    }

    # $loader->redirect( -stream => 'TEST', -fh     => *STDOUT);
    $loader->write();
    $loader->process_ready();

    $mt->user($olduser);
    my @recover = $self->get_filters( -user       => $user,
                                      -filtername => $name );
    my $edge;
    if ($#recover == 0) {
        $edge = $recover[0];
        $edge->register_full_javascript();
    }

    if (my $mid = $args->{MSGID}) {
        my @stack;
        if ($edge) {
            push @stack, "[ 'Filter Edge ID', ". $edge->id." ]";
        } else {
            push @stack, "[ 'Error', 'Could not recover filter $name' ]";
        }
        $mt->register_javascript_literal
            ( "{ type:'msg', id:'$mid', stack:[ ".join(",",@stack)." ] }" );
    }
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 get_filters

 Title   : 
 Usage   : $obj->
 Function: 
 Returns : 
 Args    : 

=cut


sub get_filters {
    my $self  = shift;
    my $mt    = $self->tracker;
    return unless ($mt);
    my $args = $self->parseparams
        ( -user     => undef,
          -filter   => undef,
          @_ );

    my $ns    = '#Filters#';
    my $fname = $args->{FILTER} || $args->{FILTERNAME};
    my $set;
    my @userlist;
    if (my $user = $args->{USER}) {
        # Just for a specific user
        # Make sure we can write to database (in case user is 'read only')
        my $olduser = $mt->user;
        $mt->user($user);
        my $uns = $mt->make_namespace( -name => "User $user",
                                       -desc => "User data for $user",
                                       -sensitive => 0);
        return unless ($uns);
        $uns    = '#' . $uns->name . '#';
        $set    = $ns . "$user Filters";
        my $seq = $mt->get_seq($set);
        @userlist = ($seq);
        $mt->user($olduser);
    } else {
        # Get all filters
        my $root = $mt->get_seq($ns . "All Filters");
        return () unless ($root);
         my @edges = $root->read_edges( -keeptype => $ns.'has member' );
        foreach my $edge (@edges) {
            push @userlist, $edge->other_seq($root);
        }
    }

    my @filters;
    my @baseargs = ( -return   => 'object array',
                     -keeptype => $ns.'MEMBEROF');
    foreach my $ulist (@userlist) {
        my @dumpargs = @baseargs;
        if ($set && $fname) {
            push @dumpargs,  ( -name1 => $fname,
                               -name2 => $set );
        } else {
            push @dumpargs, ( -name => $ulist);
        }
        
        my $elist = $mt->get_edge_dump( @dumpargs );
        foreach my $edge (@{$elist}) {
            my $ename = $edge->other_seq( $ulist );
            $mt->register_javascript($ename->desc);
            $edge->register_full_javascript();
            next unless ($edge->node2->id == $ulist->id);
            push @filters, $edge;
            # print $edge->to_html;
        }
    }
    return @filters;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 

 Title   : 
 Usage   : $obj->
 Function: 
 Returns : 
 Args    : 

=cut


sub f {
    my $self = shift;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

