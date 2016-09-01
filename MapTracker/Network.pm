# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::Network;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

$BMS::MapTracker::Network::VERSION = 
    ' $Id$ ';

use strict;
use vars qw(@ISA);
use Scalar::Util qw(weaken);
use CGI;
use GD;


use LWP::UserAgent;
use LWP::Simple;
use HTTP::Request;
use HTTP::Cookies;

use BMS::Branch;
use BMS::MapTracker;
use BMS::MapTracker::Shared;

@ISA = qw(BMS::MapTracker::Shared);


my $debug = BMS::Branch->new
    ( -skipkey => ['TRACKER', 'CHILDREN', 'CLASSES', 'FP',
		   'SEQNAMES', 'AUTHORITIES','TYPES', 'obj', 'READS',
		   'DATA', 'EVERY_CHILD', 'COLLECTION', 'KEYS'],
      -format => 'html', -noredundancy => 1, );

my @hier_colors = ('ccffcc','ffccff','ffff99','ccffff');

my $gv_params = {
    shape => [ qw(box rectangle ellipse circle diamond pentagon hexagon octagon trapezium house  doublecircle Mcircle plaintext) ],
    color => [ qw(aqua coral cyan gold gray khaki lime olive orange red violet yellow turquoise deepskyblue) ],
    
};

my $gv2gml = {
    'color' => 'fill',
    'shape' => 'type',
};

my $nocgi = $ENV{'HTTP_HOST'} ? 0 : 1;

my $gv_arrowmap = {

    'multiple children in one edge' => {
 	arrowhead => 'crow',
	arrowtail => 'none',
	# style     => 'bold',
	color     => '#33ccff',	
    },

    # Nomenclature-type edges:
    'is an alias for' => {
	arrowhead => 'empty',
	arrowtail => 'empty',
	color     => '#669933',
    },
    'is a reliable alias for' => {
	arrowhead => 'normal',
	arrowtail => 'none',
	color     => '#669933',
    },
    'is reliably aliased by' => {
	mirror => 'is a reliable alias for',
    },
    'is the same as a version of' => {
	arrowhead => 'obox',
	arrowtail => 'none',
	color     => '#669933',
    },
    'has a version that is the same as' => {
	mirror => 'is the same as a version of',
    },
    'is the same as' => {
	arrowhead => 'box',
	arrowtail => 'box',
	dir       => 'both',
	color     => '#669933',
    },
    'is similar to' => {
	arrowhead => 'orbox',
	arrowtail => 'orbox',
	dir       => 'both',
	color     => '#99cc33',
    },
    'is a lexical variant of' => {
 	arrowhead => 'odiamond',
	arrowtail => 'none',
	color     => '#669933',
    },
    'is the preferred lexical variant of' => {
	mirror => 'is a lexical variant of',
    },
    'is a specific version of' => {
 	arrowhead => 'odot',
	arrowtail => 'none',
	color     => '#669933',
    },
    'is an unversioned accession of' => {
	mirror => 'is a specific version of',
    },
    'is a deprecated entry for' => {
 	arrowhead => 'diamond',
	arrowtail => 'tee',
	color     => '#cc9933',
    },
    'is an updated entry from' => {
	mirror => 'is a deprecated entry for',
    },
    'is a later version of' => {
 	arrowhead => 'odot',
	arrowtail => 'none',
	style     => 'dotted',
	color     => '#669933',
    },
    'is an earlier version of' => {
	mirror => 'is a later version of',
    },
    'is a shorter term for' => {
	arrowhead => 'open',
	arrowtail => 'none',
	color     => '#669933',
    },
    'is a longer term for' => {
	mirror => 'is a shorter term for',
    },

    # Target-compound edges:
    'is an antagonist for' => {
 	arrowhead => 'tee',
	arrowtail => 'none',
	color     => '#ff0000',
    },
    'is antagonized by' => {
	mirror => 'is an antagonist for',
    },

    'is a functional antagonist for' => {
 	arrowhead => 'tee',
	arrowtail => 'none',
	color     => '#ff0000',
	style     => 'dashed',
    },
    'is functionally antagonized by' => {
	mirror => 'is a functional antagonist for',
    },

    'is an agonist for' => {
 	arrowhead => 'open',
	arrowtail => 'none',
	color     => '#00ff00',
    },
    'is agonized by' => {
	mirror => 'is an agonist for',
    },

    'genetically interacts with' => {
 	arrowhead => 'dot',
	arrowtail => 'dot',
	color     => '#3300cc',
    },

    'physically interacts with' => {
 	arrowhead => 'box',
	arrowtail => 'box',
	color     => '#3300cc',
    },

    'has an impact on' => {
 	arrowhead => 'none',
	arrowtail => 'dot',
	color     => '#3300cc',
    },
    'is impacted by' => {
	mirror => 'has an impact on',
    },

    'is a functional agonist for' => {
 	arrowhead => 'open',
	arrowtail => 'none',
	color     => '#00ff00',
	style     => 'dashed',
    },
    'is functionally agonized by' => {
	mirror => 'is a functional agonist for',
    },

    'inhibits' => {
 	arrowhead => 'tee',
	arrowtail => 'none',
	color     => '#ff6600',
    },
    'is inhibited by' => {
	mirror => 'inhibits',
    },

    'is coregulated with' => {
 	arrowhead => 'halfopen',
	arrowtail => 'halfopen',
	color     => '#339900',
    },

    'is antiregulated from' => {
 	arrowhead => 'halfopen',
	arrowtail => 'halfopen',
	color     => '#ff0000',
    },

    'is a substrate for' => {
 	arrowhead => 'normal',
	arrowtail => 'inv',
	color     => '#ffcc33',
    },
    'has substrate' => {
	mirror => 'is a substrate for',
    },

    'was assayed against' => {
 	arrowhead => 'invempty',
	arrowtail => 'none',
	color     => '#996633',
    },
    'was assayed with' => {
	mirror => 'was assayed against',
    },

    'has molecular formula' => {
 	arrowhead => 'odot',
	arrowtail => 'none',
	color     => '#66ffcc',
	style     => 'dashed',
    },
    'is the molecular formula for' => {
	mirror => 'has molecular formula',
    },
    'is an isomer of' => {
	arrowhead => 'empty',
	arrowtail => 'empty',
	color     => '#cccc00',
    },

    'is a simpler form of' => {
	arrowhead => 'halfopen',
	arrowtail => 'empty',
	color     => '#cccc00',
    },
    'is a more complex form of' => {
	mirror => 'is a simpler form of',
    },

    

    # Biological edges:
    'is a locus containing' => {
 	arrowhead => 'invdot',
	arrowtail => 'none',
	color     => '#ff9900',
    },
    'is contained in locus' => {
	mirror => 'is a locus containing',
    },
    'is a locus with protein' => {
 	arrowhead => 'box',
	arrowtail => 'none',
	color     => '#ff9900',
    },
    'is a protein from locus' => {
	mirror => 'is a locus with protein',
    },
    'can be transcribed to generate' => {
 	arrowhead => 'invodot',
	arrowtail => 'none',
	color     => '#ff9900',
    },
    'is transcribed from' => {
	mirror => 'can be transcribed to generate',
    },
    'can be translated to generate' => {
 	arrowhead => 'inv',
	arrowtail => 'none',
	color     => '#ff9900',	
    },
    'is translated from' => {
	mirror => 'can be translated to generate',
    },
    'fully contains' => {
 	arrowhead => 'obox',
	arrowtail => 'none',
	color     => '#ff9900',	
    },
    'is fully contained by' => {
	mirror => 'fully contains',
    },
    'overlaps with' => {
 	arrowhead => 'tee',
	arrowtail => 'tee',
	style     => 'dashed',
	color     => '#ff9900',	
    },

    'has feature' => {
 	arrowhead => 'invempty',
	arrowtail => "none",
	color     => '#ff66ff',	
    },
    'is a feature on' => {
	mirror => 'has feature',
    },
    'is mapped to' => {
 	arrowhead => 'obox',
	arrowtail => "none",
	color     => '#ff66ff',	
    },
    'is mapped from' => {
	mirror => 'is mapped to',
    },


    'is the orthologue of' => {
 	arrowhead => 'open',
	arrowtail => "open",
	color     => '#ff0000',	
    },

    'is homologous to' => {
 	arrowhead => 'empty',
	arrowtail => "empty",
	color     => '#33ff99',	
    },

    'is syntenic with' => {
 	arrowhead => 'halfopen',
	arrowtail => "halfopen",
	color     => '#33ff99',	
    },

    'is a probe for' => {
 	arrowhead => 'open',
	arrowtail => "none",
	color     => '#cc9933',	
    },
    'can be assayed with probe' => {
	mirror => 'is a probe for',
    },


    'is also in group' => {
 	arrowhead => 'open',
	arrowtail => "none",
	color     => '#999999',
	style     => 'dashed',
    },


    # Text edges
    'is referenced in' => {
 	arrowhead => 'dot',
	arrowtail => "none",
	color     => '#cccc99',
    },
    'contains a reference to' => {
	mirror => 'is referenced in',
    },
    'contributes to' => {
 	arrowhead => 'open',
	arrowtail => "none",
	color     => '#cccc99',
    },
    'has contribution from' => {
	mirror => 'contributes to',
    },

    'may be purchased from' => {
 	arrowhead => 'obox',
	arrowtail => "none",
	color     => '#cccc99',
    },
    'sells' => {
	mirror => 'may be purchased from',
    },


    'owns' => {
 	arrowhead => 'box',
	arrowtail => "none",
	color     => '#dddd00',
    },
    'is owned by' => {
	mirror => 'owns',
    },


    # Ontology / Tree edges
    'is a parent of' => {
 	arrowhead => 'normal',
	arrowtail => "none",
	color     => '#0033cc',	
    },
    'is a child of' => {
	mirror => 'is a parent of',
    },
    'has attribute' => {
 	arrowhead => 'odiamond',
	arrowtail => "none",
	color     => '#ff66ff',	
    },
    'is attributed to' => {
	mirror => 'has attribute',
    },
    'reports to' => {
 	arrowhead => 'open',
	arrowtail => "none",
	color     => '#0033cc',	
    },
    'has report' => {
	mirror => 'reports to',
    },
    'has rank' => {
 	arrowhead => 'rdiamond',
	arrowtail => "none",
	color     => '#0033cc',	
    },
    'is a rank with member' => {
	mirror => 'has rank',
    },
    'is located within' => {
 	arrowhead => 'teeobox',
	arrowtail => "none",
	color     => '#0033cc',	
    },
    'is a locale containing' => {
	mirror => 'is located within',
    },
    'is assigned to task' => {
 	arrowhead => 'empty',
	arrowtail => "none",
	color     => '#00ff33',	
    },
    'is being worked on by' => {
	mirror => 'is assigned to task',
    },


    'is a member of' => {
 	arrowhead => 'none',
	arrowtail => "crow",
	color     => '#0033cc',	
    },
    'has member' => {
	mirror => 'is a member of',
    },
    'is a cluster with sequence' => {
 	arrowhead => 'crow',
	arrowtail => "none",
	color     => '#cccc33',	
    },
    'is a sequence in cluster' => {
	mirror => 'is a cluster with sequence',
    },
    'does not contain' => {
 	arrowhead => 'obox',
	arrowtail => "none",
        color     => '#FF0066',
	style     => 'dotted',
    },
    'is absent from' => {
	mirror => 'does not contain',
    },

    'is denormalized to' => {
 	arrowhead => 'vee',
	arrowtail => "crow",
	color     => '#ff0000',	
    },
    'is denormalized from' => {
	mirror => 'is denormalized to',
    },

    'is a cause for' => {
 	arrowhead => 'olbox',
	arrowtail => "ltee",
	color     => '#993300',	
    },
    'is caused by' => {
	mirror => 'is a cause for',
    },
    'is not linked to' => {
 	arrowhead => 'tee',
	arrowtail => 'none',
        color     => '#FF0066',
        
    },
    'is not linked from' => {
	mirror => 'is not linked to',
    },
    'is a comment for' => {
	arrowhead => 'open',
	arrowtail => 'none',
	color     => '#669933',
	style     => 'dashed',
    },
    'has comment' => {
	mirror => 'is a comment for',
    },

    'is the source for' => {
 	arrowhead => 'normal',
	arrowtail => 'inv',
	color     => '#33ffff',	
    },
    'was derived from' => {
	mirror => 'is the source for',
    },
};

=head1 PRIMARY METHODS
#-#-#-#-#--#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-

=head2 new

 Title   : new
 Usage   : my $obj = BMS::MapTracker::Network->new(@arguments)
 Function: Creates a new object and returns a blessed reference to it.
 Returns : A blessed BMS::MapTracker::Network object
 Args    : Associative array of arguments. Recognized keys [Default]:

  -tracker Required. A BMS::MapTracker object

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = { 
	TRACKER     => undef,
        FAST_TAXA   => {},
    };
    bless ($self, $class);
    $self->clear_all;
    my $args = $self->parseparams( -tracker => undef,
				   -root    => undef,
				   @_ );
    if ($args->{TRACKER}) {
        weaken( $self->{TRACKER} = $args->{TRACKER});
    } else {
        $self->{TRACKER} = BMS::MapTracker->new();
    }
    $self->{USER_ID} = $self->tracker->user->id;
    $self->add_root( $args->{ROOT} ) if ($args->{ROOT});
    my $mt = $self->tracker;
    $self->{TELSPACE} = $mt->get_namespace('Telescope')->id;
    return $self;
}

sub DESTROY {
    my $self = shift;
    # Try to break potential circular references:
    foreach my $key (keys %{$self}) {
        my $val = $self->{$key};
        $self->{$key} = undef;
    }
}

sub param {
    my $self = shift;
    my $tag = uc($_[0]);
    if (defined $_[1]) {
        $self->{PARAM}{$tag} = $_[1];
    }
    my $val = $self->{PARAM}{$tag};
    $val = $self->{DEFAULTS}{$tag} unless (defined $val);
    return $val;
}

sub clear_all {
    my $self = shift;
    $self->clear_roots;

    $self->{NODES}       = {};
    $self->{NODE_FORMAT} = {};
    $self->{EDGES}       = {};
    $self->{DISTANCE}    = {};
    $self->{CONNECTIONS} = {};
    $self->{USED_REL}    = {};
    $self->{EDGE_ADDED}  = {};
    $self->{OVERLAYS}    = {};
    $self->{PATH_CACHE}  = {};
    $self->{PROXY}       = {};
    $self->{ITER}        = {};
    $self->{ROOT_MEMORY} = [];
    $self->{PARAM}       = {};

    $self->{EDGE_LOOKUP} = {
        KEY     => {},
        EID     => {},
    };

    $self->{GROUPS}       = {};
    $self->{GVEDGE}       = 0;

    $self->{TOSS_NODE}   = {};
    $self->{TOSS_EDGE}   = {};
    $self->{ITERATION}   = 0;
    $self->{MAX_ITER}    = 0;
    $self->{NW_LOADED}   = {};
    $self->{NW_EXCLUDED} = [];

    $self->{TASKS}       = {};

}

sub tracker {
    my $self = shift;
    return $self->{TRACKER};
}

sub add_root {
    my $self = shift;
    $self->benchstart;
    my @added;
    foreach my $name (@_) {
        my $seq = $self->tracker->get_seq( $name );
        unless ($seq) {
            $self->error("Can not find MapTracker Sequence for '$name'");
            next;
        }
        my $sid = $seq->id;
        unless (defined $self->{ISROOT}{ $sid }) {
            push @{$self->{ROOTS}}, $seq;
            $self->{ISROOT}{$sid} = $#{$self->{ROOTS}};
        }
        push @added, $self->node( $sid );
    }
    $self->benchstop;
    return @added;
}

sub remove_root {
    my $self = shift;
    $self->benchstart;
    my @removed;
    my %to_kill;
    foreach my $name (@_) {
        my $seq = $self->tracker->get_seq( $name );
        unless ($seq) {
            $self->error("Can not find MapTracker Sequence for '$name'");
            next;
        }
        my $sid = $seq->id;
        next unless (defined $self->{ISROOT}{$sid});
        push @removed, $seq;
        $to_kill{ $sid } = $seq;
    }
    if ($#removed > -1) {
        # We need to remove one or more roots.
        my @old_roots = $self->each_root;
        $self->clear_roots;
        foreach my $root (@old_roots) {
            next if ($to_kill{ $root->id });
            $self->add_root( $root );
        }
    }
    $self->benchstop;
    return @removed;
}

sub proxy {
    my $self = shift;
    my ($real, $proxy, $remove) = @_;
    my $rseq = $self->node($real);
    return undef unless ($rseq);
    my $rid = $rseq->id;
    if ($remove) {
        # The user no longer wishes to have this name proxied
        delete $self->{PROXY}{$rid};
    } elsif ($proxy) {
        # Setting a new proxy. We do not use $self->node() to get
        # $pseq, because the proxy name may not be an official node in
        # the net
        my $mt = $self->tracker;
        if (my $pseq = $mt->get_seq($proxy) ) {
            $self->{PROXY}{$rid} = $pseq;
            $mt->register_javascript( $rseq );
        }
        
    }
    return $self->{PROXY}{$rid} || $rseq;
}

sub proxy_by_edge {
    my $self = shift;
    my $args = $self->parseparams( -node       => undef,
				   -edge       => 'is a shorter term for',
                                   -keepclass  => [],
                                   -delete     => 1,
                                   -multiple   => 'smallest',
                                   @_);
    my $mt    = $self->tracker;
    my $reads = $args->{READS} || $args->{EDGE};
    return () unless ($reads);
    my @nodes;
    if (my $req = $args->{NODE}) {
        @nodes = (ref($req) && ref($req) eq 'ARRAY') ? @{$req} : ($req);
    } else {
        @nodes = $self->all_seqs;
    }
    my @kc = $mt->param_to_list($args->{KEEPCLASS},'class');
    my $mult = lc($args->{MULTIPLE});
    foreach my $node (@nodes) {
        my @edges = $self->edges_from_node( $node, $reads );
        next if ($#edges < 0);
        # Turn each edge into an array of the other node and edge_id
        @edges = map { [ $_->node($node), $_ ] } @edges;
        if ($#kc > -1) {
            my @keep;
            foreach my $edat (@edges) {
                push @keep, $edat if ($edat->[0]->is_class( @kc ) );
            }
            @edges = @keep;
            next if ($#edges < 0);
        }
        if ($#edges > 0) {
            # More than one edge of this type!
            if ($mult eq 'smallest') {
                # Use the edge with the shortest name
                @edges = sort { length( $a->[0]->name ) <=>
                                    length( $b->[0]->name ) } @edges;
            } elsif ($mult eq 'alpha') {
                # Use the edge that occurs first when sorted
                @edges = sort { uc( $a->[0]->name ) cmp
                                    uc( $b->[0]->name ) } @edges;
            } else {
                # Not sure what to do - do not proxy this edge.
                next;
            }
        }
        my ($proxy, $edge) = ($edges[0][0], $edges[0][1]);
            
        next unless ($proxy);
        $self->proxy( $node, $proxy);
        $self->delete_edge($edge) if ($args->{DELETE});
    }
}

sub clear_roots {
    my $self = shift;
    $self->{ISROOT}      = {};
    $self->{ROOTS}       = [];
}

sub each_root {
    my $self = shift;
    return @{$self->{ROOTS}};
}

sub remember_roots {
    my $self = shift;
    push @{$self->{ROOT_MEMORY}}, [ $self->each_root ];
}

sub recall_roots {
    my $self   = shift;
    my $recall = pop @{$self->{ROOT_MEMORY}};
    $recall ||= [];
    $self->clear_roots;
    $self->add_root( @{$recall} );
}

sub node {
    my $self = shift;
    my ($req) = @_;
    my $seq = $self->tracker->get_seq( $req );
    return undef unless ($seq);
    my $sid = $seq->id;
    $self->{NODES}{ $sid } = $seq;
    return $seq;
}

sub each_node {
    my $self = shift;
    my @nodes;
    while (my ($sid, $node) = each %{$self->{NODES}}) {
        next if ($self->{TOSS_NODE}{$sid});
        push @nodes, $node;
    }
    return @nodes;
}

sub node_count {
    my $self = shift;
    my @sids = keys %{$self->{NODES}};
    # This will count tossed nodes!
    return $#sids + 1;
}

*format_node = \&node_format;
sub node_format {
    my $self = shift;
    my ($node, $param, $value) = @_;
    return undef unless ($node && defined $param);
    my $lcnode = lc($node);
    my ($sid, $name);
    if ($lcnode eq 'graph' || $lcnode eq 'edge' || $lcnode eq 'node' ) {
        # Global formatting
        $sid = $name = $lcnode;
    } else {
        my $seq = $self->node( $node );
        $sid    = $seq->id;
        $name   = $seq->name;
    }
    $param = lc($param);
    #my $opts = $gv_params->{ $param };
    #unless ($opts) {
    #    $self->error("Could not set node format for '$name' using unknown ".
    #                 "parameter '$param'");
    #    return;
    #}
    $self->{NODE_FORMAT}{$sid} ||= {};
    if (defined $value) {
        $self->{NODE_FORMAT}{$sid}{$param} = $value;
    }
    return $self->{NODE_FORMAT}{$sid}{$param};
}

sub exclude_node {
    my $self = shift;
    my ($name) = @_;
    my $seq = $self->tracker->get_seq( $name );
    return undef unless ($seq);
    my $sid = $seq->id;
    $self->{TOSS_NODE}{ $sid } = $seq;
    $self->remove_root($sid);
    return $seq;
}

sub visualize_edge {
    my $self = shift;
    my ($reads, $level) = @_;
    $reads = lc($reads);
    if (defined $level && $level =~ /^\d+$/) {
        $level = 2 if ($level > 2);
        $self->{TOSS_EDGE}{ $reads } = $level;
    }
    return $self->{TOSS_EDGE}{ $reads } || 0;
}


sub each_excluded_node {
    my $self = shift;
    my @exc;
    foreach my $obj (values %{$self->{TOSS_NODE}}) {
        push @exc, $obj if (ref($obj));
    }
    @exc  = sort { $a->name cmp $b->name } @exc;
    return @exc;
}

sub each_edge {
    my $self = shift;
    return values %{$self->{EDGES}};
}

sub add_edge {
    my $self = shift;
    my ($edge) = @_;
    my $eid = $edge->id;
    return if ($self->{EDGES}{$eid});
    $self->{EDGES}{$eid} = $edge;
    # Add the nodes
    foreach my $node ($edge->nodes) {
        $self->node($node);
    }
    # Add lookup information
    $self->add_edge_lookup( $edge );
    return $edge;
}

sub filter_edges {
    my $self = shift;
    my $args = $self->parseparams( -tag => '',
                                   -val => '',
                                   -num => '',
                                   @_);
    my ($tag, $val, $num) = ( uc($args->{TAG}), uc($args->{VAL}),
                              $args->{NUM} );
    $num = sprintf('$matched += (%%s %s) ? 1 : 0', $num) if ($num);
    # warn "tag: $tag\nval: $val\nnum: $num\n  ";
    my $required = 0;
    map { $required += $_ ? 1 : 0 } ($tag, $val, $num);
    foreach my $edge ( $self->each_edge ) {
        my $matched = 0;
        foreach my $tagobj ($edge->each_tag) {
            $matched++ if ($tag && uc($tagobj->tagname) eq $tag);
            $matched++ if ($val && uc($tagobj->valname) eq $val);
            eval(sprintf($num, $tag->num)) if ($num);
            if ($matched == $required) {
                $self->delete_edge( $edge );
                last;
            }
        }
    }
}

sub delete_edge {
    my $self = shift;
    my ($edge) = @_;
    my $eid    = $edge->id;

    delete $self->{EDGE_LOOKUP}{EID}{$eid};

    my @nodes = $edge->nodes;
    my @sids  = map { $_->id } @nodes;
    my $tid   = $edge->type->id;
    my @keyz  = ( @sids,  
                  join("\t", sort @sids),
                  join("\t", @sids, $tid),
                  "TYPE_$tid" );
    foreach my $key (@keyz) {
        delete $self->{EDGE_LOOKUP}{KEY}{$key}{$eid};
    }
    foreach my $sid (@sids) {
        # Delete nodes if they no longer have an edge associated
        my @eids = keys %{$self->{EDGE_LOOKUP}{KEY}{$sid}};
        if ($#eids < 0) {
            delete $self->{EDGE_LOOKUP}{KEY}{$sid};
            delete $self->{NODES}{ $sid };
        }
    }
    delete $self->{EDGES}{$eid};
    return $edge;
}

# For any edge added to the network, record useful indices:
sub add_edge_lookup {
    my $self   = shift;
    my ($edge) = @_;
    my $eid    = $edge->id;
    # Skip if this edge has already been keyed
    return if ($self->{EDGE_LOOKUP}{EID}{$eid});
    # {SID}
    #   Used to count edges attached to a node - edge_count()
    #   Recover all edges for a node - edges_from_node(), find_all_distances()
    #       _init_traverse()
    # {KEY}
    #   SID1/SID2 Used to index edges between two specific nodes:
    #     Existance of edge - hash_edge()
    #   SID1/READS/SID2 Index for particular edge
    #     Edge existance - expand(), connect_internal()
    # {READS}
    #   Indexes pairs of nodes by edge type - node_pairs_for_edge_type()
    #   Sort for print out - to_flat_file()
    #   Sort for HTML output - show_html_options()
    # {AID}
    #   Find edges by authority - 

    my @nodes = $edge->nodes;
    my @sids  = map { $_->id } @nodes;
    my $tid   = $edge->type->id;

    # Key off of:
    # edge_id
    $self->{EDGE_LOOKUP}{EID}{$eid} = $edge;


    # {SID}!!!

    # name1
    # name2
    # sorted( name1 name2 )
    # name1 name2 type_id
    # type_id (padded with TYPE_ to avoid collision with name_id)
    foreach my $key ( @sids,  
                      join("\t", sort @sids), 
                      join("\t", @sids, $tid),
                      "TYPE_$tid") {
        $self->{EDGE_LOOKUP}{KEY}{$key}{$eid} = $edge;
    }
}

sub edge_count {
    my $self = shift;
    my @list;
    if ($_[0]) {
        # The user wants edges for a specific node
        @list = $self->edges_from_node( @_ ); 
    } else {
        # Edge count for whole graph requested
        @list = keys %{$self->{EDGES}};
    }
    return $#list + 1;
}

# Return a list of edges from the requested node
# 
*edges_for_node = \&edges_from_node;
sub edges_from_node {
    my $self = shift;
    my ($request, $readreq, $secondRequest) = @_;
    my $mt   = $self->tracker;
    my $node = $mt->get_seq($request);
    return () unless ($node);
    my $sid = $node->id;
    my @keyz = ($sid);
    my $filter;
    if ($readreq) {
         # The user has supplied an edge type
        my @requests = ref($readreq) ? @{$readreq} : ($readreq);
        my %texts;
        foreach my $req (@requests) {
            my ($type, $dir) = $mt->get_type($req);
            if ($type) {
                map { $texts{$_}++ } $type->reads($dir || undef);
            }
        }
        if ($secondRequest) {
            # User wants specific node -- reads --> node edges
            my $node2 = $mt->get_seq($secondRequest);
            return () unless ($node2);
            @keyz = ();
            foreach my $reads (keys %texts) {
                my ($type, $dir) = $mt->get_type($reads);
                my @sids = ($sid, $node2->id);
                @sids = reverse @sids if ($dir < 0);
                push @keyz, join("\t", @sids, $type->id);
            }
        } else {
            # User wants to filter hits based on a type
            $filter = \%texts;
        }
    } elsif ($secondRequest) {
        # User wants all edges between two nodes
        my $node2 = $mt->get_seq($secondRequest);
        return () unless ($node2);
        @keyz = ( join("\t", sort ($sid, $node2->id)) );
    }
    my @populated;
    foreach my $key (@keyz) {
        push @populated, $key if (exists $self->{EDGE_LOOKUP}{KEY}{$key});
    }
    return () if ($#populated < 0);
    my @edges;
    if ($filter) {
        # User wants to discard any nodes not matching an edge type
        foreach my $key (@populated) {
            foreach my $edge (values %{$self->{EDGE_LOOKUP}{KEY}{$key}}) {
                my @reads = $edge->reads_careful($node);
                my $count = 0;
                map { $filter->{$_} ? $count++ : undef } @reads;
                push @edges, $edge if ($count);
            }
        }
    } else {
        foreach my $key (@populated) {
            push @edges, values %{$self->{EDGE_LOOKUP}{KEY}{$key}};
        }
    }
    return @edges;
}

sub nodes_from_node {
    my $self = shift;
    my ($request) = @_;
    $self->benchstart;
    my @retval;
    foreach my $edge ( $self->edges_from_node( @_ ) ) {
        my $node = $edge->nodes( $request );
        push @retval, $node if ($node);
    }
    $self->benchstop;
    return @retval;
}

*edges_hash = \&edge_hash;
sub edge_hash {
    my $self = shift;
    my $hash = {};
    my ($request) = @_;
    foreach my $edge ( $self->edges_from_node( @_ ) ) {
        my ($reads, $theRequest, $other) = $edge->reads($request);
        $hash->{$reads}{$other->id}{$edge->id} = $edge;
    }
    return $hash;
}


# Copy edges from one network to another
sub copy_edges {
    my $self = shift;
    $self->benchstart;
    my ($nw) = @_;
    foreach my $edge ( $nw->each_edge ) {
        $self->copy_single_edge( $edge );
    }
    $self->benchstop;
}

sub copy_single_edge {
    my $self = shift;
    my ($edge) = @_;
    $self->benchstart;
    if ($edge->id !~ /^\d+$/) {
        # This is a transient edge
        ($edge) = $self->tracker->make_transient_edge( -edge => $edge );
    }
    $self->add_edge($edge);
    $self->benchstop;
}


# CLEAN UP
# GVgroup
#     $self->{GROUPAUTH}   = {};
#    $self->{GROUPREADS}  = {};
# edge_ids_in_group
# authorities_for_group
# nodes_in_group


sub group_edge {
    my $self = shift;
    my ($edge, $parent) = @_;
    my $eid = $edge->id;
    # An edge can belong to only one group:
    return $self->{GROUPS}{$eid} if ($self->{GROUPS}{$eid});
    # Defining an edge requires the parent to be defined:
    return undef unless ($parent);
    $parent = $self->node($parent);
    my $pid = $parent->id;
    my ($reads, $theRequest, $child) = $edge->reads($parent);
    # Key the group as "Parent edge_type"
    my $key   = "$pid\t$reads";
    my $group = $self->{GROUPS}{$key};
    unless ($group) {
        # We need to define a new group
        # Two IDs - one for the group of edges, one for group of nodes.
        $self->{GROUPS}{$key} = $group = {
            egid   => $self->tracker->new_unique_id(),
            ngid   => $self->tracker->new_unique_id(),
            key    => $key,
            reads  => $reads,
            parent => $parent,
            edges  => {},
        };
        foreach my $key ('egid', 'ngid') {
            my $id = $group->{$key};
            $self->{GROUPS}{"group_$id"} = $group;
        }
    }
    # If not done already, add the edge to the group:
    $group->{edges}{$eid} ||= $edge;
    # If not done already, and the edge_id to the group lookup:
    $self->{GROUPS}{$eid} ||= $group;
    return $group;
}

sub ungroup_edge {
    my $self = shift;
    my ($edge) = @_;
    my $eid = $edge->id;
    return undef unless (exists $self->{GROUPS}{$eid});
    my $group = $self->{GROUPS}{$eid};
    my $key = $group->{key};
    delete $group->{edges}{$eid};
    delete $self->{GROUPS}{$eid};
    return $group;
}

sub is_grouped {
    my $self = shift;
    my ($edge, $parent) = @_;
    my $eid = $edge->id;
    # This edge is not part of any group:
    return 0 unless (exists $self->{GROUPS}{$eid});
    my $group = $self->{GROUPS}{$eid};
    # The edge is part of a group, and user does not care who parent is:
    return $group->{egid} unless ($parent);
    $parent = $self->node($parent);
    # Return true if the parent matches the assigned group parent:
    return ($group->{parent}->id == $parent->id) ? $group->{egid} : 0;
}

# distance() is very important to assure that recursion occurs to the
# maximum depth in loop structures. It tracks the minimum distance
# between two nodes - used in expand(), one of the nodes is always the
# root. It is a set/get routine. If you pass it a value, it will set
# that, unless the value was already set and the new value is greater
# than the old value. A node will then always recurse if its distance
# is within recursion limits.

sub distance {
    my $self = shift;
    $self->benchstart;
    my ($name1, $name2, $distance) = @_;
    my @names = sort ($name1, $name2);
    $self->{DISTANCE}{$names[0]} ||= {};
    if (defined $distance) {
        # The user is trying to set a distance
	if ( defined $self->{DISTANCE}{$names[0]}{$names[1]}) {
	    # If the distance was already set, reset if new is shorter
	    $self->{DISTANCE}{$names[0]}{$names[1]} = $distance
		if ($self->{DISTANCE}{$names[0]}{$names[1]} > $distance);
	} else {
	    # Set the distance if it was not done so already
	    $self->{DISTANCE}{$names[0]}{$names[1]} = $distance;
	}
    }
    $self->benchstop;
    return (defined $self->{DISTANCE}{$names[0]}{$names[1]}) ?
	$self->{DISTANCE}{$names[0]}{$names[1]} : undef;
}

sub find_all_distances {
    my $self = shift;
    $self->benchstart;
    my $mt   = $self->tracker;
    my ($name, $level, $data) = @_;
    $level ||= 0;
    my $node = $mt->get_seq( $name );
    unless ($node) {
	$mt->error("Can not find a sequence for '$name'.");
        $self->benchstop;
	return;
    }
    my $sid  = $node->id;
    unless (exists $self->{NODES}{ $sid } ) {
	my $sname = $node->namespace_name;
	$mt->error("Can not calculate distances for $sname as it is not ".
                   "in the Network.");
        $self->benchstop;
	return;
    }
    # Initialize data structure if needed
    unless ($data) {
        $data = {
            root => $sid,
            seen => {
                $sid => $level,
            },
        };
        $self->distance($sid, $sid, 0);
    }
    my @edges = $self->edges_from_node( $sid );
    if ($#edges < 0) {
        # Exit if there are no edges from this node
        $self->benchstop;
        return;
    }
    # Increment the distance from the node
    $level++;
    my $root = $data->{root};
    my @recurse;
    foreach my $edge (@edges) {
        my $other = $edge->other_seq($node);
        my $oid   = $other->id;
        my $prior = $data->{seen}{$oid};
        if ( defined $prior ) {
            # We have already encountered this child node...
            if ($prior > $level) {
                # ... the child should have shorter distance than it does
                $data->{seen}{$oid} = $self->distance($root, $oid, $level);
                # Recurse to see if it has children in need of update
                push @recurse, $oid;
            } elsif ($prior < $level - 2) {
                # ... the CURRENT node should have a shorter distance
                $level = $prior + 1;
                $data->{seen}{$sid} = $self->distance($root, $sid, $level);
                # warn sprintf("%s [%2d] %s = %d UPDATE via %s\n", "  " x $level, $level, $node->name, $data->{seen}{$sid}, $other->name);
                # We should re-calculate from this node again -
                # Set to recurse ONLY on this node and halt current loop
                @recurse = ($sid);
                last;
            }
        } else {
            # Have not encountered the node yet
            $data->{seen}{$oid} = $self->distance($root, $oid, $level);
            push @recurse, $oid;
        }
       # warn sprintf("%s [%2d] %s = %d\n", "  " x $level, $level, $other->name, $data->{seen}{$oid});
    }

    # Now repeat process for all new nodes encountered:
    foreach my $oid (@recurse) {
	$self->find_all_distances($oid, $level, $data);
    }
    $self->benchstop;
    return $data->{seen};
}

*all_node_ids = \&all_seq_ids;
sub all_seq_ids {
    my $self = shift;
    return keys %{$self->{NODES}};
}

*all_nodes = \&all_seqs;
sub all_seqs {
    my $self = shift;
    return map { $self->node($_) } $self->all_seq_ids;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub expand_all {
    my $self   = shift;
    my @all    = $self->all_seq_ids;
    my @retval = $self->expand( @_, -node => \@all );
    return @retval;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 expand

 Title   : expand
 Usage   : $nw->expand()

 Function: Expands the network following the provided rules.

 Returns : An array of name_ids encountered during expansion
 Args    : Optional arguments array:

     -node Default is the first root that you set. This is the node to
           begin expansion from. It can be any node in the network,
           but it needs to be at least somewhere in the network. You
           can use add_root() to add a new node.

     -root Alias for node

  -recurse Default 0. The maximum depth that you wish to trace the
           network. Zero will expand out one edge in the network from
           the starting node. 3 will expand up to 4 edges from the
           starting node.

  -groupat Default 5. A problem with networks is they can expand too
           rapidly. This is particularly true if each edge has many
           child nodes. If a particular type of edge (that is, an edge
           like 'is an alias of') has more children than specified by
           -groupat, then expansion along that edge will be halted.

  -nogroup Default undef. You may specify this attribute as one or
           more classes. Any sequence that is a member of such a class
           will be removed from groups, in order to allow network
           expansion through such nodes. This option was added to
           allow RefSeq nodes to be fully displayed.

 -sametaxa Default 0. If this value is true, then the taxa for the
           starting node will be considered. If the starting node has
           no taxa assigned, this parameter is ignored. If one or more
           taxa are assigned to the starting node, then any child
           added to the expanding network must be assigned at least
           one of those taxa. If the child has no taxa assigned, it
           will still be added to the network.

  -settaxa Normally, sametaxa will match the taxa defined by the
           current node. You can instead define the taxa explicitly
           using this term.

    -limit Default 0. Ignored if false. This parameter is passed to
           MapTracker::get_edge_dump(), and specifies the maximum
           number of any type of edge to return. Often needed
           to prevent explosion when passing through highly populated
           nodes.

 -keeptype Default []. Also used by get_edge_dump(), if not an
           empty array it specifies that only those edge types
           should be used. Can be either non-directional names (like
           'translate') or directional (like 'can be translated to
           generate').

 -haltclass Optional list of classes. If a node is a member of any of
           the classes, then no recursion will continue through the
           node, although the node will be added.

 -halttype Optional list of types. If such edges exist, the nodes they
           connect to will be added to the network, but will not be
           used for recursion, unless reached via another edge.

 -decorate Default 'class taxa'. Specifies if nodes on the tree should
           have additional meta data automatically recovered for them.

=cut

sub expand {
    my $self = shift;
    my $mt   = $self->tracker;
    my $args = $self->parseparams( -node      => $self->{ROOTS}[0],
				   -haltclass => undef,
                                   -nogroup   => undef,
				   -recurse   => 0,
				   -groupat   => 5,
				   -sametaxa  => 0,
				   -istaxa    => {},
				   -decorate  => 'class taxa',
				   -isdecor   => {},
				   @_ );

    my $recurse = $args->{RECURSE} || 0;
    if ($recurse < 0) {
        # This is really a request to connect_internal
        return $self->connect_internal( @_ );
    }
    $self->benchstart;

    my $isparent = 0;
    my @nodes;
    if ($args->{_DATA_}) {
        # We are continuing an earlier expansion - we KNOW that we are
        # passing a list of name_ids in -NODE
        my $hash = $mt->bulk_seq_for_ids( @{$args->{NODE}} );
        @nodes = values %{$hash};
    } else {
        # Initiating a new expansion
        no warnings 'recursion';
	$isparent = 1;
        my $nodelist = $self->_init_expand( $args );
        @nodes = $nodelist ? @{$nodelist} : ();
    }
    if ($#nodes < 0) {
        $self->benchend;
        return $self->_finish_expand( $args, $isparent)
    }
    my $exdat     = $args->{_DATA_};
    my $firstpass = [];
    my %requested = map { $_->id => $_ } @nodes;
    while (my @set = splice(@nodes, 0, 50)) {
        # Get edges in bulk, 50 nodes at a time
        my $edge_data = $mt->get_edge_dump( %{$args},
                                            -name   => \@set,
                                            -return => 'object array' );
        # Tease out which edges belong to which node
        foreach my $edge (@{$edge_data}) {
            # Ignore the edge if we have already traversed it:
            next if ($exdat->{VISITED}{EID}{$edge->id});
            delete $edge->{_PROVENANCE_};
            my $hits = 0;
            my @enodes = $edge->nodes;
            foreach my $node (@enodes) {
                my $nid = $node->id;
                if ($requested{$nid}) {
                    # This node was one of the requested roots
                    $edge->{_PROVENANCE_}{$nid} = $node;
                    $hits++;
                }
            }
            if ($hits == 2) {
                # This edge is between two roots - always keep
                $self->add_edge( $edge );
                my @sids = map { $_->id } @enodes;
                # Note that we reached these nodes:
                map { $exdat->{VISITED}{SID}{$_->id} = $_ } @enodes;
                # Make sure that we add recipricol distances
                $self->_set_distances( $sids[0], $sids[1], $exdat);
                $self->_set_distances( $sids[1], $sids[0], $exdat);
                next;
            } elsif ($hits == 0) {
                warn "Recovered edge that is not from any request!";
                warn $edge->to_text;
                next;
            }
            push @{$firstpass}, $edge;
        }
    }
    if ($#{$firstpass} < 0) {
        $self->benchend;
        return $self->_finish_expand( $args, $isparent);
    }

    my $secondpass = $firstpass;
    if (exists $exdat->{SAMETAXA}) {
        # We need to filter results by species
        my @allnodes = map { $_->nodes } @{$firstpass};
        my @nids = map { $_->id } @allnodes;
        $self->_bulk_taxa_load( @nids );
        my @keep;
        foreach my $edge (@{$firstpass}) {
            next unless ($self->_passes_taxa( $edge, $args ));
            push @keep, $edge;
        }
        $secondpass = \@keep;
    }
    if ($#{$secondpass} < 0) {
        $self->benchend;        
        return $self->_finish_expand( $args, $isparent);
    }

    my (%edge_reads, @dorecurse, %othersids);
    foreach my $edge (@{$secondpass}) {
        my $eid   = $edge->id;
        my ($req) = values %{$edge->{_PROVENANCE_}};
        my ($reads, $theRequest, $other) = $edge->reads($req);
        my $oid = $other->id;
        my $nid = $req->id;
        $othersids{$oid}++;
        $exdat->{VISITED}{EID}{$eid} = 1;
        $self->add_edge( $edge );
        my $dist = $self->_set_distances( $req->id, $oid, $exdat );
        # We need to group by requested node to process grouping logic
        
        push @{$edge_reads{$nid}{ $reads }}, [ $edge, $other, $dist ];
    }

    my $haltclass = $exdat->{HC_PARSED};
    my $halttype  = $exdat->{HT_PARSED};
    my $ng        = $exdat->{NG_PARSED};
    my $groupat   = $args->{GROUPAT};

    if ($ng || $haltclass) {
        # We will be testing the class of the nodes
        $self->_bulk_class_load( keys %othersids );
    }

    # Now see if any of the edge types need grouping:
    foreach my $nid (keys %edge_reads) {
        while (my ($reads, $dat) = each %{$edge_reads{$nid}}) {
            my $rcount = $#{$dat} + 1;
            my @recurse;
            foreach my $d (@{$dat}) {
                my ($edge, $other, $dist) = @{$d};
                my $oid = $other->id;
                my $isnovel = $exdat->{VISITED}{SID}{$oid} ? 0 : 1;
                $exdat->{VISITED}{SID}{$oid} = $other;
                if ($rcount >= $groupat && 
                    !$exdat->{PRIOR_EDGE}{ $edge->id} &&
                    (!$ng || !$other->is_class(@{$ng}))) {
                    # 1. There are enough edges to form a group
                    # 2. The edge was not already in the network
                    # 3. The user did not make an ungroup request
                    #    OR they did, and node is not one of those classes
                    # -> group this edge
                    $self->group_edge( $edge, $edge->other_node($other) );
                    next;
                }
                # Do not recurse a node if this edge should be halted on
                next if ($halttype->{$reads});
                next unless ($isnovel);
                # We did not group this edge - add it to recursion, if it is
                # not too far away from the root:
                warn "Distance not defined:\n" . $edge->to_text
                    unless (defined $dist);
                push @recurse, $oid if ($dist < $recurse);
            }
            push @dorecurse, @recurse;
            if ($mt->{DEBUG}) {
                warn sprintf
                    ("   %s %s: %d nodes, recursion for %d\n", 
                     ' ' x $args->{THISLEVEL}, $reads, $rcount, $#recurse+1);
            }
        }
    }
    if ($#dorecurse < 0) {
        $self->benchend;
        return $self->_finish_expand( $args, $isparent);
    }

    if ($haltclass) {
        # We want to filter nodes based on class
        my $seqs = $self->_bulk_class_load( @dorecurse );
        my @keep;
        foreach my $nid (@dorecurse) {
            push @keep, $nid
                unless ($seqs->{$nid}->is_class( @{$haltclass} ) );
        }
        @dorecurse = @keep;
    }
    $self->expand( %{$args},
                   -thislevel => $args->{THISLEVEL} + 1,
                   -node      => \@dorecurse, );

    $self->benchend;
    return $self->_finish_expand( $args, $isparent);
}

sub _request_to_nodes {
    my $self = shift;
    my ($args) = @_;
    my $rarg = $args->{ROOT} || $args->{NODE} || $args->{NODES};
    return undef unless ($rarg);
    my @requests = (ref($rarg) && ref($rarg) eq 'ARRAY') ? @{$rarg} : ($rarg);
    my @nodes;
    my $mt = $self->tracker;
    foreach my $req (@requests) {
        my $seq = $mt->get_seq( $req );
        next unless ($seq);
        unless (exists $self->{NODES}{ $seq->id } ) {
            warn "Could not analyze ". $seq->name." as it is not in Network. ".
                "You can use add_root() to add it as a new root.";
            next;
        }
        push @nodes, $seq;
    }
    return ($#nodes < 0) ? undef : \@nodes;
}

sub _init_expand {
    my $self   = shift;
    my $mt     = $self->tracker;
    my ($args) = @_;

    my $nodelist = $self->_request_to_nodes($args);
    return undef unless ($nodelist);
    
    $args->{THISLEVEL} = 0;
    $args->{_DATA_} = {
        HC_PARSED => undef,
        HT_PARSED => {},
        VISITED   => {
            SID   => {},
            EID   => {},
        },
        START     => time,
        PRIOR_EDGE => { map { $_->id => 1 } $self->each_edge },
    };
    my $tax_override;
    if (my $settax = $args->{SETTAXA}) {
        # The user is manually specifying a species
        my ($check) = $mt->get_taxa( $settax );
        if ($check) {
            $tax_override = $check->id;
        } else {
            $self->error
                ("Could not -settaxa to '$settax' - no such taxa. Ignored.");
            delete $args->{SAMETAXA};
        }
    }

    if ($args->{SAMETAXA}) {
        # Pre-load NCBI taxids for each root noode
        $self->_bulk_taxa_load( map { $_->id } @{$nodelist});
    }

    # Record information for each of these root nodes
    foreach my $node (@{$nodelist}) {
        my $sid = $node->id;
        $args->{_DATA_}{HAS_ROOT}{$sid}{$sid} = 1;
        $self->distance($sid, $sid, 0);
        if ($args->{SAMETAXA}) {
            # The user wants to only keep consitent species info
            if ($tax_override) {
                $args->{_DATA_}{SAMETAXA}{$sid}{$tax_override} = 1;
            } else {
                $args->{_DATA_}{SAMETAXA}{$sid} = {
                    map { $_ => 1 } @{$self->{FAST_TAXA}{$sid}},
                };
            }
	}
    }
    $self->{LIMIT_SET} = $args->{LIMIT};

    if ($mt->{DEBUG}) {
        my $msg = ('-' x 70) . "\n";
        my @names = map { $_->name } @{$nodelist};
        my $what = ($#names < 10) ? join(',', @names) : ($#names+1). " Nodes";
        $msg .= sprintf("Beginning expansion from %s, depth %d", 
                        $what, $args->{RECURSE} || 0);
        $msg .= sprintf(", grouping at %d", $args->{GROUPAT}) 
            if ($args->{GROUPAT});
        $msg .= "\n";
        if (my $gt = $args->{KEEPTYPE} || $args->{TYPE}) {
            my @requestType  = ref($gt) eq 'ARRAY' ? @{$gt} : ($gt);
            my @tnames;
            foreach my $t (@requestType) {
                my ($type, $dir) = $mt->get_type($t);
                next unless $type;
                push @tnames, $dir ? $type->reads($dir) : $type->name;
            }
            $msg .= "  Keeping : ".join(", ", sort @tnames)."\n";
        } else {
            $msg .=  " Keeping all edges\n";
        }
        warn $msg;
    }
    if ($args->{HALTCLASS}) {
        # HC_PARSED is an internal hash key - it is generated once per
        # recursion using param_to_list
        $args->{_DATA_}{HC_PARSED} = 
            [$mt->param_to_list($args->{HALTCLASS},'class')];
    }
    # HT_PARSED is an internal hash key - it is generated once per
    # recursion using param_to_list
    if (my $htreq = $args->{HALTTYPE}) {
        my @reqs = ref($htreq) ? @{$htreq} : ($htreq);
        foreach my $req (@reqs) {
            my ($type, $dir) = $mt->get_type($req);
            my @reads = $dir ? ($type->reads($dir)) : $type->reads;
            foreach my $read (@reads) {
                $args->{_DATA_}{HT_PARSED}{$read} = 1;
            }
        }
    }
    if ($args->{NOGROUP}) {
        # NG_PARSED is an internal hash key - it is generated once per
        # recursion using param_to_list
        $args->{_DATA_}{NG_PARSED} = 
            [$mt->param_to_list($args->{NOGROUP},'class')];
    }
    foreach my $key ('ROOT', 'NODE', 'SAMETAXA') {
        delete $args->{$key};
    }
    return $nodelist;
}

sub _finish_expand {
    my $self = shift;
    my ($args, $isparent) = @_;
    return () unless ($isparent);
    $self->benchstart;
    # We are now at the end of the parent call - not in a
    # recursive child call.
    my $dectaxa = ($args->{DECORATE} =~ /tax/i   ) ? 1 : 0;
    my $declass = ($args->{DECORATE} =~ /class/i ) ? 1 : 0;
    my $decaka  = ($args->{DECORATE} =~ /short/i ) ? 1 : 0;

    my @found = values %{$args->{_DATA_}{VISITED}{SID}};
    my @fids  = keys   %{$args->{_DATA_}{VISITED}{SID}};
    use warnings 'recursion';
    # Request to decorate node with taxa:
    $self->_bulk_taxa_load( @fids) if ($dectaxa);
    # Request to decorate node with classes:
    $self->_bulk_class_load( @fids ) if ($declass);
    $self->benchstop;
    return @found;
}

sub _set_distances {
    # Updates distances to root.
    # primary = node that already has been related to one or more roots
    # secondary = node that was just attached to primary
    # exdat = data structure holding root assignments
    my $self = shift;
    my ($primary, $secondary, $exdat) = @_;
    my $min = 99999;
    foreach my $rid (keys %{$exdat->{HAS_ROOT}{$primary}}) {
        # Retrieve the distance to that root
        my $dist = $self->distance( $rid, $primary );
        # The other node is that distance + 1 away - unless it has already
        # been set to a shorter distance
        my $odist = $self->distance( $rid, $secondary, $dist + 1);
        $min = $odist if ($odist < $min);
        # The secondary node is now also related to that root

        # THIS MAY NOT BE TRUE FOR DIRECTIONAL EDGES THAT JOIN TWO REQUESTS!

        $exdat->{HAS_ROOT}{$secondary}{$rid}++;
        # warn "$rid -[ $primary ]-> $secondary = $odist";
    }
    return $min;
}

sub _passes_taxa {
    my $self = shift;
    my ($edge, $args) = @_;
    my $exdat = $args->{_DATA_};
    # What is the query node?
    my ($req) = values %{$edge->{_PROVENANCE_}};
    my $reqid = $req->id;
    # What root(s) does it hail from?
    my @rootids = keys %{$exdat->{HAS_ROOT}{$reqid}};
    my %uniqtax = map { $_ => 1 } map { 
        keys %{$args->{_DATA_}{SAMETAXA}{$_}} } @rootids;
    my @taxids = keys %uniqtax;
    # If there are no values to test, then this edge passes the test:
    return -1 if ($#taxids < 0);
    my $other = $edge->other_node($req);
    my $oid = $other->id;
    # Taxa ids for the other node should have been _bulk_loaded already
    my @otax = @{$self->{FAST_TAXA}{$oid}};
    # Likewise, if the other entry has no taxa assignments, pass the test:
    return -1 if ($#otax < 0);
    foreach my $taxid ( @otax ) {
        # If we find a match, pass the test
        return 1 if ($uniqtax{ $taxid });
    }
    # No matches found - return 0 (fail):
    return 0;
}

sub _bulk_edge_tag_load {
    my $self = shift;
    my $mt   = $self->tracker;
    my @needed;
    foreach my $edge (@_) {
        next if ($edge->{COLLECTION});
        $edge->{COLLECTION} = BMS::MapTracker::TagCollection->new( $edge );
        next unless ($edge->id =~ /^\d+$/);
        push @needed, $edge;
    }
    my %edges = map { $_->id => $_ } @needed;
    my @eids  = keys %edges;

    my $bulksize = 20;
    $bulksize    = $#eids+1 if ($bulksize > $#eids + 1);
    my $bulkList = join(",", map { '?' } (1..$bulksize));
    my $bulkSTH  = $mt->dbi->prepare
        ( "SELECT edge_id, authority_id, tag_id, value_id, numeric_value".
          "  FROM edge_meta WHERE edge_id IN ( $bulkList )");
    my %alltvs;
    while (my @binds = splice(@eids, 0, $bulksize)) {
        my $short = $bulksize - ($#binds + 1);
        push @binds, map { $binds[0] } (1..$short) if ($short > 0);
        my $rows = $bulkSTH->selectall_arrayref( @binds );
        my @tagvals = map { $_->[2], $_->[3] } @{$rows};
        my $tvs = $mt->bulk_seq_for_ids( @tagvals );
        map { $alltvs{$_} = $tvs->{$_} } keys %{$tvs};
        foreach my $row (@{$rows}) {
            my $eid = shift @{$row};
            $edges{$eid}->add_tag( @{$row} );
        }
    }
    # We could bulk load callback data here, too
}

sub _bulk_class_load {
    my $self = shift;
    $self->benchstart;
    my $mt   = $self->tracker;
    my $seqs = $mt->bulk_seq_for_ids( @_ );
    my @needed;
    foreach my $seq (values %{$seqs}) {
        push @needed, $seq->id unless ( $seq->task('read_classes') );
    }
    if ($#needed < 0) {
        $self->benchstop;
        return $seqs;;
    }
    @needed = sort { $a <=> $b } @needed;

    my $bulksize = 20;
    $bulksize    = $#needed+1 if ($bulksize > $#needed + 1);
    my $bulkList = join(",", map { '?' } (1..$bulksize));
    my $bulkSTH  = $mt->dbi->prepare
        ( "SELECT name_id, class_id, authority_id".
          "  FROM seq_class WHERE name_id IN ( $bulkList )");
    foreach my $sid (@needed) {
        $seqs->{$sid}->task('read_classes', 1);
    }
    while (my @binds = splice(@needed, 0, $bulksize)) {
        my $short = $bulksize - ($#binds + 1);
        push @binds, map { $binds[0] } (1..$short) if ($short > 0);
        my $rows = $bulkSTH->selectall_arrayref( @binds );
        foreach my $row (@{$rows}) {
            my ($sid, $cid, $aid) = @{$row};
            $seqs->{$sid}->add_class($cid, $aid);
        }
    }
    $self->benchstop;
    return $seqs;
}

sub _bulk_taxa_load {
    my $self = shift;
    my %unique;
    foreach my $sid (@_) {
       $unique{$sid} = 1 unless ($self->{FAST_TAXA}{$sid});
    }
    my @needed = sort { $a <=> $b } keys %unique;
    return if ($#needed < 0);
    $self->benchstart;
    my $mt   = $self->tracker;

    my $bulksize = 20;
    $bulksize    = $#needed+1 if ($bulksize > $#needed + 1);
    my $bulkList = join(",", map { '?' } (1..$bulksize));
    my $bulkSTH  = $mt->dbi->prepare
        ( "SELECT name_id, tax_id, authority_id".
          "  FROM seq_species WHERE name_id IN ( $bulkList )");
    while (my @binds = splice(@needed, 0, $bulksize)) {
        my $short = $bulksize - ($#binds + 1);
        push @binds, map { $binds[0] } (1..$short) if ($short > 0);
        my $rows = $bulkSTH->selectall_arrayref( @binds );
        my $seqs = $mt->bulk_seq_for_ids( @binds );
        my %got;
        foreach my $row (@{$rows}) {
            my ($sid, $tid, $aid) = @{$row};
            $got{$sid}{$tid} = 1;
            # While we are here, read in taxa to seq objects:
            $seqs->{$sid}->add_taxa($tid, $aid);
        }
        foreach my $sid (@binds) {
            my $tids = $got{$sid} || {};
            $self->{FAST_TAXA}{$sid} = [ keys %{$tids} ];
            $seqs->{$sid}->task('read_taxa', 1);
        }
    }
    $self->benchstop;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 explicitly_expand

 Title   : explicitly_expand
 Usage   : $nw->explicitly_expand()

 Function: Very similar to expand() when -keeptype is used. In that
           circumstance, at any point in the iteration any of the
           allowed types may be followed. Using explicitly_expand()
           allows the user to define a seperate edge type (or types)
           for each level of iteration.

 Returns : An array of name_ids encountered during expansion
 Args    : Optional arguments array:

           All options available to expand, except for -keeptype and
           -recurse. Instead, you will provide -edgelist, which will
           be used for iterative substitution of -keeptype in calls to
           expand():

 -edgelist An array reference of edge types. Each entry can be either
           a single edge type, or an array reference of them.

           Alternatively, you may pass each entry in the edge list as
           separate parameters, ie -edge1, -edge2, -edge3. This option
           is included for easier implementation in PathWalker path
           files. Each param can be a single edge type, or hash ref of
           such.

 -complete Default 1. If true, then the network expansion is only
           allowed if at least one node was found at the very end of
           the expansion (that is, if at least one explicit path was
           fully followed).

   -update Default 1. If expansion was succesful, and -update is true,
           then the network will be expanded.

   -return Default 'encountered'. Defines the value to be returned on
           completion. The default will return a list of all nodes
           encountered in the expansion - this is the behavior of
           expand(). You may also specify 'terminal', which will only
           return the nodes found at the end of the most terminal
           edges. Mode 'leaf' is similar to terminal, but will include
           leaf nodes that did not complete traversal of the edge
           list. Alternatively, you may specify 'network', which will
           return a Network object used in the analysis of the path.

=cut

*expand_explicit = \&explicitly_expand;
sub explicitly_expand {
    my $self = shift;
    my $mt   = $self->tracker;
    my $args = $self->parseparams( -edgelist => undef,
                                   -complete => 1,
                                   -update   => 1,
                                   -return   => 'encountered',
				   @_ );

    my $node = $mt->get_seq
        ( $args->{NODE} || $args->{ROOT} || $self->{ROOTS}[0]);
    my $nid   = $node->id;
    $args->{NODE} = $node;
    
    my @edgelist;
    if (my $el = $args->{EDGELIST}) {
        @edgelist = ref($el) eq 'ARRAY' ? @{$el} : ( $el );
    } elsif ($args->{EDGE1}) {
        for my $i (1..100) {
            my $key = 'EDGE' . $i;
            my $kt = $args->{$key};
            delete $args->{$key};
            last unless ($kt);
            push @edgelist, $kt;
        }
    } else {
        $mt->error("You must define either -edgelist or -edge1, -edge2, etc");
        return ();
    }

    my %passed   = %{$args};

    foreach my $key ('NODE', 'ROOT', 'EDGELIST', 'COMPLETE') {
        delete $passed{$key};
    }

    my $rettype = 'TERM';
    if ($args->{RETURN} =~ /encounter/i) {
        $rettype = 'ENCT';
    } elsif ($args->{RETURN} =~ /net/i) {
        $rettype = 'NETW';
    }  elsif ($args->{RETURN} =~ /leaf/i) {
        $rettype = 'LEAF';
    }

    my $seed  = BMS::MapTracker::Network->new( -tracker => $mt );
    $seed->add_root( $node );
    # Network Object, Node list to expand from
    my @stack = ( [ $seed, $node  ] );
    # Mini-networks that could no longer extend:
    my @halted;
    # Nodes at the far reaches:
    my %terminal;
    for my $ei (0..$#edgelist) {
        my $kt = $edgelist[$ei];
        my @new_nets;
        foreach my $prev_data (@stack) {
            my ($old_nw, $new_node) = @{$prev_data};
            my $temp_nw = BMS::MapTracker::Network->new( -tracker => $mt );
            $temp_nw->add_root( $new_node );
            my @hits = $temp_nw->expand
                ( %passed,
                  -keeptype => $kt,
                  -recurse  => 0, );
            if ($#hits < 0) {
                # No edges leading off from this node
                #print "<pre>".$new_node->name." $kt -> no edges</pre>";
                $terminal{ $new_node->id } = $new_node 
                    unless ($rettype eq 'TERM' && $ei != $#edgelist);
                push @halted, [ $old_nw, $ei - 1 ];
                next;
            }
            # printf("<pre>%s%s %s -> %s</pre>", "  " x ($ei+1), $new_node->name, $kt, $#hits == 0 ? $hits[0]->name : $#hits + 1 . " edges");
            # At least some hits. Copy over the old edges
            foreach my $hit (@hits) {
                my $new_nw = BMS::MapTracker::Network->new( -tracker => $mt );
                $new_nw->add_root( $node );
                $new_nw->copy_edges( $old_nw );
                foreach my $edge ($temp_nw->edges_from_node($hit)) {
                    $new_nw->copy_single_edge( $edge );
                }
                push @new_nets, [ $new_nw, $hit ];
            }
        }
        @stack = @new_nets;
    }

    # The stack holds the last set of networks to be expanded
    my @mini_nets = map { $_->[0] } @stack;
    if ($args->{COMPLETE}) {
        # Request that the path be completely followed to be allowed
        # Return if no paths went all the way.
        return () if ($#mini_nets < 0);
    } else {
        # Keep all paths followed
        push @mini_nets, map { $_->[0] } @halted;
    }

    my @retval;
    my %encountered;
    my $global_nw = BMS::MapTracker::Network->new( -tracker => $mt );
    $global_nw->add_root( $node );
    
    foreach my $mini_net (@mini_nets) {
        # Modify our network, if so requested
        $self->copy_edges($mini_net) if ($args->{UPDATE});
        if ($rettype eq 'ENCT') {
            map { $encountered{ $_->id } = $_ } $mini_net->each_node;
        } elsif ($rettype eq 'NETW') {
            $global_nw->copy_edges( $mini_net );
        }
    }

    if ($rettype eq 'ENCT') {
        delete $encountered{ $nid };
        return ( values %encountered) ;
    } elsif ($rettype eq 'NETW') {
        return $global_nw;
    }

    # Make sure terminal nodes are noted from the stack
    map { $terminal{ $_->[1]->id } = $_->[1] } @stack;
    delete $terminal{ $nid };
    return values %terminal;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 telescope

 Title   : telescope
 Usage   : $nw->telescope()

 Function: This method uses the same options as expand, and performs
           much the same task. However, rather than storing a faithful
           representation of the full network, the resulting data will
           be collapsed into a single edge type attaching the starting
           node to one or more nodes reaped from the resulting
           expansion.

 Returns : An array of seqnames that were recovered
 Args    : Optional arguments array:

           All options available to expand. In addition, you should
           provide the following rule options to define how the
           subgraph should be collapsed:

 -teleclass One or more classes that you wish to keep

  -teleedge A single edge type used to associate kept nodes with the
            root used to telescope. The default is "is a shorter term
            for", as it is assumed that this method will usually be
            used to find distantly-linked alias for a node (at least
            that is what the method was initially designed for).

 -usesmallest If true, and more than one node was found after
            telescoping, then use the one with the shortest length name.


=cut

sub telescope {
    my $self = shift;
    my $mt   = $self->tracker;
    my $args = $self->parseparams( -teleclass => undef,
				   -teleedge  => 'is a shorter term for',
				   @_ );
    
    my $node = $mt->get_seq
        ( $args->{NODE} || $args->{ROOT} || $self->{ROOTS}[0]);
    my $nid   = $node->id;
    $args->{NODE} = $node;
    
    my @kc = $mt->param_to_list($args->{TELECLASS},'class');
    return if ($#kc < 0);

    my @found;
    if ($args->{EDGELIST} || $args->{EDGE1}) {
        # Request for explicit expansion
        @found = $self->explicitly_expand( %{$args},
                                           -update => 0,
                                           -return => 'terminal' );
        
    } else {
        # Make a subnetwork that we will use to expand:
        my $nw = BMS::MapTracker::Network->new( -tracker => $mt );
        $nw->add_root( $node );
        $nw->expand( %{$args} );
        @found = $nw->all_seqs;
    }

    my @survivors;
    $self->_bulk_class_load( map { $_->id } @found );
    foreach my $seq (@found) {
        push @survivors, $seq if ($seq->is_class( @kc ));
    }
    return () if ($#survivors < 0);
    
    my ($type, $dir) = $mt->get_type($args->{TELEEDGE});
    my $tid   = $type->id;
    my $auth  = $mt->get_authority('Telescope');
    my $aid   = $auth->id;
    if ($args->{USESMALLEST} && $#survivors > 0) {
        @survivors = sort { length($a->name) <=> length($b->name) ||
                                $a->name cmp $b->name } @survivors;
        @survivors = ($survivors[0]);
    }
    foreach my $seq (@survivors) {
        my $sid = $seq->id;
        # Do not relate to self:
        next if ($sid == $nid);
        my ($edge) = $mt->make_transient_edge
            ( -name1 => $nid,
              -name2 => $sid,
              -type  => $args->{TELEEDGE},
              -space => 'Telescope',
              -auth  => 'Telescope',);
        $self->add_edge($edge);
    }
    return @survivors;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 connect_internal

 Title   : connect_internal
 Usage   : $nw->connect_internal()

 Function: This method will not expand the number of nodes within the
           network, but will instead try to find new edges with
           existing nodes.

 Returns : A list of nodes that had new edges added

 Args    : Optional arguments array:

      -node Required. One or more nodes that you wish to connect
            from. A single value or array ref is allowed. You can
            specify nodes that are not already in the network - they
            will be added to the net before beginning the analysis.

  -keeptype Default []. The edge or edge types you wish to allow. If
            empty or an empty array, then all edges will be allowed.

    -target The node or nodes you want to connect to. If not provided,
            then all nodes in the network will be considered.


=cut

sub connect_internal {
    my $self = shift;
    my $mt   = $self->tracker;
    my $args = $self->parseparams( -node     => [],
                                   -keeptype => [],
                                   -target   => [],
				   @_ );
    my @from;
    my $nd = $args->{NODE};
    if (ref($nd) && ref($nd) eq 'ARRAY') {
        @from = @{$nd};
    } elsif ($nd) {
        @from = ($nd);
    }
    if ($#from < 0) {
        $mt->error("You must define at least 1 -node for connect_internal");
        return 0;
    }
    $self->benchstart;

    # Make sure froms are as IDs
    @from =  sort {$a <=> $b } map { $self->node( $_ )->id } @from;

    # What are valid target IDs?
    my @to   = ref($args->{TARGET}) ? @{$args->{TARGET}} : ( $args->{TARGET});
    if ($#to < 0) {
        # Use all existing nodes as targets
        @to = sort {$a <=> $b} $self->all_seq_ids;
    } else {
        # Make sure targets are as IDs
        @to =  sort {$a <=> $b } map { $self->node( $_ )->id } @to;
    }

#    my $fcl = $#from == 0 ? "= $from[0]" : "IN (".join(",", @from).")";
#    my $tcl = $#to   == 0 ? "= $to[0]"   : "IN (".join(",", @to).")";

    if ($mt->{DEBUG}) {
        warn sprintf("\nConnecting internal nodes: From = %d, To = %d\n",
               $#from + 1, $#to + 1);
    }

    my $gt = $args->{KEEPTYPE} || [];
    my @requestType  = ref($gt) eq 'ARRAY' ? @{$gt} : ($gt);
    my %types = ();
    if ($#requestType > -1 ) {
	# Only specific types have been requested
	foreach my $t (@requestType) {
	    my ($type, $dir) = $mt->get_type($t);
	    next unless $type;
            $types{$dir} ||= {};
            $types{$dir}{$type->id} = 1;
	}
    } else {
        %types = ( 0 => undef );
    }
    my $dbi = $mt->dbi;
    my %nodes;

    # @from and @to could be VERY large. Break the space into chunks to
    # avoid killing DB - managed to do this with PostGres, which is
    # supposed to have arbitrary query length
    my $chunk = 20;
    my (@fcls, @tcls);
    for (my $fs = 0; $fs < $#from; $fs += $chunk) {
        my $fe = $fs + $chunk - 1;
        $fe = $#from if ($fe > $#from);
        # A block of up to $chunk 'from' nodes:
        my @cf = @from[$fs..$fe];
        my $fcl = $#cf == 0 ? "= $cf[0]" : "IN (".join(",", @cf).")";
        push @fcls, $fcl;
    }
    for (my $ts = 0; $ts < $#to; $ts += $chunk) {
        my $te = $ts + $chunk - 1;
        $te = $#to if ($te > $#to);
        # A block of up to $chunk 'to' nodes:
        my @ct = @to[$ts..$te];
        my $tcl = $#ct == 0 ? "= $ct[0]" : "IN (".join(",", @ct).")";
        push @tcls, $tcl;
    }

    # Crude estimate on size of task - will be most accurate for large
    # matrices, which is where the estimate will be needed
    my $matrix_size = ($#fcls +1) * ($#tcls  + 1);
    my $waitdat = {
        quiet  => 0,
        start  => time,
        intv   => 15,
        header => "Internally connecting network:\n",
        total  => $matrix_size,
        unit   => 'iteration',
    };

    my @tabs = ('edge e');
    my $tagwh = "";
    if (my $filt = $args->{FILTER}) {
        my ($tags, $em) = $mt->_process_tag_filters($filt);
        if ($tags) {
            $tagwh = join(" AND ", map { "$_.edge_id = e.edge_id" } @{$em});
            $tagwh .= " AND $tags AND ";
            push @tabs, map { "edge_meta $_" } @{$em};
        }
    }
    
    
    my $bs = "SELECT e.name1, e.name2, e.type_id, e.edge_id, e.space_id FROM ";
    $bs .= join (", ", @tabs);
    $bs .= " WHERE $tagwh";
    while ( my ($dir, $idhash) = each %types) {
        my $typeWhere = "";
        if ($idhash) {
            # We need to look for specific nodes
            my @tids = sort {$a <=> $b} keys %{$idhash};
            if ($mt->{DEBUG}) {
                my @tps = map { [$mt->get_type($_)]} @tids;
                my @names = map 
                { $dir ? $_->[0]->reads($dir) : $_->[0]->name } @tps;
                warn "  Considering connections:\n    " . 
                    join(", ", @names)."\n";
            }
            $typeWhere = $#tids == 0 ? 
                "= $tids[0]" : "IN (".join(",", @tids).")";
        }
        my @collected = ();
        my @old_new = (0,0);
        my $done = 0;
        foreach my $fcl (@fcls) {
            foreach my $tcl (@tcls) {
                my @union = ($bs);
                if ($dir) {
                    # Directional edge
                    my @names = $dir > 0 ? ($fcl, $tcl) : ($tcl, $fcl);
                    $union[0] .= sprintf("e.name1 %s AND e.name2 %s", @names);
                } else {
                    # Non directional
                    $union[0] .= sprintf
                        ("e.name1 %s AND e.name2 %s", $fcl, $tcl);
                    unless ($fcl eq $tcl) {
                        $union[1]  = $bs;
                        $union[1] .= sprintf
                            ("e.name1 %s AND e.name2 %s", $tcl, $fcl);
                    }
                }
                if ($typeWhere) {
                    # We need to look for specific edge types
                    for my $i (0..$#union) {
                        $union[$i] .= " AND e.type_id $typeWhere";
                    }
                }
                @union = map { "$_ AND e.live = 't'" } @union;
                my $sql = join(' UNION ', @union);
                my $rows = $dbi->selectall_arrayref
                    ( -sql   => $sql,
                      -name  => "Get internally connected edges",
                      -level => 2 );
                push @collected, @{$rows};
                $done++;
                $self->show_progress($waitdat, ++$done);
            }
        }
        $self->show_progress($waitdat, 0);
        foreach my $row (@collected) {
            my ($edge, $isnew);
            my ($n1, $n2, $tid, $eid, $esid) = @{$row};
            $edge  = $self->{EDGES}{$eid};
            $isnew = 0;
            unless ($edge) {
                $isnew   = 1;
                $edge = BMS::MapTracker::Edge->new
                    ( -name1 => $n1,
                      -name2 => $n2,
                      -type  => $tid,
                      -id    => $eid,
                      -space => $esid,
                      -tracker => $self->tracker );
                $self->add_edge($edge);
            }
            
            $old_new[$isnew]++;
            # Carry on if the edge has already been captured
            next if ($isnew);
            $self->add_edge( $edge );
            map { $nodes{ $_->id } = $_ } $edge->nodes;
        }
        warn sprintf("      Edges observed (old/new): %d/%d\n", @old_new)
            if ($mt->{DEBUG});
    }
    
    warn "Finished\n" if ($mt->{DEBUG});

    my @objs = values %nodes;
    $self->benchstop;
    return @objs;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub traverse_as_network {
    my $self = shift;
    my $args = $self->parseparams(@_);
    my $traverse = $self->traverse_network(@_);
    my @roots;
    if ($args->{NODE}) {
        @roots = $self->node( $args->{NODE} );
    } else { 
        @roots = $self->each_root;
    }
    my $tn  = BMS::MapTracker::Network->new( -tracker => $self->tracker );
    foreach my $root (@roots) {
        $tn->add_root ($root);
    }
    foreach my $edge (@{$traverse->{edges}}) {
        $tn->add_edge($edge);
    }
    return $tn;
}


=head2 traverse_network

 Title   : traverse_network
 Usage   : my $string = $obj->traverse_network
 Function: 
 Returns : 
 Args    : 

 -highlight Optional array reference of sequence names that you wish
            to highlight.

=cut

sub traverse_network {
    my $self = shift;
    my $mt   = $self->tracker;
    my $args = $self->parseparams
	( -data       => { doroot => [] }, 
	  -depth      => 0,
	  -maxdepth   => 0,
	  -skipreads  => [ 'is a shorter term for', ],
	  -labelwith  => undef,
	  -keepreads  => undef,
	  -node       => undef,
	  -maxcluster => 5,
	  -abort      => [],
	  -alsogroup  => 0,
          -nosingle   => 0,
          -splitat    => 0,
          -rankorder  => 0,
          -linearize  => 0,
	  @_);

    if (0) {
        $args->{LINEARIZE} = {
            ATTRIBUTE    => 0.9,
            PROBES       => 0.8,
            UNVERSIONED  => 0.7,
            PRIORVERSION => 0.6,
            SAMEAS       => 0.55,
            CLUSTERWITH  => 0.55,
            RELIABLE     => 0.5,
            SHORTFOR     => 0.4,
            ALIAS        => 0.3,
        };

    }

    # If recursion has gone too deep then stop:
    my $depth = $args->{DEPTH};
    my $maxdp = $args->{MAXDEPTH};
    return undef if ( $maxdp && $depth >= $maxdp );
    my $data     = $args->{DATA};
    my $mtdb     = $mt->{DEBUG};
    my $isparent = 0;

    unless ($data->{seen}) {
        # Supporting data structures have not been initialized.
        # This means that the current version of the block is the parent
        $isparent = 1;
        no warnings 'recursion';
        # Do so now:
        $data = $self->_init_traverse( $data, $args);
    }
    # {seen}{SID} is used both to note that we have seen a node, and to
    # track the relative order of appearance of each node
    my $sd     = $data->{seen};
    my $linear = $data->{linearize};
    my $queue  = $data->{queued};

    # If a specific node was requested, use it:
    my $req = $args->{NODE};
    # Otherwise get the next root on the doroot stack:
    $req ||= shift @{$data->{doroot}};
    # Exit if there is nothing left to use:
    return undef unless ($req);
    my $node = $self->node( $req );
    my $sid  = $node->id;
    $self->{CONNECTIVITY}{$sid} ||= 0;
    # Exit if we are not supposed to use this node:
    return undef if ($self->{TOSS_NODE}{$sid});
    $sd->{SID}{$sid} ||= ++$data->{rank};
    my $edge_hash = $self->edges_hash( $sid );
    # The order in which edges are added to the graph will affect how
    # graphViz draws the image. Values are sorted in order to get consistent
    # results
    my @read_list = sort keys %{$edge_hash};
    # If linearization is requested, sort the read types as requested by user
    @read_list = sort { ($linear->{$b} || 1) <=> ($linear->{$a} || 1) ||
                            uc($a) cmp uc($b) } @read_list 
                            if ($linear);
    warn sprintf("[%3d] %sExpanding from %s - %d edge types ... \n", $depth, 
                 '  ' x $depth, $node->namespace_name, $#read_list+1)
        if ($mtdb);

    # If this is taking a while, let the user know:
    unless (++$data->{iters} % 100) {
        my @sids = keys %{$sd->{SID}};
        my @eids = keys %{$sd->{EID}};
        warn sprintf($data->{itfrm}, $data->{iters}, $#sids+1, $#eids+1);
    }

    my %following;
    foreach my $reads (@read_list) {
        # Do not record if we are ignoring this edge type:
	next if ($data->{skip}{$reads});

        # Do not follow if this edge is neither being visualized nor followed
	next if ($data->{hide}{$reads} && $data->{hide}{$reads} > 1);

        # Do not record if the user has requested that only certain
        # edge types be used, and this is not one of them:
	next if ($data->{keep} && !$data->{keep}{$reads});

        # This edge type is being used as a label - skip here, we will
        # deal with it after the traverse is finished.
        next if ($data->{label}{$reads});

        my $back = $mt->reverse_type($reads);
        # Get a list of all child nodes attached to $sid by this edge type
        my @nids = sort keys %{$edge_hash->{$reads}};
        warn sprintf("      %s%s = %d nodes\n",'  ' x $depth, $reads,$#nids+1)
            if ($mtdb);
        foreach my $nid (@nids) {
            while (my ($eid, $edge) = each %{$edge_hash->{$reads}{$nid}}) {
                # Skip if we have seen this particular edge already:
                next if ($sd->{EID}{$eid});
                $sd->{EID}{$eid} = ++$data->{edgerank};
                # Do not record if we are ignoring the destination node:
                next if ($self->{TOSS_NODE}{$nid});

                $self->{CONNECTIONS}{$nid} ||= {};
                $self->{CONNECTIONS}{$sid}{$nid} = 1;
                $self->{CONNECTIONS}{$nid}{$sid} = 1;

                if (!$sd->{SID}{$nid}) {
                    # 1. We have not seen this node
                    # Give it the next available ranking:
                    $sd->{SID}{$nid} = ++$data->{rank};
                    if (!$following{$nid} &&
                        !$self->is_grouped($edge, $node) &&
                        !$data->{abort}{$sid} ) {
                        # 2. We are not already following it
                        # 3. It is not part of a group (in this direction)
                        # 4. An abort flag was not set for the parent
                        
                        # Queue the node for recursion
                        $following{$nid} = 1;
                        if ($data->{rankorder} eq 'immediate') {
                            # Rank order recursion was requested -
                            # Follow the child immediately
                            $self->traverse_network( %{$args},  
                                                     -data  => $data, 
                                                     -depth => $depth + 1,
                                                     -node  => $nid, )
                                unless ($maxdp && $depth + 1 >= $maxdp);
                            # ... unless we are depth restricted and at limit
                        } elsif ($data->{rankorder} eq 'ranked') {
                            # We are trying to follow nodes in some level of
                            # preferred hierarchy, as defined by the user
                            my $score = ($linear && $linear->{$reads}) ?
                                $linear->{$reads} : 1;
                            
                            $queue->{$nid} ||= [ $score, $depth ];
                        }
                        # If we are not using rank order, children will
                        # be followed below, after end of $reads loop.
                    }
                } elsif ($linear) {
                    # We have seen this node, and the user is requesting
                    # an a-circular traverse (each node represented once)
                    # Do not add the edge to the traverse
                    next;
                }

                # If we are hiding the edge and have gotten to this point,
                # It means we still want to follow the edge, but not draw it
                next if ($data->{hide}{$reads});

                push @{$data->{traverse}}, $edge;
                $data->{edges}++;
            } # End of $edge loop
        } # End of snid loop
    } # End of $reads loop

    # Is the queue populated? Get the highest rank, lowest distance, lowest ID
    my ($queuetop) = sort { $queue->{$b}[0] <=> $queue->{$a}[0] || 
                                $queue->{$a}[1] <=> $queue->{$b}[1] ||
                                $a <=> $b} keys %{$queue};
    if ($queuetop) {
        # The queue still has members - recurse into the best ranked
        # member of the queue:
        my ($score, $dist) = @{$queue->{$queuetop}};
        delete $queue->{$queuetop};
        $self->traverse_network( %{$args},  
                                 -data  => $data, 
                                 -depth => $dist + 1,
                                 -node  => $queuetop, )
            if (!$maxdp || $depth + 1 < $maxdp);
    } elsif ($data->{rankorder} eq 'level') {
        # Follow child nodes in batch - children are analyzed only
        # after all edges of the parent have been analyzed:
        my @follow = sort { $sd->{SID}{$a} <=>
                                $sd->{SID}{$b} } keys %following;
        if ($maxdp && $depth + 1 >= $maxdp) {
            warn sprintf("      %s  %d children halt at depth %d\n",
                         '  ' x $depth, $#follow + 1, $maxdp) if ($mtdb);  
        } else {
            warn sprintf("      %s  -> %d nodes to follow\n",
                         '  ' x $depth, $#follow + 1) if ($mtdb);
            foreach my $kid (@follow) {
                $self->traverse_network( %{$args},  
                                         -data  => $data, 
                                         -depth => $depth + 1,
                                         -node  => $kid, );
            }
        }
    }

    # If the current iteration is not the initial one, then
    # return nothing:
    return undef unless ($isparent);
    
    $data->{edges}   = 0;
    # Now make sure that any remaining roots are done:
    foreach my $root (@{$data->{doroot}}) {
        my $sid = $root->id;
        # If we have already expanded through the node, skip:
        next if ($sd->{SID}{$sid});
        $self->traverse_network( %{$args},  
                                 -data  => $data, 
                                 -depth => 0,
                                 -node  => $root, );
        $data->{edges}   = 0;
    }

    my @sids = keys %{$sd->{SID}};
    my @eids = keys %{$sd->{EID}};
    if ($data->{iters} > 100) {
        warn sprintf($data->{itfrm}, $data->{iters}, $#sids+1, $#eids+1);
        warn "Recursion complete. Layout rendering may take some time.\n";
    }

    # Locate labels for each node
    foreach my $reads (keys %{$data->{label}}) {
        foreach my $sid (@sids) {
            foreach my $edge ($self->edges_from_node($sid, $reads)) {
                my $other = $edge->nodes($sid);
                my $nid = $other->id;
                $self->{LABEL_WITH}{$sid} ||= {};
                $self->{LABEL_WITH}{$sid}{$nid}++;
            }
        }
    }

    $self->{ITER} = {
        Nodes      => $#sids+1,
        Edges      => $#eids+1,
    };
    use warnings 'recursion';
    my @node_ord = sort { $sd->{SID}{$a} <=> $sd->{SID}{$b} } keys %{$sd->{SID}};
    #my @edge_ord = sort { $sd->{$a} <=> $sd->{$b} } keys %{$sd->{SID}};
    return { nodes   => \@node_ord,
             edges   => $data->{traverse}, };
}

sub _init_traverse {
    my $self = shift;
    my ($data, $args) = @_;
    my $mt = $self->tracker;
    warn "\nInitializing traverse data structure\n" if ($mt->{DEBUG});

    # Clear some internal structures:
    $self->{CONNECTIVITY} = {};
    $self->{CONNECTIONS}  = {};
    $self->{USED_REL}     = {};
    $self->{LABEL_WITH}   = {};

    # Record the read types that we want to skip or keep:
    my $skipping = { map { lc($_) => 1 } @{$args->{SKIPREADS}} };
    my $keeping;
    if ($args->{KEEPREADS}) {
        my @tokeep = ref($args->{KEEPREADS}) eq 'ARRAY' ?
            @{$args->{KEEPREADS}} : ( $args->{KEEPREADS} );
        $keeping = { map { lc($_) => 1 } @tokeep };
        if ($mt->{DEBUG} && $#tokeep > -1) {
            print "Keeping reads: " . join(", ", sort @tokeep) . "\n";
        }
    }

    # Identify 'abort' nodes. These are nodes that we will collect
    # edges for, but will not recurse any further with
    my $abort = {};
    if ($args->{ABORT}) {
        my @toabort = ref($args->{ABORT}) eq 'ARRAY' ?
            @{$args->{ABORT}} : ( $args->{ABORT} );
        foreach my $ab (@toabort) {
            my $ab_seq =  $mt->get_seq( $ab );
            $abort->{ $ab_seq->id } = 1;
        }
        if ($mt->{DEBUG} && $#toabort > -1) {
            print "Aborting on: " . join(", ", sort @toabort) . "\n";
        }
    }
    
    # Identify edge types that will serve as additional labels for the nodes
    my $labwith = {};
    if ($args->{LABELWITH} || $self->param('label_edge')) {
        my @labs = ref($args->{LABELWITH}) eq 'ARRAY' ?
            @{$args->{LABELWITH}} : ( $args->{LABELWITH} );
        foreach my $lab (@labs) {
            $labwith->{$lab} = 1;
            delete $skipping->{$lab};
        }
        if ($mt->{DEBUG} && $#labs > -1) {
            warn "Labeling with: " . join(", ", sort @labs) . "\n";
        }
    }

    # If no node is provided, then assume the user wants all the
    # roots drawn out, and store them for recursive retrieval:
    my (@roots, @localroots);
    if ($args->{NODE}) {
        @localroots = $self->node( $args->{NODE} );
    } else { 
        @roots = @localroots = $self->each_root;
    }

    my ($linear, $queue);
    my $ro = $args->{RANKORDER} ? 'immediate' : 'level';
    if ($linear = $args->{LINEARIZE}) {
        if (ref($linear)) {
            # Standardize the keys for the linear request to "reads as" strings
            my %standardize;
            while (my ($read, $score) = each %{$linear}) {
                my ($type, $dir) = $mt->get_type($read);
                next unless ($type);
                # If directional, use just that reads_as for the key
                # Otherwise, assign BOTH reads_as keys
                my @reads = $dir ? ($type->reads($dir)) : $type->reads;
                map { $standardize{ $_ } = $score } @reads;
            }
            $linear = \%standardize;
        } else {
            $linear = {};
        }
        $queue = {};
        $ro = 'ranked';
    }
    
    $data = { 
        seqs  => {}, # Sequence object storage
        seen  => {
            SID => {},
            EID => {},
        }, # prevents looped recursion
        rank      => 0,  # Ranks sequences to determine edge direction
        edgerank  => 0,  # Ranks edges to determine edge direction
        skip      => $skipping,
        keep      => $keeping,
        abort     => $abort,
        label     => $labwith,
        doroot    => \@roots,
        rootsids  => { map { $_->id => 1 } @localroots },
        iters     => 0,
        edges     => 0,
        edgecount => {},
        split     => undef,
        hide      => {},
        rankorder => $ro,
        traverse  => [],
        linearize => $linear,
        queued    => {},
    };

    # Make note of the edges that the user does not want displayed:
    while (my ($reads, $level) = each %{$self->{TOSS_EDGE}}) {
        next unless ($level);
        $data->{hide}{$reads} = $level;
        warn sprintf("User display level %s = %s", $reads, $level)
            if ($mt->{DEBUG});
    }
    
    # Track nodes that we have printed multiple times:
    $self->{split_edges} = {};
    if (my $sa = $args->{SPLITAT}) {
        $data->{split} = {};
        # Find highly connected nodes
        foreach my $node ($self->each_node) {
            my $sid   = $node->id;
            my @edges = $self->edges_from_node( $node );
            my $count = $#edges + 1;
            $data->{edgecount}{$sid} = $count;
            if ($count >= $sa) {
                # There are more edges to this node than desirable
                my %oids;
                foreach my $edge (@edges) {
                    my ($reads, $theRequest, $other) = $edge->reads($sid);
                    $oids{$other->id}++;
                }

                # NO IDEA WHAT I AM DOING HERE
                my $count = 0;
                $data->{split}{$sid} = {};
                foreach my $oid (sort { $a <=> $b } keys %oids) {
                    my @edges = $self->edges_from_node($oid);
                    # Associate all leaves with a single node:
                    my $num = $#edges == 0 ? 0 : ++$count;
                    $data->{split}{$sid}{$oid} = 
                        sprintf("ALIAS%sv%03d", $sid, $num);
                }
            }
        }
    }

    # Some user feedback information:
    my $frm = 'Network iteration %d [ %d nodes, %d edges]';
    $data->{itfrm} = "$frm\n";
    $args->{DATA} = $data;
    return $data;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_text

 Title   : to_text
 Usage   : my $string = $obj->to_text
 Function: 
 Returns : A string containing the network as simply a set of edges
 Args    : 

=cut

sub to_text {
    my $self = shift;
    my @edges = sort map { $_->to_text_short } $self->each_edge;
    return join("", @edges);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_hyperview

 Title   : to_hyperview
 Usage   : my $string = $obj->to_hyperview
 Function: 
 Returns : A string containing the hyperview deffinition
 Args    : 

=cut

sub to_hyperview {
    my $self = shift;
    my $mt = $self->tracker;
    my $args = $self->parseparams
	( -traverse   => undef,
          -rankorder  => 1,
	  @_);
    
    my $traverse = $args->{TRAVERSE} || $self->traverse_network( @_ );
    return "" unless ($traverse);
    return "" if ($#{$traverse->{edges}} < 0);

    my %ranked;
    foreach my $edge (@{$traverse->{edges}}) {
        my ($aid, $bid) = map { $_->id } $edge->nodes;
        my ($node, $kid, @kids);
        if ($node = $ranked{$bid}) {
            # Already defined B node
            $kid = $aid;
        } else {
            # Already defined A node, or need to make a new one
            unless ($ranked{$aid}) {
                $ranked{$aid} = { id   => $aid, };
            }
            $node = $ranked{$aid};
            $kid  = $bid;
        }
        if ($self->{GROUP_LOOKUP}{KEY}{$kid}) {
            # $kid is really a group ID
            my $group = $self->{GROUPS}{$kid};
	    foreach my $sid (keys %{$group->{SIDS}}) {
                push @kids, $sid;
            }
        } else {
            @kids = ( $kid );
        }
        $node->{kids} ||= {};
        foreach my $id (@kids) {
            $node->{kids}{$id} = 1;
            my $kn = $ranked{$id} ||= { id => $id, };
            $kn->{pars} ||= {};
            $kn->{pars}{$node->{id}} = 1;
        }
    }

    # Get the name for each node
    while (my ($id, $node) = each %ranked) {
        my @names = ( $self->node( $node->{id} )->name );
        my %seen = ( $names[0] => 1 );
        my $labs  = $self->{LABEL_WITH}{$id} || {};
        foreach my $lid (sort keys %{$labs}) {
            my $lab = $mt->get_seq($lid)->name;
            push @names, $lab unless ($seen{$lab});
            $seen{$lab} = 1;
        }
        my $name = join('/', @names);
        $name =~ s/\s+/_/g;
        $node->{name} = $name;
    }
    my $string = "";
    while (my ($id, $node) = each %ranked) {
        next if ($node->{pars});
        $string .= $self->_recurse_hyperview( \%ranked, $node, 0);
    }
    return $string;
}

sub _recurse_hyperview {
    my $self = shift;
    my ($list, $node, $level) = @_;
    
    $node->{done} = 1;
    my $string = sprintf("%d %s 1 main\n", $level, $node->{name});
    return $string unless ($node->{kids});
    my @kids = sort { $list->{$a}{name} cmp
                          $list->{$b}{name}} keys %{$node->{kids}};
    foreach my $kid (@kids) {
        my $kn = $list->{$kid};
        if ($kn->{done}) {
            # This node was explored - just make an edge to it
            $string .= sprintf("%d %s 1 main\n", $level+1, $kn->{name});
        } else {
            # We have not explored this node yet
            $string .= $self->_recurse_hyperview( $list, $kn, $level+1 );
        }
    }
    return $string;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_sif

 Title   : to_sif
 Usage   : my $string = $obj->to_sif
 Function: Generates the SIF deffinition describing the edges.
 Returns : A string containing the dot deffinition
 Args    : 

=cut

sub to_sif {
    my $self = shift;
    my $mt = $self->tracker;
    my $args = $self->parseparams
	( -nosingle   => 0,
          -traverse   => undef,
	  @_);
    
    my $traverse = $args->{TRAVERSE} || $self->traverse_network( @_ );
    return "" unless ($traverse);
    return "" if ($#{$traverse->{edges}} < 0);

    my $string = "";
    foreach my $edge (@{$traverse->{edges}}) {
        my ($reads, $node1, $node2) = $edge->reads;
        my ($seq, $keq) = ($self->proxy($node1) || $node1, 
                           $self->proxy($node2) || $node2 );
        $reads =~ s/ /_/g;
        $string .= sprintf("%s\t%s\t%s\n", $seq->name, $reads, $keq->name);
    }
    return $string;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_gml

 Title   : to_gml
 Usage   : my $string = $obj->to_gml
 Function: Generates the GML deffinition describing the edges.
 Returns : A string containing the dot deffinition
 Args    : 

=cut

sub to_gml {
    my $self = shift;
    my $mt = $self->tracker;
    my $args = $self->parseparams
	( -nosingle   => 0,
          -traverse   => undef,
	  @_);
    
    my $traverse = $args->{TRAVERSE} || $self->traverse_network( @_ );
    return "" unless ($traverse);
    # We need at least one node and one edge:
    return "" if ($#{$traverse->{nodes}} < 0 || $#{$traverse->{edges}} < 0);

    my $string = "graph [\n";
    $string .= "    comment \"Generated by MapTracker\"\n";
    $string .= "    label \"I can add more useful labels in my Copius Spare Time\"\n";
    $string .= "    directed 1\n";
    my %done_stuff;
    my %edgecount;
    my $edge_string = "";
    foreach my $edge (@{$traverse->{edges}}) {
        my ($reads, $node1, $node2) = $edge->reads;
        my ($seq, $keq) = ($self->proxy($node1) || $node1,
                           $self->proxy($node2) || $node2 );
        my ($sid, $kid) = map { $_->id } ($seq, $keq);
        my @todraw = ( [ $sid, $kid, $reads ] );

        foreach my $td (@todraw) {
            my ($sid, $kid, $reads) = @{$td};
            my $key = "$sid\t$kid";
            next if ($done_stuff{$key});
            $done_stuff{$key} = 1;
            $edge_string .= "    edge [\n";
            $edge_string .= "        source $sid\n";
            $edge_string .= "        target $kid\n";
            $edge_string .= "        label \"$reads\"\n";
            $edge_string .= "    ]\n";
            
        }
        $edgecount{ $sid }++;
        $edgecount{ $kid }++;
    }
    foreach my $real_sid (@{$traverse->{nodes}}) {
        if ($self->{GROUP_LOOKUP}{KEY}{$real_sid}) {
            next;
        }
        my $node = $self->proxy( $real_sid );
        my $sid  = $node->id;
        # Proxies may result in duplicate entries:
        next if ($done_stuff{$sid});
        $done_stuff{$sid} = 1;

        my $name = $node->name;
        my %has_lab = ( $name => 1 );
        my @aliases;
	if (my $extralab = $self->{LABEL_WITH}{$sid}) {
	    foreach my $nid (sort keys %{$extralab}) {
                my $en = $mt->get_seq($nid)->name;
                next if ($has_lab{$en});
                $has_lab{$en} = 1;
		push @aliases, $en;
	    }
	}
        $string .= "    node [\n";
        $string .= "        id $sid\n";
        $string .= "        label \"$name\"\n";
        $string .= sprintf("        totalEdges %d\n", $edgecount{$sid} || 0);
        $string .= sprintf("        alias \"%s\"\n", join(", ", @aliases) )
            if ($#aliases > -1);
        my @graphics;
        if (exists $self->{NODE_FORMAT}{$sid}) {
            while (my ($param, $val) = each %{$self->{NODE_FORMAT}{$sid}}) {
                $param = $gv2gml->{$param};
                next unless ($param);
                push @graphics, sprintf("%s \"%s\"", $param, $val)
                    if (defined $val && $val ne "");
            }
        }
        if ($#graphics > -1) {
            $string .= "        graphics [\n";
            foreach my $gdat (@graphics) {
                
            }
            $string .= "        ]\n";
        }
        $string .= "    ]\n";
    }
    $string .= $edge_string;
    $string .= "]\n";
    return $string;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_graphviz

 Title   : to_graphviz
 Usage   : my $string = $obj->to_graphviz
 Function: Generates the graphviz dot deffinition describing the edges.
 Returns : A string containing the dot deffinition
 Args    : 

 -highlight Optional array reference of sequence names that you wish
            to highlight.

=cut

sub to_graphviz {
    my $self = shift;
    my $mt = $self->tracker;
    my $args = $self->parseparams
	( -program    => 'dot',
          -nosingle   => 0,
	  -alsogroup  => 0,
	  -highlight  => [],
          -traverse   => undef,
          -useimage   => 0,
	  @_);
    my $traverse = $args->{TRAVERSE} || $self->traverse_network( @_ );
    return "" unless ($traverse);
    my $nodes   = $traverse->{nodes};


    return "" if ($#{$nodes} < 0);
    $self->benchstart;
    my %nid_rank;
    for my $i (0..$#{$nodes}) {
        $nid_rank{$nodes->[$i]} = $i;
    }

    
    if ($args->{ISHTML}) {
        # We will be generating HTML code, which will draw on 
        $self->_bulk_class_load( @{$nodes} );
        $self->_bulk_taxa_load( @{$nodes} );
    } else {
        if ($args->{USEIMAGE}) {
            # We will be doing class comparisons, so some bulk loading
            $self->_bulk_class_load( @{$nodes} );
        }
        unless ($self->param('no_tax_tags')) {
            # We will be testing taxa, bulk load
            $self->_bulk_taxa_load( @{$nodes} );
        }
    }

    # Some parameters
    # edge[minlen=NUMBER] - minimum edge length


    my $string = "digraph seq_edges {\n";
    $string .= "  // GraphViz specification - see:\n";
    $string .= "  // http://www.research.att.com/sw/tools/graphviz/\n";
    $string .= "  // for more information on GraphViz!\n\n";

    my $formats = {
        Default => {
            node => {
                shape    => "box",
                fontname => "helvetica",
                fontsize => 8,
                height   => 0.2,
            },
            edge => {
                fontname => "helvetica",
                fontsize => 9,
                weight   => 2,
            },
            graph => {
                fontsize => 10,
                ranksep  => 0.20,
                nodesep  => 0.1,
                compound => 'true',
            },
        },
        User => {
        },
    };

    if ($args->{PROGRAM} =~ /neato/i) {
        my $opts = $formats->{Default}{graph};
        $opts->{maxiter}  = "1000";
        $opts->{overlap}  = "false";
        $opts->{splines}  = "true";
        $opts->{epsilon}  = "0.0001";  #termination condition
        $opts->{packmode} = "graph";   # how to compact nodes after layout
        $opts->{start}    = "regular"; # random, regular (circle)
    }
    if ($args->{PROGRAM} =~ /twopi/i) {
        my $opts = $formats->{Default}{graph};
        $opts->{ranksep}    = "1.5";
    }
    
    # User-defined global formats
    foreach my $global_key ('graph', 'edge', 'node') {
        next unless (exists $self->{NODE_FORMAT}{$global_key});
        my @opts;
        while (my ($param, $val) = each %{$self->{NODE_FORMAT}{$global_key}}) {
            delete $formats->{Default}{$global_key}{$param};
            $formats->{User}{$global_key}{$param} = $val;
        }
    }

    foreach my $fclass (sort keys %{$formats}) {
        my @lines;
        foreach my $global_key (sort keys %{$formats->{$fclass}}) {
            my @params;
            foreach my $param (sort keys %{$formats->{$fclass}{$global_key}}) {
                my $val = $formats->{$fclass}{$global_key}{$param};
                next unless (defined $val && $val ne '');
                unless ($val =~ /(true|false)/i ||
                        $val =~ /^[\+\-]?\d+$/  ||
                        $val =~ /^[\+\-]?\d?\.\d+$/) {
                    $val = '"' . $val . '"';
                }
                push @params, $param .'='. $val;
            }
            while (my @chunk = splice(@params, 0, 5)) {
                push @lines, sprintf
                    ("  %s [%s];\n", $global_key, join(', ', @chunk));
            }
        }
        next if ($#lines < 0);
        $string .= 
            "  // $fclass Global Parameters:\n".join('', @lines)."\n";
    }


    my $data_string = "";
    my @edges = @{$traverse->{edges}};

    # Fast load tag information
    $self->_bulk_edge_tag_load( @edges );
    # We want to collect all the 'same' edges, where same is
    # "Node1 reads Node2"
    my %collected;
    foreach my $edge (@edges) {
        # Get the two nodes for this edge
        my @nodes = $edge->nodes;
        # Get a relative rank for this edge
        my $erank = $nid_rank{ $nodes[0]->id } * $nid_rank{ $nodes[1]->id };
        # Update nodes to proxy if needed
        @nodes    = map { $self->proxy($_) || $_ } @nodes;
        my @sids  = map { $_->id } @nodes;
        my $reads = $edge->reads;
        print $edge->to_html unless ($reads);
        my $key = join("\t", @sids, $reads);
        $collected{$key} ||= {
            key   => $key,
            group => 0,
            rank  => $erank,
            edges => [],
        };
        $collected{$key}{rank} = $erank if ($collected{$key}{rank} > $erank);
        $collected{$key}{group} ||= $self->is_grouped($edge);
        push @{$collected{$key}{edges}}, $edge;
    }
    $mt->register_javascript( @edges );

    my @edata = sort { $a->{group} <=> $b->{group} ||
                           $a->{key} cmp $b->{key} } values %collected;

    # ISSUE
    # There are two levels of grouping that may be in conflict
    # Proxies group divergent nodes under a single (different) node
    # Groups group divergent edges under a single (artificial) edge
    # I am not sure what will happen when groups and proxies interact...

    # Render all ungrouped edges
    my %edgecount;
    my @edgelines;
    my %edge_tags;
    if ($args->{SIMPLEEDGE}) {
        $edge_tags{label} = "";
        $edge_tags{nourl} = 1;
    }
    if (my $ltag = $args->{LENGTHTAG}) {
        $edge_tags{lengthtag} = $ltag;
        $edge_tags{lengthmod} = $args->{LENGTHMOD};
    }
    if (my $edgeopts = $args->{EDGEOPTS}) {
        map { $edge_tags{$_} = $edgeopts->{$_} } keys %{$edgeopts};
    }
    $edge_tags{stylemap} = $args->{STYLEMAP};

    while (my $edat = shift @edata) {
        if ($edat->{group}) {
            # We have hit the grouped edges - halt here
            unshift @edata, $edat;
            last;
        }
        # Note that we have an explicit edge between each of these nodes:
        my ($n1, $n2) = split("\t", $edat->{key});
        $edgecount{$n1}++;
        $edgecount{$n2}++;
        push @edgelines, [ $edat->{rank},
                           $self->_graphviz_edge( $edat, \%edge_tags ) ];
    }

    # Examine grouped edges - some of these may need to be ungrouped
    # if both nodes were rendered above
    my %groups;
    #$debug->branch(\@edata);
    foreach my $edat (@edata) {
        my ($n1, $n2, $reads) = split("\t", $edat->{key});
        my $gid    = $edat->{group};
        my $gname  = "group_$gid";
        my $group  = $self->{GROUPS}{$gname};
        my $parent = $group->{parent};
        my $proxp  = $self->proxy($parent) || $parent;
        my $parid  = $proxp->id;
        my $kidid;
        if ($n1 == $parid) {
            $kidid = $n2;
        } elsif ($n2 == $parid) {
            $kidid = $n1;
        } else {
            warn "Grouped edge $n1 $reads $n2 does not match parent $parid\n";
            next;
        }
        if ($edgecount{$kidid}) {
            # The child node already exists ungrouped on the graph
            # Just make the connection
            $edat->{group} = 0;
            push @edgelines, [ $edat->{rank}, $self->_graphviz_edge($edat) ];
            $edgecount{$n1}++;
            $edgecount{$n2}++;
            next;
        } else {
            # Store the node (edge) in the group
            my $rank = $edat->{rank};
            $groups{$gid} ||= {
                rank  => $rank,
                edges => [],
            };
            push @{$groups{$gid}{edges}}, @{$edat->{edges}};
            $groups{$gid}{rank} = $rank if ($groups{$gid}{rank} > $rank);
        }
    }

    # Now deal with each remaining group:
    my %group_nodes;
    my %nodes_in_group;
    foreach my $gid (sort { $a <=> $b } keys %groups) {
        my $gname = "group_$gid";
        my $group = $self->{GROUPS}{$gname};
        my $par   = $group->{parent};
        my $reads = $group->{reads};
        my $key   = join("\t", $par->id, 'group_' . $group->{ngid}, $reads);
        my @edges = @{$groups{$gid}{edges}};
        
        # Describe the nature of this group:
        my %members;
        foreach my $edge (@edges) {
            my $node = $edge->nodes($par);
            unless ($node) {
                # This should not happen - but may due to the
                # group vs. proxy issue described above
                next;
            }
            my $nid = $node->id;
            $members{ $nid } ||= $node;
            $nodes_in_group{$nid} = $gname;
        }
        my @seqs = sort { uc($a->name) cmp uc($b->name) } values %members;
        my $num   = $#seqs + 1;
        my $name  = sprintf("Group of %d Item%s", $num, $num == 1 ? '' : 's');
        my $label = sprintf
            ("%s,%s which %s %s", $name, $num == 1 ? '' : ' each of',
             $mt->reverse_type($reads), $par->name);

        my $nid = $group->{ngid};
        $mt->register_javascript_group( -group => \@seqs,
                                        -id    => $nid,
                                        -name  => $label,);

        $group_nodes{$nid} = $name;
        my $edat = {
            key   => $key,
            group => $group,
            edges => $groups{$gid}{edges},
            count => $num,
            name  => $name,
        };
        push @edgelines, [ $groups{$gid}{rank}, $self->_graphviz_edge($edat)];
    }

    # Highlight the parent sequence, and any others requested
    my $highlight = ref($args->{HIGHLIGHT}) eq 'ARRAY' ?
    { map { $_ => '#99ffcc' } @{$args->{HIGHLIGHT}} } : $args->{HIGHLIGHT};
    my @userPassed = keys %{$highlight};

    unless ($#userPassed > -1) {
	foreach my $root ($self->each_root) {
            $root = $self->proxy($root) || $root;
            $highlight->{ $root->id } = '#99ffcc'
                unless ($args->{QUIETROOT});
	}
    }

    my %shown;
    my $gsidlk = $self->{GROUP_LOOKUP}{SID};
    my %done_nt;
    my %ranks;
    foreach my $real_sid (@{$nodes}) {
        if ($nodes_in_group{$real_sid}) {
            # This node is contained in a group
            next;
        }
        if ($args->{NOSINGLE} && !$edgecount{ $real_sid }) {
            # This is a node with no edges attached to it
            next;
        }
        $shown{$real_sid} = 1;
	my ($name, $type, $sid);
        # Accomodate individual nodes as proxies, if possible
        my $node = $self->proxy( $real_sid );
        $sid     = $node->id;
        if ($args->{ALSOGROUP} && $gsidlk->{$sid}) {
            # This node is printed singly, but is also in a group
            # Make an edge connecting it.
            while (my ($gid, $ok) = each %{$gsidlk->{$sid}}) {
                next unless ($ok);
                $data_string .= $self->_graphviz_edge_OLD
                    ( $sid, $gid, "is also in group", []);
            }
        }
        $name    = $node->name;
        if (length($name) > 50) {
            $name = substr($name, 0, 50) . '...';
        }
        $name =~ s/\\/\\\\/g;
        $type    = 'node';
        # Can we add cute little species icons?
        unless ($self->param('no_tax_tags')) {
            my @species = map { $_->as_gd } $node->each_taxa;
            if ($#species > -1) {
                $name = "\\n$name"; # Add padding for the icons
                $self->{OVERLAYS}{$sid} = \@species;
            }
        }
	

	my @labels = ($name);
        my %has_lab = map {$_ => 1} @labels;
	if (my $extralab = $self->{LABEL_WITH}{$sid}) {
	    my @extras = ();
	    foreach my $nid (sort keys %{$extralab}) {
                my $en = $mt->get_seq($nid)->name;
                $en = substr($en, 0, 50) . '...' if (length($en) > 50);
                next if ($has_lab{$en});
                $has_lab{$en} = 1;
		push @extras, $en;
	    }
	    push @labels, sort @extras;
	}

        my %opts;
        
        my @node_tags = ($sid);
        my $ntfrm = "";
        if (my $aliases = $self->{split_edges}{ $sid }) {
            # This node is being put in several places
            @node_tags = sort keys %{$aliases};
            $opts{shape} = 'house';
        }

        if (my $color = $highlight->{$sid}) {
            $opts{color} = $color;
            $opts{peripheries} = 2;
        }

        if (exists $self->{NODE_FORMAT}{$sid}) {
            while (my ($param, $val) = each %{$self->{NODE_FORMAT}{$sid}}) {
                next unless (defined $val && $val ne "");
                if ($param eq 'rank') {
                    push @{$ranks{$val}}, $sid;
                } elsif ($param eq 'label') {
                    push @labels, $val;
                } else {
                    $opts{$param} = $val;
                }
            }
        }

        $opts{style} = 'filled' if ($opts{color} && !$opts{style});
        $opts{label} = join("\\n", @labels);
        $opts{URL}   = $type . '_' . $sid;

        if ($node->is_class('anonymous')) {
            $opts{label} = "";
            # $opts{style} = 'invis';
            $opts{shape}  = 'point';
            $opts{width}  = 0.1;
            $opts{height} = 0.1;
            if ($args->{ANONOPTS}) {
                while (my ($tag, $val) = each %{$args->{ANONOPTS}}) {
                    $opts{$tag} = $val;
                }
            }
            
        }
        if ($args->{USEIMAGE}) {
            # Check to see if the node is a SMILES string:
            my $seq = $mt->get_seq($sid);
            # /stf/sys/src/graphviz-1.16/
            if ($seq->is_class('smiles')) {
                my $file = $self->_smiles_image_file($seq);
                $opts{label} = '<<TABLE BORDER="0"><TR><TD><IMG SRC="' .$file. 
                    '" /></TD></TR></TABLE>>' if ($file);
            }
        }

        # Format the options string:
        my @pairs;
        while (my ($tag, $val) = each %opts) {
            if ($val =~ /^\<.+\>$/) {
                # Graphviz indication that information is HTML text
                push @pairs, sprintf("%s=%s", $tag, $val);
            } else {
                push @pairs, sprintf("%s=\"%s\"", $tag, $val);
            }
        }
        my $optstr = join(",", @pairs);
        
        foreach my $nt (@node_tags) {
            next if ($done_nt{$nt});
            $done_nt{$nt} = 1;
            $data_string .= sprintf("  %s [%s];\n", $nt, $optstr);
        }
    }
    my @ranks = sort { $a <=> $b } keys %ranks;
    if ($#ranks > -1) {
        $data_string .= "\n  // Rank specifications\n";
        foreach my $rank (@ranks) {
            my $rnode = "rank$rank";
            $data_string .= sprintf
                ("  { rank=same; %s }\n", 
                 join(" ", map { "$_;" } @{$ranks{$rank}}, $rnode));
        }
        $data_string .= join
            ('', map { " rank$_ [ style=\"invis\",label=\"\" ];\n" } @ranks);
        $data_string .= sprintf
            ("  {   edge [ style=invis ]; %s;}\n",
             join(" -> ", map { "rank$_" } @ranks));
    }

    while (my ($nid, $gname) = each %group_nodes) {
        $data_string .= sprintf
            ('  group_%d [ URL="group_%d", label="%s", color="#dddddd", '.
             'style="filled"];'."\n",  $nid, $nid, $gname);
    }

    if ($#edgelines > -1) {
        # Write out edge information
        $data_string .= "\n";
        foreach my $dat (sort { $a->[0] <=> $b->[0] } @edgelines) {
            $data_string .= $dat->[1];
        }
    }

    if ($data_string) {
        $string .= $data_string . "}\n";
    } else {
        $string = "";
    }
    warn "\n$string\n" if ($mt->{DEBUG});
    $self->benchstop;
    return $string;
}

sub _smiles_image_file {
    my $self = shift;
    my ($seq) = @_;
    my $path = $self->tracker->file_path('nodeimg');
    my $file = $seq->id . '.png';
    my $full = "$path/$file";
    return $file if (-e $full);

    my %cookies;

    foreach my $hc ( split (/; /,$ENV{'HTTP_COOKIE'} ) ){
        if ($hc =~ /^([^=]+)=(.+)$/) {
            $cookies{$1} = $2;
        }
    } 
    return "";

    # Need a way to authenticate!

    my $smi = $seq->name;
    my $esc = CGI::escape($smi);
    my $url = "http://cheminfo.pri.bms.com:8080/cgi-auth/moldy.cgi?size=4&smiles=$esc";
    # getstore($url, $path . $file);
    # warn "<pre>$url\n$path$file\n</pre>";
    my $request = HTTP::Request->new('GET', $url);
    my $ua = $self->{USER_AGENT} ||= LWP::UserAgent->new;
    my $cj = HTTP::Cookies->new({});
    $cj->add_cookie_header($request);
    while (my ($key, $val) = each %cookies) {
        #( $vers, $key, $val, $path, $domain, $port, $path_spec, $secure, $maxage, $discard, \%rest )
        $cj->set_cookie( 1, $key, $val, "/", ".bms.com", "80", 0, 0, 2000, 0 );
    }
    $ua->cookie_jar($cj);
    my $foo = $ua->request($request, $full);
    $debug->branch(-refs => [$ua,$foo]);
    return $file;
    return "fly.png";
}

sub _wait_progress {
    my $self = shift;
    my ($data, $msg) = @_;
    my $wait      = time - $data->{start};
    $data->{wait} = $wait;
    # Do not display anything if:
    # 1 This is not running in a web browser
    # 2 No time has passed
    # 3 User requests the system be quiet
    # 4 The wait interval has not been reached yet 
    if ($nocgi || !$wait || $data->{quiet} || $wait % $data->{intv}) {
        
        return $wait;
    }
    if ($data->{prefix}) { 
        warn $data->{prefix};
        $data->{prefix} = 0;
    }
    warn sprintf("  %3d seconds - %s\n", $wait, $msg);
    sleep(1);
    return $wait;
}

sub show_progress {
    my $self = shift;
    # We only really want this for the web interface:
    return 0 unless ($ENV{'HTTP_HOST'});
    my ($data, $count) = @_;
    unless ($count) {
        # The user wants to close out
        return 0;
    }
    my $pad  = "";
    my $lt   = $data->{'last'} || $data->{start};
    my $ti   = time;
    my $wait = $ti - $lt;
    return $wait if (!$wait || $wait < $data->{intv});
    unless ($data->{'last'}) {
        # First message
        warn $pad . $data->{header};
    }
    my $tot = $data->{total};
    my $unit = $data->{unit} || "";
    $unit .= 's' if ($unit && $tot != 1);
    my $msg = sprintf("  %s%d of %d %s [%.2f%%]", $pad, $count, $tot, 
                      $unit, 100 * $count / $tot);
    my $remain = ($ti - $data->{start}) * (1 - $count / $tot);
    $msg .= sprintf(" %d sec ETA", $remain) if ($remain);
    warn "$msg\n";
    $data->{'last'} = $ti;
    return $wait;
}

sub _hyperview_edge {
    my $self = shift;
    my ($sid, $depth, $seen) = @_;
    my @ids = ($sid);
    if ($self->{GROUP_LOOKUP}{KEY}{$sid}) {
        my $group = $self->{GROUPS}{$sid};
        @ids = ();
    }
    my $string = "";
    foreach my $id (@ids) {
        my $name = $self->node( $id )->name;
        $name =~ s/\s+/_/g;
        my $line = sprintf("%d %s 1 foo\n", $depth, $name);
        next if ($seen->{$line});
        $string .= $line;
        $seen->{$line} = 1;
    }
    return $string;
}

sub _graphviz_edge {
    my $self = shift;
    $self->benchstart;
    my $mt = $self->tracker;
    my ($edat, $passTag) = @_;
    my ($sid1, $sid2, $reads) = split("\t", $edat->{key});
    my @edges = @{$edat->{edges}};
    my %tags = %{$passTag || {} };


    my $explicitLabel = (defined $tags{label}) ? 1 : 0;
    

    if ($#edges > -1) {
        my @multiple;
        my $group = $edat->{group};
        # $tags{label} = '*' unless ($explicitLabel);
        if ($group) {
            # This is a group
            @multiple = values %{$group->{edges}};
        } elsif ($#edges == 0) {
            my $edge = $edges[0];
            if ($tags{nourl}) {
                delete $tags{nourl};
            } else {
                $tags{URL} = 'edge_' . $edge->id;
            }
            if ($edge->reads eq 'is denormalized to') {
                # Single denormalized edge
                my ($tag) = $edge->each_tag('Preferred Edge');
                if ($tag) {
                    $reads = $tag->valname;
                }
            }
        } else {
            # Multiple edges
             @multiple = @edges;
        }
        if ($#multiple > -1) {
            my $id = $group ? $group->{egid} : $mt->new_unique_id();
            $tags{URL}   = 'group_' . $id;
            $tags{label} = '+' unless ($explicitLabel);
            
            $mt->register_javascript_group
                ( -group => \@multiple,
                  -id    => $id,
                  
                  );
            
        }
        if (my $tagname = $tags{lengthtag}) {
            delete $tags{lengthtag};
            my $len = 0;
            foreach my $edge (@edges) {
                foreach my $tag ($edge->each_tag($tagname)) {
                    $len += $tag->num || 0;
                }
            }
            if (my $mod = $tags{lengthmod}) {
                $len *= $mod;
            }
            $tags{len} = $len;
            delete $tags{lengthmod};
        }
    }

    my @sorted = sort ($sid1, $sid2);
    if ($sid1 ne $sorted[0]) {
        # GraphViz does not perform well with circular graphs
        # Standardize all edges pointing from low name_id to high name_id
        # If the two nodes need to be swapped, also swap the reads string
        ($sid1, $sid2) = @sorted;
        $reads = $mt->reverse_type($reads);
    }

    # Record that we have used this edge - will be checked when key made
    # KLUDGE ??? DO WE NEED TO DO THIS? inelegant
    $self->{USED_REL}{$reads}++;
    my $userstyle = $tags{stylemap} || {};
    my $style     = $userstyle->{ $reads } || $gv_arrowmap->{ $reads };

    if ($style) {
        my $mirror   = $style->{mirror};
        if ($mirror) {
            $style = $userstyle->{ $mirror } || $gv_arrowmap->{ $mirror };
        }
        while (my ($tag, $val) = each %{$style}) {
            $tags{$tag} = $val unless (defined $tags{$tag});
        }
        
        ($tags{arrowhead}, $tags{arrowtail},) = 
	    ($tags{arrowtail}, $tags{arrowhead}, ) if ($mirror);
        # $tags{label} = '*' unless ($explicitLabel);
    } else {
        $tags{label} = $reads unless ($explicitLabel);
    }
    $tags{fontcolor} = $tags{color};
    

    delete $tags{label} unless ($tags{label});
    delete $tags{stylemap};

    my @tagvals;
    foreach my $tag (sort keys %tags) {
        my $val = $tags{$tag};
	push @tagvals, "$tag=\"$val\"" if ($tag && defined $val);
    }
    my $param = join(",", @tagvals); 
    $param = " [ $param ]" if ($param);

    # If a node is proxied, update the name_id now:
    my @prox = ($sid1, $sid2);
    for my $i (0..$#prox) {
        next unless ($prox[$i] =~ /^\d+$/);
        my $proxy = $self->proxy($prox[$i]);
        $prox[$i] = $proxy->id if ($proxy);
    }
    
    $self->benchstop;
    return sprintf("  %s -> %s%s;\n", @prox, $param);
}

sub _graphviz_edge_OLD {
    my $self = shift;
    $debug->branch(["OLD METHOD CALL"]);
    die;
}

sub graphviz_key {
    my $self = shift;
    $self->benchstart;
    my $string = "digraph seq_edges {\n";
    $string .= "  // Global parameters:\n";
    $string .= "  node [shape=box,fontname=Helvetica]\n";
    $string .= "  edge [fontsize=9,fontname=Helvetica];\n";
    $string .= "  graph [fontsize=10, rankdir=LR];\n";
    $string .= "  weight = 2;\n";

    $string .= "\n  // Parameters for clusters\n";
    $string .= "  node [fontsize=8, height=0.2];\n";
    $string .= "  graph [ranksep=0.1,nodesep=0.1];\n";
    my $nodeCount = 0;
    my %toSort;
    foreach my $reads (keys %{$self->{USED_REL}}) {
        next unless (exists $gv_arrowmap->{$reads});
        my $std = $gv_arrowmap->{$reads}{mirror} ? 
            $gv_arrowmap->{$reads}{mirror} : $reads;
        next unless (exists $gv_arrowmap->{$std} && 
                     $gv_arrowmap->{$std}{color});
        $toSort{$std} = $gv_arrowmap->{$std}{color};
    }
    my @all_reads = sort { $toSort{$a} cmp $toSort{$b} } keys %toSort;
    return "" if ($#all_reads < 0);
    foreach my $reads (@all_reads) {
	my $tags = $gv_arrowmap->{$reads};
	$tags->{label} = $reads;
	my $na = ++$nodeCount;
	my $nb = ++$nodeCount;
        my $edat = {
            key   => join("\t", $na, $nb, $reads),
            group => 0,
            edges => [],
        };
	$string .= $self->_graphviz_edge( $edat, $tags);
    }
    for my $i (1..$nodeCount) {
	$string .= sprintf("  %d [label=\"%s Node\"];\n", 
			   $i, $i % 2 ? "This" : "That");
    }
    $string .= "}\n";
    my $mt = $self->tracker;

    $self->benchstop;
    return $self->to_graphviz_html( -string => $string,
                                    -program  => 'dot',
				    -filename => "GV_Key",
				    @_, ) . "<br />\n";
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_graphviz_html

 Title   : to_graphviz_html
 Usage   : my $string = $obj->to_graphviz_html;
 Function: Generates the graphviz dot deffinition describing the edges.
 Returns : 
 Args    : 

=cut

sub to_graphviz_html {
    my $self = shift;
    my $mt = $self->tracker;
    my $args = $self->parseparams
        ( -string    => undef,
          -filename  => undef,
          -pathkey   => 'TMP',
          -maxwait   => 0,
          -nodeclick => undef,
          edgeclick  => undef,
          @_);
    my $basename = $args->{FILENAME};
    unless ($basename) {
        my $priroot = $self->{ROOTS}[0];
        $basename = $priroot ? $priroot->name : "PID_$$";
    }
    $self->{BASENAME} = $basename;
    $basename   .= "_GV";
    $basename    =~ s/[\W]/_/g unless ($args->{NOCLEAN});
    my $pathkey  = $args->{PATHKEY} || 'TMP';
    my $maxwait  = $args->{MAXWAIT};
    my $basepath = $mt->file_path($pathkey);
    my $baseurl  = $mt->file_url($pathkey);
    my $imapfile = "$basepath$basename.imap";
    my $dotfile  = "$basepath$basename.dot";
    my $doturl   = "$baseurl$basename.dot";
    my $pngfile  = "$basepath$basename.png";
    my $pngurl   = "$baseurl$basename.png";
    my ($dotString, $data) = ($args->{STRING}, {} );
    my $prg = $args->{PROGRAM} || $self->node_format('graph','program')
        || 'dot';
    $prg = lc($prg);

    # print "<a href='$doturl' target='_blank'>$dotfile</a>"; warn "foo!";

    $dotString = $self->to_graphviz( %{$args},
                                     -ishtml  => 1,
                                     -program => $prg, ) unless ($dotString);
    return wantarray ? ("", $data) : "" unless ($dotString);
    
    $self->benchstart;
    open(DOTFILE, ">$dotfile"."") || 
	$self->death("Write to GraphViz dot file '$dotfile' failed:\n$!");
    print DOTFILE $dotString;
    close DOTFILE;
    chmod(0777, $dotfile);

    # Program options: dot, neato
    my $prgpath = "/stf/sys/bin/$prg";
    unless (-e $prgpath) {
	$self->death("I could not find the GraphViz program '$prgpath'");
    }

    # WE CAN NOT FORK - it totally screws with the DBI handle.

    my $cmd = "$prgpath -Timap -o $imapfile -Tpng -o $pngfile $dotfile";
    my $waitdat = {
        quiet  => ($basename =~ /_key_/ ? 1 : 0),
        start  => time,
        intv   => 15,
        prefix => "Waiting for GraphViz to render network\n",
    };
    my $watch =  "$basepath$basename.gv_pid";
    unlink($watch) if (-e $watch); 
    unlink("$watch.done") if (-e "$watch.done");

    # warn "GV PID watch file : $watch\nGV command : $cmd\n  ";
    system("/stf/biocgi/gv_launcher.pl $watch '$cmd' &");
    while (1) {
        # Spin wheels until PID file appears
        last if (-s $watch);
        my $wait = $self->_wait_progress($waitdat, "Waiting for GV execution");
        $self->death("GraphViz execution never began!") if ($wait >= 30);
    }
    open(FILE, "<$watch") || $self->death("Failed to read from '$watch':\n$!");
    my $pid = <FILE>;
    close FILE;
    unless ($pid) {
        $self->death("Failed to get process ID from $watch");
    }
    chomp $pid;
    unless ($pid =~ /^\d+$/) {
        $self->death("Process ID '$pid' from $watch is non-numeric");
    }
    while (1) {
        # Wait for the pid to go away
        my $val = `ps -p $pid -o 'pid='`; 
        chomp($val);
        last unless ($val);
        my $wait = $self->_wait_progress($waitdat, "Waiting for GraphViz to finish");
        if ($maxwait && $wait > $maxwait) {
            my $txt = "<font color='red'>GraphViz failed to render network after $wait seconds</font><br />\n";
            return wantarray ? ($txt, $data) : $txt;
        }
    }
    my $ti = $waitdat->{wait};
    until (-e "$watch.done") {
        # MAKE SURE the process has finished - PID may not be visible if it
        # is sleeping (?)
        my $wait = $self->_wait_progress($waitdat, "Hurry up and finish!");
        $ti = $waitdat->{wait};
        $self->death("GraphViz should have finished!")
            if ($ti && $wait - $ti > 15);
    }


    unlink($watch); 
    unlink("$watch.done");

    if (!$waitdat->{prefix} && $ENV{'HTTP_HOST'}) {
        warn sprintf("Done. %d seconds overall.\n", $ti);
    }
    unless (-s $pngfile) {
        $self->benchstop;
        return wantarray ? ("", $data) : "";
    }


    my $string = "";
    foreach my $fpath ( $pngfile, $dotfile, $imapfile ) {
        chmod(0777, $fpath);
    }

    unless (-e $pngfile && -s $pngfile) {
        $string = "<font color='red'><b>Network Image Generation Failure</b></font><br />";
        $self->benchstop;
        return wantarray ? ($string, $data) : $string;
    }

    my @proxids = sort {$a <=> $b} keys %{$self->{PROXY}};
    if ($#proxids > -1) {
        my $hash = {
            ToProxy => {},
            FromProxy => {},
        };
        foreach my $sid (@proxids) {
            my $pid = $self->proxy($sid)->id;
            $hash->{ToProxy}{$sid}    = $pid;
            $hash->{FromProxy}{$pid} ||= [];
            push @{$hash->{FromProxy}{$pid}}, $sid;
        }
        # $string .= $mt->get_arbitrary_data( $hash );
    }

    if ($data) {
	my @gids = keys %{$self->{GROUPS}};
	if ($#gids > -1 ) {
	    my $divcols = 3;
	    while (my ($gid, $group) = each %{$self->{GROUPS}}) {
		my @nodes = map { $self->node($_) } keys %{$group->{members}};
		my @seqs = sort {$a->name cmp $b->name}  @nodes;
		my $numNames = $#seqs + 1;
		my $divtext = "";
		my $getAll = join(",", map { $_->namespace_name } @seqs);
		$divtext .= sprintf
		    ("<center><i><a href='mapTracker.pl?seqnames=%s'>Get all %d</a></i></center>",
		     $getAll, $numNames);
		$divtext .= "<table><tr>";
		my $colnum = int( 0.9999 + $numNames / $divcols);
		while (my @col = splice(@seqs, 0, $colnum) ) {
		    my @links;
		    foreach my $seq (@col) {
			my $l = sprintf
			    ("<a href='mapTracker.pl?seqname=%s'>%s</a>", 
			     $seq->name, $seq->namesapce_name);
			$l = "$l&nbsp;*" if ($self->{CONNECTIONS}{ $seq->id });
			push @links, $l;
		    }
		    $divtext .= sprintf("<td valign='top'><font size='-1'>%s</font></td>",
					join("<br />", @links));
		}
		$divtext .= "</table>";
		
	    }
	}
        $data->{imap} = $imapfile;
        $data->{path} = $pngfile;
        $data->{url}  = $pngurl;
        $data->{dir}  = $baseurl;


        my $nC = $args->{NODECLICK};
        my $eC = $args->{EDGECLICK};

	open(IMAP, $imapfile) || 
	    $self->death("Read from IMAP file '$imapfile' failed:\n$!");
	$string .= "<map name='$basename'>\n";
	my @overlays;
        my %seen;
	while (<IMAP>) {
	    chomp;
            my @bits  = split(/\s+/, $_);
            my $shape = shift @bits;
            my $tag   = shift @bits;
            my $coord = join(",", @bits);
            $seen{$tag}++;
	    my ($type, $tvals) = split("_", $tag);
	    my ($obj, $mtObj);
            my $hfunc;
            my $href = "";
	    if ($type eq 'node') {
		$mtObj = $self->node( $tvals );
                $obj = 'node';
		push @overlays, [ $bits[0], $self->{OVERLAYS}{$tvals} ] 
		    if ($self->{OVERLAYS}{$tvals});
                if ($args->{SHOWNAME} && !$mtObj->is_class('anonymous')) {
                    my $alt = $mtObj->name;
                    $alt =~ s/\'/\\\'/g;
                    $href .= "title='$alt' ";
                }
                $hfunc = $nC;
	    } elsif ($type eq 'edge') {
                # edges generate 3 lines in the imap file. Only the first
                # appears to be the edge label - the others seem to be out
                # in random space...
                $obj = 'edge' unless ($seen{$tag} > 1);
                $hfunc = $eC;
	    } elsif ($type eq 'group') {
                $obj = 'group';
	    }
	    next unless ($obj);
            my $func = $hfunc ? &{$hfunc}($mtObj) : 
                $self->javascript_function("{token:'$obj"."_$tvals'}");
            next unless ($func);
            $href .= "href='#' onclick=\"$func; return false\"";
	    $string .= sprintf("  <area shape='%s' coords='%s' %s />\n", 
			       $shape, $coord, $href);
	}

	if ($#overlays > -1) {
	    # We need to make a blank image, then copy in the GV PNG
	    # This has to be done in order to scour the GraphViz color
	    # table, which appears to be bloated with unused entries.
	    my $base = GD::Image->new($pngfile);
	    my ($bw,$bh) = $base->getBounds();
	    my $image = GD::Image->new($bw,$bh);
	    $image->copy($base,0,0,0,0,$bw,$bh);
	    foreach my $dat (@overlays) {
		my ($ur, $species) = @{$dat};
		my ($x,$y) = split(",", $ur);
		foreach my $overlay (@{$species}) {
		    my ($width,$height) = $overlay->getBounds();
		    $image->copy($overlay,$x,$y,0,0,$width,$height);
		    $x += $width;
		}
	    }
	    open (REWRITE,">$pngfile") || 
		$mt->death("Could not re-write data to $pngfile");
	    binmode REWRITE;
	    print REWRITE $image->png;
	    close REWRITE;
	}
	close IMAP;
	$string .= "</map>\n";
    }
    $string .= "<img src='$pngurl' border=0 usemap='#$basename'/>\n";
    $self->benchstop;
    return wantarray ? ($string, $data) : $string;
}

*tree_root = \&find_tree_root;
sub find_tree_root {
    my $self = shift;
    my $args = $self->parseparams( -node      => $self->{ROOTS}[0],
				   -rootdata  => undef,
				   -distance  => 0,
                                   -childedge => 'is a child of',
				   @_);

    $self->benchstart;
    my $mt   = $self->tracker;
    unless ($args->{ROOTDATA}) {
        # First iteration, initialize data structure
        my $cedges = ref($args->{CHILDEDGE}) ? 
            $args->{CHILDEDGE} : [ $args->{CHILDEDGE} ];
        foreach my $cedge (@{$cedges}) {
            my $safety = $mt->get_type($cedge);
            return () unless ($safety);
        }
        my $pedges = [ map { $mt->reverse_type( $_ ) } @{$cedges} ];
        $args->{ROOTDATA} = {
            nodes => {},
            cedges => $cedges,
            pedges => $pedges,
            followed => {},
        };
    }
    my ($pedges, $cedges) = 
        ($args->{ROOTDATA}{pedges}, $args->{ROOTDATA}{cedges});
    my $node = $mt->get_seq( $args->{NODE} );
    my $sid  = $node->id;
    my $dist = $args->{DISTANCE};
    my @par_nodes;
    foreach my $cedge (@{$cedges}) {
        foreach my $par ($self->nodes_from_node( $sid, $cedge)) {
            push @par_nodes, $par unless ($self->{TOSS_NODE}{$par->id});
        }
    }
    if ($self->{TOSS_NODE}{$sid}) {
        # this node has been excluded from the network
    } elsif ($#par_nodes > -1) {
        # This node has 'parent' nodes, follow them
        # Record that we have followed the parent edges:
        $args->{ROOTDATA}{followed}{$sid} = $#par_nodes + 1;
        foreach my $seq (@par_nodes) {
            my $nid = $seq->id;
            #print $self->node($sid)->name . " $cedge " . $seq->name ."<br>\n";
            # Do not recurse into nodes already followed - if this
            # case exists, the tree has a circular component to it.
            next if ($args->{ROOTDATA}{followed}{$nid});
            $self->find_tree_root( -node     => $seq,
                                   -distance => $dist + 1,
                                   -rootdata => $args->{ROOTDATA}, );
        }
    } else {
        my @kid_nodes;
        foreach my $pedge (@{$pedges}) {
            foreach my $kid ($self->nodes_from_node( $sid, $pedge)) {
                push @kid_nodes, $kid unless ($self->{TOSS_NODE}{$kid->id});
            }
        }
        if ($#kid_nodes > -1) {
            # The node does not have parents, but it does have 'children' -
            # therefore it is a/the root
            if ($args->{ROOTDATA}{nodes}{$sid}) {
                $args->{ROOTDATA}{nodes}{$sid} = $dist
                    if ($args->{ROOTDATA}{nodes}{$sid} < $dist);
            } else {
                $args->{ROOTDATA}{nodes}{$sid} = $dist;
            }
        }
    }

    my @found = keys %{$args->{ROOTDATA}{nodes}};
    if ($#found < 0) {
        # No true parents found, so use a node with the *fewest*
        # number of parents
        @found = sort { $args->{ROOTDATA}{followed}{$b} <=> 
                            $args->{ROOTDATA}{followed}{$a} }
        keys %{$args->{ROOTDATA}{followed}};

    } else {
        # Sort the list to have most distant parent first
        @found = sort { $args->{ROOTDATA}{nodes}{$b} <=> 
                            $args->{ROOTDATA}{nodes}{$a} } @found;
    }
    $self->benchstop;
    return @found;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_flat_file

 Title   : to_flat_file
 Usage   : $nw->to_flat_file( -file => $path );
 Function: Store the network as a simple flat file
 Returns : 
 Args    : 

=cut

sub to_flat_file {
    my $self = shift;
    my $args = $self->parseparams( -file => undef,
				   @_ );
    my $file = $args->{FILE};
    my $mt   = $self->tracker;
    my $iter = ++$self->{ITERATION};
    $self->{FILE_SOURCE} = $file;
    $self->{MAX_ITER}    = $iter;

    $self->{NW_EXC_COUNT} = 0;
    my $fh = *STDOUT;

    if ($file) {
        open(FILE, ">$file") || $self->death
            ("Failed to write '$file' to_flat_file:\n$! ");
        $fh = *FILE;
    }
    $self->{FF_FH} = $fh;

    my $dt = `date`; chomp($dt);
    $self->_ff_line('COM', "MapTracker Network flat file dump");
    $self->_ff_line('COM', "Written on $dt");
    $self->_ff_line('COM', "First column indicates edit iteration - lower numbers are data first seen early in editting");

    if ($self->{ITER}) {
        my @bits;
        foreach my $key (sort keys %{$self->{ITER}}) {
            my $val = $self->{ITER}{$key};
            push @bits, sprintf("%s = %d", $key, $val);
        }
        if ($#bits > -1) {
            $self->_ff_line('COM', "Rendering summary - ".join(", ", @bits));
        }
    }

    $self->_ff_header("Program Parameters: parameter value");
    print $fh "0\tPARAM\tITERATION\t$iter\n";
    foreach my $key (keys %{$self->{PARAM}}) {
        my $val = $self->{$key};
        $val = '-UNDEF-' unless (defined $val);
        $self->_ff_line('PARAM', $key, $val );
    }
    $self->_ff_excluded('PARAM');

    my @proxied = sort { uc($a->namespace_name) cmp uc($b->namespace_name) } 
    map { $self->node($_) } keys %{$self->{PROXY}};
    if ($#proxied > -1) {
        $self->_ff_header("Node proxies: ActualNode ProxyUsed");
        foreach my $node (@proxied) {
            my $proxy = $self->proxy($node);
            $self->_ff_line('PROXY', $node->namespace_name, $proxy->namespace_name);
        }
    }

    $self->_ff_header("Root nodes: name");
    foreach my $root ($self->each_root) {
        $self->_ff_line('ROOT', $root->namespace_name);
    }
    $self->_ff_excluded('ROOT');

    my @eseqs = $self->each_excluded_node;
    if ($#eseqs > -1) {
        $self->_ff_header("Excluded nodes: name");
        foreach my $seq (@eseqs) {
            $self->_ff_line('TOSS', $seq->namespace_name);
        }
    }
    $self->_ff_excluded('TOSS');
    
    my (%reads_as);
    foreach my $tid (sort keys %{$self->{EDGE_LOOKUP}{READS}}) {
        $tid =~ s/TYPE_//;
        my $type = $mt->get_type( $tid );
        map { $reads_as{ $_ } = 1 } $type->reads;
    }
    my @vis = sort keys %reads_as;
    if ($#vis > -1) {
        $self->_ff_header("Edge Visualization: 0=Show 1=Hide 2=Suppress");
        foreach my $reads (@vis) {
            my $level = $self->visualize_edge( $reads );
            $self->_ff_line('VISUALIZE', $level, $reads);
        }
    }
    

    my @param_nodes = sort keys %{$self->{NODE_FORMAT}};
    if ($#param_nodes > -1) {
        $self->_ff_header("Node formatting: name parameter value");
        foreach my $sid (@param_nodes) {
            my $seq = $self->node($sid);
            while (my ($param, $val) = each %{$self->{NODE_FORMAT}{$sid}}) {
                $val = "" unless (defined $val);
                $self->_ff_line('FORMAT', $seq->namespace_name, $param, $val);
            }
        }
    }
    $self->_ff_excluded('FORMAT');

    $self->_ff_header("Edges: edge_id name1 reads name2 namespace",
                      "Tags : authority tag_name value_name numeric_value");
    my @edges = sort { uc($a->node1->name) cmp uc($b->node1->name) ||
                       uc($a->node2->name) cmp uc($b->node2->name) ||
                       $a->space->id <=> $b->space->id } $self->each_edge;
    foreach my $edge (@edges) {
        my ($n1, $n2) = map { $_->namespace_name } $edge->nodes;
        my $reads = $edge->reads;
        my $esn   = $edge->space->name;
        my $eid   = $edge->id;
        $self->_ff_line('EDGE', $eid, $n1, $reads, $n2, $esn);
        foreach my $tag ($edge->each_tag) {
            $self->_ff_line('TAG', $eid, $tag->auth->name, 
                            $tag->tag_nsname, $tag->val_nsname, $tag->num);
        }
        print $fh "\n";
    }
    $self->_ff_excluded('EDGE');

    my @groups = values %{$self->{GROUPS}};

    if ($#groups > -1) {
        $self->_ff_header("Groups: parent reads");
        my %seengroup;
        foreach my $group (@groups) {
            my $gid = $group->{egid};
            next if ($seengroup{$gid});
            $seengroup{$gid} = 1;
            my $pn = $group->{parent};
            $self->_ff_line('GROUP', $pn->namespace_name, $group->{reads});
            my @eids = sort keys %{$group->{edges}};
            $self->_ff_line('MEMBERS', @eids);
            print $fh "\n";
        }
    }
    my @gids = sort keys %{$self->{GROUPS}};
    $self->_ff_excluded( 'GROUP','MEMBERS' );

    if ($file) {
        close $fh;
        chmod(0777, $file);
    }
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 from_flat_file

 Title   : from_flat_file
 Usage   : my $string = $obj->from_flat_file;
 Function: Store the network as a simple flat file
 Returns : 
 Args    : 

=cut

sub from_flat_file {
    my $self = shift;
    my $args = $self->parseparams( -file => undef,
                                   -fh   => undef,
                                   -iteration => undef,
				   @_ );
    my $fh = $args->{FH};
    unless ($fh) {
        my $file = $args->{FILE};
        unless ($file) {
            $self->death("Network::from_flat_file requires either a ".
                         "-file or -fh argument");
        }
        if ($file =~ /\.\./) {
            $self->death("Loading relative paths is disallowed");
        }
        
        if ($file =~ /^\/(apps|bin|etc|lib|lib64|logs|lost\+found|mnt|opt|proc|root|scratch|sbin|selinux|sys|usr|var)/) {
            $self->death("Can not load file from protected directory");
        }
        open(FILE, "<$file") || $self->death
            ("Could not read network flat file", $file, $!);
        $fh = *FILE;
        $self->{FILE_SOURCE} = $file;
    }
    my $mt = $self->tracker;
    my $useiter = $args->{ITERATION} || 0;
    $self->{CURRENT_ITER} = $useiter;

    # Currently active objects (and what they are needed for):
    # $edge       - Edge (assigning tags)
    # $grp_parent - Group parent (assigning edges)
    my ($edge, $grp_parent);

    my %old_edge_ids;
    while (<$fh>) {
        chomp;
        next if ($_ =~ /^\s*$/);
        next if ($_ =~ /^\#/);
        my @cols = split(/\t/, $_);
        my $iter = shift @cols;
        $self->{MAX_ITER} = $iter if ($self->{MAX_ITER} < $iter);
        unless ($iter =~ /^\d+/) {
            # Compatibility with pre-iteration files
            unshift @cols, $iter;
            $iter = 1;
        }
        my $line = join("\t", @cols);
        $self->{NW_LOADED}{$line} = $iter;
        my $act  = uc( shift @cols);
        if ($useiter && $iter > $useiter) {
            push @{$self->{NW_EXCLUDED}}, [$act, $_] unless ($act eq 'COM');
            next;
        }
        if ($act eq 'COM') {
            next;
        } elsif ($act eq 'PARAM') {
            my ($key, $val) = @cols;
            $val = "" unless (defined $val);
            $val = undef if ($val eq '-UNDEF-');
            $self->{$key} = $val;
        } elsif ($act eq 'ROOT') {
            my ($name) = @cols;
            $self->add_root($name);
        } elsif ($act eq 'TOSS') {
            my ($name) = @cols;
            $self->exclude_node($name);
        } elsif ($act eq 'VISUALIZE') {
            my ($level, $reads) = @cols;
            $self->visualize_edge($reads, $level);
        } elsif ($act eq 'PROXY') {
            my ($node, $proxy) = @cols;
            $self->proxy($node, $proxy);
        } elsif ($act eq 'FORMAT') {
            my ($name, $param, $val) = @cols;
            $self->node_format( $name, $param, $val);
        } elsif ($act eq 'EDGE') {
            my ($eid, $name1, $reads, $name2, $space) = @cols;
            if ($eid =~ /^\d+$/) {
                # This edge is stored in the database
                $edge = BMS::MapTracker::Edge->new
                    ( -name1 => $name1,
                      -name2 => $name2,
                      -type  => $reads,
                      -id    => $eid,
                      -space => $space,
                      -tracker => $mt );
            } else {
                # This is a transient edge
                ($edge) = $mt->make_transient_edge
                    ( -name1   => $name1,
                      -name2   => $name2,
                      -type    => $reads,
                      -space   => $space );
                # Creation of a transient edge could result in a new
                # edge_id being assigned
            }
            $old_edge_ids{$eid} = $edge;
            $self->add_edge($edge);
        } elsif ($act eq 'TAG') {
            my ($eid, $areq, $treq, $vreq, $num) = @cols;
            my $edge = $old_edge_ids{$eid};
            unless ($edge) {
                $self->error("Attempt to set EDGETAG without an EDGE");
                next;
            }
            my $auth = $mt->get_authority($areq);
            my $tag  = $mt->get_seq($treq);
            my $val  = $mt->get_seq($vreq);
            $edge->add_tag($auth->id, $tag->id, $val ? $val->id : undef, $num);
        } elsif ($act eq 'GROUP') {
            # Initial group deffinition
            ($grp_parent) = @cols;
            # We do not care about 'reads' - that will be figured out
            # with the first edge.
        } elsif ($act eq 'MEMBERS') {
            unless ($grp_parent) {
                $self->error("Attempt to set group MEMBERS without a GROUP");
                next;
            }
            foreach my $eid (@cols) {
                my $edge = $old_edge_ids{$eid};
                unless ($edge) {
                    $self->error("Attempt to add unknown edge $eid to group");
                    next;
                }
                $self->group_edge( $edge, $grp_parent);
            }
        }
    }
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub _ff_line {
    my $self = shift;
    my $fh   = $self->{FF_FH};
    my $line = join("\t", map { defined $_ ? $_ : "" } @_);
    my $iter = $self->{NW_LOADED}{$line} ? 
        $self->{NW_LOADED}{$line} : $self->{ITERATION};
    print $fh "$iter\t$line\n";
}

sub _ff_header {
    my $self = shift;
    my $fh   = $self->{FF_FH};
    print $fh "\n"; 
    $self->_ff_line('COM', ('- ' x 35));
    foreach my $head (@_) {
        $self->_ff_line('COM', $head);
    }
    $self->_ff_line('COM', ('- ' x 35));
    print $fh "\n"; 
}

sub _ff_excluded {
    my $self = shift;
    my @to_print = @_;
    my $header = "Entries excluded while viewing prior iteration";
    my $fh     = $self->{FF_FH};
    while ($self->{NW_EXC_COUNT} <= $#{$self->{NW_EXCLUDED}}) {
        my $printit = 0;
        my ($act, $line) = @{$self->{NW_EXCLUDED}[ $self->{NW_EXC_COUNT} ]};
        for my $i (0..$#to_print) {
            $printit = 1 if ($act eq $to_print[$i]);
        }
        last unless ($printit);
        if ($header) {
            print $fh "\n"; 
            $self->_ff_line('COM', $header);
            print $fh "\n";
            $header = 0;
        }
        print $fh "$line\n";
        $self->{NW_EXC_COUNT}++;
    }
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 show_html_options

 Title   : show_html_options
 Usage   : my $string = $obj->show_html_options;
 Function: Generate HTML data for modifying the file
 Returns : 
 Args    : 

=cut

sub show_html_options {
    my $self = shift;
    my $args = $self->parseparams( -picks => [],
				   @_ );
    my $string = "";
    my $file = $self->{FILE_SOURCE};
    return $string unless ($file);
    my $mt   = $self->tracker;
    my $cols = 4;
    my $halfcol = $cols / 2;
    my $color;
    my $baseurl = "http://$ENV{SERVER_NAME}$ENV{SCRIPT_NAME}";
    $string .= "<table><tr><td colspan='$cols' bgcolor='#66ffff'>";
    $string .= "<form action='$baseurl' method='get'><center>\n";
    $string .= "<b>Global Network Alterations</b><br />\n";
    $string .= "<input type='submit' value='Alter Network' name='altnet'/>\n";
    $string .= "</center>\n";
    $string .= "<b>Taxa Icons:</b> ";
    my ($tion, $tioff) = $self->param('no_tax_tags') ?
        ('', ' CHECKED') : (' CHECKED', '');
    my $ttr = "<input type='radio' name='setnetparam' value='NO_TAX_TAGS\t";
    $string .= $ttr."0'$tion > On ";
    $string .= $ttr."1'$tioff > Off<br />";
    $string .= "<input type='hidden' value='$file' name='nwfile'/>\n";
    $string .= "</td></tr>\n";
    my %types;
    
    foreach my $reads (sort keys %{$self->{EDGE_LOOKUP_OLD}{READS}}) {
        my ($type, $dir) = $mt->get_type($reads);
        my $index = $dir == -1 ? 1 : 0;
        my $tid = $type->id;
        $types{$tid} ||= [];
        $types{$tid}[$index] = $reads;
    }
    my @tids = sort keys %types;
    if ($#tids > -1) {
        $color = "bgcolor='#ffff66'";
        $string .= "<tr><th $color colspan='$cols'>Edge Visualization (Show, Hide, Suppress)</th></tr>\n";
        my $vc = 0;
        foreach my $tid (@tids) {
            $string .= "<tr>";
            for my $i (0..1) {
                my $reads = $types{$tid}[$i];
                $string .= "<td colspan='$halfcol' $color>";
                unless ($reads) {
                    $string .= "</td>";
                    next;
                }
                my $level = $self->visualize_edge( $reads );
                $vc++;
                for my $i (0..2) {
                    $string .= sprintf("<input type='radio' name='nwvis%d' ".
                                       "value='%s=%d' %s> ", $vc, $reads, $i, 
                                       $i == $level ? 'CHECKED' : '');
                }
                $string .= " $reads<br />\n";
                $string .= "</td>";
            }
            $string .= "</tr>\n";
        }
    }

    $string .= "</form>\n";
    $string .= "</td></tr>\n";
    my @picks = @{$args->{PICKS}};
    unshift @picks, [ 'All Roots', [$self->each_root] ];
    unshift @picks, [ 'All Nodes', [$self->each_node] ];
    $color = "bgcolor='#99ffcc'";
    $string .= "<tr><th colspan='$cols' $color>Add Network Nodes to PickList</th></tr>";
    for my $i (0..$#picks) {
        my ($text, $list) = @{$picks[$i]};
        my @sids = map { $self->node($_)->id } @{$list};
        next if ($#sids < 0);
        my $url  = sprintf("<a href=\"javascript:pick_sid('%s',1)\">%s</a>", 
                           join(',', @sids), $text  );
        $string .= "<tr>" unless ($i % $cols);
        $string .= "<td align='center' $color>$url</td>";
        $string .= "</tr>\n" unless ( ($i+1) % $cols);
    }
    $string .= "</tr>\n" if ( ($#picks +1 ) % $cols);

    my @allsids = map { $self->node($_)->id } $self->each_node;
    # my $allstr  = join('%0A', @allsids);
    my @quick   = ( ['Add Description', 'Label_With_Description'], 
                    ['Add Gene Symbol', 'Label_With_GeneSymbol'],
                    ['Substitute Symbol', 'Proxy_With_GeneSymbol'], );
    $color = "bgcolor='#ffcccc'";
    $string .= "<tr><th colspan='$cols' $color>Quick Annotate</th></tr>";
    for my $i (0..$#quick) {
        my ($lab, $path) = @{$quick[$i]};
        $string .= "<tr>" unless ($i % $cols);
        $string .= sprintf("<td align='center' %s><a href='mapTracker.pl?".
                           "usepath=%s&nwfile=%s&expandnodes=%s'>%s</a></td>",
                           $color, $path, $file, 'all', $lab);
        $string .= "</tr>\n" unless ( ($i+1) % $cols);
    }
    $string .= "</tr>\n" if ( ($#quick +1 ) % $cols);
    $string .= "\n</table>\n";
    return $string;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub hierarchy_html {
    my $self = shift;
    my $args = $self->parseparams( -node       => $self->{ROOTS}[0],
				   -maxdepth   => 2,
                                   -showparent => 1,
                                   -childedge  => 'is a child of',
				   @_);
    my $mt     = $self->tracker;
    my $seq    = $mt->get_seq( $args->{NODE} );
    my $maxd   = $args->{MAXDEPTH};
    my $cedge  = $args->{CHILDEDGE};
    my $safety = $mt->get_type($cedge);
    unless ($safety) {
        return ();
    }
    my $pedge = $mt->reverse_type( $cedge );
    my $string = "";
    my $proxy  = $self->proxy($seq);
    if ($args->{SHOWPARENT}) {
        my $sid = $seq->id;
        my ($parent) = $self->find_tree_root( @_,
                                              -node => $sid );
        return $string unless ($parent);
        my $string2 = $self->to_graphviz_html
            ( -filename  => $sid."_tree",
              -node      => $parent,
              -clusterat => 10,
              -skipreads => [],
              -labelwith => [ 'is a shorter term for' ],
              -highlight => [ $self->proxy($sid)->id ],
              -abort     => [ $sid ],
              );
        if ($string2) {
            $string .= sprintf("<b>Parents for %s:</b><br />\n", $proxy->name);
            $string .= $string2 . "<br />\n";
        } else {
            $string = sprintf("<b>%s has no parents</b><br />\n",$proxy->name);
        }
    }

    $string .= sprintf("<b>Children for %s</b> (maximum depth of %d):<br />\n",
		       $proxy->name, $maxd);
    $string .= $self->_nested_list( -node     => $seq, 
                                    -maxdepth => $maxd,
                                    -paredge  => $pedge );
    return $string;
}

sub _nested_list {
    my $self = shift;
    my $args = $self->parseparams( -node       => undef,
				   -maxdepth   => 1,
                                   -paredge    => 'is a parent of',
                                   -level      => 0,
				   @_);
    $self->benchstart;
    my ($maxdepth, $level, $pedge) = ( $args->{MAXDEPTH},
                                       $args->{LEVEL} || 0,
                                       $args->{PAREDGE}, );
    my $mt    = $self->tracker;
    my $seq   = $mt->get_seq( $args->{NODE} );
    my $edges = $self->edges_hash($args->{NODE});
    my $short = "";
    if (exists $edges->{'is a shorter term for'}) {
	my @terms = ();
	foreach my $shid (keys %{$edges->{'is a shorter term for'}}) {
	    my $shseq = $mt->get_seq( $shid );
	    push @terms, $shseq->name;
	}
	$short = join(" <font color='brick'>aka</font> ", sort @terms);
    }
    my $col = $hier_colors[ ($level - 1) % ($#hier_colors + 1) ];
    my $text = "";
    my $prox = $self->proxy($seq);

    $text .= sprintf("<table><tr><td nowrap bgcolor='#%s'><b>%s</b>".
		     "</td><td width='1000'><i>%s</i></td></tr></table>",
		     $col, $prox->javascript_link(), $short) if ($level);
    if ($level + 1 <= $maxdepth && exists $edges->{ $pedge }) {
	$text .= "<table><tr>";
	$text .= "<td width='10' bgcolor='#$col'>&nbsp;&nbsp;</td>" 
	    if ($level);
	$text .= "<td>\n";
	my @kids;
	foreach my $nid (keys %{ $edges->{ $pedge }}) {
	    next unless ($nid =~ /^\d+$/);
	    my $kid = $mt->get_seq( $nid );
	    next unless $kid;
	    push @kids, $kid;
	}
	@kids = sort {$a->name cmp $b->name } @kids;
	foreach my $kid (@kids) {
	    $text .= $self->_nested_list( @_,
                                          -node  => $kid,
                                          -level => $level + 1);
	}
	$text .= "</td></tr></table>\n";
    }
    $self->benchstop;
    return $text;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# Turns a hash ref keyed to authority IDs into a list of authority names

sub _authority_list {
    my $self = shift;
    my ($hash) = @_;
    my $mt = $self->tracker;
    my @auths = map { $mt->get_authority($_) } keys %{$hash};
    my @autnames = sort map { $_->name } @auths;
    return @autnames;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub find_path {
    my $self = shift;
    my $args = $self->parseparams( -start     => $self->{ROOTS}[0],
				   -end       => undef,
				   -weights   => undef,
				   @_ );
    my $mt     = $self->tracker;
    my $path = NetworkPath->new( -network => $self );
    # my $go = $args->{'END'}; printf("%s : Parent of GO:0007186: %d\n", $go->name, $go->has_parent('GO:0007186') );
    $path->start( $args->{'START'} || $self->{ROOTS}[0] );
    $path->end( $args->{'END'} );
    $path->weights( $args->{WEIGHTS} );
    $path->blaze( %{$args} );
    return $path;
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package NetworkPath;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use strict;
use BMS::Branch;
use vars qw(@ISA);
use BMS::MapTracker::Shared;

@ISA = qw(BMS::MapTracker::Shared);

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = { 
	NETWORK       => undef,
	WEIGHTS       => {},
	PATHS         => [],
	DEFAULTWEIGHT => 0.95,
    };
    bless ($self, $class);
    my $args = $self->parseparams( -network => undef,
				   @_ );
    my $nw = $self->{NETWORK} = $args->{NETWORK};
    $self->death("A NetworkPath object must be provided with a Network object")
	unless ($nw);
    return $self;
}

sub DESTROY {
    my $self = shift;
    foreach my $key ('TRACKER', 'NETWORK') {
	$self->{$key} = undef;
    }
}

sub tracker {
    my $self = shift;
    return $self->{NETWORK}->tracker;
}

sub network {
    my $self = shift;
    return $self->{NETWORK};
}

sub start {
    my $self = shift;
    if ($_[0]) {
	my $mt  = $self->tracker;
	my $seq = $mt->get_seq( $_[0] );
	unless ($seq) {
	    $mt->error("Could not set NetworkPath start '$_[0]' as it is not in MapTracker DB");
	    return;
	}
	my $sid = $seq->id;
	my $nw  = $self->network;
	unless (exists $nw->{NODES}{ $sid } ) {
	    $mt->error($seq->namespace_name . " can not be used as a ".
                       "NetworkPath start as it is not in the Network.");
	    return;
	}
	$self->{START} = $seq;
    }
    return $self->{START};
}

sub end {
    my $self = shift;
    if ($_[0]) {
	my $mt  = $self->tracker;
	my $seq = $mt->get_seq( $_[0] );
	unless ($seq) {
	    $mt->error("Could not set NetworkPath end '$_[0]' as it is not in MapTracker DB");
	    return;
	}
	my $sid = $seq->id;
	my $nw  = $self->network;
	unless (exists $nw->{NODES}{ $sid } ) {
	    $mt->error($seq->namespace_name . " can not be used as a ".
                       "NetworkPath end as it is not in the Network.");
	    return;
	}
	$self->{'END'} = $seq;
    }
    return $self->{'END'};
}

sub default_weight {
    my $self = shift;
    if ($_[0]) {
	if ($_[0] =~ /^\d*\.?\d*$/ && $_[0]) {
	    $self->{DEFAULTWEIGHT} = $_[0];
	} else {
	    $self->tracker->error("The default weight must be a positive real number, not '$_[0]'");
	}
    }
    return $self->{DEFAULTWEIGHT};
}

sub weights {
    my $self = shift;
    if ($_[0]) {
	my $mt  = $self->tracker;
	$self->{WEIGHTS} = {};
	while ( my ($tname, $weight) = each %{$_[0]}) {
	    if ($tname =~ /default/i) {
		$self->default_weight( $weight );
		next;
	    }
	    my ($type, $dir) = $mt->get_type($tname);
	    my @reads = $type->reads;
	    if ($dir) {
		@reads = $dir < 0 ? ( $reads[1] ) : ( $reads[0] );
		# Directional type, need only one of the reads
	    }
	    foreach my $read (@reads) {
		$self->{WEIGHTS}{$read} = $weight;
	    }
	}
    }
    return $self->{WEIGHTS};
}

sub clear {
    my $self = ();
    $self->{TOTALDIST} = undef;
}

sub total_distance {
    my $self = shift;
    return $self->{TOTALDIST} if (defined $self->{TOTALDIST});
    my ($start, $end) = ($self->start, $self->end);
    my $nw = $self->network;
    my $dist = $nw->distance( $start->id, $end->id);
    unless (defined $dist) {
	$nw->find_all_distances( $start );
	$dist = $nw->distance( $start->id, $end->id);
    }
    $self->{TOTALDIST} = $dist;
    return $dist;
}

sub blaze {
    my $self = shift;
    my $args = $self->parseparams( -path      => undef,
				   -dist      => undef,
				   -backtrack => 0,
				   -minscore  => 0.00000001,
				   -maxlegs   => 999999,
				   @_ );

    my ( $path, $lastdist ) = ($args->{PATH}, $args->{DIST});
    unless ($path) {
	$self->clear;
	unless ($self->end) {
	    $self->error("You can not blaze() without setting end()");
	    return undef;
	}
	unless ($self->start) {
	    $self->error("You can not blaze() without setting start()");
	    return undef;
	}
	my $eid = $self->end->id;
	$path = [ { nodes => [ $self->end ],
		    edges => [],
		    score => [],
		    seen  => { $eid => 1 },
		    total => 1, } ];
	$lastdist = $self->total_distance + $args->{BACKTRACK};
    }
    if ( $lastdist < 1 ) {
	# All paths have been completed
	return $path;
    }
    if ($#{$path} < 0 ) {
	# No paths survived
	return $path;
    }
    my @extended = ();
    my $weights = $self->weights;
    my $nw      = $self->network;
    my $sid     = $self->start->id;
    my $dw      = $self->default_weight;
    my $maxedge = $args->{MAXEDGE} || $args->{MAXLEGS};
    foreach my $subpath (@{$path}) {
	# Find the last node that was added to the subpath:
	my $lid = $subpath->{nodes}[-1]->id;
	my @keepers = ();
	# Do we already know how to get from this node to the start?
	if ($nw->{PATH_CACHE}{$sid} && $nw->{PATH_CACHE}{$sid}{$lid}) {
	    foreach my $frag (@{$nw->{PATH_CACHE}{$sid}{$lid}}) {
		my $newpath = {
		    nodes => [ @{$subpath->{nodes}}, @{$frag->{nodes}}, ],
		    edges => [ @{$subpath->{edges}}, @{$frag->{edges}}, ],
		    score => [ @{$subpath->{score}}, @{$frag->{score}}, ],
		    seen  => { %{$subpath->{seen}},  },
		};
		my $tot =  $subpath->{total};
		foreach my $sc (@{$frag->{score}}) {
		    $tot *= $sc;
		}
		$newpath->{total} = $tot;
		push @keepers, $newpath;
	    }
	} else {
            if ($maxedge && $maxedge <= $#{$subpath->{edges}} + 1 ) {
                # This path is too long
                next;
            }
	    # What edges are available from that node?
	    foreach my $edge ( $nw->edges_from_node( $lid ) ) {
                my ($reads, $lastNode, $other) = $edge->reads($lid);
                my $oid   = $other->id;
		next if ($subpath->{seen}{$oid});
		my $dist = ($oid == $sid) ? 0 : $nw->distance($oid, $sid);
		unless (defined $dist) {
                    # Calculate all distances to the starting node.
		    $nw->find_all_distances( $self->start );
		    $dist = $nw->distance( $oid, $sid);
		}
		# We want to find any edge that gets us closer to the start
		next unless ($dist < $lastdist);
		my $w = exists $weights->{$reads} ? $weights->{$reads} : $dw;
		my $score = $subpath->{total} * $w;
		# The extended path should be kept:
		# Need to de-reference everything, since we will make copies:
		my $newpath = {
		    nodes => [ @{$subpath->{nodes}}, $other   ],
		    edges => [ @{$subpath->{edges}}, $edge ],
		    score => [ @{$subpath->{score}}, $w ],
		    seen  => { %{$subpath->{seen}}, $oid => 1 },
		    total => $score,
		};
		push @keepers, $newpath;
	    }
	}
	
	foreach my $newpath (@keepers) {
	    next if ($newpath->{total} < $args->{MINSCORE});
            if ($maxedge && $maxedge < $#{$newpath->{edges}} + 1 ) {
                # This path is too long
                next;
            }
	    if ($newpath->{nodes}[-1]->id == $sid) {
		# We got to the end
		$self->_add_path( $newpath );
	    } else {
		# We need to keep extending
		push @extended, $newpath;
		# $self->_add_path( $newpath );
	    }
	}
    }
    $self->blaze( %{$args},
		  -path => \@extended, 
		  -dist => $lastdist - 1 );
}

sub _add_path {
    my $self = shift;
    my ($path) = @_;
    push @{$self->{PATHS}}, $path;
    return $path;
}

sub save_best {
    my $self = shift;
    my $args = $self->parseparams( -cache => undef,
				   @_ );
    $self->benchstart;
    my %ranked;
    foreach my $path ( @{$self->{PATHS}} ) {
	my $score = $path->{total} || 50;
	$ranked{$score} ||= [];
	push @{$ranked{$score}}, $path;
    }
    my @sort = sort { $b <=> $a } keys %ranked;
    
    $self->{PATHS} = $#sort > -1 ? $ranked{ $sort[0] } : [];
    if ($args->{CACHE}) {
	my $nw = $self->network;
	my %seen;
	foreach my $path ( @{$self->{PATHS}} ) {
	    my @nodes = @{$path->{nodes}};
	    my @edges = @{$path->{edges}};
	    my @score = @{$path->{score}};
	    my $start = pop @nodes;
	    $nw->{PATH_CACHE}{$start} ||= {};
	    my $max = $#nodes;
	    for my $i (0..$max) {
		my $end = $nodes[$i]->id;
		if (!$seen{$end}) {
		    # First time this fragment end point is observed
		    if ($nw->{PATH_CACHE}{$start}{$end}) {
			# Paths already stored for this start-end combo
			next;
		    }
		    $seen{$end} = {};
		    $nw->{PATH_CACHE}{$start}{$end} = [];
		}
		my $frag = {
		    edges => [ @edges[$i..$max] ],
		    nodes => [ @nodes[$i..$max] ],
		    score => [ @score[$i..$max] ],
		};
		shift @{$frag->{nodes}};
		push  @{$frag->{nodes}}, $start;
		my $tot = 1;
		my $key = "";
		foreach my $j (0..$#{$frag->{score}}) {
		    $tot *= $frag->{score}[$j];
                    my $node  = $frag->{nodes}[$j];
                    my $reads = $frag->{edges}[$j]->reads( $node );
                    $key .= $node->id . " $reads ";
		}
		$key .= $start;
		next if ($seen{$end}{$key});
		$seen{$end}{$key} = 1;
		$frag->{total} = $tot;
		push @{$nw->{PATH_CACHE}{$start}{$end}}, $frag;
	    }
	}
    }
    $self->benchstop;
    return $self->{PATHS};
}

sub paths_as_text {
    my $self = shift;
    $self->benchstart;
    my $mt      = $self->tracker;
    my @paths   = sort { $b->{total} <=> $a->{total} } @{$self->{PATHS}};
    my @strings = ();
    foreach my $path (@paths) {
	my @nodes = @{$path->{nodes}};
	my @edges = @{$path->{edges}};
        my $string = "";
        for my $i (0..$#nodes) {
            my $node = $nodes[$i];
            $string .= $node->name;
            if(my $edge = $edges[$i]) {
                $string .= " " . $edge->reads($node) . " ";
            }
        }
	push @strings, $string;
    }
    $self->benchstop;
    return @strings;
}

sub paths_as_html {
    my $self = shift;
    $self->benchstart;
    my $mt      = $self->tracker;
    my @paths   = sort { $b->{total} <=> $a->{total} } @{$self->{PATHS}};
    my @strings = ();
    foreach my $path (@paths) {
	my @nodes = @{$path->{nodes}};
	my @edges = @{$path->{edges}};
        my $string = "";
        for my $i (0..$#nodes) {
            my $node = $nodes[$i];
            $string .= sprintf("<font color='green'><b>%s</b></font>",
                               $node->name);
            if(my $edge = $edges[$i]) {
                $string .= sprintf(" <font color='brown'><i>%s</i></font> ",
                                   $edge->reads($node));
            }
        }
        push @strings, $string;
    }
    $self->benchstop;
    return @strings;
}

sub paths_as_parts {
    my $self = shift;
    $self->benchstart;
    my $mt   = $self->tracker;
    my @paths = sort { $b->{total} <=> $a->{total} } @{$self->{PATHS}};
    my @allparts = ();
    foreach my $path (@paths) {
	my @nodes = @{$path->{nodes}};
	my @edges = @{$path->{edges}};
	push @allparts, [ \@nodes, \@edges ];
    }
    $self->benchstop;
    return @allparts;
}

sub paths_as_tab {
    my $self = shift;
    $self->benchstart;
    my $mt   = $self->tracker;
    my @paths = sort { $b->{total} <=> $a->{total} } @{$self->{PATHS}};
    my @tab = ();
    foreach my $path (@paths) {
	my @cols   = ();
	my @nodes  = @{$path->{nodes}};
	my $prior  = shift @nodes;
	my $end    = pop @nodes;
	my @edges  = @{$path->{edges}};
        my $string = "";
	push @cols, $path->{total};
	push @cols, $prior->name;
	push @cols, $end->name;
        for my $i (0..$#edges) {
            $string .= $edges[$i]->reads( $prior );
            if ($prior = $nodes[$i]) {
                $string .= " " . $prior->name;
            }
        }
	push @cols, $string;
	push @tab, \@cols;
    }
    $self->benchstop;
    return @tab;
}
# - # - # - # - # - # - # - # - # - # - # - # - # - # - # - # - # - # - # - # 
1;
