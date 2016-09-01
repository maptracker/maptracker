# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::Taxa;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use strict;
BEGIN {
    # Squelch irritating BioPerl error:
    # UNIVERSAL->import is deprecated and will be removed in a future perl
    # at /apps/sys/perl/lib/site_perl/5.12.0/Bio/Tree/TreeFunctionsI.pm line 94
    use BMS::MapTracker::Shared;
    my $foo = BMS::ErrorInterceptor->new();
    $foo->ignore_error('UNIVERSAL->import is deprecated');
}

$BMS::MapTracker::Taxa::VERSION = 
    ' $Id$ ';

use vars qw(@ISA);

use GD;
use Bio::Species;
use Scalar::Util qw(weaken);

use BMS::Branch;
use BMS::MapTracker;

@ISA = qw(BMS::MapTracker::Shared);

our @rank_order = qw(species genus subfamily family suborder order 
                    class phylum kingdom superkingdom);

our $species_icons = {
    genus => {
        sort  => 100,
        match => {
            9903  => 'cow',         # oxen, cattle
            9605  => 'human',       # Lonely taxa
            10088 => 'mouse',       # Mus
            10114 => 'rat',         # Rattus
            10140 => 'cavy',        # Guinea pigs
            3701  => 'arabidopsis', # Arabidopsis
            4527  => 'rice',        # Oryza
            9611  => 'dog',         # Canis
        },
    },
    subfamily => {
        sort  => 200,
        match => {
            10026 => 'hamster', # Cricetinae
        },
    },
    family => {
        sort  => 300,
        match => {
            9821 => 'pig',      # Suidae
            9681 => 'cat',      # Felidae
            9788 => 'horse',    # Equidae
            9979 => 'rabbit',   # Leporidae
            7157 => 'mosquito', # Culicidae
            4894 => 'yeast',    # fission yeasts
        },
    },
    order => {
        sort  => 400,
        match => {
            7147 => 'fly',    # Diptera = flies
            8342 => 'frog',   # frogs and toads
            9443 => 'monkey', # Primates
            4892 => 'yeast',  # Budding yeasts
        },
    },
    class => {
        sort  => 500,
        match => {
            7898 => 'fish', # fishes
            8782 => 'bird', # Aves
        },
    },
    phylum => {
        sort  => 600,
        match => {
            119089 => 'worm',  # nematodes
            3193   => 'plant', # Embryophyta
        },
    },
    superkingdom => {
        sort  => 800,
        match => {
            2     => 'prokaryote', # bacteria
            2157  => 'prokaryote', # Archaes
            10239 => 'virus',      # Viruses - not really a superkingdom
            12908 => 'unknown',    # Unclassified - not really superkingdom
        },
    },
};

our @icon_order = sort {$species_icons->{$a}{sort} <=> 
                            $species_icons->{$b}{sort}} keys %{$species_icons};

my $debug = BMS::Branch->new
    ( -skipkey => ['TRACKER', 'CHILDREN', 'CLASSES', 'FP', 'BENCHMARKS',
		   'SEQNAMES', 'AUTHORITIES','TYPES',],
      -format => 'html', -noredundancy => 1, );

=head1 PRIMARY METHODS
#-#-#-#-#--#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-

=head2 new

 Title   : new
 Usage   : my $obj = BMS::MapTracker::Taxa>new(@arguments)
 Function: Creates a new object and returns a blessed reference to it.
 Returns : A blessed BMS::MapTracker::Taxa object
 Args    : Associative array of arguments. Recognized keys [Default]:

     -name The name of the species / taxa

       -id The database ID for this taxa

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = { };
    bless ($self, $class);
    my $args = $self->parseparams( -id       => undef,
                                   -tracker  => undef,
                                   @_ );
    my $mt = $self->tracker( $args->{TRACKER} ||
                             BMS::MapTracker->new(-username => 'ReadOnly') );
    my $dbi       = $mt->dbi;
    my $id        = $self->{ID} = $args->{ID};
    my $mergePath = $args->{PREVIOUS} || [ $id ];
    my ($name, $pid, $rank, $hide, $merged) = $dbi->named_sth
        ("Retrieve Taxa details")->selectrow_array( $id );
        
    # If this node is merged, return the 'real' value:
    if ($merged && $merged != $id) {
        # Prevent infinite loops
        my %seen = map { $_ => 1 } @{$mergePath};
        if ($seen{$merged}) {
            $self->death("Loop structure discovered in merged_id: ".
                         join(" < ", "!$merged!", @{$mergePath}));
        }
        unshift @{$mergePath}, $merged;
        return BMS::MapTracker::Taxa->new( -id       => $merged,
                                           -tracker  => $mt,
                                           -previous => $mergePath,);
    } elsif (defined $merged) {
        $self->{DEPRECATED} = 1;
    }
    my $nd = "-NOT DEFINED IN MAPTRACKER-";
    # All set operations now defined in new
    $self->{NAME}  = $name || $nd;
    $self->{PID}   = $pid  || 0;
    $self->{RANK}  = $rank || $nd;
    $self->{HIDE}  = (!$hide || $hide eq 'f') ? 0 : 1;
    $self->{MERGE} = $mergePath;
    return $self;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 name

 Title   : name
 Usage   : $obj->name($name)
 Function: Gets the name of the taxa
 Returns : The name
 Args    :

=cut

sub name {
    my $self = shift;
    return $self->{NAME};
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
 Usage   : $obj->id
 Function: Gets the database ID
 Returns : The database ID
 Args    :

=cut


sub id {
    my $self = shift;
    return $self->{ID};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 is_hidden

 Title   : is_hidden
 Usage   : $obj->is_hidden
 Function: Returns 1 if this node is normally hidden, otherwise 0
 Returns : 0 or 1
 Args    :

=cut

*hidden = \&is_hidden;
sub is_hidden {
    my $self = shift;
    return $self->{HIDE};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 is_deprecated

 Title   : is_deprecated
 Usage   : $obj->is_deprecated
 Function: Returns 1 if this node is deprecated (merged == 0)
 Returns : 0 or 1
 Args    :

=cut

sub is_deprecated {
    return shift->{DEPRECATED} ? 1 : 0;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 rank

 Title   : rank
 Usage   : $obj->rank($id)
 Function: Gets the taxa rank (eg 'genus' or 'species')
 Returns : A string
 Args    :

=cut


sub rank {
    my $self = shift;
    return $self->{RANK};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_text_full

 Title   : to_text_full
 Usage   : print $obj->to_text_full($id)
 Function: Represents the taxa as a human-readable text string
 Returns : A string
 Args    :

=cut


sub to_text_full {
    my $self = shift;
    unless ($self->{TO_TEXT_FULL}) {
        my $string = $self->to_text();
        my $alis   = $self->each_alias_class;
        foreach my $class (sort keys %{$alis}) {
            next if ($class eq 'ALL');
            $string .= "  $class\n";
            $string .= join('', map { "    $_\n" } @{$alis->{$class}});
        }
        $self->{TO_TEXT_FULL} = $string;
    }
    return $self->{TO_TEXT_FULL};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_text

 Title   : to_text
 Usage   : print $obj->to_text($id)
 Function: Represents the taxa as a human-readable text string
 Returns : A string
 Args    :

=cut


sub to_text {
    my $self = shift;
    unless ($self->{TO_TEXT}) {
        my $string = sprintf("%s [%d] %s\n", $self->name, $self->id, 
                             $self->rank || "");
        $self->{TO_TEXT} = $string;
    }
    return $self->{TO_TEXT};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 javascript_link

 Title   : javascript_link
 Usage   : my $html_anchor = $obj->javascript_link( $classes, $noicon )
 Function: Generates an <A> string for use with maptracker javascript
 Returns : A string
    Args : Optional HTML classes

=cut

sub javascript_link {
    my $self = shift;
    my ($userClass, $noicon) = @_;
    my $lnk = "";
    $lnk .= $self->img_tag() unless ($noicon);
    $lnk .= $self->SUPER::javascript_link
        ($userClass, 'taxa', $self->id, $self->name);
    return $lnk;
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
        my $txt  = sprintf("type:'taxa', id:%d, name:'%s', hide:%d, ".
                           "rank:'%s', partax:%d",
                           $self->id, $name, $self->hidden, $self->rank,
                           $self->{PID});
        my $aliases = $self->each_alias_class;
        my @abits;
        foreach my $class (sort keys %{$aliases}) {
            next if ($class eq 'ALL');
            my @cbits;
            foreach my $cb (@{$aliases->{$class}}) {
                $cb =~ s/\//\/\//g;
                $cb =~ s/\'/\\\'/g;
                push @cbits, "'$cb'";
            }
            push @abits, "'$class':[ ".join(",",@cbits) . " ]";
        }
        $txt .= ", alias: {" . join(", ", @abits)."}";
        my $icon = $self->icon('url');
        $txt .= ", icon:'$icon'" if ($self->icon);
        $self->{JSTEXT} = "{ $txt }";   
    }
    return $self->{JSTEXT};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 is_taxa

 Title   : is_taxa
 Usage   : my $obj->is_taxa( $testTaxaObject1, [ $testTaxaObject2 ...] );

 Function: Tests to see if this taxa 'is a' single taxa, or one of a
           provided list.

 Returns : 0 if false, 1 or more if match
 Args    : If a true value is passed, then hidden parents will be returned

=cut

sub is_taxa {
    my $self   = shift;
    my $depth  = 0;
    my $mt     = $self->tracker;
    my $focus  = $self;
    my %to_match;
    foreach my $request (@_) {
        my ($taxa) = $mt->get_taxa($request, 'auto');
        next unless ($taxa);
        $to_match{ $taxa->id } = 1;
    }
    while (my $focus_id = $focus->id) {
        $depth++;
        # Return the distance from the taxa if the current parent matches:
        return $depth if ($to_match{ $focus_id });
        # Failed to match, try the next level up
        return 0 unless ($focus = $focus->parent);
    }
    # No matches
    return 0;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 parent

 Title   : parent
 Usage   : my $parent_taxa = $obj->parent
 Function: Gets the parent taxa object, if any
 Returns : The parent taxa object
 Args    : If a true value is passed, then hidden parents will be returned

=cut

# Normally, the parent link is weakend, while child links are
# strong. However, because we can use the methods below to reliably
# assign a parent to all recovered children, but not all children to a
# recovered parent, we will weaken the link to children instead.

# That is, if we do not want the parent or child object to immediately
# evaporate, we need at least *one* strong link to it - so the method
# that sets a weak link must also set one strong link. Because we know
# that each recovered child has $self and only $self as a parent, we
# choose to weaken the parent-to-child link while we simultaneously
# generate a reliable child-to-parent strong link.

sub parent {
    my $self = shift;
    unless ($self->{PARENT}) {
        # We have not read in the parent yet
        my $focus = $self;
        while (1) {
            my $pid = $focus->{PID};
            last unless ($pid);
            my @parents = $self->tracker->get_taxa( $pid, 'auto' );
            if ($#parents < 0) {
                $self->error("Could not locate parent PID $pid for taxa ".
                             $focus->name);
                # Make sure we don't keep trying this call...
                $self->{PID} = 0; last;
            } elsif ($#parents > 0) {
                $self->error
                    ("Taxa ". $focus->name . " has multiple parents under ".
                     "PID $pid: ". join(", ", map {$_->name} @parents));
                # Make sure we don't keep trying this call...
                $self->{PID} = 0; last;
            } else {
                # Unique match
                my $par = $parents[0];
                if ($par->is_hidden) {
                    # This is a normally hidden node, keep searching
                    # for a displayed node
                    $self->{HIDDEN} ||= $par;
                    $focus = $par;
                } else {
                    $self->{PARENT} = $par;
                    last;
                }
            }
        }
        $self->{HIDDEN} ||= $self->{PARENT};
    }
        #warn join(' / ', map { $_->name } ($self, $self->{HIDDEN}, $self->{PARENT}));
    return ($_[0]) ? $self->{HIDDEN} : $self->{PARENT};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 all_parents

 Title   : all_parents
 Usage   : my @parents = $obj->all_parents
 Function: Gets a list of all parents in the taxa hierarchy
 Returns : An array of parent objects
 Args    : If a true value is passed, then hidden parents will be returned

=cut

*each_parent = \&all_parents;
sub all_parents {
    my $self = shift;
    my @parents;
    my $focus = $self;
    while (my $par = $focus->parent( @_ )) {
        push @parents, $par;
        $focus = $par;
    }
    return @parents;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 children

 Title   : children
 Usage   : my @child_taxa = $obj->children
 Function: Gets all child taxa objects, if any
 Returns : An array of taxa objects
 Args    : 

=cut


sub children {
    my $self = shift;
    unless ($self->{CHILDREN}) {
        # We have not read in the children yet
        $self->{CHILDREN} = {};
        my $id  = $self->id;
        if ($id) {
            my $mt  = $self->tracker;
            my $ish = $self->is_hidden;
            my @ids = $mt->dbi->named_sth("Find child taxa")->
                get_array_for_field( $id );
            foreach my $id (@ids) {
                my ($child) = $mt->get_taxa( $id, 'auto' );
                unless ($child) {
                    $self->error("Could not locate child taxa $id for ".
                                 $self->name);
                    next;
                }
                # The link from parent to child is weak:
                weaken( $self->{CHILDREN}{$id} = $child );
                # But the links from child to parent are always strong:
                $child->{HIDDEN} = $self;
                $child->{PARENT} = $self unless ($ish);
            }
        }
    }
    return sort { $a->id <=> $b->id } values %{$self->{CHILDREN}};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 bioperl_species

 Title   : 
 Usage   : $obj->bioperl_species
 Function: Returns a Bio::Species object for the Taxa object.
 Returns : Bio::Species object
 Args    : 

=cut

sub bioperl_species {
    my $self = shift;
    unless ($self->{BIOPERL_SPECIES}) {
        # Only generate object once...
        my $specobj = Bio::Species->new();
        $specobj->ncbi_taxid( $self->id );
        my ($comname) = $self->each_alias('GENBANK COMMON NAME');
        unless ($comname) {
            ($comname) = $self->each_alias('COMMON NAME');
        }
        if ($comname) {
            $comname =~ s/([\w\']+)/\u\L$1/g; # capitalize first letter
            $specobj->common_name($comname);
        }
        $self->{BIOPERL_SPECIES} = $specobj;
        $specobj->rank( $self->rank );
        $specobj->scientific_name( $self->name );

        # Set classification array, if possible.
        # The Bio::Species object requires that the classification array
        # MUST start with the species.

        my $recurse = $self;
        my %rankLookup; my $iLoop = 0;
        while ($recurse) {
            push @{$rankLookup{ $recurse->rank }}, $recurse;
            $recurse = $recurse->parent(1);
            if (++$iLoop > 100) {
                $self->error("Failure to find species root", $self->to_text());
                last;
            }
        }
        if (my $ss = $rankLookup{subspecies}) {
            my $ssn = $ss->[-1]->name;
            $ssn =~ s/^\S+\s+\S+\s+//;
            $specobj->sub_species( $ssn );
        }
        if (my $s = $rankLookup{species}) {
            my $sn = $s->[-1]->name;
            $sn =~ s/^\S+\s+//;
            $specobj->species( $sn );
        }
        # die $self->branch( -ref => \%rankLookup, -skipkey => [qw(PARENT TRACKER MERGE)],);
        # Get the "highest" genus refered to, if available:
        my $hasGenus = $rankLookup{genus};
        my $refer    = $hasGenus ? $hasGenus->[-1] : $self;
        $specobj->genus( $refer->name ) if ($hasGenus);
        my @parents  = $refer->each_parent;
        my @class    = map { $_->name } ($refer, @parents);
        pop @class if ($class[-1] eq 'root');
        if ($hasGenus && $refer->id != $self->id ){
            # Our node is more specific than the Genus
            my $spec = $rankLookup{subspecies} || 
                $rankLookup{species} || [ $self ];
            # Use the "highest" refered-to rank
            if (my $node = $spec->[-1]) {
                unshift @class, $node->name();
            }
        }
        $specobj->classification( @class );
    }
    return $self->{BIOPERL_SPECIES};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 each_alias

 Title   : each_alias
 Usage   : $obj->each_alias($class)
 Function: Returns a list of all aliases, or only those for a name_class
 Returns : An array of strings
    Args : Optional name_class. If provided, then only aliases for
           that class will be returned. Otherwise, all aliases will be
           returned.

=cut

sub each_alias {
    my $self = shift;
    my ($class) = @_;
    $class ||= 'ALL';
    $class = uc(substr($class, 0, 50));
    my $alihash = $self->each_alias_class;
    my $retval = $alihash->{$class} || [];
    return @{$retval};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 each_alias_class

 Title   : each_alias_class
 Usage   : $obj->each_alias_class
 Function: Returns a hash reference keyed to alias class, with each
           key pointing to an array of aliases associated with that
           class.
 Returns : A hash reference
 Args    : 

=cut

sub each_alias_class {
    my $self = shift;
    unless ($self->{ALIAS}) {
        # We need to query the database to find aliases
        $self->{ALIAS}{ALL} = [];
        my $rows = $self->tracker->dbi->named_sth("Find Taxa aliases")->
            selectall_arrayref( $self->id );
        foreach my $row (@{$rows}) {
            my ($name, $class) = @{$row};
            next unless ($name);
            $class = uc($class || 'UNKNOWN');
            $self->{ALIAS}{$class} ||= [];
            foreach my $cn ($class, 'ALL') {
                push @{$self->{ALIAS}{$cn}}, $name;
            }
        }
        # Sort each collection
        foreach my $class (keys %{$self->{ALIAS}}) {
            $self->{ALIAS}{$class} = 
                [ sort { uc($a) cmp uc($b) } @{$self->{ALIAS}{$class}} ];
        }
    }
    return { %{$self->{ALIAS}} };
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 as_color

 Title   : as_color
 Usage   : my $color_string = $obj->as_color

 Function: Returns a web color string generated by hashing up the
           scientific name.

 Returns : A string of form '#e374e5'
 Args    : 

=cut

sub as_color {
    my $self = shift;
    unless ($self->{AS_COLOR}) {
        my $name    = $self->name;
        my $rounded = "";
        my $digit   = "";
        my $web;
        for (my $i = 0; $i < length($name) && $i < 10; $i++) {
            $digit .= ord(substr($name, $i, 1)) % 10;
        }
        $digit  = $digit % (16 ** 6);
        my $hex = sprintf('%06x', $digit);
        if (0) {
            for (my $i = 0; $i <=2; $i++) {
                my $char = substr($hex, $i * 2, 1);
                # Round to 256 pallete
                $char =~ tr/124578abde/0336699ccf/;
                $rounded .= "$char$char";
            }
            $web = '#' . $rounded;
        } else {
            $web = '#' . $hex;
        }
        # Eliminate darkest values:
        $web =~ tr/012/333/;
        $self->{AS_COLOR} = $web;
        #print "$name = $digit = $hex = $rounded -> $web<br />\n";
    }
    return $self->{AS_COLOR};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 icon

 Title   : icon
 Usage   : my $filename =$obj->icon

 Function: Gets the icon associated with the taxa. Intended to be the
           path to a little image

 Returns : The name of the icon
    Args : Optional new value

=cut

sub icon {
    my $self = shift;
    unless ($self->{ICON}) {
        # We have not yet figured out the icon for this taxa
        my $icon;
        foreach my $rank (@icon_order) {
            foreach my $tid (keys %{$species_icons->{$rank}{match}}) {
                if ($self->is_taxa($tid)) {
                    $icon = $species_icons->{$rank}{match}{$tid} . '.png';
                    last;
                }
            }
            last if ($icon);
        }
        $icon = "species_without_icon.png" unless ($icon);
        $self->{ICON} = $icon;
    }
    my $icon = $self->{ICON};
    my $prefix = $_[0];
    if ($prefix) {
        if ($prefix =~ /^\// || $prefix =~ /^http/) {
            $icon = $prefix .'/'. $icon;
        } elsif ($prefix =~ /url/i) {
            if ($self->tracker) {
                $icon = $self->tracker->file_url('STATIC') . $icon;
            } else {
                return "";
            }
        } else {
            $icon = $self->tracker->file_path('STATIC') . $icon;            
        }
    }
    return $icon;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 img_tag

 Title   : img_tag
 Usage   : my $htmlCode =$obj->img_tag()

 Function: Gets a string of HTML text representing an IMG tag for the icon

 Returns : A string
    Args : 

=cut

sub img_tag {
    my $self = shift;
    return sprintf("<img src='%s' />", $self->icon('url'));
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 as_gd

 Title   : as_gd
 Usage   : my $GD_object = $obj->as_gd()

 Function: Returns a GD graphics object of the icon associated with
           the taxa. Used when overlaying the icon on an existing GD
           object; used to put the icons on Graphviz network
           images.
 Returns : A GD object, or undef if it was not possible to find the icon
    Args : 

=cut

sub as_gd {
    my $self = shift;
    unless ($self->{GD}) {
        my $path = $self->icon('path');
        $self->{GD} = GD::Image->new( $path );
    }
    return $self->{GD};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
1;
