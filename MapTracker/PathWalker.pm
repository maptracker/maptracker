# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::PathWalker;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

$BMS::MapTracker::PathWalker::VERSION = 
    ' $Id$ ';

use strict;
use BMS::Branch;
my $debug = BMS::Branch->new
    ( -skipkey => ['TRACKER', 'CHILDREN', 'CLASSES', 'FP', 'BENCHMARKS',
		   'SEQNAMES', 'AUTHORITIES','TYPES', 'obj', 'READS',
		   'DATA', ],
      -format => 'html', 
      -noredundancy => 1, );
use BMS::CommonCAT;
use BMS::MapTracker::Network;
use BMS::MapTracker;
use Scalar::Util qw(weaken);

=head1 WRITING PATH FILES

 Lines starting with '#' are assumed to be comments and are ignored.

 Blank lines or lines that are just whitespace are ignored.

 The path consists of a series of steps that will be executed
 sequentially. A new step is indicated either by starting the line
 with an integer, or by starting a line with 'step'. For example:

 1 Find all aliases

 ... indicates that a step called '1' should be started, and it has a
 description of 'Find all aliases'. The description is optional, and
 will be shown during a walk if verbose is on.

 STEP Find_Alias Find all aliases

 ... is essentially the same as above, but it will name the step 'FIND_ALIAS'.

 Step names do not need to be unique, and if you are using integers
 they do not need to be in any order.

=head1 SPECIFYING COMMANDS

 Commands are associated with the step that they follow. The command
 name is specified first, without any preceeding whitespace. Most
 command names have options associated with them. Options are named
 parameters with associated values, and are set in the format:

 -optionname => optionvalue

 If the value of the option includes spaces, then the value must be
 enclosed in single quotes. If you have many options, you may continue
 listing them on subsequent lines, so long as at least some whitespace
 is included at the start of the line (lack of leading whitespace will
 be interpreted as a new command).

 If you wish to specify the value of the option as an array, use the
 nomenclature:

 -optionname => list:(value1, value2, value3, ...)

 The values in the list may be separated by any number of spaces
 and/or commas. Hashes are specified in the same fashion, but use the
 format:

 -optionname => hash:(key1, value1, key2, value2, ...)

 NOTE: I think commas inside values will cause problems - need to work
 on this...

 The order in which you specify commands within a step will be
 ignored. Instead, the commands will be executed in a pre-set order
 (see the code for subroutine walk() if you wish to know this
 order). Put commands in their own steps if order of execution is
 important.

 Unrecognized command names will be quietly ignored. For this reason,
 you can 'comment out' a command by appending gibberish (eg 'xxx') to
 the command name.

=head1 PATH FILE COMMANDS

 Command: pick - select out a subset of one or more nodes
 Options:

   -category Default 'DEFAULT'. Sets the category name for the
             picks. Can be any string (do not use 'all', however). All
             category names are case insensitive.

  -hasparent An optional node name. If another node is a child (or
             grandchild of any depth) of this node, then it will be
             picked.

    -isclass Optional class name, or list of such. Any node that is
             that class will be picked.

   -allclass Like -isclass, but unless a node is a member of *all*
              provided classes then it will *not* be picked.

     -regexp Optional regular expression. Any node whose name matches
             this RE will be picked.

   -mustpick If specified, then processing of the current step in the
             path will terminate unless at least one pick is found.


 Command: unpick - remove nodes from a pick category
 Options: Has the same options as -pick, except that nodes will be
          removed from categories if any of the tests are true. The
          option -mustpick is not used.


 Command: expand - expand the network from one or more root nodes
 Options: All options recognized by MapTracker::Network::expand(). In
          addition, the following special options may be used:

     -useall If specified, then every node in the network will be used
             as a root for the expansion. Alternatively, specify
             -usepick => all

    -usepick Use this to specify the root(s) to be used by expand. If
             specified as 'all', then every pick in every category
             will be used. If any other single value or list, then
             only those pick categories will be used.

             If no explicit pick requests are specified, then all the
             roots of the network (as set by add_root()) will be used.

    -addpick Optional pick category name. If specified, then all nodes
             encountered in the network expansion will be added to
             that category. Note that the nodes *encountered* may not
             be *new* nodes - you may need to call -unpick to take out
             unwanted nodes.


 Command: findpath - find one or more paths between two nodes
 Options: All options recognized by Network::find_path(). In addition,
          the following modifications are present

      -start 

        -end Normally, these parameters can be set to a specific
             node. When building networks, the user usually does not a
             priori know the exact nodes in the net. So in PathWalker
             this parameter is instead used to set a pick
             category. Paths between all combination of start and end
             will be generated.

             If not set, -start will default to the first root set in
             the network, and -end will default to all picks in
             category 'DEFAULT'.

   -savebest Save only the best paths found - will call
             BMS::Network::NetworkPath::save_best(). The value passed
             will be used to set -cache. This option should only be
             used after you have stopped expanding the network. It
             should improve performance when multiple start and end
             nodes are possible.




 Command: tabpath - save found paths to a tab-delineated file
 Options:

       -file Default 'PathWalker.tab'. The file path to save data to.


 Command: findroot - identify the 'primary' root of a directed tree
 Options: All options recognized by Network::find_tree_root(). In
          addition, the following modifications are present

      -reset A network can have more than one roots, but the first
             root added is treated specially, particularly when
             rendering the network graphically. Calling 'reset' will
             cause all previously defined roots to be kept, but the
             new primary root discovered by findroot will now be set
             as the first root.


 Command: saveimage - save network as a Graphviz PNG file
 Options: All options recognized by Network::to_graphviz_html(). In
          addition, the following modifications are present

        -dir Defaults to '/stf/biohtml/tmp/MapTracker'

   -filename Defaults to "PathWalker$$"


 Command: clearpicks - delete all previously selected picks


 Command: clearcache - delete all saved paths


 Command: showpicks - prints a list of each pick, by category


 Command: showpath - print out all the paths as text


=cut


=head1 PRIMARY METHODS
#-#-#-#-#--#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-

=head2 new

 Title   : new
 Usage   : my $obj = BMS::MapTracker::PathWalker->new(@arguments)
 Function: Creates a new object and returns a blessed reference to it.
 Returns : A blessed BMS::MapTracker::PathWalker object
 Args    : Associative array of arguments. Recognized keys [Default]:

  -tracker Required. A BMS::MapTracker object

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = { 
	TRACKER     => undef,
	NETWORK     => undef,
	FILES       => {},
	PATHCACHE   => [],
        LABELS      => {},
        RETURN_TO   => [],
        VISIT_COUNT => {},
        PARAMS      => {},
        DEFAULTS    => {},
        SOURCEFILE  => "",
    };
    bless ($self, $class);
    my $args = $self->parseparams( -tracker => undef,
				   -root    => undef,
				   -verbose => 0,
				   @_ );
    $self->tracker( $args->{TRACKER} );
    $self->clear_network;
    $self->{USER_ID} = $self->tracker->user->id;
    $self->{FRM}     = $ENV{'HTTP_HOST'} ? 
        '<pre><font color=\'orange\'>%s</font></pre>' : '%s';
    $self->{FRM} .= "\n";

    foreach my $key ('VERBOSE') {
	$self->{$key} = $args->{$key};
    }
    return $self;
}

sub param {
    my $self = shift;
    my $tag = uc($_[0]);
    if (defined $_[1]) {
        $self->{PARAM}{$tag} = $_[1];
        $self->msg("Parameter $tag set to '$_[1]'")
            if ($self->{VERBOSE});
    }
    my $val = $self->{PARAM}{$tag};
    $val = $self->{DEFAULTS}{$tag} unless (defined $val);
    return $val;
}

sub msg {
    my $self = shift;
    my ($txt) = @_;
    # Now using $SIG{__WARN__} to format warnings
    warn "$txt\n" if ($txt);
    # warn sprintf($self->{FRM}, $txt);
}

sub process_args {
    my $self = shift;
    my $args = $_[0] || {};
    # Make local copy of stored arguments:
    my %exargs = %{$args};
    # Substitute parameters
    while (my ($key, $data) = each %exargs) {
        next if (!$data || ref($data));
        my $iloop = 0;
        while ($data =~ /(.?)PARAM\:([A-Z0-9_]+)(.?)/i) {
            my ($l, $tag, $r) = ($1, $2, $3);
            my $val = $self->param($tag);
            if ($l || $r) {
                # The substitution is into a string
                $val = '-UNDEF-' unless (defined $val);
                $data =~ s/PARAM\:$tag/$val/i;
            } else {
                # The substitution will entirely replace the parameter
                $data = $val;
                last;
            }
            if (++$iloop > 500) {
                $self->error("Infinite loop parsing parameter $tag");
                last;
            }
        }
        $exargs{$key} = $data;
    }
    # $debug->branch(\%exargs);
    return %exargs;
}

sub show_html_options {
    my $self = shift;
    my $nw   = $self->network;
    my @picks;
    foreach my $cat ($self->each_pick_category) {
        push @picks, [ "Categeory $cat", [ $self->each_pick($cat) ] ];
    }
    return $nw->show_html_options( -picks => \@picks );
}

sub tracker {
    my $self = shift;
    if ($_[0]) {
	$self->death("'$_[0]' is not a MapTracker object!")
	    unless (ref($_[0]) && $_[0]->isa("BMS::MapTracker"));
        # Prevent circular references by weakening link to tracker object
	weaken($self->{TRACKER} = $_[0]);
    } elsif (!$self->{TRACKER}) {
        # Need to make a new tracker object, which should NOT be weakend
        $self->{TRACKER} = BMS::MapTracker->new;
    }
    return $self->{TRACKER};
}

sub network {
    my $self = shift;
    if ($_[0]) {
	$self->death("'$_[0]' is not a Network object!")
	    unless (ref($_[0]) && $_[0]->isa("BMS::MapTracker::Network"));
	$self->{NETWORK} = $_[0];
    }
    return $self->{NETWORK};
}

sub clear_network {
    my $self = shift;
    my $old  = $self->network;
    if ($self->{VERBOSE}) {
	if ($old) {
            $self->msg("Clearing network");
	} else {
	    $self->msg("Initializing network");
	}
    }
    my $new = BMS::MapTracker::Network->new( -tracker => $self->tracker );
    $self->network( $new );
    $self->clear_picks();
    $self->clear_associations();
    return $old;
}

sub _find_path {
    my $self = shift;
    my ($file) = @_;
    my @to_check = ( $file );
    # Include a variant that has '.path' suffixed to it:
    push @to_check, "$file.path" unless ($file =~ /\.path$/);

    # Include the directory that the source file is from:
    if ($self->{SOURCEFILE} =~ /^(.+)\/[^\/]+$/) {
        my $srcdir = $1;
        my @newc;
        foreach my $tc (@to_check) {
            # Do not bother if the variant is already an absolute path
            next if ($tc =~ /^\//);
            push @newc, "$srcdir/$tc";
        }
        push @to_check, @newc;
    }

    # Include variations using the default path directory:
    if (my $path_dir = $self->param('path_dir')) {
        my @newc;
        foreach my $tc (@to_check) {
            # Do not bother if the variant is already an absolute path
            next if ($tc =~ /^\//);
            push @newc, "$path_dir/$tc";
        }
        push @to_check, @newc;
    }

    my $found;
    # Check all name variants
    foreach my $check (@to_check) {
        if (-e $check) {
            $found = $check;
            last;
        }
    }
    if ($found) {
        # Safety checks
        if ($found =~ /\.\./) {
            $self->death("Loading relative paths is disallowed");
        }
        
        if ($found =~ /^\/(apps|bin|etc|lib|lib64|logs|lost\+found|mnt|opt|proc|root|scratch|sbin|selinux|sys|usr|var)/) {
            $self->death("Can not load file from protected directory");
        }
    }

    return $found;
}

sub load_path {
    my $self = shift;
    my ($file) = @_;
    my $found = $self->_find_path($file);
    $self->death("Could not locate path file '$file':\n$!\n ")
        unless ($found);
    my $string = "";
    open(FILE, "<$found") || 
	$self->death("Could not read path file '$found':\n$!\n ");
    while (<FILE>) {
	$string .= $_;
    }
    close FILE;
    $self->{SOURCEFILE} = $found;
    return $self->path( $string );
}

sub path {
    my $self = shift;
    my ($pathstring) = @_;
    return $self->{PATH} unless ($pathstring);
    $pathstring =~ s/\s*[\n\r]/\n/g; # Kill trailing spaces
    $pathstring =~ s/[\n\r]\s+/ /g;  # Leading spaces = continuation of line
    $pathstring =~ s/\t/ /g;         # Tabs are saved for lists
    my @path; my @comments;
    foreach my $step (split(/[\n\r]+/, $pathstring)) {
	$step =~ s/^\s+//; $step =~ s/\s+$//;
	next if ($step =~ /^\s*$/); # Blank line
	if ($step =~ /^\#(.*)$/) {
	    # Comment
	    push @{$path[-1]{coms}}, $1 || "" if ($#path > -1);
	    next;
	}
	if ($step =~ /^(\d+)\s*(.*)$/ ||
            $step =~ /^STEP\s+(\S+)\s*(.*)$/i) {
	    # Defines a new step in the path
            my ($lab, $desc) = (uc($1), $2);
	    push @path, {
		label => $lab,
		desc  => $desc,
		args  => [],
		coms  => [],
	    };
            $self->{LABELS}{$lab} ||= [];
            push @{$self->{LABELS}{$lab}}, $#path;
	    next;
	}
        
        if ($#path < 0) {
            $self->error("Command '$step' has occured before a STEP defined");
            next;
        }
        
	if ($step =~ /^(\S+)\s+(.+)$/) {
            my $command = uc($1);
	    push @{$path[-1]{args}}, [ $command, $self->parse_args( $2 ) ];
	} elsif ($step =~ /^(\S+)\s*$/) {
	    # Simple command with no arguments
            my $command = uc($1);
	    push @{$path[-1]{args}}, [ $command, {} ];
	}
    }
    return $self->{PATH} if ($#path < 0);


    $self->{PATH} = \@path;
    $self->msg(sprintf("Path set with %d steps", $#path + 1))
        if ($self->{VERBOSE});
    $self->_check_path;
    return $self->{PATH};
}

sub _check_path {
    my $self = shift;
    my $path = $self->path;
    my $key_aliases = {
        'CLEAR_PICKS'       => 'CLEARPICKS',
        'SHOWPICK'          => 'SHOWPICKS',
        'ADD_ROOT'          => 'ADDROOT',
        'REMOVE_ROOT'       => 'REMOVEROOT',
        'KILL_ROOT'         => 'REMOVEROOT',
        'KILLROOT'          => 'REMOVEROOT',
        'EXPLICIT'          => 'EXPAND_EXPLICIT',
        'EXPLICIT_EXPAND'   => 'EXPAND_EXPLICIT',
        'EXPLICITLY_EXPAND' => 'EXPAND_EXPLICIT',
        'EXPAND_INTERNAL'   => 'CONNECT',
        'EXPANDINTERNAL'    => 'CONNECT',
        'CONNECTINTERNAL'   => 'CONNECT',
        'CONNECT_INTERNAL'  => 'CONNECT',
        'SET_PARAM'         => 'PARAM',
        'SETPARAM'          => 'PARAM',
        'SET_NET_PARAM'     => 'NETPARAM',
        'SETNETPARAM'       => 'NETPARAM',
        'USEPATH'           => 'WALK',
        'PROXY_EDGE'        => 'PROXY_BY_EDGE',
    };
    foreach my $step ( @{$path} ) {
        my $command_list = $step->{args};
        # Translate aliases
        foreach my $data (@{$command_list}) {
            my ($command, $args) = @{$data};
            if (my $realkey = $key_aliases->{$command}) {
                $data->[0] = $realkey;
            }
        }

        my @keepers;
        foreach my $data (@{$command_list}) {
            my ($command, $args) = @{$data};
            # Do not bother cleaning up steps after a SKIP
            if ($command eq 'SKIP') {
                push @keepers, [ $command, $args ];
                last;
            }
            my $test  = lc('_check_'.$command);
            unless ($self->can($test)) {
                # No test for this argument
                push @keepers, [ $command, $args ];
                next;
            }
            # Test each argument set (substep)
            my $failed;
            eval('$failed = $self->'.$test.'($args)');
            if ($failed) {
                my $msg = sprintf
                    ("Step %s has syntax errors in %s command: %s",
                     $step->{label}, $command, $failed);
                $self->error($msg);
            } else {
                push @keepers, [ $command, $args ];
            }
        }
        $step->{args} = \@keepers;
    }
}

# *_check_endjump = \&_check_jump;

sub _check_goto {
    my $self = shift;
    my ($args)   = @_;
    my @targets  = keys %{$args};
    if ($#targets < 0) {
        return "no target steps defined";
    } elsif ($#targets > 0) {
        return "More than one target step defined";
    }

    delete $args->{$targets[0]};
    my $target    = uc($targets[0]);
    return 0 if ($target eq 'RETURN');
    my $targarray = $self->{LABELS}{$target};
    if (!$targarray) {
        return "step '$target' is not defined";
    } elsif ($#{$targarray} > 0) {
        return "step '$target' is defined multiple times";
    }
    $args->{T_INDEX} = $targarray->[0];
    return 0;
}

sub _check_param {
    my $self = shift;
    my ($args)   = @_;
    return "You have not specified the parameter name with -tag"
        unless ($args->{TAG});
    $args->{VALUE} = $args->{VAL} unless (defined $args->{VALUE});
    if (defined ($args->{DEFAULT})) {
        my $tag = uc($args->{TAG});
        $self->{DEFAULTS}{$tag} = $args->{DEFAULT};
    } else {
        return "You have not specified the parameter value with -value"
            unless (defined $args->{VALUE});
    }
    $self->param($args->{TAG}, $args->{VALUE});
    return 0;
}

sub _check_netparam {
    my $self = shift;
    my ($args)   = @_;
    return "You have not specified the parameter name with -tag"
        unless ($args->{TAG});
    $args->{VALUE} = $args->{VAL} unless (defined $args->{VALUE});
    if (defined ($args->{DEFAULT})) {
        my $tag = uc($args->{TAG});
        $self->{DEFAULTS}{$tag} = $args->{DEFAULT};
    } else {
        return "You have not specified the parameter value with -value"
            unless (defined $args->{VALUE});
    }
    return 0;
}
sub _check_walk {
    my $self = shift;
    my ($args)   = @_;
    my $filepath = $args->{PATH};
    return "You have not specified the PathWalker file with -path"
        unless ($filepath);

    my $found = $self->_find_path( $filepath );
    return "Unable to locate PathWalker file '$filepath'"
        unless ($found);

    $self->msg("Utilizing external path '$found'") if ($self->{VERBOSE});
    my $pw = BMS::MapTracker::PathWalker->new
        ( -tracker => $self->tracker,
          -verbose => $self->{VERBOSE} );
    $pw->load_path( $found );
    $args->{PATH} = $pw;
    my $mode;
    if ($args->{MODE} =~ /integrate/i) {
        $mode = 'INTEGRATE';
    } elsif ($args->{MODE} =~ /bulk/i) {
        $mode = 'BULK';
    } elsif ($args->{MODE} =~ /single/i) {
        $mode = 'SINGLE';
    }
    return "You have not specified an allowed -mode (integrate, bulk, single)"
        unless ($mode);

    if (my $mp = $args->{MAPPICKS} || $args->{MAPPICK}) {
        if (uc($mp) eq 'ALL') {
            $mp = 'ALL';
        } elsif (ref($mp) ne 'HASH') {
            return "-mappicks should be passed as a hash";
        }
        $args->{MAPPICK} = $mp;
    }

    if (my $ma = $args->{MAPASSOC} || $args->{MAPASSOCIATION}) {
        if (uc($ma) eq 'ALL') {
            $ma = 'ALL';
        } elsif (ref($ma) ne 'HASH') {
            return "-mapassoc should be passed as a hash";
        }
        $args->{MAPASSOC} = $ma;
    }
    
    $args->{MODE} = $mode;
    return 0;
}

sub _check_format {
    my $self = shift;
    my ($args)   = @_;
    return "You have not specified the format parameter name with -param"
        unless ($args->{PARAM});
    $args->{VALUE} = $args->{VAL} unless (defined $args->{VALUE});
    $args->{NODE} = $args->{NODES} unless (defined $args->{NODE});
    return "You have not specified the format value with -value"
        unless (defined $args->{VALUE});
    return 0;
}

sub _check_pick {
    my $self = shift;
    my ($args)   = @_;
    my $mt       = $self->tracker;
    if (my $ic = $args->{ISCLASS}) {
        my @expect = ref($ic) eq 'ARRAY' ? @{$ic} : ( $ic );
        my @check  = $mt->param_to_list( $ic, 'class' );
        my $delta  = $#expect - $#check;
        if ($delta) {
            return sprintf
                ("The -isclass request for '%s' failed to find %d class%s - ".
                 "check your spelling", join(', ', @expect), $delta, 
                 $delta == 1 ? '' : 'es');
        }
    }
    if (my $nc = $args->{NOTCLASS}) {
        my @expect = ref($nc) eq 'ARRAY' ? @{$nc} : ( $nc );
        my @check  = $mt->param_to_list( $nc, 'class' );
        my $delta  = $#expect - $#check;
        if ($delta) {
            return sprintf
                ("The -notclass request for '%s' failed to find %d class%s - ".
                 "check your spelling", join(', ', @expect), $delta, 
                 $delta == 1 ? '' : 'es');
        }
    }
}

sub _check_jump {
    my $self = shift;
    my ($args)   = @_;
    my $mt       = $self->tracker;
    return "No -test deffinition provided" unless ($args->{TEST});
    my $test = $args->{TEST} = lc( $args->{TEST} );

    unless ($self->can( '_test_' . $test )) {
        return "No such -test '$test'";
    }
    
    if ($test eq 'none') {
        $args->{TRUEVAL}  = $args->{ONTRUE};
        unless (defined $args->{ONTRUE}) {
            return "-test NONE must also define -ontrue";
        }
    }

    if ($test =~ /class/) {
        # Need to convert passed arguments into classes
        my @true     = $mt->param_to_list( $args->{TRUEVAL},  'class');
        $args->{TRUEVAL}  = $#true > -1  ? \@true : undef;
        my @false    = $mt->param_to_list( $args->{FALSEVAL}, 'class');
        $args->{FALSEVAL} = $#false > -1 ? \@false : undef;
        $args->{ALLCLASS} = defined $args->{ALLCLASS}? $args->{ALLCLASS} : 1;
    }

    if ($test =~ /count/) {
        # Need to parse counting arguments
        if (defined $args->{TRUEVAL}) {
            my $val = $self->_check_count_argument( $args->{TRUEVAL} );
            return "-count syntax error: $val" unless (ref($val) );
            $args->{TRUEVAL} = $val;
        }
        if (defined $args->{FALSEVAL}) {
            my $val = $self->_check_count_argument( $args->{FALSEVAL} );
            return "-count syntax error: $val" unless (ref($val) );
            $args->{FALSEVAL} = $val;
        }
    }

    if ($test =~ /pick/) {
        $args->{ALLPICK} = defined $args->{ALLPICK} ? $args->{ALLPICK}:1;
    }

    if (defined $args->{TRUEVAL}) {
        if (!defined $args->{ONTRUE}) {
            return "You've defined -trueval, but not -ontrue (where to go)";
        }
        my $target    = uc($args->{ONTRUE});
        if ($target eq 'RETURN') {
            $args->{T_INDEX} = $target;
        } else {
            my $targarray = $self->{LABELS}{$target};
            if (!$targarray) {
                return "-ontrue target step '$target' is not defined";
            } elsif ($#{$targarray} > 0) {
                return "-ontrue target step '$target' defined multiple times";
            }
            $args->{T_INDEX} = $targarray->[0];
        }
    }

    if (defined $args->{FALSEVAL}) {
        if (!defined $args->{ONFALSE}) {
            return "You've defined -falseval, but not -onfalse (where to go)";
        }
        my $target    = uc($args->{ONFALSE});
        if ($target eq 'RETURN') {
            $args->{F_INDEX} = $target;
        } else {
            my $targarray = $self->{LABELS}{$target};
            if (!$targarray) {
                return "-onfalse target step '$target' is not defined";
            } elsif ($#{$targarray} > 0) {
                return "-onfalse target step '$target' defined multiple times";
            }
            $args->{F_INDEX} = $targarray->[0];
        }
    }

    unless (defined $args->{FALSEVAL} || defined $args->{TRUEVAL}) {
        return "You need to define either -truval or -falseval";
    }

    if (!defined $args->{TRUEVAL} && defined $args->{ONTRUE}) {
        return "You have defined -ontrue, but not -trueval (the test value)";
    }
    if (!defined $args->{FALSEVAL} && defined $args->{ONFALSE}) {
        return "You have defined -onfalse, but not -falseval (the test value)";
    }

    return 0;
}

sub _check_count_argument {
    my $self   = shift;
    my $text   = uc($_[0]);
    $text =~ s/\s+//g; # remove all whitespace
    my $num;
    if ($text =~ /([\+\-]?\d+\.?\d*)/ || $text =~ /([\+\-]?\d*\.?\d+)/) {
        $num = $1;
        $text =~ s/\Q$num\E//;
        # warn "$num -> $text\n";
    } else {
        return "could not find a number argument in '$text'";
    }
    my $operation;
    if ($text eq '' || $text eq 'EQ' || $text =~ /^EQUALS?$/) {
        $operation = 'EQ';
    } elsif ($text eq '>' || $text eq 'GT' || $text eq 'GREATERTHAN') {
        $operation = 'GT';
    } elsif ($text eq '<' || $text eq 'LT' || $text eq 'LESSTHAN') {
        $operation = 'LT';
    } else {
        return "could not determine operation from '$text'";
    }
    return [$num, $operation];
}

sub _check_message {
    my $self = shift;
    my ($args)   = @_;
    return "No -text specified" unless ($args->{TEXT});
    return 0;
}

sub _check_addroot {
    my $self = shift;
    my ($args)   = @_;
    return "No -source specified" unless ($args->{SOURCE});
    return 0;
}

sub walk {
    my $self = shift;
    my $args = $self->parseparams(  @_ );
    my $path = $self->path;
    my $nw   = $self->network;
    $self->msg("Starting walk:") if ($self->{VERBOSE});
    $self->{VISIT_COUNT} = {};
  STEPLOOP: for (my $si = 0; $si <= $#{$path}; $si++) {
      $self->{VISIT_COUNT}{ $si }++;
      my $step = $path->[$si];
      my $slab = $step->{label};
      if ($self->{VISIT_COUNT}{$si} > 100) {
          $self->death("Probable infinite loop passing through step $slab");
      }

      $self->msg(sprintf("  Step %s : %s", $slab, $step->{desc}))
          if ($self->{VERBOSE});
    COMMANDLOOP: for (my $di = 0; $di <= $#{$step->{args}}; $di++) {
        my ($command, $comargs) = @{$step->{args}[$di]};
        my ($retval, $modifier);
        if ($command eq 'JUMP') {
            $retval   = $self->_do_jump( $comargs );
            $modifier = "test '" . ($comargs->{TEST} || '-UNDEF') . "'";
        } elsif ($command eq 'EXPAND') {
            $retval = $self->_do_expand( $comargs );
        } elsif ($command eq 'WALK') {
            $retval = $self->_do_walk( $comargs );
        } elsif ($command eq 'EXPAND_EXPLICIT') {
            $retval = $self->_do_expand_explicit( $comargs );
        } elsif ($command eq 'PARAM') {
            $retval = $self->_do_param( $comargs );
        } elsif ($command eq 'NETPARAM') {
            $retval = $self->_do_netparam( $comargs );
        } elsif ($command eq 'FORMAT') {
            $retval = $self->_do_format( $comargs );
        } elsif ($command eq 'CONNECT') {
            $retval = $self->_do_connect( $comargs );
        } elsif ($command eq 'TELESCOPE') {
            $retval = $self->_do_telescope( $comargs );
        } elsif ($command eq 'FINDROOT') {
            $retval = $self->_do_find_root( $comargs );
        } elsif ($command eq 'ADDROOT') {
            $retval = $self->_do_add_root( $comargs );
        } elsif ($command eq 'REMOVEROOT') {
            $retval = $self->_do_remove_root( $comargs );
        } elsif ($command eq 'CLEARPICKS') {
            $retval = $self->clear_picks( $comargs );
        } elsif ($command eq 'PICK') {
            $retval = $self->_do_addpick( $comargs );
        } elsif ($command eq 'UNPICK') {
            $retval = $self->_do_unpick(  $comargs );
        } elsif ($command eq 'SHOWPICKS') {
            $retval = $self->_do_showpick( $comargs );
        } elsif ($command eq 'FILTER_EDGES') {
            $retval = $self->_do_filter_edges( $comargs );
        } elsif ($command eq 'FINDPATH') {
            $retval = $self->_do_findpath( $comargs );
        } elsif ($command eq 'SHOWPATH') {
            $retval = $self->_do_showpath( $comargs );
        } elsif ($command eq 'TABPATH') {
            $retval = $self->_do_tabpath( $comargs );
        } elsif ($command eq 'CLEARCACHE') {
            $retval = $self->clear_cache( $comargs );
        } elsif ($command eq 'SAVEIMAGE') {
            $retval = $self->_do_saveimage( $comargs );
        } elsif ($command eq 'PROXY_BY_EDGE') {
            $retval = $self->_do_proxy_by_edge( $comargs );
        } elsif ($command eq 'RETURN') {
            $retval = 'RETURN';
        } elsif ($command eq 'SKIP') {
            $retval = 'SKIP';
        } elsif ($command eq 'GOTO') {
            $retval   = 'JUMP ' . $comargs->{T_INDEX};
            $modifier = 'GOTO';
        } elsif ($command eq 'MESSAGE') {
            $retval = $self->_do_message( $comargs );
        } elsif ($command eq '') {

        } elsif ($command eq '') {
            
        } elsif ($self->{VERBOSE}) {
            $self->msg( "    Unknown command '$command'");
        }

        if (defined $retval) {
            # print "<font color='brick'>$slab rv='$retval'</font><br />\n";
            if ($retval =~ /^JUMP (\d+)/) {
                # A decision has been made to jump to another step index
                push @{$self->{RETURN_TO}}, [ $si, $di ];
                $si   = $1;
                if ($self->{VERBOSE}) {
                    my $label = $path->[$si]{label};
                    $self->msg("    JUMP: $modifier redirects to step $label");
                }
                # Must back the index off by 1, since next will increment
                $si--;
                next STEPLOOP;  # Jump!
            } elsif ($retval eq 'SKIP') {
                # Stop processing of this step
                if ($self->{VERBOSE}) {
                    my $remain = $#{$step->{args}} - $di;
                    $self->msg(sprintf("    %s command%s in this step skipped",
                                 $remain, $remain == 1 ? '' : 's'));
                }
                next STEPLOOP;
            } elsif ($retval eq 'ABORT') {
                # Stop all processing
                $self->msg("    Walk aborted.") if ($self->{VERBOSE});
                last STEPLOOP;
            } elsif ($retval eq 'RETURN' || $retval eq 'JUMP RETURN') {
                # Request to return to the place we last jumped from
                # - in reality, this means go to the step *after*
                # the one we last jumped # from.
                my $lastjump = pop @{$self->{RETURN_TO}};
                if (defined $lastjump) {
                    # Reset the index counters to the last jump
                    ($si, $di) = @{$lastjump};
                    # Also need to reset the variable $step
                    $step = $path->[$si];
                    if ($self->{VERBOSE}) {
                       
                        my $label = $self->path->[ $si ]{label};
                        $self->msg("    RETURN: Resuming execution after $label ".
                            "command #".($di+1));
                    }
                    next;
                }
            }
        }

    } # COMMANDLOOP
  } # STEPLOOP
}

sub _do_jump {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    my $dojump;
    eval('$dojump = $self->_test_'.$exargs{TEST}.'(\%exargs)');
    #print "<pre>";$debug->branch($dojump); print "</pre>";
    if (defined $dojump) {
        # A decision has been made to jump to another step
        if ($#{$dojump} > 0) {
            # More than one step possible! Should only occur with both
            # -ontrue and -onfalse defined
            $self->msg("    JUMP conditions point to two different steps: ".
                 join(", ", $exargs{ONTRUE}, $exargs{ONFALSE}). 
                 " - no jump executed") if ($self->{VERBOSE});
        } elsif ($#{$dojump} == 0) {
            # Single unique step defined - return it
            my $si = $dojump->[0];
            return "JUMP $si";
        }
    }
    # No jumps triggered - return undef
    return undef;
}

sub _test_none {
    my $self     = shift;
    my %exargs = $self->process_args( @_ );
    return [ $exargs{T_INDEX} ];
}

sub _test_boolean {
    my $self     = shift;
    my %exargs = $self->process_args( @_ );
    my %results  = 
        ( true  => { $exargs{TRUEVAL}  ? 1 : 0 => 1 }, 
          false => { $exargs{FALSEVAL} ? 0 : 1 => 1 } );
    return $self->_return_test_results(\%exargs, \%results );
}

sub _test_pick_count {
    my $self     = shift;
    my %exargs = $self->process_args( @_ );
    my $cat      = $exargs{CATEGORY};
    my @seqs     = $self->each_pick( $cat );
    my %results  = ( true => {}, false => {} );
    my $count    = $#seqs + 1;
    my %tests    = ( 'true'  => $exargs{TRUEVAL},
                     'false' => $exargs{FALSEVAL}, );
    foreach my $type ('true', 'false') {
        my $param = $tests{$type};
        next unless ($param);
        my ($val, $comp) = @{$param};
        my $bool;
        if ($comp eq 'GT') {
            $bool = ($count > $val);
        } elsif ($comp eq 'LT') {
            $bool = ($count < $val);
        } elsif ($comp eq 'EQ') {
            $bool = ($count == $val);
        }
        $bool ||= 0;
        $results{ $type }{ $bool } = $count;
        # print "$type that $count $comp $val? ".($bool?'Yes':'No')."!<br />";
    }
    return $self->_return_test_results(\%exargs, \%results, \@seqs);
}

sub _test_pick_class {
    my $self     = shift;
    my $nw       = $self->network;
    my %exargs   = $self->process_args( @_ );
    my $cat      = $exargs{CATEGORY};
    my @seqs     = $self->each_pick( $cat );
    my @true     = $exargs{TRUEVAL}  ? @{$exargs{TRUEVAL}}  : ();
    my @false    = $exargs{FALSEVAL} ? @{$exargs{FALSEVAL}} : ();
    my $allclass = $exargs{ALLCLASS};
    my %results  = ( 'true' => {}, 'false' => {} );
    # Fast load sequence data for the nodes to be tested:
    $nw->_bulk_class_load( map { $_->id } @seqs );
    for my $si (0..$#seqs) {
        my $seq = $seqs[$si];
        my $istrue  = undef;
        my $isfalse = undef;
        if ($allclass) {
            # Tests are only valid if all provided classes match
            foreach my $trueclass (@true) {
                unless ($seq->is_class( $trueclass)) {
                    $istrue = 0;
                    last;
                }
                $istrue = 1;
            }
            foreach my $falseclass (@false) {
                if ($seq->is_class( $falseclass)) {
                    $isfalse = 0;
                    last;
                }
                $isfalse = 1;
            }
        } else {
            # Tests are valid if *any* of the provided classes match
            $istrue  = $seq->is_class( @true )  if ($#true > -1);
            $isfalse = !$seq->is_class( @false ) if ($#false > -1);
        }
        $results{ 'true'  }{ $istrue }++  if (defined $istrue);
        $results{ 'false' }{ $isfalse }++ if (defined $isfalse);
    }
    return $self->_return_test_results(\%exargs, \%results, \@seqs);
}

sub _return_test_results {
    my $self     = shift;
    my ($args, $results, $seqs)   = @_;
    my $truejmp  = $args->{T_INDEX};
    my $falsejmp = $args->{F_INDEX};
    
    my ($t1, $t0, $f1, $f0) = 
        ( $results->{'true'}{1} , $results->{'true'}{0},
          $results->{'false'}{1}, $results->{'false'}{0},);

    if ($seqs && $args->{ALLPICK}) {
        # All picks must have agreed
        my $num_roots = $#{$seqs}  + 1;
        if (defined $truejmp && defined $t1) {
            return undef unless ($t1 == $num_roots);
        }
        if (defined $falsejmp && defined $f1) {
            return undef unless ($f1 == $num_roots);
        }
    }

    my %jumpto;
    $jumpto{ $truejmp }  = 1 if ( defined $truejmp  && defined $t1 );
    $jumpto{ $falsejmp } = 1 if ( defined $falsejmp && defined $f1 );
    # print "<pre>($t1, $t0, $f1, $f0)\n";$debug->branch(-refs=>[$results,$args]); print "</pre>";
    return [ keys %jumpto ];
}

sub _do_expand {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    my @nodes  = $self->_choose_nodes( \%exargs );
    return undef if ($#nodes < 0);

    my $nw     = $self->network;
    my $assoc  = $exargs{ASSOCIATE};

    $self->_message_plus_nodes("Expanding", \@nodes) if ($self->{VERBOSE});

    my $waitdat = {
        quiet  => 0,
        start  => time,
        intv   => 10,
        prefix => ($ENV{'HTTP_HOST'} ? "<pre>" : ""),
        suffix => ($ENV{'HTTP_HOST'} ? "</pre>" : ""),
        header => "Expanding network:\n",
        total  => $#nodes + 1,
        unit   => 'node',
    };
    my @found;
    
    if ($assoc) {
        for my $i (0..$#nodes) {
            my $sid = $nodes[$i];
            my @iteration = $nw->expand( %exargs, -node => $sid );
            if ($#iteration > -1) {
                push @found, @iteration;
                $self->add_association( $assoc, $sid, \@iteration);
            }
            $nw->show_progress($waitdat, $i + 1);
        }
    } else {
        # Expand all at once
        push @found, $nw->expand( %exargs, -node => \@nodes );
    }
    $nw->show_progress($waitdat, 0);
    $self->_add_nodes( \%exargs, \@found );
    return undef;
}

sub _message_plus_nodes {
    my $self = shift;
    my ($msg, $nodes) = @_;
    if ($#{$nodes} < 0) {
        $self->msg("  $msg [Nothing]");
        return;
    }
    my @sorted = sort { uc($a) cmp uc($b) } map { $_->name } @{$nodes};
    $msg .= sprintf (" [%d node%s ", $#sorted +1,$#sorted == 0 ? '' : 's');
    # The first node:
    my @list = ( $sorted[0] );
    # A node in the middle
    push @list, $sorted[int($#sorted / 2)] if ($#sorted > 1);
    # The last node
    push @list, $sorted[-1] if ($#sorted > 0);
    @list = map  { substr($_,0,20) } @list;
    if ($#list > 1) {
        $msg .= join(" || ", @list);
    } else {
        $msg .= join(" && ", @list);
    }
    $msg .= "]";
    $self->msg("  $msg");
}

sub _do_proxy_by_edge {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    my @nodes  = $self->_choose_nodes( \%exargs );
    return undef if ($#nodes < 0);
    my $nw     = $self->network;
    $nw->proxy_by_edge( %exargs, -node => \@nodes );
    return undef;
}

sub _do_walk {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    my @nodes  = $self->_choose_nodes( \%exargs );
    return undef if ($#nodes < 0);

    my $pw     = $exargs{PATH};
    my $mode   = $exargs{MODE};
    $pw->clear_network;
    if ($self->{VERBOSE}) {
        $self->msg("  Executing PathWalker file");
    }
    if ($mode eq 'BULK') {
        # Seperate network, but expand all nodes in bulk
        $pw->add_root( @nodes );
        $pw->walk;
    } elsif ($mode eq 'INTEGRATE') {
        # Expand using the existing network
        my $nw = $self->network;
        $nw->remember_roots;
        $nw->clear_roots;
        $nw->add_root( @nodes );
        $pw->network( $nw );
        $pw->walk;
        $nw->recall_roots;
    } elsif ($mode eq 'SINGLE') {
        # Expand as isolated networks, one at a time
        foreach my $sid (@nodes) {
            $pw->clear_network;
            $pw->add_root( $sid );
            $pw->walk;
            $self->copy_picks($pw, $exargs{MAPPICK}) if ($exargs{MAPPICK});
            $self->copy_associations($pw, $exargs{MAPASSOC}) 
                if ($exargs{MAPASSOC});
        }
    }

    unless ($mode eq 'SINGLE') {
        # Copy information over in bulk
        $self->copy_picks($pw, $exargs{MAPPICK}) if ($exargs{MAPPICK});
        $self->copy_associations($pw, $exargs{MAPASSOC}) 
            if ($exargs{MAPASSOC});
    }
    return undef;
}

sub copy_associations {
    # Copy associations from another network
    my $self = shift;
    my ($other_pw, $keymap) = @_;
    if (!$keymap || $keymap eq 'ALL') {
        # The user wants to copy all associations verbatim
        $keymap = { map {$_ => $_} $other_pw->each_association_category };
    }

    # Keymap relates a list of source names in $other_pw to
    # destination names in $self

    while (my ($source, $dest) = each %{$keymap}) {
        foreach my $adat ($other_pw->each_association($source)) {
            my ($name, $parents) = @{$adat};
            foreach my $pdat (@{$parents}) {
                my ($parent, $kids) = @{$parents};
                $self->add_association($dest, $parent, $kids);
            }
        }
    }
}

sub copy_picks {
    # Copy picks from another network
    my $self = shift;
    my ($other_pw, $keymap) = @_;
    if (!$keymap || $keymap eq 'ALL') {
        # The user wants to copy all associations verbatim
        $keymap = { map {$_ => $_} $other_pw->each_pick_category };
    }

    # Keymap relates a list of source categories in $other_pw to
    # destination categories in $self

    while (my ($source, $dest) = each %{$keymap}) {
        my @sources = $other_pw->each_pick($source);
        $self->add_pick(\@sources, $dest);
    }
}

sub _do_expand_explicit {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    my $nw   = $self->network;
    my @found;
    my @nodes = $self->_choose_nodes( \%exargs );
    $self->_message_plus_nodes("Explicitly expanding", \@nodes) 
        if ($self->{VERBOSE});
    foreach my $node ( @nodes ) {
       my @iter = $nw->explicitly_expand( %exargs, -node => $node );
    }
    $self->_add_nodes( \%exargs, \@found );
    # print $nw->to_graphviz;
    
    return undef;
}

sub _do_filter_edges {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    my $nw   = $self->network;
    $nw->filter_edges( %exargs );
    return undef;
}

sub _do_format {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    my $nw   = $self->network;
    if (my $nodes = $exargs{NODE}) {
        # explicitly passed node:
        my @nodes = ref($nodes) ? @{$nodes} : ($nodes);
        foreach my $node (@nodes) {
            $nw->format_node( $node, $exargs{PARAM}, $exargs{VALUE});
        }
    } else {
        # Check for nodes defined by pick categories
        foreach my $sid ($self->_choose_nodes( \%exargs ) ) {
            $nw->format_node( $sid, $exargs{PARAM}, $exargs{VALUE});
        }
    }
    return undef;
}

sub _do_connect {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    my $nw   = $self->network;
    my @nodes;
    if ($exargs{NODE}) {
        @nodes = $self->_choose_nodes( { USEPICK => $exargs{NODE} } );
    } else {
        @nodes = $self->_choose_nodes( \%exargs );
    }
    return if ($#nodes < 0);
    my @targs;
    if ($exargs{TARGET}) {
        @targs = $self->_choose_nodes( { USEPICK => $exargs{TARGET} } );
    }
    my @found = $nw->connect_internal( %exargs, 
                                       -node   => \@nodes,
                                       -target => \@targs, );
    return undef;
}

sub _do_telescope {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    my $nw   = $self->network;
    my @found;
    foreach my $sid ($self->_choose_nodes( \%exargs ) ) {
        push @found, $nw->telescope( %exargs, -node => $sid );
    }
    $self->_add_nodes( \%exargs, \@found );
    return undef;
}

sub _choose_nodes {
    my $self = shift;
    my $args = $_[0];
    my $nw   = $self->network;
    my @sids;
    if (my $up = $args->{USEPICK}) {
        # Expand from pick list categories
        @sids = $self->each_pick( $up );
    } elsif ($args->{USEALL}) {
        # Expand every node in tree
        @sids = $nw->all_seq_ids;
    } else {
        # Simple expansion from root(s)
        @sids = $nw->each_root;
    }
    return @sids;    
}

sub _add_nodes {
    my $self = shift;
    my ($args, $list) = @_;
    if (my $cat = $args->{ADDPICK}) {
        my %nonredun;
        my $nw = $self->network;
        foreach my $req (@{$list}) {
            my $seq = $nw->node($req);
            $nonredun{ $seq->id } = $seq;
        }
        my @found = values %nonredun;
        $self->add_pick(\@found, $cat);

        $self->_message_plus_nodes("    Added to $cat: ", \@found) 
            if ($self->{VERBOSE});
    } elsif ($self->{VERBOSE}) {
        my %nonredun;
        my $nw = $self->network;
        foreach my $req (@{$list}) {
            my $seq = $nw->node($req);
            $nonredun{ $seq->id } = $seq;
        }
        my @found = values %nonredun;
        $self->_message_plus_nodes("    Added to network: ", \@found);
    }
    
}

sub _do_add_root {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    my @source = $self->each_pick( $exargs{SOURCE} );
    $self->add_root( @source );
    return undef;
}

sub _do_remove_root {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    my @source = $self->each_pick( $exargs{SOURCE} );
    $self->network->remove_root( @source );
    return undef;
}

sub _do_find_root {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    my $nw   = $self->network;
    my @oldroots  = $nw->each_root;
    my ($newroot) = $nw->find_tree_root( %exargs );
    if ($newroot) {
        if ($exargs{RESET}) {
            $nw->clear_roots;
            $nw->add_root( $newroot );
            $nw->add_root( @oldroots );
        }
        $self->_add_nodes( \%exargs, [ $newroot ] );
    }
    return undef;
}

sub _do_addpick {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    my @picks = $self->pick( %exargs );
    if ($self->{VERBOSE}) {
        my $cat = $exargs{CATEGORY} || 'DEFAULT';
        $self->_message_plus_nodes("  Added to $cat: ", \@picks)
    }
    if ($#picks < 0) {
        return 'ABORT' if ( $exargs{MUSTPICK} );
    }
    return undef;
}

sub _do_unpick {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    my @picks  = $self->unpick( %exargs );
    if ($#picks > 0) {
        my $cat = $exargs{CATEGORY} || 'DEFAULT';
        $self->_message_plus_nodes("Removed from $cat: ", \@picks)
    }
    return undef;
}

sub _do_showpick {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    my @cats = $self->each_pick_category;
    foreach my $cat (@cats) {
        print " + Pick Category $cat\n";
        my @picks = sort { $a->name cmp $b->name } $self->each_pick($cat);
        foreach my $pick (@picks) {
            print "   - " . $pick->name . "\n";
        }
    }
    return undef;
}

sub _do_findpath {
    my $self   = shift;
    my %exargs = $self->process_args( @_ );
    my $nw     = $self->network;
    my (@start, @end);
    # What node(s) do we start with?
    if (my $cat = $exargs{START}) {
        @start = $self->each_pick($cat);
    } else {
        @start = (undef);
    }

    # What node(s) should we end at?
    if (my $cat = $exargs{'END'}) {
        @end = $self->each_pick($cat);
    } else {
        # Unspecified - use the DEFAULT category
        @end = $self->each_pick('DEFAULT');
    }

    delete $exargs{'END'};
    delete $exargs{'START'};
    foreach my $sseq (@start) {
        foreach my $eseq (@end) {
            my $pathobj = $nw->find_path( %exargs,
                                          -end   => $eseq,
                                          -start => $sseq, );

            $pathobj->save_best( -cache => $exargs{SAVEBEST})
                if ( defined $exargs{SAVEBEST} );
            $self->cache_path( $pathobj );
        }
    }
    return undef;
}

sub _do_showpath {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    foreach my $pathobj ( $self->cache_path ) {
        my @allpaths = $pathobj->paths_as_text;
        if ($#allpaths > -1) {
            print join("\n", $pathobj->paths_as_text)."\n";
        } else {
            #printf("-- No satisfactory path between %s and %s\n",
            #   $sseq->name, $eseq->name);
        }
    }
    return undef;
}

sub _do_tabpath {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    my $file = $exargs{FILE};
    unless ($file) {
        $file = "PathWalker_Output.tab";
    }
    unless ($self->{FILES}{$file}) {
        warn "Initiating Tab output to '$file'\n";
        unlink($file);
        $self->{FILES}{$file} = 1;
    }
    open(OUT, ">>$file") || 
        die "Could not append to '$file':\n$!\n ";
    foreach my $pathobj ($self->cache_path ) {
        my @rows = $pathobj->paths_as_tab;
        foreach my $row (@rows) {
            print OUT join("\t", @{$row})."\n";
        }
    }
    close OUT;
    return undef;
}

sub _do_saveimage {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    my $nw   = $self->network;
    $exargs{FILENAME} ||= "PathWalker$$";
    $exargs{DIR}      ||= '/stf/biohtml/tmp/MapTracker';
    $exargs{DIR} =~ s/\/$//;
    my ($html, $data) = $nw->to_graphviz_html( %exargs );
    return undef;
}

sub _do_param {
    my $self = shift;
    my %exargs = $self->process_args( @_ );
    $self->param( $exargs{TAG}, $exargs{VALUE});
    return undef;
}

sub _do_netparam {
    my $self = shift;
    my $nw   = $self->network;
    my %exargs = $self->process_args( @_ );
    $nw->param( $exargs{TAG}, $exargs{VALUE});
    return undef;
}

sub _do_message {
    my $self   = shift;
    my %exargs = $self->process_args( @_ );
    my $msg    = $exargs{TEXT};
    my $frm    = $exargs{FORMAT};
    unless ($frm) {
        $frm = $ENV{'HTTP_HOST'} ? '<p>%s</p>' : '%s';
    }
    my $iloop = 0;
    while ($msg =~ /(SHOW|COUNT)PICKS?\:(\S+)/i) {
        my ($what, $cat) = (uc($1), $2);
        my @picks = $self->each_pick($cat);
        my @names = map { $_->name } @picks;
        my $replace = "";
        if ($what eq 'SHOW') {
            $replace = sprintf("%s = [%s]", $cat, join(", ", @names));
        } else {
            $replace = $#names + 1;
        }
        my $swap = $what . 'PICKS?';
        $msg =~ s/$swap\:\Q$cat\E/$replace/i;
	if (++$iloop > 500) {
	    $self->error("Likely infinite loop formatting message for ".
                         $exargs{TEXT});
	    last;
	}
    }
    printf($frm, $msg. "\n"); 
    return undef;
}

sub cache_path {
    my $self = shift;
    foreach my $path (@_) {
	push @{$self->{PATHCACHE}}, $path;
    }
    return @{$self->{PATHCACHE}};
}

sub clear_cache {
    my $self = shift;
    $self->{PATHCACHE} = [];
}

sub parse_args {
    my $self = shift;
    my ($string) = @_;
    $string = "" unless (defined $string);
    my (%hash, $iloop);
    # remove spaces flanking equals signs
    $string =~ s/\s*\=\>\s*/\=/g;

    # Identify quoted blocks:
    my @found_quote;
    foreach my $type ("\'", "\"") {
        my $tre = "\Q$type\E";
        my $hackup = $string;
        $iloop = 0;
        while ($hackup =~ /($tre[^$tre]*?$tre)/) {
            my $target = $1;
            push @found_quote, $target;
            $hackup =~ s/\Q$target\E/QUOTE_STRIPPED/;
            if (++$iloop > 500) {
                $self->error("Likely infinite loop parsing '$_[0]' - exiting, data may be corrupted.");
                last;
            }
        }
    }

    @found_quote = sort { length($b) <=> length($a) } @found_quote;
    my @quoted;
    my $qtag = 'QUOTED_ITEM_%d_';
    foreach my $fqre (@found_quote) {
        if ($string =~ /(\Q$fqre\E)/) {
            my $found = $1;
            my $rep = sprintf($qtag, $#quoted + 1);
            $string =~ s/\Q$found\E/$rep/;
            # Remove flanking quotes
            $found = substr($found, 1, length($found)-2);
            push @quoted, $found;
        }
    }

    # Mask out spaces inside lists:
    $iloop = 0;
    while ($string =~ /\(([^\(\)]*?[\s\,][^\(\)]*?)\)/) {
	my $target = $1;
	my $replace = $target;
	$replace =~ s/[\s\,]+/LiStSeP/g;
	my $estr = '$string =~ s/\(\Q' .$target. '\E\)/\(' .$replace. '\)/g;';
	eval($estr);
	if (++$iloop > 500) {
	    $self->error("Likely infinite loop parsing '$_[0]' - exiting, data may be corrupted.");
	    last;
	}
    }

    # Get individual assignments
    # warn "\n\n$string\n\n";
    my $qmat = $qtag;
    $qmat =~ s/\%d/\(\\d\+\)/;
    my @parts = split(/\s+/, $string);
    foreach my $part (@parts) {
        # de-mask quoted regions
        while ($part =~ /$qmat/) {
            my $num = $1;
            my $mat = sprintf($qmat, $num);
            $part =~ s/$mat/$quoted[$num]/;
        }
        my @bits = split(/\=/, $part);
        my $arg = shift @bits;
        my $val = join('=', @bits);
	unless (defined $arg) {
	    $self->error("No argument specified for '$part'");
	    next;
	}
	$arg =~ s/^\-//;
	$arg = uc($arg);
	$val = 1 unless (defined $val);
	if ($val =~ /^(LIST|ARRAY|HASH)\:?[\[\(](.*)[\)\]]$/i) {
	    my ($type, $data) = (uc($1), $2);
            $data =~ s/^LiStSeP//; $data =~ s/LiStSeP$//;
	    $data =~ s/\\?\'//g;
	    $data =~ s/\\\./\./g;
	    my @stuff = split(/LiStSeP/, $data);
	    $hash{$arg} = $type eq 'HASH' ? { @stuff } : [ @stuff ];
	} else {
	    if ($val =~ /^\\?\'(.*)\\?\'$/) {
		# Capture content inside quotes
		$val = $1;
	    }
	    $hash{$arg} = $val;
	}
    }
    return \%hash;
}

sub add_root {
    my $self = shift;
    my @seqs = $self->network->add_root( @_ );
    return () if ($#seqs < 0);
    if ($self->{VERBOSE}) {
        foreach my $seq (@seqs) {
            $self->msg(sprintf("Adding root '%s' to network", $seq->name ));
        }
    }
    return @seqs;
}

sub clear_picks {
    my $self = shift;
    my ($cat) = @_;
    if (defined $cat) {
	$self->{PICKS}{ uc($cat) } = {};
    } else {
	$self->{PICKS} = {};
    }
    if ($self->{VERBOSE}) {
	my $msg = "    Pick list cleared";
	$msg .= " for category '$cat'" if (defined $cat);
	$self->msg($msg);
    }
    return $self->{PICKS};
}

sub each_pick_category {
    my $self = shift;
    return sort keys %{$self->{PICKS}};
}

sub each_pick {
    my $self = shift;
    my ($cat) = @_;
    my @cats;
    if (!defined $cat) {
        # No category defined, use ALL categories
        @cats = $self->each_pick_category;
    } elsif (ref($cat)) {
        # A list of categories is wanted
        @cats = map {uc($_)} @{$cat};
    } else {
        $cat = uc($cat);
        if ($cat eq 'ROOT') {
            # Request to get only those nodes that are roots
            return $self->network->each_root;
        } elsif ($cat eq 'ALL') {
            # Explicit request to get everything
            return $self->network->all_seqs;
        } else {
            # Single category requested
            @cats = ( $cat );
        }
    }
    # warn "[".join(',', @cats)."] ";
    my @retval = ();
    foreach my $rcat (@cats) {
	next unless ($self->{PICKS}{$rcat});
	foreach my $sid (keys %{$self->{PICKS}{$rcat}}) {
	    push @retval, $self->{PICKS}{ $rcat }{ $sid };
	}
    }
    return @retval;
}

sub add_pick {
    my $self = shift;
    my ($req, $cat) = @_;
    return wantarray ? () : undef unless ($req);
    my @requests = (ref($req) && ref($req) eq 'ARRAY') ? @{$req} : ($req);
    $cat ||= 'DEFAULT';
    $cat = uc($cat);
    $self->{PICKS}{$cat} ||= {};
    my @found;
    foreach my $request (@requests) {
        if (my $seq = $self->tracker->get_seq( $request )) {
            push @found, $self->{PICKS}{$cat}{ $seq->id } = $seq;
        }
    }
    return wantarray ? @found : $found[0];
}

sub add_association {
    my $self = shift;
    my ($name, $parent, $kids) = @_;
    my $mt   = $self->tracker;
    my $pseq = $mt->get_seq($parent);
    return undef unless ($pseq);

    my $pid = $pseq->id;
    $kids   = [ $kids ] if (!ref($kids) || ref($kids) ne 'ARRAY');
    $name ||= 'DEFAULT';
    $name   = uc($name);
    $self->{ASSOC}{$name} ||= {};
    $self->{ASSOC}{$name}{$pid} ||= {};
    foreach my $kid (@{$kids}) {
        my $kseq = $mt->get_seq($kid);
        next unless ($kseq);
        $self->{ASSOC}{$name}{$pid}{ $kid->id } = $kid;
    }
    return $self->{ASSOC}{$name}{$pid};
}

sub each_association {
    my $self = shift;
    my ($req) = @_;
    my @names = $req ? ($req) : $self->each_association_category;
    my @retval;
    foreach my $name (@names) {
        next unless (exists $self->{ASSOC}{$name});
        my @parents;
        foreach my $pid (keys %{$self->{ASSOC}{$name}}) {
            my $pseq = $self->tracker->get_seq($pid);
            my @kids = sort { lc($a->name) cmp lc($b->name) } values
                %{$self->{ASSOC}{$name}{$pid}};
            push @parents, [ $pseq, \@kids ];
        }
        push @retval, [ $name, \@parents ];
    }
    return @retval;
}

sub each_association_category {
    my $self = shift;
    return sort keys %{$self->{ASSOC}};
}


sub clear_associations {
    my $self = shift;
    my ($name) = @_;
    if (defined $name) {
        $self->{ASSOC}{uc($name)} = {};
    } else {
        $self->{ASSOC} = {};
    }
    if ($self->{VERBOSE}) {
	my $msg = "    Association list cleared";
	$msg .= " for '$name'" if (defined $name);
	$self->msg($msg);
    }
    return $self->{ASSOC};
}

sub remove_pick {
    my $self = shift;
    my ($req, $cat) = @_;
    my $seq = $self->tracker->get_seq( $req );
    return undef unless ($seq);
    $cat ||= 'DEFAULT';
    $cat = uc($cat);
    $self->{PICKS}{$cat} ||= {};
    my $sid = $seq->id;
    my $rv = $self->{PICKS}{$cat}{ $sid };
    delete $self->{PICKS}{$cat}{ $sid };
    return $rv;
}

sub pick {
    my $self = shift;
    my $nw   = $self->network;
    my $mt   = $self->tracker;
    my $args = $self->parseparams(  -clear    => 0,
				    -regexp    => undef,
				    -category  => 'DEFAULT',
				    -source    => undef,
				    -hasparent => undef,
				    -childof   => undef,
                                    -allclass  => undef,
                                    -addall    => undef,
                                    -node      => undef,
				    @_ );
    
    my $cat    = $args->{CATEGORY};

    $self->clear_picks( $cat ) if ($args->{CLEAR});

    my @picked = ();
    my @source;
    if ($args->{SOURCE}) {
	@source = $self->each_pick( $args->{SOURCE} );
    } elsif ($args->{NODE}) {
        @source = ( $mt->get_seq( $args->{NODE} ) );
    } else {
	@source = $nw->all_seqs;
    }

    if ($args->{ADDALL}) {
        # Every node in the source should be added to the pick
        $self->add_pick(\@source, $cat);
        return @source;
    }

    my $class   = $args->{ISCLASS};
    my $noclass = $args->{NOTCLASS};
    my $re      = $args->{REGEXP} || $args->{RE};
    my $haspar  = $args->{HASPARENT} || $args->{CHILDOF};
    my @allc    = $mt->param_to_list( $args->{ALLCLASS}, 'class');

    if ($class || $noclass || $#allc > -1) {
        # Fast load classes for tested sequences
        $nw->_bulk_class_load( map { $_->id } @source );
    }

    foreach my $seq ( @source ) {
	my $addpick = 0;
	if ($re) {
	    $addpick = 1 if ($seq->name =~ /$re/);
	}
	if ($haspar) {
	    $addpick = 1 if ($seq->has_parent( $haspar ));
	}
	if ($class) {
	    $addpick = 1 if ($seq->is_class( $class ));
	}
	if ($noclass) {
	    $addpick = 1 if (!$seq->is_class( $noclass ));
	}
        if ($#allc > -1) {
            $addpick = 1;
            for my $ci (0..$#allc) {
                unless ($seq->is_class( $allc[$ci] )) {
                    $addpick = 0;
                    last;
                }
            }
            next unless ($addpick);
        }
        push @picked, $seq if ($addpick)
    }
    $self->add_pick(\@picked, $cat);
    return @picked;
}

sub unpick {
    my $self = shift;
    my $nw   = $self->network;
    my $mt   = $self->tracker;
    my $args = $self->parseparams(  -clear     => 0,
				    -regexp    => undef,
				    -category  => 'DEFAULT',
				    -hasparent => undef,
				    -childof   => undef,
				    @_ );
    
    my $re     = $args->{REGEXP} || $args->{RE};
    my $haspar = $args->{HASPARENT} || $args->{CHILDOF};
    my $class  = $args->{ISCLASS};
    my $notclass  = $args->{NOTCLASS};
    if ($class) {
        $class = ref($class) ? $class : [ $class ];
    }
    if ($notclass) {
        $notclass = ref($notclass) ? $notclass : [ $notclass ];
    }
    $self->clear_picks() if ($args->{CLEAR});

    my $cat    = $args->{SOURCE} || $args->{CATEGORY};
    my $addcat = $args->{ADDPICK};
    my @source = $self->each_pick( $cat );
    my @removed = ();
    if ($class || $notclass) {
        # Fast load classes for tested sequences
        $nw->_bulk_class_load( map { $_->id } @source );
    }
    foreach my $seq ( @source ) {
	my $keeppick = 1;
	if ($re) {
	    $keeppick = 0 if ($seq->name =~ /$re/);
	}
	if ($haspar) {
	    $keeppick = 0 if ($seq->has_parent( $haspar ));
	}
	if ($class) {
	    $keeppick = 0 if ($seq->is_class( @{$class} ));
	}
	if ($notclass) {
            $keeppick = $seq->is_class( @{$notclass} ) ? 1 : 0;
	}
	unless ($keeppick) {
	    my $rem = $self->remove_pick($seq, $cat);
	    push @removed, $rem if ($rem);
	}
    }
    $self->add_pick(\@removed, $addcat) if ($addcat);
    return @removed;
}
#-#-#-#-#--#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-
1;
