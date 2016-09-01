# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

BEGIN {
    # See if there is a parameter file to assure correct environment
    # You can either assure that %ENV is set properly outside of Perl
    # or you can edit the paramter file. If this module is at
    # /foo/bar/BMS/MapTracker.pm then the parameter file is:
    # /foo/bar/BMS/MapTracker.param
    if (my $ppath = $INC{'BMS/MapTracker.pm'}) {
        $ppath =~ s/\.pm$/\.param/;
        if (-e $ppath && !$ENV{MT_PFILE_READ}) {
            $ENV{MT_PFILE_READ} = $ppath;
            if (open(PPATH, "<$ppath")) {
                while (<PPATH>) {
                    s/[\n\r]+$//;
                    if (/^ENV\s+(\S+)\s+(.+)$/) {
                        my ($var, $val) = ($1, $2);
                        if ($val =~ /\$\$/) {
                            my $pid = $$;
                            $val =~ s/\$\$/$pid/g;
                        }
                        my $nowVal = $ENV{$var};
                        if ($var ne 'PATH' || !$nowVal) {
                            $ENV{$var} = $val;
                        } elsif ($nowVal !~ /\Q$val\E/) {
                            $ENV{$var} = "$val:$ENV{$var}";
                        }
                        # warn "$var = $ENV{$var}\n";
                    }
                }
                close PPATH;
            } else {
                warn "Failed to read parameter file $ppath\n  $!\n  ";
            }
        } else {
            $ENV{MT_NO_PARAM_FILE} = $ppath unless ($ENV{MT_PFILE_READ});
        }
    }
    # This probably no longer needs to be in a BEGIN block ... but
    # I can not recall if a use()ed module needs the environment...
}

$BMS::MapTracker::VERSION = 
    ' $Id$ ';

use strict;
use vars qw(@ISA);

use BMS::SimpleTree;
use BMS::JavaPop;


use BMS::MapTracker::DBI;
use BMS::MapTracker::Shared;
use BMS::MapTracker::Transform;
use BMS::MapTracker::Class;
use BMS::MapTracker::Taxa;
use BMS::MapTracker::Authority;
use BMS::MapTracker::Mapping;
use BMS::MapTracker::Seqname;
use BMS::MapTracker::Namespace;
use BMS::MapTracker::Edge;
use BMS::MapTracker::Relationship;
use BMS::MapTracker::Searchdb;
use BMS::MapTracker::LoadHelper;

@ISA = qw(BMS::MapTracker::Shared);

my ($ns_none);
my $case_codes = {
    '?'  =>  1, # This single, specific case
    '!?' => -1, # Any case but this one
    '?!' => -1, # Any case but this one
    '??' =>  0, # Any case at all
};

my $server = $ENV{MT_SERVER_NAME} || ""; $server =~ s/\/+$//;
my $script = $ENV{MT_SCRIPT_NAME} || ""; $script =~ s/^\/+//;
my $mthttp = "http://$server/$script";

# View current PG locks:
# SELECT l.mode, l.granted, l.pid, r.relname FROM pg_locks l, pg_database d, pg_class r WHERE d.datname = 'maptracker' AND d.oid=l.database AND r.oid=l.relation;

my @colors = ('blue','brick','green','orange');

=head1 PRIMARY METHODS
#-#-#-#-#--#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-

=head2 new

 Title   : new
 Usage   : my $obj = BMS::MapTracker->new(@arguments)
 Function: Creates a new object and returns a blessed reference to it.
 Returns : A blessed BMS::MapTracker object
 Args    : Associative array of arguments. Recognized keys [Default]:

 -username The name of the user

 -userdesc A description for the user. Needed if you think the user
           will be a new entry.

  -dumpsql SQL debugging. If 0, then no SQL will be dumped. If 1, then
           only 'big' or slow SQL statements will be dumped. 'medium'
           SQL dumped at 2. All SQL dumped at 3 or greater.

    -debug Default 0. If true, then debugging information will be
           provided.

   -dumpfh Default *STDERR. The filehandle SQL and debugging data are
           dumped to.

  -dumplft
  -dumprgt Default "\n". The text printed around each SQL / debugging
           statement. Could be changed to "<pre>" and "</pre>" for web
           browsers.

 -cacheseq Default 1. The program maintains a cache of commonly used
           objects. Normally, sequence information is cached, but this
           could cause memory bloat when performing loading
           operations. For this reason, if you are loading a lot of
           data, you should set -cacheseq to 0.

 -database Default 'maptracker'. The name of the postgres database. DO
           NOT change this value unless you know what you are doing!

 -postgres Default '8.1.0' (a string). The version of Postgres being
           used. DO NOT change this value unless you know what you are
           doing!

   -pgport The module is aware of the major PostGres versions at BMS,
           and should be able to determine which port to use. However,
           if you are using an esoteric version, you may need to
           explicitly provide port information with -pgport

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = { 
        UNIQUE_ID => 0,
	CACHE => { 
            name_id      => {},
            authority_id => {},
        },
        SEQ_CACHE_COUNT => 0,
        REQUESTED_TAXA  => {},
	OBJECTS => {
            seqnames    => {},
            edges       => {},
            transforms  => {},
            authorities => {},
            classes     => {},
            types       => {},
            taxa        => {},
            namespaces  => {},
            searchdbs   => {},
        },
        JSOBJS   => {},
        STORE_JS => [],

        URIS => {
            paths => {},
            p2www => {},
        },
        TRANSIENTS => {
            COUNTER => {},
            LOOKUP  => {},
        },
	PATHS => {
	    url  => "http://$server/",
	    path => $ENV{MT_ROOT_DIR},
	    dir  => $ENV{MT_WORK_DIR},
            tmp  => $ENV{MT_TEMP_DIR},
            images => $ENV{GV_FILE_PATH},
	},
    };
    bless ($self, $class);
    foreach my $key (keys %{$self->{OBJECTS}}) {
        $self->{OBJ_COUNT}{$key} = 0;
    }
    if (my $baseDir = $ENV{MT_ROOT_DIR}) {
        $baseDir .= '/' unless ($baseDir =~ /\/$/);
        $self->file_path('BASE', $baseDir);
        $self->file_url('BASE', "http://$server/");
        if (my $tdir = $ENV{MT_TEMP_DIR}) {
            $self->file_path('TMP',     $baseDir.$tdir);
            $self->file_path('NODEIMG', $baseDir.$tdir);
        }
        if (my $wdir = $ENV{MT_WORK_DIR}) {
            $self->file_path('STATIC',  $baseDir.$wdir."/");
            $self->file_path('JS',      $baseDir.$wdir."js");
        }
    }
    # When push comes to shove security trumps everything
    my $sspd = "Wpctsste";
    my $args = $self->parseparams
	( -username => 'ReadOnly',
	  -userdesc => '',
	  -dumpfh   => *STDERR,
	  -dumpsql  => 0,
	  -debug    => 0,
	  -dumplft  => "\n",
	  -dumprgt  => "\n",
	  -cacheseq => 500,
	  -ishtml   => $ENV{'HTTP_HOST'} ? 1 : 0,
	  -help     => undef,
          -safety   => $ENV{MT_LOCK_FILE},
          # -ssapbd   => $sspd,
	  @_ );

    foreach my $makeIfNeeded (qw(GV_FILE_PATH)) {
        if (my $path = $ENV{$makeIfNeeded}) {
            $self->file_path('TEMPTAG', $path);
        }
    }

    $args->{USERNAME} ||= 'ReadOnly';

    foreach my $arg ('DUMPFH', 'DUMPSQL', 'DUMPLFT', 'DUMPRGT', 'SAFETY',
		     'DEBUG', 'CACHESEQ', 'ISHTML', 'HELP', 'USERNAME') {
	$self->{$arg} = $args->{$arg};
    }
    $self->{DUMPSQL} ||= 0;
    
    unless ($args->{NOINIT}) {
        if (my $dbi = $self->dbi($args)) {
            # Was able to establish connection to DB
        } else {
            $self->death("Failure to establish connection to MapTracker database.");
        }
	$self->user( $args->{USERNAME}, $args->{USERDESC} );
	$self->{READONLY} = 1 if ($args->{USERNAME} =~ /readonly/i);
    }
    $ns_none = $self->get_space( 1 );
    return $self;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 dbi

 Title   : dbi
 Usage   : $self->dbi
 Function: 
 Returns : The database interface object
 Args    : 

=cut

sub dbi {
    my $self = shift;
    my $args = shift || {};
    unless (defined $self->{DBI}) {
        if (my $safe = $self->{SAFETY}) {
            if (-e $safe) {
                my $msg = "Lockfile is preventing database access.\n".
                    "  Contents of $safe :\n";
                if (open(SAFE, "<$safe")) {
                    while (<SAFE>) {
                        $msg .= "    $_";
                    }
                    close SAFE;
                } else {
                    $msg .= "    Failed to read information from lockfile:\n    $!";
                }
                warn "$msg\n";
                return $self->{DBI} = 0;
            }
        }
        my $admin = $args->{DBADMIN};
        $admin    = $ENV{DBADMIN}  unless (defined $admin);
        my @opts  = ( -dbadmin => $admin );
        my $configs = {
            escape_string_warning => 'off',
        };
        my $port = $ENV{PGPORT};
        my $host = $ENV{PGHOST};
        # my $host = $ENV{PGHOST} = 'elephant.pri.bms.com';
        # $ENV{PGLIB} = '';
        # warn $host;
        my $d = $self->{DBI} = BMS::MapTracker::DBI->new
            ( -dbtype  => 'postgres',
              -dbuser  => $ENV{MT_DB_USER},
              -dbname  => $ENV{MT_DB_NAME},
              -dbport  => $port,
              -dbhost  => $host,
              -dumpsql => $self->{DUMPSQL},
              @opts,
              );
        while (my ($p,$v) = each %{$configs}) {
            my $csth = $d->prepare("SET $p TO '$v'");
            # warn "$p : $v";
            $csth->execute();
        }
        $self->{DBI}->username( $self->{USERNAME} );
        
    }
    return $self->{DBI};
}

*dismiss = \&release;
sub release {
    my $self = shift;
    $self->{DBI}->release() if ($self->{DBI});
}

sub fork_safe   { 
    my $self = shift;
    my $aggressive = shift;
    if (my $dbi = $self->dbi()) {
        $dbi->dbh->{InactiveDestroy} = 1;
        if ($aggressive) {
            # Just clear all the handles
            delete $dbi->{STHS};
        } else {
            foreach my $sth (values %{$dbi->{STHS}}) {
                $sth->{InactiveDestroy} = 1;
            }
        }
    }
}

sub fork_unsafe   { 
    my $self = shift;
    if (my $dbi = $self->dbi()) {
        $dbi->dbh->{InactiveDestroy} = 0;
        foreach my $sth (values %{$dbi->{STHS}}) {
            $sth->{InactiveDestroy} = 0;
        }
    }
}


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 file_path

 Title   : file_path
 Usage   : my $path = $self->file_path( $tag, $optional_new_value)
 Function: Sets / Gets a base file path for a tag
 Returns : A string (could be "" if not set)
 Args    : [0] The tag for the path
           [1] Optional new value for the tag

=cut

sub file_path  {
    my $self = shift;
    my $uris = $self->{URIS};
    my ($tag, $nv) = @_;
    $tag ||= 'BASE';
    $tag = uc($tag);
    if ( $nv ) {
        $nv =~ s/\/+$//;
        unless (-e $nv) {
            # This directory does not exist - try to make it
            my @bits = split(/\//, $nv);
            my $path = "";
            while ($#bits > -1) {
                my $bit = shift @bits;
                next unless ($bit);
                $path .= "/$bit";
                next if (-e $path);
                mkdir($path, 0777);
                # For reasons unknown, mkdir is not honoring the mask
                chmod(0777, $path);
            }
        }
        $self->{URIS}{paths}{$tag} = "$nv/";
    }
    return $self->{URIS}{paths}{$tag} || "";
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 file_url

 Title   : file_url
 Usage   : my $url = $self->file_url( $tag, $newURL )

 Function: Gets / Sets a base URL for a tag. Similar to above, but returns
           the URL needed to access the file from a web browser. You
           must first set path_to_url() for the tag.

 Returns : A string (could be "" if not set)
 Args    : [0] The tag will be set to 'BASE' if not provided
           [1] Optional new URL

    If an URL has not been set for the tag that you are requesting,
    the URL set for 'BASE' will be attempted. So if you have set:

    $mt->file_path('BASE', '/home/homer/')
    $mt->file_path('IMAGES', '/home/homer/images')
    $mt->file_url('BASE', 'http://www.homersite.net/')

    ... and then you call:

    my $url = $mt->file_url('IMAGES')

    ... $url will equal 'http://www.homersite.net/images'

=cut

sub file_url  {
    my $self = shift;
    my ($tag, $nv) = @_;
    $tag ||= 'BASE';
    $tag = uc($tag);
    if ( $nv ) {
        $nv =~ s/\/+$//;
        $self->{URIS}{p2www}{$tag} = "$nv/";
    }
    return $self->{URIS}{p2www}{$tag} if ($self->{URIS}{p2www}{$tag});
    # Hmm. Not set for this tag. Try to build it from base:
    my $url = $self->{URIS}{p2www}{BASE};
    unless ($url) {
        # Neither $tag nor BASE set
        $self->err("Can not make url for '$tag'");
        return '';
    }
    my ($bpath, $tpath) = ($self->{URIS}{paths}{BASE},
                           $self->{URIS}{paths}{$tag});
    #warn "($bpath, $tpath)";
    unless ($bpath && $tpath) {
        $self->err
            ("Set file_path for both BASE and $tag to make URL for $tag");
        return '' ;
    }
    if ($tpath =~ /^\Q$bpath\E/) {
        $tpath =~ s/^\Q$bpath\E//;
        return $url . $tpath;
    } else {
       $self->err
           ("BASE path is not a parent directory of $tag - ".
            "set file_url() explicitly for $tag");
       return '' ; 
   }
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 classMenu

 Title   : classMenu
 Usage   : print $self->classMenu
 Function: Dumps the class tree as a javascript pop-up menu
 Returns : A string of the class tree as HTML
 Args    : Associative array of arguments. Recognized keys [Default]:

  -classid The initial id to use 

 -classform Default 'update'. The name of the form that the element
           will appear inside.

 -classinput Default 'setclass'. The name of the input field that you
           wish changed when a class is selected.

=cut

sub classMenu  {
    my $self = shift;
    $self->benchstart;
    my $args = $self->parseparams( -classid => 0,
				   -classform    => 'update',
				   @_ );
    my ($id, $tree, $parent) = ( $args->{CLASSID}, $args->{TREE}, $args->{PARENT});

    unless( $tree ) {
	$tree = BMS::SimpleTree->new;
    }
    my $class = $self->get_class($id);
    my $node  = $tree->node( $class->token );
    $node->tag('alt', $class->desc);
    $parent->add_child($node) if ($parent);
    foreach my $kid ($class->each_child) {
	$self->classMenu( -classid => $kid->id, -tree => $tree, 
			  -parent  => $node);
    }
    my $txt = "";
    unless ($parent) {
	my $field = $args->{CLASSFORM}.".".$args->{CLASSINPUT};
	my $jtree = $tree->javatree
	    ( -root           => $class->token,
	      -clues          => "status",
	      -mode           => "field=$field",
	      -cookie         => "MapTrackerClass",
	      -selectinternal => 1, );
	$txt .= $jtree->baseLink( title => "Select a Class");
	$txt .= $jtree->style_string;
	$txt .= $jtree->html_string;
    }
    $self->benchstop;
    return $txt;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 classTree

 Title   : classTree
 Usage   : print $self->classTree
 Function: Dumps the class tree as text
 Returns : A string of the class tree as text
 Args    : The class id to start with. Default 0.

=cut

sub classTree  {
    my $self = shift;
    $self->benchstart;
    my ($id, $in) = @_;
    $id ||= 0;
    $in ||= 0;
    my $class = $self->get_class($id);
    my $pad = "   " x $in;
    my $txt = sprintf("$pad %s : %s\n", $class->name, $class->desc);
    foreach my $kid ($class->each_child) {
	$txt .= $self->classTree($kid->id, $in+1);
    }
    $self->benchstop;
    return $txt;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 classTreeHTML

 Title   : classTreeHTML
 Usage   : $self->classTreeHTML
 Function: As classTree(), but outputs as HTML formatted text
 Returns : A string of the class tree as text
 Args    : The class id to start with. Default 0.

=cut

sub classTreeHTML  {
    my $self = shift;
    $self->benchstart;
    my ($id, $in) = @_;
    $id ||= 0;
    $in ||= 0;
    my $txt = "";
    my $pad = '  ' x $in;
    unless ($id) {
        $txt .= "<ul class='DynamicList'>\n";
        $self->get_all_classes();
    }
    my $class = $self->get_class($id);
    my ($name, $token, $desc) = ($class->name, $class->token, $class->desc);
    $txt .= sprintf
        ("%s<li><b><font size='+1'><a class='mtclass' ".
         "href='%s?getexample=class_%s'>%s</a></font></b> ".
         "<font color='brown'>[%d]</font> ".
         "<font color='grey' size='-1'><i>%s</i></font> %s</li>\n", 
         $pad, $mthttp, $token, $name, $id, $token, $desc);

    my @kids = sort { lc($a->name) cmp lc($b->name) } ( $class->each_child );
    if ($#kids > -1) {
        $txt .= "$pad<ul>\n";
        foreach my $kid (@kids) {
            $txt .= $self->classTreeHTML($kid->id, $in+1);
        }
        $txt .= "$pad</ul>\n";
    }
    unless ($id) {
        $txt .= "</ul>\n";
    }
    $self->benchstop;
    return $txt;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 friendlyPanelOptions

 Title   : friendlyPanelOptions
 Usage   : $self->friendlyPanelOptions
 Function: Gets FriendlyPanel options for all classes

 Returns : A hash reference to be passed to Friendly Panel as the
           -opts reference.

 Args    : 

=cut

sub friendlyPanelOptions {
    my $self = shift;
    $self->benchstart;
    # Rare event: 0.2441 sec in 1 events(AVG 0.2441 s)
    $self->classTreeHTML; # This just makes sure all classes are in the cache
    my $opts = { };
    my $nameSub = sub {
	my ($feat) = @_;
	return "" unless ($feat->isa('Bio::SeqFeature::Generic') && 
                          $feat->has_tag('name'));
	my @names = $feat->each_tag_value('name');
        my $count = $#names + 1;
        if ($#names > 0) {
            my %hash;
            foreach my $name (@names) {
                if ($name =~ /^(.+) (\S+)$/) {
                    push @{$hash{$1}}, $2;
                } else {
                    $hash{$name} ||= [];
                }
            }
            @names = ();
            while (my ($n1, $arr) = each %hash) {
                if ($#{$arr} < 0) {
                    push @names, $n1;
                } elsif ($#{$arr} > 0) {
                    push @names, sprintf("%ss %s", $n1,join(',',sort @{$arr}));
                } else {
                    push @names, "$n1 " . $arr->[0];
                }
            }
        }
        if ($#names < 5) {
            return join(", ", sort @names);
        } else {
            return sprintf("%d Entries", $count);
        }
    };
    my $colorScore = sub {
	my ($feat) = @_;
	return "" unless ($feat->isa('Bio::SeqFeature::Generic'));
        
    };
    # Should be ordered from most to least specific:
    my @special = 
	( [ 'variant',       [ -glyph   => 'triangle',    
			       -bgcolor => 'blue',     ] ],
	  [ 'probe',         [ -glyph   => 'transcript2',
			       -bgcolor => '#339999' ] ],
	  [ 'patented',      [ -glyph   => 'transcript2',    
			       -bgcolor => '#3399cc',     ] ],
	  [ 'patent',        [ -glyph   => 'transcript2',    
			       -bgcolor => '#3399cc',     ] ],
	  [ 'genomicregion', [ -glyph   => 'transcript2',    
			       -bgcolor => 'brown',     ] ],
	  [ 'gdna',          [ -glyph   => 'transcript2',    
			       -bgcolor => 'orange',     ] ],
	  [ 'dna',           [ -glyph   => 'transcript2',    
			       -bgcolor => '#ccccff',     ] ],
	  [ 'rna',           [ -glyph   => 'transcript2',    
			       -bgcolor => '#99ccff',     ] ],
	  [ 'protein',       [ -glyph   => 'transcript2',    
			       -bgcolor => '9933cc',     ] ],
	  [ 'na',            [ -glyph   => 'transcript2',
			       -bgcolor => '#00ccff' ] ],
	  [ 'bio',           [ -glyph   => 'transcript2',    
			       -bgcolor => '#00cccc',     ] ],
	  # [ 'query',         'transcript2', 'black', ],
	  );
    for my $i (0..$#special) {
	$special[$i][0] = $self->get_class($special[$i][0]);
    }

    my @classes = values %{$self->{OBJECTS}{classes}};
    # Special class for the query sequence:
    my $base = BMS::MapTracker::Class->new
	( -name    => 'Query', -token => 'Query',
	  -tracker => $self, -id => 999, 
	  -desc   => 'The Query Sequence - all other features are oriented relative to it',
	   );

    push @classes, $base;
    foreach my $class (@classes) {
	my ($name, $desc) = ($class->token, $class->desc);
	my @options = ( -key      => $desc,
			-label    => $nameSub,
			-bgcolor  => "gray",
			-glyph    => "generic", );
	foreach my $dat (@special) {
	    # Is there a parent 
	    my ($parent, $opts) = @{$dat};
	    if ($parent->has_child($class)) {
		push @options, @{$opts};
		last;
	    }
	}
        push @options, ( -stranded => 1 ) unless ($class->is_class('variant'));
	$opts->{$name} = { @options };
    }
    foreach my $name (keys %{$opts}) {
        my %hash = %{$opts->{$name}};
        $hash{'-key'} = "Cross-species " . $hash{'-key'};
        my $color = $hash{'-bgcolor'};
        my $colorSpecies = sub {
            my ($feat,$option_name,$part_no,$total_parts,$glyph) = @_;
            my $col = $feat->{_BGCOLOR_} || $color;
            return $col;
        };
        if ($hash{'-glyph'} eq 'triangle') {
            $hash{'-hilite'} = $colorSpecies;
        } else {
            $hash{'-bgcolor'} = $colorSpecies;
        }
        # $hash{'-fgcolor'} = $colorSpecies if ($hash{'-glyph'} eq 'triangle');
        delete $hash{'-label'};
        my $xname = $name . "_XENO";
        $opts->{$xname} = \%hash;
    }
    $self->benchstop;
    return $opts;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 getHumanGenomeIDs

 Title   : getHumanGenomeIDs
 Usage   : my $hash = $self->getHumanGenomeIDs
 Function: All types (relationships) as HTML text.
 Returns : A string with HTML code describing all relationships
 Args    : The class id to start with. Default 0.

NEEDS WORK. Now using forms like 'GRCh37'

=cut

sub getHumanGenomeIDs {
    my $self = shift;
    unless ($self->{GENOME_IDS}) {
        $self->benchstart;
        my @seq = $self->get_seq('HUMAN_%.');
        my $rows = $self->dbi->selectall_arrayref
            ( -sql => "SELECT seqname, name_id FROM seqname WHERE ".
              "upper(seqname) LIKE 'HUMAN\\_%.NCBI_??'",
              -name => "Select all human genome clones",
              -level => 1 );
        my $hash = {};
        foreach my $row (@{$rows}) {
            my ($name, $id) = @{$row};
            if ($name =~ /\.NCBI_(\d+)$/i) {
                my $vers = $1;
                $hash->{$vers} ||= [];
                push @{$hash->{$vers}}, $id;
            }
        }
        $self->{GENOME_IDS} = $hash;
        $self->benchstop;
    }
    return $self->{GENOME_IDS};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 get_all_types

 Title   : get_all_types
 Usage   : my @types = $self->get_all_types
 Function: Retrieve all Relationships.
 Returns : An array of Relationship objects
 Args    : 

=cut

sub get_all_types {
    my $self = shift;
    unless ($self->{ALLTYPES}) {
        $self->get_all_classes();    
        $self->benchstart;
        my $type_rows = $self->dbi->
            named_sth("Load all Types")->selectall_arrayref();
        my @types;
        foreach my $row (@{$type_rows}) {
            my ($id, $name, $forw, $back, $desc, $cid1, $cid2) = @{$row};
            ($forw, $back) = (lc($forw), lc($back));
            my $class1 = $self->get_class( $cid1 );
            my $class2 = $self->get_class( $cid2 );
            my $type = BMS::MapTracker::Relationship->new
                ( -id      => $id,     -name     => $name, -desc   => $desc,
                  -forward => $forw,   -backward => $back,
                  -class1  => $class1, -class2   => $class2);
            push @types, $type;
            $self->{OBJECTS}{types}{ $id } = $type;
            $self->{OBJECTS}{types}{ lc($name) } = $type;
            $self->{OBJECTS}{types}{ $forw } = $type;
            $self->{OBJECTS}{types}{ $back } = $type;
            $self->{OBJ_COUNT}{types}++;
        }
        $self->{ALLTYPES} = \@types;
        $self->benchstop;
    }
    return @{$self->{ALLTYPES}};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 get_type

 Title   : get_type
 Usage   : $self->get_type($id)
 Function: Gets a Relationship object for an $id or name.
 Returns : 
 Args    : 

=cut

sub get_type {
    my $self  = shift;
    my $val   = $_[0] || 0;
    my $tinfo = $self->type_information;
    my $type;
    my ($direction, $space) = (0,undef);
    if (!$val) {
	# undefined request
    } elsif ($self->_safe_isa($val, 'BMS::MapTracker::Relationship')) {
	# The call was already made with a type object
	$type = $val;
    } else {
	($val, $space) = $self->strip_tokens( lc($val) );
	if (exists $tinfo->{$val}) {
	    $type      = $tinfo->{$val}{OBJ};
	    $direction = $tinfo->{$val}{DIR};
	} else {
	    $self->err("I am not aware of a type called '$val'");
	}
    }
    return wantarray ? ($type, $direction, $space || $ns_none) : $type;
}

sub reverse_type {
    my $self  = shift;
    my $tinfo = $self->type_information;
    my $val   = lc( $_[0] || '');
    if (exists $tinfo->{$val}) {
        return $tinfo->{$val}{BACK};
    } else {
        return undef;
    }
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 type_information

 Title   : type_information
 Usage   : my $hash_ref = $self->type_information
 Function: Retrieve metadata for all relationships.
 Returns : A hash reference keyed to relationship name and ID
 Args    : 

=cut

sub type_information {
    my $self = shift;
    return $self->{TYPE_INFO} if ( $self->{TYPE_INFO} );
    $self->benchstart;
    my %hash;
    foreach my $type ($self->get_all_types) {
        my ($rf, $rb) = $type->reads;
        my ($tok, $id) = (lc($type->name), $type->id );
        my $isSym = $rf eq $rb ? 1 : 0;
        my $th = {
            TOKEN => $tok,
            ID    => $id,
            SYM   => $isSym,
            OBJ   => $type,
            FOR   => $rf,
            REV   => $rb,
            # These three values will be flipped for reverse data:
            READ  => $rf,
            BACK  => $rb,
            DIR   => !$isSym,
        };
        $hash{$id}  = $th;
        $hash{$rf}  = $th;

        # Non-directional data:
        my $nth          = $isSym ? $th : { %{$th} };
        $hash{$tok}      = $nth;
        $hash{$tok}{DIR} = 0;

        # Reverse Data:
        my $rth = { %{$th} };
        $rth->{DIR} *= -1;
        $rth->{BACK} = $rf;
        $rth->{READ} = $rb;
        $hash{$id * -1} ||= $rth;
        $hash{$rb}      ||= $rth;
    }
    $self->benchend;
    return $self->{TYPE_INFO} = \%hash;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 typeListHTML

 Title   : typeListHTML
 Usage   : $self->typeListHTML
 Function: All types (relationships) as HTML text.
 Returns : A string with HTML code describing all relationships
 Args    : The class id to start with. Default 0.

=cut

sub typeListHTML {
    my $self = shift;
    $self->benchstart;
    my $txt = "";
    foreach my $type ( $self->get_all_types ) {
	$txt .= $type->to_html . "<br />\n";
    }
    $self->benchstop;
    return $txt;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 typeTableHTML

 Title   : typeTableHTML
 Usage   : $self->typeTableHTML
 Function: All types (relationships) as HTML text, table format.
 Returns : A string with HTML code describing all relationships
 Args    : The class id to start with. Default 0.

=cut

sub typeTableHTML {
    my $self = shift;
    $self->benchstart;
    my $txt = "<table class='mtmild SortableTable' border='1'>";
    $txt .="<tr><th>Name</th><th>ID</th><th>Forward</th><th>Reverse</th></tr>";
    foreach my $type ( $self->get_all_types ) {
        my ($s1, $s2) = $type->classes;
        my ($f, $r)   = $type->reads;
        my $link      = sprintf
            ("<a class='mtrel' href='mapTracker.pl?getexample=type_%s'>%s</a>",
             $type->name, $type->name);
        $txt .= sprintf
            ("<tr><td>%s</td><td><font color='brown' size='-1'>%d</font></td>",
             $link, $type->id, );
        $txt .= sprintf
            ("<td align='center'><font color='green'>%s</font> ".
             "<font color='orange'>%s</font> ".
             "<font color='brick'>%s</font></td>", 
             $s1->name, $f, $s2->name);
        $txt .= sprintf
            ("<td align='center'><font color='brick'>%s</font> ".
             "<font color='orange'>%s</font> ".
             "<font color='green'>%s</font></td></tr>\n", 
             $s2->name, $r, $s1->name, );
    }
    $txt .= "</table>\n";
    $self->benchstop;
    return $txt;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 spaceTableHTML

 Title   : spaceTableHTML
 Usage   : $self->spaceTableHTML
 Function: All namespaces as HTML text, table format.
 Returns : A string with HTML code describing all namespaces
 Args    : 

=cut

sub spaceTableHTML {
    my $self = shift;
    $self->benchstart;
    my $txt = "<table class='mtmild SortableTable' border='1'>";
    $txt .="<tr><th>Name</th><th>ID</th><th>Description</th><th>Case Sensitive</th></tr>";
    my @spaces = sort { $a->id <=> $b->id } $self->get_all_namespaces;
    foreach my $ns (@spaces) {
        $txt .= sprintf
            ("<tr><td>%s</td><td>%d</td><td>%s</td><td>%s</td></tr>\n", 
             $ns->name, $ns->id, $ns->desc, $ns->sensitive ? 'Yes' : '');
    }
    $txt .= "</table>\n";
    $self->benchstop;
    return $txt;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 authorityTableHTML

 Title   : authorityTableHTML
 Usage   : $self->authorityTableHTML
 Function: All authorities as HTML text, table format.
 Returns : A string with HTML code describing all authorities
 Args    : 

=cut

sub authorityTableHTML {
    my $self = shift;
    $self->benchstart;
    my $txt = "<table class='mtmild SortableTable' border='1'>";
    $txt .="<tr><th>Name</th><th>ID</th><th>Description</th></tr>";
    my @auths = sort { $a->id <=> $b->id } $self->get_all_authorities;
    foreach my $auth (@auths) {
        $txt .= sprintf
            ("<tr><td class=''>%s</td><td>%d</td><td>%s</td></tr>\n", 
             $auth->name, $auth->id, $auth->desc);
    }
    $txt .= "</table>\n";
    $self->benchstop;
    return $txt;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 searchdbTableHTML

 Title   : searchdbTableHTML
 Usage   : $self->searchdbTableHTML
 Function: All search DBs as HTML text, table format.
 Returns : A string with HTML code describing all authorities
 Args    : 

=cut

sub searchdbTableHTML {
    my $self = shift;
    $self->benchstart;
    my $txt = "<table class='mtmild SortableTable' border='1'>";
    $txt .="<tr><th>Name</th><th>ID</th><th>Type</th><th>Path</th></tr>";
    my @objs = sort { $a->id <=> $b->id } $self->get_all_searchdbs;
    foreach my $obj (@objs) {
        $txt .= sprintf
            ("<tr><td>%s</td><td>%d</td><td>%s</td><td>%s</td></tr>\n",
             $obj->name, $obj->id, $obj->type, $obj->path );
    }
    $txt .= "</table>\n";
    $self->benchstop;
    return $txt;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 typeSelect

 Title   : typeSelect
 Usage   : print $self->typeSelect

 Function: Generate a string containing HTML code for a <SELECT> dialog

 Returns : A string to insert into a form

    Args : The name that you want the varaible assigned to in the
           CGI. The default is 'typeid'

=cut

sub typeSelect {
    my $self = shift;
    $self->benchstart;
    my $args = $self->parseparams( -name    => 'typeid',
				   -reads   => 'for',
				   @_ );
    my @types = $self->get_all_types();
    my $txt = "<select name='$args->{NAME}'>\n";
    $txt .= sprintf("<option value='%s'>%s</option>\n", 
                    0 , "- select relationship -");
    my %reads;
    my $dir = $args->{READS};
    $dir = ($dir =~ /rev/ || $dir < 0) ? -1 : ($dir) ? 1 : 0;
    foreach my $type (@types) {
        map { $reads{$_} = $type } $type->reads($dir);
    }
    foreach my $read (sort keys %reads) {
        my $type = $reads{$read};
	$txt .= sprintf("<option value='%s'>%s</option>\n",
                        $dir * $type->id, $read);
    }
    $txt .= "</select>\n";
    $self->benchstop;
    return $txt;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 classTreeSelect

 Title   : classTreeSelect
 Usage   : $self->classTreeSelect

 Function: Generate a string containing HTML code for a <SELECT> dialog

 Returns : A string to insert into a form

    Args : The name that you want the varaible assigned to in the
           CGI. The default is 'classname'

=cut

sub classTreeSelect {
    my $self = shift;
    $self->benchstart;
    my ($id, $in) = @_;
    my $name = 'classname';
    if ($id && !$in && $id !~ /^\d$/) {
	$name = $id;
	$id = 0;
    }
    $id ||= 0;
    $in ||= 0;
    my $class = $self->get_class($id);
    my $pad = '&nbsp;' x ($in * 3);
    my $txt = "";
    unless ($id) {
	$txt = "<select name='$name'>\n";
    }
    $txt .= sprintf("<option value='%s'>%s</option>\n", 
		      $class->token, $pad . $class->name);
    foreach my $kid ($class->each_child) {
	$txt .= $self->classTreeSelect($kid->id, $in+1);
    }
    $txt .= "</select>\n" unless ($id);
    $self->benchstop;
    return $txt;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 user

 Title   : user
 Usage   : $self->user($username, $userdesc);

 Function: Returns an Authority object representing the user, who may
           be defined by either a name or id.. If a user *name* is
           provided, the ID for that name will be returned (and a new
           user entry will be made in the DB if needed - for this
           reason be sure to provide a description, too).

 Returns : An authority object
 Args    : To set the value provide either a user id or user name.

=cut

sub user {
    my $self = shift;
    $self->benchstart;
    my ($user, $desc) = @_;
    if ($user) {
	if ($self->_safe_isa($user,'BMS::MapTracker::Authority')) {
	    $self->{USER} = $user;
	} elsif ($self->{USER} = $self->get_authority($user)) {
	    
	} else {
            $desc ||= "";
            if ($desc eq 'BMS Username') {
                # See if we can append a full name
                if (my $seq = $self->get_seq('#LDAP#' . $user)) {
                    my $sf = $self->get_edge_dump
                        ( -name     => $seq,
                          -keeptype => 'is a shorter term for' );
                    if ($#{$sf} == 0) {
                        my $sid2 = $sf->[0][1];
                        my $name = $self->get_seq($sid2);
                        $desc = "BMS Username: " . $name->name if ($name);
                    }
                }
            }
            my $newUser;
            if ($desc) {
                $newUser = $self->make_authority($user, $desc);
            } else {
                $self->err("Can not make new authority for '$user' without ".
                             " a defined -userdesc. User set to 'ReadOnly'");
                $newUser = $self->get_authority('ReadOnly');
            }
	    $self->{USER} = $newUser;
	}
        $self->{READONLY} = ($self->{USER} && 
                             $self->{USER}->name =~ /readonly/i) ? 1 : 0;
    }
    
    $self->death("The user must be defined when MapTracker is initiated.") 
	unless ($self->{USER});
    $self->benchstop;
    return $self->{USER};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 bulk_class_assignment

 Title   : bulk_class_assignment
 Usage   : $self->bulk_class_assignment(@sequence_objects)
 Function: Takes a list of seq objects and reads class data in bulk.
 Returns : 
 Args    : 

=cut

sub bulk_class_assignment {
    my $self  = shift;
    # Structure queries as hash keyed off name_id
    my %hash  = map { $_->id => $_ } @_;
    my @sids  = sort { $a <=> $b } keys %hash;

    my $bulksize = 20;
    my $bulkList = join(",", map { '?' } (1..$bulksize));
    my $bulkSTH  = $self->dbi->prepare
        ( "SELECT name_id, class_id, authority_id FROM seq_class".
          " WHERE name_id IN ( $bulkList )");
    while (my @binds = splice(@sids, 0, $bulksize)) {
        my $short = $bulksize - ($#binds + 1);
        push @binds, map { $binds[0] } (1..$short) if ($short > 0);
        my $rows = $bulkSTH->selectall_arrayref( @binds );
	foreach my $row (@{$rows}) {
            my ($sid, $cid, $aid) = @{$row};
            $hash{$sid}->add_class($cid, $aid);
	}
    }
    map { $_->task('read_classes', 1) } values %hash;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 bulk_seq_for_ids

 Title   : bulk_seq_for_ids
 Usage   : $self->bulk_seq_for_ids(@seqnames)
 Function: Takes a list of seq IDs and returns a hash reference, keyed
           to seq_id and pointing at Seqname objects.
 Returns : A hash reference
 Args    : 

=cut

sub bulk_seq_for_ids {
    my $self  = shift;
    my $cache = $self->{OBJECTS}{seqnames};
    my (%rv, %needed);
    foreach my $req (@_) {
        next unless ($req);
        if ($cache->{$req}) {
            $rv{$req} ||= $cache->{$req}[0];
        } else {
            $needed{$req} = $req;
        }
    }
    my @uniq     = sort { $a <=> $b } keys %needed;
    my $bulksize = 20;
    $bulksize    = $#uniq + 1 if ($#uniq + 1 < $bulksize);
    my $bulkList = join(",", map { '?' } (1..$bulksize));
    my $bulkSTH  = $self->dbi->prepare
        ( -name  => "Bulk get seqname entries in chunks of $bulksize",
          -level => 3,
          -sql   => 
          "SELECT name_id, seqname, space_id FROM seqname".
          " WHERE name_id IN ( $bulkList )" );
    while (my @binds = splice(@uniq, 0, $bulksize)) {
        my $short = $bulksize - ($#binds + 1);
        push @binds, map { $binds[0] } (1..$short) if ($short > 0);
        my $rows = $bulkSTH->selectall_arrayref( @binds );
	foreach my $row (@{$rows}) {
            my ($sid, $name, $nsid) = @{$row};
            next if ($rv{$sid} );
	    $rv{$sid} = BMS::MapTracker::Seqname->new
		( -id      => $sid,
		  -name    => $name,
                  -space   => $self->get_space($nsid),
		  -tracker => $self, );
            $cache->{$sid} = [ $rv{$sid} ];
            $self->{OBJ_COUNT}{seqnames}++;
	}
    }
    return \%rv;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 bulk_seq2id

 Title   : bulk_seq2id
 Usage   : $self->bulk_seq2id(@seqnames)

 Function: Gets the database ids for a list of seqnames.

 Returns : An array of seq ids
 Args    : 

=cut

sub bulk_seq2id {
    my $self = shift;
    $self->benchstart;
    my $dbi     = $self->dbi;
    my $cache   = $self->{CACHE}{name_id};
    my @ids     = ();
    my %hunt    = ();
    # Do as much searching as possible without locking the database:
    for my $i (0..$#_) {
        my $req = $_[$i];
	if ($req =~ /^(\d+)$/ || $req =~ /^MTID:(\d+)$/i) {
            # Simple integer ID, just populate as is
	    $ids[$i] = $1;
	} elsif (exists $cache->{$req}) {
            # We have already determined the ID for this request:
            $ids[$i] = $cache->{$req};
        } else {
            # We have not seen this request yet - we need to query for it
            my ($clean, $space, $case_override) = $self->strip_tokens($req);
            # If no namespace is defined, then ALWAYS use the undefined one:
            $space  ||= $ns_none;
            my $nsid  = $space->id;
            my $cokey = defined $case_override ? $case_override : 'U';
            # Cluster first by case-override value, then by namespace:
            $hunt{$cokey}{$nsid} ||= {
                space     => $space,
                override  => $case_override,
                names     => {},
                sensitive => (defined $case_override) ? 
                    $case_override : $space->case_sensitive,
            };
            # For case-insensitive searches, key using upper-case name:
            my $nkey = $hunt{$cokey}{$nsid}{sensitive} ? $clean : uc($clean);
            # Finally cluster by the case-sensitized name:
            $hunt{$cokey}{$nsid}{names}{$nkey} ||= {
                name    => $clean,
                indices => [],
            };
            push @{$hunt{$cokey}{$nsid}{names}{$nkey}{indices}}, $i;
	}
    }
    my @overs = keys %hunt;
    if ($#overs < 0) {
        # If everything was found, return without locking:
        $self->benchstop;
        return wantarray ? @ids : [\@ids, 0];
    }

    # Try to find the unfound ones in the DB without locking:
    my $base = "SELECT seqname, name_id FROM seqname WHERE upper(seqname) ";
    my @not_in_database;
    my %recovered;
    
    my $bulksize = 20;
    my $bulkList = join(",", map { '?' } (1..$bulksize));
    foreach my $cokey (@overs) {
        # A particular 'case override' token
        while (my ($nsid, $space_dat) = each %{$hunt{$cokey}}) {
            my $sensitive = $space_dat->{sensitive};
            my $space     = $space_dat->{space};
            my $spacemod  = " AND space_id IN ". $space->all_ids_sql;
            my $baseWhere = " = upper(?) $spacemod";
            my $scanSTH;
            if ($sensitive) {
                # We also need to match case criteria, can not query in bulk
                $baseWhere .= sprintf
                    (" AND seqname %s ?", ($sensitive > 0) ? '=' : '!=');
                $scanSTH = $dbi->prepare("$base $baseWhere");
             } else {
                # If we can ignore case, we can query in bulk
                my $sql = sprintf("%s IN ( %s ) %s",$base,$bulkList,$spacemod);
                $scanSTH = $dbi->prepare($sql);
            }

            my $name_dat = $space_dat->{names};
            my @search   = keys %{$name_dat};
            while ($#search > -1) {
                my @binds;
                if ($sensitive) {
                    # Need to query one at a time, using two bind variables
                    my $query = shift @search;
                    @binds = ($query, $query);
                } else {
                    # We can bulk query - build a list up to maximum char len
                    @binds = map { uc($_) } splice( @search, 0, $bulksize);
                    my $short = $bulksize - ($#binds + 1);
                    push @binds, map { $binds[0] } (1..$short) if ($short > 0);
                }
                # warn $scanSTH->pretty_print( @binds );
                my $rows = $scanSTH->selectall_arrayref( @binds );
                # $scanSTH->pretty_print( @binds );

                # The recovered seqname will serve as the key for
                # accessing data from %hunt, and for setting values in
                # the cache. Unless we are honoring case, we need to
                # upper-case all these values:
                map { $_->[0] = uc($_->[0]) } @{$rows} unless ($sensitive);

                my %found_keys;
                foreach my $row (@{$rows}) {
                    my ($nkey, $sid) = @{$row};
                    foreach my $pos (@{$name_dat->{$nkey}{indices}}) {
                        $recovered{$pos}{$sid}++;
                    }
                    $found_keys{$nkey}++;
                }
                # These entries were succesfully found - remove from hash:
                foreach my $nkey (keys %found_keys) {
                    delete $name_dat->{$nkey};
                }
            }

            # What entries were not found in the database?
            my @remaining = values %{$name_dat};
            next if ($#remaining < 0);

            if (defined $space_dat->{override} && $space_dat->{override} !=1){
                # The user was looking for entries other than this
                # case - we should not use their query as the basis
                # for making a new entry. Note their IDs as zero:
                foreach my $data (@remaining) {
                    foreach my $pos (@{$data->{indices}}) {
                        $cache->{$_[$pos]} = $ids[$pos] = 0;
                    }
                }
            } else {
                # Queue these names for addition to the DB
                my $dualBind = $sensitive ? 1 : 0;
                my $checkSTH = $dbi->prepare
                    ("SELECT name_id FROM seqname".
                     " WHERE upper(seqname) $baseWhere");
                foreach my $data (@remaining) {
                    $data->{check} = $checkSTH;
                    $data->{space} = $space;
                    $data->{dual}  = $dualBind;
                    push @not_in_database, $data;
                }
            }
        }
    }

    my $needNum = $#not_in_database + 1;
    unless ($needNum == 0) {
        # Apparently we need to make some new entries:
        my $lockSTH = $dbi->named_sth("Lock seqname exclusively");
        $dbi->begin_work;
        $lockSTH->execute();
        my $counter = 0;
        foreach my $data (@not_in_database) {
            unless (++$counter % 5000) {
                # Occasionally release the lock to let other queries have
                # a chance of getting access to DB:
                $dbi->commit;
                $dbi->begin_work;
                $lockSTH->execute();
            }

            # We still need to check the DB - another process may have
            # added the name while the table was unlocked.
            my $name  = $data->{name};
            my @binds = $data->{dual} ? ( $name, $name ) : ($name);
            my @sids  = $data->{check}->get_array_for_field( @binds);

            my $sid;
            if ($#sids > 0) {
                # Oops. Now there are multiple entries
                warn "Multiple seq_name entries for $name!\n  ";
                $sid = 0;
            } elsif ($#sids < 0) {
                # Nothing found, make a new entry:
                $sid = $self->_add_new_seqname( $name, $data->{space});
            } else {
                # Unique entry in database
                $sid = $sids[0];
            }
            # Record the name_id identified (or zero)
            foreach my $pos (@{$data->{indices}}) {
                $recovered{$pos}{$sid}++;
            }
        }
        $dbi->commit;
    }

    # Now set the name_id for each array index position that we found
    # (or created) entries for:
    foreach my $pos (keys %recovered) {
        my @sids = keys %{$recovered{$pos}};
        # If a unique sid was found, use it - otherwise store value as zero
        # (zero represents a query that does not return a unique node)
        my $sid  = ($#sids == 0) ? $sids[0] : 0;
        $cache->{$_[$pos]} = $ids[$pos] = $sid;
    }
    $self->benchstop;
    return wantarray ? @ids : [\@ids, $needNum];
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 bulk_edge2id

 Title   : bulk_edge2id
 Usage   : my $lookup_hash = $self->bulk_edge2id( $array_ref )

 Function: Gets the database ids for a list of edges.

 Returns : A hash reference, keyed to edge tokens, with values being edge_ids.

    Args : An array reference of edge specifications. Edges are
           specified by listing [ name1, name2, type_id, space_id ] -
           it is important that the values be listed in the order
           shown. The values may be passed as 4-element array
           references as shown, or as a string, with each value
           separated by a tab. All values MUST be defined integers,
           except space_id, which will automatically be set as 1 if
           not provided (or zero).

=cut

sub bulk_edge2id {
    my $self = shift;
    $self->benchstart;
    my ($arr, $is_partial) = @_;

    # The user can provide either 4-element array refs, or tab-separated
    # strings. Normalize user input to array refs:
    my $norm = [ map { ref($_) ? $_ : [ split("\t", $_) ] } @{$arr} ];
    
    my ($hash, $missing, $counts);
    if ($is_partial) {
        # The user is requesting edges that are only partially specified
        ($hash, $counts) = $self->_partly_specified_edge_hash( $norm );
    } else {
        my $known = 0;
        ($hash, $missing, $known) = $self->_fully_specified_edge_hash( $norm );
        $counts->{distinct} = $known;
    }

    my @not_in_db = keys %{$missing || {}};
    my $needed    = $#not_in_db + 1;
    if ($needed == 0) {
        # Either we found all edges, or we requested partially-specified data
        # and therefore would not be able to generate new edges anyway.
        $self->benchstop;
        return wantarray ? ($hash, $counts) : $hash;
    }

    my $dbi  = $self->dbi;

    $dbi->user_column_order
        ('edge', qw(edge_id name1 name2 type_id space_id live created));

    # Some edges do not exist in the database - add them:
    my $now    = $self->db_timestamp;
    $dbi->begin_work;
    my $lockSTH = $dbi->named_sth("Lock edge exclusively");
    $lockSTH->execute();
    my $counter = 0;
    my @insert;
    foreach my $key (@not_in_db) {
        # We still need to check the DB - another process may have
        # added the name while the table was unlocked.
        my ($sth, $ids) = @{$missing->{$key}};
        $sth->execute( @{$ids} );
        my $rows = $sth->selectall_arrayref;

        if ($#{$rows} < 0) {
            # Nope, not there yet - need to make a new entry
            my $eid = $dbi->nextval('edge_seq');
            $hash->{$key} = $eid;
            $counts->{new}++;
            # Newly created edges will be live = false
            push @insert, [ $eid, @{$ids}, 'f', $now ];
            unless (++$counter % 500) {
                # Occasionally release the lock to let other queries have
                # a chance of getting access to DB:
                $dbi->write_array('edge', \@insert);
                @insert = ();
                $dbi->commit;
                $dbi->begin_work;
                $lockSTH->execute();
            }
        } elsif ($#{$rows} == 0) {
            # Someone added it to the database
            $hash->{$key} = $rows->[0][0];
            delete $missing->{$key};
            next;
        } else {
            $self->death("Multiple rows returned from edge",
                         $key,
                         $self->branch($rows));
        }
    }
    $dbi->write_array('edge', \@insert) if ($#insert > -1);
    $dbi->commit;
    $self->benchstop;
    $hash->{NEW} = $counter;
    return wantarray ? ($hash, $counts) : $hash;
}

# Find edge_ids when all four columns have been specified
sub _fully_specified_edge_hash {
    my $self = shift;
    my ($norm)    = @_;
    my $type_info = $self->type_information();
    my $dbi       = $self->dbi;
    my $sth       = $dbi->named_sth
        ( "Identify edges via full specification" );

    my (%hash, %missing, %distinct);
    foreach my $ids (@{$norm}) {
        my $key = $self->edge_columns_to_key( $ids, $type_info );
        # Carry on if we have already analyzed this edge request:
        next if ($hash{$key} || $missing{$key});
        my $rows = $sth->selectall_arrayref( @{$ids} );

        if ($#{$rows} < 0) {
            # No existing edge for these IDs
            $missing{$key} = [ $sth, $ids];
        } elsif ($#{$rows} == 0) {
            # Unique edge for these IDs - as it should be
            my $eid     = $rows->[0][0];
            $hash{$key} = $eid;
            $distinct{$eid} = 1;
        } else {
            # Ooo... This is a problem
            $self->death("Multiple rows returned from edge",
                         join(",", @{$ids}),
                         $self->branch($rows));
        }
    }
    my @dis = keys %distinct;
    return (\%hash, \%missing, $#dis + 1);
}

# Find edge_ids when at least one of the four columns is NOT specified
my $symEdges;
sub _partly_specified_edge_hash {
    my $self = shift;
    my ($norm)    = @_;
    my $dbi       = $self->dbi;
    my $type_info = $self->type_information();
    my @cols      = qw(name1 name2 type_id space_id);
    my (%hash, %counts);

    unless ($symEdges) {
        $symEdges = {};
        foreach my $type ( $self->get_all_types() ) {
            my $tid = $type->id;
            my ($f, $r) = $type->reads();
            $symEdges->{$tid} = ($f eq $r) ? 1 : 0;
        }
    }
    foreach my $ids (@{$norm}) {
        my $base = 'SELECT e.edge_id FROM edge e';
        my $key  = $self->partial_edge_columns_to_key( $ids, $type_info );
        # Carry on if we have already analyzed this request:
        next if ($hash{$key});

        my (@where, @binds);
        my $symEdge = 0;
        my $oneName = (!($ids->[0] && $ids->[1]) && ($ids->[0] || $ids->[1]));

        # Build a WHERE clause using only those columns that were specified
        if (my $tid = $ids->[2]) {
            # Edge type is specified
            if ($tid !~ /^\d+$/) {
                warn "Malformed type_id [$tid]: ".
                    join(',',map { defined $_ ? $_ : '-undef-' } @{$ids});
                next;
            }
            push @where, "e.type_id = ?";
            push @binds, $tid;
            if ($symEdges->{$tid}) {
                if ($oneName) {
                    # Symmetrical edge type with only a single name provided
                    $symEdge = 1;
                } elsif ($ids->[0] > $ids->[1]) {
                    # Symmetrical edge with nam$ids->[1], $ids->[0]);
                }
            }
        } elsif ($oneName) {
            # No type ID, only a single name provided
            # We will check both name1 and name2 to be safe
            $symEdge = 1;
        }

        if (my $sid = $ids->[3]) {
            # Edge space_id is specified
            if ($sid !~ /^\d+$/) {
                warn "Malformed space_id [$sid]: ".
                    join(',',map { defined $_ ? $_ : '-undef-' } @{$ids});
                next;
            }
            push @where, "e.space_id = ?";
            push @binds, $sid;
        }

        # Deal with the integer IDs first, and see if we have wildcards
        my $intIDs = 0;
        my @wildcards;
        for my $i (0..1) {
            my $id = $ids->[$i];
            next unless ($id);
            if ($id =~ /^(\d+)$/ || $id =~ /^MTID:(\d+)$/i) {
                # Pure name_id specified, or MTID:1234 nomenclature
                $id = $1;
                $intIDs++; # Note that we have at least one unique ID
                if ($symEdge) {
                    push @where, "(e.name1 = ? OR e.name2 = ?)";
                    push @binds, ($id, $id);
                } else {
                    push @where, "e.$cols[$i] = ?";
                    push @binds, $id;
                }
            } else {
                # Not a pure integer = wild card name specification
                # Deal with it below
                push @wildcards, [$i, $id];
            }
        }

        # warn "$base\n".$self->branch(\@wildcards);
        foreach my $idArr (@wildcards) {
            my ($i, $id) = @{$idArr};
            my $snTok = "s".($i+1);
            my ($clean, $space, $hc) = $self->strip_tokens($id);
            # warn "$id = ($clean, $space, $hc)";
            my @wcbind;
            # Basic case-insensitive LIKE query against the node:
            my $clause = "upper($snTok.seqname) LIKE ?";
            push @wcbind, uc($clean);
            if ($space) {
                # Node namespace has been specified
                $clause .= " AND $snTok.space_id = ?";
                push @wcbind, $space->id;
            }
            if ($hc) {
                # Case-sensitivity modifier
                $clause .= sprintf(" AND $snTok.seqname %s ?", $hc < 0 ? 
                                   'NOT LIKE' : 'LIKE');
                push @wcbind, $clean;
            }
            if ($intIDs == 0) {

                # The "name1 IN (SELECT)" approach is not working so well
                # in some cases. If the wildcard recovers many rows, then
                # the internal SELECT takes a LONG time
                # For example, it took 500msec to recover 11 rows for:
                # name2 = '545063034' + name1 LIKE 'ILMN_%'

                # The query below is modified to be an explicit join
                # against seqname. It is about 40% faster but still
                # slow.

                # However, we will use this block if only wildcards
                # are being queried (intIDs = 0), since a pure EXISTS
                # against a wildcard would likely be disasterous

                $base  .= ", seqname $snTok";
                $clause = " = $snTok.name_id AND $clause";
                # $clause = "(SELECT $snTok.name_id FROM seqname $snTok WHERE $clause)";
                if ($symEdge) {
                    push @where, "(e.name1 $clause OR e.name2 $clause)";
                    # push @where, "(e.name1 IN $clause OR e.name2 IN $clause)";
                    push @binds, (@wcbind, @wcbind);
                } else {
                    push @where, "$cols[$i] $clause";
                    # push @where, "$cols[$i] IN $clause";
                    push @binds, @wcbind;
                }
            } else {

                # Using EXISTS to test for the wildcard seems much
                # faster, at least in cases where the wildcard matches
                # many hits. The same query criteria as above took a
                # bit less than 1msec (500x speed up)

                $clause = "EXISTS (SELECT $snTok.name_id FROM seqname $snTok WHERE $snTok.name_id = %s AND $clause)"; 
                if ($symEdge) {
                    push @where, sprintf("($clause OR $clause)",
                                         'e.name1', 'e.name2');
                    push @binds, (@wcbind, @wcbind);
                } else {
                    push @where, sprintf($clause, 'e.'.$cols[$i]);
                    push @binds, @wcbind;
                }
            }
        }

        my $sql = "$base WHERE ". join(' AND ', @where);
        # my $sth = $dbi->prepare(-sql => $sql); $sth->pretty_print( @binds );
        my @eids = $dbi->get_array_for_field
            ( -sql     => $sql,
              -level   => 2,
              -name    => "Identify edges via partial specification",
              -bind    => \@binds);
        if ($#eids == -1) {
            $counts{null}++;
        } elsif ($#eids == 0) {
            $counts{distinct}++;
        } else {
            $counts{multiple}++;
        }
        $hash{$key} = \@eids;
    }
    # die;
    return wantarray ? (\%hash, \%counts) : \%hash;
}

# Takes a list of 4 ids and makes a string key
sub edge_columns_to_key {
    my $self = shift;
    my ($cols, $type_info) = @_;
    # Allow user to pass type_info explicitly - this allows the call to
    # type_information to be made outside a loop (a little bit faster)
    $type_info ||= $self->type_information();
    # Expect name1, name2, type_id, space_id
    if ($type_info->{$cols->[2]}{SYM} && $cols->[0] > $cols->[1]) {
        # This is a symmetrical edge.
        # Sort low name_id first for symetrical edges
        ( $cols->[0], $cols->[1] ) = ($cols->[1], $cols->[0]);
    }
    # Edge namespace is by default 1
    $cols->[3] ||= 1;
    return join("\t", @{$cols});
}

# As above, but does not assume that all columns are populated
sub partial_edge_columns_to_key {
    my $self = shift;
    my ($cols, $type_info) = @_;
    $type_info ||= $self->type_information();
    # Expect name1, name2, type_id, space_id
    if ($cols->[2] && $type_info->{$cols->[2]}{SYM} && 
        $cols->[0] && $cols->[1] && $cols->[0] > $cols->[1]) {
        # Sort low name_id first for symetrical edges
        ( $cols->[0], $cols->[1] ) = ($cols->[1], $cols->[0]);
    }
    # Edge namespace is by default 1
    $cols->[3] ||= 1;
    return join("\t", map { $_ || "" } @{$cols});
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

# THIS METHOD DOES *NOT*
#   - Lock table seqname
#   - Check $name for case-sensitive tokens
#   - Check that we really should be adding $name in the first place
# You should do these yourself before calling the method!!

sub _add_new_seqname {
    my $self = shift;
    my ($name, $space) = @_;
    my $dbi   = $self->dbi;
    my $add   = $dbi->named_sth("Create a new seqname entry");
    my $nsid  = $space ? $space->id : 1;
    my $sid   = $dbi->nextval('seqname_seq');
    # NO NO NO - problems with '/' tokens in SMILES strings
    # $name =~ s/\\.//g;    # Remove escapes
    # warn "$sid, $name, $nsid";
    $add->execute( $sid, $name, $nsid );
    return $sid;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# THIS METHOD DOES *NOT*
#   - Lock table namespace
# You should do these yourself before calling the method!!

sub _add_new_namespace {
    my $self = shift;
    my ($name, $desc, $sens) = @_;
    my $space = $self->get_space( $name );
    return $space if ($space);
    $self->death("Attempt to create namespace without name") unless ($name);
    $self->death("Namespace '$name' is longer than 20 characters")
        if (length($name) > 20);
    $self->death("Namespace '$name' can not be created without description") 
        unless ($desc);
    $desc = substr($desc, 0, 100);
    $self->death
        ("Please explicitly indicate if Namespace '$name' is case-sensitive")
        unless (defined $sens);

    my $dbi   = $self->dbi;
    my $bool  = (!$sens || $sens eq 'f') ? 'f' : 't';
    my $sid   = $dbi->nextval('namespace_seq');

    $dbi->do
        ( -sql     => "INSERT INTO namespace ". 
          "(space_id, space_name, descr, case_sensitive) VALUES (?,?,?,?)",
          -name    => "Create a new namespace entry",
          -bind    => [$sid, $name, $desc, $bool],
          -level   => 1, );

    $space = BMS::MapTracker::Namespace->new
        ( -id        => $sid,
          -name      => $name,
          -desc      => $desc,
          -sensitive => $sens,
          -tracker   => $self);
    $self->{OBJECTS}{namespaces}{ uc($name) }  = $space;
    $self->{OBJECTS}{namespaces}{ $sid }  = $space;
    $self->{OBJ_COUNT}{namespaces}++;
    return $space;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 _showSQL

 Title   : _showSQL
 Usage   : $self->_showSQL($sqlText, $label)
 Function: Pretty-prints a SQL string
 Returns : 
 Args    : 

=cut

sub _showSQL {
    my $self = shift;
    my ($text, $name, $level) = @_;
    return if ($level && $level > $self->{DUMPSQL});
    $self->benchstart;
    my $sqlcom = "/* %s : %s */\n";
    $text =~ s/[\s\n\t]+/ /g;
    $text = " $text ";
    my $maxtag = 12;
    my @tags = ("CREATE TABLE", "AS SELECT", "INSERT INTO", "SELECT", 
                "UPDATE", 
		["UNION", "\nUNION\t\n"], "LIMIT", "VALUES",
		"DELETE FROM", "FROM", "WHERE", "OR", "AND", "ORDER BY",
                "GROUP BY", "HAVING");
    foreach my $set (@tags) {
	# Case sensitive - the SQL should have keywords in caps
	my ($tag, $out);
	if (ref($set)) {
	    ($tag, $out) = @{$set};
	} else {
	    $tag = $set;
	    $out = "\n$tag\t";
	}
	$text =~ s/[\n ]+$tag[\n ]+/$out/g;
    }
    $text =~ s/\([\n ]*/\(/g;

    
    $text =~ s/^ //; $text =~ s/ $//;
    $text .= ';' unless ($text =~ /\;$/);
    $text =~ s/\;/\;\n/g;
    my @lines = split("\n", $text);
    my @newlines;
    my $maxline = 60;
    my $indent = 0; my @pad = ("");
    while ($#lines > -1) {
	my $line = shift @lines;
	next if ($line =~ /^\s*$/);
	# Wrap long lines:
	if (length($line) > $maxline) {
            # Temporarily mask spaces inside quotes:
            my $iloop = 0;
            while ($line =~ /(\'[^\']*?) ([^\']*?\')/) {
                $line =~ s/(\'[^\']*?) ([^\']*?\')/$1\n$2/;
                last if (++$iloop > 500);
            }
	    my $pos = rindex($line, " ", $maxline - 2);
            # Reset spaces that were inside quotes:
            $line =~ s/\n/ /g;
	    if ($pos > 0) {
		my $tail = " \t" . substr($line, $pos+1);
		unshift @lines, $tail;
		$line = substr($line, 0, $pos);
	    }
	}
        my @bits = split("\t", $line);
        my $pre = shift @bits;
        my $pro = join(" ", @bits);
	# Manage parentheses indenting:
	if ($indent > 0) {
	    ($pre, $pro) = ("", $pad[-1] . "$pre $pro");
	}
        # print "<pre>($pre, $pro)</pre>";
	my $newindent = $indent;
	$newindent += ( $line =~ tr/\(/\(/ );
	$newindent -= ( $line =~ tr/\)/\)/ );
	if ($newindent > $indent) {
	    my $ppos = index($pro, "(");
	    push @pad, " " x ($ppos+1);
	} elsif ($newindent < $indent) {
	    pop @pad;
	}
	$indent = $newindent;
	my $pline = sprintf("%".$maxtag."s %s", $pre, $pro);
	push @newlines, $pline;
    }
    $text = join("\n", @newlines);
    my @history;
    for my $hist (1..3) {
	my @f = split "::", (caller($hist))[3] || ""; # Calling funciton
	push @history, $f[$#f] if ($f[$#f]);
    }
    my $callHist = (join " < ", @history) || "";
    $text = sprintf($sqlcom, $name || "Un-named SQL", $callHist) . $text;
    $self->benchstop;
    my $fh = $self->{DUMPFH};
    print $fh $self->{DUMPLFT} . $text . $self->{DUMPRGT};
    return $text;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub stack_trace {
    my @history;
    my $hist = 2;
    while (1) {
        my ($pack, $file, $j4, $subname) = caller($hist);
        last unless ($subname);
        my ($j1, $j2, $line) = caller($hist-1);
        push @history, sprintf("  %50s : %d\n", $subname, $line);
        $hist++;
    }
    return join('', @history) || '-No stack trace-';
}

=head2 get_seq

 Title   : get_seq
 Usage   : $mt->get_seq($id)    or    $mt->get_seq( @arguments )
 Function: Gets a Seqname object for an $id or name.

 Returns : An array of seqname objects. If you call the function in
           scalar context, then the object with the highest database
           ID (name_id) will be returned.

 Args    : Associative array of arguments. Recognized keys [Default]:

     -name The name of the entry you want. Can be presented as:

           - A MapTracker::Seqname object (will just be given back to you)
           - An integer (will recover the entry with that name_id)
           - A string with wildcards (% or * recognized, all hits returned)
           - An exact string

           Also note that a question mark at the start of the string
           will cause the match to be case-sensitive. An exclamation
           point is used to specify "not this case". So if you have
           entries for Bird, BIRD and bird, you will get:

           bird   => (Bird, BIRD, bird)
           ?bird  => (bird)
           !?bird => (BIRD, bird)

 -nocreate Default [0]. When false, if a match is not found, a new
           entry will automatically be made in the database (unless
           wildcards were used). If -nocreate is true, a failed search
           will always return an empty list (or undef, if called in a
           scalar context).


           Also note: If you are happy with the defaults, then you can
           simply call the method as $mt->get_seq($request) - that is,
           by passing a single value (the request).

   -create Default 0. If true, then the entry will ALWAYS be
           made. Allows forced creation of entries with wildcard symbols.

=cut

sub get_seq_by_id {
    my $self = shift;
    my ($id) = @_;
    return undef unless ($id);
    return $self->{OBJECTS}{seqnames}{$id}[0] if
        (exists $self->{OBJECTS}{seqnames}{$id});
    return $self->get_seq( $id );
}

sub get_seq_ids {
    my $self = shift;
    unshift @_, '-name' if ($#_ == 0 );
    my $args = $self->parseparams( -name      => undef,
				   -nocreate  => 0,
				   @_);
    my $val = $args->{NAME} || $args->{ID} || $args->{SEQ};
    if (!$val) {
        # No request made
        return wantarray ? () : [];
    } elsif ($self->_safe_isa($val,'BMS::MapTracker::Seqname')) {
        # Request is already a Seqname object
        return wantarray ? ($val->id) : $val->id;
    } elsif ($val =~ /^\d+$/) {
        return wantarray ? ($val) : $val;
    }

    my $cache = $self->{OBJECTS}{seqnames};
    if (exists $cache->{$val}) {
        # We have already recovered this request
        my @rv = map { $_->id() } @{$cache->{$val}};
	return wantarray ? @rv : \@rv;
    }

    # Ok, we need to go look for a database entry
    $self->benchstart;
    my $dbi     = $self->dbi;

    my ($wc, $bindvals, $isdef) = $self->seqname_where_clause
        ( $val, undef, $args->{DEFINED} );
    my $sql     = "SELECT name_id FROM seqname WHERE $wc";
    my $sth     = $dbi->prepare
        ( -sql     => $sql,
          -level   => 3,
          -name    => "Retrieve data for a seqname",
          -limit   => $args->{LIMIT}, );
    my @rv = $sth->get_array_for_field( @{$bindvals} );
    return wantarray ? @rv : \@rv;
}

sub get_seq {
    my $self = shift;
    unshift @_, '-name' if ($#_ == 0 );
    my $args = $self->parseparams( -name      => undef,
				   -nocreate  => 0,
				   @_);
    my $val = $args->{NAME} || $args->{ID} || $args->{SEQ};
    if (!$val) {
        # No request made
        return wantarray ? () : undef;
    } elsif ($self->_safe_isa($val,'BMS::MapTracker::Seqname')) {
        # Request is already a Seqname object
        return wantarray ? ($val) : $val;
    }

    my $cache = $self->{OBJECTS}{seqnames};
    if (exists $cache->{$val}) {
        # We have already recovered this request
	return wantarray ? @{$cache->{$val}} : $cache->{$val}[0];
    }

    # Ok, we need to go look for a database entry
    $self->benchstart;
   # my ($clean_name, $space, $case_override) = $self->strip_tokens($val);
   # my $sqlval  = $clean_name;
    my $dbi     = $self->dbi;

    my ($wc, $bindvals, $isdef) = $self->seqname_where_clause
        ( $val, undef, $args->{DEFINED} );
    my $makeNew = ( ($isdef || $args->{CREATE}) 
                    && !$dbi->readonly && !$args->{NOCREATE}) ? 1 : 0;
    my $sql     = "SELECT name_id, seqname, space_id FROM seqname WHERE $wc";
    my $lim     = $args->{LIMIT} || 0;
    my $sth     = $self->{KEPT_STH}{$sql."\t$lim"} ||= $dbi->prepare
        ( -sql     => $sql,
          -level   => 3,
          -name    => "Retrieve data for a seqname",
          -limit   => $lim, );
    # $sth->bind_param(1, undef, TEXT);


    #$sth->pretty_print( @{$bindvals} ); warn `date`;
    $sth->execute( @{$bindvals} );
    my $rows = $sth->fetchall_arrayref();

    my @retval = ();
    if ($#{$rows} > -1) {
	# At least one hit was found
	foreach my $row (@{$rows}) {
	    my $seq = BMS::MapTracker::Seqname->new
		( -id      => $row->[0],
		  -name    => $row->[1],
                  -space   => $self->get_space($row->[2]),
		  -tracker => $self, );
	    push @retval, $seq;
	}
    } elsif ( $makeNew ) {
        # We enter this block if:
	# No hits were found
        # The user has write permissions
        # No wild cards were used
        # -nocreate was false
        # Request was not an integer
        # Case override was undef or 1
        my ($clean_name, $space, $case_override) = $self->strip_tokens($val);
        if ($clean_name =~ /[\n\t\r]/) {
            $self->err("Can not make new seuqence entry that contains ".
                         "newlines, tabs, or returns: <pre>$clean_name</pre>");
            return wantarray ? () : undef;
        }

	$dbi->begin_work;
        $dbi->named_sth("Lock seqname exclusively")->execute();

	# We need to verify that the sequence has not since been added
	# by another process while we were doing the above logic - as
	# can happen when multiple forks are bulk-loading data. This
	# repeated DB access is inefficient when new data is being
	# added, but is more efficient than locking the table at the
	# outset of the subroutine, so should result in better
	# performance for day-to-day usage.
	
	my ($id, $name, $nsid) = $dbi->selectrow_array
            ( -sql     => $sql,
              -level   => 3,
              -name    => "Recheck seqname prior to insert",
              -bind    => $bindvals );
	if ($id) {
            $space = $self->get_space( $nsid || 1);
        } else {
	    # Nope, still need to make a new entry
            $space ||= $self->get_space( 1 );
            $id = $self->_add_new_seqname( $clean_name, $space );
            # If a namespace was requested, use that
            # Otherwise, use the unassigned namespace (space_id = 1)
            $name = $clean_name;
	}
	$dbi->commit;

	my $seq = BMS::MapTracker::Seqname->new
	    ( -id      => $id,
	      -name    => $name,
              -space   => $space,
	      -tracker => $self, );
	@retval = ($seq);
    }

    # Sort by most recent first, in case user just wants one...
    @retval = sort { $b->id <=> $a->id } @retval;

    if ($self->{CACHESEQ}) {
        # We are allowed to store the returned values in an
        # internal cache object in an internal cache
        if ($self->{OBJ_COUNT}{seqnames} > $self->{CACHESEQ}) {
            # We should clear the internal sequence cache
            # (to prevent memory overrun)
            $cache = $self->{OBJECTS}{seqnames} = {};
            $self->{OBJ_COUNT}{seqnames} = 0;
        }
        $cache->{$val} = \@retval;
        # Make sure we are also caching by name_id:
        map { $cache->{$_->id} = [ $_ ] } @retval;
        $self->{OBJ_COUNT}{seqnames} += $#retval + 1;
    }
    
    $self->benchstop;
    return wantarray ? @retval : $retval[0];
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub destroy_seq {
    my $self = shift;
    my $seq  = $self->get_seq( @_ );
    unless ( $seq ) {
        warn "destroy_seq(): No entries found for [".join(',', @_)."]\n";
        return;
    }
    my $id = $seq->id;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub strip_tokens {
    my $self   = shift;
    my ($text) = @_;
    my ($honor, $space) = (undef, undef);
    while (1) {
        if ($text =~ /^([\!\?]{1,2})(.+)/) {
            # Case-sensitive flags
            # ?foo  => Ignore case
            # !?foo => get all cases except foo
            $text  = $2;
            $honor = $case_codes->{ $1 };
            next;
        }
        if ($text =~ /^\#([^\#]{1,20})\#(.+)/) {
            # Namespace token
            # #GENESYMBOLS#
            # #NONE#
            $text  = $2;
            $space = $self->get_namespace( $1 );
            next;
        }
        last;
    }
    # $honor = $space->case_sensitive if ($space && !defined $honor);
    return ($text, $space, $honor);
}

sub seqname_where_clause {
    my $self = shift;
    my ($val, $tabAlias, $noLike) = @_;
    my ($clean_name, $space, $case_override) = $self->strip_tokens($val);
    my $sqlval  = $clean_name;
    my $dbi     = $self->dbi;
    my @binds   = ();
    my $sql     = "";
    my $isdef   = 0;
    $tabAlias   = $tabAlias ? $tabAlias . '.' : '';

    if ($val =~ /^(\d+)$/ || $val =~ /^MTID:(\d+)$/i) {
        # Integer request assumes direct call for primary key (name_id)
        $sql = $tabAlias . "name_id = ?";
        push @binds, $1;
    } else {
        my $match = '=';
        # We need to match a string
	if (($val =~ /\%/ || $val =~ /\?/) && !$noLike) {
	    # Wildcard match
            $sqlval = $dbi->clean_like_query( $sqlval );
            if ($sqlval =~ /^[\%\_]*$/) {
                $sqlval = "Refusing to recover all DB entries via '$sqlval'";
            } else {
                $match  = 'LIKE';
            }
	} else {
            # Unique name - the request 'is defined', and could be
            # written into the seqname table later if needed.
            $isdef  = 1 
	}
        # Primary search is against upper() of name:
	$sql .= "upper(".$tabAlias."seqname) $match ?";
        push @binds, uc($sqlval);
        if (my $rtype = ref($sqlval)) {
            # Oops, $sqval is not a string
            warn "SeqName request via $rtype:\n".&stack_trace();
            $isdef = 0;
        }
        my $case_match;
        if (defined $case_override) {
            if ($case_override < 0) {
                # Request to get everything EXCEPT this case
                $case_match = ($match eq '=') ? '!=' : 'NOT LIKE';
                # An exclusionary search should not trigger object creation:
                $isdef = 0; 
            } elsif ($case_override > 0) {
                # Request to get a specific match
                $case_match = $match;
                # We do not change $makeNew - this is a specific search, so
                # we can still generate a new entry if otherwise ok
            }
        }
        if ($space) {
            # If a specific namespace is requested, filter on it and children:
            $sql .= " AND ".$tabAlias."space_id IN " . $space->all_ids_sql;
            if ($sqlval =~ /^(ARRAY|HASH)\(/) {
                # Oops, $sqval is not a string
                warn "SeqName request via $sqlval:\n".&stack_trace();
                $isdef = 0;
            }
            if ($space->case_sensitive) {
                # This namespace is case-sensitive
                if (defined $case_override && $case_override != 1) {
                    # The user is over-riding search parameters
                    # Inappropriate to populate the database with what looks
                    # like just a query, unless it is for a specific case
                    $isdef = 0;
                } else {
                    # The user is not over-riding case sensitivity
                    $case_match = $match;
                }
            }
        }
        if ($case_match) {
            # We also need to check case sensitivity:
            $sql .= " AND ".$tabAlias."seqname $case_match ?";
            push @binds, $sqlval;
        }
    }
    return ($sql, \@binds, $isdef);
}

sub seqname_where_clause_complex {
    my $self = shift;
    my ($req, $seqnameTableAlias, $noLike) = @_;
    return () unless ($req);
    my $sidwhere;
    my $sidbvs  = [];
    my $usedAli = 0;
    if ($req =~ /^(\d+)$/ || $req =~ /^MTID:(\d+)$/i) {
        # Simple integer ID
        $sidwhere = "= ?";
        $sidbvs = [ $1 ];
    } elsif (ref($req) || $self->dbi->inefficient_subselect ) {

        # Either the user has passed a sequence object or an explicit
        # list of requests, or the database being used shows
        # poor performance on more complex queries

        my @names = $self->param_to_list($req, 'sequence');
        if ($#names < 0) {
            # No matches
            return ();
        }
        my $slist = join(",",sort {$a <=> $b} map {$_->id} @names);
        if ($#names > 0) {
            # Multiple hits
            $sidwhere = "IN ($slist)";
        } else {
            # Single entity
            $sidwhere = "= ?";
            $sidbvs = [ $slist ];
        }
    } else {
        # Non-integer string, could be wildcard
        # warn "($req, $seqnameTableAlias, $noLike) ";
        my $wc;
        ($wc, $sidbvs) = 
            $self->seqname_where_clause( $req, $seqnameTableAlias, $noLike);
        if ($seqnameTableAlias) {
            $sidwhere = "= $seqnameTableAlias.name_id AND $wc";
            $usedAli  = 1;
        } else {
            $sidwhere = "IN (SELECT name_id FROM seqname WHERE $wc)";
        }
    }
    return ($sidwhere, $sidbvs, $usedAli);
}


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 get_namespace

 Title   : get_namespace
 Usage   : my $namespace = $mt->get_namespace($id)
 Function: Gets a Namespace object for an $id or name.
 Returns : The namespace requested, or undef if no match found.
 Args    : [0] The request, either the name or integer ID

=cut

*get_space = \&get_namespace;
sub get_namespace {
    my $self  = shift;
    my ($val) = @_;
    if (!$val) {
        # No request made
        return undef;
    } elsif ($self->_safe_isa($val,'BMS::MapTracker::Namespace')) {
        # Request is already a Namespace object
        return $val;
    }
    # Strip out flanking '#' signs (used to tokenize namespaces):
    if ($val =~ /^[\?\!]*\#(.+)\#$/) { $val = $1 };
    my $cache_key = uc($val);
    my $cache = $self->{OBJECTS}{namespaces};
    if (exists $cache->{$cache_key}) {
        # We have already recovered this request
	return $cache->{$cache_key};
    }
    
    # We need to get the request from the database:
    $self->benchstart;
    my $sthkey = ($cache_key =~ /^\d+$/) ?'ID' : 'name';
    my $sth = $self->dbi->named_sth("Retrieve namespace by $sthkey");

    my $rows = $sth->selectall_arrayref( $cache_key );

    if ($#{$rows} > 0) {
        # Ooops. Multiple hits
        $self->death
            ("DB error: search for namespace '$val' yields multiple hits");
    } elsif ($#{$rows} < 0) {
        # Nothing in namespace at all
        $self->benchstop;
        return undef;
    }
    my ($id, $name, $desc, $cs) = @{$rows->[0]};

    my $space = BMS::MapTracker::Namespace->new
        ( -id        => $id,
          -name      => $name,
          -desc      => $desc,
          -sensitive => $cs,
          -tracker   => $self);
    $cache->{uc($name)} = $space;
    $cache->{$id} = $space;
    $self->{OBJ_COUNT}{namespaces}++;
    $self->benchstop;
    return $space;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 make_namespace

 Title   : make_namespace
 Usage   : my $namespace = $mt->make_namespace($id)
 Function: Creates a new namespace
 Returns : The namespace object
 Args    : Associative array of arguments. Recognized keys [Default]:

     -name A short name for the namespace

     -desc A brief description of what is contained in this namespace

 -sensitive Case-sensitive flag - if true, then the namespace is
            case-sensitive.

=cut

*make_space = \&make_namespace;
sub make_namespace {
    my $self  = shift;
    my $args = $self->parseparams( -name => undef,
				   @_);
    $self->benchstart;
    my $dbi   = $self->dbi;
    $dbi->begin_work;
    $dbi->named_sth("Lock namespace exclusively")->execute();
    my $space = $self->_add_new_namespace
        ( $args->{NAME}, $args->{DESC}, $args->{SENSITIVE});
    $dbi->commit;
    $self->benchend;
    return $space;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 

 Title   : get_all_namespaces
 Usage   : $self->get_all_namespaces()
 Function: Gets all namespaces in the DB.
 Returns : An array of Namespace objects.
 Args    : 

=cut

sub get_all_namespaces {
    my $self = shift;
    unless ($self->{ALL_NAMESPACE_IDS}) {
	my @ids = $self->dbi->named_sth("Get all namespaces")
            ->get_array_for_field();
	$self->{ALL_NAMESPACE_IDS} = \@ids;
    }
    my @spaces = ();
    foreach my $sid ( @{$self->{ALL_NAMESPACE_IDS}} ) {
        my $u = $self->get_namespace($sid);
        push @spaces, $u if ($u);
    }
    return @spaces;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 

 Title   : get_all_searchdbs
 Usage   : $self->get_all_searchdbs()
 Function: Gets all searchdbs in the DB.
 Returns : An array of Authority objects.
 Args    : 

=cut

sub get_all_searchdbs {
    my $self = shift;
    unless ($self->{ALL_SDBS}) {
	my @ids = $self->dbi->get_array_for_field
            (-sql   => "SELECT db_id FROM searchdb",
             -name  => "Get all searchdb IDs" ,
             -level => 3, );
	$self->{ALL_SDB_IDS} = \@ids;
    }
    my @sdbs = ();
    foreach my $id ( @{$self->{ALL_SDB_IDS}} ) {
        my $u = $self->get_searchdb($id);
        next unless ($u);
        push @sdbs, $u;
    }
    return @sdbs;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 make_searchdb

 Title   : make_searchdb
 Usage   : $self->make_searchdb($id)
 Function: Gets / creates a Searchdb object for an $id or name.
 Returns : 
 Args    : 

=cut

*get_sdb = \&get_searchdb;
sub get_searchdb {
    my $self = shift;
    unshift @_, '-name' if ($#_ == 0 );
    return $self->make_searchdb( @_,
                                 -nocreate => 1);
}

*make_sdb = \&make_searchdb;
sub make_searchdb {
    my $self = shift;
    unshift @_, '-name' if ($#_ == 0 );
    my $args = $self->parseparams( -nocreate => 0,
				   @_);
    my $val = $args->{NAME} || $args->{ID};
    return undef unless ($val);
    if (ref($val)) {
        return $val if ($self->_safe_isa($val, 'BMS::MapTracker::Searchdb'));
        $self->death("You can not request a SearchDB object with '$val'");
    }
    $val = substr($val, 0, 100);
    $val =~ s/\\.//g;    # Remove escapes
    $val =~ s/\\$//;     # Remove terminal backslash
    $val =~ s/\'/\'\'/g; # Escape quotes
    my $uc_val = uc($val);
    my $cache = $self->{OBJECTS}{searchdbs};
    if (exists $cache->{$uc_val}) {
	return $cache->{$uc_val};
    }
    $self->benchstart;
    my $dbi = $self->dbi;
    my $createNewEnrty = 0;
    my $sth;
    if ($val =~ /^\d+$/) {
        $sth = $dbi->named_sth("Retrieve searchdb by ID");
    } else {
        $sth = $dbi->named_sth("Retrieve searchdb by name");
	$createNewEnrty = !$self->{READONLY};
    }
    my $rows = $sth->selectall_arrayref( $uc_val );

    my $sdb = undef;
    if ($#{$rows} == 0) {
	# Unique match found
	my $row = $rows->[0];
	$sdb = BMS::MapTracker::Searchdb->new
	    ( -id      => $row->[0],
	      -name    => $row->[1],
	      -type    => $row->[2],
	      -path    => $row->[3],
	      );
    } elsif ( $#{$rows} > 0 ) {
	$self->death("DB fail - multiple Searchdbs found for '$val'");
    } elsif ( $createNewEnrty && !$args->{NOCREATE} ) {
	# No hits were found, the user can write, no wild cards used
	# and the request is for a name... Just make a new entry
	$dbi->begin_work;
        $dbi->named_sth("Lock searchdb exclusively")->execute();

	# We need to verify that the sequence has not since been added
	# by another process while we were doing the above logic - as
	# can happen when multiple forks are bulk-loading data. This
	# repeated DB access is inefficient when new data is being
	# added, but is more efficient than locking the table at the
	# outset of the subroutine, so should result in better
	# performance for day-to-day usage.
	
	my ($id, $name, $type, $path) = $sth->selectrow_array( $uc_val );

	unless ($id) {
	    # Nope, still need to make a new entry
            ($id, $name, $type, $path) = 
                ($dbi->nextval('searchdb_seq'),
                 $args->{NAME}, $args->{TYPE}, $args->{PATH}, );
            my $add = $dbi->named_sth("Create a new searchdb entry");
            $add->execute($id, $name, $type, $path);
	}
	$dbi->commit;
	$sdb = BMS::MapTracker::Searchdb->new
	    ( -id      => $id,
	      -name    => $name,
	      -type    => $type,
	      -path    => $path,
	      );
    } elsif ($args->{NOCREATE}) {
        $self->err("-nocreate flag prevents creation of searchDB '$val'")
            unless ($args->{QUIET});
    } else {
        $self->death("You do not have permissions to make_searchdb($val)");
    }
    $cache->{$uc_val} = $sdb;
    $self->{OBJ_COUNT}{searchdbs}++;
    $self->benchstop;
    return $sdb;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 get_mappings

 Title   : get_mappings
 Usage   : $self->get_mappings(@args)
 Function: Gets mappings for a sequence, or between two sequences
 Returns : 
 Args    : Associative array of arguments. Recognized keys [Default]:

    -name1 (if passing just one name, can use -name instead)
    -name2 At least one of these fields is required. If only one is
           provided, all mappings for that sequence will be retrieved,
           otherwise, only those between name1 and name2 will be
           recovered.

    -mapid A simple map_id can be used to retrieve one or more maps.

  -include Optional array ref of [start, stop]. You must also provide
           name1 (-name2 will be ignored), only mappings for that name
           that are completely contained within the range will be
           returned.

  -overlap Optional array ref of [start, stop]. Similar to -include,
           but will also get any mappings that overlap the specified
           range.

 -authority Optional authority. Only mappings for that authority would
           then be returned.

 -searchdb Optional searchdb. Only mappings from that search db will
           be used. An array reference of SDBs can also be
           provided. Alias -sdb or -db

   -oneway Default 0. The mapping table contains name1 and name2
           columns, but it is assumed that argument -name1 could match
           to either of the table columns. If -oneway is true, then
           -name1 will be checked ONLY against column name1 (likewise
           for -name2).

    -limit Default 100. If set, will limit the number of mappings
           returned. This is to prevent overload when searching for
           names like 'Chr1'. Set to zero for no limits.

 -keepclass

 -tossclass Optional Class object or array reference of such. Useful
            only if a single name is being used - the classes of the
            other name in the mapping will be compared to those
            provided, and the mapping will be kept (or tossed) if at
            least one of the classes matches.

  -edgeonly Default 0. If true, then only the 'left' and 'right' edge
            positions will be recovered. This is a very rapid method
            to get map data, but will obviously provide much less
            information. Designed to get data for whole chromosomes.

  -minscore Optional hash reference of minimum scores to use as
            cutoff. The keys of the hash are names (or parts of names)
            of authorities you wish to filter, while the values are
            the minimum score needed to keep a map.

     -quiet Do not complain about too many mappings returned

=cut

sub get_mappings {

    # (old) Postgres notes: There is a btree index for name1,
    # name2. However, postgres consistently ignores this index and
    # reverts to seq scan

    my $self = shift;
    $self->benchstart;
    my $args = $self->parseparams( -name1     => undef,
				   -name      => undef,
				   -name2     => undef,
				   -oneway    => 0,
				   -authority => 0,
				   -limit     => 100,
				   -keepclass => [],
				   -tossclass => [],
				   -mapid     => undef,
				   -edgeonly  => 0,
				   -minscore  => 0,
                                   -overlap   => 0,
				   @_ );

    my @tables = ('mapping m');
    my ($id1, $id2);
    my ($bv1, $bv2) = ([], []);
    if (my $seq1 = $args->{NAME1} ||= $args->{NAME}) {
        my $tabAli = 'sn' . ($#tables+1);
        my ($sidwhere, $sidbvs, $aliUsed) = 
            $self->seqname_where_clause_complex
            ($seq1, $tabAli, $args->{DEFINED});
        if ($sidwhere) {
            push @tables, "seqname $tabAli" if ($aliUsed);
            $id1 = $sidwhere;
            push @{$bv1}, @{$sidbvs};
        }
    }
    if (my $seq2 = $args->{NAME2}) {
        my $tabAli = 'sn' . ($#tables+1);
        my ($sidwhere, $sidbvs, $aliUsed) = 
            $self->seqname_where_clause_complex
            ($seq2, $tabAli, $args->{DEFINED});
        if ($sidwhere) {
            push @tables, "seqname $tabAli" if ($aliUsed);
            $id2 = $sidwhere;
            push @{$bv2}, @{$sidbvs};
        }
    }
    my $getwhat = "Mappings";
    
    my $seqOnly = $args->{SEQIDONLY};
    my $oneway  = $args->{ONEWAY};
    my $mapid   = $args->{MAP_ID} || $args->{MAPID};
    unless ($id1 || $id2 || $mapid) {
        if ($args->{NAME1} || $args->{NAME2}) {
            # The user requested maps for specific sequences not in DB
            $self->benchstop;
            return ();
        }
        $self->death("Must define -name1 or -name2 to retrieve mappings");
    }

    # Determine any class restrictions
    my %toss = $self->ids_in_param( $args->{TOSSCLASS}, 'class' );
    my %keep = $self->ids_in_param( $args->{KEEPCLASS}, 'class' );

    my %ktax = $self->ids_in_param( $args->{KEEPTAXA}, 'taxa' );

    my $totoss  = join(',', sort {$a <=> $b} keys %toss);
    my $tokeep  = join(',', sort {$a <=> $b} keys %keep);
    my $taxKeep = join(',', sort {$a <=> $b} keys %ktax);
    my $limit   = $args->{LIMIT};
    my @mapcols = ('map_id', 'name1', 'start1', 'end1', 'name2', 'start2', 
		   'end2', 'trans_id', 'authority_id', 'map_score', 'strand',
		   'db_id');
    my @tabcols = map { "m.$_" } ($seqOnly ? ('name1', 'name2') : @mapcols);
    my @w = ("", "");
    my @binds = ( [], [] );
    if ($id1 && $id2) {
	$w[0] = "m.name1 $id1 AND m.name2 $id2";
        push @{$binds[0]}, ( @{$bv1}, @{$bv2} );
        unless ($oneway) {
            $w[1] = "m.name1 $id2 AND m.name2 $id1";
            push @{$binds[1]}, ( @{$bv2}, @{$bv1} );
        }
    } elsif ($args->{OVERLAP}) {
	unless (ref($args->{OVERLAP}) eq 'ARRAY') {
	    $args->{OVERLAP} = [ split(/[\r\n\s\,]+/, $args->{OVERLAP}) ];
	}
	my ($start, $stop) = @{$args->{OVERLAP}};
	$w[0] = "m.name1 $id1 AND m.start1 <= ? AND m.end1 >= ?";
        push @{$binds[0]},( @{$bv1}, $stop, $start );
	$w[1] = "m.name2 $id1 AND m.start2 <= ? AND m.end2 >= ?";
        push @{$binds[1]},( @{$bv1}, $stop, $start );
        $getwhat .= ", overlapping from $start to $stop";
    } elsif ($args->{INCLUDE}) {
	unless (ref($args->{INCLUDE}) eq 'ARRAY') {
	    $args->{INCLUDE} = [ split(/[\r\n\s\,]+/, $args->{INCLUDE}) ];
	}
	my ($start, $stop) = @{$args->{INCLUDE}};
	$w[0] = "m.name1 $id1 AND m.start1 >= ? AND m.end1 <= ?";
        push @{$binds[0]},( @{$bv1}, $start, $stop );
	$w[1] = "m.name2 $id1 AND m.start2 >= ? AND m.end2 <= ?";
        push @{$binds[1]},( @{$bv1}, $start, $stop );
        $getwhat .= ", including $start to $stop";
    } elsif ($id1) {
	$w[0] = "m.name1 $id1";
        push @{$binds[0]}, @{$bv1};
        unless ($oneway) {
            $w[1] = "m.name2 $id1";
            push @{$binds[1]}, @{$bv1};
        }
    } elsif ($id2) {
	$w[0] = "m.name2 $id2";
        push @{$binds[0]}, @{$bv2};
        unless ($oneway) {
            $w[1] = "m.name1 $id2";
            push @{$binds[1]}, @{$bv2};
        }
    }
    unless ($id2) {
        my $classfrm = ' AND %sEXISTS ( SELECT name_id FROM seq_class WHERE '.
            'name_id = m.name%s AND class_id IN (%s))';
        my $taxfrm = ' AND %sEXISTS ( SELECT name_id FROM seq_species WHERE '.
            'name_id = m.name%s AND tax_id IN (%s))';
        if ($tokeep) {
            $w[0] .= sprintf($classfrm, '','2', $tokeep);
            $w[1] .= sprintf($classfrm, '','1', $tokeep);
        }
        if ($totoss) {
            $w[0] .= sprintf($classfrm, 'NOT ','2', $totoss);
            $w[1] .= sprintf($classfrm, 'NOT ','1', $totoss);
        }
        if ($taxKeep) {
            $w[0] .= sprintf($taxfrm, '','2', $taxKeep);
            $w[1] .= sprintf($taxfrm, '','1', $taxKeep);
        }
    }
    if (my $minScoreReq = $args->{MINSCORE}) {
	my @false = ();
        if (ref($minScoreReq)) {
            # The user is requesting specific cutoffs for specific
            # authorities
            while( my ($namematch, $score) = each %{$minScoreReq}) {
                my @auths = $self->get_authority( '%' . $namematch.'%');
                my @aids  = map { $_->id } @auths;
                if ($#aids > -1) {
                    my $aidtest = $#aids == 0 ? "= $aids[0]" :
                        "IN (" . join(",", @aids) . ")";
                    push @false, sprintf
                        ("(m.authority_id %s AND m.map_score < %f)",
                         $aidtest, $score);
                }
            }
        } else {
            # The user is just passing a single value
            push @false, "(m.map_score < " . $minScoreReq . ")";
        }
	if ($#false > -1) {
	    my $test = join(" AND NOT ", @false);
	    $w[0] .= " AND NOT $test";
	    $w[1] .= " AND NOT $test";
	}
    }
    if ($mapid) {
	unless ( ref($mapid) eq 'ARRAY' ) {
	    $mapid = [ split(/[\s\r\n\,]+/, $mapid) ];
	}
	if ($#{$mapid} > -1) {
	    @w = ( "m.map_id in (".join(',', @{$mapid}).")" );
            @binds = ([]);
	}
    }

    if (my $sreq = $args->{SEARCHDB} || $args->{SDB} || $args->{DB} ) {
        my @reqs = (ref($sreq) && ref($sreq) eq 'ARRAY') ? @{$sreq} : ($sreq);
        my %dsids;
        foreach my $req (@reqs) {
            my $sdb = $self->get_searchdb( $req );
            $dsids{$sdb->id} = 1 if ($sdb);
        }
        my @sdbs = keys %dsids;
        my $sdbsql = "";
        if ($#sdbs == 0) {
            $sdbsql = " AND m.db_id = $sdbs[0]"
        } elsif ($#sdbs > 0) {
            $sdbsql = " AND m.db_id IN (". join(",", @sdbs).")";
        }
        if ($sdbsql) {
            for my $i (0..$#w) {
                $w[$i] .= $sdbsql;
            }
        }
    }

    if (my $areq = $args->{AUTHORITY}) {
        my @reqs = ref($areq) ? @{$areq} : ($areq);
        my @aids;
        foreach my $req (@reqs) {
            if (my $auth = $self->get_authority($req)) {
                push @aids, $auth->id;
            }
        }
        unless ($#aids == -1) {
            my $wh = " AND m.authority_id ".(($#aids == 0) ? 
                "= $aids[0]" : "IN (".join(',', @aids).")");
            for my $i (0..$#w) {
                $w[$i] .= $wh;
            }
        }
    }

    
    my @toUnionize = ();
    foreach my $where (@w) {
	push @toUnionize, 
	sprintf("SELECT %s FROM %s WHERE %s", 
		join(", ", @tabcols), join(",", @tables), $where) if ($where);
    }
    my @allbinds = map { @{$_} } @binds;

    my $sql = join(" UNION ", @toUnionize);
    $sql .= " ORDER BY map_score DESC" unless ($seqOnly);
    my $dbi = $self->dbi;

    my $sth = $dbi->prepare
        ( -sql   => $sql,
          -limit => $limit,
          -name  => "Select Mappings for given criteria" ,
          -level => 3 );
    # $sth->pretty_print( @allbinds );
    $sth->execute( @allbinds );
    my $rows = $sth->fetchall_arrayref();
   #$dbi->dumpsql(3);

    #my $rows = $dbi->selectall_arrayref
    #    (-sql   => $sql,
    #     -bind  => \@allbinds,
    #     -limit => $limit,
    #     -name  => "Select Mappings for given criteria" ,
    #     -level => 3, );
    
    if ($seqOnly) {
        my %ids = map { $_ => 1 } map { @{$_} } @{$rows};
        $self->benchstop;
        return keys %ids;
    }

    my @mappings;
    foreach my $row (@{$rows}) {
	my %data = ( -tracker => $self );
	for my $i (0..$#mapcols) {
	    $data{ $mapcols[$i] } = $row->[$i];
	}
	$data{authority} = $self->get_authority( $data{authority_id} );
	if ($args->{EDGEONLY}) {
	    # Just use the boundary positions for the locations:
	    $data{locations} = [ [$data{start1}, $data{start1}, 
				  $data{start2}, $data{start2},],
				 [$data{end1}, $data{end1}, 
				  $data{end2}, $data{end2},],];
	} else {
	    $data{locations} = 
                $dbi->named_sth("Select Locations for a Mapping")->
                selectall_arrayref( $data{map_id} );
	}
	$data{searchdb} = $self->make_searchdb( $data{db_id} );
	my $map = BMS::MapTracker::Mapping->new( %data );
	push @mappings, $map;
    }

    if ($limit && $#mappings + 1 > $limit && !$args->{QUIET}) {
        my @keepers = $self->param_to_list( $args->{KEEPCLASS}, 'class' );
        $getwhat .= ", keeping class ".join(", ", map {$_->name} @keepers)
            if ($#keepers > -1);
        my @tossers = $self->param_to_list( $args->{TOSSCLASS}, 'class' );
        $getwhat .= ", discarding class ".join(", ", map {$_->name} @tossers)
            if ($#tossers > -1);
	my $msg = sprintf
	    ("Your search for %s was limited to %d hits - there are more mappings present than your limit!", $getwhat, $limit);
        pop @mappings; # Probably would not hurt to keep the extra one
	$self->err($msg, 37);
    }
    $self->benchstop;
    return @mappings;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 get_transform

 Title   : get_transform
 Usage   : $self->get_transform($id)
 Function: Gets a Transform object for an $id or name.
 Returns : 
 Args    : 

=cut

sub get_transform {
    my $self = shift;
    my ($val) = @_;
    return $val if (!$val || 
                    $self->_safe_isa($val, 'BMS::MapTracker::Transform'));
    my $cache = $self->{OBJECTS}{transforms};
    return $cache->{$val} if (exists $cache->{$val});
    $self->benchstart;
    my $sql = 
        "SELECT trans_id, transname, step1, step2 FROM transform WHERE ";
    if ($val =~ /^\d+$/) {
	$sql .= "trans_id = ?";
    } else {
	$sql .= "transname = ?";
    }
    my ($id, $name, $s1, $s2) = $self->dbi->selectrow_array
        (-sql   => $sql,
         -bind  => [ $val ],
         -name  => "Get mapping transform" ,
         -level => 3, );
    #warn $sql;
    $self->death("Could not find a transform for '$val'") 
        unless (defined $s1);
    my $transform = BMS::MapTracker::Transform->new
	( -step1 => $s1, -step2 => $s2, -id => $id, -name => $name);
    $cache->{$id} = $cache->{$name} = $transform;
    $self->{OBJ_COUNT}{transforms}++;
    $self->benchstop;
    return $transform;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 get_authority

 Title   : get_authority
 Usage   : $self->get_authority($id)
 Function: Gets an Authority object for an $id or name.
 Returns : 
 Args    : 

=cut

*get_auth = \&get_authority;
sub get_authority {
    my $self = shift;
    my ($val) = @_;
    return undef unless ($val);
    if ($self->_safe_isa($val, 'BMS::MapTracker::Authority')){
        return $val;
    }
    $val = uc($val);
    my $cache = $self->{OBJECTS}{authorities};
    if (exists $cache->{$val}) {
	return wantarray ? @{$cache->{$val}} : $cache->{$val}[0];
        # if ($cache->{$val}[0]);
    }
    my $sql = "SELECT authority_id, authname, descr FROM authority WHERE ";
    if ($val =~ /^\d+$/) {
	$sql .= "authority_id = ?";
    } elsif ($val =~ /\%/) {
	$sql .= "upper(authname) LIKE ?";
    } else {
	$sql .= "upper(authname) = ?";
    }
    my @auths;
    my $sth = $self->dbi->prepare
        (-sql   => $sql,
         -name  => "Get information for authority" ,
         -level => 3, );
    # warn $sth->pretty_print( $val );
    my $rows = $sth->selectall_arrayref( $val );
    # warn "$sql + '$val'";
    #my $rows  = $self->dbi->selectall_arrayref
    #    (-sql   => $sql,
    #     -bind  => [ $val ],
    #     -name  => "Get information for authority" ,
    #     -level => 3, );
    # warn "[$#{$rows} via $val] $sql";
    foreach my $row (@{$rows}) {
        my ($id, $name, $desc) = @{$row};
        # warn "($id, $name, $desc)";
        next unless (defined $id);
        my $authority = BMS::MapTracker::Authority->new
            ( -desc => $desc, -id => $id, -name => $name);
        push @auths, $authority;
        $cache->{$id} = $cache->{ uc($name) } = [ $authority ];
        $self->{OBJ_COUNT}{authorities}++;
    }
    $cache->{$val} = \@auths;
    return wantarray ? @auths : $auths[0];
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 

 Title   : get_all_authorities
 Usage   : $self->get_all_authorities()
 Function: Gets all authorities in the DB.
 Returns : An array of Authority objects.
 Args    : 

=cut

sub get_all_authorities {
    my $self = shift;
    unless ($self->{ALL_AUTHORITY_IDS}) {
	my $sql = "SELECT authority_id FROM authority";
        my @ids  = $self->dbi->get_array_for_field
            (-sql   => $sql,
             -name  => "Get all authority IDs" ,
             -level => 3 );
	$self->{ALL_AUTHORITY_IDS} = \@ids;
    }
    my @auths = ();
    foreach my $aid ( @{$self->{ALL_AUTHORITY_IDS}} ) {
	my $u = $self->get_authority($aid);
	next unless ($u);
	push @auths, $u;
    }
    return @auths;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 make_authority

 Title   : make_authority
 Usage   : $self->make_authority($name, $desc)
 Function: Makes an Authority object if it does not already exist.
 Returns : The Authority object
 Args    : Two strings, a name and a description

=cut

sub make_authority {
    my $self = shift;
    $self->benchstart;
    my ($name, $desc) = @_;
    my $search = $self->get_authority($name);
    if ($search) {
	$self->benchstop;
	return $search;
    }
    $self->death("You lack write privaleges to make_authority() for '$name'") 
	if ($self->{READONLY});
    my $auth = BMS::MapTracker::Authority->new
	( -desc => $desc, -name => $name);
    $auth->write( $self );
    my $cache = $self->{OBJECTS}{authorities};
    my $id = $auth->id;
    $cache->{$id} = $cache->{$name} = [ $auth ];
    $self->{OBJ_COUNT}{authorities}++;
    $self->benchstop;
    return $auth;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 get_all_classes

 Title   : get_all_classes
 Usage   : print $self->get_all_classes
 Function: Returns every known Class object
 Returns : An array of Class objects
 Args    : 

=cut

sub get_all_classes  {
    my $self = shift;
    unless ($self->{FULL_CLASS_LIST}) {
        $self->benchstart;
        my $sql = 
            "SELECT class_id, parent_id, seqclass, descr, class_name".
            "  FROM class_list ORDER BY class_id";
        my $rows  = $self->dbi->selectall_arrayref
            (-sql   => $sql,
             -name  => "Select all class data" ,
             -level => 3 );
        my $cache = $self->{OBJECTS}{classes};
        my %parentage;
        my @classes;
        foreach my $row (@{$rows}) {
            my ($id, $pid, $token, $desc, $name) = @{$row};
            unless ($cache->{$id}) {
                $cache->{$id} = $cache->{$token} = $cache->{uc($name)} =
                    BMS::MapTracker::Class->new
                    ( -parent  => $pid,  -id     => $id,
                      -name    => $name, -desc   => $desc,
                      -tracker => $self, -token  => $token, );
            }
            $parentage{ $id } = $pid;
            push @classes, $cache->{$id};
        }
        # Add children to their parents:
        while (my ($id, $pid) = each %parentage) {
            next unless ($id && $pid != $id);
            next unless (exists $cache->{$id} && exists $cache->{$pid});
            $cache->{$pid}->add_child($cache->{$id});
            $self->{OBJ_COUNT}{classes}++;
        }
        $self->{FULL_CLASS_LIST} = \@classes;
        $self->benchstop;
    }
    return @{$self->{FULL_CLASS_LIST}};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 get_class

 Title   : get_class
 Usage   : $self->get_class($id)
 Function: Gets a Class object for an $id or name.
 Returns : 
 Args    : 

=cut

sub get_class {
    my $self = shift;
    my ($val) = @_;
    return undef unless (defined $val);
    return $val if ($self->_safe_isa($val, 'BMS::MapTracker::Class'));
    my $ucval = uc($val);
    my $cache = $self->{OBJECTS}{classes};
    if (exists $cache->{$ucval}) {
	return $cache->{$ucval};
    }
    $self->benchstart;
    my $sql = "SELECT class_id, parent_id, seqclass, descr, class_name ".
	"  FROM class_list WHERE ";
    my @binds;
    if ($ucval =~ /^\d+$/) {
	$sql .= "class_id = ?";
        @binds = ($ucval);
    } else {
	$sql .= "seqclass = ? OR upper(class_name) = ?";
        @binds = ($ucval, $ucval);
    }
    my $dbi = $self->dbi;
    my ($id, $pid, $token, $desc, $name) = $dbi->selectrow_array
        (-sql   => $sql,
         -level => 3,
         -name  => "Get class data",
         -bind  => \@binds );
    #my $foo = $self->get_transform('impossible');# warn $foo;
    # warn $id;
    unless (defined $id) {
	$self->benchstop;
	return undef;
    }
    my $class = BMS::MapTracker::Class->new
	( -parent  => $pid,  -id     => $id,
	  -name    => $name, -desc   => $desc,
	  -tracker => $self, -token  => $token, );
    # Add the children of this class:
    my @kids = $dbi->named_sth("Get children of a class")->
        get_array_for_field( $id );

    foreach my $kidID ( @kids ) {
        # Skip if column is null or equal to the parent
	next if ($kidID == $id || !$kidID);
	my $kidClass = $self->get_class($kidID);
	$class->add_child($kidClass);
    }
    $cache->{$id} = $cache->{$token} = $cache->{uc($name)} = $class;
    $self->{OBJ_COUNT}{classes}++;
    $self->benchstop;
    return $class;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub get_relations_dump {
    my $self = shift;
    $self->death("BMS::MapTracker::get_relations_dump() is deprecated.",
                 "Please use get_edge_dump instead");
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub make_transient_edge {
    my $self = shift;
    my $args = $self->parseparams( -existing => undef,
                                   @_ );
    my $tdat = $self->{TRANSIENTS};
    my $edge;
    my $isnew = 1;
    if (my $exspace = $args->{EXISTING}) {
        # User is requesting to use existing edges, if any
        my $found = $self->get_edge_dump( %{$args},
                                          -space => $exspace );
        $edge = $found->[0];
    }

    unless ($edge) {
        my $check;
        if ($check = $args->{EDGE}) {
            # The user is passing an edge object as the primary query
            if ($check->id =~ /^\d+$/) {
                # This is not a transient edge at all - return as is
                return ($check, 0);
            }
        } else {
            # Make an edge, primarily to parse arguments:
            $check = BMS::MapTracker::Edge->new
                ( %{$args},
                  -id      => 'TEMP:0',
                  -created => $self->db_timestamp,
                  -space   => $args->{SPACE} || 'Transient',
                  -tracker => $self, );
        }
        my $space = $check->space;
        my $key = $space->id . "\t" . $check->hash_key;
        $edge = $tdat->{LOOKUP}{$key};
        if ($edge) {
            # We have encountered this edge previously
            $isnew = 0;
        } else {
            # First time we saw this edge
            $edge     = $tdat->{LOOKUP}{$key} = $check;
            my $nsnm  = $space->name;
            $edge->{ID} = $nsnm . ':' . ++$tdat->{COUNTER}{$nsnm};
            # Store in cache:
            $self->{OBJECTS}{edges}{ $edge->{ID} } = [ $edge ];
            $self->{OBJ_COUNT}{edges}++;
        }
    }

    # If authorities are provided, set time stamps:
    my $now   = $self->db_timestamp;
    my $areq  = $args->{AUTHORITY} || $args->{AUTH};
    my @auths = $self->param_to_list($areq, 'authority' );
    $edge->{HISTORY} = {};
    $edge->{LIVEAUTH} = {};
    $edge->{DEADAUTH} = {};
    foreach my $auth (@auths){
        my $aid = $auth->id;
        push @{$edge->{HISTORY}{$aid}}, [ $now, 't' ];
        $edge->{LIVEAUTH}{$aid} = $auth;
    }
    if ($#auths == 0 && $args->{TAGS}) {
        my $tags = $args->{TAGS};
        foreach my $tag (@{$args->{TAGS}}) {
            $edge->add_tag($auths[0]->id, @{$tag});
        }
    }
    return ($edge, $isnew);
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 get_edge_dump

 Title   : get_edge_dump
 Usage   : $self->get_edge_dump( @args )
 Function: Get edge data for seqnames with minimal object creation
 Returns : A reference to either an array or a hash
 Args    : Associative array of arguments. Recognized keys:

     -name The name (name, id, object) you want to search on, or an
           array ref of them.

    -limit Default 0. If non-zero, the maximum number of edges you
           want to retrieve. This will be applied to each type
           separately!

 -keeptype Default []. Optional type, or array ref of types, that you
           wish to retrieve. If zero or undef, then all types will be
           recovered.

 -keepauth Default []. Optional authority, or array ref of
           authorities, that you wish to retrieve. If zero, then all
           authorities will be recovered.

 -tossauth Default 0. As above, but specifies authorities to ignore.

 -keepclass Default []. Optional class, or array ref of classes, that
            you wish to retrieve. If zero, then all classes will be
            recovered.

 -tossclass Default []. Optional class, or array ref of classes, that
            you wish to exclude. If zero, then all classes will be
            recovered. If -keepclass is also set, then -tossclass will
            be ignored.

    -space Default 1 (None). Optional namespace to limit the search
           to. If you use 'all', then ALL namespaces will be searched.

   -orient Default 0. Normally, each row will be returned as it is
           found in the database:

           [ name1, name2, type_id, edge_id, space_id ]

           If orient is true, then the program will return:

           [ other_id, reads_as, edge_id, query_id, space_id]

           ... where query_id is the name_id that matched your query
           (one of the nodes passed in -name), other_id is the other
           name_id of the edge pair, and reads_as is the English
           string describing the directionality of type_id.

   -return Default 'array' will return an array reference of rows
           recovered, with each row being set as described
           above. Option 'hash' will return the same data, but as a
           hash ref keyed to type_id.

           Option 'object' will generate Edge objects, and will
           override -orient.

 -connects Default undef. Optional single node or array of nodes. If
           provided, then any edge recovered must point to a node that
           also points to those provided by connects. So if your main
           query is NodeA, and you are requesting -connects [NodeB,
           NodeC], then the system will return edges from NodeA to
           NodeX provided:

           NodeA <-> NodeX <-> NodeB   OR  NodeA <-> NodeX <-> NodeC

  -deadtoo Default undef. If true, then non-live edges will be
           included, too.

=cut

*edge_dump = \&get_edge_dump;
sub get_edge_dump {
    my $self = shift;
    $self->benchstart;
    my $tinfo = $self->type_information;
    my $dbi = $self->dbi;
    my $args = $self->parseparams( -name     => [],
				   -keeptype => 0,
				   -keepauth => 0,
                                   -tossauth => 0,
				   -limit    => 0,
				   -return   => 'array',
				   -orient   => 0,
                                   -space    => 1,
                                   -nodistinct => 1,
                                   -connects => undef,
				   @_ );

    my $noDist  = $args->{NODISTINCT} ? 1 : 0;
    my $asObj   = ($args->{RETURN} =~ /obj/i)   ? 1 : 0;
    my $asArray = ($args->{RETURN} =~ /array/i) ? 1 : 0;
    my $deadToo = $args->{DEADTOO} ? 1 : 0;
    my $asName  = ($args->{RETURN} =~ /name/i)  ?
        $dbi->prepare( -sql => "SELECT seqname FROM seqname WHERE name_id = ?",
                       -name => "Fast retrieval of seqname",
                       -level => 3 ) : undef;

    # Flag that tracks if the query will return all possible edges for
    # a given edge type:
    my $isComplete = 1;
    my @seqTables;
    
    my $orient = 0;
    # Basic query parameters:
    my (@name_w, @name_binds);
    # What nodes are we querying on?
    if ($args->{NAME1} || $args->{NAME2}) {
        my (@sids, @binds);
        for my $req ($args->{NAME1}, $args->{NAME2}) {
            next unless ($req);
            my $tabAli = 'sn' . ($#seqTables+1);
            my ($sidwhere,$sidbvs, $aliUsed) = 
                $self->seqname_where_clause_complex
                ($req, $tabAli, $args->{DEFINED});
            unless ($sidwhere) {
                # If we can not find the node anywhere, there is nothing
                # to recover
                $self->benchend;
                return $asArray ? [] : {};
            }
            push @seqTables, $tabAli if ($aliUsed);
            push @sids,  $sidwhere;
            push @binds, $sidbvs;
        }
        if ($#sids == 1) {
            @name_w = (["e.name1 $sids[0] AND e.name2 $sids[1]"], 
                       ["e.name1 $sids[1] AND e.name2 $sids[0]"]);
            @name_binds = ( [ @{$binds[0]}, @{$binds[1]} ],
                            [ @{$binds[1]}, @{$binds[0]} ] );
        } elsif ($args->{NAME1} && $args->{NAME2}) {
            # User specified both items, but at least one is not in DB
            $self->benchend;
            return $asArray ? [] : {};
        } else {
            $self->err
                ("If you specify -name1 or -name2 you must specify both");
        }
        $isComplete = 0;
    } else {
        my $tabAli = 'sn' . ($#seqTables+1);
        my ($sidwhere, $sidbvs, $aliUsed) = 
            $self->seqname_where_clause_complex
            ( $args->{NAME}, $tabAli, $args->{DEFINED} );
        unless ($sidwhere) {
            my $msg = "get_edge_dump() no nodes passed in -name. Args:\n";
            foreach my $key (sort keys %{$args}) {
                my $val = $args->{$key};
                $val = defined $val ? "'$val'" : '-UNDEF-';
                $msg .= sprintf
                    ("  %10s : '%s'\n", $key ? '-'.lc($key) : '-UNDEF-', $val);
            }
            # $self->err($msg);
            $self->benchend;
            return $asArray ? [] : {};
        }
        push @seqTables, $tabAli if ($aliUsed);
        @name_w     = (["e.name1 $sidwhere"], ["e.name2 $sidwhere"]);
        @name_binds = ( [ @{$sidbvs} ], [ @{$sidbvs} ] );
        if ($args->{ORIENT}) {
            # Request to orient returned data to a single query name
            my @sids;
            if ($sidwhere eq '= ?') {
                # We already just have the name_id
                @sids = @{$sidbvs};
            } else {
                # If a table alias was used, we need to regenerate the WHERE
                my ($sw, $bv) = $aliUsed ?$self->seqname_where_clause_complex
                    ( $args->{NAME}, undef, $args->{DEFINED} )
                    : ($sidwhere, $sidbvs);
                
                # warn "SELECT name_id FROM seqname WHERE name_id $sw\n";
                @sids = $dbi->get_array_for_field
                    (-sql => "SELECT name_id FROM seqname WHERE name_id $sw",
                     -level => 3,
                     -name  => "Get user query name_ids for orienting edges",
                     -bind  => $bv );
            }
            $orient = { map { $_ => 1 } @sids };
        }
    }

    my $cxt;
    if (my $cxreq = $args->{CONNECTS} || $args->{CONNECT}) {

        # The user is requesting that any secondary node (one
        # connected to your query) also be connected to a tertiary
        # node ($cxreq). Generate the SQL block for this constraint
        my ($sidbvs, $aliUsed);
        my $tabAli = 'sn' . ($#seqTables+1);
        ($cxt, $sidbvs, $aliUsed) = 
            $self->seqname_where_clause_complex
            ( $cxreq, $tabAli, $args->{DEFINED} );
        if ($cxt) {
            push @seqTables, $tabAli if ($aliUsed);
            my %cxtyp;
            if (my $gt = $args->{CONTYPE}) {
                # The user is also requesting that the edge connecting
                # the tertiary node also be constrained to one or more
                # specific types
                my @requestType  = ref($gt) eq 'ARRAY' ? @{$gt} : ($gt);
                foreach my $t (@requestType) {
                    my ($type, $dir, $space) = $self->get_type($t);
                    $cxtyp{ $dir }{$type->id} = 1;
                }
            } else {
                # No type request made - but we need to put a blank
                # holder in the hash to make sure we iterate once.
                %cxtyp = (0 => {'' => 1 });
            }

            for my $i (0..$#name_w) {

                # $onode is the secondary node, 'ecx' is the edge
                # connecting secondary to tertiary.

                my $onode = "e.name" . (!$i + 1);
                my (@tests, @tbvs);
                # Cycle through each edge request (or the single blank holder)
                while ( my ($dir, $hash) = each %cxtyp) {
                    my @bits;
                    if ($dir > -1) {
                        # Check the forward direction
                        push @bits, "(ecx.name1 = $onode AND ecx.name2 $cxt)";
                        push @{$name_binds[$i]}, @{$sidbvs};
                    }
                    if ($dir < 1) {
                        # Check the reverse direction
                        push @bits, "(ecx.name2 = $onode AND ecx.name1 $cxt)";
                        push @{$name_binds[$i]}, @{$sidbvs};
                    }
                    my $clause = ($#bits < 1) ? 
                        $bits[0] : '('.join(" OR ", @bits).')';

                    my $tids = join(",", sort keys %{$hash});
                    if ($tids) {
                        # Constrain by specific edge types for this direction:
                        my $tsql = "";
                        if ($tids =~ /\,/) {
                            $tsql = "ecx.type_id IN ($tids)";
                        } else {
                            $tsql = "ecx.type_id = ?";
                            push @{$name_binds[$i]}, $tids;
                        }
                        $clause = "($clause AND $tsql)";
                    }
                    push @tests, $clause;
                }
                my $main = ($#tests < 1) ?
                    $tests[0] : '('.join(" OR ", @tests).')';
                $main = "$main AND ecx.live = 't'";
                push @{$name_w[$i]}, $main;
            }
        }
    }

    # Filter variables:
    my ($aid, $cid, $snsid, $tax, $tags) = ("","","","");

    ##############
    # Filter by edge_meta assignments

    my %ems;
    if (my $filt = $args->{FILTER}) {
        my $em;
        ($tags, $em) = $self->_process_tag_filters($filt);
        $em = [''] if ($tags && $#{$em} < 0);
        map { $ems{$_} = 1 } @{$em};
        $isComplete = 0;
    }

    ##############
    # Filter by authorities

    my $eahFrm = '%s EXISTS ( SELECT eah.edge_id FROM edge_auth_hist eah WHERE eah.edge_id = e.edge_id AND eah.authority_id IN (%s) AND eah.live = \'t\')';
    if (my $ka = $args->{KEEPAUTH}) {
        # Keep specific authorities
        my $aids = join(',', sort map { $_->id } 
                        $self->param_to_list($ka, 'authority') );
        if ($aids) {
            $aid .= ' AND ' if ($aid);
            $aid .= sprintf($eahFrm, "", $aids);
        } else {
            $ka = ($#{$ka} == -1) ? "[Empty Array]" : join("+", @{$ka})
                if (ref($ka) && ref($ka) eq 'ARRAY');
            $self->err("Can not keep edges with unknown authority '$ka'");
        }
        $isComplete = 0;
    }

    if (my $ta = $args->{TOSSAUTH}) {
        # Discard specific authorities
        my $aids = join(',', sort map { $_->id } 
                        $self->param_to_list($ta, 'authority') );
        if ($aids) {
            $aid .= ' AND ' if ($aid);
            $aid .= sprintf($eahFrm, "NOT ", $aids);
        } else {
            $ta = ($#{$ta} == -1) ? "[Empty Array]" : join("+", @{$ta})
                if (ref($ta) && ref($ta) eq 'ARRAY');
            $self->err("Can not toss edges with unknown authority '$ta'");
        }
        $isComplete = 0;
    }

    my @meta_ali = sort keys %ems;
    if ($#meta_ali > -1) {
        # We need to join to table edge_meta
        for my $i (0..$#name_w) {
            foreach my $edgetoken (@meta_ali) {
                push @{$name_w[$i]}, "$edgetoken.edge_id = e.edge_id"
                if ($edgetoken);
            }
            foreach my $meta_extra ($aid, $tags) {
                push @{$name_w[$i]}, $meta_extra if ($meta_extra);
            }
        }
    }

    ##############
    # Filter by classes

    if (my $kc = $args->{KEEPCLASS}) {
        # Keep specific classes
        my %classes = $self->ids_in_param($kc, 'class');
        my $cids = join(',', sort { $a <=> $b } keys %classes);
        if ($cids) {
            $cid = " AND c.class_id IN ($cids)";
            # Join table class to edge:
            for my $i (0..$#name_w) {
                my $j = !$i + 1;
                push @{$name_w[$i]}, "c.name_id = e.name$j";
            }
        } else {
            $self->err("Can not keep edges with unknown class '$kc'");
        }
        $isComplete = 0;
    }

    if (my $tc = $args->{TOSSCLASS}) {
        # Discard specific classes
        my %tcs = $self->ids_in_param($tc, 'class');
        my $tcids = join(',', sort { $a <=> $b } keys %tcs);
        if ($tcids) {
            # If we are discarding classes, we need to know if ANY class
            # assignments have been made to the other name
            for my $i (0..$#name_w) {
                my $j = !$i + 1;
                push @{$name_w[$i]},  
                "1 > (SELECT count(*) FROM seq_class WHERE ".
                    "name_id = e.name$j AND class_id IN ($tcids) LIMIT 1)";
            }
        } else {
            $self->err("Can not toss edges with unknown class '$tc'");
        }
        $isComplete = 0;
    }

    ##############
    # Filter by taxa

    if (my $ks = $args->{KEEPTAXA}) {
        # Keep specific taxa
        my %taxa = $self->ids_in_param($ks, 'taxa');
        my $tids = join(',', sort { $a <=> $b } keys %taxa);
        if ($tids) {
            $tax = " AND s.tax_id IN ($tids)";
            # Join table species to edge:
            for my $i (0..$#name_w) {
                my $j = !$i + 1;
                push @{$name_w[$i]}, "s.name_id = e.name$j";
            }
        } else {
            my $etxt = $ks;
            if (ref($etxt)) {
                $etxt = join(',',@{$ks}) || '-EMPTY ARRAY-';
            }
            $self->err("Can not keep edges with unknown taxa '$etxt'");
        }
        $isComplete = 0;
    }

    ##############
    # Filter by edge namespace
    
    my $nsid = '';
    my $nsReq = lc($args->{NAMESPACE} || $args->{NS} || $args->{SPACE} || 1);
    unless ($nsReq eq 'all') {
        my @default_ns = $self->param_to_list($nsReq, 'namespace');
        my @ids = map {$_->id} @default_ns;
        if ($#ids < 0) {
            $nsid = ' = 1';
        } elsif ($#ids == 0) {
            $nsid = " = $ids[0]";
        } else {
            $nsid = " IN (". join(',', @ids). ")";
        }
    }

    ##############
    # Filter by node namespace

    if (my $ks = $args->{KEEPSPACE}) {
        my %spaces = $self->ids_in_param($ks, 'namespace');
        my $nsids = join(',', sort { $a <=> $b } keys %spaces);
        if ($nsids) {
            $snsid = " AND sn.space_id IN ($nsids)";
            # Join table class to edge:
            for my $i (0..$#name_w) {
                my $j = !$i + 1;
                push @{$name_w[$i]}, "sn.name_id = e.name$j";
            }
        } else {
            $self->err("Can not keep edges with unknown class '$ks'");
        }
        $isComplete = 0;
    }

    my $lim     = $args->{LIMIT} || 0;

    # @name_w is a collection of filters as arrays, turn into strings
    @name_w = map { "(". join(" AND ", @{$_}) .")" } @name_w;

    my $sql = 
        "SELECT ".($noDist ? "" : "DISTINCT").
        " e.name1, e.name2, e.type_id, e.edge_id, e.space_id, ".
        "e.live, e.created FROM edge e";

    # Iterate the tables we are going to select from
    foreach my $edgetoken (@meta_ali) {
        $sql .= ", edge_meta $edgetoken" if ($edgetoken);
    }
    foreach my $tabAli (@seqTables) {
        $sql .= ", seqname $tabAli";
    }
    $sql .= ", edge ecx"      if ($cxt);
    $sql .= ", seq_class c"   if ($cid);
    $sql .= ", seqname sn"    if ($snsid);
    $sql .= ", seq_species s" if ($tax);
    $sql .= " WHERE ";
     
    # %complete tracks 'reads as' strings that have been completely
    # recovered - that is, all entries in the database have been
    # gathered for those reads.

    my (%thash, %complete);
    if (my $gt = $args->{KEEPTYPE} || $args->{TYPE}) {
        my @requestType  = ref($gt) eq 'ARRAY' ? @{$gt} : ($gt);
        if ($#requestType > -1 ) {
            # Filter by type
            # Only specific types have been requested
            my @tids;
            # -revtype allows edge types to be interpreted in reverse
            my @tIndex = $args->{REVTYPE} ? (1, 0) : (0, 1);
            foreach my $t (@requestType) {
                my ($type, $dir, $space) = $self->get_type($t);
                unless ($type) {
                    $self->err("Can not keep edges with unknown type '$t'");
                    next;
                }
                my $nsparam = $nsid;
                if ($t =~ /^\#/) {
                    $nsparam = '= ' . $space->id;
                }
                # The token is used to slot type IDs into different hash
                # keys - this assures that each type will be retrieved as
                # a separate query.
                my $tid  = $type->id;
                my $tok  = "";
                if ($lim) {
                    $tok = $tid;
                }
                my $tkey = "$nsparam\t$tok";
                if ($dir) {
                    # This type should only be queried in one direction
                    my $tindex = ($dir > 0) ? $tIndex[0] : $tIndex[1];
                    push @{$thash{$tkey}[$tindex]}, $tid;
                    $complete{$tinfo->{$tid * $dir}{READ}} =1 
                        if ($isComplete && !$lim);
                } else {
                    # Query in both directions
                    map { $complete{ $_ } = 1 } $type->reads
                        if ($isComplete && !$lim);
                    for my $tindex (0..1) {
                        push @{$thash{$tkey}[$tindex]}, $tid;
                    }
                }
            }
            # Convert arrays of type_ids into WHERE clauses
            while (my ($tkey, $arr) = each %thash) {
                for my $i (0..1) {
                    if (!$arr->[$i] || $#{$arr->[$i]} < 0) {
                        $arr->[$i] = '';
                    } elsif ($#{$arr->[$i]} == 0) {
                        $arr->[$i] = "= " . $arr->[$i][0];
                    } else {
                        $arr->[$i] = sprintf
                            ("IN (%s)", join(",", sort {$a<=>$b}
                                             @{$arr->[$i]}));
                    }
                }
            }
        }
    }

    my @thits = keys %thash;
    if ($#thits < 0) {
        # No explicit type requests were made
	if (!$lim) {
	    # If there is no limit, then we can simply query in bulk
            %thash = ( "\t\t" => [ '','' ] );
        } else {
	    # If a limit is provided, we need to apply the limit to
	    # each edge type seperately - otherwise a highly populated
	    # edge type will prevent us from seeing less populated
	    # edges. We need to iterate all the possible edges, and
	    # set a query for each edge type.

            %thash = ();
            my $base = "SELECT DISTINCT(type_id) FROM edge e WHERE ";
            for my $i (0..1) {
                my $check = $base . $name_w[$i];
                my @tids = $dbi->get_array_for_field
                    ( -sql => $check,
                      -level => 2,
                      -name  => "Get all distinct Edge types for a ".
                      "pair of names (needed for LIMIT)",
                      -bind  => $name_binds[$i] );
                map { $thash{"\t$_"}[$i] = "= $_" } @tids;
            }
	}
    }

    my $dsql = $args->{DUMPSQL} || $self->{DUMPSQL};

    my @found;
    my $sqlLim = $lim ? $lim + 1 : undef;
    while (my ($tkey, $tarr) = each %thash) {
        my ($nsparam, $typeID) = split("\t", $tkey);
        $nsparam ||= $nsid;
        my $tail = " ";
        $tail   .= "AND e.live = 't'" unless ($deadToo);
        $tail   .= $cid . $snsid . $tax . ($aid ? " AND $aid" : "");
        my @types = @{$tarr};
        $types[0] ||= '';
        $types[1] ||= '';
        my ($lt, $rt) = @types;

        if (!$lt && !$rt) {
            # No type tests at all
            my @union;
            my @binds;
            for my $i (0..$#name_w) {
                my $req  = $name_w[$i];
                my $ubit = $sql;
                $ubit .= $req;
                $ubit .= " AND e.space_id $nsparam" if ($nsparam);
                $ubit .= $tail;
                push @union, $ubit;
                push @binds, @{$name_binds[$i]};
            }
            my $fullsql = join(' UNION ', @union);
            my $got = $dbi->get_all_rows
                ( -sql     => $fullsql,
                  -level   => $args->{SQLLEVEL} || 1,
                  -name    => "Unrestricted edge dump for query",
                  -dumpsql => $dsql,
                  -limit   => $sqlLim,
                  -bind    => \@binds );

            if ($lim && $#{$got} >= $lim) {
                # Limit exceeded, take off the extra row
                pop @{$got};
            } else {
                $complete{all} = 1 if ($isComplete);
            }
            push @found, @{$got};
        } else {
            # Type restrictions or limits are in place
            my $tdat = $tinfo->{$typeID};
            if ($lt eq $rt && $tdat && $tdat->{SYM}) {
                # Symetrical edge, we can query both sides at once
                my $fullsql = $sql;
                $fullsql .= sprintf("(%s OR %s) ", @name_w);
                $fullsql .= " AND e.space_id $nsparam" if ($nsparam);
                $fullsql .= " AND e.type_id $lt";
                $fullsql .= $tail;

                my $got = $dbi->get_all_rows
                    ( -sql     => $fullsql,
                      -level   =>  $args->{SQLLEVEL} || 1,
                      -name    => "Edge dump for symmetrical edge type",
                      -dumpsql => $dsql,
                      -limit   => $sqlLim,
                      -bind    => [ @{$name_binds[0]},  @{$name_binds[1]} ]);

               if ($lim && $#{$got} >= $lim) {
                    pop @{$got};
                } elsif ($tdat) {
                    $complete{ $tdat->{FOR} } = 1 if ($isComplete);
                }
                push @found, @{$got};
            } else {
                # The edge is not symetrical or only a request on one side
                for my $i (0..1) {
                    my $twhere = $types[$i];
                    next unless ($twhere && $name_w[$i]);
                    my $fullsql = $sql;
                    $fullsql .= $name_w[$i];
                    $fullsql .= " AND e.space_id $nsparam" if ($nsparam);
                    $fullsql .= " AND e.type_id $twhere";
                    $fullsql .= $tail;
                    print $args->{SQLLEVEL} if ($args->{SQLLEVEL});
                    my $got = $dbi->get_all_rows
                        ( -sql     => $fullsql,
                          -level   => 1,
                          -dumpsql => $dsql,
                          -name    => "Edge dump for asymmetrical edge type",
                          -limit   => $sqlLim,
                          -bind    => $name_binds[$i] );
                    if ($lim && $#{$got} >= $lim) {
                        pop @{$got};
                    } elsif ($tdat) {
                        my $tk = $i ? 'REV' : 'FOR';
                        $complete{ $tdat->{$tk} } = 1 if ($isComplete);
                    }
                    push @found, @{$got};
                }
            }
        }
    }
    if ($noDist) {
        # Handle distinct in code
        # Postgres has problems with DISTINCT, it forces a seqscan
        # https://stackoverflow.com/a/6033977
        my %seen;
        foreach my $row (@found) {
            my $key = join("\t", map { defined $_ ? $_ : "" } @{$row});
            $seen{$key} ||= $row;
        }
        @found = values %seen;
    }
    my @comps = sort keys %complete;
    my $rv;
    if ($asObj) {
        # Convert IDs into an object
        my %ehash;
        my @ids = map { $_->[0], $_->[1] } @found;
        my $id_hash = $self->bulk_seq_for_ids( @ids );
        foreach my $row (@found) {
            my ($n1, $n2) = ($id_hash->{ $row->[0] }, $id_hash->{ $row->[1] });
            unless ($n1 && $n2) {
                $self->err("Failed to find sequence for ID " . $row->[0])
                    unless ($n1);
                $self->err("Failed to find sequence for ID " . $row->[1])
                    unless ($n2);
                next;
            }
            my $edge = $self->get_edge
                ( $row->[3], $row->[4], $n1, $n2, 
                  $row->[2], $row->[5], $row->[6] );
            $ehash{ $edge->id } = $edge;
        }
        my @edges = values %ehash;
        if ($asArray) {
            $rv = \@edges;
        } else {
            my %hash;
            map { push @{$hash{$_->type->id}}, $_ } @edges;
            $rv = \%hash;
        }
    } elsif ($orient) {
        # Alter the data to be oriented to a single query:
        my (@rows, %hash);
        for my $i (0..$#found) {
            my ($sid1, $sid2, $tid, $eid, $esid) = @{$found[$i]};
            # Which direction is this relationship in?
            my ($oid, $dir, $myid) = ($orient->{$sid1}) ? 
                ($sid2, $tinfo->{$tid}{FOR}, $sid1) : 
                ($sid1, $tinfo->{$tid}{REV}, $sid2 );
            $oid = $asName->get_single_value($oid) if ($asName);
            if ($asArray) {
                push @rows, [ $oid, $dir, $eid, $myid, $esid ];
            } else {
                push @{$hash{ $tid }}, [ $oid, $dir, $eid, $myid, $esid ];
            }
        }
        $rv = $asArray ? \@rows : \%hash;
    } elsif ($asArray) {
        $rv = \@found;
    } else {
        my %hash;
        map { push @{$hash{$_->[2]}}, $_ } @found;
        $rv = \%hash;
    }
    $self->benchstop;
    return wantarray ? ($rv, \@comps) : $rv;
}

sub get_edge {
    my $self = shift;
    my $val  = shift;
    if (!$val) {
        return wantarray ? () : undef;        
    } elsif ($self->_safe_isa($val,'BMS::MapTracker::Edge')) {
        # Request is already a Seqname object
        return wantarray ? ($val) : $val;
    }
    my $cache = $self->{OBJECTS}{edges};
    if (exists $cache->{$val}) {
        # We have already recovered this request
	return wantarray ? @{$cache->{$val}} : $cache->{$val}[0];
    }
    my ($eid, $space_id, $name1, $name2, $type_id, $live, $cd);
    if ($val =~ /^\d+$/) {
        # Edge ID
        $eid = $val;
        if ($#_ == 5) {
            # Remaining arguments are the other columns
            ($space_id, $name1, $name2, $type_id, $live, $cd) = @_;
        } else {
            # Query the database
            ( $space_id, $name1, $name2, $type_id, $live, $cd ) =
                $self->dbi->named_sth("Retrieve edge by ID")
                ->selectrow_array( $val );
        }
    } else {
        $self->err("I do not know how to find edges using '$val'");
        ## unless ($val =~ /^TEMP\:/);
        return wantarray ? () : undef; 
    }
    unless ($eid) {
        $self->err("Failed to find data for edge ID $val");
        return wantarray ? () : undef; 
    }
    my $edge = BMS::MapTracker::Edge->new
        ( -name1   => $name1,   -name2   => $name2,
          -type    => $type_id, -id      => $eid,
          -live    => $live,    -space   => $space_id,
          -created => $cd,      -tracker => $self );
    $cache->{$eid} = [ $edge ];
    $self->{OBJ_COUNT}{edges}++;
    return $edge;
}

sub parse_genomic_sequence_name {
    my $self = shift;
    my ($name) = @_;
    my ($spec, $type, $sname, $build, $start, $end);
    if ($name =~ /^(.+)\:(\d+)$/ || $name =~ /^(.+)\:(\d+)\-(\d+)$/) {
        ($name, $start, $end) = ($1, $2, $3);
        $end = $start unless (defined $end);
    }
    if ($name =~ /^(.+)\.([^\.]+)\.(.+)\.(.+)$/) {
        # BMS-named ensembl chromosome
        ($spec, $type, $sname, $build) = ($1, $2, $3, $4);
    } elsif ($name =~ /(\S+)_(Chr|Frag)_(.+)\.([^\.]+)$/i) {
        ($spec, $type, $sname, $build) = ($1, $2, $3, $4);
    }
    $type = 'chromosome' if ($type && $type =~ /chr/i);
    ($spec, $type, $build) = 
        $self->_standard_genome_types( $spec, $type, $build) if ($spec);
    return ($spec, $type, $sname, $build, $start, $end);
}

sub parse_genomic_build_name {
    my $self = shift;
    my ($name) = @_;
    my ($spec, $type, $build, $ignore, $hash);
    if ($name =~ /\//) {
        my @bits = split(/\//, $name);
        $name = $bits[-1];
    }
    if ($name =~ s/\.(\d+)\.(\d+)\.btdb//) {
        $hash = "$1.$2";
    }
    $name =~ s/\.fa//;
    if ($name =~ /^(.+)\.genome\.(.+)$/) {
        ($spec, $build) = ($1, $2);
        $type = "Unmasked";
    } elsif ($name =~ /^(NCBI_\d+)_(Unmasked|Masked)_Chromosomes$/i) {
        ($spec, $build, $type) = ('Homo sapiens', $1, $2);
    } elsif ($name =~ /^(.+?)_([^_]+_\d+)_(Unmasked|Masked)_Chromosomes$/i ||
             $name =~ /^(.+)_([^_]+)_(Unmasked|Masked)_Chromosomes$/i) {
        ($spec, $build, $type) = ($1, $2, $3);
    }
    ($spec, $ignore, $build) = 
        $self->_standard_genome_types( $spec, "", $build) if ($spec);
    return ($spec, $build, $type, $hash);
}

my $stndTypes = {
    chromosome   => 'Chromosome',
    chunk        => 'Genomic DNA',
    clone        => 'Clone',
    contig       => 'Genomic Assembly',
    frag         => 'Genomic Assembly',
    genescaffold => 'Genomic Assembly',
    group        => 'Genomic DNA',
    reftig       => 'Genomic DNA',
    scaffold     => 'Genomic Assembly',
    supercontig  => 'Contig',
    superlink    => 'Genomic DNA',
    ultracontig  => 'Contig',
};
sub _standard_genome_types {
    my $self = shift;
    my ($spec, $type, $build) = @_;
    my $key = lc("$spec\t$type\t$build");
    unless ($self->{GENOMETYPES}{$key}) {
        if ($spec) {
            $spec =~ s/_+/ /g;
            my @taxa = $self->get_taxa($spec);
            $spec = $taxa[0]->name if ($#taxa == 0);
        }
        # $build =~ s/_//g;
        if ($type) {
            $type = lc($type);
            if (my $st = $stndTypes->{$type}) {
                $type = $st;
            } else {
                substr($type,0,1) = uc(substr($type,0,1));
            }
        }
        $self->{GENOMETYPES}{$key} = [ $spec, $type, $build ];
    }
    return @{$self->{GENOMETYPES}{$key}};
}

sub _process_tag_filters {
    my $self   = shift;
    my ($filt) = @_;
    my $index  = 1;
    my (@tagsql, @tabs);
    my @filters = ref($filt) ? @{$filt} : ($filt);
    foreach my $txt (@filters) {
        my $ts = $self->_process_tag_filter($txt, $index);
        next unless ($ts);
        push @tagsql, $ts;
        push @tabs, "em$index" if ($ts =~ /em$index\./);
        $index++;
    }
    my $tag = join(" AND ", @tagsql);
    return ($tag, \@tabs);
}

sub _process_tag_filter {
    my $self = shift;
    my ($request, $index) = @_;
    unless (defined $self->{TAG_FILTERS}{$request}) {
        my $txt = " $request ";
        $txt =~ s/ TAG / em.tag_id /g;
        $txt =~ s/ VAL / em.value_id /g;
        $txt =~ s/ NUM / em.numeric_value /g;
        $txt =~ s/\'\'/QUOTE_PLACE/g;
        my $problem = 0;
        # Deal with lists:
        foreach my $opre ("NOT\\s+IN", "IN") {
            while ($txt =~ /em.(\S+)\s+($opre)\s+\(\s*(\'[^\)]+?)\s*\)/) {
                my ($col, $op, $values) = ($1, $2, $3);
                my $swapout = $values;
                my @allsids;
                while ($values =~ /(\'[^\']+\')/) {
                    my $value = $1;
                    my @sids  = $self->_quote_to_name_id($value, $col);
                    push @allsids, @sids;
                    $problem++ if ($#sids < 0);
                    $values =~ s/\Q$value\E//g;
                }
                my $list = join(",", @allsids);
                $txt =~ s/\Q$swapout\E/$list/;
            }
        }
        # Deal with simple tests (which may become lists):
        while ($txt =~ /em.(\S+)\s+(\S+)\s+(\'[^\']+\')/) {
            my ($col, $op, $value) = ($1, $2, $3);
            my @sids = $self->_quote_to_name_id($value, $col);
            if ($#sids < 0) {
                $problem++;
            } elsif ($#sids == 0) {
                $txt =~ s/\Q$value\E/$sids[0]/;
            } else {
                my $newop = $op =~ /\!/ ? 'NOT IN' : 'IN';
                my $list = "(".join(",", @sids).")";
                $txt =~ s/\Q$op\E\s+\Q$value\E/$newop $list/;
            }
        }
        if ($problem) {
            $txt = "";
        } else {
            # Tidy up whitespace:
            $txt =~ s/\s+/ /g; $txt =~ s/^\s+//; $txt =~ s/\s+$//;
        }
        $self->{TAG_FILTERS}{$request} = $txt;
    }
    my $sql = $self->{TAG_FILTERS}{$request};
    # Alter the edge_meta row to include index terms:
    $sql =~ s/em\./em$index./g if ($index);
    return "($sql)";
}

sub _quote_to_name_id {
    my $self = shift;
    my ($value, $col) = @_;
    $value =~ s/^\s*\'//; $value =~ s/\'\s*$//; 
    $value =~ s/QUOTE_PLACE/\'\'/g;
    # By default use the Meta_Tags namespace for the tag name:
    $value = '#META_TAGS#'.$value if ($col eq 'tag_id' && $value !~ /^\#/);
    my @sids = sort {$a <=> $b } map { $_->id } $self->get_seq($value);
    if ($#sids == -1 ) {
        $self->err("Could not find $col match for '$value'");
    }
    return @sids;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 get_taxa

 Title   : get_taxa
 Usage   : $self->get_taxa($id)
 Function: Gets a Taxa object for an $id or name.
 Returns : 
 Args    : 

=cut

*get_species = \&get_taxa;
sub get_taxa {
    my $self = shift;
    my ($val, $is_auto) = @_;
    return undef unless ($val);
    if (ref($val)) {
        return ($val) if ($self->_safe_isa($val,"BMS::MapTracker::Taxa") );
	$self->err("Can not get a taxa object using '$val'");
	return undef;
    }
    $val = uc($val);
    my $cache = $self->{OBJECTS}{taxa};

    unless (exists $cache->{$val}) {
        $self->benchstart;
        # No cache entry, we need to search for the object
        my $dbi = $self->dbi;
        my ($sth, $ambiguity);
        if ($val =~ /^\d+$/) {
            $sth = $dbi->named_sth("Find taxa by ID");
        } else {
            $ambiguity = ($val =~ /[\%\?]/) ? 'ambiguous' : 'unambiguous';
            $sth = $dbi->named_sth("Find taxa by $ambiguity name");
        }
        my @ids = $sth->get_array_for_field( $val );

        if ($#ids == -1 && $ambiguity) {
            # See if we can find the taxa as an alias
            $sth = $dbi->named_sth("Find taxa by $ambiguity alias");
            @ids = $sth->get_array_for_field( $val );
        }
        # warn "$val => ".join(',', @ids)."\n".$sth->pretty_print($val) if ($ids[0] && $ids[0] !~ /^\d+$/);
        # Need to group by taxid to deal with merged IDs
        my %byId;
        foreach my $id (@ids) {
            my $obj = BMS::MapTracker::Taxa->new
                ( -tracker   => $self,
                  -id        => $id, );
            next unless ($obj->id);
            $byId{$obj->id} = $obj;
        }
        my @taxa = values %byId;
        $cache->{$val} = \@taxa;
        $self->{OBJ_COUNT}{taxa} += $#taxa + 1;
        $self->benchstop;
    }
    unless ($is_auto) {
        # This taxa request was made by the user
        map { $self->{REQUESTED_TAXA}{$_->id} = $_ } @{$cache->{$val}};
    }
    return @{$cache->{$val}};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 param_to_list

 Title   : param_to_list
 Usage   : $self->param_to_list( $parameter, $objectTypeString )

 Function: A parameter is provided, which can be an object, object
           name or object ID, *or* an array ref of such. It is then
           turned into a true array of MapTracker objects. The type of
           object must be supplied in order to search properly.

 Returns : An array of objects.

    Args : The parameter (or array ref of them) and a string that is
           either 'class', 'type', 'taxa' or 'sequence'.

=cut

sub param_to_list {
    my $self = shift;
    # Rapid algorithm : 0% = 0.0464 sec in 250 events(AVG 0.0002 s)
    my ($req, $type) = @_;
    my @objs = ref($req) eq 'ARRAY' ? @{$req} : ( $req );
    my @retval;
    foreach my $o (@objs) {
	next unless (defined $o);
	my @newobjs;
	if ($type =~ /class/i) {
	    $newobjs[0] = $self->get_class($o);
            # if ($newobjs[0] && $type =~ /deep/i) { }
	} elsif ($type =~ /type/i) {
	    $newobjs[0] = $self->get_type($o);
	} elsif ($type =~ /sequence/i) {
	    @newobjs = $self->get_seq($o);
	} elsif ($type =~ /taxa/i) {
	    @newobjs = $self->get_taxa($o);
	} elsif ($type =~ /auth/i) {
	    @newobjs = $self->get_authority($o);
	} elsif ($type =~ /space/i) {
	    @newobjs = $self->get_namespace($o);
	}
	foreach my $obj (@newobjs) {
	    push @retval, $obj if ($obj);
	}
    }
    return @retval;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 ids_in_param

 Title   : ids_in_param
 Usage   : $self->ids_in_param( $parameter, $objectTypeString  )

 Function: Similar to above, but rather than returning an object list,
           returns a hash keyed with all the ids that make up the
           objects. For classes, will also include their
           children. This is a utility function used for parameters
           such as keepclass and tossclass.

 Returns : A hash with keys being database IDs of the objects found.
 Args    : The parameter (or array ref of them) and a string that is
           either 'class', 'type', 'taxa' or 'sequence'.

=cut

sub ids_in_param {
    my $self = shift;
    # Rapid algorithm : 0% = 0.0468 sec in 104 events(AVG 0.0005 s)
    my ($req, $type) = @_;
    my @objects = $self->param_to_list($req, $type);
    my %ids;
    if ($type =~ /^class$/i) {
	foreach my $class (@objects) {
	    foreach my $kid ($class->me_and_the_kids) {
		$ids{ $kid->id } = 1;
	    }
	}
    } else {
	%ids = map { $_->id => 1 } @objects;
    }
    return %ids;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 clear_cache

 Title   : clear_cache
 Usage   : $self->( $cacheName  )

 Function: The program keeps several caches (listed below). Normally,
           it is not expected that these would become a memory
           problem. For names in particular, however, a load operation
           could fill up the cache with lots of objects. This method
           lets you clear a specific cache, freeing its memory.

 Returns : 

 Args    : The name of the cache. Known names are:

           transforms   authorities  classes
           types        taxa         seqnames
           edges

=cut

sub clear_cache {
    my $self = shift;
    my @list = @_;
    my $lim  = 1000;
    if ($#list == -1 || $list[0] =~ /all/i) {
        # Clear everything
        @list = keys %{$self->{OBJECTS}};
        $lim  = 0;
    }
    @list = map { lc($_) } @list;
    foreach my $name (@list) {
        next if ($lim && $self->{OBJ_COUNT}{$name} > $lim);
        $self->{OBJECTS}{$name} = {};
    }
    return $#list + 1;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 genomeVersions

 Title   : genomeVersions
 Usage   : my ( $bestvers, $blisvers ) = $mt->genomeVersions

 Function: Returns the most recently analyzed NCBI genome version, as
           well as the NCBI genome version of currently installed
           BLIS.

 Returns : An array of two values
 Args    : 

=cut

sub genomeVersions {
    my $self = shift;
    return (31,30);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 html_css

 Title   : html_css
 Usage   : print $mt->html_css;

 Function: Returns an HTML string containing CSS deffinitions for
           HTML. Will only return the string the *first* time it is
           called - this allows other modules to call the
           method repeatedly with only one printout

 Returns : A string
 Args    : 

=cut

sub html_css {
    my $self = shift;
    return "" if ($self->{CSSPRINTED});
    $self->{CSSPRINTED} = 1;
    return <<EOF;

<style type='text/css'>
.nt0 {
  text-align: center; background-color: #eeeeee;
}
.nt1 {
  text-align: center; background-color: #f7f7f7;
}
.blue { color: blue }
.smaller { font-size: smaller }
.build {
  color: red;
    font-weight: bold;
}
.multicopy {
  color: red;
    font-weight: bold;
}
.coord {
    font-size: smaller;
  color: #933;
}
.hang {
    text-indent: -2em;
    padding-left: 2em;
}
.score {
  color: green;
    font-weight: bold;
}

.buildtable caption {
    text-align: left;
    font-weight: bold;
  color: navy;
}
.buildtable td {
      vertical-align: top;
  white-space: nowrap;
}
.buildtable th {
    text-align: left;
  white-space: nowrap;
}
.sn {
    font-size: larger;
    font-weight: bold;
  color: blue;
}
.nsdesc {
    font-size: smaller;
    font-style: italic;
  color: #996;
}
.idc {
    font-size: smaller;
  color: brown;
}
.hidemap {
    font-weight: bold;
    font-style: italic;
  color: tan;
}
.auth {
  color: grey;
}
.alias {
    font-size: smaller;
  color: grey;
}
.warn {
    background-color: yellow;
  color: red;
    font-style: italic;

}

</style>


EOF

}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub javascript_head {
    my $self   = shift;
    my ($forcebeta) = @_;

    my @modules = ("jsmtk.js","widgets/TabbedPane.js","utils/MapTracker.js",
                   "widgets/SortableTable.js");    
    push @modules, "utils/ObjectTree.js" if ($0 =~ /working/);

    my $txt = "<!-- JavaScript ToolKit : Always start with jsmtk.js -->\n";
    foreach my $mod (@modules) {
        $txt .= $self->javascript_include( $mod, $forcebeta );
    }

    my $uname = $self->user ? $self->user->name : '';
    $txt .= "<script>\n  maptracker_user = '$uname'\n</script>\n";
    $txt .= "<!-- End JavaScript ToolKit -->\n";
    return $txt;
}

sub javascript_path {
    my $self   = shift;
    my ($forcebeta) = @_;
    my $jspath = $ENV{MT_JS_URL};
    my $path   = 
        $ENV{SCRIPT_NAME}     || $ENV{REQUEST_URI}  || 
        $ENV{SCRIPT_FILENAME} || $ENV{HTTP_REFERER} || $0 || '';
    my $isbeta = ($path =~ /working/ ) ? 1 : 0;

    if ($isbeta || $forcebeta) {
        $jspath .= 'jstk_cat';
    } else {
        $jspath .= "jsmtk";
    }
    return $jspath;
}

sub javascript_include {
    my $self = shift;
    my ($module, $forcebeta) = @_;
    my $jspath = $self->javascript_path( $forcebeta );
    return sprintf("<script src='%s/%s'></script>\n", $jspath, $module);
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 register_javascript

 Title   : register_javascript
 Usage   : $obj->register_javascript

 Function: Recover javascript object deffinitions for a list of
           objects. Calling this method only stores the deffinitions
           in RAM - to get a string representation, call
           $obj->javascript_data()

 Returns : 
 Args    : A list of MapTracker obejcts

=cut

sub register_javascript {
    my $self = shift;
    my @vals;
    foreach my $req (@_) {
        next unless ($req && ref($req) && $req->can('to_javascript'));
        my $txt = $req->to_javascript();
        push @vals, $txt;
        $self->{JSOBJS}{ $txt }++;
    }
    return @vals;
}

sub register_javascript_literal {
    my $self = shift;
    foreach my $txt (@_) {
        $self->{JSOBJS}{ $txt }++;
    }
}

sub register_javascript_cache {
    my $self = shift;

    my @types;
    if ($#_ < 0) {
        # Use all cached types
        @types = keys %{$self->{OBJECTS}};
    } else {
        @types = map { lc($_) } @_;
    }
    foreach my $type (@types) {
        my @objs = $self->each_cache_entry( $type );
        next if ($#objs < 0 || ! $objs[0]->can('to_javascript') );
        $self->register_javascript( @objs );
    }
}

sub register_javascript_group {
    my $self = shift;
    my $args = $self->parseparams
        ( -group => undef,
          -name  => undef,
          -id    => undef,
          @_ );
    my $group = $args->{GROUP};
    my $id    = $args->{ID};
    my $name  = $args->{NAME};
    return unless ($group && ref($group) eq 'ARRAY');
    
    my @vals = $self->register_javascript( @{$group} );
    my $txt = "type:'group', id:$id";
    if ($name) {
        $name =~ s/\//\/\//g;
        $name =~ s/\'/\\\'/g;
        $txt .= ", name:'$name'";
    }
    my %objs;
    foreach my $js (@vals) {
        if ($js =~ /type\:\'([a-z]+)\'.+id\:(\d+)/) {
            my $tag = sprintf("'%s_%d'", $1, $2);
            $objs{$tag}++;
        }
    }
    $txt .= ", objects:[ ".join(",", sort keys %objs)." ]";
    $txt = "{ $txt }";
    $self->{JSOBJS}{ $txt }++;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 javascript_data

 Title   : javascript_data
 Usage   : my $txt = $obj->javascript_data()
 Function: 
 Returns : 
 Args    : 

=cut


sub javascript_data {
    my $self   = shift;
    my $frm    = shift || 'html';
    my $string = "";
    my @vals;
    foreach my $val ( keys %{$self->{JSOBJS}} ) {
        # We assume that so long as this object is active, we only want
        # to print a given object once:
        next if ($self->{JSSEEN}{$val});
        $self->{JSSEEN}{$val} = 1;
        push @vals, $val;
    }
    
    my $rv   = "";
    if ($frm eq 'js') {
        # The string is intended to be parsed directly in javascript
        my $txt  = join(",\n  ", sort @vals) || "";
        $rv = "[ $txt ]";
    } elsif ($frm eq 'html' && $#vals > -1) {
        # 45,000 seems ok, 50,000 crashes
        my $maxchar   = 30000;
        # This value may be irrelevant:
        my $maxobjs   = 200;
        my $joiner    = ",\n    ";
        my $jlen      = length($joiner);
        # The string is being added as part of an HTML document
        $rv  = "<script type='text/javascript'>\n";

        @vals = sort @vals;
        my @chunks = ( [[], 0 - $jlen] );
        while (my $obj = shift @vals) {
            my $len = length($obj) + $jlen;
            if ($chunks[-1][1] + $len > $maxchar ||
                $#{$chunks[-1][0]} >= $maxobjs) {
                # We need to add a new chunk
                push @chunks, [[], 0 - $jlen];
            }
            push @{$chunks[-1][0]}, $obj;
            $chunks[-1][1] += $len;
        }

        $rv .= 
            "/* Loading chunked to prevent spectacular browser crash:\n".
            "   Maximum character content: $maxchar\n".
            "   Maximum object count: $maxobjs */\n" if ($#chunks > 0);
        foreach my $chunk (@chunks) {
            my @chunk = @{$chunk->[0]};
            my $clen  = $#chunk + 1;
            my $data  = join($joiner, @chunk);
            my $com   = sprintf
                ("%d object%s in %d chars", 
                 $clen, $clen == 1 ? '' : 's', length($data));
            $rv .= 
                "try {\n".
                "  /* $com  */\n".
                "  maptracker_add_data([\n    ".
                join(",\n    ", @chunk). "\n".
                "  ]);\n".
                "} catch (e) {\n".
                "  jsmtk_error('Failed to parse data block with $com', e);\n".
                "}\n";
        }

=pod

        my $cs = 1;
        while (my @chunk = splice(@vals, 0, $blocksize)) {
            my $clen = $#chunk + 1;
            my $com = sprintf
                ("%d MapTracker object%s", $clen, $clen == 1 ? '' : 's');
            $rv .= 
                "try {\n".
                "  /* MapTracker objects $cs - ".($cs+$#chunk)."  */\n".
                "  maptracker_add_data([\n    ".
                join(",\n    ", @chunk). "\n".
                "  ]);\n".
                "} catch (e) {\n".
                "  jsmtk_error('Failed to parse data block with $com', e);\n".
                "}\n";
            $cs += $clen;
        }

=cut

        $rv .= "</script>\n";
    } else {
        # Does not appear to be a javascript-appropriate format - do nothing
    }
    $self->{JSOBJS} = {};
    return $rv;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 _store_js

 Title   : _store_js
 Usage   : $obj->_store_js()

 Function: 'Hides' the javascript RAM cache, preventing it from being
           serialized by javascript_data()

 Returns : 
 Args    : 

=cut


sub _store_js {
    my $self = shift;
    push @{$self->{STORE_JS}}, $self->{JSOBJS};
    $self->{JSOBJS} = {};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 _restore_js

 Title   : _restore_js
 Usage   : $obj->_restore_js()
 Function: Returns stored JS RAM cache to normal visibility
 Returns : 
 Args    : 

=cut


sub _restore_js {
    my $self = shift;
    foreach my $hash ( @{$self->{STORE_JS}} ) {
        map { $self->{JSOBJS}{$_} = 1 } keys %{$hash};
    }
    $self->{STORE_JS} = [];
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 _safe_isa

 Title   : 
 Usage   : $mt->_safe_isa( $obj, $classname )

 Function: Many get_ functions can accept either an integer ID, a text
           identifier, or a blessed object as a parameter. -isa() is
           used to see if the object is the desired one. ref() is used
           to make sure isa is not called on a scalar
           value. Unfortunately, isa must be called on a blessed
           reference, and ref() can allow arrays and hashes to sneak
           through. This method avoids these problems.

 Returns : 0 or 1
 Args    : [0] The object to be tested
           [1] The object class name

=cut

my $basicReferences = {
    SCALAR => 1,
    ARRAY  => 1,
    HASH   => 1,
    CODE   => 1,
    GLOB   => 1,
    REF    => 1,
    LVALUE => 1,
    'IO::Handle' => 1,
};

sub _safe_isa {
    my $self = shift;
    my ($obj, $class) = @_;
    my $ref = ref($obj);
    #print "$obj = $ref<br />";
    return 0 if (!$ref || $basicReferences->{$ref});
    return $obj->isa($class);
}

sub new_unique_id {
    my $self = shift;
    return ++$self->{UNIQUE_ID};
}

sub taxa_html_key {
    my $self = shift;

    my (%icons, @rows);
    my @taxas = values %{$self->{REQUESTED_TAXA}};
    map { push @{$icons{ $_->icon('url') }}, $_ } @taxas;
    my @names = sort keys %icons;

    foreach my $name (@names) {
        my @tx   = sort { $a->name cmp $b->name } @{$icons{$name}};
	my $lab  = sprintf("<tr><td><img src='%s' /></td>", $name);
        my @bits = split(/\//, $name);
        my $tok  = $bits[-1];
	$tok =~ s/\.\w+?$//; $tok =~ s/\_/ /g;
	$lab .= "<th>$tok</th><td>";
        $lab .= join(", ", map { $_->javascript_link('', 1) } @tx);
        $lab .= "</td></tr>";
        push @rows, $lab;
    }
    return ($#rows < 0) ? '' :
        "<b>Observed Taxa:</b><br /><table>".join("\n", @rows)."\n</table>"
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 load_external

 Title   : load_external
 Usage   : $mt->load_external;

 Function: Returns an HTML string containing JavaScript code for
           managing the pop-up boxes used to display data about
           MapTracker objects in a web page.

 Returns : A string
 Args    : Associative array of arguments. Recognized keys [Default]:

     -file Required. The name of the flat file that contains the
           external data.

=cut


sub load_external {
    my $self = shift;
    my $args = $self->parseparams
        ( -file     => 'You forgot to provide a file path!',
          @_ );
    my $file = $args->{FILE};
    unless (-e $file) {
        $self->err("I could not find the file '$file'");
        return;
    }
    open (EXTF, "<$file") ||
        $self->death("Could not read external file", $file, $!);
    while (<EXTF>) {
        chomp;
        my @cols = split(/\t/, $_);
        for my $i (0..$#cols) {
            if ($cols[$i] =~ /^\'(.+)\'$/ || $cols[$i] =~ /^\"(.+)\"$/) {
                # Strip flanking quotes
                $cols[$i] = $1;
            }
        }
        my $act = uc( shift @cols );
        if ($act eq 'ONLYUSER') {
            my $list = join(' ', @cols);
            my %users = map { lc($_) => 1 } split(/[\s\,\n]/, $list);
            my $this_user = $self->user->name;
            unless ($users{$this_user}) {
                $self->err("You are not allowed to read '$file'");
                last;
            }
        } elsif ($act eq 'MAP') {
            
        }
    }
    close EXTF;
}

sub each_cache_entry {
    my $self = shift;
    my $cname = $_[0] || 'seqnames';
    return () unless (exists $self->{OBJECTS}{$cname});
    my %vals;
    
    foreach my $val (values %{$self->{OBJECTS}{$cname}}) {
        my @arr = ref($val) eq 'ARRAY' ? @{$val} : ($val);
        map { $vals{$_->id} ||= $_ } @arr;
    }
    return values %vals;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

1;

=head3 Random DB statistics:

... about 3 minutes to run (under load)

maptracker=# select count (*) from seqname where length(seqname) > 4000;
   20

maptracker=# select count (*) from seqname where length(seqname) > 1000;
   20170

maptracker=# select count (*) from seqname where length(seqname) > 500;
  134438


=cut
