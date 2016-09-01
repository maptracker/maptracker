# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::Shared;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

=pod

 Functionality common to all modules: argument parsing, error handling

=cut

$BMS::MapTracker::Shared::VERSION = 
    ' $Id$ ';

use strict;
use BMS::Utilities::Benchmark;
use BMS::HelpManager;
use BMS::ErrorInterceptor;

use Scalar::Util qw(weaken);
# use Time::HiRes;

use vars qw(@ISA);
@ISA    = qw(BMS::Utilities::Benchmark BMS::ErrorInterceptor);

my $query;
our $help;
our $popfunc = "mt_pop(%s, event)";
our $popurl  = "<a class='%s' dragtag='%s' token='%s_%s' onclick=\"%s\">%s</a>";
our $sharedInfoMT = {};

sub TWH {
    my ($hurl, $tiddler, $name, $class) = @_;
    unless ($hurl =~ /^http/) {
        $hurl  = "http://bioinformatics.bms.com/biohtml/HelpTiddlyWikis/$hurl";
        $hurl .= ".html" unless ($hurl =~ /html$/i);
    }
    $name  ||= '[?]';
    $class   = 'twhelp' unless (defined $class);
    my $url  = sprintf
        ("<a class='%s' href='%s#%s' onclick=\"var win = window.open(this.href, '_help', 'width=900,toolbar=no,scrollbars=yes'); if (window.focus) win.focus(); return false;\">%s</a>", 
         $class, $hurl, $tiddler, $name);
    return $url;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 help

 Title   : help
 Usage   : my $helpManagerObject = $mt->help
 Function: Gets a help manager object, used to make integrated help.
 Returns : A blessed BMS::HelpManager object.
 Args    : 

Not being used for new coding - help interface is now managed through
TiddlyWikis rather than BMS::HelpManager.

=cut

sub help {
    my $self = shift;
    unless ($help) {
        $help = BMS::HelpManager->new( -file => "mtHelp.txt", );
    }
    unless ($help->{USER}) {
        if ($self->can('user')) {
            $help->{USER} = $self->user->name;
        } elsif ($self->can('tracker')) {
            $help->{USER} = $self->tracker->user->name;
        }
    }
    return $help;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub live_timestamp {
    my $self = shift;
    my $dbi;
    if ($self->can('dbi')) {
        $dbi = $self->dbi;
    } elsif ($self->can('tracker')) {
        $dbi = $self->tracker->dbi;
    }
    return $dbi->get_single_value('select current_timestamp') if ($dbi);
    return undef;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub db_timestamp {
    # Intentionally designed to NOT update with each call
    # It captures the time reported by the database when the program
    # first starts
    my $self = shift;
    $sharedInfoMT->{TIMESTAMP} ||= $self->live_timestamp();
    return $sharedInfoMT->{TIMESTAMP};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 javascript_link

 Title   : javascript_link
 Usage   : my $html_anchor = $obj->javascript_link($classes, $type, $id, $name, $args)
 Function: Generates an <A> string for use with maptracker javascript
 Returns : A string
    Args : Optional HTML classes

=cut

sub javascript_link {
    my $self = shift;
    my ($classes, $type, $id, $name, $args) = @_;
    $classes = $classes ? "mt$type $classes" : "mt$type";
    $classes .= " Dragable";
    $name = substr($name,0,40) . "..." if (length($name) > 40);
    my $func = $self->javascript_function($args);
    return sprintf($popurl, $classes, $type, $type, $id, $func, 
                   $self->escape_html($name));
}

sub javascript_function {
    my $self = shift;
    my ($args) = @_;
    $args ||= '{}';
    return sprintf($popfunc, $args);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 escape

 Title   : escape
 Usage   : my $newText = $obj->escape( $oldText );
 Function: 
 Returns : 
 Args    : 

=cut


sub escape_html {
    my $self = shift;
    my $txt  = $_[0];
    $txt = '' unless(defined $txt);
    $txt =~ s/\</\&lt\;/g unless ($txt =~ /^\</);
    return $txt;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
