# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::GenAccService;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use strict;
use BMS::BmsArgumentParser;
use vars qw(@ISA);
@ISA      = qw(BMS::BmsArgumentParser);

our $multiLine = { map { uc($_) => 1 } qw(queries id ids query term terms) };
our $ad;
sub new {
    my ($class, @args) = @_;
    my $self = $class->SUPER::new( -blankparam => 1, @args );
    map { $self->blockquote($_, 'QUOTEDBLOCK') } keys %{$multiLine};
    $self->use_beta( $self->val(qw(gasbeta gasusebeta)) );
    $self->istrial( $self->val(qw(gastrial gasistrial)) );
    return $self;
}

sub set_param {
    my $self = shift;
    my ($var, $val) = @_;
    return unless ($var);
    if ($multiLine->{uc($var)} && ref($val) && ref($val) eq 'ARRAY') {
        $val = join('', map { defined $_ ? "$_\n" : "\n" } @{$val});
    }
    return $self->SUPER::set_param($var, $val);
}

*usebeta = \&use_beta;
sub use_beta {
    my $self = shift;
    if (defined $_[0]) {
        $self->{USEBETA} = $_[0] ? 1 : 0;
    }
    return $self->{USEBETA};
}

*istrial = \&trial;
*is_trial = \&trial;
sub trial {
    my $self = shift;
    if (defined $_[0]) {
        $self->{ISTRIAL} = $_[0] ? 1 : 0;
    }
    return $self->{ISTRIAL};
}

sub denorm {
    require BMS::MapTracker::AccessDenorm;
    return $ad ||= BMS::MapTracker::AccessDenorm->new();

}

sub executable {
    my $self = shift;
    return "/stf/biocgi/". 
        ($self->use_beta() ? "tilfordc/working/maptracker/MapTracker/" : "").
        "genacc_service.pl";
}

sub last_param_file {
    return shift->{LAST_PFILE} || "";
}

*last_cmd = \&last_command;
sub last_command {
    return shift->{LAST_CMD};
}

sub to_file {
    my $self = shift;
    my $file = shift;
    if (-e $file) {
        unlink($file);
        return "Failed to remove already existing file" if (-e $file);
    }
    if (open(PARAM, ">$file")) {
        print PARAM $self->to_text( @_ );
        close PARAM;
        chmod(0666, $file);
        return 0;
    } else {
        return $!;
    }
}

sub run {
    my $self = shift;
    my $params;
    if (my $file = shift) {
        if (my $err = $self->to_file($file)) {
            $self->death("Failed to write parameter file", $file, $err);
        } else {
            $self->{LAST_PFILE} = $file;
            $file   = "'$file'" if ($file =~ /\s/);
            $params = "-nocgi -valuefile $file";
        }
    } else {
        $params = $self->to_command_line();
    }
    my $cmd = $self->{LAST_CMD} = 
        join(' ', "/usr/bin/nice -n 19", $self->executable(), $params);
    if ($self->trial()) {
        $self->msg("Trial mode - would have run:", $cmd);
        return undef;
    } else {
        # warn $cmd;
        my $ec = system($cmd);
        return $ec;
    }
}

sub cached_file {
    my $self = shift;
    my ($clobber) = shift;
    my $out  = $self->val(qw(output outfile));
    unless ($out) {
        if ($self->val('nonfatal')) {
            return undef;
        } else {
            $self->death
                ("Can not utilize cached_file() unless -output is defined");
        }
    }
    unless ($self->use_existing_file($out, $clobber)) {
        # The file does not exist or we need to recreate it
        $self->run( "$out.param" );
    }
    chmod(0666, $out);
    return $out;
}

sub use_existing_file {
    my $self = shift;
    my ($file, $clobber) = @_;
    # Can not use the file if it is not defined or does not exist:
    return 0 unless ($file && -e $file);
    # Never use the file if clobber is specified:
    return 0 if ($clobber);
    # The file exists, see how old it is
    my $age = $self->val(qw(age ageall));
    $age = $self->denorm->standardize_age($age) if ($age);
    # If the user has not specified the age, then we can use the file:
    return 1 if (!$age);
    # There is an age, how old is the file itself?
    my $fage = -M $file;
    # An age was specified, and the file is fresher than it:
    return 1 if ($fage <= $age);
    # There are a variety of reasons why this is imperfect, but
    # it should be good for most cases
    return 0;
}

sub file_as_array {
    my $self = shift;
    my ($file) = @_;
    open(FILE, "<$file") || $self->death
        ("Failed to read cached data file", $file, $!);
    my @rows;
    while (<FILE>) {
        s/[\n\r]+$//;
        push @rows, [ split(/\t/) ];
    }
    close FILE;
    return wantarray ? @rows : \@rows;
}

sub cached_array {
    my $self = shift;
    my $file = $self->cached_file( @_ );
    return $self->file_as_array( $file );
}

1;
