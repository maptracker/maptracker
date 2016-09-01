# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::SciTegicSmiles;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use strict;
use vars qw(@ISA @EXPORT );
use Scalar::Util qw(weaken);
use SOAP::Lite;

use BMS::MapTracker;

@ISA          = qw(BMS::MapTracker::Shared);
# my $debug     = $BMS::MapTracker::Shared::debug;
# my $priClass  = 'SciTegic SMILES';
my $priClass  = 'InChI';
my $nonIsomer = 'NONISOMERIC';
my $isIsomer  = 'ISOMERIC';
my $isProblem = 'SUSPICIOUS';
# my $uName     = 'SciTegic';
my $uName     = 'OpenBabel';

=head2 Change from SMILES to InChIKey

In July 2015 it was found that Mark Hermsmeyer's SOAP service was no
longer functional. A decision was made to move away from SMILES to
InChIKey as the primary node in MapTracker, using OpenBabel locally as
the main conversion mechanism.


=head3 OpenBabel settings

OpenBabel has a limit of 1000 atoms, though apparently only for output.

 -xt           : add molecule name after InChI
 -xw           : ignore less important warnings
 -r            : Remove all but the largest contiguous fragment (strip salts)
 -xT /nostereo : ignore E/Z and sp3 stereochemistry

This will strip the molecule down to just the three components of the
main layer: formula, connection and hydrogens. This is the layer that
comprises the first 14-character hash in an InChIKey, so molecules
processed with this and -r should have common primary leading InChIKey
segments.

=head4 Conversions with OpenBabel

All examples are sprintf'ed with (inputStructure, inputFormat)

InChIKey conversiions are the same, but with -oinchikey

Full InChI:

  echo "%s" | obabel -oinchi -i%s -xt -xw

NonIsomeric InChI:

  echo "%s" | obabel -oinchi -i%s -xt -xw -xT /nostereo -r

When using /nostereo with -r, I get warnings with a test structure:

  echo "Cl.CN(C)[C@@]1(C)C[C@H](c2ccccc2)c3ccccc3C1" |\
     obabel -oinchi -ismiles -xT /nostereo -r

  #1 :Wrong 0D stereo descriptor(s): #1; #2; Omitted undefined stereo

However, if I first convert to InChI, then strip stereo and salts, I
do NOT get the error:

  echo "Cl.CN(C)[C@@]1(C)C[C@H](c2ccccc2)c3ccccc3C1" |\
     obabel -oinchi -ismiles > foo.inchi

   cat foo.inchi | obabel -oinchi -iinchi -xT /nostereo -r

=head2 OpenBabel Properties

obprob is a small utility that reports basic information regarding the
molecule:

  obprop filename

It is likely a useful tool as fallback for diagnosing problems


=cut


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
        SALTFILE => '/home/tilfordc/public_html/CommonSalts.smi',
        VERBOSE  => 0,
        MSGOUT   => *STDERR,
        MSGFILE  => "",
    };
    bless ($self, $class);

    
    my $args = $self->parseparams
        ( -verbose   => 1,
          @_ );

    if (my $lh = $args->{LOADER} || $args->{LOADHELPER}) {
        $self->{LOADER} = $lh;
        weaken( $self->{TRACKER} = $lh->tracker );
    } elsif (defined $args->{TRACKER}) {
        weaken( $self->{TRACKER} = $args->{TRACKER} )
            if ($args->{TRACKER});
    } else {
        eval {
            $self->{TRACKER} = BMS::MapTracker->new
                ( -username => $uName, 
                  -dbadmin  => 0, );
        };
    }
    if (my $mt = $self->{TRACKER}) {
        
    } else {
        $self->msg("MapTracker database is not available",
                   "Cached SMILES strings will not be queried",
                   "All SMILES will be calculated de-novo.") 
    }
    $self->report_format( $args->{FORMAT} || "SMILES");
    $self->verbose( $args->{VERBOSE} );
    $self->msg_output( $args->{MSGFILE} );
    $self->load_cache( $args->{LOADCACHE} || $args->{CACHE} );
    return $self;
}

sub msg_output {
    my $self = shift;
    if (my $redirect = shift) {
        $self->{MSGFILE} = "";
        if (ref($redirect)) {
            $self->{MSGOUT} = $redirect;
        } else {
            if (open(STSMSG, ">$redirect")) {
                $self->{MSGOUT}  = *STSMSG;
                $self->{MSGFILE} = $redirect;
            } else {
                warn "Failed to redirect messages to $redirect:\n  $!\n  ";
            }
        }
    }
    return $self->{MSGOUT};
}

sub msg {
    my $self = shift;
    return unless ($self->verbose);
    my $fh   = $self->msg_output();
    my @bits = map { split(/\n/, $_) } @_;
    # Add a time stamp
    my ($sec, $min, $hr) = localtime;
    $bits[0] .= sprintf(" (%02d:%02d:%02d)", $hr, $min, $sec);
    print $fh "[STS] ".join("\n      ", @bits)."\n";
}

sub DESTROY {
    my $self = shift;
    return unless ($self);
    if (my $mt = $self->tracker) {
        if (my $dbi = $mt->{DBI}) {
            $dbi->release;
        }
    }
}

sub reset {
    my $self = shift;
    $self->{SOAP}   = undef;
    $self->{HANDLE} = undef;
}

sub temp_folder {
    my $self = shift;
    my $nval = shift;
    if ($nval) {
        $nval =~ s/\/+$//;
        if (-d $nval) {
            $self->{TEMP_FOLDER} = $nval;
        } else {
            $self->death("Failed to set temporary folder - not a folder!",
                         $nval); 
        }
    }
    return $self->{TEMP_FOLDER} || "/tmp";
}

sub verbose {
    my $self = shift;
    my $nval = shift;
    if (defined $nval) {
        $self->{VERBOSE} = ($nval =~ /^\d+$/) ? $nval : $nval ? 1 : 0;
    }
    return $self->{VERBOSE};
}

sub retry_suspicious {
    my $self = shift;
    my $nval = shift;
    if (defined $nval) {
        $self->{RETRY_SUSPICIOUS} = $nval ? 1 : 0;
    }
    return $self->{RETRY_SUSPICIOUS};
}

sub _normalize_format_name {
    my $self = shift;
    if (my $req = lc(shift || "")) {
        if ($req =~ /key/i) {
            return 'InChIKey';
        } elsif ($req =~ /inchi/i) {
            return 'InChI';
        } elsif ($req =~ /smi/i) {
            return 'SMILES';
        } elsif ($req =~ /(mt|map)/) {
            return 'MTID';
        }
    }
}

sub report_format {
    my $self = shift;
    my $nval = shift;
    if (defined $nval) {
        $self->{RPT_FMT} = $self->_normalize_format_name( $nval ) || 'MTID';
    }
    return $self->{RPT_FMT};
}

my $rptCBs = {
    # These callbacks presume that $id is an InChI string
    # and that $seq is the MapTracker object representing it
    InChI => sub {
        my ($self, $id) = @_;
        return $id;
    },
    MTID => sub {
        my ($self, $id, $seq) = @_;
        $seq ||= $self->seq_for_string( $id );
        return $seq ? $seq->mtid() : 0;
    },
    InChIKey => sub {
        my ($self, $id, $seq) = @_;
        my $mt = $self->tracker();
        unless ($mt) {
            $self->msg_once("Can not reliably get InChIKey without MapTracker");
            return "";
        }
        
        die "Need to implement InChIKey callback";
    },
    SMILES => sub {
        die "Need to implement SMILES callback";
    },
};
sub _report_callback {
    my $self = shift;
    my $fmt  = $self->report_format();
    return $rptCBs->{$fmt};
}

sub OLD_report_mtids {
    my $self = shift;
    my $nval = shift;
    if (defined $nval) {
        $self->{USE_MTID} = $nval ? 1 : 0;
    }
    return $self->{USE_MTID};
}

sub common_salts {
    my $self = shift;
    unless ($self->{SALTS}) {
        my $file = $self->{SALTFILE};
        open(SFILE, "<$file") || die "Failed to read $file:\n  $!\n  ";
        my %hash;
        while(<SFILE>) {
            s/[\n\r]+$//;
            if (/^(\S+)/) {
                my $smi = $1;

                # SHOULD PROBABLY CANONICALIZE FOR SAFETY...

                $hash{$smi} = 1;
            }
        }
        close SFILE;
        $self->{SALTHASH} = \%hash;
        $self->{SALTS} = [ sort { length($a) <=> length($b) ||
                                      uc($a) cmp uc($b) } keys %hash ];
    }
    return wantarray ? @{$self->{SALTS}} : { %{$self->{SALTHASH}} };
}

sub load_cache {
    my $self = shift;
    $self->{LOADCACHE} = $_[0] if (defined $_[0]);
    return $self->{LOADCACHE};    
}

sub handle {
    return shift->{HANDLE};
}

=head2 tracker

 Title   : tracker
 Usage   : my $mt = $ad->tracker( )
 Function: Gets the MapTracker interface
 Returns : A BMS::MapTracker object
 Args    : 

=cut

sub tracker {
    return shift->{TRACKER};
}

sub loader {
    my $self = shift;
    unless ($self->{LOADER}) {
        if (my $mt = $self->tracker) {
            require BMS::MapTracker::LoadHelper;
            $self->{LOADER} = BMS::MapTracker::LoadHelper->new
                ( -username  => $uName,
                  -tracker   => $mt,
                  -testmode  => 0 );
        }
    }
    return $self->{LOADER};
}

sub standardize_input {
    my $self = shift;
    my $req  = $_[0];
    return undef unless ($req);
    my @rv;
    if (ref($req)) {
        # Array reference being passed
        @rv = map { split(/[\n\r]+/, $_ ) } @{$req};
    } elsif (-e $req) {
        # A file name is being passed
        open(INP, "<$req") || die "Failed to open input file $req:\n  $!\n  ";
        while (<INP>) {
            s/[\n\r]+$//;
            push @rv, $_;
        }
        close INP;
    } else {
        # String being passed, break on new lines
        @rv = map { split(/[\n\r]+/, $_ ) } @_;
    }
    # Remove MapTracker namespace tokens:
    map { s/^\#[A-Z]+\#//i; } @rv;
    return \@rv;
}

sub guess_format {
    my $self = shift;
    my $text = shift;
    return wantarray ? ("","") : "" unless ($text);
    # Default format is to presume SMILES:
    my @rv = ("SMILES", $text);
    if ($text =~ /^MTID:\d+$/i) {
        @rv = ("MTID", uc($text));
    } elsif ($text =~ /^InChI=/) {
        $rv[0] = 'InChI';
    } elsif ($text =~ /^(InChIKey=)?([A-Z]{14}-[A-Z]{10}-[A-Z])$/i) {
        @rv = ('InChIKey', "InChIKey=".uc($2) );
    }
    return wantarray ? @rv : $rv[0];
}

sub convert_formats {
    my $self = shift;
    my ($reqs, $outReq, $inReq) = @_;
    return [] if (!$reqs || $#{$reqs} == -1);
    $self->death("You must provide a desired output format for convert_formats!") unless ($outReq);
    my $outFmt = $self->_normalize_format_name( $outReq );
    $self->death("Output format request '$outReq' not recognized") unless ($outFmt);

    my @rv;
    $inReq = $self->_normalize_format_name( $inReq );
    foreach my $req (@{$reqs}) {
        unless ($req) {
            push @rv, "";
            next;
        }
        my $inFmt = $inReq || $self->guess_format($req);
        if ($inFmt eq $outFmt) {
            # Already the desired format
            push @rv, $req;
            next;
        }
        
    }
    
}

my $fmt2mtns = {
    InChI    => '#InChI#',
    InChIKey => '#None#',
    SMILES   => '#SMILES#',
};
sub _normalize_id {
    my $self = shift;
    my $id   = shift;
    return 0 unless ($id);
    if ($id =~ /^MTID\:(.+)/) {
        my $n = $1;
        if ($n =~ /^\d+$/) {
            return $id;
        }
        $self->msg("Illegal MTID designation '$id'");
        return 0;
    }
    
    my $fullid = $id;
    unless ($fullid =~ /^\#/) {
        # No leading namespace token
        my ($fmt, $clean) = $self->guess_format( $id );
        my $ns  = $fmt2mtns->{$fmt} || "";
        $fullid = $ns . $clean;
    }
    my $obj   = $self->tracker->get_seq
        ( -name => $fullid, -defined => 1);
    return $obj->mtid() if ($obj);
    $self->msg("Failed to find DB entry for '$fullid'");
    return 0;
}

my $blockSize = 20000;
sub canonical {
    my $self = shift;
    my ($rv, $needed) = $self->db_canonical( @_ );
    return undef unless ($rv);
    return $rv   unless ($needed && $#{$needed} != -1);
    $self->bench_start();
    my $mapped = $self->obabel_canonical( $needed );
    if ($mapped) {
        push @{$rv}, @{$mapped};
        map { $self->classify_problematic( $_ ) } $self->failed();
        foreach my $dat (@{$mapped}) {
            $self->store_mapping( @{$dat} ) if ($dat->[0]);
        }
    }
    $self->bench_end();
    return $rv;
}

sub simplify {
    my $self = shift;
    my ($rv, $needed) = $self->db_simplify( @_ );
    return undef unless ($rv);
    return $rv   unless ($needed && $#{$needed} != -1);
    # Hit the soap server in managable blocks:
    while (my @block = splice(@{$needed}, 0, $blockSize) ) {
        my @smis = map { $_->[1] } @block;
        my $mapped = $self->soap_simplify( \@smis);
        next unless ($mapped);
        # There are newly mapped SMILES we need to record
        push @{$rv}, @{$mapped};
        foreach my $dat (@{$mapped}) {
            $self->store_simplified( @{$dat} );
        }
    }
    return $rv;
}

sub full_canonical {
    my $self = shift;
    my $rv = $self->canonical( @_ );
    my %canon = map { $_->[0] => 1 } @{$rv};
    $self->simplify( [ keys %canon ] );
    return $rv;
}


sub store_simplified {
    my $self = shift;
    my ($out, $in) = @_;
    unless ($out && $in) {
        $self->msg("store_mapping() requires two arguments",
                   "Input  = " .($in  || '-UNDEF-'),
                   "Output = " .($out || '-UNDEF-'),);
        return;
    }
    my $mtIn  = $self->_normalize_id( $in );
    return unless ($mtIn);
    my $lh    = $self->loader();
    my $mt    = $lh->tracker();
    my @mtOut;
    foreach my $o (@{$out}) {
        if (my $oid = $self->_normalize_id( $o )) {
            push @mtOut, $oid;
        }
    }
    map { $lh->set_class( $_, $priClass, $uName);
          $lh->set_class( $_, $nonIsomer, $uName) } @mtOut;
    # If the output is the same as the input just leave
    return if ($#mtOut == 0 && $mtOut[0] eq $mtIn);
    # Note that the input is more complex than each of the outputs
    map { $lh->set_edge( -name1 => $mtIn,
                         -name2 => $_,
                         -auth  => $uName,
                         -type  => "is a more complex form of", ) } @mtOut;
    # Also explicitly note that the input is isomeric:
    $lh->set_class($mtIn, $isIsomer, $uName);
    if (my $lc = $self->load_cache()) {
        $lh->write_threshold_quick($lc);
    }
}

sub store_mapping {
    my $self = shift;
    my ($out, $in) = @_;
    unless ($out && $in) {
        $self->msg("store_mapping() requires two arguments",
                   "Input  = " .($in  || '-UNDEF-'),
                   "Output = " .($out || '-UNDEF-'),);
        return;
    }
    my $lh = $self->loader();
    my $mt = $lh->tracker();
    # Normalize everything to MTIDs
    my $mtOut = $self->_normalize_id( $out );
    my $mtIn  = $self->_normalize_id( $in );
    return unless ($mtOut && $mtIn);
    $lh->set_class( $mtOut, $priClass, $uName);
    unless ($mtOut eq $mtIn) {
        $lh->set_class( $mtIn, 'SMILES');
        # We don't really need to get back to the other SMILES, do we?
        # We primarily need to be able to quickly get from
        # non-canonical to canonical:
        $lh->set_edge( -name1 => $mtIn,
                       -name2 => $mtOut,  
                       -auth  => $uName,
                       -type  => 'reliable');
    }
    if (my $lc = $self->load_cache()) {
        $lh->write_threshold_quick($lc);
    }
}

sub classify_problematic {
    my $self = shift;
    my $smi  = shift;
    return unless ($smi);
    my $mtSmi = $self->_normalize_id( $smi );
    return unless ($mtSmi);
    my $lh    = $self->loader();
    $lh->set_class($mtSmi, $self->problem_class(), $uName );
    $lh->set_edge( -name1 => $mtSmi,
                   -name2 => "Problematic SMILES Queries",  
                   -auth  => $uName,
                   -type  => 'is a member of');
    if (my $lc = $self->load_cache()) {
        $lh->write_threshold_quick($lc);
    }
}

sub components {
    my $self = shift;
    # Start by canonicalizing the queries:
    my $rv   = $self->canonical( @_ );
    my $mt   = $self->tracker();
    my $lh   = $self->loader;
    foreach my $row (@{$rv}) {
        my $canon = $row->[0];
        my $cSeq  =  $self->seq_for_string( $canon );
        my $rel = $mt->get_edge_dump
            ( -name      => $cSeq,
              -return    => 'object array',
              -keeptype  => 'has part',
              -keepclass => $priClass,
              -dumpsql   => 0 );
        my @kids;
        if ($#{$rel} == -1) {
            # We have not recorded canonical parts yet
            # First make note of distinct strings in canonical parent:
            my %parts = map { $_ => 1 } split(/\./, $canon);
            # Then canonicalize those:
            my $karr = $self->canonical( keys %parts );
            my %kp   = map { $_->[0] => 1 } @{$karr};
            @kids    = sort keys %kp;
            my $mtcan = $self->_normalize_id( $canon );
            next unless ($mtcan);
            foreach my $kid (@kids) {
                if (my $mtkid = $self->_normalize_id( $kid )) {
                    $lh->set_edge( -name1 => $mtcan,
                                   -name2 => $mtkid,
                                   -type  => 'has part' );
                }
            }
        } else {
            # MapTracker is holding part information
            @kids = map { $_->other_seq($cSeq)->name } @{$rel};
        }
        $row->[0] = \@kids;
    }
    if (my $lc = $self->load_cache()) {
        $lh->write_threshold_quick($lc);
    }
    return $rv;
}

sub db_canonical {
    my $self = shift;
    my $mt   = $self->tracker();
    return wantarray ? () : undef unless ($mt);
    my $inp  = $self->standardize_input( @_ );
    return wantarray ? () : undef unless ($inp);
    $self->bench_start();
    $self->msg("Database canonicalization for ".($#{$inp}+1)." requests")
        if ($self->verbose > 1);
    my @rv;
    my @needed;
    my @suspicious;
    my $fmtCB   = $self->_report_callback();
    #my $useMTID = $self->report_mtids();
    foreach my $req (@{$inp}) {
        my ($id, $com) = ($req);
        if ($id =~ /^(\S+)\s(.+)/) {
            ($id, $com) = ($1, $2);
        }
        if (my $seq = $self->seq_for_string( $id )) {
            if ($seq->is_class($priClass)) {
                # The string is already canonicalized
                push @rv, [&{$fmtCB}($self, $id, $seq),
                           $id, $com, 'Already Canonical'];
            } else {
                my $rel = $mt->get_edge_dump
                    ( -name      => $seq,
                      -return    => 'object array',
                      -keeptype  => 'is a reliable alias for',
                      -keepauth  => $uName,
                      -keepclass => $priClass,
                      -dumpsql   => 0 );
                if ($#{$rel} == -1) {
                    # No mappings exist
                    if ($seq->is_class($isProblem) && 
                        !$self->retry_suspicious) {
                        # The SMILES has been flagged as suspicious
                        # and we don't wish to retry loading it
                        push @rv, [undef, $id, $com, 'Flagged Suspicious'];
                        push @suspicious, $id;
                    } else {
                        push @needed, $req;
                    }
                } else {
                    # Already mapped to a canonical SMILES
                    my $msg;
                    my @oseqs = map { $_->other_seq($seq) } @{$rel};
                    if ($#oseqs == 0) {
                        $msg = "Unique Mapping";
                        my $oseq = $oseqs[0];
                        push @rv, [&{$fmtCB}($self, $oseq->name(), $oseq),
                                   $id, $com, $msg];
                    } else {
                        $msg = sprintf("%d mappings exist!", $#{$rel} + 1);
                        $self->classify_problematic( $seq->name )
                            unless ($seq->is_class($isProblem));
                        push @rv, [undef, $id, $com, $msg];
                        push @suspicious, $id;
                        $self->msg("Multiple 'canonical' $priClass",
                                   "Query ".$seq->id,
                                   "IN (". join(',', map {$_->id} @oseqs).")");
                    }
                }
            }
        } else {
            push @needed, $req;
        }
    }
    unless ($#suspicious == -1) {
        my $failed = $self->failed();
        push @{$failed}, @suspicious;
    }
    $self->bench_end();
    return wantarray ? (\@rv, \@needed) : \@rv;
}

sub db_simplify {
    my $self = shift;
    my $mt   = $self->tracker();
    return wantarray ? () : undef unless ($mt);
    my $bits = $self->split_smiles( @_ );
    return wantarray ? () : undef unless ($bits);
    $self->msg("Database simplification for ".($#{$bits}+1)." substances")
        if ($self->verbose > 1);

    my @rv;
    my $isSalt   = $self->common_salts();

    my $isSimple = sub {
        # If the object is already a canonical, non-isomeric SMILES,
        # returns 1, otherwise returns 0
        my ($seq) = @_;
        return ($seq->is_class($nonIsomer) && $seq->is_class($priClass)) ?
            1 : 0;
    };

    my $simpleFromEdge = sub {
        # If the object is already connected to simpler entities,
        # return an array of those entities. Otherwise return undef
        my ($seq)  = @_;
        my $pEdges = $mt->get_edge_dump
            ( -name      => $seq,
              -return    => 'object array',
              -keeptype  => 'is a more complex form of',
              -keepclass => $priClass,
              -dumpsql   => 0 );
        my @ok;
        foreach my $edge (@{$pEdges}) {
            my $par = $edge->other_seq($seq);
            if (&{$isSimple}($par)) {
                push @ok, $par->name;
            } else {
                $self->msg("Suspicious edge ". $edge->id,
                           "Child ".$par->id." of ".$seq->id." is not simple");
            }
        }
        return ($#ok == -1) ? undef : \@ok;
    };
    
    my $duoSimple = sub {
        # Combine a check for immediate simplicity with a mapping check
        my ($seq, $id, $com, $mode)  = @_;
        if ( &{$isSimple}( $seq ) ) {
            return [[$seq->name], $id, $com, 
                    "Simple via ".($mode || 'Unknown')];
        } elsif (my $pars = &{$simpleFromEdge}( $seq )) {
            return [ $pars, $id, $com, "Mapped via ".($mode || 'Unknown') ];
        }
        return undef;
    };

    # How many of these are already simplified?
    my (@notSimple, @needed);
    my $canonCache = {};
    foreach my $dat (@{$bits}) {
        my ($pieces, $id, $com) = @{$dat};
        my $seq = $self->seq_for_string( $id );
        if ($seq) {
            # The object is known in the database
            if (my $hit = &{$duoSimple}( $seq, $id, $com, "Self")) {
                # The object has already been simplified
                push @rv, $hit;
                next;
            }
            # No previous simplifications stored for this object
            # Maybe we can canonicalize it?
            push @{$dat}, $seq;
            $canonCache->{$id} = "";
        } elsif ($#{$pieces} == 0) {
            # The parent is unitary, and is not known in the database
            # There is nothing else we can do!
            push @needed, [[$id], $id, 'Unknown Entry'];
            next;
        }
        # We could not quickly find a pre-computed entry
        # We will try to canonicalize it...
        push @notSimple, $dat;
    }

    # Canonicalize the remaining input in bulk:
    my $canonical = $self->db_canonical( keys %{$canonCache} );
    foreach my $canon (@{$canonical || []}) {
        $canonCache->{ $canon->[1] } = $canon->[0];
    }

    my $canSimple = sub {
        my ($seq, $id, $com)  = @_;
        my $smi = $canonCache->{ $id };
        if ($smi && $smi ne $id) {
            # We were able to pull up a canonical form of the SMILES
            if ($isSalt->{$smi}) {
                # ... and it is just a salt
                return [ [$id], $id, $com, "Simple Salt"];
            }
            $seq = $self->seq_for_string( $smi );
            return &{$duoSimple}( $seq, $id, $com, "Canonical");
        }
        return undef;
    };

    my (@breakUp, %pieceCache);
    foreach my $dat (@notSimple) {
        my ($pieces, $id, $com, $seq) = @{$dat};
        if (my $hit = &{$canSimple}( $seq, $id, $com)) {
            # Able to find cached simplified info after canonicalizing
            push @rv, $hit;
            next;
        }
        if ($#{$pieces} < 1) {
            # The query is a known unitary object that was not previously
            # simplified
            push @needed, [[$id], $id, 'Unitary unmapped'];
            next;
        }
        # We will need to cope with the pieces individually
        # We can only do this if every piece is known to the database
        my %uniq = map { $_ => 1 } @{$pieces};
        my @ids  = keys %uniq;
        my (@seqs, @failed);
        foreach my $piece (@ids) {
            if (my $seq = $self->seq_for_string( $piece )) {
                push @seqs, $seq;
            } else {
                push @failed, $piece;
            }
        }
        if ($#failed == -1) {
            # All pieces are known to the database, we might be able
            # to map them over
            push @breakUp, $dat;
            # Update the pieces array to be MapTracker objects
            $dat->[0] = \@seqs;
            # Note the SMILES strings
            map { $pieceCache{$_} = '' } @ids;
        } else {
            # At least one piece is unknown; no possibility of mapping
            push @needed, [\@failed, $id, 'Unknown Pieces'];
        }
    }

    # Bulk canonicalize the pieces
    my @neededPieces;
    foreach my $piece (keys %pieceCache) {
        next if ($canonCache->{$piece});
        push @neededPieces, $piece;
        $canonCache->{$piece} = '';
    }
    unless ($#neededPieces == -1) {
        $canonical  = $self->db_canonical( @neededPieces );
        foreach my $canon (@{$canonical || []}) {
            $canonCache->{ $canon->[1] } = $canon->[0];
        }
    }

    foreach my $dat (@breakUp) {
        my ($pseqs, $id, $com) = @{$dat};
        my (%simple, @failed);
        foreach my $pseq (@{$pseqs}) {
            my $hit = 
                &{$duoSimple}($pseq) || &{$canSimple}($pseq, $pseq->name);
            if ($hit) {
                # We we able to find a simplified form for this piece
                my $simp = $hit->[0][0];
                unless ($isSalt->{$simp}) {
                    # Only include it if it is not a salt
                    $simple{$simp} = 1;
                }
            } else {
                # No simple form available!
                push @failed, $pseq->name;
            }
        }
        if ($#failed == -1) {
            # All pieces were simplified!
            my @mapped = sort keys %simple;
            push @rv, [ \@mapped, $id, $com, "SubComponents Mapped"];
        } else {
            # Failure to simplify based on DB alone
            push @needed, [\@failed, $id, 'SubMapping Failed'];
        }
    }
    return wantarray ? (\@rv, \@needed) : \@rv;
}

sub split_smiles {
    my $self = shift;
    my $inp  = $self->standardize_input( @_ );
    return wantarray ? () : undef unless ($inp);

    my @rv;
    foreach my $req (@{$inp}) {
        my ($full, $com) = ($req);
        if ($full =~ /^(\S+)\s(.+)/) {
            ($full, $com) = ($1, $2);
        }
        my %pars = map { $_ => 1 } split(/[\.]+/, $full);
        push @rv, [[keys %pars], $full, $com];
    }
    return wantarray ? @rv : \@rv;
}

sub diversify_bms_ids {
    my $self = shift;
    # We need to cope with frustrating naming variants
    # 'AB-008411' +  'AB -008411' + 'AB-8411'
    my %queries;
    foreach my $bmsID (@_) {
        my ($prfx, $num) = split(/\s*\-\s*/, $bmsID);
        $queries{ sprintf("%-3s-%06d", $prfx, $num) } = 1; # AB -008411
        $queries{ sprintf("%s-%06d", $prfx, $num) }   = 1; # AB-008411
        $queries{ sprintf("%s-%d", $prfx, $num+0) }   = 1; # AB-8411
    }
    return keys %queries;
}

sub preferred_bms_id {
    my $self = shift;
    my @nice;
    foreach my $id (@_) {
        if ($id && $id =~ /^([A-Z][A-Z\s]{0,2})\-(\d{1,6})$/i) {
            # Standardize the ID to a single variant (no spaces, zero pad)
            my ($prfx,$num) = ($1, $2);
            $prfx   =~ s/\s+//g;
            push @nice, sprintf("%s-%06d", uc($prfx), $num);
        }
    }
    return wantarray ? @nice : $nice[0];
}

sub term_to_canonical {
    my $self   = shift;
    my $mt     = $self->tracker;
    my %uniq;
    foreach my $tReq (@_) {
        next unless ($tReq);
        my $tArr = (ref($tReq) && ref($tReq) eq 'ARRAY') ? $tReq : [$tReq];
        foreach my $term (@{$tArr}) {
            next unless ($term);
            my $seq = $mt->get_seq( -name     => $term,
                                    -defined  => 1,
                                    -nocreate => 1 );
            next unless ($seq);
            my $type = 'is reliably aliased by';
            if ($seq->is_class($priClass) || $seq->is_class('RNAI')) {
                # The term is already canonical
                $uniq{$seq->id} = $seq;
                next;
            } elsif ($seq->is_class('SMILES')) {
                $type = 'is a reliable alias for';
            }
            # warn "$term = ".$seq->namespace_name()." via $type";
            my $rel = $self->tracker->get_edge_dump
                ( -name      => $seq->id,
                  -orient    => 1,
                  -keeptype  => $type,
                  -keepclass => $priClass );
            map { $uniq{$_->[0]} = 1 } @{$rel};
        }
    }
    my @seqs = map { $mt->get_seq( -name => $_, -defined => 1) } keys %uniq;
    my @rv = map 
    { $_->namespace->id == 1 ? $_->name : $_->namespace_name } @seqs;
    return @rv;
}


sub seq_for_string {
    my $self = shift;
    my ($id, $noCreate) = @_;
    my $mtid = $self->_normalize_id( $id );
    return undef unless ($mtid);
    my @seqs = $self->tracker->get_seq( -name     => $mtid,
                                        -defined  => 1,
                                        -nocreate => $noCreate );
    if ($#seqs == -1) {
        $self->msg("Failed to create new SMILES entry",$id) unless ($noCreate);
        return undef;
    }
    # We need to make sure that wildcards are not being mis-interpreted
    foreach my $s (@seqs) {
        if ($s->name() eq $id) {
            $self->msg("Multiple lookups for [".$s->id."]",
                       "IN (". join(',', map { $_->id} @seqs).")")
                if ($#seqs != 0);
            return $s;
        }
    }
    $self->msg("Query is being mis-read!","Q: $id",
               map { "   ".$_->name } @seqs);
    return undef;
}

sub soap_simplify {
    die "DEPRECATED";
    my $self = shift;
    # Find all the unique bits associated with the input IDs
    my $bits = $self->split_smiles( @_ );
    my %pieces;
    foreach my $dat (@{$bits}) {
        my ($pcs, $id) = @{$dat};
        map { $pieces{$_}{$id} = 1 } @{$pcs};
    }
    my @uniq = keys %pieces;
    # Canonicalize while striping isomerism
    $self->next_soap_is_nonisomeric();
    my $canonPieces = $self->soap_canonical( \@uniq );

    # Now re-associate each canonical piece with its original ID(s)
    my %reconstituted;
    foreach my $pdat (@{$canonPieces}) {
        my ($smiOut, $smiIn) = @{$pdat};
        if (my $ids = $pieces{$smiIn}) {
            map { push @{$reconstituted{$_}}, $smiOut } keys %{$ids};
        } else {
            $self->msg("Failed to match simplified fragment to parent",
                       "Input : $smiIn", "Output: $smiOut");
        }
    }
    my @rv;
    my $isSalt = $self->common_salts();
    foreach my $dat (@{$bits}) {
        my ($pcs, $id, $com) = @{$dat};
        if (my $rdat = $reconstituted{$id}) {
            if ($#{$pcs} == $#{$rdat}) {
                my (@salt, @notSalt);
                map { if ($isSalt->{$_}) {
                    push @salt, $_;
                } else {
                    push @notSalt, $_;
                } } @{$rdat};
                if ($#notSalt == -1) {
                    # This compound is ONLY salts
                    push @rv, [\@salt, $id, $com, 'SciTegic Pure Salt'];
                } else {
                    # The substance has at least some non-salts; only use those
                    push @rv, [\@notSalt, $id, $com, 'SciTegic Converted'];
                }
            } else {
                $self->msg("Simplified components do not match original count",
                           "Input : $id",
                           "Pieces: ".join(".", @{$pcs}),
                           "Output: ".join(".", @{$rdat}),);
  
            }
        } else {
            $self->msg("Failed to get simplified components for request", $id);
        }
    }
    return \@rv;
}

sub soap_nonisomeric {
    die "DEPRECATED";
    my $self = shift;
    $self->next_soap_is_nonisomeric();
    return $self->soap_canonical( @_ );
}


my $fmt2obfmt = {
    InChI    => 'inchi',
    InChIKey => '',
    SMILES   => 'smiles',
};

sub obabel_canonical {
    my $self = shift;
    my $inp  = $self->standardize_input( @_ );
    return undef unless ($inp);
    my @rv;

    my $temp  = $self->temp_folder();
    my $tout  = "$temp/$$-err";
    my $errF  = "$temp/$uName-Errors.txt";
    my $fmtCB = $self->_report_callback();
    my @failed;
    delete $self->{FAILED_INPUT};
    foreach my $txt (@{$inp}) {
        my $in  = $txt;
        my $com = "";
        if ($txt =~ /^(\S+)\s+(.+?)$/) {
            ($in, $com) = ($1, $2);
        }
        my $fmt = $self->guess_format( $in );
        my $obf = $fmt2obfmt->{$fmt};
        unless ($obf) {
            push @failed, $in;
            $self->msg_once("Unusuable input format $fmt : $in");
            push @rv, ["", $in, $com, "$uName FAILED"];
            next;
        }
        if ($in =~ /\"/) {
            push @failed, $in;
            $self->msg_once("Illegal characters in input : $in");
            push @rv, ["", $in, $com, "$uName FAILED"];
            next;
        }
        my $cmd   = sprintf("echo \"%s\" | obabel -i%s -oinchi 2> \"%s\"", $in, $obf, $errF);
        my @lines = split(/[\n\r]+/, `$cmd`);
        my $msg;
        if ($#lines == 0) {
            if (my $out = $lines[0]) {
                push @rv, [&{$fmtCB}($self, $out), $in, $com, $uName .
                           ($out eq  $in ? ' Verified' : ' Converted')];
                next;
            } else {
                $msg = "Null return for $fmt input : $in";
            }
        } elsif ($#lines == -1) {
            $msg = "Failure to convert $fmt input : $in";
        } else {
            $msg = "Multiple output lines for $fmt input : $in";
        }
        push @failed, $in;
        $self->msg_once($msg);
        push @rv, ["", $in, $com, "$uName FAILED"];
        my $errTxt = "-----------------\n[$fmt] $in\n";
        $errTxt .= "$msg\n";
        $errTxt .= `cat $errF`;
        if (open(ERRF, ">>$errF")) {
            print ERRF $errTxt;
            close ERRF;
        }
    }
    $self->{FAILED_INPUT} = \@failed;
    return \@rv;
}

sub failed {
    my $self = shift;
    my $rv   = $self->{FAILED_INPUT} ||= [];
    return wantarray ? @{$rv} : $rv;
}

sub tidy_error {
    my $err = shift;
    return $err unless ($err);
    if ($err =~ /(.+) at \/.+?\/([^\/]+) line (\d+)\s*$/) {
        $err = "$1 [$2 line $3]";
    }
    return $err;
}

sub primary_class   { return $priClass; }
sub nonisomer_class { return $nonIsomer; }
sub isomer_class    { return $isIsomer; }
sub problem_class   { return $isProblem; }
