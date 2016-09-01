# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::Mapping;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

BEGIN {
}

$BMS::MapTracker::Mapping::VERSION = 
    ' $Id$ ';

use strict;
use BMS::Utilities;
use BMS::MapTracker::Transform;
use Bio::Location::Split;
use Bio::Location::Simple;
use Bio::Location::Fuzzy;
use Scalar::Util qw(weaken);

use vars qw(@ISA);
@ISA    = qw(BMS::Utilities);


=head1 PRIMARY METHODS
#-#-#-#-#--#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-

=head2 new

 Title   : new
 Usage   : my $obj = BMS::MapTracker::Mapping->new(@arguments)
 Function: Creates a new object and returns a blessed reference to it.
 Returns : A blessed BMS::MapTracker::Mapping object
 Args    : Associative array of arguments. Recognized keys [Default]:

 -transform Required. A Transform object defining the relationship
           between the two sequences. AKA -trans_id

   -strand Required. +1 or -1

   -map_id The database id for the mapping (not the sequences)

 -searchdb Optional Searchdb. Can be either an object or a name/ID.

    -name1 Sequence database ID for the first sequence

    -name2 Sequence database ID for the second sequence

  -tracker MapTracker object - needed for certain database queries.

 -locations A 2D array ref of locations, each row being [id1_start,
           id1_end, id2_start]. Can optionally include id2_end. For
           id1, start is always less than end. For id2, start is less
           than end only if strand is +1.

   -start1 -end1 -start2 -end2. If you just have 1 location, you can
           provide coordinates in this fashion. Please note that for
           both 1 and 2, start should always be less than end. This is
           different than -locations, where the second sequence will
           have -start > -end if strand is -1.

    -score Optional (but recommended) real value

 -authority Optional authority object. AKA -authority_id

    -onfail Default 'die'. What to do when there is a problem. Other
            option is 'return', which will cause the function to
            return undef.

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
	TAGS => {},
        PARENTS  => [],
    };
    bless ($self, $class);
    my $args = $self->parseparams( -name1     => undef,
				   -name2     => undef,
				   -start1    => undef,
				   -start2    => undef,
				   -end1      => undef,
				   -end2      => undef,
				   -transform => undef,
				   -strand    => 'None Supplied',
				   -authority => undef,
				   -score     => undef,
				   -tracker   => undef,
				   -onfail    => 'die',
				   -searchdb  => undef,
				   @_ );
    weaken( $self->{TRACKER} = $args->{TRACKER} );
    my $tid  = $args->{TRANSFORM} || $args->{TRANS_ID} || "None Supplied";
    my $seq1 = $args->{NAME1}     || $args->{ID1};
    my $seq2 = $args->{NAME2}     || $args->{ID2};
    $self->transform( $tid );
    $self->authority( $args->{AUTHORITY} || $args->{AUTHORITY_ID} );
    $self->seqs($seq1, $seq2);
    $self->score( $args->{SCORE} || $args->{MAP_SCORE} );
    $self->strand( $args->{STRAND} );
    $self->map_id( $args->{MAP_ID} );
    $self->searchdb( $args->{SEARCHDB} || $args->{SEARCH_DB}  || $args->{SDB});
    $self->{ONFAIL} = $args->{ONFAIL};
    my $locs = $args->{LOCATIONS} || $args->{DATA};
    $locs = [ [ $args->{START1}, $args->{END1},
		$args->{START2}, $args->{END2}, ] ] 
                    if (!$locs || $#{$locs} < 0);
    my $ok = $self->locations( @{$locs} );
    return $ok ? $self : undef;
}

sub DESTROY {
    my $self = shift;
    $self->{SEQ1} = undef;
    $self->{SEQ2} = undef;
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
    return $self->{TRACKER};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 transform

 Title   : transform
 Usage   : $obj->transform($transform)
 Function: Gets / Sets the transform object
 Returns : 
 Args    : 

=cut

sub transform {
    my $self = shift;
    my ($tran) = @_;
    if ($tran) {
	unless (ref($tran) && $tran->isa('BMS::MapTracker::Transform')) {
	    my $mt = $self->tracker;
	    $self->death("'$tran' is not a Transform object, and you ".
			 "have not supplied a tracker object for searching")
		unless ($mt);
	    my $found = $mt->get_transform( $tran );
	    $self->death("I could not find the Transform '$tran'")
		unless ($found);
	    $tran = $found;
	}
	$self->{TRANSFORM} = $tran;
    }
    return $self->{TRANSFORM};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 authority

 Title   : authority
 Usage   : $obj->authority($authority)
 Function: Gets / Sets the authority object
 Returns : 
 Args    : 

=cut

*auth = \&authority;
sub authority {
    my $self = shift;
    my ($auth) = @_;
    if ($auth) {
	unless (ref($auth) && $auth->isa('BMS::MapTracker::Authority')) {
	    my $mt = $self->tracker;
	    $self->death("'$auth' is not an Authority object, and you ".
			 "have not supplied a tracker object for searching")
		unless ($mt);
	    my $found = $mt->get_authority( $auth );
	    $self->death("I could not find the Authority '$auth'")
		unless ($found);
	    $auth = $found;
	}
	$self->{AUTHORITY} = $auth;
    }
    return $self->{AUTHORITY};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 score

 Title   : score
 Usage   : $obj->score($score)
 Function: Gets / Sets the score for this mapping
 Returns : 
 Args    : 

=cut


sub score {
    my $self = shift;
    my ($score) = @_;
    if (defined $score) {
	$self->death("'$score' does not appear to be numeric")
	    unless ($score =~ /[\d\.\+\-E]/);
	$self->{SCORE} = $score;
    }
    return $self->{SCORE};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 tag

 Title   : tag
 Usage   : $value = $obj->tag($tagname, $newvalue)
 Function: Gets / Sets the unconstrained tag data
 Returns : The value for a tag
 Args    : The name of the tag, and an optional new value

=cut


sub tag {
    my $self = shift;
    my ($name, $value) = @_;
    my $oldvalue;
    if (defined $name) {
        $oldvalue = $self->{TAGS}{$name};
        $self->{TAGS}{$name} = $value if (defined $value);
    }
    return $oldvalue;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 strand

 Title   : strand
 Usage   : $obj->strand($id)
 Function: Gets / Sets the strand
 Returns : 
 Args    : 

=cut


sub strand {
    my $self = shift;
    my ($strand) = @_;
    if (defined $strand) {
	$self->death("Strand should be 1 or -1, not '$strand'")
	    unless ($strand =~ /^[+-]?1$/);
	$self->{STRAND} = $strand;
    }
    return $self->{STRAND};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 map_id

 Title   : map_id
 Usage   : $obj->map_id($id)
 Function: Gets / Sets the map_id for the mapping
 Returns : 
 Args    : 

=cut

*id = \&map_id;

sub map_id {
    my $self = shift;
    my ($id) = @_;
    if ($id) {
	$self->death("Map ID '$id' is not an integer")
	    unless ($id =~ /^\d+$/);
	$self->{MAP_ID} = $id;
    }
    return $self->{MAP_ID};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 searchdb

 Title   : searchdb
 Usage   : $obj->searchdb($id)
 Function: Gets / Sets the searchdb for the mapping
 Returns : 
 Args    : 

=cut


*sdb = \&searchdb;
sub searchdb {
    my $self = shift;
    my ($sdb) = @_;
    if ($sdb) {
	if ($sdb->isa('BMS::MapTracker::Searchdb')) {
	    $self->{SEARCHDB} = $sdb;
	} else {
	    my $mt = $self->tracker;
	    my $found = $mt->make_searchdb( $sdb );
	    unless ($found) {
		$self->death("Could not find Searchdb '$sdb'");
	    }
	    $self->{SEARCHDB} = $found;
	}
    }
    return $self->{SEARCHDB};
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 seqs

 Title   : seqs
 Usage   : $obj->seqs($id1, $id2)
 Function: Gets / Sets the name_ids for the two sequences
 Returns : 
 Args    : If setting the sequences, you must provide both.

=cut


sub seqs {
    my $self = shift;
    my ($id1, $id2) = @_;
    if ($id1) {
	$self->death("You are required to set both sequences at once")
	    unless ($id2);
	unless (ref($id1) && $id1->isa('BMS::MapTracker::Seqname')) {
	    my $mt = $self->tracker;
	    $self->death("'$id1' is not a Seqname object, and you ".
			 "have not supplied a tracker object for searching")
		unless ($mt);
	    my $found = $mt->get_seq( $id1 );
	    $self->death("I could not find the Seqname '$id1'")
		unless ($found);
	    $id1 = $found;
	}
	$self->{SEQ1} = $id1;
	unless (ref($id2) && $id2->isa('BMS::MapTracker::Seqname')) {
	    my $mt = $self->tracker;
	    $self->death("'$id2' is not a Seqname object, and you ".
			 "have not supplied a tracker object for searching")
		unless ($mt);
	    my $found = $mt->get_seq( $id2 );
	    $self->death("I could not find the Seqname '$id2'")
		unless ($found);
	    $id2 = $found;
	}
	$self->{SEQ2} = $id2;
    }
    return ($self->{SEQ1}, $self->{SEQ2});
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 bridge

 Title   : bridge
 Usage   : $obj->bridge( @args )

 Function: Provided with seqname $name, will attempt to use the other
           seqname in the Mapping to find twice-removed sequences that
           fall in the same area.

 Returns : 
 Args    : 

     -name The name of the sequence you wish to build bridges to. -seq
           is also accepted.

 -cutbridge Default 0. If true, apply filters to the scores generated
           for the scaffolds. This is normally off, since the scoring
           algorithm is a little shakey, and it may be reasonable for
           a bridged sequence to only cover part of the query.

=cut


sub bridge {
    my $self = shift;
    my $args = $self->parseparams( -name       => undef,
				   -seq        => undef, 
				   -keepclass  => [],
				   -tossclass  => [],
				   -cutbridge  => 0,
				   @_);
    my $nameReq = $args->{NAME} ||= $args->{SEQ};
    my $mt = $self->tracker;
    $self->death("You can not bridge a mapping without a Tracker object")
	unless ($mt);
    my $query = $mt->get_seq( $nameReq );
    my $qid   = $query->id;
    my @seqs  = $self->seqs;
    my $qin   = $self->seq_index( $query );
    return () unless (defined $qin);
    my $sin   = !$qin;
    my $subj  = $seqs[$sin];
    my @ranges = $self->ranges;
    # Check to see if only a subset range of query is being shown:
    my ($qs, $qe) = $query->range;
    unless ($qs && $qe) {
        # If no query range was defined, use the map range
        ($qs, $qe) = ($ranges[$qin * 2], $ranges[$qin * 2 + 1]);
    }
    # Now get the corresponding positions on the subject:
    my ($sts, $ste) = $self->map_triplet( $query, $qs, $qe);
    # If exact matches are found (index 1 in the triplet), use them
    # Otherwise use the extreme edges (left = 0, right = 2)
    my ($start, $stop) = ( $sts->[1] || $sts->[0] || $sts->[2], 
                           $ste->[1] || $ste->[2] || $ste->[0], );
    ($start, $stop) = ( $stop, $start) if ($start > $stop);
    my @maps = ();
    my %passedArgs = %{$args};
    delete $passedArgs{SEQ};
    delete $passedArgs{NAME};

    # Check location-by-location for maps that overlap
    my %checkMaps;
    my $mid = $self->id;
    foreach my $loc ($self->locations_for_seq($subj)) {
        my ($lstart, $lstop) = @{$loc};
        next if ($lstop < $start || $lstart > $stop);
        $lstart = $start if ($lstart < $start);
        $lstop  = $stop  if ($lstop  > $stop);
        my @locMaps = $mt->get_mappings( -name1     => $subj, 
                                         -overlap   => [$lstart,$lstop],
                                         %passedArgs, );
        foreach my $lmap (@locMaps) {
            my $lmid = $lmap->id;
            # We already found this map from another location:
            next if ($checkMaps{$lmid});
            # Do not include the map if it is THIS map
            next if ($mid && $lmid eq $mid);
            # Do not include reverse hits to the query:
            my $los = $lmap->other_seq($subj);
            next if ($los->id == $qid);
            $checkMaps{$lmid} = $lmap;
        }
    }

    my $score = $self->score;
    my $len   = $self->length($subj);
  IML: foreach my $map (values %checkMaps) {
#      foreach my $newseq( $map->seqs ) {
#	  # One of the maps should be the original hit - do not include it:
#	  next IML if ($newseq->id == $qid);
#      }
      my $imap = $self->intersection( $map );
      next unless ($imap);
      
      my $frac_data = $imap->tag('FRACTIONAL_COVERAGE');

      # Need to think of a smart way to come up with a combined score...
      my $sscore = $map->score;
      # Find the shorter map, and use it as the score base:
      if ($score && $sscore) {
	  my $slen   = $map->length($subj);
	  my $useSeq = 0; # 0 = use the 'query', 1 = use the subject
	  
	  if ($slen < $len) {
	      # The other map is shorter, calculate hits with the 'subject'
	      $useSeq = 1;
	  } elsif ($slen == $len) {
	      # The maps cover the same distance, use the lower score:
	      $useSeq = 1 if ($score > $sscore);
	  }
          my @both_scores = ($score, $sscore);
          # Recover the fractional coverage reported by intersection()
          my $usefrac   = $frac_data->[ $useSeq ];
	  my $iscore    = $both_scores[ $useSeq ] * $usefrac;
	  my $iauth     = $useSeq ? 
              $map->authority->name : $self->authority->name;
          
	  $iscore = int(0.5 + 100 * $iscore)/100;
	  if ($args->{CUTBRIDGE} && $args->{MINSCORE}) {
	      my ($namematch, $minscore);
	      # Filter by score, if requested.
	      my @keys = keys %{$args->{MINSCORE}};
	      foreach my $namematch ( keys %{$args->{MINSCORE}} ) {
		  my $minscore = $args->{MINSCORE}{$namematch};
		  if ($iauth =~ /$namematch/i && $iscore < $minscore) {
		      next IML;
		  }
	      }
	  }
	  $imap->score($iscore);
      }
      push @maps, $imap;
  }
    return @maps;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 common_sequence

 Title   : common_sequence
 Usage   : $obj->common_sequence( $map )

 Function: Given another map object, will attempt to identify a common
           name (based on database ID) between them. If none exists,
           returns undef.

 Returns : A Sequence object, or undef

 Args    : [0] The other Mapping object you wish to compare to

=cut

sub common_sequence {
    my $self = shift;
    my $subj = $_[0];

    # 'q' = query   = *this* mapping
    # 's' = subject = the other, provided mapping
    my @qSeqs = $self->seqs; # Sequence pair for the query
    my @sSeqs = $subj->seqs; # Sequence pair for the subject
    my ($qin, $sin); # indicies of paired sequences
    for my $i (0..1) {
	for my $j (0..1) {
            # Keep looking if this pairing does not match
	    next unless ($qSeqs[$i]->id == $sSeqs[$j]->id);
            # The pairs match - we found two sequences that are common
	    if (defined $qin) {

		# oops - we had *already* found two sequences in
		# common. This could either be because the two
		# mappings are between the same pair of sequences, or
		# because one of the mappings is a mapping to
		# itself. We can not be sure what the common
		# sequence for comparison is!

		$self->err("More than one common seq found while comparing ".
                           "map_ids ".$self->map_id." to ". $subj->map_id);
		return undef;
	    }
            # Record the query and subject index of the shared sequence
	    ($qin, $sin) = ($i, $j);
	}
    }
    # Could not find a common sequence in the two mappings:
    return undef unless (defined $qin);
    return $qSeqs[$qin];
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 intersection

 Title   : intersection
 Usage   : $obj->intersection( $map )

 Function: Given another map object, will attempt to identify a common
           name (based on database ID) between them. If none exists,
           returns undef. If there is a common name, will generate a
           Mapping object that defines the intersection between the
           loci in the two mappings.

 Returns : A Mapping object, or undef

 Args    : [0] The other Mapping object you wish to compare to

=cut

sub intersection {
    my $self = shift;
    my $subj = $_[0];

    
    # Try to find which sequence is common between two mappings
    my $commonSeq = $self->common_sequence( $subj );
    return undef unless ($commonSeq);

    # 'q' = query   = *this* mapping
    # 's' = subject = the other, provided mapping
    my @qSeqs = $self->seqs; # Sequence pair for the query
    my @sSeqs = $subj->seqs; # Sequence pair for the subject
    my ($qin, $sin) = ($self->seq_index( $commonSeq ),
                       $subj->seq_index( $commonSeq ) );

    # Deterimine if any of the locations overlap:
    my @qLocs = $self->locations; # each loc is format
    my @sLocs = $subj->locations; # [seq1_start,seq1_end,seq2_start,seq2_end]
    my $mt = $self->tracker;
    my $bAuth = $mt->get_authority("Bridge Builder");
    $bAuth = $bAuth->copy;
    my @allLocs = ();
    my @comLocs = ();

    my @qStep = $self->transform->step;
    my @sStep = $subj->transform->step;
    # The interval separating adjacent positions in the *un*common sequence:
    my $singleQ = 1 / $qStep[$qin]; # Query separation
    my $singleS = 1 / $sStep[$sin]; # Subject separation
    my $singleI = 1 / $sStep[!$sin]; # Intersection (Common) separation

    # For efficiency, we need to know if the subject coordinates are
    # going to be ascending or descending as we cycle through
    # @sLocs. They will always be ascending *unless* $sin is 1 and
    # strand is -1

    my $isascending = ($subj->strand < 0 && $sin == 1) ? 0 : 1;

    foreach my $qloc (@qLocs) {
	# s/e = start/end
	my ($qs, $qe) = ($qloc->[$qin * 2], $qloc->[$qin * 2 + 1]);
	foreach my $sloc (@sLocs) {
	    my ($ss, $se) = ($sloc->[$sin * 2], $sloc->[$sin * 2 + 1]);

	    # DOUBLE CHECK THIS FOR EDGE GAPS!!
            if ($isascending) {
                # Go to next query if location is 'past' the current query,
                # because all other subject locations will be past, too:
                last if ($ss > $qe);
                # Otherwise, go to next subject if we are not overlapping -
                # subjects to come may still be able to overlap
                next if ($se < $qs);
            } else {
                # Just the reverse logic for when subjects are descending
                last if ($se < $qs);
                next if ($ss > $qe);
            }

	    # If we got here, then the locs overlap at least some. If
	    # a gap location is adjacent to a normal location, the
	    # above logic will reject it as non-overlapping - this is
	    # probably the desired behavior. For example, for a query
	    # loc where $qs..$qe = 10..20 , then the flanking gaps
	    # where $ss..$se are either 10..9 or 21..20 will fail.

	    # Find the intersection start/end of the SHARED sequence:
	    my $is = $qs > $ss ? $qs : $ss; # Take the largest start coordinate
	    my $ie = $qe < $se ? $qe : $se; # Take the smallest end coordinate
	    # Now, what are the mapped coordinates for the uncommon seqs?
	    my ($qis, $qie, $sis, $sie);
	    if ($is == $ie + $singleI) {


		# This is a gap position - we have NOT tracked if Q or
		# S (or both!) is responisble for the gap, so we need
		# to grab flanking sequences, then make sure that
		# start > end. There may be a more elegant way to do
		# this, but this works, and it prevents the need to
		# independantly check the subject and query to see if
		# they are gaps.

		my ($qa, $qb) = ( $self->map($commonSeq, $is),
				  $self->map($commonSeq, $ie) );
		my ($sa, $sb) = ( $subj->map($commonSeq, $is),
				  $subj->map($commonSeq, $ie) );
		$qis = ($qa > $qb) ? $qa : $qb;
		$sis = ($sa > $sb) ? $sa : $sb;
		($qie, $sie) = ($qis - $singleQ, $sis - $singleS);
	    } else {
		# warn "<pre>($qs, $qe) - ($ss, $se) not a gap\n";
		($qis, $qie) = $self->map($commonSeq, $is, $ie);
		($qis, $qie) = ($qie, $qis) if ($qis > $qie);
		($sis, $sie) = $subj->map($commonSeq, $is, $ie);
		($sis, $sie) = ($sie, $sis) if ($sis > $sie);
	    }
	    # warn "<pre> ($qs, $qe | $ss, $se) ($is, $ie) ($qis, $qie | $sis, $sie)";
	    push @allLocs, [$qis, $qie, $sis, $sie];
	    push @comLocs, [$is, $ie];
	}
    }
    return undef if ($#allLocs < 0);

    @comLocs = sort { $a->[0] <=> $b->[0] } @comLocs;

    # We now need to make a transform for the new mapping:
    my $comloc = sprintf("%s %d-%d",$commonSeq->name,
			 $comLocs[0][0],$comLocs[-1][1]);

    # We need to know how much we are stepping per unit on the
    # intersection - name1 will be based on the query
    my $step1 = $sStep[$sin] * $qStep[ !$qin ];
    my $step2 = $qStep[$qin] * $sStep[ !$sin ];
    # This is the interval that should separate adjacent locations for
    # name1 and name2:
    my ($single1, $single2) = (1 / $step2, 1 / $step1);

    my $transform = BMS::MapTracker::Transform->new
	( -step1 => $step1, -step2 => $step2, 
	  -name => "Bridge from ". $commonSeq->name);

    # Now see if the locations can be joined up...

    # The logic here is that if, say, you are using the genome to
    # combine maps, there will be discrete locations that are actually
    # adjacent (two genes share adjacent exons, for example). These
    # can conceptually be blobbed together. This appears to work for
    # direct mappings in the same strand. I have not verified it for
    # -1 strand (should be ok) or non 1:1 transforms (makes me leary).

    @allLocs = sort { $a->[0] <=> $b->[0] } @allLocs;
    my @joined = shift @allLocs;
    my $newStrand = $self->strand == $subj->strand ? 1 : -1;
    foreach my $loc (@allLocs) {
	my ($s1, $e1, $s2, $e2) = @{$loc};
	my $delta1 = $s1 - $joined[-1][1];
	my $delta2 = $newStrand > 0 ? 
	    $s2 - $joined[-1][3] : $joined[-1][2] - $e2;
	if ($delta1 == $single1 && $delta2 == $single2 ) {
	    $joined[-1][1] = $e1;
	    if ($newStrand > 0) {
		$joined[-1][3] = $e2;
	    } else {
		$joined[-1][2] = $s2;
	    }
	} else {
	    push @joined, $loc;
	}
    }
    $transform->bridge($commonSeq, \@comLocs);
    # We have an intersection!
    my ($qseq, $sseq) = ( $qSeqs[ !$qin ], $sSeqs[ !$sin ] );
    my $int = BMS::MapTracker::Mapping->new
	( -locations => \@joined,
	  -transform => $transform,
	  -name1     => $qseq,
	  -name2     => $sseq,
	  -strand    => $newStrand,
	  -authority => $bAuth, 
	  -tracker   => $mt, );
    $int->parents( $self, $subj, $commonSeq );
    
    # Calculate the fractional coverage for query and subject:
    my ($qfrac, $sfrac) = (1,1);
    if (my $denom = $self->length( $qseq )) {
        my $numer =  $int->length( $qseq );
        $qfrac = $numer / $denom;
        # warn "Query: $qfrac = $numer / $denom<br />\n";
    }
    if (my $denom = $subj->length( $sseq )) {
        my $numer =  $int->length( $sseq );
        $sfrac = $numer / $denom;        
        # warn "Subject: $sfrac = $numer / $denom<br />\n";
    }
    $int->tag('FRACTIONAL_COVERAGE', [$qfrac, $sfrac]);
    return $int;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub colocalized_seqs {
    my $self = shift;
    my $qry  = shift;
    my $qind = $self->seq_index( $qry );
    unless (defined $qind) {
        $self->err("Query '$qry' is not part of the mapping object");
        return wantarray ? () : undef;
    }
    my @seqs = $self->seqs();
    my @rng  = $self->ranges();
    my $sind = $qind ? 0 : 1;
    my $sID  = $seqs[$sind]->id;
    my ($s, $e) = ($rng[$sind * 2], $rng[1 + $sind * 2]);

    my $dbi = $self->tracker->dbi;
    my $sth = $dbi->{COLOCALIZED_SEQ_STH};
    unless ($sth) {
        my $sel = join(', ', qw(authority_id map_id db_id strand));
        my $sql = <<CLSQL;
SELECT name1, $sel FROM mapping
 WHERE name2 = ? AND start2 = ? and end2 = ?
UNION
SELECT name2, $sel FROM mapping
 WHERE name1 = ? AND start1 = ? and end1 = ?
CLSQL

        $sth = $dbi->{COLOCALIZED_SEQ_STH} = $dbi->prepare
        ( -name => "Find maps for specific coordinates",
          -sql  => $sql);
    }
    $sth->execute($sID, $s, $e, $sID, $s, $e);
    my $rv = $sth->fetchall_arrayref();
    return map { $_->[0] } @{$rv} if (wantarray);
    return $rv;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 parents

 Title   : parents
 Usage   : $obj->parents( [ $map1, $map2 ] )

 Function: Some maps are synthetic results of two other maps - for
           example, intersection() generates a new Mapping object that
           is derived from two other maps sharing a common
           sequence. parents() is a mechanism to store such
           maps. Without arguments, it returns the two parents (if
           any). Two arguments will be assumed to be a request to set
           the parent maps - the first map should correspond to the
           first sequence (name1).

 Returns : The two parent Mappings, if previously set.

 Args    : A sequence identifier or object, and one or more positions.

=cut

sub parents {
    my $self = shift;
    my ($map1, $map2, $commonSeq) = @_;
    if ($map1 && $map2) {
	unless (ref($map1) && $map1->isa('BMS::MapTracker::Mapping') &&
		ref($map2) && $map2->isa('BMS::MapTracker::Mapping') ) {
	    $self->death("Can not set parents($map1, $map2) - must use ".
			 "Mapping objects for both arguments");
	}
	$self->{PARENTS} = [ $map1, $map2 ];
        $self->common_parent_seq($commonSeq || $map1->common_sequence($map2));
    }
    return @{$self->{PARENTS}};
}

sub common_parent_seq {
    my $self = shift;
    if ($_[0]) {
        $self->{COMPAR} = $_[0]; 
    }
    return $self->{COMPAR};
}

sub parent_for_seq {
    my $self = shift;
    my @parents = $self->parents;
    return undef if ($#parents < 0);
    my ($req) = @_;
    my @hits;
    foreach my $map (@parents) {
        push @hits, $map if (defined $map->seq_index($req));
    }
    return $#hits == 0 ? $hits[0] : undef;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 map

 Title   : map
 Usage   : $obj->map($seq, $pos [,$pos2, $pos3, ... ])

 Function: Given a sequence and one or more positions, return the
           corresponding coordinate in the other sequence.

 Returns : A position in the other sequence for each queried position
           provided. undef will be returned if the queried position
           has no corresponding location in the other position, or if
           the provided sequence is not one of the pair in the Mapping
           object. If the return value is requested as a scalar, then
           the first position will be returned - otherwise an array
           will be returned.

 Args    : A sequence identifier or object, and one or more positions.

=cut

sub map {
    my $self  = shift;
    my $query = shift;
    my $in    = $self->seq_index( $query );
    return undef unless (defined $in); # subj is not in mapping
    my $ni    = !$in; # The index of the subject in the mapping
    my @retv  = ();
    my @locs  = $self->locations;
    my @steps = $self->transform->step;
    my ($qStep, $sStep) = ($steps[$in], $steps[$ni]);
    unless ($qStep && $sStep && $#locs > -1) {
	# 0 step size = impossible mapping
	#warn "($qStep && $sStep)<br />";
	@retv = map { undef } @_;
	return wantarray ? @retv : $retv[0];
    }
    my $str   = $self->strand;
    foreach my $pos (@_) {
	# $pos is the position in $self that we want to map into $query coords
	my $mapLoc;
	foreach my $loc (@locs) {
	    my ($qs, $qe) = ($loc->[$in * 2], $loc->[$in * 2 + 1]);

	    # $offset is how far away $pos is from the edge nearest
	    # the subject start. For non-gaps, we will need to
	    # consider $str in the calcualtion, for gaps it stays 0.

	    my $offset = 0;
	    if ($qs == $qe + 1) {
		# This is a gap position
		# warn "<pre>  COMP TO : ($pos < $qs || $pos > $qe) ";
		# Accept $pos if it is on *either* side of the gap location
		next if ($pos < $qe || $pos > $qs);
		# Take the larger of the values (for gaps the first one):
		$mapLoc = $loc->[$ni * 2];
		#warn "<pre>Gap ($qs|$qe) taking $mapLoc for POS $pos\n";
		next;
	    } else {
		next if ($pos < $qs || $pos > $qe);
		$offset = $str > 0 ? $pos - $qs : $qe - $pos;
	    }

	    # The position falls within a location if we got here - we
	    # can now calculate the position in the other sequence.

	    # Did we already get a location? If so, there is a problem:
	    if ($mapLoc) {
		my @seqs = $self->seqs;
		$self->err("Multiple map locations for $pos in ".
                           $seqs[$in]->name." vs. ".$seqs[!$in]->name);
		last;
	    }

	    # What is the corresponding location in the other sequence:
	    my ($ss, $se) = ($loc->[$ni * 2], $loc->[$ni * 2 + 1]);
	    # The new position is the subject start plus the offset, adjusted
	    # by the step multiplier:
	    $mapLoc = $ss + ($offset * $sStep / $qStep);
	    # warn "<pre>POS: $pos CommonSeq:($qs-$qe) SLoc: $ss Off: $offset MAPPED: $mapLoc\n";
	}
	push @retv, $mapLoc;
    }
    return wantarray ? @retv : $retv[0];
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 map_triplet

 Title   : map_triplet
 Usage   : $obj->map_triplet($seq, $pos [,$pos2, $pos3, ... ])

 Function: Similar to map(), but each position will return an array
           reference of three positions - the subject coordinate to
           the 'left', the coordinate that matches, and the coordinate
           to the right. Null matches will be undef, as above.

 Returns : An array of array refs

 Args    : A sequence identifier or object, and one or more positions.

=cut

sub map_triplet {
    my $self  = shift;
    my $query = shift;
    my $in    = $self->seq_index( $query );
    return undef unless (defined $in); # query is not in mapping
    my $ni    = !$in; # The index of the subject in the mapping
    my @retv  = ();
    my @locs  = $self->locations;
    my @steps = $self->transform->step;
    my ($qStep, $sStep) = ($steps[$in], $steps[$ni]);
    unless ($qStep && $sStep && $#locs > -1) {
	# 0 step size = impossible mapping
	#warn "($qStep && $sStep)<br />";
	@retv = map { undef } @_;
	return wantarray ? @retv : $retv[0];
    }
    my $str   = $self->strand;
    foreach my $pos (@_) {
	# $pos is the position in $self that we want to map into $query coords
	my @triplet = (undef, undef, undef);
        my @close = (0, 99999999999999999999999);
	foreach my $loc (@locs) {
	    my ($qs, $qe) = ($loc->[$in * 2], $loc->[$in * 2 + 1]);
	    # What is the corresponding location in the other sequence:
	    my ($ss, $se) = ($loc->[$ni * 2], $loc->[$ni * 2 + 1]);
            # warn "<pre>$pos ? ($qs, $qe) : ($ss, $se)</pre>\n";
            if ($qs == $qe + 1 && $pos >= $qe && $pos <= $qs) {
		# This is a gap position
		# Accept $pos if it is on *either* side of the gap location
		# Take the larger of the values (for gaps the first one):
		$triplet[1] = $ss;
		next;
	    } elsif ($qe < $pos && $pos - $qe < $pos - $close[0]) {
                # The end of this location is less than the position
                # and is nearer to this position than other ends seen
                $triplet[0] = $str > 0 ? $se : $ss;
                $close[0] = $qe;
                # Carry on, since no other tests will match:
                next;
            } elsif ($qs > $pos && $qs - $pos < $close[1] - $pos) {
                # The start of this location is greater than the position
                # and is nearer to this position than other starts seen
                $triplet[2] = $str > 0 ? $ss : $se;
                $close[1] = $qs;
                # Carry on, since no other tests will match:
                next;
            }
            next if ($pos < $qs || $pos > $qe);
            
            

	    # $offset is how far away $pos is from the edge nearest
	    # the subject start. For non-gaps, we will need to
	    # consider $str in the calcualtion, for gaps it stays 0.

	    my $offset = $str > 0 ? $pos - $qs : $qe - $pos;

	    # The position falls within a location if we got here - we
	    # can now calculate the position in the other sequence.

	    # The new position is the subject start plus the offset, adjusted
	    # by the step multiplier:
	    $triplet[1] = $ss + ($offset * $sStep / $qStep);
            # print "$pos (-$offset / $qStep) in [$qs,$qe] =>=> ". $triplet[1]. " ( x $sStep)in [$ss,$se] <br />\n";
	}
	push @retv, \@triplet;
    }
    return wantarray ? @retv : $retv[0];
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 seq_index

 Title   : seq_index
 Usage   : $obj->seq_index($seq)

 Function: Given a sequence, return the index of the sequence in the
           mapping. Sequences are matched by database IDs. As a a
           special case, if either 1 or 0 are provided, it is assumed
           that you already have an index (this speeds up operations
           when seq_index is called by another method).

 Returns : 0     if the sequence is name1
           1     if the sequence is name2
           undef if the sequence is not in the mapping
                 (or if *both* seqs are the requested one)

 Args    : A sequence identifier or object

=cut

sub seq_index {
    my $self = shift;
    my ($subj) = @_;
    return undef unless (defined $subj);
    # Allow explicit pre-set indices to be passed and simply returned:
    return $subj if ($subj eq '0' || $subj eq '1');
    my %sids;
    if (ref($subj)) {
        # Reference - hope it's a blessed object than can('id')...
        $sids{ $subj->id } = 1;
    } else {
        my $mt = $self->tracker;
        my @seqs = $mt->get_seq( $subj );
        $self->death("Can not find a sequence for '$subj'") if ($#seqs < 0);
        %sids = map { $_->id => 1 } @seqs;
    }
    my @mapSeqs = $self->seqs;
    my $index = undef;
    for my $i (0..1) {
	next unless ($sids{$mapSeqs[$i]->id});
	return undef if (defined $index);
	$index = $i;
    }
    return $index;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 locations

 Title   : locations
 Usage   : my @locs = $obj->locations($locs)

 Function: Gets / Sets locations. You need to set strand and transform
           first

 Returns : An array of arrays. Each element is a single location, and
           consists of 4 coordinates - [ Seq1start, Seq1end,
           Seq2start, Seq2end ]. The method seq_index is useful here
           to determine which sequence is Seq1 and which is Seq2.

    Args : If you pass an array reference, it will be used to set the
           locations for this object.

=cut

sub locations {
    my $self = shift;
    if ($_[0]) {
	$self->death("Add locations either as an array of array refs, or as ".
		     "a single 2D array ref") unless (ref($_[0]) eq 'ARRAY');
	my @locs;
	if (ref($_[0][0]) eq 'ARRAY') {
	    # 2D array ref
	    @locs = @{$_[0]};
	} else {
	    # Array of array refs
	    @locs = @_;
	}
	my ($step1, $step2) = $self->transform->step;
	my $strand = $self->strand;

        # Fractional positions (such as occur in RNA to Protein
        # mappings) can cause rounding errors - values that should be
        # integer have tiny fractions appended to them. For this
        # reason, many values are rounded to 6 decimal places

	foreach my $row (@locs) {
	    my ($s1, $e1, $s2, $e2) = @{$row};
	    my $len1 = ($e1 - $s1) + (1 / $step2);
	    if (defined $e2) {
		# End2 is supplied, double-check coordinate
		my $len2 =  ($e2 - $s2) + (1 / $step1);
		my $ratio;
		if (&ROUND_SIX($len2) == 0) {
                    # This is a gap - set the ratio to be '1' if the other
                    # location is also a gap
		    $ratio = &ROUND_SIX( $len1 ) == 0 ? 1 : 99999999999;
		} else {
		    $ratio = &ROUND_SIX( ($len1 * $step2) / ($len2 * $step1));
		}
		unless ($ratio == 1) {
		    my $msg = "Specified transformation is for a ".
			"$step1:$step2 mapping, but coordinates ($s1,$e1) [$len1] ".
			    "and ($s2,$e2) [$len2] have a ratio of $ratio";
		    if ($self->{ONFAIL} =~ /return/i) {
			warn "$msg\n";
			return undef;
		    } else {
			$self->death($msg);
		    }
		}
	    } else {
		# Fill in end2
		$e2 = $s2 + ($len1 / $step1) - ( 1 / $step2);
		$row->[3] = $e2;
	    }
	}
	@locs = sort {$a->[0] <=> $b->[0] } @locs;
	$self->{LOCATIONS} = \@locs;
    }
    # Probably best to make a local copy, rather than sending originals:
    my @retval = map { [ @{$_} ] } @{$self->{LOCATIONS}};
    return @retval;
}

sub ROUND_SIX {
    my ($val) = @_;
    return int(0.5 + 100000 * $val) / 100000;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 locations_for_seq

 Title   : locations_for_seq
 Usage   : my @locs = $obj->locations_for_seq($seqobject)

 Function: Gets location array oriented to a specific sequence of the
           mapping.

 Returns : An array of arrays. Each element is a single location, and
           consists of 4 coordinates - [ RequestStart, RequestEnd,
           OtherStart, OtherEnd ].

    Args : The request sequence object

=cut

sub locations_for_seq {
    my $self = shift;
    my ($request) = @_;
    my $in = $self->seq_index($request);
    my @locs = $self->locations;
    if ($in) {
	# The requested sequence is Seq2, we need to swap coordinates
	@locs = map { [ $_->[2], $_->[3], $_->[0], $_->[1] ] } @locs;
        if ($self->strand < 0) {
            # Also, if Seq2 is on the reverse strand, we should:
            @locs = reverse @locs;
        }
    }
    return @locs;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 key_for_seq

 Title   : key_for_seq
 Usage   : my @locs = $obj->key_for_seq($seqobject)

 Function: Gets a string key describing this mapping (relative to the
           provided sequence). Useful for eliminating redundant
           mappings.

 Returns : An array of arrays. Each element is a single location, and
           consists of 4 coordinates - [ RequestStart, RequestEnd,
           OtherStart, OtherEnd ].

    Args : The request sequence object

=cut

sub key_for_seq {
    my $self = shift;
    my ($seq, $posonly) = @_;
    my $oseq = $self->other_seq( $seq );
    my $string = $oseq->id . ":" . $self->strand . ':';
    my @locs = $self->locations_for_seq($seq);
    my @tls  = map { join(',', @{$_}) } @locs;
    $string .= join(';', @tls);
    return $string if ($posonly);
    $string .= sprintf(":%s:%s", $self->score || '', $self->authority->name);
    return $string;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 congealed_locations

 Title   : congealed_locations
 Usage   : my @clocs = $map->congealed_locations($maxdist)

 Function: Returns an array of locations which have been
           'congealed'. This process is useful for reconstituting
           exons in a transcript. Because a location is an ungapped
           run of alignment, it is possible for an exon to be broken
           into multiple locations due to gaps in the alignment.

 Returns : An array of arrays, as per locations(). Note that the
           congealing process will return the same number, or fewer
           (if two or more individual locations are congealed),
           locations as the locations() method.

    Args : [0] Default 25. The maximum distance *difference* to use
           for congealing. For each pair of locations, first the
           distance between to two Seq1 locations is calculated. Then
           the difference between the two Seq2 locations is
           calculated. Finally the difference between the two
           *differences* is calculated. The pair of locations will be
           congealed unless this value is greater than $maxdist.


=cut

sub congealed_locations {
    my $self = shift;
    my ($maxdist) = @_;
    $maxdist ||= 25;
    my @locs = @{$self->{LOCATIONS}};
    my $seed = shift @locs;
    my @cong = ( [ @{$seed} ]  );
    my $str  = $self->strand;
    foreach my $loc (@locs) {
	# Find the distances between the last congealed loc and this one
	my $dist1 = $loc->[0] - $cong[-1][1];
	my $dist2 = $str >= 0 ? 
	    $loc->[2] - $cong[-1][3] : $cong[-1][2] - $loc->[3];
	if (abs($dist1 - $dist2) > $maxdist) {
	    # Distance over limit, treat as distinct locations
	    push @cong, [ @{$loc} ];
	} else {
	    # The locations should be congealed:
	    $cong[-1][1] = $loc->[1];
	    if ($str >= 0) {
		$cong[-1][3] = $loc->[3];
	    } else {
		$cong[-1][2] = $loc->[2];
	    }
	}
    }
    return @cong;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 congealed_locations_for_seq

 Title   :  congealed_locations_for_seq
 Usage   : my @locs = $obj->congealed_locations_for_seq($seqobject)

 Function: Gets location array oriented to a specific sequence of the
           mapping.

 Returns : An array of arrays. Each element is a single location, and
           consists of 4 coordinates - [ RequestStart, RequestEnd,
           OtherStart, OtherEnd ].

    Args : The request sequence object

=cut

sub congealed_locations_for_seq {
    my $self = shift;
    my ($request) = @_;
    my $in = $self->seq_index($request);
    my @locs = $self->congealed_locations;
    if ($in) {
	# The requested sequence is Seq2, we need to swap coordinates
	@locs = map { [ $_->[2], $_->[3], $_->[0], $_->[1] ] } @locs;
        if ($self->strand < 0) {
            # Also, if Seq2 is on the reverse strand, we should:
            @locs = reverse @locs;
        }
    }
    return @locs;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 ranges

 Title   : ranges
 Usage   : $obj->ranges( $optionalSeq );
 Function: Gets the minimum and maximum positions for both sequences
 Returns : Array of ($start1, $end1, $start2, $end2)
 Args    : Optional sequence - if provided, then start1 and start2 will
           be for that sequence, start2 and end2 will be for the
           other.

=cut


sub ranges {
    my $self = shift;
    my ($request) = @_;
    my @range = ( $self->{LOCATIONS}[0][0], $self->{LOCATIONS}[-1][1] );
    if ($self->strand > 0) {
	push @range, ($self->{LOCATIONS}[0][2], $self->{LOCATIONS}[-1][3]);
    } else {
	# Negative strand means that the order is reversed for name 2
	push @range, ($self->{LOCATIONS}[-1][2], $self->{LOCATIONS}[0][3]);
    }
    if ($request) {
	# User wishes to have the range ordered with one member of map first:
	my $in = $self->seq_index($request);
	# Swap pairs if the request is name2:
	@range = ( $range[2], $range[3], $range[0], $range[1] ) if ($in); 
    }
    return @range;
}

sub start {
    my $self = shift;
    my ($request) = @_;
    $self->die("You must indicate the sequence of interest in start()")
        unless ($request);
    my @ranges = $self->ranges($request);
    return $ranges[0];
}

sub end {
    my $self = shift;
    my ($request) = @_;
    $self->die("You must indicate the sequence of interest in end()")
        unless ($request);
    my @ranges = $self->ranges($request);
    return $ranges[1];
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 build_array

 Title   : build_array
 Usage   : $obj->build_array($trackerObject)
 Function: Write the location to a MapTracker database with build_array
 Returns : 
 Args    : 

=cut

*copyload = \&build_array;
sub build_array {
    my $self = shift;
    my ($mt) = @_;
    my $dbi  = $mt->dbi;
    my $mid  = $self->map_id;
    $mid     = $dbi->nextval( 'mapping_seq' ) unless ($mid);
    my ($start1, $end1, $start2, $end2) = $self->ranges;
    my ($id1, $id2) = $self->seqs;
    my $tid   = $self->transform->id;
    my $aid   = $self->authority->id;
    my $sdbid = $self->searchdb;
    $sdbid    = $sdbid->id if ($self->searchdb);
    $dbi->build_array( 'mapping', { 'map_id'       => $mid,
                                    'name1'        => $id1->id,
                                    'start1'       => $start1,
                                    'end1'         => $end1,
                                    'name2'        => $id2->id,
                                    'start2'       => $start2,
                                    'end2'         => $end2,
                                    'trans_id'     => $tid,
                                    'authority_id' => $aid,
                                    'map_score'    => $self->score,
                                    'db_id'        => $sdbid || 0,
                                    'strand'       => $self->strand, } );
    my @locs = $self->locations;
    unless ($#locs < 1) {
	# Only add locations if there are more than one
	# (Mapping table captures the first location)
	foreach my $row (@locs) {
	    my ($s1, $e1, $s2) = @{$row};
            $dbi->build_array( 'location', { map_id => $mid,
                                             start1 => $s1,
                                             end1   => $e1,
                                             start2 => $s2, } );
	}
    }
    $self->map_id($mid);
    return $self;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_text

 Title   : to_text
 Usage   : $obj->to_text($pad)
 Function: Generates an ASCII string describing the map
 Returns : The string
 Args    : Optional pad string (for left offset)

=cut


sub to_text {
    my $self = shift;
    my ($pad) = @_;
    $pad ||= "";
    my $string = "";
    my @seqs = $self->seqs;
    @seqs = $seqs[0] ? map { $_->name } @seqs : ('Unk1','Unk2');
    my $auth = $self->authority ? $self->authority->name : 'Unknown';
    my ($s1, $e1, $s2, $e2) = $self->ranges;
    $string = sprintf("%s%25s %10s -%10s (%s) [%15s] %6s\n", $pad,
		      $seqs[0], &HAPPY_NUM($s1), &HAPPY_NUM($e1),
		      $self->strand < 0 ? '-1' :'+1',
		      $auth, &HAPPY_NUM($self->score || ""));
    # ($s2, $e2) = ($e2, $s2) if ($self->strand < 0);
    $string .= sprintf("%s%25s %10s -%10s\n", $pad, $seqs[1], 
		       &HAPPY_NUM($s2), &HAPPY_NUM($e2));
    return $string;
}

sub to_canvasXpress {
    my $self = shift;
    my ($anch, $meta) = @_;
    my $obj  = $self->other_seq( $anch );
    return undef unless ($obj);
    my $str = $self->strand() || 0;
    my @locs = $self->locations_for_seq( $anch );
    my $hash = {
        %{$meta || {}},
        id      => $obj->name(),
        type    => 'box',
        dir     => $str < 0 ? 'left' : 'right',
        data    => \@locs,
        showDir => 1,
        outline => "#993300",
        fill    => "#cc9933", #'rgb(200,255,255)',
    };
    return $hash;
}


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 full_text

 Title   : full_text
 Usage   : $obj->full_text($pad)
 Function: Generates an ASCII string describing the map in full
 Returns : The string
 Args    : Optional pad string (for left offset)

=cut


sub full_text {
    my $self = shift;
    my ($pad) = @_;
    $pad ||= "";
    my $string = sprintf("%sMapping [ID %s]\n", $pad,
			 $self->map_id || 'not assigned');
    $string .= $self->transform->full_text($pad);
    my @seqs = $self->seqs;
    if ($seqs[0]) {
        foreach my $seq (@seqs) {
            $string .= $seq->to_text("$pad  ");
        }
        @seqs = map { $_->name } @seqs;
    } else {
        @seqs = ('Unk1','Unk2');
    }
    my @locs = $self->locations;
    $string .= sprintf("%s%d Location%s on strand %s [ Score %s ]\n", $pad,
		       $#locs + 1, $#locs == 0 ? '' : 's',
		       $self->strand < 0 ? '-1' :'+1',
		       &HAPPY_NUM($self->score || "unknown"));
    $string .= sprintf("%s %22.22s   %22.22s\n", $pad, @seqs);
    my ($tot1, $tot2) = (0,0);
    my ($step1, $step2) = $self->transform->step;
    foreach my $loc (@locs) {
	my ($s1, $e1, $s2, $e2) = @{$loc};
	$tot1 += ($e1 - $s1) + (1 / $step2);
	$tot2 += ($e2 - $s2) + (1 / $step1);
	$string .= sprintf("%s  %10s -%10s   %10s -%10s\n", $pad,
			  &HAPPY_NUM($s1), &HAPPY_NUM($e1),
			  &HAPPY_NUM($s2), &HAPPY_NUM($e2),
			  );
    }
    $string .= sprintf("%s %22.22s   %22.22s\n", $pad,
		       "Total $tot1", "Total $tot2");
    
    return $string;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_html

 Title   : to_html
 Usage   : print $obj->to_html($pad)
 Function: Generates an HTML string describing the map in full
 Returns : The string
 Args    : Associative array of arguments. Recognized keys [Default]:

    -table Default 1. If true, will flank the string with "<table>"
           tags. If false, it allows you to group multiple mappings
           into a single table.

  -congeal Default 0. If nonzero, then display congealed locations.

 -linkargs Default "". Additional arguments to append to HREFs.

=cut


sub to_html {
    my $self = shift;
    my $args = $self->parseparams( -table     => 1,
				   -congeal   => 0,
				   -linkargs  => "foo=1",
				   @_);
    my $ctd = "td align='center'";
    my $larg = $args->{LINKARGS};
    my $string = "";
    $string .= "<table border='1'>" if ($args->{TABLE});
    $string .= sprintf
	("<tr><td colspan='7' bgcolor='#ccffcc' nowrap><font color='green'><b>Score %s</b></font> <font size='-1' color='brown'>ID %s</font> by %s<br />\n", &HAPPY_NUM($self->score || "unknown"),
	 $self->map_id || 'not assigned', $self->authority->to_html);
    my @seqs = $self->seqs;
    my @locs = $self->locations;
    my $loccount = $#locs + 1;
    my $constr = "";
    if (my $condist = $args->{CONGEAL}) {
	@locs = $self->congealed_locations($condist);
	my $diff = $loccount - ($#locs + 1);
	$loccount = $#locs + 1;
	if ($diff) {
	    $constr = "<br /><font color='red' size='-1'>$diff locations congealed at cuttoff of $condist bp</font>";
	}
    }
    my $str  = $self->strand; my $isFor = ($str > 0);
    $string .= sprintf("%d '%s' location%s in <font color='purple'>".
		       "%s</font> orientation<br/>\n", $loccount,
		       $self->transform->name, $#locs == 0 ? '' : 's',
		       $isFor ? 'forward' :'reverse',
		       );
    my $sdb = $self->searchdb;
    $string .= sprintf("Search Database <font color='blue'>%s</font>\n", 
                       $sdb ? $sdb->name : "Unknown");
    
    $string .= $constr . "</td></tr><tr><th></th>\n";
    my @step = $self->transform->step;
    my @oneStep = ((1 / $step[1]), (1 / $step[0]));

    for my $i (0..$#seqs) {
	$string.= sprintf("<$ctd colspan='3'><i>Step = %s</i><br /><b><a href='mapTracker.pl?%sseqname=%s'>%s</a></b></td>",  &HAPPY_NUM($oneStep[$i]),
			  $larg, $seqs[$i]->name, $seqs[$i]->name, );
    }
    $string .= "</tr>\n";
    $string .= "<tr><th>#</th><th>Location</th><th>Length</th><th>Gap</th><th>Location</th><th>Length</th><th>Gap</th></tr>\n";

    # These are important numbers - they are the unit distance one
    # step will move for each sequence. For example, in a translate
    # transform, seq1 (rna) will have a oneStep of 1/1 = 1, seq 2
    # (prot) will be 1/3 = 0.3333

    my $g1 = "<font color='grey'>n/a</font>"; my $g2 = $g1;
    my ($locnum, $tot1, $tot2, $gap1, $gap2, ) = (0,0,0,0,0);
    for my $i (0..$#locs) {
	my ($s1, $e1, $s2, $e2) = @{$locs[$i]};
	$tot1 += ($e1 - $s1) + $oneStep[0];
	$tot2 += ($e2 - $s2) + $oneStep[1];
	if ($i) {
	    $g1 = ($locs[$i][0] - $locs[$i-1][1] - $oneStep[0]);
	    $g2 = $isFor ? ($locs[$i][2] - $locs[$i-1][3]) : 
		($locs[$i-1][2] - $locs[$i][3]);
	    $g2 -= $oneStep[1];
	    $gap1 += $g1; $gap2 += $g2;
	    $g1 ||= "";
	    $g2 ||= "";
	}
	$string .= sprintf
	    ("<tr><$ctd>%d</td><$ctd>%s - %s</td><$ctd>%s</td><$ctd>%s</td><$ctd>%s - %s</td><$ctd>%s</td><$ctd>%s</td></tr>\n", 
	     ++$locnum,
	     &HAPPY_NUM($s1), &HAPPY_NUM($e1), &HAPPY_NUM($e1 - $s1 + 1), $g1,
	     &HAPPY_NUM($s2), &HAPPY_NUM($e2), &HAPPY_NUM($e2 - $s2 + 1), $g2,
	     );
    }
    my $tc = "bgcolor='#ffcccc'";
    my $ttd = "$ctd $tc";
    $string .= sprintf("<tr><th $tc>Total:</th><$ttd>%d</td><td /><$ttd>%d</td><$ttd>%d</td><td /><$ttd>%d</td></tr>",
		       $tot1, $gap1, $tot2, $gap2,);
    $string .= "</table>\n" if ($args->{TABLE});
    return $string;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 to_html_row

 Title   : to_html_row
 Usage   : print $obj->to_html_row(@args)

 Function: Generates an HTML string describing the map in an
           abreviated form, and relative to one of the sequences
           (-query).

 Returns : The string
 Args    : Associative array of arguments. Recognized keys [Default]:




=cut

sub html_row_header {
    my $self = shift;
    my ($addquery) = @_;
    my @columns = ( "Subject Name", "Subject Class", "Score", "Strand", "# Locs", "Query Range", "Subject Range", "Authority", "Transform", "Map ID","DB ID");
    unshift @columns, "Query Name" if ($addquery);
    return "<tr><th>" . join("</th><th>", @columns) . "</tr>\n"
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub to_html_row {
    my $self = shift;

    my $ctd;

    my $args = $self->parseparams( -query     => undef,
				   -linkargs  => "foo=1",
				   -ingroup   => 0,
				   -isfirst   => 0,
				   -highlight => 0,
                                   -addquery  => 0,
				   @_);
    my $mt    = $self->tracker;
    my $query = $mt->get_seq($args->{QUERY});
    my $larg  = $args->{LINKARGS};
    $larg     = $larg ? "$larg&" : "";
    my $grp   = ($args->{INGROUP} && $args->{INGROUP} > 1 ) ? 
	" rowspan='" . $args->{INGROUP} . "'" : "";

    my $hl = $args->{HIGHLIGHT};
    
    my $string = "<tr>";
    my $td   = "<td align='center'>%s</td>";
    my $tdex = "<td align='center'%s>%s</td>";
    my ($qs, $qe, $ss, $se)  = $self->ranges( $query );
    if (!$grp || $args->{ISFIRST}) {
        if ($args->{ADDQUERY}) {
            my $qname = $query->name;
            $string .=sprintf("<td$grp><a href='mapTracker.pl?%sseqname=%s'>".
                              "%s</a></td>", $larg, $qname, $qname );
        }
	my $oseq  = $self->other_seq( $query );
	my $oname = $oseq->name;
	$string .= sprintf("<td$grp><a href='mapTracker.pl?%sseqname=%s'>".
			   "%s</a></td>", $larg, $oname, $oname );
	$string .= sprintf("<td align='center'$grp>%s</td>", 
			   $oseq->representative_class->name);
    }
    $string .= sprintf($td, &HAPPY_NUM($self->score || "unknown"));
    $string .= sprintf($td, &HAPPY_NUM($self->strand || ""));
    $string .= sprintf($td, $#{$self->{LOCATIONS}} + 1);

    # Query Range:
    $string .= &RANGE_STRING( $qs, $qe, $hl ? $hl->{'Query Range'} : "");

    # Subject Range:
    $string .= &RANGE_STRING( $ss, $se, $hl ? $hl->{'Subject Range'} : "");

    # Authority, Transform, ID:
    $string .= sprintf($td, $self->authority->name );
    $string .= sprintf($td, $self->transform->name );
    my $mid = $self->map_id;
    $string .= $mid ? sprintf
	("<td><a href='mapTracker.pl?%smap_id=%d'>%d</a></td>",
	 $larg, $mid, $mid ) : "<td></td>";
    my $sdb = $self->searchdb;
    $string .= sprintf($td, $sdb ? $sdb->id || "unk" : "unk");
    $string .= "</tr>\n";
    return $string;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub RANGE_STRING {
    my ($start, $end, $highlight) = @_;
    $highlight = $highlight ? " bgcolor='$highlight'" : "";
    my $text;
    if ($start > $end) {
	my $gap = "Gap";
	$gap .= " after $end" if ($end);
	$text = "<font color='red'>$gap</font>";
    } elsif ($start == $end) {
        $text = &HAPPY_NUM($start);
    } else {
	$text = &HAPPY_NUM($start) . "&nbsp;-&nbsp;" . &HAPPY_NUM($end);
    }
    return sprintf("<td align='center'%s>%s</td>", $highlight, $text);
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub RANGE_STRING_TEXT {
    my ($start, $end) = @_;
    my $text;
    if ($start > $end) {
	$text = "Gap";
	$text .= " after $end" if ($end);
    } elsif ($start == $end) {
        return &HAPPY_NUM($start);
    } else {
	$text = &HAPPY_NUM($start) . "-" . &HAPPY_NUM($end);
    }
    return $text;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub to_excel {
    my $self = shift;
    my ($query, $eh, $type) = @_;
    my $mt    = $self->tracker;
    my $qseq  = $mt->get_seq( $query );
    my $qname = $qseq->name;
    my $oseq  = $self->other_seq( $qseq );
    my $sname = $oseq->name;

    my ($qs, $qe, $ss, $se)  = $self->ranges( $qseq );
    my $sdb      = $self->searchdb;
    my $dbname   = $sdb ? $sdb->name : 'Unknown';
    my $repclass = $oseq->representative_class->name;
    my $score    = $self->score;
    my $str      = $self->strand;
    $eh->add_row('Map Detail', 
                 [ $qname, $type, $sname, $repclass, $score, $str,
                   $#{$self->{LOCATIONS}} + 1,
                   $qs, $qe, $qe - $qs + 1, $ss, $se, $se - $ss + 1, 
                   $self->authority->name, $self->transform->name,
                   $self->map_id, $dbname] );
    $eh->add_row('Map Overview', 
                 [ $qname, $type, $sname, $repclass, $score, $str,
                   &RANGE_STRING_TEXT($qs, $qe),
                   &RANGE_STRING_TEXT($ss, $se), ] );

    $oseq->to_excel($eh, "Mapped to $qname")
        unless ($eh =~ /Mapped/);
    
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub HAPPY_NUM {
    my ($num) = @_;
    return $num unless ($num =~ /^[\d\.\+\-]+$/);
    if ($num > 0.1 && $num < 10000) {
	return int(0.5 + 1000 * $num) / 1000;
    }
    return $num;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 length

 Title   : length
 Usage   : $len = $obj->length($seq, $minStart, $maxEnd)

 Function: Provided with a sequence, return the length that the
           sequence covers in the mapping - simply the sum of each
           location length. Boudnary positions can be optionally
           provided - bases outside the boundary will not be counted.

           Note - requiring a reference sequence is technically
           unneccesary for a 1:1 transform (since the length of each
           HSP is identical for either sequence), but is essential for
           other transforms (eg 1:3 translation transforms)

 Returns : An integer, undef if the sequence is not in the map
 Args    : [0] A sequence name, object or id
           [1] Optional minimum start coordinate
           [2] Optional maximum end coordinate

=cut

sub length {
    my $self = shift;
    my ($seq, $min, $max) = @_;
    my $in    = $self->seq_index($seq);
    return undef unless (defined $in);
    my @locs  = $self->locations;
    my @steps = $self->transform->step;
    my $step = $steps[!$in];
    $step = $step ? 1 / $step : $step;
    my $len = 0;
    foreach my $loc (@locs) {
        my ($ss, $se) = ($loc->[$in * 2], $loc->[$in * 2 + 1]);
        next if ( ($min && $se < $min) ||
                  ($max && $ss > $max) );
        $ss = $min if ($min && $ss < $min);
        $se = $max if ($max && $se > $max);
	$len += $se - $ss + $step;
    }
    # warn $seq->name ." Index $in Step $step Len $len<br />\n";
    return $len;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 loc_for_seq

 Title   : loc_for_seq
 Usage   : $locationObject = $obj->loc_for_seq($seq, $offset)

 Function: Provided with a sequence, return a BioPerl Location object
           encoding the locations for that sequence.

 Returns : A BioPerl Location object, either a Simple or Split location
 Args    : Associative array of arguments. Recognized keys [Default]:

     -name Required. A sequence name, object or id

   -offset Default 0. Optional sequence offset. This value will be
           subtracted from all the coordinates. This option is used to
           specify a location that will be displayed from the middle
           of a sequence.

    -scale Optional scaling value. If nonzero, all coordinates will be
           divivded by this number. Scaling is used when the relative
           positions of coordinates are important, but not their exact
           value - for example, when using Bio::Graphics::Panel.

  -congeal If true, then locations will be recovered with
           congealed_locations() rather than locations()

=cut


sub loc_for_seq {
    my $self = shift;
    my $args = $self->parseparams( -name     => undef,
				   -offset   => undef,
				   -scale    => undef,
                                   -congeal  => undef,
                                   -nicegap  => undef,
                                   @_ );
    my ($seq, $offset, $scale) = 
        ($args->{NAME} || $args->{SEQ}, $args->{OFFSET}, $args->{SCALE});
    my $niceGap = $args->{NICEGAP};
    unless ($seq) {
        $self->death("You must define -name when calling loc_for_seq()");
    }
    $offset ||= 0;
    my $in    = $self->seq_index($seq);
    unless (defined $in) {
        $self->death("You requested a location for '$seq', I only know of ".join(", ", map { $_->name } $self->seqs()));
    }
    my @locs  = $args->{CONGEAL} ? 
        $self->congealed_locations : $self->locations;
    # warn $self->full_text();
    # warn "loc_for_seq(".$seq->name().")\n".$self->branch(\@locs);
    my $str   = $self->strand;
    my @locations;
    foreach my $loc (@locs) {
	my ($start, $stop) = ($loc->[$in * 2]     - $offset, 
			      $loc->[$in * 2 + 1] - $offset );
	
	my $isgap = 0;
	if ($start > $stop) {
	    # Gap location
	    $isgap = 1;

	    # NCBI seems to prefer to use the base to the LEFT of the
	    # gap as the gap position, so we will use the smaller value
	    # ($stop) for our BioPerl-friendly, width = 1 gap.

            # 2011 June - CHANGE!
	    $start = $stop unless ($niceGap);

	    # We could use Bio::Location::Fuzzy
	    #my $l = new Bio::Location::Fuzzy( -start    => int(0.5 + $stop), 
            #                                  -end      => int(0.5 + $start), 
            #                                  -strand   => $str, 
            #                                  -loc_type => 'BETWEEN',);

	}

	if ($scale) {
	    $start = int(0.5 + $start / $scale);
	    $stop  = int(0.5 + $stop  / $scale);
	}

        # Round positions using 0.99 - this means that protein
        # sub-codon positions 0.333, 0.666 and 1.000 will all map to
        # position "1".
        my ($s, $e, $l) = (int(0.99 + $start), int(0.99 + $stop));
        if ($isgap && $niceGap) {
            $l = new Bio::Location::Simple
                ( -start  => $e,
                  -end    => $s,
                  -location_type => 'IN-BETWEEN',
                  -strand => $str );
        } else {
            $l = new Bio::Location::Simple
                ( -start  => $s,
                  -end    => $e,
                  -strand => $str );
        }
        $l->{OTHER_PAIR} = [ $loc->[!$in * 2], $loc->[!$in * 2 + 1] ];
	$l->{IS_GAP}      = $isgap;
	push @locations, $l;
    }
    my $locobj;
    if ($#locations > 0 ) {
	$locobj = Bio::Location::Split->new();
        if ($str >= 0) {
            @locations = sort { $a->start <=> $b->start } @locations;
            $locobj->{OTHER_PAIR} = [ $locations[0]{OTHER_PAIR}[0],
                                      $locations[-1]{OTHER_PAIR}[1] ];
        } else { 
	    @locations = sort { $b->start <=> $a->start } @locations;
            $locobj->{OTHER_PAIR} = [ $locations[-1]{OTHER_PAIR}[0],
                                      $locations[0]{OTHER_PAIR}[1] ];
        }
	foreach my $loc (@locations) {
            weaken($loc->{PARENTOBJ} = $locobj);
	    $locobj->add_sub_Location( $loc );
	}
    } else {
	$locobj = $locations[0];
    }
    return $locobj;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 loc_for_other_seq

 Title   : loc_for_other_seq
 Usage   : ($location, $seq2) = $obj->loc_for_other_seq($seq1, $offset)

 Function: As above, but will return the location.

 Returns : A BioPerl Location object, either a Simple or Split location
 Args    : The same arguments as loc_for_seq()

=cut


sub loc_for_other_seq {
    my $self = shift;
    my $args = $self->parseparams( -name     => undef,
				   -offset   => undef,
				   -scale    => undef,
                                   -congeal  => undef,
                                   @_ );
    my $seq1 = $args->{NAME} || $args->{SEQ};
    delete $args->{NAME};
    delete $args->{SEQ};
    my $in    = $self->seq_index($seq1);
    my @seqs  = $self->seqs;
    my $seq2  = $seqs[ !$in ];
    my $loc   = $self->loc_for_seq( -seq => $seq2, %{$args} );
    return wantarray ? ($loc, $seq2) : $loc;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 das_for_seq

 Title   : das_for_seq
 Usage   : $dastext = $obj->das_for_seq($seq)

 Function: Provided with one of the sequences, will return the DAS XML
           specification for the location.

 Returns : An XML string
 Args    : 

=cut

# http://www.biodas.org/documents/spec.html#features
sub das_for_seq {
    my $self = shift;
    unshift @_, '-name' if ($#_ == 0);
    my $args = $self->parseparams( -name     => undef,
                                   -congeal  => undef,
                                   @_ );
    my $in   = $self->seq_index( $args->{NAME} || $args->{SEQ});
    my $text = "";
    return $text unless (defined $in);
    my @seqs = $self->seqs();
    my ($qry, $sbj) = $in ? reverse @seqs : @seqs;

    my $offset = 0;
    my $qname  = $qry->name;
    my $id     = $self->id;
    my $class  = $qry->representative_class();
    my $score  = $self->score;
    $score     = '-' unless (defined $score);
    my $str    = $self->strand || 0;
    $str       = $str > 0 ? '+' : '-' if ($str);
    my $auth   = $self->auth;

    my ($cname, $cid) = ($class->name, $class->id);
    my ($aname, $aid) = ($auth->name, $auth->id);
    my @feats;
    my @locs  = $args->{CONGEAL} ? $self->congealed_locations_for_seq( $sbj ) 
        : $self->locations_for_seq( $sbj );
    # $text .= "<SEGMENT id='$id' start='' stop='' type='$cname' version='1.0' label='$qname'>\n";
    for my $i (0..$#locs) {
        my $mod = $i + 1;
        my $loc = $locs[$i];
	my ($start, $stop) = @{$loc};
        my $label = $#locs ? "$qname HSP $mod" : $qname;
        $text .= " <FEATURE id='$id.$mod' label='$label'>\n";
        $text .= "  <TYPE id='$cid'>$cname</TYPE>\n";
        $text .= "  <METHOD id='$aid'>$aname</METHOD>\n";
        $text .= "  <START>$start</START>\n  <END>$stop</END>\n";
        $text .= "  <SCORE>$score</SCORE>\n";
        $text .= "  <ORIENTATION>$str</ORIENTATION>\n";
        $text .= "  <PHASE>-</PHASE>\n"; # Could enhance
        $text .= "  <GROUP id='$id' />\n"; # Could enhance
        $text .= " </FEATURE>\n";
    }
    # $text .="</SEGMENT>\n";
    return $text;
}

sub das_for_other_seq {
    my $self = shift;
    unshift @_, '-name' if ($#_ == 0);
    my $args = $self->parseparams( @_ );
    my $seq  = $self->other_seq( $args->{NAME} || $args->{SEQ} );
    return $self->das_for_seq( %{$args}, NAME => $seq );
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

sub other_seq {
    my $self = shift;
    my ($seq1) = @_;
    my $in    = $self->seq_index($seq1);
    return undef unless (defined $in);
    my @seqs  = $self->seqs;
    my $seq2  = $seqs[ !$in ];
    return $seq2;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 shares_coordinates

 Title   : shares_coordinates
 Usage   : $obj->shares_coordinates($othermap, $seq)

 Function: Determines if the mapping has the same coordinates recorded
           for name $seq in the second map $othermap

 Returns : 
 Args    : 

=cut


sub shares_coordinates {
    my $self = shift;
    my ($othermap, $name) = @_;
    # Maps are not on the same strand:
    return 0 unless ($self->strand == $othermap->strand);
    my $mt  = $self->tracker;
    my $seq = $mt->get_seq( $name );
    # That name not defined in database:
    return 0 unless ($seq);
    my $id = $seq->id;
    my ($in1, $in2) = ($self->seq_index($id), $othermap->seq_index($id));
    my ($ni1, $ni2) = (!$in1, !$in2);
    # That name not defined in both mappings:
    return 0 unless (defined $in1 && defined $in2);
    my @loc1 = $self->locations;
    my @loc2 = $othermap->locations;
    # Different number of locations:
    return 0 unless ($#loc1 == $#loc2);
    for my $i (0..$#loc1) {
	# warn $loc1[$i][$in1 * 2]."-".$loc2[$i][$in2 * 2] . " / ".$loc1[$i][$in1 * 2 + 1]."-".$loc2[$i][$in2 * 2 + 1]."<br />\n";
	# Different start coordinates for this sequence:
	return 0 unless ($loc1[$i][$in1 * 2] == $loc2[$i][$in2 * 2]);
	# Different stop coordinates for this sequence:
	return 0 unless ($loc1[$i][$in1 * 2 + 1] == $loc2[$i][$in2 * 2 + 1]);

	# Different start coordinates for other sequence:
	return 0 unless ($loc1[$i][$ni1 * 2] == $loc2[$i][$ni2 * 2]);
	# Different stop coordinates for other sequence:
	return 0 unless ($loc1[$i][$ni1 * 2 + 1] == $loc2[$i][$ni2 * 2 + 1]);
	
    }
    return 1;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

=head2 

 Title   : 
 Usage   : $obj->($id)
 Function: 
 Returns : 
 Args    : 

=cut


sub f {
    my $self = shift;
    my ($id) = @_;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

1;
