#!/stf/biobin/perl -w

BEGIN {
    # Needed to make my libraries available to Perl64:
    use lib '/stf/biocgi/tilfordc/released';
    # Allows usage of beta modules to be tested:
    my $prog = $0;
    my $pwd = `pwd`;
    if ($prog =~ /working/ || $pwd =~ /working/) {
	print "\n*** THIS VERSION IS BETA SOFTWARE ***\n\n";
	require lib;
	import lib '/stf/biocgi/tilfordc/perllib';
    }
    $| = 1;
    print '';
}
my $VERSION = ' $Id$ ';

=head1 Relevant files

 names.dmp
 nodes.dmp
 merged.dmp
 delnodes.dmp

=cut

use strict;
use BMS::MapTracker;
use BMS::BmsArgumentParser;

my $args = BMS::BmsArgumentParser->new
    ( -wget     => "/work5/tilfordc/WGET/taxonomy_wget.sh",
      -verbose  => 1,
      -testmode => 1,
      -errormail  => 'charles.tilford@bms.com',
      -progress => 60,
      );


my $mt  = BMS::MapTracker->new( -username => 'tilfordc' );
my $dbi = $mt->dbi;

my $wcmd    = $args->{WGET} || '-UNDEF-';
my $vb      = $args->val(qw(vb verbose)) || 0;
my $tm      = $args->{TESTMODE};
my $limit   = $args->{LIMIT};
my $onlynew = $args->val(qw(newonly onlynew));

$args->msg_callback(0, 'global') unless ($vb);

$args->death("Failed to find shell script", $wcmd) unless (-e $wcmd);

# Parse file to figure out where stuff is going
my %vars;
open (PW, "<$wcmd") || $args->death("Failed to read wget shell", $wcmd, $!);
while (<PW>) {
    if (/^\s*([A-Z]+)\s*\=\s*(\S+)/) {
        my ($key, $val) = ($1, $2);
        if ($val =~ /^\'(.+)\'$/ || $val =~ /^\"(.+)\"$/) {
            $val = $1;
        }
        $vars{$key} = $val;
    }
}

foreach my $key qw(TARGDIR SOURCEDIR LOGFILE) {
    $args->death("Could not parse the $key variable from '$wcmd'")
        unless ($vars{$key});
}

my $log  = $vars{TARGDIR} .'/'. $vars{LOGFILE};
my $dir  = $vars{TARGDIR} .'/'. $vars{SOURCEDIR};
my $cdir = $vars{TARGDIR} ."/TaxonomyChanges";

$args->msg("Working directory", $dir, "Logfile", $log);

unless (-d $cdir) {
    mkdir($cdir);
    chmod(0777, $cdir);
}

unless ($args->{REPARSE}) {
    $args->msg("Updating local data via FTP to NCBI");
    system($wcmd);
}

$args->death("Failed to find the wget log file", $log) unless (-e $log);
my %newfiles;
open (LOG, "<$log") || $args->death("Failed to read wget log", $log, $!);
while(<LOG>) {
    if (/ \-\s+(.+)\s+saved/) {
        my $full = $1;
        $full    =~ s/\P{IsASCII}//g;
        $full    =~ s/^[\`\'\"]//; 
        $full    =~ s/[\`\'\"]$//; 
        my @path = split(/\//, $full);
        $newfiles{ $path[-1] } = $full;
    }
}
close LOG;

my $sep   = '\t\|\t';


$args->msg("New files:", map {"'$_'"} sort keys %newfiles) if ($vb > 1);
&read_tree if ($newfiles{'taxdump.tar.gz'} || $args->{REPARSE} );

$args->msg("Taxonomy load finished");

sub read_names {
    my $file = "names.dmp";
    my $path = "$dir/$file";
    my $total = &line_count($path);
    $args->msg("Loading $total Taxa Names", $file);
    open(NAMES, "<$path") || $args->death("Could not read name file",$path,$!);
    my $prog = {
        start => time,
        prior => time,
        count => 0,
        total => $total,
    };
    my %namehash;
    while (<NAMES>) {
        chomp;
	s/\t\|$//;
	my ($taxid, $name, $unique, $class) = split(/$sep/, $_);
        # Clean up the name
        if ($name =~ /^\s*\'\s*(.+)\s*\'\s*$/ ||
            $name =~ /^\s*\"\s*(.+)\s*\"\s*$/) {
            # Remove flanking quotes
            $name = $1;
        }
        # Remove leading and trailing spaces
        $name =~ s/^\s*//;
        $name =~ s/\s*$//;
        # Collapse whitespace runs
        $name =~ s/\s+/ /;

        $class = uc($class);
        push @{$namehash{$taxid}{$class}}, $name;
        $prog->{recent} = sprintf("%8d : %s (%s)", $taxid, $name, $class);
        $prog->{count}++;
        &NOTE_PROGRESS( $prog );
    }
    close NAMES;
    return \%namehash;
}

sub existing_taxa {
    $args->msg("Finding existing taxa");
    my $data = $dbi->selectall_arrayref
        ( -sql   => "SELECT tax_id, parent_id, merged_id FROM species",
          -name  => "Find existing taxa",
          -level => 1 );
    my %exist;
    foreach my $row (@{$data}) {
        my ($tid, $pid, $mid) = @{$row};
        $exist{$tid} = {
            pid => $pid,
            mid => $mid,
        };
    }
    $args->msg("  ",sprintf("Total of %d TaxIDs already exist", $#{$data}+1));
    return \%exist;
}

sub read_tree {
    my $namehash = &read_names;
    my $exist    = &existing_taxa;
    my $file = "nodes.dmp";
    my $path = "$dir/$file";
    my $total = &line_count($path);
    $args->msg("Loading $total Taxa Nodes from file", $file);
    my %ranks;

    my $prog = {
        start => time,
        prior => time,
        count => 0,
        total => $total,
    };


    my @changes;
    my @cols = qw(tax_id parent rank embl_code div_id div_flag code_id gc_flag
                  mito_id mgc_flag hidden_flag hidden_subtree_flag comments);
    open(NODES, "<$path") || $args->death("Could not read nodes", $path, $!);
    my $novel   = 0;
    my $newtree = 0;


    my $upd = $dbi->prepare
        ( -sql => "UPDATE species SET".
          " taxa_name = ?, parent_id = ?, taxa_rank = ?, hide_flag = ?".
          " WHERE tax_id = ?",
          -name => "Update basic information for TaxID",
          -level => 2 );

    my $ins = $dbi->prepare
        ( -sql => "INSERT INTO species ".
          "  ( taxa_name, parent_id, taxa_rank, hide_flag, tax_id ) ".
          "  VALUES (?, ?, ?, ?, ?)",
          -name => "Insert new TaxID into table",
          -level => 2 );
    
    my $delAli = $dbi->prepare
        ( -sql => "DELETE FROM species_alias WHERE tax_id = ?",
          -name => "Clear old species aliases",
          -level => 2 );

    my $insAli = $dbi->prepare
        ( -sql => "INSERT INTO species_alias".
          " (tax_id, alias, name_class) VALUES(?, ?, ?)",
          -name => "Add species aliases",
          -level => 2 );
        

    while (<NODES>) {
        chomp;
	s/\t\|$//;
        my @row = split(/$sep/, $_);
        my %hash;
        for my $i (0..$#cols) {
            $hash{ $cols[$i] } = $row[$i];
        }
        my $taxid = $hash{tax_id};
        unless ($taxid) {
            &msg("Row lacking taxid", $_);
            next;
        }
        next if ($onlynew && $exist->{$taxid});
        my $names = $namehash->{$taxid};
        unless ($names) {
            &msg("No name information for taxid", $taxid);
            next;
        }
        my $scinames = $names->{'SCIENTIFIC NAME'};
        unless ($scinames) {
            &msg("Scientific name absent", $taxid);
            next;
        }

        if ($#{$scinames} > 0) {
            &msg("Multiple scientific names", $taxid, join(",", @{$scinames}));
        }
        
        my $sciname = $scinames->[0];

        my ($parent, $rank, $hide) = ($hash{parent} || 0, lc($hash{rank}), 
                                      $hash{hidden_flag} ? 'TRUE' : 'FALSE' );
        $parent = 0 if ($parent == $taxid);

        my @sths;
        if (my $edat = $exist->{$taxid}) {
            # Update an existing taxonomy entry
            $edat->{pid} ||= 0;
            if ($parent != $edat->{pid}) {
                $newtree++;
                push @changes, [ '%s parent changed from %s to %s',
                                 $taxid, $edat->{pid}, $parent];
            }
            push @sths, [ $upd, [$sciname, $parent, $rank, $hide, $taxid] ];
            push @sths, [ $delAli, [$taxid] ];
        } else {
            # Entirely new taxa
            push @sths, [ $ins, [$sciname, $parent, $rank, $hide, $taxid] ];
            push @changes, [ '%s - new taxa', $taxid, ];
            $novel++;
        }

        # Now add aliases
        while (my ($class, $list) = each %{$names}) {
            foreach my $ali (@{$list}) {
                push @sths, [ $insAli, [$taxid, $ali, $class] ];
            }
        }
        &execute_sths( @sths );

        $exist->{$taxid} = 1;
        $ranks{ $hash{rank} }++;
        $prog->{recent} = "$taxid : $sciname";
        $prog->{count}++;
        &NOTE_PROGRESS( $prog );
        if ($limit && $prog->{count} >= $limit) {
            $args->msg("[LIMIT]", "-limit halt at $limit");
            last;
        }
    }
    close NODES;

    $args->msg
        ("Changes:", "$novel new taxa", "$newtree parentage modifications",
         "Observed ranks:",
         map { sprintf("%30s [%6d] %d char", $_, $ranks{$_}, length($_)) 
               } sort { $ranks{$b} <=> $ranks{$a} } keys %ranks);

    push @changes, &merged_nodes($exist);
    push @changes, &deleted_nodes($exist);

    return if ($#changes < 0);

    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
        localtime(time);
    my $now = sprintf
        ("%s/Taxonomy_Changes_%04d-%02d-%02d",$cdir,$year+1900, $mon+1, $mday);
    my ($mod, $mnum) = ("",'');
    while (-e $now.$mod.$mnum) {
        $mod  ||= "_Mk";
        $mnum ||= 0;
        $mnum++;
    }
    my $cfile = $now.$mod.$mnum;

    open (CFILE, ">$cfile") || $args->death
        ("Failed to write changes", $cfile, $!);
    foreach my $row (@changes) {
        my @tids = @{$row};
        my $frm = shift @tids;
        my @names;
        foreach my $tid (@tids) {
            my ($tx) = $mt->get_taxa($tid);
            my $name = $tx ? $tx->name : '-Unknown-';
            push @names, "$name [$tid]";
        }
        printf(CFILE "$frm\n", @names);
    }
    close CFILE;
}

sub merged_nodes {
    my ($exist) = @_;
    my $file = "merged.dmp";
    my $path = "$dir/$file";
    my $total = &line_count($path);
    my $prog = {
        start => time,
        prior => time,
        count => 0,
        total => $total,
    };
    $args->msg("Flagging $total merged nodes from file", $file);

    my $null = $dbi->prepare
        ( -sql => "INSERT INTO species (tax_id) VALUES (?)",
          -name => "Add empty TaxID stub for merging",
          -level => 2 );

    my $find = $dbi->prepare
        ( -sql => "SELECT taxa_name, parent_id, taxa_rank, hide_flag ".
          "  FROM species WHERE tax_id = ?",
          -name => "Find existing TaxID for merging",
          -level => 2 );
    
    my $upd = $dbi->prepare
        ( -sql => "UPDATE species SET merged_id = ?, taxa_name = ?,".
             " parent_id = ?, taxa_rank = ?, hide_flag = ? ".
             " WHERE tax_id = ?",
          -name => "Merge one TaxID to another",
          -level => 2 );

    open(FH, "<$path") || $args->death("Could not read", $path, $!);
    my $mergedTo = 0;
    my @changes;
    while (<FH>) {
        chomp;
	s/\t\|$//;
        my ($oldid, $newid) = split(/$sep/, $_);
        my @sths;
        if (my $edat = $exist->{$oldid}) {
            next if (defined $edat->{mid} && $newid == $edat->{mid});
        } else {
            push @sths, [ $null, [$oldid] ];
        }
        push @changes, [ '%s merged to %s', $oldid, $newid, ];
        $mergedTo++;
        $find->execute( $newid );
        my $data = $find->fetchall_arrayref( );
        if ($#{$data} > 0) {
            &msg("Multiple entries found while merging", $newid);
            next;
        } elsif ($#{$data} < 0) {
            &msg("No entries found while merging", $newid);
            next;
        }
        my ($name, $pid, $rank, $hide) = @{$data->[0]};
        push @sths, [$upd, [$newid, $name, $pid, $rank || '', $hide, $oldid]];

        &execute_sths( @sths );
        $exist->{$oldid} = 1;

        $prog->{recent} = "$oldid > $newid";
        $prog->{count}++;
        &NOTE_PROGRESS( $prog );
        if ($limit && $prog->{count} >= $limit) {
            $args->msg("[LIMIT]", "-limit halt at $limit");
            last;
        }
    }
    close FH;
    $args->msg("Changes:", "$mergedTo taxa merged");
    return @changes;
}

sub line_count {
    my ($file) = @_;
    my $count = 0;
    unless (-e $file) {
        $args->err("Could not count entries in file", $file, "No such file");
        return $count;
    }
    if (`wc -l $file` =~ /^\s*(\d+)\s*/) {
        $count = $1;
    } else {
        $args->err("Could not count entries in file", $file, "Count failed");
    }
    return $count;
}

sub deleted_nodes {
    my ($exist) = @_;
    my $count = 0;
    my $file = "delnodes.dmp";
    my $path = "$dir/$file";
    my $total = &line_count($path);

    $args->msg("Flagging $total deleted nodes from file", $file);

    my $null = $dbi->prepare
        ( -sql => "INSERT INTO species".
          " (tax_id, taxa_name, parent_id, taxa_rank)".
          " VALUES (?, 'Deleted taxa with unknown name', 0, 'unknown')",
          -name  => "Add empty TaxID stub for reporting deleted nodes",
          -level => 2 );

    my $upd = $dbi->prepare
        ( -sql   => "UPDATE species SET merged_id = 0 WHERE tax_id = ?",
          -name  => "'Delete' a TaxID by setting merged to zero",
          -level => 2 );


    open(FH, "<$path") || $args->death("Could not read file", $path, $!);

    my $prog = {
        start => time,
        prior => time,
        count => 0,
        total => $total,
    };
    my $deprecated = 0;
    my @changes;
    while (<FH>) {
        chomp;
	s/\t\|$//;
        my ($taxid) = split(/$sep/, $_);
        unless ($taxid =~ /^\d+$/) {
           &msg("Poorly formed deleted node id", $taxid);
            next; 
        }
        my @sths;
        if (my $edat = $exist->{$taxid}) {
            if (defined $edat->{mid} && $edat->{mid} == 0) {
                # The entry was already deprecated
                next;
            }
        } else {
            # Put a null entry in
            push @sths, [ $null, [$taxid] ];
        }
        $deprecated++;
        push @changes, [ '%s deprecated', $taxid, ];
        push @sths, [ $upd, [$taxid] ];
        &execute_sths( @sths );

        $exist->{$taxid} = 1;
        $prog->{recent} = $taxid;
        $prog->{count}++;    
        &NOTE_PROGRESS( $prog );
        if ($limit && $prog->{count} >= $limit) {
            $args->msg("[LIMIT]", "-limit halt at $limit");
            last;
        }
    }
    close FH;
    $args->msg("Changes:","$deprecated entries deprecated");
    return @changes;
}

sub execute_sths {
    if ($tm) {
        unless ($tm =~ /quiet/i) {
            foreach my $sdat (@_) {
                my ($sth, $binds) = @{$sdat};
                $sth->pretty_print( @{$binds} );
            }
        }
    } else {
        $dbi->begin_work;
        foreach my $sdat (@_) {
            my ($sth, $binds) = @{$sdat};
            $sth->execute( @{$binds} );
        }
        $dbi->commit();
    }
}

sub msg {
    my $msg = join("\t", @_)."\n";
    warn $msg;
}
sub _escape_text {
    my ($text) = @_;
    $text =~ s/\\/\\\\/g;
    $text =~  s/\'/\\\'/g;
    return $text;
}

sub NOTE_PROGRESS {
    return unless ($vb);
    my ($prog) = @_;
    my $prior  = $prog->{prior};
    my $now    = time;
    return if ($now - $prior < $args->{PROGRESS});
    my $elapsed = $now - $prog->{start};
    my $done    = $prog->{count};
    my $total   = $prog->{total};
    my $remain = '-';
    if ($total && $done) {
        $remain = $elapsed * ( $total - $done ) / $done;
        $remain = sprintf("[ %4.1f min remain]", $remain / 60);
    }
    warn sprintf(" %6d done, %4.1f min %s %s\n",
                 $done, $elapsed / 60, $remain, $prog->{recent} || '');
    $prog->{prior} = $now;
}

