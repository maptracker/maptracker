# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::AccessDenorm;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


# EXEC DBMS_STATS.gather_schema_stats('GENACC', estimate_percent => 100);
# EXEC DBMS_STATS.gather_table_stats('GENACC', 'CONVERSION', estimate_percent => 100);
# -OLD-  EXEC DBMS_UTILITY.analyze_schema('GENACC','COMPUTE');

$BMS::MapTracker::AccessDenorm::VERSION = 
    ' $Id$ ';

=head1 NAME

 BMS::MapTracker::AccessDenorm - API access to the GenAcc database

=head1 MODULE DESCRIPTION

 This module provides convienent access to information stored in
 GenAcc. In addition to retrieving information, the module also serves
 as the means for populating the database. If information is missing
 from the database (or is too old by the user's criteria), new data
 will be populated upon query.

=cut

# Recalculate 2009:    -ageall '3:50pm Aug 31'
# Recalculate 2009:    -ageall '10:10am Sep 1'
# Recalculate 2009:    -ageall '9 Sep 2009'

our $usePostgres = 1;
our $authJoiner  = ' + ';

use strict;
use Scalar::Util qw(weaken);

# use BMS::MapTracker::Shared;
# use BMS::MapTracker;
use BMS::FriendlyDBI;
use BMS::ArgumentParser;
use BMS::MapTracker::PopulateByGenbank;
use BMS::SetCollection;
use Date::Manip;
use CGI;

use vars qw(@ISA);
@ISA      = qw(BMS::MapTracker::Shared);

my $safeAge     = "1 Sep 2009";
my $veryOldAge  = '1 Jan 1796';
my $depToken    = "{Deprecated}";
my $unkToken    = "{UnknownID}";
my $badToken    = "{IllegalNamespace}";
our $converters = {};
our $condChain  = {};

my $nsName = {
    AAD  => 'Affy Array Design',
    AC   => 'Any Chemical',
    AGIL => 'Agilent Probe',
    AL   => 'Any Locus',
    AMBS => 'Ambit Gene Symbol',
    AP   => 'Any Protein',
    APS  => 'Affy Probe Set',
    APRB => 'Affy Probe',
    AR   => 'Any RNA',
    ASEQ => 'Any Sequence',
    AUR  => 'Aureus ID',
    AVAR => 'Any Variant',
    BIO  => 'Biological Object',
    BMSC => 'BMS Compound ID',
    BMSS => 'BMS Gene Symbol',
    BAAD => 'BrainArray Design',
    BAPS => 'BrainArray Probe Set',
    BAPB => 'BrainArray Probe',
    BPS  => 'BMS Probe Set',
    BTFO => 'BMS TF Pathway Ontology',
    CAS  => 'CAS Number',
    CDD  => 'Conserved Domain Database',
    CHR  => 'Chromosome',
    CHEM => 'Chemical Object',
    CLAD => 'CodeLink Array Design',
    CLPS => 'CodeLink Probe Set',
    EC   => 'Evidence Code',
    ENSE => 'Ensembl Exon',
    ENSG => 'Ensembl Gene',
    ENSP => 'Ensembl Protein',
    ENST => 'Ensembl Transcript',
    ENZ  => 'ENZYME',
    FEAT => 'Sequence Feature',
    FLOC => 'Fallback Locus',
    FRNA => 'Fallback RNA',
    FPRT => 'Fallback Protein',
    GDNA => 'Genomic DNA',
    GI   => 'GenBank ID',
    GO   => 'GeneOntology Term',
    HG   => 'HomoloGene',
    ILMD => 'Illumina Design',
    ILMN => 'Illumina Probe',
    ILMG => 'Illumina GI',
    IPI  => 'International Protein Index',
    IPR  => 'InterPro Family',
    IUO  => 'Invitrogen Ultimate ORF',
    LL   => 'LocusLink Gene',
    LT   => 'ListTracker List',
    MANK => 'Manning Kinase Symbol',
    MESH => 'MeSH Term',
    MIRB => 'miRBase',
    MSIG => 'MSigDB Gene Set',
    MDL  => 'MDL Identifier',
    NRDB => 'NRDB',
    NS   => 'Namespace Name',
    ONT  => 'Any Ontology Term',
    ORTH => 'Orthologue',
    PATH => 'WikiPathways',
    PHPS => 'Pharmagene Probe Set',
    PMID => 'PubMed ID',
    PRDT => 'Predicted Transcript',
    PUBC => 'PubChem ID',
    RS   => 'Reliable Synonym',
    RSP  => 'RefSeq Protein',
    RSR  => 'RefSeq RNA',
    SEQ  => 'Biological Sequence',
    SET  => 'Conceptual Set',
    SMI  => 'SMILES ID',
    SMDL => 'SMDL Index',
    SNP  => 'dbSNP',
    SP   => 'Swiss-Prot',
    SPN  => 'Swiss-Prot Name',
    SYM  => 'Gene Symbol',
    SYML => 'Specified Symbol',
    TAX  => 'Taxonomy',
    TASK => 'Load Task',
    TEST => 'Testing Value',
    TR   => 'TrEMBL',
    TRC  => 'RNAi Consortium Reagent',
    TRN  => 'TrEMBL Name',
    UCSC => 'UCSC Gene',
    UG   => 'UniGene',
    UP   => 'UniProt',
    UPC  => 'UniParc',
    UPN  => 'UniProt Name',
    UNK  => 'Unknown',
    WP   => 'Wikipedia Article',
    XONT => 'Xpress Ontology',
};

if (0) {
    my $ldap = $ENV{USER} || $ENV{LDAP_USER};
    if ($ldap eq 'tilfordc') { warn "ALL OTHER USERS ARE LOCKED OUT!" } else { die "AccessDenorm rule set is under construction and is locked out as unsafe!"; }
}

# Namespaces that should not be used on conditional automated paths
# TO: Starting with the alias
#     Maybe waht to keep SET here??
my $skipCondTo   = { map {$_ => 1} qw(NS ORTH RS SET TAX) };
# FROM: Ending with the alias
my $skipCondFrom = { map {$_ => 1} qw(NS ORTH RS SET) };

# SP : http://www.expasy.org/sprot/userman.html#AC_line
# UP : http://www.expasy.org/sprot/relnotes/sp_soon.html#AC
# SPN / TRN: http://www.expasy.org/sprot/userman.html#ID_line
# Infered, but not certain: TR == UP , SP != UP

my $reliableGuess = {
    map { $_ => 1 } qw(RSP RSR BTFO CDD CAS XONT ENSE ENSG ENSP ENST IPR GO HG PUBC PMID LL IPI IUO ENZ TRC AUR SMDL MDL MSIG GI UG ILMN ILMG PHPS MIRB) };

# Make note of namespaces that have subset categories

my $hasSubsetCategory = {
    map { $_ => 1 } qw(GO XONT MSIG)
};

my $guessRegExp =
    [ # Highest confidence first
      ['RSP',  '[NXY]P_(\d{9}|\d{6})(\.\d+)?(\{[^\}]+\})?'],
      ['RSR',  '[NX][MR]_(\d{9}|\d{6})(\.\d+)?(\{[^\}]+\})?'],
      ['BTFO', 'BTFO\:\S+'],
      ['TASK', 'TASK\:.+'],
      ['CDD',  'CDD\:\d+'],
      ['PATH', 'WikiPathways\:\d+'],
      ['CAS',  'CAS\:\d+\-\d+\-\d+'],
      ['BPS',  'BMS\.\d{2,}\_.+_[sdmco]'],
      ['XONT', 'XONT\:[A-Z]\d+'],
      ['XONT', 'XONT\:[A-Z]\d+[A-Z]\d+'],
      ['MESH', 'MeSH\:.+'],
      ['MIRB', 'MIMAT\d{7,}'],
      ['MSIG', 'MSigDB\:.+'],
      ['LT',   'ListTracker\:\d+'],
      ['ENSE', 'ENS[A-Z]{0,4}E\d{11}(\.\d+)?'],
      ['ENSG', 'ENS[A-Z]{0,4}G\d{11}(\.\d+)?'],
      ['ENSP', 'ENS[A-Z]{0,4}P\d{11}(\.\d+)?(\{[^\}]+\})?'],
      ['ENST', 'ENS[A-Z]{0,4}T\d{11}(\.\d+)?(\{[^\}]+\})?'],
      ['IPR',  'IPR\d{6}'],
      ['ILMN', 'ILMN[_=]\d+'],
      ['ILMG', 'GI_\d+\-[AIS]'],
      ['PHPS', 'PH:.+'],
      ['GO',   'GO\:\d{7}'],
      ['UPC',  'UPI[A-F0-9]{10}'], # UniParc
      ['HG',   'HomoloGene:\d+'],
      ['PUBC', '(A|S|C)ID\:\d+'],
      ['PMID', 'PMID:\d+'],
      ['AVAR', 'rs\d+'],
      ['AGIL', 'A_[23][2-4]_P\d+'],
      ['LL',   'LOC\d+'],
      ['IPI',  'IPI\d{3,}(\.\d+)?(\{[^\}]+\})?'],
      ['IUO',  'IO[HM]\d+'],
      ['ENZ',  'EC\:[0-9\-]+\.[0-9\-]+\.[0-9\-]+\.[0-9\-]+'],
      ['PRDT', '(GENSCAN|GENEFINDER|SNAP)\d+'],
      ['TRC',  'TRCN\d{10}'],
      ['AUR',  'AUR\d+'],
      ['SMDL', 'SMDL\-\d+'],
      ['MDL',  'MFCD\d+'],
      ['GI',   'GI\:?\d+'],
      ['BAPS', 'BrAr:.+_at'],
      ['BAAD', 'BrAr:.+'],
      ['BMSC', '[A-Z][A-Z\s]{0,2}\-\d{1,6}'],
      # Unusual mouse SNP nomenclature
      ['AVAR', 'mm\d+-(\d{1,2}|[XY])-\d+'],
      ['GI',   'gi\d{4,}'],
      # $*^%! Affy Polymorphism
      ['AVAR', '(AFFX-)?SNP(_[A-Z])?[-_]\d+'],
      ['AVAR', 'CN_\d{6,}'],
      ['APS',  'AFFX-\S+_[as]t'],
      ['APS',  '(Can|CHO|Dro|Hu|Mo|Ra)Gene_\d+'],
      ['UCSC', 'uc\d{3}[a-z]{3}\.\d+'],
      
      ['APS',  '(Contig)?\d+(_[fgirsx])?_at'],
#      ['APRB', 'AFFX-\S+_[as]t\:\d+'],
#      ['APRB', '(Contig)?\d+(_[fgirsx])?_at\:\d+'],
      # Lower confidence
      ['NRDB', 'P\d+_\d+'],
      ['UG',   '[A-Z][a-z]+\.\d+'],
      ['UP',   '([OPQ]\d[A-Z\d]{3}\d|[A-NR-Z]\d[A-Z][A-Z\d]{2}\d)(\-\d+)?(\.\d+)?'],
      # Swiss prot is now including UniProt format??
      ['SP',   '([OPQ]\d[A-Z\d]{3}\d|[A-NR-Z]\d[A-Z][A-Z\d]{2}\d)(\-\d+)?(\.\d+)?'],
      ['SP',   '[OPQ]\d[A-Z\d]{3}\d(\-\d+)?'],
      ['TR',   '([OPQ]\d[A-Z\d]{3}\d|[A-NR-Z]\d[A-Z][A-Z\d]{2}\d)(\-\d+)?(\.\d+)?'],
      # Low-digit numbers without leading zeros should be presumed symbols:
      # eg ABC-1
      ['SYM',  '[A-Z]{1,4}\-?\d{0,2}L?'],
      # Very low confidence
      ['NRDB', 'P\d+_\d+ \(.+\)'],
      ['UPC',  'UPI[A-F0-9]{10} \(.+\)'], # UniParc
      ['TAX',  '[A-Z][a-z]+ [a-z]+'],
      ['APS',  '\S+(_[fgirsx])?_[as]t'],
      ['SYML',  '(\S+) \([^\)]+\)'],
      ['APS',  '\d{1,2}trn[A-Z]{1,2}-\S{3,4}'],
#      ['APRB', '\S+(_[fgirsx])?_[as]t\:\d+'],
#      ['APRB', '\d{1,2}trn[A-Z]{1,2}-\S{3,4}\:\d+'],
      # Uniprot names go last to avoid collision with Affy RegExp
      ['UPN',  '[A-Z\d]{1,6}_[A-Z\d]{1,5}'],
      ['SPN',  '[A-Z\d]{1,5}_[A-Z\d]{1,5}'],
      ['TRN',  '[A-Z\d]{1,6}_[A-Z\d]{1,5}'],
      # Unusual
      ['RSP',  'ZP_\d{8}(\.\d+)?(\{[^\}]+\})?'],
      ];

my $nsRegExp = {};
map { push @{$nsRegExp->{$_->[0]}}, '^'.$_->[1].'$' } @{$guessRegExp};

my $suppressConversion = {
    # RS   => { ANY  => 1, },
    EC   => { map { $_ => 1 } qw(RS SET TAX) },
    NS   => { TAX  => 1, },
    TAX  => { TAX  => 1, },
};

my $canonical = {
    AL   => "AR",
    AR   => "AP",
    LL   => "RSR",
    RSR  => "RSP",
    ENSG => "ENST",
    ENST => "ENSP",
};
my $canonHash = {};
while (my ($ns1, $ns2s) = each %{$canonical}) {
    map { $canonHash->{$ns1}{$_} = 1;
          $canonHash->{$_}{$ns1} = 1; } split(/\s+/, $ns2s);
}

my $seq2seqType = {
    AR => { AP => 'can be translated to generate', },
    AP => { AR => 'is translated from', },
};

my $bioTypes = {
    AR => { 
        AP => 'can be translated to generate',
        AL => 'is contained in locus',
    },
    AP => {
        AR => 'is translated from',
        AL => 'is a protein from locus',
    },
    AL => {
        AR => 'is a locus containing',
        AP => 'is a locus with protein',
    },
};
our ($bioRoot);

my $sequenceNs = {
    map { uc($_) => 1 } qw(AP AR ASEQ RSP RSR ENST ENSP ENSE UP UPC SP TR IPI IUO MANK BMSS AMBS)
};

my $crossSpeciesNs = {
    map { uc($_) => 1 } qw(NRDB UPC SYM MSIG)
};

my $caseIsImportant = {
    map { uc($_) => 1 } qw(SYM)
};

my $unversionedNS = {
    map { uc($_) => 1 } qw(NRDB UG MANK BMSS AMBS UCSC)
};

my $nsAlias = {
    AG      => 'AL',
    AONT    => 'ONT',
    BRAD    => 'BAAD',
    BRARAD  => 'BAAD',
    BRARPB  => 'BAPB',
    BRARPRB => 'BAPB',
    BRARPS  => 'BAPS',
    BRPB    => 'BAPB',
    BRPRB   => 'BAPB',
    BRPS    => 'BAPS',
    ILPS    => 'ILMN',
    ILGI    => 'ILMG',
    IOH     => 'IUO',
    IOM     => 'IUO',
    IOU     => 'IUO',
    JOB     => 'TASK',
    LIST    => 'LT',
    LOC     => 'LL',
    LSYM    => "SYML",
    MFCD    => 'MDL',
    MSIGDB  => 'MSIG',
    NSN     => "NS",
    PH      => 'PHPS',
    PPS     => 'PHPS',
    TAXA    => "TAX",
    TRCN    => "TRC",
    UPI     => 'UPC',
    VAR     => 'AVAR',
    WPA     => 'WP',
};

my $directSymbolNS = { map { $_ => 1 } qw(LL ENSG) };

my $nsParents = {
    # TRC  => [ 'AC' ],
    AGIL => [ 'BIO' ],
    AL   => [ 'BIO' ],
    AMBS => [ 'AP' ],
    AP   => [ 'ASEQ' ],
    APS  => [ 'BIO' ],
    BAPS => [ 'BIO' ],
    AR   => [ 'ASEQ' ],
    ASEQ => [ 'BIO' ],
    AUR  => [ 'AC' ],
    BMSC => [ 'AC' ],
    BMSS => [ 'AP' ],
    BPS  => [ 'APS' ],
    BTFO => [ 'ONT' ],
    CAS  => [ 'AC' ],
    CDD  => [ 'ONT' ],
    CHR  => [ 'GDNA' ],
    ENSG => [ 'AL' ],
    ENSP => [ 'AP' ],
    ENST => [ 'AR' ],
    GO   => [ 'ONT' ],
    ILMN => [ 'BIO' ],
    IPI  => [ 'AP' ],
    IPR  => [ 'ONT' ],
    IUO  => [ 'AR' ],
    LL   => [ 'AL' ],
    LT   => [ 'ONT' ],
    MANK => [ 'AP' ],
    MDL  => [ 'AC' ],
    MESH => [ 'ONT' ],
    MIRB => [ 'AR' ],
    MSIG => [ 'ONT' ],
    NRDB => [ 'AP' ],
    PATH => [ 'ONT' ],
    PHPS => [ 'BIO' ],
    PMID => [ 'ONT' ],
    PRDT => [ 'AR' ],
    PUBC => [ 'AC' ],
    RSP  => [ 'AP' ],
    RSR  => [ 'AR' ],
    SEQ  => [ 'AC' ],
    SMDL => [ 'AC' ],
    SMI  => [ 'AC' ],
    SNP  => [ 'AVAR' ],
    SP   => [ 'UP' ],
    SPN  => [ 'UPN' ],
    SYM  => [ 'BIO' ],
    SYML => [ 'AL' ],
    TR   => [ 'UP' ],
    TRN  => [ 'UPN' ],
    UCSC => [ 'AR' ],
    UG   => [ 'AL' ],
    UP   => [ 'AP' ],
    UPC  => [ 'UP' ],
    XONT => [ 'ONT' ],
};

# Some RegExps (ie for UniProt) are such that it is possible to guess
# a general parent, but not the specific child namespace from a
# name. $confoundedGuess is provided to help provide specificity when
# guessing a namespace when a specific known namespace is also provided.

my $confoundedGuess = {
    UP => {
        SP => 1,
        TR => 1,
    },
    UPN => {
        SPN => 1,
        TRN => 1,
    },
    SYM => {
        MANK => 1,
        BMSS => 1,
        AMBS => 1,
    },
};

our @probeSetMatchAuthorities = qw(nosha ensembl affy pharmagene);

our $integerNamespaces = {
    CDD  => 'CDD:%d',
    GI   => 'gi%d',
    GO   => 'GO:%07d',
    IPR  => 'IPR%06d',
    LL   => 'LOC%d',
    LT   => 'ListTracker:%d',
    PMID => 'PMID:%d',
    HG   => 'HomoloGene:%d',
    IPI  => 'IPI%d',
    AUR  => 'AUR%d',
    SMDL => 'SMDL-%d',
    MDL  => 'MFCD%d',
    MIRB => 'MIMAT%07d',
    IPR  => 'IPR%06d',
    ILMN => 'ILMN_%d',
};

my $nsChildren  = { };
my $hasParent   = { };
while (my ($kid, $pars) = each %{$nsParents}) {
    map { push @{$nsChildren->{$_}}, $kid;
          $hasParent->{$kid}{$_} = 1; } @{$pars};
}

my $useMTIDformat = {
    map { uc($_) => 1 } qw(SMI SEQ)
};

my $canonicalChemical = {
    map { uc($_) => 1 } qw(SMI SEQ)
};

my $mappingNS = {
    APRB => '',
    BAPB => '',
    AGIL => '',
    APS  => '',
    AR   => '',
    ASEQ => '',
    AVAR => '',
    BAPS => '',
    BPS  => '',
    ENST => '',
    ILMN => '*AR',
    RSR  => '',
    SNP  => '',
    AL   => 'AR',
    AMBS => 'AR',
    ENSG => 'ENST',
    HG   => 'RSR',
    LL   => 'RSR',
    MANK => 'AR',
    PHPS => '*AR',
    RSP  => '*RSR',
    ENSP => '*ENST',
};

our $mtNamespaces = {
    AC   => '',
    AMBS => '#AmbitSymbol#',
    APRB => '#Sequence#',
    BAPB => '#Sequence#',
    BMSC => '#BMSID#',
    BMSS => '#BmsSymbol#',
    EC   => '#Evidence_Codes#',
    MANK => '#ManningKinase#',
    MSIG => '#MSigDB#',
    SEQ  => '#Sequence#',
    SMI  => '?#SMILES#',
    SYM  => '#GeneSymbols#',
    SYML => '#GeneSymbols#',
    WP   => '#Wikipedia#',
#    PATH => '#WikiPathways#',
};
my $mtNs2adNs = {};
while (my ($adns, $mtns) = each %{$mtNamespaces}) {
    if ($mtns =~ /\#([^\#]+)\#/) {
        $mtns = uc($1);
        $mtNs2adNs->{$mtns} = $adns;
        $useMTIDformat->{$mtns} = 1 if ($useMTIDformat->{$adns});
    }
}
$mtNs2adNs->{uc('GeneSymbols')} = 'SYM';

my $mtClasses = {
    AAD  => [ 'Affy Array Design' ],
    BAAD => [ 'BrainArray Design' ],
    AC   => [ 'CHEMICAL'],
    AGIL => [ 'AGILENTPROBE' ],
    AL   => [ 'Locus' ],
    AMBS => [ 'Protein' ],
    AP   => [ 'Protein' ],
    APS  => [ 'Affy Probe Set' ],
    BAPS => [ 'BrainArray probe set' ],
    APRB => [ 'Affy Probe Sequence' ],
    BAPB => [ 'Affy Probe Sequence' ],
    AR   => [ 'RNA' ],
    ASEQ => [ 'Biomolecule' ],
    AUR  => [ 'AURIDMOL' ],
    BMSC => [ 'BMSCMPD' ],
    BMSS => [ 'Protein' ],
    BPS  => [ 'BMS Probe Set' ],
    BTFO => [ 'BMSTFPATH' ],
    CAS  => [ 'CAS Number' ],
    CDD  => [ 'CDD' ],
    CLAD => [ 'CodeLink Array' ],
    CLPS => [ 'CodeLink Probe' ],
    ENSG => [ 'Locus' ],
    ENSP => [ 'Ensembl', 'Protein'],
    ENST => [ 'Ensembl', 'RNA'],
    ENSE => [ 'Ensembl', 'Exon'],
    GI   => [ 'GI Number' ],
    GO   => [ 'GO' ],
    HG   => [ 'HomoloGene' ],
    ILMD => [ 'Illumina BeadArray' ],
    ILMN => [ 'Illumina Probe' ],
    IPI  => [ 'IPI' ],
    IPR  => [ 'InterPro' ],
    IUO  => [ 'Ultimate ORF' ],
    LL   => [ 'Locus' ],
    LT   => [ 'ListTracker List' ],
    MANK => [ 'Protein' ],
    MDL  => [ 'MFCD' ],
    MIRB => [ 'MIMAT' ],
    NRDB => [ 'NRDBP' ],
    PHPS => [ 'Pharmagene Probe Set'],
    PMID => [ 'PubMed'],
    PUBC => [ 'PubChem' ],
    RSP  => [ 'RefSeq', 'Protein'],
    RSR  => [ 'RefSeq', 'RNA'],
    SEQ  => [ 'Biomolecule' ],
    SMDL => [ 'SMDLIDX' ],
    SMI  => [ 'SMILES' ],
    SP   => [ 'Swiss-Prot' ],
    SYM  => [ 'Gene Symbol' ],
    SYML => [ 'Specified Symbol' ],
    TR   => [ 'Trembl' ],
    TRC  => [ 'TRC', 'RNAi' ],
    UG   => [ 'UniGene' ],
    UP   => [ 'UniProt' ],
    UPC  => [ 'UniParc' ],
    UCSC => [ 'UCSC Gene' ],
    WP   => [ 'Wikipedia Article' ],
    XONT => [ 'XPRESS Ontology' ],
    PATH => [ 'WikiPathways' ],
};

my $mtAlsoOkClasses = {
    AR   => [ 'cDNA' ],    
};

my $evRank = {
    IDA => 13,
    IGI => 13,
    IMP => 13,
    IPI => 13,
    
    IEP => 12,
    
    EXP => 11,
    
    TAS => 10,

    ISO => 6,
    ISM => 4,
    ISA => 3,
    ISS => 3,

    IGC => 3,
    
    RCA => 2,

    IBA => 2,
    IBD => 2,
    IRD => 2,
    
    NAS => 2,

    IEA => 1,
    P   => 1,
    
    IKR => 0,
    IC  => 0,

    ECO => -1,
    NR  => -1,
    ND  => -1,
    E   => -1,
    P   => -1,
};

my $nsID = {};
my @allNs = keys %{$nsName};
foreach my $id (@allNs) {
    $nsID->{ uc($id) } = uc($id);

    my $name = uc($nsName->{$id});
    $nsName->{ $name } = $nsName->{$id};
    $nsID->{ $name }   = uc($id);
    $name =~ s/ /_/g;
    $nsName->{ $name } = $nsName->{$id};
    $nsID->{ $name }   = uc($id);
}

while (my ($alias, $real) = each %{$nsAlias}) {
    $alias = uc($alias);
    if (my $name = $nsName->{$alias}) {
        warn "Namespace token alias '$alias' is already used for $name\n";
    } else {
        $nsID->{ $alias }   = uc($real);
        $nsName->{ $alias } = $nsName->{uc($real)};   
    }
}

my $taxaLookup = {
    UCSC => {
        'Homo sapiens' => 'Human',
        'Pan troglodytes' => 'Chimp',
        'Macaca mulatta' => 'Rhesus',
        'Felis catus' => 'Cat',
        'Canis familiaris' => 'Dog',
        'Bos taurus' => 'Cow',
        'Mus musculus' => 'Mouse',
        'Didelphis virginiana' => 'Opossum',
        'Rattus norvegicus' => 'Rat',
        'Gallus gallus' => 'Chicken',
        'Xenopus tropicalis' => 'X. tropicalis',
        'Danio rerio' => 'Zebrafish',
        'Tetraodon nigroviridis' => 'Tetraodon',
        'Takifugu rubripes' => 'Fugu',
        'Ciona intestinalis' => 'C. intestinalis',
        'Strongylocentrotus purpuratus' => 'S. purpuratus',
        'Drosophila melanogaster' => 'D. melanogaster',
        'Drosophila simulans' => 'D. simulans',
        'Drosophila sechellia' => 'D. sechellia',
        'Drosophila yakuba' => 'D. yakuba',
        'Drosophila erecta' => 'D. erecta',
        'Drosophila ananassae' => 'D. ananassae',
        'Drosophila persimilis' => 'D. persimilis',
        'Drosophila pseudoobscura' => 'D. pseudoobscura',
        'Drosophila virilis' => 'D. virilis',
        'Drosophila mojavensis' => 'D. mojavensis',
        'Drosophila grimshawi' => 'D. grimshawi',
        'Anopheles mellifera' => 'A. mellifera',
        'Anopheles gambiae' => 'A. gambiae',
        'Caenorhabditis elegans' => 'C. elegans',
        'Caenorhabditis briggsae' => 'C. briggsae',
        'Saccharomyces cerevisiae' => 'Yeast',
        'SARS coronavirus' => 'SARS',
    },
};

my $rvcols = {
    #               0        1      2    3       4          5       6
    convert => [ qw(term_out ns_out auth matched ns_between term_in ns_in) ],
    assign  => [ qw(acc onto ec matched acc_ns onto_ns onto_subset 
                    acc_desc onto_desc parentage) ],
};

=head1 PRIMARY METHODS

These methods are the workhorses of the program, they provide the
primary functionality that makes this module useful.

=head2 convert

 Title   : convert
 Usage   : my $rows = $ad->convert( @args )
 Function: Converts IDs in one namepsace to those in another
 Returns : If called in array context, will return a list of just the
           other IDs. If called in scalar context, will return a 2D
           array reference
 Args    : Associative array of arguments. Recognized keys [Default]:

       -id Required. The query ID (scalar string) or IDs (1D array reference
           of strings) you wish to convert from.

      -ns1 The namespace of the query. Required if you wish the system
           to generate missing data.

      -ns2 The namespace of the identifiers you wish to convert
           to. Required if you wish the system to generate missing
           data. If not provided, then all previously calculated
           namespaces for the query will be returned.

      -min Optional minimum matched value. If provided then only
           results in the databse with that value or greater will be
           returned. A value of zero is ignored.

      -age Optional minimum freshness age, in days. Will over-ride any
           globally defined value set with age().

  -ageonce [False] If true, then will prevent age constraints from
           being enforced when chaining conversions. Note that this
           obviously could allow old data to shape the returned
           content.

 -intersection Optional intersecting ID. If provided, any output IDs
           must also be an output ID for the intersecting ID. This
           feature is useful for constraining broad output to be from
           only a particular set; for example, getting all orthologues
           that are only from mouse (-intersection 'Mus musculus').

    -intns [undef] Only relevant if -intersection is defined. The
           optional namespace of the intersection identifier.

    -chain Optional array reference of namespaces. Will result in
           chained conversion taking place. For example, if you pass
           ['RSR', 'LL', 'SYM'] then your queries will be converted
           first to RefSeq RNA. Those RNAs will be converted to
           LocusLink Genes, which will finally be converted to Gene
           Symbols.

    -limit [undef] If non-zero, then return only the specified number
           of rows from the database. Primarily useful for testing, or
           if you want just a few examples.

 -redonull [false] If a true value, then any empty database rows
           (signifying failure to find a conversion on a past
           calculation) will be treated as no rows returned (forces
           re-calculation of the conversion).

 -nullrows [false] If true, then the search will use an optimized SQL
           to find queries that would have returned null
           information. The return value will be a 1D array ref of
           such query IDs (scalar context call), or that array ref
           plus a 1D array of IDs that were not null.

   -nolist [false] If true, will force the system to perform iterative
           queries. Otherwise, if more than 20 queries are passed, the
           system will search via a join to a temporary table of the
           IDs.

  -dumpsql [false] If defined, the SQL statement being executed to
           perform the query will be pretty printed.

 -progress [undef] If defined, long searches will show some progress
           information. The value is treated as a title (eg 'Searching
           for Affy IDs...').

A typical call might be:

 my @probesets = $ad->convert
    ( -id => 'nm_001234', -ns1 => 'RSR', -ns2 => 'APS', -min => 0.8 );

...which will return an array of affy probeset identifiers. If called
in scalar context (my $probesets), a 2D array ref will be returned
with column order:

 term_out ns_out auth matched ns_between term_in ns_in

=cut

# About 1.8 mSec for data that does not need to be generated
sub convert {
    my $self  = shift;
    unshift @_, '-id' if ($#_ == 0);
    my $args  = $self->parseparams( @_ );
    my @chain;
    my $doChain = 0;
    if (my $chainReq = $args->{CHAIN}) {
        # Namespace chain is defined
        @chain         = ref($chainReq) ? 
            @{$chainReq} : split(/\s*\,\s*/, $chainReq);
        
        $args->{NS1}   = shift @chain;
        $args->{NS2}   = $chain[0];
        $args->{CHAIN} = undef;
        $doChain       = $#chain > 0 ? 1 : 0;
    }
    my $query    = $args->{ID} || $args->{IDS};
    my $ns1      = $self->namespace_token( $args->{NS1} );
    my $ns2      = $self->namespace_token( $args->{NS2} );
    return wantarray ? () : [] unless ($query);
    my $idReq  = ref($query) ? $query : [ $query ];
    my @ids;
    foreach my $id (@{$idReq}) {
        # Remove leading and trailing spaces
        $id =~ s/^\s+//;
        $id =~ s/\s+$//;
        # Remove MapTracker namespace hashes
        $id =~ s/^\#[^\#]*\#\s*//;
        push @ids, $id if ($id);
    }
    my $idRemap;
    if ($ns1 eq 'BAPS') {
        # Sigh
        for my $i (0..$#ids) {
            my $id = $ids[$i];
            my ($stnd) = $self->standardize_id( $id, $ns1 );
            if ($stnd ne $id) {
                $idRemap ||= {};
                $idRemap->{$stnd} = $id;
                $ids[$i] = $stnd;
            }
        }
    }
    my $isStandardCapture;
    my $flavor   = "";
    my $noBind   = 0;
    my $dbh      = $self->dbh;
    my $min      = $args->{MIN} || $args->{MATCHED} || $args->{SCORE};
    my $nsInt    = $self->namespace_name($args->{INTNS});
    my $intID    = $args->{INTERSECTION} || $args->{INTERSECT} || $args->{INT};
    my $redo     = $args->{REDONULL} || 0;
    my $noNull   = $args->{NONULL} ? 1 : 0;
    my $forceNul = $args->{ASSURENULL} && !$noNull ? 1 : 0;
    my $igCase   = $args->{IGNORECASE} ? 1 : 0;
    my $miss     = $args->{NULLROWS};
    my $limit    = $args->{LIMIT} || $args->{ROWNUM};
    my $nullSc   = $args->{NULLSCORE};
    my $nLinks   = $args->{LINKS};
    my $bestOnly = $args->{BEST} || $args->{KEEPBEST} || $args->{BESTONLY};
    my $doPop    = $args->{POPULATE};
    my $toval    = $args->{TERMOUT};
    my $rvHash   = $args->{ASHASH} ? 1 : 0;
    my $rvList   = $doChain || (!$rvHash && wantarray) ? 1 : 0;
    my $updateAll = $args->{FORCEUPDATE};
    my $addD     = $args->{ADDDATE};
    my $asList   = $args->{USELIST} || $miss ||
        ($#ids > 100 && !$args->{NOLIST}) ? 1 : 0;
    my $cReq     = $args->{COLUMNS} || $args->{COLS};
    my $age      = $self->standardize_age($args->{AGE});
    unless ($args->{NOSPECIFICAGE}) {
        my $specAge = $self->specific_age($ns1, $ns2, $args->{AGE});
        if ($specAge && $specAge > $age) {
            # When a specific age is provided and is older, use it
            $age = $specAge;
            $redo = 1;
        }
    }
    return wantarray ? () : [] if ($#ids == -1);
    $self->benchstart;

    # Establish a structure of persistent data that will carry through
    # recursion
    my $isInitiator = 0;
    my $persistent = $self->{CONV_CACHE};
    unless ($persistent) {
        # This is the original method call
        $isInitiator = 1;
        my $wnType = lc($args->{WARN} || 0);
        $wnType = 'build' if ($wnType =~ /build/i);
        $persistent  = $self->{CONV_CACHE} = { 
            WARN => $wnType,
            DSQL => $args->{DUMPSQL},
            ESQL => $args->{EXPLAINSQL},
        };
    }
    my $explSQL = $persistent->{ESQL} || $args->{EXPLAINSQL};
    my $dumpSQL = $explSQL || $persistent->{DSQL} || $args->{DUMPSQL};

    # Remove versioning:
    my $gns = $ns1 || $self->most_likely_namespace( \@ids );
    map { s/\.\d+$// } @ids if ($self->namespace_is_sequence($gns));
    if ($gns eq 'BMSC') {
        # BMS IDs have special case handling to clean up variants. This
        # can cause SQL to fail (eg AL -000111 != AL-000111)
        for my $i (0..$#ids) {
            if ($ids[$i] =~ /^([A-Z][A-Z\s]{0,2})\-(\d{1,6})/) {
                my ($l, $n) = ($1, $2);
                $l =~ s/\s+//;
                $ids[$i] = sprintf("%s-%06d", $l, $n);
            }
        }
    }

    my $cs    = 0;
    my $qNum  = 0;
    my (@binds, @where);
    my @tables = "conversion c";
    my @sName  = (sprintf("%d quer%s", $#ids + 1, $#ids == 0 ? 'y' : 'ies'));
    my $alreadyPopulated = $miss || 0;

    if ($args->{NOSELF}) {
        $self->assure_conversion( \@ids, $ns1, $ns2, $age )
            unless ($alreadyPopulated++);
        my $cs = $ns1 && $self->namespace_is_case_sensitive($ns1) && !$igCase;
        push @where, $cs ? "c.term_out != ? " : 
            "upper(c.term_out) != upper(?)";
        $asList = 0;
        $qNum++;
    }

    if ($asList) {
#      push @where, "upper(c.term_in) IN (SELECT upper(member) FROM temp_list)";
        push @tables, "temp_list tl" unless ($miss);
        push @where, "upper(c.term_in) = upper(tl.member)";
        push @where, "upper(c.term_in) = ANY ((SELECT array(SELECT upper(member) FROM temp_list))::text[])";
    } else {
        push @where, "upper(c.term_in) = upper(?)";
        $qNum++;
    }


    my ($nsIn, $nsOut, $matchingCase);
    if ($ns1) {
        $nsIn = $self->namespace_name($ns1);
        if ($self->namespace_is_case_sensitive($ns1)) {
            # Input namespace is case-sensitive
            if ($igCase) {
                # We need to expand the query to explicitly include
                # all case variants. This is neccesary in order to
                # allow the get-or-create functionality to work
                # properly.
                my $mt    = $self->tracker;
                if ($mt) {
                    my $mtns  = $self->maptracker_namespace( $ns1 );
                    my %distinct;
                    foreach my $id (@ids) {
                        my @seqs = $mt->get_seq( -id => '??'.$mtns.$id,
                                                 -defined  => 1,
                                                 -nocreate => 1 );
                        if ($#seqs == -1) {
                            # No hits - record the original ID
                            $distinct{ $id } = 1;
                        } else {
                            map { $distinct{ $_->name } = 1 } @seqs;
                        }
                    }
                    if ($alreadyPopulated) {
                        # NEED TO ASSURE CONVERSION FOR NEW IDS
                        my %needed = %distinct;
                        map { delete $needed{$_} } @ids;
                        my @need = keys %needed;
                        $self->assure_conversion( \@need, $ns1, $ns2, $age )
                            unless ($#need == -1);
                    }
                    my $prior = $#ids + 1;
                    @ids = keys %distinct;
                    my $expand = $#ids + 1;
                    if ($expand != $prior) {
                        $sName[0] .= " (case-expanded to $expand)";
                    }
                    # die join(" + ", @ids);
                } elsif (!$self->{COMPLAIN}{IGNORECASE}++){
                    warn "Unable to reliably do a case-insensitive search without the MapTracker database. It is possible that some results may be excluded from your search.";
                }
            }
            push @where, $asList ? "c.term_in = tl.member" : "c.term_in = ?";
            $cs = 1;
            $qNum++;
        } elsif ($useMTIDformat->{$ns1}) {
            # The input namespace should be reduced to a MapTracker ID
            my $mt    = $self->tracker;
            for my $i (0..$#ids) {
                my $req = $ids[$i];
                if ($req =~ /^mtid:\d+$/i) {
                    # Already as an ID
                    $ids[$i] = uc($req);
                } elsif ($mt) {
                    my $seq = $mt->get_seq
                        ( -name => $req, -defined => 1, -nocreate => 1 );
                    if ($seq && $useMTIDformat->{uc($seq->namespace->name)}) {
                        $ids[$i] = "MTID:".$seq->id;
                    }
                }
            }
        } elsif ($ns1 eq 'AC') {
            # The input namespace should MAYBE be reduced to a MapTracker ID
            if (my $mt = $self->tracker) {
                for my $i (0..$#ids) {
                    my $req = $ids[$i];
                    if (my $seq = $mt->get_seq
                        ( -name => $req, -defined => 1, -nocreate => 1 )) {
                        if ($useMTIDformat->{uc($seq->namespace->name)}) {
                            $ids[$i] = "MTID:".$seq->id;
                        } else {
                            $ids[$i] = $seq->name;
                        }
                    }
                }
            }
        } elsif ($ns1 eq 'NS') {
            my @standard;
            foreach my $id (@ids) {
                if (my $st = $self->namespace_name( $id )) {
                    push @standard, $id;
                }
            }
            @ids = @standard;
        }
        push @where, 'c.ns_in = ?';
        push @binds, $nsIn;
        push @sName, "input NS";
    }

    if ($ns2) {
        $nsOut = $self->namespace_name($ns2);
        push @where, 'c.ns_out = ?';
        push @binds, $nsOut;
        push @sName, "output NS";
    }

    if (defined $age) {
        if (my $asql = $self->age_filter_sql( $age, 'c.updated')) {
            push @where, $asql;
            push @sName, "age";
            $age = $veryOldAge if ($args->{AGEONCE});
        } else {
            $self->err("Failed to calculate age SQL for '$age' days");
        }
    }
    my $mainCol = 'c.term_out';

    # FILTERS
    # These SQL clauses require that rows be pre-populated in the DB
    # Otherwise, a zero-row result could be due either to a filter,
    # or because the data never existed at all
    my $intText = "";
    if ($intID) {
        # $self->err("Intersecting [$ns1] -> [$ns2] via $intID");
        my @intList = $self->list_from_request( $intID );
        if ($#intList == -1) {
            $self->err("Failed to implement intersection filter off empty list found in '$intID'");
        } else {
            my $bestNs2 = $self->effective_namespace($ns2, $ns1);
            my $bName   = $self->namespace_name($bestNs2);
            my $iName   = $self->namespace_name($nsInt);
            my $gNsInt  = $iName ? $iName : 
                $self->namespace_name($self->pick_namespace($intList[0]));
            $intText   = " intersecting ".join(' OR ', @intList);
            $intText  .= " [".$self->namespace_token($gNsInt)."]" if ($gNsInt);
            $flavor   .= "[INT]".join('+',@intList);
            # Is the request case-sensitive?
            my $isCS   = (!$nsInt) || $igCase || 
                (!$self->namespace_is_case_sensitive($nsInt)) ? 0 : 1;

            # Get all target values without filtering. This step must
            # be done regardless of which way we try to perform the
            # intersection, since we need these rows populated
            $self->bench_start("Primary Intersection");
            my @ns2ids   = $self->convert
                ( -id => \@ids, -ns1 => $ns1, -ns2 => $ns2, -age => $age );
            # die join(",", sort { uc($a) cmp uc($b) } @ns2ids);
            $alreadyPopulated++;
            $self->bench_end("Primary Intersection");
            # warn "[$ns1] -- {$gNsInt} --> [$ns2 / $bestNs2]\n";
            # The biggest challenge in efficiently intersecting the query
            # is figuring out 'which way to go' - that is, should we join
            # from the target IDs (ie @ns2ids) or from the desired
            # intersecting IDs (@intList)
            my ($qTok, $iTok);

            if ($gNsInt) {
                # The user has defined the intersection namespace or we have
                # guessed it. We will make sure that the conversions from
                # the target IDs to this namespace are populated
                $self->bench_start("Secondary Intersection");
                # warn "Converting ".join("+", @ns2ids). " [$bestNs2] to [$gNsInt]\n";
                $self->assure_conversion( \@ns2ids, $bestNs2, $gNsInt, $age );
                ($qTok, $iTok) = ('in', 'out');
                push @where, "int.ns_$iTok = '$gNsInt'";
                $flavor .= "[INTNS]$gNsInt";
                $self->bench_end("Secondary Intersection");
            } else {
                # We are not really sure what the intersection is.

                my @com  = ( -age => 999, -limit => 30, 
                             -xtraname => 'IntOpt' );
                # To do this, we get a quick measure of the number of rows
                # involved in each direction. We want to pivot the way that
                # has the fewest.
                
                $self->bench_start("Sample Directionality");
                my @inpRes = $self->convert
                    ( -id => \@ns2ids, -ns1 => $bestNs2, -ns2 => $nsInt, @com);
                my @intRes = $self->convert
                    ( -id => \@intList,-ns1 => $nsInt, -ns2 => $bestNs2, @com);
                #$self->err(($#ns2ids+1)." entries for $ns2 => $bestNs2",
                #           ($#inpRes+1)." entries for $bestNs2 => $nsInt",
                #           ($#intRes+1)." entries for $nsInt => $bestNs2");
                $self->bench_end("Sample Directionality");

                $self->bench_start("Secondary Intersection");
                if ($#inpRes < $#intRes) {
                    # There is a relatively small number of rows involved if we
                    # try to get from the output namespace to the intersecting one
                    # We are going to pre-calculate $bestNs2 -> $nsInt
                    ($qTok, $iTok) = ('in', 'out');
                    map {
                        $self->assure_conversion( $_, $bestNs2, $nsInt, $age );
                    } @ns2ids;
                } else {
                    # We deal with a smaller number of hits when we start with
                    # $nsInt and back-calculate $bestNs2
                    ($qTok, $iTok) = ('out', 'in');
                    map {
                        $self->assure_conversion( $_, $nsInt, $bestNs2, $age );
                    } @intList;
                }
                $self->bench_end("Secondary Intersection");
            }
            push @tables, "conversion int";

            my $wSql =  "upper(int.term_$iTok) ";
            if ($#intList == 0) {
                $wSql .= "= upper(?)";
            } else {
                $wSql .= "IN (".join(", ", map { 'upper(?)' } @intList).")";
            }
            push @where, $wSql;
            push @binds, @intList;
            push @where, "c.term_out = int.term_$qTok";

            push @where, "int.ns_$qTok = '$bName'" if ($bName);
            push @sName, "intersection";
            $noBind = 1;
        }
    }

    if ($args->{DIRECTONLY}) {
        # We only want direct rows.
        # To apply in SQL, we need to assure that the database is populated
        $self->assure_conversion( \@ids, $ns1, $ns2, $age )
            unless ($alreadyPopulated++);
        push @where, $usePostgres ? 
            "(c.ns_between IS NULL OR c.ns_between = '')" : 
            "c.ns_between IS NULL";
        push @sName, "direct edge";
    }

    if ($toval) {
        # We only want rows linke to a particular value
        $self->assure_conversion( \@ids, $ns1, $ns2, $age )
            unless ($alreadyPopulated++);
        if ($args->{TERMOUTCS}) {
            push @where, "c.term_out = ?";
            push @binds, $toval;            
            $flavor .= "[TOCS]$toval";
        } else {
            push @where, "upper(c.term_out) = ?";
            push @binds, uc($toval);
            $flavor .= "[TO]$toval";
        }
        push @sName, "output term";
        $mainCol = 'c.term_in';
    }

    if ($min) {
        # Only getting rows with a minimum score
        # To apply in SQL, we need to assure that the database is populated
        $self->assure_conversion( \@ids, $ns1, $ns2, $age )
            unless ($alreadyPopulated++);
        $min /= 100 if ($min > 1);
        push @sName, "min score";
        $flavor .= "[MIN]$min";
        if ($args->{KEEPNULL}) {
            my @nullbits = ("c.matched >= ?","c.matched IS NULL");

            # Silly, do not test a number against a string!
            # push @nullbits, "c.matched = ''" if ($usePostgres);

            push @where, "(".join(" OR ", @nullbits).")";
            push @sName, "non-null";
            $flavor .= "[MinNN]";
        } else {
            push @where, "c.matched >= ?";
        }
        push @binds, $min;
    }
    if (my $areq = $args->{AUTH} || $args->{AUTHS}) {
        # Only getting rows for a certain author
        # To apply in SQL, we need to assure that the database is populated
        $self->assure_conversion( \@ids, $ns1, $ns2, $age )
            unless ($alreadyPopulated++);
        my $hash = $self->process_filter_request($areq);
        my @abits;
        foreach my $key (keys %{$hash}) {
            my $wc   = "upper(c.auth) ".
                ($key eq 'NOT IN' ? 'NOT LIKE' : 'LIKE')." ?";
            my $list = $hash->{$key};
            push @binds, map { '%'.uc($_).'%' } @{$list};
            # push @where, "(". join(" OR ", map { $wc } @{$list}).")";
            push @where, map { $wc } @{$list};
            $flavor .= "[AUTH]-$wc-".join('+', @{$list});
        }
        push @sName, "authority";
    }

    my $sql = "SELECT ";
    my @cols;
    if ($miss) {
        $sql .= " tl.member FROM temp_list tl WHERE NOT EXISTS ( SELECT ";
        @cols = ();
        $isStandardCapture = 0;
        $flavor .= "[MISS]";
        $cReq = undef;
        $addD = undef;
    } else {
        my $useDistinct = $args->{NODISTINCT} ? 0 : 1;
        if ($doPop) {
            @cols = ($mainCol);
            $isStandardCapture = 0;
        } elsif ($rvList) {
            # We just want the output terms
            @cols        = ($mainCol);
            $isStandardCapture = 0;
            $cReq        = undef;
            $useDistinct = 1;
            # push @where, "c.term_out IS NOT NULL";
        } else {
            @cols = map { "c.$_" } @{$rvcols->{convert}};
        }
        if ($useDistinct) {
            $sql .= "DISTINCT ";
        }

    }
    my ($oPos, @sQ, @required);
    if ($cReq) {
        $cReq  = [ split(/[^a-z_]+/, $cReq)] unless (ref($cReq));
        my %ok = map { $_ => 1 } @cols;
        $oPos  = { map { $cols[$_] => $_ } (0..$#cols) };
        @cols  = ();
        foreach my $col (@{$cReq}) {
            $col = lc($col);
            if ($col eq 'updated') {
                my $frm = $args->{TIMEFORMAT} || $dbh->date_format;
                push @sQ, "to_char(c.updated , '$frm')";
                $addD = 0;
            } elsif ($ok{"c.$col"}) {
                push @sQ, "c.$col";
            } else {
                $self->death("Attempt to recover unknown column '$col' from conversion");
            }
            push @cols, "c.$col";
        }

        # term_out is needed for so many things, always insist that it is added
        push @required, $mainCol;
    } else {
        @sQ = @cols;
        $isStandardCapture = 1 unless (defined $isStandardCapture);
    }
    my @xtraCols;
    push @xtraCols, $self->extend_array
        ( \@cols, [qw(auth)], "c.") if ($nLinks);
    push @xtraCols, $self->extend_array
        ( \@cols, [qw(term_in ns_in ns_out matched)], "c.") if ($bestOnly);
    unless ($#xtraCols == -1) {
        push @cols, @xtraCols;
        push @sQ,   @xtraCols;
    }

    $sql .= join(", ", @sQ);
    if ($addD) {
        push @cols, 'c.updated';
        my $tf  = $args->{TIMEFORMAT};
        $tf     = $dbh->date_format() unless (defined $tf);
        if ($tf) {
            $sql .= ", to_char(c.updated , '$tf')";
        } else {
            $sql .= ", ".$self->age_recover_sql("c.updated");
        }
    }
    if ($asList) {
        my $tm = 'tl.member';
        $sql .= $#cols == -1 ? $tm : ", $tm";
        push @cols, $tm;
    }

    my %cPos = map { $cols[$_] => $_ } (0..$#cols);
    my $mapCols;
    if ($oPos) {
        $mapCols = [ map { defined $oPos->{$_} ? $oPos->{$_} : 10 } @cols ];
    }
    my $toPos = $cPos{$mainCol} || 0;

    $sql   .= " FROM " .join(', ', @tables)." WHERE ".join(' AND ', @where);
    $sql   .= ")" if ($miss);
    my $snm = $miss ? "Find missing" : "Conversion";
    if (my $xnm = $args->{XTRANAME}) { $snm .= " ($xnm)"; }
    
    my $tempListName;
    if ($asList && !$updateAll) {
        # $self->set_temp_list( \@ids, undef, 'upper(%s)'  );
        # Postgres is happy to prepare the statement handle without the table
        # in existence, but Oracle is not. Do so here.
        $tempListName = $dbh->list_to_temp_table
            ( \@ids,
              [ "varchar(4000)" ],
              [ "member" ],
              "temp_list",
              ['upper(member)'] );
    }
    my $sth = $dbh->prepare
        ( -sql   => $sql,
          -name  => "$snm via ". join($authJoiner, @sName),
          -level => 1,
          -limit => $limit || 0);

    my $traceDat;
    if ($self->{TRACE}) {
        $self->{TRACE_DATA} ||= {
            LEVEL => 0,
            LMOD  => 0,
            DATA  => [],
            NOW   => [],
        };
        my $msg = join(", ", @ids);
        $msg    = substr($msg, 0, 80) . ' ...' if (length($msg) > 80);
        $self->{TRACE_DATA}{LMOD}++ if ($args->{TRACETOK} && $args->{TRACETOK} eq '+');
        $traceDat = {
            level => ++$self->{TRACE_DATA}{LEVEL} - $self->{TRACE_DATA}{LMOD},
            text  => $msg,
            in    => $#ids + 1,
            ns1   => $ns1,
            ns2   => $ns2,
            tok   => $args->{TRACETOK},
        };
        push @{$self->{TRACE_DATA}{DATA}}, $traceDat;
        push @{$self->{TRACE_DATA}{NOW}}, $traceDat;
    }

    if ($persistent->{WARN} && $persistent->{WARN} ne 'build') {
        $self->{BENCHITER} ||= time;
        my $what = $ids[0] || '-UNDEF-';
        $what .= " + ".($#ids)." others" if ($#ids > 0);
        printf(STDERR "%s%30s [%4s] => [%4s]%s", 
               (++$self->{SCROLL_ITER} % $self->{SCROLL_SIZE}) ?
               "\r" : "\n", $what, $ns1, $ns2, $intText );
    }

    # print "<pre>".$sth->pretty_print(  )."</pre>";
    # warn $sth->pretty_print(  );
    my (@results, @needed);
    if ($updateAll) {
        @needed = @ids;
    } elsif ($asList){
        # Operate in list mode
        if ($dumpSQL) {
            print STDERR "\n" if ($persistent->{WARN});
            my $msg = $sth->pretty_print( @binds );
            $msg .= $sth->explain_text( \@binds )."\n" if ($explSQL);
            $self->msg("[SQL]", $msg);
        }
        my $bName = $miss ? "Find missing via list" : "Conversion via list";
        $self->bench_start($bName);
        my $usedSTH = $sth->flex_execute( \@binds, $noBind );
        my $rows   = $usedSTH->fetchall_arrayref;
        $self->bench_end($bName);
        if ($miss) {
            delete $self->{CONV_CACHE} if ($isInitiator);
            # The user just wants to know if this conversion is populated
            @needed = map { $_->[0] } @{$rows};
            if ($idRemap) {
                @needed = map { $idRemap->{$_} || $_ } @needed;
            }
            $self->benchend;
            # warn "Total missing = ".($#needed+1);
            return \@needed unless (wantarray);

            # Need to make @results....
            return (\@needed, \@results);
        }
        @results   = @{$rows};
        if (!$limit || $#results + 1 < $limit) {
            $self->bench_start('Determine Missing List Members');
            # If there is no limit, or we recovered fewer rows than
            # the limit, we can see if any query terms were missed:
            # If REDONULL is specified, null rows are marked as zero,
            # otherwise they are counted as 1.
            my $markNull = $redo ? 0 : 1;
            # Identify all query terms that returned results
            my %found;
            my $tiPos    = $cPos{'tl.member'};
            map { $found{$_->[$tiPos]} = $_->[$toPos] || $markNull } @results;
            # Identify all query terms NOT returning results
            foreach my $id (@ids) {
                next if (!$id || $found{$id});
                # Maybe we missed it because it was not a standard format?
                push @needed, $id;
            }
            $self->bench_end('Determine Missing List Members');
        }
    } else {
        # Iterate over the requests
        if ($dumpSQL) {
            print STDERR "\n" if ($persistent->{WARN});
            my @idBind = map { $ids[0] } (1..$qNum);
            my $msg = $sth->pretty_print( @idBind, @binds );
            $msg .= $sth->explain_text( [@idBind, @binds] )."\n" if ($explSQL);
            $self->msg("[SQL]", $msg);
        }
        # warn "$query [$ns1] = ".$sth->pretty_print( (map { $ids[0] } (1..$qNum)), @binds );

        my $progKey;
        my $pVerbose;
        if (my $title = $args->{PROGRESS}) {
            # The user wants progress warnings
            if ($title eq '1') {
                $title = "Converting IDs";
                $title .= " from $nsIn" if ($nsIn);
                $title .= " to $nsOut"  if ($nsOut);
            }
            $pVerbose = $dbh->verbose;
            $dbh->verbose(1);
            $progKey = $dbh->initiate_progress( $#ids + 1, $title);
        }

        $self->bench_start('Conversion via statement handle');
        foreach my $id (@ids) {
            next unless ($id);
            my $ckey = join('-', $id, $ns2, $ns1, $flavor);
            # warn "$ckey [$redo]\n";
            my $rows = $persistent->{$ckey};
            # if ($alreadyPopulated || !$rows) {
            unless ($rows) {
                my @idBind = map { $id } (1..$qNum);
                my $usedSTH = $sth->flex_execute( [@idBind, @binds], $noBind );
                $rows = $usedSTH->fetchall_arrayref;
            }
            if ( $#{$rows} == -1  || 
                 ($redo && $#{$rows} == 0 && !$rows->[0][$toPos])) {
                # Nothing is in the database
                # OR only a single blank row present, and request to redo
                push @needed, $id;
            } else {
                push @results, @{$rows};
                $persistent->{$ckey} ||= $rows if ($isStandardCapture);
            }
            $dbh->track_progress($progKey) if ($progKey);
        }
        if ($progKey) {
            $dbh->finish_progress($progKey);
            $dbh->verbose($pVerbose);
        }
        $self->bench_end('Conversion via statement handle');
    }

    if ($#needed == -1 || $alreadyPopulated) {
        # All rows were already added to the database
        
    } else {
        # There are some queries for which we found no rows
        # Code reference used to populate DB if the query fails:
        my $updater = ($ns1 && $ns2) ?
            $self->get_converter($ns1, $ns2) : undef;
        my $alreadyFound;
        if ($rvList) {
            $alreadyFound = { map { $_->[$toPos] || '' => 1 } @results };
        }
        if ($updater) {
            # We have the capacity to calculate new rows
            my %nonRedun = map { $_ => 1 } @needed;
            my @needIds  = sort keys %nonRedun;
            if ($persistent->{WARN} && $persistent->{WARN} eq 'build') {
                $self->{BENCHITER} ||= time;
                my $what = $needIds[0] || '-UNDEF-';
                $what .= " + ".($#needIds)." others" if ($#needIds > 0);
                printf(STDERR "%s%30s [%4s] => [%4s]", 
                       (++$self->{SCROLL_ITER} % $self->{SCROLL_SIZE}) ?
                       "\r" : "\n", $what, $ns1, $ns2 );
            }
            foreach my $id (@needIds) {
                # warn "$id [$ns1] -> [$ns1]" if ($id =~ /\-1/);
                my $ckey = join('-', $id, $ns2, $ns1);
                my $rows;
                push @{$persistent->{IDSTACK}}, "$id [$ns1 > $ns2]";
                if (my $priorAge = $persistent->{iLoopCatch}{$ckey}) {
                    my $compAge = $age || $veryOldAge;
                    if ($priorAge <= $compAge) {
                        # These data should already have been calculated!
                        # We are at risk of i-Looping
                        my @stck = @{$persistent->{IDSTACK} || ["No stack??"]};
                        my $bar  = '-' x 40;
                        $self->err("RECURSION on convert()", $bar, @stck,
                                   "Failed to find $id [$ns1] -> [$ns2]", $bar)
                            if ($persistent->{WARN});
                        next;
                    }
                }
                $persistent->{iLoopCatch}{$ckey} = $age || $veryOldAge;
                # warn "$id [$ns1] = ".$sth->pretty_print( (map { $id } (1..$qNum)), @binds );
                #$self->msg("[DEBUG]", "$id [$ns1] -> $ns2")
                #    if ($persistent->{WARN});
                my $novel = &{$updater}( $self, $id, $ns2, $ns1, $age);
                if ($alreadyFound) {
                    # We are only capturing unique term_out values
                    foreach my $row (@{$novel}) {
                        if (my $val = $row->{term_out}) {
                            unless ($alreadyFound->{$val}++) {
                                push @results, [ $val ];
                            }
                        }
                    }
                    next;
                }

                # WHY DO WE NEED format_update_rv() ??
                $rows = $self->format_update_rv( $novel, $ns2, $ns1 );
                if ($args->{ADDDATE}) {
                    my $now = $self->dbh->oracle_now();
                    map { push @{$_}, $now } @{$rows};
                }
                
                if ($alreadyFound) {
                    # We are only capturing unique term_out values
                    foreach my $row (@{$rows}) {
                        if (my $val = $row->[$toPos]) {
                            unless ($alreadyFound->{$val}++) {
                                push @results, [ $val ];
                            }
                        }
                    }
                    next;
                }
                $rows = [[undef, $nsOut,undef,undef,undef,$id,$nsIn]]
                    if ($#{$rows} == -1);
                if ($mapCols) {
                    my @remapped;
                    foreach my $row (@{$rows}) {
                        push @remapped, [ map { $row->[$_] } @{$mapCols} ];
                    }
                    $rows = \@remapped;
                }
                push @results, @{$rows};
                last if ($limit && $#{$rows} + 1 >= $limit);
            }
        }
    }
    if ($persistent->{WARN} =~ /bench/i) {
        if (time - $self->{BENCHITER} > 60) {
            warn $self->showbench();
            $self->{BENCHITER} = time;
        }
    }
    if ($isInitiator) {
        # warn "All Conversions:";foreach my $ckey (sort keys %{$self->{CONV_CACHE}}) { warn "  $ckey = ".($#{$self->{CONV_CACHE}{$ckey}} + 1)."\n";}
        delete $self->{CONV_CACHE};
    }

    if ($nLinks) {
        # Filter by number of authorities
        my @keeping;
        my $cp = $cPos{'c.auth'};
        foreach my $row (@results) {
            if (my $auth = $row->[$cp]) {
                my @auths = split(' < ', lc($auth));
                # Discard if we want links of limited length
                next if ($#auths >= $nLinks);
                push @keeping, $row;
            }
        }
        @results = @keeping;
    }

    if ($bestOnly) {
        # For each term_in,ns_in,ns_out, only keep the best row(s)
        my @keyCols = qw(term_in ns_in ns_out);
        my @inds = map { $cPos{"c.$_"} } @keyCols;
        my $mind = $cPos{'c.matched'};
        @results = $self->best_only( \@results, $bestOnly, $mind, \@inds, $nullSc );
    }

    if ($traceDat) {

        my %oh = map { $_->[$toPos] || '' => 1 } @results;
        delete $oh{''};
        my @oids = sort keys %oh;
        $traceDat->{out} = $#oids + 1;
        pop @{$self->{TRACE_DATA}{NOW}};
        $self->{TRACE_DATA}{LMOD}-- if ($args->{TRACETOK} && $args->{TRACETOK} eq '+');
        unless (--$self->{TRACE_DATA}{LEVEL}) {
            my $report = "";
            foreach my $t (@{$self->{TRACE_DATA}{DATA}}) {
                my $ch = join($authJoiner, map { join(' --> ', @{$_}) }
                              @{$t->{chain} || []});
                my $lvl = $t->{level} - 1;
                $report .= sprintf
                    ("%s%s%s (%s) -> %s (%d)%s\n","  " x $lvl, $t->{tok} || '',
                     $t->{ns1}, $t->{text}, $t->{ns2}, $t->{out},
                     $ch ? " <via> $ch" : '');
            }
            warn $report."\n";
            $self->{TRACE_DATA} = undef;
        }
    }
    if (my $lfh = $self->{LOGFH}) {
        my $what = $#ids == 0 ? $ids[0] : ($#ids + 1) . ' entries';
        my $count = $#results + 1;
        $count = 0 if ($count == 1 && !$results[0][$toPos]);
        print $lfh join("\t", "Convert", $what, $ns1, $ns2, $count, `date`);
    }
    $dbh->clear_temp_table( $tempListName ) if ($tempListName);

    if ($idRemap && defined $cPos{"c.term_in"}) {
        # warn $self->branch(\@results);
        my $t1 = $cPos{"c.term_in"};
        map { $_->[$t1] = $idRemap->{$_->[$t1]} || $_->[$t1]
                  if($_->[$t1]) } @results;
    }
       
    
    if ($doPop) {
        $self->benchend;
        return \@results;
    }

    if ($forceNul) {
        # The user wants to be sure that null querys are included in
        # the results
        my ($tmi, $nsi, $nso) = ($cPos{'c.term_in'},
                                 $cPos{'c.ns_in'}, $cPos{'c.ns_out'});
        if (defined $tmi) {
            my %byNS;
            if ($igCase || !defined $nsi) {
                $byNS{''} = [ map { $_->[$tmi] } @results ];
            } else {
                map { push @{$byNS{ $_->[$nsi] }}, $_->[$tmi] } @results;
            }
            my %needed;
            if ($idRemap) {
                %needed = map { uc($_) => $_ } map 
                { $idRemap->{$_} || $_ } @ids;
            } else {
                %needed = map { uc($_) => $_ } @ids;
            }
            while (my ($ns, $ids) = each %byNS) {
                if (!$igCase && $self->namespace_is_case_sensitive($ns)) {
                    map { delete $needed{ uc($_) } if
                              (exists $needed{ uc($_) } &&
                               $needed{ uc($_) } eq $_) } @{$ids};
                } else {
                    map { delete $needed{ uc($_) } } @{$ids};
                }
            }
            my @nullRow = map { undef } @cols;
            $nullRow[$nsi] = $nsIn  if ($nsIn  && defined $nsi);
            $nullRow[$nso] = $nsOut if ($nsOut && defined $nso);
            foreach my $id (sort values %needed) {
                my $nr = [ @nullRow ];
                $nr->[$tmi] = $id;
                push @results, $nr;
            }
        }
    }

    if (defined $nullSc) {
        my $mc = $cPos{'c.matched'};
        if (defined $mc) {
            map { $_->[$mc] = $nullSc unless (defined $_->[$mc]) } @results;
        }
    }

    if ($rvHash) {
        $self->bench_start('Map results to hash');
        my @hashRefs;
        my @niceCols;
        foreach my $cn (@cols) {
            if ($cn =~ /^[^\.]+\.(.+)$/) { $cn = $1 }
            push @niceCols, $cn;
        }
        foreach my $row (@results) {
            my %hash = map { $niceCols[$_] => $row->[$_] } (0..$#niceCols);
            next if ($noNull && !$hash{term_out});
            push @hashRefs, \%hash;
        }
        $self->bench_end('Map results to hash');
        $self->benchend;
        return wantarray ? @hashRefs : \@hashRefs;
    }  elsif ($rvList) {
        $self->bench_start('Extract values as list');
        my @rv;
        @results = map { $_->[$toPos] || '' } @results;
        @results = sort @results unless ($doChain);
        foreach my $out (@results) {
            push @rv, $out if ($out);
        }
        $self->bench_end('Extract values as list');
        $self->benchend;
        # If there are at least two namespaces left in a chain, then recurse:
        return $self->convert( %{$args},
                               -xtraname => 'ChainRecurse',
                               -id    => \@rv,
                               -chain => \@chain ) if ($doChain);
        return @rv;
    } elsif ($noNull) {
        $self->bench_start('Exclude null rows');
        my @nonNull;
        map { push @nonNull, $_ if ($_->[$toPos]) } @results;
        $self->bench_end('Exclude null rows');
        $self->benchend;
        return \@nonNull;
    } else {
        $self->benchend;
        return \@results;
    }
}

sub extend_array {
    # Just extends an array with additional values not already present
    # Used for adding extra columns to an existing list
    my $self = shift;
    my ($nowCols, $addCols, $prfx) = @_;
    my %have = map { $_ => 1 } @{$nowCols};
    $prfx  ||= "";
    my @rv;
    map { push @rv, $_ unless ($have{$_}) } map { $prfx.$_ } @{$addCols};
    return wantarray ? @rv : \@rv;
}

sub best_only {
    my $self = shift;
    my ( $rows, $bestOnly, $scInd, $keyInds, $nullSc ) = @_;
    $bestOnly ||= 1;
    $bestOnly = 1 unless ($bestOnly =~ /^\d+$/);
    $bestOnly--;
    my $nullSortVal = -1;
    my %struct;
    foreach my $row (@{$rows}) {
        # warn "Noted: ".$row->[ $cPos{'c.term_out'} ];
        $row->[$scInd] = $nullSortVal unless (defined $row->[$scInd] && $row->[$scInd] ne '');
        my $key = join("\t", map { defined $row->[$_] ? $row->[$_] : "" } @{$keyInds});
        push @{$struct{$key}}, $row;
    }
    my @rv;
    foreach my $arr (values %struct) {
        if ($#{$arr} == 0) {
            # Single entry, keep
            # warn "Singleton: ".$arr->[0][ $cPos{'c.term_out'} ];
            push @rv, $arr->[0];
            next;
        }
        my @sorted = sort { $b->[$scInd] <=> $a->[$scInd] } @{$arr};
        my $sind   = $#sorted < $bestOnly ? $#sorted : $bestOnly;
        my $best   = $sorted[$sind][$scInd];
        # warn "Tracking: [$best] ".$sorted[0][ $cPos{'c.term_out'} ];
        foreach my $row (@sorted) {
            last if ($row->[$scInd] < $best);
            push @rv, $row;
        }
    }
    if (!defined $nullSc || $nullSc eq '' || ($nullSc ne $nullSortVal)) {
        # We need to map back null scores from something other than -1
        map { $_->[$scInd] = $nullSc if ($_->[$scInd] eq $nullSortVal) } @rv;
    }
    return wantarray ? @rv : \@rv;
}

=head2 assignments

 Title   : assignments
 Usage   : my $rows = $ad->assignments( @args )
 Function: Gets ontology assignments for an accession
 Returns : If called in array context, will return a list of just the
           other IDs. If called in scalar context, will return a 2D
           array reference
 Args    : Associative array of arguments. Recognized keys [Default]:

      -acc Required. The identifier for the accession (the
           non-ontology component of the assignment).

      -ans The namespace of the accession. Required if you wish the
           system to generate missing data.

      -ons The namespace of the identifiers you wish to convert
           to. Required if you wish the system to generate missing
           data. If not provided, then all previously calculated
           namespaces for the query will be returned.

      -min Optional minimum matched value. If provided then only
           results in the databse with that value or greater will be
           returned. A value of zero is ignored. Alias -matched

      -age Optional minimum freshness age, in days. Will over-ride any
           globally defined value set with age().

       -ec Optional evidence code filter. Pass one or more evidence
           codes as either a space separated string or an array
           reference, and only those rows that match will be included
           in the results. To exclude a code, put an exclamation point
           in front of the code, eg "!IEA". Alias -ecs

 -parentage Optional parentage filter (an integer >= 0). If provided,
           then only rows matching will be included in results.

   -nonull Default undef. If true, then rows with onto = null will be
           excluded from the results.

    -limit [undef] If non-zero, then return only the specified number
           of rows from the database. Primarily useful for testing, or
           if you want just a few examples.

  -discard If true, then do not bother collecting results. This
           parameter is really only useful when performing database
           loads where you are not interested in the return value.

 -nullrows [false] If true, then the search will use an optimized SQL
           to find queries that would have returned null
           information. The return value will be a 1D array ref of
           such query IDs (scalar context call), or that array ref
           plus a 1D array of IDs that were not null.

=cut

sub assignments {
    my $self  = shift;
    $self->benchstart;
    unshift @_, '-acc' if ($#_ == 0);
    my $args    = $self->parseparams( @_ );
    my $acc     = $args->{ACC} || $args->{ID};
    my $age     = $self->standardize_age( $args->{AGE} );
    my $ans     = $self->namespace_token( $args->{ANS} || $args->{NS1} );
    my $ons     = $self->namespace_token( $args->{ONS} || $args->{NS2} );
    my $ereq    = $args->{EC} || $args->{ECS};
    my $preq    = $args->{PARENTAGE};
    my $noNull  = $args->{NONULL} ? 1 : 0;
    my $discard = $args->{DISCARD};
    my $min     = $args->{MIN} || $args->{MATCHED};
    my $title   = $args->{PROGRESS};
    my $miss    = $args->{NULLROWS};
    my $cs      = 0;

    my @cols    = @{$rvcols->{assign}};
    my @reqs    = ref($acc) ? @{$acc} : ($acc);
    my $dbh     = $self->dbh;
    my $sql     = 
        "SELECT ".join(", ",@cols). ", ".$self->age_recover_sql("a.updated").
        " FROM assign_onto a WHERE upper(a.acc) = upper(?)";

    my @binds;
    # $filt tracks parts of the query that are filtering (not simple existance)
    my $filt  = 0;
    my ($ann, $onn);
    if ($ans) {
        if ($self->namespace_is_case_sensitive($ans)) {
            # Input namespace is case-sensitive
            $sql .= " AND a.acc = ?";
            $cs = 1;
        }
        $sql .= " AND a.acc_ns = ?";
        $ann  = $self->namespace_name($ans);
        push @binds, $ann;
    }
    if ($ons) {
        $sql .= " AND a.onto_ns = ?";
        $onn  = $self->namespace_name($ons);
        push @binds, $onn;
    }
    if ($min) {
        $min /= 100 if ($min > 1);
        $sql .= " AND a.matched >= ?";
        push @binds, $min;
        $filt++;
    }
    my @ecs;
    if ($ereq) {
        @ecs = ref($ereq) ? @{$ereq} : split(/[^A-Z]/, uc($ereq));
        $sql .= sprintf(" AND a.ec IN (%s)", join(",", map { '?' } @ecs));
        push @binds, @ecs;
    }
    if ($age) {
        if (my $asql = $self->age_filter_sql( $age, 'a.updated')) {
            $sql .= " AND $asql";
        } else {
            $self->err("Failed to calculate age SQL for '$age' days");
        }
    }
    if (defined $preq) {
        $sql .= " AND a.parentage <= ?";
        push @binds, $preq;
    }
    my $lim = $args->{LIMIT};
    $lim = 1 if ($miss);

    if ($args->{WARN}) {
        my $what = $reqs[0] || '-UNDEF-';
        $what .= " + ".($#reqs)." others" if ($#reqs > 0);
        printf(STDERR "%s%30s [%4s] -Assign-> [%4s]", 
               (++$self->{SCROLL_ITER} % $self->{SCROLL_SIZE}) ?
               "\r" : "\n", $what, $ans, $ons );
    }
    
    my $sth = $dbh->prepare
        ( -sql   => $sql,
          -name  => "Find ontology assignments",
          -level => 2,
          -limit => $lim);

    my ($check, $codeRef);
    if ($ann && $onn) {
        # Both namespaces are defined
        $codeRef = $self->get_assigner($ons, $ans);
        if ($codeRef) {
            my $csql = 
                "SELECT count(*) FROM assign_onto WHERE upper(acc) = upper(?)";
            $csql .= " AND acc = ?" if ($cs);
            $csql .= " AND acc_ns = ? and onto_ns = ?";
            if ($age) {
                if (my $asql = $self->age_filter_sql( $age, 'updated')) {
                    $csql .= " AND $asql";
                    $age  = undef if ($args->{AGEONCE});
                } else {
                    $self->err("Failed to calculate age SQL for '$age' days");
                }
            }
            $check = $dbh->prepare
                ( -name  => "Check assignment population via term",
                  -level => 3,
                  -sql   => $csql,
                  -limit => 1,);
        }
    }

    my @rv;
    my $progKey;
    if ($title) {
        # The user wants progress warnings
        if ($title eq '1') {
            $title = "Finding ontology assignments";
            $title .= " from ". $self->namespace_name( $ans ) if ($ans);
            $title .= " to ". $self->namespace_name( $ons ) if ($ons);
            $title .= sprintf(" no more than %.1f days old", $age)
                if (defined $age);
            $title .= " with parentage <= $preq" if (defined $preq);
        }
        $progKey = $dbh->initiate_progress( $#reqs + 1, $title);
    }

    $age = $veryOldAge if ($args->{AGEONCE});
    foreach my $req (@reqs) {
        my @idBind = $cs ? ($req, $req) : ($req);
        warn $sth->pretty_print( @idBind, @binds ) if ($args->{DUMPSQL});
        $sth->execute( @idBind, @binds );
        my $rows = $sth->fetchall_arrayref;
        if ($miss) {
            # The user is just interested in finding holes in the database
            push @rv, [ undef, $req ] if ($#{$rows} == -1);
        } else {
            if ($#{$rows} < 0) {
                # No hits recovered
                if ($check && 
                    (!$filt || !$check->get_single_value(@idBind,$ann, $onn))){
                    # 1. We have all parameters needed to generate new entries
                    # 2. We have not filtered OR
                    # 3. We have filtered, but checking shows no rows anyway
                    # We need to populate the DB
                    $rows =  &{$codeRef}( $self, $req, $ons, $ans, $age );
                    # The above call will generate all possible rows,
                    # which we may need to filter:

                    if (defined $preq) {
                        my @keep;
                        map { push @keep, $_ if (defined $_->[9] && 
                                                 $_->[9] <= $preq) } @{$rows};
                        $rows = \@keep;
                    }
                    if ($min) {
                        my @keep;
                        map { push @keep, $_ 
                                  if (defined $_->[3] && 
                                      $_->[3] <= $min) } @{$rows};
                        $rows = \@keep;
                    }
                    if ($#ecs > -1) {
                        my %echash = map { $_ => 1 } @ecs;
                        my @keep;
                        map { push @keep, $_ 
                                  if ($echash{$_->[2]||''} ) } @{$rows};
                        $rows = \@keep;
                    }
                    # The age of all these entries is zero:
                    map { push @{$_}, 0 } @{$rows};
                }
            }
            if ($discard) {
                # The user is not interested in the return value
                # Presumably the function is being run just to populate the DB
                # Do nothing
            } elsif ($noNull) {
                # Only keep rows that have an ontology assignment
                foreach my $row (@{$rows}) {
                    push @rv, $row if ($row->[1]);
                }
            } else {
                # Keep all rows, including explicit nulls
                push @rv, @{$rows};
            }
        }
        $dbh->track_progress($progKey) if ($progKey);
    }
    $dbh->finish_progress($progKey) if ($progKey);

    if (wantarray) {
        my %nonredun = map { defined $_->[1] ? $_->[1] : '' => 1 } @rv;
        delete $nonredun{''};
        $self->benchend;
        return sort keys %nonredun;
    } else {
        $self->benchend;
        return \@rv;
    }
}

# Will NOT auto-populate assignment
# WILL populate set conversion

sub assignments_for_set {
    my $self  = shift;
    $self->benchstart;
    unshift @_, '-set' if ($#_ == 0);
    my $args  = $self->parseparams( @_ );
    my $set   = $args->{SET};
    my $min   = $args->{MIN};
    my $sns   = $self->namespace_token( $args->{NS1} || 'SET' );
    my $ans   = $self->namespace_token( $args->{ANS} || $args->{NS2} );
    my $ss    = $args->{SUBSET} || '';
    my $ereq  = $args->{EC} || $args->{ECS};
    my $oterm = $args->{ONTO} || $args->{TERM} || $args->{TERMS};
    my $aonly = $args->{ACCONLY};
    my $cust  = ref($set) ? 1 : 0;
    my $tick  = $args->{TICKET};
    my $age   = $self->standardize_age( $args->{AGE} );
    my $vb    = $args->{VERBOSE};

    if ($args->{STH}) {
        $self->death("API CHANGED",
                     "CHARLES NEEDS TO IMPLEMENT -STH argument!!");
        # return( $sth, \@setMem, $set );
    }
    $min /= 100 if ($min && $min > 1);
    # Get all members of the set
    my @setMem;
    my $refname = $set;
    if ($cust) {
        # The user is passing an explicit set
        @setMem  = @{$set};
        $refname = $args->{SETNAME} || "CustomSet";
    } elsif (! $aonly) {
        # The user is passing a set identifier
        $tick->status('GetSetMembers', "Retrieve all ".
                      $self->namespace_name($ans)." members for $set")
            if ($tick);
        $self->msg("Fetching members for set","$set [$sns] -> [$ans]") if ($vb);
        @setMem = $self->convert
            ( -id    => $set, 
              -ns1   => $sns,
              -ns2   => $ans,
              -min   => $min,
              -age   => $args->{NOSETAGE} ? $veryOldAge : $age,
              -limit => $args->{LIMIT} );
        $self->msg("  Recovered ".scalar(@setMem)." members") if ($vb);
        if ($#setMem < 0) {
            # We failed to find any set members
            my $origNS = $self->namespace_token( $args->{ORIGINALNS} );
            if ($ans && $origNS ne $ans) {
                # The set natively points to a different namespace
                # Map the set over by chaining
                my $check = $self->get_converter($origNS, $ans);
                if ($check) {
                    $tick->status('TransformNamespace', "$origNS to $ans")
                        if ($tick);
                    my @rows = $self->chain_conversions
                        ( -id       => $set, 
                          -chain    => ['SET', $origNS, $ans ], 
                          -age      => $age,
                          # -guessout => 1,
                          );
                    $self->dbh->update_rows( 'conversion', \@rows );
                    my $raw = $self->format_update_rv( \@rows, $ans );
                    my %filtered;
                    foreach my $row (@{$raw}) {
                        next if ($min && (!$row->[3] || $row->[3] < $min));
                        next unless ($row->[0]);
                        $filtered{$row->[0]}++;
                    }
                    @setMem = sort keys %filtered;
                } else {
                    my ($oN, $aN) = ($self->namespace_name($origNS),
                                     $self->namespace_name($ans) );
                    warn "No logic found to convert $oN -> $aN ".
                        "(needed to find $aN within set $set)\n";
                }
            }
        }
    }

    my (%ontoNS, %notOntoNS);
    if (my $ons = $args->{ONS} ) {
        my $hash = $self->process_filter_request($ons);
        while (my ($tag, $list) = each %{$hash}) {
            foreach my $nsr (@{$list}) {
                my $sstag;
                if ($nsr =~ /(.+)\s+\:\s+(.+)/) {
                    ($nsr, $sstag) = ($1, $2);
                }
                my $ns = $self->namespace_name($nsr);
                if ($ns) {
                    if ($sstag) {
                        push @{$ontoNS{$ns}{$tag}}, $sstag;
                    } elsif ($tag eq 'IN') {
                        $ontoNS{$ns} ||= {};
                    } else {
                        $notOntoNS{$ns} = 1;
                    }
                }
            }
        }
    }
    my @aocheck = keys %ontoNS;
    my $anyOntNsn = $self->namespace_name('ONT');
    if ($#aocheck == -1 || exists $ontoNS{$anyOntNsn}) {
        my $aoParam = $ontoNS{$anyOntNsn} || {};
        map { $ontoNS{$_} ||= $aoParam } map { $self->namespace_name($_) }
        $self->namespace_children($anyOntNsn) ;
    }
    delete $ontoNS{$anyOntNsn};

    map { delete $ontoNS{$_} } keys %notOntoNS;

    # $self->err("TRUNCATING SET MEMEBER LIST"); @setMem = splice(@setMem,0,20);

    my @getArgs = ( -age     => $age,
                    -ids     => \@setMem,
                    -ns1     => $ans,
                    -min     => $min,
                    -nonull  => 1,
                    -adddate => 1,
                    -timeformat => 0,
                    );

    $tick->status('GetSetAssignments', "Query database for assignmets to ".
                  ($#setMem+1)." $refname members") if ($tick);

    my $qt   = time;
    my $rows = [];
    foreach my $ons (sort keys %ontoNS) {
        $self->msg("Fetching $ons ontology assignments") if ($vb);
        my $data = $self->convert( @getArgs,
                                   -ns2 => $ons, ) || [];
        $self->msg("  [>]",sprintf("Recovered %.3fk associations",
                                   ($#{$data}+1)/1000)) if ($vb);
        push @{$rows}, @{$data};
    }
    $qt = time - $qt;

    # Organize data by output namespace
    my %byNS;
    map { $byNS{ $_->[1] }{ $_->[0] }++ } @{$rows};

    # Find subsets
    my %subsets;
    while (my ($ns, $ids) = each %byNS) {
        my $ntok = $self->namespace_token($ns);
        my @list = keys %{$ids};
        $self->msg("Getting subsets for ".($#list+1)." IDs [$ns]") if ($vb);
        unless ($hasSubsetCategory->{$ntok}) {
            $subsets{$ns} = { map { $_ => '' } @list };
            next;
        }
        # Bulk convert:
        my $sdat = $self->convert
            ( -id => \@list, -ns1 => $ns, -ns2 => 'SET', 
              -age => $age, -cols => ['term_in', 'term_out'] );
        # Organize as nested hash:
        my $targ = $subsets{$ns} = {};
        foreach my $sd (@{$sdat}) {
            $targ->{$sd->[0]}{$sd->[1]} = 1;
        }
        foreach my $term (@list){
            # Verify that terms have a unique subset
            my @uniq = keys %{$targ->{$term} ||{}};
            $targ->{$term} = $#uniq == 0 ? $uniq[0] : '';
        }
    }

    # Do we need to filter results by subset?
    my %subFilters;
    # Global filters (-subset) are applied regardless of namespace:
    my $globalFilter = $ss ? $self->process_filter_request($ss) : {};
    while (my ($ons, $filtH) = each %ontoNS) {
        my %filts = %{$filtH};
        map { push @{$filts{$_}}, $globalFilter->{$_} } keys %{$globalFilter};
        my $filterCode = "";
        while (my ($tag, $list) = each %filts) {
            next if ($#{$list} == -1);
            $filterCode .= 'return 0 if ($val '.($tag eq 'IN' ? '!' : '=').
                '~ /^('.join('|', @{$list}).')$/);'."\n";
        }
        next unless ($filterCode);
        $subFilters{$ons} = eval
            ("sub {\nmy \$val = shift;\n$filterCode\nreturn 1;\n}");
        $self->msg("Subset filter for [$ons]", $filterCode) if ($vb);
    }
    my @ssFiltNS = keys %subFilters;
    unless ($#ssFiltNS == -1) {
        # We need to filter the data
        my @keep;
        foreach my $row (@{$rows}) {
            my $ns = $row->[1];
            my $cb = $subFilters{$ns};
            unless ($cb) {
                # No filter for this namespace, keep all rows
                push @keep, $row;
                next;
            }
            if (my $term = $row->[0]) {
                push @keep, $row if (&{$cb}( $subsets{$ns}{$term} ));
            }
        }
        $rows = \@keep;
    }
    
    if ($ereq) {
        # Evidence code filter requested
        my $hash = $self->process_filter_request($ereq, "[^A-Z\\!]");
        my $filterCode = "";
        my @toks;
        while (my ($tag, $list) = each %{$hash}) {
            $filterCode .= 'return 0 if ($val '.($tag eq 'IN' ? '!' : '=').
                '~ /^('.join('|', @{$list}).') /);'."\n";
            my $pre = $tag eq 'NOT IN' ? '!' : '';
            push @toks, map { uc("$pre$_") } @{$list};
        }
        $args->{ECS} = $ereq = join(' ', @toks) || '';
        delete $args->{EC};
        if ($filterCode) {
            my $cb = eval
                ("sub {\nmy \$val = shift;\n$filterCode\nreturn 1;\n}");
            $self->msg("Evidence filter being applied", $filterCode) if ($vb);
            my @keep;
            foreach my $row (@{$rows}) {
                push @keep, $row if (&{$cb}( $row->[2] ));
            }
            $rows = \@keep;
        }
    }

    if ($oterm) {
        # The user wants only data for specific ontology terms
        my %okTerm = map { uc($_) => 1 } ( ref($oterm) ? @{$oterm} : $oterm);
        my @keep;
        foreach my $row (@{$rows}) {
            if (my $term = $row->[0]) {
                push @keep, $row if ($okTerm{uc($term)});
            }
        }
        $self->msg("Term filter applied, filtered ".($#{$rows}+1)." to ".
                   ($#keep + 1)." rows") if ($vb);
        $rows = \@keep;
    }

    if ($aonly) {
        my @list = map { $_->[5] } @{$rows};
        return \@list;
    }

    # Generate a set collection
    $self->benchstart("Read Set data from database");
    $self->msg("Generating SetCollection object") if ($vb);
    my @coms = ("Collection from BMS::MapTracker::AccessDenorm::assignments_for_set() with arguments:");

    foreach my $key (sort keys %{$args}) {
        my $val = $args->{$key};
        next unless (defined $val);
        if (my $r = ref($val)) {
            if ($r eq 'ARRAY') {
                $val = join(',', @{$val});
            } elsif ($r =~ /Ticket/) {
                $val = $val->ticket;
            } else {
                next;
            }
        }
        push @coms, sprintf('  -%s => %s', lc($key), $val);
    }
    push @coms, sprintf(" Basic Age: %.2f days", $age);
    push @coms, sprintf(" Cloud Age: %.2f days", $self->cloud_age());
    push @coms, '';
    my $sc      = $args->{SETCOLLECTION} || BMS::SetCollection->new
        ( -help => $args->{HELP} );
    my $ref     = $sc->set($refname);
    $sc->param('ReferenceSet', $refname);
    $sc->alias('ReferenceSet', $refname);
    $sc->param('CreateTime', time);
    $ref->param('namespace', $self->namespace_name($ans));

    my (%desc, %conn, %ages);
    my $ti    = time;
    my $count = $#{$rows} + 1;
    # while (my $row = $sth->fetchrow_arrayref) {
    #    my ($acc,$term,$e,$m,$ans,$ons,$subset,$ad,$td,$par,$up) = @{$row};
    for my $r (0..$#{$rows}) {
        my ($term, $ons, $auth, $m, $nsbtwn, $acc, $ans, $up) = @{$rows->[$r]};
        my $onto = $ons;
        unless ($onto) {
            $self->err("No ontology namespace found",
                       join(" + ", map { defined $_ ? $_ : '-undef-' } @{$rows->[$r]}));
            next;
        }
        if (my $subset = $subsets{$ons}{$term}) {
            $onto .= " : $subset";
        }
        $conn{$onto}{$term}{$acc} = 1;
        unless (defined $desc{$onto}{$term}) {
            $desc{$onto}{$term} = $self->description
                ( -id => $term, -ns => $ons, -age => $age );
        }
        unless (defined $desc{$refname}{$acc}) {
            $desc{$refname}{$acc} = $self->description
                ( -id => $acc, -ns => $ans, -age => $age );
        }
        push @{$ages{$onto}}, $up;
        if ($tick && !(($r+1) % 25000)) {
            $tick->status('FetchRows', (($r+1)/1000)."k database rows processed");
        }
    }
    $self->benchend("Read Set data from database");
    $tick->status('FilterDataSet',"Apply user filters to $count DB rows")
        if ($tick);
    my $perc   = $args->{MAXREPRESENTATION} || $args->{MAXREP};
    my $minRep = $args->{MINREPRESENTATION} || $args->{MINREP};
    if ($perc) {
        my $maxRep = int($perc * ($#setMem + 1) / 100);
        my %deleted;
        while (my ($onto, $thash) = each %conn) {
            foreach my $term (keys %{$thash}) {
                my @accs = keys %{$thash->{$term}};
                next if ($#accs < $maxRep);
                # This ontology term is over-represented
                delete $thash->{$term};
                delete $desc{$onto}{$term};
                $deleted{$onto}++;
            }
        }
        push @coms, "Rejecting ontology terms over $perc% (count $maxRep) representation";
        foreach my $onto (sort keys %deleted) {
            push @coms, "  $onto = $deleted{$onto} terms over represented";
        }
        $sc->param('MaximumRepresentation', $perc);
    }

    if ($minRep) {
        my %deleted;
        while (my ($onto, $thash) = each %conn) {
            foreach my $term (keys %{$thash}) {
                my @accs = keys %{$thash->{$term}};
                next unless ($#accs <= $minRep);
                # This ontology term is under-represented
                delete $thash->{$term};
                delete $desc{$onto}{$term};
                $deleted{$onto}++;
            }
        }
        push @coms, "Rejecting ontology terms with less than $minRep assignments";
        foreach my $onto (sort keys %deleted) {
            push @coms, "  $onto = $deleted{$onto} terms under represented";
        }
        $sc->param('MinimumRepresentation', $minRep);
    }

    $self->benchstart("Define set member descriptions");
    foreach my $r (@setMem) {
        next if ($desc{$refname}{$r});
        my $d = $self->description( -id => $r, -ns => $ans );
        $desc{$refname}{$r} = $d;
    }
    push @coms, sprintf("Time to execute DB query: %d sec", $qt);
    push @coms, sprintf("Time to fetch data from DB: %d sec", time - $ti);

    my $ti2 = time;
    $tick->status('BuildDataSet',"Define reference set and ontologies")
        if ($tick);
    while (my ($sname, $dh) = each %desc) {
        my @ids = keys %{$dh};
        next if ($#ids == -1);
        my $setObj = $sc->set($sname);
        my $ns  = $sname;
        if ($ns =~ /(.+) \:/) { $ns = $1 };
        
        if (my $agelist = $ages{$sname}) {
            $self->benchstart("Calculate data age");
            my @s = sort { $a <=> $b } @{$agelist};
            my ($min, $max) = map {int(0.5 + 100 * $_)/100} ($s[0],$s[-1]);
            $setObj->param("MinAge", $min);
            $setObj->param("MaxAge", $max);
            $self->benchend("Calculate data age");
        }
        if ($sname =~ /(GeneOntology|Xpress|BMS TF)/) {
            $self->benchstart("Get ontology parentage");
            my %parentage;
            my @stack = keys %{$dh};
            while (my $obj = shift @stack) {
                my @pars = sort $self->direct_genealogy($obj, -1, $ns );
                next if ($#pars == -1);
                $parentage{$obj} = \@pars;
                foreach my $par (@pars) {
                    unless (defined $dh->{$par}) {
                        $dh->{$par} = $self->description
                            ( -id => $par, -ns => $ns, -age => $age);
                        push @stack, $par;
                    }
                }
            }
            $self->benchend("Get ontology parentage");
            $setObj->bulk_obj_parents( \%parentage );
        }
        $setObj->param('namespace', $ns)
            unless ($setObj->param('namespace'));
        $setObj->param('id count', $#ids + 1);
        $setObj->bulk_obj_param( 'desc', $dh );
    }
    push @coms, sprintf("Time to set descriptions: %d sec", time - $ti2);
    $self->benchend("Define set member descriptions");

    $ti2 = time;
    $tick->status('BuildDataSet',"Connect reference set to ontologies")
        if ($tick);
    while (my ($oname, $condat) = each %conn) {
        $sc->bulk_connect( [ $oname, { $ref->name => $condat } ] );
    }
    # $sc->bulk_connect( [ $ref, \%conn ] );
    push @coms, sprintf("Time to build connections: %d sec", time - $ti2);
    $sc->param('DatabaseRows', $count);
    $sc->param('BuildTime', time - $ti);
    $sc->comments( @coms );
    $tick->status('DataSetDone',"Data set fully recovered")
        if ($tick);
    return $sc;
}

=head2 description

 Title   : description
 Usage   : my $desc = $ad->description( @args )
 Function: Gets descriptive text for a term
 Returns : In scalar context, a single description, otherwise an array
           of descriptions. If a namespace is defined, then there will
           always be only a single description for a given term.
 Args    : Associative array of arguments. Recognized keys [Default]:

       -id Required. The query ID. Alias -term

       -ns The namespace of the query. Required if you wish the system
           to generate missing data (in the absence of a namespace,
           the system will use guess_namespace() in an attempt to
           guess the correct value).

      -age Optional minimum freshness age, in days. Will over-ride any
           globally defined value set with age().

 -nullrows [false] If true, then the search will use an optimized SQL
           to find queries that would have returned null
           information. The return value will be a 1D array ref of
           such query IDs (scalar context call), or that array ref
           plus a 1D array of IDs that were not null.

  -ageonce [False] If true, then will prevent age constraints from
           being enforced when utilizing secondary calculations. Note
           that while potentially faster, this obviously could allow
           old data to shape the returned content.

 -redonull [false] If a true value, then any empty database rows
           (signifying failure to find a conversion on a past
           calculation) will be treated as no rows returned (forces
           re-calculation of the conversion).

Most namespaces utilize the L<generic description|generic_description>
logic; this method will query the SeqStore database if deep_dive() is
active.

=cut

sub description {
    my $self = shift;
    unshift @_, '-id' if ($#_ == 0);
    my $args    = $self->parseparams( @_ );
    my $ns      = $self->namespace_token($args->{NS} || $args->{NS1});
    my ($id, $seq) = $self->standardize_id
        ( $args->{ID} || $args->{TERM}, $ns );
    unless ($id && ($seq || !$self->tracker)) {
        if (!$seq && $self->deep_dive() && $id && 
            $self->tracker() && !$args->{RECURSING}) {
            if (my $change = $self->update_maptracker_sequence( $id )) {
                return $self->description( @_, -recursing => 1 );
            }
        }
        return wantarray ? ($unkToken) : $unkToken if ($id && $self->tracker);
        return wantarray ? () : '';
    }
    $self->benchstart;
    my $age     = $self->standardize_age( $args->{AGE} );
    my $dbh     = $self->dbh;
    my $miss    = $args->{NULLROWS};

    my $sql     = "SELECT descr FROM description WHERE upper(term) = upper(?)";
    my @binds   = ($id);
    my $gns     = $ns;
    if ($ns eq 'RS') {
        $gns = $self->guess_namespace_very_careful($id)
    } elsif ($ns eq 'SYM') {

    } else {
        $gns = $self->guess_namespace_careful($id, $ns);
    }


    if ($ns) {
        if ($useMTIDformat->{$gns}) {
            ($id) = $self->standardize_id($id, $gns);
        } elsif ($self->namespace_is_sequence($ns)) {
            # Remove sequence versioning
            $id =~ s/\.\d+$//;
        }
        unless ($ns eq 'TAX' || $self->verify_namespace($id, $ns)) {
            # Do not bother working with numbers or items with whitespace
            return wantarray ? () : '' if 
                ($self->is_number($id) || $id =~ /[\n\r\s]/);
            # $ns = 'UNK';
        }
        if ($self->namespace_is_case_sensitive($ns)) {
            # Input namespace is case-sensitive
            $sql .= " AND term = ?";
            push @binds, $id;
        }
        $sql .= " AND ns = ?";
        push @binds, $self->namespace_name($ns);
    }
    my @terms;
    unless ($args->{FORCE}) {
        if ($age) {
            if (my $asql = $self->age_filter_sql( $age, 'updated')) {
                $sql .= " AND $asql";
                $age  = undef if ($args->{AGEONCE});
            } else {
                $self->err("Failed to calculate age SQL for '$age' days");
            }
        }
        my $sth = $dbh->prepare( -name => "Get descriptions",
                                 -sql  => $sql );
        if ($args->{WARN}) {
            $self->{DESCWARN} = 1;
            printf(STDERR "%s%30s [%4s] => Description",
                   (++$self->{SCROLL_ITER} % $self->{SCROLL_SIZE}) ?
                   "\r" : "\n", $id, $ns || '');
        }
        @terms = $sth->get_array_for_field( @binds );
    }
    if (!$args->{NOCREATE} &&
        ($#terms < 0 || ($args->{REDONULL} && $#terms == 0 && !$terms[0]))) {
        if ($ns) {
            $ns = $self->namespace_token($ns);
        } else {
            $ns = $self->guess_namespace($id);
        }
        if ($ns) {
            my $func    = "update_${gns}_description";
            my $codeRef = $self->can($func) || \&update_GENERIC_description;
            @terms      = &{$codeRef}( $self, $id, $ns, $age );
        }
    }
    $self->benchend;
    $self->{DESCWARN} = 0;
    return wantarray ? @terms : $terms[0] || '';
}

sub bulk_description {
    my $self = shift;
    $self->benchstart;
    my $args    = $self->parseparams( @_ );
    my $asHash  = $args->{ASHASH} || 0;
    my $ids     = $args->{IDS};
    my $ns      = $self->namespace_name($args->{NS} || $args->{NS1});
    my $age     = $self->standardize_age( $args->{AGE} );
    my $dbh     = $self->dbh;
    my $tempListName = $dbh->list_to_temp_table
        ( $ids, [ "varchar(4000)" ], ["member"], undef, ['upper(member)'] );
    my $sql = <<EOF;
SELECT tl.member, d.descr FROM description d, $tempListName tl
 WHERE upper(term) = ANY ((SELECT array(SELECT upper(member) FROM 
       $tempListName))::text[])
   AND upper(d.term) = upper(tl.member)
EOF

    my @binds;
    if ($ns) {
        if ($self->namespace_is_case_sensitive($ns)) {
            # Input namespace is case-sensitive
            $sql .= " AND term = tl.member";
        }
        push @binds, $self->namespace_name($ns);
        $sql .= " AND d.ns = ?";
    }
    if ($age) {
        if (my $asql = $self->age_filter_sql( $age, 'updated')) {
            $sql .= " AND $asql";
        } else {
            $self->err("Failed to calculate age SQL for '$age' days");
        }
    }
    my $sth = $dbh->prepare( -name => "Get descriptions in bulk",
                             -sql  => $sql );
    if ($args->{WARN}) {
        printf(STDERR "%s%30s [%4s] => Description",
               (++$self->{SCROLL_ITER} % $self->{SCROLL_SIZE}) ?
               "\r" : "\n", ($#{$ids} + 1)." Identifiers",
               $self->namespace_token($ns) || '');
    }
    $sth->execute( @binds );
    my $rows  = $sth->fetchall_arrayref();
    # Allow the user to pass an existing hash lookup
    my $lookup = $asHash && ref($asHash) ? $asHash : {};
    map { $lookup->{$_->[0]} ||= $_->[1] } @{$rows};
    my @rv;
    foreach my $id (@{$ids}) {
        push @rv, $lookup->{$id} ||= $self->description
            ( -id => $id, @_ ) || "";
    }
   
    $self->benchend;
    return $args->{ASHASH} ? $lookup : wantarray ? @rv : \@rv;
}

sub description_lookup {
    my $self = shift;
    unshift @_, '-desc' if ($#_ == 0);
    my $args = $self->parseparams( @_ );
    my $descR = $args->{DESC} || $args->{DESCR} || $args->{TEXT} || $args->{DESCRIPTION};
    return wantarray ? () : [] unless ($descR);
    $self->benchstart;
    my @binds;
    my @where = ("description d");
    my $match;
    my $sql  = " WHERE ";
    my $split = $args->{SPLIT} || "[\\n\\r\\t]+";
    my $join  = uc($args->{JOIN} || 'AND');
    my $hash = $self->process_filter_request($descR,$split);
    my $cs   = $args->{SENSITIVE} ? 1 : 0;
    my $isEx = $args->{EXACT};
    my @texts;
    while (my ($type, $vals) = each %{$hash}) {
        my $isNot = $type eq 'IN' ? 0 : 1;
        # Always use AND for NOT IN sets:
        my $tjoin = $type eq 'IN' ? $join : 'AND';
        my @ttxt;
        foreach my $desc (@{$vals}) {
            if ($isEx) {
                push @binds, $desc;
                $match = $isNot ? '!=' : '=';
            } else {
                push @binds, ($desc =~ /\%/) ? $desc : "%$desc%";
                $match = $isNot ? 'NOT LIKE' : 'LIKE';
            }
            push @ttxt, ($cs ? "d.descr $match ?" :
                          "upper(d.descr) $match upper(?)");
        }
        push @texts, "(".join(" $tjoin ", @ttxt).")";
    }
    # Always combine IN and NOT IN with AND:
    $sql .= "(".join(" AND ", @texts).")";
    if (my $ncl = $self->_namespace_select($args->{NS} || $args->{NS1})) {
        $sql .= " AND ns $ncl";
    }
    if (my $int = $args->{INT} || $args->{INTERSECTION}) {
        my @intList = ref($int) ? @{$int} : ($int);
        push @where, "conversion c";
        $sql .= " AND c.term_in = d.term AND c.ns_in = d.ns AND upper(c.term_out) ";
        if ($#intList == 0) {
            $sql .= "= upper(?)";
        } else {
            $sql .= "IN (".join(", ", map { 'upper(?)' } @intList).")";
        }
        push @binds, @intList;
        if (my $insR = $args->{INTNS}) {
            my $ins = $self->namespace_name($insR);
            if ($ins) {
                $sql .= " AND c.ns_out = ?";
                push @binds, $ins;
            } else {
                warn "Unknown namespace '$insR'\n  ";
            }
        }
    }


    $sql = "SELECT d.term, d.ns, d.descr FROM ".join(', ', @where) . $sql;
    my $sth = $self->dbh->prepare
        ( -sql   => $sql,
          -name  => "Find terms matching a description",
          -limit => $args->{LIMIT},
          -level => 1);
    warn $sth->pretty_print(@binds) if ($args->{DUMPSQL});
    $sth->execute( @binds );
    my $rows  = $sth->fetchall_arrayref;
    $self->benchend;
    return wantarray ? map { $_->[0] } @{$rows} : $rows;
}

=head2 mappings

 Title   : mappings
 Usage   : $ad->mappings( @args )
 Function: Gets mapping data for the requested queries.
 Returns : All rows found in the database from the search (as an array
           or array ref, depending on calling context).
 Args    : Associative array of arguments. Recognized keys [Default]:

       -id Required. The query ID (scalar string) or IDs (1D array reference
           of strings) you wish to get mapping information for.

       -ns The namespace of the query. Required if you wish the system
           to generate missing data.

       -db An identifier for the MapTracker genomic database being searched.

      -min Optional minimum matched value. If provided then only
           results in the databse with that value or greater will be
           returned. A value of zero is ignored.

      -age Optional minimum freshness age, in days. Will over-ride any
           globally defined value set with age().

 -nullrows [false] If true, then the search will use an optimized SQL
           to find queries that would have returned null
           information. The return value will be a 1D array ref of
           such query IDs (scalar context call), or that array ref
           plus a 1D array of IDs that were not null.

    -limit [undef] If non-zero, then return only the specified number
           of rows from the database. Primarily useful for testing, or
           if you want just a few examples.

  -dumpsql [false] If defined, the SQL statement being executed to
           perform the query will be pretty printed.

If no entries exist in the GenAcc database, they will be pulled from
MapTracker. Note that the alignments must have already been performed,
the program will not initiate new sequence alignments if none are
found.

=cut

sub mappings {
    my $self  = shift;
    unshift @_, '-id' if ($#_ == 0);
    my $args   = $self->parseparams( @_ );
    my $query  = $args->{ID} || $args->{QUERY};
    return wantarray ? () : undef unless ($query);
    $self->bench_start();

    my @ids     = ref($query) ? @{$query} : split(/[\t\r\n\,]+/,$query);
    my $dbh     = $self->dbh;
    my $min     = $args->{MIN} || $args->{MINSCORE};
    my $age     = $self->standardize_age( $args->{AGE} );
    my $miss    = $args->{NULLROWS};
    my $noNull  = $args->{NONULL} ? 1 : 0;
    my $doWarn  = $args->{WARN};
    my $howBad  = $args->{HOWBAD};
    my $qns     = $self->namespace_token
        ( $args->{NS} || $args->{NS1} || $args->{QNS});
    my $current = $args->{CURRENT};
    my @cols    = $dbh->column_order('mapping');
    if (my $cr = $args->{COLS} || $args->{COLUMNS}) {
        @cols = map { lc($_) } (ref($cr) ? @{$cr} : split(/[\,\s]+/, $cr));
    }
    my %chash   = map { $cols[$_] => $_ } (0..$#cols);
    my $build   = $args->{BUILD};
    my $bestBld = $args->{BESTBUILD};
    if (($build || $bestBld) && !defined $chash{sub_vers}) {
        push @cols, 'sub_vers';
        $chash{sub_vers} = $#cols;
    }
    if ($bestBld && !defined $chash{qry}) {
        push @cols, 'qry';
        $chash{qry} = $#cols;
    }
    my $qvInd   = $chash{qry_vers};
    my $sInd    = $chash{'sub'};

    my $sql = "SELECT ".join(', ', map { "m.$_" } @cols).
        " FROM mapping m WHERE m.qry = ?";
    my $check = "SELECT m.qry FROM mapping m WHERE m.qry = ?";
    my @dblist = $self->_database_reqs( $args );
    if ($#dblist > -1) {
        $sql   .= " AND m.sub_set = ?";
    } else {
        @dblist = (0);
    }
    my (@binds, @cbinds, $needToCheck);
    if ($age) {
        if (my $asql = $self->age_filter_sql( $age, 'm.updated')) {
            $sql   .= " AND $asql";
            $check .= " AND $asql";
        } elsif ($age) {
            $self->err("Failed to calculate age SQL for '$age' days");
        }
    }
    if ($min) {
        $min *= 100 if ($min && $min < 1);
        if ($min >= 0 && $min <= 100) {
            $sql   .= " AND m.score >= ?";
            push @binds, $min;
            $needToCheck = 1;
        }
    }
    if (defined $howBad) {
        $sql .= " AND m.howbad <= ?";
        push @binds, $howBad;
    }
    my $lim = $args->{LIMIT};
    $lim    = 1 if ($miss);

    my $sthU = $self->dbh->prepare
        ( -sql   => $sql,
          -name  => "Get mappings for user request",
          -level => 1,
          -limit => $lim || 0 );

    my $sthC = $self->dbh->prepare
        ( -sql   => $check,
          -name  => "Check for existance of mappings",
          -level => 1,
          -limit => 1,);

    my $sthV = $self->dbh->prepare
        ( -sql   => "$sql AND m.qry_vers = ?",
          -name  => "Get mappings for versioned user request",
          -level => 1,
          -limit => $lim || 0 );
    
    my @results;
    my %nulls;
    for my $i (0..$#ids) {
        my $idReq = $ids[$i];
        my $id = $idReq;
        my $vers;
        if ($sequenceNs->{$qns} && 
            $id =~ /^([^\.]+)\.(\d+)$/) { 
            ($id, $vers) = ($1, $2);
        }
        ($id) = $self->standardize_id( $id, $qns );
        if ($doWarn) {
            printf(STDERR "%s%30s [%4s] => Genomic Maps", 
                   (++$self->{SCROLL_ITER} % $self->{SCROLL_SIZE}) ?
                   "\r" : "\n", $id, $qns);
        }
        warn $sthU->pretty_print($dblist[0] ? ($id, $dblist[0], @binds) :
                                 ($id, @binds)) if ($args->{DUMPSQL} && !$i);
        my $ntc = $needToCheck;
        foreach my $db (@dblist) {
            my @lbinds = $db ? ($id, $db, @binds) : ($id, @binds);
            my $sth = $sthU;
            if ($vers) {
                $sth = $sthV;
                push @lbinds, $vers;
            }
            $sth->execute( @lbinds );
            my $rows  = $sth->fetchall_arrayref;
            if ($#{$rows} < 0) {
                my $refresh = 1;
                if ($ntc) {
                    # It is possible that we got zero rows not because the
                    # data were never calculated, but rather because we have
                    # used a SQL filter
                    $sthC->execute($id, @cbinds);
                    my $crows = $sthC->fetchall_arrayref;
                    $ntc = 0;
                    
                }
                if ($refresh) {
                    # Nothing in the database
                    my $uns = $qns;
                    unless ($uns) {
                        $uns = $self->guess_namespace($id);
                        $uns = "" unless ($reliableGuess->{$uns});
                    }

                    my $novel = 0;
                    my $indNS = $mappingNS->{$uns};
                    if ($indNS) {
                        $novel = $self->update_indirect_genomic_mappings
                            ( $id, $uns, $db, $indNS, $age, $doWarn );
                    } elsif (defined $indNS) {
                        my $passId = $vers ? "$id.$vers" : $id;
                        $novel = $self->update_genomic_mappings
                            ( $passId, $uns, $db, $age, $doWarn );
                    }
                    if ($novel) {
                        # The update completed succesfully
                        $sth->execute( @lbinds );
                        $rows = $sth->fetchall_arrayref();
                    }
                }
            }
            my $keep = $rows;
            if ($vers) {
                # The user wants only a specific version
                $keep = [];
                foreach my $row (@{$rows}) {
                    push @{$keep}, $row if ($row->[$qvInd] && 
                                            $row->[$qvInd] == $vers);
                }
            }
            my $wasHit = 0;
            if ($miss) {
                my $isNull = [];
                foreach my $row (@{$keep}) {
                    if ($row->[$sInd]) {
                        $wasHit = 1;
                    } else {
                        push @{$isNull}, $row;
                    }
                }
                $keep = $isNull;
            }
            push @results, @{$keep};
            $nulls{$id} = 1 if ($#{$keep} == -1 && !$wasHit);
        }
    }
    if ($current) {
        my %struct;
        my ($qind, $svind) = map { $chash{$_} } qw(qry sub_vers);
        my @inds = ($qind, $qvInd, $svind);
        foreach my $row (@results) {
            my ($q, $qv, $sv) = map { $row->[$_] }@inds;
            $qv ||= 0;
            my $svns = $sv ||= '';
            if ($sv =~ /^([^\d]*)(\d+)([^\d]*)$/) {
                my ($pre, $num, $post) = ($1 || '', $2, $3 || '');
                $svns = "$pre\t$post";
                $sv   = $num;
            } else {
                $sv = 0;
            }
            push @{$struct{$q}{$qv}{$svns}{$sv}}, $row;
        }
        @results = ();
        while (my ($q, $qvs) = each %struct) {
            # For each query, find the most recent (largest) query version:
            my ($bqv) = sort { $b <=> $a } keys %{$qvs};
            my @keep;
            while (my ($svns, $svs) = each %{$qvs->{$bqv}}) {
                # Keep all subject "version namespaces", eg "NCBI" or "RSGC_4"
                my ($bsv) = sort { $b <=> $a } keys %{$svs};
                if ($bsv) {
                    push @keep, @{$svs->{$bsv}};
                } else {
                    # No build version - probably all null rows, just
                    # keep one if so
                    my %stat;
                    foreach my $row (@{$svs->{$bsv}}) {
                        push @{$stat{ $row->[$sInd] ? 1 : 0 }}, $row;
                    }
                    my ($s) = sort {$b <=> $a} keys %stat;
                    if ($s) {
                        # Ok, these all have entries
                        push @keep, @{$stat{$s}};
                    } else {
                        # Null rows, keep only one
                        push @keep, $stat{$s}[0];
                    }
                }
            }
            push @results, @keep;
            $nulls{$q} = 1;
        }
    }
    if ($noNull) {
        # Remove rows with no subject column
        if (defined $sInd) {
            my @keep;
            foreach my $res (@results) {
                push @keep, $res if ($res->[$sInd]);
            }
            @results = @keep;
        }
    } else {
        my @nullRow = map { '' } (0..$#cols);
        my $qInd = $chash{'qry'};
        my $nInd = $chash{'qry_ns'};
        my $nsn  = $self->namespace_name($qns);
        foreach my $id (sort keys %nulls) {
            my $unsn = $nsn;
            unless ($unsn) {
                $unsn = $self->guess_namespace($id);
                $unsn = $unsn ? $self->namespace_name($unsn) : "";
            }
            my @row     = @nullRow;
            $row[$qInd] = $id if (defined $qInd);
            $row[$nInd] = $unsn if (defined $nInd);
            push @results, \@row;
        }
    }
    if ($build) {
        # Filter results by build
        my $bInd = $chash{sub_vers};
        $build = uc($build);
        my @keep;
        foreach my $res (@results) {
            my $b = $res->[$bInd];
            push @keep, $res if (!$b || uc($b) eq $build);
        }
        @results = @keep;
    } elsif ($bestBld) {
        # Pick out the best build for each query
        my $bInd = $chash{sub_vers};
        my $qInd = $chash{qry};
        my %bldHash;
        foreach my $res (@results) {
            if (my $q = $res->[$qInd]) {
                $b = $res->[$bInd] || 0;
                if ($b =~ /(\d+)/) {
                    # Will not always work!!
                    $b = $1;
                } else {
                    $b = 0;
                }
                push @{$bldHash{$q}{$b}}, $res;
            }
        }
        @results = ();
        foreach my $bHash (values %bldHash) {
            my ($bst) = sort { $b <=> $a } keys %{$bHash};
            push @results, @{$bHash->{$bst}};
        }
        
    }
    $self->bench_end();
    return wantarray ? @results : \@results;
}

sub genomic_overlap {
    my $self = shift;
    $self->bench_start();
    $self->bench_start('Setup Query');
    unshift @_, '-id' if ($#_ == 0);
    my $args  = $self->parseparams( @_ );
    my $dist  = $args->{DIST};
    $dist     = $args->{DISTANCE} unless (defined $dist);
    $dist     = 1000000 unless (defined $dist);
    my $howBad = $args->{HOWBAD};
    my $subBad = $args->{SUBBAD}; $subBad = $howBad unless (defined $subBad);
    my $keepPat = $args->{KEEP};
    my $tossPat = $args->{TOSS};

    if ($dist =~ /^(\d+)\s*(m|k|g)b?$/i) {
        $dist = $1;
        my $u = $2;
        if ($u eq 'k') {
            $dist *= 1000;
        } elsif ($u eq 'm') {
            $dist *= 1000 * 1000;
        } elsif ($u eq 'g') {
            $dist *= 1000 * 1000 * 1000;
        }
    }
    my $idReq = $args->{ID} || $args->{QUERY} ||
        $args->{CHR} || $args->{CHROMOSOME};
    my $ns1   = $self->namespace_token
        ( $args->{NS} || $args->{NS1} || $args->{QNS}) || 
        $self->guess_namespace( $idReq );
    my ($id, $seq) = $self->standardize_id
        ( $args->{ID} || $args->{TERM}, $ns1 );
    if ($args->{WARN}) {
        printf(STDERR "%s%30s [%4s] => Genomic Overlap %.3fkb", 
               (++$self->{SCROLL_ITER} % $self->{SCROLL_SIZE}) ?
               "\r" : "\n", $id || $idReq, $ns1, $dist / 1000 );
    }
    unless ($id) {
        $self->bench_end('Setup Query');
        $self->bench_end();
        printf(STDERR "Unrecognized ID $idReq!\n") if ($args->{WARN});
        return wantarray ? () : { id => $idReq, ns1 => $ns1 };
    }
    my $nsReqs = $args->{NS2} || $args->{SNS} || "LL";
    my @nsExp  = ref($nsReqs) ? @{$nsReqs} : split(/\s*\,\s*/, $nsReqs);
    unless ($args->{NONSKIDS} || $args->{NONSCHILDREN}) {
        # Add child namespaces for generic requests
        push @nsExp, map { $self->namespace_children($_) } @nsExp;
    }
    my %u2s   = map { $self->namespace_name( $_ ), 1 } @nsExp;
    my @ns2s  = sort keys %u2s;
    my $rv    = { id => $id, ns1 => $ns1 };
    my @taxae = $self->convert( -id => $id, -ns1 => $ns1, -ns2 => 'TAX' );
    $rv->{taxa} = join(",", @taxae);
    if ($args->{UPDATE}) {
        unless ($#taxae == 0) {
            $rv->{err} = $#taxae == -1 ? "No species defined for query" : 
                "Query has multiple species assigned to it";
            $self->bench_end('Setup Query');
            $self->bench_end();
            return wantarray ? () : $rv;
        }
        my $taxa  = $rv->{taxa} = $taxae[0];
        map { $self->update_genome_mappings
                  ( -taxa => $taxa, 
                    -age  => $args->{UPDATEAGE},
                    -ns   => $_ ) } @ns2s;
    }
    my $minSc   = $args->{MIN};
    my $dumpSql = $args->{DUMPSQL};
    $minSc     *= 100 if ($minSc && $minSc < 1);
    my $subSc   = $args->{SUBMIN}; $subSc = $minSc unless (defined $subSc);
    $self->bench_end('Setup Query');
    
    my (%builds, %bn, $bestB);
    if ($ns1 eq 'CHR') {
        my $s = $args->{START};
        my $e = $args->{END};
        if ($id =~ /(.+)\:(\d+)\-(\d+)$/) {
            # Start-End embedded at end of ID
            ($id, $s, $e) = ($1, $2, $3);
        }
        if ($bestB = $args->{BUILD}) {
            # User request to use a specific build
        } elsif ($id =~ /^([^\.]+\.[^\.]+\.[^\.]+)\.([^\.]+)$/) {
            # Build number is embedded in provided ID
            ($id, $bestB) = ($1, $2);
        }
        if ($bestB) {
            $bn{$bestB} = $bestB;
            push @{$builds{$bestB}}, [$id, $s, $e, 1, 1,"$s..$e"];
        }
    } else {
        $self->bench_start('Recover Query Maps');
        my @maps  = $self->mappings
            ( -id => $id, -ns1 => $ns1 , -min => $minSc, -dumpsql => $dumpSql,
              -build => $args->{BUILD}, -howbad => $howBad );
        if ($#maps == -1) {
            $rv->{err} = "No locations found for query";
            printf(STDERR " No locations found for $id\n") if ($args->{WARN});
            $self->bench_end('Recover Query Maps');
            $self->bench_end();
            return wantarray ? () : $rv;
        }
        foreach my $map (@maps) {
            my ($subj, $sc, $str, $s, $e, $qv, $build, $qFT) = 
                ($map->[1], $map->[2], $map->[4],  $map->[7], 
                 $map->[8], $map->[9], $map->[10], $map->[17]);
            next unless ($subj);
            my $bn = $build || "";
            $bn   =~ s/[^\d]+//g;
            $bn ||= 0;
            $qv ||= 0;
            my $key = sprintf("%d.%05d", $qv, $bn);
            push @{$builds{$key}}, [$subj, $s, $e, $str, $sc, $qFT];
            $bn{$key} ||= $build;
        }
        ($bestB) = sort { $b <=> $a } keys %builds;
        $self->bench_end('Recover Query Maps');
    }
    unless (defined $bestB) {
        $rv->{err} = "No locations found for query ??";
        printf(STDERR "No locations found for $id!\n") if ($args->{WARN});
        $self->bench_end();
        return wantarray ? () : $rv;
    }
    $self->bench_start('Organize Subjects');
    my %bySubj;
    # ($ds, $de) = the start/end coordinates of the region we wish to overlap
    # It will be set by the start/end coords of the subject, and then expanded
    # by $dist
    foreach my $dat (sort { $a->[1] <=> $b->[1] } @{$builds{$bestB}}) {
        my ($subj, $s, $e, $str, $sc, $qFT) = @{$dat};
        $self->death("($subj, $s, $e, $str, $sc)") unless ($s && $e);
        if ($s > $e+1) {
            # Gaps are often represented as [10,9], so $e+1 allows them
            $self->err("[start,end] being inverted from [$s,$e]");
            ($s,$e) = ($e, $s);
        }
        next if ($minSc && $sc < $minSc);
        my ($ds, $de) = ($s - $dist, $e + $dist);
        my $qdat = [$s, $e, $str, $sc, $qFT];
        if ($bySubj{$subj} && $ds <= $bySubj{$subj}[-1][1]) {
            # This location overlaps (within the requested distance) with prior
            $bySubj{$subj}[-1][1] = $de if ($bySubj{$subj}[-1][1] < $de);
            push @{$bySubj{$subj}[-1][2]}, $qdat;
        } else {
            # Add as new location
            push @{$bySubj{$subj}}, [ $ds, $de, [$qdat]];
        }
    }
    my @locs;
    my $build   = $bn{$bestB};
    my $sth     = $self->dbh->named_sth
        ("Get Genomic Overlaps" . 
         (defined $subBad ? ' HowBad' : '').
         ($subSc ? ' Scored' : '')
         );
    my $slowSql = $dumpSql ? 0 : $args->{SLOWSQL};
    my $mt      = $self->tracker();
    my %discarded;
    $self->bench_end('Organize Subjects');
    $self->bench_start('Recover Overlaps');
    foreach my $subj (sort %bySubj) {
        foreach my $sdat (@{$bySubj{$subj}}) {
            my ($ds, $de, $qdats) = @{$sdat};
            my @sLocs;
            foreach my $ns2 (@ns2s) {
                my $ns2Tok = $self->namespace_token($ns2);
                my @binds = ($subj, lc($subj), $ds, $de, $build, $ns2);
                push @binds, $subBad if (defined $subBad);
                push @binds, $subSc  if (defined $subSc);
                warn $sth->pretty_print(@binds) if ($dumpSql);
                my $elapsed = time;
                $sth->execute(@binds);
                $elapsed = time - $elapsed;
                warn $sth->pretty_print(@binds)
                    if ($slowSql && $elapsed > $slowSql);
                my $rows = $sth->fetchall_arrayref;
                if ($keepPat || $tossPat) {
                    # We want to filter the objects
                    my @keep;
                    foreach my $row (@{$rows}) {
                        next if ($tossPat && $row->[0] =~ /$tossPat/i);
                        next if ($keepPat && $row->[0] !~ /$keepPat/i);
                        push @keep, $row;
                    }
                    $rows = \@keep;
                }
                if (1) {
                    # Remove deprecated entries
                    my @keep;
                    my $qInd    = 1;
                    foreach my $row (@{$rows}) {
                        my $qry  = $row->[0];
                        my $mtNs = $mtNamespaces->{$ns2Tok} || "#None#";
                        my $seq  = $mt->get_seq( $mtNs . $qry );
                        if ($seq) {
                            if ($self->fast_class($seq->id,'Deprecated')) {
                                $discarded{$qry}++;
                            } else {
                                push @keep, $row;
                            }
                        } else {
                            # Not sure what the best option here is...
                            push @keep, $row;
                            # $discarded{$qry}++;
                        }
                    }
                    $rows = \@keep;
                }
                next if ($#{$rows} == -1);
                push @sLocs, [ $subj, $qdats, $rows, $build, $ns2 ];
            }
            push @locs, $#sLocs == -1 ? [$subj, $qdats, [], $build ] : @sLocs;
        }
    }
    if ($args->{WARN}) {
        my $dtxt = join(",", sort keys %discarded);
        printf(STDERR "$id : discarded deprecated entries : $dtxt\n") if ($dtxt);
    }
    $rv->{locs} = \@locs;
    # die $self->branch($rv);
    $self->bench_end('Recover Overlaps');
    $self->bench_end();
    return wantarray ? @locs : $rv;
}

sub coordinates_to_loci {
    my $self  = shift;
    my $args  = $self->parseparams( @_ );
    my $chr   = $args->{CHR};
    my $start = $args->{START} || $args->{POS};
    return [] unless ($chr && $start);
    $self->bench_start;
    my $end   = $args->{END} || $start;
    my $dbh   = $self->dbh;
    my $build = $args->{BUILD};
    my $hb    = $args->{HOWBAD};
    my $ns    = $args->{NS} || "LL";
    my $nsn   = $self->namespace_name( $ns );
    if (!$build && $chr =~ /^(\S+)\.([^\.]{4,})$/) {
        ($chr, $build) = ($1, $2);
    }
    # Can not really prepare the range part of the query.
    # The index is on (sub, sub_start, sub_end)
    # If Postgres is provided these as bind variables, it will usually
    # default to a seq_scan. Put them in explicitly to guide the
    # query planner to use the index properly:
    my $sql = sprintf("SELECT qry, score FROM mapping WHERE sub = %s AND sub_end >= %d AND sub_start <= %d AND qry_ns = ?", $dbh->quote($chr), $start, $end);
    my @binds = ($nsn );
    if (defined $hb) {
        $sql .= " AND howbad <= ?";
        push @binds, $hb;
    }
    if ($build) {
        $sql .= " AND sub_vers = ?";
        push @binds, $build;
    }
    my $sth = $dbh->prepare
        ( -sql   => $sql,
          -name  => "Find loci overlapping coordinates",
          -level => 1, );
    $sth->execute(@binds);
    my $rv = $sth->fetchall_arrayref;
    $self->bench_end;
    return $rv;
}

my $genomicMappingAges = {};
sub update_genome_mappings {


    # THIS IS NOT update_genomic_mappings() !!!!!
    #                         ^^
    # Poor method name choice :/

    my $self  = shift;
    my $args  = $self->parseparams( @_ );
    my $age   = $self->standardize_age( $args->{AGE} );
    return 0 unless ($age);
    my $dbh   = $self->dbh;
    return -1 unless ($dbh);
    my $taxa  = $args->{TAXA};
    return -1 unless ($taxa);
    my $nsN   = $self->namespace_name( $args->{NS} || $args->{NS1} );
    return -1 unless ($taxa);
    $self->bench_start();
    my $setN  = "$taxa $nsN Genome Maps";
    my $dbAge;
    my $where = "set_name = ? AND table_name = 'mapping' AND ns1 = ?";
    my @binds = ($setN, $nsN);
    unless ($dbAge = $genomicMappingAges->{$setN}) {
        my $sth = $dbh->prepare
            ( -sql   => "SELECT updated FROM bulk_loads WHERE $where",
              -name  => "Find age of bulk genome mapping",
              -level => 1, );
        my @ages = $sth->get_array_for_field( @binds );
        ($dbAge) = sort {$b <=> $a} @ages;
        $genomicMappingAges->{$setN} = $dbAge;
    }
    if (defined $dbAge && $dbAge < $age) {
        # The genome set is up-to-date
        $self->bench_end();
        return $age - $dbAge;
    }
    # We need to update the mappings
    my $briefTime = 0.0001;
    my @subjs = $self->convert( -id => $taxa, -ns1 => 'TAX', 
                                -ns2 => $nsN, -age => $age);
    $self->err("STILL WORKING ON THIS", "Got ".scalar(@subjs)." subjects",
               "doing nothing!"); return -1;

    foreach my $sbj (@subjs) {
        $self->mappings( -id => $sbj, -ns1 => $nsN, -age => $briefTime );
    }
    $self->dbh->update_rows( 'bulk_loads', [{
        set_name   => $setN,
        table_name => 'mapping',
        ns1        => $nsN,
        age        => 0,
        set_size   => $#subjs + 1,
    }], [qw(set_name mapping ns1)] );
    $genomicMappingAges->{$setN} = $briefTime;
    $self->bench_end();
    return $age;
}

=head1 GENERAL METHODS

=head2 new

 Title   : new
 Usage   : my $ad = BMS::MapTracker::AccessDenorm->new( @args )
 Function: Generate a new AccessDenorm object
 Returns : A blessed object
 Args    : Associative array of arguments. Recognized keys [Default]:

      -age [90] Global data freshness, in days. Calls age().

  -tracker Optional BMS::MapTracker object. If not provided, one will
           be created.

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
        DEEPDIVE    => 0,
        CONVERTERS  => {},
        DEFAULT_AGE => 90,
        MINIMUM_AGE => 0.0001, # a bit less than 10 seconds
        USE_REL_AGE => 1,
        CACHE       => {},
        CACHE_COUNT => 0,
        MAXCACHE    => 1000,
        SCROLL_ITER => 0,
        SCROLL_SIZE => 50,
    };
    bless ($self, $class);
    my $debug = $self->debug();
    $debug->max_array(300);
    $debug->max_hash(30);
    $debug->extend_param('skip_key', ['TRACKER','SCHEMA','URLS','CHILDREN','SPACE', 'CLASSES']);
    $debug->format( $ENV{HTTP_HOST} ? 'html' : 'text' );

    # Will pull defaults from ..../BMS/MapTracker/AccessDenorm.param
    my $args = BMS::ArgumentParser->new
        ( -paramfile => $self,
          -nocgi     => 1,
          -dbname    => 'MTRKP1',
          @_,
          -paramalias => {
              requiremt  => [qw(requiremaptracker)],
              oracle     => [qw(useoracle)],
              postgres   => [qw(usepostgres)],
              age        => [qw(ageall)],
              cloudage   => [qw(cloud_age ageall)],
              xxxx => [qw()],
              xxxx => [qw()],
              xxxx => [qw()],
              xxxx => [qw()],
              xxxx => [qw()],
              xxxx => [qw()],
              xxxx => [qw()],
          });
    
    $args->default_only(qw(adminmail errorfile pghost pgport pglib));

    #die $self->branch($args);
    $self->bench_start("Init");
    # $self->set_oracle_environment('minerva');
    weaken( $self->{TRACKER} = $args->{TRACKER} );
    $self->{REQUIREMT} = $args->val('requiremt');
    $self->deep_dive( $args->val('deepdive'));
    
    $self->bench_end("Init");

    $self->bench_start('Database Connect');
    my ($dbType, $dbn);
    $usePostgres = $args->val('oracle') ? 0 : 
        $args->val('postgres') ? 1 : $usePostgres;
    if ($usePostgres) {
        foreach my $k (qw(PGPORT PGHOST PGLIB)) {
            if (my $v = $args->val($k)) {
                $ENV{$k} = $v;
            }
        }
        $dbType = "dbi:Pg:dbname=genacc";
        $dbn    = 'tilfordc',
    } else {
        $dbType = "dbi:Oracle:";
        $dbn    = 'genacc/genacc@'.($args->val('dbname'));
    }
    eval {
        $self->{DBH} = BMS::FriendlyDBI->connect
            ($dbType, $dbn,
             undef, { RaiseError  => 0,
                      PrintError  => 0,
                      LongReadLen => 100000,
                      AutoCommit  => 1, },
             -errorfile => $args->val('errorfile') || '',
             -noenv     => $args->val('noenv') || 0,
             -adminmail => $args->val('adminmail') || '');
    };
    # die $self->branch( -obj => \%ENV, -maxany => 100);
    unless ($self->{DBH}) {
        $self->death("Failed to connect to GENACC");
    }
    # Not sure where this is coming from, but it is causing a problem
    # in the new Perl when forking
    $self->ignore_error("uncleared implementors data");
    # However, some junk is printted by STDERR by default. Redirect it
    my $shutUpAlready = "/scratch/PerlDBI-TraceOutput.txt";
    system("touch $shutUpAlready");
    chmod(0666, $shutUpAlready);
    $self->{DBH}->trace(0, $shutUpAlready);
    $self->{DBH}->update_via_array_insert
        ( $dbType =~ /oracle/i ? 0 : 15);
    $self->bench_end('Database Connect');

    $self->bench_start("Init");
    my $age = $self->age( $args->val('age') );
    $self->cloud_age( $args->val('cloudage') || $age );
    my $schema = $self->set_schema();
    
    # $self->{DBH}->make_all if ($args->{REBUILD});
    $self->standard_sths();
    $self->finalize_conditionals();
    unless ($bioRoot) {
        $bioRoot = $self->valid_namespace_hash(qw(AL AR AP));
        $bioRoot->{FLOC} = 'AL';
        $bioRoot->{FRNA} = 'AR';
        $bioRoot->{FPRT} = 'AP';
    }
    $self->bench_end("Init");
    return $self;
}

sub DESTROY {
    my $self = shift;
    return unless ($self);
    foreach my $key (qw(TRACKER MAPLOC)) {
        if (my $obj = $self->{$key}) {
            if (my $dbi = $obj->{DBI}) {
                $dbi->release;
            }
        }
    }
}

=head2 dumpsql

 Title   : dumpsql
 Usage   : my $val = $ad->dumpsql( $newValue )
 Function: Sets SQL debugging level
 Returns : The current value
 Args    : Dump level.

   0 results in no SQL being shown, other integers will show
   increasingly verbose SQL prior to execution. Primarily useful for
   debugging (or for the curious).

=cut

sub dumpsql {
    return shift->dbh->dumpsql( @_ );
}

=head2 tracker

 Title   : tracker
 Usage   : my $mt = $ad->tracker( )
 Function: Gets the MapTracker interface
 Returns : A BMS::MapTracker object
 Args    : 

=cut

sub tracker {
    my $self = shift;
    unless (defined $self->{TRACKER}) {
        eval {
            $self->ignore_error("Failed to connect to MapTracker");
            $self->ignore_error("dbname=maptracker");
            require BMS::MapTracker;
            $self->{TRACKER} = BMS::MapTracker->new( -dbadmin => 0, );
        };
        unless ($self->{TRACKER}) {
            my @msg;
            if (-s "/stf/biohtml/MapTrackerOut.msg") {
                push @msg, ("MapTracker database is offline",
                            `cat /stf/biohtml/MapTrackerOut.msg`);
            }
            push @msg, ("MapTracker database is not available",
                        "GenAcc will not be able to generate new information, but can still query previously generated data.");
            if ($self->{REQUIREMT}) {
                $self->death(@msg);
            } else {
                $self->msg_once("[!!]", @msg);
            }
            $self->{TRACKER} = 0;
        }
    }
    return $self->{TRACKER};
}

sub maploc {
    my $self = shift;
    unless (defined $self->{MAPLOC}) {
        eval {
            require BMS::SnpTracker::MapLoc;
            $self->{MAPLOC} = BMS::SnpTracker::MapLoc->new
                ( -build    => 'GRCh37',
                  -instance => 'maploc2' );
        };
        unless ($self->{MAPLOC}) {
            $self->{MAPLOC} = 0;
            $self->msg_once
                ("[!!]", "MapLoc variant database API could not be created",
                 "New conversions requiring MapLoc will be impossible");
        }
    }
    return $self->{MAPLOC};
}

sub sts {
    my $self = shift;
    unless (defined $self->{STS}) {
        eval {
            require BMS::MapTracker::SciTegicSmiles;
            $self->{STS} = BMS::MapTracker::SciTegicSmiles->new
                ( -tracker => $self->tracker, );
        };
        unless ($self->{STS}) {
            $self->{STS} = 0;
            $self->msg_once
                ("SMILES canonicalization service not available",
                 "New conversions requiring canonical SMILES will be impossible");
        }
    }
    return $self->{STS};
}

=head2 age

 Title   : age
 Usage   : my $val = $ad->age( $newval )
 Function: Gets / sets the minimum data age
 Returns : The current value
 Args    : Optional new value, a real positive number

This value defines a global age filter. It will be used in any
age-sensitive method unless the -age parameter is explicitly passed in
that method call. The passed value will be cleaned up using
standardize_age(), allowing units other than days to be specified.

If age( ) is set to zero (see minimum_age() below), then every query
method called will automatically recalculate the requested
data. Otherwise, data will be recalculated only if it is older than
the specified age. Of course, if no data exists at all, it will always
be calculated.

=cut

sub age {
    my $self = shift;
    if (defined $_[0]) {
        $self->{DEFAULT_AGE} = ($_[0] =~ /^\-\d+$/) ? 
            undef : $self->standardize_age($_[0]);
    }
    return $self->{DEFAULT_AGE};
}

# Get a test condition to recover only data of a certain age or younger:
sub age_filter_sql {
    my $self = shift;
    my ($age, $col) = @_;
    $col ||= 'updated';
    # WE ARE ALWAYS ASSUMING THAT THE AGE SHOULD BE RELATIVE
    # Can reconsider $self->{USE_REL_AGE}

    if (my $asql = $self->_age_to_sqldate( $age )) {
        return "$col >= $asql";
    } else {
        return "";
    }
}

# Convert a date into time from 'now'
sub age_recover_sql {
    my $self = shift;
    my $col  = shift || 'updated';
    return "sysdate - $col" unless ($usePostgres);
    # can not just use "now() - $col" it returns an "interval" object rather
    # than just he number of days
    # It sure seems like there should be a better way to do this. But I think
    # PG is so hung up on leveraging intervals that this is ironically the
    # "most elegant" approach.
    return "extract(epoch from now() - $col) / 86400";
}

my $sqlDates = {};
sub _age_to_sqldate {
    my $self = shift;
    my $age  = shift;
    return "" unless ($age);
    unless ($sqlDates->{$age}) {
        my $minTxt = $self->_age_to_date( $age );
        $sqlDates->{$age} = "to_timestamp('$minTxt', 'yyyy-mm-dd hh24:mi:ss')";
        # warn "[$age] = $sqlDates->{$age}\n";
    }
    return $sqlDates->{$age};
}

my $plainDates = {};
sub _age_to_date {
    my $self = shift;
    my $age  = shift;
    return "" unless ($age);
    unless ($plainDates->{$age}) {
        my $stnd   = $self->standardize_age($age);
        my $date   = ParseDate( $self->dbh->oracle_start() );
        my $delta  = ParseDateDelta($stnd =~ /^\d+$/ ? "$stnd days ago" :
                                    int($stnd * 24 * 60 * 60)." seconds ago");
        my $min    = DateCalc($date, $delta);
        my $minTxt = UnixDate($min, "%Y-%m-%d %T");
        unless ($minTxt) {
            $self->err("Unable to generate date from '$age' days ago");
            $minTxt = "1000-01-01 00:00:00";
        }
        $plainDates->{$age} = $minTxt;
    }
    return $plainDates->{$age};
}

sub oracle_start {
    my $self = shift;
    if (my $set = shift) {
        $self->{ORA_START} = $set;
    }
    if (my $explicit = $self->{ORA_START}) { return $explicit; }
    return $self->dbh->oracle_start();
}

sub specific_age {
    my $self = shift;
    my ($ns1, $ns2, $age) = @_;
    ($ns1, $ns2) = map { $self->namespace_token($_) } ($ns1,$ns2);
    my $key = "$ns1-$ns2";
    if ($key eq '-') {
        # No namespaces were provided, just use normal age
        return $age ? $self->standardize_age($age) : $self->age();
    }
    return 
        $self->{SPECIFIC_AGE}{$key}    || 
        $self->{SPECIFIC_AGE}{"$ns1-"} || 
        $self->{SPECIFIC_AGE}{"-$ns2"} || 
        $self->standardize_age($age) || $self->age();
}

sub set_specific_age {
    my $self = shift;
    my ($ns1, $ns2, $age) = @_;
    ($ns1, $ns2) = map { $self->namespace_token($_) } ($ns1,$ns2);
    my $key = "$ns1-$ns2";
    if (defined $age) {
        # Use negative numbers to clear the assignment
        my $sage = ($age =~ /^\-\d+$/) ? 
            undef : $self->standardize_age($age);
        $self->{SPECIFIC_AGE}{$key} = $sage;
    }
    return $self->{SPECIFIC_AGE}{$key};
}

*cloudage = \&cloud_age;
sub cloud_age {
    my $self = shift;
    if (defined $_[0]) {
        $self->{CLOUD_AGE} = ($_[0] =~ /^\-\d+$/) ? 
            undef : $self->standardize_age($_[0]);
    }
    return $self->{CLOUD_AGE};
}

=head2 minimum_age

 Title   : minimum_age
 Usage   : my $val = $ad->minimum_age( $newval )
 Function: Sets the minimum age to be used by age( )
 Returns : The current value
 Args    : Optional new value, a real positive number

In the age() method above it is mentioned that a zero age request will
automatically recalculate data. In order to avoid infinite looping in
database calls, the true SQL value of a 'zero age' is typically a
small, non-zero number (default 0.00002 days, about 2 seconds). This
method allows you to reset that value.

=cut

sub minimum_age {
    my $self = shift;
    if (defined $_[0]) {
        $self->{MINIMUM_AGE} = &_clean_age( $_[0] );
    }
    return $self->{MINIMUM_AGE};
}

=head2 standardize_age

 Title   : standardize_age
 Usage   : my $age = $ad->standardize_age( $ageRequest )
 Function: Make sure that ages meet minimum_age() requirements
 Returns : A real positive number
    Args : An age in days. Alternatively, a time unit can be added,
           such as "20 min", and the value will be converted to
           days. Recognized units are sec,min,hr,day,wk,mon,yr

This simple method is used to process any user-specified age to assure
that it is no smaller than the minimum_age().

=cut

sub standardize_age {
    my $self  = shift;
    # If undefined is passed, use the default age set above
    my $age   = &_clean_age( $_[0] );
    # Use the default age if nothing defined, or value was illegal:
    $age      = $self->{DEFAULT_AGE} unless (defined $age);
    # Do not use a zero value - this can cause recursion in some cases:
    $age      = $self->{MINIMUM_AGE} if (!$age || $age < $self->{MINIMUM_AGE});
    return $age;
}

sub _clean_age {
    my ($age, $isInFuture) = @_;
    return undef unless (defined $age);
    $age =~ s/^\s+//; $age =~ s/\s+$//;
    return $age if ($age =~ /^(\d+|\d*\.\d+)$/);
    if ($age =~ /^(\d+|\d*\.\d+)\s*([a-z]+)?$/i) {
        my $num  = $1;
        my $unit = lc($2 || "day");
        if ($unit =~ /sec/) {
            return $num / (60 * 60 * 24);
        } elsif ($unit =~ /min/) {
            return $num / (60 * 24);
        } elsif ($unit =~ /(hr|hour)/) {
            return $num / 24;
        } elsif ($unit =~ /day/) {
            return $num;
        } elsif ($unit =~ /(wk|week)/) {
            return $num * 7;
        } elsif ($unit =~ /mon/) {
            return $num * 30;
        } elsif ($unit =~ /(year|yr)/) {
            return $num * 365;
        }
    }
    
    # Maybe the user provided a fixed time point?
    $age = $safeAge if ($age =~ /safe/i);
    if (my $date = ParseDateString( $age )) {
        $age = (time -  UnixDate($date, '%s'))  / (60 * 60 * 24);
        $age *= -1 if ($isInFuture && $age < 0);
        return $age;
    } else {
        warn "Unknown date '$age'\n  ";
    }
    return ($age =~ /^(\d+|\d*\.\d+)$/) ? $age : undef;
}

=head2 standardize_id

 Title   : standardize_id
 Usage   : my ($id, $seq) = $ad->standardize_id( $idString, $ns, $classFlag )
 Function: Standardize user-supplied IDs
 Returns : A string and a BMS::MapTracker::Seqname object
 Args    : [0] A string representing the ID
           [1] A required namespace
           [2] Optional flag to check MapTracker class assignment

This important method performs several functions. First, it
de-versions sequence IDs (for example NM_001234.2 becomes
NM_001234).

Second, it tries to find that ID in the MapTracker database. If it
fails, only the user-supplied ID (missing any version number) is
reutrned (so $seq in the above usage example would be undef). If a
MapTracker entry was found, the capitalization of the ID is
standardized to match it (so nm_001234 becomes NM_001234).

Finally, if the user has set the third argument to a true value, the
MapTracker object is checked to make sure that it matches any
MapTracker classes associated with the namespace. If not, then only
the ID is returned.

If all criteria pass, both the ID and the MapTracker object are
returned. The presence of the MapTracker object in a return value can
be interpreted as "ID is ok". This call is used to make sure that
various conversion functions are not fed junk or irrelevant IDs, which
helps keep the database tidy.

=cut

my $slfObject;
*standardize_name = \&standardize_id;
sub standardize_id {
    my $self = shift;
    my $id   = shift;
    return () unless ($id);
    my $ns   = shift || "";
    my $checkClass = (shift) ? 1 : 0;
    #return $self->{CACHE}{STND_ID}{$id}{$ns}{$checkClass} if
    #    (defined $self->{CACHE}{STND_ID}{$id}{$ns}{$checkClass});

    # $self->err("standardizing $id [$ns] ($checkClass)");
    $self->benchstart;

    # Destroy leading and trailing whitespace:
    $id =~ s/^\s+//; $id =~ s/\s+$//;
    if ($id =~ /^[\+\-]?\d+$/ || $id =~ /^[\+\-]?\d*\.\d+$/) {
        # Do not allow numbers!
        $self->benchend;
        return ();
    }
    if (!$ns || $self->namespace_is_sequence($ns)) {
        # No namespace, or known sequence namespace
        # Do not allow version numbers on sequences
        $id = &strip_version( $id );
    }
    my $mt = $self->tracker;
    unless ($mt) {
        ### COMPLAIN HERE
        $self->benchend;
        return ($id);
    }
    my $seq;
    my $ntok = '';
    my $sfx  = "";
    if ($ns) {
        $ntok = $self->namespace_token($ns);
        if ($crossSpeciesNs->{$ntok} && $id =~ /^(\S+) \((.+)\)/) {
            my ($taxa, $tobj) = $self->standardize_taxa($2);
            if ($tobj) {
                $sfx = " ($taxa)";
                $id  = $1;
            } else {
                $self->bench_end;
                return $id;
            }
        }
        if ($ntok eq 'TAX') {
            $checkClass = 0;
            unless ($seq = $self->cached_mt_taxa($id)) {
                unless ($slfObject) {
                    # I don't want to import this library unless I really
                    # need it. Do so here.
                    require BMS::SequenceLibraryFinder;
                    $slfObject = BMS::SequenceLibraryFinder->new
                        ( -tracker => $mt );
                }
                if ($slfObject) {
                    my @guess = $slfObject->stnd_taxa( $id );
                    if ($#guess == 0) {
                        $seq   = $self->cached_mt_taxa($guess[0]);
                    }
                }
            }
        } elsif ($ntok eq 'BMSC') {
            if (my $nice = $self->sts->preferred_bms_id($id)) {
                $id  = $nice;
                $seq = $mt->get_seq( "#BMSID#$id" );
            }
        } elsif ($ntok eq 'SYM') {
            # This module sometimes slaps on tokens to symbols
            # Remove them:
            $id =~ s/[\~\*]+$//g;
            $seq = $self->_get_mt_seq_for_id_and_namespace( $id, $ntok );
        } else {
            if ($ntok eq 'AAD' || $ntok eq 'BAAD') {
                $id =~ s/-/_/g;
            } elsif ($ntok eq 'BAPS') {
                if ($id =~ /^\d+_at$/) {
                    # We need to normalize BrainArray to MapTracker
                    # tokenization
                    $id = "BrAr:LOC$id";
                }
            }
            if ($id =~ /^mtid:(\d+)$/i) {
                $seq = $mt->get_seq( $1 );
                if (($ntok eq 'APRB' || $ntok eq 'BAPB') && $seq) {
                    $id = $seq->name();
                }
            } else {
                $seq = $self->_get_mt_seq_for_id_and_namespace( $id, $ntok );
            }
        }
    } else {
        my @seqs = $mt->get_seq
            ( -name => $id, -nocreate => 1, -defined  => 1 );  
        if ($#seqs != -1) {
            my %lu = map { $_->name() => $_ } @seqs;
            if ($seq = $lu{$id}) {
                @seqs = ($seq);
            }
        }
        $seq = $seqs[0];
    }
    unless ($seq) {
        $self->benchend;
        return ($id.$sfx);
    }
    my ($gns) = $ntok eq 'AC' ? $self->guess_namespace($id, 'AC') : $ntok;
    if ($useMTIDformat->{$gns}) {
        $id = "MTID:".$seq->id;
    } else {
        $id = $seq->name;
    }

    if ($checkClass) {
        my @checks = $self->maptracker_classes( $ns );
        foreach my $check (@checks) {
            unless ($seq->is_class($check)) {
                $self->benchend;
                return ($id.$sfx);
            }
        }
    }
    $self->benchend;
    return ($id.$sfx, $seq);
}

sub _get_mt_seq_for_id_and_namespace {
    my $self = shift;
    my ($id, $ntok) = @_;
    my $mtns  = $self->maptracker_namespace( $ntok );
    my $nsid  = $mtns . $id;
    my @seqs  = $self->tracker->get_seq
        ( -name => $nsid, -nocreate => 1, -defined  => 1 );
    my $seq;
    # $self->err("Calling for ($id, $ntok = $nsid)", "IDs = ".join($authJoiner, map { $_->id } @seqs)) if ($id eq 'SKMLCK');
    if ($#seqs == 0) {
        $seq = $seqs[0];
    } elsif ($#seqs != -1 && !$mtns) {
        # This is an open namespace request (any namespace allowed)
        my %byNS = map { $_->namespace->name => $_ } @seqs;
        if ($ntok eq 'AC') {
            # Allow FreeText entries, if available
            $seq = $byNS{Smiles} || $byNS{FreeText} || $byNS{BMSID};
        }
    } elsif ($#seqs == -1 && $ntok eq 'AP') {
        # Unfortunately we have symbol-based proteins stored under
        # particular namespaces
        foreach my $mtns ('BmsSymbol', 'AmbitSymbol', 'ManningKinase') {
            my @seqs  = $self->tracker->get_seq
                ( -name => "#$mtns#$id", -nocreate => 1, -defined  => 1 );
            if ($#seqs == 0) {
                $seq = $seqs[0];
                # warn "$ntok request for $id yields ".$seq->id();
                last;
            }
        }
    }
    # $self->err("[$ntok] #$nsid#$id", map { $_->name } @seqs);
    return $seq;
}

sub integer_format {
    my $self = shift;
    my $nsTok = $self->namespace_token( shift );
    return exists $integerNamespaces->{$nsTok} ?
        $integerNamespaces->{$nsTok} : '';
}

sub allowed_integer_namespaces {
    return sort keys %{$integerNamespaces};
}

sub integer_namespaces {
    my %rv = %{$integerNamespaces};
    return wantarray ? %rv : \%rv;
}

sub namespace_regexps {
    my @rv = @{$guessRegExp};
    return wantarray ? @rv : \@rv;
}

my $mtSpaceId     = {};
my $mtClassId     = {};
my $mtNameCache   = [];
my $mtNameCount   = 0;
my $tempCaller    = {};
my $fastSensitive = { map { $_ => 1 } (keys %{$caseIsImportant}, 'SMI') };
my $fastStandSTHs = {};
sub fast_standardize {
    my $self = shift;
    # OPT: Not using tracker()
    my $mt   = $self->tracker();
    my ($id, $ns, $checkClass) = @_;
    return () unless ($id && $mt);
    $self->benchstart;
    # OPT: Not using namespace_token()
    my $ntok    = $ns ? $nsID->{ uc($ns) } || '' : '';
    my $sfx     = "";
    if ($crossSpeciesNs->{$ntok} && $id =~ /^(\S+) \((.+)\)/) {
        my ($taxa, $tobj) = $self->standardize_taxa($2);
        if ($tobj) {
            $sfx = " ($taxa)";
        } else {
            $self->bench_end;
            return ($id);
        }
        $id = $1;
    }
    my $sid     = 0;
    my $idKey   = uc($id);
    if ($ns) {
        unless (defined $mtSpaceId->{$ntok}) {
            if (my $mtns = $self->maptracker_namespace($ntok)) {
                $mtSpaceId->{$ntok} = $mt->get_namespace( $mtns )->id;
            } else {
                $mtSpaceId->{$ntok} = 0;
            }
        }
        $sid   = $mtSpaceId->{$ntok};
        $idKey = $id if ($fastSensitive->{$ntok});
    }
    my ($name, $seqid);
    if ( my $dat = $mtNameCache->[$sid]{$idKey} ) {
        ($name, $seqid) = @{$dat};
    } else {
        if ($id =~ /^mtid\:(\d+)$/i) {
            # Explicit ID request
            $seqid        = $1;
            my $sth       = $fastStandSTHs->{"Fast name from ID"} ||=
                $mt->dbi->named_sth("Fast name from ID");
            ($name)       = $sth->get_array_for_field($seqid);
       } else {
            # String request
            my $sqlName = "Fast name standardization";
            my @binds   = ($idKey);
            if ($sid) {
                push @binds, $sid;
                if ($fastSensitive->{$ntok}) {
                    $sqlName .= " with case-sensitive namespace";
                    push @binds, $idKey;
                } else {
                    $sqlName .= " with namespace";
                }
            }
            my $sth       = $fastStandSTHs->{$sqlName} ||=
                $mt->dbi->named_sth($sqlName);
            # $sth->pretty_print( @binds);
            $sth->execute( @binds );
            my $rows   = $sth->fetchall_arrayref;
            if ($#{$rows} == 0) {
                ($name, $seqid) = @{$rows->[0]};
            } elsif ($#{$rows} != -1 && $ntok) {
                # Ick. Mutliple namespaces
                my $seq = $self->_get_mt_seq_for_id_and_namespace($id, $ntok);
                ($name, $seqid) = ($seq->name, $seq->id) if ($seq);
            }
        }
        if (++$mtNameCount > 10000) {
            $mtNameCount = 1;
            $mtNameCache = [];
        }
        if ($useMTIDformat->{$ntok}) {
            $name = $seqid ? "MTID:$seqid" : '';
        } elsif ($ntok eq 'AC') {

            # NEEDS WORK - May miss SEQ entries

            $name = "MTID:$seqid"
                if ($seqid && $self->fast_class($seqid, 'SMILES'));
        }
        $mtNameCache->[$sid]{$idKey} = [ $name, $seqid ];
    }

    if (!$seqid) {
        $name = $id;
    } elsif ($checkClass) {
        if (my $classes = $mtClasses->{$ntok}) {
            unless ($self->fast_full_class( $seqid, @{$classes} ) ) {
                my $aok = $mtAlsoOkClasses->{$ntok};
                if ($aok && $self->fast_class( $seqid, @{$aok} )) {
                    # There are additional classes that will allow the
                    # sequence to be considered valid, and this entry
                    # appears to match
                } else {
                    # The entry has failed the class filters
                    $self->benchend;
                    return ($name.$sfx);
                }
            }
        }
    }
    $self->benchend;
    return ($name.$sfx, $seqid);
}

my $fastClassSTH;
sub fast_class {
    my $self    = shift;
    $self->benchstart;
    my $seqid   = shift;
    $fastClassSTH ||= $self->tracker->dbi->named_sth("Fast class check");
    my @cids        = $fastClassSTH->get_array_for_field( $seqid );
    my $found       = 0;
    foreach my $class (@_) {
        my $all =  $mtClassId->{$class} ||= { 
            map { $_->id => 1 } 
            $self->tracker->get_class($class)->me_and_the_kids()
            };
        map { $found += $all->{$_} || 0 } @cids;
    }
    $self->benchend;
    return $found;
}

sub fast_full_class {
    my $self    = shift;
    $self->benchstart;
    my $seqid   = shift;
    $fastClassSTH ||= $self->tracker->dbi->named_sth("Fast class check");
    my @cids        = $fastClassSTH->get_array_for_field( $seqid );
    foreach my $class (@_) {
        my $found       = 0;
        # warn "'$class'\n";
        #warn $self->tracker()->get_seq('NM_001234');
        my $all         = $mtClassId->{$class} ||= { 
            map { $_->id => 1 } 
            $self->tracker->get_class($class)->me_and_the_kids()
            };
        map { $found += $all->{$_} || 0 } @cids;
        unless ($found) {
            $self->benchend;
            return 0;
        }
    }
    $self->benchend;
    return 1;
}

our $classNsMatch = {};
sub class_matches_namespace {
    my $self = shift;
    my $name = shift;
    return 0 unless ($name);
    my $ns   = shift || ""; # should already be a token!
    if ($ns =~ /^A[PRL]$/) {
        my $cnm = $classNsMatch->{nsOkClasses}{$ns};
        unless ($cnm) {
            $cnm = $classNsMatch->{nsOkClasses}{$ns} = [];
            my @mtCls = $self->maptracker_classes( $ns );
            push @{$cnm}, \@mtCls unless ($#mtCls == -1);
            push @{$cnm}, [ 'cDNA' ] if ($ns eq 'AR');
        }
        return 1 if ($#{$cnm} == -1);
        my ($stnd, $mtid) = $self->fast_standardize($name, $ns);
        return 0 unless ($mtid);
        foreach my $cc (@{$cnm}) {
            return 1 if ($self->fast_full_class( $mtid, @{$cc} ));
        }
    } else {
        return 1 if ($self->verify_namespace($name, $ns));
    }
    return 0;
}

sub strip_version {
    my ($id) = @_;
    if ($id =~ /\{[^\}]+\}$/) {
        # Do not modify explict sequence variants eg:
        # NP_000213.1{V559D,T670I}
    } elsif ($id =~ /(.+)\.\d+$/) { 
        $id = $1;
    } elsif ($id =~ /^([OPQ]\d[A-Z\d]{3}\d|[A-NR-Z]\d[A-Z][A-Z\d]{2}\d)\-\d+(\d\d+)?$/) {
        # Swiss-Prot variants, eg P28908-1
        $id = $1;
    }
    return $id;
}

sub is_number {
    my $self = shift;
    my $val = shift;
    return ($val =~ /^\s*[\-\+]?\s*(\d+|\d+\.\d+)\s*$/) ? 1 : 0;
}

=head2 standardize_taxa

 Title   : standardize_taxa
 Usage   : my ($sciName, $obj) = $ad->standardize_taxa( $request )
 Function: Standardize all taxa requests to scientific names
 Returns : A string
 Args    : A string defining the species

The method uses MapTracker to parse species requests, which can be any
form of known species text or identifier ('homo sapiens', 'human',
9606 etc). If it fails to find a unique entry in MapTracker, it will
return only the original query string.

Otherwise, it will return the taxa name (with standard capitalization)
and the BMS::MapTracker::Taxa object related to that species.

=cut

my $taxaOverRide = {
    'Xenopus (Silurana) tropicalis' => 'Xenopus tropicalis',
};

sub cached_mt_taxa {
    my $self = shift;
    my $taxa = shift || "";
    unless ($self->{MT_TAX_CACHE}{$taxa}) {
        # warn "Getting taxa for '$taxa'";
        $self->bench_start();
        my $mt     = $self->tracker;
        my @taxae  = $mt ? $mt->get_taxa($taxa) : ();
        if (++$self->{MT_TAX_CACHE_SIZE} > 1000) {
            # Clear the cache if it is too large
            $self->{MT_TAX_CACHE_SIZE} = 1;
            delete $self->{MT_TAX_CACHE};
        }
        $self->{MT_TAX_CACHE}{$taxa} = \@taxae;
        $self->bench_end();
    }
    my @rv = @{$self->{MT_TAX_CACHE}{$taxa}};
    return wantarray ? @rv : $#rv == 0 ? $rv[0] : $#rv == -1 ? 0 : "";
}

sub standardize_taxa {
    my $self = shift;
    my ($taxa) = @_;
    return () unless ($taxa);
    if (my $obj = $self->cached_mt_taxa( $taxa )) {
        my $name = $obj->name;
        return ($taxaOverRide->{$name} || $name, $obj);
    }
    return ($taxa);
}

=head2 deep_dive

 Title   : deep_dive
 Usage   : my $val = $ad->deep_dive( $newVal )
 Function: Gets / sets the deep dive flag
 Returns : The current value (0 or 1)
 Args    : Optional new value

Some convert() calls utilize logic that can consult the SeqStore
database for information if they fail to find it in MapTracker. This
will be done only if deep dive is set to a true value. Utilizing this
feature can increase processing time, and adds an additional failure
point (If SeqStore is down).

Deep dive will trigger updates using update_maptracker_sequence().

=cut

sub deep_dive {
    my $self = shift;
    if (defined $_[0]) {
        $self->{DEEPDIVE} = $_[0] ? 1 : 0;
    }
    return $self->{DEEPDIVE};
}

=head2 update_maptracker_sequence

 Title   : update_maptracker_sequence
 Usage   : $ad->update_maptracker_sequence( $sequenceID )
 Function: Refreshes MapTracker metadata using information from SeqStore
 Returns : The number of sequences updated
 Args    : A single ID or array reference of IDs.

This method is of primary use to internal methods, and can be called
when deep_dive() is true. It is typically used to get new metadata
from SeqStore (via the pbg() method) when no results are found for method calls on a sequence
ID.

=cut

sub update_maptracker_sequence {
    my $self = shift;
    my ($req) = @_;
    my @accs  = ref($req) ? @{$req} : ($req);
    return 0 if ($#accs < 0);
    my $pbg    = $self->pbg;
    my $lh     = $pbg->load_helper();
    my @tasked = $pbg->add_by_accession( @accs );
    return 0 if ($#tasked < 0);
    my $written = $pbg->analyze();
    return 0 unless ($written);
    $lh->write();
    while (1) {
        $lh->process_ready();
        my @locked = $lh->each_locked_task();
        last if ($#locked < 0);
        sleep(10);
    }
    return $written;
}

=head2 pbg

 Title   : pbg
 Usage   : my $pbg = $ad->pbg( )
 Function: Gets a PopulateByGenbank genbank object
 Returns : A BMS::MapTracker::PopulateByGenbank object
 Args    : 

This object is of primary use to internal methods, particularly
update_maptracker_sequence(). The object will fetch sequence metadata
from SeqStore when deep_dive() is true.

=cut

sub pbg {
    my $self = shift;
    unless ($self->{PBG}) {
        my $pbg = BMS::MapTracker::PopulateByGenbank->new
            ( -tracker  => $self->tracker(),
              -carpfile => ">>/work5/tilfordc/maptracker/".
              "PopulateByGenbankCarpings.txt",
              -testmode => 0 );
        $pbg->load_helper->directory('/work5/tilfordc/accessDenorm');
        $pbg->ignore_prior( 1 );
        $self->{PBG} = $pbg;
    }
    return $self->{PBG};
}

=head2 dbh

 Title   : dbh
 Usage   : my $dbh = $ad->dbh( )
 Function: Gets the GenAcc database handle
 Returns : A DBI database object
 Args    : 

Returns the DBI object for the Oracle GenAcc instance. If you want a
handle to the MapTracker Postgres database, get the MapTracker object by
calling tracker(), and call dbi( ) from it.

=cut

*dbi = \&dbh;
sub dbh {
    return shift->{DBH};
}

sub fork_safe   { 
    my $self = shift;
    $self->dbh->fork_safe( @_ );
    $fastStandSTHs = {};
    if (my $mt = $self->tracker) {
        $mt->fork_safe( @_ );
    }
}
sub fork_unsafe   { 
    my $self = shift;
    $self->dbh->fork_unsafe( @_ );
    if (my $mt = $self->tracker) {
        $mt->fork_unsafe( @_ );
    }
}

=head1 NAMESPACE METHODS

Low-level methods that provide information about the namespaces used
by AccessDenorm.

=head2 namespace_name

 Title   : namespace_name
 Usage   : my $nsName = $ad->namespace_name( $string )
 Function: Get a human-readable name for a namespace
 Returns : A string
 Args    : A namespace identifier

Takes any namespace identifier (either a name or token, cases does not
matter) and returns the name for that namespace. The returned format
(including case) will match that used by namespace columns in the
database. See also namespace_token() below.

=cut

*ns_name = \&namespace_name;
sub namespace_name {
    my ($self, $id) = @_;
    return $nsName->{ uc($id || '') } || '';
}

=head2 namespace_token

 Title   : namespace_token
 Usage   : my $nsToken = $ad->namespace_token( $string )
 Function: Get a short token for a namespace
 Returns : A string
 Args    : A namespace identifier

Takes any namespace identifier (either a name or token, cases does not
matter) and returns the token for that namespace. Tokens are not
displayed in the database, but are often more convienent to use when
providing namespaces than the full name. See also namespace_name() above.

=cut

*ns_token = \&namespace_token;
sub namespace_token {
    my ($self, $id) = @_;
    return $nsID->{ uc($id || '') } || '';
}

=head2 namespace_is_sequence

 Title   : namespace_is_sequence
 Usage   : my $bool = $ad->namespace_is_sequence( $namespace )
 Function: Determines if a namespace represents sequence objects
 Returns : 1 or 0
 Args    : A GenAcc namespace identifier

If the passed namespace is used to represent sequences, will return 1.

=cut

sub namespace_is_sequence {
    my ($self, $ns) = @_;
    my $tok = $self->namespace_token($ns);
    return $sequenceNs->{$tok} || 0;
}

=head2 namespace_is_case_sensitive

 Title   : namespace_is_case_sensitive
 Usage   : my $bool = $ad->namespace_is_case_sensitive( $namespace )
 Function: Indicates if the identifiers in a namespace are case sensitive
 Returns : 1 or 0
 Args    : A GenAcc namespace identifier

Most GenAcc namespaces are case-insensitive; the program will always
try to standardize recorded capitalization to that found in
MapTracker, but queries are case-insensitive. A few (such as Gene
Symbols) are case sensitive, however, and queries will be treated as
such. This method indicates the case sensitivity of a GenAcc
namespace.

=cut

my $nsIsSen = {};
my $nsIsSenCacheLimit = 0;
sub namespace_is_case_sensitive {
    my ($self, $ns) = @_;
    unless (defined $nsIsSen->{$ns}) {
        $nsIsSen = {} unless (++$nsIsSenCacheLimit % 100);
        my $tok  = $self->namespace_token($ns);
        $nsIsSen->{$ns} = $caseIsImportant->{$tok} || 0;
    }
    return $nsIsSen->{$ns};
}

=head2 namespace_parents

 Title   : namespace_parents
 Usage   : my @tokens = $ad->namespace_parents( $namespace )
 Function: Get all namespaces that are parents of this one
 Returns : An array of namespace tokens
 Args    : A GenAcc namespace identifier

Some namespaces are children of a more generic namespace. For example,
'RefSeq RNA' is a child of 'Any RNA'. This method will return an array
of namespace tokens. The query namespace itself will always be
included. In addition, any parents will be included as well.

if you want names instead of tokens, you can call:

  my @names = map { $ad->namespace_name( $_ ) } $ad->namespace_parents( $ns );

=cut

sub namespace_parents {
    my $self = shift;
    # Standardize to tokens
    my %tokens;
    foreach my $req (@_) {
        my $tok = $self->namespace_token($req);
        $tokens{$tok} = 0 if ($tok);
    }
    my @toks   = keys %tokens;
    # Add parents to token hash
    foreach my $tok (@toks) {
        if (my $pars = $nsParents->{ $tok }) {
            # Recursively find grandparents, if any
            my @recurse; map {push @recurse, $_ unless ($tokens{$_})} @{$pars};
            my $parHash = $self->namespace_parents( @recurse );
            while ( my ($par, $pardist) = each %{$parHash}) {
                $tokens{$par} = $pardist + 1 unless (defined $tokens{$par});
            }
        }
    }
    return wantarray ? 
        sort { $tokens{$a} <=> $tokens{$b} } keys %tokens : \%tokens
}

sub namespace_root {
    my $self = shift;
    my $ns   = $self->namespace_token( $_[0] );
    while (my $pars = $nsParents->{ $ns }) {
        $ns = $pars->[0];
    }
    return $ns;
}

sub namespace_children {
    my $self = shift;
    # Standardize to tokens
    my %tokens;
    my @stack = @_;
    while (my $req = shift @stack) {
        my $tok = $self->namespace_token($req);
        if ($tok) {
            $tokens{$tok} = 1;
            map { push @stack, $_ unless (exists $tokens{$_}) }
            @{$nsChildren->{ $tok } || []};
        }
    }
    return keys %tokens;
}

=head2 is_namespace

 Title   : is_namespace
 Usage   : my $bool = $ad->is_namespace( $ns1, $ns2 )
 Function: Determines if the first namespace ISA the second
 Returns : 0 if false, 1 (or more) if true
 Args    : [0] The query namespace
           [1] The subject (comparison) namespace

Arguments can be either tokens or names. Use this method to test if a
namespace 'ISA' a more generic one. For example, you might want to
test if a namespace is an RNA. To do so, you would:

    if ($ad->is_namespace( $unknownNamespace, 'AR'))

So RSR (RefSeq RNA) would return a true value when tested against AR
(Any RNA), but RSP (RefSeq Protein) would return 0.

=cut

sub is_namespace {
    my $self   = shift;
    my $query  = $self->namespace_token( shift );
    my $valid  = $self->valid_namespace_hash( @_ );
    return $valid->{$query} || 0;
}

# Altered 26 May 2010 to include a cache
# This may be a dumb idea, but the hope is to limit calls to ns_children
our $valid_ns_cache = {};
sub valid_namespace_hash {
    my $self = shift;
    my %reqH = map { $self->namespace_token($_) => 1 } @_;
    delete $reqH{""};
    my @reqs = sort keys %reqH;
    my $key  = join("\t", @reqs);
    my $rv;
    unless ($rv = $valid_ns_cache->{$key}) {
        $rv = $valid_ns_cache->{$key} = {};
        foreach my $tok (@reqs) {
            map { $rv->{$_} ||= $tok } $self->namespace_children($tok);
        }
    }
    # Always de-reference to prevent accidental damage to cache values:
    return wantarray ? %{$rv} : { %{$rv} };
}

=head2 is_canonical

 Title   : is_canonical
 Usage   : my $bool = $ad->is_canonical( $ns1, $ns2 )
 Function: Determines if a pair of namespaces are canonical
 Returns : 0 if false, 1 if true
 Args    : A pair of namespaces

Arguments can be either tokens or names. If the pair are canonical to
one-another, will return 1. Canonical namespaces are assumed to be
directly related by fiat, that is, there is no question that they are
linked. For example, RefSeq RNA is considered canonical to both
LocusLink Gene and RefSeq Protein. On the other hand, RefSeq RNA is
not related to Ensembl Protein by canon; that relationship may be
calculated, but it will involve a similarity comparison.

=cut

sub is_canonical {
    my $self = shift;
    my ($ns1, $ns2) = map { $self->namespace_token($_) } @_;
    return $canonHash->{$ns1}{$ns2} || 0;
}

=head2 all_namespace_tokens

 Title   : all_namespace_tokens
 Usage   : my @tokens = $ad->all_namespace_tokens( )
 Function: Gets a list of all namespaces as tokens
 Returns : An array of namespace tokens.
 Args    : 

=cut

sub all_namespace_tokens {
    my $self = shift;
    unless ($self->{ALL_TOKENS}) {
        my %nonredun = map { $self->namespace_token($_) => 1 } keys %{$nsName};
        $self->{ALL_TOKENS} = [ sort keys %nonredun ];
    }
    return @{$self->{ALL_TOKENS}};
}

=head2 all_namespace_names

 Title   : all_namespace_names
 Usage   : my @names = $ad->all_namespace_names(  )
 Function: Gets a list of all namespaces as names
 Returns : An array of namespace names.
 Args    : 

=cut

sub all_namespace_names {
    my $self = shift;
    unless ($self->{ALL_NAMES}) {
        my %nonredun = map { $self->namespace_name($_) => 1 } keys %{$nsName};
        $self->{ALL_NAMES} = [ sort keys %nonredun ];
    }
    return @{$self->{ALL_NAMES}};
}

=head2 guess_namespace

 Title   : guess_namespace
 Usage   : $ad->guess_namespace( $id, $ns )
 Function: Given an ID, guess the namespace it belongs to
 Returns : A namespace token
 Args    : [0] The ID (a string)
           [1] Optional known namespace
           [2] Flag to also check class

This method will use regular expressions to try to figure out what
sort of identifier an ID is. If it fails, the empty string will be
returned.

If you provide a known namespace, an array of tokens will be
returned. If guessing fails, the array will only contain the known
namespace. Otherwise, it will contain the guessed namespace at the
front. This is useful if you know an ID is a protein, and you want the
program to try to see if something more specific is known.

=cut

my @mtNsGuess = (["#GeneSymbols#", "SYM"],
                 ["?#SMILES#",     'SMI']);
sub guess_namespace {
    my $self = shift;
    my ($id, $nsKnown, $checkClass) = @_;
    # $self->err("Guessing namespace for $id");
    $nsKnown    = $self->namespace_token($nsKnown) if ($nsKnown);
    $id       ||= '';
    if (ref($id)) {
        $self->err("guess_namespace() called with reference $id");
        return $nsKnown ? ($nsKnown) : '';
    }
    $self->benchstart;
    my $nsGuess = '';
    if ($nsKnown && $mtNamespaces->{$nsKnown}) {
        # If the AD namespace also corresponds to a specific MT namespace,
        # do a fast check; if it matches, no more guessing and keep as-is
        my ($rid, $seq) = $self->fast_standardize( $id, $nsKnown );
        if ($seq) {
            ($rid, $seq) = $self->fast_standardize( $id, $nsKnown, 1 )
                if ($checkClass);
            if ($seq) {
                # An element was succesfully recovered with the maptracker
                # namespace, so accept the known namespace as-is
                $self->benchend;
                return ($nsKnown,$nsKnown);
            }
        }
    }
    if ($id =~ /MTID\:(\d+)$/i) {
        # A specific MapTracker entity has been provided
        if (my $mt = $self->tracker) {
            if (my $seq = $mt->get_seq($1)) {
                # Confirmed that the sequence exists ...
                if (my $tok = $mtNs2adNs->{ uc($seq->namespace->name) } ) {
                    # ... and that it has the appropriate namespace
                    $self->bench_end;
                    return $nsKnown ? ($tok, $nsKnown) : $tok;
                }
            }
        }
        $self->bench_end;
        return $nsKnown ? ($nsKnown) : '';
    }
    
    foreach my $dat (@{$guessRegExp}) {
        my ($tok, $re) = @{$dat};
        next unless ($id =~ /^$re$/i);
        # This regular expression matches the ID

        # ADDED 8 Jan 2009: ARE WE SURE ABOUT THIS??
        next if ($nsKnown && !$self->is_namespace($tok, $nsKnown));
        
        if ( $checkClass) {
            # Make sure that MapTracker classes also match
            $id = $self->sts->preferred_bms_id($id) if ($tok eq 'BMSC');
            my ($rid, $seq) = $self->fast_standardize( $id, $tok, 1);
            next unless ($seq); # Keep checking if the class checks fail
        }
        $nsGuess = $tok;
        last;
    }

    unless ($nsGuess) {
        if ( $self->namespace_name( $id ) ) {
            # Namespace
            $nsGuess = 'NS';
        } elsif ( $id =~ /^([A-Z][a-z]+ [a-z]+) (.+)/ &&
                  $self->namespace_name( $2 ) ) {
            # This is a species specific namespace?
            my ($taxa, $ns) = ($1, $self->namespace_token( $2 ));
            if ($ns =~ /^(RSR|RSP|LL)$/) {
                $nsGuess = 'SET';
            }
        } elsif (my $mt = $self->tracker) {
            # Maybe we can match this object to a MapTracker namespace...



            # NEEDS WORK
            # There are accessions that are also symbols. For example:
            # AK162044
            # We need to see if the #None# namespace has an object,
            # and if so if it is an RNA (AR) or protein (AP)



            for my $mg (0..$#mtNsGuess) {
                my ($mtNs, $adNs) = @{$mtNsGuess[$mg]};

                my $seq = $mt->get_seq
                    ( -name => $mtNs.$id, -nocreate => 1, -defined => 1 );
                if ($seq) {
                    $nsGuess = $adNs;
                    last;
                }
            }
        }
    }
    if ($nsKnown) {
        # If the user already knows a namespace, and is hoping to guess
        # additional ones, then return an array:
        my $knNS = $self->namespace_token($nsKnown);
        # Reset the guessed namespace to one more specific if possible
        $nsGuess = $knNS if ($confoundedGuess->{$nsGuess}{$knNS});
        $self->benchend;
        return $nsGuess ? ( $nsGuess, $knNS ) : ( $knNS );
    }
    # Otherwise just return the guess:
    $self->benchend;
    return $nsGuess;
}

sub pick_namespace {
    my $self = shift;
    my ($id, $nsReq, $chkCls) = @_;
    my ($stdId, $stdNs) = ($id);
    if ($nsReq) {
        # One or more possible namespaces are provided
        my @nss = sort { 
            ($reliableGuess->{$b} || 0) <=> 
                ($reliableGuess->{$a} || 0) ||
                $a cmp $b } map {
                    $self->namespace_token($_)
                    } ( ref($nsReq) ? @{$nsReq} : ($nsReq));
        foreach my $n (@nss) {
            my ($nm, $seq) = $self->standardize_id($id, $n, $chkCls);
            # warn "($id, $n, $chkCls) => ($nm, $seq)";
            if ($seq) {
                ($stdId, $stdNs) = ($nm, $n);
                last;
            }
        }
    }
    unless ($stdNs) {
        # Either no candidate namespaces provided, or no matches found
        $stdNs = $self->guess_namespace($id) 
            ||   $self->guess_namespace_from_db($id, 50) || '';
        
    }
    # Strip off version numbers where appropriate
    $stdId = &strip_version( $stdId ) if ($sequenceNs->{$stdNs});
    # warn "$id [$nsReq] {$chkCls} ==> $stdId [$stdNs]";
    return wantarray ? ($stdNs, $stdId) : $stdNs;
}

sub extract_ids {
    my $self = shift;
    my ($req, $nsReq, $regExp) = @_;
    my $list = !$req ? [] : ref($req) ? $req : [ $req ];
    my $nsOnly;
    if ($nsReq) {
        my @nss = ref($nsReq) ? @{$nsReq} : ( $nsReq );
        my @tok = map { $self->namespace_token($_) } @nss;
        if ($#tok == -1) {
            $self->err("extract_ids() called with namespace filter, but none of the requested namespaces are recognized. This will prevent any IDs from being extracted", map { "'$_'" } @nss);
        }
        $nsOnly = { map { $_ => 1 } @tok };
    }
    my %rv;

    # Rather than just remove the ID, we replace it with a token that
    # hopefully will prevent un-related flanking sequences from
    # joining into something that gets recognized
    my $repTok = '+++';
    if ($regExp) {
        # The user is providing a regular expression to use for hunting
        $regExp = "($regExp)" unless ($regExp =~ /\([^\)]+\)/);
        my %found;
        foreach my $txt (@{$list}) {
            while ($txt =~ /($regExp)/) {
                my ($rem, $id) = ($1, $2);
                $txt =~ s/\Q$rem\E/$repTok/;
                $found{$id} = 1;
            }
        }
        my @fndId = keys %found;
        my @nss   = keys %{$nsOnly || {}};
        # warn "Extracted ".join('+',@fndId)." from '".join(',',@{$list})."' using /$regExp/ via ".join("|", @nss);
        if ($#nss == -1) {
            foreach my $idReq (@fndId) {
                if ($idReq =~ /^\d+$/) {
                    $rv{$idReq} = "";
                } elsif (my $ns = $self->guess_namespace($idReq)) {
                    my ($id, $seq) = $self->standardize_id( $idReq, $ns );
                    $rv{$id} = $ns if ($seq);
                }
            }
        } else {
            my %intFormats;
            foreach my $ns (@nss) {
                if (my $ifrm = $self->integer_format( $ns )) {
                    $intFormats{$ns} = $ifrm;
                }
            }
            my @okIntFormats = keys %intFormats;
            my ($intNS, $intFrm);
            if ($#okIntFormats == 0) {
                # Single integer namespace
                $intNS  = $okIntFormats[0];
                $intFrm = $intFormats{ $intNS };
            } elsif ($#okIntFormats > 0) {
                # Multiple integer namespaces, capture integers with
                # unknown namespace
                ($intNS, $intFrm) = ("", "%d");
            }
            # warn "Found ".join("+", @fndId). " via ".join("|", @nss);
            foreach my $idReq (@fndId) {
                if ($idReq =~ /^\d+/) {
                    if ($intFrm) {
                        my $id   = sprintf($intFrm, $idReq);
                        $rv{$id} = $intNS;
                    }
                } else {
                    foreach my $ns (@nss) {
                        my ($id, $seq) = $self->standardize_id( $idReq, $ns );
                        if ($seq) {
                            $rv{$id} = $ns;
                            last;
                        }
                    }
                }
            }
        }
    } else {
        my @guesses = @{$guessRegExp};
        foreach my $txt (@{$list}) {
            next unless ($txt);
            foreach my $dat (@guesses) {
                my ($ns, $re) = @{$dat};
                next unless ($reliableGuess->{$ns});
                next if ($nsOnly && !$nsOnly->{$ns});
                # 8 Aug 2013 - including \<\> flanks in regexp
                # The issue here is that some REs are very simple, eg for SYM
                # Not enforcing some sort of word boundary can cause spurious
                # substring matches
#                while ($txt =~ /\<($re)\>/i) {
                # 2 May 2014. Eh. It did cause problems... use \b instead
                while ($txt =~ /\b($re)\b/i) {
                    my $id = $1;
                    $txt =~ s/\Q$id\E/$repTok/;
                    # ADD 30 Apr 2010 - Might cause problems??
                    # Needed because versions are themselves causing trouble
                    # Remove versioning if it is relevant and found:
                    if ($sequenceNs->{$ns} && $id =~ /(.+)\.\d+$/) {
                        $id = $1;
                    }
                    $rv{$id} = $ns;
                }
                last if ($txt =~ /^\++$/); # CHANGE IF $repTok is changed
            }
        }
    }
    return wantarray ? keys %rv : \%rv;
}

sub filter_list_by_namespace {
    my $self = shift;
    my ($ids, $ns, $checkClass, $beFast) = @_;
    $ns    = $self->namespace_token($ns);
    return $ids unless ($ns);
    $self->benchstart;
    my @rv;
    if ($mtNamespaces->{$ns}) {
        # If the AD namespace also corresponds to a specific MT namespace,
        # do a fast check; if it matches, no more guessing and keep as-is
        my @unk;
        foreach my $id (@{$ids}) {
            my ($rid, $seq) = $self->fast_standardize( $id, $ns );
            unless ($seq) {
                #$self->err("$id is not [$ns]");
                push @unk, $id;
                next;
            }
            if ($checkClass) {
                ($rid, $seq) = $self->fast_standardize( $id, $ns, 1 );
                unless ($seq) {
                    #$self->err("$id is failed class check");
                    push @unk, $id;
                    next;
                }
            }
            push @rv, $id;
        }
        $ids = \@unk;
    } elsif ($ns eq 'NS') {
        my %nst;
        foreach my $id (@{$ids}) {
            push @rv, $id if ($nst{$id} ||= $self->namespace_token($id));
        }
        $ids = [];
    } elsif ($ns eq 'TAX') {
        my $mt = $self->tracker();
        foreach my $id (@{$ids}) {
            # Recovering taxae can be slow for large lists
            # If 'fast' has been requested, ignore entries without spaces:
            next if ($beFast && $id !~ / /);
            my @tax = $self->cached_mt_taxa( $id );
            push @rv, $id unless ($#tax == -1);
        }
        $ids = [];
    }
    my @REs;
    unless ($#{$ids} == -1) {
        my $valid = $self->valid_namespace_hash( $ns );
        for my $d (0..$#{$guessRegExp}) {
            my ($tok, $re) = @{$guessRegExp->[$d]};
            if ($valid->{$tok}) {
                push @REs, $re;
            }
        }
    }

    if ($#REs == -1) {
        # There are no regular expressions for this namespace
        @rv = @{$ids} unless ($mtNamespaces->{$ns});
    } else {
        my @unk;
        foreach my $id (@{$ids}) {
            my $found = 0;
            for my $r (0..$#REs) {
                if ($id =~ /^$REs[$r]$/i) {
                    push @rv, $id;
                    $found = 1;
                    last;
                }
            }
            push @unk, $id unless ($found);
        }
        $ids = \@unk;
    }
    $self->bench_end;
    return \@rv;
}

sub guess_namespace_careful {
    my $self = shift;
    my ($n, $ns) = @_;
    my ($gns, $kns) = $self->guess_namespace($n, $ns,'checkclass');
    # warn "GUESS NS: $n [Hint:".($kns||"none")."] = $gns\n";
    return $gns if ($reliableGuess->{$gns});
    if (my $mt = $self->tracker) {
        my $doubleCheck = 0;
        my @seqs = $mt->get_seq( -name => $n, -nocreate => 1, -defined => 1 );
        my ($mtseq, $mtns);
        if ($#seqs == 0) {
            # There is a unique entry in MapTracker for this name
            # across all namespaces
            $mtseq  = $seqs[0];
            $mtns   = $mtseq->namespace->name();
        } elsif ($mtns = $self->maptracker_namespace( $ns )) {
            # We were able to get a specific MapTracker namespace from the
            # suggested AD namespace
            @seqs   = $mt->get_seq
                ( -name => $mtns . $n, -nocreate => 1, -defined => 1 );
            $mtseq = $seqs[0] if ($#seqs == 0);
        }
        # warn "$n [$ns] $gns = $mtns\n";
        if (my $mns = $mtNs2adNs->{uc($mtns || "")}) {
            # This namespace maps to a specific AD namespace
            return $mns;
        }
        $gns ||= '';
        if ($kns && $kns eq $gns) {
            # The guessed namespace is still the known namespace
            unless ($mtNamespaces->{$kns}) {
                $doubleCheck = 1;
            }
        }
        if ($gns eq 'UP') {
            if ($mtseq) {
                # See if we can refine UniProt a bit:
                if ($mtseq->is_class('Swiss-Prot')) {
                    $gns = 'SP';
                } elsif ($mtseq->is_class('TrEMBL')) {
                    $gns = 'TR';
                }
            }
        } elsif (!$gns || $doubleCheck) {
            my $bkup = $gns;
            foreach my $seq (@seqs) {
                if ($seq->is_class('Swiss-Prot')) {
                    $gns = 'SP';
                } elsif ($seq->is_class('TrEMBL')) {
                    $gns = 'TR';
                } elsif ($seq->is_class('genesymbol')) {
                    $gns = 'SYM';
                } elsif ($seq->is_class('protein')) {
                    $gns = 'AP';
                } elsif ($seq->is_class('rna','cdna')) {
                    $gns = 'AR';
                } elsif ($seq->is_class('locus')) {
                    $gns = 'AL';
                }
                last if ($gns);
            }
            $gns ||= $bkup || 'UNK';
            if ($kns && $self->is_namespace($kns, $gns)) {
                # The known namespace is more specific that the guessed one
                $gns = $kns;
            }
        }
    } elsif (!$gns && $ns) {
        $gns = $self->namespace_token($ns);
    }
    return $gns;
}

sub guess_namespace_very_careful {
    my $self = shift;
    my ($obj, $ns) = @_;
    return undef unless ($obj);
    if (ref($obj)) {
        # A MapTracker::Seqname object; see if the namespace is informative
        my $gns = $mtNs2adNs->{ uc($obj->namespace->name) };
        return $gns if ($gns);
        $obj = $obj->name;
    }
    return $self->guess_namespace_careful($obj, $ns);
}

sub guess_namespace_parents {
    my $self = shift;
    my ($id, $ns) = @_;
    my @rv = map { $self->namespace_name($_) } 
    $self->namespace_parents( $self->guess_namespace( $id, $ns));
    return @rv;
}

our @notGoodNsGuess = qw(RS ORTH BIO CHEM);
our @notGoodNsNameGuess = map { &namespace_name(undef, $_) } @notGoodNsGuess;
sub guess_namespace_from_db {
    my $self = shift;
    my ($id, $count) = @_;
    return wantarray ? () : "" unless ($id);
    $count ||= 250;
    my $uns          = "(".join(', ', map { "'$_'" } @notGoodNsNameGuess).")";
    my $sthIn = $self->dbh->prepare
        (-sql => "SELECT ns_in FROM conversion WHERE ".
         "upper(term_in) = upper(?) AND ns_in NOT IN $uns",
         -limit => $count);
    my $sthOut = $self->dbh->prepare
        (-sql => "SELECT ns_out FROM conversion WHERE ".
         "upper(term_out) = upper(?) AND ns_out NOT IN $uns",
         -limit => $count);
    
    my @ns  = $sthIn->get_array_for_field( $id );
    push @ns, $sthOut->get_array_for_field( $id );
    my %hash;
    map { $hash{$_}++ } @ns;
    my @nss = sort { $hash{$b} <=> $hash{$a} } keys %hash;
    return wantarray ? @nss : $nss[0];
}

my $vnFullNameSpaces = {
};
sub verify_namespace {
    my $self = shift;
    my ($id, $ns) = @_;
    return 0 unless ($id);
    $self->benchstart;
    my $rv        = -1;
    $ns           = lc($ns || '');
    unless ($vnFullNameSpaces->{$ns}) {
        my $tok    = $self->namespace_token($ns);
        my $name   = $self->namespace_name($ns);
        my $anonOk = 0;
        my @regExps;
        foreach my $tok ($self->namespace_children( $ns )) {
            if (my $arr = $nsRegExp->{$tok}) {
                # At least one RE is defined for this namespace
                push @regExps, @{$arr};
            } else {
                # At least one namespace has no RE restrictions.
                # That means we should exclude ALL REs
                $anonOk++;
            }
        }
        my $class;
        if ($name =~ /^Any / ||
            $tok  eq 'SYM') {
            # We do not want to be ultra picky with "Any" namespace RegExps
            # Include the capability to test the MapTracker class
            # Same with symbols
            $class = $self->primary_maptracker_class( $tok );
        }
        @regExps = () if ($tok eq 'TAX' || (!$class && $anonOk));
        $vnFullNameSpaces->{$ns} = {
            REs   => $#regExps == -1 ? 0 : \@regExps,
            tok   => $tok,
            class => $class,
        };
    }
    if (my $arr = $vnFullNameSpaces->{$ns}{REs}) {
        $rv = 0;
        for my $r (0..$#{$arr}) {
            my $re = $arr->[$r];
            if ($id =~ /$re/i) {
                $rv = 1;
                last;
            }
        }
        unless ($rv) {
            if (my $mtcl = $vnFullNameSpaces->{$ns}{class}) {
                # We can try to validate based on MapTracker class
                my ($name, $obj) = $self->fast_standardize
                    ($id, $ns, $mtcl);
                # warn "$name [$ns] = $obj ($mtcl)";
                $rv = 1 if ($obj);
            }
        }
    }
    $self->benchend;
    return $rv;
}

sub _is_list_file {
    my $self = shift;
    my ($req) = @_;
    return undef unless ($req);
    return undef if (ref($req));
    return $req =~ /\.(tsv|list|txt|rich)$/i ? lc($1) : 0;
}

sub load_list_file {
    my $self = shift;
    unshift @_, '-file' if ($#_ == 0);
    my $args = $self->parseparams( @_ );
    my $file = $args->{FILE} || $args->{INPUT};
    my @list;
    if (defined $file) {
        my $type = $self->_is_list_file($file) || "unknown";
        my $richTags = $type eq 'rich' ? {} : undef;
        if (open(LFILE, "<$file")) {
            my $col = $args->{COLUMN} || $args->{COL};
            my $extractor = sub {
                my $line = shift;
                return split(/\s*[\t]\s*/);
            };
            if ($richTags) {
                # Extract metadata
                while (<LFILE>) {
                    s/[\n\r]+$//;
                    next if (/^\s*$/);
                    if (/\#\s*(.+)/) {
                        # Potential metadata tag
                        my $data = $1;
                        if ($data =~ /(\S+)\s*[\:\=]\s*(.+)\s*$/) {
                            $richTags->{uc($1)} = $2;
                        }
                    } else {
                        last;
                    }
                }
                close LFILE;
                $self->death("Failed to re-open file after reading meta tags",
                             $file, $!) unless (open(LFILE, "<$file"));
                $col = $richTags->{COL} || $richTags->{COLUMN} unless
                    (defined $col);
            }
            $col = 0 unless (defined $col);
            while (<LFILE>) {
                s/[\n\r]+$//; s/^\s+//; s/\s+$//;
                if ($richTags) {
                    next if (/^\#/);
                    s/\s+\#.+//;
                }
                my @row = &{$extractor}( $_ );
                
                push @list, $row[$col]
                    if (defined $row[$col] && $row[$col] ne "");
            }
            close LFILE;
        } else {
            $self->err("Failed to load list file", $file, $!);
        }
    } else {
        $self->err("No file name provided to load_list_file()");
    }
    return wantarray ? @list : \@list;
}

sub list_from_request {
    my $self = shift;
    my ($req, $splitter) = @_;
    my @rv;
    if (!defined $req || $req eq "") {
        # Null request, do nothing
        return wantarray ? () : undef;
    } elsif (my $r = ref($req)) {
        if ($r eq 'ARRAY') {
            map { push @rv, $_ if (defined $_ && $_ ne '') } @{$req};
        } else {
            $self->err("Can not recover list from '$req'");
        }
    } elsif ($self->_is_list_file( $req )) {
        @rv = $self->load_list_file( $req );
    } else {
        if ($splitter) {
            @rv = split($splitter, $req);
            # Assume that we never want leading or trailing spaces:
            map { s/^\s+//; s/\s+$// } @rv;
        } else {
            @rv = ($req);
        }
    }
    return wantarray ? @rv : \@rv;
}

sub list_to_namespaces {
    my $self = shift;
    $self->benchstart;
    my %hash;
    my ($list, $dbRows);
    if (ref($_[0])) {
        ($list, $dbRows) = @_;
    } else {
        $list = [ @_ ];
    }
    for my $i (0..$#{$list}) {
        my $id = $list->[$i];
        my $len = length($id);
        if ($len < 1 || $len > 100) {
            push @{$hash{''}}, $id;
            next;
        }
        my $gns = $self->guess_namespace( $id );
        my %full = map { $_ => 1 } $self->namespace_parents( $gns );
        map { delete $full{ $_ } } @notGoodNsGuess;
        my @spaces = keys %full;
        if ($#spaces == -1) {
            if ($dbRows) {
                @spaces = ( $self->guess_namespace_from_db( $id, $dbRows ) );
            } else {
                @spaces = ('');
            }
        }
        foreach my $ns ( @spaces ) {
            push @{$hash{$self->namespace_name($ns)}}, $id;
        }
    }
    $self->benchend;
    return \%hash;
}

sub most_likely_namespace {
    my $self = shift;
    my $hash = $self->list_to_namespaces( @_ );
    my @ns = sort { $#{$hash->{$b}} <=> $#{$hash->{$a}} } keys %{$hash};
    return wantarray ? @ns : $ns[0];
}

=head2 effective_namespace

 Title   :effective_namespace 
 Usage   : my $ns = $ad->effective_namespace( $primaryNS, $relatedNS, $ID )
 Function: Returns the most 'relevant' namespace for a request
 Returns : A namespace token or the empty string
 Args    : [0] The 'primary' namespace
           [1] Optional related namespace
           [2] Optional ID, or array ref of IDs

Some namespaces are really pseudo namespaces. For example, the
'Orthologue' (ORTH) namespace is more of an association than a
namespace in itself. Only output terms will have an ORTH namespace;
their true namespace is the same as the input term.

Some namespaces represent generic parent namespaces; for example "Any
RNA" (AR) will in many cases be representing more specific namespaces
such as RefSeq RNA or Ensembl RNA.

This function is designed to allow retrieval of a 'better' namespace
in such cases. The primary namespace is required, the other entries
are used to help find a more relevant (effective) namespace.

  1. If the primary namespace is not provided, and $ID is:
     a. Not provided: Return ''
     b. A single string: Return guess_namespace() using $ID
     b. An array reference: Return most_likely_namespace() using $ID
  2. Else if the primary namespace is 'ORTH'
     Return the secondary namespace
  3. Else if the primary namespace is 'RS', and $ID is:
     a. Not provided: Return ''
     b. A single string: Return guess_namespace() using $ID
     b. An array reference: Return most_likely_namespace() using $ID
  4. Else if the primary namespace is AP / AR / AL, and $ID is:
     a. Not provided: Return $primaryNS
     b. A single string: Return guess_namespace() or $nsA using $ID
     b. An array reference: Return most_likely_namespace() using $ID
  5. Otherwise just return $primaryNS

=cut

sub effective_namespace_OLD {
    my $self = shift;
    my ($nsA, $nsB, $id) = @_;
    unless ($nsA) {
        return '' unless ($id);
        return ref($id) ? $self->most_likely_namespace([$id]) : 
            $self->guess_namespace($id);
    }
    $nsA = $self->namespace_token($nsA) || $nsA;
    return ($nsB) ? $self->namespace_token($nsB) || $nsB : ''
        if ($nsA eq 'ORTH');
    if ($nsA eq 'RS') {
        return '' unless ($id);
        return ref($id) ? $self->most_likely_namespace([$id]) : 
            $self->guess_namespace($id);
    }
    if ($nsA =~ /^(AP|AR|AL)$/) {
        return $nsA unless ($id);
        return ref($id) ? $self->most_likely_namespace([$id]) || $nsA: 
            $self->guess_namespace($id) || $nsA;
    }
    return $nsA;
}

*effective_ns = \&effective_namespace;
sub effective_namespace {
    my $self = shift;
    $self->benchstart;
    my ($nsA, $nsB, $id) = @_;
    my $rv    = "";
    my $useID = $id;
    if ($nsA) {
        $rv    = $self->namespace_token($nsA) || $nsA;
        $useID = undef;
        if ($rv eq 'ORTH') {
            $rv = ($nsB) ? $self->namespace_token($nsB) || $nsB : '';
        } elsif ($rv eq 'RS') {
            $useID = $id;
            $rv    = "";
        } elsif ($rv =~ /^(AP|AR|AL)$/) {
            $useID = $id;
        }
    }
    if ($useID) {
        if (ref($useID)) {
            $rv = $self->most_likely_namespace($useID) || $rv;
        } else {
            ($rv) = $self->guess_namespace($useID, $rv);
        }
    }
    $self->benchend;
    return $rv;
}

=head2 process_url_list

 Title   : process_url_list
 Usage   : $ad->process_url_list( $path )
 Function: Reads a set of URLs associated with a namespace
 Returns : 
 Args    : [0] The path to the file

This function parses the data needed by namespace_links(). It reads a
text file that defines hyperlink information for namespaces.

Each URL specification should be separated from other URLs by one or
more blank lines.  Each specification should define one or more
variables with the variable name, a colon, one or more spaces, then
the value to assign to the variable. Recognized variables:

   title: Text that will be displayed when the mouse hovers over the link
   token: The text that will be displayed for the link
   style: CSS style deffinition for that type of URL
 styleIE: Optional CSS for use only in IE
      ns: One or more namespaces that will use the link
    nons: One or more namespaces NOT to use (useful if ns is a generic parent)
     url: The URL
    mult: A URL suitible for use with multiple IDs
         (use 'url' if it is the same as above)
    join: The joining text to use with multiple IDs (default is a comma)
   class: Optional explicit class (will otherwise be automatically assigned)

The URL can have variables defining replacement values. Placeholders
should be enlosed with double underscores, eg __ID__. A URL without
placeholders will not be terribly useful - it will be the same for
every ID.

 ID    - the ID of the term
 NST   - the namespace token of the term
 NSN   - the namespace name of the term
 MTNS  - the MapTracker namespace
 IDNUM - the integer component of the ID
   TAX - The taxa (species name). Some modifiers are allowed:
         ID - eg TAXID - use the NCBI TaxaID instead of species name
          U - eg TAXU - replace spaces with underscores

The 'url' class is assigned to ALL links

=cut

sub process_url_list {
    my $self = shift;
    my ($path) = @_;
    return undef unless ($path);
    unless (-e $path) {
        warn  "Failed to process URL List $path - could not find file\n  ";
        return undef;
    }
    open (URLS, "<$path") || $self->death("Failed to read URL list",$path,$!);
    my (%params, %urls);
    my %thash   = map { $_ => 1 } $self->all_namespace_tokens();
    map { delete $thash{$_} } qw(TAX ORTH RS);
    my @alltok  = sort keys %thash;
    my @sets;
    while (<URLS>) {
        chomp;
        s/^\s+//; s/\s+$//; s/\t/  /g;
        # Skip comments:
        next if (/^\#/);
        if ($_) {
            if (/^\s*(\S+)\:\s+(.+)/) {
                $params{lc($1)}  = $2;
            } else {
                warn "Malformed URL line:\n  $_\n  ";
            }
        } else {
            my @k = keys %params;
            push @sets, { %params } if ($#k > -1);
            %params = ();
        }
    }
    push @sets, { %params };
    close URLS;

    my $counter = 0;
    my %classes;
    foreach my $p (@sets) {
        my $style = $p->{style} ||= '';
        my $tok   = $p->{token};
        my $nstxt = $p->{ns};
        my $lKeys = lc($p->{key} || '');
        my $cx    = $p->{url};
        next unless ($tok && ($nstxt || $lKeys) && $cx);
        my $num   = $p->{urlNum} = ++$counter;
        my $class = $p->{class};
        if ($class) {
            $classes{$style} ||= $class;
        } else {
            $p->{class} = $class ||= $classes{$style} ||=
                sprintf("Url%02d", $num);
        }
        # Set up CanvasXpress data structure
        $cx =~ s/__/\$/g;
        my $cxDat = $p->{cx} = {
            token => $tok,
            id    => $num,
            url   => $cx,
        };
        foreach my $ck (qw(desc title style icon)) {
            $cxDat->{$ck} = $p->{$ck} if ($p->{$ck});
        }
        
        my $visualKey = "$tok\t$class";
        map {$urls{key}{$visualKey}{$_} ||= $p->{$_}}
        qw(token title desc class);

        $p->{title} ||= 'Undefined Hyperlink';
        my %list;
        if ($nstxt) {
            if ($nstxt =~ /(ANY|ALL)/) {
                %list = %thash;
            } else {
                %list = map { $_ => 1 } 
                $self->namespace_children(split(/\s+/, $nstxt));
            }
        }
        if (my $nons = $p->{nons}) {
            map { delete $list{$_} }
            $self->namespace_children(split(/\s+/, $nons));
        }
        foreach my $lKey (split(/\s+/, $lKeys)) {
            next unless ($lKey);
            if (length($lKey) < 5) {
                warn "Can not key URL '$p->{title}' using $lKey: ".
                    "it is not at least 5 characters in length\n  ";
                next;
            }
            if ($self->namespace_token($lKey)) {
                warn "Can not key URL '$p->{title}' using $lKey: ".
                    "it matches a namespace name\n  ";
                next;
            }
            $list{$lKey} = 1;
        }
        map { $urls{ns}{$_}{$visualKey} ||= $p } keys %list;
    }
    while (my ($style, $class) = each %classes) {
        next unless ($style && $class);
        $urls{urls}{$class} = $style;
    }
    while (my ($ns, $hash) = each %{$urls{ns}}) {
        # Within each namespace, sort the links by title
        my @data = sort {lc($a->{title}) cmp lc($b->{title})} values %{$hash};
        $urls{ns}{$ns} = \@data;
    }
    return $self->{URLS} = \%urls;
}

=head2 namespace_url_styles

 Title   : namespace_url_styles
 Usage   : my $html = $ad->namespace_url_styles()
 Function: Get the CSS styles for hyperlinks
 Returns : An HTML string, including style tags, for use inside a head tag
 Args    : 

This function returns formatted styles that will be used by the
hyperlinks generated by namespace_links(). Note that if you have not
first parsed a hyperlink specification file using process_url_list()
the function will return an empty string.

=cut

sub namespace_url_styles {
    my $self = shift;
    my $urlDat = $self->{URLS};
    return "" unless ($urlDat);
    my $html = "<style>\n";
    foreach my $class (sort keys %{$urlDat->{urls}}) {
        $html .= sprintf("  .%s { %s }\n", $class, $urlDat->{urls}{$class});
    }
    $html .= "</style>\n";
    return $html;
}

sub link_key {
    my $self = shift;
    my $urlDat = $self->{URLS};
    return "" unless ($urlDat);
    my @viskeys = sort { $a->{title} cmp $b->{title} || $a->{token} cmp $b->{token} } values %{$urlDat->{key}};
    return "" if ($#viskeys == -1);
    my $html = "<table><caption>Hyperlink Legend</caption><body>\n";
    foreach my $k (@viskeys) {
        my @row = (sprintf
                   ("<span class='%s'>%s</span>", $k->{class}, $k->{token}));
        push @row, ($k->{title}, $k->{desc});
        $html .= " <tr>".join('',map {"<td>".($_||'')."</td>"} @row)."</tr>\n";
    }
    $html .= "</body></table>\n";
}

=head2 namespace_links

 Title   : namespace_links
 Usage   : my $html = $ad->namespace_links( $ID, $primaryNS, $relatedNS )
 Function: Get a formatted string of hyperlinks for a namespace
 Returns : A string representing hyperlinks in HTML format
 Args    : [0] ID, or array ref of IDs
           [1] Optional 'primary' namespace
           [2] Optional related namespace
           [3] Optional string to use when joining multiple links
           [4] Optional additional link key

This function will use the provided parameters to determine the
effective_namespace(), and will then return a string of hyperlinks
associated with that namespace, in the context of the requested $ID.

Note that you need to first load a file of hyperlink specifications
using process_url_list().

=cut

sub namespace_links_raw {
    my $self = shift;
    my ($id, $nsA, $nsB, $lKeys) = @_;
    my @data;
    my $urlDat = $self->{URLS};
    return wantarray ? @data : \@data unless ($urlDat && $id);
    my $ns  = $self->effective_namespace( $nsA, $nsB, $id );
    my @arr = @{$urlDat->{ns}{$ns || ''} || []};
    foreach my $lKey (split(/\s+/, lc($lKeys || ''))) {
        next unless ($lKey);
        push @arr, @{$urlDat->{ns}{$lKey} || []};
    }
    return wantarray ? @data : \@data if ($#arr == -1);
    $id     = $id->[0] if (ref($id) && $#{$id} == 0);
    my %done;
    foreach my $udat (@arr) {
        my $tok  = $udat->{token};
        my $alt  = $udat->{title};
        my $desc = $udat->{desc} || $udat->{descr};
        my $url  = $udat->{url};
        my $clas = $udat->{class};
        my $icon = $udat->{icon};
        # Keys may bring in the same URL again
        next if ($done{$url}++);
        my (%rep, %used);
        if ($url =~ /__(ID(NUM|RAW)?)__/) {
            my ($rtok, $isnum) = ($1, $2);
            if (ref($id)) {
                # A list of IDs are provided
                my @ids = @{$id};
                if ($isnum) {
                    my @tidy;
                    if ($isnum eq 'RAW') {
                        # Strip out prefix behind colon
                        foreach my $id (@ids) {
                            if ($id =~ /^[^\:]+\:(.+)/) { push @tidy, $1 }
                        }
                    } else {
                        foreach my $id (@ids) {
                            if ($id =~ /(\d+)/) { push @tidy, $1 }
                        }
                    }
                    @ids = @tidy;
                }
                $url = lc($udat->{mult} || '') eq 'url' ? $url : $udat->{mult};
                next unless ($url);
                my $j = $udat->{join} || ',';
                my $v = join($j, map { CGI->escape($_) } @ids);
                next unless ($v);
                $url =~ s/__${rtok}__/$v/g;
                $used{$rtok} = $v;
            } else {
                my $repid = $id;
                if ($isnum) {
                    if ($repid =~ /(\d+)/) { $repid = $1 } else { $repid = '' }
                }
                $rep{$rtok}  = $repid;
            }
        }
        $rep{NST}  = $ns  if ($url =~ /__NST__/);
        $rep{NSN}  = $self->namespace_name($ns) if ($url =~ /__NSN__/);
        $rep{MTNS} = $self->maptracker_namespace($ns) if ($url =~ /__MTNS__/);
        my $failed = 0;
        if ($url =~ /__([^_]*TAX[^_]*)__/) {
            # Part of the URL is a taxa specification
            my @taxa = $self->convert( -id => $id, -ns1 => $ns, -ns2 => 'TAX');
            next if ($#taxa != 0);
            my $tax = $taxa[0];
            while ($url =~ /__([^_]*TAX[^_]*)__/) {
                my $tok = $1;
                my $val = $tax;
                if ($tok =~ /ID/) {
                    # Use NCBI taxa ID
                    my @tax = $self->cached_mt_taxa( $tax);
                    if ($#tax == -1) {
                        $val = 0;
                    } else {
                        $val = $tax[0]->id;
                    }
                } elsif ($tok =~ /(UCSC)/) {
                    my $key = $1;
                    $val = $taxaLookup->{$key}{$val};
                    unless ($val) { $val = '__FAILED__'; $failed++; }
                } else {
                    # Replace spaces with underscores
                    $val =~ s/ /_/g if ($tok =~ /U/);
                }
                $url =~ s/__${tok}__/$val/g;
                $used{$tok} = $val;
            }
        }
        while (my ($t, $v) = each %rep) {
            next unless ($url =~ /__${t}__/);
            unless ($v) {
                $failed = 1;
                last;
            }
            $v = CGI->escape($v);
            $url =~ s/__${t}__/$v/g;
            $used{$t} = $v;
        }
        next if ($failed);
        
        push @data, [$id, $ns, $url, $alt, $clas, $tok, $desc, $icon,
                     \%used, $udat->{cx}];
    }
    return wantarray ? @data : \@data;
}

sub namespace_links {
    my $self = shift;
    my ($id, $nsA, $nsB, $joiner, $lKeys, $attr) = @_;
    my $data = $self->namespace_links_raw($id, $nsA, $nsB, $lKeys);
    my @aBits;
    while (my ($tag, $val) = each %{$attr || {}}) {
        $tag = lc($tag);
        next if ($tag =~ /^(class|href)$/);
        $val =~ s/\'/\&apos;/g;
        push @aBits, sprintf(" %s='%s'", $tag, $val);
    }
    my $aTxt = join(' ', @aBits) || '';
    my @bits;
    foreach my $dat (@{$data}) {
        my ($id, $ns, $url, $alt, $clas, $tok, $desc, $icon) = @{$dat};
        if ($icon) {
            $tok = "<img width='16' height='16' src='$icon' />";
        }
        push @bits, sprintf
            ("<a class='url %s' title='%s' href='%s'%s>%s</a>",
             $clas, $alt || '', $url, $aTxt, $tok) if ($url);
    }
    $joiner ||= '';
    return join($joiner, @bits);    
}

sub namespace_is_linkable {
    my $self = shift;
    my ($nsA, $nsB) = @_;
    my $urlDat = $self->{URLS};
    return "" unless ($urlDat);
    my $ns  = $self->effective_namespace( $nsA, $nsB);
    my $arr = $urlDat->{ns}{$ns || ''};
    return $arr ? 1 : 0;
}

=head1 MAPTRACKER INTEGRATION

Connection between the meta elements used by AccessDenorm (namespaces)
with those use by MapTracker (namespaces and classes).

=head2 maptracker_namespace

 Title   : maptracker_namespace
 Usage   : $ad->maptracker_namespace( $string )
 Function: Gets the MapTracker namespace for a GenAcc namespace
 Returns : A string
 Args    : A GenAcc namespace identifier

If a GenAcc namespace corresponds to a MapTracker namespace, this
method will return it (for example it will return '#GeneSymbols#' if
passed 'SYM'). Otherwise it will return the default MapTracker
namespace ('#None#'). BMS::MapTracker::Namespace may also be of
interest to you.

=cut

sub maptracker_namespace {
    my ($self, $ns) = @_;
    my $tok = $self->namespace_token($ns);
    return defined $mtNamespaces->{$tok} ? $mtNamespaces->{$tok} : '#None#';
}

=head2 maptracker_classes

 Title   : maptracker_classes
 Usage   : my @classes = $ad->maptracker_classes( $namespace )
 Function: Gets any MapTracker classes associated with a GenAcc namespace
 Returns : An array of strings
 Args    : A GenAcc namespace identifier

Returns an array of all MapTracker classes (as class names) associated
with a GenAcc namespace. The array will be empty if there are no
associations. When called in a scalar context, will only return the
first class (this is identical to a primary_maptracker_class() call).

=cut

sub maptracker_classes {
    my ($self, $ns) = @_;
    my $tok  = $self->namespace_token($ns);
    my @list = @{$mtClasses->{$tok} || []};
    return wantarray ? @list : $list[0];
}

sub extended_maptracker_classes {
    my ($self, $ns) = @_;
    my $tok   = $self->namespace_token($ns);
    my @list  = @{$mtClasses->{$tok} || []};
    push @list, @{$mtAlsoOkClasses->{$tok} || []};
    return wantarray ? @list : $list[0];
}

=head2 primary_maptracker_class

 Title   : primary_maptracker_class
 Usage   : my $class = $ad->primary_maptracker_class( $namespace )
 Function: Gets the 'most important' MapTracker class associated with
           a namespace.
 Returns : A string
 Args    : A GenAcc namespace identifier

If one or more MapTracker classes are associated with a namespace,
this method will return the first one (presumed to be the most
characteristic for the namespace). The method is a wrapper for
maptracker_classes().

=cut

sub primary_maptracker_class {
    my $self = shift;
    my $rv   = $self->maptracker_classes( @_ );
    return $rv;
}

sub known_conversions {
    my $self = shift;
    my ($useMethName) = @_;
    my @ns   = $self->all_namespace_tokens;
    my $hash = {};
    for my $i (0..$#ns) {
        my $tok1 = $ns[$i];
        for my $j (0..$#ns) {
            my $tok2 = $ns[$j];
            my ($meth, $t1, $t2, $mn) = $self->_func_for_tokens($tok1, $tok2);
            if ($meth) {
                my $tag = sprintf("%s to %s", $t1, $t2);
                if ($useMethName) {
                    $tag = $converters->{$tok1}{$tok2} || $mn ||
                        sprintf("update_%s_to_%s", $tok1, $tok2);
                    
                    if (my $cond = $condChain->{$tok1}{$tok2}) {
                        my ($mid, $param) = @{$cond};
                        my %phash = @{$param};
                        my @pbits;
                        while (my ($tag, $val) = each %phash) {
                            if (ref($val) eq 'ARRAY') {
                                $val = '['.join(',', @{$val}).']';
                            }
                            push @pbits, "$tag => $val";
                        }
                        $tag .= "(".join(', ', @pbits).") via ".
                            join('+', @{$mid});
                    } else {
                        $tag .= "()";
                    }
                }
                $hash->{$tok1}{$tok2} = $tag;
            }
        }
    }
    return $hash;
}

sub get_converter {
    my $self = shift;
    unless ($self->tracker()) {
        unless ($self->{COMPLAIN}{CONVERTER}++) {
            warn "!---!\nNew conversions can not be calculated.\nOnly previously calculated data will be recovered from the database.\nIf you have set an age limit, you may wish to remove it (set it to zero)\n!---!\n";
        }
        return undef;
    }
    my ($ns1, $ns2)   = @_;
    my ($tok1, $tok2) = ($self->namespace_token($ns1), 
                         $self->namespace_token($ns2) );
    unless (exists $self->{CONVERTERS}{$tok1}{$tok2}) {
        $self->{CONVERTERS}{$tok1}{$tok2} = 
            $self->_func_for_tokens($tok1, $tok2);
    }
    return $self->{CONVERTERS}{$tok1}{$tok2};
}

sub _func_for_tokens {
    my $self = shift;
    my ($ns1, $ns2)   = @_;
    my ($tok1, $tok2) = ($self->namespace_token($ns1), 
                         $self->namespace_token($ns2) );
    # Try explicit NS1 -> NS2, then either 1 or 2 being 'ANY'
    my @try = ( [$tok1, $tok2],
                [$tok1, 'ANY'],
                ['ANY', $tok2], );
    foreach my $pair (@try) {
        my ($n1, $n2) = (@{$pair});
        last if ($suppressConversion->{$n1}{$n2});
        # Use an explicitly defined converter, if available. Otherwise
        # use a generically-named converter:
        my $func = $converters->{$n1}{$n2} ||
            sprintf("update_%s_to_%s", $n1, $n2);
        if (my $meth    = $self->can($func)) {
            return wantarray ? ($meth, $n1, $n2, $func) : $meth;
        }
    }
    return wantarray ? (undef) : undef;
}

sub _set_conv {
    my ($ns1req, $ns2req, $meth, $ignore) = @_;
    my @ns1s = ref($ns1req) ? @{$ns1req} : ($ns1req eq 'ANY' && $ignore) ?
        @allNs : split(/\s+/, uc($ns1req) );
    my @ns2s = ref($ns2req) ? @{$ns2req} : ($ns2req eq 'ANY' && $ignore) ?
        @allNs : split(/\s+/, uc($ns2req) );
    my %skip = map { uc($_) => 1 } @{$ignore || []};
    foreach my $ns1 (@ns1s) {
        foreach my $ns2 (@ns2s) {
            next if ($skip{"$ns1 $ns2"});
            if (my $prior = $converters->{$ns1}{$ns2}) {
                warn sprintf
                    ("Attempt to reset converter %5s -> %5s\n".
                     "  Kept %s(), Ignore %s()\n", $ns1, $ns2, $prior, $meth)
                    unless ($meth =~ /CONDITIONAL/i || $prior eq $meth);
            } else {
                # warn "{$ns1}{$ns2} = $meth" if ($ns1 eq 'SET');
                $converters->{$ns1}{$ns2} = $meth;
                # warn "$ns1 -> $ns2 = $meth\n" if ($ns1 eq 'LT');
            }
        }
    }
}

sub _full_kids {
    my ($seed, $ignore) = @_;
    my @stack = map { uc($_) } @{$seed || []};
    my (%done, %nss);
    while (my $ns = shift @stack) {
        next if ($done{$ns}++);
        $nss{$ns} = 1;
        push @stack, @{$nsChildren->{$ns} || []};
    }
    map { delete $nss{uc($_)} } @{$ignore} if ($ignore);
    return sort keys %nss;
}

# As an array will return:
# [ term_out ns_out auth matched ns_between]

sub assure_conversion {
    my $self = shift;
    my ($idReq, $ns1, $ns2, $age) = @_;
    $ns1 = $self->namespace_token( $ns1 );
    $ns2 = $self->namespace_token( $ns2 );

    # No point unless we have all three primary components...
    return unless ($idReq && $ns1 && $ns2);
    my $ids = ref($idReq) ? $idReq : [$idReq];
    my @uncertain;
    foreach my $id (@{$ids}) {
        my $key = join("\t", $id, $ns1, $ns2);
        push @uncertain, $id unless ($self->{ASSURED_CONERSION}{$key}++);
        # warn $key;
    }
    return if ($#uncertain == -1);
    # warn sprintf("    ASSURE: %d IDs [%4s] -> [%4s] (%.2f)\n", $#needed + 1, $ns1, $ns2, $age);
    # Quickly find those IDs that are not populated
    my $missing = $self->convert
        ( -id  => \@uncertain, -ns1 => $ns1, -ns2 => $ns2,
          -age => $age, -nullrows => 1, -xtraname => "AssureCheck");
    
    $self->convert( -id  => $missing, -ns1 => $ns1, -ns2 => $ns2,
                    -age => $age, -populate => 1, -forceupdate => 1,
                    -xtraname => "AssureUpdate")
        unless ($#{$missing} == -1);
}

# Once cached, about 70 uSec, about 25x faster than calling convert()
# for data that is already present in DB.
sub cached_conversion {
    my $self  = shift;
    my $key   = $self->_conversion_key( @_ );
    unless ($key) {
        warn "You must specify -id, -ns1 and -ns2 to use cached_conversion()";
        return $self->convert( @_ );
    }
    $self->benchstart;
    my $rows = $self->{CACHE}{CONVERSION}{$key};
    unless ($rows) {
        $self->clear_caches() if (++$self->{CACHE_COUNT} > $self->{MAXCACHE} );
        $rows = $self->{CACHE}{CONVERSION}{$key} = $self->convert
            ( @_, -xtraname => 'Cached' );
    }
    if (wantarray) {
        my %nonredun = map { defined $_->[0] ? $_->[0] : '' => 1 } @{$rows};
        delete $nonredun{''};
        $self->benchend;
        return sort keys %nonredun;
    } else {
        $self->benchend;
        return $rows;
    }
}

sub clear_caches {
    my $self = shift;
    $self->{CACHE} = {};
    $self->{CACHE_COUNT} = 0;
}

sub conversion_count {
    my $self  = shift;
    my $key   = $self->_conversion_key( @_ );
    return undef unless ($key);
    unless ($self->{CONVERSION_COUNT}{$key}) {
        my @items = $self->convert( @_, -xtraname => 'Count' );
        $self->{CONVERSION_COUNT}{$key} = $#items + 1;
    }
    return $self->{CONVERSION_COUNT}{$key};
}

sub _conversion_key {
    my $self  = shift;
    my $args  = $self->parseparams( @_ );
    my $id    = $args->{ID};
    my $ns1   = $self->namespace_token( $args->{NS1} );
    my $ns2   = $self->namespace_token( $args->{NS2} );
    if ($id && $ns1 && $ns2 && !ref($id)) {
        return "$id\t$ns1\t$ns2";
    } else {
        return "";
    }
}

sub _database_reqs {
    my $self = shift;
    my ($args) = @_;
    my $dbreq = $args->{SDB} || $args->{DB} || $args->{SET};
    return () unless ($dbreq);
    my @reqs = ref($dbreq) ? @{$dbreq} : split(/[\t\r\n\,]+/, $dbreq);
    my %dbs;
    foreach my $req (@reqs) {
        $req =~ s/^\s+//; $req =~ s/\s+$//;
        next unless ($req);
        my ($name) = $self->_known_mapping_databases($req);
        $dbs{$name}++ if ($name);
    }
    return sort keys %dbs;
}

sub churn_conversions {
    my $self = shift;
    my $args = $self->parseparams( -limit => 1,
                                   @_ );
    my $age    = $self->standardize_age( $args->{AGE} );
    my $lim    = $args->{LIMIT} || $args->{REPEAT} || 1;
    my $vb     = $args->{VERBOSE};
    my $norv   = $args->{DISCARD};
    my $doWarn = $args->{WARN};
    my $file   = $args->{CURRENTFILE};
    my $tLimit = &_clean_age($args->{TIME}, 'IsInFuture');
    my $dSql   = $args->{DUMPSQL};
    my (@wc, @binds);
    if (my $asql = $self->age_filter_sql( $age, 'updated')) {
        $asql =~ s/\>=/\</;
        push @wc, $asql;
    } elsif ($age) {
        $self->err("Failed to calculate age SQL for '$age' days");
    }

    if (my $ns1 = $args->{NS1}) {
        my @vals = ref($ns1) ? @{$ns1} : split(/\s*[\n\r\t\,]+\s*/, $ns1);
        unless ($#vals == -1) {
            push @wc, "ns_in IN (".join(',', map { '?' } @vals ).")";
            push @binds, map { $self->namespace_name($_) } @vals;
        }
    }
    if (my $ns2 = $args->{NS2}) {
        my @vals = ref($ns2) ? @{$ns2} : split(/\s*[\n\r\t\,]+\s*/, $ns2);
        unless ($#vals == -1) {
            push @wc, "ns_out IN (".join(',', map { '?' } @vals ).")";
            push @binds, map { $self->namespace_name($_) } @vals;
        }
    }
    push @wc, $usePostgres ? "(term_out IS NULL OR term_out = '')" : 
        "term_out IS NULL" if ($args->{NULL});
    if (my $id = $args->{ID}) {
        my $op = '=';
        if ($id =~ /\%/) {
            $op = 'LIKE';
        }
        push @wc, "term_in $op ?";
        push @binds, $id;
    }
    my $sql  = "SELECT term_in, ns_in, ns_out, ".$self->age_recover_sql();
    $sql    .= ", rowid" unless ($usePostgres);
    $sql    .= " FROM conversion";
    $sql    .= " WHERE ".join(' AND ', @wc) if ($#wc != -1);
    my $sth  =  $self->dbh->prepare
        ( -sql   => $sql,
          -name  => "Find stale conversions",
          -level => 3,
          -limit => 1);
    my $remove = $usePostgres ? 
        $self->dbh->prepare
        ( -sql   => "DELETE FROM conversion WHERE ".
          "term_in = ? AND ns_in = ? and ns_out = ?",
          -name  => "Remove un-updatable conversion entry",
          -level => 3  ) :
          $self->dbh->prepare
          ( -sql   => "DELETE FROM conversion WHERE rowid = ?",
            -name  => "Remove un-updatable conversion entry by rowID",
            -level => 3,
            -limit => 1, );
    my $nullId = $self->dbh->prepare
        ( -sql   => "DELETE FROM conversion WHERE term_in IS NULL OR term_in = ''",
          -name  => "Remove null conversion entry",
          -level => 3  );

    warn $sth->pretty_print( @binds ) if ($dSql || ($vb && $vb > 1));

    my @rv;
    my $finish;
    if ($tLimit) {
        # Will be in days, convert to seconds
        $tLimit *= 24 * 60 * 60;
        $finish = time + $tLimit;
        $lim    = 1;
    }

    my $prior = "";
    my $lastTime;
    while ($lim-- > 0) {
        $sth->execute( @binds );
        my $found = 0;
        while (my $row = $sth->fetchrow_arrayref) {
            my ($id, $ns1, $ns2, $days, $rid) = @{$row};
            $found++;
            unless ($id) {
                # This should not happen, but sometimes null IDs get
                # added to the DB, and can wedge churn conversions
                $nullId->execute();
                next;
            }
            my $key = "$id\t$ns1\t$ns2";
            if ($key eq $prior) {
                # Entry that is failing to update - just nuke it
                if ($days > 0.01) {
                    my @binds = $usePostgres ? ( $id, $ns1, $ns2 ) : ($rid);
                    print STDERR sprintf
                        ("%20s [%4s] -> [%4s] %.2f [ DELETED ]\n", $id,
                         $self->namespace_token($ns1),
                         $self->namespace_token($ns2), $days) if ($vb);
                    # die "SELECT * FROM conversion WHERE rowid = '$rid'\n  ";
                    $remove->execute(@binds);
                } else {
                    $self->msg("Halting on repetition of $id") if ($vb);
                }
            } else {
                print STDERR sprintf
                    ("%20s [%4s] -> [%4s] %.2f\n", $id,
                     $self->namespace_token($ns1),
                     $self->namespace_token($ns2), $days) if ($vb);
                if ($file) {
                    unlink($file) if (-e $file && -s $file > 100000);
                    if (open(CHURNNOTE, ">>$file")) {
                        my $msg = sprintf
                            ("%10s%20s [%4s] -> [%4s] %.2f\n", $lastTime ?
                             (time - $lastTime). " sec" : "START",
                             $id, $self->namespace_token($ns1),
                             $self->namespace_token($ns2), $days);
                        print CHURNNOTE $msg;
                        close CHURNNOTE;
                        chmod(0666, $file);
                        $lastTime = time;
                    }
                }
                $self->convert
                    ( -id => $id, -ns1 => $ns1, -ns2 => $ns2, -age => $age,
                      -nospecificage => 1,
                      -warn => $doWarn, -xtraname => 'Churn');
                push @rv, [ $id, $ns1, $ns2, int(0.5 + $days) ] unless ($norv);
            }
            $prior = $key;
        }
        last unless ($found);
        if ($finish && time < $finish) {
            # We are running with a time limit and have not reached it yet
            $lim = 1;
        }
    }
    return wantarray ? @rv : \@rv;
}

sub churn_assignments {
    my $self = shift;
    my $args = $self->parseparams( -limit => 1,
                                   @_ );
    my $age    = $self->standardize_age( $args->{AGE} );
    my $lim    = $args->{LIMIT} || 1;
    my $vb     = $args->{VERBOSE};
    my $dSql   = $args->{DUMPSQL};
    my (@wc, @binds);
    if ($age) {
        if (my $asql = $self->age_filter_sql( $age, 'updated')) {
            $asql =~ s/\>=/\</;
            push @wc, $asql;
        } else {
            $self->err("Failed to calculate age SQL for '$age' days");
        }
    }

    if (my $ns1 = $args->{ANS} || $args->{NS1}) {
        push @wc, "acc_ns = ?";
        push @binds, $ns1;
    }
    if (my $ns2 = $args->{ONS} || $args->{NS2}) {
        push @wc, "onto_ns = ?";
        push @binds, $ns2;
    }
    if (my $id = $args->{ID}) {
        my $op = '=';
        if ($id =~ /\%/) {
            $op = 'LIKE';
        }
        push @wc, "acc $op ?";
        push @binds, $id;
    }
    my $sql    = "SELECT DISTINCT acc, acc_ns, onto_ns, ".
        $self->age_recover_sql()." FROM assign_onto";
    $sql .= " WHERE ".join(' AND ', @wc) if ($#wc != -1);
    my $sth  =  $self->dbh->prepare
        ( -sql   => $sql,
          -name  => "Find stale assignments",
          -limit => $lim,
          -level => 3);
    warn $sth->pretty_print( @binds ) if ($dSql || ($vb && $vb > 1));
    $sth->execute( @binds );
    while (my $row = $sth->fetchrow_arrayref) {
        my ($id, $ns1, $ns2, $days) = @{$row};
        print STDERR sprintf
            ("%20s [%4s] => [%4s] %.2f\n", $id, $self->namespace_token($ns1),
             $self->namespace_token($ns2),$days) if ($vb);
        $self->assignments
            ( -acc => $id, -ans => $ns1, -ons => $ns2, -age => $age);
    }
}

sub _namespace_select {
    my $self = shift;
    my $clause = "";
    if (my $nreq = shift) {
        my @reqs = ref($nreq) ? @{$nreq} : split(/\s*[\,\n\t]\s*/, $nreq);
        my %distinct = map { $_ => 1 } $self->namespace_children(@reqs);
        my @names = sort map { $self->namespace_name($_) } keys %distinct;
        unless ($#names == -1) {
            $clause = "IN (".join(', ', map { "'$_'" } @names).")";
        }
    }
    return $clause;
}

sub churn_descriptions {
    my $self = shift;
    my $args = $self->parseparams( -limit => 1,
                                   @_ );
    my $age    = $self->standardize_age( $args->{AGE} );
    my $lim    = $args->{LIMIT} || $args->{REPEAT} || 1;
    my $vb     = $args->{VERBOSE};
    my $norv   = $args->{DISCARD};
    my $doWarn = $args->{WARN};
    my $dSql   = $args->{DUMPSQL};
    my $file   = $args->{CURRENTFILE};
    my $tLimit = &_clean_age($args->{TIME}, 'IsInFuture');
    my (@wc, @binds);
    if (my $asql = $self->age_filter_sql( $age, 'updated')) {
        $asql =~ s/\>=/\</;
        push @wc, $asql;
    } elsif ($age) {
        $self->err("Failed to calculate age SQL for '$age' days");
    }

    if (my $ncl = $self->_namespace_select($args->{NS} || $args->{NS1})) {
        push @wc, "ns $ncl";
    }

    if (my $id = $args->{ID}) {
        my $op = '=';
        if ($id =~ /\%/) {
            $op = 'LIKE';
        }
        push @wc, "term $op ?";
        push @binds, $id;
    }
    if (my $id = $args->{DESCRIPTION} || $args->{DESC}) {
        my $op = '=';
        if ($id =~ /\%/) {
            $op = 'LIKE';
        }
        push @wc, "descr $op ?";
        push @binds, $id;
    }
    if ($args->{NULL}) {
        my $nsql = "(descr IS NULL";
        $nsql   .= " OR descr = ''" if ($usePostgres);
        $nsql   .= " OR descr = '$depToken'" unless ($args->{IGNOREDEP});
        $nsql   .= ")";
        push @wc, $nsql;
    }
    # DISTNCT was *REALLY* slowing this down. Not sure why
    # my $sql = "SELECT DISTINCT term, ns, ".$self->age_recover_sql();

    my $sql = "SELECT term, ns, ".$self->age_recover_sql();
    $sql   .= ", rowid" unless ($usePostgres);
    $sql   .= "  FROM description";
    $sql   .= " WHERE ".join(' AND ', @wc) if ($#wc != -1);

    my $sth  =  $self->dbh->prepare
        ( -sql   => $sql,
          -name  => "Find stale descriptions",
          -level => 3,
          -limit => 1, );
    my $remove = $usePostgres ? 
        $self->dbh->prepare
        ( -sql   => "DELETE FROM description WHERE term = ? AND ns = ?",
          -name  => "Remove un-updatable description entry",
          -level => 3 ) :
          $self->dbh->prepare
          ( -sql   => "DELETE FROM description WHERE rowid = ?",
            -name  => "Remove un-updatable description by rowID",
            -level => 3,
            -limit => 1, );

    if ($dSql || ($vb && $vb > 1)) {
        warn $sth->pretty_print( @binds );
    }
    my @rv;
    my $finish;
    if ($tLimit) {
        # Will be in days, convert to seconds
        $tLimit *= 24 * 60 * 60;
        $finish = time + $tLimit;
        $lim    = 1;
    }
    
    my $prior = "";
    my $lastTime;
    while ($lim-- > 0) {
        $sth->execute( @binds );
        my $found = 0;
        while (my $row = $sth->fetchrow_arrayref) {
            my ($id, $ns, $days, $rid) = @{$row};
            $id = "" if (!defined $id);
            my $key = "$id\t$ns";
            if ($key eq $prior) {
                # Entry that is failing to update - just nuke it
                if ($days > 0.001) {
                    my @binds = $usePostgres ? ( $id, $ns ) : ($rid);
                    $remove->execute(@binds);
                    $self->msg("[Churn]", sprintf
                               ("%20s [%4s] :: [ DELETED ] %.2f",
                                $id || '-NULL-',
                                $self->namespace_token($ns),$days)) if ($vb);
                } else {
                    $self->err("Halting on repetition of $id") if ($vb);
                    last;
                }
            } else {
                $self->msg("[Churn]", sprintf
                           ("%20s [%4s] :: [Desc] %.2f", $id,
                            $self->namespace_token($ns),$days)) if ($vb);
                if ($file) {
                    unlink($file) if (-e $file && -s $file > 100000);
                    if (open(CHURNNOTE, ">>$file")) {
                        my $msg = sprintf
                            ("%10s%20s [%4s] :: [Desc] %.2f\n", $lastTime ?
                             (time - $lastTime). " sec" : "START",
                             $id, $self->namespace_token($ns),$days);
                        print CHURNNOTE $msg;
                        close CHURNNOTE;
                        chmod(0666, $file);
                        $lastTime = time;
                    }
                }
                $self->description( -id => $id, -ns => $ns, -age => $age,
                                    -warn => $doWarn );
                push @rv, [ $id, $ns, int(0.5 + $days) ] unless ($norv);
            }
            $prior = $key;
            $found++;
        }
        last unless ($found);
        if ($finish && time < $finish) {
            # We are running with a time limit and have not reached it yet
            $lim = 1;
        }
    }
    return wantarray ? @rv : \@rv;
}

sub set_temp_list {
    my $self = shift;
    $self->benchstart;
    $self->death("DEPRECATED");
    my ($list, $tName, $func) = @_;
    $tName ||= 'temp_list';
    my $dbh  = $self->dbh;
    $dbh->prepare("TRUNCATE TABLE $tName")->execute();
    my %u  = map { $_ => undef } map { defined $_ ? $_ : "" } @{$list};
    delete $u{""};
    my @uList = map { [$_] } keys %u;
    $dbh->insert_array( $tName, \@uList);
    my $col = $func ? sprintf($func, 'member') : 'member';
    $dbh->_fast_add_index( $tName, [$col] );

#    my $load = $dbh->prepare("INSERT INTO $tName VALUES (?)");
#    my $nr = $#{$list} + 1;
#    for my $l (0..$#{$list}) {
#        if ($list->[$l]) {
#            $load->execute( substr($list->[$l],0,100) );
#        } else {
#            $nr--;
#        }
#    }
   # $dbh->prepare
   #     ("dbms_stats.set_table_stats( user, '$tName', numrows=> $nr )")->
   #     execute();

    # $dbh->prepare("ANALYZE TABLE $tName COMPUTE STATISTICS")->execute();
#    my $sth = $self->dbh->prepare
#        ( -sql   => "Select count(*) from $tName",
#          -name  => "count entries in temp list",
#          -level => 1);
#    warn sprintf("set_temp_list : %d vs %d", $#{$list}+1, $sth->get_single_value);
    $self->benchend;
    return $tName;
}

=head1 ONTOLOGY METHODS

These methods are used to determine the connectivity of the hierarchy
supporting an ontology. They all require a query node, and then
provide information on other nodes that are related to it by descent.

=head2 direct_genealogy

 Title   : direct_genealogy
 Usage   : my @relatives = $ad->direct_genealogy( $query, $dir, $ns, $age )
 Function: Get direct relations for ontology nodes
 Returns : An array of node names
 Args    : [0] Required. The name/id of the query node
           [1] Required. 'direction' to go, an integer
               >= 0 : get the direct children of the query
               <  0 : get the direct parents of the query
           [1] Optional namespace
           [2] Optional age

Depending on the direction chosen, the method will find immediate
children or parents of the query node by querying DIRECT_CHILDREN
($dir >= 0) or DIRECT_PARENTS ($dir < 0)

If no results are found in the table, or the data in the table are
older than the specified age, the program will attempt to calculate
new information if a namespace has been provided.

An empty array indicates that no relations exist in that direction (or
possibly that no prior attempt was made to calculate the relations if
you have not provided a namespace).

Ontology terms without parents are roots and those without children
are leaves (unless the ontology is just a flat, unconnected list,
although technically each member is then both a root and a
leaf). Those with both are internal nodes.

The methods direct_parents() and direct_children() are simple wrappers
for direct_genealogy( ). The methods all_parents() and all_children()
rely on information gathered by direct_genealogy( ).

=cut

sub direct_genealogy {
    my $self  = shift;
    my ($idReq, $dir, $nsreq, $age, $doWarn) = @_;
    my ($id, $seq) = $self->standardize_id( $idReq, $nsreq );
    return unless ($seq);
    $self->benchstart;

    my @binds      = ($id);
    my ($src,$trg) = $dir < 0 ? ('child','parent') : ('parent','child');
    my $tab        = 'direct_' . ( $dir < 0 ? 'parents' : 'children');
    my $sthnm      = "Get $tab via term";
    my $dbh        = $self->dbh;
    my $sql        = "SELECT $trg, relation FROM $tab WHERE $src = ?";

    my $ns;
    if ($nsreq) {
        unless ($ns = $self->namespace_name( $nsreq )) {
            $self->err("Could not get $tab for unknown namespace '$nsreq'");
            $self->benchend;
            return ();
        }
        if ($self->verify_namespace($id, $ns)) {
            ($id) = $self->standardize_id($id, $ns);
            $sthnm .= " + namespace";
            $sql   .= ' AND ns = ?';
            push @binds, $ns;
            
        } else {
            $ns = '';
        }
    }
    if ($age = $self->standardize_age( $age )) {
        if (my $asql = $self->age_filter_sql( $age, 'updated')) {
            $sql .= " AND $asql";
        } else {
            $self->err("Failed to calculate age SQL for '$age' days");
        }
    }
    if ($doWarn) {
        printf(STDERR "%s%30s [%4s] => %s", 
               (++$self->{SCROLL_ITER} % $self->{SCROLL_SIZE}) ?
               "\r" : "\n", $id, $self->namespace_token( $ns ),
               $dir < 0 ? 'Parents' : 'Children');
    }
    
    my $sth  = $dbh->prepare
        ( -name  => $sthnm,
          -level => 3,
          -sql   => $sql );
    # warn $sth->pretty_print(@binds);
    $sth->execute( @binds );
    my $rows = $sth->fetchall_arrayref;
    if ($#{$rows} < 0 && $ns) {
        # Nothing is in the database, and we have an explicit namespace
        my $token = $self->namespace_token( $ns );
        if (my $codeRef = $self->can("update_${token}_genealogy")) {
            $rows = &{$codeRef}( $self, $id, $token, $age, $dir );
        }
    }
    $self->benchend;
    # During forked loading sometimes multiple null values are inserted...
    while ($#{$rows} != -1 && !$rows->[0][0]) { shift @{$rows} };
    return wantarray ? map { $_->[0] } @{$rows} : $rows;
}

=head2 direct_parents

 Title   : direct_parents
 Usage   : my @parents = $ad->direct_parents( $query, $ns, $age )
 Function: Get all direct parents for a node
 Returns : An array of parent terms
 Args    : [0] The name/id of the query node
           [1] Optional namespace
           [2] Optional age

This is just a convienence wrapper for direct_genealogy()

=cut

sub direct_parents {
    my $self  = shift;
    my ($id, $nsreq, $age, $doWarn) = @_;
    return $self->direct_genealogy($id, -1, $nsreq, $age, $doWarn);
}

=head2 direct_children

 Title   : direct_children
 Usage   : my @kids = $ad->direct_children( $query, $ns, $age )
 Function: Get all direct children for a node
 Returns : An array of child terms
 Args    : [0] The name/id of the query node
           [1] Optional namespace
           [2] Optional age

This is just a convienence wrapper for direct_genealogy()

=cut

sub direct_children {
    my $self  = shift;
    my ($id, $nsreq, $age, $doWarn) = @_;
    return $self->direct_genealogy($id, 1, $nsreq, $age, $doWarn);
}

=head2 has_parent

 Title   : has_parent
 Usage   : my $bool = $ad->has_parent( $query, $ns, $age )
 Function: Indicates if a node has a parent node
 Returns : 1 if true, 0 if false (undef if $query not provided)
 Args    : [0] The name/id of the query node
           [1] Optional namespace
           [2] Optional age

Utility call that uses direct_parents() to determine if the query has
any parent nodes.

=cut

sub has_parent {
    my $self = shift;
    return undef unless ($_[0]);
    my @pars = $self->direct_parents( @_ );
    return ($#pars > -1) ? 1 : 0;
}

=head2 has_parent_cached

 Title   : has_parent_cached
 Usage   : my $bool = $ad->has_parent_cached( $query, $ns, $age )
 Function: Indicates if a node has a parent node
 Returns : 1 if true, 0 if false (undef if $query not provided)
 Args    : [0] The name/id of the query node
           [1] Optional namespace
           [2] Optional age

This method is identical in operation to has_parent(), but maintains
the results in an internal cache. Useful if you think you will be
querying the same nodes repeatedly.

=cut

sub has_parent_cached {
    my $self = shift;
    my ($id, $nsreq, $age) = @_;
    my $ns = $self->namespace_token( $nsreq );
    return undef unless ($id && $ns);
    my $key = "$id\t$ns";
    $self->{CACHE}{HAS_PARENT}{$key} = $self->has_parent($id, $nsreq, $age)
        unless (defined $self->{CACHE}{HAS_PARENT}{$key});
    return $self->{CACHE}{HAS_PARENT}{$key};
}

=head2 root_parent

 Title   : root_parent
 Usage   : my @roots = $ad->( $query, $ns, $age )
 Function: Finds the root(s) associated with a query
 Returns : An array of nodes in array context, otherwise a single root
 Args    : [0] The name/id of the query node
           [1] Optional namespace
           [2] Optional age

Will find the root nodes associated with the query. An ontology really
should have a single root, but the method will not assume that. If
called in array context, all roots (defined as parents that themselves
do not have parents) will be returned. In scalar context, only one
root will be returned.

The method utilizes recursive calls to direct_parents().

=cut

sub root_parent {
    my $self  = shift;
    my ($id, $nsReq, $age) = @_;
    return () unless ($id);
    $self->benchstart;

    my (%seen, %rhash);
    my @stack = ( $id );
    while (my $term = shift @stack) {
        next if ($seen{$term}++);
        my @gpars = $self->direct_parents($term, $nsReq, $age);
        if ($#gpars < 0) {
            $rhash{$term}++;
        } else {
            push @stack, @gpars;
        }
    }
    my @roots = sort keys %rhash;
    $self->benchend;
    return wantarray ? @roots : $roots[0];
}

=head2 all_parents

 Title   : all_parents
 Usage   : my @kids = $ad->all_parents( @params )
 Function: Transitively find all parents of the node
 Returns : An array of parent nodes, or a hash reference
 Args    : Associative array of arguments. Recognized keys:

       -id Required. The name of the starting ontology node. Multiple
           nodes can be provided by passing an array reference of node
           names. The alias -ids may also be used.

       -ns The namespace of the id(s). Optional, but if not provided
           then the program will only be able to return pre-computed
           values.

      -age Maximum data age. If not provided, the global age() will be
           used.

    -level Optional level request. If provided, then only parent nodes
           at that specific distance will be returned. The alias
           -parentage may be used instead.

   -noself By default this method will return not only the parents,
           but the query node(s) as well. If -noself is a true value,
           then the queries will not be included in the returned
           values.

If this method is called in array context, then an array of parent
nodes (strings) will be returned. If called in scalar context, then a
hash reference will be returned, where the keys are the node names,
and the values are the minimum distance between the node and any of
the queries. A distance of zero indicates that the node is a query. A
distance of 2 would indicate that the shortest path in the between the
node and any of the queries is two edges (ie, the node is a
grandparent, with one child between it and a query).

=cut

# http://www.geneontology.org/GO.ontology.relations.shtml
our $allowedRelations = {
    'is_a' => {
        ''                     => 'is_a',
        'is_a'                 => 'is_a',
        'negatively_regulates' => 'negatively_regulates',
        'part_of'              => 'part_of',
        'positively_regulates' => 'positively_regulates',
        'regulates'            => 'regulates',
    },
    'part_of' => {
        ''                     => 'part_of',
        'is_a'                 => 'part_of',
        'negatively_regulates' => '', # Can not tell
        'part_of'              => 'part_of',
        'positively_regulates' => '', # Can not tell
        'regulates'            => '', # Can not tell
    },
    'regulates' => {
        ''                     => 'regulates',
        'is_a'                 => 'regulates',
        'negatively_regulates' => '', # Can not tell
        'part_of'              => 'regulates',
        'positively_regulates' => '', # Can not tell
        'regulates'            => '', # Can not tell
    },
    'negatively_regulates' => {
        ''                     => 'negatively_regulates',
        'is_a'                 => 'negatively_regulates',
        'negatively_regulates' => '', # Can not tell
        'part_of'              => 'regulates',
        'positively_regulates' => '', # Can not tell
        'regulates'            => '', # Can not tell
    },
    'positively_regulates' => {
        ''                     => 'positively_regulates',
        'is_a'                 => 'positively_regulates',
        'negatively_regulates' => '', # Can not tell
        'part_of'              => 'regulates',
        'positively_regulates' => '', # Can not tell
        'regulates'            => '', # Can not tell
    },
};

our $relationPrecedence = {
    ''                     => 0,
    'is_a'                 => 10,
    'part_of'              => 20,
    'regulates'            => 30,
    'positively_regulates' => 40,
    'negatively_regulates' => 40,
};

sub _rel_precedence {
    my $self = shift;
    my $rels = shift || [];
    my $rkey = join(',', @{$rels});
    unless (defined $self->{RELATION_PREC}{$rkey}) {
        my @ordered = sort { ($relationPrecedence->{$b} || 0) <=> 
                                 ($relationPrecedence->{$a} || 0) ||
                                 $a cmp $b } @{$rels};
        my $best = $self->{RELATION_PREC}{$rkey} = $ordered[0] || "";
        if ($best eq 'negatively_regulates' && $#ordered > 0 &&
            $ordered[1] eq 'positively_regulates') {
            # If both positive and negative regulation, fall back to generic:
            $self->{RELATION_PREC}{$rkey} = 'regulates';
        }
        warn "$rkey ===> $best\n";
    }
    return $self->{RELATION_PREC}{$rkey};
}

*each_parent = \&all_parents;
sub all_parents {
    my $self  = shift;
    $self->benchstart;
    unshift @_, '-id' if ($#_ == 0);
    my $args   = $self->parseparams( @_ );
    my $idReq  = $args->{ID} || $args->{IDS};
    my $nsReq  = $args->{NS} || $args->{NS1};
    my $age    = $self->standardize_age( $args->{AGE} );
    my $lReq   = ref($idReq) ? $idReq : [ $idReq ];
    my $doWarn = $args->{WARN};
    my $relOne = $args->{RELATIONSHIP} || $args->{REL} || "";

    # The stack will hold node, depth and relation
    my @stack;
    foreach my $idR (@{$lReq}) {
        my ($id)  = $self->standardize_id($idR, $nsReq);
        push @stack, [$id, 0, $relOne] if ($id);
    }
    if ($#stack == -1) {
        $self->benchend;
        return wantarray ? () : {};
    }

    my %noSelf;
    map { $noSelf{$_->[0]} = 1 } @stack if ($args->{NOSELF});

    my %relStruct;
    while (my $dat = shift @stack) {
        my ($term, $lvl, $rel) = @{$dat};
        if (defined $relStruct{$term}{$rel}) {
            # We have encountered this term before
            $relStruct{$term}{$rel} = $lvl if ($lvl < $relStruct{$term}{$rel});
        } else {
            # First encounter
            $relStruct{$term}{$rel} = $lvl;
            # Add the term's parents to the stack to recurse upwards
            my $parDat = $self->direct_parents
                ($term, $nsReq, $age, $doWarn);
            foreach my $pd (@{$parDat}) {
                my $par = $pd->[0];
                my $pRel = $pd->[1] || "";
                if (my $ar = $allowedRelations->{$rel}) {
                    next unless ($pRel = $ar->{$pRel});
                }
                push @stack, [$par, $lvl + 1, $pRel];
            }
        }
    }

    my %rv;
    my $level = $args->{LEVEL} || $args->{PARENTAGE};
    while (my ($term, $rH) = each %relStruct) {
        next if ($noSelf{$term});
        my %lvls;
        while (my ($rel, $lvl) = each %{$rH}) {
            next if ($level && $lvl != $lvl);
            push @{$lvls{$lvl}}, $rel;
        }
        my @levels = $level ? ($level) : sort { $a <=> $b } keys %lvls;
        foreach my $lvl (@levels) {
            if (my $rels = $lvls{$lvl}) {
                push @{$rv{$term}}, $lvl, $rels;
            }
        }
    }
    
    $self->benchend;
    return wantarray ? sort { $rv{$a}[0] <=> $rv{$b}[0] } keys %rv : \%rv;
}

# Will find the smallest set of objects that represent the query
sub least_common_parents {
    my $self  = shift;
    my $args  = $self->parseparams( @_ );
    my $ids   = $args->{IDS} || $args->{ID};
    my $ns    = $args->{NS} || $args->{NS1};
    my $age    = $args->{AGE};
    my $doWarn = $args->{WARN};
    my @stack = ref($ids) ? @{$ids} : ($ids);
    return wantarray ? @stack : \@stack if ($#stack < 1);
    
    # Standardize queries
    for my $s (0..$#stack) {
        my $idR = $stack[$s];
        my ($id) = $self->standardize_id($idR, $ns);
        $stack[$s] = $id if ($id ne $idR);
    }
    my %queries = map { $_ => 1 } @stack;
    # Find all parent nodes for the requests
    my (@roots, %children, %done);
    while ($#stack != -1) {
        my $id  = shift @stack;
        next if ($done{$id}++);
        my @pars = $self->direct_parents($id, $ns, $age, $doWarn);
        $children{$id} ||= {};
        if ($#pars == -1) {
            push @roots, $id;
        } else {
            map { $children{$_}{$id}++ } @pars;
            push @stack, @pars;
        }
    }
    # Now find all minimal parents, starting with the roots
    my @rv;
    foreach my $root (@roots) {
        my $id = $root;
        my %iloop;
        while (1) {
            if ($iloop{$id}++) {
                $ns ||= '-undef-';
                $self->err("Loop structure encountered trying to find least common parents",
                           "$id [$ns]");
                $id = $root;
                last;
            }
            # If the ID being considered is a query, then we must use it
            # as a least common parent, as any child will be more specific:
            last if ($queries{$id});
            my @kids = keys %{$children{$id}};
            if ($#kids == 0) {
                # This node has a single child in the subtree we built
                # That child represents a better "least common" ID
                $id = $kids[0];
            } else {
                # Either:
                # The node has no children and is a terminal leaf
                # The node has two or more children and is not an appropriate
                # "least common" ID
                last;
            }
        }
        push @rv, $id;
    }
    return wantarray ? @rv : \@rv;
}

=head2 all_children

 Title   : all_children
 Usage   : my @kids = $ad->all_children( $query, $ns, $age )
 Function: Get all direct and indirect children of an ontology node.
 Returns : An array of node names, or a hash reference (scalar context)
 Args    : [0] The name/id of the query node
           [1] Optional namespace
           [2] Optional age

Given an ontology node, this method will compile a list containing the
node itself, all direct children, and all recursively-gathered
indirect children. If called in array context, a list of node names
will be returned. If called in scalar context, a hash reference will
be returned, with keys being node names and values being the shortest
distance to the query node (0 = query node, 1 = direct child, 2 =
grand child, etc).

Data for this method are stored in the PARENTAGE table. If the data do
not exist (or are older than the age limit), they will be
(re)calculated by:

 1. Finding all direct_children() (from table DIRECT_CHILDREN)
 2. Recursively calling all_children( ) with each direct child

If a child can be encountered through multiple paths in the ontology,
the reported distance to the child will represent the shortest path.

The method is relatively fast, even though it is fully recursive and
exhaustive. Full closure of GeneOntology takes a bit less than an
hour and will generate about 300k distinct rows in PARENTAGE.

  biological_process GO:0008150 ~30 min
  cellular_component GO:0005575  ~4 min
  molecular_function GO:0003674 ~15 min

The XPRESS ontology closure is quite slow to generate; possibly due to
its uniform depth of 4? It produces about 125k rows in a bit over 2
hours.

=cut

# GO:0008150,GO:0005575,GO:0003674
*each_child = \&all_children;
sub all_children {
    my $self  = shift;
    my ($idReq, $nsReq, $ageReq, $doWarn)  = @_;
    my ($id)  = $self->standardize_id($idReq, $nsReq);
    return wantarray ? () : {} unless ($id);
    $self->benchstart;

    my $sql   = "SELECT child, parentage, relation FROM parentage WHERE parent = ?";
    my @binds = ($id);
    my $sthnm = "Get all children via term";
    my $dbh   = $self->dbh;
    my $age   = $self->standardize_age( $ageReq );

    if ($nsReq) {
        my $ns = $self->namespace_name( $nsReq );
        unless ($ns) {
            $self->err("Could not get all_children for unknown namespace",
                       $nsReq);
            $self->benchend;
            return wantarray ? () : {};
        }
        $sql  .= " AND ns = ?";
        push @binds, $ns;
        $sthnm .= " + namespace";
        $nsReq = $ns;
    }

    if ($age) {
        if (my $asql = $self->age_filter_sql( $age, 'updated')) {
            $sql .= " AND $asql";
        } else {
            $self->err("Failed to calculate age SQL for '$age' days");
        }
    }

    my $isFirst = 0;
    unless ($self->{ALL_CHILD_LOOP}) {
        $isFirst = 1;
        $self->{ALL_CHILD_LOOP} = {
            seen => {},
            path => [],
        };
    }
    my $acl = $self->{ALL_CHILD_LOOP};
    push @{$acl->{path}}, $id;
    my $looped = 0;
    if (++$acl->{seen}{$id} > 1) {
        my $loop = $acl->{loop} ||= {};
        my @lpath; my %seen;
        foreach my $lid (reverse @{$acl->{path}}) {
            push @lpath, $lid;
            last if ($seen{$lid}++);
        }

        $self->err("Recursive loop found while calculating children for $id".
                   ($nsReq ? " [$nsReq]" : "").". Loop:", @lpath)
            unless ($loop->{$id});
        map { $loop->{$_}++ } @lpath;
        $looped = 1;
    } elsif ($acl->{loop} && $acl->{loop}{$id}) {
        $looped = 1;
    }

    if ($looped) {
        if ($isFirst) {
            # Huh. The seed is looped with itself
            delete $self->{ALL_CHILD_LOOP};
        } else {
            pop @{$acl->{path}};
            $acl->{seen}{$id}--;
        }
        $self->benchend;
        return wantarray ? () : {};
    }

    my $sth = $dbh->prepare
        ( -sql   => $sql,
          -name  => $sthnm,
          -level => 1 );
    $sth->execute( @binds );
    my $rows = $sth->fetchall_arrayref;

    # my %rv;
    my $saveResults;
    if ($#{$rows} == -1 && $nsReq) {
        # Nothing is in the database, and we have an explicit namespace
        # The node always returns itself at distance zero
        push @{$rows}, [$id, 0, ""];
        # Get each child of the current node:
        my $kids = $self->direct_children($id, $nsReq, $age, $doWarn);
        foreach my $kidRow (@{$kids}) {
            my ($kid, $rel1) = @{$kidRow};
            push @{$rows}, [$kid, 1, $rel1];
            # Recursively call all_children for each child
            my $grKids = $self->all_children( $kid, $nsReq, $age, $doWarn );

            # Process unless a loop has been discovered:
            next if ($acl->{loop} && $acl->{loop}{$kid});
            
            while (my ($gkid, $ddA) = each %{$grKids}) {
                for (my $di = 0; $di < $#{$ddA}; $di += 2) {
                    my ($depth, $rels) = ($ddA->[$di], $ddA->[$di+1]);
                    next unless ($depth);
                    foreach my $rel2 (@{$rels}) {
                        if (my $ar = $allowedRelations->{$rel2}) {
                            unless ($rel2 = $ar->{$rel1}) {
                                next;
                            }
                        }
                        push @{$rows}, [$gkid, $depth + 1, $rel2 ];
                    }
                }
            }
        }
        $saveResults = 1;
    }
    # Build a 3D hash structure keyed by
    # Child Node
    #  Relation
    #   Minimum Observed Distance for relation
    my %relStruct;
    foreach my $row (@{$rows}) {
        my ($term, $lvl, $rrel) = @{$row};
        if (my $term = $row->[0]) {
            my ($lvl, $rrel) = ($row->[1], $row->[2] || "");
            $relStruct{$term}{$rrel} = $lvl if 
                (!$relStruct{$term}{$rrel} || $lvl < $relStruct{$term}{$rrel});
        }
    }

    # Now pivot the relation to a 1D hash structure:
    my %rv;
    while (my ($term, $rH) = each %relStruct) {
        my %lvls;
        foreach my $rel (sort keys %{$rH}) {
            my $lvl = $rH->{$rel};
            push @{$lvls{$lvl}}, $rel;
        }
        foreach my $lvl ( sort { $a <=> $b } keys %lvls ) {
            push @{$rv{$term}}, ($lvl, $lvls{$lvl});
        }
    }
    if ($saveResults) {
        my @rows;
        my $nsn  = $self->namespace_name($nsReq);
        while (my ($kid, $ddA) = each %rv) {
            for (my $di = 0; $di < $#{$ddA}; $di += 2) {
                my ($depth, $rels) = ($ddA->[$di], $ddA->[$di+1]);
                push @rows, map {{ 
                    parent    => $id,
                    child     => $kid,
                    ns        => $nsn,
                    parentage => $depth,
                    relation  => $_ }} @{$rels};
            }
        }
        my @kids = sort { $rv{$a} <=> $rv{$b} || $a cmp $b } keys %rv;
        $self->dbh->update_rows( 'parentage', \@rows );
    }
    
    if ($isFirst) {
        delete $self->{ALL_CHILD_LOOP};
    } else {
        pop @{$acl->{path}};
        $acl->{seen}{$id}--;
    }
    $self->benchend;
    return wantarray ? 
        sort { $rv{$a}[0] <=> $rv{$b}[0]  || $a cmp $b } keys %rv : \%rv;
}

=head1 RANDOM METHODS

Junk drawer of all other methods, mostly simple little utilities.

=head2 get_all_go_subsets

 Title   : get_all_go_subsets
 Usage   : my @list = $ad->get_all_go_subsets( )
 Function: Gets the three major branches of GeneOntology
 Returns : A list of three strings
 Args    : Dump level.

Simply returns a hardcoded list comprised of three strings:

  biological_process
  cellular_component
  molecular_function

=cut

sub get_all_go_subsets {
    # GO:0008150 GO:0005575 GO:0003674
    return qw(biological_process cellular_component molecular_function);
}

sub get_assigner {
    my $self = shift;
    unless ($self->tracker()) {
        unless ($self->{COMPLAIN}{ASSIGNER}++) {
            warn "!---!\nNew assignments can not be calculated.\nOnly previously calculated data will be recovered from the database.\nIf you have set an age limit, you may wish to remove it (set it to zero)\n!---!\n";
        }
        return undef;
    }
    my ($ns1, $ns2)   = @_;
    my ($tok1, $tok2) = ($self->namespace_token($ns1), 
                         $self->namespace_token($ns2) );
    unless (exists $self->{ASSIGNERS}{$tok1}{$tok2}) {
        # Try explicit NS1 -> NS2, then either 1 or 2 being 'ANY'
        my @try = ( [$tok1, $tok2],
                    ['ANY', $tok2],
                    [$tok1, 'ANY'] );
        my $meth;
        foreach my $pair (@try) {
            my $func = sprintf("assign_%s_to_%s", @{$pair});
            $meth = $self->can($func);
            last if ($meth);
        }
        $self->{ASSIGNERS}{$tok1}{$tok2} = $meth;
    }
    return $self->{ASSIGNERS}{$tok1}{$tok2};
}


sub process_filter_request {
    my $self = shift;
    my ($request, $splitter) = @_;
    my @list;
    if (my $r = ref($request)) {
        if ($r eq 'ARRAY') {
            @list = @{$request};
        } else {
            $self->err("Can not utilize '$request' as a filter request");
            return {};
        }
    } elsif ($self->_is_list_file( $request )) {
        @list = $self->list_from_request( $request );
    } else {
        $splitter ||= "[\\n\\r\\t\\,]+";
        @list = split(/$splitter/, $request);
    }
    my %hash;
    foreach my $req (@list) {
        next if (!defined $req || $req eq '');
        my $tag = "IN";
        $req =~ s/^\s+//; $req =~ s/\s+$//;
        if ($req =~ /^\!(.+)/) { $req = $1; $tag = "NOT IN"; }
        push @{$hash{$tag}}, $req;
    }
    return \%hash;
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Reducing multiplicity of some data

sub simplify_authors {
    my $self = shift;
    return @_ if ($#_ < 1);
    my %auths = map { $_ => 1 } @_;
    if (exists $auths{RefSeq}) {
        map { delete $auths{$_} } qw(LocusLink NCBI);
    } elsif (exists $auths{LocusLink}) {
        map { delete $auths{$_} } qw(NCBI);
    }
    if (exists $auths{IPI}) {
        map { delete $auths{$_} } qw(EBI EMBL);
    } elsif (exists $auths{EBI}) {
        delete $auths{EMBL};
    }
    delete $auths{SIB}  if (exists $auths{UniProt});
    if (1) {
        my @names = keys %auths;
        my $ecs = 0;
        map { $ecs++ if ($evRank->{$_}) } @names;
        if ($ecs == $#names + 1) {
            # These are all evidence codes
            return sort {$evRank->{$b} <=> $evRank->{$a} || $a cmp $b} @names;
        } else {
            return sort @names;
        }
    }
    return sort keys %auths;
}

sub compare_evidence_codes {
    my $self = shift;
    # 0 = Same relative rank
    # + = First term 'better' than second
    # - = Second term 'better' than first
    return 
        ($evRank->{uc(shift || "")} || -2) - ($evRank->{uc(shift || "")} || -2);
}

sub simplify_namespaces {
    my $self = shift;
    return @_ if ($#_ < 1);
    my %ns = map { $self->namespace_name($_) => 1 } @_;
    if (exists $ns{'RefSeq RNA'}) {
        delete $ns{'Any RNA'};
    }
    if (exists $ns{'RefSeq Protein'}) {
        delete $ns{'Any Protein'};
    }
    return sort keys %ns;
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Utility calls to get all members of some namespaces




# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# return value formating for updaters

sub format_update_rv {
    my $self = shift;
    $self->benchstart;
    my ($rows, $ns2, $ns1) = @_;
    my $rv;
    my %toMatch;
    $toMatch{ns_out} = $self->namespace_name( $ns2 ) if ($ns2);
    $toMatch{ns_in}  = $self->namespace_name( $ns1 ) if ($ns1);
    my @matchKeys    = keys %toMatch;
    if ($#matchKeys > -1) {
        # Only keep rows matching namespace request
        $rv = [];
        foreach my $row (@{$rows}) {
            my $keep = 1;
            for my $i (0..$#matchKeys) {
                my $mkey = $matchKeys[$i];
                unless ($row->{$mkey} eq $toMatch{$mkey}) {
                    $keep = 0;
                    last;
                }
            }
            push @{$rv}, $row if ($keep);
        }
    } else {
        $rv = $rows;
    }
    if (wantarray) {
        $self->benchend;
        return (map { $_->{term_out} } @{$rv});
    } else {
        my @rv;
        my @rvmap = @{$rvcols->{convert}};
        foreach my $row (@{$rv}) {
            push @rv, [ map { $row->{$_} } @rvmap ];
        }
        $self->benchend;
        return \@rv;
    }
}

sub format_assignment_rv {
    my $self = shift;
    my ($rows, $ns2) = @_;
    my @rv;
    my @rvmap = @{$rvcols->{assign}};
    foreach my $row (@{$rows}) {
        push @rv, [ map { $row->{$_} } @rvmap ];
    }
    return \@rv;
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Just make a duplicate of the rows for another conversion

sub copy_conversion {
    my $self = shift;
    my ($id, $ns1, $ns2, $fromID, $fromNs1, $fromNs2, $age, $tok) = @_;
    $fromID  ||= $id;
    $fromNs1 ||= $ns1;
    $fromNs2 ||= $ns2;

    my $nsIn      = $self->namespace_name( $ns1 );
    my $nsOut     = $self->namespace_name( $ns2 );
    my $arr       = $self->convert
        ( -id => $fromID, -ns1 => $fromNs1, -ns2 => $fromNs2, -age => $age,
          -tracetok => $tok, -xtraname => 'Copy');

    my @rv;
    foreach my $row (@{$arr}) {
        push @rv, { term_in    => $id,
                    term_out   => $row->[0],
                    ns_in      => $nsIn,
                    ns_out     => $nsOut,
                    auth       => $row->[2],
                    matched    => $row->[3],
                    ns_between => $row->[4] };
    }
    return wantarray ? @rv : \@rv;
}

# Combine several conversions together
sub merge_conversions {
    my $self = shift;
    my ($id, $ns1, $ns2, $params, $age) = @_;
    my @merged;
    foreach my $param (@{$params}) {
        # Each param = [ fromID, fromNs1, fromNs2 ]
        $self->death("merge_conversion parameter does not have 3 elements: ",
                     join(',', map {"'$_'"} @{$param})) if ($#{$param} != 2);
        my $rv = $self->copy_conversion($id, $ns1, $ns2, @{$param}, $age);
        push @merged, @{$rv};
    }
    return \@merged;
}

sub filter_conversion {
    my $self = shift;
    my ($id, $ns1, $ns2, $filtNS, $age, $tok) = @_;
    # warn "FILTER REQUEST: $id [$ns1] -> [$ns2] -filter-> [$filtNS] {$age days}\n";
    my $priRows = $self->convert
        (-id => $id, -ns1 => $ns1, -ns2 => $ns2, -age => $age,
         -tracetok => $tok, -xtraname => 'Filter');
    my @cs = @{$rvcols->{convert}};
    my $nsOut = $self->namespace_name( $filtNS );
    my @rows;
    # warn "".($#{$priRows}+1)." primary rows:\n".$self->text_table($priRows);
    foreach my $row (@{$priRows}) {
        my $oid = $row->[0];
        unless ($oid && $self->verify_namespace($oid, $filtNS)) {
            # warn "$id [$ns1] -> ".($oid || '-UNDEF-')." [$ns2] IS NOT [$filtNS] \n";
            next;
        }
        my ($oid2, $oseq) = $self->standardize_id
            ($oid, $filtNS, 'CheckClass');
        # warn "$id [$ns1] -> [$ns2] -filter-> [$filtNS] = $oid2 ? ".($oseq ? 'PASS' : 'FAIL')."\n";
       # warn "   $oid2, $oseq";
        next unless ($oseq);
        my %hash = map { $cs[$_] => $row->[$_] } (0..$#cs);
        $hash{ns_out} = $nsOut;
        push @rows, \%hash;
#        if ($self->{CONV_CACHE} && $self->{CONV_CACHE}{WARN} =~ /bench/i) {
#            if (time - $self->{BENCHITER} > 10) {
#                warn $self->showbench();
#                $self->{BENCHITER} = time;
#            }
#        }
    }
    return @rows;
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Computational logic for building and collapsing chains

sub chain_conversions {
    my $self = shift;
    my $args     = $self->parseparams( @_ );
    my $idReq    = $args->{ID};
    my $chain    = $args->{CHAIN};
    my $age      = $self->standardize_age( $args->{AGE} );
    my $guessOut = $args->{GUESSOUT}; # The method should guess output NS
    my $aug      = $args->{AUGMENT};  # Array of rows that are being augmented
    my $allScore = $args->{ALLSCORE} || $args->{ALLSCORES};
    my $minScore = $args->{MINSCORE} || [];
    my $nsIn     = $self->namespace_name( $chain->[0] );
    my $nsOut    = $self->namespace_name( $chain->[-1] );
    my $trim     = $args->{TRIM};
    my $noloop   = $args->{NOLOOP} ? {} : undef;

    return () unless ($idReq);
    unless ($nsIn && $nsOut) {
        $self->death("Chain failed: Could not get both start ($chain->[0]) ".
                     " and end ($chain->[-1]) namespaces");
    }
    my ($id) = $self->standardize_id( $idReq, $nsIn );
    return () unless ($id);

    my $fullKey = join('-', $id, map { $self->ns_token($_) } ($nsIn, $nsOut));
    my %flags;
    if (my $fl = $args->{FLAGS}) {
        %flags = map { $_ => 1 } map { uc($_) } @{$fl}
    }    

    if (my $nsr = $args->{NOSTRICT}) {
        my @list = ref($nsr) ? @{$nsr} : ($nsr);
        my %nostrict;
        foreach my $ns (@list) {
            if (my $tok = $self->namespace_token($ns)) {
                $nostrict{$tok} = 1;
            }
        }
        delete $flags{STRICT} if ($nostrict{ $chain->[-1] });
    }

    my (%guessInternal, %guessBtwn, %filterBtwn, $scoreCluster);
    if ($args->{GUESSIN}) {
        my $gns1 = $self->guess_namespace($id);
        $nsIn    = $self->namespace_name($gns1) if ($gns1);
    }
    if (my $guessAny = $args->{GUESSANY}) {
        # If any internal hit has one of these namespaces, try to
        # guess a better namespace for it. Useful when pivoting off of
        # "Any Protein" or "Any RNA"
        my @list = ref($guessAny) ? @{$guessAny} : ($guessAny);
        map { $guessInternal{ $self->namespace_token($_) } = 1 } @list;
    }
    if (my $guessB = $args->{GUESSBTWN}) {
        # Similar to above, but guessing should only be used when
        # reporting the internal namespace - it will NOT be used to
        # set the namespace for subsequent chained conversions.  The
        # result is that a more meaningful namespace will be shown in
        # the ns_between, but the original (presumably generic)
        # namespace will be used for the next stage of chaining.
        my @list = ref($guessB) ? @{$guessB} : ($guessB);
        map { $guessBtwn{ $self->namespace_token($_) } = 1 } @list;
    }
    if (my $filter = $args->{FILTER}) {
        while (my ($nsOrig, $filts) = each %{$filter}) {
            $nsOrig  = $self->namespace_token($nsOrig);
            my @list = ref($filts) ? @{$filts} : ($filts);
            my %fil  = map { $self->namespace_token($_) => 1 } @list;
            delete $fil{''};
            $filterBtwn{$nsOrig} = [ keys %fil ];
        }
    }
    if ($allScore && ref($allScore)) {
        # Keeping all scores after clustering within certain authorities
        $scoreCluster = [map { lc($_) } @{$allScore}];
    }

    # This is a seed entry to initiate the chain
    my $input = [ {
        term_out => $id,
        ns_out   => $nsIn,
        matched  => 1,
    } ];

    my $tdat = $self->{TRACE_DATA};
    if ($tdat) {
        my @bits = map { ref($_) ? join("/", @{$_}) : $_ } @{$chain};
        push @{$tdat->{NOW}[-1]{chain}}, \@bits;
    }
    for my $i (1..$#{$chain}) {
        # What namespace(s) are we converting to?
        my $onReq = $chain->[$i];
        my @ons   = map { $self->namespace_token($_) }
        ref($onReq) ? @{$onReq} : ($onReq);
        my @complex;
        my $isInternal = ($i == $#{$chain}) ? 0 : 1;

        # %chainCache is new (28 Aug 2009) and is designed to prevent
        # recursion (and false recursion errors in convert() ).
        # It may introduce problems??
        # 20 Apr 2010 - Yes, yes it did. The cache can be used, but because
        # the data structures returned by it get modified later, they must
        # be fully cloned, rather than using the references as-is
        my %chainCache;
        foreach my $data (sort {$a->{term_out} cmp $b->{term_out}} @{$input}) {
            my $prior = $data->{term_out};
            my @hist  = @{$data->{history} || []};
            unshift @hist, $prior;
            my $ns1   = $self->namespace_token($data->{ns_out});
            # Upgrade the namespace for both display AND calculation:
            #($ns1) = $self->guess_namespace($prior, $ns1) if
            #    ($guessInternal{$ns1} && $i > 1);
            my @found;
            foreach my $ns2 (@ons) {
                # warn "CHAINING: $prior [$ns1] -> [$ns2]\n";
                my $rows = $chainCache{"$prior-$ns1-$ns2"} ||= $self->convert
                    ( -id => $prior, -ashash => 1, -nonull => 1,
                      -tracetok => $i == 1 ? '+' :  undef,
                      -xtraname => 'Chain',
                      -ns1 => $ns1, -ns2 => $ns2, -age => $age  );
                # warn "\n$prior [$ns2]"; map { warn &_show_row($_) } @{$rows};
                if (my $kF = $filterBtwn{$ns2}) {
                    my @filt;
                    foreach my $row (@{$rows}) {
                        if (my $tout = $row->{term_out}) {
                            my $gns    = $self->guess_namespace($tout);
                            my $ok   = 0;
                            for my $k (0..$#{$kF}) {
                                next unless 
                                    ($self->is_namespace($gns,$kF->[$k]));
                                $ok = $kF->[$k]; last;
                            }
                            if ($ok) {
                                # $row->{ns_out} = $self->namespace_name($ok);
                                # warn "  $tout = $ok";
                                push @filt, $row;
                            }
                        }
                    }
                    $rows = \@filt;
                }
                # Because we are using a cached data structure, we MUST
                # clone (dereference then re-reference) the row data:
                push @found, map { { %{$_} } } @{$rows};
            }
            if (my $ms = $minScore->[$i-1]) {
                my @keep;
                foreach my $row (@found) {
                    push @keep, $row if ($row->{matched} && 
                                         $row->{matched} >= $ms);
                }
                @found = @keep if ($#keep < $#found);
            }
            if ($noloop) {
                my @keep;
                $noloop->{$prior} = 1;
                foreach my $row (@found) {
                    push @keep, $row unless
                        ($noloop->{$row->{term_out} || ''});
                }
                @found = @keep if ($#keep < $#found);
            }
            next if ($#found == -1);
            if ($i > 1) {
                # Upgrade the namespace for display ONLY:
                if ($guessBtwn{$ns1} && !$guessInternal{$ns1}) {
                    my ($gns) = $self->guess_namespace($prior, $ns1);
                    if ($gns ne $ns1) {
                        my $nsn = $self->namespace_name($gns);
                        map { $_->{ns_in} = $nsn } @found ;
                    }
                }
                my @bits = ( $data->{ns_out} );
                push @bits, $data->{ns_between} if ($data->{ns_between});
                my $btwn = join(' < ', @bits);
                # Extend ns_between if appropriate
                map { $_->{ns_between} = $_->{ns_between} ?
                      $_->{ns_between} . " < $btwn" : $btwn } @found;
            }

            my ($baseAuth, $baseMatch) = map { $data->{$_} } qw(auth matched);
            # Extend authority chain if appropriate:
            map { $_->{auth} .= " < $baseAuth" } @found if ($baseAuth);
            # Calculate new scores:
            if (defined $baseMatch) {
                map { $_->{matched} = defined $_->{matched} ?
                      $_->{matched} * $baseMatch : undef } @found;
            } else {
                map { $_->{matched} = undef } @found;
            }
            map { $_->{history} = \@hist } @found;
            if ($isInternal) {
                my %hmap = map { $_ => 1 } @hist;
                foreach my $row (@found) {
                    push @complex, $row unless ($hmap{ $row->{term_out} });
                }
            } else {
                push @complex, @found;
            }
        }
        my @simple = $self->simplify_rows
            ( -rows       => \@complex,
              -show       => $args->{SHOW},
              -allscore   => $allScore,
              -isinternal => $isInternal,
              -cluster    => $scoreCluster,
              );
        $input = \@simple;
        # Exit if the chain has collapsed:
        last if ($#simple == -1);
    }

    my @rv;
    unless ($#{$input} == -1) {
        # Strip out empty rows or redundant rows
        my %skip = map { $_->{term_out} || '' => 1 } @{$aug || []};
        $skip{$id} = 1 if ($flags{NOSELF});
        my @keep;
        foreach my $row (@{$input}) {
            my $id2 = $row->{term_out};
            next if (!$id2 || $skip{$id2});
            push @keep, $row;
        }
        $input = \@keep;

        my @bounds = (['auth', 500], ['ns_between', 1000]);
        foreach my $row (@{$input}) {
            foreach my $bdat (@bounds) {
                my ($key, $len) = @{$bdat};
                my $v = $row->{$key};
                $row->{$key} = substr($v, 0, $len-3).'...'
                    if (length($v) > $len);
            }
            if (defined $row->{matched} && $row->{matched}) {
                # This is a defined, non-zero value
                if ($flags{STRICT} && $row->{auth} =~ /\[(SIM|RBM)\]/) {
                    # Strict paths that use similarity values are degraded
                    # unless they are perfect:
                    $row->{matched} = undef unless ($row->{matched} == 1);
                } else {
                    # Round to 4 decimals, do not round to zero
                    $row->{matched} = int
                        (0.5 + 10000 * $row->{matched}) / 10000 || 0.0001;
                }
            }
            $row->{term_in} = $id;
            $row->{ns_in}   = $nsIn;
        }

        @rv = $self->simplify_rows
            ( -rows       => $input,
              -allscore   => $allScore,
              -bestmid    => $args->{BESTMID},
              -show       => 0,
              -shrinkauth => $args->{SHRINKAUTH},
              -cluster    => $scoreCluster, );
    }

    if ($#rv == -1 && !($aug || $args->{NONULL})) {
        @rv = ( { term_in  => $id,
                  ns_in    => $nsIn,
                  ns_out   => $nsOut, } );
    }
    return @rv;
}

sub _show_row {
    my $row = $_[0];
    if ($row->{term_out}) {
        my $msg = sprintf("%s [%s] -> %s [%s] %s (%s)", 
                          map { defined $row->{$_} ? $row->{$_} : ''} 
                          qw(term_in ns_in term_out ns_out matched auth));
        if (my $via = $row->{ns_between}) { $msg .= " via $via" }
        if (my $hist = $row->{history}) { $msg .= " : ".join(' < ', @{$hist}) }
        return "$msg\n";
    } else {
        return sprintf("%s [%s] -> n/a [%s]\n", map { $row->{$_} } 
                     qw(term_in ns_in ns_out));
    }
}

our $filterCB = {
    maxmid     => \&_filter_maximum_internal_namespace,
    bestmid    => \&_filter_shortest_internal_namespace,
    nomid      => \&_filter_exclude_internal_namespaces,
    bestscore  => \&_filter_keep_best_score,
    shortauth  => \&_filter_keep_shortest_authority,
    internal   => \&_filter_is_internal,
    cluster    => \&_filter_cluster_scores,
    shrinkauth => \&_filter_shrink_authorities,
};

sub simplify_rows {
    my $self = shift;
    my $args      = $self->parseparams( @_ );
    my $rows      = $args->{ROWS} || [];
    return () if ($#{$rows} == -1);
    $self->benchstart;

    if (my $scReq = $args->{CLUSTER}) {
        # We are going to cluster scores around specific authorities
        # We will need to maintain all scores during processing
        my @sc = ref($scReq) ? @{$scReq} : ($scReq);
        map { $_ = lc($_) } @sc;
        $args->{scoreCluster} = \@sc;
        $args->{ALLSCORE} ||= 1;
    } elsif ($args->{ALLSCORE} && ref($args->{ALLSCORE})) {
        # Keeping all scores after clustering within certain authorities
        $args->{scoreCluster} = [map { lc($_) } @{$args->{ALLSCORE}}];
    }
    my $ns1OverRide = $args->{NS1};
    my $ns2OverRide = $args->{NS2};

    my %data;
    my $avgScore = $args->{AVGSCORE};
    my $rn       = $#{$rows} + 1;
    foreach my $row (@{$rows}) {
        my $t2 = $row->{term_out};
        next unless ($t2);
        my ($t1, $ns1, $ns2, $btwn) =
            ( $row->{term_in},
              $ns1OverRide || $row->{ns_in},
              $ns2OverRide || $row->{ns_out},
              $row->{ns_between} );

        my $score = defined $row->{matched} ? $row->{matched} : -1;
        my @auths = split(' < ', $row->{auth} || 'Unknown');
        my $pauth = shift @auths;
        my $bauth = join(' < ', @auths) || 0;
        my $dtarg = $data{$t1}{$ns1}{$t2} ||= {};
        if ($avgScore) {
            if ($score == -1) {
                $dtarg->{NULL}++;
            } else {
                my $key   = join("\t", $bauth, $pauth, $btwn || 0, $ns2);
                $dtarg->{keys}{$key} += $score;
                $dtarg->{num}++;
            }
        } else {
            #                      0       1       2       3            4
            my $key   = join("\t", $score, $bauth, $pauth, $btwn || 0, $ns2);
            # warn "$key";
            $dtarg->{$key}++;
        }
    }
    if ($avgScore && $rn) {
        while (my ($t1, $nsH) = each %data) {
            while (my ($ns1, $t2H) = each %{$nsH}) {
                while (my ($t2, $dtarg) = each %{$t2H}) {
                    my $newTarg = {};
                    if ($dtarg->{num}) {
                        # There were at least some scored rows
                        while (my ($key, $scTot) = each %{$dtarg->{keys}}) {
                            my $avgSc = int(0.5 + 1000 * $scTot / $rn) / 1000;
                            $avgSc  ||= 0.0001;
                            $newTarg->{"$avgSc\t$key"} = 1;

                            # This treats any undef as zero. I think
                            # this is the best option for an otherwise
                            # complex situation

                        }
                    } else {
                        # All assignments were undef
                        foreach my $key (keys %{$dtarg->{keys}}) {
                            $newTarg->{"-1\t$key"} = 1;
                        }
                    }
                    $t2H->{$t2} = $newTarg;
                }
            }
        }
    }
    if (my $noMid = $args->{NOMID}) {
        $args->{noMid} = {map { $self->namespace_name($_) => 1 } @{$noMid}};
    }
    my $dumpTerm = 'LOC12391';

    my (@filters, @filtNames);
    if (my $fReq = $args->{FILTERS} || $args->{FILTER}) {
        my @reqs = ref($fReq) ? @{$fReq} : split(/\s*[\s\,]\s*/, $fReq);
        foreach my $req (@reqs) {
            next unless ($req);
            my $cb;
            if (my $r = ref($req)) {
                $cb = $req if ($r eq 'CODE');
                push @filtNames, "CUSTOM";
            } else {
                $cb = $filterCB->{lc($req)};
                push @filtNames, $req;
            }
            if ($cb) {
                push @filters, $cb;
            } else {
                $self->death("Unknown simplify filter '$req'");
            }
        }
    } else {
        push @filtNames, 'maxmid'     if ($args->{MAXMID});
        push @filtNames, 'bestmid'    if ($args->{BESTMID});
        push @filtNames, 'nomid'      if ($args->{NOMID});
        push @filtNames, 'bestscore'  unless ($args->{ALLSCORE});
        push @filtNames, 'shortauth'  unless ($args->{NOTEXT});
        push @filtNames, 'internal'   if ($args->{ISINTERNAL});
        push @filtNames, 'cluster'    if ($args->{scoreCluster});
        push @filtNames, 'shrinkauth' if ($args->{SHRINKAUTH});
        push @filters, map { $filterCB->{$_} } @filtNames;
    }

    my @simplified;
    while (my ($t1, $hash1) = each %data) {
        while (my ($ns1, $tHash) = each %{$hash1}) {
            while (my ($t2, $keyDat) = each %{$tHash}) {
                my @raw  = map { [ split("\t", $_) ] } keys %{$keyDat};
                my $targ = \@raw;

                # Now apply all filters specified by the user
                for my $i (0..$#filters) {
                    last if ($#{$targ} == -1);
                    $targ = &{$filters[$i]}($self, $targ, $args);
                }
                # Skip this target if no rows survive filters
                next if ($#{$targ} == -1);

                foreach my $dat (@{$targ}) {
                    my $auth = $dat->[1] ? 
                        $dat->[2] .' < '. $dat->[1] : $dat->[2];
                    push @simplified, {
                        term_in    => $t1,
                        term_out   => $t2,
                        ns_in      => $ns1,
                        ns_out     => $dat->[4],
                        auth       => $auth,
                        matched    => ($dat->[0] < 0) ? undef : $dat->[0],
                        ns_between => $dat->[3] || '',
                    };
                }
            }
        }
    }

    if ($args->{SHOW}) {
        my $ti  = time;
        my $txt = "\n-- SIMPLIFY $ti --\n";
        $txt .= "INPUT:\n"; 
        map { $txt .= '  '. &_show_row($_) } @{$rows};
        $txt .= "\n";
        $txt .= "STRUCTURE:\n"; 
        foreach my $t1 (sort keys %data) {
            foreach my $ns1 (sort keys %{$data{$t1}}) {
                $txt .= "$t1 [$ns1]\n";
                my %t2s;
                while (my ($t2, $keyDat) = each %{$data{$t1}{$ns1}}) {
                    my @raw  = map { [ split("\t", $_) ] } keys %{$keyDat};
                    foreach my $row (@raw) {
                        my ($sc, $ba, $pa, $btwn, $ns2) = @{$row};
                        push @{$t2s{"$t2 [$ns2]"}}, 
                        [$sc,  $ba ? "$pa < $ba" : $pa, $btwn || '---'];
                    }
                }
                foreach my $t2 (sort keys %t2s) {
                    $txt .= "  $t2\n";
                    $txt .= join('',map {sprintf("    [%d] %s <%s>\n", @{$_})}
                                 sort { $b->[0] <=> $a->[0] } @{$t2s{$t2}});
                }
            }
        }
        $txt .= "\nOUTPUT:\n"; 
        map { $txt .= '  '. &_show_row($_) } @simplified;
        $txt .= "-- SIMPLIFY $ti --\n";
        warn "$txt\n";
    }
    $self->benchstop;
    return @simplified;
}

sub _filter_maximum_internal_namespace {
    # We are only keeping rows with a maximum number
    # of internal namespaces
    my ($self, $targ, $args) = @_;
    my $maxMid = $args->{MAXMID};
    my @keep;
    foreach my $row (@{$targ}) {
        my @mids = split(' < ', $row->[3]);
        push @keep, $row if ($#mids < $maxMid);
    }
    return \@keep;
}

sub _filter_shortest_internal_namespace {
    # We want to keep the shortest paths available
    my ($self, $targ, $args) = @_;
    my @sorter;
    foreach my $row (@{$targ}) {
        if ($row->[3]) {
            my @mids = split(' < ', $row->[3]);
            push @sorter, [$row, $#mids];
        } else {
            push @sorter, [$row, -1];
        }
    }
    @sorter = sort { $a->[1] <=> $b->[1] } @sorter;
    my @keep;
    my $best = $sorter[0][1];
    foreach my $sdat (@sorter) {
        last if ($sdat->[1] > $best);
        push @keep, $sdat->[0];
    }
    return \@keep;
}

sub _filter_exclude_internal_namespaces {
    # Exclude any rows that have an internal namespace matching the filter
    my ($self, $targ, $args) = @_;
    my @keep;
    my $noMid = $args->{noMid};
    foreach my $row (@{$targ}) {
        my $hit  = 0;
        map { $hit++ if ($noMid->{$_}) } split(' < ', $row->[3]);
        push @keep, $row unless ($hit);
    }
    return \@keep;
}

sub _filter_keep_best_score {
    my ($self, $targ, $args) = @_;
    # We are only keeping the best score
    my @sorted = sort { $b->[0] <=> $a->[0] } @{$targ};
    my @keep;
    foreach my $row (@sorted) {
        last if ($row->[0] < $sorted[0][0]);
        push @keep, $row;
    }
    return \@keep;
}

sub _filter_keep_shortest_authority {
    my ($self, $targ, $args) = @_;
    # First we are going to clean up the 'base' authority:
    my @allBase = $self->simplify_authors( map { $_->[1] } @{$targ} );
    unless ($args->{ALLSCORE}) {
        # Keep only one base authority
        my ($mostCompact) = sort {
            $#{$a->[1]}         <=> $#{$b->[1]} ||
                length($a->[1]) <=> length($b->[1])
            } map {
                [ $_, [split(' < ', $_)]]
                } @allBase;
        @allBase = ($mostCompact->[0]);
    }
    my %keepBase = map { $_ => 1 } @allBase;
    my @keepB;
    foreach my $row (@{$targ}) {
        push @keepB, $row if ($keepBase{ $row->[1] });
    }

    # Then we clean up the 'primary' authority
    my @allPri = $self->simplify_authors( map { $_->[2] } @{$targ} );
    unless ($args->{ALLSCORE}) {
        # Keep only one primary authority
        my ($mostCompact) = sort {
            $#{$a->[1]}         <=> $#{$b->[1]} ||
                length($a->[0]) <=> length($b->[0])
            } map {
                [ $_, [split(' < ', $_)]]
                } @allPri;
        @allPri = ($mostCompact->[0]);
    }
    my %keepPri = map { $_ => 1 } @allPri;
    my @keepP;
    foreach my $row (@{$targ}) {
        push @keepP, $row if ($keepPri{ $row->[2] });
    }
    return \@keepP if ($args->{ALLSCORE});

    # Clean up the between namespaces - take the smallest one
    my %allBNS = map { $_->[3] => 1 } @{$targ};
    my (@simpleNS, @bnss);
    foreach my $ns3 (keys %allBNS) {
        if ($ns3 =~ /\</) {
            push @bnss, $ns3;
        } else {
            push @simpleNS, $ns3;
        }
    }
    push @bnss, $self->simplify_namespaces(@simpleNS);
    my ($bns) = sort { length($a) <=> length($b) } @bnss;
    $bns ||= 0;
    my @keep3;
    foreach my $row (@{$targ}) {
        push @keep3, $row if ($row->[3] eq $bns);
    }
    return \@keep3;
}

sub _filter_is_internal {
    my ($self, $targ, $args) = @_;
    # This is part of an internal conversion
    # Keep only one namespace
    my ($single) = $self->simplify_namespaces( map { $_->[4] } @{$targ} );
    $single = $self->namespace_name($single);
    my @keep;
    foreach my $row (@{$targ}) {
        push @keep, $row if ($row->[4] eq $single);
    }
    return \@keep;
}

sub _filter_cluster_scores {
    my ($self, $targ, $args) = @_;
    # We will keep all entries that are not explicitly
    # part of a cluster
    my (%clust, %multiclust, @keep);
    my $scCluster = $args->{scoreCluster};
    my $anonClust = $args->{ANONCLUSTER};
    for my $r (0..$#{$targ}) {
        my $row  = $targ->[$r];
        my $comp = lc($row->[2]);
        $comp   .= ' < ' . lc($row->[1]) if ($row->[1]);
        my @cbits = split(/ \< /, $comp);
        # The number of authorities on this edge:
        my $anum = $#cbits + 1;
        # The character length of all authorities concatenated:
        my $alen = length($comp);

        if ($args->{CLUSTERLEFT}) {
            # Only use the 'left-most' authority to cluster
            $comp = $cbits[-1];
        }
        # Cycle through each authority
        foreach my $atag (@{$scCluster}) {
            next unless ($comp =~ /$atag/);
            my $cl = $clust{$atag} ||= [];
            push @{$cl}, [$r, $anum, $alen];
            push @{$multiclust{$r}}, $cl;
        }
        unless ($multiclust{$r}) {
            # Was not assigned to any of the clusters
            if ($anonClust) {
                # We want anonymous clusters (not specifically defined) to
                # be considered as their own single cluster
                push @{$clust{$anonClust}}, [$r, $anum, $alen];
            } else {
                push @keep, $row;
            }
        }
    }

    # We do not want rows to be present in multiple clusters
    while (my ($r, $cls) = each %multiclust) {
        next unless ($#{$cls} > 0);
        # This row is present in two or more clusters
        # We will leave it only in the largest cluster
        my @sorted = sort { 
            $#{$b} <=> $#{$a} 
        } @{$cls};
        # Keep the first cluster
        shift @sorted;
        # For the remainder, find the index of the row
        # and remove it
        foreach my $cl (@sorted) {
            for my $ri (0..$#{$cl}) {
                next unless ($cl->[$ri][0] == $r);
                splice(@{$cl}, $ri, 1);
                last;
            }
        }
    }

    my %indices;
    # Now find the best row for each cluster:
    foreach my $cl (values %clust) {
        # If this is a completely depleted cluster, ignore
        next if ($#{$cl} == -1);
        # Within each authority cluster, 
        # keep the most 'compact' high score
        my ($best) = sort {
            $targ->[$b->[0]][0] <=> $targ->[$a->[0]][0] || 
                $a->[1]         <=> $b->[1] ||
                $a->[2]         <=> $b->[2]
            } @{$cl};
        $indices{ $best->[0] } = 1;
    }
    # Finally, resolve the kept indices back to rows:
    push @keep, map { $targ->[$_] } sort { $a <=> $b } keys %indices;
    return \@keep;
}

sub _filter_shrink_authorities {
    my ($self, $targ, $args) = @_;
    my %clust;
    # %clust will organize rows by 
    # 1. the non-author components of the row (eg terms, ns, etc)
    # 2. the number of authors they have
    foreach my $dat (@{$targ}) {
        my @auths = ($dat->[2]);
        push @auths, split(/ \< /, $dat->[1]) if ($dat->[1]);
        my $key = join("\t", $dat->[0], $dat->[3], $dat->[4]);
        push @{$clust{$key}{$#auths}}, \@auths;
    }
    my @keep;
    while (my ($key, $szs) = each %clust) {
        # @common holds the common row elements for this author-number
        # cluster, with placeholders added for the two author components
        my @common = split("\t", $key);
        splice(@common, 1, 0, '', '');
        # We are now going to collapse the authors for this row.
        # If we had three rows with authors:
        # Bob < Ted < Amy
        # Al  < Ted < Amy
        # Bob < Ted < Jim
        # ... we would end up with one row reading:
        # Al + Bob < Ted < Amy + Jim
        while (my ($sz, $alist) = each %{$szs}) {
            my @observed = map { {} } (0..$sz);
            foreach my $list (@{$alist}) {
                for my $l (0..$sz) {
                    map { $observed[ $l ]{$_} = 1 }
                    split(/\Q$authJoiner\E/, $list->[$l]);
                }
            }
            map { $_ = join($authJoiner, keys %{$_}) } @observed;
            my @row = @common;
            $row[2] = shift @observed;
            $row[1] = join(' < ', @observed) || '';
            push @keep, \@row;
        }
    }
    return \@keep;
}


sub best_aliases {
    my $self = shift;
    my ($id, $ns, $age) = @_;
    if (!defined $self->{CACHE}{ALIAS}{$ns}{$id} &&
        $self->verify_namespace($id, $ns)) {
        $self->clear_caches() if (++$self->{CACHE_COUNT} > $self->{MAXCACHE} );
        my $rows = $self->convert
            ( -id => $id, -ns1 => $ns, -ns2 => 'RS', -age => $age);
        my %hits;
        foreach my $row (@{$rows}) {
            my ($oid, $score) = ($row->[0], $row->[3]);
            next unless ($oid);
            $score = -1 unless (defined $score);
            $hits{$score}{$oid}++;
        }
        my ($best)  = sort { $b <=> $a } keys %hits;
        $self->{CACHE}{ALIAS}{$ns}{$id} = $best ?
            [ sort keys %{$hits{$best}} ] : [];
    }
    return @{$self->{CACHE}{ALIAS}{$ns}{$id} || []};
}

sub go_alias {
    my $self = shift;
    my ($go, $age) = @_;
    unless (defined $self->{CACHE}{GO_ALIAS}{$go}) {
        $self->clear_caches() if (++$self->{CACHE_COUNT} > $self->{MAXCACHE});
        my $ali = "";
        if ($go =~ /^GO\:\d{7}$/i) {
            ($ali) = $self->convert
                ( -id => $go, -ns1 => 'GO', -ns2 => 'RS', -age => $age);
        }
        $self->{CACHE}{GO_ALIAS}{$go} = $ali;
    }
    return $self->{CACHE}{GO_ALIAS}{$go};
}


sub _ncbi_from_affy {
    my $self = shift;
    my ($id, $ns, $age) = @_;
    my $locs = $self->convert
        ( -id => $id, -ns1 => $ns, -ns2 => "LL", -age => $age);
    my $prot = $self->convert
        ( -id => $id, -ns1 => $ns, -ns2 => "AP", -age => $age);
    return (@{$locs}, @{$prot});
}

sub update_indirect_genomic_mappings {
    my $self = shift;
    my ($id, $qns, $db, $indNS, $age, $doWarn) = @_;
    if ($indNS =~ /^\*(.+)/) {
        return $self->update_intersected_genomic_mappings
            ($id, $qns, $db, $1, $age, $doWarn);
    }
    $self->bench_start();
    my @other = $self->convert
        ( -id => $id, -ns1 => $qns, -ns2 => $indNS,
          -age => $age, -warn => $doWarn);
    
    my $qnsName = $self->namespace_name($qns);
    my @mapColOrd = qw(sub score auth strand sub_start sub_end sub_vers sub_set
                       qry_species sub_ns sub_ft);
    my %struct;
    foreach my $oid (@other) {
        my $maps = $self->mappings
            ( -id => $oid, -ns => $indNS, -db => $db,
              -cols => \@mapColOrd,
              -age => $age, -warn => $doWarn );
        foreach my $map (@{$maps}) {
            my ($sid, $sc, $auth, $str, $ss, $se, $sv, 
                $set, $tax, $sns, $sft) = 
                    map { defined $_ ? $_ : "" } @{$map};
            next unless ($sv);
            my $key = join("\t",  $sns, $tax, $sv, $sid, $str);
            my $dat = $struct{$key} ||= {
                'sub'       => $sid,
                'sub_ns'    => $sns,
                'sub_set'   => $set,
                'sub_vers'  => $sv,
                locs        => [],
                qry         => $id,
                qry_end     => 0,
                qry_ft      => '',
                qry_ns      => $qnsName,
                qry_species => $tax,
                qry_start   => 0,
                strand      => $str,
            };
            next unless ($sft);

            my @locs = sort { $a->[0] <=> $b->[0] } map
            { [ split(/\.\./, $_) ] } split(/,/, $sft);
            # The feature table cell is only 4000 chars wide, we may have lost
            # terminal coordinates
            # We may have accidentally trimmed off the end of an HSP:
            $locs[-1][1] ||= $locs[-1][0];
            unshift @locs, [$ss,$ss] unless ($locs[0][0] <= $ss);
            push @locs,    [$se,$se] unless ($locs[-1][1] >= $se);
            $sc = -1 unless (defined $sc);
            push @{$dat->{locs}}, [ \@locs, [$auth], [$sc] ];
        }
    }
    # Now collapse overlaps into each other
    my @rows;
    foreach my $dat (values %struct) {
        # Organize the regions by start coordinate
        my @ldats = sort { $a->[0][0][0] <=> $b->[0][0][0] } @{$dat->{locs}};
        my @collapsed;
        if ($#ldats == -1) {
            push @collapsed, [];
        } else {
            # Seed the stack:
            push @collapsed, shift @ldats;
            foreach my $ndat (@ldats) {
                # $ndat = "Now" information, the more distant data
                # $pdat = "Prior" information, the earlier one
                my $pdat = $collapsed[-1];
                if ($ndat->[0][0][0] > $pdat->[0][-1][1]) {
                    # No overlap between this feature and the prior one
                    # Add this feature as a new location
                    push @collapsed, $ndat;
                    next;
                }
                # There is overlap, we need to combined them
                # For non location data, just stuff them together
                for my $i (1..$#{$ndat}) {
                    push @{$pdat->[$i]}, @{$ndat->[$i]};
                }
                # We need to do a coordinate merge for the location data
                my @all = sort { 
                    $a->[0] <=> $b->[0] } (@{$ndat->[0]}, @{$pdat->[0]});
                my @merged = (shift @all);
                while (my $next = shift @all) {
                    my $pe = $merged[-1][1]; # The end coord of last loc
                    if ($next->[0] > $pe) {
                        # This location does not overlap with the prior one
                        # Add it as a new location
                        push @merged, $next;
                    } elsif ($next->[1] > $pe) {
                        # This location extends the end coordinate
                        $merged[-1][1] = $next->[1];
                    }
                }
                # Reset the location range
                $pdat->[0] = \@merged;
            }
        }
        # Now turn the collapsed regions into rows for the table
        foreach my $cdat (@collapsed) {
            my %row   = %{$dat};
            delete $row{locs};
            my ($locs, $auths, $scs) = @{$cdat};
            if ($locs) {
                my ($sc)  = sort { $b <=> $a } @{$scs};
                my %ahash = map { $_ => 1 } @{$auths};
                my @fLocs = map { $_->[0] .'..'.$_->[1] } @{$locs};
                my @ft    = (join(',', $row{strand} && $row{strand} < 0 ?
                                  reverse @fLocs : @fLocs));
                &_truncate_feature_table_strings( \@ft );
                $row{sub_start} = $locs->[0][0];
                $row{sub_end}   = $locs->[-1][1];
                $row{sub_ft}    = $ft[0];
                $row{auth}      = join($authJoiner, $self->simplify_authors
                                       (keys %ahash));
                $row{score}     = $sc < 0 ? undef : $sc;
            }
            push @rows, \%row;
        }
    }
    $self->_calculate_how_bad( \@rows );
    $self->dbh->update_rows( 'mapping', \@rows, ['qry','qry_ns'] );
    $self->bench_end();
    return $#rows + 1;
}

sub _calculate_how_bad {
    # Calculate the "howbad" column for a set of MAPPING rows
    my $self = shift;
    my $rows = shift;
    return if (!$rows || $#{$rows} == -1);
    # Organize the hits by subject version
    my %bySV;
    map { push @{$bySV{ $_->{sub_vers} || "" }}, $_ } @{$rows};
    foreach my $arr (values %bySV) {
        my ($bestSC) = sort { $b <=> $a } 
        map { defined $_ ? $_ : -1 } 
        map { $_->{score} } @{$arr};
        if (defined $bestSC) {
            foreach my $row (@{$arr}) {
                $row->{howbad} = int(0.5 + 100 * ($bestSC - $row->{score}))/100
                if (defined $row->{score});
            }
        }
    }
}

sub _truncate_feature_table_strings {
    my $ft = shift;
    for my $f (0..$#{$ft}) {
        my $t = $ft->[$f];
        if (length($t) <= 4000) {
            # $ft->[$f] = '^' if ($t eq '0');
            next;
        }
        # Truncate the feature string
        # This is heavy handed, but the FT string is largely a
        # 'courtesy' and is not intended to be hard-core correct.
        $t = substr($t, 0, 4000);
        # Remove the last position in case it was corrupted by truncation
        # That is, we do not want the end of the FT to be hacked from:
        #    ,9999000..9999100  to ,999990.
        $t =~ s/\,[^\,]*$//;
        # This did NOT work - too much truncation! '?' should have worked??
        #   s/\,.*?$//;
        $ft->[$f] = $t;
    }
}

my $dbSnpMapName = 'dbSNP Genomic Mappings';
my $prbMapName = "Probe to genome mappings";
my $setMapName = "Probeset to genome mappings";
my $specialMapSDBs = { map { $_ => 1 } ($prbMapName,$dbSnpMapName,$setMapName) };
sub sdbs_for_query {
    my $self = shift;
    my ($id, $qns, $db) = @_;
    my $mt  = $self->tracker;
    return () unless ($mt);
    $qns ||= $self->guess_namespace( $id );
    return () unless ($qns);

    my $mtns  = $self->maptracker_namespace( $qns );
    my $idseq = $mt->get_seq(-defined => 1, -nocreate => 1, -id => $mtns.$id );
    return () unless ($idseq);
    my @taxa = sort map { $_->name } $idseq->each_taxa();
    my $sdb;
    if ($db) {
        $sdb  = $self->_known_mapping_databases($db);
    } elsif ($qns eq 'APRB' || $qns eq 'BAPB') {
        $sdb  = $self->_known_mapping_databases($prbMapName);
    } elsif ($qns eq 'APS' || $qns eq 'BAPS') {
        $sdb  = $self->_known_mapping_databases($setMapName);
    } else {
        my $sdbs = $self->_known_mapping_databases();
        my %keep;
        if ($qns eq 'AVAR') {
            map { $keep{$_}++ } @{$sdbs->{lc($dbSnpMapName)}{sdbs} || []}; 
        }
        foreach my $key (keys %{$sdbs}) {
            my $matched = 0;
            foreach my $tax (@taxa) {
                $matched++ if ($key =~ /^$tax Genome/i);
            }
            if ($matched) {
                map { $keep{$_}++ } @{$sdbs->{$key}{sdbs}};
            }
        }
        $sdb = [];
        map { push @{$sdb}, $_ if ($_ =~ /^\d+$/) } keys %keep;
    }
    return ($sdb, \@taxa);
}

sub update_intersected_genomic_mappings {
    my $self = shift;
    $self->bench_start();
    my ($id, $qns, $db, $indNS, $age, $doWarn) = @_;
    my $isProt    = $self->is_namespace($qns, 'AP');
    my $vQ        = $qns eq 'PHPS' || $isProt ? 1 : 0;
    my $doCongeal = $qns =~ /^(PH|A)PS$/  ? 0 : 1;
    my $mt    = $self->tracker;
    my $mtns  = $self->maptracker_namespace( $qns );
    my $idseq = $mt->get_seq(-defined => 1, -nocreate => 1, -id => $mtns.$id );
    my $qnsname = $self->namespace_name($qns);
    my @other = $self->convert
        ( -id => $id, -ns1 => $qns, -ns2 => $indNS, -age => $age);
    my $mapID = $id;
    my %unMap;
    if ($qns eq 'ILMN') {
        # The maps are linked to the 50mer oligo associated with the ILMN ID
        my $seqs = $mt->get_edge_dump
            ( -name      => $id,
              -tossclass => 'Deprecated',
              -return  => 'obj array',
              -keeptype  => "is a reliable alias for");
        my @goodSeq;
        foreach my $edge (@{$seqs}) {
            my $seq = $edge->other_seq($id);
            next unless ($seq->namespace->name() eq 'Sequence');
            push @goodSeq, $seq->name();
        }
        if ($#goodSeq == 0) {
            $mapID = $goodSeq[0];
            $unMap{$mapID} = $id;
        }
    }
    my %maps;
    foreach my $oid (@other) {
        my ($sdb, $taxa) = $self->sdbs_for_query($oid, $indNS, $db);
        my $sqlOid = "$oid.%";
        my @omaps = $mt->get_mappings
            ( -name => $sqlOid, -sdb  => $sdb, -warn => $doWarn );
        # warn "$oid [".join("+",@{$taxa})."] ".join("+", @{$sdb})." = ".scalar(@omaps)." maps";
        foreach my $omap (@omaps) {
            my ($vOid, $gId) = map { $_->name } $omap->seqs();
            unless ($vOid =~ /^\Q$oid\E/) {
                ($vOid, $gId) = ($gId, $vOid);
                next unless ($vOid =~ /^\Q$oid\E/);
            }
            my $sqlId = $vQ ? "$mapID.%" : $mapID;
            my @qmaps = $mt->get_mappings( -name1 => $sqlId,
                                           -name2 => $vOid );
            # warn "$mapID [$sqlId] vs $vOid = ($vOid, $gId)";
            foreach my $qmap (@qmaps) {
                # For some reason we occationally recover the same map
                next if ($qmap->id() == $omap->id());
                my $iMap = $qmap->intersection( $omap );
                next unless ($iMap);
                # warn "\nDirect:\n".$omap->to_text()."Intersected:\n".$iMap->to_text()."\n";
                if (!$iMap->score()) {
                    my $osc = $omap->score();
                    my $qsc = $qmap->score();
                    $iMap->score( $osc * $qsc / 100 )
                        if (defined $osc && defined $qsc);
                }
                my $comSeq = $qmap->common_sequence( $omap );
                my $qSeq   = $qmap->other_seq($comSeq);
                my $gSeq   = $iMap->other_seq($qSeq);
                my $str    = $iMap->strand();
                my @keyTxt = ($gSeq->name(), $qSeq->name(), $str);
                my @locs = map { 
                    $iMap->loc_for_seq( -name    => $_, 
                                        -congeal => $doCongeal );
                } ($qSeq, $gSeq);

                # Generate the FeatureTable strings:
                my @ft = map { $_->to_FTstring } @locs;
                # Remove 'join' and parens from the FT strings:
                map { s/(join|[\(\)])//g } @ft;
                if ($isProt) {
                    # Intersected protein coordinates are odd in that they are
                    # in 1/3 increments, which results in screwy coordinates
                    # at the start of each HSP

                }
                if ($str < 0) {
                    map { s/complement//g } @ft;
                    $ft[1] = join(',', reverse split(',', $ft[1]));
                }
                push @keyTxt, @ft;
                my $kt = join("\t", @keyTxt);
                $maps{$kt} ||= [$iMap, @locs, $qSeq, $taxa, $comSeq, $omap];
                # $maps{$kt}{$oid} = 1;
            }
        }
    }
    my @rows;
    while (my ($gdat, $objs) = each %maps) {
        my ($s, $q, $str, $qft, $sft) = split(/\t/, $gdat);
        $q = $unMap{$q} || $1;
        # warn "($s, $q, $str, $qft, $sft)";
        my @ft = ($qft, $sft);
        &_truncate_feature_table_strings( \@ft );
        my ($spec,$type,$sname,$sv) = $mt->parse_genomic_sequence_name($s);
        my ($iMap, $ql, $sl, $qSeq, $taxa, $comSeq, $omap) = @{$objs};
        unless ($spec && $type && $sname) {
            # Could not parse genomic name
            # Not common, but some weird stuff has ended up in MapTracker, eg
            #   NM_016312.2 -> "X"
            # Ignore such entries.
            # warn "$q -> $s via ".$comSeq->name();
            next;
        }
        my $g = lc(join('.', $spec,$type,$sname));
        $g =~ s/\s+/_/g;
        my $snsname = $self->namespace_name( $type =~ /chr/i ? 'CHR' : 'GDNA');
        my $set   = "Bridged Mapping";
        # my ($set) = $self->_known_mapping_databases( $iMap->searchdb->name );
        my $qv = undef;
        if ($q =~ /(.+)\.(\d+)$/) {
            ($q, $qv) = ($1, $2);
        }
        my @qlens = $qSeq->read_lengths;
        my $qlen  = $#qlens == 0 ? $qlens[0] : undef;
        my $sc    = $iMap->score();
        # Get protein-to-chr scores from the original protein-RNA score
        $sc       = $omap->score() if (!defined $sc && $self->is_namespace($qns, 'AP'));
        push @rows, {
            'sub'       => $g,
            'sub_end'   => $sl->end,
            'sub_ft'    => $ft[1],
            'sub_ns'    => $snsname,
            'sub_set'   => $set,
            'sub_start' => $sl->start,
            'sub_vers'  => $sv,
            auth        => $iMap->authority->name,
            qry         => $q,
            qry_end     => $ql->end,
            qry_ft      => $ft[0],
            qry_len     => $qlen,
            qry_ns      => $qnsname,
            qry_species => substr(join(',', @{$taxa}), 0, 100),
            qry_start   => $ql->start,
            qry_vers    => $qv,
            score       => $sc,
            strand      => $iMap->strand,
        };
    }
    $self->_calculate_how_bad( \@rows );
    my $purgeCols = ['qry', 'qry_vers', 'sub_set'];
    $self->dbh->update_rows( 'mapping', \@rows, $purgeCols );
    $self->bench_end();
    return $#rows + 1;
}

sub update_genomic_mappings {
    my $self = shift;
    my ($id, $qns, $db, $age, $doWarn) = @_;
    my $mt  = $self->tracker;
    return 0 unless ($mt);
    $qns = $self->guess_namespace( $id ) unless ($qns);
    return 0 unless ($qns);

    my $indNS = $mappingNS->{$qns};
    return 0 unless (defined $indNS);
    return $self->update_indirect_genomic_mappings
        ($id, $qns, $db, $indNS, $age, $doWarn) if ($indNS);

    my $mtns  = $self->maptracker_namespace( $qns );
    my $idseq = $mt->get_seq(-defined => 1, -nocreate => 1, -id => $mtns.$id );
    return 0 unless ($idseq);
    $self->bench_start();
    $id = $idseq->name;
    my $idreq = $id;
    my $purgeCols = ['qry', 'qry_vers'];
    unless ($idreq =~ /\.\d+$/) {
        $idreq = [ $id, "$id.%"];
        # $purgeCols = undef;
    }
    my ($sdb, $taxa) = $self->sdbs_for_query($id, $qns, $db);
    if (!$sdb || $#{$sdb} == -1) {
        $self->bench_end();
        return 0;
    }
    # Track databases that we might need
    my %needed;
    foreach my $sid (@{$sdb}) {
        my ($set) = $self->_known_mapping_databases($sid);
        my $num = $set; 
        # Remove flanking non-numeric characters
        $num =~ s/^[^\d]+//; $num =~ s/[^\d]+$//;
        my $decimal;
        my @bits = split(/[^\d]+/, $num);
        $decimal = pop @bits if ($#bits > 0);
        $num = join('', @bits);
        $num .= ".$decimal" if ($decimal);
        $needed{$set} = $num || 0;
    }
    my @rankedBuilds = sort { $needed{$b} <=> $needed{$a} } keys %needed;

    my $qspec  = substr(join(',', @{$taxa}), 0, 100);
    my @maps   = $mt->get_mappings( -name => $idreq,
                                    -sdb  => $sdb, );
    my $qnsname = $self->namespace_name($qns);
    my %organizeByQueryVersion;
    my $doCongeal = $qns eq 'APS' ? 0 : 1;
    foreach my $map (@maps) {
        my ($q, $s) = map { $_->name } $map->seqs();
        if ($s =~ /^$id/i) {
            ($q, $s) = ($s, $q);
        }
        my ($set) = $self->_known_mapping_databases( $map->searchdb->name );
        delete $needed{  $set };

        my ($spec,$type,$sname,$sv) = $mt->parse_genomic_sequence_name($s);
        next unless ($spec);
        $type = 'chromosome' if ($type eq 'chr');
        if ($sname =~ /^NT_\d+$/) {
            $type = 'supercontig';
        }
 
        my $g = lc(join('.', $spec,$type)).'.'.uc($sname);
        $g =~ s/\s+/_/g;

        my @qlens = $map->other_seq($s)->read_lengths;
        my $qlen  = $#qlens == 0 ? $qlens[0] : undef;
        my ($ql, $sl) = map { 
            $map->loc_for_seq( -name    => $_,
                               -nicegap => 1,
                               -congeal => $doCongeal );
        } ($q, $s);
        # Generate the FeatureTable strings:
        my @ft =  map { $_->to_FTstring() } ($ql, $sl);
        # Remove 'join' and parens from the FT strings:
        map { s/(join|[\(\)])//g } @ft;

        if ($map->strand < 0) {
            # Strip out 'complement' from the string
            map { s/complement//g } @ft;
            # Put the query HSPs in forward order:
            $ft[0] = join(',', reverse split(',', $ft[0]));
        }
        &_truncate_feature_table_strings( \@ft );
            
        my $qv;
        if ($q =~ /^(.+)\.(\d+)$/) {
            # This is a versioned accession
            ($q, $qv) = ($1, $2);
        }
        # warn "($q, $qv) -> $s";
        
        my $snsname = $self->namespace_name( $type =~ /chr/i ? 'CHR' : 'GDNA');
        push @{$organizeByQueryVersion{$qv || 0}}, {
            'sub'       => $g,
            'sub_end'   => $sl->end,
            'sub_ft'    => $ft[1],
            'sub_ns'    => $snsname,
            'sub_set'   => $set,
            'sub_start' => $sl->start,
            'sub_vers'  => $sv,
            auth        => $map->authority->name,
            qry         => $q,
            qry_end     => $ql->end,
            qry_ft      => $ft[0],
            qry_len     => $qlen,
            qry_ns      => $qnsname,
            qry_species => $qspec,
            qry_start   => $ql->start,
            qry_vers    => $qv,
            score       => $map->score,
            strand      => $map->strand,
        };
    }

    my ($bestQV) = sort { $b <=> $a } keys %organizeByQueryVersion;
    unless (defined $bestQV) {
        # No hits were found in MapTracker
        # What version numbers should we consider
        my @vers;
        my $accU = $id;
        if ($id =~ /^(.+)\.(\d+)$/) {
            # The query was a versioned acc
            @vers = ($2);
            $accU = $1;
        } else {
            # Query was unversioned, recover all known versions:
            my %uniq;

            # this edge is not reliably populated?
            my @edges = $idseq->read_edges
                ( -keeptype => 'is an unversioned accession of');
            foreach my $edge (@edges) {
                my $other = $edge->other_seq($idseq)->name;
                if ($other =~ /^$id\.(\d+)$/) { $uniq{$1}++ };
            }

            @vers = sort { $a <=> $b } keys %uniq;
            @vers = (0) if ($#vers < 0);
        }

        # Can we find notes indicating the query is in fact absent?
        foreach my $qv (@vers) {
            my $acc    = $accU;
            $acc      .= ".$qv" if ($qv);
            my $seq    = $mt->get_seq( -name => $acc, -defined => 1);
            my $absent = $mt->get_edge_dump
                ( -name    => $acc,
                  -keeptype => "is absent from",
                  -return  => 'obj array' );
            foreach my $edge (@{$absent}) {
                my $miss = $edge->other_seq($acc)->name;
                my $auth = join($authJoiner, sort $edge->each_authority_name);
                my $set;
                if ($miss =~ /NCBI Genome Build (\d+)/) {
                    $set = "Homo sapiens Genome Build NCBI$1";
                } elsif ($miss =~ /Genome Build/) {
                    $set = $miss;
                }
                push @{$organizeByQueryVersion{$qv}}, {
                    'sub_set'   => $set,
                    qry         => $accU,
                    qry_ns      => $qnsname,
                    qry_species => $qspec,
                    qry_vers    => $qv || undef,
                    score       => 0,
                    auth        => $auth,
                } if ($set);
            }
        }
        my ($bestMiss) = sort { $b <=> $a } keys %organizeByQueryVersion;
        unless (defined $bestMiss) {
            # We do not even have explicit information that the query
            # was attempted to be matched to a genome and failed.
            my ($highestVers) = sort {$b <=> $a} @vers;
            my $set;
            if ($db) {
                ($set) = $self->_known_mapping_databases($db);
                $purgeCols = ['qry', 'qry_vers', 'sub_set'];
#            } else {
#                $purgeCols = ['qry', 'qry_vers'];
            }
            push @{$organizeByQueryVersion{$highestVers}}, {
                'sub_set'   => $set,
                qry         => $accU,
                qry_ns      => $qnsname,
                qry_species => $qspec,
                qry_vers    => $highestVers || undef,
            };
        }
        ($bestQV) = sort { $b <=> $a } keys %organizeByQueryVersion;
    }
    # warn "Purging $id : ".join(',', @{$purgeCols || []});
    unless ( defined $bestQV) {
        $self->bench_end();
        return 0;
    }
    # We have at least one hit - make sure we represent the best
    # available query version
    my @novel = @{$organizeByQueryVersion{$bestQV}};
    delete $organizeByQueryVersion{$bestQV};
    my @residual = map { @{$_} } values %organizeByQueryVersion;
    if ($#residual > -1) {
        # Now make sure we also represent best genome build
        my %hitSet = map { $_->{'sub_set'} => 1 } @novel;
        my %resSets; 
        map { push @{$resSets{$_->{'sub_set'}}}, $_ } @residual;
        foreach my $set (@rankedBuilds) {
            last if ($hitSet{$set});
            next unless ($resSets{$set});
            # The best genome build involves hits to an older
            # version of the query. At this point, it becomes
            # difficult to decide which hits to keep - so keep
            # them all.
            push @novel, @residual;
            last;
        }
    }
    $self->_calculate_how_bad( \@novel );
    # Meh. Problem with duplicate rows in MT:mapping()
    my @cols = sort keys %{$novel[0] || {}};
    my (%seen, @distinct);
    foreach my $row (@novel) {
        my $key = join("\t", map { defined $_ ? $_ : "" } 
                       map { $row->{$_} } @cols );
        push @distinct, $row unless ($seen{$key}++);
    }

    $self->dbh->update_rows( 'mapping', \@distinct, $purgeCols );
    $self->bench_end();
    return $#distinct + 1;
}

my $grcLetters = {
    h => 'Homo sapiens',
    m => 'Mus musculus',
};
sub _known_mapping_databases {
    my $self = shift;
    my ($db) = @_;
    unless ($self->{SDBS}) {
        my $mt  = $self->tracker;
        return unless ($mt);
        $self->bench_start();
        my $sth = $mt->dbi->prepare( "SELECT db_id, dbname FROM searchdb");
        $sth->execute();
        my %sdbs;
        while (my $dat = $sth->fetchrow_arrayref) {
            my ($id, $name) = @{$dat};
            my ($spec, $build, $dbtype);
            if ($name =~ /^Affymetrix (\S+) SNP Mappings$/) {
                $build = $1;
                $spec = "Homo sapiens" if ($build =~ /^NCBI\d+$/);
            } elsif ($name =~ /^Ensembl (GRC([a-z])\d+) Mappings$/) {
                $build = $1;
                $spec = $grcLetters->{$2};
            } else {
                ($spec, $build, $dbtype) = 
                    $mt->parse_genomic_build_name($name);
            }
            my @skeys = (lc($name), $id);
            if ($build && $spec) {
                # This is a genomic database
                my $gname = "$spec Genome Build $build";
                foreach my $key (lc($gname), @skeys) {
                    $sdbs{$key}{type} = 'gDNA';
                    $sdbs{$key}{name} = $gname;
                    push @{$sdbs{$key}{sdbs}}, $id;
                }
            } elsif ($specialMapSDBs->{$name}) {
                my $gname = $name;
                foreach my $key (@skeys) {
                    $sdbs{$key}{type} = 'gDNA';
                    $sdbs{$key}{name} = $gname;
                    push @{$sdbs{$key}{sdbs}}, $id;
                }
                
            }
        }
        $self->{SDBS} = \%sdbs;
        $self->bench_end();
    }
    if ($db) {
        my $sdbs = $self->{SDBS}{ lc($db) };
        return wantarray ? () : undef unless ($sdbs);
        my ($name, $dbs) = ( $sdbs->{name}, $sdbs->{sdbs});
        return wantarray ? ( $name, $dbs ) : $dbs;
    }
    return $self->{SDBS};
}


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Pre-cache STHs used frequently

sub weak_lock {
    my $dbh   = shift;
    my $table   = lc($_[0]);
    my @binds   = split(/\t/, $_[1]);
    # Pad the bind columns out to 4 columns
    while ($#binds < 3) { push @binds, undef; }

    # Put our request into the table - a single SQL statement is used
    # to get a ticket and make the entry - this is not guaranteed to
    # be atomic, but should be very fast

    # warn "LOCKING $table on ".join(" + ", map { defined $_ ? $_ : '-' } @binds)."\n";

    
    my $ls = $dbh->nextval('lock_seq');
    $dbh->named_sth("Set a task lock")->execute( $table, @binds, "$ls.$$" );

    my $find    = "SELECT lock_key FROM update_lock WHERE tabname = ?";
    my @fbinds  = ($table);
    for my $i (0..$#binds) {
        my $val = $binds[$i];
        my $cn  = "col". ($i+1);
        $find  .= " AND ";
        if (defined $val && $val ne '') {
            $find .= "$cn = ?";
            push @fbinds, $val;
        } elsif ($usePostgres) {
            $find .= "($cn IS NULL OR $cn = '')";
        } else {
            $find .= "$cn IS NULL";
        }
    }
    $find .= " ORDER BY lock_key";


    my $fsth = $dbh->prepare
        ( -sql   => $find,
          -name  => "See if a task lock is held",
          -level => 4);

    my $lock_key;
    while (1) {
        my @keys = $fsth->get_array_for_field( @fbinds );
        while ($lock_key = shift @keys) {
            my ($ticket, $pid) = split(/\./, $lock_key);
            if ($pid == $$) {
                # Hey - we are up!
                last;
            }
            # check to see if the process is really alive
            my $size = `ps --no-header -o size -p$pid`;
            warn "'$_[1]' Held by $pid with ticket $ticket";
            chomp($size);
            if ($size) {
                # Ok, there is in fact an active process. Wait
                $lock_key = 0;
                last;
            } else {
                # The process is dead (??), clear the lock
                warn "  CLEARED AS DEAD";
                &clear_lock( $dbh, $lock_key );
            }
        }
        last if ($lock_key);
        sleep(1);
    }
    return $lock_key;
}

sub clear_lock {
    my ($dbh, $key) = @_;
    $dbh->named_sth("Clear Lock Table lock")->execute( $key );
}

sub standard_sths {
    my $self = shift;
    my $dbh  = $self->dbh;
    $dbh->update_lock_method( \&weak_lock );
    $dbh->update_unlock_method( \&clear_lock );

    my $dfm = $dbh->date_format;
    my $toCache =
        [

#         ["Clear temporary list", 4,
#          "TRUNCATE TABLE temp_list"],
#         ["Populate temporary list", 4,
#          "INSERT INTO temp_list values (?)"],

         ["Lock Lock Table", 4,
          "LOCK TABLE update_lock IN EXCLUSIVE MODE"],
         ["Clear Lock Table lock", 4,
          "DELETE FROM update_lock WHERE lock_key = ?"],
         ["Set a task lock", 4,
          "INSERT INTO update_lock (tabname, col1, col2, col3, col4, ".
          "lock_key) VALUES (?, ?, ?, ?, ?, ?)"],

         ["Find clouds for node", 2, 
          "SELECT DISTINCT cloud_id FROM cloud".
          " WHERE upper(node1) = upper(?) AND ns1 = ? AND type = ?"],
         ["Verbosely Find clouds for node", 2, 
          "SELECT cloud_id, node1 FROM cloud".
          " WHERE upper(node1) = upper(?) AND ns1 = ? AND type = ?"],
         ["Delete clouds", 2, 
          "DELETE FROM cloud WHERE cloud_id = ?"],
         ["Read cloud", 2, 
          "SELECT node1, node2, matched, auth, ns1, ns2 FROM cloud".
          " WHERE cloud_id = ?"],

         ["Get Genomic Overlaps", 2, 
          "SELECT qry, sub_start, sub_end, strand, score, qry_vers, sub_ft, howbad FROM mapping ".
          "WHERE (sub = ? OR sub = ?) AND sub_end >= ? AND sub_start <= ? AND sub_vers = ? AND qry_ns = ? ORDER BY sub_start, qry"],

         ["Get Genomic Overlaps HowBad", 2, 
          "SELECT qry, sub_start, sub_end, strand, score, qry_vers, sub_ft, howbad FROM mapping ".
          "WHERE (sub = ? OR sub = ?) AND sub_end >= ? AND sub_start <= ? AND sub_vers = ? AND qry_ns = ? AND howbad <= ? ORDER BY sub_start, qry"],

         ["Get Genomic Overlaps HowBad Scored", 2, 
          "SELECT qry, sub_start, sub_end, strand, score, qry_vers, sub_ft, howbad FROM mapping ".
          "WHERE (sub = ? OR sub = ?) AND sub_end >= ? AND sub_start <= ? AND sub_vers = ? AND qry_ns = ? AND howbad <= ? AND score >= ? ORDER BY sub_start, qry"],

         ["Get Genomic Overlaps Scored", 2, 
          "SELECT qry, sub_start, sub_end, strand, score, qry_vers, sub_ft, howbad FROM mapping ".
          "WHERE (sub = ? OR sub = ?) AND sub_end >= ? AND sub_start <= ? AND sub_vers = ? AND qry_ns = ? AND score >= ? ORDER BY sub_start, qry"],

         ];

    foreach my $row (@{$toCache}) {
        my ($name, $level, $sql, $limit) = @{$row};
        next unless ($name && $sql);
        $dbh->note_named_sth( $name, $sql, $level);
    }
}

sub _standard_set {
    my $self = shift;
    $self->benchstart;
    my ($basename, $basequery, $variants, $level) = @_;
    $level ||= 3;
    my $set = [ [ $basename, $level, $basequery ] ];

    for my $i (0..$#{$variants}) {
        my @prior = @{$set};
        my @novel;
        my $sql  = $basequery;
        my $name = $basename;
        my ($nmod, $smod) = @{$variants->[$i]};
        foreach my $dat (@prior) {
            push @novel, 
            [ $dat->[0] ." + $nmod", $level, $dat->[2] ." AND $smod" ]; 
        }
        $set = [ @prior, @novel ];
    }
    $self->benchend;
    return @{$set};
}

sub set_schema {
    my $self = shift;
    return $self->{SCHEMA} if ($self->{SCHEMA});

    my %tables;

    $tables{"cloud"} =
    { name  => 'cloud',
      com   => 'A cloud is a collection of connected identifiers. Clouds are generally used where highly recursive linkages prevent easy graph representation via the CONVERSION table. Each row defines one edge in a cloud, with edges sharing the same cloud_id being from the same cloud',
      sequence => { 
          'cloud_seq' => 1,
      },
      index => {
          cloud_primary   => {
              cols => [ 'cloud_id' ],
          },
          cloud_search   => {
              cols => [ 'upper(node1)', 'ns1', 'type' ],
          },
          cloud_secondary_search   => {
              cols => [ 'node1','node2' ],
          },
          cloud_by_date   => {
              cols => [ 'updated', 'node1' ],
          },
      },
      cols  => 
          [['cloud_id', 'integer',
            'Integer primary key for the cloud. Rows with the same cloud_id are part of the same cloud.' ],

           ['type', 'varchar(100)',
            'String describing the type of cloud' ],

           ['node1', 'varchar(100)',
            'One node in the edge' ],

           ['node2', 'varchar(100)',
            'The other node in the edge' ],

           ['ns1', 'varchar(100)',
            'The namespace for node1' ],

           ['ns2', 'varchar(100)',
            'The namespace for node2' ],

           ['auth', 'varchar(100)',
            'The authority responsible for the edge' ],

           ['matched', 'number',
            'A value between 0 and 1 representing the reliability of the edge. Null indicates unknown reliability.' ],

           ['updated', 'date',
            'The date this row was last updated' ],

           ] };

    $tables{ "mapping" } =
    { name  => 'mapping',
      com   => 'Simple coordinate mappings between two sequences',
      update => [ 'qry', 'qry_vers', 'sub_set' ],
      index => {
          map_qry_idx   => {
              cols => [ 'qry','sub_set', 'score' ],
          },
          map_sub_idx   => {
              cols => [ 'sub','sub_start', 'sub_end' ],
          },
          map_qs_idx   => {
              cols => [ 'qry','sub' ],
          },
          map_set_idx   => {
              cols => [ 'sub_set','qry','qry_vers','auth' ],
          },
          map_by_date   => {
              cols => [ 'updated','qry' ],
          },
      },
      cols  => 
          [['qry', 'varchar(100)',
            'The primary, query sequence in the alignment' ],

           ['sub', 'varchar(100)',
            'The other sequence in the alignment' ],

           ['score', 'number',
            'A numeric score associated with this mapping' ],

           ['auth', 'varchar(100)',
            'The authority making the alignment' ],

           ['strand', 'number',
            '+1 or -1 strand assingment, or null' ],

           ['qry_start', 'number',
            'The start coordinate for the query' ],

           ['qry_end', 'number',
            'The end coordinate for the query' ],

           ['sub_start', 'number',
            'The start coordinate for the subject' ],

           ['sub_end', 'number',
            'The end coordinate for the subject' ],

           ['qry_vers', 'varchar(20)',
            'The sequence version for the query' ],

           ['sub_vers', 'varchar(20)',
            'The sequence version for the subject' ],

           ['sub_set', 'varchar(100)',
            'A conceptual set that the subject is contained within' ],

           ['qry_species', 'varchar(100)',
            'The species name for the query' ],

           ['qry_ns', 'varchar(100)',
            'The namespace for the query' ],

           ['sub_ns', 'varchar(100)',
            'The namespace for subject' ],

           ['qry_len', 'integer',
            'The length of the query, useful to determine if terminal sequence is missing from the alignment' ],

           ['updated', 'date',
            'The date this row was last updated' ],

           ['qry_ft', 'varchar(4000)',
            'Genbank Feature Table location string for the query' ],

           ['sub_ft', 'varchar(4000)',
            'Genbank Feature Table location string for the subject (reverse HSP order for -1 strand matches)' ],

           ['howbad', 'number',
            'How bad this mapping is compared to the best mapping for the qry / sub_vers data set. A value of zero indicates this is the best score available in the mapping set.' ],
           ] };

#     $tables{ "genomic_mapping" } =
#     { name  => 'genomic_mapping',
#       com   => 'Mapping data for features aligned to a genome',
#       index => {
#           gm_feat_idx   => {
#               cols => [ 'feature' ],
#           },
#           gm_feat_idx   =>  {
#               cols => [ 'gdna','gdna_vers','map_start','map_end','strand'],
#           },
#       },
#       cols  => 
#           [['feature', 'varchar(50)',
#             'A sequence or region aligned to the genome' ],

#            ['descr', 'varchar(1000)',
#             'A description of the feature, if available' ],

#            ['gdna', 'varchar(100)',
#             'The genomic DNA accession' ],

#            ['score', 'number',
#             'A numeric score associated with this mapping' ],

#            ['map_start', 'number',
#             'The start coordinate on the genome' ],

#            ['map_end', 'number',
#             'The end coordinate on the genome' ],

#            ['strand', 'number',
#             '+1 or -1 strand assingment, or null' ],

#            ['feature_type', 'varchar(10)',
#             'Token describing the nature of the feature' ],

#            ['feature_vers', 'number',
#             'The version number of the feature' ],

#            ['gdna_vers', 'varchar(20)',
#             'The version number of the gDNA' ],

#            ['species', 'varchar(255)',
#             'The scientific name associated with the feature' ],
#            ] };


    $tables{ "description" } =
    { name  => 'description',
      com   => 'Free text description of items from a variety of namespaces',
      update => [ 'term', 'ns' ],
      ignore => { insert => ['ORA-00001','unique constraint'] },
      checklength => [ 'term', 'descr' ],
      index => {
          descr_primary   => {
              cols => [ 'term', 'ns' ],
          },
          descr_primary_upper   => {
              cols => [ 'upper(term)', 'ns' ],
          },
          desc_by_date   => {
              cols => [ 'updated', 'term' ],
          },
      },
      cols  => 
          [['term', 'varchar(100)',
            'The name or identifier' ],

           ['ns', 'varchar(100)',
            'The namespace for the identifier' ],

           ['descr', 'varchar(4000)',
            'A description associated with the accession. Null indicates that no description is known' ],

           ['updated', 'date',
            'The date this row was last updated' ],

           ] };

    $tables{ "conversion" } =
    { name   => 'conversion',
      com    => 'Converts terms from one namespace into another',
      update => [ 'term_in', 'ns_in', 'ns_out' ],
      checklength => [ 'term_in', 'term_out', 'auth', 'ns_between' ],
      index  => {
          conv_primary   => {
              cols => [ 'term_in', 'ns_out', 'ns_in', 'matched' ],
          },
          conv_primary_upper_mk2   => {
              cols => [ 'upper(term_in)', 'ns_out', 'ns_in', 'updated',
                        'matched' ],
          },
          conv_secondary   => {
              cols => [ 'term_out', 'term_in' ],
          },
          conv_secondary_upper   => {
              cols => [ 'upper(term_out)', 'upper(term_in)' ],
          },
          conv_by_date   => {
              cols => [ 'updated', 'term_in' ],
          },
          conv_match   => {
              cols => [ 'term_in', 'matched' ],
          },
          conv_namespaces   => {
              cols => [ 'ns_in', 'ns_out' ],
          },
      },
      cols  => 
          [['term_in', 'varchar(100)',
            'The input identifier' ],

           ['term_out', 'varchar(100)',
            'The output identifier. If this column is null, then conversion to this namespace was not found (by this authority).' ],

           ['ns_in', 'varchar(100)',
            'The input namespace' ],

           ['ns_out', 'varchar(100)',
            'The output namespace' ],

           ['auth', 'varchar(500)',
            'The authority stating the conversion' ],

           ['matched', 'number',
            'A value between 0 and 1 representing the reliability of the conversion. Null indicates unknown reliability.' ],

           ['ns_between', 'varchar(1000)',
            'Namespaces between the input and output that were used for this conversion. Could be multiple, separated by ||' ],

           ['updated', 'date',
            'The date this row was last updated' ],

           ] };

#    $tables{ "wordhits" } =
#    { name  => 'wordhits',
#      com   => 'Word-based index based on diverse descriptive text (aliases, related identifiers, etc) associated with id',
#      update => [ 'word', 'ns' ],
#      index => {
#          wordhits_primary => {
#              cols => [ 'word', 'ns' ],
#          },
#      },  
#      cols  => 
#          [['word', 'varchar(25)',
#            'An observed word. Always lowercase' ],
#
#           ['ns', 'varchar(100)',
#            'The namespace where the word is found' ],
#
#           ['terms', 'varchar(4000)',
#            'Space-concatenated list of terms associated with the word' ],
#
#           ['updated', 'date',
#            'The date this row was last updated' ],
#
#           ] };

    $tables{ "parentage" } =
    { name  => 'parentage',
      com   => 'Fully denormalized child to parent structure for an ontology',
      update => [ 'parent', 'ns' ],
      index => {
          p_by_child => {
              cols => [ 'child', 'ns' ],
          },
          p_by_parent => {
              cols => ['parent', 'ns'],
          },
          p_by_date   => {
              cols => [ 'updated' ],
          },
      },  
      cols  => 
          [['child', 'varchar(100)',
            'The child ontology term' ],

           ['parent', 'varchar(100)',
            'The parent ontology term. All direct and indirect parents will be recorded' ],

           ['ns', 'varchar(100)',
            'The namespace of the parent and child' ],

           ['parentage', 'integer',
            'The minimum number of edges between the child and parent. One is a parent, two a grandparent, etc. Zero is the child referencing itself' ],

           ['updated', 'date',
            'The date this row was last updated' ],

           ['relation', 'varchar(100)',
            'The relation of the child to the parent (eg GO is_a)' ],

           ] };

    $tables{ "direct_children" } =
    { name  => 'direct_children',
      com   => 'Parent-centric hierarchy table, lists all *immediate* children of a particular parent node. Use table PARENTAGE for transitive closures',
      update => [ 'parent', 'ns' ],
      index => {
          dc_primary => {
              cols => [ 'parent', 'ns' ],
          },
          dc_by_date   => {
              cols => [ 'updated' ],
          },
      },  
      cols  => 
          [['parent', 'varchar(100) NOT NULL',
            'The parent ontology term' ],

           ['ns', 'varchar(100) NOT NULL',
            'The namespace of the parent and child' ],

           ['child', 'varchar(100)',
            'The child ontology term. A null value indicates that the parent has no children.' ],

           ['updated', 'date NOT NULL',
            'The date this row was last updated' ],

           ['relation', 'varchar(100)',
            'The relation of the child to the parent (eg GO is_a)' ],

           ] };

    $tables{ "direct_parents" } =
    { name  => 'direct_parents',
      com   => 'Child-centric hierarchy table, lists all *immediate* parents of a particular child node. Use table PARENTAGE for transitive closures',
      update => [ 'child', 'ns' ],
      index => {
          dp_primary => {
              cols => [ 'child', 'ns' ],
          },
          dp_by_date   => {
              cols => [ 'updated' ],
          },
      },  
      cols  => 
          [['child', 'varchar(100) NOT NULL',
            'The child ontology term' ],

           ['ns', 'varchar(100) NOT NULL',
            'The namespace of the parent and child' ],

           ['parent', 'varchar(100)',
            'The parent ontology term. A null value indicates that the child has no parents - it is presumably a root node.' ],

           ['updated', 'date NOT NULL',
            'The date this row was last updated' ],

           ['relation', 'varchar(100)',
            'The relation of the child to the parent (eg GO is_a)' ],

           ] };

    $tables{ "update_lock" } =
    { name  => 'update_lock',
      com   => 'Prevents collision during bulk updates without locking target table',
      sequence => { 
          'lock_seq' => 1,
      },
      index => {
          primary_lock => {
              cols => [ 'tabname', 'col1', 'col2','col3','col4' ],
              unique => 0,
          },
          key_lock => {
              cols => [ 'lock_key' ],
          }
      },
      cols  => 
          [['lock_key', 'varchar(100)',
            'A PKEY identifying the process owning the lock' ],
           
           ['tabname', 'varchar(50)',
            'The name of the target table' ],
           
           ['col1', 'varchar(100)',
            'The value of the first column in the synthetic key' ],

           ['col2', 'varchar(100)',
            'The value of the second column in the synthetic key' ],

           ['col3', 'varchar(100)',
            'The value of the third column in the synthetic key' ],

           ['col4', 'varchar(100)',
            'The value of the fourth column in the synthetic key' ],
           ] };

    $tables{ "bulk_loads" } =
    { name  => 'bulk_loads',
      com   => 'History of automated load jobs for large sets of data',
      update => [ 'set_name','table_name','ns1','ns2' ],
      index => {
          load_primary => {
              cols => [ 'set_name', 'table_name', 'ns1', 'ns2' ],
          },
          load_secondary => {
              cols => [ 'ns1', 'ns2' ],
          },
      },  
      cols  => 
          [['set_name', 'varchar(100)',
            'A description, often a GenAcc set, of what was analyzed' ],

          ['table_name', 'varchar(50)',
            'The table that was loaded' ],

          ['ns1', 'varchar(100)',
            'The first namespace associated with the load' ],

          ['ns2', 'varchar(100)',
            'The second namespace associated with the load, will be NULL for some tables' ],

          ['age', 'integer',
            'The freshness age set for the load job, in days.' ],

          ['set_size', 'integer',
            'The number of members contained in the set' ],

           ['updated', 'date',
            'The date the load finished' ],

           ] };

    $tables{ "assign_onto" } =
    { name   => 'assign_onto',
      com    => 'Relates ontology terms to biological accessions',
      update => [ 'acc', 'acc_ns', 'onto_ns' ],
      index  => {
          ao_by_acc => {
              cols => [ 'acc', 'onto_ns' ],
          },
          ao_by_upper_acc => {
              cols => [ 'upper(acc)', 'onto_ns' ],
          },
          ao_by_onto => {
              cols => ['onto', 'acc_ns'],
          },
          ao_full_set => {
              cols => ['acc_ns', 'onto_ns'],
          },
          ao_by_date   => {
              cols => [ 'updated', 'acc' ],
          },
      },
      cols  => 
          [['acc', 'varchar(100)',
            'The biological accession' ],

           ['onto', 'varchar(100)',
            'The ontology accession' ],
           
           ['ec', 'varchar(5)',
            'Evidence Code for the assignment' ],

           ['matched', 'number',
            "A score assigned to the ontolgy, ranging from zero ('awful') to one ('fabulous'). Null indicates no score assigned. Interpretation of this value will be dependant on the mechanism of assignment." ],

           ['acc_ns', 'varchar(100)',
            'The namespace of the biological accession' ],

           ['onto_ns', 'varchar(100)',
            'The namespace of the ontology accession' ],

           ['onto_subset', 'varchar(100)',
            'The sub-branch of the ontology, if any (eg the three main branches of GeneOntology)' ],

           ['acc_desc', 'varchar(4000)',
            'The description of the biological accession' ],

           ['onto_desc', 'varchar(4000)',
            'The description of the ontology term' ],

           ['parentage', 'integer',
            'Indicates if this assignment is directly specified, or a more generic parent from the ontology. A value of zero indicates that the ontology term is directly assigned, 1 would be a parent ontology term, 2 a grandparent ontology, etc.' ],

           ['updated', 'date',
            'The date this row was last updated' ],

           ] };

    $tables{ "expectation" } =
    { name  => 'expectation',
      com   => 'Empirically computed expectations for statistical calculations against randomly sorted reference sets.',
      update => [ 'set_size', 'class_size', 'subset_size' ],
      index => {
          expect_set => {
              cols => ['set_size', 'class_size', 'subset_size' ],
          },
          expect_class => {
              cols => [ 'class_size', 'set_size' ],
          },
          expect_subset => {
              cols => [ 'subset_size', 'class_size' ],
          },
      },  
      cols  => 
          [['set_size', 'integer',
            'The size of the reference set (m+n)' ],

           ['class_size', 'integer',
            'The number of objects in the set that match the class (n)' ],

           ['subset_size', 'integer',
            'The number of entities randomly drawn from the reference set (N). A value of zero indicates that the entire reference set was used and analyzed as a GSEA ranked list, and the reported LOD scores represent maxima found across the entire GSEA.' ],

           ['lod0', 'smallint',
            'This is a count of the number of tests performed (all test of LOD at least 0, which should be everything).'],

           ['lod1', 'smallint',
            'The number of random tests that returned a LOD value of AT LEAST 1. It includes the count of LOD2, LOD3, LOD4 and LOD5.' ],

           ['lod2', 'smallint',
            'The number of random tests that returned a LOD value of AT LEAST 2. It includes the count of LOD3, LOD4 and LOD5.' ],

           ['lod3', 'smallint',
            'The number of random tests that returned a LOD value of AT LEAST 3. It includes the count of  LOD4 and LOD5.' ],

           ['lod4', 'smallint',
            'The number of random tests that returned a LOD value of AT LEAST 2. It includes the count of LOD5.' ],

           ['lod5', 'smallint',
            'The number of random tests that returned a LOD value of AT LEAST 5. It is the only LOD column that does not include counts from the others.' ],
           ] };

    # Specify views
    my $dateChk = $usePostgres ?
        "now() - INTERVAL '1 minute'" : "SYSDATE - 0.0007";

    $tables{ "newconversion" } =
    { name  => 'newconversion',
      com   => 'Finds conversions generated in the last minute.',
      view  =>
"SELECT term_in, ns_in, ns_out AS ns_out,
        count(distinct(term_out)) AS Count, updated
  FROM CONVERSION
 WHERE updated >= $dateChk
GROUP BY term_in, ns_in, ns_out, updated
ORDER BY updated DESC, term_in, ns_in, ns_out"
};
    
    $tables{ "newclouds" } =
    { name  => 'newclouds',
      com   => 'Finds clouds generated in the last minute.',
      view  =>
"SELECT cloud_id, type, ns1, count(distinct(node1)) AS Count, updated
  FROM cloud
 WHERE updated >= $dateChk
GROUP BY cloud_id, type, ns1, updated"
};
    
    $tables{ "newdescriptions" } =
    { name  => 'newdescriptions',
      com   => 'Finds descriptions generated in the last minute.',
      view  =>
"SELECT term, ns, substring(descr,1,100) AS Description, updated
  FROM description
 WHERE updated >= $dateChk
ORDER BY updated"
};
    
    $tables{ "newparentage" } =
    { name  => 'newparentage',
      com   => 'Finds parents generated in the last minute.',
      view  =>
"SELECT p.child, p.ns, count(p.parent) AS parents
   FROM parentage p
  WHERE p.updated >= (now() - '00:01:00'::interval)
  GROUP BY p.child, p.ns, p.updated ORDER BY p.updated
"
};
    
    $tables{ "newchildren" } =
    { name  => 'newchildren',
      com   => 'Finds children generated in the last minute.',
      view  =>
"SELECT dc.parent, dc.ns, count(dc.child) AS child_count, dc.updated
   FROM direct_children dc
  WHERE dc.updated >= (now() - '00:01:00'::interval)
  GROUP BY dc.parent, dc.ns, dc.updated ORDER BY dc.updated DESC
"
};
    
    $tables{ "newparents" } =
    { name  => 'newparents',
      com   => 'Finds parents generated in the last minute.',
      view  =>
"SELECT dp.child, dp.ns, count(dp.parent) AS parent_count, dp.updated
   FROM direct_parents dp
  WHERE dp.updated >= (now() - '00:01:00'::interval)
  GROUP BY dp.child, dp.ns, dp.updated ORDER BY dp.updated DESC
"
};
    
    $tables{ "newmaps" } =
    { name  => 'newmaps',
      com   => 'Finds mappings generated in the last minute.',
      view  =>
"SELECT m.qry, count(m.sub) AS subject_count, m.updated
   FROM mapping m
  WHERE m.updated >= (now() - '00:01:00'::interval)
  GROUP BY m.qry, m.updated ORDER BY m.updated DESC
"
};
    
    $tables{ "gaq" } =
    { name  => 'gaq',
      com   => 'Shows Postgres SQL statements currently running for this DB.',
      db    => 'postgres',
      view  =>
"
 SELECT date_trunc('second'::text, now() - pg_stat_activity.query_start) AS query_age, date_trunc('second'::text, now() - pg_stat_activity.backend_start) AS backend_age, pg_stat_activity.procpid AS pid, substring(btrim(pg_stat_activity.current_query), 1, 150) AS current_query
   FROM pg_stat_activity
  WHERE pg_stat_activity.current_query <> '<IDLE>'::text 
    AND pg_stat_activity.datname ~~ 'genacc%'::text
    AND upper(pg_stat_activity.current_query) !~~ '% FROM GENACC%'::text
  ORDER BY date_trunc('second'::text, now() - pg_stat_activity.query_start), date_trunc('second'::text, now() - pg_stat_activity.backend_start)
"
};
    
    $tables{ "queries" } =
    { name  => 'queries',
      com   => 'Shows Postgres SQL statements currently running for ALL databases',
      db    => 'postgres',
      view  =>
"
 SELECT pg_stat_activity.datname, pg_stat_activity.usename, date_trunc('second'::text, now() - pg_stat_activity.query_start) AS query_age, date_trunc('second'::text, now() - pg_stat_activity.backend_start) AS backend_age, btrim(pg_stat_activity.query) AS query
   FROM pg_stat_activity
  WHERE pg_stat_activity.state != 'idle'
  ORDER BY date_trunc('second'::text, now() - pg_stat_activity.query_start), date_trunc('second'::text, now() - pg_stat_activity.backend_start)
"
};

    $tables{ "v_lock" } =
    { name  => 'v_lock',
      com   => 'Shows objects currently locked by database',
      db    => 'postgres',
      view  =>
"
SELECT r.relname, l.locktype, l.pid, l.granted, l.mode, 
       date_trunc('second'::text, now() - a.query_start) AS query_age,
       substring(btrim(a.query), 1, 60) AS sql_60chars
  FROM pg_locks l, pg_stat_user_tables r, 
       pg_stat_database d, pg_stat_activity a
 WHERE l.relation = r.relid
   AND l.database = d.datid
   AND d.datname = 'genacc'
   AND a.procpid = l.pid
 ORDER BY l.granted DESC, 
       date_trunc('second', now() - a.query_start) DESC, r.relname, l.mode
"
};

    $tables{ "v_xid" } =
    { name  => 'v_xid',
      com   => 'Shows state of transaction IDs in Postgres',
      db    => 'postgres',
      view  =>
"
 SELECT c.relname, ts.spcname AS tablespace, 
  c.relpages::double precision / 1000::double precision AS kilopages,
  floor(c.reltuples / 1000::double precision) AS kilotuples,
  age(c.relfrozenxid)::double precision /1000000::double precision AS mega_xid,
  pg_size_pretty(pg_total_relation_size(c.relname::text)) AS disk
   FROM pg_namespace ns, pg_class c
   LEFT JOIN pg_tablespace ts ON c.reltablespace = ts.oid
  WHERE ns.oid = c.relnamespace
    AND (ns.nspname <> ALL (ARRAY['pg_catalog'::name, 'information_schema'::name, 'pg_toast'::name]))
    AND c.relkind = 'r'
    AND c.relname != 'temp_list'
  ORDER BY c.reltuples DESC;
"
};

    # http://stackoverflow.com/questions/2204058/show-which-columns-an-index-is-on-in-postgresql
    # array_agg() not available prior to 8.4
    $tables{ "v_ind" } =
    { name  => 'v_ind',
      com   => 'Summarizes size and location of indices',
      db    => 'postgres',
      view  =>
"
 SELECT c.relname AS Index, tc.relname AS Table, 
        s.usename, ts.spcname AS tablespace,
  c.relpages::double precision / 1000::double precision AS kilopages,
  floor(c.reltuples / 1000::double precision) AS kilotuples,
  pg_size_pretty(pg_total_relation_size(c.relname)) AS disk
   FROM pg_class c, pg_class tc, pg_namespace ns, pg_shadow s,
        pg_tablespace ts, pg_index ix
  WHERE ns.oid = c.relnamespace AND ns.nspname = 'public'
    AND c.relkind  = 'i'
    AND tc.relkind = 'r' 
    AND s.usesysid = c.relowner
    AND ts.oid     = c.reltablespace
    AND tc.oid     = ix.indrelid
    AND c.oid      = ix.indexrelid
  ORDER BY c.reltuples DESC
"
};

    $tables{ "v_tab" } =
    { name  => 'v_tab',
      com   => 'Summarizes activities on tables',
      db    => 'postgres',
      view  =>
"
SELECT relname,  seq_scan, idx_scan, 
       n_tup_ins AS Inserts, n_tup_upd AS Updates, n_tup_del AS Deletes,
       to_char(last_analyze, 'YYYY Mon DD') AS Analyzed,
       to_char(last_vacuum, 'YYYY Mon DD') AS Vacuumed
  FROM pg_stat_all_tables where schemaname = 'public'
 ORDER BY relname
"
};

    $tables{ "v_clients" } =
    { name  => 'v_clients',
      com   => 'Show clients currently connected to postgres',
      db    => 'postgres',
      view  =>
"
SELECT datname, usename,
       date_trunc('second'::text, now() - backend_start) AS backend_age,
       client_addr 
  FROM pg_stat_activity 
 ORDER BY datname, client_addr, backend_age
"
};

    $tables{ "activity" } =
    { name  => 'activity',
      com   => 'A high level summary of newly calculated rows',
      db    => 'postgres',
      view  =>
"
SELECT 'Conversion' AS type, count(*) AS count
   FROM newconversion
UNION 
 SELECT 'Cloud' AS type, count(*) AS count
   FROM newclouds
UNION 
 SELECT 'Description' AS type, count(*) AS count
   FROM newdescription
UNION 
 SELECT 'Parentage' AS type, count(*) AS count
   FROM newparentage
UNION 
 SELECT 'Parents' AS type, count(*) AS count
   FROM newparents
UNION 
 SELECT 'Children' AS type, count(*) AS count
   FROM newchildren
UNION 
 SELECT 'Mappings' AS type, count(*) AS count
   FROM newmaps
"
};

    $tables{ "v_size" } =
    { name  => 'v_size',
      com   => 'Show size of installed postgres databases',
      db    => 'postgres',
      view  =>
"
SELECT datid, datname, 
       pg_size_pretty(pg_database_size(datname)) AS size_on_disk
  FROM pg_stat_database
 ORDER BY pg_database_size(datname) DESC;
"
};

    $tables{ "v_wait" } =
    { name  => 'v_wait',
      com   => 'Find queries that are not immediately returning',
      db    => 'postgres',
      requires => ['queries'],
      view  =>
"
SELECT count(queries.query) AS count,
       floor(100::double precision * (avg(date_part('minutes'::text, queries.query_age) * 60::double precision + date_part('seconds'::text, queries.query_age)) / 60::double precision)) / 100::double precision AS minutes,
        queries.query
   FROM queries
  GROUP BY queries.query
  ORDER BY floor(100::double precision * (avg(date_part('minutes'::text, queries.query_age) * 60::double precision + date_part('seconds'::text, queries.query_age)) / 60::double precision)) / 100::double precision DESC;
"
};


    $self->{SCHEMA} = \%tables;
    
    $self->dbh->schema(\%tables);
    return $self->{SCHEMA};
}


our $listTrackerSetName = 'ListTracker Lists';

&_set_conv( 'NS', 'LT', 'update_NS_to_LT');
sub update_NS_to_LT {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $id    = $self->namespace_name($idReq);
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    unless ($id eq $nsOut) {
        die "Can not get LT IDs from namespace '$idReq'";
        return [{ term_in  => $idReq,
                  ns_in    => $nsIn,
                  ns_out   => $nsOut, }];
    }

    my $struct = $self->fast_edge_hash
        ( -name      => $listTrackerSetName,
          -keepclass => 'ListTracker List',
          -tossclass => 'Deprecated',
          -keeptype  => "has member");

    my @rows;
    foreach my $ltid (sort keys %{$struct}) {
        my $auths = $self->_auth_for_listtracker( $ltid );
        push @rows, { 
            term_in  => $idReq,
            term_out => $ltid,
            ns_in    => $nsIn,
            ns_out   => $nsOut,
            auth     => $auths,
            matched  => 1,
        };
    }
    @rows = ( { term_in  => $idReq,
                ns_in    => $nsIn,
                ns_out   => $nsOut, } ) if ($#rows == -1);

    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

sub update_LT_to_NS {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    return $self->_all_listtracker_namespaces()
        if (lc($idReq) eq lc($listTrackerSetName));
    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    unless ($id && $self->verify_namespace($id, $ns1)) {
        return [];
    }
    $self->bench_start();
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my $raw = $self->convert
        ( -id => $id, -ns1 => $ns1, -ns2 => 'UNK',
          -nonull => 1, -age => $age, -min => 0.01 );
    my @rows;
    my (%nss, %auths);
    foreach my $row (@{$raw}) {
        if ($row->[2] =~ /(.+) \[(\S+)\]$/) {
            my ($au, $ns) = ($1, $2);
            $nss{$ns}++;
            $auths{$ns}{$au} = 1;
        } else {
            $nss{UNK}++;
        }
    }
    my $tot = 0; map { $tot += $_ } values %nss;
    while (my ($ns3, $num) = each %nss) {
        my $auth = join('/', sort keys %{$auths{$ns3}});
        my $tout = $self->namespace_name( $ns3 );
        my $sc   = int(0.5 + $num * 1000 / $tot) / 1000;
        push @rows, {
            term_in    => $id,
            term_out   => $tout,
            ns_in      => $nsIn,
            ns_out     => $nsOut,
            auth       => "$auth [$num]",
            matched    => $sc,
        };
    }
    if ($#rows < 0) {
        @rows = ( { term_in  => $id,
                    ns_in    => $nsIn,
                    ns_out   => $nsOut, } ); 
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    $self->bench_end();
    return \@rows;    
}

sub _all_listtracker_namespaces {
    my $self = shift;
    my ($age) = @_;
    my @ltids = $self->convert( -id => 'LT', -ns1 => 'NS',
                                -ns2 => 'LT', -age => $age );
    my $nss   = $self->convert( -id => \@ltids, -ns1 => 'LT', -ns2 => 'NS',
                                -nonull => 1,
                                -cols => ['term_out', 'auth' ], -age => $age );
    my $id    = $listTrackerSetName;
    my $nsIn  = $self->namespace_name( 'LT' );
    my $nsOut = $self->namespace_name( 'NS' );
    my @rows;
    
    my %rv;
    foreach my $dat (@{$nss}) {
        my ($ns, $auth) = @{$dat};
        next unless ($ns && $auth);
        if ($auth =~ /\[(\d+)\]$/) {
            $rv{$ns} += $1;
        }
    }
    delete $rv{""};
    while (my ($nsn, $count) = each %rv) {
        push @rows, { 
            term_in  => $id,
            term_out => $nsn,
            ns_in    => $nsIn,
            ns_out   => $nsOut,
            auth     => "ListTracker [$count]",
            matched  => 1,
        };
    }
    @rows = ( { term_in  => $id,
                ns_in    => $nsIn,
                ns_out   => $nsOut, } ) if ($#rows == -1);

    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

sub _auth_for_listtracker {
    my $self = shift;
    my $id   = shift;
    my $auths = $self->fast_edge_hash
        ( -name      => $id,
          -keepclass => 'Entity',
          # Keep deprecated entities = former colleagues!
          # -tossclass => 'Deprecated',
          -keeptype  => "has contribution from");
    return join($authJoiner, sort keys %{$auths}) || "Unknown";
}

my $listTrackerIsPrimaryTag = "List Tracker Primary Identifier Column";
# This method is to blindly grab all members of an LT ID
sub update_LT_to_UNK {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    unless ($id && $self->verify_namespace($id, $ns1)) {
        return [];
    }
    $self->bench_start();
    my @rows;
    if ($seq) {
        my $auth = $self->_auth_for_listtracker( $seq );
        # We are going to include deprecated entries in this conversion:
        my $list = $self->tracker->get_edge_dump
            ( -name      => $seq,
              -return    => 'object array',
              # -tossclass => 'deprecated',
              -keepauth  => 'ListTracker',
              -keeptype  => 'has member' );
        foreach my $edge (@{$list}) {
            my $other = $edge->other_seq( $seq );
            my $omtns = $other->namespace->name;
            my $oname = $other->name;
            my $gns;
            if ($omtns eq 'Unusual') {
                $gns = 'UNK';
            } else {
                $gns = $self->guess_namespace_very_careful( $other );
            }
            # Should this column be used to pivot to other namespaces?
            # If it is set as being "primary", then yes
            my ($isPrimary) = sort { $b <=> $a } map { ($_->num() || 0) } 
            $edge->has_tag($listTrackerIsPrimaryTag);
            # We will use the score to record 'pivotability' of the list
            # If the member is deprecated, then it is unpivotable
            # If there is no column defined, unknown
            # If it is from the first column, it is a pivot
            # Otherwise, it is not pivotable
            my $isDep = $self->fast_class($other->id,'Deprecated');
            my $score = 
                $isDep     ? 0 :
                $isPrimary ? 1 : -1;
            
            # warn "$id -> $oname [$gns]: Dep:$isDep, SrcCol:".(defined $col ? $col : '-UNDEF-').", Score: $score\n";
            push @rows, { term_in    => $id,
                          term_out   => $oname,
                          ns_in      => $nsIn,
                          ns_out     => $nsOut,
                          auth       => "$auth [$gns]",
                          matched    => $score };
        }
    }
    if ($#rows < 0) {
        @rows = ( { term_in  => $id,
                    ns_in    => $nsIn,
                    ns_out   => $nsOut, } ); 
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    $self->bench_end();
    return \@rows;    
}

sub _raw_listtracker_members {
    my $self = shift;
    my ($idReq) = @_;
    my $ns1     = 'LT';
    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    unless ($seq && $self->verify_namespace($id, $ns1)) {
        return ();
    }
    $self->bench_start();
    my $auth = $self->_auth_for_listtracker( $seq );
    my $list   = $self->tracker->get_edge_dump
        ( -name      => $seq,
          -return    => 'object array',
          -tossclass => 'deprecated',
          -keepauth  => 'ListTracker',
          -keeptype  => 'has member' );
    my @rv;
    foreach my $edge (@{$list}) {
        my $other = $edge->other_seq( $seq );
        my $omtns = $other->namespace->name;
        my $oname = $other->name;
        my $gns;
        if ($omtns eq 'Unusual') {
            $gns = 'UNK';
        } else {
            $gns = $self->guess_namespace_very_careful( $other );
        }
        # Should this column be used to pivot to other namespaces?
        # Currently we are just using the left-most column
        my ($isPrimary) = sort { $b <=> $a } map { ($_->num() || 0) } 
        $edge->has_tag($listTrackerIsPrimaryTag);;
        push @rv, [ $oname, $gns, $omtns, $isPrimary ? 0 : 99999999 ];
    }
    # To determine pivotability, we need to find the smallest column
    my ($leftCol) = sort { $a <=> $b } map { $_->[3] } @rv;
    map { $_->[3] = $_->[3] == $leftCol ? 1 : 0 } @rv;
    $self->bench_end();
    return ($id, $auth, \@rv);
}

&_set_conv( [ keys %{$nsName} ], 'LT', 'update_ANYTHING_to_LT',
    ["NS LT", "LT LT", "ILMG LT"] );

sub update_ANYTHING_to_LT {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my $rootNS = $self->is_namespace($ns1, 'BIO') ? 'BIO' :
        $self->is_namespace($ns1, 'CHEM') ? 'CHEM' : '';
    return [] unless ($rootNS);

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    unless ($id && $self->verify_namespace($id, $ns1)) {
        return [];
    }
    $self->bench_start();
    my $allNS   = $self->convert
        ( -id => $listTrackerSetName, -ns1 => 'LT', -ns2 => 'NS',
          -nonull => 1, -age => $age, -cols => ['term_out', 'auth' ], );

    my $ltClass    = 'ListTracker List';
    my $ignoreCase = 0;
    my $needSimp   = 0;

    # Gather all the associated IDs for the query:
    my (%idHash, %pivot);
    foreach my $nr (@{$allNS}) {
        my ($nsn3, $nsAuth) = @{$nr};
        my $ns3 = $self->namespace_token($nsn3);
        my $pivot;
        if ($ns3 eq $ns1) {
            # This is the namespace we are already in
            $idHash{uc($id)} = $nsn3;
        } elsif ($ns3 eq 'UNK' ||
                 !$self->is_namespace($ns3, $rootNS)) {
            # Do not pivot into the unknown!
            # Only pivot if it is the same 'type' of namespace:
            next;
        } else {
            # Ignore namespaces with next-to-nothing represented
            # next if ($num < 10);
            my $pdata = $self->convert
                ( -id => $id, -ns1 => $ns1, -ns2 => $ns3, -age => $age,
                  -ashash => 1, -nonull => 1 );
            foreach my $row (@{$pdata}) {
                my $ucid = $row->{term_out} = uc($row->{term_out});
                $idHash{$ucid} ||= $nsn3;
                push @{$pivot{$ucid}}, $row;
            }
        }
    }
    my @ids = keys %idHash;
    my $mt  = $self->tracker();
    my $edges   = $mt->get_edge_dump
        ( -name      => \@ids,
          -orient    => 1,
          -tossclass => 'deprecated',
          -keepclass => $ltClass,
          -keepauth  => 'ListTracker',
          -filter    => "TAG = '#META_TAGS#$listTrackerIsPrimaryTag' AND NUM = 1",
          # -dumpsql   => 1,
          -keeptype  => 'is a member of' );
    my (%found, %mthash);
    foreach my $edat (@{$edges}) {
        my ($oid, $ra, $eid, $qid) = @{$edat};
        my $ltid  = $mthash{$oid} ||= $mt->get_seq($oid)->name;
        my $qname = $mthash{$qid} ||= $mt->get_seq($qid)->name;
        $found{$ltid}{uc($qname)} = $qname;
    }
    my @rows;
    while (my ($ltid, $fHash) = each %found) {
        my $auth = $self->_auth_for_listtracker( $ltid );
        if (exists $fHash->{uc($id)}) {
            # This list can be found directly by the query
            push @rows,{
                term_in  => $id,
                term_out => $ltid,
                ns_in    => $nsIn,
                ns_out   => $nsOut,
                auth     => $auth,
                matched  => 1,                
            };
            next;
        }
        # The list was found indirectly
        foreach my $qry (values %{$fHash}) {
            my $ucqy = uc($qry);
            my $qnsn = $idHash{$ucqy};
            unless ($qnsn) {
                $self->err("Failed to recover namespace for indirect query '$qry'");
                next;
            }
            my $qrow = {
                term_in  => $ucqy,
                term_out => $ltid,
                ns_in    => $qnsn,
                ns_out   => $nsOut,
                auth     => $auth,
                matched  => 1,                
            };
            my $indRows = $pivot{$ucqy};
            unless ($indRows) {
                $self->err("Failed to recover indirect mappings from '$qry'");
                next;
            }
            my @stitch = $self->stitch_rows($indRows, [$qrow]);
            @stitch = $self->simplify_rows( -rows => \@stitch,
                                            -shrinkauth => 1);
            push @rows, @stitch;
            $self->err("Failed to stitch indirect mappings from '$qry'")
                if ($#stitch == -1);
        }
    }
    if ($#rows < 0) {
        @rows = ( { term_in  => $id,
                    ns_in    => $nsIn,
                    ns_out   => $nsOut, } ); 
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    $self->bench_end();
    return \@rows;    
}

sub update_EVERYTHING_to_LT {
    # This is a special function that does a bulk update from a namespace
    # to ListTracker. It does so by starting with all LT IDs, converting to
    # the namespace, then flipping back.
    my $self = shift;
    my ($ns1, $age, $doWarn) = @_;
    my $ns2   = "LT";
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my @ltids = $self->convert( -id => $ns2, -ns1 => 'NS',
                                -ns2 => $ns2, -age => $age );
    my @invKeys = qw(auth ns_between);
    my (@rv, @rows);
    foreach my $ltid (@ltids) {
        my $rev = $self->convert
            ( -id => $ltid, -ns1 => $ns2, -ns2 => $ns1, -age => $age,
              -ashash => 1, -nonull => 1, -warn => $doWarn );
        foreach my $p (@{$rev}) {
            foreach my $key (@invKeys) {
                # Invert concatenated strings eg 'NP_001225 < ENSP00000380525'
                $p->{$key} = join(' < ', reverse 
                                  split(/ \< /, $p->{$key} || ""));
            }
            # Swap in/out
            ($p->{ns_in}, $p->{ns_out})     = ($p->{ns_out}, $p->{ns_in} );
            ($p->{term_in}, $p->{term_out}) = ($p->{term_out}, $p->{term_in} );
        }
        push @rows, @{$rev};
    }
    # Now find null entries
    my %u = map { $_->{term_in} => 1 } @rows;
    my @hits = keys %u;
    $self->msg("Finding all sets for ".scalar(@hits)." $nsIn IDs");
    # Find all the sets represented by the $ns1 IDs hitting ListTracker IDs
    my @allSets = $self->convert
        ( -id => \@hits, -ns1 => $ns1, -ns2 => 'SET', -age => $age,
          -nonull => 1, -warn => $doWarn );
    my @sets;
    foreach my $set (@allSets) {
        push @sets, $set unless ($set =~ /^ListTracker/);
    }
    $self->msg("Finding all nulls for $nsOut", @sets) if ($doWarn);
    # For all those sets now find all possible $ns1 IDs
    my @full = $self->convert
        ( -id => \@sets, -ns1 => 'SET', -ns2 => $ns1, -age => $age,
          -nonull => 1, -warn => $doWarn );
    # Figure out which $ns1 IDs are NOT in ListTracker lists:
    my %needed = map { $_ => 1 } @full;
    map { delete $needed{$_} } @hits;

    #
    my @rvmap = @{$rvcols->{convert}};
    foreach my $row (@rows) {
        push @rv, [ map { $row->{$_} } @rvmap ];
    }
    # Add the missing $ns1 IDs as blank rows:
    foreach my $id (keys %needed) {
        push @rows, {
            term_in  => $id,
            ns_in    => $nsIn,
            ns_out   => $nsOut,
        };
    }

    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rv;
}
{
    my @skipIt = map { "LT $_" } qw(NS TAX SET UNK CHR ILMG);
    &_set_conv( 'LT', [ keys %{$nsName} ], 'update_LT_to_EVERYTHING',
                \@skipIt );
}
sub update_LT_to_EVERYTHING {
    my $self = shift;
    $self->bench_start();
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    my $raw = $self->convert
        ( -id => $id, -ns1 => $ns1, -ns2 => 'UNK',
          -nonull => 1, -age => $age, -min => 0.01 );

    my (@direct, %auths, %indirect);
    foreach my $row (@{$raw}) {
        if ($row->[2] =~ /(.+) \[(\S+)\]$/) {
            my ($au, $gns) = ($1, $2);
            $auths{$gns}{$au} = 1;
            my $oname = $row->[0];
            if ($gns eq $ns2) {
                push @direct, $oname;
            } else {
                $indirect{$gns}{$oname} = 1;
            }
        }
    }

    my @rows;
    if ($#direct != -1) {
        # The list holds the namespace of interest already
        my $auth = join('/', sort keys %{$auths{$ns2}});
        @rows = map { {
            term_in    => $id,
            term_out   => $_,
            ns_in      => $nsIn,
            ns_out     => $nsOut,
            auth       => $auth,
            matched    => 1,
        } } @direct;
    } else {
        # We need to use the "best" column to map to our namespace
        # Do not use the "unusual" IDs for mapping
        delete $indirect{UNK};
        while (my ($ns3, $idhash) = each %indirect) {
            # See if we can convert these data to the other namespace
            my $auth = join('/', sort keys %{$auths{$ns3}});
            my @id3  = keys %{$idhash};
            my $ns3n = $self->namespace_name($ns3);
            my $hop  = $self->convert
                ( -id => \@id3, -ns1 => $ns3, -ns2 => $ns2, -nonull => 1,
                  -age => $age, -ashash => 1, -nonull => 1);
            unless ($#{$hop} == -1) {
                my $irows = [ map { {
                    term_in    => $id,
                    term_out   => $_,
                    ns_in      => $nsIn,
                    ns_out     => $ns3n,
                    auth       => $auth,
                    matched    => 1,
                } } @id3 ];
                push @rows, $self->stitch_rows($irows, $hop);
            }
        }
        @rows = $self->simplify_rows( -rows => \@rows, );
    }

    if ($#rows < 0) {
        @rows = ( { term_in  => $id,
                    ns_in    => $nsIn,
                    ns_out   => $nsOut, } ); 
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    $self->bench_end();
    return \@rows;
}




=head3 RELIABLE_SYNONYMS

 Convert : Anything to a reliable synonym
 Input   : Any identifier
 MT Link : is a deprecated entry for / 
 Output  : RS

=cut

my $authorCache = {};
&_set_conv( 'ANY', 'RS',  'update_RELIABLE_SYNONYMS');
&_set_conv( 'RS',  'ANY', 'update_RELIABLE_SYNONYMS');
sub update_RELIABLE_SYNONYMS {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my $mt    = $self->tracker;

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    unless ($id && $self->verify_namespace($id, $ns1) && $mt) {
        return [];
    }
    my $isSeq = $self->namespace_is_sequence($ns1);
    if ($ns1 eq 'RS' && $seq && $ns1 =~ /(.+)\.\d+$/) {
        my $unv = $1;
        if ($seq->is_class('biomolecule')) {
            $isSeq = 1;
            ($id, $seq) = $self->standardize_id( $unv, $ns1 );
        }
    }
    my %match = ( $id => -1 );
    my %authData;
    if ($seq) {
        $match{$id} = 1;
        # First find deprecations
        my ( %deprecated, @keepers );
        $match{$id} = 0 if ($seq->is_class('deprecated'));
        my @deps = ($seq);
        while ($#deps != -1) {
            my $list = $mt->get_edge_dump
                ( -name      => \@deps,
                  -return    => 'object array',
                  -keeptype  => 'is a deprecated entry for' );
            my $lnum = $#{$list} + 1;
            last unless ($lnum);
            if ($lnum > 10) {
                # Huh? It does not make sense for something to be deprecated to
                # that many IDs. It could theoretically happen, but at some
                # point it becomes not useful. I have seen it in places
                # where it seems to be clearly a data error, so I will
                # suppress it
                $self->msg("[!!]", "Excessive [$lnum] deprecations for $id")
                    if ($self->{DESCWARN} ||
                        ($self->{CONV_CACHE} && $self->{CONV_CACHE}{WARN}));
                last;
            }
            map {$deprecated{$_->id} ||= $_ } @deps;
            @deps = ();
            foreach my $edge (@{$list}) {
                my $other = $edge->node2;
                if ($other->is_class('deprecated')) {
                    # We got to another deprecated ID. Try to cycle from it
                    push @deps, $other unless ($deprecated{$other->id});
                } else {
                    push @keepers, $other;
                }
            }
        }
        # Set deprecated entries to be score 0, otherwise 1
        map { $match{ $_->name } = 0 } values %deprecated;
        map { $match{ $_->name } = 1 } @keepers;

        my @queries = ($seq);
        if ($isSeq) {
            my $vedge = $mt->get_edge_dump
                ( -name     => $seq,
                  -orient   => 1,
                  -keeptype => "is an unversioned accession of" );
            # Also follow from most recent version of query:
            my %vhash;
            my @versions = map { $mt->get_seq_by_id($_->[0]) } @{$vedge};
            foreach my $vers (@versions) {
                if ($vers->name =~ /\.(\d+)$/) {
                    push @{$vhash{$1}}, $vers;
                }
            }
            my ($best) = sort { $b <=> $a } keys %vhash;
            push @queries, @{$vhash{$best}} if ($best);
        }

        # Only travel one edge from our queries
        my $rtype = ($ns1 eq 'RS') ?
            'is reliably aliased by' : 'is a reliable alias for';
        my $edges = $mt->get_edge_dump
            ( -name      => \@queries,
              -orient    => 1,
              -tossclass => ['DEPRECATED', 'GI'],
              -keeptype  => [ # 'fully contains',
                              $rtype,
                              'is the same as' ], );
        foreach my $os ( map {$mt->get_seq_by_id($_->[0])} @{$edges} ) {
            # Only consider un-namespaced matches
            next unless ($os->namespace->id == 1);
            $match{$os->name} = 0.99;
        }
        if ($self->is_namespace($ns1, 'AVAR')) {
            # For variations, consider anything with the same real estate
            # as being 'the same'
            my %cl;
            my @maps = $mt->get_mappings( -name => $seq->id() );
            foreach my $map (@maps) {
                my $coloc = $map->colocalized_seqs( $seq );
                my $srcAid = $map->authority->id();
                foreach my $cdat (@{$coloc}) {
                    my ($sid, $aid) = @{$cdat};
                    $cl{$sid}{$aid} = 1;
                    $cl{$sid}{$srcAid} = 1;
                }
            }
            my %ahash;
            my $overLapScore = 0.99;
            my $mtClass      = 'Variant';#$self->maptracker_classes($ns1);
            while (my ($sid, $aids) = each %cl) {
                my $subS = $mt->get_seq($sid);
                if ($subS && $subS->is_class($mtClass) && 
                    !$subS->is_class('Deprecated')) {
                    my $subj = $subS->name();
                    my @auths = sort map {
                        $authorCache->{$_} ||= 
                            $mt->get_authority($_)->name } keys %{$aids};
                    if (!$match{$subj} || $match{$subj} < $overLapScore) {
                        $match{$subj} = $overLapScore;
                        $authData{$subj} = join($authJoiner, @auths);
                    }
                }
            }
        }
    }

    # Only use unversioned IDs, and only those with at least 6 characters
    my %unv;
    while (my ($acc, $score) = each %match) {
        if ($isSeq) {
            $acc =~ s/\.\d+$//;
            next unless (length($acc) >= 6);
        }
        push @{$unv{$acc}}, $score;
    }
    my @rows;
    foreach my $acc (sort keys %unv) {
        unless ($ns2 eq 'RS') {
            next unless ($self->verify_namespace($acc, $ns2));
        }
        my ($score) = sort { $b <=> $a } @{$unv{$acc}};
        $score = undef if ($score < 0);
        my $au = $id eq $acc ? 'SelfReferential' :
            $authData{$acc} || 'tilfordc';
        push @rows, { term_in    => $id,
                      term_out   => $acc,
                      ns_in      => $nsIn,
                      ns_out     => $nsOut,
                      auth       => $au,
                      matched    => $score };
    }
    push @rows, { term_in  => $id,
                  ns_in    => $nsIn,
                  ns_out   => $nsOut } if ($#rows == -1);

    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head1 Direct Conversions

Each of these conversions represents capture of a direct connection
within the MapTracker database.

=head2 Core Biology

Conversions involved in the relationship between basic biological entities

=head3 CHEMICAL_to_CHEMICAL

 Convert : 
 Input   : 
 MT Link : 
 Output  : 

=cut

{ my %chemsH = map { $_ => 1 } &_full_kids( ['AC'] );
  delete $chemsH{TRC};
  my @chems = keys %chemsH;
  my @notHere = qw(SET ORTH PMID LT TRC CHR ILMG);
  my @noCC;
  foreach my $ca (@chems) {
      push @noCC, map { ("$_ $ca", "$ca $_") } (@notHere, @chems);
  }

  &_set_conv( \@chems, \@chems, 'update_CHEMICAL_to_CHEMICAL',
              ["BMSC TRC", "TRC BMSC"]);
  &_set_conv( 'ANY', \@chems,   'update_ANY_to_CHEMICAL', \@noCC);
  &_set_conv( \@chems,'ANY',    'update_CHEMICAL_to_ANY', \@noCC);
  
}

my %biggerBetterChemType  = map { $_ => 1 } ('%KD', '% Inhibition');
my %binningMethodChemType = 
    ( EC50           => \&_log_chem_binner, 
      Ki             => \&_log_chem_binner, 
      IC50           => \&_log_chem_binner, 
      '%KD'          => \&_percent_binner,
      '% Inhibition' => \&_percent_binner,
      '%Ctrl 1uM'    => \&_inverse_percent_binner,
      ''             => sub { return -1 } );
my %unBinningMethodChemType = 
    ( EC50           => \&_chem_log_unbinner,
      Ki             => \&_chem_log_unbinner,
      IC50           => \&_chem_log_unbinner,
      '%KD'          => \&_chem_percent_unbinner,
      '% Inhibition' => \&_chem_percent_unbinner,
      '%Ctrl 1uM'    => \&_chem_inverse_percent_unbinner, );
# Note some assay aliases:
my %goodChemType = ( '% inh' => '% Inhibition');
# ... and then note all the primary assay names:
map { $goodChemType{$_} = $_ } keys %binningMethodChemType;
delete $goodChemType{''};
my $goodChemRE = '\[('.join('|', sort keys %goodChemType).')\]';

my @mtChemClass;
{ my %ns = map { $_ => 1 } &_full_kids( ['AC'] );
  map { delete $ns{$_} } qw(AC SMI SEQ);
  my %classes = map { $_ => 1 } map { $_->[0] || '' } map { $mtClasses->{$_} || [] } keys %ns;
  delete $classes{''};
  @mtChemClass = keys %classes;
  # warn join($authJoiner, @mtChemClass);
}

sub update_CHEMICAL_to_CHEMICAL {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my $mt    = $self->tracker;
    my ($gns) = ($ns1 eq 'AC') ? $self->guess_namespace($idReq, 'AC') : ($ns1);


    # VERIFY CLASS IS FAILING FOR AC


    my ($id, $seq) = $self->standardize_id( $idReq, $gns, 'verifyClass' );
    my $sid        = $seq ? $seq->id : 0;
    $id = "MTID:$sid" if ($sid && $useMTIDformat->{$gns});
    # warn "$idReq [$ns1] = $id [$gns] seqid = $sid\n";
    return [] unless ($gns eq 'AC' ||
                      ($self->verify_namespace($id, $ns1) && $mt));
    my @rows;
    if ($seq) {
        my %canon;
        $gns ||= $ns1;
        if ($gns eq 'SMI') {
            # We need to make sure this is canonical
            if ($seq->is_class('STSMILES')) {
                # The entry is already canonical
                %canon = ( $sid => ['SMI', 1, [''] ] );
            } else {
                # Find canonical forms of this SMILES string
                my $cSmi = $mt->get_edge_dump
                    ( -name      => $seq,
                      -return    => 'array',
                      -orient    => 1,
                      -keeptype  => 'is a reliable alias for',
                      -keepclass => 'STSMILES',
                      -dumpsql   => 0 );
                %canon = map { $_->[0] => ['SMI', 1, ['SciTegic']] } @{$cSmi};
            }
        } elsif ($gns eq 'SEQ') {
            # Sequences are always canonical
            %canon = ( $sid => ['SEQ', 1, ['']] );
        } else {
            # We need to convert the input into Sequences / canonical SMILES
            my $cSmi = $mt->get_edge_dump
                ( -name      => $sid,
                  -return    => 'array',
                  -orient    => 1,
                  -keeptype  => 'is reliably aliased by',
                  -keepclass => 'STSMILES',
                  -dumpsql   => 0 ) unless ($ns2 eq 'SEQ');
            my $cLtf = $mt->get_edge_dump
                ( -name      => $sid,
                  -return    => 'array',
                  -orient    => 1,
                  -keeptype  => 'is a longer term for',
                  -keepclass => 'STSMILES',
                  -dumpsql   => 0 ) unless ($ns2 eq 'SEQ');

            $cLtf = [];
            
            my $cSeq = $mt->get_edge_dump
                ( -name      => $sid,
                  -return    => 'array',
                  -orient    => 1,
                  -keeptype  => 'is reliably aliased by',
                  -keepspace => 'Sequence', ) unless ($ns2 eq 'SMI');
            
            my (%struct, %smeta);
            foreach my $cDat ([$cSmi, 1, 'SMI'], [$cLtf, 0.9, 'SMI'],
                              [$cSeq, 1, 'SEQ'], ) {
                my ($rows, $sc, $typ) = @{$cDat};
                foreach my $cs (@{$rows}) {
                    my ($mtid, $ra, $eid) = @{$cs};
                    $struct{$mtid}{$eid} = 1;
                    $smeta{$mtid} ||= [$sc, $typ];
                }
            }
            $self->edge_ids_to_authorities( \%struct );
            while (my ($mtid, $auths) = each %struct) {
                my ($sc, $dns) = @{$smeta{$mtid}};
                $canon{$mtid} = [ $dns, $sc, $auths ];
            }
            if ($gns eq 'BMSC') {
                # Special case handling for BMS compound IDs
                # We will try to enforce a single 'consistent' SMILES string
                # in order to deal with multiple isoforms
                my @uniq = keys %canon;
                if ($#uniq > 0) {
                    my @lcp = $self->least_common_parents
                        ( -ids => [ map { "MTID:$_" } @uniq ], -ns => 'SMI',
                          -warn => $self->{CONV_CACHE} ? $self->{CONV_CACHE}{WARN} : undef, -age => $age);
                    if ($#lcp == 0) {
                        # We were able to reduce multiple structures to one
                        if ($lcp[0] =~ /^MTID:(\d+)$/) {
                            my $mtid = $1;
                            my $dat  = $canon{$mtid};
                            unless ($dat) {
                                my ($worstSc) = sort { $a <=> $b }
                                map { $_->[1] } values %canon;
                                $dat = [ 'SMI', $worstSc, [ 'LeastCommonParent' ]];
                            }
                            %canon = ( $mtid => $dat );
                        } else {
                            $self->err("Could not recover MapTracker ID from least_common_parents",
                                       "$id [$gns] = LCP : ".join(',', @lcp));
                        }
                    }
                }
            }
        }


#=pod NOT NEEDED ?

        my %direct;
        map {push @{$direct{$canon{$_}[0]}},"MTID:$_"} keys %canon;
        while (my ($dns, $ids) = each %direct) {
            my $rel = $self->_expand_chemicals($ids, $dns, $age);
            while (my ($rid, $tHash) = each %{$rel}) {
                if ($rid =~ /^MTID\:(\d+)$/) {
                    my $mtid = $1;
                    while (my ($rAuth, $dids) = each %{$tHash}) {
                        map { s/^MTID\:// } @{$dids};
                        foreach my $ds (@{$dids}) {
                            my ($dns, $sc, $auths) = @{$canon{$ds}};
                            next if ($canon{$mtid} && 
                                     $canon{$mtid}[1] >= $sc);
                            $self->err("Changing $mtid", $self->branch($canon{$ds})) if ($canon{$mtid});
                            if ($#{$auths} != -1 && $auths->[0]) {
                                $canon{$mtid} = 
                                    [$dns,$sc,["$rAuth < $auths->[0]"],[$dns]];
                                #warn "Related $mtid [$dns] $sc $rAuth < $auths->[0]\n";
                            } else {
                                $canon{$mtid} = [ $dns, $sc, [$rAuth], [] ];
                                #warn "Related $mtid [$dns] $sc $rAuth\n";
                            }
                        }
                    }
                } else {
                    $self->err("Unusual related ID - $rid");
                }
            }
        }


#=cut



        my %names;
        if ($useMTIDformat->{$ns2}) {
            # If we are simply going to SMILES / Sequence, just use the 
            # canonical IDs captured above:
            while (my ($mtid, $dat) = each %canon) {
                my ($dns, $sc, $auths, $btwn) = @{$dat};
                next unless ($dns eq $ns2);
                map { push @{$names{"MTID:$mtid"}{$sc}}, [$_,$btwn || []]} @{$auths};
            }
        } else {
            # We need to move out of the canonical SMILES/SEQ to the other NS
            my $mtClass;
            my $isAC = ($ns2 eq 'AC') ? 1 : 0;
            if ($isAC) {
                # If we are getting Any Chemical as output, be sure that
                # we capture the seed values as well
                while (my ($mtid, $dat) = each %canon) {
                    my ($dns, $sc, $auths, $btwn) = @{$dat};
                    map { push @{$names{"MTID:$mtid"}{$sc}}, [$_,$btwn || []]} @{$auths};
                }
                $mtClass = [@mtChemClass];
            } else {
                $mtClass = $self->primary_maptracker_class( $ns2 );
            }
            my $others = $mt->get_edge_dump
                ( -name      => [ keys %canon ],
                  -return    => 'array',
                  -orient    => 1,
                  -keeptype  => 'is a reliable alias for',
                  -keepclass => $mtClass,
                  -dumpsql   => 0 );

            my (%struct, %source);
            foreach my $cs (@{$others}) {
                my ($mtid, $ra, $eid, $qid) = @{$cs};
                $struct{$mtid}{$eid} = 1;
                $source{$mtid}{$qid} = 1;
            }
            $self->edge_ids_to_authorities( \%struct );
            while (my ($mtid, $auths) = each %struct) {
                # Normalize the target name:
                my $seq2 = $mt->get_seq( $mtid );
                my $name;
                if ($useMTIDformat->{$ns2} || 
                    ($ns2 eq 'AC' && 
                     $useMTIDformat->{uc($seq2->namespace->name)})) {
                    $name = "MTID:".$seq2->id;
                } else {
                    $name = $seq2->name;
                    next unless ($isAC || 
                                 $self->verify_namespace($name, $ns2));
                }
                my @sources = keys %{$source{$mtid}};
                foreach my $mauth (@{$auths}) {
                    next unless ($mauth); # Should not be needed, but be safe
                    foreach my $mtid (@sources) {
                        my ($dns, $sc, $sauths, $sbtwn) = @{$canon{$mtid}};
                        foreach my $sauth (@{$sauths}) {
                            my $auth = $sauth ? "$mauth < $sauth" : $mauth;
                            my $btwn = $sbtwn ? [$dns, @{$sbtwn}] : [$dns];
                            push @{$names{$name}{$sc}}, [$auth, $btwn];
                        }
                    }
                }
            }
        }
        foreach my $name (sort keys %names) {
            my ($sc, $auth, $btwn);
            if ($name eq $id) {
                ($sc, $auth) = (1, 'SelfReferential');
            } else {
                ($sc) = sort { $b <=> $a } keys %{$names{$name}};
                my @adats = sort { $#{$a->[1]} <=> $#{$b->[1]} } @{$names{$name}{$sc}};
                my %btwns;
                foreach my $adat (@adats) {
                    my ($au, $bt) = @{$adat};
                    my $btt = $#{$bt} != -1 ? join
                        (" < ", map { $self->namespace_name($_) } @{$bt}) : "";
                    $btwns{$btt}{$au} = 1;
                }
                ($btwn) = sort { length($a) <=> length($b) } keys %btwns;
                $auth = join($authJoiner, sort keys %{$btwns{$btwn}});
            }
            push @rows, {
                term_in  => $id,
                term_out => $name,
                ns_in    => $nsIn,
                ns_out   => $nsOut,
                ns_between  => $btwn,
                auth     => $auth,
                matched  => $sc,
            };
        }
    }
    if ($#rows == -1) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

{
  &_set_conv( 'BMSC', 'TRC',    'link_BMSC_to_TRC');
  &_set_conv( 'TRC',  'BMSC',   'link_BMSC_to_TRC');
}

sub link_BMSC_to_TRC {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
                               
    my ($id, $seq) = $self->fast_standardize( $idReq, $ns1 );
    return [] unless ($self->verify_namespace($id, $ns1));
    my @directSeqs = $self->convert( -id => $id, -ns1 => $ns1, -ns2 => 'SEQ',
                                     -age => $age);
    # TRC reagents are matched to the full hairpin
    # BMSC IDs are matched to the short oligo
    my $dir = $ns1 eq 'TRC' ? 1 : $ns1 eq 'BMSC' ? -1 : 0;
    my @rows;
    if ($dir && $#directSeqs != -1) {
        # Move from the direct match to that needed in the other namespace
        my %targets;
        foreach my $src (@directSeqs) {
            my @targs = $self->direct_genealogy($src, $dir, 'SEQ', $age);
            map { $targets{ $_ } = undef } @targs;
        }
        my @targs = keys %targets;
        my @out   = $self->convert
            ( -id => \@targs, -ns1 => 'SEQ', -ns2 => $ns2, -age => $age);
        push @rows, map { { term_in    => $id,
                            term_out   => $_,
                            ns_in      => $nsIn,
                            ns_out     => $nsOut,
                            auth       => 'CDR',
                            matched    => 0.9999 } } @out;
    }
    push @rows, { term_in  => $id,
                  ns_in    => $nsIn,
                  ns_out   => $nsOut } if ($#rows == -1);

    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

sub update_ANY_to_CHEMICAL {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
                               
    my ($id, $seq) = $self->fast_standardize( $idReq, $ns1 );
    return [] unless ($self->verify_namespace($id, $ns1));
    my @rows;
    if ($seq) {
        if ($ns2 ne 'AC') {
            # If we are not getting general 'Any Chemical' hits, perform
            # the AC analysis first and then filter the results
            @rows = $self->filter_conversion( $id, $ns1, 'AC', $ns2, $age);
        } else {
            # In this block $ns2 == 'AC'

            my $meth = \&_to_chem_logic;
            # Get direct assignments
            @rows    = &{$meth}($self, $id, $ns1, $ns2, $age);
            my $doSimp = 0;
            foreach my $nsMid ('AP', 'AR', 'AL') {
                # Get from the object to other potential direct targets
                my $other = $self->convert
                    ( -id => $id, -ns1 => $ns1, -ns2 => $nsMid, -noself => 1,
                      -ashash => 1, -nonull => 1, -age => $age );
                my %hash;
                map { push @{ $hash{$_->{term_out}} }, $_ } @{$other};
                # For each related target, find direct links to it:
                foreach my $mRows (values %hash) {
                    my $mId = $mRows->[0]{term_out};
                    my @indirect = &{$meth}
                    ($self, $mId, $nsMid, $ns2, $age);
                    next if ($#indirect == -1);
                    my @found = $self->stitch_rows($mRows, \@indirect);
                    push @rows, @found;
                    $doSimp = 1;
                }
            }
            # @rows now contains direct AC assignments to the query,
            # and indirect AC assignments via an AP/AR/AL intermediate

            # Now also expand the rows to related chemicals
            my %toExp;
            foreach my $row (@rows) {
                my $mtid  = $row->{term_out};
                my ($gns) = $self->guess_namespace($mtid, 'AC');
                next unless ($gns eq 'SEQ' || $gns eq 'SMI');
                push @{$toExp{$gns}}, $row;
            }
            while (my ($gns, $src) = each %toExp) {
                my %ids = map { $_->{term_out} => 1 } @{$src};
                my @srcIDs = keys %ids;
                my $exr = $self->convert
                    ( -id => \@srcIDs, -ns1 => $gns, -ns2 => $ns2,
                      -ashash => 1, 
                      -noself => 1, -age => $age, -nonull => 1);
                next if ($#{$exr} == -1);
                # Make local copy of rows
                my @loc = map { {%{$_}} } @{$src};
                # Make sure ns_out is ok for stitching
                $gns = $self->namespace_name($gns);
                map { $_->{ns_out} = $gns } @loc;
                # Add in the expanded rows
                my @stitched = $self->stitch_rows(\@loc, $exr);
                push @rows, @stitched;
                $doSimp = 1;
            }



            @rows = $self->simplify_rows
                ( -rows    => \@rows,
                  # -shrinkauth => 1,
                  -bestmid => 1,
                  -show    => 0) if (0 && $doSimp);
        }
    }
    # Also include more specific / generic forms of the compound
    my %byOut; map { push @{$byOut{$_->{term_out}}}, $_ } @rows;
    while (my ($chem, $oRows) = each %byOut) {
        my $sRows = "";
    }

    # warn "$id [$ns1] -> [$ns2]\n".$self->hashes_to_text(\@rows)."\n  ";
    if ($#rows == -1) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

our $chemBridgeNS = {
    SYM => 'AL',
    GO  => ['AP','AL'],
    APS => 'AR',
    HG  => 'RSP',
};

sub update_CHEMICAL_to_ANY {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my ($gns) = $self->guess_namespace($idReq, 'AC');
    my $guessDifferent = $self->namespace_name($gns) eq $nsIn ? 0 : 1;
    my ($id, $seq) = $self->standardize_id( $idReq, $gns );
    $id = "MTID:".$seq->id if ($seq && $useMTIDformat->{$gns});

    return [] unless ($self->verify_namespace($id, $ns1));
    my (@queries, @stitchQueries);
    my $reMapId;
    if ($seq) {
        if ($gns eq 'SEQ') {
            # Always canonical
            @queries = ( {
                term_out => $id, 
                ns_out   => $self->namespace_name($gns) } );
            
        } elsif ($gns eq 'SMI') {
            # Verify that it is in fact canonical
            my @smis = $self->convert
                ( -id => $id, -ns1 => $gns, -ns2 => $gns,
                  -nonull => 1, -age => $age,
                  -auth => '!MoreSpecific,!MoreGeneric' );
            if ($#smis == 0) {
                @queries = ( {
                    term_out => $id,
                    ns_out   => $self->namespace_name($gns) } );
            } elsif ($#smis > 0) {
                $self->err("Multiple canonical smiles returned for $id", @smis);
            }
        } else {
            # Need to translate the request to canonical queries
            # Hopefully just one, but sloppy IDs may have multiple
            $reMapId = 1;
            foreach my $cns ('SEQ','SMI') {
                my $hits = $self->convert
                    ( -id => $id, -ns1 => $gns, -ns2 => $cns,
                      -nonull => 1, -age => $age, -ashash => 1, );
                # map { $_->{ns_in} => $nsIn } @{$hits} if ($guessDifferent);
                push @stitchQueries, @{$hits};
            }
        }
        if ($gns eq 'SEQ' || $gns eq 'SMI') {
             # Also get expanded queries
            my $hits = $self->convert
                ( -id => $id, -ns1 => $gns, -ns2 => $gns, -noself => 1,
                  -nonull => 1, -age => $age, -ashash => 1, );
            # map { $_->{ns_in} => $nsIn } @{$hits} if ($guessDifferent);
            push @stitchQueries, @{$hits};
        }
    }
    my $doSimp;
    my $isGen = 0;
    my @rows;
    foreach my $genericNS ('AR','AP','AL') {
        if ($genericNS eq $ns2) {
            $isGen = $ns2;
            last;
        } elsif ($self->is_namespace($ns2, $genericNS)) {
            $isGen = $genericNS;
        }
    }

    
    if (!$isGen ) {
        # target is not a locus / rna / protein
        if (my $midNS = $chemBridgeNS->{$ns2}) {
            @rows = $self->chain_conversions
                ( -id          => $id, 
                  -chain       => [$ns1, $midNS, $ns2 ], 
                  -age         => $age,
                  -shrinkauth  => 1, );
        } else {
            # NEED LOGIC HERE!
            $self->death("No logic created to convert $ns1 -> $ns2");
        }
    } elsif ($isGen ne $ns2) {
        # target is a non-generic namespace
        @rows = $self->filter_conversion( $id, $ns1, $isGen, $ns2, $age);
        $reMapId = 0;
    } else {
        # We are converting to a generic biological target: AR/AP/AL
        my $meth = \&_from_chem_logic;
        my %byNS;
        map { $byNS{$_->{ns_out}}{$_->{term_out}} ||= $_->{term_in} } 
        (@queries, @stitchQueries);
        while (my ($cns, $cids) = each %byNS) {
            my $ctok = $self->namespace_token($cns);
            foreach my $cid (keys %{$cids}) {
                my $seed = $byNS{$cns}{$cid} || "";
                my @found;
                foreach my $nsMid ('AP', 'AR', 'AL') {
                    # Find direct connections from the chem to likely targets
                    my @direct = &{$meth}($self, $cid, $ctok, $nsMid, $age);
                    if ($#direct == -1) {
                        next;
                    }

                    
                    if ($ns2 eq $nsMid) {
                        # Direct target is what we wanted anyway, keep as is
                        push @found, @direct;
                    }
                    # Now try to go from the direct target to requested NS
                    my %hash;
                    map { push @{ $hash{$_->{term_out}} }, $_ } @direct;
                    # for each direct target, try to expand to other hits:
                    foreach my $mRows (values %hash) {
                        my $other = $self->convert
                            ( -id => $mRows->[0]{term_out}, -ns1 => $nsMid,
                              -ns2 => $ns2, -ashash => 1, -nonull => 1,
                              -age => $age );
                        next if ($#{$other} == -1);
                        # print "$cid [$cns]:\n".$self->rows_to_text($other);

                        my @stitched = $self->stitch_rows($mRows, $other);
                        push @found, @stitched;
                        $doSimp = 1;
                    }
                }
                if ($#found == -1) {
                    next;
                }
                if ($seed) {
                    # These rows were found via an expanded term
                    my @stitched = $self->stitch_rows
                        (\@stitchQueries, \@found);
                    if ($#stitched == -1) {
                        $self->err("Failed to stitch_rows() for $cid ($id)",
                                   $self->branch(\@found));
                    } else {
                        push @rows, @stitched;
                        $doSimp = 1;
                    }
                } else {
                    # Direct query
                    push @rows, @found;
                }
            }
        }
    }
    if ($#rows == -1) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    } else {
        # A lot of namespace guessing has been done above. It is important
        # that the written rows have appropriate namespaces. Do that in bulk
        # here for simplicity
        @rows = $self->simplify_rows
            ( -rows    => \@rows,
              -ns1     => $nsIn,
              -ns2     => $nsOut,
              -allscore => 1,
              -filter => [ \&_chem_edge_custom_filter ],
              # -shrinkauth => 1,
              # -bestmid => 1,
              -show    => 0) if ($doSimp);
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

sub _chem_edge_custom_filter {
    # Modified best score to aggregate authority data
    my ($self, $targ, $args) = @_;
    return $targ if ($#{$targ} < 1);
    my %byAuth;
    foreach my $row (@{$targ}) {
        my $mkey = -1;
        my $sc   = defined $row->[0] ? $row->[0] : -1;
        my $auth = $row->[2];
        if ($row->[3]) {
            my @mids = split(' < ', $row->[3]);
            $mkey = $#mids;
            $auth .= " < ". ($row->[1] || "");
        }
        if ($auth =~ /(.+) $goodChemRE/) {
            $auth = $1;
            my $tag = $2;
            $auth =~ s/.* < //;
            $auth =~ s/ (PMID|\d+xPubMed).*//;
            $auth .= "-$tag";
        } else {
            $auth = "";
        }
        push @{$byAuth{$auth}{$sc}{$mkey}}, $row;
    }
    my @keep;
    while (my ($auth, $scHash) = each %byAuth) {
        my ($bsc) = sort {$b <=> $a } keys %{$scHash};
        my ($bmk) = sort {$a <=> $b } keys %{$scHash->{$bsc}};
        push @keep, @{$scHash->{$bsc}{$bmk}};
    }
    # warn $self->branch({1 => $targ, 2 => \%byAuth, 3 => \@keep});
    return \@keep;
}

sub _to_chem_logic {
    my $self = shift;
    # NS2 will always be AC
    my ($id, $ns1, $ns2, $age) = @_;
    my $nsIn   = $self->namespace_name( $ns1 );
    my $nsOut  = $self->namespace_name( $ns2 );
    my $chems  = $self->_get_chem_edges( [$id], 0, $ns1, $age );

    my @rows;
    foreach my $dat (@{$chems}) {
        my ($mtID, $score, $auth) = @{$dat};
        my $name  = "MTID:$mtID";
        my ($gns) = $self->guess_namespace($name, $ns2);
        if ($canonicalChemical->{$gns}) {
            push @rows, {
                term_in  => $id,
                term_out => $name,
                ns_in    => $nsIn,
                ns_out   => $nsOut,
                auth     => $auth,
                matched  => $score
            };
        } else {
            my $seq = $self->tracker->get_seq($mtID);
            $self->err("Recovered unusual entry  $name [$gns] from $id [$ns1]",
                       $seq->name());
        }
    }
    return @rows;
}

sub _from_chem_logic {
    my $self = shift;
    my ($id, $ns1, $ns2, $age) = @_;
    my $nsIn    = $self->namespace_name( $ns1 );
    my $nsOut   = $self->namespace_name( $ns2 );

    # The query will always be a canonical entity
    my $bios = $self->_get_chem_edges( [$id], $ns2, $ns1, $age );
    my @rows;
    my $mt = $self->tracker;
    foreach my $dat (@{$bios}) {
        my ($bid, $score, $auth) = @{$dat};
        my $seq = $mt->get_seq($bid);
        my $bio = $seq->name;
        if ($bio =~ /(.+)\-\d+$/) {
            # UniProt IDs such as Q13255-1 are causing problems
            # We allow these IDs in clouds, but not in conversion
            # We need to strip off the variant number
            if ($seq->is_class('UniProt')) {
                # warn "Cleansing UniProt id '$bio' to '$1'";
                $bio = $1;
            }
        }
        next unless ($self->verify_namespace($bio, $ns2));
        push @rows, {
            term_in  => $id,
            term_out => $bio,
            ns_in    => $nsIn,
            ns_out   => $nsOut,
            auth     => $auth,
            matched  => $score };
    }
    return @rows;
}

my @chemTypes =
    ('was assayed with','is antagonized by','is agonized by','is inhibited by',
     'is functionally antagonized by','is functionally agonized by',
     'has substrate','is the source for');
my $log10 = 1 / log(10);
my $undefNum = 12345678901234567890;
sub _get_chem_edges {
    my $self = shift;
    my ($idReq, $isRev, $ns, $age) = @_;
    my $mt = $self->tracker;

    my @kt    = @chemTypes;
    my @dumpArgs;
    if ($isRev) {
        # We are going from a chemical to a target
        my $class = $self->primary_maptracker_class( $isRev );
        @dumpArgs = ( [ -keepclass => $class ] );
    } else {
        # Target to a chemical. We need to do this in two steps, first
        # to capture SciTegic Smiles, then to get raw sequences
        @dumpArgs = ( [ -keepclass => 'STSMILES' ],
                      [ -keepspace => 'Sequence' ] );
    }
    map { push @{$_}, ( -return    => 'object array',
                        -keeptype  => \@kt,
                        -revtype   => $isRev, ) } @dumpArgs;

    # Data is keyed by other ID
    # $data{ ChemID }{ AssayTag }{ Authority }[ NumericValues ]
    my (%data, %needed, %captured);
    foreach my $id (@{$idReq}) {
        foreach my $DAs (@dumpArgs) {
            my $list  = $mt->get_edge_dump
                ( -name      => $id, @{$DAs});
            foreach my $edge (@{$list}) {
                # warn $edge->other_seq( $id )->name;
                my $id2 = $edge->other_seq( $id )->id;
                my $dat = $data{$id2} ||= {};
                # Note all authorities touching the edge:
                map { $needed{$id2}{$_} = 1 } $edge->each_authority_name();
                foreach my $tag ($edge->each_tag) {
                    # Note any publications
                    my $tn = $tag->tagname;
                    if ($tn eq 'Referenced In') {
                        $dat->{PMID}{$tag->valname || ""} = 1;
                    } elsif ($tn = $goodChemType{ $tn }) {
                        # Capture specific known tags of interest
                        my $auth = $tag->authname;
                        my $num  = $tag->num;
                        unless (defined $num) {
                            $num  = ($biggerBetterChemType{$tn}) ?
                                0 - $undefNum : $undefNum;
                        }
                        # Note that we have specific tag data for this Auth:
                        $captured{$id2}{$auth} = 1;
                        push @{$dat->{ $tn }{ $auth }}, $num;
                    }
                }
            }
        }
    }
    # Make note of authorities that were observed but did not have
    # specific tags associated with them:
    while (my ($id2, $caph) = each %captured) {
        map { delete $needed{$id2}{$_} } keys %{$caph};
    }
    while (my ($id2, $hash) = each %needed) {
        map { $data{$id2}{""}{$_} = [] } keys %{$hash};
    }

    my @rv;
    while (my ($id2, $tagHash) = each %data) {
        my $pmTxt = "";
        if (my $pmids = $tagHash->{PMID}) {
            delete $tagHash->{PMID};
            my @u;
            foreach my $pmid (keys %{$pmids}) {
                push @u, $pmid if ($pmid =~ /^PMID:\d+$/);
            }
            if ($#u == 0) {
                $pmTxt = " $u[0]";
            } elsif ($#u != -1) {
                $pmTxt = " ".($#u + 1)."xPubMed";
            }
        }
        while (my ($tag, $authHash) = each %{$tagHash}) {
            my %scoreAuth;
            my $binMeth = $binningMethodChemType{$tag};
            while (my ($auth, $scores) = each %{$authHash}) {
                my $bin = &{$binMeth}($scores);
                push @{$scoreAuth{$bin}}, $auth;
                # warn join("/", @{$scores})." = $bin [$idReq -> $id2] [$auth / $tag]\n" if ($bin <= 0 && $tag && $tag ne '% Inhibition');
            }
            while (my ($bin, $auths) = each %scoreAuth) {
                my $auth = join($authJoiner, @{$auths});
                $auth   .= $pmTxt;
                $auth   .= " [$tag]" if ($tag);
                push @rv, [ $id2, $bin == -1 ? undef : $bin, $auth ];
            }
        }
    }
    return \@rv;
}

sub _expand_chemicals {
    my $self = shift;
    my ($ids, $ns, $age) = @_;
    my %related;
    foreach my $id (@{$ids}) {
        my @pars  = $self->all_parents( -id => $id, -ns => $ns,
                                        -age => $age, -noself => 1 );
        foreach my $rel (@pars) {
            if ($rel =~ /^MTID\:\d+$/) {
                push @{$related{$rel}{"MoreGeneric"}}, $id;
            }
        }
        my @kids = $self->all_children($id, $ns, $age);
        foreach my $rel (@kids) {
            if ($rel =~ /^MTID\:\d+$/ && $rel ne $id) { 
                push @{$related{$rel}{"MoreSpecific"}}, $id;
            }
        }
    }
    return \%related;
}

sub score_to_chem_value {
    my $self = shift;
    my ($sc, $auth) = @_;
    if (defined $sc && $auth) {
        while ($auth =~ /(\[([^\]]+)\])/) {
            my ($rep, $assay) = ($1, $2);
            if (my $cb = $unBinningMethodChemType{$assay}) {
                my $val = &{$cb}( $sc );
                return wantarray ? ($val, $assay) : $val;
            }
            $auth =~ s/\Q$rep\E//;
        }
    }
    return wantarray ? () : undef;
}

sub _chem_log_unbinner {
    my ($score) = @_;
    return undef unless ( defined $score);
    return '' if ($score <= 0);
    return sprintf("%.1e", 10 ** (0 - 10 * $score));
}

sub _chem_percent_unbinner {
    my ($score) = @_;
    return undef unless ( defined $score);
    return '' if ($score < 0);
    return (100 * $score).'%';
}

sub _chem_inverse_percent_unbinner {
    my ($score) = @_;
    return undef unless ( defined $score);
    return '' if ($score < 0);
    return (100 * $score).'%';
}

sub _log_chem_binner {
    # Get the smallest value, extract the exponent
    my ($scores) = @_;
    my $bin = -1;
    my @bestests = sort { $a <=> $b } @{$scores};
    my $best;
    while ($#bestests != -1) {
        # Huh. There are some negative values that are upsetting log()
        $best = shift @bestests;
        last if ($best > 0);
    }
    if (defined $best) {
        if ($best > 0) {
            # Turn the assay value into a bin between 0-1
            # The score will be the negative logarithm of the activity
            # Values <= 1-e5 AND > 1e-6 will then be a value of 5
            # which will be a bin of 0.5
            $bin = int(0.001 - (log($best) * $log10)) / 10;
            if ($bin > 1) {
                # Do not allow bins > 1.
                $bin = 1;
            } elsif ($bin < 0) {
                # Do not allow bins < 0.
                $bin = 0;
            }
        } elsif ($best < 0) {
            # Undefined?
            $bin = -1;
        } else {
            $bin = 0;
        }
    }
    return $bin;
}

sub _percent_binner {
    # Get the largest value, divide by 100
    my ($best) = sort { $b <=> $a } @{$_[0]};
    if (defined $best) {
        $best /= 100;
        if ($best < 0) {
            $best = 0;
        } elsif ($best > 1) {
            $best = 1;
        } else {
            $best = int(0.5 + $best * 1000) / 1000;
        }
    } else {
        $best = -1;
    }
    return $best;
}

sub _inverse_percent_binner {
    # 100 is worst, 0 is best. Invert the scores to 'normal' order:
    my @inverted = map { 100 - $_ } @{$_[0]};
    return &_percent_binner( \@inverted );
}


=head3 BIND_LOCUS_RNA_PROTEIN

 Convert : RNA to Loci
 Input   : AR RSR ENST
 MT Link : is contained in locus
 Output  : AL LL ENSG

=cut

#&_set_conv( 'AR RSR ENST', 'AL LL ENSG',  'update_BIND_LOCUS_RNA_PROTEIN');
#&_set_conv( 'AL LL ENSG',  'AR RSR ENST', 'update_BIND_LOCUS_RNA_PROTEIN');

{ my @nss = &_full_kids( ['AL', 'AR', 'AP'], ['IUO'] );
  &_set_conv( \@nss, \@nss,  'update_BIND_LOCUS_RNA_PROTEIN');
#  while (my ($na, $nb) = each %{$canonical}) {
#      &_set_conv( $na, $nb,  'update_BIND_LOCUS_RNA_PROTEIN');
#      &_set_conv( $nb, $na,  'update_BIND_LOCUS_RNA_PROTEIN');
#  }
}

my $rna_loc_expand = {
    AR => [ 'RSR', 'ENST'],
    AL => [  'LL', 'ENSG'],
};
my $rna_loc_canon = {
    RSR  => 'LL',
    LL   => 'RSR',
    ENST => 'ENSG',
    ENSG => 'ENST',
};

sub update_BIND_LOCUS_RNA_PROTEIN {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age, $iter) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    unless ($self->verify_namespace($id, $ns1)) {
        return wantarray ? () : [];
    }

    my @rows;
    if ($seq) {
        my ($gns1)  = $self->guess_namespace($id, $ns1);
        my $root1   = $bioRoot->{$ns1};
        my $root2   = $bioRoot->{$ns2};
        my @clouds = BMS::MapTracker::AccessDenorm::Cloud->new
            ( -denorm  => $self,
              -cleanseed => 0,
              -age     => $self->cloud_age,
              -seed    => [[$id, $gns1]],
              -warn    => $self->{CLOUDWARN},
              -type    => 'GeneCluster' );
        my $opts  = 'NoOscillate NoMult';
        # Require canonical connections to be perfect score:
        $opts .= ' perfect maxedge1' if ($self->is_canonical($gns1, $ns2));
        # Not needed with NoOscillate:
        #$opts .= ' noRPR' if ($self->is_namespace($ns2, 'AL') && 
        #                      $self->is_namespace($ns1, 'AL'));
        if ($root1 eq $root2) {

            # A big problem with this method is that clouds are
            # generally designed to auto-trim themselves when they
            # grow too big. This makes the cloud more managable and
            # immediately informative, but it also means that some
            # links might be lost. If the conversion is prot/prot or
            # nuc/nuc, also consider the protein or transcript
            # clusters, which might allow finer granularity of
            # information.

            # Example where this helps find data:
            # -id NP_060003 -ns1 rsp -ns2 upc

            if ($root1 eq 'AL') {
                $opts .= " L2Lok";
            } else {
                my $type = ($root1 eq 'AP') ? 'Protein'
                    : ($root1 eq 'AR') ? 'Transcript' : undef;
                push @clouds, BMS::MapTracker::AccessDenorm::Cloud->new
                    ( -denorm    => $self,
                      -cleanseed => 0,
                      -age       => $self->cloud_age,
                      -seed      => [[$id, $gns1]],
                      -warn      => $self->{CLOUDWARN},
                      -type      => $type.'Cluster' ) if ($type);
            }
        }
        foreach my $cloud (@clouds) {
            # warn $cloud->to_text()."  ";
            my $paths = $cloud->paths
                ( #-id   => $id,
                  #-ns1  => $gns1,
                  -ns2  => $ns2,
                  -opts => $opts,);
            map { $_->{ns_out} = $nsOut;
                  $_->{ns_in}  = $nsIn; } @{$paths};
            push @rows, @{$paths};
        }
        push @rows, { term_in  => $id,
                      term_out => $id,
                      auth     => 'SelfReferential',
                      matched  => 1,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut }
        if ($self->is_namespace($gns1, $ns2));
        @rows = $self->simplify_rows( -rows => \@rows );
    }
    if ($#rows < 0) {
        if ($self->deep_dive && !$iter) {
            my $change = $self->update_maptracker_sequence( $id );
            return $self->update_BIND_LOCUS_RNA_PROTEIN
                ($id, $ns2, $ns1, $age, $iter) if ($change);
        }
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    # warn "\n$id [$ns1 -> $ns2]\n".$self->rows_to_text(\@rows);
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

my $fallbackMap = {
    'RSR' => {
        AP => 'RSP',
        AL => 'LL',
    },
    'ENST' => {
        AP => 'ENSP',
        AL => 'ENSG',
    },
    'RSP' => {
        AR => 'RSR',
        AL => 'LL',
    },
    'ENSP' => {
        AR => 'ENST',
        AL => 'ENSG',
    },
    'LL' => {
        AR => 'RSR',
        AP => 'RSP',
    },
    'ENSG' => {
        AR => 'ENST',
        AP => 'ENSP',
    },
};

&_set_conv( [&_full_kids( ['AL', 'AR', 'AP']) ], [qw(FLOC FRNA FPRT)],
            'update_FALLBACK_MAPPINGS' );
sub update_FALLBACK_MAPPINGS {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age, $iter) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my $ns2r = $ns2 eq 'FLOC' ? 'AL' : 
        $ns2 eq 'FRNA' ? 'AR' : $ns2 eq 'FPRT' ? 'AP' : '';
    return [] unless ($ns2r);
    my $targNS = $fallbackMap->{$ns1}{$ns2r} || $ns2r;
    # In most cases we should be able to get the data directly
    my $rows = $self->convert( -id => $idReq, -ns1 => $ns1, -ns2 => $targNS,
                               -nonull => 1,
                               -age => $age, -ashash => 1 );

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    if ($#{$rows} != -1) {
        map { $_->{ns_out} = $nsOut } @{$rows};
    } elsif ($seq) {
        # Unable to find standard rows
        my $ns1r = $bioRoot->{$ns1};
        my $lu   = $self->{FALLBACK_STH};
        my $mt   = $self->tracker() || return [];
        unless ($lu) {
            my $lw = $mt->get_type('locuswith')->id();
            my $tr = $mt->get_type('translate')->id();
            my $lp = $mt->get_type('locusprot')->id();
            $lu = $self->{FALLBACK_STH} = {
                AR => {
                    AP => "SELECT s.seqname FROM seqname s, edge e WHERE s.name_id = e.name2 AND e.name1 = ? AND e.type_id = $tr",
                    AL => "SELECT s.seqname FROM seqname s, edge e WHERE s.name_id = e.name1 AND e.name2 = ? AND e.type_id = $lw",
                },
                AP => {
                    AR => "SELECT s.seqname FROM seqname s, edge e WHERE s.name_id = e.name1 AND e.name2 = ? AND e.type_id = $tr",
                    AL => "SELECT s.seqname FROM seqname s, edge e1, edge e2 WHERE s.name_id = e1.name1 AND e2.name2 = ? AND e2.type_id = $tr AND e1.name2 = e2.name1 AND e1.type_id = $lw UNION SELECT us.seqname FROM seqname us, edge u WHERE us.name_id = u.name1 AND u.name2 = ? AND u.type_id = $lp",
                },
                AL => {
                    AR => "SELECT s.seqname FROM seqname s, edge e WHERE s.name_id = e.name2 AND e.name1 = ? AND e.type_id = $lw",
                    AP => "SELECT s.seqname FROM seqname s, edge e1, edge e2 WHERE s.name_id = e2.name2 AND e1.name1 = ? AND e1.type_id = $lw AND e2.name1 = e1.name2 AND e2.type_id = $tr UNION SELECT us.seqname FROM seqname us, edge u WHERE us.name_id = u.name2 AND u.name1 = ? AND u.type_id = $lp",
                },
            };
            while (my ($n1, $n2h) = each %{$lu}) {
                while (my ($n2, $sql) = each %{$n2h}) {
                    $n2h->{$n2} = $mt->dbi->prepare($sql);
                }
            }
        }
        if (my $sth = $lu->{$ns1r}{$ns2r}) {
            my @binds = ($seq->id());
            push @binds, $binds[0] if (($ns1r eq 'AP' && $ns2r eq 'AL') ||
                                       ($ns1r eq 'AL' && $ns2r eq 'AP') );
            my %u = map { $_ => 1 } $sth->get_array_for_field( @binds );
            push @{$rows}, map { {
                term_in      => $id,
                term_out     => $_,
                ns_in        => $nsIn,
                ns_out       => $nsOut,
                matched      => undef,
                auth         => "Fallback Recovery",
                ns_between   => undef,
            } } sort keys %u;
        } else {
            # warn "{$ns1r}{$ns2r}";
        }
    }
    if (!$rows || $#{$rows} < 0) {
        $rows = [ { term_in  => $id,
                    ns_in    => $nsIn,
                    ns_out   => $nsOut, } ]; 
    }
    $self->dbh->update_rows( 'conversion', $rows );
    return $rows;
    
}

&_set_conv('ENSE', 'ENST', 'update_BIND_ENS_EXONS');
&_set_conv('ENST', 'ENSE', 'update_BIND_ENS_EXONS');
sub update_BIND_ENS_EXONS {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age, $iter) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    unless ($self->verify_namespace($id, $ns1)) {
        return wantarray ? () : [];
    }
    my @rows;
    if ($seq) {
        my ($type, $class) = $ns1 eq 'ENSE' ?
            ('is fully contained by', 'RNA') : ('fully contains', 'Exon');
        my $struct = $self->fast_edge_hash
            ( -name      => $seq,
              -keepclass => $class,
              -keeptype  => $type, );
        my $exTag = "Exon Number";
        while (my ($name2, $eidHash) = each %{$struct}) {
            my @eids  = keys %{$eidHash};
            my @nums  = $self->tags_for_edge_ids( \@eids, $exTag, 'numeric' );
            my $nTxt  = ($#nums == -1) ? "" : " [Exon ".join(',', @nums)."]";
            push @rows, {
                term_in    => $id,
                term_out   => $name2,
                ns_in      => $nsIn,
                ns_out     => $nsOut,
                auth       => "Ensembl".$nTxt,
                matched    => 1,
            };
        }
    }
    if ($#rows == -1) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head2 Taxonomy Assignments

Assignment of objects to a species. Getting the taxonomy for a
specific object is trivial (ANY_to_TAX), but finding all members of an
object class for a specific taxonomy is much more difficult, and
requires specific converters for each object class.

=head3 ANY_to_TAX

 Convert : Any object to taxonomy
 Input   : Any namespace
 MT Link : Species assignment table
 Output  : TAX

=cut

&_set_conv( 'ANY', 'TAX', 'update_ANY_to_TAX');
sub update_ANY_to_TAX {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    unless ($self->verify_namespace($id, $ns1)) {
        return [];
    }
    my @rows;
    if ($seq) {
        my $taxa = $seq->each_taxa('hash name');
        while (my ($tname, $alist) = each %{$taxa}) {
            my @auths = $self->simplify_authors
                ( map { $_->name } @{$alist} );
            push @rows, { term_in  => $id,
                          term_out => $tname,
                          ns_in    => $nsIn,
                          ns_out   => $nsOut,
                          auth     => join($authJoiner, @auths),
                          matched  => 1 };
        }
    }
    if ($#rows < 0) {
        if ($self->deep_dive && $self->namespace_is_sequence($ns1)) {
            # Nothing found in MapTracker, deep dive is requested, and the item
            # should be a sequence
            my $change = $self->update_maptracker_sequence( $id );
            return $self->update_ANY_to_TAX( $id, $ns2, $ns1, $age, 1)
                if ($change);
        }
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}


&_set_conv( 'BAAD', 'TAX', 'update_BAAD_to_TAX');
sub update_BAAD_to_TAX {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    unless ($self->verify_namespace($id, $ns1)) {
        return [];
    }
    my @rows;
    if ($seq) {
        my $src = $self->fast_edge_hash
            ( -name      => $seq->id,
              -keeptype  => "was derived from");
        my @srcs = keys %{$src};
        if ($#srcs == 0) {
            my $aad = $srcs[0];
            my $via = 'AAD';
            my $kidRows = $self->convert
                ( -id  => $aad, -ns1 => $via, -ns2 => $ns2, 
                  -age => $age, -ashash => 1, );
            my $fakeRow = {
                term_in  => $id,
                term_out => $aad,
                ns_in    => $nsIn,
                ns_out   => $via,
                auth     => 'BrainArray',
                matched  => 1,
            };
            @rows = $self->stitch_rows([$fakeRow], $kidRows);
         }
    }
    if ($#rows < 0) {
        if ($self->deep_dive && $self->namespace_is_sequence($ns1)) {
            # Nothing found in MapTracker, deep dive is requested, and the item
            # should be a sequence
            my $change = $self->update_maptracker_sequence( $id );
            return $self->update_ANY_to_TAX( $id, $ns2, $ns1, $age, 1)
                if ($change);
        }
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head3 TAX_to_OBJECT

 Convert : Taxonomy-based set retrieval
 Input   : TAX
 Output  : IPI LL SP TR UP
 Method  : Generates appropriate set name and then converts SET -> ns2

=cut

&_set_conv('TAX', 'ENSG IPI LL SP TR UP AL', 'update_TAX_to_OBJECT');
sub update_TAX_to_OBJECT {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my ($taxaName, $obj) = $self->standardize_taxa( $idReq );
    my $rows;
    if ($ns2 eq 'AL') {
        my @kidns = $self->namespace_children($ns2);
        my @kidrows;
        foreach my $kns (@kidns) {
            next if ($kns eq $ns2);
            push @kidrows, $self->copy_conversion
                ( $taxaName, $nsIn, $ns2, $taxaName, $ns1, $kns, $age );
            # die $self->hashes_to_text(\@kidrows);
        }
        $rows = \@kidrows;
    } elsif ($obj) {
        # This is a known taxa
        my $setName = $nsOut;
        if ($setName eq 'LocusLink Gene') {
            $setName = 'LocusLink';
        } elsif ($nsOut eq 'International Protein Index') {
            $setName = 'IPI';
        }
        # Use SET_to_ANY to grab entries from 'Genus species LocusLink'
        my $gsName = "$taxaName $setName";
        $rows = $self->copy_conversion
            ( $taxaName, $nsIn, $nsOut, $gsName, 'SET', $ns2, $age );
    }

    if (!$rows || $#{$rows} < 0) {
        $rows = [ { term_in  => $taxaName,
                    ns_in    => $nsIn,
                    ns_out   => $nsOut, } ]; 
    }
    $self->dbh->update_rows( 'conversion', $rows );
    return $rows;
}

=head3 TAX_to_AAD

 Convert : Taxonomy to Affy Array Design
 Input   : TAX
 Output  : AAD
 Method  : Sequential scan of results from NS -> AAD

=cut

&_set_conv( 'TAX', 'AAD', 'update_TAX_to_AAD');
&_set_conv( 'TAX', 'BAAD', 'update_TAX_to_AAD');
sub update_TAX_to_AAD {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my ($id, $obj) = $self->standardize_taxa( $idReq );
    my @rows;
    my %bytax;
    if ($obj) {
        # Get ALL arrays
        my ($nsIds, $auth);
        if ($ns2 eq 'BAAD') {
            ($nsIds, $auth) = ('BrainArray Design', 'BrainArray');
        } else {
            $nsIds = ['Affy Array Design', 'BMS Custom Array Designs'];
            $auth  = 'Affymetrix';
        }
        my @arrays = $self->convert
            ( -id => $nsIds, -ns1 => 'NS', -ns2 => $ns2, -age => $age );
        foreach my $arr (@arrays) {
            # Get taxa for each array
            my @taxa = $self->convert
                ( -id => $arr, -ns1 => $ns2, -ns2 => 'TAX', -age => $age );
            foreach my $tax (@taxa) {
                next unless ($tax);
                my $row = { term_in  => $tax,
                            term_out => $arr,
                            ns_in    => $nsIn,
                            ns_out   => $nsOut,
                            auth     => $auth,
                            matched  => 1 };
                push @rows, $row;
                push @{$bytax{$tax}}, $row;
            }
        }
    }
    unless ($bytax{$id}) {
        # No hits for requested ID
        my $row = { term_in  => $id,
                    ns_in    => $nsIn,
                    ns_out   => $nsOut };
        push @rows, $row;
        push @{$bytax{$id}}, $row;
    }
    # Note that since we already have all the data, we update all taxa here
    $self->dbh->update_rows( 'conversion', \@rows );
    return $bytax{$id};
}

=head3 TAX_to_APS

 Convert : Taxonomy to Affy Probe Set
 Input   : TAX
 Output  : APS
 Method  : TAX -> AAD -> APS -> TAX : Filter results

=cut

&_set_conv( 'TAX', 'APS', 'update_TAX_to_APS');
&_set_conv( 'TAX', 'BAPS', 'update_TAX_to_APS');
sub update_TAX_to_APS {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my ($id, $obj) = $self->standardize_taxa( $idReq );

    my %bytax;
    if ($obj) {
        my $aNs = $ns2 eq 'APS' ? 'AAD' : $ns2 eq 'BAPS' ? 'BAAD' : 'UNK';
        # Get all arrays assigned to this taxa
        my @arrays = $self->convert( -id => $id, -ns1 => 'TAX',
                                     -ns2 => $aNs, -age => $age );
        # Remove some non-standard arrays:
        my %clean = map { $_ => 1 } @arrays;
        map { delete $clean{$_} } qw(250K_NSP_SNP 250K_STY_SNP);
        @arrays = sort keys %clean;
        # Get all probes in those arrays
        my @probes = $self->convert( -id => \@arrays, -ns1 => $aNs,
                                     -ns2 => $ns2, -age => $age );
        my $full   = $self->convert
            ( -id => \@probes, -ns1 => $ns2, 
              -ns2 => 'TAX', -age => $age );
        map { $bytax{$_->[0]}{$_->[5]}{$_->[2]} ||= $_->[3] 
                  if ($_->[0] && $_->[5]) } @{$full};
    }
    my @rows;
    if ($bytax{$id}) {
        while (my ($probe, $hash1) = each %{$bytax{$id}}) {
            while (my ($auth, $match) = each %{$hash1}) {
                push @rows, { term_in  => $id,
                              term_out => $probe,
                              ns_in    => $nsIn,
                              ns_out   => $nsOut,
                              auth     => $auth,
                              matched  => $match };
            }
        }
    } else {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

sub update_RNA_TO_SNP {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    
    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    if ($seq) {
        
    }
    return [] unless ($self->verify_namespace($id, $ns1));
}

{ my @seqs = &_full_kids( ['ASEQ'] );
  &_set_conv( \@seqs, 'FEAT', 'update_sequence_to_feature');
}

our $featSearch = 
    [ qw(region_name type name) ];
sub update_sequence_to_feature {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $mt    = $self->tracker;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    return [] unless ($self->verify_namespace($id, $ns1) && $mt);
    my @rows;
    my @bss = $self->fetch_bioseq
        ( -id => $id, -format => 'genbank', -version => 1 );
    my $isProt = $self->is_namespace($ns1, 'AP') ? 1 : 0;
    my %vers;
    my @search = qw();
    foreach my $bs (@bss) {
        my $acc  = $bs->accession();
        my $v    = $bs->seq_version() || 0;
        my $molt = $bs->alphabet() || "";
        my $bip  = $molt =~ /prot/i ? 1 : 0;
        # Make sure if we had a protein we got a protein, and nuc for nuc
        next unless ($bip == $isProt);
        foreach my $feat ($bs->get_SeqFeatures()) {
            my $pt = uc($feat->primary_tag);
            next unless ($pt eq 'REGION');
            my $loc = $feat->location->to_FTstring();
            my $name;
            my %fids;
            foreach my $tag ('interpro', 'name', 'hit') {
                # Look for motif IDs
                my $fid = &_get_feat_tag($feat, $tag) || "";
                next unless ($fid =~ /^(PIRSF|PF|PS|PR|IPR|SM)\d+$/);
                $fids{$fid}++;
                my $seq = $mt->get_seq( -id => "#None#$fid", -nocreate => 1);
                next unless ($seq);
                if (my $desc = $seq->desc()) {
                    last if 
                        ($name = &_acceptable_feature_name($desc->name));
                } else {
                    my $alias = $self->fast_edge_hash
                        ( -name      => $seq->id,
                          -keeptype  => "is an alias for");
                    my ($ali) = sort { length($a) 
                                           <=> length($b) } keys %{$alias};
                    last if ($name = &_acceptable_feature_name($ali));
                }
            }
            unless ($name) {
                for my $fs (0..$#{$featSearch}) {
                    my $v = &_get_feat_tag($feat, $featSearch->[$fs]);
                    last if ($name = &_acceptable_feature_name($v));
                }
            }
            unless ($name) {
                # Resort to PFAM etc:
                my $fid = join('/', sort keys %fids);
                if ($fid) {
                    $name = $fid;
                } else {
                    if (0) {
                        # Debugging to see if I am missing anything
                        my %tags = map { $_ => 1 } $feat->get_all_tags();
                        map { delete $tags{$_} } qw(score note);
                        my $fTxt = join(' / ', map { 
                            "$_ : ". &_get_feat_tag($feat, $_) 
                            } sort keys %tags);
                        $self->msg_once("[FEAT]", $fTxt);
                    }
                    next;
                }
            }
            $vers{$v}{$loc}{$name} = 1;
        }
    }
    foreach my $v (sort {$a <=> $b} keys %vers) {
        while (my ($loc, $nHash) = each %{$vers{$v}}) {
            my $auth = sprintf("SeqStore [%s%s %s]", 
                               $isProt ? 'Prot' : 'Nuc', $v ? ".$v" : "", $loc);
            foreach my $name (sort keys %{$nHash}) {
                push @rows, { 
                    term_in    => $id,
                    term_out   => $name,
                    ns_in      => $nsIn,
                    ns_out     => $nsOut,
                    auth       => $auth,
                    matched    => 1, 
                };
            }
        }
    }
    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;    
}

our $uselessFeat = { 
    map { $_ => 1 }
    ( qw(other Seg seg undef),
      ('Pfam domain', 'Superfamily domain','Protein domain','Prosite profiles domain'))
  };
sub _acceptable_feature_name {
    my $name = shift;
    return "" unless ($name);
    $name =~ s/^\s+//;
    $name =~ s/\s+$//;
    return "" if (exists $uselessFeat->{$name} || $name =~ /^\d+$/);
    $name =~ s/_+/ /g;
    return $name;
}

sub _get_feat_tag {
    my ($feat, $tag) = @_;
    my @v = $feat->has_tag($tag) ? $feat->get_tag_values($tag) : ();
    return wantarray ? @v : $v[0] || "";
}

sub seqstore_fetcher {
    my $self = shift;
    unless ($self->{FETCH3}) {
        require BMS::Fetch3;
        $self->{FETCH3} = BMS::Fetch3->new( );
    }
    return $self->{FETCH3};
}

our $seqgrps = {
    RSR => 'REFSEQN',
    RSP => 'REFSEQP',
    ENST => 'ENSEMBLN',
    ENSP => 'ENSEMBLP',
    SP   => 'GCGPROT',
};
sub fetch_bioseq {
    my $self  = shift;
    my $args  = $self->parseparams( @_ );
    my $id    = $args->{ID} || $args->{ID};
    return () unless ($id);
    my $ns    = $args->{NS} || $args->{NS1} || $self->guess_namespace( $id );
    my $fetch = $self->seqstore_fetcher();
    my @bss   = $fetch->fetch
        ( -seqname  => $id,
          -version  => $args->{VERSION},
          -snper    => $args->{SNPER},
          -clear    => 1,
          -seqgroup => $seqgrps->{$ns} || "",
          -format   => $args->{FORMAT});
    return @bss;
}


our $snpRnaLinkCmd = '/stf/biocgi/snpTracker.pl -vb 0 -isclean -format text -seqacc \'%s\'';
our @impactPriority = qw(STP DEL NON SYN SPL UTR INT REF GEN VAR);

{ my @snps = &_full_kids( ['AVAR'] );
  my @rnas = &_full_kids( ['AR'] );
#  &_set_conv( \@snps, \@rnas,   'update_SNP_RNA_LINK');
#  &_set_conv( \@rnas, \@snps,   'update_SNP_RNA_LINK');
}
sub update_SNP_RNA_LINK {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $mt    = $self->tracker;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    return [] unless ($self->verify_namespace($id, $ns1) && $mt);
    my @rows;
    if ($seq) {
        my $isRNA = $self->is_namespace($ns1, 'AR') ? 1 : 0;
        if ($isRNA) {

            my $cmd = sprintf($snpRnaLinkCmd, $id);
            my $table = `$cmd`;
            my %imps;
            foreach my $row (split(m/[\n\r]+/, $table)) {
                my ($vacc, $snps, $impact, $auth) = split(/\t/, $row);
                map { push @{$imps{$_}{$impact}}, $auth } split(',', $snps);
            }
            die $self->branch(\%imps);
            while (my ($snp, $impH) = each %imps) {
            }
        }
        my @maps = $mt->get_mappings
            ( -name => $seq,
              -keepclass => 'GDNA' );
        $self->msg("Maps for $id [$ns1]", map { $_->to_text() } @maps);
        foreach my $map (@maps) {

        }
    }
    die "STILL WORKING ON THIS!";
}

=head2 Transcriptional Profiling

Connecting array designs to probe sets, and probe sets to their RNA targets

=head3 DESIGN_to_PROBESETS

 Convert : Array Design to Probeset
 Input   : AAD / CLAD
 MT Link : has member
 Output  : APS / CLPS

=cut

&_set_conv( 'ILMN', 'ILMD', 'update_DESIGN_to_PROBESETS');
&_set_conv( 'ILMD', 'ILMN', 'update_DESIGN_to_PROBESETS');
&_set_conv( 'AAD',  'BPS',  'update_DESIGN_to_PROBESETS');
&_set_conv( 'AAD',  'APS',  'update_DESIGN_to_PROBESETS');
&_set_conv( 'APS',  'AAD',  'update_DESIGN_to_PROBESETS');
&_set_conv( 'BAAD', 'BAPS', 'update_DESIGN_to_PROBESETS');
&_set_conv( 'BAPS', 'BAAD', 'update_DESIGN_to_PROBESETS');
&_set_conv( 'AVAR', 'AAD',  'update_DESIGN_to_PROBESETS');
&_set_conv( 'BPS',  'AAD',  'update_DESIGN_to_PROBESETS');
&_set_conv( 'CLAD', 'CLPS', 'update_DESIGN_to_PROBESETS');
&_set_conv( 'CLPS', 'CLAD', 'update_DESIGN_to_PROBESETS');
sub update_DESIGN_to_PROBESETS {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $mt    = $self->tracker;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1, 'checkclass' );
    return [] unless ($mt);
    my @rows;
    if ($seq) {
        my $class  = $self->maptracker_classes( $ns2 );
        my $toss   = "deprecated";
        my $type   = ($ns1 =~ /(PS|VAR|ILMN)$/) 
            ? 'is a member of' : 'has member';
        if ($ns2 eq 'APS') {
            $toss = ['BRAINSET', $toss];
        }
        my @bait  = ($seq);
        @bait = $self->_expand_unversioned_baps( $seq )
            if ($ns1 eq 'BAPS' && $seq->is_class('Unversioned accession'));

        my $struct = $self->_aggregated_edge_struct
            ( \@bait,
              -keepclass => $class,
              -tossclass => $toss,
              -keeptype  => $type, );
        $self->_expand_baps_output( $struct) if ($ns2 eq 'BAPS');
        $self->edge_ids_to_authorities( $struct );
        while (my ($oname, $auths) = each %{$struct}) {
            my @simp = $self->simplify_authors(@{$auths});
            push @rows, { term_in  => $id,
                          term_out => $oname,
                          ns_in    => $nsIn,
                          ns_out   => $nsOut,
                          auth     => join($authJoiner, @simp),
                          matched  => 1 };
        }
    } else {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut, };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

&_set_conv( 'APRB BAPB', 'APS BPS BAPS',  'link_PROBES_TO_SETS');
&_set_conv( 'APS BPS BAPS', 'APRB BAPB', 'link_PROBES_TO_SETS');
sub link_PROBES_TO_SETS {
    my $self  = shift;
    my $mt    = $self->tracker;
    return [] unless ($mt);
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name($ns2);

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1, 1 );
    my @rows;
    if ($seq) {
        my $keep  =  $self->maptracker_classes( $ns2 );
        my $type  = ($ns1 =~ /PRB$/) ? 'is a member of' : 'has member';
        my $toss  = "deprecated";
        if ($ns2 eq 'APS') {
            $toss = ['BRAINSET', $toss];
        }
        my @bait  = ($seq);
        @bait = $self->_expand_unversioned_baps( $seq )
            if ($ns1 eq 'BAPS' && $seq->is_class('Unversioned accession'));
        # warn "$keep, $toss, $type";
        my $struct = $self->_aggregated_edge_struct
            ( \@bait,
              -keepclass => $keep,
              -tossclass => $toss,
              -keeptype  => $type, );
        # warn $self->branch($struct);
        $self->_expand_baps_output( $struct) if ($ns2 eq 'BAPS');
        $self->edge_ids_to_authorities( $struct );
        while (my ($oname, $auths) = each %{$struct}) {
            my @simp = $self->simplify_authors(@{$auths});
            push @rows, { term_in  => $id,
                          term_out => $oname,
                          ns_in    => $nsIn,
                          ns_out   => $nsOut,
                          auth     => join($authJoiner, @simp),
                          matched  => 1 };
        }
    }
    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;    
}

sub _expand_baps_output {
    my $self = shift;
    my $struct = shift;
    # We need to add in the unversioned IDs
    my @vids = keys %{$struct};
    foreach my $vid (@vids) {
        if ($vid =~ /^(BrAr:)[^:]+:(.+)/) {
            my $uid = $1.$2;
            while (my ($aid, $v) = each %{$struct->{$vid}}) {
                $struct->{$uid}{$aid} ||= $v;
            }
        }
    }
}

sub _expand_unversioned_baps {
    # For brain array queries we need to take into account
    # the versioned / unversioned information
    # BrAr:HUGENE_2_1_ST_V1:LOC360203_at
    #                  BrAr:LOC360203_at
    # Probes and arrays are associated only with the versioned ID
    # We will need to get all versions
    my $self = shift;
    my $seq  = shift;
    my $hash = $self->fast_edge_hash
        ( -name      => $seq,
          -keepclass => 'BrainArray probe set',
          -tossclass => 'deprecated',
          -keeptype  => "is an unversioned accession of", );
    my @vers;
    foreach my $vid (keys %{$hash}) {
        my ($id2, $seq2) = $self->standardize_id( $vid, 'BAPS', 1 );
        push @vers, $seq2 if ($seq2);
    }
    return @vers;
}

sub _aggregated_edge_struct {
    my $self  = shift;
    my $baits = shift;
    my $struct;
    foreach my $s (@{$baits}) {
        my $hash = $self->fast_edge_hash( -name => $s, @_ );
        # warn "'$s' => ".$self->branch($hash);
        if ($struct) {
            while (my ($id2, $aHash) = each %{$hash}) {
                while (my ($aid, $v) = each %{$aHash}) {
                    $struct->{$id2}{$aid} ||= $v;
                }
            }
        } else {
            $struct = $hash;
        }
    }
    return $struct;
}

=head3 PROBESET_to_TRANSCRIPT

 Convert : Probesets to RNA
 Input   : APS CLPS
 MT Link : is a probe for
 Output  : AR RSR ENST PRDT

=cut


&_set_conv('APS BAPS CLPS BPS PHPS ILMN AGIL','AR RSR ENST ENSE PRDT LL ENSG',
           'update_PROBESET_to_TRANSCRIPT');
our $ps2rnaMatchTag = {
    PHPS => 'Oligo Match',
    ILMN => 'Fraction oligo hybridizied',
    AGIL => 'Fraction oligo hybridizied',
};
sub update_PROBESET_to_TRANSCRIPT {
    my $self  = shift;
    my $mt    = $self->tracker;
    return [] unless ($mt);
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name($ns2);
    my ($id, $seq) = $self->standardize_id( $idReq, $ns1, 1 );
    return [] unless ($id);
    # $self->msg("$id [$ns1] => [$ns2]");

    my @rows;
    if ($seq) {
        if ($ns2 eq 'AR') {
            my @taxa  = $self->convert
                ( -id => $id, -ns1 => $ns1, -ns2 => 'TAX', -age => $age );
            my @bait = ($id);
            if ($ns1 eq 'BAPS' && $seq->is_class('Unversioned accession')) {
                @bait = map { $_->name() }
                $self->_expand_unversioned_baps( $seq );
            }
            my %hits;
            my $tn = $ps2rnaMatchTag->{$ns1} || 'Fraction of probes matched';
            foreach my $bId (@bait) {
                my $redges  = $mt->get_edge_dump
                    ( -name      => $bId,
                      -return    => 'object array',
                      -keeptype  => 'is a probe for',
                      -keepclass => ['RNA','CDNA'],
                      -tossclass => ['Genomic DNA', 'deprecated'],
                      -keeptaxa  => $#taxa == -1 ? undef : \@taxa,
                      -dumpsql   => 0 );

                foreach my $re (@{$redges}) {
                    my $rname  = $re->other_seq( $bId )->name;
                    my $vnum   = 0;
                    if ($rname =~ /(.+)\.(\d+)$/) {
                        # The RNA target itself is versioned
                        ($rname, $vnum) = ($1, $2);
                    }
                    map { push @{$hits{$rname}{$_}{$vnum}}, -1 } 
                    ($re->each_authority_name());
                    foreach my $tag ($re->has_tag($tn)) {
                        my $tvnum = $vnum;
                        if (my $val = $tag->valname) {
                            # Associated with the sequence version number
                            my @bits = split(/\//, $val);
                            if ($#bits > 0) {
                                my @vs;
                                foreach my $bit (@bits) {
                                    if ($bit =~ /\.(\d+)$/) {
                                        push @vs, sprintf("%05d", $1);
                                    }
                                }
                                $tvnum = join('', @vs);
                            } else {
                                $tvnum = $1 if ($val =~ /\.(\d+)$/);
                            }
                        }
                        push @{$hits{$rname}{$tag->authname}{$tvnum}},
                        $tag->num;
                    }
                }
            }

            while (my ($rname, $auths) = each %hits) {
                while ( my ($auth, $mdat) = each %{$auths}) {
                    # Get the best sequence version
                    my ($bestVers) = sort { $b <=> $a } keys %{$mdat};
                    # Get the best score
                    my ($matched) = sort { $b <=> $a } @{$mdat->{$bestVers}};
                    if ($matched < 0) {
                        $matched = undef;
                    } else {
                        # Round to two significant figures
                        $matched = int(0.5 + 100 * $matched) / 100;
                    }
                    push @rows, { term_in  => $id,
                                  term_out => $rname,
                                  ns_in    => $nsIn,
                                  ns_out   => $nsOut,
                                  auth     => $auth,
                                  matched  => $matched };
                }
            }
            @rows = $self->simplify_rows
                ( -rows       => \@rows,
                  -shrinkauth => 1,
                  -cluster    => [ @probeSetMatchAuthorities ], );
        } elsif ($ns2 eq 'LL' || $ns2 eq 'ENSG') {
            # First get direct assignments to the locus
            # This is needed to accomodate the custom locus-based CDF
            # probesets
            my @taxa  = $self->convert
                ( -id => $id, -ns1 => $ns1, -ns2 => 'TAX', -age => $age);
            my $mtClass = $self->maptracker_classes( $ns2 );
            my $ledges  = $mt->get_edge_dump
                ( -name      => $id,
                  -return    => 'object array',
                  -keeptype  => 'is a probe for',
                  -keepclass =>  $mtClass,
                  -tossclass => ['Genomic DNA', 'deprecated'],
                  -keeptaxa  => $#taxa == -1 ? undef : \@taxa,
                  -dumpsql   => 0 );

            my %hits;
            foreach my $le (@{$ledges}) {
                my $lname  = $le->other_seq( $id )->name;
                map { push @{$hits{$lname}{$_}}, -1 }
                ($le->each_authority_name());
                foreach my $tag ($le->has_tag('Fraction of probes matched')) {
                    push @{$hits{$lname}{$tag->authname}}, $tag->num;
                }
            }
            while (my ($lname, $auths) = each %hits) {
                next unless ($self->verify_namespace($lname, $ns2));
                while ( my ($auth, $matches) = each %{$auths}) {
                    # Get the best score
                    my ($matched) = sort { $b <=> $a } @{$matches};
                    if ($matched < 0) {
                        $matched = undef;
                    } else {
                        # Round to two significant figures
                        $matched = int(0.5 + 100 * $matched) / 100;
                    }
                    push @rows, { term_in  => $id,
                                  term_out => $lname,
                                  ns_in    => $nsIn,
                                  ns_out   => $nsOut,
                                  auth     => $auth,
                                  matched  => $matched };
                }
            }
            # We have now gathered all direct assignments to loci. See
            # if we can also gather assignments via RNA. This should really
            # only be necessary if the direct locus matches are not available
            my $rnaMid = 'AR'; # $canonical->{$ns2};

            push @rows, $self->chain_conversions
                ( -id          => $id, 
                  -chain       => [$ns1, $rnaMid, $ns2],
                  -age         => $age,
                  -shrinkauth  => 1,
                  -nonull      => 1,
                  -flags       => [ 'strict' ],
                  -clusterleft => 1, # Only when FROM aps
                  -allscore    => [@probeSetMatchAuthorities],);
            @rows = $self->simplify_rows
                ( -rows        => \@rows,
                  -shrinkauth  => 1,
                  -clusterleft => 1, # Only when FROM aps
                  -cluster     => [@probeSetMatchAuthorities], );
        } else {
            @rows = $self->filter_conversion( $id, $ns1, 'AR', $ns2, $age);
        }
    }
    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head3 TRANSCRIPT_to_PROBESET

 Convert : RNA to Probesets
 Input   : AR RSR ENST PRDT
 MT Link : can be assayed with probe
 Output  : APS CLPS

=cut

&_set_conv('AR RSR ENST ENSE PRDT LL ENSG','APS CLPS BAPS BPS ILMN PHPS AGIL',
           'update_TRANSCRIPT_to_PROBESET');
sub update_TRANSCRIPT_to_PROBESET {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my $mt    = $self->tracker;

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    unless ($self->verify_namespace($id, $ns1) && $mt) {
        return [];
    }

    my @rows;
    if ($seq) {
        my @taxa  = $self->convert
            ( -id => $id, -ns1 => $ns1, -ns2 => 'TAX', -age => $age);
        my $mtClass = $self->maptracker_classes( $ns2 );
        my $toss  = "deprecated";
        if ($ns2 eq 'APS') {
            $toss = ['BRAINSET', $toss];
        }

        my $redges  = $mt->get_edge_dump
            ( -name      => $seq,
              -return    => 'object array',
              -keeptype  => 'can be assayed with probe',
              -tossclass => $toss,
              -keepclass => $mtClass,
              -keeptaxa  => $#taxa == -1 || $ns2 eq 'PHPS' ? undef : \@taxa,
              -dumpsql   => 0 );

        my %hits;
        my $tn = $ps2rnaMatchTag->{$ns1} || 'Fraction of probes matched';
        foreach my $re (@{$redges}) {
            my $aps  = $re->other_seq( $id )->name;
            my $vnum = 0;
            map { push @{$hits{$aps}{$_}{$vnum}}, -1 } 
            ($re->each_authority_name());
            foreach my $tag ($re->has_tag($tn)) {
                my $tvnum = $vnum;
                if (my $val = $tag->valname) {
                    # Associated with the sequence version number
                    my @bits = split(/\//, $val);
                    if ($#bits > 0) {
                        my @vs;
                        foreach my $bit (@bits) {
                            if ($bit =~ /\.(\d+)$/) {
                                push @vs, sprintf("%05d", $1);
                            }
                        }
                        $tvnum = join('', @vs);
                    } else {
                        $tvnum = $1 if ($val =~ /\.(\d+)$/);
                    }
                }
                push @{$hits{$aps}{$tag->authname}{$tvnum}}, $tag->num;
                if ($ns2 eq 'BAPS' && $aps =~ /^(BrAr:)[^:]+:(.+)/) {
                    push @{$hits{$1.$2}{$tag->authname}{$tvnum}}, $tag->num;
                }
            }
        }
        while (my ($aps, $auths) = each %hits) {
            my %scores;
            while ( my ($auth, $mdat) = each %{$auths}) {
                # Get the best sequence version
                my ($bestVers) = sort { $b <=> $a } keys %{$mdat};
                # Get the best score
                my ($matched)  = sort { $b <=> $a } @{$mdat->{$bestVers}};
                push @{$scores{$matched}}, $auth;
            }
            while (my ($matched, $auths) = each %scores) {
                $matched = undef if ($matched < 0);
                push @rows, { term_in  => $id,
                              term_out => $aps,
                              ns_in    => $nsIn,
                              ns_out   => $nsOut,
                              auth     => join($authJoiner, @{$auths}),
                              matched  => $matched };
            }
        }
        # warn "\n$id [$ns1 -> $ns2]\n".$self->rows_to_text(\@rows);
        if ($self->is_namespace($ns1, 'AL')) {
            # If we are starting with a locus, also bridge using RNA
            push @rows, $self->chain_conversions
                ( -id          => $id, 
                  -chain       => [$ns1, 'AR', $ns2],
                  -age         => $age,
                  -shrinkauth  => 1,
                  -nonull      => 1,
                  -flags       => [ 'strict' ],
                  #-clusterleft => 1, # Only when FROM aps
                  -allscore    => [@probeSetMatchAuthorities],);

            @rows = $self->simplify_rows
                ( -rows        => \@rows,
                  -shrinkauth  => 1,
                  #-clusterleft => 1, # Only when FROM aps
                  -cluster     => [@probeSetMatchAuthorities], );

        }
    }
    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut, };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

{ my @bios = &_full_kids( [qw(AR LL ENSG ENSE APS AGIL)] );
    
  &_set_conv( \@bios, 'CHR', 'update_OBJECTS_to_CHR', [qw(AAD)]);
}
sub update_OBJECTS_to_CHR {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );

    my ($id, $seq) = $self->fast_standardize( $idReq, $ns1 );
    my @rows;
    if ($seq) {
        my $maps = $self->mappings
            ( -id => $id, -ns1 => $ns1, -cols => 
              [qw(sub sub_start sub_end sub_vers strand score auth)] );
        foreach my $row (@{$maps}) {
            my ($chr, $s, $e, $build, $str, $sc, $au) = @{$row};
            next unless $chr;
            my $cname = sprintf("%s.%s:%d-%d", $chr, $build, $s, $e);
            my $auth  = $au || "Unknown";
            my @abits;
            push @abits, $build if ($build);
            push @abits, $str > 0 ? "+$str" : $str if ($str);
            $auth .= " [".join(' ', @abits)."]" unless ($#abits == -1);
            push @rows, { term_in  => $id,
                          term_out => $cname,
                          ns_in    => $nsIn,
                          ns_out   => $nsOut,
                          auth     => $auth,
                          matched  => $sc };
            
        }
    }
    if ($#rows == -1) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;    
}

=head2 Sets and Groups

Finding all members of a conceptual set or group. Note that
technically the L<Taxonomy Assignments|taxonomy_assignments> can be
considered group associations.

=head3 GENERIC_SETS

 Convert : Conceptual Set <-> Any namespace
 Input   : SET / ANY
 MT Link : has member / is a member of
 Output  : ANY / SET

The method will be used in the absence of more specific logic for the
non-SET namespace. All non-deprecated output terms will be recovered
using the MEMBEROF edge type.

=cut

&_set_conv( 'ANY', 'SET', 'update_GENERIC_SETS');
&_set_conv( 'SET', 'ANY', 'update_GENERIC_SETS');
sub update_GENERIC_SETS {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my $dbh   = $self->dbh;
    my $mt    = $self->tracker;

    my ($id, $obj) = $self->standardize_id( $idReq, $ns1 );
    if (!$obj && $ns1 eq 'SET') {
        # Maybe the set node is in the FreeText namespace?
        $obj = $mt->get_seq
            ( -name => "#FreeText#$idReq", -nocreate => 1, -defined => 1 );
        $id = $obj->name if ($obj);
    }

    unless ($self->verify_namespace($id, $ns1) && $mt) {
        return [];
    }

    my @rows;
    if ($obj) {
        my $toss  = ['DEPRECATED'];
        my $reads = 'is a member of';
        if ($nsIn eq 'Conceptual Set') {
            $reads = 'has member';
            my $check = $self->fast_edge_hash
                ( -name      => $obj,
                  -keeptype  => 'is a member of' );
            if ($check->{'Deprecated Collections'}) {
                # This set is designed to contain deprecated entries
                # Keep them all!
                $toss = undef;
            }
        } elsif ($self->is_namespace($ns1, 'AL')) {
            # Grr. UniGene is not really what we want here
            # Can get that by converting to UG
            push @{$toss}, 'UniGene';
        }

        $self->bench_start('Query MapTracker');
        my $struct = $self->fast_edge_hash
            ( -name      => $obj,
              -tossclass => $toss,
              -dumpsql   => 0,
              -keeptype  => $reads );
        $self->bench_end('Query MapTracker');

        my ($bestGuess);
        if ($reads eq 'has member') {
            my $found = [keys %{$struct}];
            if ($#{$found} == -1) {
                $struct = undef;
            } else {
                # We recovered members for the set
                # Are the members already in the desired namespace?
                my $ok = $self->filter_list_by_namespace
                    ($found, $ns2, undef, 'fast');
                if ($#{$ok} == -1) {
                    $self->bench_start('Guess member namespace');
                    # Nothing survives filter. Can we try to determine
                    # what the objects are?
                    my %guesses;
                    foreach my $oname (keys %{$struct}) {
                        my $ns = $self->guess_namespace($oname);
                        $guesses{$ns}{$oname} = 1 if ($ns);
                    }
                    # Standardize the guessed namespace hashes to lists:
                    while (my ($ns, $hash) = each %guesses) { 
                        $guesses{$ns} = [sort keys %{$hash}];
                    }
                    # Find the most populous guessed namespace:
                    ($bestGuess) = sort {
                        $#{$guesses{$b}} <=>
                            $#{$guesses{$a}}
                    } keys %guesses;
                    $ok = $guesses{$bestGuess} if ($bestGuess);
                    $self->bench_end('Guess member namespace');
                }
                if ($#{$ok} == -1) {
                    $struct = undef;
                } else {
                    my %keep = map { $_ => 1 } @{$ok};
                    map { delete $struct->{$_} unless ($keep{$_}) } @{$found};
                }
            }
        }
        if ($bestGuess) {
            # We found stuff, but it does not appear to be the right NS
            # Can we use it to go to a namespace that helps us?
            my $otok   = $self->namespace_token($nsOut);
            my $bgName = $self->namespace_name($bestGuess);
            if ($self->_func_for_tokens( $bestGuess, $otok )) {
                $self->edge_ids_to_authorities( $struct );
                my %primaries;
                $self->bench_start("Organize primary connections");
                while (my ($oname, $auths) = each %{$struct}) {
                    my @simp = $self->simplify_authors(@{$auths});
                    $primaries{$oname} = { term_in  => $id,
                                           term_out => $oname,
                                           ns_in    => $nsIn,
                                           ns_out   => $bgName,
                                           auth     => join($authJoiner, @simp),
                                           matched  => 1 };
                }
                $self->bench_end("Organize primary connections");
                $self->bench_start("Find secondary connections");
                # Convert in bulk:
                my @pids = keys %primaries;
                my $secRows = $self->convert
                    ( -id     => \@pids,
                      -ns1    => $bestGuess,
                      -ashash => 1,
                      -nonull => 1,
                      -ns2    => $nsOut,
                      -age    => $age );
                # Organize by primary ID:
                my %secByPri;
                foreach my $sR (@{$secRows}) {
                    push @{$secByPri{ $sR->{term_in} }}, $sR;
                }
                while (my ($pid, $prow) = each %primaries) {
                    # Convert the set members to the desired output namespace:
                    #my $secondary = $self->convert
                    #    ( -id     => $pid,
                    #      -ns1    => $bestGuess,
                    #      -ashash => 1,
                    #      -nonull => 1,
                    #      -ns2    => $nsOut,
                    #      -age    => $age );
                    #next if ($#{$secondary} == -1);
                    if (my $secondary = $secByPri{ $pid }) {
                        push @rows, $self->stitch_rows([$prow], $secondary);
                    }
                }
                $self->bench_end("Find secondary connections");
                @rows = $self->simplify_rows
                    ( -rows     => \@rows,
                      -avgscore => $ns2 eq 'TAX' ? 1 : 0, );
            }
        } elsif ($struct) {
            # Ok, it looks like the recovered items are proper namespace
            # (or this is ANY -> SET and we should take all)
            $self->edge_ids_to_authorities( $struct );
            while (my ($oname, $auths) = each %{$struct}) {
                my @simp = $self->simplify_authors(@{$auths});
                push @rows, { term_in  => $id,
                              term_out => $oname,
                              ns_in    => $nsIn,
                              ns_out   => $nsOut,
                              auth     => join($authJoiner, @simp),
                              matched  => 1 };
            }
        } else {
            
        }
        if ($useMTIDformat->{$ns2} || $ns2 eq 'AC') {
            foreach my $row (@rows) {
                my ($chem, $seq) = $self->standardize_id
                    ($row->{term_out}, $ns2);
                $row->{term_out} = $chem;
                $self->err("Unable to standardize putative chemical ID",
                           $chem) unless ($seq);
            }
        }
    }

    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
        
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}


&_set_conv( 'SYM', 'SET', 'update_SYMBOL_SETS');
&_set_conv( 'SET', 'SYM', 'update_SYMBOL_SETS');
sub update_SYMBOL_SETS {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $locns = 'AL';
    my $sfx   = " Gene Symbols";
    my $found;
    if ($ns1 eq 'SET') {
        if ($idReq =~ /^(.+)$sfx$/) {
            # This looks like the special gene symbol set
            my $tax = $1;
            my @genes = $self->convert
                ( -id => $tax, -ns1 => 'TAX', -ns2 => $locns, -age => $age );
            $found = $self->convert
                ( -ids => \@genes, -ns1 => $locns, -ns2 => $ns2, -nonull => 1,
                  -nullscore => -1,
                  -cols => ['term_out', 'matched', 'auth'], -age => $age );
            # We do not want to add the suffix to the output:
            $sfx = "";
        } else {
            # This is not a gene symbol set, use normal logic
            return $self->update_GENERIC_SETS( @_ );
        }
    } else {
        # We are starting with a symbol, find genes
        $found = $self->convert
            ( -id => $idReq, -ns1 => $ns1, -ns2 => $locns, -nonull => 1,
              -nullscore => -1,
              -cols => ['term_out', 'matched', 'auth'], -age => $age );
        # We now need to map the genes into taxae
        my %geneH = map { $_->[0] => {} } @{$found};
        my @genes = keys %geneH;
        my $mapgene = $self->convert
            ( -id => \@genes, -ns1 => $locns, -ns2 => 'TAX', -nonull => 1,
              -nullscore => -1,
              -cols => ['term_in','term_out'], -age => $age );
        map { $geneH{$_->[0]}{$_->[1]} = 1 } @{$mapgene};
        while (my ($gene, $taxH) = each %geneH) {
            my @tax = keys %{$taxH};
            # Assign a taxa to a gene only if it is unique:
            $geneH{$gene} = ($#tax == 0) ? $tax[0] : "";
        }
        map { $_->[0] = $geneH{$_->[0]} } @{$found};
    }
    my %byOut;
    # Structure the data by term, authorities, and score:
    foreach my $dat (@{$found}) {
        my ($out, $sc, $authTxt) = @{$dat};
        next unless ($out);
        foreach my $auth (split(/\Q$authJoiner\E/, $authTxt)) {
            push @{$byOut{$out}{$auth}}, $sc;
        }
    }
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my @rows;
    foreach my $out (sort keys %byOut) {
        # We will aggregate authoirities by shared score
        my %scH;
        while (my ($auth, $scA) = each %{$byOut{$out}}) {
            my ($sc) = sort { $b <=> $a } @{$scA};
            push @{$scH{$sc}}, $auth;
        }
        # For each score group, and a row with concatenated authorities:
        while (my ($sc, $authA) = each %scH) {
            push @rows,  { 
                term_in  => $idReq,
                term_out => "$out$sfx",
                ns_in    => $nsIn,
                ns_out   => $nsOut,
                auth     => join($authJoiner, sort @{$authA}),
                matched  => ($sc == -1) ? undef : $sc,
            };
        }
    }
    if ($#rows < 0) {
        @rows = ( { term_in  => $idReq,
                    ns_in    => $nsIn,
                    ns_out   => $nsOut, } ); 
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    # $self->bench_end();
    return \@rows;    
}


=head3 Probe Set Linkages

 NS1     : APS CLPS
 Link To : AR|RSR|ENST|PRDT
 NS2     : ANY (Conditional)

Links Affy and CodeLink probe sets to any other entries via their RNA
target(s).

=cut

&_set_conv( "ILMG", 'ILMN', "TIE_ILMN_ILMG");
&_set_conv( 'ILMN', "ILMG", "TIE_ILMN_ILMG");

sub TIE_ILMN_ILMG {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    my @rows;
    if ($seq) {
        my $type = ($ns1 eq 'ILMG') ?
            "is reliably aliased by" : "is a reliable alias for";
        my $struct = $self->fast_edge_hash
            ( -name      => $id,
              -keeptype  => $type);
        $self->edge_ids_to_authorities( $struct );
        while (my ($oname, $auths) = each %{$struct}) {
            my @simp = $self->simplify_authors(@{$auths});
            push @rows, { term_in  => $id,
                          term_out => $oname,
                          ns_in    => $nsIn,
                          ns_out   => $nsOut,
                          auth     => join($authJoiner, @simp),
                          matched  => 1 };
        }
    }
    if ($#rows == -1) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

&_set_conv( "ILMG", 'ANY', "LINK_ILMG", ["ILMG ILMN"]);
&_set_conv( 'ANY', "ILMG", "LINK_ILMG", ["ILMN ILMG"]);

sub LINK_ILMG {
    my $self = shift;
    my ($id, $ns2, $ns1, $age) = @_;
    # warn "$id [$ns1] -> $ns2\n";

    my @rows = $self->chain_conversions
        ( -id          => $id, 
          -chain       => [$ns1, "ILMN", $ns2 ], 
          -age         => $age,
          -shrinkauth  => 1,);

    if ($#rows == -1) {
        push @rows, { term_in  => $id,
                      ns_in    => $self->namespace_name($ns1),
                      ns_out   => $self->namespace_name($ns2) };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;    
}

{ my @skip = &_full_kids
      ( [qw(AC AR AAD APS APRB BAAD BAPS BAPB LL ENSG ENSE ORTH XONT CLPS CLAD SET TAX NS LT TRC ILMD ILMG PATH CHR)] );
  foreach my $ns (qw(APS BPS BAPS CLPS PHPS ILMN AGIL)) {
      &_set_conv($ns, 'ANY', "LINK_AFFY",
                 ["$ns $ns", map {"$ns $_"} @skip ]);
      &_set_conv('ANY',$ns, "LINK_AFFY",
                  ["$ns $ns", map {"$_ $ns"} @skip ]);
  }
}


sub LINK_AFFY {
    my $self = shift;
    my ($id, $ns2, $ns1, $age) = @_;

    my $clusLeft = ($ns1 =~ /^(A|PH)PS$/) ? 1 : 0;

    # WE NEED TO COME UP WITH OTHER TARGET NAMESPACES THAT SHOULD **NOT**
    # BE STRICT
    # Right now just have PMID

    my @rows = $self->chain_conversions
        ( -id          => $id, 
          -chain       => [$ns1, 'AR', $ns2 ], 
          -age         => $age,
          -shrinkauth  => 1,
          -flags       => $ns2 eq 'PMID' ? undef : [ 'strict' ],
          -nostrict    => 'MSIG',
          -clusterleft => $clusLeft,
          -allscore    => [ @probeSetMatchAuthorities ],);
    # warn "$id [$ns1] -> [$ns2]\n".$self->hashes_to_text(\@rows)."\n  ";
    @rows = $self->simplify_rows
        ( -shrinkauth  => 1,
          -clusterleft => $clusLeft,
          -allscore    => [ @probeSetMatchAuthorities ],
          -rows        => \@rows,
          ) unless ($#rows == -1);

    if ($#rows == -1) {
        push @rows, { term_in  => $id,
                      ns_in    => $self->namespace_name($ns1),
                      ns_out   => $self->namespace_name($ns2) };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;    
}

{ my @skip = &_full_kids
      ( [qw(AC AAD APRB BAAD BAPB ORTH APS BAPS BPS ILMG XONT CLPS CLAD SET NS LT CHR)] );
  foreach my $ns ("APRB", "BAPB") {
      &_set_conv($ns, 'ANY', "LINK_PROBES",
                 [map {"$ns $_"} ('TAX', @skip, &_full_kids(['AVAR'])) ]);
      &_set_conv('ANY',$ns, "LINK_PROBES",
                 [map {"$_ $ns"} (@skip) ]);
  }
}

sub LINK_PROBES {
    my $self = shift;
    my ($id, $ns2, $ns1, $age) = @_;

    my @rows = $self->chain_conversions
        ( -id          => $id, 
          -chain       => [$ns1, 'APS', $ns2 ], 
          -age         => $age,
          -shrinkauth  => 1,
          -flags       => [ 'strict' ], );

    if ($#rows == -1) {
        push @rows, { term_in  => $id,
                      ns_in    => $self->namespace_name($ns1),
                      ns_out   => $self->namespace_name($ns2) };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;    
}

&_set_conv( 'APRB',  [&_full_kids(['AVAR'])], 'update_PROBES_to_VARIANTS');
sub update_PROBES_to_VARIANTS {
    my $self = shift;
    my ($id, $ns2, $ns1, $age) = @_;
    my $nsOut = $self->namespace_name($ns2);
    my $nsIn  = $self->namespace_name( $ns1 );

    my $ml    = $self->maploc() || return [];
    my $cids  = $self->{MAPLOC_VAR_CATS} ||= [ map {
        $ml->get_text($_)->id() } ('dbSNP Polymorphisms',
                                   "1000 Genomes Polymorphisms") ];
    my @maps  = $self->mappings
        ( -id => $id, -ns1 => $ns1, -howbad => 0,
          -cols => ['sub', 'sub_vers', 'sub_ft', 'strand'] );
    my %bins;
    foreach my $row (@maps) {
        # Had been filtering to keep only highest build. However,
        # probes can match to multiple species. This runs the risk of
        # the most recent highly-numbered tunicate build taking
        # precedence over any human build

        my ($chr, $build, $ftext, $str) = @{$row};
        # warn "($chr, $build, $ftext, $str)";
        next unless ($chr);
        $chr =~ s/.+\.//;
        foreach my $hsp (split(/\,/, $ftext)) {
            if ($hsp =~ /^(\d+)\.\.(\d+)$/) {
                my ($s, $e) = ($1, $2);
                foreach my $lid ($ml->loc_ids_between_flanks
                                 ( $chr, $s-1, $e+1, $build)) {
                    my $loc = $ml->get_location( $lid );
                    # $self->msg("[+] Location $chr:$s..$e", $loc->to_text());
                    my $acc = $loc->best_accession();
                    my $pm  = $loc->population_maf( -catid => $cids );
                    while (my ($pid, $maf) = each %{$pm}) {
                        next unless ($maf);
                        my $bin  = int(10 * $maf);
                        my $bdat = $bins{$acc}{$bin} ||= [ 0, {} ];
                        $bdat->[0] = $maf if ($bdat->[0] < $maf);
                        $bdat->[1]{$pid} = 1;
                    }
                }
            }
        }
    }
    # One row per accession per 10% bin (which hopefully is one per location)
    my @rows;
    # We will be special handling 1000 genomes
    # We want to be sure that the classes are captured, rather than summarized
    my $oneKGclass = "1000 Genomes";
    while (my ($acc, $bH) = each %bins) {
        foreach my $bdat (values %{$bH}) {
            my ($sc, $pidH) = @{$bdat};
            my (%clss, @pops, @oneKG);
            foreach my $pid (keys %{$pidH}) {
                unless ( $self->{CACHED_ML_POP}{$pid}) {
                    my ($name, $cls) = ("", "Other");
                    if (my $pop  = $ml->get_population($pid)) {
                        $name = $pop->name();
                        if ($name =~ /^Pop:\d+/) {
                            $name = $pop->simple_value('Handle');
                            $cls  = "dbSNP";
                        } elsif ($name =~ /^1kG\.(.+)$/) {
                            $cls = $oneKGclass;
                            $name = $1;
                        }
                    }
                    $self->{CACHED_ML_POP}{$pid} = [ $name, $cls ];
                }
                my ($name, $cls) = @{$self->{CACHED_ML_POP}{$pid}};
                if ($name) {
                    if ($cls eq $oneKGclass) {
                        push @oneKG, $name;
                    } else {
                        push @pops, $name;
                        $clss{$cls}++;
                    }
                }
            }
            my @abits;
            my $tkg = join('.', sort @oneKG);
            push @abits, "1kG.$tkg" if ($tkg);
            push @abits, @pops if ($#pops != -1);
            my $auth = join($authJoiner, @abits);
            if (length($auth) > 100) {
                @abits = ();
                push @abits, "1kG.$tkg" if ($tkg);
                push @abits, map { "$clss{$_}x $_" } sort keys %clss;
                $auth = join($authJoiner, @abits);
            }
            push @rows, { term_in  => $id,
                          term_out => $acc,
                          ns_in    => $nsIn,
                          ns_out   => $nsOut,
                          auth     => $auth,
                          matched  => $sc };
        }
    }
    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

&_set_conv( 'LL',  'PATH', 'update_LOC_to_WIKIPATH');
&_set_conv( 'PATH', 'LL',  'update_LOC_to_WIKIPATH');
sub update_LOC_to_WIKIPATH {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $mt    = $self->tracker;
    my $nsreq = $self->namespace_token($ns1);
    my $nsOut = $self->namespace_name($ns2);
    my $nsIn  = $self->namespace_name( $ns1 );
    
    my ($id, $seq) = $self->fast_standardize( $idReq, $ns1, 1 );
    # return [] unless ($self->verify_namespace($id, $ns1) && $mt);
    my @rows;
    if ($seq) {
        my ($type, $cls);
        if ($ns1 eq 'LL') {
            ($type, $cls) = ('is a member of','WikiPathways');
        } else {
            ($type, $cls) = ('has member','Gene');            
        }
        my $edges  = $mt->get_edge_dump
            ( -name      => $seq,
              -return    => 'object array',
              -tossclass => 'deprecated',
              -keepclass => $cls,
              -keeptype  => $type );
        foreach my $edge (@{$edges}) {
            my $oname  = $edge->other_seq( $id )->name;
            my @simp   = $self->simplify_authors($edge->each_authority_name());
            my @scs    = (-1);
            foreach my $tag ($edge->has_tag("Assignment Confidence")) {
                my $sc = $tag->num();
                push @scs, $sc if (defined $sc);
            }
            my ($sc) = sort { $b <=> $a } @scs;
            push @rows, { term_in  => $id,
                          term_out => $oname,
                          ns_in    => $nsIn,
                          ns_out   => $nsOut,
                          auth     => join($authJoiner, @simp),
                          matched  => $sc };
        }
    }
    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

{ 
  my $ns1 = 'LL';
  my $ns2 = 'PATH';
  my @bio = &_full_kids( [qw(BIO) ], [$ns1, $ns2] );
  map { &_set_conv($_, $ns2, "LINK_WIKIPATHWAYS");
        &_set_conv($ns2, $_, "LINK_WIKIPATHWAYS"); } @bio;
}

sub LINK_WIKIPATHWAYS {
    my $self = shift;
    my ($id, $ns2, $ns1, $age) = @_;

    my @rows = $self->chain_conversions
        ( -id          => $id, 
          -chain       => [$ns1, 'LL', $ns2 ], 
          -age         => $age,
          #-shrinkauth  => 1,
          #-flags       => [ 'strict' ], 
          );

    if ($#rows == -1) {
        push @rows, { term_in  => $id,
                      ns_in    => $self->namespace_name($ns1),
                      ns_out   => $self->namespace_name($ns2) };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;    
}

&_set_conv( 'NS', 'PATH', 'update_NS_to_PATH');
sub update_NS_to_PATH {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn   = $self->namespace_name( $ns1 );
    my $nsOut  = $self->namespace_name( $ns2 );
    my $id     = $nsOut;
    my $mt     = $self->tracker;
    return [] unless ($mt);
    my $top = $self->fast_edge_hash
        ( -name      => "All WikiPathways",
          -keepclass => 'Group',
          -tossclass => 'Deprecated',
          -keeptype  => "has member");
    my %wpIds;
    foreach my $taxGrp (sort keys %{$top}) {
        my $mem = $self->fast_edge_hash
            ( -name      => $taxGrp,
              -keepclass => 'WikiPathways',
              -tossclass => 'Deprecated',
              -keeptype  => "has member");
        foreach my $wpid (sort keys %{$mem}) {
            if ($wpid =~ /^WikiPathways:\d+$/) {
                $wpIds{$wpid} = 1;
            }
        }
    }
    my @rows = map { { term_in  => $id,
                       term_out => $_,
                       ns_in    => $nsIn,
                       ns_out   => $nsOut,
                       auth     => 'WikiPathways',
                       matched  => 1 } } sort keys %wpIds;

    push @rows, { term_in    => $id,
                  ns_in      => $nsIn,
                  ns_out     => $nsOut } if ($#rows == -1);

    $self->dbh->update_rows( 'conversion', \@rows )
        unless (uc($idReq) ne uc($id));
    return \@rows;
}

=head3 NS_to_AAD

 Convert : Namespace to Affy Array Design
 Input   : NS
 MT Link : has member
 Output  : AAD

This method will return a list of all known Affymetrix array
designs. The input term is irrelevant; the search will always be
performed using the MapTracker name 'Affymetrix Designs' and recorded
in the database with a term_in of 'Affy Array Design'.

=cut

&_set_conv( 'NS', 'AAD', 'update_NS_to_AAD');
&_set_conv( 'NS', 'BAAD', 'update_NS_to_AAD');
sub update_NS_to_AAD {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn   = $self->namespace_name( $ns1 );
    my $nsOut  = $self->namespace_name( $ns2 );
    my $id     = $nsOut;
    my $mt     = $self->tracker;
    return [] unless ($mt);
    my ($rName, $auth);
    if ($ns2 eq 'BAAD') {
        ($rName, $auth) = ('BrainArray Designs', 'BrainArray');
    } else {
        ($rName, $auth) = ('Affymetrix Designs', 'Affymetrix');
    }
    my $root   = $mt->get_seq($rName);
    my $struct = $self->fast_edge_hash
        ( -name      => $root,
          -keeptype  => "has member");

    my @rows = map { { term_in  => $id,
                       term_out => $_,
                       ns_in    => $nsIn,
                       ns_out   => $nsOut,
                       auth     => $auth,
                       matched  => 1 } } sort keys %{$struct};

    $self->dbh->update_rows( 'conversion', \@rows )
        unless (uc($idReq) ne uc($id));
    return \@rows;
}

&_set_conv( 'NS', 'ILMD', 'update_NS_to_ILMD');
sub update_NS_to_ILMD {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn   = $self->namespace_name( $ns1 );
    my $nsOut  = $self->namespace_name( $ns2 );
    my $id     = $nsOut;
    my $mt     = $self->tracker;
    return [] unless ($mt);
    my ($setName, $auth);
    my $root   = $mt->get_seq('Illumina BeadArray');
    my $struct = $self->fast_edge_hash
        ( -name      => $root,
          -keeptype  => "has member");

    my @rows = map { { term_in  => $id,
                       term_out => $_,
                       ns_in    => $nsIn,
                       ns_out   => $nsOut,
                       auth     => 'Illumina',
                       matched  => 1 } } sort keys %{$struct};

    $self->dbh->update_rows( 'conversion', \@rows )
        unless (uc($idReq) ne uc($id));
    return \@rows;
}

=head3 NS_to_APS

 Convert : Namespace to Affy Probe Set
 Input   : NS
 Output  : APS

This method will return a list of all known Affymetrix probe sets. The
input term is irrelevant; the database will always record a term_in of
'Affy Probe Set'. Probes will be found by first recovering all known
array designs, and then getting all probes in each design.

=cut

&_set_conv( 'NS', 'APS', 'update_NS_to_APS');
sub update_NS_to_APS {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn   = $self->namespace_name( $ns1 );
    my $nsOut  = $self->namespace_name( $ns2 );
    my $id     = $nsOut;
    my ($noUpdate, @probes);
    if (uc($id) ne uc($idReq)) {
        # Recurse through convert() to make sure we use the right ID
        @probes = $self->convert
            ( -id => $id, -ns1 => $ns1, -ns2 => $ns2, -age => $age);
        $noUpdate = 1;
    } else {
        my @arrays = $self->convert( -id => 'Affy Array Design', -ns1 => 'NS',
                                     -ns2 => 'AAD', -age => $age);
        @probes = $self->convert( -id => \@arrays, -ns1 => 'AAD',
                                  -ns2 => 'APS', -age => $age);
    }
    my @rows = map { { term_in  => $id,
                       term_out => $_,
                       ns_in    => $nsIn,
                       ns_out   => $nsOut,
                       auth     => 'Affymetrix',
                       matched  => 1 } } @probes;

    $self->dbh->update_rows( 'conversion', \@rows ) unless ($noUpdate);
    return \@rows;
}

&_set_conv( 'NS', 'PHPS ILMN', 'update_NS_to_PHPS');
sub update_NS_to_PHPS {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn   = $self->namespace_name( $ns1 );
    my $nsOut  = $self->namespace_name( $ns2 );
    my $id     = $nsOut;
    my ($noUpdate, @probes);
    my ($setName, $auth);
    if ($ns2 eq 'PHPS') {
        ($setName, $auth) = ('Pharmagene Probe Sets', 'Pharmagene');
    } elsif ($ns2 eq 'ILMN') {
        ($setName, $auth) = ('Illumina Probes', 'Illumina');
    } else {
        $self->err("Unknown target namespace '$ns2'");
        return [];
    }
    if (uc($id) ne uc($idReq)) {
        # Recurse through convert() to make sure we use the right ID
        @probes = $self->convert
            ( -id => $id, -ns1 => $ns1, -ns2 => $ns2, -age => $age);
        $noUpdate = 1;
    } else {
        my $root   = $self->tracker->get_seq($setName);
        my $struct = $self->fast_edge_hash
            ( -name      => $root,
              -keeptype  => "has member");
        @probes = sort keys %{$struct};
    }
    my @rows = map { { term_in  => $id,
                       term_out => $_,
                       ns_in    => $nsIn,
                       ns_out   => $nsOut,
                       auth     => $auth,
                       matched  => 1 } } @probes;

    $self->dbh->update_rows( 'conversion', \@rows ) unless ($noUpdate);
    return \@rows;
}

=head2 Ontologies

Associating ontology terms or other classifiers with biological entities

=head3 OBJECT_to_ONTOLOGY

 Convert : Objects to GO or BFTO
 Input   : AP RSP ENSP SP TR UP ENSG LL AL NRDB 
 MT Link : has attribute
 Output  : GO

Also converts :

  LL  -> BTFO
  APS -> XONT

UniProt chains through RSP. The authority will be the evidence code
for the assignment.

=cut

&_set_conv
    ( 'AP AR AL AMBS BMSS RSR RSP LL ENST ENSP ENSG SP TR UP IPI NRDB UG',
      'GO BTFO', 'update_OBJECT_to_ONTOLOGY');
&_set_conv( 'APS',  'XONT', 'update_OBJECT_to_ONTOLOGY');
&_set_conv( 'PMID', 'MESH', 'update_OBJECT_to_ONTOLOGY');
sub update_OBJECT_to_ONTOLOGY {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $mt    = $self->tracker;
    my $nsreq = $self->namespace_token($ns1);
    my $nsOut = $self->namespace_name($ns2);
    my $nsIn  = $self->namespace_name( $ns1 );
    
    my ($id, $seq) = $self->fast_standardize( $idReq, $ns1 );
    return [] unless ($self->verify_namespace($id, $ns1) && $mt);

    my @rows;
    if ($seq) {
        my $meth = \&_to_onto_logic;
        # What namespaces can we expect direct connections to the ontology?
        my @using = ($ns1);
        my $linkGene = 0;
        if ($ns2 eq 'GO') {
            # ENSG had been left out (27 Apr 2015). Not sure why
            # The absence caused an asymetry in GO>OBJ vs OBJ>GO
            @using = qw(LL RSP RSR ENSG ENSP UP);
            $linkGene = 1;
        } elsif ($ns2 eq 'BTFO') {
            @using = qw(LL);
            $linkGene = 1;
        }
        my $valid = $self->valid_namespace_hash(@using);
        # Get direct assignments, if appropriate:
        @rows     = &{$meth}($self, $id, $ns1, $ns2, $age) if ($valid->{$ns1});
        if ($linkGene) {
            # Expand to include gene linkages
            my @exp = $self->expand_through_cloud
                ( -rows  => \@rows,
                  -id    => $id,
                  -ns1   => $ns1,
                  -ns2   => $ns2,
                  -age   => $age,
                  -meth  => $meth,
                  -using => \@using,
                  -opts  => 'NoOscillate NoMult',
                  # -opts  => 'maxedge 6',
                  -type  => 'GeneCluster', );
            unless ($#exp == -1) {
                push @rows, @exp;
                # We need to assure that NS_IN is appropriate - for example,
                # we may have queried with NM_001234 [AR] - the paths from
                # the cloud will be using the guessed namespace [RSR], and
                # will need to be re-cast as [AR].
                map { $_->{ns_in} = $nsIn } @rows;
            }
        }
        # print "$id [$ns1] -> [$ns2]\n".$self->hashes_to_text(\@rows)."\n  ";
        # Now expand to include all parent terms
        @rows = $self->_expand_to_include_parents( \@rows, $ns2, $ns2, $age );
        if ($#rows < 0 && $self->deep_dive && 
            $self->namespace_is_sequence($ns1)) {
            # 1. Nothing found in MapTracker
            # 2. We have not already re-iterated
            # 3. deep dive is requested, and 
            # 4. the item should be a sequence
            # Query SeqStore to see if we can find more data
            my $change = $self->update_maptracker_sequence( $id );
            return $self->update_OBJECT_to_ONTOLOGY( $id, $ns2, $ns1, $age, 1)
                if ($change);
        }
    }
    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

sub _expand_to_include_parents {
    my $self = shift;
    my ($rows, $ns2, $ns1, $age) = @_;
    my @expanded;
    my $nsOut = $self->namespace_name($ns2);
    foreach my $row (@{$rows}) {
        push @expanded, $row;
        my @pars = $self->all_parents( -id     => $row->{term_out},
                                       -age    => $age,
                                       -ns     => $ns2,
                                       -noself => 1);
        next if ($#pars == -1);
        my %pRow  = %{$row};
        my @auths = split(/ \< /, $pRow{auth});
        splice(@auths, 1, 0, 'Inheritance');
        $pRow{auth} = join(' < ', @auths);
        $pRow{ns_between} = $pRow{ns_between} ? 
            "$nsOut < " . $pRow{ns_between} : $nsOut;
        foreach my $par (@pars) {
            push @expanded, {%pRow, term_out => $par};
        }
    }
    my @rv = $self->simplify_rows
        ( -rows    => \@expanded,
          -filter  => ['bestscore', \&_filter_by_evidence_code, 'bestmid'],
          -bestmid => 0 );
    return wantarray ? @rv : \@rv;
}

sub _filter_by_evidence_code {
    my ($self, $targ, $args) = @_;
    my @sorter;
    foreach my $row (@{$targ}) {
        my $pAuth = $row->[2] || '';
        $pAuth =~ s/\s+\[.+//;
        my ($sc) = sort { 
            $b <=> $a
            } map {
                $evRank->{$_} || 0
                } split(/\Q$authJoiner\E/, $pAuth);
        push @sorter, [ $sc, $pAuth, $row];
    }
    @sorter = sort {
        $b->[0] <=> $a->[0] || $a->[1] cmp $b->[1]
        } @sorter;
    my $best = $sorter[0][0];
    my @keep;
    foreach my $dat (@sorter) {
        last if ($dat->[0] < $best);
        push @keep, $dat->[2];
    }
    return \@keep;
}

sub _to_onto_logic {
    my $self = shift;
    my ($id, $ns1, $ns2, $age) = @_;
    my $mt     = $self->tracker;
    my $isGo   = ($ns2 eq 'GO') ? 1 : 0;
    my $ecTag  = $isGo ? 'GO Evidence' : ($ns2 eq 'XONT' || $ns2 eq 'MESH') ? '' : 'Evidence';
    my $class  = $self->primary_maptracker_class( $ns2 );
    my $nsIn   = $self->namespace_name( $ns1 );
    my $nsOut  = $self->namespace_name( $ns2 );
    my $list   = $mt->get_edge_dump
        ( -name      => $id,
          -return    => 'object array',
          -keeptype  => 'has attribute',
          -keepclass => $class,
          -tossclass => 'deprecated' );
    my %ontos;
    foreach my $edge (@{$list}) {
        # make sure we use up-to-date alias
        my $onto  = $edge->node1->name;
        $onto     = $self->go_alias($onto , $age ) || $onto if ($isGo);
        my @auths = $edge->each_authority_name();
        my @ecs;
        my $score = 1;
        if ($ecTag) {
            foreach my $etag ($edge->has_tag($ecTag)) {
                push @ecs, $etag->valname;
            }
            # If Affy is the only authority, we do not fully trust it:
            $score = -1 if ($#auths == 0 && $auths[0] eq 'Affymetrix');
        } elsif ($ns2 eq 'XONT') {
            @ecs = ('IEP');
        } elsif ($ns2 eq 'MESH') {
            @ecs = ('');
        }
        @ecs = ('NR') if ($#ecs == -1);
        map { my $ec = $_;
              map { $ontos{$onto}{$score}{$ec}{$_} = 1 } @auths } @ecs;
    }
    my @rows;
    while (my ($onto, $scores) = each %ontos) {
        my ($score) = sort { $b <=> $a } keys %{$scores};
        my @auths;
        while (my ($ec, $ahash) = each %{$ontos{$onto}{$score}}) {
            my @aus;
            foreach my $au (sort keys %{$ahash}) {
                # Ignore old GO_EC nomenclature:
                push @aus, $au unless ($au =~ /^GO_/);
            }
            @aus = ("Unknown") if ($#aus == -1);
            my $auth = join(',',@aus);
            push @auths, $ec ? "$ec [$auth]" : $auth;
        }
        foreach my $auth (@auths) {
            # Unrecorded evidence is always null matched value:
            my $lsc = ($auth eq 'NR' || $score < 0) ? undef : $score;
            push @rows, { term_in  => $id,
                          term_out => $onto,
                          ns_in    => $nsIn,
                          ns_out   => $nsOut,
                          auth     => $auth,
                          matched  => $lsc };
        }
    }
    return @rows;
}

our $msigPreferredNS = [ 'RefSeq Protein', 'Ensembl Protein', 'Swiss-Prot',
                         'RefSeq RNA', 'Ensembl Transcript', ];
our $msigPreferHash  = { 
    map { $msigPreferredNS->[$_] => $_ + 1 } (0..$#{$msigPreferredNS})
};

sub update_MSIG_to_LL {
    my $self  = shift;
    my $mt    = $self->tracker;
    return [] unless ($mt);
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name($ns2);
    my @rows;
    if ($idReq =~ /^(\S+) \((.+)\)/) {
        # Taxa-specified request
        $idReq = $1;
        my ($taxa, $tobj) = $self->standardize_taxa($2);
        return [] unless ($tobj);
        my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
        return [] if (length($id) > 100); # IGNORE OVERLENGTH
        my $tid = "$id ($taxa)";
        if ($seq) {
            # Recover all loci for the term:
            my $allRows = $self->convert
                ( -id  => $id, -ns1 => $ns1, -ns2 => $ns2, 
                  -age => $age, -ashash => 1, );
            # Determine the taxa of each loci
            my %outs = map { $_->{term_out} => 1 } @{$allRows};
            my $taxes  = $self->convert
                ( -id => [keys %outs], -ns1 => $ns2, -ns2 => 'TAX',
                  -cols => ['term_in','term_out'], -age => $age );
            my %okOut;
            foreach my $trow (@{$taxes}) {
                my ($out, $otax) = @{$trow};
                $okOut{$out}++ if ($out && $otax eq $taxa);
            }
            # Filter the rows
            foreach my $row (@{$allRows}) {
                next unless ($okOut{$row->{term_out}});
                $row->{term_in} = $tid;
                push @rows, $row;
            }
        }
        if ($#rows == -1) {
            push @rows, { term_in  => $tid,
                          ns_in    => $nsIn,
                          ns_out   => $nsOut };
        }
        $self->dbh->update_rows( 'conversion', \@rows );
        return \@rows;
    }
    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    return [] if (length($id) > 100); # IGNORE OVERLENGTH
    if ($seq) {
        my $struct = $self->fast_edge_hash
            ( -name      => $id,
              -tossclass => 'deprecated',
              -keepclass => 'Gene',
              -keeptype  => 'is attributed to' );
        $self->edge_ids_to_authorities( $struct );
        while (my ($oname, $auths) = each %{$struct}) {
            my @simp = $self->simplify_authors(@{$auths});
            push @rows, { term_in  => $id,
                          term_out => $oname,
                          ns_in    => $nsIn,
                          ns_out   => $nsOut,
                          auth     => join($authJoiner, @simp),
                          matched  => 1 };
        }
        my %direct   = map { $_->{term_out} => 1 } @rows;
        my @dirLoc   = keys %direct;
        my @indirect = $self->convert
            ( -id     => \@dirLoc, 
              -ns1    => $ns2, 
              -ns2    => 'ORTH', 
              -age    => $age,
              -nonull => 1,
              -ashash => 1, );
        $self->_collapse_orthologue_authors( \@indirect, $nsOut);
        push @rows, $self->stitch_rows(\@rows, \@indirect);
        foreach my $kid ($self->direct_children( $id, $ns1, $age )) {
            my $kidRows = $self->convert
                ( -id  => $kid, -ns1 => $ns1, -ns2 => $ns2, 
                  -age => $age, -ashash => 1, );
            my $fakeRow = {
                term_in  => $id,
                term_out => $kid,
                ns_in    => $nsIn,
                ns_out   => $nsIn,
                auth     => 'Inheritance',
                matched  => 1,
            };
            foreach my $row ( $self->stitch_rows([$fakeRow], $kidRows) ) {
                push @rows, $row;
                # There is probably a better way to do this
                # cleaning up Inheritance < Inheritance chains
                $row->{auth}       =~
                    s/( < Inheritance){2,}/ < Inheritance/;
                $row->{ns_between} =~ 
                    s/( < MSigDB Gene Set){2,}/ < MSigDB Gene Set/;
            }
        }
        
        @rows = $self->simplify_rows( -rows    => \@rows, );
    }
    if ($#rows == -1) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

sub _collapse_orthologue_authors {
    my $self = shift;
    my ($rows, $nsOut) = @_;
    foreach my $row (@{$rows}) {
        $row->{ns_out} = $nsOut if ($nsOut);
        next unless ($row->{auth} =~ /\[RBM\]/);
        # This is a reciprocal best match row, which will be common
        my %auths;
        foreach my $au (split(/ < /, $row->{auth})) {
            if ($au =~ /(.+) \[RBM\]/) {
                $auths{$1}++;
            }
        }
        $row->{auth} = "Orthologue < ".join
            ($authJoiner, map { sprintf("%s [RBM%s]", $_, $auths{$_} == 1 ? 
                                  "" : "x". $auths{$_})} sort keys %auths);
        my %intNS;
        foreach my $ns (split(/ < /, $row->{ns_between})) {
            $intNS{$ns} = $msigPreferHash->{$ns} ||= 999;
        }
        my ($ins) = sort { $intNS{$a} <=> $intNS{$b} || $a cmp $b } keys %intNS;
        $row->{ns_between} = $ins;
    }
}

sub update_LL_to_MSIG {
    my $self  = shift;
    my $mt    = $self->tracker;
    return [] unless ($mt);
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name($ns2);
    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    my @rows;
    if ($seq) {
        my %queries = ( $id => [] );
        my $orths   = $self->convert
            ( -id => $id, -ns1 => $ns1, -ns2 => 'ORTH',
              -nonull => 1, -ashash => 1, -age => $age);
        $self->_collapse_orthologue_authors( $orths, $nsIn);
        foreach my $row (@{$orths}) {
            # $row->{ns_out} = $nsIn;
            push @{$queries{$row->{term_out}}}, $row;
        }
        while (my ($qid, $oRows) = each %queries) {
            my $struct = $self->fast_edge_hash
                ( -name      => $qid,
                  -tossclass => 'deprecated',
                  -keepclass => 'MSigDB',
                  -keeptype  => 'has attribute' );
            $self->edge_ids_to_authorities( $struct );
            my @direct;
            while (my ($oname, $auths) = each %{$struct}) {
                next if (length($oname) > 100); # IGNORE OVERLENGTH
                my @simp = $self->simplify_authors(@{$auths});
                push @direct, { term_in  => $qid,
                                term_out => $oname,
                                ns_in    => $nsIn,
                                ns_out   => $nsOut,
                                auth     => join($authJoiner, @simp),
                                matched  => 1 };
            }
            next if ($#direct == -1);
            if ($#{$oRows} == -1) {
                # These assignments are direct from the query
                push @rows, @direct;
            } else {
                # These are rows reached via an orthologue
                # Stitch the orthologue edges to the direct ones
                push @rows, $self->stitch_rows($oRows, \@direct);
            }
        }
        @rows = $self->simplify_rows
            ( -rows    => \@rows, );
        @rows = $self->_expand_to_include_parents
            ( \@rows, $ns2, $ns2, $age );
    }
    if ($#rows == -1) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

&_set_conv( "MSIG", "ANY", "CONDITIONAL_TO_LL",   ["MSIG LL","MSIG SET" ]);
&_set_conv( "ANY", "MSIG", "CONDITIONAL_FROM_LL", ["LL MSIG", 'NS MSIG' ]);

=head3 ONTOLOGY_to_OBJECT

 Convert : Ontology terms to objects
 Input   : GO
 MT Link : is attributed to
 Output  : AP RSP ENSP SP TR UP ENSG LL AL

Also converts:

  BTFO -> LL AL
  XONT -> APS

=cut

&_set_conv( 'GO BTFO',
            'AP AR AL AMBS BMSS RSR RSP LL ENST ENSP ENSG SP TR UP IPI NRDB UG',
            'update_ONTOLOGY_to_OBJECT');
&_set_conv( 'XONT', 'APS', 'update_ONTOLOGY_to_OBJECT');
# Small test GO term: GO:0004770 (3 seconds)
# Small test: GO:0032452 (5 sec)
# Medium test: GO:0016895 (40 sec)
# Medium test: GO:0032813 (60 sec)
# Long test: GO:0009975 (320 sec)
sub update_ONTOLOGY_to_OBJECT {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );

    my ($id, $seqId) = $self->fast_standardize( $idReq, $ns1 );
    unless ($self->verify_namespace($id, $ns1)) {
        return [];
    }
    # warn "GO Request: $idReq [$ns1] -> $ns2 returns $id = ".($seqId || '???????') if ($idReq =~ /^GO/);
    my @rows;
    my @generic = qw(AL AR AP);
    if ($seqId) {
        for my $g (0..$#generic) {
            my $gns = $generic[$g];
            if ($self->is_namespace($ns2, $gns) && $ns2 ne $gns) {
                # The user wants is using a specific query that is
                # represented by a more generic namespace. Get the
                # general output and then filter to the specific
                # namespace.
                @rows = $self->filter_conversion( $id, $ns1, $gns, $ns2, $age);
                $seqId  = undef;
                # warn "Filtering $id -> $gns to $ns2 ( $id, $ns1, $gns, $ns2, $age) = $#rows\n";
                last;
            }
        }
    }
    if ($seqId) {
        my $meth = \&_from_onto_logic;
        my @using = ('APS');
        my $ecTag = 'Evidence';
        if ($ns1 eq 'GO') {
            @using = @generic;
            $ecTag = 'GO Evidence';
        } elsif ($ns1 eq 'BTFO') {
            @using = qw(AL);
        } elsif ($ns1 eq 'XONT') {
            $ecTag = '';
        }

        my %objs;
        # We will recover objects through three paths, processed from
        # least authoritative to most authoritative:
        # 1. Inheritance from more specific child terms
        # 2. Transitively from linked objects in GeneClusters
        # 3. From direct assignment

        # One row per object/evidence code pair. The highest score
        # will be kept, or the shortest path when the score is tied,
        # or the more authoritative source when both are tied.

        # Start with child terms:
        my @kids    = $self->direct_children( $id, $ns1, $age );
        my $kidRows = $self->convert
            ( -id => \@kids, -ns1 => $ns1, -ns2 => $ns2, -age => $age) || [];
        foreach my $row (@{$kidRows}) {
            my ($obj, $atxt) = ($row->[0], $row->[2]);
            next unless ($obj && $atxt);
            my ($auth, $extAuth) = &_extract_ec( $atxt );
            $objs{$obj}{$auth} = [$row->[3], $row->[4], $extAuth];
        }

        my $valid = $self->valid_namespace_hash(@using);
        # Get direct assignments, if appropriate:
        my @directRows  = &{$meth}($self, $id, $ns1, $ns2, $age, $ecTag)
            if ($valid->{$ns2});

        if ($self->is_namespace($ns2, @generic)) {
            # We can try to enter a GeneCluster through a different
            # gene namespace in an attempt to reach the desired
            # namespace.

            my @seed;
            # Use available generic namespaces to try to recover GeneClusters
            map { push @seed, &{$meth}
                  ( $self, $id, $ns1, $_, $age, $ecTag)
                      unless ($self->is_namespace($_, $ns2) ) } @generic;
            # The direct assignments will also be used as seeds:
            push @seed, @directRows;

            # warn "Expanding through cloud with ".($#seed + 1); warn $self->showbench;

            # Use these connections to seed GeneCluster clouds
            my @indirectRows = $self->expand_through_cloud
                    ( -rows      => \@seed,
                      -cleanseed => 1,
                      -terminal  => 1,
                      -ns2       => $ns2,
                      -age       => $age,
                      -nonrdb    => 1,
                      -opts      => 'NoOscillate NoMult perfect maxedge4',
                      -type      => 'GeneCluster', );


            # Add the indirect rows to the structure:
            $self->_keep_best_rows( \%objs, \@indirectRows );
        }

        # Finally, add the direct rows to the structure:
        $self->_keep_best_rows( \%objs, \@directRows );
        # Now convert the structure into a formal set of rows:
        @rows = ();
        while (my ($obj, $auths) = each %objs) {
            while (my ($auth, $dat) = each %{$auths}) {
                my ($score, $btwn, $extAuth) = @{$dat};
                # Unrecorded evidence is always null matched value:
                $score = undef if ($auth eq 'NR');
                $auth = "$extAuth < $auth" if ($extAuth);
                push @rows, { term_in    => $id,
                              term_out   => $obj,
                              ns_in      => $nsIn,
                              ns_out     => $nsOut,
                              auth       => $auth,
                              ns_between => $btwn || '',
                              matched    => $score };
            }
        }
    }
    push @rows, { term_in  => $id,
                  ns_in    => $nsIn,
                  ns_out   => $nsOut, } if ($#rows < 0);

    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

sub _extract_ec {
    my ($auth) = @_;
    if ($auth =~ /(.+) \< ([^\<]+)$/) {
        # Multiple authorities, the last is the evidence code
        return ($2, $1);
    }
    return ($auth);
}

sub _keep_best_rows {
    my $self = shift;
    $self->benchstart();
    my ($objs, $newRows) = @_;
    my @rowKeys = qw(term_out auth matched ns_between);
    foreach my $row (@{$newRows}) {
        my ($obj, $atxt, $score, $mid) = map { $row->{$_} } @rowKeys;
        next unless ($obj && $atxt);
        my ($auth, $extAuth) = &_extract_ec( $atxt );
        if (my $prior = $objs->{$obj}{$auth}) {
            # There is prior info for this entry - should we replace?
            my ($psc, $pmid) = @{$prior};
            my $checkMid = 0;
            if (!defined $psc && !defined $score) {
                # Neither defined, we'll check the ns_between
                $checkMid = 1;
            } elsif (!defined $score) {
                # The current score is undefined - worse than defined
                next;
            } elsif (!defined $psc) {
                # Prior score not defined - keep the current
            } elsif ($psc > $score) {
                # Prior score is better than current
                next;
            } elsif ($psc == $score) {
                # Equal scores, check the number of intermediate nodes
                $checkMid = 1;
            }
            if ($checkMid) {
                my @pbits = split(/ \< /, $pmid || '');
                my @bits  = split(/ \< /, $mid  || '');
                next if ($#bits > $#pbits);
            }
        }
        $objs->{$obj}{$auth} = [$score, $mid, $extAuth];
    }
    $self->benchend;
}

sub _from_onto_logic {
    my $self = shift;
    $self->benchstart;
    my ($id, $ns1, $ns2, $age, $ecTag) = @_;
    my $mt     = $self->tracker;
    my $class  = $self->primary_maptracker_class( $ns2 );
    my $nsIn   = $self->namespace_name( $ns1 );
    my $nsOut  = $self->namespace_name( $ns2 );

    # Now get direct assignments
    $self->benchstart("Query raw ontology edges");
    my $struct = $self->fast_edge_hash
        ( -name      => $id,
          -keeptype  => 'is attributed to',
          -keepclass => $class,
          -tossclass => 'deprecated' );
    $self->benchend("Query raw ontology edges");

    my $isSeq = $self->namespace_is_sequence($ns2);
    my @rows;
    while (my ($rawname, $eidHash) = each %{$struct}) {
        my $obj = $isSeq ? &strip_version($rawname) : $rawname;
        next unless ($self->verify_namespace($obj, $ns2));
        my @auths;
        my $score = 1;
        if ($ecTag) {
            my @eids  = keys %{$eidHash};
            # The authorities are the evidence codes:
            @auths    = $self->tags_for_edge_ids( \@eids, $ecTag );
            # If Affy is the only authority, we do not fully trust it:
            my @names = $self->auths_for_edge_ids( \@eids );
            $score = undef if ($#names == 0 && $names[0] eq 'Affymetrix');
        } elsif ($ns1 eq 'XONT') {
            @auths = ('IEP');
        }
        @auths = ('NR') if ($#auths < 0);
        # Direct assignments will over-write the child assignments
        push @rows, map { {
            term_in    => $id,
            term_out   => $obj,
            ns_in      => $nsIn,
            ns_out     => $nsOut,
            auth       => $_,
            matched    => $score
        } } @auths;
    }
    $self->benchend;
    return @rows;
}

=head3 PROTEIN_to_FAMILY

 Convert : Protein <=> Family
 Input   : AP RSP ENSP / CDD IPR
 MT Link : has feature / is a feature on
 Output  : CDD IPR / AP RSP ENSP

NEED: Get RS for input CDD

=cut

{ my @prots = &_full_kids( ['AP'] );
  my @pairs = ( [ "CDD", "AP ENSP RSP SP TR UP"],
                [ "IPR", \@prots] );

  # NOTE - if more ontologies are added, must also alter the line
  #    if ($ns2 eq 'CDD' || $ns2 eq 'IPR' || ...

  # IPR => is a feature on IPI
  # IPR is attributed to IPI SP
  foreach my $pair (@pairs) {
      my ($na, $nb) = @{$pair};
      &_set_conv($na, $nb, 'update_PROTEIN_to_FAMILY');
      &_set_conv($nb, $na, 'update_PROTEIN_to_FAMILY');
  }
}

sub update_PROTEIN_to_FAMILY {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsOut = $self->namespace_name( $ns2 );
    my $nsIn  = $self->namespace_name( $ns1 );

    my @rows;
    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    # warn "$id [$ns1] = ".$seq->id() if ($idReq eq 'SKMLCK');
    unless ($self->verify_namespace($id, $ns1)) {
        return [];
    }
    if ($seq) {
        if ($ns2 eq 'CDD' || $ns2 eq 'IPR' || $ns2 eq 'AP') {
            # Get direct assignments
            my $meth  = \&_prot_family_logic;
            @rows     = &{$meth}( $self, $id, $ns1, $ns2 );
            my @exp;
            if ($ns2 eq 'AP') {
                # We are starting with an ontology term
                # Use ProteinCluster to expand output
                @exp = $self->expand_through_cloud
                    ( -rows     => \@rows,
                      -cleanseed => 1,
                      -terminal => 1,
                      -ns2      => $ns2,
                      -age      => $age,
                      -type     => 'ProteinCluster', );                
                map { $_->{ns_out} = $nsOut } @exp;
            } else {
                # We are starting with a protein but not AP
                # Use ProteinCluster to expand input
                my $fullRows = $self->exhaustive_cloud_expansion
                    ( -id   => $id,
                      -cleanseed => 1,
                      -ns1  => $ns1,
                      -nsint => 'AP',
                      -ns2  => $ns2,
                      -age  => $age,
                      -meth => $meth,
                      -type => 'ProteinCluster', );
                my @simp = $self->simplify_rows( -rows    => $fullRows,
                                                 -shrinkauth => 1,
                                                 -bestmid => 1,
                                                 -show    => 0);
                
                $self->dbh->update_rows( 'conversion', \@simp );
                
                foreach my $row (@simp) {
                    if ($row->{term_in} eq  $id) {
                        push @exp, $row;
                        $row->{ns_in} = $nsIn;
                    }
                }
                # print "QUERY ONLY:\n".$self->hashes_to_text( \@exp )."SIMPLIFIED:\n".$self->hashes_to_text( \@simp )."FULL:\n".$self->hashes_to_text( $fullRows );
 
            }
            unless ($#exp == -1) {
                push @rows, @exp;
                @rows = $self->simplify_rows( -rows    => \@rows,
                                              -bestmid => 1,
                                              -show    => 0);
            }
            
        } else {
            # Get data for All Protein, and filter for this namespace
            @rows = $self->filter_conversion( $id, $ns1, 'AP', $ns2, $age);
        }
    }
    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut, };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    # warn $self->show_bench();
    return \@rows;
}

sub _prot_family_logic {
    my $self = shift;
    $self->benchstart();
    my ($id, $ns1, $ns2, $age) = @_;
    # warn "($id, $ns1, $ns2, $age)\n";
    my $nsIn   = $self->namespace_name( $ns1 );
    my $nsOut  = $self->namespace_name( $ns2 );
    my $class  = $self->primary_maptracker_class( $ns2 );
    my $type;
    if ($ns1 eq 'IPR') {
        $type = ['is attributed to', 'is a feature on'];
    } elsif ($ns2 eq 'IPR') {
        $type = ['has attribute', 'has feature'];
    } elsif ($self->is_namespace($ns1, 'AP')) {
        $type = 'has feature';
    } else {
        $type = "is a feature on";
    }
    my $struct = $self->fast_cached_edge_hash
        ( "ProtFamily-$id-$ns1-$ns2-AUTH",
          -name      => $id,
          -tossclass => 'deprecated',
          -keepclass => $class,
          -keeptype  => $type);
    my @rows;
    while (my ($oname, $auths) = each %{$struct}) {
        next unless ($self->verify_namespace( $oname, $ns2 ));
        my @simp = $self->simplify_authors(@{$auths});
        push @rows, { term_in    => $id,
                      term_out   => $oname,
                      ns_in      => $nsIn,
                      ns_out     => $nsOut,
                      auth       => join($authJoiner, @simp),
                      matched    => 1 };
    }
    $self->benchend();
    return @rows;
}



{  my @geneNS = &_full_kids( ['AL', 'AR'], ['IUO'] );
   &_set_conv( 'IPR', \@geneNS, "update_GENE_THINGS_VIA_PROTEIN");
}

sub update_GENE_THINGS_VIA_PROTEIN {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $dbh   = $self->dbh;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    unless ($self->verify_namespace($id, $ns1)) {
        return [];
    }
    my @rows;
    if ($seq) {
        # Find direct connections between the input and proteins:
        my @prots = $self->convert
            ( -id     => $id, 
              -ns1    => $ns1, 
              -ns2    => 'AP', 
              -age    => $age,
              -nonull => 1,
              -directonly => 1,
              -ashash => 1, );
        my $edges = 4;
        if ($self->is_namespace($ns2, 'AL')) {
            $edges += 2;
        } elsif ($self->is_namespace($ns2, 'AR')) {
            $edges += 1;
        }
        @rows = $self->expand_through_cloud
            ( -rows      => \@prots,
              -cleanseed => 1,
              -terminal  => 1,
              -ns2       => $ns2,
              -noNRDB    => 1,
              -opts     => "NoOscillate NoMult maxedge$edges",
              -age       => $age,
              -type      => 'GeneCluster', );
        map { $_->{ns_out} = $nsOut } @rows;
        unless ($#rows == -1) {
            @rows = $self->simplify_rows( -rows    => \@rows,
                                          -bestmid => 0,
                                          -show    => 0);
        }
    }
    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut, };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head2 expand_through_cloud

 Title   : expand_through_cloud
 Usage   : $ad->expand_through_cloud( @args )
 Function: Generate a new AccessDenorm object
 Returns : A blessed object
 Args    : Associative array of arguments. Recognized keys [Default]:

      -age [90] Global data freshness, in days. Calls age().

  -tracker Optional BMS::MapTracker object. If not provided, one will
           be created.

=cut

sub expand_through_cloud {
    my $self   = shift;
    my $args   = $self->parseparams( @_ );
    my $rowsIn = $args->{ROWS};
    my $age    = $args->{AGE};
    my $type   = $args->{TYPE};
    my $ns1    = $args->{NS1};
    my $ns2Req = $args->{NS2};
    my $meth   = $args->{METH};
    my $opts   = $args->{OPTS};
    my $using  = $args->{USING};
    my $noNRDB = $args->{NONRDB};
    my (%input, %output);
    if ($#{$rowsIn} == -1) {
        if (my $id = $args->{ID}) { $input{$id} = $ns1 }
    } else {
        %input  = map { $_->{term_in}        => $_->{ns_in}  } @{$rowsIn};
        %output = map { $_->{term_out} || '' => $_->{ns_out} } @{$rowsIn};
        delete $output{''};
    }
    my $isTerm = $args->{TERMINAL} ? 1 : 0;
    my $source = ($isTerm) ? \%output : \%input;
    my (@found, @ns2s, @seeds);
    if ($ns2Req) {
        @ns2s = ref($ns2Req) ? @{$ns2Req} : ($ns2Req);
    } else {
        my %sought = map { $_ => 1 } values %output;
        @ns2s = keys %sought;
    }
    return @found unless ($isTerm || $#ns2s != -1);
    $self->benchstart;
    if ($using) {
        $using = $self->valid_namespace_hash( @{$using} );
    }
    while (my ($id, $ns) = each %{$source}) {
        # NRDB entries can trigger a lot of convert() calls to find their
        # taxae - user can skip using these as seeds if desired.
        next if ($noNRDB && $id =~ /^P\d+_\d+$/);
        if ($ns1) {
            # An explicitly defined seed NS is requested
            push @seeds, [$id, $ns1] if ($self->is_namespace($ns, $ns1));
        } else {
            # Use all input nodes as found
            push @seeds, [$id, $ns];
        }
    }
    if ($#seeds == -1) {
        $self->benchend;
        return @found;
    }
    my @clouds = $self->cached_clouds
        ( -age    => $self->cloud_age || $age,
          -cleanseed => $args->{CLEANSEED},
          -seed   => \@seeds,
          -type   => $type );
    # warn "Recovered ".($#clouds+1)." Clouds [$isTerm]\n".$self->showbench;
    if ($isTerm) {
        # We may loop over many clouds. Prepare the input row first
        my $inBin = $self->prepare_bin($rowsIn, 'isFirst');
        foreach my $cloud (@clouds) {
            # The cloud itself contains the terminal results
            my $paths = $cloud->paths
                ( -ns2  => ($#ns2s == -1) ? undef : \@ns2s,
                  -opts => $opts,);
            # warn "Terminal (".(join(',',@ns2s)||'')."):\n".$cloud->paths_to_text( $paths );
            #my @stitched = $self->stitch_rows($inBin, $paths);
            #push @found, @stitched;
            # warn $self->hashes_to_text(\@stitched);
            # warn $self->showbench;
            push @found, $self->stitch_rows($inBin, $paths);
        }
    } else {
        foreach my $cloud (@clouds) {
            my @novel;
            # The cloud is being used as an alternative starting point,
            # we still need to expand from the cloud to the final NS2
            # Does the target get us where we want to go?
            my $nodes = $cloud->all_nodes();
            while (my ($name, $ns) = each %{$nodes}) {
                # Ignore nodes that were found in either the input or
                # output of our original rows:
                next if ($input{$name} || $output{$name});
                # If we have specified that only certain namespaces are
                # useful expansion targets, skip if the node is not valid:
                next if ($using && !$using->{$ns});
                my @expand;
                foreach my $ns2 (@ns2s) {
                    if ($meth) {
                        # Use defined mapping logic (avoids recursion)
                        # $self->benchstart("ExpandCloudMethod");
                        push @expand, &{$meth}($self, $name, $ns, $ns2, $age);
                        # $self->benchend("ExpandCloudMethod");
                    } else {
                        # Standard conversion logic
                        push @expand, $self->convert
                            ( -id => $name, -ns1 => $ns, -ns2 => $ns2,
                              -ashash => 1, -age => $age, -nonull => 1);
                    }
                }
                # Now find which of the rows (if any) are actually new
                map { my $n = $_->{term_out};
                      push @novel, $_ if ($n && !$output{$n}) } @expand;
                next if ($#novel == -1);
                # This cloud node is able to contribute new information
                # We now need to find the best path from the seed(s) to
                # the node.
                my $paths = $cloud->paths( -id2  => $name,
                                           -ns2  => $ns,
                                           -opts => $opts,);
                my @stitched = $self->stitch_rows($paths, \@novel);
                # warn "($#stitched) $name [$ns]\n".$cloud->paths_to_text( $paths );
                # warn "($#stitched) $name [$ns]\n".$cloud->paths_to_text( \@stitched );
                push @found, @stitched;
                
            }
        }
    }

    my @rv = $self->simplify_rows( -rows  => \@found  );
    if ($isTerm) {
    } else {
        
    }
    $self->benchend;
    return @rv;
}


sub exhaustive_cloud_expansion {
    # This method is designed to return a single result, but to also invest
    # the time to calculate all results for the "siblings" of the primary
    # query. The idea is that these other results will probably be desired
    # as well, and it is more effecient to calculate them in bulk
    my $self   = shift;
    $self->benchstart();
    my $args   = $self->parseparams( @_ );
    my $type   = $args->{TYPE};
    my $id     = $args->{ID};
    my $ns1    = $args->{NS} || $args->{NS1};
    my $ns2    = $args->{NS2};
    my $age    = $args->{AGE};
    my $nsInt  = $args->{NSINT} || $ns1;
    my $meth   = $args->{METH} || $args->{METHOD};
    my $opts   = $args->{OPTS};
    
    my $intName = $self->namespace_name( $nsInt );
    my $ns2Name = $self->namespace_name( $ns2 );
    my %keepNS = $self->valid_namespace_hash( $nsInt || $ns1);
    my @clouds = $self->cached_clouds
        ( -age    => $age || $self->cloud_age(),
          -cleanseed => $args->{CLEANSEED},
          -seed   => [ [$id, $ns1] ],
          -type   => $type );
    my (@allRows, %allIDs);
    foreach my $cloud (@clouds) {
        my $nodes = $cloud->all_nodes();
        my (@using, %nns, %cloudName, @direct);
        while (my ($n, $ns) = each %{$nodes}) {
            if ($keepNS{$ns}) {
                my $qry = $n;
                if ($crossSpeciesNs->{$ns} && $n =~ /^(\S+) \((.+)\)/) {
                    $qry = $1;
                }
                unless ($nns{$qry}) {
                    $nns{$qry} = $ns;
                    $cloudName{$qry} = $n;
                    push @using, $qry;
                    $allIDs{$qry}++;
                }
            }
        }
        foreach my $name (@using) {
            push @direct, &{$meth}( $self, $name, $nns{$name}, $ns2, $age );
            # Reset the namespace
            map { $_->{ns_in} = $intName } @direct;
        }
        push @allRows, @direct;

        # Ok, we have all the *direct* connections to nodes within the
        # cloud We presume that this is a smaller (maybe much smaller)
        # subset of nodes We will start with these "useful" nodes and
        # find all paths to all other nodes, then invert the paths to
        # have all the possible source-to-target paths from this cloud
        
        my %useful;
        foreach my $dir (@direct) {
            push @{$useful{$dir->{term_in}}}, $dir;
        }
        while (my ($qry, $dirs) = each %useful) {
            my $paths = $cloud->paths
                ( -id   => $cloudName{$qry},
                  -ns1  => $nns{$qry},
                  -ns2  => $nsInt,
                  -opts => $opts,
                  -invert => 1);
            # Reset the namespace for the join
            map { $_->{ns_out} = $intName } @{$paths};
            my @stitched = $self->stitch_rows($paths, \@direct );
            push @allRows, @stitched;
        }
    }
    # Make note of any IDs that failed to have a row
    # Also reset the input namespace
    map { delete $allIDs{ $_->{term_in} };
          $_->{ns_in} = $intName; } @allRows;

    push @allRows, map { {
        term_in => $_,
        ns_in   => $intName,
        ns_out  => $ns2Name,
        
    } } sort keys %allIDs;
    $self->benchend;
    return \@allRows;
}

*rows_to_text = \&hashes_to_text;
sub hashes_to_text {
    my $self = shift;
    $self->benchstart;
    my ($hashes) = @_;
    my %colH;
    map { $colH{$_} ||= 1 } map { keys %{$_} } @{$hashes};
    my @cols = sort keys %colH;
    my @table;
    foreach my $hash (@{$hashes}) {
        my @row = map { $hash->{$_} } @cols;
        push @table, \@row;
    }
    $self->benchend;
    return $self->text_table(\@table, \@cols);
}

sub text_table {
    my $self = shift;
    my ($rowRef, $head) = @_;
    $self->benchstart;
    my @rows =  @{$rowRef};
    unshift @rows, $head if ($head);
    my @widths = map { 0 } @{$head ? $head : $rows[0]};
    if ($#widths == -1) {
        $self->benchend;
        return "Empty Table\n";
    }
    foreach my $row (@rows) {
        map { my $l = defined $row->[$_] ? length($row->[$_]) : 0; 
              $widths[$_] = $l if ($widths[$_] < $l) } (0..$#{$row});
    }
    my $frm = "| ".join(' | ', map { '%'.$_.'s' } @widths)." |\n";
    my $line = sprintf($frm, map { '-' x $_ } @widths);
    $line =~ s/\|/+/g; $line =~ s/ /-/g;
    my $txt  = $line;
    $txt    .= sprintf($frm, @{shift @rows}) . $line if ($head);
    foreach my $row (@rows) {
        $txt .= sprintf($frm, map { defined $row->[$_] ? $row->[$_] : '' }
                        (0..$#widths));
    }
    $txt .= $line;
    $self->benchend;
    return $txt;
}


sub trim_rows {
    my $self = shift;
    my ($rows, $exRows) = @_;
    # Make note of any IDs already gathered
    my %exclude = map { $_->{term_out} || '' => 1 } @{$exRows || []};
    $exclude{''} = 1;
    my %struct;
    foreach my $row (@{$rows}) {
        my $id2 = $row->{term_out} || '';
        next if ($exclude{$id2});
        my $match = defined $row->{matched} ? $row->{matched} : -1;
        my @btwn  = split(" < ", $row->{ns_between} || '');
        push @{$struct{$id2}{$match}{$#btwn}}, $row;
    }
    my @keep;
    foreach my $id2 (sort keys %struct) {
        # Take the best score
        my ($match) = sort { $b <=> $a } keys %{$struct{$id2}};
        # Take the least intermediate namespaces:
        my ($btwn) = sort { $a <=> $b } keys %{$struct{$id2}{$match}};
        my @rows = @{$struct{$id2}{$match}{$btwn}};
        push @keep, @rows;
    }
    return @keep;
}

sub prepare_bin {
    my $self   = shift;
    my ($rows, $isFirst, $isLast) = @_;
    return { count => 0 } unless ($rows);
    # Bin may already have been prepared
    return $rows if (ref($rows) eq 'HASH');
    # 12.63us
    # $self->benchstart;
    my $bin = {
        rows     => $rows,
        count    => $#{$rows} + 1,
        term_in  => {},
        term_out => {},
    };
    unless ($isFirst) {
        foreach my $row (@{$rows}) {
            push @{$bin->{term_in}{$row->{term_in}}}, $row;
        }
    }
    unless ($isLast) {
        foreach my $row (@{$rows}) {
            if (my $out = $row->{term_out}) { 
                push @{$bin->{term_out}{$out}}, $row;
            }
        }
    }
    # $self->benchend;
    return $bin;
}

my @stitchBetween = ( ['ns_between','ns_out'], ['term_between','term_out'] );
sub stitch_rows {
    my $self = shift;
    $self->bench_start();
    my @bins = map { $self->prepare_bin( $_ ) } @_;
    while ($#bins > 0) {
        # Find the smallest bin to use as the 'primary' bin
        my ($smallest) = sort { 
            $bins[$a]{count} <=> $bins[$b]{count} ||
                $a <=> $b 
            } (0..$#bins);
        my $pri = $bins[$smallest];
        unless ($pri->{count}) {
            # If the smallest bin is empty then our chains have collapsed
            @bins = ();
            last;
        }
        # 14.07us
        # $self->benchstart('find links');
        # Identify the bins next to the smallest one:
        my @neighbors;
        push @neighbors, $smallest - 1 if ($smallest);
        push @neighbors, $smallest + 1 if ($smallest < $#bins);
        # ... and pick the smallest of those to use as the 'secondary'
        my ($nextSmall) = sort { 
            $bins[$a]{count} <=> $bins[$b]{count} ||
                $a <=> $b 
            } @neighbors;
        my $sec = $bins[$nextSmall];

        # Determine the relative order of primary and secondary:
        my $priBeforeSec = ($smallest < $nextSmall) ? 1 : 0;
        my ($pKey, $sKey) = $priBeforeSec ? 
            ('term_out', 'term_in') : ('term_in', 'term_out');

        # Now try to find in/out pairs that allow us to build links
        # between the primary and secondary bins

        my @links;
        for my $r (0..$#{$pri->{rows}}) {
            if (my $secRows = $sec->{$sKey}{ $pri->{rows}[$r]{$pKey} || ''}) {
                # We can link this row in the primary bin to at least
                # one row in the neighboring bin
                my $row = $pri->{rows}[$r];
                if ($priBeforeSec) {
                    push @links, map { [$row, $_] } @{$secRows};
                } else {
                    push @links, map { [$_, $row] } @{$secRows};
                }
            }
        }
        # $self->benchend('find links');
        if ($#links == -1) {
            # No links found at all
            @bins = ();
            last;
        }
        # 39.74us
        # $self->benchstart('stitch links');
        my @stitched;
        for my $l (0..$#links) {
            my ($pre, $pro) = @{$links[$l]};
            # Initialize the row as being the prior member of the pair
            my %row = %{$pre};
            push @stitched, \%row;

            # REMEMBER:
            # The ' < '-joined chains are in REVERSE order

            # Extend the authority chain
            $row{auth} = join(' < ', $pro->{auth}, $row{auth});
            # Build ns_between
            my @nsb;
            push @nsb, $pro->{ns_between} if ($pro->{ns_between});
            my ($nsb1, $nsb2) = ($row{ns_out}, $pro->{ns_in});
            # For the stitched namespace, use the more specific one
            if ($self->is_namespace($nsb1, $nsb2)) {
                push @nsb, $nsb1;
            } else {
                push @nsb, $nsb2;
            }
            push @nsb, $row{ns_between} if ($row{ns_between});
            $row{ns_between} = join(' < ', @nsb);

            # Build term_between
            my @tmb;
            push @tmb, $pro->{term_between} if ($pro->{term_between});
            push @tmb, $row{term_out};
            push @tmb, $row{term_between} if ($row{term_between});
            $row{term_between} = join(' < ', @tmb);



            # Build 'between' nodes
           # foreach my $btk (@stitchBetween) {
           #     my ($btwn, $out) = @{$btk};
           #     my @arr;
           #     push @arr, $pro->{$btwn} if ($pro->{$btwn});
           #     push @arr, $row{$out};
           #     push @arr, $row{$btwn} if ($row{$btwn});
           #     $row{$btwn} = join(' < ', @arr);
           # }

            # The output values should be those of the later member:
            $row{term_out} = $pro->{term_out};
            $row{ns_out}   = $pro->{ns_out};

            my $sc = $row{matched};
            if (defined $sc) {
                # Update the score
                if (defined $pro->{matched}) {
                    if ($sc >= 0 && $pro->{matched} >= 0) {
                        # Score is the product of the pair, round to 4 decimals
                        $row{matched} = 
                            int(0.5 + 10000 * $sc * $pro->{matched}) / 10000
                            || 0.0001;
                    } else {
                        # If either value is -1, the score is undef:
                        $row{matched} = undef;
                    }
                } else {
                    # If the later member is undef, the pair becomes so too
                    $row{matched} = undef;
                }
            }
        }
        # $self->benchend('stitch links');

        my ($min, $max) = sort { $a <=> $b } ($smallest, $nextSmall);
        my $stitchBin   = $self->prepare_bin
            ( \@stitched, $min == 0 ? 1 : 0, $max == $#bins ? 1 : 0 );
        splice(@bins, $min, 2, $stitchBin);
    }
    $self->bench_end();
    return ($#bins == -1) ? () : @{$bins[0]{rows}};
}

=head3 OBJECT_to_PMID

 Convert : Objects to a PubMed identifier
 Input   : AL AP AR ENSG ENSP ENST GO IPI LL RSP RSR SP TR UG UP
 MT Link : is referenced in
 Output  : PMID

Queries with LL or ENSG will spawn a chained search using their
associated proteins and RNA (AP+AR).

=cut

{ my $na = "PMID";
  my @pmTargs = &_full_kids( ['AC','AR','AP','AL'] );
  my $nb = join(' ', @pmTargs, qw(GO));
  # my $nb = "AL AP AR ENSG ENSP ENST GO IPI LL RSP RSR SP TR UG UP";
  &_set_conv( $na, $nb, 'update_PUBMED_TIE' );
  &_set_conv( $nb, $na, 'update_PUBMED_TIE' );
}

sub update_PUBMED_TIE {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    unless ($self->verify_namespace($id, $ns1)) {
        return [];
    }
    my @rows;
    if ($seq) {
        # Get direct PubMed assignment edges:
        my $objNs = ($ns1 eq 'PMID') ? $ns2 : $ns1;
        my $meth  = \&_pubmed_logic;
        @rows = &{$meth}( $self, $id, $ns1, $ns2 );
        if ($self->is_namespace($objNs, 'AL', 'AR', 'AP')) {
            # Expand through the full gene cluster
            my @exp;
            # PubMed IDs are currently only linked to a handful of IDs:
            my @using = qw(UP RSR RSP LL);
            if ($ns1 eq 'PMID') {
                # We need to make sure we've linked to something we
                # can expand through
                my @seed = @rows;
                foreach my $ns (qw(AR AL AP)) {
                    push @seed, &{$meth}( $self, $id, $ns1, $ns )
                        unless ($self->is_namespace($ns, $objNs));
                }
                @exp = $self->expand_through_cloud
                    ( -rows => \@seed,
                      -cleanseed => 1,
                      -terminal => 1,
                      -ns2  => $ns2,
                      -age  => $age,
                      -type => 'GeneCluster', );
                # Make sure that the output namespace matches what we
                # want (it may otherwise be more specific):
                map { $_->{ns_out} = $nsOut } @exp;
            } else {
                @exp = $self->expand_through_cloud
                    ( -rows  => \@rows,
                      -cleanseed => 1,
                      -id    => $id,
                      -ns1   => $ns1,
                      -ns2   => $ns2,
                      -age   => $age,
                      -meth  => $meth,
                      -using => \@using,
                      -type  => 'GeneCluster', );
                # We need to remap the input namespace
                # For example, an AR request against a RefSeq RNA will
                # recover rows with an RSR namespace:
                map { $_->{ns_in} = $nsIn } @exp;
                # warn "$id [$ns1] -> [$ns2] via ".scalar(@rows)." rows\n".$self->hashes_to_text(\@exp)."\n  ";
            }
            unless ($#exp == -1) {
                # die "$id [$ns1] -> [$ns2]\n".$self->hashes_to_text(\@rows)."\n  ";
                push @rows, @exp;
                @rows = $self->simplify_rows
                    ( -rows    => \@rows,
                      # -shrinkauth => 1,
                      -bestmid => 1,
                      -show    => 0);
            }
        }
    }
    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut, };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

sub _pubmed_logic {
    my $self = shift;
    my ($id, $ns1, $ns2) = @_;
    my $class  = $self->primary_maptracker_class( $ns2 );
    my $isFwd  = ($ns1 eq 'PMID') ? 1 : 0;
    my $type   = $isFwd ? "contains a reference to" : "is referenced in";
    # Forcing the authority to be LocusLink
    # There is a bunch of junk that has come in via SwissProt and RefSeq
    # They reference malformed or incorrect PMIDs
    my $list   = $self->tracker->get_edge_dump
        ( -name      => $id,
          -return    => 'object array',
          -keepclass => $class,
          -tossclass => 'deprecated',
          -keepauth  => 'LocusLink',
          -keeptype  => $type );
    my %struct;
    foreach my $edge (@{$list}) {
        my $other = $edge->other_seq($id);
        my $oname = $other->name;
        next unless ($self->verify_namespace($oname, $ns2));
        map { $struct{$oname}{$_}++ } $edge->each_authority_name;
    }
    my @rows;
    my ($nsIn, $nsOut) = map { $self->namespace_name($_) } ($ns1, $ns2);
    foreach my $oname (sort keys %struct) {
        my @simp = $self->simplify_authors(keys %{$struct{$oname}});
        my $auth = join($authJoiner, @simp);
        $auth    = $isFwd ? "$auth < TAS" : "TAS < $auth";
        push @rows, { term_in  => $id,
                      term_out => $oname,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut,
                      auth     => $auth,
                      matched  => 1 };
    }
    # warn sprintf("%20s [%4s] => [%4s] (%d)\n", $id, $ns1, $ns2,$#rows+1) if (1);
    return @rows;
}

=head3 NS_to_GO

 Convert : Namespace to GeneOntology Term
 Input   : NS
 Output  : GO

This function simply gets all current GO terms by utilizing
get_all_go_subsets().

=cut

&_set_conv( 'NS', 'GO', 'update_NS_to_GO');
sub update_NS_to_GO {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );

    my @sets  = $self->get_all_go_subsets();
    my %terms;
    foreach my $set (@sets) {
        my @gos = $self->convert
            ( -id => $set, -ns1 => 'SET', -ns2 => $ns2, -age => $age );
        map { $terms{$_}++ } @gos;
    }
    my @rows;
    foreach my $goid (sort keys %terms) {
        push @rows, { term_in  => $nsOut,
                      term_out => $goid,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut,
                      auth     => 'GeneOntology',
                      matched  => 1 };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head3 NS_to_MSIG

 Convert : Namespace to MSiGDB entries
 Input   : NS
 Output  : MSIG

This function simply gets all current MSIG terms

=cut

&_set_conv( 'NS', 'MSIG', 'update_NS_to_MSIG');
sub update_NS_to_MSIG {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $id    = $self->namespace_name($idReq);
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    return [] unless ($id eq $nsOut);

    # Get the high-level categories:
    my $struct = $self->fast_edge_hash
        ( -name      => 'MSigDB Gene Sets',
          -keepclass => 'MSigDB',
          -tossclass => 'Deprecated',
          -keeptype  => "has member");

    # Then get all the child nodes:
    my $doWarn = $self->{CONV_CACHE} ? $self->{CONV_CACHE}{WARN} : undef;
    foreach my $cat (keys %{$struct}) {
        my @kids = $self->all_children( $cat, $ns2, $age, $doWarn );
        map { $struct->{$_} = 1 } @kids;
    }
    my @rows;
    foreach my $msig (sort keys %{$struct}) {
        next if (length($msig) > 100); # IGNORE OVERLENGTH
        push @rows, {
            term_in  => $id,
            term_out => $msig,
            ns_in    => $nsIn,
            ns_out   => $nsOut,
            auth     => 'MSigDB',
            matched  => 1,
        };
    }

    @rows = ( { term_in  => $id,
                ns_in    => $nsIn,
                ns_out   => $nsOut, } ) if ($#rows == -1);

    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

&_set_conv( 'NS', 'CDD', 'update_NS_to_CDD');
sub update_NS_to_CDD {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $id    = $self->namespace_name($idReq);
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    return [] unless ($id eq $nsOut);

    my $struct = $self->fast_edge_hash
        ( -name      => 'Conserved Domain Database',
          -keepclass => 'CDD',
          -tossclass => 'Deprecated',
          -keeptype  => "has member");

    my @rows = map {{ 
        term_in  => $id,
        term_out => $_,
        ns_in    => $nsIn,
        ns_out   => $nsOut,
        auth     => 'NIH',
        matched  => 1,
    } } sort keys %{$struct};
    @rows = ( { term_in  => $id,
                ns_in    => $nsIn,
                ns_out   => $nsOut, } ) if ($#rows == -1);

    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

&_set_conv( 'NS', 'IPR', 'update_NS_to_IPR');
sub update_NS_to_IPR {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $id    = $self->namespace_name($idReq);
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    return [] unless (uc($id) eq uc($nsOut));

    my $mt    = $self->tracker;
    my @seqs  = $mt->get_seq( "IPR%" );
    my %ids;
    foreach my $seq (@seqs) {
        my $id = $seq->name();
        $ids{$id}++ if ($id =~ /^IPR\d{6}$/);
    }

    my @rows = map {{ 
        term_in  => $id,
        term_out => $_,
        ns_in    => $nsIn,
        ns_out   => $nsOut,
        auth     => 'InterPro',
        matched  => 1,
    } } sort keys %ids;

    @rows = ( { term_in  => $id,
                ns_in    => $nsIn,
                ns_out   => $nsOut, } ) if ($#rows == -1);

    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

&_set_conv( 'NS', 'PMID', 'update_NS_to_PMID');
sub update_NS_to_PMID {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $id    = $self->namespace_name($idReq);
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    return [] unless ($id eq $nsOut);

    my $mt    = $self->tracker;
    my $linked  = $mt->get_edge_dump
        ( -name      => 'PMID:%',
          -orient    => 1,
          -keeptype  => "contains a reference to" );
    my %ids = map { $_ => 1 } map { $mt->get_seq( $_->[0] )->name } @{$linked};

    my @rows = map {{ 
        term_in  => $id,
        term_out => $_,
        ns_in    => $nsIn,
        ns_out   => $nsOut,
        auth     => 'PubMed',
        matched  => 1,
    } } sort keys %ids;

    @rows = ( { term_in  => $id,
                ns_in    => $nsIn,
                ns_out   => $nsOut, } ) if ($#rows == -1);

    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head2 Orthology and Similarity

Converters to link loci, protein and RNA between different species, as
well as tools to find the 'same' object between two namespaces within
the same species.

=head3 HOMOLOGENE_TIE

 Convert : Homologene <-> Protein
 Input   : HG / RSP
 MT Link : is a cluster with sequence / is a sequence in cluster 
 Output  : RSP / HG

Homologene data are primarily tied to protein. Loci and transcripts
are then chained from protein.

=cut

&_set_conv( 'HG', 'RSP AP', 'update_HOMOLOGENE_TIE');
&_set_conv( 'RSP AP', 'HG', 'update_HOMOLOGENE_TIE');
&_set_conv( "HG", 'ANY', "CONDITIONAL_TO_RSP", 
            ["HG RSP", "HG AP"]);
&_set_conv( 'ANY', "HG", "CONDITIONAL_FROM_RSP",
            ["RSP HG", "AP HG" ]);
sub update_HOMOLOGENE_TIE {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsOut = $self->namespace_name( $ns2 );
    my $nsIn  = $self->namespace_name( $ns1 );
    my $mt    = $self->tracker;
    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    unless ($self->verify_namespace($id, $ns1) && $mt) {
        return [];
    }
    my @rows;
    if ($seq) {
        my $class = $self->primary_maptracker_class( $ns2 );
        my $type  = ($ns1 eq 'HG') ? 'is a cluster with sequence'
            : 'is a sequence in cluster';
        my $list =  $mt->get_edge_dump
            ( -name      => $seq,
              -return    => 'object array',
              -keeptype  => $type,
              -tossclass => 'deprecated',
              -keepclass => $class,
              -dumpsql   => 0 );
        foreach my $edge (@{$list}) {
            my $oname   = $edge->other_seq( $seq )->name;
            my @simp   = $self->simplify_authors
                ( $edge->each_authority_name );
            push @rows, { term_in  => $id,
                          term_out => $oname,
                          ns_in    => $nsIn,
                          ns_out   => $nsOut,
                          auth     => join($authJoiner, @simp),
                          matched  => 1 };
        }
    }
    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head3 OBJECT_to_ORTH

 Convert : RNA and Proteins to their Orthologues
 Input   : AR AP RSP RSR ENSP ENST SP TR UP
 MT Link : is homologous to
 Output  : ORTH

=cut

our %directORTH = map { $_ => 1 } qw
(LL ENSG HG RSR ENST RSP ENSP SP IPI);
our @directOrthList = sort keys %directORTH;


{
    my @nss = &_full_kids( ['AR','AL','AP'] );
    my $n1  = join(' ', @nss);
    &_set_conv( $n1, 'ORTH',
                'update_OBJECT_to_ORTH');
}

sub update_OBJECT_to_ORTH {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    return [] unless ($id);
    my @rows;
    if ($seq) {
        my @cloudRows;
        my $genericNS;
        foreach my $nsM (qw(AP AR AL)) {
            next unless ($self->is_namespace($ns1, $nsM));
            $genericNS = $nsM;
            last;
        }
        my @specialAuths = qw(HomoloGene MGI);
        my $spAuthText   = join('|', @specialAuths);
        # my $opts  = "tag=$spAuthText noRPR maxedge6 l2lok";
        my $opts  = "tag=$spAuthText noRPR maxedge6 l2lok";
        if ($directORTH{$ns1}) {
            # The query ID can be part of an Ortholog cloud
            # warn "($idReq, $ns2, $ns1, $age)  $id [$ns1]";
            my ($cloud) =  $self->cached_cloud
                ( -age  => $self->cloud_age,
                  -seed => [[$id, $ns1]],
                  -type => 'OrthologCluster' );
            if ($cloud) {
                my $paths = $cloud->paths
                    ( -ns2  => $ns1,
                      -opts => $opts,);
                # warn $cloud->paths_to_text($paths);
                @cloudRows = @{$paths};
            } else {
                $self->msg_once("No cloud recovered for '$id' [$ns1]");
            }
        } else {
            # We need to map the query over to a cloud namespace
            my @seeds;
            my %nst;
            foreach my $nsM (@directOrthList) {
                my $drows = $self->convert
                    ( -id => $id, -ns1 => $ns1, -ns2 => $nsM,
                      -age => $age, -nonull => 1, -ashash => 1);
                foreach my $row (@{$drows}) {
                    my @mids = map { $nst{$_} ||= $self->namespace_token($_) }
                    split(' < ', $row->{ns_between} || '');
                    # Ignore the row if it transits through a cloud NS
                    my $midOrth = 0;
                    map { $midOrth += $directORTH{$_} || 0 } @mids;
                    next if ($midOrth);
                    push @seeds, $row;
                }
            }
            my @links = $self->expand_through_cloud
                ( -rows      => \@seeds,
                  -cleanseed => 1,
                  -terminal  => 1,
                  -opts      => $opts,
                  -age       => $age,
                  -type      => 'OrthologCluster', );
            my @secondary;
            my %id3s = map { $_->{term_out} => $_->{ns_out} } @links;
            while (my ($id3, $ns3) = each %id3s) {
                push @secondary, $self->convert
                    ( -id => $id3, -ns1 => $ns3, -ns2 => $ns1,
                      -age => $age, -nonull => 1, -ashash => 1);
            }
            @cloudRows = $self->stitch_rows(\@links, \@secondary);
        }

        # Note the taxa of the query, so we can exclude it from results
        my @taxa = $self->convert( -id => $id, -ns1 => $ns1, -ns2 => 'TAX',
                                   -age => $age, -nonull => 1);
        my $tossTax = ($#taxa == -1) ? undef : { map {$_ => 1} @taxa };
        # Organize results by taxa
        my %org;
        my $unkCount = 0;
        for my $r (0..$#cloudRows) {
            my @taxae = $self->convert
                ( -id => $cloudRows[$r]{term_out}, -ns1 => $ns1, -ns2 => 'TAX',
                  -age => $age, -nonull => 1);
            if ($tossTax) {
                my $toss = 0; map { $toss += $tossTax->{$_} || 0 } @taxae;
                next if ($toss);
            }
            @taxae = ("UNK".++$unkCount) if ($#taxae == -1);
            my $row = $cloudRows[$r];
            $row->{ORTH_NUM} = $r;
            foreach my $taxa (@taxae) {
                push @{$org{$taxa}}, $row;
            }
        }
        my %kept;
        while (my ($taxa, $tarr) = each %org) {
            # Pick the best scored hits for this taxa
            my @unused;
            map { push @unused, $_ unless ($kept{$_->{ORTH_NUM}}++) } @{$tarr};
            my @simp = $self->simplify_rows
                ( -rows        => \@unused,
                  -notext      => 1,
                  -cluster     => \@specialAuths,
                  -anoncluster => 'Other', );
            my %groups;
            foreach my $row (@simp) {
                my $grp = $row->{auth} =~ /($spAuthText)/ ? $1 : 'Other';
                push @{$groups{$grp}}, $row;
                $row->{matched} = -1 unless (defined $row->{matched});
            }
            # Now cluster by ID:
            my %tHits;
            while (my ($grp, $arr) = each %groups) {
                my @sorted = sort { $b->{matched} <=> $a->{matched} } @{$arr};
                my ($best) = $sorted[0]{matched};
                foreach my $row (@sorted) {
                    last if ($row->{matched} < $best);
                    # Count the number of internal nodes in the path:
                    my $btwn   = $row->{ns_between};
                    $btwn      = $btwn ? [ split(' < ', $btwn) ] : [];
                    $row->{BC} = $#{$btwn};
                    push @{$tHits{$row->{term_out}}}, $row;
                }
            }
            foreach my $idGroup (values %tHits) {
                # Consider all rows captured for each ID
                if ($#{$idGroup} == 0) {
                    # Just one path found for this ID
                    push @rows, $idGroup->[0];
                    next;
                }
                my @idr = sort { $b->{matched} <=> $a->{matched} ||
                                 $a->{BC} <=> $b->{BC} } @{$idGroup};
                my $best = $idr[0]{matched};
                my $bbc  = $idr[0]{BC};
                foreach my $row (@idr) {
                    # Exit loop if it has a lower score than the best
                    # for this ID, or if it is a longer path:
                    last if ($row->{matched} < $best || $row->{BC} > $bbc);
                    delete $row->{BC};
                    push @rows, $row;
                }
            }
        }
        map { $_->{matched} = undef if ($_->{matched} && $_->{matched} == -1);
              $_->{ns_out} = $nsOut } @rows;
    }
    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut, };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;

}

sub _homologene_links {
    my $self = shift;
    my ($id, $ns1, $age) = @_;
    my @hg = $self->chain_conversions
        ( -id        => $id, 
          -chain     => [$ns1, 'HG', $ns1 ], 
          -age       => $age,
          -guessany  => 'ORTH' );
    # exit unless we have homologene hits.
    return () if ($#hg == -1);

    my $mt    = $self->tracker;
    my $class = $self->primary_maptracker_class( $ns1 );
    # This chain will always have a score of 1.0:
    #   RSP -> HG -> RSP

    # We need to adjust that score to reflect the similarity between
    # the two proteins. To do that, we will need to find the relevant
    # RefSeq proteins (both source and target) and determine the
    # homology between them.

    # What are the target RefSeq proteins?
    my %targets = map { $_->{term_out} || '' => 1 } @hg;
    delete $targets{''}; delete $targets{$id};

    # What other RSPs is the query tagged as homologous to?
    my $listA = $mt->get_edge_dump
        ( -name      => $id . '.%',
          -return    => 'object array',
          -dumpsql   => 0,
          -keepclass => $class,
          -keeptype  => "is homologous to" );

    my %sameTaxa = map { $_ || '' => 1 } $self->convert
        ( -id => $id, -ns1 => $ns1, -ns2 => 'TAX', -age => $age);
    $sameTaxa{''} = 1;

    my %scMods;
    foreach my $edge (@{$listA}) {
        # Properly identify the source and target members of edge:
        my ($other, $query) = $edge->seqs();
        ($other, $query) = ($query, $other)
            unless ($query->name =~ /^$id\.\d+/);
        my ($ov, $qv);
        my $orsp = $other->name;
        if ($orsp =~ /(.+)\.(\d+)$/) {
            ($orsp, $ov) = ($1, $2);
        } else { next; }
        if ($query->name =~ /^$id\.(\d+)$/) {
            $qv = $1;
        } else { next; }

        map { push @{$scMods{$orsp}}, [$qv, $ov, $_->num] }
        $edge->has_tag('Alignment Identity');
    }

    # Now filter all chained rows, and modify relevant columns
    my @rows;
    foreach my $row (@hg) {
        # Only use defined targets that are not the same as query:
        my $out = $row->{term_out};
        next if (!$out || $out eq $id);
        my @taxae =  $self->convert
            (-id => $out,-ns1 => $ns1,-ns2 => 'TAX',-age => $age);
        my $tc = 0; map { $tc += $sameTaxa{$_} || 0 } @taxae;
        next if ($tc);

        # What is the best modifier? Use the highest
        # available version for the query, then the
        # highest version for the subject, then the best
        # overall score.

        my ($best) = sort {$b->[0] <=> $a->[0] ||
                               $b->[1] <=> $a->[1] ||
                               $b->[2] <=> $a->[2]}
        @{$scMods{$out} || []};
        my $score = $row->{matched};
        if (defined $best && defined $score) {
            # Multiply the chain score by the modifier, round
            # to four decimal places.
            $row->{matched} =
                int(0.5 + 10000 * $score * $best->[2])/10000 || 0.0001
                if ($row->{matched});
        } else {
            # If either the original score or the modifier
            # is undefined then the whole chain is undefined
            $row->{matched} = undef;
        }
        push @rows, $row;
    }
    # Clean up the namespace path - clearly indicate the sequence type
    # (protein or RNA) that the relationship is built through.
    my $nsBtwn = $self->namespace_name($ns1);
    $nsBtwn    = "HomoloGene ($nsBtwn)";
    foreach my $row (@rows) {
        $row->{ns_between} = $nsBtwn;
        # Take 
        $row->{auth}       =~ s/ < HomoloGene < HomoloGene < / < /;
    }
    return @rows;
}

=head3 APS_to_ORTH

 Convert : Affy ProbeSet to Orthologue
 Input   : APS
 MT Link : is the orthologue of / Xeno Blast Shadow hits
 Output  : ORTH

=cut

&_set_conv( 'APS', 'ORTH', 'update_APS_to_ORTH');
sub update_APS_to_ORTH {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my $mt    = $self->tracker;
    return [] unless ($mt);

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    my @rows;
    if ($seq) {
        my $class  = $self->primary_maptracker_class( $ns1 );
        my $struct = $self->fast_edge_hash
            ( -name      => $id,
              -keepclass => $class,
              -keeptype  => "is the orthologue of");
        $self->edge_ids_to_authorities( $struct );
        while (my ($oname, $auths) = each %{$struct}) {
            my @simp = $self->simplify_authors(@{$auths});
            push @rows, { term_in    => $id,
                          term_out   => $oname,
                          ns_in      => $nsIn,
                          ns_out     => $nsOut,
                          auth       => join($authJoiner, @simp),
                          matched    => undef };
        }


        # Now get orthologues through XBS
        my $xaname = "Xeno Blast Shadow";
        my $list = $mt->get_edge_dump
            ( -name      => $seq,
              -return    => 'object array',
              -keepauth  => $xaname,
              -keepclass => 'RNA',
              -keeptype  => "is similar to" );

        my %xbs;
        foreach my $edge (@{$list}) {
            my $other   = $edge->other_seq($seq);
            my $oname   = $other->name;
            my $ns3     = $self->guess_namespace( $oname ) || 'AR';
            my ($ospid) = sort { $b <=> $a } map { ($_->num || 0) / 100 }
            $edge->has_tag('Overall Shadow Percent ID');
            next unless ($ospid);
            my ($disTag) = $edge->has_tag('Dissent');
            $ospid = 0 if ($disTag);
            # We have found a RefSeq RNA that is 'similar to' the probe set
            # Now get the xeno probesets for that RNA
            # warn "$oname [$ospid]";
            my $secondary = $self->convert
                ( -id     => $oname,
                  -ns1    => $ns3,
                  -ashash => 1,
                  -nonull => 1,
                  -ns2    => 'APS',
                  -age    => $age );
            my $primary = [ { term_in    => $id,
                              term_out   => $oname,
                              ns_in      => $nsIn,
                              ns_out     => $self->namespace_name($ns3),
                              auth       => $xaname,
                              matched    => $ospid } ];
            my @stitched = $self->stitch_rows($primary, $secondary);
            map { $_->{ns_out} = $nsOut } @stitched;
            push @rows, @stitched;
        }
        @rows = $self->simplify_rows
            ( -rows        => \@rows,
              -shrinkauth  => 1,
              -clusterleft => 1,
              -cluster     => [@probeSetMatchAuthorities,'xeno'], );
    }
    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut, }; 
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}
=head3 SEQUENCE_to_SEQUENCE

 Convert : SEQUENCE_to_SEQUENCE
 Input   : ENSP RSP AP SP TR UP IPI
 MT Link : SAMEAS, plus SIMILAR / CONTAINS via Reciprocal Best Match
 Output  : ENSP RSP AP SP TR UP IPI

This converter is designed to find the same sequence in another
namespace; the hope is the two terms will represent the same
biological object, even if they are not 100% identical.

=cut

{ my @nucs = qw(AR RSR ENST); my @prots = qw(NRDB AP RSP ENSP SP TR UP IPI);
  my @seqs = (@nucs, @prots);
  foreach my $pa (@seqs) {
      my $ca = ($pa =~ /^(SP|UP|TR)$/) ? 'UP' : $pa;
      foreach my $pb (@seqs) {
          my $cb = ($pb =~ /^(SP|UP|TR)$/) ? 'UP' : $pb;
         # &_set_conv( $pa, $pb, 'update_SEQUENCE_to_SEQUENCE')
         #     unless ($ca eq $cb && $ca !~ /A[PR]/);
      }
  }
}

my $seq2seqCloud = {
    AP => 'ProteinCluster',
    AR => 'TranscriptCluster',
};

my $seq2seqCanon = {
    AR   => 'AP',
    RSR  => 'RSP',
    ENST => 'ENSP',
};
map { $seq2seqCanon->{ $seq2seqCanon->{$_} } = $_ } keys %{$seq2seqCanon};

sub update_SEQUENCE_to_SEQUENCE {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsOut = $self->namespace_name( $ns2 );
    my $nsIn  = $self->namespace_name( $ns1 );
    my $mt    = $self->tracker;
    return [] unless ($mt);

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1, 'verifyClass' );
    $seq = undef if ($seq && !$self->verify_namespace($id, $ns1));
    my @rows;
    my $ssd = $self->{SEQ_SEQ_DATA} ||= {
        LEVEL    => 0,
        COMPUTED => {},
        BRIDGES  => {},
        CHAIN    => [],
    };
    my $compKey      = "$id-$ns1-$ns2";
    my $priorResults = $ssd->{COMPUTED}{$compKey};
    my $cloudAge     = $self->cloud_age || $age;
    # warn "!! [$ssd->{LEVEL}] $compKey ".$#{$priorResults} if ($priorResults);
    if ($priorResults) {
        my @simp = $self->simplify_rows( -rows  => $priorResults  );
        return \@simp;
    }

    $ssd->{LEVEL}++;
    # warn sprintf("%s[%s] %s > %s\n", "  " x $ssd->{LEVEL}, $id, $ns1, $ns2);
    if ($seq) {
        my ($gns1) = $self->guess_namespace($id, $ns1);
        my @mol  = ($ns1, $ns2);
        for my $m (0..1) {
            my $ns = $mol[$m];
            if ($self->is_namespace($ns, 'AR')) {
                $mol[$m] = 'AR';
            } elsif ($self->is_namespace($ns, 'AP')) {
                $mol[$m] = 'AP';
            }
        }
        if ($ns2 ne $mol[1] && $gns1 eq $ns2) {
            # The secondary namespace is a specific namespace AND
            # The guessed primary namespace matches it. Return empty.
            delete $self->{SEQ_SEQ_DATA} unless (--$ssd->{LEVEL});
            return [];
        }


        my $class  = $self->primary_maptracker_class( $ns2 );
        my @tossC  = ('deprecated');
        my $oToss  = $self->primary_maptracker_class( $ns1 );
        push @tossC, $oToss unless ($oToss =~ /^(Protein|RNA)$/);
        push @tossC, 'exon' if ($mol[1] eq 'AR');

        if ($mol[0] eq $mol[1]) {
            # Linkages between the same type of molecule

            # Do we want to find undeprecated IDs here??
            # my $rs = $self->convert ( -id => $id, -ns1 => $gns1,
            #                          -tracetok => '*',
            #                          -ns2 => 'RS', -age => $age);
            
            my $mol1  = $mol[0];
            my $mol2  = $mol1 eq 'AP' ? 'AR' : 'AP';
            my ($cloud) = $self->cached_clouds
                ( -age    => $cloudAge,
                  -seed   => [[ $id, $gns1 ]],
                  -type   => $seq2seqCloud->{$mol1} );
            my $paths = $cloud->paths( -ns2 => $ns2  );
            # warn $cloud->paths_to_text($paths);
            my %direct;
            foreach my $row (@{$paths}) {
                #$row->{term_in}  = $id if ($gns1 eq 'NRDB');
                #$row->{term_out} =~ s/ .+// if ($row->{ns_out} eq 'NRDB');
                $row->{ns_in}    = $nsIn;
                $row->{ns_out}   = $nsOut;
                $direct{ $row->{term_out} } = $row->{ns_out};
                push @rows, $row;
            }

            # Can we step into the other namespace and then step back?
            my @tclouds;
            if (my $transNS = $seq2seqCanon->{$gns1}) {
                my @trans1 = $self->convert
                    ( -id => $id, -ns1 => $gns1, -ns2 => $transNS,
                      -age => $age, -ashash => 1, -nonull => 1);
                my @seeds;
                foreach my $trow1 (@trans1) {
                    push @seeds, [ $trow1->{term_out} , $transNS, $trow1 ];
                }
                @tclouds = $self->cached_clouds
                        ( -age    => $cloudAge,
                          -type   => $seq2seqCloud->{$mol2},
                          -seed   => \@seeds, ) unless ($#seeds == -1);
            }
            # If we found any clouds in the other molecule type, see if
            # we can translate out to new values
            my @stitched;
            my $validTargets = $self->valid_namespace_hash($ns2);
            foreach my $tcloud (@tclouds) {
                # warn $tcloud->to_text;
                my $nodes = $tcloud->all_nodes();
                while (my ($id3, $ns3) = each %{$nodes}) {
                    my $transNS = $seq2seqCanon->{$ns3};
                    # Only continue if we can translate back to the
                    # appropriate molecule type *AND* that type is
                    # valid for the namespace we want ($ns2):
                    next unless ($transNS && $validTargets->{$transNS});
                    my @trans2 = $self->convert
                        ( -id => $id3, -ns1 => $ns3, -ns2 => $transNS,
                          -age => $age, -ashash => 1, -nonull => 1);
                    foreach my $trow2 (@trans2) {
                        # We were able to translate back to the target NS
                        my $id4 = $trow2->{term_out};
                        next if ($direct{$id4}); # We already have this one
                        # What is the path through the cloud?
                        foreach my $dat ($tcloud->all_seed_meta) {
                            my ($idm, $nsm) = 
                                map { $dat->[0]{$_} } qw(term_out ns_out);
                            my $tpaths = $tcloud->paths( -id2    => $id3 );
                            next if ($#{$tpaths} != 0);
                            push @stitched, $self->stitch_rows
                                ([$dat->[0]], [$tpaths->[0]], [$trow2]);
                        }
                    }
                }
            }
            unless ($#stitched == -1) {
                map { $_->{ns_in} = $nsIn; $_->{ns_out} = $nsOut;
                      push @rows, $_; } @stitched;
                # If we are moving through a protein cloud, do not
                # trust the transitive scores
                map { $_->{matched} = undef } @stitched
                    if ($mol2 eq 'AP');
            }
            
        } else {
            # The molecules are different (AP->AR or AR->AP)
            my $type  = $seq2seqType->{$mol[0]}{$mol[1]};
            unless ($type) {
                $self->death("No maptracker edges defined for $ns1 -> $ns2");
            }

            # See if direct edges exist:
            my $struct = $self->fast_edge_hash
                ( -name      => $seq,
                  -tossclass => 'deprecated',
                  -keepclass => $class,
                  -keeptype  => $type, );
            $self->edge_ids_to_authorities( $struct );
            while (my ($oname, $auths) = each %{$struct}) {
                my @simp = $self->simplify_authors(@{$auths});
                push @rows, { term_in    => $id,
                              term_out   => $oname,
                              ns_in      => $nsIn,
                              ns_out     => $nsOut,
                              auth       => join($authJoiner, @simp),
                              matched    => 1 };
            }

            unless ($self->is_canonical($gns1,$ns2)) {
                # Can we get to the other molecule type by bridging
                # through a canonical partner? This step will only be
                # considered if the direct linkage (utilizing the
                # guessed input namespace) is non-canonical.
                my @mids;
                # There are two canonical paths we can take:
                # NS1 -> NS1_Canon_Partner -> NS2
                # NS1 -> NS2_Canon_Partner -> NS2
                map { push @mids, $seq2seqCanon->{$_} 
                      if ($seq2seqCanon->{$_}) } ($gns1, $ns2);
                
                map { push @rows, $self->chain_conversions
                          ( -id         => $id, 
                            -chain      => [$ns1, $_, $ns2],
                            -noloop     => 1,
                            -bestmid    => 1,
                            -nonull     => 1,
                            -age        => $age ) } @mids;
            }
        }
       
        @rows = $self->simplify_rows( -rows    => \@rows,
                                      -show    => 0);
    }
    delete $self->{SEQ_SEQ_DATA} unless (--$ssd->{LEVEL});
    if ($#rows < 0) {
        if ($seq && $self->deep_dive) {
            #### NEEDS WORK ####
            # Nothing found in MapTracker, deep dive is requested
            my $change = $self->update_maptracker_sequence( $id );
            return $self->update_SEQUENCE_to_SEQUENCE
                ( $id, $ns2, $ns1, $age) if ($change);
        }
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut, };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head3 INVITROGEN_TIE

 Convert : Invitrogen IDs to RefSeq RNA and Protein
 Input   : IUO / AR AP RSP RSR
 MT Link : SAMEAS / SIMILAR / CONTAINS
 Output  : AR AP RSP RSR / IUO

=cut

map { &_set_conv( 'IUO', $_, 'update_INVITROGEN_TIE');
      &_set_conv( $_, 'IUO', 'update_INVITROGEN_TIE') } ('AR AP RSP RSR');
sub update_INVITROGEN_TIE {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsOut = $self->namespace_name( $ns2 );
    my $nsIn  = $self->namespace_name( $ns1 );
    my $mt    = $self->tracker();
    return [] unless ($mt);
    
    my @rows;
    my ($id, $seq) = $self->standardize_id( $idReq, $ns1, 'verifyClass' );
    if ($seq) {
        my $targClass = $self->primary_maptracker_class( $ns2 );
        my @types = ();
        my $bait = $id;
        if ($ns1 eq 'IUO') {
            # Starting from Invitrogen
            if ($ns2 =~ /^(RSR|AR)$/) {
                @types = ('is similar to', 'is fully contained by',
                          'is the same as');
            } elsif ($ns2 =~ /^(RSP|AP)$/) {
                @types = ('can be translated to generate');
            }
        } else {
            # Use any versioned ID as bait
            $bait = "$id.%";
            if ($ns1 =~ /^(RSR|AR)$/) {
                @types = ('is similar to', 'fully contains',
                          'is the same as');
            } elsif ($ns1 =~ /^(RSP|AP)$/) {
                @types = ('is translated from');
            }
        }
        if ($#types >  -1 ) {
            my $list   = $mt->get_edge_dump
                ( -name      => $bait,
                  -keepclass => $targClass,
                  -keeptype  => \@types,
                  -return    => 'object array', );
            
            foreach my $edge (@{$list}) {
                my $reads = $edge->reads();
                my ($rs, $inv) = $edge->nodes();
                ($rs, $inv) = ($inv, $rs) unless
                    ($inv->is_class('Ultimate ORF'));
                my ($score, @auths);
                if ( $reads =~ /(trans|same|contain)/) {
                    $score = 1;
                    @auths = $edge->each_authority_name;
                } else {
                    my $iname = $inv->name;
                    my @tags = $edge->has_tag('Total Percent ID');
                    my %scs = map { $_->valname => $_ } @tags;
                    if (my $tag = $scs{$iname}) {
                        $score = $tag->num / 100;
                        @auths = ($tag->authname);
                    }
                }
                my $sname = $inv->name;
                if ($ns1 eq 'IUO') {
                    $sname = $rs->name;
                    $sname =~ s/\.\d+$//;
                }
                @auths = $self->simplify_authors( @auths);
                push @rows, { term_in  => $id,
                              term_out => $sname,
                              ns_in    => $nsIn,
                              ns_out   => $nsOut,
                              auth     => join($authJoiner, @auths),
                              matched  => $score };
            }
        }
    }
    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut, };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head2 Synonyms and Aliases

Finding synonyms and aliases for identifiers. Generally involves
converting a reliable (unique) term to a less reliable one, but could
also involve an attempt to find the current ID from a potentially
deprecated one (as per GO).

=head3 LOCI_to_SYM

 Convert : LocusLink Gene to Gene Symbol
 Input   : AL LL ENSG
 MT Link : is a reliable alias for / is the preferred lexical variant of
 Output  : SYM

Official symbols have matched=1, Unofficial will be 0.3, unless they
were picked out as the "main" symbol, in which case they are 0.4 -
these numbers were picked after USD7.78M worth of cloud computing time
as being the ONLY reasonable values to represent these three states,
so please do not change them.

=cut

&_set_conv( 'AL LL ENSG', 'SYM', 'update_LOCI_to_SYM');
sub update_LOCI_to_SYM {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my $mt    = $self->tracker;

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    unless ($self->verify_namespace($id, $ns1) && $mt) {
        return [];
    }
    my @rows;
    if ($seq) {
        my $list = $mt->get_edge_dump
            ( -name     => $seq,
              -return   => 'object array',
              -keeptype => 'is a reliable alias for',
              -keepspace => 'GeneSymbols',
              -keepclass => 'GENESYMBOL', );
        #my @taxa = $self->convert
        #    ( -id => $id, -ns1 => $ns1, -ns2 => 'TAX', -age => $age);
        # warn "$id = " . join(', ', @taxa);
        my @edges;
        my ($ofSc, $unSc) = ($seq->is_class('Deprecated')) ? (-1,-1) : (1,0.3);
        my %OfficialSyms;
        foreach my $edge (@{$list}) {
            my $other   = $edge->other_seq( $seq );
            my @authn   = $edge->has_tag('Authorized Nomenclature');
            my %type    = map { $_->valname => 1 } @authn;
            my $sym     = $other->name();
            # We will add a small bonus to the symbol if it is not official
            # but is found at the "main" level attached to the object.
            my $score   = $type{'Official'} ? $ofSc : 
                $unSc == -1 ? -1 : $unSc + 0.1;
            my $edat    = [ $sym, $edge, $score ];
            push @edges, $edat;
            # Track the official symbols that we identify:
            push @{$OfficialSyms{$sym}}, $edat if ($score == 1);
            my $alt = $mt->get_edge_dump
                ( -name      => $other,
                  -return    => 'object array',
                  -keeptype  => 'is the preferred lexical variant of',
                  -keepspace => 'GeneSymbols',
                  -keepclass => 'GENESYMBOL', );
            foreach my $lve (@{$alt}) {
                my $lv   =  $lve->other_seq( $other );
                my %loci = map { $_->valname => 1 } $lve->has_tag('Locus');
                next unless ($loci{$id});
                push @edges, [ $lv->name, $lve, $unSc ];
            }
        }

        my @offSym = keys %OfficialSyms;
        if ($#offSym > 0) {
            # There are more than one official symbols!!
            # Not supposed to happen, but was seen with Ensembl
            # eg ENSG00000163938 = GNL3 + SNORD19B
            # Downgrade the score - this also lets 'true' symbols
            # pop out via another source (eg Entrez)
            my $badOfficialScore = 0.9;
            foreach my $edats (values %OfficialSyms) {
                map { $_->[2] = $badOfficialScore } @{$edats};
            }
        }

        my %nonredun;
        foreach my $edat (@edges) {
            my ($sym, $edge, $score) = @{$edat};
            # next if ($sym eq $id); # Stupid LOC 'symbols'
            map { $nonredun{$sym}{$score}{$_}++ } $edge->each_authority_name;
        }

        while (my ($sym, $scores) = each %nonredun) {
            # some weird non-symbol stuff sneaking in - max char size filter:
            next if (length($sym) > 30);
            my ($score) = sort { $b <=> $a } keys %{$scores};
            my @simp    = $self->simplify_authors
                ( keys %{$nonredun{$sym}{$score}});
            push @rows, { term_in    => $id,
                          term_out   => $sym,
                          ns_in      => $nsIn,
                          ns_out     => $nsOut,
                          auth       => join($authJoiner, @simp),
                          matched    => $score == -1 ? undef : $score };
        }
        # Sort by score, then alphabetize
        #@rows = sort { $b->{matched} <=> $a->{matched} ||
        #                   $a->{ns_out} cmp $b->{ns_out} } @rows;
    }
    if ($#rows < 0) {
            push @rows, { term_in    => $id,
                          ns_in      => $nsIn,
                          ns_out     => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

&_set_conv( 'LL', 'WP', 'update_LOCI_to_WIKIPEDIA');
&_set_conv( 'WP', 'LL', 'update_LOCI_to_WIKIPEDIA');
sub update_LOCI_to_WIKIPEDIA {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my $mt    = $self->tracker;

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    unless ($self->verify_namespace($id, $ns1) && $mt) {
        return [];
    }
    
    my @rows;
    if ($seq) {
        my $type = $ns1 eq 'WP' ?
            'contains a reference to' : 'is referenced in';
        my $mtns    = $self->maptracker_namespace($ns2);
        my $mtClass = $self->primary_maptracker_class( $ns2 );
        my $list    = $mt->get_edge_dump
            ( -name      => $seq,
              -return    => 'object array',
              -keeptype  => $type,
              -keepspace => $mtns,
              -keepclass => $mtClass, );
        foreach my $edge (@{$list}) {
            my $oname   = $edge->other_seq( $seq )->name;
            my @simp   = $self->simplify_authors
                ( $edge->each_authority_name );
            my $lvl = $edge->first_tag_value('Level');
            next unless ($lvl);
            my $sc = $lvl == 1 ? 1 : 0.3;
            
            push @rows, { term_in  => $id,
                          term_out => $oname,
                          ns_in    => $nsIn,
                          ns_out   => $nsOut,
                          auth     => join($authJoiner, @simp),
                          matched  => $sc };
        }
    }
    if ($#rows < 0) {
            push @rows, { term_in    => $id,
                          ns_in      => $nsIn,
                          ns_out     => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head3 SYM_to_LOCI

 Convert : Gene Symbol to LocusLink Gene
 Input   : SYM
 MT Link : is a lexical variant of / is reliably aliased by
 Output  : AL LL ENSG

Official symbols have matched=1, otherwise 0.3

=cut

&_set_conv( 'SYM', 'AL LL ENSG', 'update_SYM_to_LOCI');
sub update_SYM_to_LOCI {
    my $self = shift;
    my ($idReq, $ns2, $ns1) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my $mt    = $self->tracker;

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1, 'verifyClass' );
    unless ($id && $self->verify_namespace($id, $ns1) && $mt) {
        return [];
    }
    # warn "($id, $seq)( $idReq, $ns1,";
    my @rows;
    if ($seq) {
        my $taxa;
        if ($id =~ / \(([^\)]+)\)$/) {
            my ($tname, $tobj) = $self->standardize_taxa($1);
            $taxa = $tname if ($tobj);
        }
        my @baits = ( [$seq, 1] );
        my $pref = $mt->get_edge_dump
            ( -name     => $seq,
              -return   => 'object array',
              -keeptype => 'is a lexical variant of',
              -keepspace => 'GeneSymbols',
              -keepclass => 'genesymbol', );
        foreach my $edge (@{$pref}) {
            my $other = $edge->other_seq( $seq );
            my %ok  = map { $_->valname => 1 } $edge->has_tag('Locus');
            push @baits, [$other, $other->is_class('deprecated') ?
                          -1 : 0.3, \%ok];
        }
        my %hits;
        foreach my $bdat (@baits) {
            my ($bait, $score, $ok) = @{$bdat};
            my $list = $mt->get_edge_dump
                ( -name      => $bait,
                  -return    => 'object array',
                  -keeptype  => 'is reliably aliased by',
                  -keeptaxa  => $taxa,
                  -keepclass => 'locus', );
            foreach my $edge (@{$list}) {
                my $other = $edge->other_seq( $bait );
                my $oname = $other->name;
                next unless ($self->verify_namespace($oname, $ns2));
                my $sc = $score;
                if ($ok && !$ok->{$oname}) {
                    next;
                } elsif ($other->is_class('deprecated')) {
                    $sc = -1;
                } elsif ($sc == 1) {
                    my @authn   = $edge->has_tag('Authorized Nomenclature');
                    my %type    = map { $_->valname => 1 } @authn;
                    $sc = 0.4 unless ($type{'Official'});
                }
                map {$hits{$oname}{$sc}{$_} = 1} $edge->each_authority_name;
            }
        }
        while (my ($oname, $scores) = each %hits) {
            my ($score) = sort { $b <=> $a } keys %{$scores};
            my @simp    = $self->simplify_authors
                ( keys %{$hits{$oname}{$score}});
            push @rows, { term_in    => $id,
                          term_out   => $oname,
                          ns_in      => $nsIn,
                          ns_out     => $nsOut,
                          auth       => join($authJoiner, @simp),
                          matched    => $score == -1 ? undef : $score };
        }
    }
    if ($#rows < 0) {
            push @rows, { term_in    => $id,
                          ns_in      => $nsIn,
                          ns_out     => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head3 GO_to_RS

 Convert : GeneOntology terms to reliable aliases
 Input   : GO
 MT Link : is a reliable alias for
 Output  : RS

Gets the 'preferred' GO term. There are a few non-deprecated terms that
have a preferred alias associated with them.

=cut

&_set_conv( 'GO', 'RS', 'update_GO_to_RS');
sub update_GO_to_RS {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my $mt    = $self->tracker;

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    unless ($self->verify_namespace($id, $ns1) && $mt) {
        return [];
    }
    my @rows;
    if ($seq) {
        my $alis  = $mt->get_edge_dump
            ( -name      => $seq,
              -orient    => 1,
              -keepclass => "go",
              -keeptype  => "is a reliable alias for" );
        my @names = map { $mt->get_seq( $_->[0] )->name } @{$alis};
        my $ali = $id;
        if ($#names > 0) {
            # warn "Multiple aliases for $id : ".join(",", @names)."\n  ";
            push @rows, { term_in  => $id,
                          ns_in    => $nsIn,
                          ns_out   => $nsOut, }; 
        } elsif ($#names == 0) {
            $ali = $names[0];
        }
        push @rows, { term_in  => $id,
                      term_out => $ali,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut,
                      auth     => 'GeneOntology',
                      matched  => 1 };
    } else {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut, }; 
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head3 CDD_to_RS

 Convert : Conserved domain database terms to reliable aliases
 Input   : CDD
 MT Link : is mapped from
 Output  : RS

New CDD IDs use pure integers. Old ones reference the parent database
the term comes from. For example:

CDD:cd00265 == cd00265 -is mapped from-> CDD:29020

=cut

&_set_conv( 'CDD', 'RS', 'update_CDD_to_RS');
sub update_CDD_to_RS {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my $mt    = $self->tracker;

    my ($id, $subID);
    if ($mt && $idReq =~ /^cdd\:(.+)$/i) {
        $subID = $1;
        $id = "CDD:$subID";
    } else {
        return [];
    }
    my %rs;
    if ($subID =~ /^\d+$/) {
        # This is a modern CDD identifier
        $rs{$id} = 1;
    } else {
        my $modern  = $mt->get_edge_dump
            ( -name      => $subID,
              -orient    => 1,
              -keepclass => "cdd",
              -keeptype  => "is mapped from" );
        my @names = map { $mt->get_seq( $_->[0] )->name } @{$modern};
        map { $rs{$_} = 1 } @names;
    }
    my @rows = map { { term_in  => $id,
                       term_out => $_,
                       ns_in    => $nsIn,
                       ns_out   => $nsOut,
                       auth     => 'tilfordc',
                       matched  => $rs{$_} } } sort keys %rs;
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head3 UNIPROT_NAMES

 Mirror  : Connect UniProt accessions to their names
 NS1     : SP / TR / UP
 MT Link : is a reliable alias for
 NS2     : SPN / TRN / UPN

=cut

map { &_set_conv( $_, $_.'N', 'update_UNIPROT_NAMES');
      &_set_conv( $_.'N', $_, 'update_UNIPROT_NAMES')} qw(SP TR UP);
sub update_UNIPROT_NAMES {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my $mt    = $self->tracker();
    return [] unless ($mt);

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1, 'verifyClass' );
    my @queries;
    if ($seq) {
        if ($ns1 =~ /^(SP|TR|UP)N$/) {
            # We should track name deprecations
            my @rs = $self->convert
                ( -id => $id, -ns1 => $ns1, -ns2 => 'RS', -age => $age);
            map { push @queries, $_ if $self->verify_namespace($_, $ns1) } @rs;
        } else {
            @queries = ($id);
        }
    }
    my @rows;
    if ($#queries > -1) {
        my $class = $self->primary_maptracker_class( $ns2 );
        my $edges = $mt->get_edge_dump
            ( -name      => \@queries,
              -return    => 'object array',
              -keeptype  => 'is a reliable alias for',
              -keepclass => $class,
              -tossclass => 'deprecated',
              -dumpsql   => 0 );
        my %hits;
        foreach my $edge (@{$edges}) {
            my $name   = $edge->node2->name;
            next unless ($self->verify_namespace($name, $ns2));
            my %auths  = map { $_ => 1 } $edge->each_authority_name();
            $hits{$name} = 1 if ($auths{'UniProt'});
        }
        foreach my $name (sort keys %hits ) {
            push @rows, { term_in  => $id,
                          term_out => $name,
                          ns_in    => $nsIn,
                          ns_out   => $nsOut,
                          matched  => 1,
                          auth     => 'UniProt' };
        }
    }
    push @rows, { term_in  => $id,
                  ns_in    => $nsIn,
                  ns_out   => $nsOut, } if ($#rows < 0);
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

sub fast_edge_hash {
    my $self = shift;
    $self->benchstart;
    my $list = $self->tracker->get_edge_dump
        ( @_,
          -orient    => 1,
          -return    => 'name array' );
    my %struct;
    map { $struct{ $_->[0] }{ $_->[2] } = 1 } @{$list};
    $self->benchend;
    return \%struct;
}

my $fceh_cache = {};
my $fceh_count = 0;
sub fast_cached_edge_hash {
    my $self = shift;
    my $key  = shift;
    unless ($fceh_cache->{$key}) {
        if (++$fceh_count > 50000) {
            $fceh_cache = {};
            $fceh_count = 0;
        }
        my $struct = $fceh_cache->{$key} = $self->fast_edge_hash( @_ );
        $self->edge_ids_to_authorities( $struct ) if ($key =~ /AUTH/);
        
    }
    return { %{$fceh_cache->{$key}} };
}

sub edge_ids_to_authorities {
    my $self    = shift;
    $self->benchstart;
    my ($hash)  = @_;
    my $cnt = 0;
    while (my ($key, $eids) = each %{$hash}) {
        my @auths = $self->auths_for_edge_ids( [ keys %{$eids} ] );
        $hash->{$key} = \@auths;
    }
    $self->benchend;
}


my $authSTH;
sub auths_for_edge_ids {
    my $self    = shift;
    my ($eids) = @_;
    return () if (!$eids || $#{$eids} == -1);
    # 533.51us
    # $self->benchstart;
    my $mt     = $self->tracker();
    $authSTH ||= $mt->dbi->named_sth("Live authorities for edge");
    my %aids;
    foreach my $eid (@{$eids}) {
        map { $aids{$_} = 1 } $authSTH->get_array_for_field( $eid );
    }
    my @auths = map { $authorCache->{$_} ||= 
                          $mt->get_authority($_)->name } keys %aids;
    @auths = ('Unknown') if ($#auths == -1);
    # $self->benchend;
    return @auths;
}

my $tagIDcache = {};
my ($tagSTH, $numSTH);
sub tags_for_edge_ids {
    my $self    = shift;
    my ($eids, $tag, $getNumeric) = @_;
    return () if (!$tag || !$eids || $#{$eids} == -1);
    $self->benchstart;
    my $tagid  = $tagIDcache->{$tag} ||= $self->tracker->get_seq($tag)->id;
    my $sth    = $getNumeric ?
        $numSTH ||= $self->tracker->dbi->named_sth
        ("Fast tag numbers for edge + tag value") :
        $tagSTH  ||= $self->tracker->dbi->named_sth
        ("Fast tag names for edge + tag value");
    my %vals;
    foreach my $eid (@{$eids}) {
        map { $vals{ $_ } = 1 } $sth->get_array_for_field($eid, $tagid);
    }
    $self->benchend;
    return sort keys %vals;
}

=head3 ONTOLOGY_SUBSET

 Convert : Ontology Term to Conceptual Set
 Input   : GO XONT
 MT Link : Search for root parent
 Output  : SET

The set will be the primary GeneOntology or XPRESS Ontology subset
root as found by root_parent(), represented as its description().

=cut

&_set_conv( 'GO', 'SET', 'update_ONTOLOGY_SUBSET');
&_set_conv( 'MSIG', 'SET', 'update_ONTOLOGY_SUBSET');
&_set_conv( 'XONT', 'SET', 'update_ONTOLOGY_SUBSET');
sub update_ONTOLOGY_SUBSET {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );

    my ($id, $seq) = $self->standardize_id( $idReq, $nsIn );
    unless ($self->verify_namespace($id, $ns1)) {
        return [];
    }
    my @rows;
    if ($seq) {
        my $parent = $self->root_parent( $id, $ns1, $age );
        if ($parent) {
            my $parName = $parent;
            if ($ns1 eq 'GO' || $ns1 eq 'XONT') {
                $parName = $self->description
                    ( -id => $parent, -ns => $ns1, -age => $age );
                if ($ns1 eq 'GO') {
                    my %okPar = map { $_ => 1 } $self->get_all_go_subsets();
                    $parName = undef unless ($okPar{$parName});
                }
            }
            push @rows, { term_in  => $id,
                          term_out => $parName,
                          ns_in    => $nsIn,
                          ns_out   => $nsOut,
                          auth     => $nsIn,
                          matched  => 1 } if ($parName);
        }
    }
    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut, };
        
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

sub update_PMID_genealogy {
    my $self = shift;
    my ($idReq, $ns, $age, $dir, ) = @_;
    my ($id)  = $self->fast_standardize($idReq, $ns);
    my $nsn   = $self->namespace_name($ns);
    my ($table, $qcol, $scol) = $dir < 0 ?
        ('direct_parents',  'child',  'parent') :
        ('direct_children', 'parent', 'child');
    my @rows = ( { $qcol => $id,
                   $scol => undef,
                   ns    => $nsn } );
    $self->dbh->update_rows( $table, \@rows );
    return [];
}

# PubMed is flat, it does not really have subsets, not in the sense
# of GO or MSIG, at least. However, it does have "is a member of"
# relations in MapTracker, that point to things like "ISSN:0018-0661"
# or "Biomeditsinskaia khimiia". This method is to explicitly set
# the subset to null
&_set_conv( ['LT', 'PMID'], 'SET', 'update_FLAT_SUBSET');
sub update_FLAT_SUBSET {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );

    my ($id, $seq) = $self->standardize_id( $idReq, $nsIn );
    unless ($self->verify_namespace($id, $ns1)) {
        return [];
    }

    my @rows =  ({ term_in  => $id,
                   ns_in    => $nsIn,
                   ns_out   => $nsOut, } );
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;    
}

=head3 SET_to_GO

 Convert : Conceptual Set to GeneOntology Term
 Input   : SET
 MT Link : Full collapsed parentage
 Output  : GO

The method requires that one of the three major GO subsets be provided
(either as returned by get_all_go_subsets(), or the GO ID representing
those descriptions). All child nodes are found using all_children()
and then associated as the output terms.

=cut

&_set_conv( 'SET', 'GO', 'update_SET_to_GO');
sub update_SET_to_GO {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $mt = $self->tracker();
    return [] unless ($mt);

    my %valid = map { $_ => 1 } $self->get_all_go_subsets();

    my ($setName, $rootNode);
    my $text  = lc($idReq); $text =~ s/\s+/_/g;
    if ($valid{ $text }) {
        # The user has passed an explicit set name
        # Convert the description to the GO ID:
        my $list = $mt->get_edge_dump
            ( -name      => $text,
              -return    => 'object array',
              -keeptype  => "is a longer term for",
              -keepclass => 'GO', );
        my %hits = map { $_->other_seq($text)->name() => 1 } @{$list};
        my @nodes = keys %hits;
        ($setName, $rootNode) = ($text, $nodes[0]) if ($#nodes == 0);
    } else {
        my ($id, $seq) = $self->standardize_id( $idReq, 'GO', 'verifyClass');
        if ($seq) {
            # The user has passed a GO term
            my $desc = $self->description
                ( -id => $id, -ns => 'GO', -age => $age );
            if ($valid{ $desc }) {
                # And the term is a true subset root
                ($setName, $rootNode) = ($id, $id);
            }
        }
    }
    # This is not a valid GO subset ID
    return [] unless ($rootNode);
    
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );

    my @kids  = $self->all_children( $rootNode, 'GO', $age );
    my @rows  = map {{ term_in  => $setName,
                       term_out => $_,
                       ns_in    => $nsIn,
                       ns_out   => $nsOut,
                       auth     => 'GeneOntology',
                       matched  => 1 } } @kids;

    if ($#rows < 0) {
        push @rows, { term_in  => $setName,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head3 UNIGENE_MEMBERS

 Convert : UniGene <-> LocusLink
 Input   : UG / AP AR RSR RSP SP TR LL AL UP
 MT Link : MEMBEROF
 Output  : AL AP AR RSR RSP SP TR LL UP / UG

=cut

#map { &_set_conv( 'UG', $_, 'update_UNIGENE_MEMBERS');
#      &_set_conv( $_, 'UG', 'update_UNIGENE_MEMBERS') }
#qw(AL LL ENSG AR RSR ENST AP RSP ENSP UP TR SP UPC);
sub update_UNIGENE_MEMBERS {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    unless ($self->verify_namespace($id, $ns1)) {
        return [];
    }

    my @rows;
    my ($gns)   = $self->guess_namespace($id, $ns1);
    my @clouds = $self->cached_clouds
        ( -age    => $self->cloud_age || $age,
          -type   => 'GeneCluster',
          -seed   => [[$id, $gns]] );
    foreach my $cloud (@clouds) {
        # warn $cloud->to_text;
        push @rows, $cloud->paths( -ns2 => $ns2 );
        # warn $cloud->to_text();
    }

    if ($#rows < 0) {
        push @rows, { term_in    => $id,
                      ns_in      => $nsIn,
                      ns_out     => $nsOut };
    } else {
        map { $_->{ns_out} = $nsOut } @rows;
        # Allow direct assignments to have a score of 1
        map { $_->{matched} = 1 unless ($_->{ns_between}) } @rows;
        @rows = $self->simplify_rows( -rows => \@rows,
                                      -shrinkauth => 1, );
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head3 NCBI_GI_ID

 Convert : GI <-> Any Protein / Any RNA
 Input   : GI / AP AR
 MT Link : is a reliable alias for
 Output  : AL AP AR RSR RSP SP TR LL UP / UG

=cut

{ my @seqs = &_full_kids( ['AP', 'AR'] );
  &_set_conv( 'GI', \@seqs, 'update_NCBI_GI_ID');
  &_set_conv( \@seqs, 'GI', 'update_NCBI_GI_ID');
}
sub update_NCBI_GI_ID {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $mt = $self->tracker();
    return [] unless ($mt);

    my ($id, $seq) = $self->standardize_id( $idReq, $ns1 );
    unless ($self->verify_namespace($id, $ns1)) {
        return wantarray ? () : [];
    }
    my $nsIn      = $self->namespace_name( $ns1 );
    my $nsOut     = $self->namespace_name( $ns2 );
    my @ids       = ($id);
    my @rows;
    if ($seq) {
        my $type  = 'is a reliable alias for';
        my $mtId  = $id;
        if ($ns1 eq 'GI') {
            if ($id =~ /^GI\:(\d+)$/i) {
                $mtId = "gi$1";
            }
        } else {
            $mtId .= ".%";
        }
        
        my $class = $self->primary_maptracker_class( $ns2 );
        my $struct = $self->fast_edge_hash
            ( -name      => $mtId,
              -keepclass => $class,
              # -auth      => 'NCBI',
              -keeptype  => $type);
        while (my ($oname, $auths) = each %{$struct}) {
            next unless ($self->verify_namespace( $oname, $ns2 ));
            # my @simp = $self->simplify_authors(@{$auths});
            if ($ns1 eq 'GI') {
                ($oname) = $self->standardize_id($oname, $ns2);
                next unless ($oname);
            }
            push @rows, map { { term_in    => $_,
                                term_out   => $oname,
                                ns_in      => $nsIn,
                                ns_out     => $nsOut,
                                auth       => 'NCBI', # join($authJoiner, @simp),
                                matched    => 1 } } @ids;
        }
    }
    if ($#rows == -1) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}


=head2 Assorted

Various converters that do not fit neatly into one of the above groupings.

=head3 RNAI_CONSORTIUM

 Convert : RNAi Consortium <-> RNA
 Input   : TRC / AR RSR
 MT Link : is inhibited by
 Output  : AR RSR / TRC

=cut

{ my @rnas = &_full_kids( ['AR'] );
  my $ns = "TRC";
  &_set_conv( \@rnas, $ns, 'update_RNAI_CONSORTIUM');
  &_set_conv( $ns, \@rnas, 'update_RNAI_CONSORTIUM');
#  my @skip = @rnas;
#  &_set_conv($ns, 'ANY', "LINK_TRC",
#             [map {"$ns $_"} @skip ]);
#  &_set_conv('ANY',$ns, "LINK_TRC",
#             [map {"$_ $ns"} @skip ]);
}

&_set_conv( "TRC", "ANY", "CONDITIONAL_TO_AR",   [ ]);
&_set_conv( "ANY", "TRC", "CONDITIONAL_FROM_AR", [ ]);

my $trcAssayTypes = ["was assayed against",
                     "is an agonist for",
                     "is an antagonist for"];
sub update_RNAI_CONSORTIUM {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my $mt    = $self->tracker();
    return [] unless ($mt);

    my ($id, $seq) = $self->fast_standardize( $idReq, $ns1, 'verifyClass' );
    my $viaTRC = $ns1 eq 'TRC' ? 1 : 0;
    my @rows;
    if (!$seq) {
        # Malformed / unrecognized user request - do nothing
    } elsif ($ns2 eq 'AR') {
        # User is starting with a TRC ID and requesting generic RNA
        my %directRNAs;
        # First get TRC assignments via the hairpin:
        my $hairpins = $self->fast_edge_hash
            ( -name      => $id,
              -keepclass => 'shRNA',
              -keeptype  => 'is reliably aliased by' );

        foreach my $sh (keys %{$hairpins}) {
            # Now get targetted RNAs
            my $tested = $mt->get_edge_dump
                ( -name      => "#Sequence#$sh",
                  -return    => 'object array',
                  -dumpsql   => 0,
                  -keeptype  => $trcAssayTypes );
            foreach my $edge (@{$tested}) {
                my $rna  = $edge->node2->name();
                my @auth = $edge->each_authority_name();
                my $sc   = -1;
                foreach my $tag ($edge->has_tag('%KD Conservative')) {
                    my $perc = $tag->num;
                    if (defined $perc) {
                        $perc /= 100;
                        $sc = $perc if ($perc > $sc);
                    }
                }
                map { push @{$directRNAs{$rna}{$_}}, $sc } @auth;
            }
        }
        # Get assignments not via the hairpin
        my $probed = $self->fast_edge_hash
            ( -name      => $id,
              -keepclass => 'RNA',
              -keeptype  => "is a probe for");
        $self->edge_ids_to_authorities( $probed );
        while (my ($rna, $auths) = each %{$probed}) {
            my $sc = -1;
            map { push @{$directRNAs{$rna}{$_}}, $sc } @{$auths};
        }
        my (@primaryRows, @secondaryRows);
        while (my ($rna, $authH) = each %directRNAs) {
            my @pr;
            while (my ($auth, $scArr) = each %{$authH}) {
                my ($sc) = sort { $b <=> $a } @{$scArr};
                push @pr, {
                    term_in  => $id,
                    term_out => $rna,
                    ns_in    => $nsIn,
                    ns_out   => $nsOut,
                    auth     => $auth,
                    matched  => $sc < 0 ? undef : $sc,
                };
            }
            @pr = $self->simplify_rows
                ( -rows => \@pr, -shrinkauth => 1, -allscore => 1);
            map {$_->{auth} .= " [%KD]" if (defined $_->{matched})} @pr;
            push @rows, @pr;
            my $secRows = $self->convert
                ( -id => $rna, -ns1 => 'AR', -ns2 => 'AR', -age => $age,
                  -ashash => 1, -auth => '!SelfReferential' );
            map { $_->{matched} = undef unless 
                      ($_->{matched} && $_->{matched} == 1) } @{$secRows};
            push @secondaryRows, $self->stitch_rows( \@pr, $secRows);
        }
        
        @secondaryRows = $self->simplify_rows
            ( -rows => \@secondaryRows, -shrinkauth => 1 );
        # push @rows, @primaryRows;
        foreach my $row (@secondaryRows) {
            push @rows, $row unless ($directRNAs{ $row->{term_out} } );
        }
        # @rows = $self->simplify_rows( -rows => \@rows, -shrinkauth => 1, -allscore => 1);
    } elsif ($ns1 eq 'TRC') {
        # User is starting with a TRC ID and requesting specific RNA namespace
        @rows = $self->filter_conversion( $id, $ns1, 'AR', $ns2, $age);
    } else {
        # User wants to finish with a TRC ID
        my %seeds = ( $id => [] );
        # Expand the query to other RNA identifiers
        my $secRows = $self->convert
            ( -id => $id, -ns1 => $ns1, -ns2 => 'AR', -age => $age,
              -ashash => 1, -auth => '!SelfReferential' );
        foreach my $row (@{$secRows}) {
            my $seed = $row->{term_out};
            next if ($seed eq $id);
            $row->{matched} = undef
                if ($row->{matched} && $row->{matched} != 1);
            push @{$seeds{$seed}}, $row;
        }
        my $arnsn = $self->namespace_name('AR');
        my @secondary;
        while (my ($rna, $seedRows) = each %seeds) {
            my %trcs;
            # Get reported assays:
            my $tested = $mt->get_edge_dump
                ( -name      => $rna,
                  -return    => 'object array',
                  -dumpsql   => 0,
                  -revtype   => 1,
                  -keepclass => 'shRNA',
                  -keeptype  => $trcAssayTypes );
            foreach my $edge (@{$tested}) {
                # We found short hairpins associated with the RNA
                my $sh   = $edge->node1->name();
                my $trcH = $self->fast_edge_hash
                    ( -name      => $sh,
                      -keepclass => 'The RNAi Consortium',
                      -keeptype  => 'is a reliable alias for' );
                my @trcIDs = sort keys %{$trcH};
                next if ($#trcIDs == -1);
                my @auth = $edge->each_authority_name();
                my $sc   = -1;
                foreach my $tag ($edge->has_tag('%KD Conservative')) {
                    my $perc = $tag->num;
                    if (defined $perc) {
                        $perc /= 100;
                        $sc = $perc if ($perc > $sc);
                    }
                }
                foreach my $trc (@trcIDs) {
                    map { push @{$trcs{$trc}{$_}}, $sc } @auth;
                }
            }
            # Now get non-hairpin assignments
            my $probed = $self->fast_edge_hash
                ( -name      => $id,
                  -keepclass => 'The RNAi Consortium',
                  -keeptype  => "can be assayed with probe");
            $self->edge_ids_to_authorities( $probed );
            while (my ($trc, $auths) = each %{$probed}) {
                my $sc = -1;
                map { push @{$trcs{$trc}{$_}}, $sc } @{$auths};
            }
            my @trcLinks;
            my $isPri = $#{$seedRows} == -1 ? 1 : 0;
            my $nsRNA = $isPri ? $nsIn : $arnsn;
            while (my ($trc, $authH) = each %trcs) {
                while (my ($auth, $scArr) = each %{$authH}) {
                    my ($sc) = sort { $b <=> $a } @{$scArr};
                    push @trcLinks, {
                        term_in  => $rna,
                        term_out => $trc,
                        ns_in    => $nsRNA,
                        ns_out   => $nsOut,
                        auth     => $auth,
                        matched  => $sc < 0 ? undef : $sc,
                    };
                }
            }
            next if ($#trcLinks == -1);
            @trcLinks = $self->simplify_rows
                ( -rows => \@trcLinks, -shrinkauth => 1, -allscore => 1);
            map {$_->{auth} .= " [%KD]" if (defined $_->{matched})} @trcLinks;
            if ($isPri) {
                push @rows, @trcLinks;
            } else {
                push @secondary, $self->stitch_rows( $seedRows, \@trcLinks);
            }
        }
        my %gotPri = map { $_->{term_out} => 1 } @rows;
        foreach my $sec ( $self->simplify_rows
                          ( -rows => \@secondary, -shrinkauth => 1 )) {
            next if ($gotPri{ $sec->{term_out} });
            push @rows, $sec;
        }
    }
    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head3 TEST_to_TEST

 Convert : Testing Value to Testing Value
 Input   : TEST
 MT Link : n/a
 Output  : TEST

The method is only useful for test cases.

=cut

sub update_TEST_to_TEST {
    my $self = shift;
    my ($id, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my @rows = ({
        term_in  => $id,
        term_out => "$id Out",
        ns_in    => $nsIn,
        ns_out   => $nsOut,
        auth     => 'The Lorax',
        matched  => int(0.5 + 1000 * rand())/ 1000,
    });
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head1 Chained Conversions

The following conversions work through a chain of one or more direct
conversions

=head2 Core Biology Chains

=head3 LOCUS_to_PROTEIN

 Convert : Locus to Protein
 Input   : LL / ENSG / LL
 Via     : AR (or direct)
 Output  : RSP AR / ENSP AR

Namespace is guessed for output objects if they are AP. This method
also contains direct logic to find proteins directly associated with
the locus, if it fails to find any transitively. This is needed for
some LocusLink entries (sometimes whole species!) that do not have a
defined mRNA intermediate. GRRR!

=cut

{ my $na = "AL LL ENSG";
  my $nb = "AP ENSP RSP SP TR UP";
  # &_set_conv( $na, $nb, 'update_TIE_PROTEIN_to_LOCUS' );
  # &_set_conv( $nb, $na, 'update_TIE_PROTEIN_to_LOCUS' );
}

my $prot_loc_canon = {
    RSP  => 'RSR',
    LL   => 'RSR',
    ENSP => 'ENST',
    ENSG => 'ENST',
};

sub update_TIE_PROTEIN_to_LOCUS {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my $mt    = $self->tracker();
    return [] unless ($mt);

    my ($id, $seq) = $self->fast_standardize( $idReq, $ns1 );
    # Try going through the RNA first:
    my $mid  = $prot_loc_canon->{$ns1} || 'AR';
    my @rows = $self->chain_conversions
        ( -id       => $id, 
          -nonull   => 1,
          -chain    => [$ns1, $mid, $ns2], 
          -age      => $age, );
    if ($#rows == -1) {
        # Then try going directly from Locus to Protein
        if ($seq) {
            my $class = $self->primary_maptracker_class( $ns2 );
            my $type  = $self->is_namespace($ns2, 'AP') ?
                'is a locus with protein' : 'is a protein from locus';
            my $list  = $mt->get_edge_dump
                ( -name      => $seq,
                  -return    => 'object array',
                  -keeptype  => $type,
                  -tossclass => 'deprecated',
                  -keepclass => $class,
                  -dumpsql   => 0 );
            my %struct;
            foreach my $edge (@{$list}) {
                my $other = $edge->other_seq( $seq );
                next unless ($other->is_class('protein'));
                map { $struct{ $other->name }{ $_ } = 1 }
                $edge->each_authority_name;
            }
            while (my ($oname, $auths) = each %struct) {
                my @simp = $self->simplify_authors(keys %{$auths});
                push @rows, { term_in  => $id,
                              term_out => $oname,
                              ns_in    => $nsIn,
                              ns_out   => $nsOut,
                              auth     => join($authJoiner, @simp),
                              matched  => 1 };
            }
        }
    }
    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

&_set_conv( "ENSE", 'ANY', "CONDITIONAL_TO_ENST",
            ["ENSE ENST"]);
&_set_conv( 'ANY', "ENSE", "CONDITIONAL_FROM_ENST",
            ["ENST ENSE"]);


=head2 Taxonomy Chains

=head3 TAX_to_SEQUENCE

 Convert : Taxonomy to reference sequence identifiers
 Input   : TAX
 Via     : LL / ENSG / LL+ENSG
 Output  : RSR RSP / ENST ENSP / AR AP

Namespace is guessed for output objects if they are AP/AR

=cut

&_set_conv( 'TAX', 'RSR RSP ENST ENSP AR AP', 'update_TAX_to_SEQUENCE');
sub update_TAX_to_SEQUENCE {
    my $self = shift;
    my ($id, $ns2, $ns1, $age) = @_;
    my @mid;
    push @mid, 'LL' unless ($ns2 =~ /^ENS/);
    push @mid, 'ENSG' unless ($ns2 =~ /^RS/);
    my @rows = $self->chain_conversions
        ( -id       => $id, 
          -chain    => [$ns1, \@mid, $ns2],
          # -guessout => ($ns2 =~ /^(AP|AR)$/) ? 1 : 0,
          -age      => $age );
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head2 Namespace conversions

=head3 Locus Namespace

 NS1     : AL LL ENSG
 Link To : AR/AP
 NS2     : AL LL ENSG

Maps between loci namespaces using both Any RNA and Any Protein as a bridge.

=cut


&_set_conv( "AL LL ENSG", "AL LL ENSG", "FLAGS:NOSELF_CONDITIONAL_TO_AR|AP", 
            ["LL LL", "ENSG ENSG"]);

=head2 Synonym and Alias Chains

=head3 Wikipedia

 NS1     : WP
 Link To : AL
 NS2     : ANY (Conditional)

Links Wikipedia articles beyond their loci

=cut

{ my @targs = &_full_kids
      ( [qw(AR AL AP SYM)] );
  &_set_conv( "WP", \@targs, "CONDITIONAL_TO_LL", 
              ["WP LL" ]);
  &_set_conv( \@targs, "WP", "CONDITIONAL_FROM_LL",
              ["LL WP" ]);
}

=head3 Gene Symbol associations

 NS1     : SYM
 Link To : AL
 NS2     : ANY (Conditional)

Links gene symbols to any data associated with the loci they point to.

=cut


&_set_conv( "SYM", 'ANY', "CONDITIONAL_TO_AL", 
            ["SYM AL", "SYM LL", "SYM ENSG"]);
&_set_conv( 'ANY', "SYM", "CONDITIONAL_FROM_AL",
            ["AL SYM","LL SYM","ENSG SYM" ]);

=head3 GI Numbers

 NS1     : GI / ANY
 Link To : Any RNA | Any Protein
 NS2     : ANY /GI (Conditional)

Ties GI numbers to the rest of the world through Any RNA or Any Protein

=cut

{
    &_set_conv( "GI", 'ANY', "CONDITIONAL_TO_AP|AR" );
    &_set_conv( 'ANY', "GI", "CONDITIONAL_FROM_AP|AR");
}

=head3 UniProt Names

 NS1     : SPN / TRN / UPN
 Link To : SP / TR / UP
 NS2     : ANY (Conditional)

Links UniProt names to all converters that the UniProt accessions
themselves are linked to.

=cut

map { &_set_conv( "${_}N", 'ANY', "CONDITIONAL_TO_$_",
                  ["${_}N $_", "${_}N ${_}N"]);
      &_set_conv( 'ANY', "${_}N", "CONDITIONAL_FROM_$_",
                  ["$_ ${_}N", "${_}N ${_}N"])} qw(SP TR UP);

=head3 RNAi Consortium linkages

 NS1     : TRC
 Link To : AR
 NS2     : ANY (Conditional)

Links RNAi consortium reagents to any data associated with their
target RNAs.

=cut

&_set_conv( "TRC", 'ANY', "CONDITIONAL_TO_SEQ", 
            ["TRC SEQ"]);
&_set_conv( 'ANY', "TRC", "CONDITIONAL_FROM_SEQ",
            ["SEQ TRC" ]);

=head3 Unigene linkages

 NS1     : UG
 Link To : AR|AP|AL
 NS2     : ANY (Conditional)

Links UniGene IDs to most other objects through RNA, Protein or Loci
assigned to that UniGene cluster.

=cut

{ my @skip = qw(AP AR RSR RSP SP TR AL LL ENSG PMID TAX UP APS CLPS);
  my $ns = 'UG'; my $mid = "AR|AP|AL";
  &_set_conv( $ns, 'ANY', "CONDITIONAL_TO_$mid", 
              [map {"$ns $_"} @skip ]);
  &_set_conv( 'ANY', $ns, "CONDITIONAL_FROM_$mid",
              [map {"$_ $ns"} @skip ]);
}

=head3 Array Design Linkages

 NS1     : AAD / CLAD
 Link To : APS / CLPS
 NS2     : ANY (Conditional)

Links Affy and CodeLink array designs to any other entries via their
probe sets.

=cut

{ my @skip = qw(APS CLPS AAD CLAD XONT);
  foreach my $dat (['AAD','APS'],['CLAD','CLPS']) {
      my ($ns, $mid) = @{$dat};
      &_set_conv( $ns, 'ANY', "CONDITIONAL_TO_$mid", 
                  [map {"$ns $_"} @skip ]);
      &_set_conv( 'ANY', $ns, "CONDITIONAL_FROM_$mid",
                  [map {"$_ $ns"} @skip ]);
  }
  &_set_conv( 'ANY', 'XONT', "CONDITIONAL_FROM_APS",
              [map {"$_ XONT"} qw(CDD APS CLPS CLAD BTFO) ]);
}


=head3 CDD

 NS1     : CDD / ANY
 Link To : AP
 NS2     : ANY / CDD (Conditional)

Links CDD ontology to other entities via protein.

=cut


#{
#    &_set_conv( 'CDD', 'ANY', "CONDITIONAL_TO_AP" );
#    &_set_conv( 'ANY', 'CDD', "CONDITIONAL_FROM_AP" );
#}







=head3 XPRESS Ontology Linkages

 NS1     : XONT
 Link To : APS
 NS2     : ANY (Conditional)

Links XPRESS ontology terms to any other entries via their Affymetrix
probe sets.

=cut

{ my @skip = qw(CDD APS CLPS CLAD BTFO);
  my $ns   = 'XONT'; my $mid = 'APS';
  &_set_conv( $ns, 'ANY', "CONDITIONAL_TO_$mid", 
              [map {"$ns $_"} @skip ]);
  &_set_conv( 'ANY', $ns, "CONDITIONAL_FROM_$mid",
              [map {"$_ $ns"} @skip ]);
}

=head3 Protein Linkages

 NS1     : NRDB IPI
 Link To : AP
 NS2     : ANY (Conditional)

Links NDRB entries to other terms via any proteins associated with them

=cut

{ my @skip = qw(ENSP RSP AP SP TR UP IPI NRDB);
  my $ns = 'NRDB IPI'; my $mid = join('|', @skip);
  &_set_conv( $ns, 'ANY', "CONDITIONAL_TO_AP", 
              [map {"$ns $_"} @skip ]);
  &_set_conv( 'ANY', $ns, "CONDITIONAL_FROM_AP",
              [map {"$_ $ns"} @skip ]);
}

=head3 Invitrogen Linkages

 NS1     : IUO
 Link To : AR RSR
 NS2     : ANY (Conditional)

Links Invitrogen Ultimate ORFs to data associated with their source
RNAs. For [GO CDD], source proteins [AP RSP] will be used instead.

=cut

{ my @skip = qw(AP AR RSR RSP PMID);
  my @prot = qw(GO CDD);
  my $ns = 'IUO'; my $mid = 'AR|RSR';
  &_set_conv( $ns, 'ANY', "CONDITIONAL_TO_$mid", 
              [map {"$ns $_"} (@skip, @prot) ]);
  &_set_conv( 'ANY', $ns, "CONDITIONAL_FROM_$mid",
              [map {"$_ $ns"} (@skip, @prot) ]);
  # Get certain information via the protein links
  $mid = "AP|RSP";
  &_set_conv( $ns, \@prot, "CONDITIONAL_TO_$mid", );
  &_set_conv( \@prot, $ns, "CONDITIONAL_FROM_$mid", );
}

=head3 BTFO Linkages

 NS1     : BTFO
 Link To : LL
 NS2     : ANY (Conditional)

Links BMS Transcription Factor Ontology entries to other terms via the
loci associated with them

=cut

{ my @skip = qw(LL GO CDD ONT PMID XONT);
  my $ns = 'BTFO'; my $mid = 'LL';
  &_set_conv( $ns, 'ANY', "CONDITIONAL_TO_$mid", 
              [map {"$ns $_"} @skip ]);
  &_set_conv( 'ANY', $ns, "CONDITIONAL_FROM_$mid",
              [map {"$_ $ns"} @skip ]);
}

=head3 CDD / IPR Linkages

 NS1     : CDD IPR / ANY (Conditional)
 Link To : AP
 NS2     : ANY (Conditional) / CDD IPR

Links CDD entries to other terms via any proteins associated with them

=cut

{ my @skip = qw(AP RSP ENSP GO BTFO PMID XONT);
  my $ns = 'CDD IPR'; my $mid = 'AP';
  &_set_conv( $ns, 'ANY', "CONDITIONAL_TO_$mid", 
              [map {"$ns $_"} @skip ]);
  &_set_conv( 'ANY', $ns, "CONDITIONAL_FROM_$mid",
              [map {"$_ $ns"} @skip ]);
}

=head2 Set and Group Chains

=head3 SET_to_REFSEQ

 Convert : Set (Taxonomy, really) to RefSeq RNA / Protein
 Input   : SET
 Via     : TAX
 Output  : RSP RSR

The input set should be of the form Species + Type, for example "Homo
sapiens RefSeq Protein" or "Mus musculus RefSeq RNA". This method will
verify that the set name is valid and then retrieve data by executing
L<TAX_to_OBJECT|tax_to_object>.

=cut

&_set_conv( 'SET', 'RSP RSR', 'update_SET_to_REFSEQ');
sub update_SET_to_REFSEQ {
    my $self = shift;
    my ($id, $ns2, $ns1, $age) = @_;
    my $dbh   = $self->dbh;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my $rows;
    # warn "($id, $ns2, $ns1, $age)";
    if ($id =~ /(.+) \Q$nsOut\E$/) {
        my ($species, $obj) = $self->standardize_taxa($1);
        if ($obj) {
            # Just copy over the mappings from TAX_to_RSP/R
            $id = "$species $nsOut";
            $rows = $self->copy_conversion
                ( $id, $nsIn, $nsOut, 
                  $species, 'TAX', $nsOut, $age );
        }
    }
    if (!$rows || $#{$rows} == -1) {
        return $self->update_GENERIC_SETS( @_ );
    #    $rows = [ { term_in  => $id,
    #                ns_in    => $nsIn,
    #                ns_out   => $nsOut, } ]; 
    }
    $self->dbh->update_rows( 'conversion', $rows );
    return $rows;
}

=head3 REFSEQ_to_SET

 Convert : RefSeq RNA / Protein to Set (Taxonomy, really)
 Input   : RSP RSR
 MT Link : LL
 Output  : SET

The method will return the appropriate taxonomy-based set for the
RefSeq object.

=cut

&_set_conv( 'RSP RSR', 'SET', 'update_REFSEQ_to_SET');
sub update_REFSEQ_to_SET {
    my $self = shift;
    my ($idReq, $ns2, $ns1, $age) = @_;
    my $dbh   = $self->dbh;
    my $nsIn  = $self->namespace_name( $ns1 );
    my $nsOut = $self->namespace_name( $ns2 );
    my ($id, $obj) = $self->standardize_id( $idReq, $ns1 );
    unless ($self->verify_namespace($id, $ns1)) {
        return [];
    }
    my @rows;
    if ($obj) {
        my @found = $self->chain_conversions
            ( -id       => $id, 
              -chain    => [$ns1, 'LL', 'SET'], 
              -age      => $age, );
        foreach my $row (@found) {
            next unless ($row->{term_out});
            if ($row->{term_out} =~ /^(.+) LocusLink$/) {
                $row->{term_out} = "$1 $nsIn";
            }
            push @rows, $row;
        }
    }
    if ($#rows < 0) {
        push @rows, { term_in  => $id,
                      ns_in    => $nsIn,
                      ns_out   => $nsOut };
    }
    $dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

=head1 Assignment Updaters

These methods are used to populate the assign_onto table.

=head2 CDD_to_PROTEIN

 Assign  : Conserved Domain Database to Proteins
 Input   : AP RSP
 MT Link : 
 Output  : CDD

=cut

*assign_CDD_to_RSP = \&assign_CDD_to_PROTEIN;
*assign_CDD_to_AP  = \&assign_CDD_to_PROTEIN;
*assign_CDD_to_LL  = \&assign_CDD_to_PROTEIN;
*assign_CDD_to_APS = \&assign_CDD_to_PROTEIN;
sub assign_CDD_to_PROTEIN {
    my $self = shift;
    my ($idReq, $ons, $ans, $age) = @_;

    my ($id, $seq) = $self->fast_standardize( $idReq, $ans );
    unless ($self->verify_namespace($id, $ans)) {
        return $self->format_assignment_rv( [] );
    }
    my %struct;
    if ($seq) {
        my $rows = $self->convert
            ( -id => $id, -ns1 => $ans, -ns2 => $ons, -age => $age);
        foreach my $row (@{$rows}) {
            my ($onto, $ontoNS, $auth, $matched) = @{$row};
            next unless ($onto);
            $matched = -1 unless (defined $matched);
            $struct{$onto}{'ISS'}{$matched} = 0;
        }
    }
    return $self->_standardize_assignment
        ( $id, $ons, $ans, $age, \%struct, 0);
}

=head2 ONTOLOGY_to_ANY

 Assign  : Ontology terms to any namespace
 Input   : Any Namespace
 MT Link : 
 Output  : GO BTFO

=cut

*assign_BTFO_to_ANY = \&assign_ONTOLOGY_to_ANY;
*assign_GO_to_ANY   = \&assign_ONTOLOGY_to_ANY;
*assign_XONT_to_ANY = \&assign_ONTOLOGY_to_ANY;
*assign_PMID_to_ANY = \&assign_ONTOLOGY_to_ANY;
sub assign_ONTOLOGY_to_ANY {
    my $self = shift;
    my ($idReq, $ons, $ans, $age) = @_;

    my ($id, $seq) = $self->fast_standardize( $idReq, $ans );
    unless ($self->verify_namespace($id, $ans)) {
        return $self->format_assignment_rv( [] );
    }
    my %struct;
    if ($seq) {
        my $rows = $self->convert
            ( -id => $id, -ns1 => $ans, -ns2 => $ons, -age => $age);
        foreach my $row (@{$rows}) {
            my ($onto, $gns, $auth, $matched) = @{$row};
            next unless ($onto);
            my ($ecs) = split(' < ', $auth);
            $matched  = -1 unless (defined $matched);
            map { $struct{$onto}{$_}{$matched} = 0 } split(/\Q$authJoiner\E/, $ecs);
        }
    }
    # Include the XONT root parent
    my $requireParent = ($ons eq 'PMID') ? 0 : 1;
    return $self->_standardize_assignment
        ( $id, $ons, $ans, $age, \%struct, $requireParent );
}

sub _standardize_assignment {
    my $self = shift;
    $self->benchstart;
    my ($id, $ontoNS, $accessionNS, $age, $struct, $requireParent) = @_;
    # Find the best score for each Ontology / EvidenceCode pair
    my @direct = keys %{$struct};
    foreach my $onto (@direct) {
        my @ecs = keys %{$struct->{$onto}};
        foreach my $ec (@ecs) {
            my ($best) = sort { $b <=> $a } keys %{$struct->{$onto}{$ec}};
            # Restructure so that only the best match remains
            $struct->{$onto}{$ec} = $best;
        }
    }

    # Now denormalize to include all parents:
    my (%denorm, %ontoSet, %ontoDesc);
    foreach my $onto (@direct) {
        my $parhash = 
            $self->all_parents( -id => $onto, -ns => $ontoNS, -age => $age );
        foreach my $par (keys %{$parhash}) {
            if (my $dd = $parhash->{$par}) {
                while (my ($ec, $best) = each %{$struct->{$onto}}) {
                    push @{$denorm{$par}{$ec}{$best}}, $dd->[0];
                }
            }
        }
    }

    # Expand the accession namespace
    #my @guessed = $self->guess_namespace($id, $accessionNS);
    #my @full    = $self->namespace_parents( @guessed );
    my @nsAccs  = map { $self->namespace_name($_) } ($accessionNS); #@full;

    my $ons     = $self->namespace_name( $ontoNS );
    my $accDesc = $self->description
        ( -id => $id, -ns => $accessionNS, -age => $age );
    my @rows;
    while (my ($onto, $ehash) = each %denorm) {
        # Only include ontology terms that have parents
        next if ($requireParent && 
                 ! $self->has_parent_cached( $onto, $ontoNS ));
        my $subset = '';
        if ($ontoNS =~ /^(GO|XONT)$/) {
            my @sets  = $self->cached_conversion
                ( -id => $onto, -ns1 => $ontoNS, -ns2 => 'SET', -age => $age);
            # Do not mess with ontologies belonging to multiple subsets:
            next if ($#sets > 0);
            $subset = $sets[0] || '';
        }

        my $ontoDesc = $self->description
            ( id => $onto, -ns => $ontoNS, -age => $age);
        while ( my ($ec, $mhash) = each %{$ehash}) {
            # Find the best score again (parents could have improved it)
            my ($best) = sort { $b <=> $a } keys %{$mhash};
            # Find the shortest distance for that score:
            my ($dist) = sort { $a <=> $b } @{$mhash->{$best}};
            # We kludged undef to -1 for sorting purposes, unkludge now
            if ($best < 0) {
                $best = undef;
            } else {
                $best = int(0.5 + $best * 10000) / 10000 || 0.0001 if ($best);
            }
            foreach my $ans (@nsAccs) {
                push @rows, {
                    acc         => $id,
                    onto        => $onto,
                    ec          => $ec,
                    matched     => $best,
                    acc_ns      => $ans,
                    onto_ns     => $ons,
                    onto_subset => $subset,
                    acc_desc    => $accDesc,
                    onto_desc   => $ontoDesc,
                    parentage   => $dist,
                };
            }
        }
    }
    if ($#rows < 0) {
        foreach my $ans (@nsAccs) {
            push @rows, {
                acc         => $id,
                acc_ns      => $ans,
                onto_ns     => $ons,
                acc_desc    => $accDesc,
            };
        }
    }
    $self->benchend;
    $self->dbh->update_rows( 'assign_onto', \@rows );
    return $self->format_assignment_rv( \@rows );
}

=head1 Description Updaters

These methods populate the description table.

=head2 Generic Description

This method will be used in the absence of any other
namespace-specific description recovery logic. It uses
BMS::MapTracker::Seqname::desc() to get the shortest description (via
'is a shorter term for' edge type) for the sequence.

If deep_dive() is true, the method will search the SeqStore database
to find descriptions absent from MapTracker.

=cut

sub update_GENERIC_description {
    my $self = shift;
    my ( $idReq, $ns, $age) = @_;
    my $nsIn = $self->namespace_name($ns);
    my ($id, $seq) = $self->standardize_id( $idReq, $ns );

    if ($ns eq 'SYM' && 
                  (!$seq || $seq->namespace->name ne 'GeneSymbols')) {
        $self->dbh->update_rows( 'description', [ { term  => $id,
                                                    descr => $badToken,
                                                    ns    => $nsIn } ] );
        return $badToken;
    }
    my $meta = "";
    my %descs;

    my $isFirst = 0;
    my $done = $self->{RECURSIVE_DESCRIPTION};
    if ($done) {
        # If we are in a loop trying to find a description then leave now
        return () if ($done->{$id}++);
    } else {
        # Make a note if this is the initiating call
        $done = $self->{RECURSIVE_DESCRIPTION} = { $id => 1 };
        $isFirst = 1;
    }
    my $doWarn = $self->{DESCWARN};
    if ($seq) {
        if ($ns eq 'TAX') {
            my ($desc) = $seq->each_alias('genbank common name');
            $desc = $desc ? "$desc: " : '';
            my @pars = reverse (map { $_->name() } $seq->all_parents());
            shift @pars if ($#pars != -1 && $pars[0] eq 'root');
            $desc .= ($#pars == -1 ? "Unknown lineage" : join('; ', @pars));
            $desc  = "$depToken $desc" if ($seq->is_deprecated);
            $descs{uc($desc)} = $desc;
        } elsif ($ns eq 'PMID') {
            my $struct = $self->fast_edge_hash
                ( -name      => $seq,
                  -keeptype  => 'is a shorter term for', );
            my @titles;
            # Get dates associated with articles
            # They are kludged onto the edge holding the title:
            while (my ($name2, $eidHash) = each %{$struct}) {
                my @eids  = keys %{$eidHash};
                # There are some old (and incorrect) data from non-PubMed
                # authorities in the database:
                my %auths = map { $_ => 1 } $self->auths_for_edge_ids(\@eids);
                next unless ($auths{PubMed});
                # See if we can get the date of the publication, too:
                my @nums  = sort { $b <=> $a } 
                $self->tags_for_edge_ids(\@eids,'Date','numeric');
                push @titles, [ $nums[0] || 0, $name2 ];
            }
            # Get most recent date, or longest description:
            my ($best) = sort { $b->[0] <=> $a->[0] || 
                                    length($b->[1]) <=> length($a->[1]) ||
                                    uc($a) cmp uc($b) } @titles;
            if ($best) {
                if (my $desc = $best->[1]) {
                    if (my $ymd = $best->[0]) {
                        # 2010.0409 = April 9, 2010 = 2010-04-09
                        my $dt = int($ymd);
                        if (my $md = $ymd - $dt) {
                            # There is a month component
                            # Grrr. Rounding issues
                            $md    = int(0.5 + $md * 10000) / 100;
                            my $mn = int($md);
                            $dt   .= sprintf("-%02d", $mn);
                            if (my $d = $md - $mn) {
                                # Day component
                                $dt .= sprintf("-%02d", int(0.5 + 100 * $d));
                            }
                        }
                        $desc = "[$dt] $desc";
                    }
                    $descs{uc($desc)} = $desc;
                }
            }
        } else {
            my @seqs = ($seq);
            foreach my $seq (@seqs) {
                my $desc = $seq->desc;
                my $isDep = $seq->is_class('deprecated');
                if ($desc) {
                    $desc = $desc->name;
                    if ($desc =~ /^ENS[A-Z]{1,4}\d+ [\d \/]+$/) {
                        # Non-informative Ensembl descriptions
                        $desc = "";
                    }
                   # if ($idReq =~ /^\Q$id\E-\d+$/) {
                   #     # Swiss-Prot variant
                   #     my $vseq = 
                   # }
                }
                $desc ||= "";
                if ($isDep) {
                    # Deprecated entry with no description
                    # Try to get it from more recent entries.
                    my $rs = $self->convert( -id => $id, 
                                             -ns1 => $ns, -ns2 => 'RS');
                    my (@dep, @nondep);
                    foreach my $rr (@{$rs}) {
                        my ($sc, $out) = ($rr->[3], $rr->[0]);
                        next if (!$out || uc($out) eq uc($id));
                        # Make sure the ID is in the same namespace:
                        my ($sout, $seq) = $self->standardize_id
                            ( $out, $ns, 'checkclass' );
                        next unless ($seq);
                        if (!$sc) {
                            if (defined $sc) {
                                # A score=0 Reliable Synonym should indicate
                                # another but newer deprecated ID
                                push @dep, $out;
                            }
                        } elsif ($sc == 1) {
                            # A score=1 Reliable Synonym should indicate
                            # a non-deprecated replacement for this term
                            push @nondep, $out;
                        }
                    }
                    unless ($desc) {
                        my @getDesc = $#nondep != -1 ? @nondep : @dep;
                        my @got;
                        foreach my $did (@getDesc) {
                            my $dd = $self->description
                                ( -id => $did, -ns => $ns, -age => $age);
                            push @got, [$dd, $did] if ($dd);
                        }
                        foreach my $sd (sort { length($a->[0]) <=>
                                                   length($b->[0]) } @got) {
                            my ($d, $did) = @{$sd};
                            next if ($d eq $depToken);
                            $desc = "$d [$did]";
                            last;
                        }
                    }
                    if ($#nondep != -1) {
                        if ($desc) {
                            $desc .= "." if ($desc !~ /\.$/);
                            $desc .= " ";
                        }
                        $desc .= "More recent records: ".join(', ', @nondep); 
                    }
                }

                my $gns = "";
                ($gns)  = $self->guess_namespace($id, $ns) unless ($desc);
                if ($desc) {
                    # One of the methods above recovered a description
                } elsif ($id =~ /^UPI[0-9A-F]{10}$/ ||
                         $id =~ /^OTT[A-Z]{3}P\d{11}$/) {
                    # UniParc or Ensembl ID
                } elsif ($gns =~ /^(SP|TR|UP)N$/) {
                    # UniProt names, devolve to their accessions
                    my $ns2 = $1;
                    my @accs = $self->convert
                        ( -id => $id, -ns1 => $gns,-ns2 => $ns2, -age => $age,
                          -warn => $doWarn);
                    $desc = $self->_inherited_description
                        ( [[$ns2, \@accs]], $age, $desc);
                } elsif ($id =~ /^(MGI\:|FBgn)\d+$/) {
                    my @rss = $self->convert
                        ( -id => $id, -ns1 => $gns,-ns2 => 'RS', -age => $age,
                          -warn => $doWarn);
                    my @accs;
                    map { push @accs, $_ if (/^LOC\d+$/) } @rss;
                    $desc = $self->_inherited_description
                        ( [['LL', \@accs]], $age, $desc);
                } elsif ($gns =~ /^(SYM|MANK|AMBS|BMSS)$/) {
                    # Get descriptions from the loci
                    my @related;
                    foreach my $min (1, 0.4, 0.3) {
                        foreach my $ns2 (qw(LL ENSG AL)) {
                            my @accs = $self->convert
                                (-id => $id, -ns1 => $gns, -ns2 => $ns2,
                                 -age => $age, -min => $min, -warn => $doWarn);
                            push @related, [$ns2, \@accs]
                                unless ($#accs == -1);
                        }
                    }
                    $desc = $self->_inherited_description
                        ( \@related, $age, $desc);
                } elsif ($gns =~ /^(ENSE)$/) {
                    # Get descriptions from the RNA
                    my $ns2  = "ENST";
                    my $rnas = $self->convert
                        (-id => $id, -ns1 => $gns, -ns2 => $ns2,
                         -cols => ['term_out','auth'], -nonull => 1,
                         -age => $age, -warn => $doWarn);
                    unless ($#{$rnas} == -1) {
                        my $related = [[ $ns2, [ map { $_->[0] } @{$rnas}]]];
                        $desc = $self->_inherited_description
                            ( $related, $age, $desc);
                        my %nums;
                        foreach my $row (@{$rnas}) {
                            if ($row->[1] && $row->[1] =~ /\[Exon (\d+)\]/) {
                                $nums{$1}++;
                            }
                        }
                        my @n = sort { $a <=> $b } keys %nums;
                        unless ($#n == -1) {
                            my $tag = "Exon number ".join(',', @n);
                            $desc = $desc ? "$tag $desc" : $tag;
                        }
                    }
                } elsif ($gns eq 'NRDB') {
                    # Try to use more informative entries, if possible:
                    my @related;
                    foreach my $ns2 (qw(RSP SP ENSP AP)) {
                        my $class = $self->primary_maptracker_class($ns2);
                        my $struct = $self->fast_edge_hash
                            ( -name      => $seq,
                              -keepclass => $class,
                              -keeptype  => 'is the same as', );
                        my @accs = keys %{$struct};
                        # warn "$id : $class : ".join(',', @accs);
                        push @related, [$ns2, \@accs] unless ($#accs == -1);
                    }
                    $desc = $self->_inherited_description
                        ( \@related, $age, $desc);
                } elsif ($gns eq 'IUO') {
                    # Invitrogen clones inherit RefSeq data
                    my $ns2 = 'RSR';
                    my @related;
                    foreach my $ns2 (qw(RSR AR)) {
                        my @accs = $self->convert
                            (-id => $id, -ns1 => $gns, -ns2 => $ns2,
                             -age => $age, -warn => $doWarn );
                        push @related, [$ns2, \@accs]
                            unless ($#accs == -1);
                    }
                    $desc = $self->_inherited_description
                        ( \@related, $age, $desc);
                } elsif ($id =~ /(.+)\{([^\}]+)\}$/ && 
                         $self->is_namespace($gns, 'AP', 'AR')) {
                    my $pdesc = $self->description
                        ( -id => $1, -ns => $gns, -age=> $age);
                    $desc = "{Variant $2}" . ($pdesc ? " $pdesc" : "");
                } elsif (my $canon = $canonHash->{$gns}) {
                    my %found;
                    my %prefer = ( P => 1, R => 2, L => 3 );
                    my @ns2s   = sort {
                        ($prefer{substr($a, -1 , 1)} || 4) <=>
                            ($prefer{substr($b, -1 , 1)}  || 4)
                        } keys %{$canon};
                    my @related;
                    foreach my $ns2 (@ns2s) {
                        my @cas = $self->convert
                            (-id => $id,-ns1 => $gns,-ns2 => $ns2,
                             -age => $age, -warn => $doWarn);
                        if ($#cas == -1) {
                            # Maybe we are not getting related nodes because
                            # they are themselves deprecated?
                            my $nsRt1 = $bioRoot->{$gns};
                            my $nsRt2 = $bioRoot->{$ns2};
                            my $class = $self->primary_maptracker_class($ns2);
                            my $type   = $bioTypes->{$nsRt1}{$nsRt2};
                            if ($type && $class) {
                                my $struct = $self->fast_edge_hash
                                    ( -name      => $id,
                                      -keepclass => $class,
                                      -keeptype  => $type );
                                @cas = keys %{$struct};
                            }
                        }
                        # warn "$ns2 : ".(join(',', @cas) || '');
                        push @related, [$ns2, \@cas] unless ($#cas == -1);
                    }
                    $desc = $self->_inherited_description
                        ( \@related, $age, $desc);

                    if (! $desc && ($gns eq 'AR' || $gns eq 'RSR')) {
                        # Maybe it belongs to a UniGene cluster?
                        my $struct = $self->fast_edge_hash
                            ( -name      => $seq,
                              -keepclass => 'UniGene',
                              -keeptype  => 'is a sequence in cluster', );
                        $desc = $self->_inherited_description
                            ( [['UG', [keys %{$struct}]]], $age, $desc);
                        # $desc = $desc ? "EST from $desc" : "Anonymous EST";
                    }

                }
                if (!$desc && $self->is_namespace($gns, 'AP')) {
                    # For stubborn proteins, see if they are in an IPI cluster
                    my $struct = $self->fast_edge_hash
                        ( -name      => $seq,
                          -keepclass => 'IPI',
                          -keeptype  => 'is a sequence in cluster', );
                    $desc = $self->_inherited_description
                        ( [['IPI', [keys %{$struct}]]], $age, $desc);
                    unless ($desc) {
                        # Maybe it is a protein directly associated with locus?
                        my $ns2 = 'LL';
                        my @accs = $self->convert
                            ( -id => $id, -ns1 => $gns, -ns2 => $ns2,
                              -age => $age, -warn => $doWarn);
                        $desc = $self->_inherited_description
                            ( [[$ns2, \@accs]], $age, $desc);
                    }
                }
                if (!$desc && $gns =~ /^RS[RP]$/) {
                    # There are some RefSeq IDs that have had all data removed
                    my $struct = $self->fast_edge_hash
                        ( -name      => $seq,
                          -keeptype  => 'is a member of', );
                    my @sets = sort keys %{$struct};
                    my @ds;
                    foreach my $set (@sets) {
                        my $sd = $self->description
                            ( -id => $set, -ns => "SET", -age => $age );
                        push @ds, "Member of $set : $sd" if ($sd);
                    }
                    ($desc) = sort {length($a) <=> length($b)} @ds;
                }
                if ($desc && $gns =~ /^ENS[EGTP]$/) {
                    # Get rid of [Source:MGI (curated);Acc:Rbm43-002]
                    $desc =~ s/\s*\[Source:[^\]]+\]\s*/ /g;
                    $desc =~ s/\s{2,}/ /g;
                    $desc =~ s/ $//;
                }
                if ($ns eq 'MSIG') {
                    if ($id =~ /GO(_|:)(\d{7})/ ||
                        ($desc && $desc =~ /^GO(_|:)(\d{7})$/)) {
                        my $gid = "GO:$2";
                        my $mdesc = $self->description
                            (-id => $gid, -ns1 => 'GO', -age => $age);
                        $desc = "$mdesc ($gid)" if ($mdesc);
                    }
                    if ($desc) {
                        # Remove HTML tags:
                        $desc =~ s/\<[^\>]+\>//g;
                        # Standardize whitespace:
                        $desc =~ s/[\s\t\n\r]+/ /g;
                        $desc = substr($desc, 0, 4000);
                    } else {
                        warn "\n[$id]\n\n";
                    }
                }
                if (!$desc && $seq->is_class('Data Missing')) {
                    $desc = "{Known ID that lacks supporting data}";
                }
                $desc = $depToken . ($desc ? " $desc" : "") if ($isDep);

                $descs{uc($desc)} = $desc if ($desc);
            }
        }
    } else {
        $descs{$unkToken} = 1;
    }
    my $row = { term  => $id,
                ns    => $nsIn };
    my @nonredun = values %descs;
    delete $self->{RECURSIVE_DESCRIPTION} if ($isFirst);
    if ($#nonredun < 0 && $self->deep_dive &&
        $self->namespace_is_sequence($ns)) {
        # Nothing found in MapTracker, deep dive is requested, and the item
        # should be a sequence
        my $change = $self->update_maptracker_sequence( $id );
        return $self->update_GENERIC_description( $id, $ns, $age)
            if ($change);
    } elsif ($#nonredun == 0) {
        $row->{descr} = $nonredun[0];
    } else {
        # Multiple descriptions...
    }

    # Do not update the database if the description is null and we are
    # calling the function as a recursive attempt to find a related
    # description for another query.

    $self->dbh->update_rows( 'description', [ $row ] )
        if ($isFirst || ($row->{descr} && $row->{descr} ne $depToken));
    return ($row->{descr});
}

sub _inherited_description {
    my $self = shift;
    my ($related, $age, $desc) = @_;
    for my $r (0..$#{$related}) {
        my ($ns, $list) = @{$related->[$r]};
        map { s/\.\d+$// } @{$list} if ($self->namespace_is_sequence($ns));
        my %descs;
        foreach my $other (@{$list}) {
            if (my $adesc = $self->description
                ( -id => $other, -ns => $ns, -age => $age )) {
                if ($adesc =~ /^(\{Deprecated\}) (.+)/ ||
                    $adesc =~ /^(\{Deprecated\})$/) {
                    ($other, $adesc) = ("$1 $other", $2);
                    next unless ($adesc);
                }
                push @{$descs{$adesc}}, $other;
            }
        }
        my @adescs = sort keys %descs;
        unless ($#adescs == -1) {
            foreach my $adesc (@adescs) {
                $desc .= ", " if ($desc);
                $desc .= sprintf("%s [%s]", $adesc,
                                 join(',', @{$descs{$adesc}}));
            }
            last;
        }
    }
    return $desc;
}

=head2 Comment Description

  Namespaces: CDD

Some identifiers have the most useful descriptive information linked
via the 'has comment' edge type (rather than the 'is a shorter term
for' edge type). This method will recover descriptions via that edge
for the indicated namespaces.

=cut

# Entities that have most useful data in comments
*update_CDD_description = \&update_COMMENT_description;
sub update_COMMENT_description {
    my $self = shift;
    my ( $idReq, $ns, $age, $iter ) = @_;
    my $nsIn = $self->namespace_name($ns);
    my $mt   = $self->tracker;
    return "" unless ($mt);

    my ($id, $seq) = $self->fast_standardize( $idReq, $ns );
    my $row = { term  => $id,
                ns    => $nsIn };
    if ($seq) {
        my $coms  = $mt->get_edge_dump
            ( -name      => $seq,
              -orient    => 1,
              -keeptype  => "has comment" );
        my @names = map { $mt->get_seq( $_->[0] )->name } @{$coms};
        my ($shortest) = sort { length($a) <=> length($b) } @names;
        return $self->update_GENERIC_description($idReq, $ns, $age, $iter)
            unless ($shortest);
        $row->{descr} = $shortest;
    } else {
        $row->{descr} = $unkToken;
    }
    $self->dbh->update_rows( 'description', [ $row ] );
    return ($row->{descr});
}

sub update_NS_description {
    my $self = shift;
    my ( $idReq, $ns, $age, $iter ) = @_;
    my $nsIn = $self->namespace_name($ns);
    my $id   = $self->namespace_name($idReq);
    my $desc;
    if ($id) {
        my $tok = $self->namespace_token($id);
        $desc = sprintf("GenAcc namespace %s, token '%s'", $id, $tok);
        if ($idReq !~ /\s/ && length($idReq) <= 4) {
            $id = $tok;
        }
    } else {
        $desc = "$unkToken - Not a recognized namespace within GenAcc";
        $id   = $idReq;
    }
    my $row = { term  => $id,
                descr => $desc,
                ns    => $nsIn };
    
    $self->dbh->update_rows( 'description', [ $row ] );
    return ($row->{descr});
}

=head2 Probeset Description

  Namespaces: APS CLPS

Probeset descriptions are constructed from the description of the
gene(s) they represent. The description will be synthetic,
incorporating the locus description prefixed by the locus symbol, in
the form:

 [SYM] Text text text text

Loci are recovered via convert() with a minimum score of 0.8. If more
than one loci is found for a probeset, then the descriptions will be
concatenated with double vertical bars (to a maximal length of 4k
characters):

 [ABC] The ABC gene || [XYZ] Last known gene || [XXX] Naughty locus

=cut

*update_AGIL_description = \&update_PROBESET_description;
*update_APS_description  = \&update_PROBESET_description;
*update_BPS_description  = \&update_PROBESET_description;
*update_BAPS_description = \&update_PROBESET_description;
*update_CLPS_description = \&update_PROBESET_description;
*update_ILMN_description = \&update_PROBESET_description;
*update_ILMG_description = \&update_PROBESET_description;
*update_PHPS_description = \&update_PROBESET_description;
*update_TRC_description  = \&update_PROBESET_description;
my $apsGoodScore  = 0.8;
my $apsSubOptNS   = {
    RSR  => "RefSeq",
    ENST => "Ensembl Transcript",
    ENSG => "Ensembl Gene",
    AR   => "Non-standard RNA",
};
my $apsOkMidRna = { LL => 'RSR', ENSG => 'ENST' };
my $apsConsideredNS = { map { $_ => $_ } qw(RSR ENST MIRB AR) };

my $brainArrayConsistency = {
    map { $_ => 1 } qw(HG_U95A HG_U133A HG_U133_PLUS_2 
                       HG_U219 HUGENE_2_1_ST_V1),
};

# Prefer LocusLink > Ensembl Gene > RefSeq > Ensembl RNA
# Try to find one with a good or not-good-but non-zero score
# Use scored Any RNA entries if none of the above can be found.
# Finally, fall back first to explicit zero scores,
# then finally to undefined scores.

my $apsClassRank = 
    [qw(LL2 LL1 ENSG2 ENSG1 RSR2 RSR1 ENST2 ENST1 MIRB2 MIRB1 AR2 AR1
        LL-2 ENSG-2 RSR-2 ENST-2 MIRB-2 AR-2 
        LL0 ENSG0 RSR0 ENST0 MIRB0 AR0 
        LL-1 ENSG-1 RSR-1 ENST-1 MIRB-1 AR-1 )];

# So much of TRC assignments are undefined that we need to move deprecated
# entries to the end of the priority queue:
my $trcClassRank = 
    [qw(LL2 LL1 ENSG2 ENSG1 RSR2 RSR1 ENST2 ENST1 AR2 AR1
        LL0 ENSG0 RSR0 ENST0 AR0 LL-1 ENSG-1 RSR-1 ENST-1 AR-1
        LL-2 ENSG-2 RSR-2 ENST-2 AR-2)];

sub update_PROBESET_description {
    my $self = shift;
    my ( $idReq, $ns, $age ) = @_;

    my ($id, $seq) = $self->standardize_id( $idReq, $ns );
    my $desc;
    unless ($self->verify_namespace($id, $ns)) {
        my $nsn = uc($self->namespace_name($ns));
        $desc = $badToken;
        if (my $gns1 = $self->guess_namespace($id)) {
            if ($gns1 eq 'AVAR') {
                $desc = "" ;
            }
            unless ($self->{UPD_PRBSET_RECURSION}++) {
                if (my $dd = $self->description
                    ( -id => $id, -ns => $gns1, -age => $age)) {
                    my $gnsn = $self->namespace_name($gns1);
                    if ($desc) {
                        $desc .= " $dd [$gnsn]";
                    } else {
                        $desc = $dd;
                        my $row  = { term  => $id,
                                     descr => $desc,
                                     ns    => $self->namespace_name($ns) };
                        $self->dbh->update_rows( 'description', [ $row ] );
                    }
                }
                $self->{UPD_PRBSET_RECURSION} = 0;
            }
        }
        return $desc;
    }
    my $doWarn = $self->{DESCWARN};
    my ($rows, $poor, @notes, %syms);
    if ($seq) {
        return $self->update_variation_description( @_ ) 
            if ($id =~ /^SNP_[AB]-\d+$/);

        # First get all transcript assignments:
        $rows = $self->convert
            ( -id => $id, -ns1 => $ns, -age => $age, -warn => $doWarn,
              -nullscore => -1, -nonull => 1, -ns2 => 'AR');
        my $struct = $self->fast_edge_hash
            ( -name      => $id,
              -keepclass => 'Deprecated',
              -keeptype  => 'is a probe for', );
        $self->edge_ids_to_authorities( $struct );
        while (my ($dep, $auths) = each %{$struct}) {
            push @{$rows}, [$dep, '', join($authJoiner, @{$auths}), -2 ];
        }
    } else {
        $poor = $unkToken;
    }
    my $goodSc  = $ns eq 'ILMN' ? 0.98 : 0.8;
    my $goodPer = $goodSc * 100;
    if ($rows && $#{$rows} != -1) {
        # If Nosha alignments exist we will use them
        my @priNosha;
        foreach my $row (@{$rows}) {
            my $ind = 0;
            if ($row->[2] =~ /Nosha-Oligo/) {
                # This is a Nosha hit
                $ind = 1;
            } elsif ($row->[2] =~ /tilfordc/ && $row->[0] =~ /^MIMAT/) {
                # This is an internally checked miRNA hit
                $ind = 1;
            }
            push @{$priNosha[$ind]}, $row;
        }
        unless ($rows = pop @priNosha) {
            $self->err("Failure prioritizing probe sets by authority");
            return "SOFTWARE ERROR";
        }
        # Now organize by namespace and quality
        my (%nsqual, %r2l);
        foreach my $row (@{$rows}) {
            my $rna     = $row->[0];
            my ($gns)   = $self->guess_namespace($rna, 'AR');
            $gns        = $apsConsideredNS->{$gns} || 'AR';
            my $sc      = $row->[3];
            my $scClass = $sc >= $goodSc ? 2 : $sc < 0 ? $sc : $sc ? 1 : 0;
            #  2 = Good (80% or better)
            #  1 = Not good, but defined and non-zero
            #  0 = Known non-match
            # -1 = Not defined
            # -2 = Deprecated
            my $targ    = $nsqual{$gns.$scClass} ||= {
                ns   => $gns,
                sc   => $scClass,
                rows => [],
            };
            push @{$targ->{rows}}, $row;
            # Simultaneously get locus mappings from this RNA:
            my @rowCopy = @{$row};
            foreach my $lns ('LL', 'ENSG') {
                my $lrows = $r2l{$rna}{$lns} ||= $self->convert
                    ( -id => $rna, -ns1 => $gns, -ns2 => $lns,
                      -nullscore => -1,  -nonull => 1,
                      -cols => ['term_out', 'matched', 'ns_between']);
                foreach my $lr (@{$lrows}) {
                    my ($loc, $lsc, $lmid) = @{$lr};
                    # Make a copy of the row, but substitute the locus
                    my $lrow    = [ @rowCopy ];
                    # What is the (real) source namespace?
                    my ($rnaNs) = $self->guess_namespace($lrow->[0], 'AR');
                    $lrow->[0]  = $loc;
                    my $bc = $lrow->[3] =
                        ($lsc < 0 || $sc < 0) ? -1 : $lsc * $sc;
                    my $lc = $bc >= $goodSc ? 2 : $bc < 0 ? -1 : $bc ? 1 : 0;
                    my $targ    = $nsqual{$lns.$lc} ||= {
                        ns   => $lns,
                        sc   => $lc,
                        rows => [],
                        gene => 1,
                        via  => {},
                    };
                    push @{$targ->{rows}}, $lrow;
                    $targ->{via}{$loc}{$rnaNs}++;
                }
            }
        }
    
        my $ranking = $ns eq 'TRC' ? $trcClassRank : $apsClassRank;
        my $bestRows;
        for my $acr (0..$#{$ranking}) {
            last if ($bestRows = $nsqual{$ranking->[$acr]});
        }

        unless ($bestRows) {
            $self->err("Failure prioritizing probe sets by NS and Score",
                       $self->branch(\%nsqual) );
            return "SOFTWARE ERROR";
        }
        my $tns = $bestRows->{ns};
        $rows   = $bestRows->{rows};
        my $scC = $bestRows->{sc};
        if ($scC == 2) {
            # If we have the "good" score class, go ahead and add the
            # "poor" hits to the list to allow a top hit of 90% to
            # also capture 70% near-by neighbors.
            if (my $secondBest = $bestRows->{$tns.'1'}) {
                push @{$rows}, @{$secondBest->{rows}};
            }
        }

        my $worst;
        my @sorted = sort { $b->[3] <=> $a->[3] } @{$rows};
        $rows = [];
        foreach my $row (@sorted) {
            # Consider all hits within 20% of best
            my $sc = $row->[3];
            last if ($sc < $sorted[0][3] - 0.2);
            push @{$rows}, $row;
            $worst = int(0.5 + 100 * $sc);
        }

        # Begin cataloging the "poor" flags that will be added to the
        # front of the description
        my @pbits;
        if ($scC == 1) {
            if ($worst < $goodPer) {
                my $metric = $ns eq 'ILMN' ?
                    int(0.5 + (100 - $worst)/2)." mismatches" : $worst .'%';
                push @pbits, "POOR HIT $metric";
            }
        } elsif ($scC == 0) {
            push @pbits, "ZERO PROBES MATCH";
        } elsif ($scC == -1) {
            push @pbits, "UNKNOWN CONFIDENCE";
        } elsif ($scC == -2) {
            push @pbits, "DEPRECATED TARGET";
        }

        # Note if Nosha was not used to find the target:
        push @pbits, "UNVALIDATED HIT" if ($#priNosha == -1);

        my %uniq   = map { $_->[0] => 1 } @{$rows};
        my @targs  = sort keys %uniq;
        my $isGene = $tns eq 'LL' || $tns eq 'ENSG' ? 1 : 0;

        my %descs;
        if ($ns eq 'BAPS') {
            # Capture number of probes and arrays:
            my $sd = $seq->desc;
            $sd    = $sd ? $sd->name() : "";
            if ($sd =~ /\(([0-9\-]+prb([^\)]*))\)/) {
                push @notes, "BrainArray $1";
            }
            if ($id =~ /^BrAr:([^:]+):.+$/) {
                # Versioned ID, for a specific array
                if ($#notes == 0) {
                    $notes[0] .= " from $1";
                }
            } elsif ($id =~ /^BrAr:([^:]+)$/) {
                # This is an unversioned ID
                my @vers;
                foreach my $vseq ($self->_expand_unversioned_baps( $seq )) {
                    my $vid = $vseq->name();
                    if ($vid =~ /^BrAr:([^:]+):/) {
                        my $aad = $1;
                        push @vers, $vid if ($brainArrayConsistency->{$aad});
                    }
                }
                unless ($#vers == -1) {
                    my $vr = $self->convert
                        ( -id => \@vers, -ns1 => $ns, -age => $age, 
                          -warn => $doWarn, -nonull => 1, -ns2 => 'RSR',
                          -cols => ['term_in', 'term_out', 'matched'] );
                    my (%consistent, %useArrays);
                    foreach my $vrow (@{$vr}) {
                        my ($vid, $rsr, $sc) = @{$vrow};
                        push @{$consistent{$rsr}{$vid}}, $sc;
                        $useArrays{$vid}++;
                    }
                    # warn $self->branch({bait => \@vers, hits => \%consistent});
                    # Find the minimum and maximum discrepancies between arrays
                    my $max = 0; my $min = 1;
                    my @rnas = keys %consistent;
                    my @arrays = keys %useArrays;
                    while (my ($rsr, $vidH) = each %consistent) {
                        # Find all the matching scores for a given RNA
                        # across all the 'chosen' arrays
                        # (chosen defined in $brainArrayConsistency)
                        my @scores;
                        while (my ($vid, $scs) = each %{$vidH}) {
                            # For a given array, if there are more than
                            # one measurement we will take the best
                            my ($sc) = sort { $b <=> $a } @{$scs};
                            push @scores, $sc;
                        }
                        # If we are missing an array toss in a zero
                        # for it:
                        push @scores, 0 if ($#scores < $#arrays);
                        # Now find the worst and best possible consistency
                        # for this RNA
                        @scores = sort { $a <=> $b } @scores;
                        my $maxDiff = $scores[-1] - $scores[0];
                        my $minDiff = 1;
                        for my $i (1..$#scores) {
                            # compare sorted neighbors
                            my $diff = $scores[$i] - $scores[$i-1];
                            $minDiff = $diff if ($diff < $minDiff);
                        }
                        $min = $minDiff if ($min > $minDiff);
                        $max = $maxDiff if ($max < $maxDiff);
                        # warn "Min: $minDiff, Max: $maxDiff = ".join(',', @scores);
                    }
                    # Round, make percentage, and flip to uniformity rather
                    # than discrepancy
                    ($max, $min) = map { 100 - int(0.5 + 100 * $_) }
                    ($min,$max);
                    my $scTok = $min;
                    $scTok .= "-$max" if ($min < $max);
                    push @notes, sprintf
                        ("%s%% uniform over %d RNA%s",
                         $scTok, $#rnas + 1, $#rnas == 0 ? '' : 's');
                    my $ptag;
                    if ($min < 50) {
                        $ptag = "Very Inconsistent";
                    } elsif ($min < 80) {
                        $ptag = "Inconsistent";
                    }
                    if ($ptag) {
                        $ptag = "Maybe $ptag" if ($max >= 80);
                        push @pbits, $ptag;
                    }
                    # 
                }
            } else {
                push @pbits, "WEIRD ID";
            }
        }
        if ($tns eq 'MIRB') {
            # This is a microRNA hit
            my (%taxTags, %miTargs);
            my @tax = $self->convert
                ( -id  => \@targs, -ns1 => $tns,
                  -ns2 => 'TAX', -age => $age );
            foreach my $t (@tax) {
                $t =~ s/ \([^\)]+\)//g;
                my @tb = split(/\s+/, $t);
                if ($#tb > 0) {
                    $taxTags{substr($tb[0], 0, 1).substr($tb[1], 0, 2)}++;
                }
            }
            my @ttags = sort keys %taxTags;
            my $tnum = $#ttags + 1;
            
            my @rss = $self->convert
                ( -id  => \@targs, -ns1 => $tns,
                  -auth => ['tilfordc', '!SelfReferential'],
                  -age => $age, -ns2 => 'RS' );
            foreach my $s (@rss) {
                if ($s =~ /^[a-z]{3}\-(.+)$/) {
                    $miTargs{$1}++;
                }
            }
            my @mtgs = sort keys %miTargs;
            my $mnum = $#mtgs + 1;
            my @miBits;
            if ($mnum == 0) {
                push @miBits, "non-mirBase miRNA";
            } else {
                push @miBits, sprintf("%d micro RNA%s: %s", $mnum,
                                      $mnum == 1 ? '' : 's', join(', ', @mtgs));
            }
            if ($tnum == 0) {
                push @pbits, "UNKNOWN TAXA";
            } else {
                push @miBits, sprintf("%d Taxa%s: %s", $tnum,
                                      $tnum == 1 ? '' : 'e', join(' ',@ttags));
            }
            $descs{join(' | ', @miBits)}++;
            @targs = ();
        } elsif ($isGene) {
            # Did we have to move through weird RNAs to get to the locus?
            my $midNum = -1; my %midRNA;
            my $okMid  = $apsOkMidRna->{$tns} || "";
            my $tnum   = $#targs + 1;
            foreach my $loc (@targs) {
                my $via = $bestRows->{via}{$loc};
                unless ($via->{$okMid}) {
                    map { $midRNA{$_}++ } keys %{$via};
                    $midNum++;
                }
            }
            unless ($midNum == -1) {
                my @mids = sort map { $apsSubOptNS->{$_} || $_ } keys %midRNA;
                push @pbits, sprintf
                    ("%soc%s via %s",
                     $midNum == $#targs ? 'L' : 'Some l',
                     $tnum == 1 ? 'us' : 'i', join(', ', @mids));
            }
            push @pbits, sprintf("Matches %d Loci", $tnum) if ($tnum > 1);
        }
        if (my $son = $apsSubOptNS->{$tns}) {
            push @pbits, "Matches $son";
        }

        $poor = join('; ', @pbits) || "";


        my $counter = 0;
        foreach my $loc (@targs) {
            $counter++;
            if ($isGene) {
                if (my $sym = $self->best_possible_symbol
                    ($loc, $tns, 'warn short')) {
                    $syms{$sym} ||= $counter;
                } else {
                    $syms{$loc} ||= $counter;
                }
            }
            #map { $syms{ $_ || '' } ||= ++$counter }
            #$self->convert
            #    ( -id => $loc,  -ns1 => $tns, -ns2 => 'SYM', 
            #      -min => $min, -age => $age, -warn => $doWarn);
            map { $descs{ $_ || '' } ||= $counter }
            $self->description( -id => $loc, -ns => $tns, -age => $age );
        }
        delete $descs{''};
        my @d = keys %descs;
        if ($#d == -1) {
            # No descriptive text
            if ($#targs > 3) {
                $descs{ sprintf
                            ("Hits %d undescribed %ss", $#targs + 1,
                             $self->namespace_name($tns))} = 1;
            } else {
                $descs{"Hits ".join(",", @targs)." (no description)"} = 1;
            }
        }
        
        my %primary;
        my $dedges  = $self->tracker->get_edge_dump
            ( -name      => $id,
              -return    => 'object array',
              -keeptype  => 'was derived from',
              -dumpsql   => 0 );
        my (@gd, @pd);
        foreach my $re (@{$dedges}) {
            my $src = $re->other_seq( $id )->name;
            my @isPoor = $re->has_tag('Source is considered poor');
            if ($#isPoor == -1) {
                push @gd, $src;
            } else {
                push @pd, $src;
            }
        }
        if ($#gd == 0 && $#pd != -1) {
            $primary{"($gd[0] is intended primary target)"} = 1;
        }

        my @bits;
        # Symbol Warning Notes Description
        my @parts = (\%descs, \%primary);
        for my $p (0..$#parts) {
            my $hash = $parts[$p];
            next unless ($hash);
            # Remove blank components
            delete $hash->{''};
            # Sort most common components to front of list:
            my @sbits = sort {$hash->{$a}  <=> $hash->{$b}}  keys %{$hash};
            # Multiple components separated by ||
            my $bit = join(' || ', @sbits);
            next unless ($bit);
            if ($p == 0) {
                # Description block
                my $check = join(' ', @bits, $poor || '', $bit);
                if (length($check) > 500 && $#sbits > 0) {
                    # Too long to be useful
                    my $examp = shift @sbits;
                    $bit = sprintf
                        ("%d '%s' entries, for example: %s",
                         $#targs+2,$self->namespace_name($tns),$examp);
                }
            }
            push @bits, $bit;
        }
        $desc = substr(join(' ', @bits) || '',0,4000);
    } elsif ($seq) {
        $poor = "NO TARGETS";
        if (my $sd = $seq->desc) {
            $desc = $sd->name();
        }
    }
    my @dbits;
    my @usym = sort {$syms{$a}  <=> $syms{$b}}  keys %syms;
    push @dbits, '['.join(' || ', @usym).']' unless ($#usym == -1);
    push @dbits, "{$poor}" if ($poor);
    push @dbits, $desc if ($desc);
    unless ($#notes == -1) {
        my $nd = join(', ', @notes);
        $nd = "- $nd" if ($desc);
        push @dbits, $nd;
    }
    $desc = substr(join(' ', @dbits) || '', 0, 4000);
    my $row  = { term  => $id,
                 descr => $desc,
                 ns    => $self->namespace_name($ns) };
    $self->dbh->update_rows( 'description', [ $row ] );
    return $desc;
}


*update_AC_description   = \&update_CHEMICAL_description;
*update_AUR_description  = \&update_CHEMICAL_description;
*update_BMSC_description = \&update_CHEMICAL_description;
*update_SMDL_description = \&update_CHEMICAL_description;
*update_MDL_description  = \&update_CHEMICAL_description;
*update_SMI_description  = \&update_CHEMICAL_description;
*update_PUBC_description = \&update_CHEMICAL_description;
my @bmsPrefCmpd = qw(BMS BMY BMT SQ DP MCM MM);
my %bmsPC = map { $bmsPrefCmpd[$_] => $_ } (0..$#bmsPrefCmpd);
sub update_CHEMICAL_description {
    my $self = shift;
    my ( $idReq, $ns, $age, $iter ) = @_;
    my ($gns) = ($ns eq 'AC') ? $self->guess_namespace($idReq, 'AC') : ($ns);
    my $mt    = $self->tracker();
    return "" unless ($mt);

    my ($id, $seq) = $self->standardize_id( $idReq, $gns, 'verifyClass' );
    my $nsIn = $self->namespace_name($ns);
    my $row = { term  => $id,
                ns    => $nsIn };
    unless ($seq) {
        $row->{descr} = $unkToken;
        $self->dbh->update_rows( 'description', [ $row ] );
        return $unkToken;
    }
    $id = "MTID:".$seq->id if ($seq && $useMTIDformat->{$gns});
    unless ($self->verify_namespace($id, $gns)) {
        $row->{descr} = $badToken;
        $self->dbh->update_rows( 'description', [ $row ] );
        return $badToken;
    }

    my $ns2 = 'SMI';
    my @canon = $self->convert
        ( -id => $id, -ns1 => $ns, -ns2 => $ns2, -age => $age,
          -warn => $self->{DESCWARN} );
    if ($#canon == -1) {
        $ns2 = 'SEQ';
        @canon = $self->convert
            ( -id => $id, -ns1 => $ns, -ns2 => $ns2, -age => $age,
              -warn => $self->{DESCWARN} );
    }
    if ($#canon == -1) {
        $row->{descr} = "{No canonical structure associated with ID}";
    } elsif ($ns2 eq 'SEQ') {
        
    } else {
        my $ft    = $mt->get_namespace('freetext')->id;
        my $inchi = $mt->get_namespace('inchi')->id;

        # NEEDS WORK (consideration):
        # DO WE WANT FALL-OVER THAT CONSIDERS PARENTS AND/OR CHILDREN??

        my (%texts, %accs);
        foreach my $smi (@canon) {
            my $list  = $mt->get_edge_dump
                ( -name      => $smi,
                  -return    => 'object array',
                  -keeptype  => 'is a reliable alias for',
                  -tossclass => 'SMILES',
                  -keepclass => 'CHEMICAL',);
            foreach my $edge (@{$list}) {
                my $alias = $edge->other_seq( $smi );
                my $name  = $alias->name;
                my $nsid  = $alias->namespace->id();
                next if ($nsid == $inchi);
                if ($alias->is_class('IUPAC Nomenclature')) {
                    $accs{IUPAC}{$name} = 1;
                } elsif ($nsid == $ft || $name =~ /\s/) {
                    $texts{$name} = 1;
                } else {
                    my $nns = $self->guess_namespace($name, undef, 'check');
                    if ($nns) {
                        $nns = $self->namespace_name($nns);
                    } else {
                        my @classes = map { $_->name } $alias->each_class();
                        if ($#classes == 0) {
                            $nns = $classes[0];
                            $nns =~ s/Identifier/ID/;
                            if (my $tok = $self->namespace_token($nns)) {
                                unless ($self->verify_namespace($name, $tok)) {
                                    $texts{$name} = 1;
                                    next;
                                }
                            }
                        } else {
                            next;
                        }
                    }
                    if ($nns eq 'Chemical Entity' ||
                        $nns eq 'Chemical Accession') {
                        $texts{$name} = 1;
                    } else {
                        $accs{$nns}{$name} = 1;
                    }
                }
            }
        }
        my @bits;
        # Database accessions
        foreach my $dbNs 
            ('BMS Compound ID', 'Aureus ID', 'DrugBank ID',
             'PubChem ID', 'SMDL Index', 'NIH Clinical Collection',
             'MDL Identifier') {
            next unless (exists $accs{$dbNs});
            my $useId;
            my @all = keys %{$accs{$dbNs}};
            if ($dbNs eq 'BMS Compound ID') {
                my (%u, %pfx);
                foreach my $id (@all) {
                    if ($id =~ /^([A-Z ]+)\-(\d+)$/) {
                        my ($p,$n) = ($1, $2);
                        $p =~ s/\s+//g;
                        $id = sprintf("%s-%06d", $p, $n);
                        $u{$id}       = $n;
                        $pfx{$p}{$id} = $n;
                    }
                }
                foreach my $prefer ('BMS', 'BMY', 'SQ', 'BMT') {
                    if (my $h = $pfx{$prefer}) {
                        ($useId) = sort { $h->{$a} <=> $h->{$b} } keys %{$h};
                        last;
                    }
                }
                @all = sort keys %u;
            }
            ($useId) = @all unless ($useId);
            if ($useId) {
                $useId .= " (+$#all)" if ($#all);
                push @bits, $useId;
            }
            delete $accs{$dbNs};
        }

        my %clean;
        foreach my $txt (sort { $a cmp $b } keys %texts) {
            $txt    =~ s/\s*hydrochloride$/ HCl/i;
            my $len = length($txt);
            next if (!$len || $len > 25);
            if ($txt =~ /^(dl|d,l|rs|r,s)[- ](\S+)/i) {
                next if (exists $clean{uc($2)});
            }
            my $key = uc($txt);
            #my $numchar = $txt; $numchar =~ s/[^\d]+//g;
            #my $numBias = '---' x int(length($numchar) / $len);
            #$key .= ($numBias || "");
            $clean{$key} ||= {
                txt  => $txt,
                xtra => 0,
            }
        }
        foreach my $key (sort { length($b) <=> length($a) } keys %clean) {
            # Will associate R,S-Verapamil with Verapamil
            my $hack = $key;
            my $par;
            while ($hack =~ /([A-Z]{5,})/) {
                my $pkey = $1;
                $hack =~ s/$pkey//g;
                next if ($pkey eq $key);
                if (exists $clean{$pkey}) {
                    $par = $pkey;
                    last;
                }
            }
            if ($par) {
                $clean{$par}{xtra} += $clean{$key}{xtra} + 1;
                delete $clean{$key};
            }
        }
        my @tbits;
        foreach my $key (sort { length($a) <=> length($b)
                                    || $a cmp $b } keys %clean) {
            my $txt = $clean{$key}{txt};
            if (my $num = $clean{$key}{xtra}) { $txt .= " (+$num)"; }
            push @tbits, $txt;
        }
        push @bits, @tbits; # "Free Text: ".join(", ", @tbits) unless ($#tbits == -1);

        my %pri = ( 'IUPAC' => 99 );

        foreach my $nsn (sort { ($pri{$a} || 0) <=> ($pri{$b} || 0) ||
                                    lc($a) cmp lc($b) } keys %accs) {
            my @accs = keys %{$accs{$nsn}};
            if ($nsn eq 'BMSC') {
                my @pri;
                foreach my $acc (@accs) {
                    if ($acc =~ /^([A-Z])+/) {
                        push @pri, [ $bmsPC{$1} || 999, $acc];
                    }
                }
                unless ($#pri == -1) {
                    @accs = map { $_->[1] } sort
                    { $a->[0] <=> $b->[0]
                          ||  length($a->[1]) <=> length($b->[1])
                          || $a->[1] cmp $b->[1] } @pri;
                }
            } else {
                @accs = sort { length($a) <=> length($b)
                                   || $a cmp $b } @accs;
            }
            my $bit = "$nsn: " . $accs[0];
            $bit .= sprintf(" (+%d)", $#accs) if ($#accs);
            push @bits, $bit;
        }
        $row->{descr} = join(', ', @bits) unless ($#bits == -1);
    }
    $self->dbh->update_rows( 'description', [ $row ] );
    return ($row->{descr});
}


*update_SNP_description = \&update_variation_description;
*update_AVAR_description = \&update_variation_description;
sub update_variation_description {
    my $self = shift;
    my ( $idReq, $ns, $age, $iter ) = @_;
    my $mt    = $self->tracker();
    return "" unless ($mt);
    my ($id, $seq) = $self->standardize_id($idReq, $ns, 'CheckClass');
    my $nsIn = $self->namespace_name($ns);
    unless ($seq) {
        $self->dbh->update_rows( 'description', [ { term  => $id,
                                                    descr => $unkToken,
                                                    ns    => $nsIn } ] );
        return $unkToken;
    }
    my $refNS = 'LL';
    my $mapDat = $self->genomic_overlap
        ( -id => $id, -ns1 => $ns, -ns2 => $refNS, -min => 90 );
    my @descBits;
    # We will allow up to this fraction of overlap between a previously
    # allowed locus and a lower-scored hopeful:
    my $okOverlap = 0.05;
    # Also have an upper limit on the amount of overlap:
    my $okOverSize = 100;
    foreach my $loc (@{$mapDat->{locs}}) {
        my ($chr, $sLocs, $neigh, $build) = @{$loc};
        next unless ($chr);
        my $chrName = $chr;
        $chr = uc($chr);
        if ($chr =~ /^([^\.]+)\.[^\.]+\.([^\.]+)$/) {
            my ($spec, $id) = ($1, $2);
            my @sbits = map { substr($_, 0, 1) } split(/_/, lc($spec));
            $sbits[0] = uc($sbits[0]);
            if ($id =~ /^([1-4][0-9]|[1-9]|X|Y|W|Z|MT)$/) {
                $id = "Chr$id";
            }
            $chrName = join(" ", join('', @sbits), $id, $build);
        }
        my (@sL, %locPos, $lCount);
        foreach my $sLoc (sort { $a->[0] <=> $b->[0] } @{$sLocs}) {
            my ($s, $e) = @{$sLoc};
            my @lbits = $s == $e ? ($s) : ($s, $e);
            for my $b (0..$#lbits) {
                my $txt = $lbits[$b];
                my $com = "";
                while ($txt =~ /(\d+)(\d{3})$/) {
                    $com = ",$2$com";
                    $txt = $1;
                }
                $com = "$txt$com" if ($txt);
                $lbits[$b] = $com;
            }
            $locPos{join("..", @lbits)} ||= ++$lCount;
            push @sL, [$s, $e];
        }
        my $locName = "$chrName ".join('/', sort { $locPos{$a} <=> $locPos{$b} } keys %locPos);
        my $slNum  = $#sL;
        my %groups = map { $_ + 1 => [] } (0..$slNum);

        # Multiple related genes can stack up in the same spot. Find
        # the best hits for each region and keep only those
        my @keptNeighbors;
        my %footPrint;
        # Calculate the footprint size for each locus:
        map { $_->[5] = $_->[2] - $_->[1] + 1 } @{$neigh};
        foreach my $nd (sort { $b->[4] <=> $a->[4] ||
                                   $b->[5] <=> $a->[5] } @{$neigh}) {
            # Consider top-scored hits first (or longest top scored)
            my ($loc, $ls, $le, $str, $sc, $len) = @{$nd};
            my $keepIt = 1;
            foreach my $prior (@{$footPrint{$str} || []}) {
                my ($ps, $pe, $plen) = @{$prior};
                next unless ($pe >= $ls && $le >= $ps);
                # A previously placed locus overlaps with this one
                my @coords   = sort { $a <=> $b } ($ps, $pe, $ls, $le);
                my $overlap  = $coords[2] - $coords[1] + 1;
                my $shortest = ($plen < $len) ? $plen : $len;
                if (!$shortest || $overlap > $okOverSize ||
                    $overlap / $shortest > $okOverlap) {
                    $keepIt = 0;
                    last;
                }
            }
            next unless ($keepIt);
            # Location does not significantly overlap with others on the strand
            push @{$footPrint{$str}}, [$ls, $le, $len];
            push @keptNeighbors, $nd;
        }
        

        foreach my $nd ( sort { $a->[1] <=> $b->[1] } @keptNeighbors) {
            my ($loc, $ls, $le) = @{$nd};
            my $syms = $self->convert( -id => $loc, -ns1 => $refNS,
                                       -nonull => 1,
                                       -ns2 => 'SYM', -keepbest => 1 );
            my $sym = $#{$syms} == -1 ? '?' : $syms->[0][0] || '?';

            my @indices;
            my $prior;
            for my $l (0..$slNum) {
                my ($s, $e) = @{$sL[$l]};
                if ($ls <= $e && $le >= $s) {
                    # The SNP overlaps the locus at this location
                    push @indices, $l + 1;
                } elsif ($l < $slNum && $ls > $e && $le < $sL[$l+1][0]) {
                    # The locus is between this SNP and the next
                    push @indices, $l + 1.5;
                } elsif ($l == 0 && $le < $s) {
                    # The locus is before the first SNP
                    push @indices, $l + 0.5;
                } elsif ($l == $slNum && $ls > $e) {
                    # The locus is after the last SNP
                    push @indices, $l + 1.5;
                }
            }
            if ($#indices == -1) {
                warn "Failed to position $loc [$sym] relative to $id\n  ";
            } else {
                # We were able to put the locus overlapping a SNP
                map { push @{$groups{$_}}, $sym } @indices;
                next;
            }
        }
        my @sgroups;
        foreach my $gind (sort { $a <=> $b } keys %groups) {
            my $syms = join(' ', @{$groups{$gind}});
            if (int($gind) eq $gind) {
                $syms = $syms ? "[$syms]" : "[ ]";
            } else {
                next unless ($syms);
            }
            push @sgroups, $syms;
        }
        my $neighTxt = join(" ", @sgroups);
        # push @syms, "[ No neighbors! ]" if ($#syms == -1);
        push @descBits, join(" ", $locName, '|', $neighTxt, '|');
    }

    my $desc = join(' /OR/ ', @descBits) || "No genomic locations found";
    $self->dbh->update_rows( 'description', [ {
        term  => $id,
        descr => $desc,
        ns    => $nsIn } ] );
    return $desc;
}

sub update_GI_description {
    my $self = shift;
    my ( $idReq, $ns, $age, $iter ) = @_;
    my ($id, $seq) = $self->standardize_id( $idReq, $ns );
    my $nsIn = $self->namespace_name($ns);
    unless ($seq) {
        $self->dbh->update_rows( 'description', [ { term  => $id,
                                                    descr => $unkToken,
                                                    ns    => $nsIn } ] );
        return $unkToken;
    }
    my $isFirst = 0;
    if (my $done = $self->{RECURSIVE_DESCRIPTION}) {
        # If we are in a loop trying to find a description then leave now
        return () if ($done->{$id}++);
    } else {
        # Make a note if this is the initiating call
        $self->{RECURSIVE_DESCRIPTION} = { $id => 1 };
        $isFirst = 1;
    }
    my $desc;
    # Find the protein that the GI number references
    my @srcs = $self->convert
        ( -id => $id, -ns1 => $ns, -ns2 => 'AP', -age => $age, -nonull => 1);
    if ($#srcs == 0) {
        my $src = $srcs[0];
        $desc = $src;
        my $sdesc = $self->description
            ( -id => $src, -ns => 'AP', -age => $age);
        $desc .= " - $sdesc" if ($sdesc);
    } elsif ($#srcs != -1) {
        $desc = "Multiple proteins - ".join(", ", @srcs);
    }
    if ($isFirst) {
        delete $self->{RECURSIVE_DESCRIPTION};
        $self->dbh->update_rows( 'description', [ {
            term  => $id,
            descr => $desc,
            ns    => $nsIn } ] );
    }
    return $desc;
}

*update_SEQ_description  = \&update_SEQUENCE_description;
sub update_SEQUENCE_description {
    my $self = shift;
    my ( $idReq, $ns, $age, $iter ) = @_;
    my ($gns) = ($ns eq 'AC') ? $self->guess_namespace($idReq, 'AC') : ($ns);
    my $mt    = $self->tracker();
    return "" unless ($mt);

    my ($id, $seq) = $self->standardize_id( $idReq, $gns );
    if ($seq) {
        my $tok = $mtNs2adNs->{ uc($seq->namespace->name) };
        return if ($tok && $tok ne $gns);
    }
    $id = "MTID:".$seq->id if ($seq && $useMTIDformat->{$gns});
    return "" unless ($self->verify_namespace($id, $gns));
    
    my $isFirst = 0;
    if (my $done = $self->{RECURSIVE_DESCRIPTION}) {
        # If we are in a loop trying to find a description then leave now
        return () if ($done->{$id}++);
    } else {
        # Make a note if this is the initiating call
        $self->{RECURSIVE_DESCRIPTION} = { $id => 1 };
        $isFirst = 1;
    }

    my $nsIn = $self->namespace_name($ns);
    my ($desc);
    if ($seq) {
        my $isDep = $seq->is_class('deprecated');
        if (my $dobj = $seq->desc) {
            $desc = $dobj->name;
        } else {
            my $rsh = $self->fast_edge_hash
                ( -name      => $seq,
                  -keeptype  => 'is a reliable alias for', );
            my @rs = sort { length($a) <=> length($b) ||
                                $a cmp $b } keys %{$rsh};
            my @three = splice(@rs, 0, 3);
            $desc = join(', ', @three);
            $desc .= sprintf(" plus %d other%s", $#rs+1, $#rs == 0 ? '' : 's')
                unless ($#rs == -1);
        }
        my $dvh = $self->fast_edge_hash
                ( -name      => $seq,
                  -keeptype  => 'was derived from', );
        my @dv  = sort { length($a) <=> length($b) ||
                             $a cmp $b } keys %{$dvh};
        unless ($#dv == -1) {
            my @three = splice(@dv, 0, 3);
            my @bits;
            foreach my $did (@three) {
                my ($dns) = $self->guess_namespace($did);
                my $dvd = $self->_inherited_description
                    ( [[$dns, [$did]]], $age, "");
                push @bits, $dvd if ($dvd);
            }
            my $dtxt = "Derived from ".join(', ', @bits);
            $dtxt .= sprintf(" plus %d other%s", $#dv+1, $#dv == 0 ? '' : 's')
                unless ($#dv == -1);
            $desc = $desc ? "$desc. $dtxt" : $dtxt;
        }
        my @pars = $self->direct_parents( $id, $ns, $age );
        unless ($#pars == -1) {
            my $pdesc = $self->_inherited_description
                ( [[$ns, \@pars]], $age, $desc);
            $desc = $desc ? "$desc / $pdesc" : $pdesc;
        }
        $desc = $depToken . ($desc ? " $desc" : "") if ($isDep);
    }
    delete $self->{RECURSIVE_DESCRIPTION} if ($isFirst);
    $self->dbh->update_rows( 'description', [ {
        term  => $id,
        descr => $desc,
        ns    => $nsIn } ] )
        if ($isFirst || ($desc && $desc ne $depToken));
    return $desc;
}


=head1 Parentage Updaters

These methods populate the parentage table. The methods come in pairs,
one for building parentage from a child up, another for building from
a parent down.

=head2 Generic Parentage

  Namespaces: BTFO XONT

Generic logic for dealing with non-complex ontologies.

=cut

sub update_TAX_genealogy {
    my $self = shift;
    my ($idReq, $ns, $age, $dir, ) = @_;
    my $mt    = $self->tracker;
    my $taxa  = $self->cached_mt_taxa( $idReq );
    return () unless ($taxa);
    my $id    = $taxa->name;
    my $nsn   = $self->namespace_name($ns);
    my (@rels, $table, $qcol, $scol);
    if ($dir < 0) {
        # Finding parents from children
        ($table, $qcol, $scol) = 
            ('direct_parents', 'child', 'parent');
        my $par = $taxa->parent;
        push @rels, $par if ($par);
    } else {
        # Finding children from parents
        ($table, $qcol, $scol) = 
            ('direct_children', 'parent', 'child');
        @rels = $taxa->children();
    }
    my @tooLong;
    for my $r (0..$#rels) {
        my $name = $rels[$r]->name();
        if (length($name) > 100) {
            push @tooLong, $name;
            $name = substr($name, 0, 97). '...';
        }
        $rels[$r] = [$name];
    }
    $self->msg("[!!]","Some taxae had to be truncated to fit into database",
               @tooLong) unless ($#tooLong == -1);
    my @rows = map { {$qcol => $id,
                      $scol => $_->[0],
                      ns    => $nsn } } @rels;
    push @rows, { $qcol => $id,
                  ns    => $nsn } if ($#rows == -1);
    $self->dbh->update_rows( $table, \@rows );
    return \@rels;
}


*update_GO_genealogy   = \&update_GENERIC_genealogy;
*update_BTFO_genealogy = \&update_GENERIC_genealogy;
*update_XONT_genealogy = \&update_GENERIC_genealogy;
*update_MSIG_genealogy = \&update_GENERIC_genealogy;
*update_MESH_genealogy = \&update_GENERIC_genealogy;
sub update_GENERIC_genealogy {
    my $self = shift;
    my ($idReq, $ns, $age, $dir, ) = @_;
    my $mt    = $self->tracker;
    return () unless ($self->verify_namespace($idReq, $ns) && $mt);
    return () if ($ns eq 'MSIG' && length($idReq) > 100); # IGNORE OVERLENGTH
    my ($type, $table, $qcol, $scol, $sInd);
    if ($dir < 0) {
        # Finding parents from children
        ($type, $table, $qcol, $scol, $sInd) = 
            ('is a child of', 'direct_parents', 'child', 'parent', 1);
    } else {
        # Finding children from parents
        ($type, $table, $qcol, $scol, $sInd) = 
            ('is a parent of', 'direct_children', 'parent', 'child', 0);
    }
    my $relTag = $ns eq 'GO' ? 'GO Type' : '';
    my $nsn   = $self->namespace_name($ns);
    my ($id)  = $self->fast_standardize($idReq, $ns);
    my @bait  = $self->best_aliases( $id, $ns, $age );
    my @bseq;
    foreach my $idReq (@bait) {
        my ($id, $seq) = $self->standardize_id( $idReq, $ns);
        push @bseq, $seq;
    }
    my $edges = $mt->get_edge_dump
        ( -name      => \@bseq,
          # -orient    => 1,
          -return    => 'object array',
          -keeptype  => $type,
          -tossclass => 'deprecated', );
    
    # Convert name_ids to names, then to aliases, and remove redundancy
    my %relHash;
    foreach my $edge (@{$edges}) {
        my @seqs = $edge->seqs();
        my $oseq = $seqs[$sInd];
        my @alis = $self->best_aliases($oseq->name(), $ns, $age);
        my $rel  = "";
        if ($relTag) {
            my @rels = map { $_->valname() } $edge->has_tag($relTag);
            $rel = $rels[0] || "";
        }
        map { $relHash{$_} ||= $rel } @alis;
    }
    # my %relHash = map { $_ => 1 } map { $self->best_aliases($_, $ns, $age) }
    # map { $mt->get_seq_by_id($_->[0])->name } @{$edges};

    my (@rels, @rows);
    foreach my $node (sort keys %relHash) {
        # IGNORE OVERLENGTH
        next if ($ns eq 'MSIG' && length($node) > 100);
        my $rel = $relHash{$node};
        push @rows, {$qcol    => $id,
                     $scol    => $node,
                     relation => $rel,
                     ns       => $nsn };
        push @rels, [$node, $rel];
    }
    push @rows, { $qcol => $id,
                  ns    => $nsn } if ($#rows == -1);
    $self->dbh->update_rows( $table, \@rows );
    return \@rels;
}

*update_SMI_genealogy = \&update_CHEMISTRY_genealogy;
sub update_CHEMISTRY_genealogy {
    my $self = shift;
    my $mt    = $self->tracker;
    my ($idReq, $ns, $age, $dir, ) = @_;
    my ($id, $seqid)  = $self->fast_standardize($idReq, $ns);
    return () unless ($seqid);
    my ($type, $table, $qcol, $scol);
    if ($dir < 0) {
        # Finding parents from children
        ($type, $table, $qcol, $scol) = 
            ('is a more complex form of', 'direct_parents', 'child', 'parent');
    } else {
        # Finding children from parents
        ($type, $table, $qcol, $scol) = 
            ('is a simpler form of', 'direct_children', 'parent', 'child');
    }
    my $nsn   = $self->namespace_name($ns);
    my $edges = $mt->get_edge_dump
        ( -name      => $seqid,
          -orient    => 1,
          -keeptype  => $type,
          -keepclass => 'STSMILES', );
    my $chemKey = "MTID:$seqid";
    my @rels    = map { ["MTID:".$_->[0]] } @{$edges};
    my @rows    = map { {$qcol => $chemKey,
                         $scol => $_->[0],
                         ns    => $nsn } } @rels;
    push @rows, { $qcol => $chemKey,
                  ns    => $nsn } if ($#rows == -1);
    $self->dbh->update_rows( $table, \@rows );
    return \@rels;
}

*update_SEQ_genealogy = \&update_SEQUENCE_genealogy;
sub update_SEQUENCE_genealogy {
    my $self = shift;
    my $mt    = $self->tracker;
    my ($idReq, $ns, $age, $dir, ) = @_;
    my ($id, $seqid)  = $self->fast_standardize($idReq, $ns);

    return () unless ($seqid);
    my ($type, $table, $qcol, $scol);
    if ($dir < 0) {
        # Finding parents from children
        ($type, $table, $qcol, $scol) = 
            ('is fully contained by', 'direct_parents', 'child', 'parent');
    } else {
        # Finding children from parents
        ($type, $table, $qcol, $scol) = 
            ('fully contains', 'direct_children', 'parent', 'child');
    }
    my $nsn   = $self->namespace_name($ns);
    my $edges = $mt->get_edge_dump
        ( -name      => $seqid,
          -orient    => 1,
          -keeptype  => $type,
          -keepspace => 'Sequence', );
    my $chemKey = "MTID:$seqid";
    my @rels    = map { ["MTID:".$_->[0]] } @{$edges};
    my @rows    = map { {$qcol => $chemKey,
                         $scol => $_->[0],
                         ns    => $nsn } } @rels;
    push @rows, { $qcol => $chemKey,
                  ns    => $nsn } if ($#rows == -1);
    $self->dbh->update_rows( $table, \@rows );
    return \@rels;
}

*update_parents_for_XONT = \&update_parents_for_ONTOLOGY;
*update_parents_for_BTFO = \&update_parents_for_ONTOLOGY;
sub update_parents_for_ONTOLOGY {
    my $self = shift;
    my ($req, $ns, $age) = @_;
    my ($id, $seq) = $self->fast_standardize( $req, $ns );
    unless ($seq && $self->verify_namespace($id, $ns)) {
        return [[]];
    }
    my $hash = $self->build_parents( $id, $ns, $age );
    return $self->update_parentage( $req, $ns, $hash, 0);
}

*update_children_for_XONT = \&update_children_for_ONTOLOGY;
*update_children_for_BTFO = \&update_children_for_ONTOLOGY;
sub update_children_for_ONTOLOGY {
    my $self = shift;
    my ($req, $ns, $age) = @_;
    my ($id, $seq) = $self->fast_standardize( $req, $ns );
    unless ($seq && $self->verify_namespace($id, $ns)) {
        return [[]];
    }
    my $hash = $self->build_children( $id, $ns, $age );
    return $self->update_parentage( $req, $ns, $hash, 1);
}

=head2 Flat Parentage

  Namespaces: CDD PMID

Used for 'ontologies' that are really just flat classifier lists. The
same function is used whether starting from a 'child' or 'parent',
since all nodes are isolated (no connections between nodes).

=cut

*update_children_for_CDD  = \&update_entries_for_FLATONTOLOGY;
*update_parents_for_CDD   = \&update_entries_for_FLATONTOLOGY;
*update_children_for_LT   = \&update_entries_for_FLATONTOLOGY;
*update_parents_for_LT    = \&update_entries_for_FLATONTOLOGY;
*update_children_for_PMID = \&update_entries_for_FLATONTOLOGY;
*update_parents_for_PMID  = \&update_entries_for_FLATONTOLOGY;
sub update_entries_for_FLATONTOLOGY {
    my $self = shift;
    my ($req, $ns, $age) = @_;
    unless ($req && $self->verify_namespace($req, $ns)) {
        return [[]];
    }
    my $id     = uc($req);
    return $self->update_parentage( $id, $ns, { $id => [0] }, 0);
}

=head2 GeneOntology Parentage

  Namespaces: GO

Similar to the generic method, but also leverages go_alias() to assure
that the most current terms are being used.

=cut

sub update_parents_for_GO {
    my $self = shift;
    my ($req, $ns, $age) = @_;
    my $hash = {};
    $ns = 'GO';
    my ($id, $seq) = $self->fast_standardize( $req, $ns );
    unless ($seq && $self->verify_namespace($id, $ns)) {
        return [[]];
    }
    if ($seq) {
        if (my $ali = $self->go_alias( $id, $age )) {
            $hash = $self->build_parents( $ali, $ns, $age );
        }
    }
    return $self->update_parentage( $req, $ns, $hash, 0);
}

sub update_children_for_GO {
    my $self = shift;
    my ($req, $ns, $age) = @_;
    my $hash = {};
    $ns = 'GO';
    my ($id, $seq) = $self->fast_standardize( $req, $ns );
    unless ($id && $self->verify_namespace($id, $ns)) {
        return [[]];
    }
    if ($seq) {
        if (my $ali = $self->go_alias( $id, $age )) {
            $hash = $self->build_children( $ali, $ns, $age );
        }
    }
    return $self->update_parentage( $req, $ns, $hash, 1);
}

# Generic method that takes an ID and uses MapTracker to find
# immediate parents, and $self to find all grandparents+.
sub build_parents {
    my $self = shift;
    my ($id, $ns, $age) = @_;
    my %hash  = ( $id => [0] );
    my $mt    = $self->tracker;
    my $edges = $mt->get_edge_dump
        ( -name      => $id,
          -orient    => 1,
          -keeptype  => "is a child of",
          -tossclass => 'deprecated', );

    # We need to map over the names
    my @ontos = map { $mt->get_seq_by_id($_->[0])->name } @{$edges};
    # Then turn names to aliases
    @ontos = map { $self->go_alias($_, $age) } @ontos
        if ($self->namespace_token($ns) eq 'GO');
    # Then hash
    my %phash = map { $_ => 1 } @ontos;
    

    # $id could be included as a child if the tree is restructured, or
    # because of a child was aliased to point to $id.
    delete $phash{$id};
    my @pars = sort keys %phash;
    map { push @{$hash{$_}}, 1 } @pars;

    foreach my $par (@pars) {
        # Use standard method to recurse:
        my $parhash = $self->all_parents(-id => $par, -ns => $ns,-age => $age);
        while (my ($gpar, $dd) = each %{$parhash}) {
            # The distance to the query is parent distance plus one:
            push @{$hash{$gpar}}, $dd->[0] + 1;
        }
    }
    return \%hash;
}

# Generic method that takes an ID and uses MapTracker to find
# immediate children, and $self to find all grandchildren+.
sub build_children {
    my $self = shift;
    my ($id, $ns, $age) = @_;
    #return {} if ($self->{RECURSION} && $self->{RECURSION}{$id}++);
    my $mt    = $self->tracker;
    my $edges = $mt->get_edge_dump
        ( -name      => $id,
          -orient    => 1,
          -keeptype  => "is a parent of",
          -tossclass => 'deprecated', );
    # We need to map over the names
    my @ontos = map { $mt->get_seq_by_id($_->[0])->name } @{$edges};
    # Then turn names to aliases
    @ontos = map { $self->go_alias($_, $age) } @ontos
        if ($self->namespace_token($ns) eq 'GO');
    # Then hash
    my %khash = map { $_ => 1 } @ontos;

    # $id could be included as a child if the tree is restructured, or
    # because of a child was aliased to point to $id.
    delete $khash{$id};
    my @kids = sort keys %khash;

    my %hash = ( $id => [ 0 ] );
    map { push @{$hash{$_}}, 1 } @kids;

    foreach my $kid (@kids) {
        # Use standard method to recurse:
        my $kidhash = $self->all_children( $kid, $ns, $age );
        while (my ($gkid, $gdist) = each %{$kidhash}) {
            $gdist = $gdist->[0];
            # The distance to the query is child distance plus one:
            next unless ($gdist);
            push @{$hash{$gkid}}, $gdist + 1;
        }
    }
    return \%hash;
}

sub update_parentage {
    my $self = shift;
    my ($id, $ns, $hash, $idIsParent) = @_;
    my ($idKey, $keyB) = $idIsParent ? ('parent','child') : ('child','parent');

    my @rv;
    foreach my $term (sort keys %{$hash}) {
        # Get the smallest parentage count possible for each term
        my ($parentage) = sort { $a <=> $b } @{$hash->{$term}};
        push @rv, [ $term, $parentage ];
    }

    my $nsName = $self->namespace_name($ns);
    my @rows = map { { $idKey    => $id,
                       $keyB     => $_->[0],
                       ns        => $nsName,
                       parentage => $_->[1] } } @rv;
    if ($#rows < 0) {
        # No data whatsoever
        push @rows, { $idKey    => $id,
                      ns        => $nsName };
        @rv = ([]);
    }
    $self->dbh->update_rows( 'parentage', \@rows, [$idKey] );
    return \@rv;
}

sub finalize_conditionals {
    my $self = shift;
    my @toks = $self->all_namespace_tokens;
    my @tests;
    while (my ($ns1req, $hash) = each %{$converters}) {
        my @ns1s = ($ns1req eq 'ANY') ? @toks : ($ns1req);
        while (my ($ns2req, $meth) = each %{$hash}) {
            if ($meth =~ /^(.*)CONDITIONAL_(FROM|TO)_(\S+)/) {
                # This conversion is a request to chain through an
                # intermediate namespace, but only if the proper
                # converters are already in place.
                my ($mod, $type, $mid)  = ($1 || '', $2, [split(/[_\|]+/,$3)]);
                my @ns2s = ($ns2req eq 'ANY') ? @toks : ($ns2req);
                foreach my $ns1 (@ns1s) {
                    next if ($skipCondFrom->{$ns1} && $type eq 'FROM');
                    foreach my $ns2 (@ns2s) {
                        next if ($skipCondTo->{$ns2} && $type eq 'TO');
                        next if ($ns1 eq $ns2);
                        push @tests, [ $ns1, $mid, $ns2, $type, uc($mod) ];
                    }
                }
                # Clear the conditional entry from $converters:
                delete $hash->{$ns2req};
            }
        }
    }
    while ($#tests > -1) {
        my @retry;
        foreach my $test (@tests) {
            my ($ns1, $mid, $ns2, $type, $mod) = @{$test};
            my $matched = 0;
            foreach my $mns (@{$mid}) {
                my @pair = $type eq 'TO' ? ($mns, $ns2) : ($ns1, $mns);
                my $meth = $self->_func_for_tokens( @pair );
                $matched++ if ($meth);
            }
            if ($matched) {
                # The required conversion is available
                &_set_conv( $ns1, $ns2, 'conditional_chain');
                my @params;
                if ($mod) {
                    foreach my $bit (split(/_+/, $mod)) {
                        my ($op, $args) = split(':', $bit);
                        if ($op =~ /ALLSCORE/) {
                            my $val = $args ? [split(/[^\w]+/, $args)] : 1;
                            push @params, ( -allscore => $val );
                        } elsif ($op =~ /guess/) {
                            push @params, ( lc("-$op") => 1 );
                        } elsif ($op =~ /FLAG/) {
                            my $val = [split(/[^\w]+/, $args || '')];
                            if ($#{$val} == -1) {
                                warn "Empty -flag set for $ns1 -> $ns2";
                            } else {
                                push @params, ( -flags => $val );
                            }
                        }
                    }
                }
                $condChain->{$ns1}{$ns2} = [ $mid, \@params ];
                #warn "{$ns1}{$ns2} = ".join("|",@{$mid}) if ($ns1 eq 'APS');
            } else {
                push @retry, $test;
            }
        }
        # If no converters were found, stop recursion:
        last if ($#retry == $#tests);
        # Try again with unsuccesful pairs:
        @tests = @retry;
    }
}

sub conditional_chain {
    my $self = shift;
    my ($id, $ns2, $ns1, $age) = @_;
    my ($mid, $param) = @{$condChain->{$ns1}{$ns2}};
    my $filt1 = ($ns1 =~ /^(AR|AP)$/) ? $ns1 : undef;
    my $filt2 = ($ns2 =~ /^(AR|AP)$/) ? $ns2 : undef;
    my $any   = []; map { push @{$any}, $_ if ($_ =~ /^(AR|AP)$/) } @{$mid};
    $any      = undef if ($#{$any} < 0);
    my @rows;
    map { push @rows, $self->chain_conversions
              ( -id       => $id, 
                -chain    => [$ns1, $_, $ns2],
                # -guessin  => $filt1 ? 1 : 0,
                # -guessout => $filt2 ? 1 : 0,
                -guessany => $any,
                -age      => $age,
                @{$param}) } @{$mid};
    @rows = $self->simplify_rows( -rows   => \@rows,
                                  -show   => 0) if ($#{$mid} > 0);
    
    $self->dbh->update_rows( 'conversion', \@rows );
    return \@rows;
}

sub _standardize_cloud_requests {
    my $self = shift;
    my $isClean = shift;
    $self->benchstart;
    my @rv;
    foreach my $req (@_) {
        my $n1    = shift @{$req};
        my $ns1   = $req->[0];
        my ($gns) = $self->guess_namespace( $n1, $ns1);
        $req->[0] = $gns;
        if ($crossSpeciesNs->{$gns}) {
            if ($n1 =~ / \(.+\)$/) {
                # The entry already has a taxa appended to it
                push @rv, [$n1, @{$req}];
            } else {
                # We need to expand the entry to all possible taxa
                my @taxa  = $self->convert
                    ( -id  => $n1,   -ns1 => $gns, 
                      -ns2 => 'TAX', -age => $self->age);
                push @rv, map { ["$n1 ($_)", @{$req}] } @taxa;
                next;
            }
        } else {
            if ($gns eq 'UP') {
                my $seqid;
                ($n1, $seqid) = $self->fast_standardize( $n1, $ns1 );
                # Can we make a more specific UniProt assignment?
                if ($self->fast_class($seqid,'Swiss-Prot')) {
                    $req->[0] = 'SP';
                } elsif ($self->fast_class($seqid,'TrEMBL')) {
                    $req->[0] = 'TR';
                }
            } elsif (!$isClean) {
                # Make sure that the name is using proper capitalization
                ($n1) = $self->fast_standardize($n1, $ns1);
            }
            push @rv, [$n1, @{$req}];
        }
    }
    $self->benchend;
    return @rv;
}

*cached_clouds = \&cached_cloud;
sub cached_cloud {
    my $self  = shift;
    $self->benchstart;
    my $args  = $self->parseparams( @_ );
    my @clouds;
    my $sreq  = $args->{SEED};
    my $type  = $args->{TYPE};
    my $clean = $args->{CLEANSEED};
    my $tok   = "$type\t";
    my ($lu, $cache) = $self->_cloud_cache();
    my (%hit, @seeds);
    foreach my $seed ($self->_standardize_cloud_requests($clean, @{$sreq} )) {
        my ($n, $ns1) = @{$seed};
        if (my $ind = $lu->{"$tok\t$n\t$ns1"}) {
            # The seed exisits in a cached cloud
            push @{$hit{$ind}}, $seed;
        } else {
            # We will need to get a new cloud
            push @seeds, $seed;
        }
    }
    while (my ($ind, $s) = each %hit) {
        my $cloud = $cache->[$ind];
        $cloud->reset_seed();
        $cloud->set_seed($s);
        push @clouds, $cloud;
    }
    if ($#seeds != -1) {
        my @novel = BMS::MapTracker::AccessDenorm::Cloud->new
            ( -denorm  => $self,
              -nostand => 1,
              -age     => $args->{CLOUDAGE} || $args->{AGE},
              -seed    => \@seeds,
              -warn    => $self->{CLOUDWARN},
              -type    => $type );
        push @clouds, @novel;
        if ($#{$cache} > 100) {
            # Occasionally purge the clouds
            $self->{CLOUD_CACHE} = undef;
            ($lu, $cache) = $self->_cloud_cache();
        }
        foreach my $cloud (@novel) {
            push @{$cache}, $cloud;
            my $ind = $#{$cache};
            # Record cloud membership
            my $nodes = $cloud->all_nodes();
            while (my ($n, $ns1) = each %{$nodes}) {
                $lu->{"$tok\t$n\$ns1"} = $ind;
            }
        }
    }
    $self->benchend;
    return @clouds;
}

sub _cloud_cache {
    my $self = shift;
    my $cache = $self->{CLOUD_CACHE} ||= {
        key    => {},
        clouds => [ undef ],
    };
    return ($cache->{key}, $cache->{clouds});
}

sub bulk_best_symbol {
    my $self = shift;
    my $args = $self->parseparams( @_ );
    my $ns      = $self->namespace_token( $args->{NS} || $args->{NS1});
    my $optR    = lc($args->{OPTS} || '');
    my $idR     = $args->{ID} || $args->{IDS};
    my $iRef    = ref($idR);
    my $doTrunc = $args->{TRUNC}      ? 1 : 0;
    my $doUnof  = $args->{UNOFFICIAL} ? 1 : 0;
    my $doPoor  = $args->{POOR}       ? 1 : 0;
    my $maxLen  = $args->{MAXLEN} || 0;
    my $min     = $args->{MIN}    || $args->{MATCH};
    my $doShort = ($args->{SHORT} || $maxLen) ? 1 : 0;
    my $doWarn  = $args->{WARN};
    my $doBest  = $args->{BEST};
    my $exs     = $args->{EXPLAINSQL};

    my ($ids, $nss);
    if (!$iRef) {
        $ids = [$idR];
    } elsif ($iRef eq 'HASH') {
        $ids = [ keys %{$idR} ];
        $nss = {};
        while (my ($id, $nsR) = each %{$idR}) {
            next unless ($id);
            my $ns = $self->namespace_token($nsR) || '';
            push @{$nss->{$ns}}, $id;
        }
    } else {
        $ids = $idR;
    }
    unless ($nss) {
        if ($ns) {
            $nss->{$ns} = $ids;
        } else {
            foreach my $id (@{$ids}) {
                next unless ($id);
                my $gns = $self->guess_namespace($id) || '';
                push @{$nss->{$gns}}, $id;
            }
        }
    }

    my %locs;
    while (my ($ns, $list) = each %{$nss}) {
        if ($directSymbolNS->{$ns}) {
            foreach my $id (@{$list}) {
                next unless ($id);
                my ($loc) = $self->standardize_id($id, $ns);
                $locs{$loc}{$id} = 1;
            }
        } else {
            # Convert the terms to loci
            # Do we want to consider BOTH LL and ENSG ie:
            # keys %{$directSymbolNS} ?
            # my $dbg = "debugQueries.tsv"; open(FOOT, ">$dbg"); print FOOT join("\n", @{$list})."\n"; close FOOT; warn "QUERIES in $dbg";
            my $lRows = $self->convert
                (-id => $list, -ns1 => $ns, -ns2 => 'LL', -explainsql => $exs,
                 -min => $min, -nullscore => -1, -warn => $doWarn );
            foreach my $row (@{$lRows}) {
                my ($loc, $sc, $id) = ($row->[0], $row->[3], $row->[5]);
                next unless ($loc);
                $locs{$loc}{$id} = $sc;
            }
        }
    }
    # Now convert the discrete loci to gene symbols
    my %syms;
    my @ll    = keys %locs;
    # $args->msg("[DEBUG]","Conversion to loci done") if ($doWarn);
    # open(FOOT, ">foo4.tsv"); print FOOT join("\n", @ll)."\n"; close FOOT;
    my $sRows = $self->convert
        ( -id => \@ll, -ns1 => 'LL', -ns2 => 'SYM',
          -nullscore => -1, -warn => $doWarn, -explainsql => $exs );
    foreach my $row (@{$sRows}) {
        my ($sym, $sc, $loc) = ($row->[0], $row->[3], $row->[5]);
        next unless ($sym);
        my $psc = $syms{$loc}{$sym};
        $syms{$loc}{$sym} = $sc if (!defined $psc || $sc > $psc);
    }

    my %rv;
    while (my ($loc, $sdat) = each %syms) {
        my @ranked = sort { $b->[1] <=> $a->[1] } 
        map { [ $_, $sdat->{$_} ] } keys %{$sdat};
        my $bsc = $ranked[0][1];
        my @list;
        for my $i (0..$#ranked) {
            # Keep all symbols matching the highest score
            my ($sym, $sc) = @{$ranked[$i]};
            last if ($sc < $bsc);
            push @list, $sym;
        }
        next if ($#list == -1);
        # Shorten predicted symbols:
        map { s/\_predicted$/\?/g } @list if ($doTrunc);
        # Highlight non-official symbols
        # This was a bad idea
        # map { $_ .= '*' unless (/\?/) } @list
        #     if ($doUnof && $bsc < 0.8);
        if ($maxLen) {
            my @ok;
            map { push @ok, $_ unless (length($_) > $maxLen) } @list;
            @list = @ok;
        }
        while (my ($qry, $qsc) = each %{$locs{$loc}}) {
            my @qsyms = @list;
            # Another bad idea:
            # map { $_ .= '~' } @qsyms if ($doPoor && $qsc < 0.8);
            # my $fullSc = ($qsc == -1 || $bsc == -1) ? -1 : $qsc * $bsc;
            my $fullSc = $qsc;
            foreach my $sym (@qsyms) {
                my $psc = $rv{$qry}{$sym};
                $rv{$qry}{$sym} = $fullSc if (!defined $psc || $fullSc > $psc);
            }
        }
    }
    while (my ($qid, $syms) = each %rv) {
        my @list = keys %{$syms};
        if ($doBest && $#list > 0) {
            my @ranked = sort { $syms->{$b} <=> $syms->{$a} } @list;
            @list = ();
            my $best = $syms->{$ranked[0]};
            foreach my $sym (@ranked) {
                last if ($syms->{$sym} < $best);
                push @list, $sym;
            }
        }
        @list = sort {length($a) <=> length($b) || $a cmp $b} @list;
        @list = ($list[0]) if ($doShort);
        $rv{$qid} = \@list;
    }
    map { $rv{$_ || ""} ||= [] } @{$ids};
    return \%rv;
}

sub best_possible_symbol {
    my $self = shift;
    my ($term, $ns, $opts, $doWarn) = @_;
    my @params = ( -id => $term, -ns => $ns );
    $opts = lc($opts || '');
    if ($opts =~ /match(\d+)/) { push @params, ( -min => $1) }
    if ($opts =~ /max(\d+)/)   { push @params, ( -maxlen => $1) }
    push @params, ( -trunc      => 1 ) if ($opts =~ /trunc/);
    # push @params, ( -unofficial => 1 ) if ($opts =~ /warn/);
    # push @params, ( -poor       => 1 ) if ($opts =~ /poor/);
    push @params, ( -best       => 1 ) if ($opts =~ /best/);
    push @params, ( -short      => 1 ) if ($opts =~ /short/ ||
                                           $opts =~ /max\d+/);
    my $hash = $self->bulk_best_symbol( @params );
    my %uniq = map { $_ => 1 } map { @{$_} } values %{$hash};
    return join(',', sort keys %uniq);
}


our $cmpdNsPriority = {
    BMSC => 1,
    CAS  => 10,
    MDL  => 20,
    SMDL => 30,
    AUR  => 40,
    PUBC => 50,
    SMI  => 60,
    SEQ  => 70,
    MTID => 80,
};

sub best_compound_id {
    my $self = shift;
    my ($req, $ns) = @_;
    return "" unless ($req);
    my $r = ref($req);
    if (!$r) {
        # The user is passing an object ID, find compounds related to it
        $self->death("not yet implemented for raw query");
    } elsif ($r eq 'ARRAY') {
        # Array of IDs
        my %byNS;
        foreach my $name (@{$req}) {
            my $ns = $self->guess_namespace( $name );
            push @{$byNS{$ns}}, $name;
        }
        $req = \%byNS;
    } else {
        # Normalize namespaces to tokens
        my %norm;
        while (my ($nsn, $alis) = each %{$req}) {
            my $ns = $self->namespace_token($nsn) || "ZZZ";
            push @{$norm{$ns}}, @{$alis};
        }
        $req = \%norm;
    }
    # print "<pre>".$self->branch($req)."</pre>";
    my ($bestNs) = sort { ($cmpdNsPriority->{$a} || 999) <=>
                              ($cmpdNsPriority->{$b} || 999) || $a cmp $b }
    keys %{$req};
    my @names = @{$req->{$bestNs || ""} || []};
    return $names[0] || "" if ($#names < 1);
    if ($bestNs eq 'BMSC') {
        my %byPrfx;
        foreach my $name (@names) {
            if ($name =~ /^\s*([A-Z]+)\s*\-(\d+)/) {
                my ($p, $n) = (uc($1), $2 + 0);
                push @{$byPrfx{$p}}, [ $n, sprintf("%s-%06d", $p, $n)];
            } else {
                push @{$byPrfx{'??'}}, [ 0, $name ];
            }
        }
        my $usePrfx = "";
        for my $pi (0..$#bmsPrefCmpd) {
            my $p = $bmsPrefCmpd[$pi];
            if (exists $byPrfx{$p}) {
                $usePrfx = $p;
                last;
            }
        }
        ($usePrfx) = sort keys %byPrfx unless ($usePrfx);
        @names = map {$_->[1]} sort {$a->[0] <=> $b->[0]} @{$byPrfx{$usePrfx}};
    } else {
        @names = sort { $a cmp $b } @names;
    }
    return $names[0];
}

my $savSTH;
sub best_sequence_alignment_version {
    my $self = shift;
    unless ($savSTH) {
        my $mt = $self->tracker;
        my @tids;
        foreach my $tname ('similar','sameas','contains') {
            if (my $type = $mt->get_type($tname)) { push @tids, $type->id }
        }
        my $ttxt = join(',', @tids);
        my $sql = "SELECT sn.seqname FROM seqname sn ".
            "WHERE upper(sn.seqname) LIKE upper(?) AND EXISTS ".
            "( SELECT edge_id FROM edge ".
            "WHERE (name1 = sn.name_id OR name2 = sn.name_id) ".
            "AND type_id IN ($ttxt) )";
        $savSTH = $mt->dbi->prepare($sql);
    }
    $self->bench_start;
    my %vers;
    foreach my $req (@_) {
        next unless ($req);
        $req =~ s/\.\d+$//;
        next unless ($req);
        foreach my $vid ($savSTH->get_array_for_field( $req )) {
            if ($vid =~ /^\Q$req\E\.(\d+)$/i) {
                $req = uc($req);
                $vers{$req} = $1 if (!defined($req) || $vers{$req} < $1);
            }
        }
    }
    my @rv = map { $_ . ".$vers{$_}" } sort keys %vers;
    $self->bench_end;
    return @rv;
}

sub _mtnstext_to_mtnsid {
    my $self = shift;
    my $mtns = shift || "";
    unless ($self->{MTNSID}{$mtns}) {
        my $spid = 1;
        if ($mtns) {
            my ($ignore, $space) = $self->tracker->strip_tokens($mtns . " ");
            $spid = $space->id if ($space);
        } 
        $self->{MTNSID}{$mtns} = $spid;
    }
    return $self->{MTNSID}{$mtns};
}

sub check_version_number {
    my $self = shift;
    my $id   = shift;
    return 0 unless ($id);
    my $ns   = shift || "";
    my $vers = 0;
    if ($id =~ /^(.+)\.(\d+)$/) {
        ($id, $vers) = ($1, $2);
    }
    my $best = $self->max_version_number( $id, $ns );
    return (!$vers || $vers != $best) ? $best : 0;
}

sub max_version_number {
    my $self = shift;
    my $ucid = uc(shift || "");
    my $ns   = shift || "";
    unless (defined $self->{VERS_NUM}{$ns}{$ucid}) {
        $self->bench_start();
        my $v = 0;
        if ($ucid) {
            my $spid   = $self->_mtnstext_to_mtnsid( $mtNamespaces->{$ns} );
            my $dbi    = $self->tracker->dbi;
            my $sqlval = $dbi->clean_like_query( $ucid );
            # In the long-long-ago, before SwissProt was versioned, we made
            # our own pseudo versions. These are now problematic. So for
            # UniProt nodes, make sure that the versioned ID is classed as
            # UniProt
            my $upC    = $self->{UniProtClass} ||= 
                $self->tracker->get_authority('UniProt');
            my $sth    = ($ns eq 'SP' && $upC) ?
                $self->{GET_VERS_STH_FOR_SWISS} ||= $dbi->prepare
                ( -name => "Get all versions of an unversioned entry",
                  -sql  => "SELECT s.seqname FROM seqname s WHERE ".
                  "upper(s.seqname) LIKE ? AND s.space_id = ? AND EXISTS ".
                  "(SELECT c.class_id FROM seq_class c WHERE c.name_id = s.name_id AND c.authority_id = ".$upC->id().")",) 
                :
                # Otherwise, make sure it at least has a class. This should
                # assure that it has been loaded by the primary data source
                # loader, and is not just an ID that was transiently
                # observed
                $self->{GET_VERS_STH} ||= $dbi->prepare
                ( -name => "Get all versions of an unversioned entry",
                  -sql  => "SELECT s.seqname FROM seqname s WHERE ".
                  "upper(seqname) LIKE ? AND space_id = ? AND EXISTS ".
                  "(SELECT c.class_id FROM seq_class c WHERE c.name_id = s.name_id)",);
            my @names = $sth->get_array_for_field("$sqlval.%", $spid);
            my @vers;
            foreach my $name (@names) {
                if ($name =~ /^\Q$ucid\E\.(\d+)$/) {
                    push @vers, $1;
                }
            }
            unless ($#vers == -1) {
                @vers = sort { $a <=> $b } @vers;
                $v = $vers[-1];
            }
            # $self->msg("[VERS]", "[$ns] #$spid#$ucid >>$v<< from ".join(',', @vers));
        }
        $self->{VERS_NUM}{$ns}{$ucid} = $v;
        $self->bench_end();
    }
    return $self->{VERS_NUM}{$ns}{$ucid};
}

sub all_pubmed_for_chembio {
    my $self = shift;
    my $args  = $self->parseparams( @_ );
    my $cmpd  = $args->{CMPD} || $args->{COMPOUND} || $args->{CHEM};
    my $targ  = $args->{TARG} || $args->{TARGET}   || $args->{BIO};
    my $mt    = $self->tracker();
    return wantarray ? () : {} unless ($cmpd && $targ && $mt);
    my ($cid) = $mt->get_seq_ids( $cmpd );
    return wantarray ? () : {} unless ($cid);
    my $getEids = $self->{MT_GET_EID_STH} ||= $mt->dbi->prepare
        ( -name => "Get edge ID for two nodes",
          -sql  => "SELECT edge_id FROM edge WHERE ".
          "(name1 = ? AND name2 = ?) OR (name2 = ? AND name1 = ?)");
    
    my (@tids, @eids);
    if ( my ($tid) = $mt->get_seq_ids( $targ )) { push @tids, $tid; }
    unless ($targ =~ /\.\d+$/) {
        push @tids, $mt->get_seq_ids( "$targ.%" );
    }
    foreach my $tid (@tids) {
        push @eids, $getEids->get_array_for_field($cid, $tid, $cid, $tid);
    }
    return wantarray ? () : {}  if ($#eids == -1);
    my $getMeta = $self->{MT_GET_PMID_META_STH} ||= $mt->dbi->prepare
        ( -name => "Get references for edge IDs",
          -sql  => "SELECT s.seqname FROM edge_meta e, seqname s WHERE ".
          "e.edge_id = ? AND e.tag_id = ".
          $mt->get_seq( "#META_TAGS#Referenced In" )->id().
          " AND s.name_id = e.value_id");
    my %u;
    foreach my $eid (@eids) {
        foreach my $pmid ($getMeta->get_array_for_field( $eid )) {
            $u{$pmid} = 1 if ($pmid =~ /^PMID:\d+$/);
        }
    }
    return wantarray ? keys %u : \%u;
}

sub get_maptracker_seqid {
    my $self = shift;
    my $name = shift;
    return 0 unless ($name);
    my $obj = $self->tracker->get_seq
        ( -name => $name, -defined => 1, -nocreate => 1 );
    return $obj ? $obj->id : undef;
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
package BMS::MapTracker::AccessDenorm::Cloud;
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

use strict;
use Scalar::Util qw(weaken);
use vars qw(@ISA);
use BMS::MapTracker::Shared;

@ISA    = qw(BMS::MapTracker::Shared);



our $cloudStack;
our $fullIgnore;
our $nodeStatus;
sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = { };
    bless ($self, $class);
    my $args  = $self->parseparams( @_ );
    my $ad    = $args->{DENORM};
    $self->reset();

    my $doWarn = $self->{WARNARG} = defined $args->{WARN} ? 
        lc($args->{WARN}) : lc($ad->{CLOUDWARN} || '');
    
    my $warnAll = $doWarn =~ /all/ ? 1 : 0;
    $self->{WARN} = {
        add    => ($doWarn =~ /add/)  ? 1 : $warnAll,
        any    => $doWarn               ? 1 : $warnAll,
        build  => ($doWarn =~ /build/)  ? 1 : $warnAll,
        clean  => ($doWarn =~ /clean/)  ? 1 : $warnAll,
        del    => ($doWarn =~ /del/)    ? 1 : $warnAll,
        desc   => ($doWarn =~ /desc/)   ? 1 : $warnAll,
        detail => ($doWarn =~ /detail/) ? 1 : $warnAll,
        full   => ($doWarn =~ /full/)   ? 1 : $warnAll,
        read   => ($doWarn =~ /read/)   ? 1 : $warnAll,
        score  => ($doWarn =~ /score/)  ? 1 : $warnAll,
        nosc   => ($doWarn =~ /nosc/)   ? 1 : $warnAll,
        toss   => ($doWarn =~ /toss/)   ? 1 : $warnAll,
        write  => ($doWarn =~ /write/)  ? 1 : $warnAll,
    };

    weaken( $self->{DENORM} = $ad );
    $self->type( $args->{TYPE} );
    $self->age( $args->{AGE} || $ad->cloud_age || $ad->age );
    my @rv;
    my $isFirst = 0;
    unless ($cloudStack) {
        $cloudStack = {};
        $fullIgnore = {};
        $nodeStatus = {};
        $isFirst    = 1;
    }
    while ($doWarn =~ /(track(\S+))/i) {
        my ($rep, $id) = ($1, uc($2));
        $nodeStatus->{_track_}{$id} = 1;
        $doWarn =~ s/\Q$rep\E//g;
        $nodeStatus->{_trackNum_}++;
    }
    if (my $seed = $args->{SEED}) {
        $seed = [$seed] unless (ref($seed->[0]));
        if ($args->{NOSTAND}) {
            # The seeds have already been fully standardized
            @rv = $self->get( @{$seed} );
        } else {
            # CLEANSEED indicates that the seeds are clean
            # (proper capitalization) but still should be standardized
            my @seeds = $ad->_standardize_cloud_requests
                ($args->{CLEANSEED}, @{$seed});
            @rv = $self->get( @seeds );
        }
    } else {
        @rv = ( $self );
    }
    if ($isFirst) {
        $fullIgnore = undef;
        $nodeStatus = undef;
        $cloudStack = undef;
    }
    return @rv;
}

sub reset {
    my $self = shift;
    $self->reset_seed();
    $self->{NODES}        = [];
    $self->{EDGES}        = {};
    $self->{IDS}          = {};
    $self->{NODE_STATS}   = {};
    $self->{VALIDATED}    = {};
    $self->{IDCNT}        = 0;
    $self->{ECNT}         = 0;
    $self->{TYPE}       ||= 'Undefined';
    # Once we have decided to ignore a node, I believe it should
    # always be ignored - reasons:
    # Non-unique taxa
    # Deprecated
    # Bad version
    #$self->{IGNORE}       = {};
}

sub reset_seed {
    my $self = shift;
    $self->{SEED}     = [];
    $self->{SEEDMETA} = {};
}

sub spawn {
    my $self  = shift;
    my ($spawn) = BMS::MapTracker::AccessDenorm::Cloud->new
        ( -denorm => $self->denorm,
          -age    => $self->age,
          -warn   => $self->{WARNARG},
          -type   => $self->type );
    return $spawn;
}

sub denorm   { return shift->{DENORM}; }

sub cloud_id { return shift->{ID}; }
sub max_version_number {
    return shift->denorm->max_version_number( @_ );
}
sub age {
    my $self = shift;
    if (defined $_[0]) {
        $self->{AGE} = $self->denorm->standardize_age( $_[0] );
    }
    return $self->{AGE};
}

sub type {
    my $self = shift;
    if (my $type = $_[0]) {
        my $meth = $self->can("build_$type");
        if ($meth || $_[1]) {
            $self->{TYPE}  = $type;
            $self->{METH}  = $meth;
            $self->{VMETH} = $self->can("verify_${type}_node")
                || \&verify_generic_node;
        } else {
            $self->warn_msg("Unknown type('$type')", '!');
        }
    }
    return $self->{TYPE};
}

our $fastToken = {};
sub has_node {
    my $self = shift;
    my ($node, $ns) = @_;
    my $ucn = uc($node);
    return 0 unless ($node);
    my $hash = $self->{IDS}{uc($node)};
    return 0 unless ($hash);
    if ($ns) {
        $ns = $fastToken->{$ns} ||= $self->denorm->namespace_token($ns);
        return $hash->{$ns} || 0;
    } else {
        my ($id) = sort { $a <=> $b } values %{$hash};
        return $id;
    }
}

sub gnc {
    return shift->denorm->guess_namespace_careful( @_ );
}

sub gnvc {
    return shift->denorm->guess_namespace_very_careful( @_ );
}

sub id4node {
    my $self = shift;
    my ($node, $ns) = @_;
    return 0 unless ($node);
    my $ucn = uc($node);
    if ($ns) {
        $ns = $self->denorm->namespace_token($ns);
    } else {
        #if (my $hash = $self->{IDS}{$ucn}) {
        #    # This name already encountered. Pick one value and return
        #    #warn "Multiple namespaces for $node:\n".$self->branch($hash) if ($ucn eq 'Q5MH36');
        #    my ($id) = sort { $a <=> $b } values %{$hash};
        #    return $id;
        #} else {
        #    $ns = $self->gnc( $node ) || '';
        #}
        my $stat = $self->_seq_node_status( $node, $ns, "New Node" );
        $ns = $stat->{ns};
    }
    $self->death("Weird NS '$_[1]' for $node") unless ($ns);
    unless ($self->{IDS}{$ucn}{$ns}) {
        my $id  = $self->{IDS}{$ucn}{$ns} = ++$self->{IDCNT};

        # All nodes should either be recovered directly from
        # MapTracker, or should have come in as standardized seeds -
        # so we should not need to standardize them.

        # $noStnd indicates that the node capitalization is already standard
        # ($node) = $self->denorm->fast_standardize($node, $ns) unless ($noStnd);
        $self->{NODES}[$id] = [ $node, $ns ];
        $self->{NODE_STATS}{byNS}{$ns}++;
        $self->{NODE_STATS}{byNS}{ALL}++;
        if ($ns =~ /^(LL|ENSG)$/) {
            my $taxa = $self->_nice_taxa( $node, $ns );
            $self->{NODE_STATS}{byTaxa}{$taxa}{$ns}++;
        }
        if ($nodeStatus->{_track_}{$ucn}) {
            $self->warn_msg
                ("TRACK: $node [$ns] given ID $id", 'o', 2)
        }
    }
    return $self->{IDS}{$ucn}{$ns};
}

sub node4id {
    my $self = shift;
    my ($id) = @_;
#    if ($id > $#{$self->{NODES}}) {
#        warn $self->to_text;
#        $self->death("ID $id outside normal range for $self");
#    }
    return wantarray ? @{$self->{NODES}[$id]} : $self->{NODES}[$id][0];
}

sub all_node_ids {
    return (1..shift->{IDCNT});
}

sub each_connected_id {
    my $self  = shift;
    my ($id1) = @_;
    my @rv = $id1 && $self->{EDGES}{$id1} ? keys %{$self->{EDGES}{$id1}} : ();
    return wantarray ? @rv : \@rv;
}

sub all_nodes {
    my $self = shift;
    if (wantarray) {
        return map { $self->{NODES}[$_][0] } $self->all_node_ids();
    } else {
        my %rv;
        foreach my $id ($self->all_node_ids) {
            my ($name, $ns) = @{$self->{NODES}[$id]};
            $rv{$name} = $ns;
        }
        return \%rv;
    }
}

sub connect {
    my $self = shift;
    my ($n1, $n2, $matched, $auth, $ns1, $ns2) = @_;
    my ($id1, $id2) = ( $self->id4node($n1,$ns1), $self->id4node($n2,$ns2));
    if ($id1 && $id2) {
        $self->connect_ids($id1, $id2, $matched, $auth);
    }
    return ($id1, $id2);
}

my $debugID = 'BTK';
sub connect_ids {
    my $self = shift;
    my ($id1, $id2, $matched, $auth) = @_;
    return 0 if ($id1 == $id2);
    my $prior = $self->{EDGES}{$id1}{$id2};
    $matched  = -1 unless (defined $matched);
    my $novel = 0;
    if ($prior) {
        # The connection already exists
        if ($matched > $prior->[0]) {
            # This data has higher score
            $prior->[0] = $matched;
            $prior->[1] = $auth;
            $novel = 'score';
        } elsif ($matched == $prior->[0]) {
            # The score is the same but maybe we can get a better auth
            if ($auth =~ /\[RBM\]/) {
                $prior->[1] = $auth;
                $novel = 'RBM';
            }
        }
    } else {
        # First time seeing this edge
        $self->{ECNT}++;
        $self->{EDGES}{$id1}{$id2} = $self->{EDGES}{$id2}{$id1} =
            [$matched,$auth];
        $novel = 'new';
    }
    if ($novel && $self->{WARN}{add}) {
        my ($n1, $ns1) = $self->node4id($id1);
        my ($n2, $ns2) = $self->node4id($id2);
        $self->warn_msg(sprintf("%20s - %20s (%5s) %s %s", $n1, $n2,
                                $novel, $matched, $auth),'+', 2, 1);
    } elsif ($nodeStatus->{_trackNum_}) {
        my ($n1, $ns1) = $self->node4id($id1);
        my ($n2, $ns2) = $self->node4id($id2);
        if ($nodeStatus->{_track_}{uc($n1)} || $nodeStatus->{_track_}{uc($n2)}) {
            $self->warn_msg
                ("TRACK: $n1 [$ns1] connected to $n2 [$ns2]", 'o', 2);
        }
    }
    return $novel;
    # if ($self->node4id($id1) =~ /$debugID/ || $self->node4id($id2) =~ /$debugID/) { warn $self->node4id($id1) . " [$id1] + [$id2] " . $self->node4id($id2) . " = $self->{EDGES}{$id1}{$id2}[0] / $self->{EDGES}{$id1}{$id2}[1]"; }
}

sub disconnect_ids {
    my $self = shift;
    my ($id1, $id2) = @_;
    if ($self->{EDGES}{$id1}{$id2}) {
        delete $self->{EDGES}{$id1}{$id2};
        delete $self->{EDGES}{$id2}{$id1};
        $self->{ECNT}--;
        if ($nodeStatus->{_trackNum_}) {
            my ($n1, $ns1) = $self->node4id($id1);
            my ($n2, $ns2) = $self->node4id($id2);
            if ($nodeStatus->{_track_}{uc($n1)} || 
                $nodeStatus->{_track_}{uc($n2)}) {
                $self->warn_msg
                    ("TRACK: $n1 [$ns1] DISCONNECT from $n2 [$ns2]", 'o', 2);
            }
        }
    }
}

sub copy_cloud {
    my $self = shift;
    my ($source, $nucleus) = @_;
    my %reject;
    foreach my $id (1..$source->node_count) {
        my ($n, $ns) = $source->node4id($id);
        $reject{$id} = $self->is_invalid_node($n, $ns);
    }

    my %pairs;
    if ($nucleus) {
        # We are going to expand out from a given point, applying
        # filters on the way. This is done to prevent non-rejected IDs
        # from being included if they are in isolated pairs (ie, reaching
        # them from the nucleus would require transit through a rejected ID).
        my @stack = $reject{$nucleus} ? () : ($nucleus);
        my %done;
        while (my $id1 = shift @stack) {
            next if ($done{$id1}++);
            foreach my $id2 ($source->each_connected_id($id1)) {
                next if ($reject{$id2});
                my @pair = sort ($id1, $id2);
                $pairs{$pair[0]}{$pair[1]} = 1;
                push @stack, $id2;
            }
        }
    } else {
        # Take all edges that are not rejected
        foreach my $edat ($source->all_edges('nonRedun')) {
            my ($id1, $id2) = @{$edat};
            next if ($reject{$id1} || $reject{$id2});
            $pairs{$id1}{$id2} = 1;
        }
    }
    my %internalIDs;
    while (my ($id1, $id2s) = each %pairs) {
        # Convert from ID space in the source to plain text name + NS:
        my ($n1, $ns1) = $source->node4id($id1);
        foreach my $id2 (keys %{$id2s}) {
            my ($matched, $auth) = @{$source->{EDGES}{$id1}{$id2}};
            my ($n2, $ns2)       = $source->node4id($id2);
            map { $internalIDs{$_ || 0} = 1 }
            $self->connect($n1, $n2, $matched, $auth, $ns1, $ns2);
        }
    }
    return \%internalIDs;
}

sub node_count {
    return shift->{IDCNT};
}

sub edge_count {
    return shift->{ECNT};
}

*each_edge  = \&all_edges;
*every_edge = \&all_edges;
sub all_edges {
    my $self = shift;
    my ($nonredundant) = @_;
    my (%nodes, %uniqEdge, %done, @conns, @edges);

    foreach my $id1 ($self->all_node_ids) {
        # For every node in the cloud...
        if ($nonredundant) {
            map { push @edges, [ $id1, $_ ] } keys %{$self->{EDGES}{$id1}};
            next;
        }
        # Note all other nodes attached to it:
        my @links;
        map { push @links, $_;
              my $key = join(' ', sort ($id1, $_));
              $uniqEdge{$key} = 1; } keys %{$self->{EDGES}{$id1}};
        $nodes{$id1} = 1;
        $done{$id1}  = 0;
        push @conns, [$id1, \@links] unless ($#links == -1);
    }
    return @edges if ($nonredundant);

    while ($#conns != -1) {
        my @remainder;
        # Start with the nodes having fewest connections
        @conns = sort { $#{$a->[1]}    <=> $#{$b->[1]} ||
                        $done{$a->[0]} <=> $done{$b->[0]} ||
                        $a->[0]        cmp $b->[0] } @conns;
        foreach my $cdat (@conns) {
            my ($id1, $links) = @{$cdat};
            # Take a single connected node from the links list:
            my $id2 = shift @{$links};
            my $key = join(' ', sort ($id1, $id2));
            if ($uniqEdge{$key}) {
                # We have not captured this edge yet - do so:
                push @edges, [$id1, $id2];
                # Note that the edge AND node are now represented:
                delete $uniqEdge{$key};
                delete $nodes{$id1};
                $done{$id1}++;
            }
            push @remainder, [$id1, $links] unless ($#{$links} == -1);
        }
        @conns = @remainder;
    }

    # We want to make sure that every node is represented in column
    # NODE1 at least once (for querying). The following loop will add
    # a single duplicated edge for any nodes that have only appeared
    # in NODE2 so far.

    foreach my $id1 (keys %nodes) {
        if (my $data = $self->{EDGES}{$id1}) {
            my ($id2) = keys %{$data};
            push @edges, [$id1, $id2] if ($id2);
        }
    }
    return wantarray ? @edges : \@edges;
}

my $edgeHead = ['Node 1', 'NS 1', 'Node 2', 'NS 2', 'Score', 'Authority'];
sub dump_edges {
    my $self = shift;
    my @lines;
    foreach my $edat ($self->all_edges) {
        my ($id1, $id2) = @{$edat};
        my ($matched, $auth) = @{$self->{EDGES}{$id1}{$id2}};
        my ($n1, $ns1) = $self->node4id($id1);
        my ($n2, $ns2) = $self->node4id($id2);
        push @lines, [$n1, $ns1, $n2, $ns2, $matched, $auth];
    }
    @lines = sort { $b->[4] <=> $a->[4] ||
                        uc($a->[0]) cmp uc($b->[0]) } @lines;
    foreach my $id (@{$self->{SEED}}) {
        my @id2s = $self->each_connected_id($id);
        next unless ($#id2s == -1);
        # Note seeds that have no edges:
        my ($n1, $ns1) = $self->node4id($id);
        push @lines, [$n1, $ns1, '','','',''];
    }
    return $self->text_table(\@lines, $edgeHead);
    return join("", sort @lines);
}

sub edge_hash {
    my $self = shift;
    my @rows;
    my $ad = $self->denorm;
    foreach my $edat ($self->all_edge_data) {
        my ( $n1, $ns1, $matched, $auth, $n2, $ns2 ) = @{$edat};
        $n1 =~ s/ .+// if ($crossSpeciesNs->{$ns1});
        $n2 =~ s/ .+// if ($crossSpeciesNs->{$ns2});
        push @rows, { term_in  => $n1,
                      ns_in    => $ad->namespace_name($ns1),
                      term_out => $n2,
                      ns_out   => $ad->namespace_name($ns2),
                      matched  => $matched < 0 ? undef : $matched,
                      auth     => $auth };
    }
    return wantarray ? @rows : \@rows;
}

sub all_edge_data {
    my $self = shift;
    my $filt = shift;
    my @rows;
    foreach my $edat ($self->all_edges) {
        my ($id1, $id2) = @{$edat};
        my ($matched, $auth) = @{$self->{EDGES}{$id1}{$id2}};
        my ($n1, $ns1) = $self->node4id($id1);
        my ($n2, $ns2) = $self->node4id($id2);
        if ($filt) {
            next if ($filt->is_invalid_node($n1, $ns1) ||
                     $filt->is_invalid_node($n2, $ns2) ||
                     $filt->is_invalid_edge
                     ($n1, $ns1, $matched, $auth, $n2, $ns2));
        }
        push @rows, [ $n1, $ns1, $matched, $auth, $n2, $ns2 ];
    }
    return wantarray ? @rows : \@rows;
}

sub to_text {
    my $self = shift;
    my $head = sprintf
        ("%s Cloud [%d] %d nodes, %d edges\n", $self->type,
         $self->cloud_id || 0, $self->node_count, $self->edge_count);
    my $seeds = $self->each_seed();
    my $snum = $#{$seeds} + 1;
    if ($snum) {
        $head .= sprintf("%d Seed%s: %s\n", $snum, $snum == 1 ? '':'s',join
                         (', ', map { $_->[0]. ' ['.$_->[1].']'} @{$seeds}));
    }
    return $head . $self->dump_edges;
}

sub edge2txt {
    my $self = shift;
    my ($id1, $id2) = @_;
    my ($matched, $auth) = @{$self->{EDGES}{$id1}{$id2}};
    my ($n1, $ns1) = $self->node4id($id1);
    my ($n2, $ns2) = $self->node4id($id2);
    return sprintf("%30s [%4s] -- %30s [%4s] (%6s) %s", $n1, $ns1, $n2, $ns2,
                   $matched < 0 ?  'null' : $matched, $auth);
}

sub write {
    my $self  = shift;
    return if ($self->cloud_id || !$self->{METH});
    my $type  = $self->type();
    my $nodes = $self->all_nodes();
    my $ad    = $self->denorm;
    return unless ($ad->tracker);
    my $dbh   = $ad->dbh;
    my $warnD = $self->{WARN}{del};
    my $find  = $dbh->named_sth(($warnD ? "Verbosely " : "") .
                                "Find clouds for node");
    my $del   = $dbh->named_sth("Delete clouds");
    my $cid   = $self->{ID} = $dbh->nextval('cloud_seq');

    my %nsns;
    my @edges;

    my %singleSeeds = map { $_ => 1 } @{$self->{SEED}};
    foreach my $edat ($self->all_edges() ) {
        my ($id1, $id2) = @{$edat};
        delete $singleSeeds{$id1};
        my ($matched, $auth) = @{$self->{EDGES}{$id1}{$id2}};
        my ($n1, $ns1) = $self->node4id($id1);
        my ($n2, $ns2) = $self->node4id($id2);
        push @edges, { cloud_id => $cid,
                       node1    => $n1,
                       node2    => $n2,
                       ns1      => $nsns{$ns1} ||= $ad->namespace_name($ns1),
                       ns2      => $nsns{$ns2} ||= $ad->namespace_name($ns2),
                       matched  => $matched < 0 ? undef : $matched,
                       auth     => $auth,
                       type     => $type, };
    }

    foreach my $id1 (keys %singleSeeds) {
        # If there are seeds that are not present in an edge, we
        # capture them here.

        my ($n1, $ns1) = $self->node4id($id1);
        push @edges, { cloud_id => $cid,
                       node1    => $n1,
                       ns1      => $nsns{$ns1} ||= $ad->namespace_name($ns1),
                       type     => $type, };
    }
    if ($#edges == -1) {
        my $seeds = $self->each_seed;
        $self->err("Cloud results in no edges!", "$type [$cid]",
                   "Seeded from ".join
                   (" + ", map { "$_->[0] [$_->[1]]"} @{$seeds}));
    }
    
    if ($self->{WARN}{write}) {
        my $seeds = $self->each_seed;
        my ($nn, $en) = ($self->{IDCNT} || 1, $#edges + 1);
        my $msg = sprintf("%s [%d] from %20s E=%d N=%d E/N=%.1f",
                          $type, $cid, join
                          (', ', map { "$_->[0] [$_->[1]]"} @{$seeds}),
                          $en, $nn, $en / $nn);
        $self->warn_msg($msg, 'W', 2);
    }

    # Make sure the cloud is written in-toto (without partial deletion)
    $dbh->begin_work;
    $dbh->update_rows( 'cloud', \@edges );
    $dbh->end_work;

    # $ad->death("Wrote to find $type cloud [$cid] via:", join($authJoiner, $self->each_seed()));

    # We have just written the cloud to disk. Now we are going to find
    # all older clouds of the same type sharing content with our
    # cloud, and delete them.
    my %old;
    my %selfDestruct;
    my $cst = $cloudStack->{$type} ||= {};
    while (my ($node, $ns) = each %{$nodes}) {
        my $nsn = $nsns{$ns} ||= $ad->namespace_name($ns);
        $find->execute( $node, $nsn, $type );
        if (my $oid = $cst->{$ns}{$node}) {
            push @{$selfDestruct{$oid}{$ns}}, $node;
        }
        if ($nodeStatus->{_track_}{uc($node)}) {
            $self->warn_msg
                ("TRACK: $node [$ns] written to $type cloud_id = $cid", 'o', 2);
        }
        $cst->{$ns}{$node} = $cid;

        my $fr = $find->fetchall_arrayref();
        foreach my $r (@{$fr}) {
            my ($oldId, $nodeName) = @{$r};
            # $nodeName is only being captured if we are WARNing 
            if ($oldId < $cid) {
                # Only delete entries that are OLDER than ourselves:
                $old{$oldId}{$nodeName || ''}++;
            }
        }
    }
    my @sdIds = sort {$a <=> $b} keys %selfDestruct;
    unless ($#sdIds == -1) {
        # We are deleting stuff we just made
        my @msg = "[Code Error] Creation of $type Cloud $cid results in self-destruction of recently created clouds";
        my @tab = ('NS', 'ID', @sdIds, $cid);
        my %cpos = map { $tab[$_] => $_ } (0..$#tab);
        my %ids;
        foreach my $oid (@sdIds) {
            my $col = $cpos{$oid};
            while (my ($ns, $idArr) = each %{$selfDestruct{$oid}}) {
                foreach my $id (@{$idArr}) {
                    my $trg = $ids{$id} ||= [ $ns, $id ];
                    $trg->[$col] = 'X';
                }
            }
        }
        while (my ($ns, $nH) = each %{$cst}) {
            while (my ($id, $ocid) = each %{$nH}) {
                if (my $col = $cpos{$ocid}) {
                    my $trg = $ids{$id} ||= [ $ns, $id ];
                    $trg->[$col] ||= 'O';
                }
            }
        }
        my @rows = sort { $a->[0] cmp $b->[0] || 
                              $a->[1] cmp $b->[1] } values %ids;
        warn "[Code Error] Creating of $type Cloud $cid:\nSelf-destruction of recently created clouds:\n".$self->text_table( \@rows, \@tab);
    }
    my @toKill = keys %old;
    if ($warnD) {
        # User wants debugging messages about the clouds being deleted
        my $msg = "";
        my $cNum = $#toKill +1;
        $self->warn_msg("New cloud results in removal of $cNum old clouds",
                        'D+', 3) if ($cNum > 1);
        
        foreach my $oldId (@toKill) {
            my @ids = sort keys %{$old{$oldId}};
            my $idNum = $#ids + 1;
            $msg .= sprintf("Deleting [%d] via %d node%s: ",
                              $oldId, $idNum, $idNum == 1 ? '' : 's');
            if ($self->{WARN}{full}) {
                $msg .= " ". join('', map { 
                    sprintf("\n      [-] %s", $_);
                } @ids );
            } else {
                my $tail = "";
                if ($#ids > 2) {
                    @ids = splice(@ids,0,3);
                    $tail = ' ...';
                }
                $msg .= join(', ', @ids) . $tail;
                $msg .= "\n";
            }
        }
        $self->warn_msg($msg || "No clouds deleted", 'D', 3);
    } elsif ($nodeStatus->{_trackNum_}) {
        $self->warn_msg
            ("TRACK: Wrote cloud_id = $cid", '-', 2);        
    }

    # Finally, actually delete all the old clouds
    foreach my $oldId (@toKill) {
        $del->execute($oldId);
    }

    # The above mechanism does not guarantee that duplicate clouds
    # could be inserted during frenetic forked loading. It should
    # assure, however, that a cloud is inserted and deleted as an
    # atomic entitiy (ie, there should be no half-dead clouds), and
    # should assure that if duplicates are present, they will have
    # been created nearly simultaneously, so should at least reflect
    # almost identical composition.

    # If duplicates become a problem, locking can be added to this
    # process - but I would really rather not do that for performance
    # reasons.

}

*add_seed = \&set_seed;
sub set_seed {
    my $self = shift;
    foreach my $data (@_){
        unless (ref($data) && $data->[0] !~ /^ARRA/) {
            $self->death("Bad seed!", $self->branch(\@_));
        }
        my $n1  = shift @{$data};
        my $ns1 = shift @{$data};
        my $id  = $self->id4node( $n1, $ns1 );
        if ($nodeStatus->{_track_}{uc($n1)}) {
            $self->warn_msg
                ("TRACK: $n1 [$ns1] set as seed", 'o', 2);
        }
        my %existing = map { $_ => 1 } @{$self->{SEED}};
        push @{$self->{SEED}}, $id unless ($existing{$id});
        $self->{SEEDMETA}{$id} = [ @{$data} ];
    }
    return @{$self->{SEED}};
}

sub each_seed {
    my $self = shift;
    my @rv;
    foreach my $id (@{$self->{SEED}}) {
        my ($n1, $ns1) = $self->node4id($id);
        push @rv, wantarray ? $n1 : [$n1, $ns1];
    }
    return wantarray ? @rv : \@rv;
}
sub each_seed_id { return @{shift->{SEED}} }

sub seed_meta {
    my $self = shift;
    my ($n1, $ns1) = @_;
    return $self->{SEEDMETA}{$n1} || [] if ($n1 =~ /^\d+$/);
    my $id = $self->has_node($n1, $ns1);
    return $id ? $self->{SEEDMETA}{$id} || [] : [];
}

sub all_seed_meta {
    return values %{shift->{SEEDMETA}};
}

sub get {
    my $self  = shift;
    # DANGER! It is assumed that all seeds (content of @_) have already been
    # passed through _standardize_cloud_requests()
    my ($clouds, $failed) = $self->read( @_ );
    unless ($#{$failed} == -1) {
        my @built = $self->build( @{$failed} );
        push @{$clouds}, @built;
    }
    return wantarray ? @{$clouds} : $clouds;
}

sub read {
    my $self  = shift;
    $self->benchstart;
    $self->reset();
    my $type  = $self->type();
    my $ad    = $self->denorm;
    my $age   = $self->age;
    my $dbh   = $ad->dbh;
    my $sql   = "SELECT cloud_id FROM cloud WHERE ".
        "upper(node1) = upper(?) AND ns1 = ? AND type = ?";
    if (my $asql = $ad->age_filter_sql( $age, 'updated')) {
        $sql .= " AND $asql";
    } elsif ($age) {
        $self->err("Failed to calculate age SQL for '$age' days");
    }
    my $find = $dbh->prepare
        ( -sql   => $sql,
          -name  => "Read clouds by node",
          -level => 1 );
    my (%clouds, @failed, @built, %done);
    
    # CAUTION
    # It is assumed that read is ONLY called from get(), which is assumed
    # to be ONLY called from new(). This is important because seed
    # standardization will only occur in new()!!!
    foreach my $req ( @_ ) {
        my ($n1, $ns1) = @{$req};
        my @cids;
        foreach my $ns ($ad->namespace_children($ns1)) {
            my $nsn   = $ad->namespace_name($ns);
            push @cids, $find->get_array_for_field($n1, $nsn, $type);
        }
        if ($#cids == -1) {
            # Nothing found
            #foreach my $ns ($ad->namespace_children($ns1)) {
            #    my $nsn   = $ad->namespace_name($ns);
            #    warn $find->pretty_print( $n1, $nsn, $type );
            #}
            push @failed, $req;
        } else {
            # At least one cloud holds this entry
            map { push @{$clouds{$_}}, [@{$req}] } @cids;
        }
    }
    my @cids = keys %clouds;
    my $cnum = $#cids + $#failed + 2;
    my $warnR = $self->{WARN}{read};
    foreach my $cid (@cids) {
        # Get existing clouds. If only one cloud exists, we can use the
        # current object. Otherwise, we need to spawn empty clouds
        $self->warn_msg("Reading cloud_id = $cid", 'R')
            if ($warnR);
        my $targ = ($cnum == 1) ? $self : $self->spawn();
        $targ->set_seed( @{$clouds{$cid}} );
        my $read = $dbh->named_sth("Read cloud");
        $read->execute( $targ->{ID} = $cid );
        my $rows = $read->fetchall_arrayref();
        map { $targ->connect(@{$_})  } @{$rows};
        if ($nodeStatus->{_trackNum_}) {
            foreach my $row (@{$rows}) {
                my ($n1, $n2, $matched, $auth, $ns1, $ns2) = @{$row};
                if ($nodeStatus->{_track_}{uc($n1)} ||
                    $nodeStatus->{_track_}{uc($n2)}) {
                    $self->warn_msg
                        ("TRACK: $n1 Read [$ns1] -> $n2 [$ns2] cloud_id = $cid", 'o', 2);
                }
            }
        }
        push @built, $targ;
    }
    $self->benchend;
    return wantarray ? (\@built, \@failed) : \@built;
}

sub build {
    my $self = shift;
    my $meth = $self->{METH};
    my $type = $self->type;
    unless ($meth) {
        $self->warn_msg("No build logic provided clouds of type '$type'",'!');
        return wantarray ? () : undef;
    }

    $self->benchstart;
    my @built  = ();
    my $ad     = $self->denorm;
    my $warnD  = $self->{WARN}{detail};
    my $warnB  = $self->{WARN}{build};
    # CAUTION
    # All seeds (in @_) are assumed to be standardized! See read().

    # $ad->death("Failed to find $type cloud via:", join($authJoiner, map { "$_->[0] [$_->[1]]"} @_ ));

    foreach my $req ( @_ ) {
        my ($n1, $ns1) = @{$req};
        my $stat = $self->_seq_node_status($n1, $ns1, "Build");
        # ($ns1) = $self->gnc($n1, $ns1);
        # $req->[1] = $ns1;
        $ns1 = $req->[1] = $stat->{ns};
        my $targ;
        for my $b (0..$#built) {
            if ($built[$b]->has_node($n1, $ns1)) {
                $targ = $built[$b];
                last;
            }
        }

        if ($targ) {
            # The ID is already in a target
            $targ->set_seed( $req );
            next;
        } else {
            # The request does not exist in any of the existing
            # clouds, use a new one - use $self if it is the first
            $self->warn_msg("$type using $n1 [$ns1]",'B') if ($warnB);
            $targ = ($#built == -1) ? $self : $self->spawn();
            $targ->{FIRST_SEED}  = $req;
            $targ->{BUILD_LEVEL} = 0;
            $targ->doBuild();
            $targ->clean();
            $targ->write();
            push @built, $targ;
        }
    }
    # map { $_->to_text()."\n" } @built if ($warnD);
    # map { $_->clean(); $_->write(); } @built;
    $self->benchend;
    return wantarray ? @built : \@built;
}

sub filter_edge_rbm {
    my $self = shift;
    my $minRBM = $self->{minRBM};
    return 0 unless ($minRBM);
    # ($n1, $ns1, $sc, $auth, $n0, $ns0) = @_;
    my ($edge) = @_;
    return 0 unless ($edge->[3] =~ /\[RBM\]/);
    my $sc = $edge->[2];
    return "RBM edge undefined" unless (defined $sc);
    return "RBM edge $sc < $minRBM" if ($sc < $minRBM);
    return 0;
}

my $maxLociInGroup = 3;
our $buildLimits = {
    ProteinCluster => {
        bump    => 100,
        level   => {
            1 => {
                nsToss => [ qw(TR UPC) ],
            },
            2 => {
                nsKeep => [ qw(RSP ENSP SP NRDB IPI MANK BMSS AMBS) ],
            },
        },
    },
    TranscriptCluster => {
        bump    => 100,
        level   => {
            1 => {
                nsKeep => [ qw(RSR ENST UG) ],
            },
            2 => {
                nsKeep => [ qw(RSR ENST) ],
                nsToss => [ qw(UG) ],
            },
        },
    },
    GeneCluster => {
        bump    => { LL   => $maxLociInGroup, 
                     ENSG => $maxLociInGroup,
                     ALL  => 100 },
        level   => {
            1 => {
                nsToss => [ qw(TR UPC) ],
            },
            2 => {
                # Loci, RefSeq, Ensembl, Swiss-Prot, NRDB
                nsKeep => [ qw(AL RSR ENST RSP ENSP SP NRDB MANK BMSS AMBS) ],
            },
            3 => {
                # Loci, RefSeq, Ensembl, Swiss-Prot
                nsKeep => [ qw(AL RSR ENST RSP ENSP NRDB SP MANK BMSS AMBS) ],
                nsToss => [ 'UG' ],
            },
            4 => {
                # Loci, RefSeq, Ensembl
                nsKeep => [ qw(AL RSR ENST RSP ENSP NRDB MANK BMSS AMBS) ],
                nsToss => [ 'UG' ],
            },
        },
    },
    OrthologCluster => {
        # bump    => { ALL => 400 },
        taxBump  => $maxLociInGroup,
        minRBM   => 0,
        autoBump => sub {
            my ($self) = @_;
            if (!$self->{minRBM}) {
                $self->{minRBM} = 0.30;
            } else {
                if ($self->{minRBM} >= 0.99) {
                    $self->{minRBM} = 0.99;
                    $self->{EXHAUSTED_FILTER} = 1;
                } elsif ($self->{minRBM} >= 0.95) {
                    $self->{minRBM} += 0.01;
                } elsif ($self->{minRBM} >= 0.85) {
                    $self->{minRBM} += 0.02;
                } else {
                    $self->{minRBM} += 0.05;
                }
            }
            $self->{FILTER} ||= {};
            push @{$self->{FILTER}{EDGE}}, \&filter_edge_rbm;
            return 0;
        },
    },
};

sub is_invalid_node {
    my $self = shift;
    my ($name, $ns) = @_;
    unless (defined $self->{VALIDATED}{$ns}{$name}) {
        $self->{VALIDATED}{$ns}{$name} = $self->{VMETH} ? 
            &{$self->{VMETH}}($self, $name, $ns) : 0;
    }
    return $self->{VALIDATED}{$ns}{$name};
}

sub is_invalid_edge {
    my $self = shift;
    if ($self->{FILTER} && $self->{FILTER}{EDGE}) {
        my @cbs  = @{$self->{FILTER}{EDGE}};
        my @args = @_;
        for my $c (0..$#cbs) {
            if (my $why = &{$cbs[$c]}( $self, \@args )) { return $why; }
        }
    }
    return $self->{INVALID_EDGE}{$_[0]}{$_[4]}
    if ($_[0] && $_[4] && $self->{INVALID_EDGE}{$_[0]}{$_[4]});
    return 0;
}

sub invalidate_edge {
    my $self = shift;
    my ($n1, $n2, $why) = @_;
    return $self->{INVALID_EDGE}{$n1}{$n2} = $self->{INVALID_EDGE}{$n2}{$n1} = $why;
}

sub invalidate_node {
    my $self = shift;
    my ($name, $ns, $why, $pad) = @_;
    $pad = $pad ? 2 + $pad : 2;
    $self->{DEPSTATE}{$ns}{$name} = $why;
    $self->warn_msg("$name [$ns] : Explicit invalidation : $why",'V', $pad)
        if ($self->{WARN}{toss});
    if ($nodeStatus->{_track_}{uc($name)}) {
        $self->warn_msg
            ("TRACK: $name [$ns] invalidated ($why)", 'o', 2);
    }
    return $why;
}

sub verify_generic_node {
    my $self = shift;
    my ($name, $ns) = @_;
    my $stat = $self->_seq_node_status($name, $ns, "Generic Verify");
    $ns = $stat->{ns};
    unless (defined $self->{DEPSTATE}{$ns}{$name}) {
        $self->{DEPSTATE}{$ns}{$name} = 0;
        if (my $toss = $stat->{toss}) {
            $self->invalidate_node($name, $ns, $toss);
        }
    }
    if (my $why = $self->{DEPSTATE}{$ns}{$name}) { return $why; }
    if (my $filter = $self->{FILTER}) {
        if (my $nsFilt = $filter->{NS}) {
            if ($filter->{Toss}{$ns} ) {
                return "Excluded namespace";
            } elsif ($filter->{Keep} && !$filter->{Keep}{$ns} ) {
                return "Not required namespace";
            }
        }
    }
    return 0;
}

sub verify_OrthologCluster_node {
    my $self = shift;
    my $rv   = $self->verify_generic_node( @_ );
    return $rv if ($rv);
    my ($name, $ns) = @_;
    unless (defined $self->{OrthCheck}{$ns}{$name}) {
        $self->{OrthCheck}{$ns}{$name} = 0;
        if ($ns eq 'ENSG') {
            my $ad = $self->denorm;
            my ($check, $seqID) = $ad->fast_standardize( $name, $ns );
            if ($ad->fast_class($seqID,'suspicious')) {
                $self->invalidate_node
                    ($ns, $name, $self->{OrthCheck}{$ns}{$name} = 
                     "Suspicious Ensembl Gene");
            }
        }
    }
    if (my $why = $self->{OrthCheck}{$ns}{$name}) { return $why; }
    return 0;
}

sub doBuild {
    my $self    = shift;
    my $meth    = $self->{METH};
    $self->reset();
    my @seeds   = $self->set_seed( [@{$self->{FIRST_SEED}}] );
    my $type    = $self->type;
    my $ad      = $self->denorm;
    my $warnT   = $self->{WARN}{toss};
    my $warnD   = $self->{WARN}{detail};
    my $warnX   = $self->{WARN}{desc};


    if (my $err = $self->prepare()) {
        $self->warn_msg($err, '!') if ($warnT);
        return $self;
    }
    
    # At the very beginning, there should be no filters applied
    $self->{FILTER} = undef;

    my $ttxt = $type; $ttxt =~ s/cluster//i;

    # In an attempt to reach bump limits faster, I am prioritizing the
    # capture of certain namespaces first.
    my @stackPriority = qw(LL ENSG RSR ENST RSP ENSP SP IPI);
    my %priPos = map { $stackPriority[$_] => $_ + 1 } (0..$#stackPriority);
    push @stackPriority, '';

    $self->{DATA_QUEUE} = { map { $_ => [] } @stackPriority };
    foreach my $seed (@seeds) {
        my ($seedN, $seedNS) = $self->node4id( $seed );
        # my $mtClass = $ad->primary_maptracker_class($seedNS);
        #my ($check, $checkSeq) = $ad->standardize_id
        #    ( $seedN, $seedNS , 'verifyClass' );
        #unless ($checkSeq) {
        unless ($ad->class_matches_namespace( $seedN, $seedNS )) {
            # We need the seed namespace to validate. Otherwise it may
            # not be recoverable by MapTracker queries when it is not
            # used as a query
            $self->invalidate_node
                ( $seedN, $seedNS, "Namespace does not validate");
        }


        # VERY IMPORTANT #
        # We must validate every seed node (should be just one?), otherwise
        # we can seed a cloud with a deprecated or filtered node.
        if (my $why = $self->is_invalid_node($seedN, $seedNS)) {
            $self->warn_msg
                ("$seedN [$seedNS] : Failed validation : $why", 'V', 2)
                if ($warnT);
            return $self;
        }
        $self->_add_to_stack( [$seedN, $seedNS] );
    }
    my $cloudNote  = $self->{BUILDMETA}{CLOUDS} ||= {};


    my $limits  = $buildLimits->{$type} || {};

    if (my $tb = $limits->{taxBump}) {
        $self->{taxBump} = $tb;
        $self->{LIMITDATA} = $limits;
    } elsif (! $limits->{bump}) {
        $self->{LIMITDATA} = $limits;
    }
    $self->{REFILTER}  = 0;

    my %processed;
    my (@rejected, @reQueue);
    while (1) {
        my $cycleStack   = 0;
        my $newNodes     = 0;
        @reQueue         = ();
        my $rePrioritize = 0;
        for my $sp (0..$#stackPriority) {
            my $skey = $stackPriority[$sp];
            while (my $dat = shift @{$self->{DATA_QUEUE}{$skey}}) {
                # New node, Source node, Score/Auth
                my ($n1, $ns1, $n0, $ns0) = $self->confirm_edge_and_add($dat);
                if (!$n1) {
                    # We did not add anything to the cloud
                    if (defined $n1) {
                        # ... but maybe we can after we add more nodes
                        push @reQueue, $dat;
                    } else {
                        # Explicitly rejected, throw away.
                        push @rejected, $dat;
                    }
                    next;
                } elsif ($processed{$ns1}{$n1}++) {
                    # We only added a new edge, no new nodes
                    next;
                }
                # We are going to try to find new edges from this node:
                my $id1 = $self->id4node($n1,$ns1);
                $newNodes++;

                if ($warnD) {
                    my $msg = sprintf
                        ("%10s : (%4d) [%4s] %20s", $ttxt, $id1, $ns1, $n1);
                    if ($n0) {
                        $msg .= sprintf(" via %-20s [%4s]", $n0, $ns0);
                    } else {
                        $msg .= sprintf("     %-20s  %4s ","","") if ($warnX);
                    }
                    if ($warnX) {
                        my $desc  = $ad->description
                            (-id => $n1, -ns => $ns1, 
                             -nocreate => 1, -age => $veryOldAge );
                        $msg .= ' - ' .substr($desc, 0,80) if ($desc);
                    }
                    $self->warn_msg($msg, $self->{BUILD_LEVEL}, 1);
                }
                my $links = &{$meth}($self, $id1);
                if ($self->{REFILTER}) {
                    $cycleStack++;
                    $self->{REFILTER} = 0;
                    my $discarded = $self->refilter_network();
                    if ($warnD || $warnT) {
                        my $remaining = ref($discarded) ?
                            $self->_summarize_edges( $discarded ) : $discarded;
                        $self->warn_msg("Severed data: $remaining", '*', 3)
                            if ($remaining);
                    }
                    if (ref($discarded)) {
                        # Some edges are removed
                        push @reQueue, @{$discarded};
                    } else {
                        return $self;
                    }
                }
                next if ($#{$links} == -1);
                my @toStack;
                foreach my $link (@{$links}) {
                    if (ref($link) ne 'ARRAY') {
                        # Another cloud has been passed
                        my @cloudE = $link->all_edge_data(  );
                        my $ctype  = $link->type();
                        push @toStack, @cloudE;
                        # Make note for all cloud members
                        map { $cloudNote->{$_->[0]}{$_->[1]}{$ctype} = 1;
                              $cloudNote->{$_->[4]}{$_->[5]}{$ctype} = 1;
                          } @cloudE;
                        if ($warnT) {
                            if (my $enum = $#cloudE + 1) {
                                my $msg = sprintf
                                    ("Absorbing %d edges from %s cloud [%d]",
                                     $enum, $ctype, $link->cloud_id() );
                                $self->warn_msg($msg, '+', 2);
                            }
                        }
                    } else {
                        # A single potential edge
                        # $link = [ $n2, $ns2, $matched, $auth ]
                        push @toStack, [ @{$link}, $n1, $ns1 ];
                    }
                }
                # Now add the edges to the stack
                $cycleStack      += $#toStack + 1;
                foreach my $edat (@toStack) {
                    $self->_add_to_stack( $edat );
                    my $pri = $priPos{ $edat->[1] } || 999999;
                    # If we are adding namespaces with a lower priority num,
                    # we are going to want to return to the start of the
                    # stack and try again
                    $rePrioritize = 1 if ($pri <= $sp);
                }
                # If we have added new nodes
                 if ($cycleStack) {
                    if (my $severed = $self->check_bump) {
                        if (ref($severed)) {
                            push @reQueue, @{$severed};
                            last;
                        } else {
                            return $self;
                        }
                    }
                }
                if ($rePrioritize) {
                    # If we have added higher priority nodes, we should
                    # break out of this loop to assay them
                    last;
                }
            } # Loop within a prioritized namespace
            # Restart the analysis cue from beginning
            last if ($rePrioritize);
        } # Iterating over each prioritized namespace

        $cycleStack++ if ($newNodes && $#reQueue != -1);
        unless ($cycleStack) {
            # No additional nodes were added to the cloud
            my $reallyFinish = 1;
            if (0 && $self->{taxBump}) {
                # We have constrained the cloud via a maximum per-taxa
                # locus filter. Was it too restrictive?
                my ($maxNum) = sort { $b <=> $a } map { values %{$_} } values %{$self->{NODE_STATS}{byTaxa}};
                if ($maxNum > $self->{taxBump}) {
                    # Even after filtering, there are more loci for some taxa
                    # than specified by bumping

                    # BUT

                    # I can not see how to make this work such that the
                    # decision to alter the taxbump value is identical
                    # depending on the seed node used to start the cloud
                }
            }
            # Break out of the while(1) loop
            last if ($reallyFinish);
        }
        # We may have skipped over some un-attached edges, add them back:
        map { $self->_add_to_stack( $_ ) } @reQueue;
    }

    # We have finished expansion. We will finally scan through all
    # excluded edges and add in any that do not bring in new nodes
    # (that is, are connecting two existing nodes in the network)
    my @finalDiscard;
    map { push @rejected, @{$_} } values %{$self->{DATA_QUEUE}};
    push @rejected, @reQueue;
    foreach my $dat (@rejected) {
        my ($n1, $ns1, $eM, $eA, $n0, $ns0 ) = @{$dat};
        if ($self->has_node($n1, $ns1) && $n0 &&
            $self->has_node($n0, $ns0)) {
            # Both nodes are in the graph, connect them.
            $self->connect($n1, $n0, $eM, $eA, $ns1, $ns0);
        } else {
            # At least one of the nodes is not in the final graph
            push @finalDiscard, $dat;
        }
    }

    if ($warnD || $warnT) {
        $self->warn_msg(sprintf("%10s Lvl.%d: Bump counts %s (%s)",
                                $ttxt, $self->{BUILD_LEVEL}, 
                                join(" > ", @{$self->{BUMP_TRACE}}),
                                $self->{FIRST_SEED}[0]), '>', 2)
            if ($self->{BUMP_TRACE});
        if (my $xtra = $#rejected - $#finalDiscard) {
            # We were able to rescue a few edges
            $self->warn_msg(sprintf("Rescued %d low-score internal edge%s",
                                    $xtra, $xtra == 1 ? '' : 's'), '<', 2);
        }
        if (my $remaining = $self->_summarize_edges( \@finalDiscard)) {
            $self->warn_msg("Ignored information: $remaining", '*', 3);
        }
    }
    return $self;
}

sub _summarize_edges {
    my $self = shift;
    my ($edges) = @_;
    return "" if ($#{$edges} == -1);
    my @ign = ("Edges:".($#{$edges} + 1));
    my %ignored;
    foreach my $dat (@{$edges}) {
        my ($n1, $ns1, $eM, $eA, $n0, $ns0 ) = @{$dat};
        if (!$self->has_node($n1, $ns1))        {$ignored{$ns1}{$n1} = 1;}
        if ($n0 && !$self->has_node($n0, $ns0)) {$ignored{$ns0}{$n0} = 1;}
    }
    foreach my $ns (sort keys %ignored) {
        my @uniq = keys %{$ignored{$ns}};
        push @ign, sprintf("%s:%d", $ns, $#uniq + 1);
    }
    return join(', ', @ign);
}

sub check_bump {
    my $self = shift;
    return 0 if ($self->{EXHAUSTED_FILTER});
    my $limits = $self->{LIMITDATA};
    my $bl     = $self->{BUILD_LEVEL};
    return 0 unless ($limits);

    # If we are already at the last bump level, and we do not have
    # autobump set, then do nothing
    return 0 unless ($limits->{autoBump} || $limits->{level});

    my $countNow = $self->node_count;
    my @over;
    if (my $taxBump = $self->{taxBump}) {
        # We will bump if we exceed a gene count within any taxa+ns
        while (my ($taxa, $nsh) = each %{$self->{NODE_STATS}{byTaxa}}) {
            while (my ($ns, $num) = each %{$nsh}) {
                push @over, "$taxa $ns [$num]" if ($num > $taxBump);
            }
        }
    }

    my $bump = $limits->{bump};
    if (ref($bump)) {
        # We have namespace-specific limits
        while (my ($bns, $b) = each %{$bump}) {
            push @over, "$bns [$b]"
                if ($self->{NODE_STATS}{byNS}{$bns} &&
                    $self->{NODE_STATS}{byNS}{$bns} > $b);
        }
    } elsif ($bump) {
        # Simple integer bump limit; If we are still below it,
        # then keep going
        push @over, "ALL [$countNow]" if ($countNow > $bump);
    }
    # No limits were reached, keep cruising through the stack
    return 0 if ($#over == -1);

    # The cluster has gotten too big, and there are additional
    # limit parameters for the next build level
    my $warnD   = $self->{WARN}{detail};
    my $warnT   = $warnD || $self->{WARN}{toss};
    my $ad      = $self->denorm;

    if ($warnT) {
        my $ttxt = $self->type; $ttxt =~ s/cluster//i;
        $self->warn_msg(sprintf
            ("%10s Level %d: Bump limit reached for %s (%s)",
             $ttxt, $self->{BUILD_LEVEL}, join(" + ", @over),
             $self->{FIRST_SEED}[0] ), '>', 2);
    }

    $bl             = ++$self->{BUILD_LEVEL};
    $self->{FILTER} = undef;
    my $ldat = {};
    if ($limits->{level}) {
        my ($max) = sort { $b <=> $a } keys %{$limits->{level}};
        if ($max) {
            my $lev = $bl;
            if ($bl > $max) {
                $self->{EXHAUSTED_FILTER} = 1;
                $lev = $max;
            }
            $ldat = $limits->{level}{$lev};
        }
    }

    my %levelFilters;
    foreach my $f (qw(Keep Toss)) {
        if (my $nss = $ldat->{"ns$f"}) {
            my @all = $ad->namespace_children( @{$nss} );
            next if ($#all == -1);
            $levelFilters{NS}{$f} = { map { $_ => 1 } @all };
            unless ($ldat->{"class$f"}) {
                my @mtc;
                foreach my $fns (@{$nss}) {
                    my $mtc = $ad->primary_maptracker_class( $fns );
                    push @mtc, $mtc if ($mtc);
                }
                $ldat->{"class$f"} = \@mtc;
            }
            $levelFilters{MTCLASS}{$f} = [ @{$ldat->{"class$f"}} ]
                unless ($#{$ldat->{"class$f"}} == -1);
        }
    }
    while (my ($key, $hash) = each %levelFilters) {
        if (my $val = $levelFilters{$key}) {
            $self->{FILTER}     ||= {};
            $self->{FILTER}{$key} = $val;
        }
    }

    if (my $abCB = $limits->{autoBump}) {
        # An autobump routine has been defined
        if (my $why = &{$abCB}($self)) {
            # Could not apply the auto filter
            
        }
    }

    unless ($self->{FILTER}) {
        # Sadly, we have no further guidance to help us filter
        $self->{EXHAUSTED_FILTER} = 1;
        $self->warn_msg("No further filters available",'<', 2) if ($warnT);
        return 0;
    }

    # We are GO for bump
    my $discarded = $self->refilter_network();
    return $discarded unless (ref($discarded));

    push @{$self->{BUMP_TRACE}}, $countNow;
    if ($#{$discarded} == -1) {
        # Nothing happened!
        $self->warn_msg("No change in cloud status",'*',3,1) if ($warnT);
        # Try bumping again.
        return $self->check_bump();
    } elsif ($warnT) {
        my $remaining = $self->_summarize_edges( $discarded );
        $self->warn_msg("Severed data: $remaining",'*', 3, 1) if ($remaining);
    }
    # For the moment, these edges are severed from the re-built network
    # However, we may be able to add them later as additional nodes get
    # added.
    return $discarded;
}

sub refilter_network {
    my $self    = shift;
    my $warnT   = $self->{WARN}{toss};
    # Gather all edges that we have set so far:
    my @priorEdges = $self->all_edge_data();
    $self->warn_msg("Refiltering ".($#priorEdges+1)." edges",
                    '*', 2) if ($warnT);
    # Purge the cloud:
    $self->reset();
    # Re-seed:
    my @seeds = $self->set_seed( [@{$self->{FIRST_SEED}}] );
    # Verify that the seed nodes are all ok with new filters:
    my %captured;
    foreach my $seed (@seeds) {
        my ($seedN, $seedNS) = $self->node4id( $seed );
        $captured{$seedNS}{$seedN} = 1;
        if (my $why = $self->is_invalid_node($seedN, $seedNS)) {
            $self->warn_msg("$seedN [$seedNS] : Seed bump failure : $why",
                            'V', 2) if ($warnT);
            foreach my $key (keys %{$self->{DATA_QUEUE}}) {
                $self->{DATA_QUEUE}{$key} = [];
            }
            return "Seed $seedN [$seedNS] can not nucleate: $why";
        }
    }

    # Test each edge for addition back into the cloud
    my @discard;
    while ($#priorEdges != -1) {
        @discard = ();
        my $reCycle = 0;
        while (my $dat = shift @priorEdges) {
            my ($n1) = $self->confirm_edge_and_add($dat, 1);
            if ($n1) {
                # The edge was succesfully added to the network
                $reCycle++;
            } elsif (defined $n1) {
                # We failed to add the edge, but maybe we can on another cycle?
                push @discard, $dat;
            }
        }
        # If we have set at least one new edge, then make another pass
        # through those we were unable to add back
        @priorEdges = @discard if ($reCycle);
    }
    return \@discard;
}

sub confirm_edge_and_add {
    my $self = shift;
    my ($dat, $pad) = @_;
    my ($n1, $ns1, $eM, $eA, $n0, $ns0 ) = @{$dat};
    my $warnT   = $self->{WARN}{toss};
    my $warnSc  = $warnT && !$self->{WARN}{nosc};
    if (my $why = $self->is_invalid_node( $n1, $ns1 )) {
        # The new node is invalid
        $self->warn_msg("$n1 [$ns1] Discarded : $why", 'V', 2 + ($pad||0))
            if ($warnT);
        return undef;
    }
    if ($n0) {
        if (my $why = $self->is_invalid_node( $n0, $ns0 )) {
            # The old node is invalid (can happen on refilter)
            $self->warn_msg("$n0 [$ns0] Discarded : $why", 'V',
                            2 + ($pad||0)) if ($warnT);
            return undef;
        }
        # The stack is defining not just a new node, but an edge
        # THis should be the case for all but the seed node
        unless ($self->has_node($n0, $ns0)) {
            # The source node has not yet been added to the cloud
            # This can happen if a level bump has occured, and we
            # are retracing the previous cloud in order to exclude
            # bump-removed nodes

            # Maybe the edge is 'backwards' - this can happen if it
            # was brought in by a whole cloud
            if ($self->has_node($n1, $ns1)) {
                # Yes, the 'new' node is actually the source
                # Flip the new / source nodes
                ($n1, $ns1, $n0, $ns0) = ($n0, $ns0, $n1, $ns1);
            } else {
                # Nope, neither node is part of cloud yet
                # Return a defined value to indicate further consideration
                $self->warn_msg("Requeuing $n1 [$ns1] + $n0 [$ns0]", '?',
                                2 + ($pad||0)) if ($warnSc);
                return 0;
            }
        }
        if (my $why = $self->is_invalid_edge
            ($n1, $ns1, $eM, $eA, $n0, $ns0)) {
            # However, the edge is failing a filter
            $self->warn_msg("$n1 [$ns1] via $n0 [$ns0] : Discarded : $why",'V',
                            2 + ($pad||0)) if ($warnT);
            return undef;
        }
        # All parts of the triple are valid, add it to the graph
        $self->connect($n1, $n0, $eM, $eA, $ns1, $ns0);
    }
    return ($n1, $ns1, $n0, $ns0);
}

sub warn_msg {
    my $self = shift;
    my ($msg, $tok, $pad, $always) = @_;
    return unless ($msg);
    return if ($self->{CARPED}{$msg}++ && !$always);
    $tok = '?' unless (defined $tok);
    $pad ||= 0;
    print STDERR sprintf("%s[%s] %s\n", "  " x $pad, $tok, $msg);
}

sub _add_to_stack {
    my $self  = shift;
    my ($data, $key) = @_;
    my $stack = $self->{DATA_QUEUE};
    $key ||= $stack->{$data->[1]} ? $data->[1] : '';
    push @{$stack->{$key}}, $data;
}

sub prepare {
    my $self = shift;
    if (my $meth = $self->can("prepare_" . $self->type)) {
        return &{$meth}($self);
    }
    return 0;
}

sub clean {
    my $self = shift;
    my $meth = $self->can("clean_" . $self->type);
    &{$meth}($self) if ($meth);
}

*prepare_TranscriptCluster = \&prepare_BioCluster;
*prepare_ProteinCluster    = \&prepare_BioCluster;
*prepare_GeneCluster       = \&prepare_BioCluster;
sub prepare_BioCluster {
    my $self  = shift;
    my %seen;
    foreach my $id ($self->each_seed_id) {
        my ($n, $ns, $taxa) = $self->name_and_taxa($id);
        $seen{$taxa} = 1;
    }
    my @distinct = keys %seen;
    if ($#distinct == 0 && $distinct[0]) {
        $self->{CLOUD_TAXA} = $distinct[0];
        return 0;
    } else {
        my $msg;
        if ($#distinct == -1 || $#distinct == 0) {
            $msg = "No taxa identified";
        } else {
            $msg = "Multiple taxa (".join(', ', @distinct).')';
        }
        return $msg . ' for '.$self->type." cloud seeded with ".
            join(", ", $self->each_seed);
    }
}

sub name_and_taxa {
    my $self = shift;
    my ($id) = @_;
    my ($n, $ns) = $self->node4id($id);
    unless ($self->{TAX_CACHE}{$ns}{$n}) {
        my $taxa;
        if ($crossSpeciesNs->{$ns} && $n =~ /^(\S+) \((.+)\)/) {
            # This is a synthetic ID that includes an accession and species
            $n    = $1;
            $taxa = $2;
        } else {
            my $stat = $self->_seq_node_status($n, $ns, "Name-and-taxa");
            $taxa    = $stat->{taxa};
        }
        $self->{TAX_CACHE}{$ns}{$n} = [$n, $ns, $taxa];
    }
    return @{$self->{TAX_CACHE}{$ns}{$n}};
}

sub _nice_taxa {
    my $self = shift;
    my ($n, $ns) = @_;
    my $key = "$n\t$ns";
    unless (defined $self->{NICE_TAXA}{$key}) {
        $self->benchstart;
        my $ad  = $self->denorm;
        # Decided not to worry about keeping taxa at same age
        # But we do need to force a reconvert if the taxa is null
        # There are entries that truly lack taxa, but more often
        # the entry has been encountered by GenAcc before taxa data have
        # been loaded into MapTracker
        my @all = $ad->convert( -id  => $n, -ns1 => $ns, -ns2 => 'TAX',
                                -age => $veryOldAge, -redonull => 1 );
        if ($#all == 0 && $all[0] !~ /^(Deleted taxa|The lineage of) /) {
            $self->{NICE_TAXA}{$key} = $all[0];
        } else {
            my @bySize = sort { length($a) <=> length($b) } @all;
            my $rescue = "";
            if (my $small = shift @bySize) {
                my $ok = 1;
                foreach my $other (@bySize) {
                    # If all the other taxae are just more wordy versions
                    # of the smallest, then keep the smallest. Designed
                    # to deal with viri, eg:

                    # Influenza A virus
                    # Influenza A virus (A/Puerto Rico/8/1934(H1N1))
                    next if ($other =~ /^\Q$small\E/);
                    $ok = 0;
                    last;
                }
                $rescue = $small if ($ok);
            }
            $self->{NICE_TAXA}{$key} = $rescue;
        }
        # $self->msg("$n [$ns] = $self->{NICE_TAXA}{$key}");
       $self->benchend;
    }
    return $self->{NICE_TAXA}{$key};
}

*build_TranscriptCluster = \&build_SequenceCluster;
*build_ProteinCluster    = \&build_SequenceCluster;
sub build_SequenceCluster {
    my $self   = shift;
    my ($id1)  = @_;
    my $ad     = $self->denorm;
    my $age    = $self->age;
    my $mt     = $ad->tracker;
    my $useTax = $self->{CLOUD_TAXA};
    my $type   = $self->type;

    return [] unless ($useTax && $mt);

    my (@lists, @links);

    my ($name, $ns1) = $self->name_and_taxa($id1);
    my $qstat = $self->_seq_node_status($name, $ns1, "Seq Build");
    return \@links if ($qstat->{toss});
    my ($id, $seqID)   = $ad->fast_standardize( $name, $ns1, 'verifyClass' );

    $self->benchstart;
    my $targType  = $ad->is_namespace($ns1, 'AP') ? 'AP' : 'AR';
    my $simKeep   = $ad->primary_maptracker_class($targType);
    my $clusKeep  = $simKeep;
    my $clusSeed  = $seqID;
    my $clusTarg  = $targType;
    my $clusType  = 'is a sequence in cluster';
    my $clusToss  = [ 'deprecated', 'gdna' ];
    my $simToss   = [ 'deprecated', 'gdna', 'exon']; #, 'ipi' ];
    my $simType   = [ 'similar','sameas','contains' ];

    if ($ns1 eq 'IPI') {
        ($clusType) = ('is a cluster with sequence');
        # Allowing similarity edges to be brought in by IPI is causing
        # problems. Enforce IPI as only having cluster edges. Also
        # modified $simToss above
        # $simType = undef;
        # ($clusType, $simType) = ('is a cluster with sequence', ['sameas']);
    } elsif ($ns1 eq 'UG') {
        ($clusType, $simType) = ('is a cluster with sequence', undef);
        $clusTarg = 'AR';
    } elsif ($targType eq 'AR') {
        $clusKeep  = $ad->primary_maptracker_class('UG');
        $clusTarg  = 'UG';
    }

    if ($self->{FILTER} && $self->{FILTER}{MTCLASS}) {
        my $ldat = $self->{FILTER}{MTCLASS};
        if (my $ltoss = $ldat->{Toss}) {
            my @mtToss = @{$ltoss};
            push @{$clusToss}, @mtToss;
            push @{$simToss},  @mtToss;
            my $invalid = $ad->valid_namespace_hash(@{$ltoss});
            $clusType = '' if ($invalid->{$clusTarg});
        }
        if (my $lkeep = $ldat->{Keep}) {
            my @mtKeep = @{$lkeep};
            $clusKeep  = \@mtKeep unless ($clusKeep eq 'UG');
            $simKeep   = \@mtKeep;
        }
    }

    # Swiss-Prot isoform variants should also be tested
    my $isoform = ($ns1 =~ /^(SP|TR|UP)$/) ? "$id-%" : undef;

    # Get cluster assignments
    if ($clusType) {
        my $list = $mt->get_edge_dump
            ( -name      => $clusSeed,
              -return    => 'object array',
              -keeptype  => $clusType,
              -keepclass => $clusKeep,
              -tossclass => $clusToss,
              -dumpsql   => 0 );
        push @lists, [$list, -1, undef, $clusTarg];
        if ($isoform) {
            my $list2 = $mt->get_edge_dump
                ( -name      => $isoform,
                  -return    => 'object array',
                  -keeptype  => $clusType,
                  -keepclass => $clusKeep,
                  -tossclass => $clusToss,
                  -dumpsql   => 0 );
            push @lists, [$list2, -1, undef, $clusTarg];
        }
    }
    
    if ($simType) {
        # Find explicit similarity edges
        my $sqlID = ($mtNamespaces->{$ns1} || '') . $id;
        my @IDs = ( [ $sqlID,    0 ] );
        unless ($unversionedNS->{$ns1}) {
            push @IDs,  [ "$sqlID.%",1 ];
            # Capturing arbitrary version numbers is introducing asymmetry
            # Get the most recent version and only use that
            # This will cause some edges to be lost, but should mostly
            # be fine, and will prevent some inappropriate chaining
            #if (my $v = $ad->max_version_number( $id, $ns1 )) {
            #    push @IDs,  [ "$sqlID.$v",1 ];
            #}
        }
        if ($targType eq 'AP') {

            # COULD THIS INTRODUCE AN ASYMETRY ???

            # push @{$simToss}, 'TrEMBL';
        }
        # die $self->branch(\@IDs);
        foreach my $iddat (@IDs) {
            my ($sid, $requireVersion) = @{$iddat};
            my $list  = $mt->get_edge_dump
                ( -name      => $sid,
                  -keepclass => $simKeep,
                  -tossclass => $simToss,
                  -keeptype  => $simType,
                  # -dumpsql   => 5,
                  -return    => 'object array', );
            push @lists, [$list, undef, $requireVersion, $targType];
            # die "select * from v_edge where edge_id in (".join(',', map { $_->id() } @{$list}).");";
       }

        if ($isoform) {
            my $list2  = $mt->get_edge_dump
                ( -name      => $isoform,
                  -keepclass => $simKeep,
                  -tossclass => $simToss,
                  -keeptype  => $simType,
                  -dumpsql   => 0,
                  -return    => 'object array', );
            push @lists, [$list2, undef, 1, $targType];
        }
    }
    $self->benchend;
    push @links, $self->_parse_sequence_list($id, $ns1, \@lists);
    return \@links;
}

sub clean_ProteinCluster {
    my $self = shift;
    $self->benchstart;
    # We want to clean out edges that are redundant to NRDB:
    # If we have X111 <-> P1_999 <-> X222
    # ... then X111 <-> X222 is redundant (if score = 1)
    my $wn = $self->{WARN}{clean};
    foreach my $id1 ($self->all_node_ids) {
        my ($name, $ns) = $self->node4id($id1);
        next unless ($ns eq 'NRDB');
        my @kids = $self->each_connected_id( $id1 );
        my %conn = map { $_ => 1 } @kids;
        foreach my $kid (@kids) {
            foreach my $okid ($self->each_connected_id( $kid )) {
                next if (!$conn{$okid} || 
                         $self->{EDGES}{$kid}{$okid}[0] != 1);
                $self->disconnect_ids($kid, $okid);
                if ($wn) {
                    my $kn = $self->node4id($kid);
                    my $on = $self->node4id($okid);
                    $self->warn_msg("Disconnected $kn -- $on ($name hub)",
                                    'V',2);
                }
            }
            delete $conn{$kid};
        }
    }
    $self->benchend;
}

sub clean_GeneCluster {
    my $self = shift;
    $self->benchstart;
    # We want to assure that all reference RNAs (RefSeq, Ensembl) have
    # a single distinct gene associated with them
    my %rna2genes;
    foreach my $id1 ($self->all_node_ids) {
        my ($rname, $rns) = $self->node4id($id1);
        next unless ($rns eq 'RSR' || $rns eq 'ENST');
        my @kids = $self->each_connected_id( $id1 );
        $rna2genes{$rns}{$rname} ||= [];
        foreach my $kid (@kids) {
            my ($kname, $kns) = $self->node4id($kid);
            push @{$rna2genes{$rns}{$rname}}, [ $kname, $kns ];
        }
    }
    my $badRNA = 0;
    while (my ($rns, $rnas) = each %rna2genes) {
        while (my ($rname, $genes) = each %{$rnas}) {
            $badRNA++ if ($self->verify_unique_gene_for_rna
                          ($rname, $rns, $genes));
        }
    }
    if ($badRNA) {
        my $discarded = $self->refilter_network();
        if ($self->{WARN}{detail} || $self->{WARN}{toss}) {
            my $remaining = ref($discarded) ? 
                $self->_summarize_edges( $discarded ) : $discarded;
            $self->warn_msg("HEY! Severed data: $remaining", '*', 3)
                if ($remaining);
        }
    }
    $self->benchend;
}

sub verify_unique_gene_for_rna {
    my $self = shift;
    my ($name, $ns, $neighbors) = @_;
    my @genes;
    foreach my $pair (@{$neighbors}) {
        my ($nn, $nns) = @{$pair};
        push @genes, $nn if ($nns eq 'LL' || $nns eq 'ENSG');
    }
    return 0 if ($#genes == 0);
    my $num = $#genes + 1;
    my $why = "Non-unique gene membership : ".
        ($num ? "$num genes : ".join(" + ",@genes) : "No genes!");
    $self->invalidate_node($name, $ns, $why);
    my $stat  = $self->_seq_node_status( $name, $ns );
    $stat->{toss} ||= $why;
    return $why;
}

our $canonicalEdges = {
    map { $_ => 1 } ("is a locus containing","is a locus with protein",
                     "is translated from","is a protein from locus",
                     "can be translated to generate","is contained in locus",
                     )
};

sub build_GeneCluster {
    my $self  = shift;
    my ($id1)      = @_;
    my $ad         = $self->denorm;
    my ($n1, $ns1) = $self->node4id($id1);
    my $qstat      = $self->_seq_node_status($n1, $ns1, "Gene Build");
    return [] if ($qstat->{toss});
    my ($id, $seqID) = $ad->fast_standardize( $n1, $ns1, 'verifyClass' );
    my $useTax     = $self->{CLOUD_TAXA};
    my $type       = $self->type;
    my $mt         = $ad->tracker;
    return [] unless ($seqID && $useTax && $mt);

    # Some genomic clones are misclassified as RNA by some
    # entities. We do NOT want to use those objects.
    return [] if     ($ns1 eq 'AR' && $ad->fast_class($seqID,'gdna'));
    $self->benchstart;
    my $age        = $self->age;
    my (@searches, @links);
    my $baseType = 
        $ad->is_namespace($ns1, 'AP') ? 'AP' :
        $ad->is_namespace($ns1, 'AR') ? 'AR' :
        $ad->is_namespace($ns1, 'AL') ? 'AL' : '';

    if (my $adNSS = $qstat->{polyENS}) {
        # This is a poorly-named Ensembl ID. We need to build self-links
        # within different namespaces
        my $canH = $canonHash->{$ns1} || {};
        while (my ($adNS, $val) = each %{$adNSS}) {
             push @links, [$n1, $adNS, 1, 'Ensembl']
                 if ($canH->{$adNS} && $val);
        }
    }
    # my $canon = $canonHash->{$ns1};

    # Consider generic canonical linkages. THIS IS IMPORTANT
    # Example : LOC3537 immunoglobulin lambda constant 1 (Mcg marker)
    # This locus has NO RefSeq RNAs, but one GenBank
    # (as of 31 Aug 2009)
    my $canon = $ns1 eq 'UG' ? undef : $canonHash->{$baseType};

    if ($canon) {
        # We can traverse the canonical Locus <-> RNA <-> Protein relations
        if (my $nsRoot1 = $bioRoot->{$ns1}) {
            foreach my $ns2 (keys %{$canon}) {
                my $nsRoot2 = $bioRoot->{$ns2};
                my $type    = $bioTypes->{$nsRoot1}{$nsRoot2};
                push @searches, {
                    ns2  => $ns2,
                    type => $type,
                    sc   => 1,
                } if ($type);
            }
        }
    }

    my $ctype;
    if ($ns1 eq 'UG') {
        # Query is a UniGene member
        # We will capture loci through a search:
        push @searches, { ns2  => 'AL',
                          type => 'has member',
                          sc   => 1, };
        # ... and recover RNA by pulling in the transcript cloud:
        unless ($self->{BUILDMETA}{CLOUDS}{$n1}{$ns1}) {
            $ctype = 'TranscriptCluster';
        }
    } elsif ($baseType eq 'AL') {
        # Query is a gene (other than unigene, if we got here)
        push @searches, 
        ( { ns2  => 'UG',
            type => 'is a member of',
            sc   => 1 },
          { ns2  => 'AL',
            type => 'sameas',
            sc   => -1 },
          );
    } elsif (!$self->{BUILDMETA}{CLOUDS}{$n1}{$ns1}) {
        # Query is either RNA or Protein
        if ($baseType eq 'AR') {
            $ctype = 'TranscriptCluster';
        } elsif ($baseType eq 'AP') {
            $ctype = 'ProteinCluster';
        }
    }
    if ($ctype) {
        # We can use the request to grab a more focused cloud:
        my ($clust) = $ad->cached_cloud
            ( -age       => $self->age,
              -cleanseed => 1,
              -seed      => [[$n1, $ns1]],
              -type      => $ctype );
        if ($clust) {
            push @links, $clust;
        } else {
            $self->{BUILDMETA}{CLOUDS}{$n1}{$ns1}{$ctype} = 1;
        }
    }

    # Deal with annoying direct assignment of protein to loci:
    push @searches, {
        ns2  => 'LL',
        type => 'is a protein from locus',
        sc   => 1,
    } if ($ad->is_namespace($ns1, 'RSP'));
    push @searches, {
        ns2  => 'RSP',
        type => 'is a locus with protein',
        sc   => 1,
    } if ($ad->is_namespace($ns1, 'LL'));

    if ($#searches != -1) {
        my @tossClass = ('deprecated','gdna');
        foreach my $sdat (@searches) {
            my $ns2   = $sdat->{ns2};
            my $sc    = $sdat->{sc};
            my $eType = $sdat->{type};
            my $class = $ad->primary_maptracker_class( $ns2 );
            my $list = $mt->get_edge_dump
                ( -name      => $seqID,
                  -keepclass => $class,
                  -tossclass => \@tossClass,
                  -keeptype  => $eType,
                  -return    => 'object array', );
            my @newLinks;
            foreach my $edge (@{$list}) {
                my $other = $edge->other_seq($seqID);
                my $n2    = $other->name;
                # my ($gns) = $self->gnc($n2, $ns2);
                my $stat  = $self->_seq_node_status($n2, $ns2, "Via $id1");
                next if ($stat->{toss});
                my $gns   = $stat->{ns};
                $n2       = $stat->{accU};
                if ($nodeStatus->{_track_}{uc($n2)}) {
                    $self->warn_msg
                        ("TRACK: $n2 [$gns] linked from $n1 [$ns1]", 'o', 2);
                }
                # Explicitly test taxa here; I have had problems
                # with entires that had multiple taxae assigned
                # (not caught by a -keeptaxa filter above)
                if ($canonicalEdges->{$eType}) {
                    # An exception is canonical edges. We need this for some
                    # species:
                    # LOC851189 (Saccharomyces cerevisiae)
                    # is a locus containing
                    # NM_001182355 (Saccharomyces cerevisiae S288c)
                } elsif ($useTax ne $self->_nice_taxa($n2, $gns)) {
                    $self->invalidate_node
                            ( $n2, $gns, "Not taxa $useTax");
                    next;
                }

                my @as    = $ad->simplify_authors($edge->each_authority_name);
                my $auth  = join($authJoiner, @as);
                unless ($auth) {
                    $auth = 'Unknown';
                    $sc   = -1;
                }
                push @newLinks, [$n2, $gns, $sc, $auth];
            }

            # Did we find anything troubling in these results? ...
            if (($ns1 eq 'RSR' || $ns1 eq 'ENST') &&
                ($ns2 eq 'LL'  || $ns1 eq 'ENSG')) {
                # Make sure that RNAs return only a single locus
                if ($self->verify_unique_gene_for_rna
                    ($id, $ns1, \@newLinks)) {
                    $self->{REFILTER}++;
                    @newLinks = ();
                }
            } elsif ($ns2 eq 'AL' && $baseType eq 'AL') {
                # When going from loci to loci, make sure we are
                # getting unique hits.
                my %counts;
                map { $counts{$_->[1]}{$_->[0]} = 1 } @newLinks;
                my @mult;
                foreach my $gns (sort keys %counts) {
                    my @uniq = sort keys %{$counts{$gns}};
                    push @mult, "$gns: ".join(" + ", @uniq)
                        unless ($#uniq == 0);
                }
                unless ($#mult == -1) {
                    my $why = "Locus $id binds multiple loci: ".
                        join(', ', @mult);
                    map { $self->invalidate_edge( $id, $_->[0], $why )
                          } @newLinks;
                    $self->{REFILTER}++;
                    if ($ns1 eq 'UG') {
                        # The starting node is bogus
                        $self->invalidate_node
                            ( $id, $ns1, "Promiscuous UniGene node");
                        # Do not even bother running the other searches
                        last;
                    }
                }
            }
            push @links, @newLinks;
        }
    }

    $self->benchend;
    return \@links;
}

# LOC154 LOC153
# [ENST]  88.1 ENSOCUT00000016180 -> ENSCPOG00000002128
sub build_OrthologCluster {
    my $self  = shift;
    my ($id1)      = @_;
    my $ad         = $self->denorm;
    my $mt         = $ad->tracker;
    my ($n1, $ns1) = $self->node4id($id1);
    my ($id, $seqID) = $ad->fast_standardize( $n1, $ns1, 'verifyClass' );
    return [] unless ($mt);
    unless ($seqID) {
        $self->warn_msg("Failure to validate $n1 [$ns1]",'!', 1)
            if ($self->{WARN}{any});
        return [];
    }
    $self->benchstart;
    my $taxIn      = $self->_nice_taxa($n1, $ns1);
    my $warnSc     = $self->{WARN}{score};
    my $warnX      = $self->{WARN}{desc};
    my $warnT      = $self->{WARN}{toss};
    my $age        = $self->age;
    my $class      = $ad->primary_maptracker_class( $ns1 );

    my (@searches, @links);

    push @searches, {
        ns2  => $ns1,
        type => 'is the orthologue of',
        sc   => -1,
    };

    if (my $canon = $canonHash->{$ns1}) {
        # We can traverse the canonical Locus <-> RNA <-> Protein relations
        if (my $nsRoot1 = $bioRoot->{$ns1}) {
            foreach my $ns2 (keys %{$canon}) {
                my $nsRoot2 = $bioRoot->{$ns2};
                my $type    = $bioTypes->{$nsRoot1}{$nsRoot2};
                push @searches, {
                    ns2  => $ns2,
                    type => $type,
                    sc   => 1,
                } if ($type);
            }
        }
    }

    my %isClass = map { $_ => $ad->is_namespace($ns1, $_) } qw(AP AR AL HG);
    my @genClasses = keys %isClass;

=pod Homologene is naughty

    if ($ns1 eq 'RSP' || $isClass{HG}) {
        # Homologene links
        my @ns2s = ($isClass{HG}) ? ('RSP') : ('HG');
        my @hgl;
        foreach my $ns2 (@ns2s) {
            my $hrows = $ad->convert( -id => $id, -ns1 => $ns1, -ns2 => $ns2,
                                      -age => $age, -nonull => 1 );
            foreach my $row (@{$hrows}) {
                my ($n2, $sc, $auth) = map { $row->[$_] } (0, 3, 2);
                $sc = -1 unless (defined $sc);
                push @hgl, [$n2, $ns2, $sc, $auth];
            }
        }
        if ($isClass{HG}) {
            # We will discard any HomoloGene node that is pulling in
            # multiple loci for the same taxa
            my %byNS; map { push @{$byNS{$_->[1]}}, $_->[0] } @hgl;
            my %byTax;
            while (my ($tns, $names) = each %byNS) {
                my @loci = $ad->convert
                    ( -id => $names, -ns1 => $tns, -ns2 => 'LL', -age => $age,
                      -nonull => 1, -best => 1 );
                my $trows = $ad->convert
                    ( -id => \@loci, -ns1 => 'LL', -ns2 => 'TAX', -age => $age,
                      -nonull => 1, -cols => ['term_out','term_in']);
                map { $byTax{$_->[0]}{$_->[1]} = 1 } @{$trows};
            }
            my @badTax;
            while (my ($tax, $lhash) = each %byTax) {
                my @loci = sort keys %{$lhash};
                next if ($#loci < 1);
                push @badTax, "$tax: ".join("+", @loci);
            }
            unless ($#badTax == -1) {
                @hgl = ();
                $self->invalidate_node
                    ($id, $ns1, "Multiple loci for some taxa: ".
                     join(', ', @badTax));
                $self->{REFILTER}++;
            }
        }
        push @links, @hgl;
    }

=cut

    
    if ($isClass{AP} && $isClass{AR}) {
        # If we can not determine that is protein *OR* RNA, then do not
        # try to find similar sequences
    } elsif ($isClass{AP} || $isClass{AR}) {
        my $pns = $isClass{AP} ? 'AP' : 'AR';
 
        my %hits;
        # First get RBM associations to unversioned entries:
        my $listU   = $mt->get_edge_dump
            ( -name      => $n1,
              -filter    => "TAG = 'Reciprocal Best Match'",
              -keeptype  => 'similar',
              -tossclass => 'deprecated',
              -return    => 'object array', );
        foreach my $edge (@{$listU}) {
            my ($name, $other) = map { $_->name } $edge->nodes();
            ($name, $other) = ($other, $name) unless ($name eq $n1);
            next unless ($name eq $n1);
            next if ($other =~ /\.\d+$/);
            my %auths;
            foreach my $apt ('Average Percent Similarity',
                             'Average Percent ID') {
                my @tags = $edge->has_tag($apt);
                foreach my $tag (@tags) {
                    my $auth = $tag->authname;
                    $auth .= " [RBM]";
                    push @{$auths{$auth}}, $tag->num / 100;
                }
                last unless ($#tags == -1);
            }
            while (my ($auth, $scores) = each %auths) {
                push @{$hits{$other}{$auth}}, @{$scores};
            }
        }
        if ($ns1 eq 'RSP') {
            # Also get Homologene edges
            
            my $listU   = $mt->get_edge_dump
                ( -name      => "$n1.%",
                  -filter    => "VAL = 'Reciprocal Best Match'",
                  -keeptype  => 'is homologous to',
                  -tossclass => 'deprecated',
                  -return    => 'object array', );
            foreach my $edge (@{$listU}) {
                my ($name, $other) = map { $_->name } $edge->nodes();
                ($name, $other) = ($other, $name) unless
                    ($name =~ /^$n1\.\d+$/);
                next unless ($name =~ /^$n1\.\d+$/);
                if ($other =~ /^(.+)\.\d+$/) {
                    $other = $1;
                }
                my %auths;
                # warn "$n1 ($name, $other)";
                foreach my $apt ('Alignment Identity' ) {
                    my @tags = $edge->has_tag($apt);
                    foreach my $tag (@tags) {
                        my $auth = $tag->authname;
                        $auth .= " [RBM]";
                        push @{$auths{$auth}}, $tag->num;
                    }
                    last unless ($#tags == -1);
                }
                while (my ($auth, $scores) = each %auths) {
                    push @{$hits{$other}{$auth}}, @{$scores};
                }
            }
        }

        while (my ($other, $hash) = each %hits) {
            my $oseq = $mt->get_seq($other);
            next unless ($oseq);
            while (my ($auth, $idArr) = each %{$hash}) {
                my ($sc) = sort {$b <=> $a } @{$idArr};
                # Alignment Identity should already be between 0 and 1
                $sc = int(0.5 + 10000 * $sc)/10000 || 0.0001 if ($sc);
                # CHANGE - I took out the second ,$ns1 argument
                # guess_namespace_careful was clobbering ENST with RSR
                # and so forth when that was done.
                # MAY CAUSE PROBLEMS
                my $stat  = $self->_seq_node_status($other);
                next if ($stat->{toss});
                # my ($gns) = $self->gnc($other);
                my $gns = $stat->{ns};

                # Make sure we have the correct type of namespace
                next unless ($ad->is_namespace($gns, $pns));
                my $taxOut = $self->_nice_taxa($other, $gns);
                # Do not consider entries of unknown taxonomy:
                next unless ($taxOut);
                if ($taxIn eq $taxOut) {
                    # These are the same species, so we should be grouping
                    # different namespace entities
                    next if ($gns eq $ns1);
                } else {
                    # Different species, it should be the *same* namespace
                    next unless ($gns eq $ns1);
                }

                push @links, [$other, $gns, $sc, $auth];
                if ($warnSc) {
                    my $sct = defined $sc ? sprintf("%.1f", $sc * 100) : "?";
                    my $msg = sprintf("%5s %10s -> %10s (%s)", 
                                      $sct, $n1, $other, $auth);
                    $msg .= ' - ' .substr
                        ($ad->description( -id => $other, -ns => $gns,
                                           -nocreate => 1, -age => $veryOldAge),0,80)
                        if ($warnX);
                    $self->warn_msg($msg, $ns1, 2);
                }
            }
        }
    }

    if ($#searches != -1) {
        foreach my $sdat (@searches) {
            my $ns2   = $sdat->{ns2};
            my $sc    = $sdat->{sc};
            my $class = $ad->primary_maptracker_class( $ns2 );
            my $list = $mt->get_edge_dump
                ( -name      => $seqID,
                  -keepclass => $class,
                  -tossclass => 'deprecated',
                  -keeptype  => $sdat->{type},
                  -return    => 'object array', );
            my @newLinks;
            foreach my $edge (@{$list}) {
                my $other = $edge->other_seq($seqID);
                my $n2    = $other->name;
                my $stat  = $self->_seq_node_status( $n2, $ns2 );
                next if ($stat->{toss});
                # my ($gns) = $self->gnc($n2, $ns2);
                my $gns   = $stat->{ns};
                my @as    = $ad->simplify_authors($edge->each_authority_name);
                my $auth  = join($authJoiner, @as);
                unless ($auth) {
                    $auth = 'Unknown';
                    $sc   = -1;
                }
                push @newLinks, [$n2, $gns, $sc, $auth];
            }
            if (($ns1 eq 'RSR' || $ns1 eq 'ENST') &&
                ($ns2 eq 'LL'  || $ns1 eq 'ENSG')) {
                # Make sure that RNAs return only a single locus
                if ($self->verify_unique_gene_for_rna
                    ($id, $ns1, \@newLinks)) {
                    $self->{REFILTER}++;
                    @newLinks = ();
                }
            }
            push @links, @newLinks;
        }
    }
    $self->benchend;
    return \@links;
}


our $requireRBM = { RSR  => { RSR => 1, ENST => 1 },
                    ENST => { RSR => 1, ENST => 1 }, };
{
    my @ns = qw(RSP ENSP IPI);
    push @ns, BMS::MapTracker::AccessDenorm::_full_kids( ['UP'] );
    for my $i (0..$#ns) {
        for my $j (0..$#ns) {
            $requireRBM->{ $ns[$i] }{ $ns[$j] } = 1;
        }
    }
}

our @scoreTags =
    ( ['Average Percent ID',   100], 
      ['Total Percent ID',     100],
      ['Sequence Differences', 1],
      ['Fraction covered',     1], );

sub _parse_sequence_list {
    my $self = shift;
    $self->benchstart;
    my $ad     = $self->denorm;
    my $useTax = $self->{CLOUD_TAXA};
    my $warnT  = $self->{WARN}{toss};
    my $warnSc = $warnT && !$self->{WARN}{nosc};
    my $track  = $nodeStatus->{_track_} ||= {};
    my ($id, $ns1, $lists) = @_;
    my $pd     = "via $id [$ns1] ";
    my @links;
    my %hits;
    my $bogusVers = 99999;
    my $ignore    = $self->{IGNORE};
    my $uniProt   = $ad->valid_namespace_hash('UP');
    if ($nodeStatus->{_track_}{uc($id)}) {
        my $found = "";
        foreach my $ldat (@{$lists}) {
            my ($list, $forceScore, $requireVers, $ns2) = @{$ldat};
            if ($#{$list} != -1) {
                $found .= "TO [$ns2]";
                $found .= " require version" if ($requireVers);
                $found .= " force score = $forceScore" if ($forceScore);
                $found .= ":\n";
                foreach my $edge (@{$list}) {
                    $found .= "   ".$edge->to_text_short();
                }
            }
        }
        if ($found) {
            $self->warn_msg("TRACK: $id [$ns1] - Edge Report:\n$found",'O',2);
        } else {
            $self->warn_msg("TRACK: $id [$ns1] - no edges found",'O',2);
        }
    }
    foreach my $ldat (@{$lists}) {
        my ($list, $forceScore, $requireVers, $ns2) = @{$ldat};
        # Organize hits as hash:
        # { SubjectAcc }{ QueryVersion + SubjectVersion }
        # This is a bit goofy, but it is needed to enforce symetry
        # (so same result will be generated if comming from S rather than Q)
        if ($self->{WARN}{full}) {
            my $lnum = $#{$list} + 1;
            $self->warn_msg("$pd : $lnum edges for [$ns2]",'>',2)
                if ($lnum >= 10);
        }
        foreach my $edge (@{$list}) {
            my ($qry, $sbj) = $edge->nodes;
            if ($sbj->name =~ /^\Q$id\E(.*)$/) {
                # Subject matches the query, Looks like we need to swap Q/S
                my $sxtra = $1;
                if ($qry->name =~ /^\Q$id\E(.*)$/) {
                    # But query matches the query, too
                    # Can happen for mutant forms
                    my $qxtra = $1;
                    ($qry, $sbj) = ($sbj, $qry)
                        if (length($sxtra) < length($qxtra));
                } else {
                    ($qry, $sbj) = ($sbj, $qry);
                }
            }
            my ($qname, $sname) = map { $_->name} ($qry, $sbj);
            unless ($qname =~ /^\Q$id\E/) {
                $self->warn_msg
                    ("$pd$qname: Discarding non-match to query '$id'",'V',2)
                    if ($warnT);
                next;
            }
            my ($qstat, $sstat) = 
                ($self->_seq_node_status($qname, $ns1, $pd),
                 $self->_seq_node_status($sname, $ns2, $pd));

            next if ($qstat->{toss} || $sstat->{toss});
            
            my ($qv, $sv);
            ($qname, $qv) = map { $qstat->{$_} } qw(accU vers);

            if ($qstat->{mut} && $id !~ /\{[^\}]+\}$/) {
                # Do not consider edges initiating from a mutation if the 
                # query was itself not a mutation:
                $self->warn_msg
                    ("$pd$qname: Discarding non-match to mutant '$id'",'V',2)
                    if ($warnT);
                next;
            }
            if ($useTax && $sstat->{taxa} ne $useTax) {
                $self->warn_msg("$pd$sname: Wrong taxa",'V',2)
                    if ($warnT);
                next;
            }
            ($sname, $sv) = map { $sstat->{$_} } qw(accU vers);
            my $gns2 = $sstat->{ns};
            my $trkTok = ($nodeStatus->{_track_}{uc($sname)} ||
                          $nodeStatus->{_track_}{uc($qname)}) ?
                          "$qname [$ns1] ".$edge->reads($qry).
                              " $sname [$gns2]" : 0;
            $self->warn_msg("TRACK: Discoverd $trkTok", 'o', 2) if ($trkTok);

            my $totVers = $qv + $sv;
            if ($totVers == 0 && $edge->has_tag("Sequence Version")) {
                # This is a fully UNversioned edge with versioning
                # stored as edge meta tags. Treat it as absolutely
                # authoritative.
                my @bad;
                foreach my $eSeq (map { $_->valname }
                               $edge->has_tag("Sequence Version")) {
                    my $vns = $eSeq =~ /^\Q$sname\E/ ? $gns2 :
                        $eSeq =~ /^\Q$qname\E/ ? $ns1 : "";
                    my $vStat = $self->_seq_node_status($eSeq, $vns, $pd);
                    push @bad, $eSeq if ($vStat->{toss});
                }
                if ($#bad != -1) {
                    # The edge was not built with the best versions
                    $self->warn_msg
                        ("$pd Improper edge versions ".join(',', @bad),'V',2)
                        if ($warnT);
                    $self->warn_msg
                        ("TRACK: $trkTok Wrong edge versions", 'o', 2) 
                        if ($trkTok);
                    next;
                }
                $totVers = $bogusVers;
            } elsif ($requireVers) {
                # Ignore the edge unless the IDs are properly versioned
                my @discard;
                push @discard, "$sname [$gns2]" if ($sstat->{noVers});
                push @discard, "$qname [$ns1]"  if ($qstat->{noVers});
                unless ($#discard == -1) {
                    if ($warnT) {
                        map { $self->warn_msg
                                  ("$pd$_: Discard unversioned ID",'V',2);
                          } @discard;
                    } elsif ($trkTok) {
                        $self->warn_msg
                            ("TRACK: $trkTok Reject unversioned", 'o', 2);
                    }
                    next;
                }
            }
            # push @{$hits{$gns2}{$sname}{$totVers}}, [$edge, $forceScore];
            # push @{$hits{$gns2}{$sname}}, [$edge, $forceScore, $totVers];
            $forceScore = -1 if ($qstat->{ns} eq 'UCSC' || $gns2 eq 'UCSC');
            my $targ = $hits{$gns2}{$sname}{$edge->id} ||= {
                e   => $edge,
                eid => $edge->id,
                tv  => 0,
            };
            $targ->{fs} ||= $forceScore;
            $targ->{tv} = $totVers if ($targ->{tv} < $totVers);
            $self->warn_msg("TRACK: Considering $trkTok", 'o', 2) if ($trkTok);
        }
    }
    while (my ($gns2, $hitHash) = each %hits) {
        while (my ($sname, $eHash) = each %{$hitHash}) {
            my (%scores, %Mods);
            my @eids = sort { $a <=> $b } keys %{$eHash};
            foreach my $eid (@eids) {
                my $edat = $eHash->{$eid};
                my $edge = $edat->{e};
                my $forceScore = $edat->{fs};
                my $bestT      = $edat->{tv};
                my @auths = $ad->simplify_authors($edge->each_authority_name);
                if ($forceScore) {
                    map { push @{$scores{$_}}, $forceScore } @auths;
                    next;
                }
                map { $scores{$_} ||= [] } @auths;

                my @rbms = $edge->has_tag('Reciprocal Best Match');
                unless ($#rbms == -1) {
                    # We are now insisting on RBM off of unversioned IDs
                    next unless ($bestT == $bogusVers);
                    map { $Mods{$_->authname} = 'RBM' } @rbms;
                }

                if ($edge->reads() eq 'is the same as') {
                    # Score is automatically 1
                    map { $scores{$_} = [ 1 ] } @auths;
                } else {
                    my $ft = 0;
                    for my $st (0..$#scoreTags) {
                        my $tn = $scoreTags[$st][0];
                        my @tags = $edge->has_tag($tn);
                        # next if ($#tags == -1);
                        foreach my $tag (@tags) {
                            my $v    = $tag->num;
                            my $tsc  = defined $v ? 
                                $v / $scoreTags[$st][1] : -1;
                            my $auth = $tag->authname;
                            if ($tn eq 'Sequence Differences') {
                                $Mods{$auth} ||= 'MUT';
                                $tsc = -1;
                            } elsif ($tsc != 1) {
                                $Mods{$auth} ||= 'SIM';
                            }
                            push @{$scores{$auth}}, $tsc;
                            $ft++;
                        }
                    }
                    if ($ft == 0) {
                        $self->warn_msg("$pd$sname: No valid tags to score",
                                        'V',2) if ($warnT);
                    }
                }
            }
            my @keep;
            while (my ($auth, $scArr) = each %scores) {
                if ($#{$scArr} == -1) {
                    # No extracted scores for the edge
                    if ($auth eq 'UniProt') {
                        # Allow undefined relationships to be defined by
                        # UniProt
                        $scArr = [ -1 ];
                    } else {
                        next;
                    }
                }
                # Use the *worst* score found.
                my @sss = sort { $a <=> $b } @{$scArr};
                my ($worst, $best) = ($sss[0], $sss[-1]);
                # Discard the linkage if the score is defined and not good
                # This once was 25% which caused serious problems with
                # scruffy GenBank entries associated with some RefSeq
                # RNAs
                if ($best < 0.90 && $best >= 0) {
                    if ($warnSc) {
                        my $msg = sprintf("%s%s : Poor score  %.4f [%s]",
                                          $pd, $sname, $best, $auth);
                        $self->warn_msg($msg,'V',2);
                    }
                    next;
                }
                if ($worst) {
                    if ($worst < 0) {
                        $worst = undef;
                    } else {
                        $worst = int(0.5 + 10000 * $worst) / 10000 || 0.0001;
                    }
                }
                my $aMod = $Mods{$auth};
                if ($aMod) {
                    if ($requireRBM->{$ns1}{$gns2}) {
                        # Some edges will insist on using reciprocal best match
                        if ($auth eq 'Ensembl' && 0) {
                            # We do not know if Ensembl data are RBM, so just
                            # use a high threshold
                            next if ($worst < 0.95);

                            # 2012-07-26 Change:
                            # Insist on perfect matches for IPI?
                            #next if (($ns1 eq 'IPI' || $gns2 eq 'IPI')
                            #         && $worst != 1);

                        } elsif ($aMod eq 'MUT') {
                            # Link between a mutant and unmtated form
                        } elsif ($aMod ne 'RBM') {
                            # Otherwise require RBM to be specified.
                            $self->warn_msg("$pd$sname [$auth - $aMod] : Not RBM",
                                            'V',2) if ($warnSc);
                            next;
                        }
                    }
                    $auth    = "$auth [$aMod]";
                }
                push @keep, [$worst, $auth];
            }
            # warn "$id [$ns1] vs $sname [$gns2]:\n" if ($sname =~ /Q5MH36/ || $id =~ /Q5MH36/ );# .$self->branch( -noheader => 1, -quietkey => 'e', -ref => { edges => $eHash, mods => \%Mods, scores => \%scores, keep => \@keep } ) if ($sname =~ /Q5MH36/ || $id =~ /Q5MH36/ );
            if ($track->{uc($id)} || $track->{uc($sname)}) {
                $self->warn_msg
                    ("TRACK: $id [$ns1] $sname [$gns2] : ".
                     ($#keep == -1 ? 'DISCARD' : 'Kept'), 'o', 2);
            }
            if ($#keep == -1) {
                $self->warn_msg("$pd$sname: No surviving scores",'V',2)
                    if ($warnSc);
            } else {
                if ($crossSpeciesNs->{$gns2}) {
                    $sname   = "$sname ($useTax)";
                }
                map { push @links, [$sname, $gns2, @{$_}] } @keep;
            }
        }
    }
    $self->benchend;
    return @links;
}

my $unhelpfulYeastFlyNS = { map { $_ => 1 } qw(AP AR SYM ENST ENSP ENSG) };

sub _seq_node_status {
    my $self = shift;
    my ($name, $nsReq, $ctxt) = @_;

    # I tried to segregate this by presumptive namespace. However,
    # there are challenges with guessing the appropriate namespace for
    # some IDs. This is resulting in different namespaces being
    # assigned under different conditions. This in turn occasionally
    # results in asymmetry in how networks get built, which causes
    # auto-destructive oscilations. Example = HLA gene/pseudogene
    # family

    # So instead a NAME is going to be treated as unique in a network
    # and given a single namespace

    # POTENTIAL ISSUE
    # If we allow a 'guide' namespace to be provided, then the guessed
    # namespace may be different for different guides. So we will clobber
    # the guide and let the program try to guess completely on its own:
    $nsReq = "";

    # ACTUAL ISSUE

    # Disallowing the guide namespace causes yeast IDs (which are
    # IDENTICAL for the gene, RNA and protein, eg YNL220W) to fall
    # flat on their face

    my $ucn = uc($name || "");

    unless ($nodeStatus->{$ucn}) {
        my $ad   = $self->denorm;
        my $stat = $nodeStatus->{$ucn} = { toss => "", taxa => "" };
        my ($unv, $vers) = ($name, 0);
        # Make an initial guess at the namespace to see if it should be
        # considered unversioned
        my $ns = $ad->guess_namespace_careful($name, $nsReq);
        unless ($unversionedNS->{$ns}) {
            if ($name =~ /^(\S+)\.(\d+)(\{[^\}]+\})?$/) {
                # This is a versioned ID
                ($unv, $vers) = ($1, $2);
                if (my $mutTok = $3) {
                    # This is a specific mutation entity
                    # Keep regardless of version number
                    $stat->{mut} = $mutTok;
                    $unv = $name;
                } else {
                    # Re-guess the namespace with the versioning gone
                    $stat->{accV} = $name;
                    $ns = $ad->guess_namespace_careful($unv, $nsReq);
                }
            } else {
                $stat->{noVers} = 1;
            }
        }
        if ($ns eq 'SYM') {
            # I am nurturing a pathalogical hatred for symbols...
            # There should be no 'symbolic' symbol nodes in our network
            # However, there are both Manning and BMS internal symbols
            # that are used to represent proteins
            my %seen;
            foreach my $seq ($ad->tracker->get_seq
                             ( -name => $unv, -nocreate => 1, -defined => 1)) {
                my $mtns = $seq->namespace->name();
                $seen{ $mtNs2adNs->{uc($mtns)} || "" }++;
            }
            $ns = 
                $seen{BMSS} ? 'BMSS' : 
                $seen{MANK} ? 'MANK' : 
                $seen{SYML} ? 'SYML' : '';
            unless ($ns) {
                # Sometimes Ensembl uses the same ID for both the symbol
                # and the gene
                my $wns = $self->_check_weakly_named_ensembl($unv);
                my $ensNS = $wns->{ns}{AL};
                if ($ensNS && $wns->{num} == 1) {
                    $ns = $ensNS;
                } else {
                    $ns = "SYM";
                }
            }
            $stat->{toss} = "Vanilla Gene Symbol" if ($ns eq 'SYM');
            delete $stat->{noVers} if ($unversionedNS->{$ns});
        }
        $stat->{ns} = $ns;
        if ($stat->{accV}) {
            my $needV = $ad->max_version_number($unv, $ns);
            unless ($needV == $vers) {
                $fullIgnore->{$ns}{$name}++;
                $stat->{toss} = "Wrong Version";
            }
        }



        my $noCache = 0;
        if ($ns eq 'ENSE') {
            $fullIgnore->{$ns}{$name}++;
            $stat->{toss} = "Ensembl Exon";
        } elsif ($ns eq 'UNK') {
            $fullIgnore->{$ns}{$name}++;
            $stat->{toss} = "Unknown Namespace";
        } elsif ($ns eq 'GI') {
            $fullIgnore->{$ns}{$name}++;
            $stat->{toss} = "GenBank ID";
        } else {
            # Trim Swiss-Prot variant number if needed:
            my $uniProt   = $nodeStatus->{_UPhash_} ||=
                $ad->valid_namespace_hash('UP');
            if ($uniProt->{$ns} && $unv =~ /(\S+)\-(\d+)$/) {
                $unv = $1;
                $stat->{uniVar} = $2;
            }

            my ($sid, $seqnameid) = $ad->fast_standardize($unv, $ns);
            if (!$seqnameid) {
                $stat->{toss} = "Failed to standardize";
                $fullIgnore->{$ns}{$name}++;
            } elsif ($ad->fast_class($seqnameid,'deprecated')) {
                # Deprecation occurs on unversioned IDs, but the
                # selection of sequences is often to their versioned
                # members - we need to check the unversioned IDs to
                # verify that they are not deprecated.
                $stat->{toss} = "Deprecated";
                $fullIgnore->{$ns}{$name}++;
            } elsif ($ad->fast_class($seqnameid,'gdna')) {
                $stat->{toss} = "Genomic DNA";
                $fullIgnore->{$ns}{$name}++;
            } elsif (! $ad->class_matches_namespace( $sid, $ns )) {
                $stat->{toss} = "Failed Class Check";
                $fullIgnore->{$ns}{$name}++;
            }
            if ($ns eq 'RSR' || $ns eq 'ENST') {
                # Why is this here??
            }
            my $tax = $stat->{taxa} = $self->_nice_taxa( $unv, $ns );
            if ($tax =~ /^(Saccharomyces|Drosophila)/) {
                if ($unhelpfulYeastFlyNS->{$ns}) {
                    $stat->{toss} = "Dangerous Yeast/Fly ID";
                }
            }
            if (!$crossSpeciesNs->{$ns} && !$tax) {
                $stat->{toss} = "Non-singular taxa";
                $fullIgnore->{$ns}{$name}++;
                # Tempting to try to salvage via the versioned ID, but
                # this can lead to asymmetries
            }
            $stat->{accU} = $unv;
            $stat->{vers} = $vers;
        }
        if ($stat->{toss} && $self->{WARN}{toss}) {
            $ctxt = $ctxt ? "$ctxt: " : "";
            $self->warn_msg("${ctxt}[$ns] $name : $stat->{toss}",'@',2);
        }
        my $track  = $nodeStatus->{_track_} ||= {};
        if ($track->{uc($unv)} || $track->{$ucn}) {
            $self->warn_msg
                ("TRACK: $name [$ns] : $unv : ".
                 ($stat->{toss} || 'ok'), '@', 2);
        }
        # warn $self->branch($stat);
        if ($noCache) {
            delete $nodeStatus->{$ucn};
            return $stat;
        }
    }
    return $nodeStatus->{$ucn};
}

sub _check_weakly_named_ensembl {
    my $self = shift;
    my $name = shift || "";
    my $cache = $nodeStatus->{_weakens_} ||= {};
    unless ($cache->{$name}) {
        my $wns = $cache->{$name} = { ns => {}, num => 0 };
        if (my $seq = $self->denorm->tracker->get_seq
            ( -name => "#None#$name", -nocreate => 1, -defined => 1)) {
            if ($seq->is_class('Ensembl')) {
                # This entry is flagged as being an Ensembl ID
                my %check = ( Locus   => ['AL' , 'ENSG'],
                              RNA     => ['AR' , 'ENST'],
                              Protein => ['AP' , 'ENSP'] );
                while (my ($mtNS, $adDat) = each %check) {
                    if ($seq->is_class($mtNS)) {
                        my ($adNS, $useNS) = @{$adDat};
                        $wns->{ns}{$adNS} = $useNS;
                        $wns->{num}++;
                    }
                }
            }
        }
    }
    return $cache->{$name};
}

sub paths {
    my $self = shift;
    my $args = $self->parseparams( @_ );
    my @seeds;
    if (my $n1   = $args->{NODE} || $args->{SEED} || $args->{ID}) {
        my $ns1  = $args->{NS} || $args->{NS1};
        my $seed = $self->has_node($n1, $ns1);
        push @seeds, $seed if ($seed);
    } else {
        @seeds = @{$self->{SEED}};
    }
    return wantarray ? () : [] if ($#seeds == -1);

    $self->benchstart;
    my $ad      = $self->denorm;
    my $ns2filt = $args->{NS2};
    my $idReq   = $args->{ID2} || $args->{TARGET};
    my $opts    = lc($args->{OPTS} || '');
    if ($ns2filt) {
        $ns2filt = $ad->valid_namespace_hash
            ( ref($ns2filt) ? @{$ns2filt} : $ns2filt);
    }
    my (@rv, %nsns);
    foreach my $seed (@seeds) {
        my ($n1, $ns1) = $self->node4id($seed);
        my $cache   = ($self->{PATH_CACHE}[$seed] && !$opts) ?
            $self->{PATH_CACHE}[$seed] : $self->_cache_paths( $seed, $opts );
        my @paths;
        while (my ($taskTag, $pathArr) = each %{$cache}) {
            if ($idReq) {
                # User wants a specific ID
                my @reqs = ref($idReq) ? @{$idReq} : ($idReq);
                foreach my $req (@reqs) {
                    my $id2;
                    if ($ns2filt) {
                        ($id2) = sort { $b <=> $a } map 
                        { $self->has_node($req, $_) } keys %{$ns2filt};
                    } else {
                        $id2 = $self->has_node($req);
                    }
                    my $path  = $pathArr->[$id2];
                    push @paths, $path if ($path);
                }
            } elsif ($ns2filt) {
                # Keep only paths with target matching a namespace request
                for my $id2 (1..$#{$pathArr}) {
                    if (my $path = $pathArr->[$id2]) {
                        push @paths, $path if ($ns2filt->{$path->[1]});
                    }
                }
            } else {
                # Keep all paths
                for my $id2 (1..$#{$pathArr}) {
                    my $path = $pathArr->[$id2];
                    push @paths, $path if ($path);
                }
            }
        }
        if ($args->{SIMPLE}) {
            push @rv, @paths;
        } else {
            # UniProt protein isoforms are causing problems
            # eg P61169-1
            my $uniProt = $ad->valid_namespace_hash('UP');
            for my $p (0..$#paths) {
                my ($n2, $ns2, $sc, $auth, $btwn, $nodes) = @{$paths[$p]};
                # Need to take out species modifier for things like NRDB
                # These are represented in the form 'P_1234 (Homo sapiens)'
                # when stored in the cloud
                $n1 =~ s/ .+// if ($crossSpeciesNs->{$ns1});
                $n2 =~ s/ .+// if ($crossSpeciesNs->{$ns2});
                # Need to remove UniProt isoform flags eg P61169-1
                $n1 =~ s/\-\d+$// if ($uniProt->{$ns1});
                $n2 =~ s/\-\d+$// if ($uniProt->{$ns2});
                push @rv, {
                    term_in      => $n1,
                    term_out     => $n2,
                    ns_in        => $nsns{$ns1} ||= $ad->namespace_name($ns1),
                    ns_out       => $nsns{$ns2} ||= $ad->namespace_name($ns2),
                    matched      => $sc,
                    auth         => $auth,
                    ns_between   => $btwn,
                    term_between => $nodes,
                };
            }
        }
    }
    if ($args->{INVERT}) {
        # Report the paths "backwards"
        my @ltKeys = qw(auth term_between ns_between);
        foreach my $p (@rv) {
            foreach my $key (@ltKeys) {
                # Invert concatenated strings eg 'NP_001225 < ENSP00000380525'
                $p->{$key} = join(' < ', reverse 
                                     split(/ \< /, $p->{$key} || ""));
            }
            # Swap in/out
            ($p->{ns_in}, $p->{ns_out})     = ($p->{ns_out}, $p->{ns_in} );
            ($p->{term_in}, $p->{term_out}) = ($p->{term_out}, $p->{term_in} );
        }
    }
    $self->benchend;
    return wantarray ? @rv : \@rv;
}

sub _cache_paths {
    my $self = shift;
    $self->benchstart;
    my ($seed, $opts)  = @_;
    my $ad      = $self->denorm;
    my $maxID   = $self->node_count;
    my $edges   = $self->{EDGES};

    # $opts is just a string of option tokens
    $opts = lc($opts || '');

    # Paths are arrays organized as an array ($bestPaths) indexed by
    # the node ID of path *TERMINUS*. Each path is an array reference with:
    # 0 = aggregate score (with -1 representing undef)
    # 1 = The actual path
    # 2 = hash counting special nodes (similarity, NRDB) encountered

    # Zero out the best paths - no score should be worse than -2.
    my %bestPaths;
    for my $id (1..$maxID) {
        $bestPaths{main}[$id] = [-2];
    }
    # Nucleate recursion with the seed
    my $rnas          = $ad->valid_namespace_hash('AR');
    my $prots         = $ad->valid_namespace_hash('AP');
    my $genes         = $ad->valid_namespace_hash('AL');
    # Perfect will force any paths with score that is not 1 or 0 to be undef
    my $forcePerfect  = $opts =~ /perfect/ ? 1 : 0;
    my ($sn, $sns)    = $self->node4id($seed);
    my $snsMod        = $sn =~ /\.\d+\{[^\}]+\}$/ ? "mut" : "";
    # Tracking high-level namespace movement
    # L = Locus
    # R = RNA
    # P = Protein
    # S = Sequence = R || P

    # SPECIAL FLAGS
    # These are used to detect potentially undesired paths in the growing
    # chain. The utilized flags are:
    # RPR   - transition from RNA to protein and back to RNA
    # LSL   - transition from a locus to RNA/protein and back to locus
    # prior - tracks the moltype of the prior node
    # L2P   - detectes any movement in L -> R -> P
    # P2L   - detectes any movement in L <- R <- P

    my %seedSpec        = ( LSL => '', prior => '', "got$sns$snsMod" => 1);
    my $maxEdge         = 0;
    my $doRPRpenalty    = 1;
    my $doSpecialCase   = 1;
    my $noOscillate     = 0;
    my $noMultNamespace = 0;
    # Edge will limit paths to a maximum number of edges
    if ($opts =~ /edge\s*(\d+)/) { $maxEdge = $1; }
    if ($opts =~ /norpr/) { $doRPRpenalty = 0; }
    if ($opts =~ /noosc/) { $noOscillate = 1; }
    if ($opts =~ /nomult/) {
        # The user does not want the same namespace transited more than once
        # eg AP < IPI < NRDB < RSP < AP < RSP < ENSP < RSP
        $noMultNamespace  = {};
        if ($opts =~ /multok(\S+)/) {
            map { $noMultNamespace->{$_} = 1 } split(/\,/, uc($1));
        }
    }





    my @illegalLSL = 'S.*L.*S';
    my $okLoop;
    if ($opts =~ /l2lok/) {
        $okLoop = '(LRRL)$';
    } else {
        push @illegalLSL, 'L.*L';
    }

    if ($rnas->{$sns}) {
        # Penalize RNA -> Protein -> RNA
        $seedSpec{RPR}   = 'R';
        $seedSpec{LSL}   = 'S';
        $seedSpec{prior} = 'R';
    } elsif ($genes->{$sns}) {
        # Penalize moving between loci
        $seedSpec{LSL}   = 'L';
        $seedSpec{prior} = 'L';
    } elsif ($sns eq 'NRDB') {
        $seedSpec{NRDB}  = 1;
        $seedSpec{LSL}   = 'S';
        $seedSpec{prior} = 'P';
    } elsif ($prots->{$sns}) {
        $seedSpec{LSL}   = 'S';
        $seedSpec{prior} = 'P';
    } else {
        $seedSpec{prior} = '';
    }
    $seedSpec{chain} = $seedSpec{prior};
    my $tagAuth;
    if ($opts =~ /tag\s*\=\s*(\S+)/) {
        $tagAuth = $1;
    }
    # Nucleate the starting seed node
    $bestPaths{main}[$seed] = [1, [$seed], \%seedSpec];
    # %tasked tracks the nodes we are currently working with
    my %tasked        = ( main => { $seed => 1 } );
    my $edgeCount     = 0;

    while (1) {
        # Consider each task tag separately
        $edgeCount++;
        # The user may have specified a maximum path length to follow:
        last if ($maxEdge && $edgeCount > $maxEdge);

        my $workDone = 0;
        # while (my ($taskTag, $tdat) = each %tasked) {
        my @tasks = keys %tasked;
        while ($#tasks != -1) {
            my $taskTag = shift @tasks;
            my $tdat    = $tasked{$taskTag};
            my $bpDat = $bestPaths{$taskTag};
            # What nodes will we expand from? Work from highest score down:
            my @ids = sort { $bpDat->[$b][0]
                                 <=> $bpDat->[$a][0] } keys %{$tdat};
            # Ignore this tasktag if none left:
            my $idnum = $#ids + 1;
            next unless ($idnum);

            $workDone += $idnum;
            # Clear task list for next recursion:
            $tdat = $tasked{$taskTag} = {};

            my %newTasks;
            foreach my $id1 (@ids) {
                # REMEMBER - $id1 represents a terminal node in a path

                # It is possible during this recursion cycle that a
                # previous task has found a better path for $id1. If that
                # is the case, do not bother trying to expand from this
                # sub-optimal path; we will re-visit the $id1 in the next
                # round of recursion, utilizing a better root path.
                next if (exists $tdat->{$id1});

                my $path = $bpDat->[ $id1 ];
                # warn $self->_localpath_to_text($path);
                my $sc   = $path->[0];
                my %seen = map { $_ => 1 } @{$path->[1]};
                # Consider each node connected to $id1:
                foreach my $id2 (keys %{$edges->{$id1} || {}}) {
                    # $id2 represents a presumptive terminus of a path
                    # extended from the one currently ending with $id1.

                    # Do not bother investigating loops
                    next if ($seen{$id2});

                    # Assume initially that the tasktag will be the same
                    my $id2tag = $taskTag;

                    # Calculate the score for the extended path
                    my $scl     = $sc;
                    my %special = %{$path->[2]};
                    my ($n2, $ns2) = $self->node4id($id2);
                    my $ns2Mod     = $n2 =~ /\.\d+\{[^\}]+\}$/ ? "mut" : "";

                    if ($noMultNamespace && $special{"got$ns2$ns2Mod"}) {
                        # We have already encountered this namespace in
                        # this path, and we have been told not to
                        # tranist multiple namespaces
                        next unless ($noMultNamespace->{$ns2});
                    }
                    my $tTok = '';
                    my $prior = $special{prior};
                    if ($prots->{$ns2}) {
                        # The target is a protein
                        $tTok = 'P';
                        $special{RPR} .= $tTok if ($special{RPR});
                        $special{LSL} .= 'S';
                        if ($prior eq 'R' || $prior eq 'L') {
                            $special{L2P}++;
                        }
                    } elsif ($rnas->{$ns2}) {
                        # The target is an RNA
                        $tTok = 'R';
                        $special{LSL} .= 'S';
                        if ($special{RPR}) {
                            # c. we follow an RNA into protein and back
                            # into RNA again - impossible to know how
                            # well RNAs match
                            if ($special{RPR} =~ /P$/) {
                                $scl = -1 if ($doRPRpenalty);
                            }
                        } else {
                            $special{RPR} = $tTok;
                        }
                        if ($prior eq 'L') {
                            $special{L2P}++;
                        } elsif ($prior eq 'P') {
                            $special{P2L}++;
                        }
                    } elsif ($genes->{$ns2}) {
                        # The target is a locus
                        $tTok = 'L';
                        $special{LSL} .= $tTok;
                        if ($prior eq 'R' ||
                            $prior eq 'P') {
                            $special{P2L}++;
                        }
                    }
                    if ($tTok) {
                        $special{prior}  = $tTok;
                        $special{chain} .= $tTok;
                    }
                    unless ($scl == -1) {
                        my ($matched, $auth) = @{$edges->{$id1}{$id2}};
                        if ($tagAuth && $auth =~ /($tagAuth)/i) {
                            $id2tag = $1;
                            unless ($bestPaths{$id2tag}) {
                                map { $bestPaths{$id2tag}[$_] = [-2] }
                                (1..$maxID);
                            }
                        }
                        if ($auth eq 'Unknown') {
                            $scl = -1;
                        } elsif ($matched < 0) {
                            $scl = -1;
                        } else {
                            # We may still have a special condition
                            # Set score to undefined if:
                            if ($auth =~ /\[(SIM|RBM|MUT)\]/) {
                                # a. we use two or more similarity edges
                                $scl = -1 if (++$special{SIM} > 1);
                            } elsif ($ns2 eq 'NRDB') {
                                # b. we transit through NRDB more than once
                                $scl = -1 if (++$special{NRDB} > 1);
                            }
                            foreach my $ilsl (@illegalLSL) {
                                # Illegal migration through loci
                                if ($special{LSL} =~ /$ilsl/) {
                                    # my ($n2) = $self->node4id($id2); $self->msg("[-]", "LSL set -1 for $n2");
                                    $scl = -1;
                                }
                            }
                            # Assuming score is still defined, round it
                            unless ($scl == -1) {
                                $scl = int(0.5 + 10000 * $scl *$matched)/10000;
                                $scl = -1 if 
                                    ($forcePerfect && $scl != 1 && $scl != 0);
                            }
                        }
                    }
                    # Discard the path if it is oscillating in both directions
                    # on the Locus <-> RNA <-> Protein chain:
                    if ($noOscillate && $special{P2L} && $special{L2P}) {
                        unless ($okLoop && $special{chain} =~ /$okLoop/) {
                            # my ($n2) = $self->node4id($id2); $self->msg("[-]", "Oscilation halt for $n2");
                            next;
                        }
                    }
                    # Discard the path if the final score is worse than that
                    # already seen:
                    unless ($scl > $bestPaths{$id2tag}[ $id2 ][0]) {
                        # my ($n2) = $self->node4id($id2); $self->msg("[-]", "Better score for $n2");
                        next;
                    }
                    $special{"got$ns2$ns2Mod"}++;
                    # Otherwise, set this new path as the best for $id2:
                    $bestPaths{$id2tag}[ $id2 ] = 
                        [ $scl, [@{$path->[1]}, $id2], \%special ];
                    # ... and make note that we will recurse off of it:
                    $newTasks{$id2tag}++;
                    $tasked{$id2tag}{$id2} = 1;
                    # Probably good idea to look for infinite loops here?
                }
            }
            push @tasks, keys %newTasks;
        }
        last unless ($workDone);
    }


    my %nsns; # local cache for token -> name lookup of namespaces
    my %final;
    while (my ($taskTag, $bpDat) = each %bestPaths) {
        for my $id2 (1..$maxID) {
            next if ($id2 == $seed);
            my $path = $bpDat->[ $id2 ];
            my ($sc, $ids, $special) = @{$path};
            next if ($sc == -2 || $#{$ids} < 1);
            my ($n2, $ns2) = $self->node4id($id2);
            # The text strings we need to build read from right to left
            my @rev = reverse @{$ids};
            # Get the chain of authorities:
            my @auths = map {$edges->{$rev[$_-1]}{$rev[$_]}[1]} (1..$#rev);

            # Are there between namespaces / nodes?
            my (@nss, @nodes);
            if ($#rev > 1) {
                # This path is more than one edge (more than two nodes)
                # Remove the first and last nodes:
                pop @rev; shift @rev;
                # The remainder represent the "between" nodes
                map { my ($n, $ns) = $self->node4id($_);
                      my $nsn = $nsns{$ns} ||= $ad->namespace_name($ns);
                      push @nss, $nsn; 
                      push @nodes, $n; } @rev;
            }
            # my $nsn2 = $nsns{$ns2} ||= $ad->namespace_name($ns2);
            $final{$taskTag}[$id2] =
                [ $n2, $ns2, $sc, join(' < ', @auths),
                  join(' < ', @nss)   || '', join(' < ', @nodes) || ''];#, $special];
        }
    }

    $self->{PATH_CACHE}[$seed] = \%final unless ($opts);
    $self->benchend;
    return \%final;
}

sub _localpath_to_text {
    my $self = shift;
    my $path = shift;
    return sprintf("%3d [%s]\n       {%s}\n", $path->[0], join(' > ', map { 
        "$_->[1]:$_->[0]"
    } map { [$self->node4id($_)] } @{$path->[1]}), join(', ', map {"$_:".$path->[2]{$_}}
                           sort keys %{$path->[2]}));
}

my $pathHead = ['Terminus', 'TNS', 'Score', 'Authority',
                'Internal NS', 'Internal Node'];
sub paths_to_text {
    my $self = shift;
    $self->benchstart;
    my ($paths, $simp) = @_;
    my @rows;
    if ($simp) {
        @rows = sort { $b->[2] <=> $a->[2] ||
                           uc($a->[0]) cmp uc($b->[0]) } @{$paths};
    } else {
        my @cols =  qw(term_out ns_out matched auth ns_between term_between);
        foreach my $row (@{$paths}) {
            my @mapped = map { $row->{$_} } @cols;
            push @rows, \@mapped;
        }
        @rows = sort { $b->[2] <=> $a->[2] ||
                           uc($a->[0]) cmp uc($b->[0]) } @rows;
    }
    $self->benchend;
    return $self->text_table(\@rows, $pathHead);
}

sub text_table { return shift->denorm->text_table( @_ ) }


#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
1;


=pod Notes



select 'select * from now(); vacuum analyze verbose ' || relname || '; select * from now();' from v_xid where xid > 120000000;


SELECT 'ALTER TABLE ' || tablename || ' SET TABLESPACE onehalftb;' AS SQL
  FROM pg_tables where schemaname = 'public';

SELECT 'ALTER INDEX ' || indexname || ' SET TABLESPACE onehalftb;' AS SQL
  FROM pg_indexes where schemaname = 'public';



  seqname  |    edge    |  mapping  |           snapshot            
-----------+------------+-----------+-------------------------------
 534122749 |  969550418 | 459661465 | 2009-09-01 11:53:50.391588-04
 603932350 | 1089367783 | 522398207 | 2010-07-16 12:12:49.497589-04

gas -warn -cloudwarn writeDelete -ageall '9 Sep 2009' -ns1 aps -populate 40 -scramble -ns2 go,pmid,cdd,btfo,msig,ipr,set,aad,ll,xont -idlist HG_U133A_APS.list

gas -warn -cloudwarn write_delete -ageall '9 Sep 2009' -ns2 go,pmid,cdd,btfo,msig,set,aad -ns1 aps -populate 30 -scramble -idlist

gas -warn -cloudwarn write_delete -ageall '10:10am Sep 1' -mode simple -addtaxa -adddesc -ns1 aps -populate 30 -scramble -idlist

LOC16065 (Mm) immunoglobulin heavy chain (S107 family)
  VERY long delay when calculating around it. See if it can be
  trimmed up for speed


gas -warn -idlist HumanMouseRat_LL.list -ns1 ll -ns2 msig,go,pmid,ipr,cdd -age '4pm Sep 11 2009' -cloudage '9 sep 2009' -populate 60 -scramble


gas -warn -ageall '9 Sep 2009' -ns1 aps -populate 20 -scramble -ns2 go,pmid,cdd,btfo,msig,ipr,set,aad,ll,xont -idlist 

 select * from v_wait where minutes != 0; select * from newconversion limit 10; select * from newclouds limit 10; select * from newmaps limit 10; select * from newdescription limit 10; select * from newparentage limit 10; select * from newparents limit 10; select * from newchildren limit 10; select * from activity;

=cut

