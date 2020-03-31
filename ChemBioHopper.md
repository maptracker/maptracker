### Chem-Bio Hopper

Chem-Bio Hopper (CBH) was written to allow conversion of genes into
compounds, and compounds into genes, via analysis of the
[MapTracker][MT] graph database. Compounds were clustered by
de-salted, de-isomerized forms, while biological entities were mapped
to Homologene loci:

![Querying MapTracker][Theory]

In action, the tool was primarily used "behind the scenes" either via
script or a set of Perl modules. However, the tool could also be
accesed interactively via a CGI GUI:

![CBH GUI][GUI]

In addition to providing a starting point for the "hop", the
researcher could also define various chemical potency
limits. Interactive usage generated two outputs, an HTML report and an
Excel workbook.

The report summarized the query (either the gene or, here, a compound):

![CBH Summary][Summary]

... as well as the reported assays that connected the query to "the
other side". MapTracker provided integration of multiple sources for
compounds, biological entities, and compound-protein assay results:

![CBH assays][Assays]

The Excel report provided the same information, but in a form more
easily portable and filterable by the researcher. Gene-centric:

![Gene-centric Excel View][Homologene]

... and compound-centric:

![Compound-centric Excel View][Compounds]

... worksheets were provided, as well as a bibliography of
publications from which the assay findings had been extracted:

![Bibliography][PubMed]

The primary driver for creating this tool was support of chemogenomic
screens. These screens utilized a library of several hundred bioactive
compounds, each determined to hit multiple known gene targets. The
library would be applied to cell lines, which would be observed over
time by high content screening (microscopic observation in multi-well
plates) to identify pre-determined phenotypes (changes in morphology,
motility, division, etc).

Compounds that produced a phenotype of interest were expanded into
their "potential gene target set". This in turn was compared to the
geneset of compounds that did NOT invoke the phenotype. Enrichment
analysis via Fishers Exact Test was then performed to identify which
of the presumptive gene targets might be the underlying biological
driver for the phenotype.

However, this tool was popular for general use, as it was tied into
our compound inventory system. A researcher could provide a gene of
interest and recover not only potential tool compounds, but also how
much of each compound was already held in stock.

[MT]: ../README.md
[Theory]: img/CBH-Theory.png
[GUI]: img/CBH-GUI.png
[Summary]: img/CBH-Summary.png
[Assays]: img/CBH-Assays.png

[Compounds]: img/CBH-Excel-Compounds.png
[Homologene]: img/CBH-Excel-Homologene.png
[PubMed]: img/CBH-Excel-PubMed.png
