### Hypergeometric Affy

Our pre-NGS workflow, like many other biotech organizations, relied
heavily on Affymetrix oligo-based profiling assays. One of the most
common requests was to determine which ontology terms appeared to be
over-represented in a set of over- or under-expressed
probesets. AffyHyperGO utilizes the [MapTracker][MT] graph database to
expand the number of ontologies available to a set of Affymetrix probesets.

MapTracker aids in this analysis by aggresively connecting probesets
to "distant" objects that may have additional ontological
annotation. These "enhanced" connections are grown primarily from:

* Additional transcript annotations - MapTracker contains not just RNA
  assignments by Affymetrix, but also _de novo_ in-house alignments of
  probe set oligos to several reference databases (primarily RefSeq
  and Ensembl). These searches identify additional targets
* "Chaining" to additional protein targets - Ontology assignments are
  usually made at the protein or gene level. MapTracker chains
  proteins from different data sources (RefSeq, Ensembl,
  SwissProt/UniProt, IPI) via in-house recriprocal best matching.

When ontologies are organized as directed graphs, AffyHyperGO will
present "trimmed" results that initially hide parent or child nodes
that have lower significance than the "driver" node. For example, the
following query (a public Huntington dataset) finds 54 significant
Biological Process hits, but initially hides 50 of them as being
"driven" by the other 4:

![Driven hits][GO]

In addition to "traditional" ontologies such as GO, the tool also
incorporates atypical ontologies and categories including:

* WikiPathways
* PubMed
* CDD
* Wikipedia

![Other ontologies][Other]

The interactive HTML reports are also accompanied by an Excel workbook
summarizing the hits for each query.

[MT]: ../README.md
[GO]: img/GSEA-GO.png
[Other]: img/GSEA-OtherOntologies.png
