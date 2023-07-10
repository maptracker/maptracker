### MapTracker Graph Database

MapTracker is a massive graph database - over a terabyte on disk, 1.2B
nodes, 2.0B edges, 3.5B metadata assignments. It's used at BMS to
"resolve X to Y" - that is, given an object of "type X" find - in a
qualitative way - all "related" objects of "type Y". This is done
using an aggresively normalized triple store and a large set of rules
that dictate what kinds of edges are reasonable to traverse when going
from X to Y.

MapTracker is generally not used "on its own", but is rather a
component in other tools. Examples avaiable here are:

* [Chem-Bio Hopper][CBH] - "Hop" from biology to chemistry, or
  vice-versa, using published chemical activities
* [Hypergeometric Affy][HA] - Given a set of "interesting" (generally
  overexpressed) Affymetrix probesets, run Fisher's Exact Test to
  identify ontologies that appear overrepresented in the set.
* [Standardize Gene][SG] - Given a set of gene identifiers (eg
  symbols), attempt to determine what they "really are" (ie, given
  messy gene symbols, convert to rigorous gene accessions)

The schema ([tables][schema]) is relatively simple. What has made
MapTracker particularly powerful is:

* [Careful normalization][loaders] of loaded data
* Segregation of nodes into namespaces. Ameliorates collisions,
  particularly with identifiers like gene symbols
* Exhaustive logic defining valid connections between
  `X-to-Y`. Example, [RNA to probeset][ARAPS]
* [Generic transitive logic][chains] that lets `X-to-Y` be
  automatically merged with `Y-to-Q` and` Q-to-W` in order to find
  `X-to-W`. Such "chains" allow only fundamental connections to be
  defined yet allow the network to be (safely, rationally) explored
  far beyone its expected "neighbors"

The image below is an auto-generated network, created by sampling
20,000 random edges from the database (created by
[exploreSelf.pl][exploreSelf]). It represents, at a high level, the
common node-edge-node triples held by the database.

![Network overview][Overview]

All edges are part of a controlled vocabulary. Most (though not all) are directional. The edges in the above sample include:

![Edge overview][Edges]

* [BMS Public Disclosure approval](PubD-Disclosure-Approval.md)

[CBH]: ChemBioHopper.md
[HA]: HypergeometricAffy.md
[SG]: MapTracker/standardizeGene.pl
[schema]: https://github.com/maptracker/maptracker/blob/master/MapTracker/DBI/Schema.pm#L100
[loaders]: https://github.com/search?q=repo%3Amaptracker%2Fmaptracker+extension%3Apl+filename%3Aload&type=Code&ref=advsearch&l=&l=
[ARAPS]: https://github.com/maptracker/maptracker/blob/master/MapTracker/AccessDenorm.pm#L12728
[chains]: https://github.com/maptracker/maptracker/blob/master/MapTracker/AccessDenorm.pm#L7522
[exploreSelf]: MapTracker/exploreSelf.pl
[Overview]: img/MapTrackerNamespaces.png
[Edges]: img/MapTrackerEdgeTypes.png
