# shogun_search

Learning the principle of search engine. This is the first time I've written Rust.

A search engine written in Rust.

### Current Features:

* Build inverted index (not optimized for memory usage)
* Perform a fuzzy search (memory usage is low and speed is okay)
* Ranking the results (based on the number of occurrences of the search term and the frequency of the term in all indexed articles)

Plans to update the real-time indexing of new articles soon (does not rebuild the index, but maintains a new index in memory, does not yet support incremental update of the index)
