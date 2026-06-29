// Package store is the durable home of a partition: the write-ahead log that
// makes every frontier mutation crash-safe, the periodic checkpoint that folds
// the log into a .meguri file through package format, and the recovery path that
// rebuilds the live engine state from the last checkpoint plus the log tail. It
// is where the engine's hot state and the cold .meguri file meet, so it imports
// both this module's format package and its data model, with no import cycle:
// store depends on format, format depends only on the top-level data model.
//
// This is the M6 milestone. The package is a placeholder until then.
package store
