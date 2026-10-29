package store

// JournalVersion is the journal file format this build reads and writes.
// It lives outside journal.go (unix only) so every platform can report it.
const JournalVersion = 1
