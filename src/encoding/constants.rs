//! Constants for the SQLite changeset/patchset binary format.

/// Operation codes used in the binary format.
pub mod op_codes {
    /// INSERT operation code.
    pub const INSERT: u8 = 0x12;
    /// DELETE operation code.
    pub const DELETE: u8 = 0x09;
    /// UPDATE operation code.
    pub const UPDATE: u8 = 0x17;
}

/// Table format markers.
pub mod markers {
    /// Changeset table marker ('T').
    pub const CHANGESET: u8 = b'T';
    /// Patchset table marker ('P').
    pub const PATCHSET: u8 = b'P';
}
