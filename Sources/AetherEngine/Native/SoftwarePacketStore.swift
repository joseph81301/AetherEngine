import Foundation

/// The operations `SoftwarePacketReadAhead` needs from its packet store. `SoftwarePacketDiskFIFO`
/// keeps the records in session files; `SoftwarePacketMemoryFIFO` keeps the same records in
/// purgeable memory, which is what tvOS uses (see there).
protocol SoftwarePacketStore: AnyObject, Sendable {
    var snapshot: SoftwarePacketDiskFIFO.Snapshot { get }
    /// Whether consumed records stay readable as history (cached seeks, published residency).
    var retainsConsumedPackets: Bool { get }
    @discardableResult func append(_ data: Data) throws -> SoftwarePacketDiskFIFO.Cursor
    func pop() throws -> Data?
    func restore(to cursor: SoftwarePacketDiskFIFO.Cursor) throws
    func readHistory(from cursor: SoftwarePacketDiskFIFO.Cursor, visit: (Data) throws -> Bool) throws
    func trimConsumed(toByteBudget budget: Int) throws
    func reset() throws
    func close() throws
    /// Drops history the system reclaimed behind the store's back. Returns whether anything went.
    func evictPurgedHistory() -> Bool
    /// The read-ahead stopped reading this store to play another stretch of the source, and keeps
    /// it for a later seek back into it (`restore`). A store whose unread records cost memory makes
    /// them reclaimable here; the disk store has nothing to give back.
    func park()
}

extension SoftwarePacketStore {
    func evictPurgedHistory() -> Bool { false }
    func park() {}
}

extension SoftwarePacketDiskFIFO: SoftwarePacketStore {}
