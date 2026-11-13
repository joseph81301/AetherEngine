import Foundation
import Testing
@testable import AetherEngine

/// A seek outside the run being played parks that run instead of discarding it, so jumping ahead
/// and then back to video already played plays it from the store, without the source.
@Suite("Software read-ahead keeps played runs across seeks", .offCooperativePool)
struct SoftwarePacketReadAheadPlayedRunsTests {

    /// 25 fps video, a keyframe every second, `seconds` long. Each packet's bytes and file position
    /// name its index, so a read can be checked for the right picture. Moves like a demuxer: to the
    /// keyframe at or before a target, and only for a read that is still current.
    final class SeekableSource: @unchecked Sendable {
        let packets: [SoftwareStoredPacket]
        private let lock = NSLock()
        private var index = 0
        private var reads: [Int] = []

        init(seconds: Int) {
            packets = (0..<(seconds * 25)).map { i in
                SoftwareStoredPacket(
                    pts: Int64(i * 40), dts: Int64(i * 40), duration: 40, position: Int64(i * 2000),
                    streamIndex: 0, flags: i % 25 == 0 ? 0x1 : 0, timeBaseNumerator: 1,
                    timeBaseDenominator: 1000, bytes: Data(repeating: UInt8(i & 0xff), count: 2000),
                    sideData: [])
            }
        }

        /// The current-read check runs under the same lock as `move`, so a read either takes a
        /// packet before the move or is refused after it.
        func next(_ isCurrent: @Sendable () -> Bool) throws -> SoftwareStoredPacket? {
            try lock.withLock {
                guard isCurrent() else { throw CancellationError() }
                guard index < packets.count else { return nil }
                reads.append(index)
                index += 1
                return packets[index - 1]
            }
        }

        func move(toSeconds seconds: Double) {
            lock.withLock {
                let target = min(packets.count - 1, Int(seconds * 25))
                index = target - target % 25
            }
        }

        var readIndices: [Int] { lock.withLock { reads } }
    }

    final class Stores: @unchecked Sendable {
        private let lock = NSLock()
        private var made: [SoftwarePacketMemoryFIFO] = []
        func make(kernelMayPurge: Bool) -> SoftwarePacketMemoryFIFO {
            let store = SoftwarePacketMemoryFIFO(chunkTargetBytes: 16 * 1024, historyCapBytes: 64 << 20,
                                                 kernelMayPurge: kernelMayPurge)
            lock.withLock { made.append(store) }
            return store
        }
        var all: [SoftwarePacketMemoryFIFO] { lock.withLock { made } }
    }

    private func makeReadAhead(_ source: SeekableSource, stores: Stores,
                               kernelMayPurge: Bool = false) -> SoftwarePacketReadAhead {
        SoftwarePacketReadAhead(
            video: .init(index: 0, numerator: 1, denominator: 1000), audio: nil,
            byteBudget: 64 << 20, forwardSeconds: 2, initialSourceClock: 0,
            fifo: stores.make(kernelMayPurge: kernelMayPurge),
            makeStore: { stores.make(kernelMayPurge: kernelMayPurge) }
        ) { isCurrent in try source.next(isCurrent) }
    }

    /// What the host does around a seek: move the source on a miss, and behind a resumed parked
    /// run once the seek has landed.
    @discardableResult
    private func seek(_ readAhead: SoftwarePacketReadAhead, _ source: SeekableSource,
                      to seconds: Double) throws -> SoftwarePacketReadAhead.SeekPreparation {
        let token = readAhead.beginSeek(to: seconds)
        let outcome = try readAhead.prepareSeekOutcome(token, to: seconds)
        let epoch = readAhead.currentSourceEpoch
        switch outcome {
        case .miss:
            source.move(toSeconds: seconds)
            readAhead.endSeek(token, sourceClock: seconds)
        case .hit:
            readAhead.endSeek(token, sourceClock: seconds)
        case .parkedHit(let resume):
            readAhead.endSeek(token, sourceClock: seconds)
            if let resume {
                source.move(toSeconds: resume)
                readAhead.finishSourceReposition(epoch, landed: true)
            }
        }
        return outcome
    }

    private func read(_ readAhead: SoftwarePacketReadAhead) throws -> SoftwareStoredPacket {
        try #require(try readAhead.read(maximumWait: 10))
    }

    @Test("a jump ahead and back into video played plays it from the store, not the source")
    func jumpBackIntoParkedRun() throws {
        let source = SeekableSource(seconds: 30)
        let stores = Stores()
        let readAhead = makeReadAhead(source, stores: stores)
        defer { readAhead.close() }
        readAhead.start()
        for i in 0..<75 { #expect(try read(readAhead).pts == Int64(i * 40)) }

        #expect(try seek(readAhead, source, to: 10) == .miss)
        #expect(try read(readAhead).pts == 10_000)
        for _ in 0..<24 { _ = try read(readAhead) }
        let readsBefore = source.readIndices.count

        let outcome = try seek(readAhead, source, to: 1)
        guard case .parkedHit(let resume?) = outcome else {
            Issue.record("expected a parked-run hit, got \(outcome)"); return
        }
        let first = try read(readAhead)
        #expect(first.pts <= 1000)
        #expect(first.bytes == source.packets[Int(first.pts / 40)].bytes)
        // The source was asked again only from the parked run's last keyframe on, never for the
        // stretch the run already holds.
        let reread = source.readIndices.dropFirst(readsBefore)
        #expect(reread.allSatisfy { $0 >= Int(resume * 25) })
    }

    @Test("playing on past a resumed run's end continues with no packet repeated or missing")
    func readOnPastResumedRunEnd() throws {
        let source = SeekableSource(seconds: 30)
        let stores = Stores()
        let readAhead = makeReadAhead(source, stores: stores)
        defer { readAhead.close() }
        readAhead.start()
        for _ in 0..<75 { _ = try read(readAhead) }
        try seek(readAhead, source, to: 12)
        for _ in 0..<10 { _ = try read(readAhead) }

        guard case .parkedHit(let resume?) = try seek(readAhead, source, to: 2) else {
            Issue.record("expected a parked-run hit"); return
        }
        var last = try read(readAhead).pts
        // Well past the run's newest packet: the source move and the duplicate drop meet there.
        let until = Int64((resume + 4) * 1000)
        while last < until {
            let next = try read(readAhead)
            #expect(next.pts == last + 40, "after \(last) came \(next.pts)")
            #expect(next.bytes == source.packets[Int(next.pts / 40)].bytes)
            last = next.pts
        }
    }

    @Test("a jump to a stretch no run holds is still a miss")
    func unplayedSpotIsAMiss() throws {
        let source = SeekableSource(seconds: 30)
        let stores = Stores()
        let readAhead = makeReadAhead(source, stores: stores)
        defer { readAhead.close() }
        readAhead.start()
        for _ in 0..<50 { _ = try read(readAhead) }
        try seek(readAhead, source, to: 10)
        _ = try read(readAhead)
        #expect(try seek(readAhead, source, to: 20) == .miss)
        #expect(try read(readAhead).pts == 20_000)
    }

    @Test("past the parked-run limit the oldest run goes, and a jump back to it is a miss")
    func oldestParkedRunIsDropped() throws {
        let source = SeekableSource(seconds: 40)
        let stores = Stores()
        let readAhead = makeReadAhead(source, stores: stores)
        defer { readAhead.close() }
        readAhead.start()
        for _ in 0..<50 { _ = try read(readAhead) }
        for target in [10.0, 18, 26] {
            #expect(try seek(readAhead, source, to: target) == .miss)
            _ = try read(readAhead)
        }
        // Three runs left behind, two kept: the first one (0 to 2 s) is gone.
        #expect(SoftwarePacketReadAhead.maximumParkedRuns == 2)
        #expect(try seek(readAhead, source, to: 1) == .miss)
        #expect(try read(readAhead).pts == 1000)
    }

    @Test("a parked run the system took back is a miss, and playback goes on from the source")
    func purgedParkedRunIsAMiss() throws {
        let source = SeekableSource(seconds: 30)
        let stores = Stores()
        let readAhead = makeReadAhead(source, stores: stores, kernelMayPurge: true)
        defer { readAhead.close() }
        readAhead.start()
        for _ in 0..<50 { _ = try read(readAhead) }
        try seek(readAhead, source, to: 10)
        _ = try read(readAhead)
        let parkedStore = try #require(stores.all.first)
        parkedStore.purgeHistoryForTesting()
        #expect(try seek(readAhead, source, to: 1) == .miss)
        #expect(try read(readAhead).pts == 1000)
    }

    @Test("without a store factory a miss still discards the run (one-run behaviour)")
    func noFactoryKeepsOneRun() throws {
        let source = SeekableSource(seconds: 30)
        let readAhead = SoftwarePacketReadAhead(
            video: .init(index: 0, numerator: 1, denominator: 1000), audio: nil,
            byteBudget: 64 << 20, forwardSeconds: 2, initialSourceClock: 0,
            fifo: SoftwarePacketMemoryFIFO(chunkTargetBytes: 16 * 1024, historyCapBytes: 64 << 20,
                                       kernelMayPurge: false)
        ) { isCurrent in try source.next(isCurrent) }
        defer { readAhead.close() }
        readAhead.start()
        for _ in 0..<50 { _ = try read(readAhead) }
        try seek(readAhead, source, to: 10)
        _ = try read(readAhead)
        #expect(try seek(readAhead, source, to: 1) == .miss)
    }

    // MARK: - Duplicate rule

    @Test("a packet at or before the run's newest one is a duplicate by file position")
    func duplicateByPosition() {
        let mark = SoftwarePacketReadAhead.StoredTail.Mark(position: 5000, dts: 200, pts: 200)
        #expect(SoftwarePacketReadAhead.StoredTail.isDuplicate(position: 5000, dts: 200, pts: 200, of: mark))
        #expect(SoftwarePacketReadAhead.StoredTail.isDuplicate(position: 4000, dts: 160, pts: 160, of: mark))
        #expect(!SoftwarePacketReadAhead.StoredTail.isDuplicate(position: 6000, dts: 240, pts: 240, of: mark))
    }

    @Test("without positions a reordered picture after the newest one is new, by decode time")
    func reorderedPictureIsNew() {
        // Decode order ... P(pts 400, dts 280), then B(pts 320, dts 320): the B is new although its
        // presentation time is before the P's.
        let mark = SoftwarePacketReadAhead.StoredTail.Mark(position: -1, dts: 280, pts: 400)
        #expect(!SoftwarePacketReadAhead.StoredTail.isDuplicate(position: -1, dts: 320, pts: 320, of: mark))
        #expect(SoftwarePacketReadAhead.StoredTail.isDuplicate(position: -1, dts: 280, pts: 400, of: mark))
    }

    @Test("with neither position nor decode time the presentation time decides")
    func presentationTimeFallback() {
        let mark = SoftwarePacketReadAhead.StoredTail.Mark(position: -1, dts: Int64.min, pts: 400)
        #expect(SoftwarePacketReadAhead.StoredTail.isDuplicate(position: -1, dts: Int64.min, pts: 400, of: mark))
        #expect(!SoftwarePacketReadAhead.StoredTail.isDuplicate(position: -1, dts: Int64.min, pts: 440, of: mark))
    }
}
