import Foundation

struct SoftwarePacketCacheTimingRecord: Sendable {
    let pts: Int64
    let duration: Int64
    let key: Bool
    let cursor: SoftwarePacketDiskFIFO.Cursor
}
struct SoftwarePacketCacheTimingBatch: Sendable {
    let invalid: Bool
    let records: [SoftwarePacketCacheTimingRecord]
    var oldestUsableChunk: UInt64? = nil
    var calibrationPressure = false
}
/// Foundation-only seam keeps the existing standalone cache tests independent of FFmpeg.
protocol SoftwarePacketCacheTimingSource: AnyObject, Sendable {
    func append(_ packet: SoftwareStoredPacket, cursor: SoftwarePacketDiskFIFO.Cursor, epoch: UInt64)
    func drain(epoch: UInt64) -> SoftwarePacketCacheTimingBatch
    func seconds(position: Int64, epoch: UInt64) -> Double?
    func evict(before chunk: UInt64, epoch: UInt64)
}

/// Compressed VOD packet prefetch, independent of renderer pacing. The producer owns source reads;
/// the consumer gets byte-identical packet envelopes from a bounded disk FIFO. No main-thread I/O.
/// Cached seeks move only the consumer cursor. A source reposition has a separate epoch, so an
/// in-flight producer packet is neither lost nor duplicated when replaying retained data.
final class SoftwarePacketReadAhead: @unchecked Sendable {
    struct Stream: Sendable {
        let index: Int32
        let numerator: Int32
        let denominator: Int32
        var audioTiming: SoftwareAudioPacketTiming? = nil
    }
    struct Snapshot: Sendable {
        let packetCount: Int
        let bytes: Int
        let residentBytes: Int
        let frontier: Double?
        let generation: UInt64
        let seeking: Bool
        let sourceEnded: Bool
        let sourceEpoch: UInt64
        let cacheSeekHits: UInt64
        let cacheSeekMisses: UInt64
    }
    enum ReadError: Error {
        case interrupted, closed, corruptFIFO
        /// The optional wait expired without consuming a packet; the source is still open.
        case idle
        /// The source could not be moved to the end of a resumed parked run: that run plays what it
        /// holds, then reads end with this.
        case sourceRepositionFailed
    }

    private let condition = NSCondition()
    /// Never acquired by a main-thread API. Serializes reset with append/pop, including the
    /// generation check, so an old consumer cannot pop and discard the first NEW-generation packet.
    private let operations = NSLock()
    /// The run being played: the stretch of the source the consumer reads and the producer extends.
    /// Swapped only with `operations` and `condition` both held, so either lock gives a stable read.
    private var fifo: any SoftwarePacketStore
    /// Makes an empty store for a new run. With one, a seek outside the run parks the run instead of
    /// discarding it (`parked`), and a later seek back into it plays from it again. nil keeps the
    /// one-run behaviour: a miss resets the store.
    private let makeStore: (@Sendable () throws -> any SoftwarePacketStore)?
    /// A run the consumer left for a seek outside it: its store and the metadata that finds a seek
    /// target in it, frozen as they were when it was parked. Newest last.
    private struct Span {
        let store: any SoftwarePacketStore
        var keyframes: [Keyframe]
        var videoCoverage: SoftwarePacketCoverage
        var audioCoverage: SoftwareAudioPacketCoverage
        var presentationCoverage: SoftwareVideoPacketCoverage?
        var residency: SoftwarePacketResidency
        var storedVideoSeconds: Double?
        var ended: Bool
        var tail: StoredTail
        /// What the store still holds against the session's byte budget (a parked memory store
        /// holds only volatile chunks, so nothing).
        var residentBytes: Int
    }
    /// The newest packet the producer stored per stream in the active run: where a source that was
    /// repositioned to a parked run's end must start appending again, and what before it is a
    /// duplicate of what the run already holds.
    struct StoredTail: Sendable, Equatable {
        struct Mark: Sendable, Equatable {
            var position: Int64
            var dts: Int64
            var pts: Int64
        }
        var video: Mark?
        var audio: Mark?

        /// Whether `packet` is one the run already holds: at or before the newest stored packet of
        /// its stream, by file position where both have one, else by decode then presentation
        /// time. Per stream, because a container's interleave need not keep positions in order
        /// across streams, and by decode order, because a reordered picture after the newest
        /// stored one can carry an earlier presentation time and still be new.
        static func isDuplicate(position: Int64, dts: Int64, pts: Int64, of mark: Mark) -> Bool {
            if position >= 0, mark.position >= 0 { return position <= mark.position }
            if dts != Int64.min, mark.dts != Int64.min { return dts <= mark.dts }
            if pts != Int64.min, mark.pts != Int64.min { return pts <= mark.pts }
            return false
        }
    }
    private var parked: [Span] = []
    /// Two parked runs besides the active one: "jump ahead, then back" and one more stretch.
    static let maximumParkedRuns = 2
    private var parkedResidentBytes = 0
    private var storedTail = StoredTail()
    /// Set while the source is being moved to a resumed parked run's end; packets up to `tail` are
    /// already in the run and are dropped instead of stored.
    private var duplicatesBefore: StoredTail?
    /// The host still has to move the source for this epoch (`finishSourceReposition`). Until then
    /// the producer stays parked even after the seek itself has ended.
    private var pendingSourceReposition = false
    private var parkedRunHits: UInt64 = 0
    private let readSource: @Sendable (@Sendable () -> Bool) throws -> SoftwareStoredPacket?
    private let beforeConsumerOperation: (@Sendable () -> Void)?
    private let video: Stream
    private let audio: Stream?
    private let byteBudget: Int
    private let forwardSeconds: Double
    /// Live forwarding keeps only reservoir timing, without seek anchors or retained coverage.
    private let forwardOnly: Bool
    /// Producer thread only. See `start()` for why this is a class the producer moves itself.
    private var producerQoS: qos_class_t = QOS_CLASS_USER_INITIATED
    /// Mirror of `producerQoS` for the consumer's diagnostics, under `condition`.
    private var reportedProducerQoS: qos_class_t = QOS_CLASS_USER_INITIATED
    /// Consumer threads currently parked in `read()`. Producer-visible, under `condition`.
    private var waitingConsumers = 0
    /// True while the producer sits in its park wait, under `condition`. A reader on another thread
    /// only gets the lock while the producer is inside `wait()`, so a true read PROVES the park; a
    /// counter that stopped moving for a while only suggests it, and a loaded machine breaks that.
    private var producerParked = false
    var producerParkedForTesting: Bool {
        condition.lock(); defer { condition.unlock() }
        return producerParked
    }
    private var consumerWaitEvents: UInt64 = 0
    #if DEBUG
    /// Captures active waits and completed wakeups under the source condition for idle-loop tests.
    var consumerWaitStateForTesting: (waiting: Int, completed: UInt64) {
        condition.lock(); defer { condition.unlock() }
        return (waitingConsumers, consumerWaitEvents)
    }
    #endif
    private var consumerBlockedSeconds: Double = 0
    private var lastStarvationLog: DispatchTime?
    private var generation: UInt64 = 0
    private var sourceEpoch: UInt64 = 0
    private var sourceRepositioning = false
    private var cacheSeekHits: UInt64 = 0
    private var cacheSeekMisses: UInt64 = 0
    private var resetPending = false
    private var seeking = false
    private var closed = false
    private var started = false
    private var ended = false
    private var failure: Error?
    private enum FailureOrigin: Int {
        case none = 0, sourceRead = 1024, storage = 2048, envelope = 4096
    }
    private var failureOrigin: FailureOrigin = .none
    // Called under condition. An upstream read failure says nothing about the integrity of
    // envelopes already committed to the FIFO. Keep those usable; read() still throws the
    // original failure after they drain, and the producer stays parked until a cold seek.
    private var retainedPacketsUsableLocked: Bool {
        failure == nil || failureOrigin == .sourceRead
    }
    private var lastCoverageDiagnosticFlags: Int?
    private var count = 0
    private var bytes = 0
    private var residentBytes = 0
    private var sourceClock: Double
    /// Newest video presentation time handed to the store, and the one the consumer last took out
    /// of it. Their distance is the reservoir the producer is filling, and unlike the coverage
    /// frontier it survives a hole and a late timestamp, which is what keeps the forward-second
    /// limit in force on material the coverage model cannot describe.
    private var storedVideoSeconds: Double?
    private var consumedVideoSeconds: Double?
    private var videoCoverage: SoftwarePacketCoverage
    private var audioCoverage: SoftwareAudioPacketCoverage
    private var presentationCoverage: SoftwareVideoPacketCoverage?
    private struct Keyframe {
        let seconds: Double
        let cursor: SoftwarePacketDiskFIFO.Cursor
    }
    private var keyframes: [Keyframe] = []
    private let maximumKeyframes = 65_536
    private var residency = SoftwarePacketResidency()
    private var timingResidency = SoftwarePacketResidency()
    private let cacheTiming: (any SoftwarePacketCacheTimingSource)?
    private var cacheTimingInvalid = false
    /// AV1 through dav1d (`SWKeyframeSnap`): a hit restores to the last retained keyframe at or before
    /// the target, not to the one before it. An AV1 keyframe opens on its own (no leading pictures, and
    /// a forward key frame is flagged only when it refreshes every reference, #931), so the earlier
    /// keyframe only made dav1d decode a whole GOP more: on an Apple TV 4K (A15) a hit onto a
    /// keyframe of 4K AV1 held the clock 1.8 to 2.5 s, and 0.13 to 0.18 s restored to that keyframe.
    private let anchorsOnLastKeyframe: Bool
    private var calibrationPressure = false

    /// Construct only off-main: creating the FIFO touches the temporary volume.
    init(video: Stream, audio: Stream?, byteBudget: Int, forwardSeconds: Double,
         initialSourceClock: Double, fifo: any SoftwarePacketStore,
         videoReorderDepth: Int? = nil,
         cacheTiming: (any SoftwarePacketCacheTimingSource)? = nil,
         coverageRangeCap: Int = 4096,
         anchorsOnLastKeyframe: Bool = false,
         forwardOnly: Bool = false,
         makeStore: (@Sendable () throws -> any SoftwarePacketStore)? = nil,
         beforeConsumerOperation: (@Sendable () -> Void)? = nil,
         readSource: @escaping @Sendable (@Sendable () -> Bool) throws -> SoftwareStoredPacket?) {
        self.cacheTiming = forwardOnly ? nil : cacheTiming
        self.forwardOnly = forwardOnly
        self.anchorsOnLastKeyframe = anchorsOnLastKeyframe
        self.video = video
        self.audio = audio
        self.byteBudget = max(1, byteBudget)
        self.forwardSeconds = max(1, forwardSeconds)
        self.sourceClock = initialSourceClock
        self.fifo = fifo
        // Parking keeps a run's own packet metadata; external cache timing is one ledger per
        // session, so those sessions keep the one-run behaviour.
        self.makeStore = forwardOnly || cacheTiming != nil ? nil : makeStore
        self.videoCoverage = SoftwarePacketCoverage(maximumRangeCount: coverageRangeCap)
        self.audioCoverage = SoftwareAudioPacketCoverage(
            timeBaseNumerator: audio?.numerator ?? 1, timeBaseDenominator: audio?.denominator ?? 1,
            timing: audio?.audioTiming, maximumRangeCount: coverageRangeCap)
        self.presentationCoverage = (forwardOnly ? nil : videoReorderDepth).map {
            SoftwareVideoPacketCoverage(timeBaseNumerator: video.numerator,
                timeBaseDenominator: video.denominator, reorderDepth: $0,
                maximumRangeCount: coverageRangeCap)
        }
        self.beforeConsumerOperation = beforeConsumerOperation
        self.readSource = readSource
    }

    func start() {
        condition.lock()
        guard !started, !closed else { condition.unlock(); return }
        started = true
        condition.unlock()
        // AE#519: a thread we own rather than a DispatchQueue, because a queue's QoS is fixed at
        // creation and this producer's urgency changes INSIDE one long-running loop: elective while
        // the reservoir is deep, latency-critical the moment the consumer can reach it. The consumer
        // blocks on `condition` whenever the store runs dry and `NSCondition` donates no priority,
        // so a permanently demoted producer is an inversion dispatch cannot see, and one that was
        // measured failing to drain the link it had been given.
        let thread = Thread { self.produce() }
        thread.name = "AetherEngine.SoftwarePacketReadAhead.producer"
        thread.stackSize = 1 << 20
        thread.qualityOfService = .userInitiated
        thread.start()
    }

    var snapshot: Snapshot {
        condition.lock(); defer { condition.unlock() }
        applyCacheTimingLocked()
        return Snapshot(packetCount: count, bytes: bytes, residentBytes: residentBytes + parkedResidentBytes,
                        frontier: sourceRepositioning || closed ? nil : frontierLocked(),
                        generation: generation, seeking: seeking, sourceEnded: ended,
                        sourceEpoch: sourceEpoch, cacheSeekHits: cacheSeekHits,
                        cacheSeekMisses: cacheSeekMisses)
    }

    /// The retained video keyframes in source seconds, from the packets' own keyframe flags: where a
    /// cached seek can start (`SWKeyframeSnap`). Copied under the short state lock, like `snapshot`.
    var retainedKeyframeSeconds: [Double] {
        condition.lock(); defer { condition.unlock() }
        return keyframes.map(\.seconds)
    }

    /// Called only by a background sampler. Copy bounded metadata under the short state lock;
    /// timestamp conversion/intersection and keyframe selection happen after releasing it. No I/O
    /// beyond a purgeable-state query per history chunk on the memory store.
    var residentRanges: [ClosedRange<Double>] {
        // A purged history chunk is gone before any restore notices. Never wait for the store:
        // a busy one is checked on the next sample.
        if operations.try() {
            if fifo.evictPurgedHistory() {
                let state = fifo.snapshot
                condition.lock()
                if !closed { copyDiskStateLocked(state) }
                condition.unlock()
            }
            operations.unlock()
        }
        condition.lock()
        applyCacheTimingLocked()
        // Fixed numeric state only, emitted on edges by this background sampler. No URLs,
        // error descriptions or packet content. Bits: closed/reposition/reset/failure,
        // nonretaining/invalid-video/EOF/empty-audio/no-keyframe/unknown-floor,
        // source-failure/storage-failure/envelope-failure/empty-video.
        var flags = failureOrigin.rawValue
        if closed { flags |= 1 }
        if sourceRepositioning { flags |= 2 }
        if resetPending { flags |= 4 }
        if failure != nil { flags |= 8 }
        if !fifo.retainsConsumedPackets { flags |= 16 }
        if presentationCoverage?.isInvalidated == true || cacheTimingInvalid { flags |= 32 }
        if ended { flags |= 64 }
        if audio != nil && audioCoverage.rangeCount == 0 { flags |= 128 }
        if keyframes.isEmpty { flags |= 256 }
        if residency.videoFloor == .infinity || residency.audioFloor == .infinity { flags |= 512 }
        if (presentationCoverage?.rangeCount ?? videoCoverage.rangeCount) == 0 { flags |= 8192 }
        let changed = lastCoverageDiagnosticFlags != flags
        lastCoverageDiagnosticFlags = flags
        defer {
            if changed { EngineLog.emit("[SWCacheCoverage] flags=\(flags)", category: .swPlayback) }
        }
        guard !closed, !sourceRepositioning, !resetPending, retainedPacketsUsableLocked,
              fifo.retainsConsumedPackets, !cacheTimingInvalid else { condition.unlock(); return [] }
        let videoCoverage = self.videoCoverage
        let presentationCoverage = self.presentationCoverage
        let audioCoverage = self.audioCoverage
        let keyframes = self.keyframes
        let videoFloor = cacheTiming == nil ? residency.videoFloor : timingResidency.videoFloor
        let audioFloor = residency.audioFloor
        condition.unlock()
        let videoRanges = presentationCoverage?.rangesSeconds ?? videoCoverage.rangesSeconds(
            timeBaseNumerator: video.numerator, timeBaseDenominator: video.denominator)
        let ranges: [ClosedRange<Double>]
        if let audio {
            ranges = SoftwarePacketCoverage.intersection(videoRanges, audioCoverage.rangesSeconds(
                timeBaseNumerator: audio.numerator, timeBaseDenominator: audio.denominator))
        } else { ranges = videoRanges }
        guard let keyframe = keyframes.lazy.map(\.seconds)
            .filter({ $0 >= videoFloor }).min() else { return [] }
        let floor = max(keyframe, audio == nil ? -.infinity : audioFloor)
        return ranges.compactMap { range in
            let start = max(range.lowerBound, floor)
            return start < range.upperBound ? start...range.upperBound : nil
        }
    }

    /// Main-thread safe: metadata only. Decode/render backpressure still belongs to the old loop.
    func updatePlayhead(_ seconds: Double) {
        guard seconds.isFinite else { return }
        condition.lock()
        sourceClock = seconds
        // Presentation history stays useful for backward cached seeks. Coverage has a fixed range
        // cap; retained keyframe cursors, not a guessed timestamp floor, decide cache eligibility.
        condition.broadcast()
        condition.unlock()
    }

    /// Legacy explicit cold seek. Production first uses beginSeek(to:) + prepareSeek off-main.
    @discardableResult
    func beginSeek() -> UInt64 {
        condition.lock(); defer { condition.unlock() }
        generation &+= 1
        sourceEpoch &+= 1
        sourceRepositioning = true
        seeking = true
        resetPending = true
        clearSourceMetadataLocked()
        condition.broadcast()
        return generation
    }

    /// Main-thread safe: freeze only the decoder/consumer, not the source reader or retained data.
    @discardableResult
    func beginSeek(to seconds: Double) -> UInt64 {
        condition.lock(); defer { condition.unlock() }
        generation &+= 1
        seeking = true
        if seconds.isFinite { sourceClock = seconds }
        condition.broadcast()
        return generation
    }

    /// What a seek found (`prepareSeekOutcome`).
    enum SeekPreparation: Equatable, Sendable {
        /// The active run holds the target: the consumer replays it, the source reads on.
        case hit
        /// A parked run holds the target and is the active run now: the consumer plays from it at
        /// once. When `resumeSourceAt` is set the host moves the source there (the run's last
        /// keyframe) and calls `finishSourceReposition`; packets the run already holds are dropped.
        /// nil: the run reaches the end of the source, nothing more to read.
        case parkedHit(resumeSourceAt: Double?)
        /// Nothing retained covers the target: the host repositions the source to it.
        case miss
    }

    /// Off-main, BEFORE any actual demuxer reposition. true means the target is already retained
    /// and the source needs no move (see `prepareSeekOutcome` for the parked-run case).
    func prepareSeek(_ token: UInt64, to seconds: Double) throws -> Bool {
        switch try prepareSeekOutcome(token, to: seconds) {
        case .hit: return true
        case .parkedHit(let resume): return resume == nil
        case .miss: return false
        }
    }

    /// Off-main, BEFORE any actual demuxer reposition. A hit in the active run does not change
    /// sourceEpoch: an in-flight source read must still be stored afterwards.
    func prepareSeekOutcome(_ token: UInt64, to seconds: Double) throws -> SeekPreparation {
        operations.lock(); defer { operations.unlock() }
        condition.lock()
        guard token == generation, seeking, !closed else {
            condition.unlock(); throw ReadError.interrupted
        }
        applyCacheTimingLocked()
        let hasCoverage = !sourceRepositioning && !resetPending && retainedPacketsUsableLocked
            && seconds.isFinite && (frontierLocked(at: seconds).map { $0 > seconds } ?? false)
        let anchor = hasCoverage ? anchorLocked(in: keyframes, at: seconds) : nil
        condition.unlock()

        if let anchor {
            do {
                try fifo.restore(to: anchor.cursor)
                let state = fifo.snapshot
                condition.lock(); defer { condition.unlock() }
                guard token == generation, !closed else { throw ReadError.interrupted }
                copyDiskStateLocked(state)
                sourceClock = seconds
                // The cursor moved, so the reservoir is measured from the target again. Left at
                // the old high-water mark, a backward hit would read as a full reservoir and a
                // forward one as an empty one.
                consumedVideoSeconds = seconds
                cacheSeekHits &+= 1
                condition.broadcast()
                return .hit
            } catch SoftwarePacketDiskFIFO.Failure.invalidCursor {
                // An expired bookmark is a cache miss, never a playback/disk failure.
            } catch SoftwarePacketDiskFIFO.Failure.retentionDisabled {
                // Keeps callers using the legacy destructive FIFO safe during migration.
            }
        }

        if let resumed = try resumeParkedRunLocked(token, at: seconds) { return resumed }

        // Made before taking the state lock: a disk store creates its session directory.
        let replacement = makeStore.flatMap { try? $0() }
        condition.lock()
        guard token == generation, !closed else {
            condition.unlock()
            if let replacement { DispatchQueue.global(qos: .utility).async { try? replacement.close() } }
            throw ReadError.interrupted
        }
        // The run being left is kept for a seek back into it, and the new one starts in an empty
        // store, in place of the discard below.
        if let replacement { swapActiveRunLocked(to: replacement) }
        sourceEpoch &+= 1
        sourceRepositioning = true
        pendingSourceReposition = false
        duplicatesBefore = nil
        storedTail = StoredTail()
        // Discarding the spool is the WORKER's job, not the seek's. Reset removes every retained
        // chunk one by one, so its cost grows with what the session has kept: measured 4.5 ms at
        // 64 MB, 18.9 ms at 256 MB, 60.9 ms at 512 MB, on a ceiling of 2 GiB. Done inline, a miss
        // got slower the longer the session had been running, for work no seek waits on: the
        // producer parks on `sourceRepositioning` until `endSeek`, and takes the reset first.
        // With a replacement the old run went whole into `parked` (or was closed), and the new
        // store starts empty: nothing to reset.
        resetPending = replacement == nil
        clearSourceMetadataLocked()
        sourceClock = seconds
        cacheSeekMisses &+= 1
        condition.broadcast()
        condition.unlock()
        return .miss
    }

    /// The keyframe a cached seek to `seconds` restores to: the last retained one at or before it,
    /// or the one before that, which supplies open-GOP and audio preroll where it is still
    /// retained, except where a keyframe needs none (`anchorsOnLastKeyframe`).
    private func anchorLocked(in keyframes: [Keyframe], at seconds: Double) -> Keyframe? {
        let candidates = keyframes.filter { $0.seconds <= seconds }.sorted { $0.seconds < $1.seconds }
        return candidates.isEmpty ? nil
            : candidates[max(0, candidates.count - (anchorsOnLastKeyframe ? 1 : 2))]
    }

    /// Called with `operations` held and `condition` not. Looks for a parked run that holds
    /// `seconds` (the same rule as a hit in the active run: coverage past the target and a retained
    /// keyframe at or before it), and if one does, swaps it in: the active run is parked, the
    /// consumer reads the parked one from that keyframe, and the source epoch moves so nothing the
    /// producer has in flight lands in it. nil when no parked run holds the target (or its store
    /// refused the cursor, purged by the system: that run is dropped and the seek is a miss).
    private func resumeParkedRunLocked(_ token: UInt64, at seconds: Double) throws -> SeekPreparation? {
        guard makeStore != nil, seconds.isFinite else { return nil }
        condition.lock()
        guard token == generation, !closed else { condition.unlock(); throw ReadError.interrupted }
        var found: (index: Int, anchor: Keyframe)?
        for index in parked.indices.reversed() {
            let span = parked[index]
            guard let end = frontier(at: seconds, video: span.videoCoverage, audio: span.audioCoverage,
                                     presentation: span.presentationCoverage), end > seconds,
                  let anchor = anchorLocked(in: span.keyframes, at: seconds) else { continue }
            found = (index, anchor)
            break
        }
        condition.unlock()
        guard let (index, anchor) = found else { return nil }
        let span = parked[index]
        do {
            try span.store.restore(to: anchor.cursor)
        } catch SoftwarePacketDiskFIFO.Failure.invalidCursor {
            dropParkedRun(at: index)
            return nil
        } catch SoftwarePacketDiskFIFO.Failure.retentionDisabled {
            dropParkedRun(at: index)
            return nil
        }
        let state = span.store.snapshot
        condition.lock(); defer { condition.unlock() }
        guard token == generation, !closed else { throw ReadError.interrupted }
        parked.remove(at: index)
        parkedResidentBytes -= span.residentBytes
        swapActiveRunLocked(to: span.store)
        // A packet the producer is reading for the run just left must not land in this one.
        sourceEpoch &+= 1
        resetPending = false
        failure = nil; failureOrigin = .none
        keyframes = span.keyframes
        videoCoverage = span.videoCoverage
        audioCoverage = span.audioCoverage
        presentationCoverage = span.presentationCoverage
        residency = span.residency
        timingResidency = SoftwarePacketResidency()
        storedVideoSeconds = span.storedVideoSeconds
        storedTail = span.tail
        ended = span.ended
        copyDiskStateLocked(state)
        sourceClock = seconds
        consumedVideoSeconds = seconds
        cacheSeekHits &+= 1
        parkedRunHits &+= 1
        let resume = span.ended ? nil : span.keyframes.last?.seconds
        if let resume {
            // The source still stands wherever the run just left had it. The producer stays parked
            // until the host has moved it to this run's last keyframe; what it then reads up to the
            // run's newest packet is already here.
            sourceRepositioning = true
            pendingSourceReposition = true
            duplicatesBefore = span.tail
        } else {
            sourceRepositioning = false
            pendingSourceReposition = false
            duplicatesBefore = nil
        }
        condition.broadcast()
        return .parkedHit(resumeSourceAt: resume)
    }

    /// Called with `operations` and `condition` held. Parks the active run (when it holds a
    /// keyframe to seek to and nothing has failed; otherwise its store is closed) and makes `next`
    /// the active store. The caller sets the active run's metadata for `next`.
    private func swapActiveRunLocked(to next: any SoftwarePacketStore) {
        let old = fifo
        if failure == nil, !cacheTimingInvalid, !resetPending, old.retainsConsumedPackets, !keyframes.isEmpty {
            old.park()
            let resident = old.snapshot.residentByteCount
            parked.append(Span(store: old, keyframes: keyframes, videoCoverage: videoCoverage,
                               audioCoverage: audioCoverage, presentationCoverage: presentationCoverage,
                               residency: residency, storedVideoSeconds: storedVideoSeconds, ended: ended,
                               tail: storedTail, residentBytes: resident))
            parkedResidentBytes += resident
        } else {
            DispatchQueue.global(qos: .utility).async { try? old.close() }
        }
        fifo = next
        while parked.count > Self.maximumParkedRuns { dropParkedRunLocked(at: 0) }
        // Parked runs share the session's byte budget with the active one: at least half of it
        // stays for the run being played (a parked memory store holds none, its chunks are volatile).
        while parkedResidentBytes > byteBudget / 2, !parked.isEmpty { dropParkedRunLocked(at: 0) }
    }

    private func dropParkedRun(at index: Int) {
        condition.lock(); defer { condition.unlock() }
        guard parked.indices.contains(index) else { return }
        dropParkedRunLocked(at: index)
    }

    private func dropParkedRunLocked(at index: Int) {
        let span = parked.remove(at: index)
        parkedResidentBytes -= span.residentBytes
        // Closing removes a disk store's files: never on the caller's thread.
        DispatchQueue.global(qos: .utility).async { try? span.store.close() }
    }

    /// Whether a source move the host started for this epoch is still the current one.
    func sourceEpochIsCurrent(_ epoch: UInt64) -> Bool {
        condition.lock(); defer { condition.unlock() }
        return epoch == sourceEpoch && !closed
    }

    var currentSourceEpoch: UInt64 {
        condition.lock(); defer { condition.unlock() }
        return sourceEpoch
    }

    /// The host moved the source for a resumed parked run (`.parkedHit`). On success the producer
    /// carries on from there, dropping what the run already holds; on failure the run plays out
    /// what it holds and then reports the error, as a source read failure does.
    func finishSourceReposition(_ epoch: UInt64, landed: Bool) {
        condition.lock(); defer { condition.unlock() }
        guard epoch == sourceEpoch, !closed, pendingSourceReposition else { return }
        pendingSourceReposition = false
        if landed {
            if !seeking { sourceRepositioning = false }
        } else {
            sourceRepositioning = false
            duplicatesBefore = nil
            failure = ReadError.sourceRepositionFailed
            failureOrigin = .sourceRead
        }
        condition.broadcast()
    }

    /// AE#605: the retained video packets a scrub still at `seconds` needs, keyframe first, or nil
    /// when the store cannot answer it. Any thread except main: it reads the disk.
    ///
    /// Eligibility is the cached seek's own rule, coverage past the target plus a retained keyframe
    /// at or before it, so a still is offered exactly where a commit to that position would be a
    /// cache hit, and the card never shows a picture the seek then has to fetch. Unlike the live
    /// ring there is no clamp at the end: a VOD target past the frontier has a real frame the store
    /// does not hold yet, and the frame before it is the wrong answer. The consumer cursor is not
    /// touched, so playback reads on from where it stood.
    func stillRun(atSeconds seconds: Double, maxPackets: Int, maxSpanSeconds: Double,
                  reorderTail: Int, isCancelled: (() -> Bool)? = nil) -> [SoftwareStoredPacket]? {
        guard seconds.isFinite, maxPackets > 0, isCancelled?() != true else { return nil }
        let deadline = ContinuousClock.now.advanced(by: .milliseconds(750))
        condition.lock()
        applyCacheTimingLocked()
        let eligible = !closed && !sourceRepositioning && !resetPending && retainedPacketsUsableLocked
            && (frontierLocked(at: seconds).map { $0 > seconds } ?? false)
        let anchor = eligible
            ? keyframes.filter { $0.seconds <= seconds }.max { $0.seconds < $1.seconds } : nil
        // The run can be parked under this walk: a parked store refuses or ends it, never misreads.
        let store = fifo
        condition.unlock()
        guard let anchor, seconds - anchor.seconds <= maxSpanSeconds else { return nil }

        var run: [SoftwareStoredPacket] = []
        var reached = false
        var tail = reorderTail
        var overflow = false
        do {
            try store.readHistory(from: anchor.cursor) { data in
                if isCancelled?() == true || (isCancelled != nil && ContinuousClock.now >= deadline) {
                    overflow = true; return false
                }
                let packet = try SoftwareStoredPacket.decode(data)
                guard packet.streamIndex == video.index else { return true }
                if run.isEmpty, packet.flags & 1 == 0 { overflow = true; return false }
                run.append(packet)
                if run.count > maxPackets { overflow = true; return false }
                if reached {
                    tail -= 1
                    return tail > 0
                }
                if let pts = videoSeconds(pts: packet.pts), pts >= seconds {
                    reached = true
                    return tail > 0
                }
                return true
            }
        } catch {
            return nil
        }
        guard reached, !overflow, isCancelled?() != true else { return nil }
        return run
    }

    func endSeek(_ token: UInt64, sourceClock: Double) {
        condition.lock(); defer { condition.unlock() }
        guard token == generation, !closed else { return }
        self.sourceClock = sourceClock
        seeking = false
        // A resumed parked run keeps the producer parked until the host has moved the source.
        if !pendingSourceReposition { sourceRepositioning = false }
        condition.broadcast()
    }

    /// Does not wait for a remote read or a disk operation. The host closes the Demuxer as usual,
    /// unblocking its reader; the worker then releases only its own unique FIFO directory.
    func close() {
        condition.lock()
        guard !closed else { condition.unlock(); return }
        closed = true
        generation &+= 1
        sourceEpoch &+= 1
        clearSourceMetadataLocked()
        let needsCleanup = !started
        let store = fifo
        let spans = needsCleanup ? parked : []
        if needsCleanup { parked.removeAll(); parkedResidentBytes = 0 }
        condition.broadcast()
        condition.unlock()
        if needsCleanup {
            DispatchQueue.global(qos: .utility).async {
                try? store.close()
                for span in spans { try? span.store.close() }
            }
        }
    }

    /// Consumes one packet on the consumer thread; only source EOF returns nil.
    ///
    /// - Parameters:
    ///   - isCurrent: Admission for the host generation that requested this packet.
    ///   - maximumWait: Optional empty-queue wait in seconds. Zero polls without waiting.
    /// - Throws: `ReadError.idle` when the wait expires before a packet arrives, without popping
    ///   the FIFO. Cancellation, seeks, closure and source failures remain distinct from idle.
    func read(
        isCurrent: @Sendable () -> Bool = { true },
        maximumWait: TimeInterval? = nil
    ) throws -> SoftwareStoredPacket? {
        condition.lock()
        let token = generation
        var waitStart: DispatchTime?
        var starvationLine: String?
        let deadline = maximumWait.map { Date(timeIntervalSinceNow: max(0, $0)) }
        var waitExpired = false
        while count == 0, !ended, failure == nil, !closed, !seeking,
              token == generation, isCurrent() {
            if waitStart == nil {
                waitStart = .now()
                waitingConsumers += 1
                // Wake a parked producer so it can see the waiter at its next retune point.
                condition.broadcast()
            }
            if let deadline {
                if !condition.wait(until: deadline) {
                    waitExpired = true
                    break
                }
            } else {
                condition.wait()
            }
        }
        if let waitStart {
            waitingConsumers -= 1
            consumerWaitEvents &+= 1
            consumerBlockedSeconds += Double(DispatchTime.now().uptimeNanoseconds
                - waitStart.uptimeNanoseconds) / 1_000_000_000
            starvationLine = starvationLineLocked()
        }
        if closed { condition.unlock(); throw ReadError.closed }
        // Decide idle while holding the state lock. A readiness change after this point cannot
        // consume a packet: the consumer has not entered a FIFO operation yet.
        if waitExpired, count == 0, !ended, failure == nil, !seeking,
           token == generation, isCurrent() {
            condition.unlock()
            if let starvationLine { EngineLog.emit(starvationLine, category: .swPlayback) }
            throw ReadError.idle
        }
        let hadPackets = count > 0
        condition.unlock()
        if let starvationLine { EngineLog.emit(starvationLine, category: .swPlayback) }

        if hadPackets { beforeConsumerOperation?() }
        operations.lock()
        defer { operations.unlock() }
        condition.lock()
        guard !closed else { condition.unlock(); throw ReadError.closed }
        // Admission belongs to the HOST generation too. Capturing only our generation at read()
        // entry can let an old host iteration steal the first packet of a completed new seek.
        guard !seeking, token == generation, isCurrent() else {
            condition.unlock(); throw ReadError.interrupted
        }
        if count == 0 {
            let error = failure
            // Only the source's own end ends the stream. The wait above also stops for a reader
            // that is not current, and a seek can make it current again while this read waits for
            // `operations`, which a cache miss holds to empty the store: that store is about to
            // refill from the new position, and reading it as the end stopped playback at the
            // landing (a DVD seek past the first cell, whose time-map seek settles at once, hit it
            // in 7 of 8 runs).
            let sourceEnded = ended
            condition.unlock()
            if let error { throw error }
            guard sourceEnded else { throw ReadError.interrupted }
            return nil
        }
        condition.unlock()
        guard let data = try fifo.pop() else { throw ReadError.corruptFIFO }
        var packet = try SoftwareStoredPacket.decode(data)
        // Equality parks the producer too. Reclaim an eligible consumed chunk at the exact ceiling,
        // otherwise it can remain asleep with unread packets until the queue drains completely.
        condition.lock(); let activeBudget = max(1, byteBudget - parkedResidentBytes); condition.unlock()
        try fifo.trimConsumed(toByteBudget: max(0, activeBudget - 1))
        let state = fifo.snapshot
        condition.lock()
        defer { condition.unlock() }
        guard !closed, !seeking, token == generation, isCurrent() else { throw ReadError.interrupted }
        copyDiskStateLocked(state)
        applyCacheTimingLocked()
        if packet.streamIndex == video.index {
            if cacheTiming != nil { packet.cacheTimingEpoch = sourceEpoch }
            if let seconds = cacheTiming?.seconds(position: packet.position, epoch: sourceEpoch)
                ?? (cacheTiming == nil ? videoSeconds(pts: packet.pts) : nil) {
                consumedVideoSeconds = seconds
            } else if cacheTiming != nil, !cacheTimingInvalid {
                // Old metadata can expire before its retained bytes. The render clock still
                // bounds elective read-ahead; it never fabricates coverage or a seek anchor.
                consumedVideoSeconds = sourceClock
            }
        }
        condition.broadcast()
        return packet
    }

    private func frontierLocked(at seconds: Double? = nil) -> Double? {
        guard !cacheTimingInvalid else { return nil }
        return frontier(at: seconds ?? sourceClock, video: videoCoverage, audio: audioCoverage,
                        presentation: presentationCoverage)
    }

    /// Where continuous coverage containing `clock` ends, for the active run or a parked one.
    private func frontier(at clock: Double, video videoCoverage: SoftwarePacketCoverage,
                          audio audioCoverage: SoftwareAudioPacketCoverage,
                          presentation presentationCoverage: SoftwareVideoPacketCoverage?) -> Double? {
        let videoEnd: Double?
        if let presentationCoverage {
            videoEnd = presentationCoverage.frontierSeconds(containing: clock,
                timeBaseNumerator: video.numerator, timeBaseDenominator: video.denominator)
        } else {
            videoEnd = videoCoverage.frontierSeconds(containing: clock,
                timeBaseNumerator: video.numerator, timeBaseDenominator: video.denominator)
        }
        let audioEnd = audio.flatMap { stream in
            audioCoverage.frontierSeconds(containing: clock,
                timeBaseNumerator: stream.numerator, timeBaseDenominator: stream.denominator)
        }
        return SoftwarePacketCoverage.combinedFrontier(
            video: videoEnd, audio: audioEnd, requiresAudio: audio != nil)
    }

    /// The playhead in a stream's ticks, rounded down so a prune never reaches past it.
    ///
    /// #613: coverage keeps its history behind the playhead for backward cached seeks, so a long
    /// session fills the range cap. A full coverage used to stop describing new packets (duration
    /// model) or invalidate itself (successor model), which left every frontier after the cap nil.
    /// Forgetting what lies wholly behind the playhead instead costs a backward cached seek into that
    /// history, which then goes cold.
    private func playheadTick(_ stream: Stream) -> Int64? {
        guard stream.numerator > 0, stream.denominator > 0 else { return nil }
        let ticks = (sourceClock * Double(stream.denominator) / Double(stream.numerator))
            .rounded(.down)
        guard ticks.isFinite, ticks > -9.0e18, ticks < 9.0e18 else { return nil }
        return Int64(ticks)
    }

    private func clearSourceMetadataLocked() {
        count = 0; bytes = 0; residentBytes = 0
        ended = false; failure = nil; failureOrigin = .none
        videoCoverage.reset(); audioCoverage.reset(); presentationCoverage?.reset()
        storedVideoSeconds = nil; consumedVideoSeconds = nil
        keyframes.removeAll(keepingCapacity: true)
        residency = SoftwarePacketResidency()
        timingResidency = SoftwarePacketResidency(); cacheTimingInvalid = false; calibrationPressure = false
    }

    /// Presentation seconds of a stored packet, or nil when this stream cannot express them.
    private func videoSeconds(pts: Int64) -> Double? {
        guard pts != Int64.min, video.numerator > 0, video.denominator > 0 else { return nil }
        let seconds = Double(pts) * Double(video.numerator) / Double(video.denominator)
        return seconds.isFinite ? seconds : nil
    }

    private func copyDiskStateLocked(_ state: SoftwarePacketDiskFIFO.Snapshot) {
        count = state.count
        bytes = state.byteCount
        residentBytes = state.residentByteCount
        if let floor = state.oldestRetainedChunkID {
            keyframes.removeAll { $0.cursor.chunkID < floor }
            residency.evict(before: floor)
            timingResidency.evict(before: floor)
            cacheTiming?.evict(before: floor, epoch: sourceEpoch)
        } else { keyframes.removeAll(keepingCapacity: true) }
    }

    private func produce() {
        // AE#519: a started producer is latency-critical by construction. Nothing is stored for
        // this epoch yet, and both entry reasons, cold start and a seek landing, have the consumer
        // waiting on the first packet it delivers.
        applyProducerQoS(QOS_CLASS_USER_INITIATED, reservoir: nil, waiting: 0)
        defer {
            try? fifo.close()
            condition.lock(); let spans = parked; parked.removeAll(); parkedResidentBytes = 0; condition.unlock()
            for span in spans { try? span.store.close() }
        }
        while true {
            condition.lock()
            while !closed && !resetPending && (sourceRepositioning || ended || failure != nil || shouldParkLocked()) {
                producerParked = true
                condition.wait()
            }
            producerParked = false
            if closed { condition.unlock(); return }
            let token = sourceEpoch
            let reset = resetPending
            let reservoir = reservoirSecondsLocked()
            let waiting = waitingConsumers
            condition.unlock()
            // Both a packet boundary and a park wake are cheap and are points where the answer can
            // have changed. A parked producer costs nothing in any class, so nothing is lost by
            // only deciding here.
            retuneProducerQoS(reservoir: reservoir, waiting: waiting)

            if reset {
                operations.lock()
                do {
                    try fifo.reset()
                    condition.lock()
                    if token == sourceEpoch { resetPending = false }
                    condition.broadcast()
                    condition.unlock()
                } catch { recordFailure(error, token: token, origin: .storage) }
                operations.unlock()
                continue
            }

            autoreleasepool {
                producePacket(token: token)
            }
        }
    }

    private func producePacket(token: UInt64) {
        var failureOrigin: FailureOrigin = .sourceRead
        do {
            let packet = try readSource { [self] in
                condition.lock(); defer { condition.unlock() }
                return token == sourceEpoch && !closed && !sourceRepositioning
            }
            guard let packet else {
                condition.lock()
                if token == sourceEpoch, !sourceRepositioning, !closed {
                    presentationCoverage?.finish()
                    ended = true
                }
                condition.broadcast()
                condition.unlock()
                return
            }
            if dropsAsDuplicate(packet, token: token) { return }
            failureOrigin = .envelope
            let data = try packet.encoded()
            operations.lock()
            condition.lock()
            let valid = token == sourceEpoch && !closed && !sourceRepositioning
            condition.unlock()
            if valid {
                do {
                    condition.lock(); let activeBudget = max(1, byteBudget - parkedResidentBytes); condition.unlock()
                    let recordReservation = min(activeBudget, data.count)
                        + min(8, activeBudget - min(activeBudget, data.count))
                    try fifo.trimConsumed(toByteBudget: activeBudget - recordReservation)
                    let cursor = try fifo.append(data)
                    let state = fifo.snapshot
                    if packet.streamIndex == video.index {
                        cacheTiming?.append(packet, cursor: cursor, epoch: token)
                    }
                    condition.lock()
                    if token == sourceEpoch, !closed, !sourceRepositioning {
                        copyDiskStateLocked(state)
                        let mark = StoredTail.Mark(position: packet.position, dts: packet.dts, pts: packet.pts)
                        if packet.streamIndex == video.index { storedTail.video = mark }
                        else if packet.streamIndex == audio?.index { storedTail.audio = mark }
                        if forwardOnly {
                            if packet.streamIndex == video.index, let seconds = videoSeconds(pts: packet.pts) {
                                storedVideoSeconds = seconds
                            }
                        } else {
                            if (packet.streamIndex == video.index && cacheTiming == nil) || packet.streamIndex == audio?.index {
                                let stream = packet.streamIndex == video.index ? video : audio!
                                residency.insert(chunkID: cursor.chunkID, pts: packet.pts,
                                    duration: packet.duration, numerator: stream.numerator,
                                    denominator: stream.denominator, isVideo: packet.streamIndex == video.index)
                            }
                            if packet.streamIndex == video.index, cacheTiming == nil {
                                if presentationCoverage != nil {
                                    if presentationCoverage?.isFull == true, let tick = playheadTick(video) {
                                        presentationCoverage?.prune(before: tick)
                                    }
                                    presentationCoverage?.insert(pts: packet.pts)
                                } else {
                                    if videoCoverage.isFull, let tick = playheadTick(video) {
                                        videoCoverage.prune(before: tick)
                                    }
                                    videoCoverage.insert(pts: packet.pts, duration: packet.duration)
                                }
                                if let seconds = videoSeconds(pts: packet.pts) { storedVideoSeconds = seconds }
                                if packet.flags & 1 != 0, let seconds = videoSeconds(pts: packet.pts) {
                                    keyframes.append(Keyframe(seconds: seconds, cursor: cursor))
                                    if keyframes.count > maximumKeyframes {
                                        keyframes.removeFirst(min(1024, keyframes.count))
                                    }
                                }
                            } else if let audio, packet.streamIndex == audio.index {
                                if audioCoverage.isFull, let tick = playheadTick(audio) {
                                    audioCoverage.prune(before: tick)
                                }
                                audioCoverage.insert(pts: packet.pts, duration: packet.duration, byteCount: packet.bytes.count)
                            }
                        }
                    }
                    applyCacheTimingLocked()
                    condition.broadcast()
                    condition.unlock()
                } catch { recordFailure(error, token: token, origin: .storage) }
            }
            operations.unlock()
        } catch { recordFailure(error, token: token, origin: failureOrigin) }
    }

    /// After a resumed parked run's source move: drops what the run already holds, until both
    /// streams have reached past its newest stored packet. Packets of other streams pass.
    private func dropsAsDuplicate(_ packet: SoftwareStoredPacket, token: UInt64) -> Bool {
        condition.lock(); defer { condition.unlock() }
        guard token == sourceEpoch, var tail = duplicatesBefore else { return false }
        let isVideo = packet.streamIndex == video.index
        let isAudio = packet.streamIndex == audio?.index
        guard isVideo || isAudio else { return false }
        if let mark = isVideo ? tail.video : tail.audio {
            if StoredTail.isDuplicate(position: packet.position, dts: packet.dts, pts: packet.pts, of: mark) {
                return true
            }
            if isVideo { tail.video = nil } else { tail.audio = nil }
            duplicatesBefore = tail.video == nil && tail.audio == nil ? nil : tail
        }
        return false
    }

    /// Only bounded metadata work under condition; parser and media I/O have already finished.
    private func applyCacheTimingLocked() {
        guard !closed, !sourceRepositioning, !resetPending, let cacheTiming else { return }
        let batch = cacheTiming.drain(epoch: sourceEpoch)
        if calibrationPressure != batch.calibrationPressure {
            calibrationPressure = batch.calibrationPressure
            condition.broadcast()
        }
        if let floor = batch.oldestUsableChunk {
            keyframes.removeAll { $0.cursor.chunkID < floor }
            timingResidency.evict(before: floor)
        }
        if batch.invalid {
            cacheTimingInvalid = true
            keyframes.removeAll(keepingCapacity: true)
            videoCoverage.reset(); presentationCoverage?.reset()
            storedVideoSeconds = nil; consumedVideoSeconds = nil
            return
        }
        guard !cacheTimingInvalid else { return }
        for record in batch.records {
            timingResidency.insert(chunkID: record.cursor.chunkID, pts: record.pts,
                duration: record.duration, numerator: video.numerator,
                denominator: video.denominator, isVideo: true)
            if presentationCoverage != nil {
                if presentationCoverage?.isFull == true, let tick = playheadTick(video) {
                    presentationCoverage?.prune(before: tick)
                }
                presentationCoverage?.insert(pts: record.pts)
            } else {
                if videoCoverage.isFull, let tick = playheadTick(video) { videoCoverage.prune(before: tick) }
                videoCoverage.insert(pts: record.pts, duration: record.duration)
            }
            if let seconds = videoSeconds(pts: record.pts) {
                storedVideoSeconds = seconds
                if record.key {
                    keyframes.append(Keyframe(seconds: seconds, cursor: record.cursor))
                    if keyframes.count > maximumKeyframes { keyframes.removeFirst(min(1024, keyframes.count)) }
                }
            }
        }
    }

    private func shouldParkLocked() -> Bool {
        // One source record can cross the budget. A consumed active chunk cannot be deleted before
        // cursor rollover, so residency also includes bounded protected chunk slack (not an
        // unbounded batch). Unknown time coverage is never guessed from bitrate.
        guard count > 0 else { return false }
        // Keep the unqualified GOP available to the decoder even when the source outruns it.
        // An empty reservoir always reads again: a very long GOP cannot deadlock qualification.
        if calibrationPressure { return true }
        if residentBytes + parkedResidentBytes >= byteBudget { return true }
        // The forward limit is measured on the RESERVOIR, from the packet the consumer last took
        // to the newest one stored, because that is what the producer is actually building and it
        // is knowable from two timestamps. The coverage frontier answers a stricter question (is
        // this stretch continuously playable) and returns nothing at all once a hole or one late
        // presentation timestamp invalidates it; keyed on that alone, the seconds limit stopped
        // existing there and only the disk budget still bounded the read-ahead. Measured on a
        // 10 s window: 283 packets read with clean timestamps, 1316 with a single late one, which
        // in a session is the difference between a window and the whole file.
        if let reservoir = reservoirSecondsLocked(), reservoir >= forwardSeconds { return true }
        return frontierLocked().map { $0 - sourceClock >= forwardSeconds } ?? false
    }

    private func recordFailure(_ error: Error, token: UInt64, origin: FailureOrigin) {
        condition.lock(); defer { condition.unlock() }
        guard token == sourceEpoch, !closed else { return }
        failure = error
        failureOrigin = origin
        resetPending = false
        condition.broadcast()
    }

    /// Seconds of video the producer has built ahead of the consumer: from the packet the consumer
    /// last took out of the store to the newest one put into it. nil when no stored timestamp can
    /// express it, which is a reason to stay responsive, never a reason to relax.
    private func reservoirSecondsLocked() -> Double? {
        guard let stored = storedVideoSeconds else { return nil }
        let seconds = stored - (consumedVideoSeconds ?? sourceClock)
        return seconds.isFinite ? max(0, seconds) : nil
    }

    /// AE#519: the producer may only drop to the efficiency class while the consumer provably
    /// cannot reach it. Two facts decide it, and both fail closed: a consumer parked in `read()` is
    /// waiting on this producer right now, and a reservoir that cannot be expressed in seconds is
    /// not a deep one. The depth rule is what keeps the inversion window from opening at all, rather
    /// than reacting once a consumer is already blocked: the boost lands when the reserve is down to
    /// a quarter of the window, which is still many seconds of playback away from a dry store.
    ///
    /// The two depths differ on purpose. A single threshold retunes once per packet for as long as a
    /// source sits on it, and each retune is a `pthread` call and a log line; with this band the
    /// reserve has to drain a quarter of the window to change the answer.
    static func producerMayRelax(currentlyRelaxed: Bool, consumersWaiting: Int,
                                 reservoirSeconds: Double?, forwardSeconds: Double) -> Bool {
        guard consumersWaiting == 0 else { return false }
        guard let reservoirSeconds, reservoirSeconds.isFinite else { return false }
        let depth = currentlyRelaxed
            ? boostReservoirSeconds(forwardSeconds: forwardSeconds)
            : relaxReservoirSeconds(forwardSeconds: forwardSeconds)
        return reservoirSeconds >= depth
    }

    /// Half the forward window. The steady state sits just under the window (the producer parks AT
    /// it and the consumer drains it), so a source that keeps up stays in the efficiency class.
    static func relaxReservoirSeconds(forwardSeconds: Double) -> Double {
        guard forwardSeconds.isFinite, forwardSeconds > 0 else { return .infinity }
        return max(2, forwardSeconds * 0.5)
    }

    /// A quarter of the window: the depth at which a relaxed producer becomes responsive again.
    static func boostReservoirSeconds(forwardSeconds: Double) -> Double {
        guard forwardSeconds.isFinite, forwardSeconds > 0 else { return .infinity }
        return max(1, forwardSeconds * 0.25)
    }

    private func retuneProducerQoS(reservoir: Double?, waiting: Int) {
        let relax = Self.producerMayRelax(currentlyRelaxed: producerQoS == QOS_CLASS_UTILITY,
                                          consumersWaiting: waiting, reservoirSeconds: reservoir,
                                          forwardSeconds: forwardSeconds)
        applyProducerQoS(relax ? QOS_CLASS_UTILITY : QOS_CLASS_USER_INITIATED,
                         reservoir: reservoir, waiting: waiting)
    }

    private func applyProducerQoS(_ requested: qos_class_t, reservoir: Double?, waiting: Int) {
        guard requested != producerQoS else { return }
        producerQoS = requested
        pthread_set_qos_class_self_np(requested, 0)
        condition.lock(); reportedProducerQoS = requested; condition.unlock()
        // Read the class back: a thread opted out of the QoS system keeps the old one silently, and
        // then the whole mechanism is a no-op that still looks configured.
        EngineLog.emit(
            "[SWReadAhead] producer qos -> \(QoSClass.name(requested)) "
            + "(now=\(QoSClass.name(qos_class_self())) "
            + "reservoir=\(reservoir.map { String(format: "%.1f", $0) } ?? "n/a")s "
            + "relax=\(String(format: "%.1f", Self.relaxReservoirSeconds(forwardSeconds: forwardSeconds)))s "
            + "boost=\(String(format: "%.1f", Self.boostReservoirSeconds(forwardSeconds: forwardSeconds)))s "
            + "waiting=\(waiting))",
            category: .swPlayback
        )
    }

    /// Consumer starvation is the observable for "the source cannot keep up", and it is the same
    /// window in which the producer must not be elective. Rate-limited to one line a second, and
    /// returned rather than emitted: the caller holds `condition` and the handler formats, redacts
    /// and locks on its own side.
    private func starvationLineLocked() -> String? {
        let now = DispatchTime.now()
        if let last = lastStarvationLog,
           now.uptimeNanoseconds - last.uptimeNanoseconds < 1_000_000_000 { return nil }
        lastStarvationLog = now
        return "[SWReadAhead] consumer starved: waits=\(consumerWaitEvents) "
            + "blocked=\(String(format: "%.2f", consumerBlockedSeconds))s "
            + "reservoir=\(reservoirSecondsLocked().map { String(format: "%.1f", $0) } ?? "n/a")s "
            + "qos=\(QoSClass.name(reportedProducerQoS))"
    }
}

/// Constant-size summaries per retained disk chunk, never per packet. The cumulative discarded
/// high-water marks also cover reordered PTS: clipping at the first packet in the new chunk alone
/// could otherwise claim an evicted B picture or audio packet still exists. Metadata pressure may
/// conservatively shorten a displayed range; it never changes disk retention or seek policy.
struct SoftwarePacketResidency: Sendable {
    private struct Chunk: Sendable {
        let id: UInt64
        var videoEnd = -Double.infinity
        var audioEnd = -Double.infinity
    }
    private var chunks: [Chunk] = []
    private(set) var videoFloor = -Double.infinity
    private(set) var audioFloor = -Double.infinity
    private let maximumChunks: Int

    init(maximumChunks: Int = 4096) { self.maximumChunks = max(1, maximumChunks) }

    mutating func insert(chunkID: UInt64, pts: Int64, duration: Int64,
                         numerator: Int32, denominator: Int32, isVideo: Bool) {
        if chunks.last?.id != chunkID {
            if chunks.count == maximumChunks { discardFirst(1) }
            chunks.append(Chunk(id: chunkID))
        }
        // A successor-duration video with no packet duration still excludes the evicted PTS.
        let (end, overflow) = pts.addingReportingOverflow(max(1, duration))
        let seconds: Double
        if pts == Int64.min || overflow || numerator <= 0 || denominator <= 0
            || Int64(exactly: Double(end)) != end {
            seconds = .infinity // unknown discarded timestamps cannot prove later residency
        } else {
            seconds = Double(end) * Double(numerator) / Double(denominator)
        }
        let index = chunks.count - 1
        if isVideo { chunks[index].videoEnd = max(chunks[index].videoEnd, seconds) }
        else { chunks[index].audioEnd = max(chunks[index].audioEnd, seconds) }
    }

    mutating func evict(before chunkID: UInt64) {
        discardFirst(chunks.prefix { $0.id < chunkID }.count)
    }

    private mutating func discardFirst(_ count: Int) {
        guard count > 0 else { return }
        for chunk in chunks.prefix(count) {
            videoFloor = max(videoFloor, chunk.videoEnd)
            audioFloor = max(audioFloor, chunk.audioEnd)
        }
        chunks.removeFirst(count)
    }
}
