import Foundation
import Testing
@testable import AetherEngine

/// A seek made while paused on the software host presents the picture at its target and stays
/// paused. The demux loop reads only while playing (`SeekWindow.loopMayRead`), so a paused landing
/// used to decode nothing and the frame from before the seek stayed on screen until `play()`.
@Suite("Software host: a paused seek presents its target", .serialized, .timeLimit(.minutes(2)))
@MainActor
struct SoftwarePausedSeekPresentsTargetTests {

    final class Frames: @unchecked Sendable {
        private let lock = NSLock()
        private var seen: [SoftwareVideoFrameTime] = []
        func record(_ frame: SoftwareVideoFrameTime) { lock.withLock { seen.append(frame) } }
        var all: [SoftwareVideoFrameTime] { lock.withLock { seen } }
    }

    @Test("a seek while paused enqueues the frame at the target without resuming playback")
    func pausedSeekPresentsTheTarget() async throws {
        let host = SoftwarePlaybackHost(reposition: { dem, target, budget, queue, superseded in
            await dem.seekBounded(to: target, timeout: budget, on: queue, isSuperseded: superseded)
        })
        defer { host.stop() }
        let frames = Frames()
        host.setVideoFrameTimeObserver { frames.record($0) }
        let dem = Demuxer()
        try dem.open(reader: DataIOReader(data: ProbeTestFixtures.hdr10Plus()), formatHint: "mp4")
        try await host.load(demuxer: dem, startPosition: nil, audioSourceStreamIndex: nil)

        host.play()
        try await waitFor { host.framesEnqueued >= 1 }
        host.pause()
        // Whatever was in flight at the pause has landed by the time the seek flushes the renderer:
        // the frames that count are the ones under the generation the seek starts.
        let before = frames.all.map(\.generation).max() ?? 0

        _ = await host.seek(to: 0)

        try await waitFor { frames.all.contains { $0.generation > before && $0.presentation.seconds < 1 } }
        #expect(!host.isPlaying)
        #expect(host.rate == 0)
    }
}
