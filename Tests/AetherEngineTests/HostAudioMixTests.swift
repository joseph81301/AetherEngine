import XCTest
import AetherLibavutil
import AetherLibswresample
@testable import AetherEngine

/// Pear fork: the host audio mix (`AetherEngine.audioMix`, `AudioMixStage`).
final class HostAudioMixTests: XCTestCase {

    private static func fold(_ labels: [String]) -> AudioMixRequest.Plan? {
        guard labels.count > 2 else { return nil }
        let rows = ["L", "R"].map { side in
            labels.map { l -> Double in
                switch l {
                case "F" + side: 1
                case "FC": 0.7071
                case "B" + side, "S" + side: 0.7071
                default: 0
                }
            }
        }
        return .init(rows: rows, gainDb: 6)
    }

    func testLabelsName51InDecodeOrder() {
        var layout = AVChannelLayout()
        av_channel_layout_default(&layout, 6)
        defer { av_channel_layout_uninit(&layout) }
        XCTAssertEqual(AudioMixStage.labels(&layout), ["FL", "FR", "FC", "LFE", "BL", "BR"])
    }

    func testFoldsSurroundButNotStereo() {
        let stage = AudioMixStage(request: AudioMixRequest(plan: { Self.fold($0) }))
        var surround = AVChannelLayout(), stereo = AVChannelLayout()
        av_channel_layout_default(&surround, 6)
        av_channel_layout_default(&stereo, 2)
        defer { av_channel_layout_uninit(&surround); av_channel_layout_uninit(&stereo) }
        XCTAssertTrue(stage.folds(&surround))
        XCTAssertFalse(stage.folds(&stereo))
        XCTAssertNil(stage.plan(for: &stereo))
    }

    func testMatrixIsAcceptedBySwr() {
        let stage = AudioMixStage(request: AudioMixRequest(plan: { Self.fold($0) }))
        var inLayout = AVChannelLayout(), outLayout = AVChannelLayout()
        av_channel_layout_default(&inLayout, 6)
        av_channel_layout_default(&outLayout, 2)
        defer { av_channel_layout_uninit(&inLayout); av_channel_layout_uninit(&outLayout) }
        var swr: OpaquePointer?
        XCTAssertGreaterThanOrEqual(swr_alloc_set_opts2(&swr, &outLayout, AV_SAMPLE_FMT_FLT, 48_000,
                                                        &inLayout, AV_SAMPLE_FMT_FLTP, 48_000, 0, nil), 0)
        defer { swr_free(&swr) }
        XCTAssertTrue(stage.configure(swr: swr, input: &inLayout, output: &outLayout, sampleRate: 48_000))
        XCTAssertGreaterThanOrEqual(swr_init(swr), 0)
        XCTAssertEqual(stage.gain, Float(pow(10, 6.0 / 20)), accuracy: 0.0001)
    }

    func testLimiterKeepsPeaksUnderFullScale() {
        let stage = AudioMixStage(request: AudioMixRequest(plan: { _ in .init(rows: [[1, 0], [0, 1]], gainDb: 10) }))
        var inLayout = AVChannelLayout()
        av_channel_layout_default(&inLayout, 2)
        defer { av_channel_layout_uninit(&inLayout) }
        var swr: OpaquePointer?
        _ = swr_alloc_set_opts2(&swr, &inLayout, AV_SAMPLE_FMT_FLT, 48_000, &inLayout, AV_SAMPLE_FMT_FLT, 48_000, 0, nil)
        defer { swr_free(&swr) }
        stage.configure(swr: swr, input: &inLayout, output: &inLayout, sampleRate: 48_000)
        var samples = [Float](repeating: 0.9, count: 2 * 480)
        samples.withUnsafeMutableBufferPointer { stage.process($0.baseAddress!, frames: 480, channels: 2) }
        XCTAssertLessThanOrEqual(samples.map(abs).max()!, 0.98 + 0.0001)
        // Quiet material comes out boosted.
        var quiet = [Float](repeating: 0.05, count: 2)
        let fresh = AudioMixStage(request: stage.request)
        fresh.configure(swr: swr, input: &inLayout, output: &inLayout, sampleRate: 48_000)
        quiet.withUnsafeMutableBufferPointer { fresh.process($0.baseAddress!, frames: 1, channels: 2) }
        XCTAssertEqual(quiet[0], 0.05 * Float(pow(10, 0.5)), accuracy: 0.001)
    }
}
