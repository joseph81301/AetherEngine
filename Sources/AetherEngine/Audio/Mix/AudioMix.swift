import Foundation
import AetherLibavutil
import AetherLibswresample

/// A host's own audio mix (Pear fork): how decoded channels are folded and how loud they play, applied where
/// the engine decodes audio itself. Infuse-style "Original" downmix levels, centre (dialogue) boost, LFE in the
/// downmix and a volume boost above full scale all need this, because AVPlayer only ever downmixes normalised
/// and caps volume at 1.
///
/// The host decides the coefficients: `plan` gets the source's channel labels in decode order (FFmpeg names:
/// FL FR FC LFE BL BR SL SR ...) and returns one row per output channel, or nil to leave the track alone.
/// Two rows for a source with more than two channels folds it to stereo; as many rows as source channels keeps
/// the layout and only scales. A source the plan touches is always decoded (never stream-copied), on the
/// loopback route through `AudioBridge` and on the software route in `AudioDecoder`. A limiter keeps the result
/// under full scale. Set it before a load, or call `reloadAtCurrentPosition()` after changing it.
public struct AudioMixRequest: Sendable {
    public struct Plan: Sendable, Equatable {
        public var rows: [[Double]]
        public var gainDb: Double
        public init(rows: [[Double]], gainDb: Double) {
            self.rows = rows
            self.gainDb = gainDb
        }
    }

    public let plan: @Sendable (_ sourceChannels: [String]) -> Plan?

    public init(plan: @escaping @Sendable (_ sourceChannels: [String]) -> Plan?) {
        self.plan = plan
    }
}

extension AetherEngine {
    nonisolated private static let audioMixLock = NSLock()
    nonisolated(unsafe) private static var storedAudioMix: AudioMixRequest?

    /// The mix every session built from now on applies (nil: the engine's own behaviour).
    public nonisolated static var audioMix: AudioMixRequest? {
        get { audioMixLock.lock(); defer { audioMixLock.unlock() }; return storedAudioMix }
        set { audioMixLock.lock(); storedAudioMix = newValue; audioMixLock.unlock() }
    }
}

/// One session's resolved mix: the swr matrix for a given input/output layout, and the gain + limiter that run
/// on interleaved float samples after it.
final class AudioMixStage {
    let request: AudioMixRequest
    private(set) var gain: Float = 1
    private var limiter: Float = 1
    private var releasePerSample: Float = 1.0 / (0.2 * 48_000)
    private static let ceiling: Float = 0.98

    init(request: AudioMixRequest) { self.request = request }

    static func labels(_ layout: UnsafePointer<AVChannelLayout>) -> [String] {
        let n = Int(layout.pointee.nb_channels)
        // An unspecified order names no channels; read it as the default layout for its count, which is what
        // swr assumes for it too.
        if layout.pointee.order == AV_CHANNEL_ORDER_UNSPEC && n > 0 {
            var def = AVChannelLayout()
            av_channel_layout_default(&def, Int32(n))
            defer { av_channel_layout_uninit(&def) }
            return labels(&def)
        }
        var out: [String] = []
        out.reserveCapacity(n)
        var buf = [CChar](repeating: 0, count: 32)
        for i in 0..<n {
            let ch = av_channel_layout_channel_from_index(layout, UInt32(i))
            let ret = av_channel_name(&buf, buf.count, ch)
            out.append(ret > 0 ? String(cString: buf) : "X\(i)")
        }
        return out
    }

    /// The plan for this source, or nil when the host leaves it alone.
    func plan(for layout: UnsafePointer<AVChannelLayout>) -> AudioMixRequest.Plan? {
        guard layout.pointee.nb_channels > 0 else { return nil }
        return request.plan(Self.labels(layout))
    }

    /// Whether the plan folds this source to stereo.
    func folds(_ layout: UnsafePointer<AVChannelLayout>) -> Bool {
        guard let p = plan(for: layout) else { return false }
        return p.rows.count == 2 && layout.pointee.nb_channels > 2
    }

    /// Sets the matrix on an allocated, not yet initialised swr context. Rows are remapped by label when the
    /// output layout keeps the source's channels in another order. Returns false (and leaves swr's own
    /// matrix) when the plan doesn't fit the layouts.
    @discardableResult
    func configure(swr: OpaquePointer?, input: UnsafePointer<AVChannelLayout>, output: UnsafePointer<AVChannelLayout>, sampleRate: Int32) -> Bool {
        gain = 1
        limiter = 1
        releasePerSample = 1.0 / (0.2 * Float(max(sampleRate, 1)))
        guard let swr, let p = plan(for: input) else { return false }
        let inLabels = Self.labels(input)
        let outLabels = Self.labels(output)
        let nIn = inLabels.count, nOut = outLabels.count
        guard p.rows.allSatisfy({ $0.count == nIn }) else { return false }
        var rows: [[Double]]
        if p.rows.count == nOut && (nOut == 2 && nIn > 2 || outLabels == inLabels) {
            rows = p.rows
        } else if p.rows.count == nIn {
            // Same channels, maybe another order (5.1 side vs back): each output takes its source's row.
            let alias = ["SL": "BL", "SR": "BR", "BL": "SL", "BR": "SR"]
            rows = []
            for label in outLabels {
                guard let r = inLabels.firstIndex(of: label) ?? alias[label].flatMap({ inLabels.firstIndex(of: $0) }) else { return false }
                rows.append(p.rows[r])
            }
        } else {
            return false
        }
        var flat = rows.flatMap { $0 }
        let ret = flat.withUnsafeMutableBufferPointer { swr_set_matrix(swr, $0.baseAddress, Int32(nIn)) }
        guard ret >= 0 else { return false }
        gain = Float(pow(10, p.gainDb / 20))
        return true
    }

    /// Gain, then a peak limiter (instant attack, ~0.2 s release), on interleaved float samples in place.
    func process(_ samples: UnsafeMutablePointer<Float>, frames: Int, channels: Int) {
        guard channels > 0 else { return }
        for f in 0..<frames {
            let base = f * channels
            var peak: Float = 0
            for c in 0..<channels {
                let v = samples[base + c] * gain
                samples[base + c] = v
                peak = max(peak, abs(v))
            }
            if peak * limiter > Self.ceiling {
                limiter = Self.ceiling / peak
            } else {
                // Release towards 1, but never past what keeps this frame under the ceiling.
                limiter = min(1, Self.ceiling / max(peak, .leastNonzeroMagnitude), limiter + (1 - limiter) * releasePerSample)
            }
            if limiter < 1 {
                for c in 0..<channels { samples[base + c] *= limiter }
            }
        }
    }

    func reset() { limiter = 1 }
}
