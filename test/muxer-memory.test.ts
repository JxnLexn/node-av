import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { AV_PKT_DATA_STRINGS_METADATA, AVERROR_ENOMEM, Demuxer, FMP4Stream, Muxer } from '../src/index.js';
import { getInputFile } from './index.js';

import type { MuxerOptions, Packet } from '../src/index.js';

function fixture(options: MuxerOptions = {}) {
  const input = Demuxer.openSync(getInputFile('demux.mp4'));
  let bytes = 0;
  const output = Muxer.openSync(
    {
      write: (data) => {
        bytes += data.length;
        return data.length;
      },
    },
    {
      format: 'mp4',
      useAsyncWrite: false,
      options: { movflags: '+frag_keyframe+empty_moov+default_base_moof' },
      ...options,
    },
  );
  const video = input.video()!;
  const index = output.addStream(video);
  output.addStream(input.audio()!);
  let packet: Packet | undefined;
  for (const p of input.packetsSync()) {
    if (p?.streamIndex === video.index) {
      packet = p.clone()!;
      break;
    }
  }
  assert.ok(packet);
  const setDts = (dts: bigint) => {
    packet.dts = dts;
    packet.pts = dts;
    packet.duration = 512n;
  };
  return {
    output,
    packet,
    index,
    setDts,
    bytes: () => bytes,
    close: async () => {
      packet.free();
      try {
        await output.close();
      } finally {
        input.closeSync();
      }
    },
  };
}

describe('Muxer live-memory safeguards', () => {
  it('reproduces one-tick timestamp correction retaining video without audio', async () => {
    const f = fixture();
    try {
      f.setDts(15360n * 3600n);
      f.output.writePacketSync(f.packet, f.index);
      const headerBytes = f.bytes();
      assert.ok(headerBytes > 0);
      for (let i = 0; i < 128; i++) {
        f.setDts(BigInt(i) * 512n);
        f.output.writePacketSync(f.packet, f.index);
      }
      assert.equal(f.bytes(), headerBytes, 'regressed video has not left the native interleaving queue');
    } finally {
      await f.close();
    }
  });

  for (const sync of [false, true]) {
    it(`rejects a large DTS regression before clamping (${sync ? 'sync' : 'async'})`, async () => {
      const f = fixture({ maxDtsCorrection: 1_000_000, exitOnError: false });
      const write = () => (sync ? f.output.writePacketSync(f.packet, f.index) : f.output.writePacket(f.packet, f.index));
      try {
        f.setDts(15360n * 3600n);
        await write();
        f.setDts(0n);
        await assert.rejects(async () => write(), /Timestamp discontinuity/);
        assert.ok(f.packet.size > 0, 'caller retains ownership of its original packet');
      } finally {
        await f.close();
      }
    });

    it(`bounds native buffering even with one-tick DTS and permissive write errors (${sync ? 'sync' : 'async'})`, async () => {
      const limit = 128 * 1024;
      const f = fixture({ maxInterleaveBytes: limit, exitOnError: false });
      let submitted = 0;
      try {
        await assert.rejects(async () => {
          // No timestamp regression here: a timestamp-only fix cannot pass.
          for (let i = 0; i < 1024; i++) {
            f.setDts(BigInt(i));
            if (sync) f.output.writePacketSync(f.packet, f.index);
            else await f.output.writePacket(f.packet, f.index);
            submitted += f.packet.size;
          }
        }, /Interleaving queue memory limit exceeded/);
        assert.ok(submitted <= limit, 'fail before admitting more than the configured payload budget');
      } finally {
        await f.close();
      }
    });
  }

  it('preserves small DTS corrections for tolerant live inputs', async () => {
    const f = fixture({ maxDtsCorrection: 1_000_000 });
    try {
      f.setDts(15360n);
      f.output.writePacketSync(f.packet, f.index);
      f.setDts(15360n - 512n);
      f.output.writePacketSync(f.packet, f.index);
    } finally {
      await f.close();
    }
  });

  it('reclaims budget as the native interleaver drains normal A/V traffic', async () => {
    const input = Demuxer.openSync(getInputFile('demux.mp4'));
    const limit = 128 * 1024;
    const output = Muxer.openSync(
      { write: (data) => data.length },
      {
        format: 'mp4',
        maxInterleaveBytes: limit,
        options: { movflags: '+frag_keyframe+empty_moov+default_base_moof' },
      },
    );
    for (const stream of input.streams) output.addStream(stream);
    let total = 0;
    const packets: Packet[] = [];
    try {
      for (const packet of input.packetsSync()) {
        if (packet) packets.push(packet.clone()!);
      }
      for (let pass = 0; pass < 32; pass++) {
        for (const packet of packets) {
          total += packet.size;
          output.writePacketSync(packet, packet.streamIndex);
          const tb = input.streams[packet.streamIndex].timeBase;
          const offset = BigInt(Math.round((10 * tb.den) / tb.num));
          packet.dts += offset;
          packet.pts += offset;
        }
      }
      assert.ok(total > 4 * limit, 'exercise recounts rather than only the initial allowance');
    } finally {
      for (const packet of packets) packet.free();
      output.closeSync();
      input.closeSync();
    }
  });

  it('low-level rejection consumes the rejected packet and allows explicit flush', async () => {
    const f = fixture();
    const packet = f.packet.clone()!;
    try {
      f.setDts(0n);
      f.output.writePacketSync(f.packet, f.index);
      const ctx = f.output.getFormatContext();
      packet.data = Buffer.alloc(8192);
      assert.equal(ctx.interleavedWriteFrameSync(packet, 1024), AVERROR_ENOMEM);
      assert.equal(packet.size, 0);
      assert.equal(await ctx.interleavedWriteFrame(null, 1024), 0);
      for (const value of [-1, NaN, Infinity, 0.5]) {
        assert.throws(() => ctx.interleavedWriteFrameSync(null, value), /safe integer/);
        await assert.rejects(async () => ctx.interleavedWriteFrame(null, value), /safe integer/);
      }
    } finally {
      packet.free();
      await f.close();
    }
  });

  it('reports a worker failure once and releases FMP4 input even if muxer close repeats it', async () => {
    const input = await Demuxer.open(getInputFile('demux.mp4'));
    const error = new Error('Timestamp discontinuity');
    let closed = 0;
    const stream = FMP4Stream.create(input, {
      onClose: (err) => {
        assert.equal(err, error);
        closed++;
      },
    });
    // Exercise the real completion/stop path with the Muxer.close contract:
    // resources have been released, then its earlier worker error is rethrown.
    const state = stream as unknown as {
      output: { close(): Promise<void> } | undefined;
      input: Demuxer | undefined;
      attachCompletion(promise: Promise<void>): void;
    };
    state.output = {
      close: async () => {
        throw error;
      },
    };
    state.attachCompletion(Promise.reject(error));
    for (let i = 0; i < 50 && closed === 0; i++) await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(closed, 1);
    assert.equal(state.output, undefined);
    assert.equal(state.input, undefined);
    await stream.stop();
  });

  it('includes side-data allocations in the native budget', async () => {
    const f = fixture();
    const packet = f.packet.clone()!;
    try {
      f.setDts(0n);
      f.output.writePacketSync(f.packet, f.index);
      const ctx = f.output.getFormatContext();
      await ctx.interleavedWriteFrame(null);
      const limit = packet.size + 4096;
      packet.newSideData(AV_PKT_DATA_STRINGS_METADATA, 64 * 1024);
      assert.equal(await ctx.interleavedWriteFrame(packet, limit), AVERROR_ENOMEM);
      assert.equal(packet.size, 0);
    } finally {
      packet.free();
      await f.close();
    }
  });

  it('ends a real FMP4 pipeline on a source timestamp reset and can start a fresh session', { timeout: 10000 }, async () => {
    const input = await Demuxer.open(getInputFile('demux.mp4'));
    const packets = input.packets.bind(input);
    let videoPackets = 0;
    input.packets = async function* (index?: number) {
      for await (const packet of packets(index)) {
        if (packet?.streamIndex === input.video()!.index && ++videoPackets >= 5) {
          const tb = input.video()!.timeBase;
          const reset = BigInt(Math.round((3600 * tb.den) / tb.num));
          packet.dts -= reset;
          packet.pts -= reset;
        }
        yield packet;
      }
    };
    let closeCount = 0;
    let resolveClosed!: (error?: Error) => void;
    const closed = new Promise<Error | undefined>((resolve) => {
      resolveClosed = resolve;
    });
    const stream = FMP4Stream.create(input, {
      supportedCodecs: 'avc1,mp4a.40.2',
      onClose: (error) => {
        closeCount++;
        resolveClosed(error);
      },
    });
    try {
      await stream.start();
      const error = await closed;
      assert.match(error?.message ?? '', /Timestamp discontinuity/);
      assert.equal(closeCount, 1);
      assert.equal(input.isInputOpen, false, 'the failed session releases its native reader');
    } finally {
      await stream.stop();
    }

    let bytes = 0;
    let finish!: (error?: Error) => void;
    const recovered = new Promise<Error | undefined>((resolve) => {
      finish = resolve;
    });
    const next = FMP4Stream.create(getInputFile('demux.mp4'), {
      supportedCodecs: 'avc1,mp4a.40.2',
      onData: (data) => {
        bytes += data.length;
      },
      onClose: finish,
    });
    try {
      await next.start();
      assert.equal(await recovered, undefined);
      assert.ok(bytes > 0);
    } finally {
      await next.stop();
    }
  });

  it('rejects invalid memory and timestamp limits', () => {
    for (const key of ['maxInterleaveBytes', 'maxDtsCorrection'] as const) {
      for (const value of [-1, NaN, Infinity, 0.5, Number.MAX_SAFE_INTEGER + 1]) {
        assert.throws(() => Muxer.openSync('unused.mp4', { [key]: value }), /non-negative safe integer/);
      }
    }
  });
});
