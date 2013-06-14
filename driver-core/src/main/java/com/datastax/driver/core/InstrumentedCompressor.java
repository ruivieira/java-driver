package com.datastax.driver.core;

import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.transport.FrameCompressor;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: jhalli
 * Date: 09/03/13
 * Time: 19:55
 * To change this template use File | Settings | File Templates.
 */
public class InstrumentedCompressor implements FrameCompressor {

    /*
    ProtocolOptions.java:
    //SNAPPY("snappy", FrameCompressor.SnappyCompressor.instance);
        SNAPPY("snappy", InstrumentedCompressor.instance);

        // main:
        InstrumentedCompressor.instance.dumpStats();
     */



    public static final InstrumentedCompressor instance = new InstrumentedCompressor();

    private final FrameCompressor delegate = SnappyCompressor.instance;

    private final AtomicLong compressorIn = new AtomicLong();
    private final AtomicLong compressorOut = new AtomicLong();

    private final AtomicLong decompressorIn = new AtomicLong();
    private final AtomicLong decompressorOut = new AtomicLong();

    public AtomicLong getCompressorIn() {
        return compressorIn;
    }

    public AtomicLong getCompressorOut() {
        return compressorOut;
    }

    public AtomicLong getDecompressorIn() {
        return decompressorIn;
    }

    public AtomicLong getDecompressorOut() {
        return decompressorOut;
    }

    public void reset() {
        compressorIn.set(0);
        compressorOut.set(0);
        decompressorIn.set(0);
        decompressorOut.set(0);
    }

    @Override
    public Frame compress(Frame frame) throws IOException {
        compressorIn.addAndGet(frame.body.readableBytes());
        Frame result = delegate.compress(frame);
        compressorOut.addAndGet(result.body.readableBytes());
        return result;
    }

    @Override
    public Frame decompress(Frame frame) throws IOException {
        decompressorIn.addAndGet(frame.body.readableBytes());
        Frame result = delegate.decompress(frame);
        decompressorOut.addAndGet(result.body.readableBytes());
        return result;
    }
}