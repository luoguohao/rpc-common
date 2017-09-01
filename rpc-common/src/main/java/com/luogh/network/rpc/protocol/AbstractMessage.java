package com.luogh.network.rpc.protocol;

import com.google.common.base.Objects;
import com.luogh.network.rpc.buffer.ManagedBuffer;

/**
 * @author luogh
 */
public abstract class AbstractMessage implements Message {

    private final boolean isBodyInFrame;

    private final ManagedBuffer body;

    public AbstractMessage(boolean isBodyInFrame, ManagedBuffer body) {
        this.isBodyInFrame = isBodyInFrame;
        this.body = body;
    }

    public AbstractMessage() {
        this(false, null);
    }

    @Override
    public ManagedBuffer body() {
        return body;
    }

    @Override
    public boolean isBodyInFrame() {
        return isBodyInFrame;
    }

    protected boolean equals(AbstractMessage other) {
        return isBodyInFrame == other.isBodyInFrame && Objects.equal(body, other.body);
    }
}
