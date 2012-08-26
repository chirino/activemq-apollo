package org.apache.activemq.apollo.broker.jetty;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

/**
 */
public class PutBackByteChannel implements ByteChannel {

    final ByteChannel next;
    final ByteBuffer putback;

    public PutBackByteChannel(ByteChannel next, ByteBuffer putback) {
        this.next = next;
        this.putback = putback;
    }

    public int read(ByteBuffer target) throws IOException {
        if( putback.hasRemaining() ) {
            int size = Math.min(putback.remaining(), target.remaining());
            if( size > 0 ) {
                int olimit = putback.limit();
                putback.limit(putback.position()+size);
                target.put( putback );
                putback.limit(olimit);
            }
            return size + next.read(target);
        }
        return next.read(target);
    }

    public void close() throws IOException {
        next.close();
    }

    public int write(ByteBuffer byteBuffer) throws IOException {
        return next.write(byteBuffer);
    }

    public boolean isOpen() {
        return next.isOpen();
    }
}
