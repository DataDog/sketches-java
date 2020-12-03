package com.datadoghq.sketch.ddsketch.store.rescaling;

import com.datadoghq.sketch.ddsketch.store.Bin;
import com.datadoghq.sketch.ddsketch.store.Store;

import java.util.Iterator;
import java.util.function.Supplier;

public abstract class RescalingStore implements Store {

    protected final Supplier<? extends Store> storeSupplier;

    protected Store delegate;
    protected int scalingFactor = 1;

    public RescalingStore(Supplier<? extends Store> storeSupplier) {
        this.storeSupplier = storeSupplier;
        this.delegate = storeSupplier.get();
    }

    protected RescalingStore(Supplier<? extends Store> storeSupplier, Store delegate, int scalingFactor) {
        this.storeSupplier = storeSupplier;
        this.delegate = delegate;
        this.scalingFactor = scalingFactor;
    }

    /**
     * @return the rescaling factor > 1 if it should rescale
     */
    protected abstract int shouldRescale();

    /**
     * Merge contiguous buckets.
     *
     * @param rescalingFactor the factor to be compounded with the current one
     */
    protected final void rescale(int rescalingFactor) {
        final Store newDelegate = storeSupplier.get();
        final int newScalingFactor = scalingFactor * rescalingFactor;
        delegate.getStream()
            .map(bin -> new Bin(Math.floorDiv(bin.getIndex(), rescalingFactor), bin.getCount()))
            .forEach(newDelegate::add);
        delegate = newDelegate;
        scalingFactor = newScalingFactor;
    }

    @Override
    public void add(int index, double count) {
        delegate.add(Math.floorDiv(index, scalingFactor), count);
        final int rescalingFactor = shouldRescale();
        if (rescalingFactor > 1) {
            rescale(rescalingFactor);
        }
    }

    @Override
    public void clear() {
        delegate.clear();
        scalingFactor = 1;
    }

    @Override
    public void mergeWith(Store store) {
        if (!(store instanceof RescalingStore) || delegate.maxShift() > 0) {
            // FIXME: implement
            throw new UnsupportedOperationException();
        }
        final RescalingStore that = (RescalingStore) store;
        final int newScalingFactor = lcm(this.scalingFactor, that.scalingFactor);
        if (newScalingFactor > scalingFactor) {
            rescale(newScalingFactor / scalingFactor);
        }
        that.getStream().forEach(this::add);
    }

    @Override
    public Iterator<Bin> getAscendingIterator() {
        final Iterator<Bin> delegateIterator = delegate.getAscendingIterator();
        return new Iterator<Bin>() {
            @Override
            public boolean hasNext() {
                return delegateIterator.hasNext();
            }

            @Override
            public Bin next() {
                final Bin bin = delegateIterator.next();
                return new Bin(bin.getIndex() * scalingFactor, bin.getCount());
            }
        };
    }

    @Override
    public Iterator<Bin> getDescendingIterator() {
        final Iterator<Bin> delegateIterator = delegate.getDescendingIterator();
        return new Iterator<Bin>() {
            @Override
            public boolean hasNext() {
                return delegateIterator.hasNext();
            }

            @Override
            public Bin next() {
                final Bin bin = delegateIterator.next();
                return new Bin(bin.getIndex() * scalingFactor, bin.getCount());
            }
        };
    }

    @Override
    public int maxShift() {
        return scalingFactor * (delegate.maxShift() + 1) - 1;
    }

    private static int gcd(int a, int b) {
        if (b == 0) {
            return a;
        } else {
            return gcd(b, a % b);
        }
    }

    private static int lcm(int a, int b) {
        return Math.multiplyExact(a, b / gcd(a, b));
    }
}
