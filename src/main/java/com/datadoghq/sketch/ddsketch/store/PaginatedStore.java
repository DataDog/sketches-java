package com.datadoghq.sketch.ddsketch.store;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This is an unbounded store which allocates storage for counts
 * in aligned pages stored in an array at offsets modulo the page
 * size. This means that if a distribution has several modes, the
 * cost of the storage for the space in between the modes is that
 * of a null pointer per page, requiring 4-8 bytes (depending on
 * CompressedOops, whether ZGC is used, and heap size) for each
 * 1KB page.
 *
 * On the contrary, if the data is uniformly distributed filling
 * each page in a range [N, N + K * PAGE_SIZE) this store will
 * require K * (20 + 4|8) extra space over
 * {@code UnboundedSizeDenseStore}, because of the metadata
 * overhead of the array headers and references to each page.
 *
 */
public final class PaginatedStore implements Store {

    private static final int GROWTH = 8;
    private static final int PAGE_SIZE = 128;
    private static final int PAGE_MASK = PAGE_SIZE - 1;
    private static final int PAGE_SHIFT = Integer.bitCount(PAGE_MASK);

    private double[][] pages = null;
    private int minPageIndex;

    public PaginatedStore() {
        this(Integer.MAX_VALUE);
    }

    PaginatedStore(int minPageIndex) {
        this.minPageIndex = minPageIndex;
    }

    PaginatedStore(PaginatedStore store) {
        this(store.minPageIndex);
        this.pages = store.isEmpty() ? null : deepCopy(store.pages);
    }

    @Override
    public boolean isEmpty() {
        // won't initialise any pages until a value is added,
        // and values can't be removed.
        return minPageIndex == Integer.MAX_VALUE;
    }

    @Override
    public int getMinIndex() {
        if (null != pages) {
            for (int i = 0; i < pages.length; ++i) {
                if (null != pages[i]) {
                    for (int j = 0; j < pages[i].length; ++j) {
                        if (pages[i][j] != 0D) {
                            return ((i + minPageIndex) << PAGE_SHIFT) + j;
                        }
                    }
                }
            }
        }
        throw new NoSuchElementException();
    }

    @Override
    public int getMaxIndex() {
        if (null != pages) {
            for (int i = pages.length - 1; i >= 0; --i) {
                if (null != pages[i]) {
                    for (int j = pages[i].length - 1; j >= 0; --j) {
                        if (pages[i][j] != 0D) {
                            return ((i + minPageIndex) << PAGE_SHIFT) + j;
                        }
                    }
                }
            }
        }
        throw new NoSuchElementException();
    }

    @Override
    public void forEach(BinAcceptor acceptor) {
        int base = minPageIndex;
        for (double[] page : pages) {
            if (null != page) {
                for (int i = 0; i < page.length; ++i) {
                    if (page[i] != 0) {
                        acceptor.accept(base + i, page[i]);
                    }
                }
            }
            base += PAGE_SIZE;
        }
    }

    @Override
    public double getTotalCount() {
        if (isEmpty()) {
            return 0D;
        }
        double total = 0D;
        for (double[] page : pages) {
            if (null != page) {
                for (double count : page) {
                    total += count;
                }
            }
        }
        return total;
    }

    @Override
    public void add(int index, double count) {
        if (count > 0) {
            int alignedIndex = alignedIndex(index);
            double[] page = getPage(alignedIndex >>> PAGE_SHIFT);
            page[alignedIndex & PAGE_MASK] += count;
        }
    }

    private double[] getPage(int pageIndex) {
        double[] page = pages[pageIndex];
        if (null == page) {
            page = pages[pageIndex] = new double[PAGE_SIZE];
        }
        return page;
    }

    private int alignedIndex(int index) {
        // get the index of the page this value should be stored in
        int pageIndex = index < 0
                ? ~(-index >>> PAGE_SHIFT)
                : index >>> PAGE_SHIFT;
        if (pageIndex < minPageIndex) {
            // then space needs to be made before the first page,
            // unless this is the first insertion
            if (isEmpty()) {
                lazyInit(pageIndex);
            } else {
                shiftPagesRight(pageIndex);
            }
        } else if (pageIndex >= minPageIndex + pages.length - 1) {
            // then space needs to be made after the last page
            extendTo(pageIndex);
        }
        // align the index relative to the start of the sketch
        return index + (-minPageIndex << PAGE_SHIFT);
    }

    private void lazyInit(int pageIndex) {
        minPageIndex = pageIndex;
        if (null == pages) {
            pages = new double[GROWTH][];
        }
    }

    private void shiftPagesRight(int pageIndex) {
        int requiredExtension = minPageIndex - pageIndex;
        if (requiredExtension > 0) {
            // check if there is space to shift into
            boolean canShiftRight = true;
            // check if there are enough null slots at the end of the array to shift into
            for (int i = 0; i < requiredExtension && canShiftRight && i < pages.length; ++i) {
                canShiftRight = null == pages[pages.length - i - 1];
            }
            if (canShiftRight) {
                System.arraycopy(pages, 0, pages, requiredExtension, pages.length - requiredExtension);
            } else {
                double[][] newPages = new double[pages.length + aligned(requiredExtension)][];
                System.arraycopy(pages, 0, newPages, requiredExtension, pages.length);
                this.pages = newPages;
            }
            Arrays.fill(pages, 0, requiredExtension, null);
            this.minPageIndex = pageIndex;
        }
    }

    private void extendTo(int pageIndex) {
        this.pages = Arrays.copyOf(pages, aligned(pageIndex - minPageIndex + 2));
    }

    @Override
    public void mergeWith(Store store) {
        if (store.isEmpty()) {
            return;
        }
        if (store instanceof PaginatedStore) {
            mergeWith((PaginatedStore) store);
        } else {
            store.getStream().forEach(this::add);
        }
    }

    private void mergeWith(PaginatedStore store) {
        if (isEmpty()) {
            this.pages = deepCopy(store.pages);
            this.minPageIndex = store.minPageIndex;
        } else {
            int min = minPageIndex;
            int max = minPageIndex + pages.length;
            int storeMin = store.minPageIndex;
            int storeMax = store.minPageIndex + store.pages.length;
            if (max < storeMin) {
                extendTo(storeMax);
                for (int i = 0; i < store.pages.length; ++i) {
                    double[] page = store.pages[i];
                    if (null != page) {
                        pages[i + storeMin - min] = Arrays.copyOf(page, page.length);
                    }
                }
            } else if (min > storeMax) {
                shiftPagesRight(storeMin);
                for (int i = 0; i < store.pages.length; ++i) {
                    double[] page = store.pages[i];
                    if (null != page) {
                        pages[i] = Arrays.copyOf(page, page.length);
                    }
                }
            } else if (min < storeMin) {
                if (storeMax > max) {
                    extendTo(storeMax);
                }
                for (int i = 0; i < store.pages.length; ++i) {
                    double[] page = store.pages[i];
                    if (null != page) {
                        double[] target = pages[i + storeMin - min];
                        if (null == target) {
                            pages[i + storeMin - min] = Arrays.copyOf(page, page.length);
                        } else {
                            for (int j = 0; j < page.length; ++j) {
                                target[j] += page[j];
                            }
                        }
                    }
                }
            } else {
                if (min > storeMin) {
                    shiftPagesRight(storeMin);
                }
                if (storeMax > max) {
                    extendTo(storeMax);
                }
                for (int i = 0; i < store.pages.length; ++i) {
                    double[] page = store.pages[i];
                    if (null != page) {
                        double[] target = pages[i];
                        if (null == target) {
                            pages[i] = Arrays.copyOf(page, page.length);
                        } else {
                            for (int j = 0; j < page.length; ++j) {
                                target[j] += page[j];
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public Store copy() {
        return new PaginatedStore(this);
    }

    @Override
    public void clear() {
        if (null != pages) {
            for (double[] page : pages) {
                if (null != page) {
                    Arrays.fill(page, 0D);
                }
            }
        }
        minPageIndex = Integer.MAX_VALUE;
    }

    @Override
    public Iterator<Bin> getAscendingIterator() {
        return new AscendingIterator();
    }

    @Override
    public Iterator<Bin> getDescendingIterator() {
        return new DescendingIterator();
    }

    private static int aligned(int required) {
        return (required + GROWTH - 1) & -GROWTH;
    }

    private static double[][] deepCopy(double[][] pages) {
        if (null != pages) {
            double[][] copy = new double[pages.length][];
            for (int i = 0; i < pages.length; ++i) {
                double[] page = pages[i];
                if (null != page) {
                    copy[i] = Arrays.copyOf(page, page.length);
                }
            }
            return copy;
        }
        return null;
    }

    private final class AscendingIterator implements Iterator<Bin> {

        int pageIndex = 0;
        int valueIndex = 0;
        double[] page = null;
        double next = Double.NaN;

        private AscendingIterator() {
            if (null != pages) {
                for (int i = 0; i < pages.length; ++i) {
                    if (pages[i] != null) {
                        page = pages[i];
                        pageIndex = i;
                        next = nextInPage();
                        break;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return !Double.isNaN(next);
        }

        @Override
        public Bin next() {
            double value = next;
            int index = ((pageIndex + minPageIndex) << PAGE_SHIFT) + valueIndex;
            ++valueIndex;
            next = nextInPage();
            if (Double.isNaN(next)) {
                for (int i = pageIndex + 1; i < pages.length; ++i) {
                    if (pages[i] != null) {
                        page = pages[i];
                        pageIndex = i;
                        valueIndex = 0;
                        next = nextInPage();
                        break;
                    }
                }
            }
            return new Bin(index, value);
        }

        private double nextInPage() {
            for (int i = valueIndex; i < page.length; ++i) {
                if (page[i] != 0D) {
                    valueIndex = i;
                    return page[i];
                }
            }
            return Double.NaN;
        }
    }

    private final class DescendingIterator implements Iterator<Bin> {

        int pageIndex = 0;
        int valueIndex = PAGE_SIZE - 1;
        double[] page = null;
        double previous = Double.NaN;

        private DescendingIterator() {
            if (null != pages) {
                for (int i = pages.length - 1; i >= 0; --i) {
                    if (pages[i] != null) {
                        page = pages[i];
                        pageIndex = i;
                        previous = previousInPage();
                        break;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return !Double.isNaN(previous);
        }

        @Override
        public Bin next() {
            double value = previous;
            int index = ((pageIndex + minPageIndex) << PAGE_SHIFT) + valueIndex;
            --valueIndex;
            previous = previousInPage();
            if (Double.isNaN(previous)) {
                for (int i = pageIndex - 1; i >= 0; --i) {
                    if (pages[i] != null) {
                        page = pages[i];
                        pageIndex = i;
                        valueIndex = page.length - 1;
                        previous = previousInPage();
                        break;
                    }
                }
            }
            return new Bin(index, value);
        }

        private double previousInPage() {
            for (int i = valueIndex; i >= 0; --i) {
                if (page[i] != 0D) {
                    valueIndex = i;
                    return page[i];
                }
            }
            return Double.NaN;
        }
    }
}
