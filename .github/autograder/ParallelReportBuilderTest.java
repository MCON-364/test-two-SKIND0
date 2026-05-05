package edu.touro.las.mcon364.test2;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class ParallelReportBuilderTest {

    /** Creates pageCount batches of consecutive transaction amounts starting at 1. */
    private List<List<ParallelReportBuilder.Transaction>> makeBatches(int batchCount, int batchSize) {
        List<List<ParallelReportBuilder.Transaction>> batches = new ArrayList<>();
        int amount = 1;
        for (int b = 0; b < batchCount; b++) {
            List<ParallelReportBuilder.Transaction> batch = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                batch.add(new ParallelReportBuilder.Transaction("t-" + amount, amount));
                amount++;
            }
            batches.add(batch);
        }
        return batches;
    }

    // ---- input validation ---------------------------------------------------

    @Test
    void throwsOnNullBatches() {
        ParallelReportBuilder b = new ParallelReportBuilder();
        assertThrows(IllegalArgumentException.class, () -> b.generateReport(null, 2));
    }

    @Test
    void throwsOnEmptyBatches() {
        ParallelReportBuilder b = new ParallelReportBuilder();
        assertThrows(IllegalArgumentException.class, () -> b.generateReport(List.of(), 2));
    }

    @Test
    void throwsOnZeroWorkers() {
        ParallelReportBuilder b = new ParallelReportBuilder();
        assertThrows(IllegalArgumentException.class, () -> b.generateReport(makeBatches(2, 3), 0));
    }

    // ---- basic correctness --------------------------------------------------

    @Test
    void generateReportReturnsNonNull() throws InterruptedException, ExecutionException {
        ParallelReportBuilder b = new ParallelReportBuilder();
        assertNotNull(b.generateReport(makeBatches(2, 3), 2));
    }

    @Test
    void totalAmountIsCorrect() throws InterruptedException, ExecutionException {
        // 2 batches: [1,2,3] and [4,5,6] -> total = 21
        ParallelReportBuilder b = new ParallelReportBuilder();
        var s = b.generateReport(makeBatches(2, 3), 2);
        assertEquals(21L, s.totalAmount());
    }

    @Test
    void totalCountIsCorrect() throws InterruptedException, ExecutionException {
        // 3 batches x 4 transactions = 12
        ParallelReportBuilder b = new ParallelReportBuilder();
        var s = b.generateReport(makeBatches(3, 4), 3);
        assertEquals(12L, s.totalCount());
    }

    @Test
    void globalMaxIsCorrect() throws InterruptedException, ExecutionException {
        // 2 batches: [1,2,3] and [4,5,6] -> max = 6
        ParallelReportBuilder b = new ParallelReportBuilder();
        var s = b.generateReport(makeBatches(2, 3), 2);
        assertEquals(6, s.globalMax());
    }

    @Test
    void globalMinIsCorrect() throws InterruptedException, ExecutionException {
        // 2 batches: [1,2,3] and [4,5,6] -> min = 1
        ParallelReportBuilder b = new ParallelReportBuilder();
        var s = b.generateReport(makeBatches(2, 3), 2);
        assertEquals(1, s.globalMin());
    }

    @Test
    void batchesProcessedEqualsInputSize() throws InterruptedException, ExecutionException {
        int batchCount = 5;
        ParallelReportBuilder b = new ParallelReportBuilder();
        var s = b.generateReport(makeBatches(batchCount, 10), 3);
        assertEquals(batchCount, s.batchesProcessed());
    }

    // ---- getProcessedBatchCount getter --------------------------------------

    @Test
    void getProcessedBatchCountMatchesSummary() throws InterruptedException, ExecutionException {
        ParallelReportBuilder b = new ParallelReportBuilder();
        b.generateReport(makeBatches(4, 5), 4);
        assertEquals(4, b.getProcessedBatchCount());
    }

    // ---- edge case: single batch --------------------------------------------

    @Test
    void singleBatchSummaryIsCorrect() throws InterruptedException, ExecutionException {
        List<List<ParallelReportBuilder.Transaction>> batches = List.of(List.of(
                new ParallelReportBuilder.Transaction("t1", 7),
                new ParallelReportBuilder.Transaction("t2", 3),
                new ParallelReportBuilder.Transaction("t3", 9),
                new ParallelReportBuilder.Transaction("t4", 1)
        ));
        ParallelReportBuilder b = new ParallelReportBuilder();
        var s = b.generateReport(batches, 1);
        assertEquals(20L, s.totalAmount());
        assertEquals(4L,  s.totalCount());
        assertEquals(9,   s.globalMax());
        assertEquals(1,   s.globalMin());
        assertEquals(1,   s.batchesProcessed());
    }

    // ---- larger parallel run ------------------------------------------------

    @Test
    void largeParallelRunProducesCorrectTotal() throws InterruptedException, ExecutionException {
        // 6 batches x 100 transactions = amounts 1..600; sum = 600*601/2 = 180300
        int batchCount = 6;
        int batchSize  = 100;
        ParallelReportBuilder b = new ParallelReportBuilder();
        var s = b.generateReport(makeBatches(batchCount, batchSize), batchCount);
        int n = batchCount * batchSize;
        long expected = (long) n * (n + 1) / 2;
        assertEquals(expected, s.totalAmount());
        assertEquals((long) n, s.totalCount());
        assertEquals(n, s.globalMax());
        assertEquals(1, s.globalMin());
        assertEquals(batchCount, s.batchesProcessed());
    }
}
