package com.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class News2Test {

    @ParameterizedTest(name = "resp {0} -> {1}")
    @CsvSource({
            "0, 3", "8, 3",
            "9, 1", "11, 1",
            "12, 0", "20, 0",
            "21, 2", "24, 2",
            "25, 3", "60, 3",
            "8.4, 3", "8.5, 1"
    })
    void respBands(double value, int expected) {
        assertEquals(expected, News2.respScore(value));
    }

    @ParameterizedTest(name = "spo2 {0} -> {1}")
    @CsvSource({
            "50, 3", "91, 3",
            "92, 2", "93, 2",
            "94, 1", "95, 1",
            "96, 0", "100, 0",
            "95.4, 1", "95.5, 0"
    })
    void spo2Bands(double value, int expected) {
        assertEquals(expected, News2.spo2Score(value));
    }

    @ParameterizedTest(name = "bps {0} -> {1}")
    @CsvSource({
            "40, 3", "90, 3",
            "91, 2", "100, 2",
            "101, 1", "110, 1",
            "111, 0", "219, 0",
            "220, 3", "300, 3"
    })
    void systolicBands(double value, int expected) {
        assertEquals(expected, News2.systolicScore(value));
    }

    @ParameterizedTest(name = "pulse {0} -> {1}")
    @CsvSource({
            "20, 3", "40, 3",
            "41, 1", "50, 1",
            "51, 0", "90, 0",
            "91, 1", "110, 1",
            "111, 2", "130, 2",
            "131, 3", "200, 3"
    })
    void pulseBands(double value, int expected) {
        assertEquals(expected, News2.pulseScore(value));
    }

    @ParameterizedTest(name = "temp {0} -> {1}")
    @CsvSource({
            "30.0, 3", "35.0, 3",
            "35.01, 1", "35.1, 1", "36.0, 1",
            "36.01, 0", "36.1, 0", "37.0, 0", "38.0, 0",
            "38.01, 1", "38.1, 1", "39.0, 1",
            "39.01, 2", "39.1, 2", "41.0, 2"
    })
    void tempBands(double value, int expected) {
        assertEquals(expected, News2.tempScore(value));
    }

    @Test
    void missingOrInvalidInputGivesNull() {
        assertNull(News2.respScore(null));
        assertNull(News2.spo2Score(null));
        assertNull(News2.systolicScore(null));
        assertNull(News2.pulseScore(null));
        assertNull(News2.tempScore(null));
        assertNull(News2.respScore(Double.NaN));
        assertNull(News2.pulseScore(-1.0));
        assertNull(News2.tempScore(Double.POSITIVE_INFINITY));
    }

    @Test
    void partialSumAndComponentsIgnoreNull() {
        assertEquals(0, News2.partialSum());
        assertEquals(0, News2.components(null, null));
        assertEquals(5, News2.partialSum(3, null, 2, null, 0));
        assertEquals(3, News2.components(3, null, 2, null, 0));
    }
}
