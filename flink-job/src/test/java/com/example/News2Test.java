package com.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @ParameterizedTest(name = "spo2 {0} with scale 1 (explicit or missing), o2 {1} -> {2}")
    @CsvSource({
            "91, true, 3", "92, false, 2", "94, , 1", "96, true, 0", "100, false, 0"
    })
    void spo2Scale1IgnoresOxygen(double value, Boolean o2, int expected) {
        assertEquals(expected, News2.spo2Score(value, 1, o2));
        assertEquals(expected, News2.spo2Score(value, null, o2));
    }

    @ParameterizedTest(name = "spo2 scale 2: {0} on air -> {1}")
    @CsvSource({
            "50, 3", "83, 3",
            "84, 2", "85, 2",
            "86, 1", "87, 1",
            "88, 0", "92, 0",
            "93, 0", "97, 0", "100, 0",
            "83.4, 3", "83.5, 2", "92.5, 0"
    })
    void spo2Scale2OnAir(double value, int expected) {
        assertEquals(expected, News2.spo2Score(value, 2, false));
    }

    @ParameterizedTest(name = "spo2 scale 2: {0} on oxygen -> {1}")
    @CsvSource({
            "50, 3", "83, 3",
            "84, 2", "85, 2",
            "86, 1", "87, 1",
            "88, 0", "92, 0",
            "93, 1", "94, 1",
            "95, 2", "96, 2",
            "97, 3", "100, 3",
            "92.4, 0", "92.5, 1", "96.5, 3"
    })
    void spo2Scale2OnOxygen(double value, int expected) {
        assertEquals(expected, News2.spo2Score(value, 2, true));
    }

    @ParameterizedTest(name = "spo2 scale 2: {0} with unknown o2 -> {1}")
    @CsvSource({"83, 3", "85, 2", "87, 1", "88, 0", "92, 0"})
    void spo2Scale2BelowTargetDoesNotNeedOxygen(double value, int expected) {
        assertEquals(expected, News2.spo2Score(value, 2, null));
    }

    @Test
    void spo2Scale2AboveTargetWithUnknownOxygenIsNull() {
        assertNull(News2.spo2Score(93.0, 2, null));
        assertNull(News2.spo2Score(100.0, 2, null));
    }

    @Test
    void spo2UnknownScaleOrInvalidValueIsNull() {
        assertNull(News2.spo2Score(95.0, 3, true));
        assertNull(News2.spo2Score(95.0, 0, false));
        assertNull(News2.spo2Score(95.0, -1, false));
        assertNull(News2.spo2Score(null, 2, true));
        assertNull(News2.spo2Score(Double.NaN, 2, false));
        assertNull(News2.spo2Score(null, 1, true));
    }

    @Test
    void o2Score() {
        assertEquals(2, News2.o2Score(true));
        assertEquals(0, News2.o2Score(false));
        assertNull(News2.o2Score(null));
    }

    @ParameterizedTest(name = "acvpu \"{0}\" -> {1}")
    @CsvSource({
            "A, 0", "a, 0", "' A ', 0",
            "C, 3", "V, 3", "P, 3", "U, 3", "u, 3"
    })
    void consciousnessScore(String acvpu, int expected) {
        assertEquals(expected, News2.consciousnessScore(acvpu));
    }

    @Test
    void consciousnessMissingOrUnknownIsNull() {
        assertNull(News2.consciousnessScore(null));
        assertNull(News2.consciousnessScore(""));
        assertNull(News2.consciousnessScore("X"));
        assertNull(News2.consciousnessScore("Alert"));
    }

    @Test
    void totalOnlyWhenAllSevenComponentsArePresent() {
        assertEquals(7, News2.COMPONENTS);
        assertTrue(News2.complete(0, 0, 0, 0, 0, 0, 0));
        assertEquals(0, News2.total(0, 0, 0, 0, 0, 0, 0));
        assertEquals(20, News2.total(3, 3, 2, 3, 3, 3, 3));
        assertEquals(5, News2.total(1, 0, 2, 0, 1, 0, 1));
        assertFalse(News2.complete(1, 0, 2, 0, 1, null, 1));
        assertNull(News2.total(1, 0, 2, 0, 1, null, 1));
        // fewer than seven arguments are never a complete score
        assertFalse(News2.complete(0, 0, 0, 0, 0));
        assertNull(News2.total(0, 0, 0, 0, 0));
        assertNull(News2.total());
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
