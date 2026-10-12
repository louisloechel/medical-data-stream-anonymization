package com.example;

/**
 * NEWS2 sub-scores according to the Royal College of Physicians,
 * National Early Warning Score (NEWS) 2, 2017.
 *
 * <p>All methods are pure. A missing or invalid input (null, NaN, negative)
 * yields {@code null} instead of a score, so that missing measurements are
 * never mistaken for extreme values.
 *
 * <p>Respiration rate, SpO2, systolic blood pressure and pulse are charted as
 * integers in NEWS2; non-integer inputs are rounded half-up before banding.
 * Temperature uses continuous comparisons (e.g. 36.05 lies in 36.1-38.0).
 *
 * <p>The full NEWS2 has seven components: respiration rate, SpO2 (scale 1 or
 * scale 2), air or oxygen, systolic blood pressure, pulse, consciousness
 * (ACVPU) and temperature. {@link #COMPONENTS} is the number of components
 * of a complete score.
 */
public final class News2 {

    /** Number of parameters of the complete NEWS2. */
    public static final int COMPONENTS = 7;

    /** SpO2 scale 1 (default) and scale 2 (target 88-92%, e.g. hypercapnic respiratory failure). */
    public static final int SPO2_SCALE_1 = 1;
    public static final int SPO2_SCALE_2 = 2;

    private News2() {
    }

    private static boolean invalid(Double value) {
        return value == null || value.isNaN() || value.isInfinite() || value < 0;
    }

    private static long integer(double value) {
        return Math.round(value);
    }

    /** Respiration rate (per minute): <=8:3, 9-11:1, 12-20:0, 21-24:2, >=25:3. */
    public static Integer respScore(Double value) {
        if (invalid(value)) {
            return null;
        }
        long v = integer(value);
        if (v <= 8) {
            return 3;
        } else if (v <= 11) {
            return 1;
        } else if (v <= 20) {
            return 0;
        } else if (v <= 24) {
            return 2;
        }
        return 3;
    }

    /** SpO2 scale 1 (%): <=91:3, 92-93:2, 94-95:1, >=96:0. */
    public static Integer spo2Score(Double value) {
        if (invalid(value)) {
            return null;
        }
        long v = integer(value);
        if (v <= 91) {
            return 3;
        } else if (v <= 93) {
            return 2;
        } else if (v <= 95) {
            return 1;
        }
        return 0;
    }

    /**
     * SpO2 sub-score on the given scale. {@code scale} null means scale 1.
     *
     * <p>Scale 2 (%): <=83:3, 84-85:2, 86-87:1, 88-92:0, >=93 on air:0,
     * 93-94 on oxygen:1, 95-96 on oxygen:2, >=97 on oxygen:3. If SpO2 is >=93
     * on scale 2 and it is unknown whether the patient is on oxygen
     * ({@code onOxygen} null), the sub-score is null.
     *
     * @return the sub-score, or null for a missing/invalid SpO2 or an unknown scale
     */
    public static Integer spo2Score(Double value, Integer scale, Boolean onOxygen) {
        if (scale == null || scale == SPO2_SCALE_1) {
            return spo2Score(value);
        }
        if (scale != SPO2_SCALE_2 || invalid(value)) {
            return null;
        }
        long v = integer(value);
        if (v <= 83) {
            return 3;
        } else if (v <= 85) {
            return 2;
        } else if (v <= 87) {
            return 1;
        } else if (v <= 92) {
            return 0;
        }
        if (onOxygen == null) {
            return null;
        }
        if (!onOxygen) {
            return 0;
        } else if (v <= 94) {
            return 1;
        } else if (v <= 96) {
            return 2;
        }
        return 3;
    }

    /** Air or oxygen: supplemental oxygen (incl. ventilation):2, air:0, unknown: null. */
    public static Integer o2Score(Boolean onOxygen) {
        if (onOxygen == null) {
            return null;
        }
        return onOxygen ? 2 : 0;
    }

    /**
     * Consciousness (ACVPU): "A" (alert):0; "C" (new confusion), "V" (voice),
     * "P" (pain), "U" (unresponsive):3. The code is trimmed and compared
     * case-insensitively; anything else (null, empty, unknown) gives null.
     */
    public static Integer consciousnessScore(String acvpu) {
        if (acvpu == null) {
            return null;
        }
        switch (acvpu.trim().toUpperCase(java.util.Locale.ROOT)) {
            case "A":
                return 0;
            case "C":
            case "V":
            case "P":
            case "U":
                return 3;
            default:
                return null;
        }
    }

    /** Systolic blood pressure (mmHg): <=90:3, 91-100:2, 101-110:1, 111-219:0, >=220:3. */
    public static Integer systolicScore(Double value) {
        if (invalid(value)) {
            return null;
        }
        long v = integer(value);
        if (v <= 90) {
            return 3;
        } else if (v <= 100) {
            return 2;
        } else if (v <= 110) {
            return 1;
        } else if (v <= 219) {
            return 0;
        }
        return 3;
    }

    /** Pulse (per minute): <=40:3, 41-50:1, 51-90:0, 91-110:1, 111-130:2, >=131:3. */
    public static Integer pulseScore(Double value) {
        if (invalid(value)) {
            return null;
        }
        long v = integer(value);
        if (v <= 40) {
            return 3;
        } else if (v <= 50) {
            return 1;
        } else if (v <= 90) {
            return 0;
        } else if (v <= 110) {
            return 1;
        } else if (v <= 130) {
            return 2;
        }
        return 3;
    }

    /** Temperature (degrees Celsius): <=35.0:3, 35.1-36.0:1, 36.1-38.0:0, 38.1-39.0:1, >=39.1:2. */
    public static Integer tempScore(Double value) {
        if (invalid(value)) {
            return null;
        }
        double v = value;
        if (v <= 35.0) {
            return 3;
        } else if (v <= 36.0) {
            return 1;
        } else if (v <= 38.0) {
            return 0;
        } else if (v <= 39.0) {
            return 1;
        }
        return 2;
    }

    /** Sum of the available (non-null) sub-scores. */
    public static int partialSum(Integer... scores) {
        int sum = 0;
        for (Integer s : scores) {
            if (s != null) {
                sum += s;
            }
        }
        return sum;
    }

    /** Number of available (non-null) sub-scores. */
    public static int components(Integer... scores) {
        int n = 0;
        for (Integer s : scores) {
            if (s != null) {
                n++;
            }
        }
        return n;
    }

    /** True if all {@link #COMPONENTS} sub-scores are given and non-null. */
    public static boolean complete(Integer... scores) {
        return scores.length == COMPONENTS && components(scores) == COMPONENTS;
    }

    /** NEWS2 total over all seven sub-scores, or null if any of them is missing. */
    public static Integer total(Integer... scores) {
        return complete(scores) ? partialSum(scores) : null;
    }
}
