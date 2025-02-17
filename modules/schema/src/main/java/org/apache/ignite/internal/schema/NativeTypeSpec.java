/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.tostring.S;

/**
 * Base class for storage built-in data types definition. The class contains predefined values for fixed-sized types and some of the
 * variable-sized types. Parameterized types, such as bitmask of size <code>n</code> bits or number of max n bytes are created using static
 * methods.
 *
 * <p>An instance of native type provides necessary indirection to read any field as an instance of {@code java.lang.Object} to avoid
 * switching inside the row methods.
 */
public enum NativeTypeSpec {
    /**
     * Native type representing a single-byte signed value.
     */
    INT8("int8", true) {
        /** {@inheritDoc} */
        @Override
        public Object objectValue(Row tup, int colIdx) {
            return tup.byteValueBoxed(colIdx);
        }
    },

    /**
     * Native type representing a two-bytes signed value.
     */
    INT16("int16", true) {
        /** {@inheritDoc} */
        @Override
        public Object objectValue(Row tup, int colIdx) {
            return tup.shortValueBoxed(colIdx);
        }
    },

    /**
     * Native type representing a four-bytes signed value.
     */
    INT32("int32", true) {
        /** {@inheritDoc} */
        @Override
        public Object objectValue(Row tup, int colIdx) {
            return tup.intValueBoxed(colIdx);
        }
    },

    /**
     * Native type representing an eight-bytes signed value.
     */
    INT64("int64", true) {
        /** {@inheritDoc} */
        @Override
        public Object objectValue(Row tup, int colIdx) {
            return tup.longValueBoxed(colIdx);
        }
    },

    /**
     * Native type representing a four-bytes floating-point value.
     */
    FLOAT("float", true) {
        /** {@inheritDoc} */
        @Override
        public Object objectValue(Row tup, int colIdx) {
            return tup.floatValueBoxed(colIdx);
        }
    },

    /**
     * Native type representing an eight-bytes floating-point value.
     */
    DOUBLE("double", true) {
        /** {@inheritDoc} */
        @Override
        public Object objectValue(Row tup, int colIdx) {
            return tup.doubleValueBoxed(colIdx);
        }
    },

    /**
     * Native type representing a BigDecimal.
     */
    DECIMAL("decimal", false) {
        /** {@inheritDoc} */
        @Override
        public Object objectValue(Row tup, int colIdx) {
            return tup.decimalValue(colIdx);
        }
    },

    /**
     * Native type representing a UUID.
     */
    UUID("uuid", true) {
        /** {@inheritDoc} */
        @Override
        public Object objectValue(Row tup, int colIdx) {
            return tup.uuidValue(colIdx);
        }
    },

    /**
     * Native type representing a string.
     */
    STRING("string") {
        /** {@inheritDoc} */
        @Override
        public Object objectValue(Row tup, int colIdx) {
            return tup.stringValue(colIdx);
        }
    },

    /**
     * Native type representing an arbitrary byte array.
     */
    BYTES("blob") {
        /** {@inheritDoc} */
        @Override
        public Object objectValue(Row tup, int colIdx) {
            return tup.bytesValue(colIdx);
        }
    },

    /**
     * Native type representing a bitmask.
     */
    BITMASK("bitmask", true) {
        /** {@inheritDoc} */
        @Override
        public Object objectValue(Row tup, int colIdx) {
            return tup.bitmaskValue(colIdx);
        }
    },

    /**
     * Native type representing a BigInteger.
     */
    NUMBER("number", false) {
        /** {@inheritDoc} */
        @Override
        public Object objectValue(Row tup, int colIdx) {
            return tup.numberValue(colIdx);
        }
    },

    /**
     * Native type representing a timezone-free date.
     */
    DATE("date", true) {
        /** {@inheritDoc} */
        @Override
        public Object objectValue(Row tup, int colIdx) {
            return tup.dateValue(colIdx);
        }
    },

    /**
     * Native type representing a timezone-free time.
     */
    TIME("time", true) {
        /** {@inheritDoc} */
        @Override
        public Object objectValue(Row tup, int colIdx) {
            return tup.timeValue(colIdx);
        }
    },

    /**
     * Native type representing a timezone-free datetime.
     */
    DATETIME("datetime", true) {
        /** {@inheritDoc} */
        @Override
        public Object objectValue(Row tup, int colIdx) {
            return tup.dateTimeValue(colIdx);
        }
    },

    /**
     * Native type representing a timestamp in milliseconds since Jan 1, 1970 00:00:00.000 (with no timezone).
     */
    TIMESTAMP("timestamp", true) {
        /** {@inheritDoc} */
        @Override
        public Object objectValue(Row tup, int colIdx) {
            return tup.timestampValue(colIdx);
        }
    };

    /** Flag indicating whether this type specifies a fixed-length type. */
    private final boolean fixedSize;

    /** Single-token type description. */
    private final String desc;

    /**
     * Constructs a varlength type with the given type description.
     *
     * @param desc Type description.
     */
    NativeTypeSpec(String desc) {
        this(desc, false);
    }

    /**
     * Constructs a type with the given description and size.
     *
     * @param desc      Type description.
     * @param fixedSize Flag indicating whether this type specifies a fixed-length type.
     */
    NativeTypeSpec(String desc, boolean fixedSize) {
        this.desc = desc;
        this.fixedSize = fixedSize;
    }

    /**
     * Get fixed length flag: {@code true} for fixed-length types, {@code false} otherwise.
     */
    public boolean fixedLength() {
        return fixedSize;
    }

    /**
     * Indirection method for getting an Object representation of the given type from the rows. This method does no type conversions and
     * will throw an exception if row column type differs from this type.
     *
     * @param row    Row to read the value from.
     * @param colIdx Column index to read.
     * @return An Object representation of the value.
     * @throws InvalidTypeException If this native type differs from the actual type of {@code colIdx}.
     */
    public abstract Object objectValue(Row row, int colIdx) throws InvalidTypeException;

    /**
     * Maps class to native type.
     *
     * @param cls Class to map to native type.
     * @return Native type.
     */
    public static NativeTypeSpec fromClass(Class<?> cls) {
        assert cls != null;

        // Primitives.
        if (cls == byte.class) {
            return NativeTypeSpec.INT8;
        } else if (cls == short.class) {
            return NativeTypeSpec.INT16;
        } else if (cls == int.class) {
            return NativeTypeSpec.INT32;
        } else if (cls == long.class) {
            return NativeTypeSpec.INT64;
        } else if (cls == float.class) {
            return NativeTypeSpec.FLOAT;
        } else if (cls == double.class) {
            return NativeTypeSpec.DOUBLE;
        } else if (cls == Byte.class) { // Boxed primitives.
            return NativeTypeSpec.INT8;
        } else if (cls == Short.class) {
            return NativeTypeSpec.INT16;
        } else if (cls == Integer.class) {
            return NativeTypeSpec.INT32;
        } else if (cls == Long.class) {
            return NativeTypeSpec.INT64;
        } else if (cls == Float.class) {
            return NativeTypeSpec.FLOAT;
        } else if (cls == Double.class) {
            return NativeTypeSpec.DOUBLE;
        } else if (cls == LocalDate.class) { // Temporal types.
            return NativeTypeSpec.DATE;
        } else if (cls == LocalTime.class) {
            return NativeTypeSpec.TIME;
        } else if (cls == LocalDateTime.class) {
            return NativeTypeSpec.DATETIME;
        } else if (cls == Instant.class) {
            return NativeTypeSpec.TIMESTAMP;
        } else if (cls == byte[].class) { // Other types.
            return NativeTypeSpec.BYTES;
        } else if (cls == String.class) {
            return NativeTypeSpec.STRING;
        } else if (cls == java.util.UUID.class) {
            return NativeTypeSpec.UUID;
        } else if (cls == BitSet.class) {
            return NativeTypeSpec.BITMASK;
        } else if (cls == BigInteger.class) {
            return NativeTypeSpec.NUMBER;
        } else if (cls == BigDecimal.class) {
            return NativeTypeSpec.DECIMAL;
        }

        return null;
    }

    /**
     * Maps object to native type.
     *
     * @param val Object to map.
     * @return Native type.
     */
    public static NativeTypeSpec fromObject(Object val) {
        return val != null ? fromClass(val.getClass()) : null;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(NativeTypeSpec.class.getSimpleName(),
                "name", name(),
                "fixed", fixedLength());
    }
}
