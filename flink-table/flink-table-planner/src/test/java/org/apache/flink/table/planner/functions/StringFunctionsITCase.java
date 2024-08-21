/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.lit;

/** Test String functions correct behaviour. */
class StringFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                        bTrimTestCases(),
                        eltTestCases(),
                        printfTestCases(),
                        regexpExtractTestCases(),
                        toNumberTestCases(),
                        translateTestCases())
                .flatMap(s -> s);
    }

    private Stream<TestSetSpec> bTrimTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.BTRIM)
                        .onFieldsWithData(null, "  \uD83D\uDE00www.apache.org  \f   ")
                        .andDataTypes(DataTypes.STRING(), DataTypes.STRING())
                        // null input
                        .testResult($("f0").btrim(), "BTRIM(f0)", null, DataTypes.STRING())
                        .testResult(
                                $("f0").btrim($("f1")), "BTRIM(f0, f1)", null, DataTypes.STRING())
                        .testResult(
                                $("f1").btrim($("f0")), "BTRIM(f1, f0)", null, DataTypes.STRING())
                        // special chars
                        .testResult(
                                $("f1").btrim(" \f\uD83D\uDE00"),
                                "BTRIM(f1, ' \f\uD83D\uDE00')",
                                "www.apache.org",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").btrim("\uD83D\uDE00 "),
                                "BTRIM(f1, '\uD83D\uDE00 ')",
                                "www.apache.org  \f",
                                DataTypes.STRING())
                        // return type
                        .testResult(
                                lit("  www.apache.org  ").btrim(),
                                "BTRIM('  www.apache.org  ')",
                                "www.apache.org",
                                DataTypes.STRING().notNull())
                        // normal cases
                        .testResult(
                                lit("www.apache.org").btrim("a"),
                                "BTRIM('www.apache.org', 'a')",
                                "www.apache.org",
                                DataTypes.STRING().notNull())
                        .testResult(
                                $("f1").btrim(),
                                "BTRIM(f1)",
                                "\uD83D\uDE00www.apache.org  \f",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").btrim("\f"),
                                "BTRIM(f1, '\f')",
                                "  \uD83D\uDE00www.apache.org  \f   ",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").btrim($("f1")), "BTRIM(f1, f1)", "", DataTypes.STRING()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.BTRIM, "Validation Error")
                        .onFieldsWithData(100, "123")
                        .andDataTypes(DataTypes.INT(), DataTypes.STRING())
                        .testTableApiValidationError(
                                $("f0").btrim(),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BTRIM(str <CHARACTER_STRING>)\n"
                                        + "BTRIM(str <CHARACTER_STRING>, trimStr <CHARACTER_STRING>)")
                        .testSqlValidationError(
                                "BTRIM(f0)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BTRIM(str <CHARACTER_STRING>)\n"
                                        + "BTRIM(str <CHARACTER_STRING>, trimStr <CHARACTER_STRING>)")
                        .testTableApiValidationError(
                                $("f1").btrim($("f0")),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BTRIM(str <CHARACTER_STRING>)\n"
                                        + "BTRIM(str <CHARACTER_STRING>, trimStr <CHARACTER_STRING>)")
                        .testSqlValidationError(
                                "BTRIM(f1, f0)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BTRIM(str <CHARACTER_STRING>)\n"
                                        + "BTRIM(str <CHARACTER_STRING>, trimStr <CHARACTER_STRING>)"));
    }

    private Stream<TestSetSpec> eltTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ELT)
                        .onFieldsWithData(null, null, null, new byte[] {1, 2, 3})
                        .andDataTypes(
                                DataTypes.INT(),
                                DataTypes.STRING(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES())
                        // null input
                        .testResult(
                                $("f0").elt("a", "b"), "ELT(f0, 'a', 'b')", null, DataTypes.CHAR(1))
                        .testResult(lit(1).elt($("f1")), "ELT(1, f1)", null, DataTypes.STRING())
                        .testResult(
                                lit(1).elt("a", $("f1")),
                                "ELT(1, 'a', f1)",
                                "a",
                                DataTypes.STRING())
                        // invalid index
                        .testResult(
                                lit(0).elt("a", "b"), "ELT(0, 'a', 'b')", null, DataTypes.CHAR(1))
                        .testResult(
                                lit(3).elt("a", "b"), "ELT(3, 'a', 'b')", null, DataTypes.CHAR(1))
                        .testResult(
                                lit(9223372036854775807L).elt("ab", "b"),
                                "ELT(9223372036854775807, 'ab', 'b')",
                                null,
                                DataTypes.VARCHAR(2))
                        // normal cases
                        .testResult(
                                lit(1).elt("scala", "java"),
                                "ELT(1, 'scala', 'java')",
                                "scala",
                                DataTypes.VARCHAR(5))
                        .testResult(
                                lit(2).elt("a", "b"), "ELT(2, 'a', 'b')", "b", DataTypes.CHAR(1))
                        .testResult(
                                lit(2).elt($("f2"), $("f3"), $("f3")),
                                "ELT(2, f2, f3, f3)",
                                new byte[] {1, 2, 3},
                                DataTypes.BYTES())
                        .testResult(
                                lit(3).elt($("f2"), $("f3"), $("f2")),
                                "ELT(3, f2, f3, f2)",
                                null,
                                DataTypes.BYTES()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ELT, "Validation Error")
                        .onFieldsWithData("1", "1".getBytes(), BigDecimal.valueOf(1))
                        .andDataTypes(
                                DataTypes.STRING(), DataTypes.BYTES(), DataTypes.DECIMAL(1, 0))
                        .testTableApiValidationError(
                                lit(1).elt($("f0"), $("f1")),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ELT(index <INTEGER_NUMERIC>, expr <CHARACTER_STRING>, exprs <CHARACTER_STRING>...)\n"
                                        + "ELT(index <INTEGER_NUMERIC>, expr <BINARY_STRING>, exprs <BINARY_STRING>...)")
                        .testSqlValidationError(
                                "ELT(1, f0, f1)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ELT(index <INTEGER_NUMERIC>, expr <CHARACTER_STRING>, exprs <CHARACTER_STRING>...)\n"
                                        + "ELT(index <INTEGER_NUMERIC>, expr <BINARY_STRING>, exprs <BINARY_STRING>...)")
                        .testTableApiValidationError(
                                $("f2").elt("a"),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ELT(index <INTEGER_NUMERIC>, expr <CHARACTER_STRING>, exprs <CHARACTER_STRING>...)\n"
                                        + "ELT(index <INTEGER_NUMERIC>, expr <BINARY_STRING>, exprs <BINARY_STRING>...)")
                        .testSqlValidationError(
                                "ELT(f2, 'a')",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ELT(index <INTEGER_NUMERIC>, expr <CHARACTER_STRING>, exprs <CHARACTER_STRING>...)\n"
                                        + "ELT(index <INTEGER_NUMERIC>, expr <BINARY_STRING>, exprs <BINARY_STRING>...)")
                        .testTableApiValidationError(
                                lit(-1).elt("a"),
                                "Index must be an integer starting from '0', but was '-1'.")
                        .testSqlValidationError(
                                "ELT(-1, 'a')",
                                "Index must be an integer starting from '0', but was '-1'."));
    }

    private Stream<TestSetSpec> printfTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.PRINTF)
                        .onFieldsWithData(
                                null,
                                "%d %s ",
                                1024,
                                10.24,
                                "1024",
                                LocalDate.of(2024, 7, 30),
                                LocalTime.of(10, 24, 0),
                                LocalDateTime.of(2024, 7, 30, 10, 24, 0, 256),
                                Instant.parse("2024-07-30T10:24:00.256Z"),
                                Duration.of(5, ChronoUnit.HOURS),
                                Boolean.TRUE,
                                10.24)
                        .andDataTypes(
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.INT(),
                                DataTypes.FLOAT(),
                                DataTypes.STRING(),
                                DataTypes.DATE(),
                                DataTypes.TIME(),
                                DataTypes.TIMESTAMP(),
                                DataTypes.TIMESTAMP_LTZ(),
                                DataTypes.INTERVAL(DataTypes.HOUR()),
                                DataTypes.BOOLEAN(),
                                DataTypes.DECIMAL(4, 2))
                        // null input
                        .testResult($("f0").printf(), "PRINTF(f0)", null, DataTypes.STRING())
                        .testResult(
                                lit("%d %s").printf($("f0"), $("f0")),
                                "PRINTF('%d %s', f0, f0)",
                                "null null",
                                DataTypes.STRING())
                        // empty obj
                        .testResult(
                                lit("empty").printf(),
                                "PRINTF('empty')",
                                "empty",
                                DataTypes.STRING())
                        // invalid format
                        .testResult(lit("%s").printf(), "PRINTF('%s')", null, DataTypes.STRING())
                        // extra args
                        .testResult(
                                lit("%s %s").printf(1, 2, 3),
                                "PRINTF('%s %s', 1, 2, 3)",
                                "1 2",
                                DataTypes.STRING())
                        // numeric
                        .testResult(
                                lit("%08d").printf($("f2")),
                                "PRINTF('%08d', f2)",
                                "00001024",
                                DataTypes.STRING())
                        .testResult(
                                lit("%.1f").printf($("f3")),
                                "PRINTF('%.1f', f3)",
                                "10.2",
                                DataTypes.STRING())
                        // string
                        .testResult(
                                lit("%s").printf($("f4")),
                                "PRINTF('%s', f4)",
                                "1024",
                                DataTypes.STRING())
                        // datetime
                        .testResult(
                                lit("%s").printf($("f5")),
                                "PRINTF('%s', f5)",
                                "19934",
                                DataTypes.STRING())
                        .testResult(
                                lit("%s").printf($("f6")),
                                "PRINTF('%s', f6)",
                                "37440000",
                                DataTypes.STRING())
                        .testResult(
                                lit("%s").printf($("f7")),
                                "PRINTF('%s', f7)",
                                "2024-07-30T10:24",
                                DataTypes.STRING())
                        .testResult(
                                lit("%s").printf($("f8")),
                                "PRINTF('%s', f8)",
                                "2024-07-30T10:24:00.256",
                                DataTypes.STRING())
                        // interval
                        .testResult(
                                lit("%s").printf($("f9")),
                                "PRINTF('%s', f9)",
                                "18000000",
                                DataTypes.STRING())
                        // boolean
                        .testResult(
                                lit("%b").printf($("f10")),
                                "PRINTF('%b', f10)",
                                "true",
                                DataTypes.STRING())
                        // decimal
                        .testResult(
                                lit("%s").printf($("f11")),
                                "PRINTF('%s', f11)",
                                "10.24",
                                DataTypes.STRING()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.PRINTF, "Validation Error")
                        .onFieldsWithData(1024)
                        .andDataTypes(DataTypes.INT())
                        .testTableApiValidationError(
                                $("f0").printf(),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "PRINTF(format <CHARACTER_STRING>, obj <ANY>...)")
                        .testSqlValidationError(
                                "PRINTF(f0)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "PRINTF(format <CHARACTER_STRING>, obj <ANY>...)"));
    }

    private Stream<TestSetSpec> regexpExtractTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.REGEXP_EXTRACT, "Check return type")
                        .onFieldsWithData("22", "ABC")
                        .testResult(
                                call("regexpExtract", $("f0"), "[A-Z]+"),
                                "REGEXP_EXTRACT(f0,'[A-Z]+')",
                                null,
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("regexpExtract", $("f1"), "[A-Z]+"),
                                "REGEXP_EXTRACT(f1, '[A-Z]+')",
                                "ABC",
                                DataTypes.STRING().nullable()));
    }

    private Stream<TestSetSpec> toNumberTestCases() {
        return Stream.of(
                // for full test cases, see NumberParserTest.java
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.TO_NUMBER)
                        .onFieldsWithData(null, "123.45")
                        .andDataTypes(DataTypes.STRING(), DataTypes.STRING())
                        // null input
                        .testResult(
                                $("f0").toNumber("999.99"),
                                "TO_NUMBER(f0, '999.99')",
                                null,
                                DataTypes.DECIMAL(5, 2))
                        // mismatch
                        .testResult(
                                lit("$").toNumber("L999"),
                                "TO_NUMBER('$', 'L999')",
                                null,
                                DataTypes.DECIMAL(3, 0))
                        .testResult(
                                lit("123").toNumber("000,099"),
                                "TO_NUMBER('123', '000,099')",
                                null,
                                DataTypes.DECIMAL(6, 0))
                        .testResult(
                                lit("123.45").toNumber("L999.99"),
                                "TO_NUMBER('123.45', 'L999.99')",
                                null,
                                DataTypes.DECIMAL(5, 2))
                        .testResult(
                                lit("123.45").toNumber("000"),
                                "TO_NUMBER('123.45', '000')",
                                null,
                                DataTypes.DECIMAL(3, 0))
                        .testResult(
                                lit("1D2").toNumber("0D0"),
                                "TO_NUMBER('1D2', '0D0')",
                                null,
                                DataTypes.DECIMAL(2, 1))
                        // match
                        .testResult(
                                lit("1 2  3.4\n5\f6").toNumber("099.099"),
                                "TO_NUMBER('1 2  3.4\n5\f6', '099.099')",
                                BigDecimal.valueOf(123456L, 3),
                                DataTypes.DECIMAL(6, 3))
                        .testResult(
                                lit("123.456").toNumber("999,999.999"),
                                "TO_NUMBER('123.456', '999,999.999')",
                                BigDecimal.valueOf(123456L, 3),
                                DataTypes.DECIMAL(9, 3))
                        .testResult(
                                lit("123.45").toNumber("999.999"),
                                "TO_NUMBER('123.45', '999.999')",
                                BigDecimal.valueOf(123450L, 3),
                                DataTypes.DECIMAL(6, 3))
                        .testResult(
                                lit(".1").toNumber("900.90"),
                                "TO_NUMBER('.1', '900.90')",
                                BigDecimal.valueOf(10L, 2),
                                DataTypes.DECIMAL(5, 2))
                        .testResult(
                                lit("<-123,456.78>").toNumber("MI999,000.99PR"),
                                "TO_NUMBER('<-123,456.78>', 'MI999,000.99PR')",
                                BigDecimal.valueOf(12345678L, 2),
                                DataTypes.DECIMAL(8, 2)),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.TO_NUMBER, "Validation Error")
                        .onFieldsWithData("123", "000")
                        .andDataTypes(DataTypes.STRING(), DataTypes.STRING())
                        .testTableApiValidationError(
                                $("f0").toNumber(900),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "TO_NUMBER(expr <CHARACTER_STRING>, format [<CHARACTER_STRING> & <LITERAL NOT NULL>])")
                        .testSqlValidationError(
                                "TO_NUMBER(f0, 900)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "TO_NUMBER(expr <CHARACTER_STRING>, format [<CHARACTER_STRING> & <LITERAL NOT NULL>])")
                        // not literal
                        .testTableApiValidationError(
                                lit("0$").toNumber($("f1")), "Literal expected.")
                        .testSqlValidationError("TO_NUMBER('0$', f1)", "Literal expected.")
                        // null format
                        .testTableApiValidationError(
                                $("f0").toNumber(null),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "TO_NUMBER(expr <CHARACTER_STRING>, format [<CHARACTER_STRING> & <LITERAL NOT NULL>])")
                        .testSqlValidationError("TO_NUMBER(f0, NULL)", "Illegal use of 'NULL'")
                        // invalid format
                        .testTableApiValidationError(
                                lit("0$").toNumber("900$"),
                                "Could not infer an output type for the given arguments.")
                        .testSqlValidationError(
                                "TO_NUMBER('0$', '900$')",
                                "Could not infer an output type for the given arguments.")
                        // invalid output type
                        .testTableApiValidationError(
                                $("f0").toNumber("9999999990999999999099999999909999999990"),
                                "Could not infer an output type for the given arguments.")
                        .testSqlValidationError(
                                "TO_NUMBER(f0, '9999999990999999999099999999909999999990')",
                                "Could not infer an output type for the given arguments."));
    }

    private Stream<TestSetSpec> translateTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.TRANSLATE)
                        .onFieldsWithData(
                                null, "www.apache.org", "", "翻译test，测试", "www.\uD83D\uDE00.org")
                        .andDataTypes(
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING())
                        // null input
                        .testResult(
                                $("f0").translate("abc", "123"),
                                "TRANSLATE(f0, 'abc', '123')",
                                null,
                                DataTypes.STRING())
                        .testResult(
                                $("f1").translate($("f0"), "123"),
                                "TRANSLATE(f1, f0, '123')",
                                "www.apache.org",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").translate("abc", $("f0")),
                                "TRANSLATE(f1, 'abc', f0)",
                                "www.phe.org",
                                DataTypes.STRING())
                        // empty input
                        .testResult(
                                $("f2").translate("abc", "123"),
                                "TRANSLATE(f2, 'abc', '123')",
                                "",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").translate($("f2"), "123"),
                                "TRANSLATE(f1, f2, '123')",
                                "www.apache.org",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").translate("abc", $("f2")),
                                "TRANSLATE(f1, 'abc', f2)",
                                "www.phe.org",
                                DataTypes.STRING())
                        // from longer than to
                        .testResult(
                                $("f1").translate("abcde", "123"),
                                "TRANSLATE(f1, 'abcde', '123')",
                                "www.1p13h.org",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").translate("abcde.", "123"),
                                "TRANSLATE(f1, 'abcde.', '123')",
                                "www1p13horg",
                                DataTypes.STRING())
                        // to longer than from
                        .testResult(
                                $("f1").translate("abc", "12345"),
                                "TRANSLATE(f1, 'abc', '12345')",
                                "www.1p13he.org",
                                DataTypes.STRING())
                        // duplicate chars in from
                        .testResult(
                                $("f1").translate("abcae", "12345"),
                                "TRANSLATE(f1, 'abcae', '12345')",
                                "www.1p13h5.org",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").translate("...", "123"),
                                "TRANSLATE(f1, '...', '123')",
                                "www1apache1org",
                                DataTypes.STRING())
                        // case sensitive
                        .testResult(
                                $("f1").translate("ABCDE", "12345"),
                                "TRANSLATE(f1, 'ABCDE', '12345')",
                                "www.apache.org",
                                DataTypes.STRING())
                        // Unicode
                        .testResult(
                                $("f3").translate("翻译测试test，", "测试翻译tset。"),
                                "TRANSLATE(f3, '翻译测试test，', '测试翻译tset。')",
                                "测试tset。翻译",
                                DataTypes.STRING())
                        .testResult(
                                $("f3").translate("翻译测试test，", "test翻译  "),
                                "TRANSLATE(f3, '翻译测试test，', 'test翻译  ')",
                                "te翻译 翻st",
                                DataTypes.STRING())
                        .testResult(
                                $("f4").translate(".\uD83D\uDE00", "\uD83D\uDE00."),
                                "TRANSLATE(f4, '.\uD83D\uDE00', '\uD83D\uDE00.')",
                                "www\uD83D\uDE00.\uD83D\uDE00org",
                                DataTypes.STRING())
                        .testResult(
                                $("f4").translate("\uD83D\uDE00w", "笑α"),
                                "TRANSLATE(f4, '\uD83D\uDE00w', '笑α')",
                                "ααα.笑.org",
                                DataTypes.STRING())
                        // return type
                        .testResult(
                                lit("www.apache.org").translate("abc", "123"),
                                "TRANSLATE('www.apache.org', 'abc', '123')",
                                "www.1p13he.org",
                                DataTypes.STRING().notNull())
                        // dict reuse (coverage)
                        .testResult(
                                lit("www.apache.org")
                                        .translate("abc", "123")
                                        .translate("abc", "123"),
                                "TRANSLATE(TRANSLATE('www.apache.org', 'abc', '123'), 'abc', '123')",
                                "www.1p13he.org",
                                DataTypes.STRING().notNull())
                        // normal cases
                        .testResult(
                                $("f1").translate("abc", "123"),
                                "TRANSLATE(f1, 'abc', '123')",
                                "www.1p13he.org",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").translate("abc", "ABC"),
                                "TRANSLATE(f1, 'abc', 'ABC')",
                                "www.ApAChe.org",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").translate("abcworg", "123 "),
                                "TRANSLATE(f1, 'abcworg', '123 ')",
                                "   .1p13he.",
                                DataTypes.STRING()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.TRANSLATE, "Validation Error")
                        .onFieldsWithData(12345)
                        .andDataTypes(DataTypes.INT())
                        .testTableApiValidationError(
                                $("f0").translate("3", "5"),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "TRANSLATE3(expr <CHARACTER_STRING>, fromStr <CHARACTER_STRING>, toStr <CHARACTER_STRING>)")
                        .testSqlValidationError(
                                "TRANSLATE(f0, '3', '5')",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "TRANSLATE3(expr <CHARACTER_STRING>, fromStr <CHARACTER_STRING>, toStr <CHARACTER_STRING>)"));
    }
}
