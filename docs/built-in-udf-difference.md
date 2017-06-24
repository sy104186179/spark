
Spark version: https://github.com/apache/spark/commit/dec9aa3b37c01454065a4d8899859991f43d4c66

Hive version: 1.2.2

MySQL version: 5.7.18

Oracle version: 11.2.0.3.0


## mathExpressions.scala

udf name | desc diff | spark | hive | mysql | oracle
---|---|---|---|---|---
ACOS, ASIN | All support string type, ACOS(2) result difference. | ACOS(2) -> NaN | ACOS(2) -> NaN | ACOS(2) -> null | ACOS(2) -> ORA-01428
CBRT | MySQL and Oracle doesn't support this function, Spark support string type, but Hive isn't, CBRT('26') result difference. | CBRT('26') -> 3.0 | CBRT('26') -> SemanticException | - | -
CEIL, FLOOR | All support string type. | - | - | - | -
COS, SIN, TAN | All support string type. | - | - | - | -
COSH, SINH, TANH | Only Spark support this function. | - | - | - | -
CONV | Oracle doesn't support this function. | - | - | - | -
CONV | Oracle doesn't support this function. | - | - | - | -
EXPM1 | Only Spark support this function. | - | - | - | -
LOG | LOG(1, 100) result difference. | LOG(1, 100) -> Infinity | LOG(1, 100) -> Null | LOG(1, 100) -> Null | LOG(1, 100) -> ORA-01428
Log2, Log10 | Oracle doesn't support this function. | - | - | - | -
Log1p | Only Spark support this function. | - | - | - | -
rint | Only Spark support this function. | - | - | - | -
SIGNUM | Only Spark support this function. | - | - | - | -
SQRT | Only Spark support this function， SQRT(-10) result difference. | SQRT(-10) -> Nan | SQRT(-10) -> Null | SQRT(-10) -> Null | SQRT(-10) -> ORA-01428
COT | Only Spark and MySQL support this function. | - | - | - | -
DEGREES | Oracle doesn't support this function. | - | - | - | -
Bin | Oracle doesn't support this function, Hive doesn't support double and string type. | - | - | - | -
Hex, Unhex | Oracle doesn't support this function. | - | - | - | -
Atan2 | Hive doesn't support. | - | - | - | -
Pow | Oracle doesn't support this function. | - | - | - | -
ShiftLeft, ShiftRight, ShiftRightUnsigned | Spark and Hive support this function, Hive doesn't support string type. | - | - | - | -
Hypot | Only Spark support this function. | - | - | - | -
Round | Round(25E-1) result difference | Round(25E-1) -> 3.0 | Round(25E-1) -> 3.0 | Round(25E-1) -> 2.0 | Round(25E-1) -> 3.0
BRound | Only Spark support this function. | - | - | - | -

## arithmetic.scala
udf name | desc diff | spark | hive | mysql | oracle | PR
---|---|---|---|---|---|---
Pmod | Spark and Hive support this function, Pmod(10,0) result difference. | Pmod(10,0) -> ArithmeticException: | Pmod(10,0) -> null | - | - | -
Least, Greatest | Spark and Hive doesn't support string type, Hive doesn't support double type. | - | - | - | - | -

## stringExpressions.scala

Only lpad/rpad, soundex result difference

udf name | desc diff | spark | hive | mysql | oracle | PR 
---|---|---|---|---|---|---
lpad | lpad('hello', -2, '') result difference.| lpad('hello', -2, '') -> Empty | lpad('hello', -2, '') -> ArrayIndexOutOfBoundsException null | lpad('hello', -2, '') -> null | lpad('hello', -2, '') -> null | [18138](https://github.com/apache/spark/pull/18138) 
soundex | soundex('1') | soundex('1') -> 1 | soundex('1') -> Empty | soundex('1') -> 1 | soundex('1') -> null | - 
