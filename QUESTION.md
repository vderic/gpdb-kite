Questions on postgres_fdw
=========================

1. For sum with numeric, gpdb7 need array(count(c1), sum(c1).

Only expect sum(c1) without count(c1)


2. For avg with float, double, gpdb7 needs array(count(c1), sum(c1), count(c1) * var_pop(c1)]

Only expect sum(c1), count(c1) without count(c1) * var_pop(c1)

3. For example SQL, 

```
SELECT avg(c1), avg(c2) from table where c1 > 3;
```

the generated SQL is as below,

```
SELECT array[count(c1) filter (where c1 > 3), sum(c1) filter (where  c1 > 3)], array[count(c2) filter (where c1 > 3) from table where c1 > 3;
```


The expect SQL is as below,

```
SELECT array[count(c1), sum(c1)], array[count(c2), sum(c2)] from table where c1 > 3
```

4. In transcoding.c, a lot of code copied from numeric.c.  Can you expose the data structure and functions in numeric.h instead?


5. Average for float and double is not done yet.

You can use this data structure to treat as ArrayType that contain three double values


```
typedef struct ExxFloatAvgTransdata
{
        ArrayType arraytype;
        int32   nelem;
        float8  data[3];     // float8[3]
} ExxFloatAvgTransdata;

ExxFloatAvgTransdata avgtrans;
ExxFloatAvgTransdata *tr0;

tr0 = &avgtrans;
SET_VARSIZE(tr0, sizeof(ExxFloatAvgTransdata));
tr0->arraytype.ndim = 1;
tr0->arraytype.dataoffset = (char*) tr0->data - (char*)tr0;
tr0->arraytype.elemtype = FLOAT8OID;
tr0->nelem = 3;
tr0->data[0] =  N;
tr0->data[1] =  sumX;
tr0->data[2] = sumX * sumX;


ArrayType *arr = (ArrayType*) tr0;

```

Do ArrayType need serialize function? ArrayType is already a varlena type and wondering it is already serialized?



