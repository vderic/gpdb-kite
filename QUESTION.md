Questions on postgres_fdw
=========================


1. for sum with numeric, gpdb7 need array(count(c1), sum(c1).  

only expect sum(c1) without count(c1)


2. for avg with float, double, gpdb7 needs array(count(c1), sum(c1), count(c1) * var_pop(c1)]

only expect sum(c1), count(c1) without count(c1) * var_pop(c1)

3.  The generated SQL is something like before.

For example SQL, select avg(c1), avg(c2) from table where c1 > 3;

the generated SQL is as below,

```
select array[count(c1) filter (where c1 > 3), sum(c1) filter (where  c1 > 3)], array[count(c2) filter (where c1 > 3) from table where c1 > 3;
```


The expect SQL is as below,

```
expected SQL is "select array[count(c1), sum(c1)], array[count(c2), sum(c2)] from table where c1 > 3
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
tr0->data[2] = 0;   // sumX2


ArrayType *arr = (ArrayType*) tr0;

```



