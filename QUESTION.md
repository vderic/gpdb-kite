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
SELECT array[count(c1) filter (where c1 > 3), sum(c1) filter (where  c1 > 3)], 
array[count(c2) filter (where c1 > 3), sum(c2) filter(where c1 > 3)] from table where c1 > 3;
```


The expect SQL is as below,

```
SELECT array[count(c1), sum(c1)], array[count(c2), sum(c2)] from table where c1 > 3
```

4. In transcoding.c, a lot of code copied from numeric.c.  Can you expose the data structure and functions in numeric.h or new header file numeric_api.h instead?

Suggested APIs

```

Datum int64_avg_aggstate_create(PG_FUNCTION_ARGS)
{
	int64 count = PG_GETARG_INT64(0);
	int64 sum = PG_GETARG_INT64(1);


	return serialized_polynumaggstate;
}


Datum int128_avg_aggstate_create(PG_FUNCTION_ARGS)
{
	int64 count = PG_GETARG_INT64(0);
	int128 sum = *((int128 *) PG_GETARG_POINTER(1));

	return serialized_polynumaggstate;
}

Datum numeric_avg_aggstate_create(PG_FUNCTION_ARGS)
{
	int64 count = (int64) PG_GETARG_INT64(0);
	Numeric sum = PG_GETARG_NUMERIC(1);

	return serialized_numericaggstate;
}

Datum float8_avg_aggstate_create(PG_FUNCTION_ARGS)
{
	int64 count = PG_GETARG_INT64(0);
	float8 sum = PG_GETARG_FLOAT8(1);

	return arraytype;
}

Datum int64_sum_aggstate_create(PG_FUNCTION_ARGS)
{
	int64 sum = PG_GETARG_INT64(0);

	return serialized_polynumaggstate;
}


Datum int128_sum_aggstate_create(PG_FUNCTION_ARGS)
{
	int128 sum = *((int128 *) PG_GETARG_POINTER(0));

	return serialized_polynumaggstate;
}

Datum numeric_sum_aggstate_create(PG_FUNCTION_ARGS)
{
	Numeric sum = PG_GETARG_NUMERIC(0));

	return serialized_numericaggstate;
}

```

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


6. How to get the Segment ID and total number of segments?

