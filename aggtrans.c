#include "postgres.h"
#include "utils/array.h"
#include "utils/fmgrprotos.h"
#include "utils/numeric.h"
#include "nodes/execnodes.h"

#include "aggtrans.h"

#define INIT_AGGSTATE(aggstate) \
{ \
	Node *node = (Node *) &aggstate; \
	memset(&aggstate, 0, sizeof(AggState)); \
	node->type = T_AggState; \
}

static Datum CallAggfunction1(FmgrInfo *flinfo, Datum arg1, fmNodePtr *context)
{
	LOCAL_FCINFO(fcinfo, 1);

	InitFunctionCallInfoData(*fcinfo, flinfo, 1, InvalidOid, (Node *)context, NULL);
	fcinfo->args[0].value = arg1;
	fcinfo->args[0].isnull = false;

	return FunctionCallInvoke(fcinfo);
}

/* KITE */
/* int */
static Datum agg_p_int128(int64 count, int128 sum) {
	PolyNumAggState *state;
	AggState aggstate;
	FmgrInfo flinfo;


	state = makePolyNumAggStateCurrentContext(false);
	state->N = count;
#ifdef HAVE_INT128
	state->sumX = sum;
#else
	// error out
	elog(ERROR, "system not support int128");
#endif

	memset(&flinfo, 0, sizeof(FmgrInfo));
	fmgr_info_cxt(fmgr_internal_function("int8_avg_serialize"), &flinfo, CurrentMemoryContext);

	INIT_AGGSTATE(aggstate);
	return CallAggfunction1(&flinfo, (Datum)state, (fmNodePtr *)&aggstate);
}

/* Int8TransTypeData. expected 2-element int8 array (numeric.c:5799) */
Datum avg_p_int64(PG_FUNCTION_ARGS) {
	int64 count = PG_GETARG_INT64(0);
	int64 sum = PG_GETARG_INT64(1);

	int sz = ARR_OVERHEAD_NONULLS(1);
	sz += 2 * sizeof(int64_t);
	ArrayType *arr = (ArrayType *) palloc(sz);
	SET_VARSIZE(arr, sz);
	arr->ndim = 1;
	arr->dataoffset = 0;
	arr->elemtype =  INT8OID;
	int64_t *p = (int64_t *) ARR_DATA_PTR(arr);
	p[0] = count;
	p[1] = sum;

	PG_RETURN_ARRAYTYPE_P(arr);
}

Datum avg_p_int128(PG_FUNCTION_ARGS) {
	int64 count = PG_GETARG_INT64(0);
	int128 sum = *((int128 *) PG_GETARG_POINTER(1));
	return agg_p_int128(count, sum);
}

Datum sum_p_int64(PG_FUNCTION_ARGS) {
	int64 count = 1;
	int64 sum = PG_GETARG_INT64(0);
	return agg_p_int128(count, sum);
}

Datum sum_p_int128(PG_FUNCTION_ARGS) {
	int64 count = 1;
	int128 sum = *((int128 *) PG_GETARG_POINTER(0));
	return agg_p_int128(count, sum);
}


/* numeric */
static Datum agg_p_numeric(int64 count, Numeric sum) {
	NumericAggState *state;
	AggState aggstate;
	FmgrInfo flinfo;

	state = makeNumericAggStateCurrentContext(false);
	state->N = count;
	do_numeric_accum(state, sum);
	state->N--;

	memset(&flinfo, 0, sizeof(FmgrInfo));
	fmgr_info_cxt(fmgr_internal_function("numeric_avg_serialize"), &flinfo, CurrentMemoryContext);

	INIT_AGGSTATE(aggstate);
	return CallAggfunction1(&flinfo, (Datum)state, (fmNodePtr *)&aggstate);
}

Datum avg_p_numeric(PG_FUNCTION_ARGS) {
	int64 count = (int64) PG_GETARG_INT64(0);
	Numeric sum = PG_GETARG_NUMERIC(1);
	return agg_p_numeric(count, sum);
}

Datum sum_p_numeric(PG_FUNCTION_ARGS) {
	int64 count = 1;
	Numeric sum = PG_GETARG_NUMERIC(0);
	return agg_p_numeric(count, sum);
}

/* float8 */
Datum avg_p_float8(PG_FUNCTION_ARGS) {
	int64 count = PG_GETARG_INT64(0);
	float8 sum = PG_GETARG_FLOAT8(1);
	float8 N = count;
	float8 Sxx = 0;
        Datum           transdatums[3];
        ArrayType  *result;

        transdatums[0] = Float8GetDatumFast(N);
        transdatums[1] = Float8GetDatumFast(sum);
        transdatums[2] = Float8GetDatumFast(Sxx);

        result = construct_array(transdatums, 3,
                                                           FLOAT8OID,
                                                           sizeof(float8), FLOAT8PASSBYVAL, 'd');

        PG_RETURN_ARRAYTYPE_P(result);
}

PGFunction GetTranscodingFnFromOid(Oid aggfnoid) {
	PGFunction refnaddr = NULL;
	if (aggfnoid == InvalidOid) 
	{
		return NULL;
	}
	switch (aggfnoid) 
	{
		case 2100:  // avg bigint
			/* - 2100 pg_catalog.avg int8|bigint */
			refnaddr = avg_p_int128;
			break;
		case 2101:  // avg integer
		case 2102:  // avg smallint
			refnaddr = avg_p_int64;
			break;
		case 2107:
			 /* - 2107 pg_catalog.sum int8|bigint */
			refnaddr = sum_p_int128;
			break;
		case 2103:
			/* - 2103 pg_catalog.avg numeric */
			refnaddr = avg_p_numeric;
			break;
		case 2114:
			 /* - 2114 pg_catalog.sum numeric */
			refnaddr = sum_p_numeric;
			break;
		case 2104:
		case 2105:
			/* - 2105 pg_catalog.avg float8 */
			refnaddr = avg_p_float8;
			break;
		default:
			break;
	}
	return refnaddr;
}
