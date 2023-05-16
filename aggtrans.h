#ifndef _AGGTRANS_H_
#define _AGGTRANS_H_

Datum avg_p_int64(PG_FUNCTION_ARGS);
Datum avg_p_int128(PG_FUNCTION_ARGS);
Datum avg_p_numeric(PG_FUNCTION_ARGS);

Datum sum_p_int64(PG_FUNCTION_ARGS);
Datum sum_p_int128(PG_FUNCTION_ARGS);
Datum sum_p_numeric(PG_FUNCTION_ARGS);

Datum avg_p_float8(PG_FUNCTION_ARGS);

PGFunction GetTranscodingFnFromOid(Oid aggfnoid);

Datum transfn_to_polynumaggstate(PG_FUNCTION_ARGS);
Datum transfn_to_numericaggstate(PG_FUNCTION_ARGS);

#endif

