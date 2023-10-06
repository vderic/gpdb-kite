#ifndef _DECODE_H_
#define _DECODE_H_

#include "postgres.h"
#include "xrg.h"

/* decode functions */
int var_decode(char *data, char flag, xrg_attr_t *attr, Oid atttypid, int atttypmod, Datum *pg_datum, bool *pg_isnull, bool int128_to_numeric);

int avg_decode(Oid aggfn, char *data, char flag, xrg_attr_t *attr, Oid atttypid, int atttypmod, Datum *pg_datum, bool *pg_isnull);

int sum_float_decode(Oid aggfn, char *data, char flag, xrg_attr_t *attr, Oid atttypid, int atttypmod, Datum *pg_datum, bool *pg_isnull);

/* partial agg result decode */
int agg_p_decode1(Oid aggfn, char *p1, char flag, xrg_attr_t *attr1, Oid atttypid, int atttypmod, Datum *pg_datum, bool *og_isnull);
int agg_p_decode2(Oid aggfn, char *p1, char f1, xrg_attr_t *attr1, char *p2, char f2, xrg_attr_t *attr2, Oid atttypid, int atttypmod, Datum *pg_datum, bool *og_isnull);
#endif
