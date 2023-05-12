#ifndef _DECODE_H_
#define _DECODE_H_

#include "postgres.h"
#include "xrg.h"

/* decode functions */
int var_decode(char *data, char flag, xrg_attr_t *attr, int atttypmod, Datum *pg_datum, bool *pg_isnull);

int avg_decode(Oid aggfn, char *data, char flag, xrg_attr_t *attr, int atttypmod, Datum *pg_datum, bool *pg_isnull);

/* partial agg result decode */
int agg_p_decode1(Oid aggfn, char *p1, xrg_attr_t *attr1, int atttypmod, Datum *pg_datum, bool *og_isnull);
int agg_p_decode2(Oid aggfn, char *p1, xrg_attr_t *attr1, char *p2, xrg_attr_t *attr2, int atttypmod, Datum *pg_datum, bool *og_isnull);
#endif
