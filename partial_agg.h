#ifndef PARTIAL_AGG_H
#define PARTIAL_AGG_H

#include "postgres.h"
#include "funcapi.h"
#include "nodes/pg_list.h"
#include "xrg.h"
#include "kitesdk.h"

typedef struct agg_p_target_t agg_p_target_t;
struct agg_p_target_t {
	Oid aggfn;
	int pgattr;
	List *attrs;
	bool gbykey;
	void *data;
};


typedef struct xrg_agg_p_t xrg_agg_p_t;
struct xrg_agg_p_t {
	int ncol;

	List *groupby_attrs;
	List *aggfnoids;
	List *retrieved_attrs;

	xrg_attr_t *attr;
	bool reached_eof;

	int ntlist;
	agg_p_target_t *tlist;
};

xrg_agg_p_t *xrg_agg_p_init(List *retrieved_attrs, List *aggfnoids, List *groupby_attrs);
int xrg_agg_p_get_next(xrg_agg_p_t *agg, xrg_iter_t *iter, AttInMetadata *attinmeta, Datum *datums, bool *flag, int n);

void xrg_agg_p_destroy(xrg_agg_p_t *agg);

#endif


