#include <limits.h>
#include "partial_agg.h"
#include "decode.h"

extern bool aggfnoid_is_avg(int aggfnoid);

static const char *column_next(xrg_attr_t *attr, const char *p) {
	if (attr->itemsz > 0) {
		p +=  attr->itemsz;
	} else {
		p += xrg_bytea_len(p) + 4;
	}

	return p;
}

static int get_ncol_from_aggfnoids(List *aggfnoids) {
	ListCell *lc;
	int i = 0;

	foreach (lc, aggfnoids) {
		int fn = lfirst_oid(lc);
		if (aggfnoid_is_avg(fn)) {
			i += 2;
		} else {
			i++;
		}
	}
	return i;
}

#if 0
static void finalize(void *context, const void *rec, void *data, AttInMetadata *attinmeta,
       	Datum *datums, bool *flags, int ndatum) {
	xrg_agg_p_t *agg = (xrg_agg_p_t *) context;
	void **translist = (void  **)data;
	const char *p = rec;
	xrg_attr_t *attr = agg->attr;

	for (int i = 0 ; i < agg->ntlist ; i++) {
		agg_p_target_t *tgt = &agg->tlist[i];
		int k = tgt->pgattr;
		void *transdata = translist[i];
		Oid aggfn = tgt->aggfn;
		int typmod = (attinmeta) ? attinmeta->atttypmods[k-1] : 0;

		// datums[k] =  value[i]
		if (transdata) {
			int top = list_length(tgt->attrs);
			// finalize_aggregate();
			if (aggfnoid_is_avg(aggfn)) {
				//finalize_avg();
				avg_decode(aggfn, transdata, 0, attr, typmod, &datums[k-1], &flags[k-1]);
			} else {
				var_decode(transdata, 0, attr, typmod, &datums[k-1], &flags[k-1]);
			}

			for (int j = 0 ; j < top ; j++) {
				p = column_next(attr, p);
				attr++;
			}
		} else {
			// MUST advance the next pointer first because bytea size header will be altered to match postgres
			const char *p1 = p;
			xrg_attr_t *attr1 = attr;
			p = column_next(attr++, p);
			var_decode((char *) p1, 0, attr1, typmod, &datums[k-1], &flags[k-1]);
		}
	}

	if (translist) {
		free(translist);
	}
}

#endif

static void build_tlist(xrg_agg_p_t *agg) {

	int i = 0, j=0;
	ListCell *lc;
	agg_p_target_t *tlist = 0;
	int attrlen = list_length(agg->retrieved_attrs);
	int aggfnlen = list_length(agg->aggfnoids);

	if (attrlen != aggfnlen) {
		elog(ERROR, "build_tlist: attrlen != aggfnlen");
		return;
	}
	agg->ntlist = aggfnlen;

	tlist = (agg_p_target_t*) malloc(sizeof(agg_p_target_t) * agg->ntlist);
	if (!tlist) {
		elog(ERROR, "out of memory");
		return;
	}

	memset(tlist, 0, sizeof(agg_p_target_t) * agg->ntlist);

	i = 0;
	foreach (lc, agg->retrieved_attrs) {
		tlist[i].pgattr = lfirst_int(lc);
		i++;
	}

	i = j = 0;
	foreach (lc, agg->aggfnoids) {
		tlist[i].aggfn = lfirst_oid(lc);
		tlist[i].attrs = lappend_int(tlist[i].attrs, j++);
		if (aggfnoid_is_avg(tlist[i].aggfn)) {
			tlist[i].attrs = lappend_int(tlist[i].attrs, j++);
		}
		i++;
	}

	if (agg->groupby_attrs) {
		foreach (lc, agg->groupby_attrs) {
			int gbyidx = lfirst_int(lc);
			for (int i = 0 ; i < agg->ntlist ; i++) {
				int idx = linitial_int(tlist[i].attrs);
				if (gbyidx == idx) {
					tlist[i].gbykey = true;
				}
			}
		}
	}

	agg->tlist = tlist;
}


xrg_agg_p_t *xrg_agg_p_init(List *retrieved_attrs, List *aggfnoids, List *groupby_attrs) {

	xrg_agg_p_t *agg = (xrg_agg_p_t*) malloc(sizeof(xrg_agg_p_t));
	Assert(agg);

	agg->reached_eof = false;
	agg->attr = 0;
	agg->retrieved_attrs = retrieved_attrs;
	agg->aggfnoids = aggfnoids;
	agg->groupby_attrs = groupby_attrs;
	build_tlist(agg);

	Assert(aggfnoids);
	agg->ncol = get_ncol_from_aggfnoids(aggfnoids);

	return agg;
}

void xrg_agg_p_destroy(xrg_agg_p_t *agg) {
	if (agg) {
		if (agg->attr) {
			free(agg->attr);
		}
		if (agg->tlist) {
			free(agg->tlist);
		}

		free(agg);
	}
}


int xrg_agg_p_get_next(xrg_agg_p_t *agg, xrg_iter_t *iter, AttInMetadata *attinmeta, Datum *datums, bool *flags, int n) {

	return 1;
}


