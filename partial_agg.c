#include <limits.h>
#include "partial_agg.h"
#include "decode.h"

extern bool aggfnoid_is_avg(int aggfnoid);

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
		if (agg->tlist) {
			free(agg->tlist);
		}

		free(agg);
	}
}


int xrg_agg_p_get_next(xrg_agg_p_t *agg, xrg_iter_t *iter, AttInMetadata *attinmeta, Datum *datums, bool *flags, int n) {

	if (iter->nvec != agg->ncol) {
		elog(ERROR, "xrg_agg_p_get_next: number of columns returned from kite not match (%d != %d)", 
				agg->ncol, iter->nvec);
		return 1;
	}

	for (int i = 0, j = 0 ; i < agg->ntlist && j < iter->nvec ; i++) {
		agg_p_target_t *tgt = &agg->tlist[i];
		int k = tgt->pgattr;
		int nkiteattr = list_length(tgt->attrs);
		Oid aggfn = tgt->aggfn;
		int typmod = (attinmeta) ? attinmeta->atttypmods[k-1] : 0;

		if (nkiteattr == 1) {
			char *p = iter->value[j];
			xrg_attr_t *attr = &iter->attr[j];
			j++;

			agg_p_decode1(aggfn, p, attr, typmod, &datums[k-1], &flags[k-1]);

		} else if (nkiteattr == 2) {
			char *p1, *p2;
			xrg_attr_t *attr1, *attr2;
			p1 = iter->value[j];
			attr1 = &iter->attr[j];
			j++;
			p2 = iter->value[j];
			attr2 = &iter->attr[j];
			j++;
			agg_p_decode2(aggfn, p1, attr1, p2, attr2, typmod, &datums[k-1], &flags[k-1]);
		} else {
			elog(ERROR, "xrg_agg_p_get_next: aggregate function won't have more than 2 columns");
			return 1;
		}
	}

	return 0;
}


